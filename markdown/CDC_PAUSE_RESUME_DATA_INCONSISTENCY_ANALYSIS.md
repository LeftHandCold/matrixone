# CDC 暂停/恢复导致数据不一致问题分析

## 问题描述

在频繁暂停和恢复 CDC 任务后，时间久了会出现**上游表和下游表数据行数不一致**的情况。

## 根本原因分析

### 核心问题：Watermark 更新时机与数据提交的时序不一致

CDC 的设计遵循"允许滞后，禁止超前"的一致性模型：
- ✅ **允许滞后**：watermark 可以滞后于实际进度（导致重复处理，可接受）
- ❌ **禁止超前**：watermark 绝不能超前于已持久化的数据（会导致数据丢失）

但在暂停/恢复场景下，存在以下时序问题：

### 问题1：异步提交导致的状态不一致 ⚠️ **高风险**

**位置**: `pkg/cdc/reader_v2_txn_manager.go:178-199`

**问题流程**:
```go
// CommitTransaction 的执行顺序：
1. tm.sinker.SendCommit()        // 将 COMMIT 命令放入 channel（异步，不等待执行）
2. tm.sinker.SendDummy()         // 确保命令被发送（但不等待执行完成）
3. tm.sinker.Error()             // 检查之前的错误状态（不是 COMMIT 执行的结果）
4. tm.watermarkUpdater.UpdateWatermarkOnly()  // 更新 watermark（此时 COMMIT 可能还没真正执行）
```

**关键问题**：
- `SendCommit()` 只是将命令放入 channel，**不等待真正执行完成**
- `Error()` 检查的是**之前的错误状态**，不是 COMMIT 执行的结果
- 如果 COMMIT 在 `UpdateWatermarkOnly()` 之后失败（比如 context canceled、网络问题），watermark 已经被更新了

**暂停场景下的问题**：
```
时间线：
T1: 数据发送到 Sinker channel
T2: SendCommit() - COMMIT 命令放入 channel
T3: UpdateWatermarkOnly() - watermark 更新到 cacheUncommitted
T4: 用户执行 PAUSE CDC TASK
T5: context 被 canceled
T6: Sinker 的 consumer goroutine 收到 cancel，COMMIT 执行失败（或部分执行）
T7: CronJob 每 3 秒将 cacheUncommitted 持久化到数据库
T8: 结果：数据可能没有成功提交到目标数据库，但 watermark 已经被更新
```

**恢复场景下的影响**：
```
T9: 用户执行 RESUME CDC TASK
T10: 从数据库读取 watermark（已经是 T3 的值）
T11: 从 watermark 继续读取数据
T12: 结果：跳过了 T1-T2 之间的数据（因为 watermark 已经更新，但数据可能没提交）
```

### 问题2：Watermark 异步持久化延迟 ⚠️ **中风险**

**位置**: `pkg/cdc/watermark_updater.go:1348-1378`

**问题流程**:
```go
// UpdateWatermarkOnly 只是将 watermark 放入内存缓存
u.cacheUncommitted[*key] = *watermark  // 立即执行

// CronJob 每 3 秒才批量持久化到数据库
cronRun() {
    // 将 cacheUncommitted 移动到 cacheCommitting
    // 然后批量 UPDATE 到数据库
}
```

**暂停场景下的问题**：
```
时间线：
T1: CommitTransaction() 调用 UpdateWatermarkOnly()
T2: watermark 更新到 cacheUncommitted（内存）
T3: 用户执行 PAUSE CDC TASK（在 CronJob 下次执行之前）
T4: 如果系统崩溃或任务被强制停止，cacheUncommitted 中的 watermark 丢失
T5: 恢复时，从数据库读取的 watermark 是旧值
T6: 结果：重复处理数据（可接受，但可能导致下游数据重复）
```

**但更严重的情况**：
```
时间线：
T1: 数据已经成功提交到目标数据库
T2: CommitTransaction() 调用 UpdateWatermarkOnly()
T3: watermark 更新到 cacheUncommitted（内存）
T4: 用户执行 PAUSE CDC TASK
T5: CronJob 还没来得及持久化 watermark（还在 cacheUncommitted 中）
T6: 如果系统崩溃，cacheUncommitted 丢失
T7: 恢复时，从数据库读取的 watermark 是旧值
T8: 从旧 watermark 重新读取数据
T9: 结果：数据重复插入到目标数据库（如果目标数据库没有主键冲突检查）
```

### 问题3：暂停时未完成事务的处理 ⚠️ **中风险**

**位置**: `pkg/frontend/cdc_exector.go:455-497`

**问题流程**:
```go
// Pause 操作
func (exec *CDCTaskExecutor) Pause() error {
    // 1. 取消 context
    exec.activeRoutine.Cancel()
    
    // 2. 等待 goroutine 退出
    // 但是，如果 TableChangeStream 正在处理事务，可能：
    // - 数据已经发送到 Sinker channel
    // - 但 COMMIT 还没有执行
    // - 或者 COMMIT 执行了但 watermark 还没更新
}
```

**暂停场景下的问题**：
```
时间线：
T1: TableChangeStream 正在处理一批数据
T2: 数据发送到 Sinker channel（包括 INSERT/UPDATE/DELETE）
T3: SendBegin() - BEGIN 命令已发送
T4: 用户执行 PAUSE CDC TASK
T5: context 被 canceled
T6: TableChangeStream 退出循环
T7: Sinker 的 consumer goroutine 收到 cancel
T8: 可能的情况：
    a) COMMIT 还没发送 → 事务回滚 → 数据丢失
    b) COMMIT 已发送但还没执行 → 事务可能提交也可能回滚（不确定）
    c) COMMIT 已执行但 watermark 还没更新 → 数据已提交，但 watermark 是旧值
```

**恢复场景下的影响**：
```
T9: 用户执行 RESUME CDC TASK
T10: 从数据库读取 watermark（旧值）
T11: 从旧 watermark 重新读取数据
T12: 结果：
    - 情况 a: 数据丢失（下游没有，上游有）
    - 情况 b: 数据可能重复或丢失（不确定）
    - 情况 c: 数据重复（下游已有，但重新读取）
```

### 问题4：UpdateWatermarkOnly 直接覆盖，不检查大小 ⚠️ **低风险**

**位置**: `pkg/cdc/watermark_updater.go:1075-1090`

**问题流程**:
```go
func (u *CDCWatermarkUpdater) UpdateWatermarkOnly(...) {
    u.Lock()
    oldWatermark, hasOld := u.cacheUncommitted[*key]
    u.cacheUncommitted[*key] = *watermark  // 直接覆盖，不检查大小
    u.Unlock()
}
```

**问题**：
- 如果多个批次同时调用 `UpdateWatermarkOnly`，后面的批次会覆盖前面的批次
- 如果后面的 watermark 更小（比如由于并发或时序问题），会导致 watermark 回退
- 虽然这种情况应该很少见，但在高并发暂停/恢复场景下可能发生

## 具体场景分析

### 场景1：暂停时数据已提交但 watermark 未持久化

**现象**：
- 上游表有 1000 行数据
- 下游表只有 950 行数据
- 差异：50 行数据丢失

**原因**：
1. 50 行数据已经成功提交到目标数据库
2. `UpdateWatermarkOnly()` 将 watermark 更新到 `cacheUncommitted`
3. 用户执行 PAUSE（在 CronJob 下次执行之前）
4. 如果系统崩溃或任务被强制停止，`cacheUncommitted` 丢失
5. 恢复时，从数据库读取的 watermark 是旧值
6. 从旧 watermark 重新读取，但数据已经提交过了
7. 由于某些原因（比如主键冲突、唯一约束），数据没有重复插入
8. 结果：数据丢失

### 场景2：暂停时数据未提交但 watermark 已更新

**现象**：
- 上游表有 1000 行数据
- 下游表有 1050 行数据
- 差异：50 行数据重复

**原因**：
1. 50 行数据发送到 Sinker channel
2. `SendCommit()` 将 COMMIT 命令放入 channel
3. `UpdateWatermarkOnly()` 更新 watermark（此时 COMMIT 可能还没执行）
4. 用户执行 PAUSE
5. context 被 canceled，COMMIT 执行失败或部分执行
6. CronJob 将 watermark 持久化到数据库
7. 恢复时，从新 watermark 继续读取
8. 但之前的数据可能已经部分提交或完全未提交
9. 重新读取时，数据重复插入
10. 结果：数据重复

### 场景3：频繁暂停/恢复导致累积误差

**现象**：
- 长时间运行后，数据差异逐渐增大
- 每次暂停/恢复后，差异可能增加或减少

**原因**：
- 每次暂停/恢复都可能引入小的误差
- 误差累积导致长时间运行后差异明显
- 误差的方向不确定（可能丢失也可能重复）

## 解决方案建议

### 方案1：确保 COMMIT 成功后才更新 watermark（推荐）✅

**思路**：改变 `CommitTransaction` 的流程，只有在确认 COMMIT 真正成功后才更新 watermark。

**实现方式**：
```go
func (tm *TransactionManager) CommitTransaction(ctx context.Context) error {
    // Step 1: Send COMMIT
    tm.sinker.SendCommit()
    tm.sinker.SendDummy()
    
    // Step 2: 等待 COMMIT 真正执行完成
    // 需要添加一个机制来确认 COMMIT 执行成功
    if err := tm.sinker.WaitForCommitComplete(ctx); err != nil {
        return err
    }
    
    // Step 3: 检查错误
    if err := tm.sinker.Error(); err != nil {
        return err
    }
    
    // Step 4: 更新 watermark（此时 COMMIT 已经确认成功）
    toTs := tm.tracker.GetToTs()
    if err := tm.watermarkUpdater.UpdateWatermarkOnly(ctx, tm.watermarkKey, &toTs); err != nil {
        return err
    }
    
    return nil
}
```

**问题**：需要修改 Sinker 接口，添加 `WaitForCommitComplete()` 方法，这可能影响性能。

### 方案2：在暂停时强制刷新 watermark（折中方案）✅

**思路**：在 `Pause()` 操作时，等待所有未完成的事务完成，并强制刷新 watermark 到数据库。

**实现方式**：
```go
func (exec *CDCTaskExecutor) Pause() error {
    // Step 1: 取消 context（停止新的处理）
    exec.activeRoutine.Cancel()
    
    // Step 2: 等待所有 TableChangeStream 退出
    // 确保所有正在处理的事务完成
    
    // Step 3: 强制刷新所有 watermark 到数据库
    exec.watermarkUpdater.ForceFlushAll(context.Background())
    
    // Step 4: 等待刷新完成（最多等待几秒）
    time.Sleep(5 * time.Second)
    
    // Step 5: 完成暂停
    return nil
}
```

**优点**：
- 改动相对较小
- 确保暂停时 watermark 已经持久化

**缺点**：
- 暂停操作可能变慢（需要等待）
- 不能完全解决异步提交的问题

### 方案3：在 UpdateWatermarkOnly 中添加大小检查（防御性）✅

**思路**：防止 watermark 回退，只更新更大的 watermark。

**实现方式**：
```go
func (u *CDCWatermarkUpdater) UpdateWatermarkOnly(
    ctx context.Context,
    key *WatermarkKey,
    watermark *types.TS,
) (err error) {
    u.Lock()
    defer u.Unlock()
    
    oldWatermark, hasOld := u.cacheUncommitted[*key]
    
    // 只更新更大的 watermark（防止回退）
    if !hasOld || watermark.GT(&oldWatermark) {
        u.cacheUncommitted[*key] = *watermark
    } else {
        // 如果新的 watermark 更小，记录警告
        logutil.Warn(
            "cdc.watermark.update_skipped_smaller",
            zap.String("key", key.String()),
            zap.String("old-watermark", oldWatermark.ToString()),
            zap.String("new-watermark", watermark.ToString()),
        )
    }
    
    return nil
}
```

**优点**：
- 防止 watermark 回退
- 改动很小

**缺点**：
- 不能解决主要问题（异步提交）

### 方案4：使用两阶段提交机制（最佳，但改动大）⚠️

**思路**：引入两阶段提交，确保数据提交和 watermark 更新的原子性。

**实现方式**：
1. 第一阶段：准备提交（数据发送到目标数据库，但不提交）
2. 第二阶段：确认提交（确认成功后，更新 watermark，然后提交事务）

**优点**：
- 完全解决一致性问题

**缺点**：
- 需要大幅修改架构
- 可能影响性能

## 推荐方案

**当前最佳方案**：**方案2（暂停时强制刷新）+ 方案3（防止回退）**

1. **方案2**：在暂停时强制刷新 watermark，确保暂停时的状态一致
2. **方案3**：防止 watermark 回退，作为防御性措施

这样可以：
- ✅ 减少暂停/恢复导致的数据不一致
- ✅ 改动相对较小
- ✅ 不影响正常运行的性能

## 测试建议

1. **暂停/恢复压力测试**：
   - 频繁暂停和恢复任务（每 10 秒一次）
   - 持续运行 1 小时
   - 检查数据一致性

2. **崩溃恢复测试**：
   - 在数据提交过程中强制停止任务
   - 恢复任务
   - 检查数据一致性

3. **并发暂停测试**：
   - 多个任务同时暂停
   - 检查 watermark 是否正确持久化

4. **长时间运行测试**：
   - 长时间运行任务（24 小时）
   - 定期暂停/恢复
   - 检查数据一致性

## 监控建议

1. **添加指标**：
   - `cdc_watermark_update_delay`: watermark 更新延迟
   - `cdc_commit_to_watermark_delay`: COMMIT 到 watermark 更新的延迟
   - `cdc_pause_watermark_flush_time`: 暂停时 watermark 刷新时间

2. **添加日志**：
   - 记录每次暂停时的 watermark 状态
   - 记录每次恢复时的 watermark 读取值
   - 记录 watermark 回退的情况

## 相关代码位置

- `pkg/cdc/reader_v2_txn_manager.go:178-199` - CommitTransaction
- `pkg/cdc/watermark_updater.go:1075-1090` - UpdateWatermarkOnly
- `pkg/cdc/watermark_updater.go:1348-1378` - CronJob 持久化
- `pkg/frontend/cdc_exector.go:455-497` - Pause 操作
- `pkg/frontend/cdc_exector.go:346-382` - Resume 操作


