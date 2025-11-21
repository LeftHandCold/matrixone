# CDC 数据不一致根本原因分析（无暂停场景）

## 问题描述

即使**没有暂停操作**，CDC 任务运行一晚上后也会出现**上游表和下游表数据行数不一致**的情况。

## 根本原因：异步 COMMIT 导致 Watermark 提前更新 ⚠️ **严重问题**

### 核心问题

在 `CommitTransaction` 中，存在一个**致命的时序问题**：

```go
// pkg/cdc/reader_v2_txn_manager.go:178-199
func (tm *TransactionManager) CommitTransaction(ctx context.Context) error {
    // Step 1: Send COMMIT to sinker (异步，不等待执行)
    tm.sinker.SendCommit()        // 只是将命令放入 channel
    tm.sinker.SendDummy()         // 确保命令被发送（但不等待执行完成）
    
    // Step 2: Check for errors (检查的是之前的错误，不是 COMMIT 执行的结果)
    if err := tm.sinker.Error(); err != nil {
        return err  // 这里检查的是之前的错误，不是 COMMIT 执行的结果
    }
    
    // Step 3: Update watermark (此时 COMMIT 可能还没真正执行，或者后续会失败)
    tm.watermarkUpdater.UpdateWatermarkOnly(ctx, tm.watermarkKey, &toTs)
    
    return nil  // 返回成功，但 COMMIT 可能还没执行或会失败
}
```

### 问题流程分析

#### 正常流程（理想情况）

```
T1: CommitTransaction() 被调用
T2: SendCommit() - COMMIT 命令放入 channel
T3: SendDummy() - 确保命令被发送
T4: Error() - 检查之前的错误（此时没有错误）
T5: UpdateWatermarkOnly() - 更新 watermark 到 cacheUncommitted
T6: [异步] Sinker 的 consumer goroutine 处理 COMMIT 命令
T7: [异步] executor.CommitTx() - 真正执行 COMMIT
T8: [异步] COMMIT 成功
T9: [异步] CronJob 将 watermark 持久化到数据库
```

#### 问题流程（COMMIT 失败但 watermark 已更新）

```
T1: CommitTransaction() 被调用
T2: SendCommit() - COMMIT 命令放入 channel
T3: SendDummy() - 确保命令被发送
T4: Error() - 检查之前的错误（此时没有错误，通过检查）
T5: UpdateWatermarkOnly() - 更新 watermark 到 cacheUncommitted ✅ watermark 已更新
T6: CommitTransaction() 返回 nil（成功）
T7: [异步] Sinker 的 consumer goroutine 处理 COMMIT 命令
T8: [异步] executor.CommitTx() - 执行 COMMIT
T9: [异步] COMMIT 失败（网络问题、数据库问题、超时等）
T10: [异步] handleCommit() 设置错误状态 s.SetError(err)
T11: [异步] CronJob 将 watermark 持久化到数据库 ✅ watermark 已持久化
T12: 结果：数据没有提交到目标数据库，但 watermark 已经更新
```

### 为什么会导致数据不一致？

#### 场景1：数据丢失（下游数据少于上游）

**现象**：
- 上游表有 1000 行数据
- 下游表只有 950 行数据
- 差异：50 行数据丢失

**原因**：
```
1. 50 行数据发送到 Sinker channel
2. BEGIN 事务成功
3. INSERT 语句执行成功（数据在事务中）
4. CommitTransaction() 被调用
5. SendCommit() - COMMIT 命令放入 channel
6. UpdateWatermarkOnly() - watermark 更新到 cacheUncommitted
7. CommitTransaction() 返回成功
8. [异步] executor.CommitTx() 执行失败（网络问题、数据库连接断开等）
9. 事务回滚，50 行数据丢失
10. 但 watermark 已经更新（或即将被持久化）
11. 下次读取时，从新 watermark 继续，跳过了这 50 行数据
12. 结果：数据丢失
```

#### 场景2：数据重复（下游数据多于上游）

**现象**：
- 上游表有 1000 行数据
- 下游表有 1050 行数据
- 差异：50 行数据重复

**原因**：
```
1. 50 行数据发送到 Sinker channel
2. BEGIN 事务成功
3. INSERT 语句执行成功
4. CommitTransaction() 被调用
5. SendCommit() - COMMIT 命令放入 channel
6. UpdateWatermarkOnly() - watermark 更新到 cacheUncommitted
7. CommitTransaction() 返回成功
8. [异步] executor.CommitTx() 执行成功
9. 数据提交到目标数据库
10. 但后续处理出错（比如 context canceled、其他错误）
11. 任务重启或恢复
12. 从 watermark 重新读取（watermark 可能滞后）
13. 重新读取这 50 行数据
14. 如果目标数据库没有主键冲突检查，数据重复插入
15. 结果：数据重复
```

### 代码证据

#### 1. CommitTransaction 不等待 COMMIT 执行完成

```go
// pkg/cdc/reader_v2_txn_manager.go:178-199
// Step 1: Send COMMIT to sinker
tm.sinker.SendCommit()        // 只是放入 channel，不等待
tm.sinker.SendDummy()         // 确保发送，但不等待执行

// Step 2: Check for errors
if err := tm.sinker.Error(); err != nil {
    return err  // 检查的是之前的错误，不是 COMMIT 执行的结果
}

// Step 3: Update watermark
tm.watermarkUpdater.UpdateWatermarkOnly(ctx, tm.watermarkKey, &toTs)
// 此时 COMMIT 可能还没执行，或者后续会失败
```

#### 2. SendCommit 是异步的

```go
// pkg/cdc/sinker_v2.go:813-816
func (s *mysqlSinker2) SendCommit() {
    s.sendCommand(NewCommitCommand())  // 只是放入 channel
}

func (s *mysqlSinker2) sendCommand(cmd *Command) {
    // ...
    cmdCh <- cmd  // 异步发送，不等待处理
}
```

#### 3. handleCommit 在独立的 goroutine 中执行

```go
// pkg/cdc/sinker_v2.go:408-444
func (s *mysqlSinker2) handleCommit(ctx context.Context) error {
    // 这个函数在 Sinker.Run() 的 goroutine 中异步执行
    if err := s.executor.CommitTx(ctx); err != nil {
        s.SetError(err)  // 设置错误，但 CommitTransaction 已经返回了
        return err
    }
    // ...
}
```

#### 4. Error() 检查的是之前的错误状态

```go
// pkg/cdc/sinker_v2.go:831-840
func (s *mysqlSinker2) Error() error {
    errPtr := s.err.Load()
    if errPtr == nil {
        return nil
    }
    return *errPtr  // 返回的是之前设置的错误，不是 COMMIT 执行的结果
}
```

## 其他潜在问题

### 问题2：Watermark 异步持久化延迟

**位置**: `pkg/cdc/watermark_updater.go:1348-1378`

**问题**：
- `UpdateWatermarkOnly()` 只是将 watermark 放入 `cacheUncommitted`（内存）
- CronJob 每 3 秒才批量持久化到数据库
- 如果在这 3 秒内系统崩溃，watermark 可能丢失

**影响**：
- 可能导致数据重复处理（可接受）
- 但如果 COMMIT 失败但 watermark 已更新，问题更严重

### 问题3：UpdateWatermarkOnly 直接覆盖，不检查大小

**位置**: `pkg/cdc/watermark_updater.go:1075-1090`

**问题**：
- 直接覆盖 `cacheUncommitted`，不检查新的 watermark 是否比旧的大
- 如果并发更新，可能导致 watermark 回退

**影响**：
- 可能导致数据重复处理
- 虽然这种情况应该很少见，但在高并发场景下可能发生

### 问题4：错误处理不完整

**位置**: `pkg/cdc/reader_v2_data_processor.go:336-400`

**问题**：
- 如果 `CommitTransaction()` 返回错误，不会更新 watermark
- 但如果 `CommitTransaction()` 返回成功（但 COMMIT 后续失败），watermark 已经被更新了

**影响**：
- 错误处理逻辑无法捕获异步 COMMIT 失败的情况

## 解决方案

### 方案1：等待 COMMIT 执行完成后再更新 watermark（推荐）✅

**思路**：改变 `CommitTransaction` 的流程，只有在确认 COMMIT 真正成功后才更新 watermark。

**实现方式**：

#### 方案1a：使用同步等待机制

```go
func (tm *TransactionManager) CommitTransaction(ctx context.Context) error {
    // Step 1: Send COMMIT
    tm.sinker.SendCommit()
    tm.sinker.SendDummy()
    
    // Step 2: 等待 COMMIT 真正执行完成
    // 需要添加一个同步机制来等待 COMMIT 执行完成
    if err := tm.sinker.WaitForCommitComplete(ctx); err != nil {
        // COMMIT 失败，不更新 watermark
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

**需要修改 Sinker 接口**：
```go
type Sinker interface {
    // ... existing methods ...
    WaitForCommitComplete(ctx context.Context) error  // 新增：等待 COMMIT 执行完成
}
```

**在 mysqlSinker2 中实现**：
```go
func (s *mysqlSinker2) WaitForCommitComplete(ctx context.Context) error {
    // 使用 channel 或 condition variable 等待 COMMIT 执行完成
    // 或者轮询 txnState 直到变为 COMMITTED 或 IDLE
    // ...
}
```

**优点**：
- ✅ 完全解决一致性问题
- ✅ 确保 COMMIT 成功后才更新 watermark

**缺点**：
- ⚠️ 需要修改 Sinker 接口
- ⚠️ 可能影响性能（需要等待）

#### 方案1b：使用回调机制

**思路**：在 `handleCommit` 成功后，回调通知 `TransactionManager` 更新 watermark。

**实现方式**：
```go
// 在 Sinker 中添加回调
type Sinker interface {
    // ... existing methods ...
    SetOnCommitSuccess(callback func(ctx context.Context, toTs types.TS))  // 新增
}

// 在 handleCommit 成功后调用回调
func (s *mysqlSinker2) handleCommit(ctx context.Context) error {
    if err := s.executor.CommitTx(ctx); err != nil {
        return err
    }
    
    // Commit 成功，通知 TransactionManager
    if s.onCommitSuccess != nil {
        s.onCommitSuccess(ctx, s.currentToTs)
    }
    
    return nil
}

// 在 TransactionManager 中注册回调
func (tm *TransactionManager) CommitTransaction(ctx context.Context) error {
    // 注册回调
    tm.sinker.SetOnCommitSuccess(func(ctx context.Context, toTs types.TS) {
        // 在 COMMIT 成功后更新 watermark
        tm.watermarkUpdater.UpdateWatermarkOnly(ctx, tm.watermarkKey, &toTs)
    })
    
    // Send COMMIT
    tm.sinker.SendCommit()
    tm.sinker.SendDummy()
    
    // 检查错误
    if err := tm.sinker.Error(); err != nil {
        return err
    }
    
    // 不在这里更新 watermark，等待回调
    return nil
}
```

**优点**：
- ✅ 完全解决一致性问题
- ✅ 不需要等待，性能更好

**缺点**：
- ⚠️ 需要修改架构
- ⚠️ 需要处理回调失败的情况

### 方案2：在 UpdateWatermarkOnly 中添加大小检查（防御性）✅

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
- ✅ 防止 watermark 回退
- ✅ 改动很小

**缺点**：
- ⚠️ 不能解决主要问题（异步 COMMIT）

### 方案3：在 Sinker 中延迟设置错误（折中方案）⚠️

**思路**：在 `handleCommit` 失败时，不立即设置错误，而是延迟一段时间，给 `CommitTransaction` 一个机会检查。

**实现方式**：
```go
func (s *mysqlSinker2) handleCommit(ctx context.Context) error {
    if err := s.executor.CommitTx(ctx); err != nil {
        // 延迟设置错误，给 CommitTransaction 一个机会检查
        go func() {
            time.Sleep(100 * time.Millisecond)  // 延迟 100ms
            s.SetError(err)
        }()
        return err
    }
    // ...
}
```

**优点**：
- ✅ 改动较小

**缺点**：
- ⚠️ 不是 100% 可靠
- ⚠️ 增加了延迟
- ⚠️ 不能完全解决问题

## 推荐方案

**当前最佳方案**：**方案1a（同步等待机制）+ 方案2（防止回退）**

1. **方案1a**：在 `CommitTransaction` 中等待 COMMIT 执行完成后再更新 watermark
2. **方案2**：在 `UpdateWatermarkOnly` 中只更新更大的 watermark，作为防御性措施

这样可以：
- ✅ 完全解决一致性问题
- ✅ 确保 COMMIT 成功后才更新 watermark
- ✅ 防止 watermark 回退

## 测试建议

1. **COMMIT 失败测试**：
   - 模拟网络问题导致 COMMIT 失败
   - 检查 watermark 是否正确
   - 检查数据是否丢失或重复

2. **长时间运行测试**：
   - 长时间运行任务（24 小时）
   - 定期检查数据一致性
   - 监控 watermark 更新情况

3. **并发测试**：
   - 多个任务同时运行
   - 检查 watermark 是否正确更新

4. **错误恢复测试**：
   - 模拟各种错误场景
   - 检查数据一致性

## 监控建议

1. **添加指标**：
   - `cdc_commit_to_watermark_delay`: COMMIT 到 watermark 更新的延迟
   - `cdc_commit_failure_after_watermark_update`: COMMIT 失败但 watermark 已更新的次数
   - `cdc_watermark_rollback_count`: watermark 回退的次数

2. **添加日志**：
   - 记录每次 COMMIT 的执行结果
   - 记录 watermark 更新的时机
   - 记录 COMMIT 失败的情况

## 相关代码位置

- `pkg/cdc/reader_v2_txn_manager.go:178-199` - CommitTransaction（问题所在）
- `pkg/cdc/sinker_v2.go:408-444` - handleCommit（COMMIT 执行）
- `pkg/cdc/sinker_v2.go:813-816` - SendCommit（异步发送）
- `pkg/cdc/watermark_updater.go:1075-1090` - UpdateWatermarkOnly（watermark 更新）
- `pkg/cdc/watermark_updater.go:1348-1378` - CronJob 持久化


