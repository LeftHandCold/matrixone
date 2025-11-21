# CDC 数据不一致其他可能原因分析

## 问题描述

即使**没有暂停操作**，也没有 `commit_failed` 日志，CDC 任务运行一晚上后也会出现**上游表和下游表数据行数不一致**的情况。

## 可能原因分析

### 原因1：Watermark 滞后导致数据重复处理 ⚠️ **最可能**

**设计理念**：CDC 遵循"允许滞后，禁止超前"的一致性模型
- ✅ **允许滞后**：watermark 可以滞后于实际进度（导致重复处理，可接受）
- ❌ **禁止超前**：watermark 绝不能超前于已持久化的数据（会导致数据丢失）

**问题场景**：

#### 场景1.1：Watermark 异步持久化延迟（3秒窗口）

```
时间线：
T1: 数据成功提交到目标数据库
T2: CommitTransaction() 调用 UpdateWatermarkOnly()
T3: watermark 更新到 cacheUncommitted（内存）
T4: 系统崩溃或重启（在 CronJob 下次执行之前，即3秒内）
T5: cacheUncommitted 中的 watermark 丢失
T6: 恢复时，从数据库读取的 watermark 是旧值（T1之前的值）
T7: 从旧 watermark 重新读取数据
T8: 数据重复插入到目标数据库
T9: 结果：下游数据多于上游
```

**关键代码**：
```go
// pkg/cdc/watermark_updater.go:1348-1378
// CronJob 每 3 秒才批量持久化
func (u *CDCWatermarkUpdater) cronRun(ctx context.Context) {
    // 每 3 秒执行一次
    // 将 cacheUncommitted 移动到 cacheCommitting
    // 然后批量 UPDATE 到数据库
}
```

**影响**：
- 如果在这 3 秒内系统崩溃，watermark 会回退
- 导致数据重复处理
- **如果目标数据库没有主键冲突检查，数据会重复插入**

#### 场景1.2：Watermark 持久化失败但数据已提交

```
时间线：
T1: 数据成功提交到目标数据库
T2: CommitTransaction() 调用 UpdateWatermarkOnly()
T3: watermark 更新到 cacheUncommitted
T4: CronJob 将 watermark 移动到 cacheCommitting
T5: 批量 UPDATE 到数据库时失败（数据库问题、网络问题等）
T6: watermark 退回 cacheUncommitted（但可能已经丢失）
T7: 下次 CronJob 执行时，从旧值重新持久化
T8: 结果：watermark 回退，数据重复处理
```

**关键代码**：
```go
// pkg/cdc/watermark_updater.go:640-699
if err != nil {
    // 持久化失败，将 watermark 退回 cacheUncommitted
    for key, watermark := range u.cacheCommitting {
        u.cacheUncommitted[key] = watermark
    }
    // 但如果系统崩溃，cacheUncommitted 也会丢失
}
```

### 原因2：删除操作处理不一致 ⚠️ **可能**

**问题场景**：

#### 场景2.1：删除操作和插入操作的时序问题

```
时间线：
T1: 上游执行 DELETE（删除 id=100）
T2: 上游执行 INSERT（插入 id=100，新数据）
T3: CDC 读取变更：
    - 先读取到 DELETE（id=100）
    - 后读取到 INSERT（id=100）
T4: 如果这两个操作在同一个事务中处理：
    - DELETE 先执行
    - INSERT 后执行
    - 结果：下游有数据（正确）
T5: 但如果这两个操作在不同事务中处理：
    - DELETE 在事务1中执行并提交
    - INSERT 在事务2中执行并提交
    - 如果事务1的 watermark 更新了，但事务2失败
    - 结果：下游没有数据（错误）
```

**关键代码**：
```go
// pkg/cdc/reader_v2_data_processor.go:276-334
// processTailDone 处理删除和插入
dp.insertAtmBatch.Append(...)  // 插入操作
dp.deleteAtmBatch.Append(...)  // 删除操作

// 发送到 Sinker
dp.sinker.Sink(ctx, &DecoderOutput{
    insertAtmBatch: dp.insertAtmBatch,
    deleteAtmBatch: dp.deleteAtmBatch,
    ...
})
```

#### 场景2.2：删除操作的去重问题

**问题**：如果同一个主键的删除操作被重复处理，可能导致数据不一致。

**场景**：
```
T1: 上游执行 DELETE（id=100）
T2: CDC 处理并提交（watermark 更新）
T3: 系统崩溃，watermark 回退
T4: 重新处理 DELETE（id=100）
T5: 如果目标数据库中没有这条数据，DELETE 不会报错
T6: 但如果后续有 INSERT（id=100），可能会出现问题
```

### 原因3：AtomicBatch 去重逻辑问题 ⚠️ **可能**

**问题场景**：

#### 场景3.1：去重逻辑可能不完整

**关键代码**：
```go
// pkg/cdc/types.go:410-430
func (bat *AtomicBatch) Append(...) {
    // 使用 B-Tree 按 (TS, PK) 排序
    // 如果相同的 (TS, PK) 已存在，标记为 duplicate
    if existing != nil {
        bat.duplicateRows++
        return  // 跳过重复的行
    }
}
```

**问题**：
- 去重是基于 (TS, PK) 的
- 但如果同一个主键在不同时间戳有多次变更，可能不会被正确去重
- 或者，如果时间戳相同但逻辑上应该去重，可能没有去重

#### 场景3.2：删除和插入的去重问题

**问题**：如果同一个主键先有 DELETE，后有 INSERT，AtomicBatch 的去重逻辑可能无法正确处理。

**场景**：
```
T1: DELETE id=100 (TS=100)
T2: INSERT id=100 (TS=200)
T3: AtomicBatch 去重：
    - DELETE 和 INSERT 的 TS 不同，不会被去重
    - 两个操作都会发送到 Sinker
T4: 如果处理顺序正确，结果是正确的
T5: 但如果处理顺序错误，可能导致数据不一致
```

### 原因4：Watermark 回退（虽然代码有检查）⚠️ **不太可能但需确认**

**问题场景**：

#### 场景4.1：并发更新导致覆盖

**关键代码**：
```go
// pkg/cdc/watermark_updater.go:1075-1090
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
- 虽然这种情况应该很少见，但在高并发场景下可能发生

**场景**：
```
T1: 批次A 处理数据 toTs=1000，调用 UpdateWatermarkOnly(1000)
T2: 批次B 处理数据 toTs=900，调用 UpdateWatermarkOnly(900)（由于某种原因，TS 更小）
T3: cacheUncommitted[key] = 900（覆盖了 1000）
T4: CronJob 持久化 watermark=900
T5: 下次读取时，从 900 开始，重复处理 900-1000 之间的数据
T6: 结果：数据重复
```

### 原因5：数据过滤或跳过问题 ⚠️ **需确认**

**问题场景**：

#### 场景5.1：Watermark 验证导致数据被跳过

**关键代码**：
```go
// pkg/cdc/sinker_v2.go:753-761
if data.toTs.LT(&watermark) {
    logutil.Error("cdc.mysql_sinker2.unexpected_watermark", ...)
    s.SetError(err)
    return  // 跳过这条数据
}
```

**问题**：
- 如果 `data.toTs < watermark`，数据会被跳过
- 这可能是由于 watermark 异常或数据时间戳异常
- 如果数据被跳过，会导致数据丢失

**检查方法**：
```bash
# 查找 unexpected_watermark 日志
grep "cdc.mysql_sinker2.unexpected_watermark" logs/
```

#### 场景5.2：CollectChanges 返回空数据

**问题**：如果 `CollectChanges` 由于某种原因返回空数据，但 watermark 已经更新，会导致数据丢失。

### 原因6：事务边界问题 ⚠️ **可能**

**问题场景**：

#### 场景6.1：多个批次在同一个事务中处理

**关键代码**：
```go
// pkg/cdc/reader_v2_data_processor.go:295-305
// 如果事务已存在，更新 toTs
if tracker != nil && tracker.hasBegin {
    tracker.UpdateToTs(dp.toTs)  // 更新 toTs
} else {
    // 创建新事务
    dp.txnManager.BeginTransaction(ctx, dp.fromTs, dp.toTs)
}
```

**问题**：
- 多个批次可能在同一个事务中处理
- 如果事务提交失败，所有批次的数据都会丢失
- 但 watermark 可能已经更新（如果之前有成功的提交）

#### 场景6.2：事务提交顺序问题

**问题**：如果多个表的事务同时提交，watermark 的更新顺序可能不一致。

## 诊断方法

### 1. 检查 Watermark 历史

```sql
-- 查看 watermark 是否有回退
SELECT 
    account_id,
    task_id,
    db_name,
    table_name,
    watermark,
    timestamp
FROM mo_catalog.mo_cdc_watermark
WHERE task_id = 'your-task-id'
ORDER BY timestamp DESC
LIMIT 100;
```

### 2. 检查日志中的关键信息

```bash
# 检查是否有 unexpected_watermark 日志
grep "cdc.mysql_sinker2.unexpected_watermark" logs/

# 检查是否有 watermark 相关的错误
grep "cdc.watermark" logs/ | grep -i error

# 检查是否有重复处理的日志
grep "duplicate" logs/ | grep -i cdc

# 检查 commit 相关的日志
grep "cdc.txn_manager.commit" logs/
grep "cdc.mysql_sinker2.commit" logs/
```

### 3. 检查数据差异模式

- **下游数据多于上游**：可能是 watermark 滞后导致重复处理
- **下游数据少于上游**：可能是数据被跳过或丢失
- **差异逐渐增大**：可能是累积误差
- **差异随机变化**：可能是并发问题

### 4. 检查 Watermark 持久化情况

```sql
-- 检查 watermark 更新的频率
SELECT 
    account_id,
    task_id,
    db_name,
    table_name,
    watermark,
    timestamp,
    LAG(watermark) OVER (PARTITION BY account_id, task_id, db_name, table_name ORDER BY timestamp) as prev_watermark
FROM mo_catalog.mo_cdc_watermark
WHERE task_id = 'your-task-id'
ORDER BY timestamp DESC;
```

## 最可能的原因

基于你的情况（没有 `commit_failed` 日志，运行一晚上后出现不一致），**最可能的原因是 Watermark 滞后导致数据重复处理**：

1. **Watermark 异步持久化延迟**：每 3 秒才批量持久化，如果在这 3 秒内系统崩溃或重启，watermark 会回退
2. **Watermark 持久化失败**：如果批量 UPDATE 失败，watermark 可能回退
3. **数据重复插入**：如果目标数据库没有主键冲突检查，重复的数据会被插入

## 建议的修复方案

### 方案1：减少 Watermark 持久化延迟（推荐）

**思路**：减少 CronJob 的执行间隔，或者在某些关键点强制刷新。

**实现**：
```go
// 在 CommitTransaction 成功后，强制刷新一次
func (tm *TransactionManager) CommitTransaction(ctx context.Context) error {
    // ... existing code ...
    
    // 强制刷新 watermark（可选，可能影响性能）
    tm.watermarkUpdater.ForceFlush(ctx)
    
    return nil
}
```

### 方案2：在 UpdateWatermarkOnly 中添加大小检查（防御性）

**思路**：防止 watermark 回退，只更新更大的 watermark。

**实现**：
```go
func (u *CDCWatermarkUpdater) UpdateWatermarkOnly(...) {
    u.Lock()
    defer u.Unlock()
    
    oldWatermark, hasOld := u.cacheUncommitted[*key]
    
    // 只更新更大的 watermark
    if !hasOld || watermark.GT(&oldWatermark) {
        u.cacheUncommitted[*key] = *watermark
    } else {
        logutil.Warn("cdc.watermark.update_skipped_smaller", ...)
    }
}
```

### 方案3：添加数据去重机制（目标数据库层面）

**思路**：在目标数据库中使用 `INSERT ... ON DUPLICATE KEY UPDATE` 或唯一索引来防止重复。

## 下一步行动

1. **检查日志**：查找 `unexpected_watermark`、`duplicate` 等关键词
2. **检查 Watermark 历史**：查看是否有回退
3. **检查数据差异模式**：确定是重复还是丢失
4. **监控 Watermark 持久化**：查看是否有持久化失败的情况


