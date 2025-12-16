# CDC 水位恢复偏差问题分析

## 问题描述

在 MatrixOne 重启后，CDC 任务的水位恢复出现偏差：某些表的水位显示为"最新"，但上游和下游的数据行数不一致。

## 根本原因

### 水位更新的异步持久化机制

CDC 的水位更新采用**三层缓存 + 异步批量持久化**的架构：

1. **`cacheUncommitted`**：事务提交后，水位立即更新到内存中的未提交缓存
2. **`cacheCommitting`**：CronJob 每 3 秒运行一次，将 `cacheUncommitted` 移到 `cacheCommitting`
3. **`cacheCommitted`**：持久化到数据库成功后，水位移到 `cacheCommitted`

### 问题场景

从日志分析可以看到：

```
17:20:47.948 - 最后一次水位持久化成功（批量持久化）
17:20:48.455 - 大量事务提交，水位更新到 cacheUncommitted（内存中）
17:20:51.xxx - 系统重启（只过了约 3 秒）
```

**关键问题**：
- CronJob 每 3 秒运行一次（`WatermarkUpdateInterval = time.Second * 3`）
- 下一次持久化应该在 `17:20:50.948` 左右
- 但系统在 `17:20:51` 重启，**只过了 3 秒**
- 如果 CronJob 还没运行，或者运行了但还没持久化完成，**内存中的水位更新就会丢失**

### 为什么会出现水位偏差？

1. **不同表的水位持久化时间不同**：
   - 某些表在 `17:20:47.948` 之前已经持久化（比如 `17:20:45`）
   - 某些表在 `17:20:48.455` 才更新到内存，但还没持久化
   - 重启后，已持久化的水位被恢复，但内存中的水位丢失

2. **数据已写入下游，但水位未持久化**：
   - 事务提交后，数据已经写入下游数据库
   - 水位更新到 `cacheUncommitted`（内存中）
   - 但还没持久化到数据库
   - 重启后，从数据库恢复的是旧水位，导致**数据重复同步或丢失**

## 代码分析

### 水位更新流程

```go
// pkg/cdc/reader_v2_txn_manager.go:CommitTransaction
// Step 1: Send COMMIT to sinker (数据已写入下游)
tm.sinker.SendCommit()

// Step 2: Update watermark (只更新到内存)
tm.watermarkUpdater.UpdateWatermarkOnly(ctx, tm.watermarkKey, &toTs)
// 这个函数只是把水位放到 cacheUncommitted，不会立即持久化
```

### 异步持久化机制

```go
// pkg/cdc/watermark_updater.go
const WatermarkUpdateInterval = time.Second * 3

// CronJob 每 3 秒运行一次
func (u *CDCWatermarkUpdater) cronRun(ctx context.Context) {
    // 1. 检查是否有正在提交的水位
    if len(u.cacheCommitting) > 0 || len(u.cacheUncommitted) == 0 {
        return // 跳过本次运行
    }
    
    // 2. 将 cacheUncommitted 移到 cacheCommitting
    for key, watermark := range u.cacheUncommitted {
        u.cacheCommitting[key] = watermark
        delete(u.cacheUncommitted, key)
    }
    
    // 3. 持久化到数据库
    err = u.ForceFlush(ctx)
}
```

### 问题点

1. **时间窗口风险**：在 3 秒的时间窗口内，如果系统崩溃，内存中的水位更新会丢失
2. **数据一致性风险**：数据已写入下游，但水位未持久化，导致重启后数据重复或丢失
3. **无同步持久化选项**：`UpdateWatermarkOnly` 总是异步的，没有强制同步持久化的选项

## 日志证据

从 `cdc.log` 中可以看到：

### 重启前最后的水位持久化
```
17:20:47.948 - cdc.watermark.persist.success
  - cdc_test_3_db3.table2: 1765790447102499426-1
  - cdc_test_3_db3.table3: (未找到，可能在更早的时间)
```

### 重启前最后的事务提交
```
17:20:48.455 - cdc.txn_manager.commit_success
  - cdc_test_3_db3.table2: to-ts=1765790448449538306-1
  - cdc_test_3_db3.table3: to-ts=1765790446736103036-1
```

### 重启后的水位恢复
```
17:21:18.xxx - cdc.watermark.recovery.success
  - cdc_test_3_db3.table2: 1765790198618355489-1 (比提交的水位旧很多！)
  - cdc_test_3_db3.table3: 1765790225481722460-1 (比提交的水位旧很多！)
```

**偏差分析**：
- `table2`: 提交水位 `1765790448449538306-1`，恢复水位 `1765790198618355489-1`，**偏差约 2.5 亿**
- `table3`: 提交水位 `1765790446736103036-1`，恢复水位 `1765790225481722460-1`，**偏差约 2.2 亿**

## 解决方案建议

### 方案 1：缩短持久化间隔（治标不治本）

```go
const WatermarkUpdateInterval = time.Second * 1 // 从 3 秒改为 1 秒
```

**缺点**：只能减少时间窗口，不能完全避免问题

### 方案 2：关键事务后强制同步持久化（推荐）

在事务提交后，对于关键的水位更新，可以选择性地强制同步持久化：

```go
// 在 CommitTransaction 中
if err := tm.watermarkUpdater.UpdateWatermarkOnly(ctx, tm.watermarkKey, &toTs); err != nil {
    // ...
}

// 对于关键事务，强制同步持久化
if shouldForceFlush {
    if err := tm.watermarkUpdater.ForceFlush(ctx); err != nil {
        logutil.Error("cdc.txn_manager.force_flush_failed", zap.Error(err))
        // 可以选择回滚或重试
    }
}
```

### 方案 3：使用事务日志持久化水位（最佳方案）

将水位更新与数据写入放在同一个事务中，确保原子性：

```go
// 在 sinker 的 Commit 中，同时更新水位
func (s *mysqlSinker2) Commit() error {
    // 1. 提交数据事务
    if err := s.db.Exec("COMMIT"); err != nil {
        return err
    }
    
    // 2. 在同一个事务中更新水位（如果支持）
    // 或者使用两阶段提交确保一致性
}
```

### 方案 4：优雅关闭时强制持久化

在系统关闭时，等待所有未持久化的水位完成持久化：

```go
func (u *CDCWatermarkUpdater) Shutdown(ctx context.Context) error {
    // 1. 停止 CronJob
    // 2. 强制持久化所有未提交的水位
    for len(u.cacheUncommitted) > 0 {
        if err := u.ForceFlush(ctx); err != nil {
            return err
        }
    }
    // 3. 等待所有正在提交的水位完成
    // ...
}
```

## 临时缓解措施

1. **增加监控**：监控 `cacheUncommitted` 的大小，如果持续增长，说明持久化可能有问题
2. **告警机制**：当 `cacheUncommitted` 超过阈值时，触发告警
3. **定期检查**：定期比较上游和下游的数据行数，及时发现不一致

## 总结

水位恢复偏差的根本原因是**异步持久化机制导致的时间窗口风险**。在系统崩溃时，内存中的水位更新会丢失，但数据已经写入下游，导致数据不一致。

**建议优先实施方案 2 或方案 3**，确保关键事务的水位能够及时持久化，或者将水位更新与数据写入放在同一个事务中，确保原子性。

