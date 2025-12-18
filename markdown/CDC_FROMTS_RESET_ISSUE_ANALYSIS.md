# CDC fromTs 重置问题分析

## 问题描述

在 MatrixOne CDC 功能中，第二次重启后出现数据不一致问题：
- **现象**：上游和下游数据行数不一致（下游多 136 行）
- **表**：`cdc_test_3.cdc_test_3_db3.table1`
- **第一次重启时间**：17:31:50.764296（没问题）
- **第二次重启时间**：17:35:50.162532（有问题）

## 关键发现

### 1. 日志分析

从 `test2.log` 分析发现：

1. **重启后第一次 round 的 `fromTs` 被重置为 `0-0`（空）**：
   ```
   "old-from-ts":"0-0"
   "new-from-ts":"1765877747361982981-1"
   "requested-from-ts":"1765877747361982981-1"
   ```

2. **`fromTs` 从缓存恢复，但可能不是最新的**：
   - 重启后，`DataProcessor.fromTs` 被重置为 `0-0`
   - 然后从缓存恢复为 `1765877747361982981-1`
   - 但这个值可能不是最新的水位

3. **`updated-from-ts` 在更新**：
   - commit 成功后，`updated-from-ts` 更新为 `1765877831755455216-1`
   - 说明 `DataProcessor.fromTs` 在 commit 后确实更新了

### 2. 根本原因

**问题流程**：

1. **第一次重启后**：
   - `DataProcessor` 被重新创建，`fromTs` 被初始化为 `0-0`
   - 从缓存恢复 `fromTs = 1765877621141910637-1`
   - 正常处理，没有问题

2. **第二次重启后**：
   - `DataProcessor` 被重新创建，`fromTs` 被初始化为 `0-0`
   - 从缓存恢复 `fromTs = 1765877747361982981-1`
   - **问题**：这个值可能不是最新的水位，导致重复处理数据

3. **为什么第一次重启没问题，第二次有问题？**
   - 第一次重启时，缓存中的水位可能是最新的
   - 第二次重启时，缓存中的水位可能滞后了（异步持久化的时间窗口）

### 3. 为什么 `fromTs` 会被重置？

1. **`DataProcessor` 在 `TableChangeStream` 中被创建**：
   - `DataProcessor` 是 `TableChangeStream` 的成员
   - 在 `NewTableChangeStream` 中创建
   - 如果 `TableChangeStream` 在重启后被重新创建，`DataProcessor` 也会被重新创建

2. **`Cleanup` 方法不会清空 `fromTs`**：
   - `Cleanup` 只清理 `insertAtmBatch` 和 `deleteAtmBatch`
   - 不会清空 `fromTs` 和 `toTs`

3. **问题在于缓存恢复的时机**：
   - 重启后，`DataProcessor.fromTs` 被初始化为 `0-0`
   - 然后从缓存恢复，但缓存中的值可能不是最新的

## 修复方案

### 方案 1：确保 `fromTs` 在重启后正确恢复（已实现）

**修改内容**：

1. **在 `processWithTxn` 中，优先使用 `DataProcessor.fromTs`**：
   ```go
   dpFromTs := s.dataProcessor.GetFromTs()
   if !dpFromTs.IsEmpty() {
       fromTs = dpFromTs  // Use DataProcessor's fromTs
   } else {
       fromTs, err = s.watermarkUpdater.GetFromCache(ctx, s.watermarkKey)
   }
   ```

2. **在 `SetTransactionRange` 中，保留已设置的 `fromTs`**：
   ```go
   if dp.fromTs.IsEmpty() || fromTs.GT(&dp.fromTs) {
       dp.fromTs = fromTs
   } else {
       // Preserve the existing fromTs
   }
   ```

3. **在 `processNoMoreData` 中，commit 成功后更新 `fromTs`**：
   ```go
   dp.fromTs = dp.toTs
   ```

4. **在 `Cleanup` 中，不清空 `fromTs`**：
   ```go
   // NOTE: We intentionally do NOT reset fromTs/toTs here
   // They need to persist across rounds to avoid using stale cached watermarks
   ```

### 方案 2：添加更多调试日志（已实现）

**添加的日志**：

1. **`cdc.data_processor.set_transaction_range`**：
   - 记录 `old-from-ts`、`new-from-ts`、`requested-from-ts`、`to-ts`
   - 级别：`INFO`

2. **`cdc.data_processor.set_transaction_range_preserved`**：
   - 记录 `fromTs` 被保留的情况
   - 级别：`INFO`

3. **`cdc.table_stream.get_from_ts_from_dp`**：
   - 记录从 `DataProcessor` 获取 `fromTs` 的情况
   - 级别：`INFO`

4. **`cdc.table_stream.get_from_ts_from_cache`**：
   - 记录从缓存获取 `fromTs` 的情况
   - 级别：`INFO`

## 调试方法

### 1. 检查 `fromTs` 重置情况

```bash
# 检查重启后第一次 round 的 fromTs
grep "set_transaction_range" cdc.log | grep "old-from-ts.*0-0"

# 检查 fromTs 从缓存恢复的情况
grep "get_from_ts_from_cache" cdc.log | grep "table1"
```

### 2. 检查 `fromTs` 更新情况

```bash
# 检查 commit 后 fromTs 是否更新
grep "no_more_data_commit_success" cdc.log | grep "table1"

# 检查下次 round 是否使用了更新后的 fromTs
grep "get_from_ts_from_dp" cdc.log | grep "table1"
```

### 3. 检查数据不一致

```bash
# 检查上游和下游行数
# 上游：SELECT COUNT(*) FROM cdc_test_3_db3.table1;
# 下游：SELECT COUNT(*) FROM cdc_test_3_db3_bak.table1;
```

## 下一步

1. **重新测试**：使用修复后的代码重新测试
2. **查看日志**：检查新增的调试日志，确认 `fromTs` 是否正确恢复和更新
3. **如果还有问题**：根据日志进一步分析，可能需要检查：
   - 缓存恢复的时机
   - 异步持久化的时间窗口
   - `TableChangeStream` 的生命周期

## 关键日志字段说明

- **`old-from-ts`**：`SetTransactionRange` 调用前的 `fromTs`
- **`new-from-ts`**：`SetTransactionRange` 调用后的 `fromTs`
- **`requested-from-ts`**：`SetTransactionRange` 请求的 `fromTs`（可能来自缓存）
- **`preserved-from-ts`**：`SetTransactionRange` 保留的 `fromTs`（来自上次 commit）
- **`updated-from-ts`**：commit 后更新到的 `fromTs`（应该是 `to-ts`）

## 注意事项

1. **`fromTs` 恢复时机**：重启后，`DataProcessor.fromTs` 被初始化为 `0-0`，然后从缓存恢复
2. **缓存可能滞后**：异步持久化的时间窗口可能导致缓存中的水位不是最新的
3. **`fromTs` 持久化**：`fromTs` 在 commit 后更新，但需要确保在重启后正确恢复

