# CDC fromTs 更新问题分析和修复总结

## 问题描述

在 MatrixOne CDC 功能中，重启后出现数据不一致问题：
- **现象**：上游和下游数据行数不一致（下游多 6 行）
- **表**：`cdc_test_6_db1.table1`
- **时间点**：重启后（17:06:32.738590 是重启后的第一条相关日志）

## 问题分析

### 1. 日志分析

从 `test1.log` 分析发现：

1. **`updated-from-ts` 在更新**：
   - `1765875992707269096-1` → `1765875992924424806-1` → `1765875993124011151-1` → ...
   - 说明 `DataProcessor.fromTs` 在 commit 后确实更新了

2. **`commit_success` 中的 `from-ts` 一直不变**：
   - 一直是 `1765875907591638996-1`
   - 说明 `TransactionTracker.fromTs` 没有更新

3. **`process_tail_done` 中的 `from-ts` 也一直不变**：
   - 一直是 `1765875907591638996-1`
   - 说明每次 `BeginTransaction` 使用的都是旧的 `fromTs`

### 2. 根本原因

**问题流程**：

1. **重启后恢复**：
   - 水位恢复到 `1765875907591638996-1`
   - `DataProcessor.fromTs` 被设置为这个值

2. **第一次 round**：
   - `processWithTxn` 从缓存获取 `fromTs = 1765875907591638996-1`
   - `SetTransactionRange(1765875907591638996-1, toTs1)` 设置 `DataProcessor.fromTs`
   - `BeginTransaction(1765875907591638996-1, toTs1)` 创建 `TransactionTracker`
   - Commit 成功后，`DataProcessor.fromTs = toTs1`

3. **第二次 round**：
   - `processWithTxn` 应该使用 `DataProcessor.fromTs`（已更新为 `toTs1`）
   - 但是 `SetTransactionRange` 被调用时，传入的 `fromTs` 参数是从缓存获取的旧值
   - 虽然 `SetTransactionRange` 有保留逻辑，但是 `BeginTransaction` 使用的是 `dp.fromTs`，而 `dp.fromTs` 可能被覆盖了

**关键问题**：

- `SetTransactionRange` 在 `processWithTxn` 中被调用，传入的 `fromTs` 参数可能来自缓存（旧值）
- 虽然 `SetTransactionRange` 有保留逻辑，但是逻辑可能有问题
- `BeginTransaction` 使用的是 `dp.fromTs`，但如果 `SetTransactionRange` 被调用时传入的 `fromTs` 比 `dp.fromTs` 大，就会覆盖

### 3. 为什么重启前没问题？

重启前，虽然 `from-ts` 也可能固定，但是：
- `CollectChanges` 返回的是增量数据（`[fromTs, toTs]` 范围）
- `AtomicBatch` 有去重机制，重复的数据会被去重
- 但是，如果 `from-ts` 一直不变，每次都会重复读取从 `from-ts` 到 `to-ts` 的数据
- 如果下游不是幂等的（比如有 DELETE 操作），就会导致数据不一致

## 修复方案

### 方案 1：在 commit 后更新 DataProcessor.fromTs（已实现）

**修改文件**：
- `pkg/cdc/reader_v2_data_processor.go`
- `pkg/cdc/table_change_stream.go`

**修改内容**：

1. **在 `processNoMoreData` 中，commit 成功后更新 `fromTs`**：
   ```go
   // Fix: Update fromTs to toTs after successful commit
   oldFromTs := dp.fromTs
   dp.fromTs = dp.toTs
   ```

2. **在 `processWithTxn` 中，优先使用 `DataProcessor.fromTs`**：
   ```go
   dpFromTs := s.dataProcessor.GetFromTs()
   if !dpFromTs.IsEmpty() {
       fromTs = dpFromTs  // Use DataProcessor's fromTs
   } else {
       fromTs, err = s.watermarkUpdater.GetFromCache(ctx, s.watermarkKey)
   }
   ```

3. **在 `SetTransactionRange` 中，保留已设置的 `fromTs`**：
   ```go
   if dp.fromTs.IsEmpty() || fromTs.GT(&dp.fromTs) {
       dp.fromTs = fromTs
   } else {
       // Preserve the existing fromTs
   }
   ```

### 方案 2：添加调试日志（已实现）

**添加的日志**：

1. **`cdc.data_processor.no_more_data_commit_success`**：
   - 记录 `old-from-ts`、`to-ts`、`updated-from-ts`
   - 级别：`INFO`

2. **`cdc.table_stream.get_from_ts_from_dp`**：
   - 记录从 `DataProcessor` 获取 `fromTs` 的情况
   - 级别：`INFO`

3. **`cdc.table_stream.get_from_ts_from_cache`**：
   - 记录从缓存获取 `fromTs` 的情况
   - 级别：`INFO`

4. **`cdc.data_processor.set_transaction_range`**：
   - 记录 `SetTransactionRange` 的调用情况
   - 级别：`INFO`

5. **`cdc.data_processor.set_transaction_range_preserved`**：
   - 记录 `fromTs` 被保留的情况
   - 级别：`INFO`

6. **`cdc.data_processor.begin_transaction`**：
   - 记录 `BeginTransaction` 的调用情况
   - 级别：`DEBUG`

## 调试方法

### 1. 检查 `fromTs` 更新情况

```bash
# 检查 commit 后 fromTs 是否更新
grep "cdc.data_processor.no_more_data_commit_success" cdc.log | grep "table1"

# 检查下次 round 是否使用了更新后的 fromTs
grep "cdc.table_stream.get_from_ts_from_dp" cdc.log | grep "table1"

# 检查 SetTransactionRange 是否保留了 fromTs
grep "cdc.data_processor.set_transaction_range" cdc.log | grep "table1"
```

### 2. 检查 `from-ts` 是否一直不变

```bash
# 检查所有 commit 的 from-ts
grep "commit_success" cdc.log | grep "table1" | awk -F'"from-ts":"' '{print $2}' | awk -F'"' '{print $1}' | sort -u
```

### 3. 检查数据不一致

```bash
# 检查上游和下游行数
# 上游：SELECT COUNT(*) FROM cdc_test_6_db1.table1;
# 下游：SELECT COUNT(*) FROM cdc_test_6_db1_bak.table1;
```

## 下一步

1. **重新测试**：使用修复后的代码重新测试
2. **查看日志**：检查新增的调试日志，确认 `fromTs` 是否正确更新
3. **如果还有问题**：根据日志进一步分析

## 关键日志字段说明

- **`old-from-ts`**：commit 前的 `fromTs`
- **`updated-from-ts`**：commit 后更新到的 `fromTs`（应该是 `to-ts`）
- **`preserved-from-ts`**：`SetTransactionRange` 保留的 `fromTs`
- **`requested-from-ts`**：`SetTransactionRange` 请求的 `fromTs`（可能来自缓存）
- **`data-processor-from-ts`**：`DataProcessor` 当前的 `fromTs`

## 注意事项

1. **`fromTs` 更新时机**：只有在 `processNoMoreData` 中 commit 成功后才会更新
2. **`SetTransactionRange` 保留逻辑**：只有当 `dp.fromTs` 不为空且新值不大于它时，才会保留
3. **缓存 vs DataProcessor**：优先使用 `DataProcessor.fromTs`，只有在它为空时才从缓存获取

