# CDC 数据一致性问题分析与修复总结

## 问题描述

在 MatrixOne CDC 功能中，当系统意外重启后，出现以下问题：
- 水位（watermark）显示为"latest"
- 但上游和下游数据库的行数不一致（下游行数 > 上游行数）
- 问题主要出现在 `INSERT` + `DELETE` 操作场景中

## 根本原因分析

### 1. `from-ts` 被重置问题

**现象：**
- 重启后，`DataProcessor.fromTs` 被重置为 `0-0`（空值）
- 然后从缓存恢复为旧的 watermark 值
- 导致重复处理已经处理过的数据范围

**日志证据：**
```
old-from-ts: 0-0
new-from-ts: 1765883821106719699-1  (从缓存恢复的旧值)
```

**影响：**
- 同一个 `from-ts` 被用于处理多个不同的 `to-ts` 范围
- 例如：`from-ts: 1765883821106719699-1` 被用于处理 681 个不同的 `to-ts`
- 导致数据被重复处理，造成下游行数增加

### 2. `AtomicBatch` 重复处理问题

**问题：**
- `ExtractAndReset()` 方法直接共享了 `Batches` 和 `Rows` 的引用
- 当同一个事务内多次调用 `process_tail_done` 时，会重复发送相同的批次
- 如果批次中的行在第一次处理后被删除，第二次处理会重新插入，导致数据不一致

**原始实现问题：**
```go
// 错误：直接共享引用
newBatch.Batches = bat.Batches
newBatch.Rows = bat.Rows
```

**影响：**
- 同一个 `srcBatch` 可能被处理多次
- 如果第一次处理时删除了行，第二次处理会重新插入
- 导致下游行数 > 上游行数

## 已实施的修复

### 1. 修复 `ExtractAndReset()` 方法

**文件：** `pkg/cdc/types.go`

**修复内容：**
- 使用 `btree.Copy()` 复制 `Rows` btree，避免共享引用
- 复制 `Batches` 切片，避免共享引用
- 确保提取的批次是独立的副本

**修复后的实现：**
```go
func (bat *AtomicBatch) ExtractAndReset() *AtomicBatch {
    // ...
    // 复制 Batches 切片
    if bat.Batches != nil && len(bat.Batches) > 0 {
        newBatch.Batches = make([]*batch.Batch, len(bat.Batches))
        copy(newBatch.Batches, bat.Batches)
    }
    
    // 复制 Rows btree（深拷贝）
    if bat.Rows != nil && bat.Rows.Len() > 0 {
        newBatch.Rows = bat.Rows.Copy()
    }
    // ...
}
```

### 2. `from-ts` 更新机制优化

**文件：** `pkg/cdc/reader_v2_data_processor.go`

**修复内容：**
- 在 `processTailDone` 和 `processNoMoreData` 中，成功 commit 后立即更新 `dp.fromTs = dp.toTs`
- 在 `SetTransactionRange` 中，优先保留已更新的 `fromTs`，避免被缓存的旧值覆盖
- 在 `TableChangeStream.processWithTxn` 中，优先使用 `DataProcessor.GetFromTs()` 而不是缓存值

**关键代码：**
```go
// 成功 commit 后更新 fromTs
oldFromTs := dp.fromTs
dp.fromTs = dp.toTs

// SetTransactionRange 中保护已更新的 fromTs
if dp.fromTs.IsEmpty() || fromTs.GT(&dp.fromTs) {
    dp.fromTs = fromTs
} else {
    // 保留已更新的 fromTs
}
```

### 3. 添加详细日志

**添加的日志包括：**
- `cdc.data_processor.set_transaction_range`: 记录 `from-ts` 的设置
- `cdc.data_processor.set_transaction_range_preserved`: 记录 `from-ts` 被保留的情况
- `cdc.data_processor.no_more_data_commit_success`: 记录 commit 成功和 `from-ts` 更新
- `cdc.txn_manager.commit_success`: 记录 commit 成功，包括 `potential-duplicate` 标记
- `cdc.mysql_sinker2.exec_insert_sql_success`: 记录 INSERT SQL 执行成功
- `cdc.mysql_sinker2.exec_delete_sql_success`: 记录 DELETE SQL 执行成功
- `cdc.atomic_batch.duplicate_row_detected`: 记录重复行检测
- `cdc.data_processor.insert_delete_overlap`: 记录 INSERT/DELETE 批次重叠

## 待解决的问题

### 1. `from-ts` 被重置为 `0-0` 的根本原因

**问题：**
- 重启后，`DataProcessor.fromTs` 被重置为 `0-0`
- 可能原因：
  1. `DataProcessor` 被重新创建（重启后）
  2. 某个地方显式重置了 `fromTs`
  3. `SetTransactionRange` 被调用时，`dp.fromTs` 已经是 `0-0`

**需要进一步调查：**
- `DataProcessor` 的生命周期管理
- 重启后 `TableChangeStream` 的恢复机制
- 是否有地方显式重置 `fromTs`

### 2. 重复 commit 问题

**现象：**
- 同一个 `from-ts` 被用于处理多个不同的 `to-ts`
- 例如：`from-ts: 1765883821106719699-1` 被 commit 了 683 次，涉及 681 个不同的 `to-ts`
- 其中 680 次标记为 `potential-duplicate: true`

**可能原因：**
- `from-ts` 一直不变，导致重复处理相同的数据范围
- 需要确保 `from-ts` 在每次成功 commit 后正确更新

## 测试建议

### 1. 重新编译并测试

```bash
# 重新编译 MatrixOne
make build

# 运行测试脚本
python3 reproduce_cdc_watermark_bias.py \
    --interval 0.1 \
    --auto-restart-hint 30 \
    --monitor-watermark
```

### 2. 验证修复效果

**检查点：**
1. 重启后，`from-ts` 是否正确恢复（不应为 `0-0`）
2. 每次 commit 后，`from-ts` 是否正确更新为 `to-ts`
3. 数据行数是否一致（上游 = 下游）
4. 日志中是否还有 `potential-duplicate: true` 的大量 commit

### 3. 监控关键日志

**关键日志：**
- `cdc.data_processor.set_transaction_range`: 检查 `old-from-ts` 是否为 `0-0`
- `cdc.data_processor.no_more_data_commit_success`: 检查 `from-ts` 是否正确更新
- `cdc.txn_manager.commit_success`: 检查 `potential-duplicate` 的数量
- `cdc.mysql_sinker2.insert_delete_batch_complete`: 检查实际写入的行数

## 下一步行动

1. **验证修复效果**
   - 重新编译并运行测试脚本
   - 检查数据一致性是否改善

2. **深入调查 `from-ts` 重置问题**
   - 添加日志追踪 `DataProcessor` 的生命周期
   - 检查重启后的恢复机制
   - 确认是否有地方显式重置 `fromTs`

3. **优化重复处理检测**
   - 在 `CommitTransaction` 中加强重复检测
   - 如果检测到重复处理，记录警告并跳过

4. **性能优化**
   - 如果 `ExtractAndReset()` 的深拷贝影响性能，考虑优化
   - 评估是否需要更高效的批次管理机制

## 相关文件

- `pkg/cdc/types.go`: `AtomicBatch.ExtractAndReset()` 修复
- `pkg/cdc/reader_v2_data_processor.go`: `from-ts` 更新机制
- `pkg/cdc/table_change_stream.go`: `from-ts` 获取优先级
- `pkg/cdc/reader_v2_txn_manager.go`: commit 日志和重复检测
- `pkg/cdc/sinker_v2.go`: SQL 执行日志
- `pkg/cdc/watermark_updater.go`: watermark 恢复和更新日志

## 时间线

- **问题发现**: 重启后数据行数不一致
- **初步分析**: 发现 `from-ts` 更新问题
- **深入分析**: 发现 `AtomicBatch` 重复处理问题
- **修复实施**: 
  - 修复 `ExtractAndReset()` 方法
  - 优化 `from-ts` 更新机制
  - 添加详细日志
- **待验证**: 重新编译并测试修复效果

