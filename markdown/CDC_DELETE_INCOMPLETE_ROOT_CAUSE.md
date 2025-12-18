# CDC DELETE 执行不完整根本原因分析

## 问题现象

从日志 `cdc3.log` 分析发现，`CollectChanges` 返回的 INSERT 和 DELETE 数量不匹配：

- **db5.table2**: INSERT=15000, DELETE=6806, 差异=8194 (54.6%)
- **db3.table3**: INSERT=24000, DELETE=0, 差异=24000 (100%)

## 关键发现

### 1. CollectChanges 返回的数据本身就不匹配

从日志中看到，`CollectChanges` 在不同的 batch 中分别返回 INSERT 和 DELETE：

**db5.table2** (8 个 batch):
- Batch 1: INSERT=0, DELETE=3000
- Batch 2: INSERT=3000, DELETE=0
- Batch 3: INSERT=3000, DELETE=0
- Batch 4: INSERT=3000, DELETE=0
- Batch 5: INSERT=0, DELETE=1332
- Batch 6: INSERT=3000, DELETE=0
- Batch 7: INSERT=3000, DELETE=0
- Batch 8: INSERT=0, DELETE=2474

**总计**: INSERT=15000, DELETE=6806, 差异=8194

### 2. 每个 fromTS 只被使用一次

从日志中看到，每个 `fromTS` 只被使用一次，不是重复使用的问题。每个 batch 都有不同的 `fromTS` 和 `toTS`。

### 3. processTailDone 的行为

- `processTailDone` 会发送当前累积的所有数据（包括之前的 TailWip），然后清空 `insertAtmBatch` 和 `deleteAtmBatch`
- 每个 `processTailDone` 都是独立的 batch，不会累积

## 根本原因分析

### 问题：为什么 CollectChanges 返回的 INSERT 和 DELETE 不匹配？

**关键理解**：
1. `CollectChanges(fromTS, toTS)` 返回的是在 `[fromTS, toTS)` 范围内的**所有变更**
2. 但是，INSERT 和 DELETE 可能在不同的 batch 中返回
3. 这是**正常的设计**，因为：
   - INSERT 和 DELETE 可能来自不同的 partition
   - INSERT 和 DELETE 可能在不同的时间点提交
   - Engine 可能在不同的 batch 中分别返回 INSERT 和 DELETE

### 为什么会导致 DELETE 少？

**关键问题**：虽然 `CollectChanges` 返回的 INSERT 和 DELETE 不匹配，但这不应该导致数据不一致，因为：
- 如果某个 `(TS, PK)` 有 INSERT，对应的 DELETE 也应该在某个 batch 中返回
- 如果某个 `(TS, PK)` 有 DELETE，对应的 INSERT 也应该在某个 batch 中返回

**但是**，如果：
1. 某个 batch 只有 INSERT，没有 DELETE
2. 对应的 DELETE 在另一个 batch 中
3. 如果这两个 batch 在不同的 transaction 中处理
4. 如果第二个 batch 的 DELETE 因为某些原因没有被处理（比如重启、错误等）
5. 就会导致 DELETE 丢失

### 更深层的问题：为什么会出现这种情况？

**可能的原因**：

1. **CollectChanges 的实现问题**：
   - `CollectChanges` 可能没有正确返回所有的 DELETE
   - 或者，某些 DELETE 被过滤掉了

2. **数据源的问题**：
   - 上游数据本身就不匹配
   - 或者，某些 DELETE 在数据源中就不存在

3. **处理顺序的问题**：
   - 如果 INSERT 和 DELETE 在不同的 batch 中，处理顺序可能影响结果
   - 如果某个 batch 的 DELETE 在处理时出错或被跳过，就会导致 DELETE 丢失

## 验证方法

1. **检查 CollectChanges 的实现**：
   - 查看 `CollectChanges` 的实现，确认它是否正确返回所有的 INSERT 和 DELETE
   - 检查是否有过滤逻辑导致某些 DELETE 被过滤掉

2. **检查数据源**：
   - 验证上游数据本身是否匹配
   - 检查是否有某些 DELETE 在数据源中就不存在

3. **检查处理顺序**：
   - 查看是否有 batch 被跳过或处理失败
   - 检查是否有错误导致某些 DELETE 没有被处理

## 解决方案建议

### 方案 1: 确保 CollectChanges 返回完整的数据

检查 `CollectChanges` 的实现，确保它返回所有的 INSERT 和 DELETE，不会被过滤或丢失。

### 方案 2: 在 CDC 层面进行匹配

在 CDC 层面，确保每个 `(TS, PK)` 的 INSERT 和 DELETE 都被正确处理：
- 如果某个 `(TS, PK)` 有 INSERT，确保对应的 DELETE 也被处理
- 如果某个 `(TS, PK)` 有 DELETE，确保对应的 INSERT 也被处理

### 方案 3: 改进错误处理

确保即使某个 batch 处理失败，也不会导致数据丢失：
- 使用事务确保原子性
- 实现重试机制
- 记录详细的错误日志

## 总结

DELETE 执行不完整的根本原因是：
1. **CollectChanges 返回的数据本身就不匹配**：INSERT 和 DELETE 在不同的 batch 中返回
2. **这是正常的设计**，但可能导致数据不一致
3. **如果某个 batch 的 DELETE 没有被处理**（比如重启、错误等），就会导致 DELETE 丢失
4. **需要进一步调查**：为什么 CollectChanges 返回的 INSERT 和 DELETE 不匹配？这是数据源的问题还是 CollectChanges 实现的问题？

