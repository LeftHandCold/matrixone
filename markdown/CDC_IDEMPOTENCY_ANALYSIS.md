# CDC 幂等性分析

## SQL 执行方式

### 1. INSERT 操作
- **使用 `REPLACE INTO`**：
  ```sql
  REPLACE INTO `db`.`table` VALUES (...)
  ```
- **幂等性**：✅ **是幂等的**
  - 如果记录已存在，会先删除再插入
  - 重复执行不会导致数据不一致

### 2. DELETE 操作
- **使用 `DELETE FROM ... WHERE pk = ...`**：
  ```sql
  DELETE FROM `db`.`table` WHERE (pk1, pk2) IN (...)
  ```
- **幂等性**：✅ **是幂等的**
  - 如果记录不存在，不会报错，也不会删除任何行
  - 重复执行不会导致数据不一致

### 3. AtomicBatch 去重
- **在单个 transaction 内去重**：
  - 使用 `bat.Rows.Set(row)` 来去重
  - 如果同一个 `(ts, pk)` 出现多次，后面的会替换前面的
  - **但是，跨 transaction 的重复不会被去重**

## 问题分析

### 场景 1：重复处理同一数据范围

**假设**：
- 第一次处理：`fromTs = T1, toTs = T2`
- 重复处理：`fromTs = T1, toTs = T2`（因为 `fromTs` 没有更新）

**处理流程**：
1. **第一次处理**：
   - 读取 `[T1, T2)` 的数据
   - `AtomicBatch` 去重（单个 transaction 内）
   - 执行：`REPLACE INTO ...`（INSERT）
   - 执行：`DELETE FROM ... WHERE pk = ...`（DELETE）
   - Commit

2. **重复处理**：
   - 再次读取 `[T1, T2)` 的数据
   - `AtomicBatch` 去重（单个 transaction 内）
   - 执行：`REPLACE INTO ...`（INSERT）
   - 执行：`DELETE FROM ... WHERE pk = ...`（DELETE）
   - Commit

**问题**：
- 如果同一行在 `[T1, T2)` 范围内先 INSERT 后 DELETE：
  - 第一次处理：INSERT row1 → DELETE row1（最终 row1 不存在）
  - 重复处理：INSERT row1 → DELETE row1（最终 row1 不存在）
  - **应该是幂等的**

- 但是，如果 `AtomicBatch` 的去重逻辑有问题：
  - 如果同一行在同一个 transaction 内出现多次（INSERT + DELETE），`AtomicBatch` 只会保留最后一个
  - 如果最后一个操作是 INSERT，最终会执行 INSERT
  - 如果最后一个操作是 DELETE，最终会执行 DELETE

### 场景 2：AtomicBatch 去重逻辑

**AtomicBatch 的去重机制**：
```go
_, replaced := bat.Rows.Set(row)
```
- 使用 `(ts, pk)` 作为 key
- 如果同一个 `(ts, pk)` 出现多次，后面的会替换前面的
- **但是，如果同一行的 INSERT 和 DELETE 有相同的 `ts`，只会保留最后一个**

**问题**：
- 如果同一行在同一个 transaction 内先 INSERT 后 DELETE，且 `ts` 相同：
  - `AtomicBatch` 只会保留最后一个操作（DELETE）
  - 最终只会执行 DELETE，不会执行 INSERT
  - **这可能导致数据不一致**

### 场景 3：跨 transaction 的重复处理

**问题**：
- `AtomicBatch` 只在单个 transaction 内去重
- 如果 `fromTs` 没有更新，会重复读取相同的数据范围
- 每次读取都会创建新的 `AtomicBatch`，不会跨 transaction 去重

**示例**：
- 第一次 transaction：读取 `[T1, T2)`，处理 row1（INSERT）
- 第二次 transaction：再次读取 `[T1, T2)`，处理 row1（INSERT）
- 虽然 `REPLACE INTO` 是幂等的，但如果 row1 在第一次 transaction 后被 DELETE，第二次 transaction 会再次 INSERT

## 根本原因

### 1. `fromTs` 没有正确更新
- 重启后，`DataProcessor.fromTs` 被重置为 `0-0`
- 从缓存恢复的 `fromTs` 可能不是最新的
- 导致重复读取相同的数据范围

### 2. AtomicBatch 去重范围有限
- 只在单个 transaction 内去重
- 跨 transaction 的重复不会被去重

### 3. INSERT + DELETE 组合的处理顺序
- 先执行 INSERT（REPLACE INTO）
- 再执行 DELETE
- 如果同一行在同一个 transaction 内先 INSERT 后 DELETE，`AtomicBatch` 只会保留最后一个操作

## 修复方案

### 方案 1：确保 `fromTs` 正确更新（已实现）
- 在 commit 后更新 `DataProcessor.fromTs = toTs`
- 优先使用 `DataProcessor.fromTs`，而不是从缓存获取
- 在 `SetTransactionRange` 中保留已设置的 `fromTs`

### 方案 2：改进 AtomicBatch 去重逻辑（需要进一步分析）
- 如果同一行在同一个 transaction 内先 INSERT 后 DELETE，应该如何处理？
- 选项 1：保留最后一个操作（当前实现）
- 选项 2：如果先 INSERT 后 DELETE，应该跳过（因为最终结果是删除）
- 选项 3：如果先 DELETE 后 INSERT，应该只执行 INSERT（因为最终结果是插入）

### 方案 3：添加跨 transaction 的去重机制（可选）
- 在 watermark 中记录已处理的数据范围
- 避免重复处理相同的数据范围

## 调试建议

### 1. 检查重复处理
```bash
# 检查是否有重复的 fromTs
grep "set_transaction_range" cdc.log | grep "table1" | awk -F'"from-ts":"' '{print $2}' | awk -F'"' '{print $1}' | sort | uniq -d
```

### 2. 检查 AtomicBatch 去重
```bash
# 检查是否有重复行
grep "atomic_batch.dedup" cdc.log | grep "table1"
```

### 3. 检查 INSERT + DELETE 组合
```bash
# 检查同一行的 INSERT 和 DELETE
grep "process_tail_done" cdc.log | grep "table1" | grep -E "insert-rows-read|delete-rows-read"
```

## 结论

1. **SQL 执行是幂等的**：
   - `REPLACE INTO` 是幂等的
   - `DELETE FROM ... WHERE pk = ...` 是幂等的

2. **但是，处理逻辑可能不是幂等的**：
   - `AtomicBatch` 只在单个 transaction 内去重
   - 如果 `fromTs` 没有正确更新，会重复处理相同的数据范围
   - 如果同一行在同一个 transaction 内先 INSERT 后 DELETE，`AtomicBatch` 只会保留最后一个操作

3. **根本原因**：
   - `fromTs` 没有正确更新，导致重复处理
   - 虽然 SQL 是幂等的，但重复处理可能导致数据不一致（特别是 INSERT + DELETE 组合）

4. **修复方向**：
   - 确保 `fromTs` 正确更新（已实现）
   - 进一步分析 `AtomicBatch` 的去重逻辑，确保 INSERT + DELETE 组合的正确处理

