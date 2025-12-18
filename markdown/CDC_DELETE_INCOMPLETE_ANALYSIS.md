# CDC DELETE 执行不完整问题分析

## 问题现象

从日志 `cdc2.log` 分析发现，重启后同一个 `fromTS` 被多次使用处理数据，导致 INSERT 和 DELETE 数量严重不匹配：

- **db5.table1**: INSERT 59082 vs DELETE 36174 (多 22908 行，差异 38.8%)
- **db4.table1**: INSERT 50186 vs DELETE 46396 (多 3790 行，差异 8.2%)
- **db4.table2**: INSERT 65527 vs DELETE 34687 (多 30840 行，差异 89.0%)

## 关键发现

### 1. 同一个 fromTS 被多次使用

从日志中看到，重启后恢复的 `fromTS` 被使用了 4-5 次，但所有 batch 的 `toTS` 都相同：

```
fromTS=1766042750737769924-1 (db5.table1)
  - Batch 1: INSERT 16344, DELETE 6372, toTS=1766042806088931758-1
  - Batch 2: INSERT 16140, DELETE 12934, toTS=1766042806088931758-1
  - Batch 3: INSERT 16207, DELETE 5823, toTS=1766042806088931758-1
  - Batch 4: INSERT 10391, DELETE 11045, toTS=1766042806088931758-1
  总计: INSERT 59082, DELETE 36174
```

### 2. AtomicBatch 的去重逻辑

`AtomicBatch` 使用 B-tree 存储数据，去重逻辑基于 `(TS, PK)`：

```go
func (row AtomicBatchRow) Less(other AtomicBatchRow) bool {
    //ts asc
    if row.Ts.LT(&other.Ts) {
        return true
    }
    if row.Ts.GT(&other.Ts) {
        return false
    }
    //pk asc
    return bytes.Compare(row.Pk, other.Pk) < 0
}
```

如果同一个 `(TS, PK)` 出现多次，`bat.Rows.Set(row)` 会返回 `replaced=true`，表示替换了旧的行。

### 3. INSERT 和 DELETE 使用不同的 AtomicBatch

```go
// processTailDone
dp.insertAtmBatch.Append(packer, data.InsertBatch, ...)
dp.deleteAtmBatch.Append(packer, data.DeleteBatch, ...)
```

INSERT 和 DELETE 使用**独立的** `AtomicBatch` 实例，它们的去重是**独立的**。

## 根本原因分析

### 问题 1: 同一个 fromTS 被多次处理

**原因**：
1. 重启后水位回退到更早的 `committed` 值
2. 每次 `processWithTxn` 都会从缓存中获取 `fromTS`
3. 如果 `fromTS` 没有及时更新，会被多次使用

**影响**：
- 每次调用 `CollectChanges(fromTS, toTS)` 时，数据可能已经变化
- 返回的 INSERT 和 DELETE 数据可能不同

### 问题 2: AtomicBatch 去重导致 DELETE 丢失

**关键问题**：当同一个 `fromTS` 被多次处理时：

1. **第一次处理**：
   - `CollectChanges` 返回 INSERT batch A 和 DELETE batch A
   - 追加到 `insertAtmBatch` 和 `deleteAtmBatch`
   - 发送到 sinker

2. **第二次处理**（使用相同的 fromTS）：
   - `CollectChanges` 返回 INSERT batch B 和 DELETE batch B
   - 如果 batch B 中的某些 DELETE 的 `(TS, PK)` 与 batch A 中的 INSERT 相同
   - 这些 DELETE 会被追加到 `deleteAtmBatch`
   - **但是**，如果 batch B 中的某些 DELETE 的 `(TS, PK)` 与 batch A 中的 DELETE 相同
   - 这些 DELETE 会被**去重掉**（因为 AtomicBatch 基于 `(TS, PK)` 去重）

3. **结果**：
   - INSERT 可能被重复处理（但 `REPLACE INTO` 是幂等的，不会导致问题）
   - DELETE 可能被去重掉，导致某些 DELETE 没有执行

### 问题 3: INSERT 和 DELETE 数量不匹配的根本原因

**场景分析**：

假设有一个行 `(TS=100, PK=1)` 的变更历史：
1. `TS=100`: INSERT (PK=1)
2. `TS=150`: DELETE (PK=1)
3. `TS=200`: INSERT (PK=1)

当 `fromTS=50, toTS=250` 时：
- 第一次 `CollectChanges` 可能返回：
  - INSERT: `(TS=100, PK=1)`, `(TS=200, PK=1)`
  - DELETE: `(TS=150, PK=1)`
- 第二次 `CollectChanges`（使用相同的 fromTS）可能返回：
  - INSERT: `(TS=200, PK=1)` （因为数据已经变化）
  - DELETE: `(TS=150, PK=1)` （这个 DELETE 会被去重掉，因为第一次已经处理过了）

**结果**：
- INSERT: 2 个（第一次）+ 1 个（第二次，但 `(TS=200, PK=1)` 可能被去重）= 2 个
- DELETE: 1 个（第一次）+ 0 个（第二次被去重）= 1 个
- **但是**，如果第二次的 INSERT `(TS=200, PK=1)` 没有被去重（因为 TS 不同），就会导致 INSERT 多出

### 问题 4: 为什么总是下游数据多？

**原因**：
1. `REPLACE INTO` 对 INSERT 是幂等的，重复 INSERT 不会导致问题
2. DELETE 如果被去重掉，就不会执行，导致下游多出数据
3. 如果同一个 `(TS, PK)` 的 DELETE 在多次处理中被去重，就会导致 DELETE 丢失

## 解决方案建议

### 方案 1: 确保 fromTS 只被使用一次

在 `processTailDone` 中，处理完数据后立即更新 `fromTS`：

```go
// 在 processTailDone 中，发送数据后立即更新 fromTS
dp.fromTs = dp.toTs
```

### 方案 2: 改进 AtomicBatch 的去重逻辑

对于 DELETE，不应该基于 `(TS, PK)` 去重，因为：
- 同一个 `(TS, PK)` 的 DELETE 可能需要在不同的 batch 中执行
- 或者，需要区分 INSERT 和 DELETE 的去重逻辑

### 方案 3: 确保 INSERT 和 DELETE 的原子性

在同一个 batch 中，确保 INSERT 和 DELETE 的数量匹配：
- 如果某个 `(TS, PK)` 有 INSERT，对应的 DELETE 也应该存在
- 或者，使用事务确保 INSERT 和 DELETE 一起执行

### 方案 4: 改进水位恢复机制

避免水位回退导致重复处理：
- 使用更频繁的水位持久化
- 或者，使用更可靠的水位恢复机制

## 验证方法

1. **添加日志**：记录每次 `CollectChanges` 返回的 INSERT 和 DELETE 数量
2. **添加日志**：记录 AtomicBatch 的去重情况，特别是 DELETE 的去重
3. **添加日志**：记录同一个 `fromTS` 被使用的次数和原因

## 总结

DELETE 执行不完整的根本原因是：
1. **同一个 fromTS 被多次使用**，导致重复处理数据
2. **AtomicBatch 的去重逻辑**基于 `(TS, PK)`，导致某些 DELETE 被去重掉
3. **INSERT 和 DELETE 使用独立的 AtomicBatch**，它们的去重是独立的，导致数量不匹配
4. **REPLACE INTO 的幂等性**使得重复 INSERT 不会导致问题，但 DELETE 丢失会导致下游多出数据

