# TPCC 多 CN `__mo_cpkey_col` duplicate 根因与修复摘要

## 结论

4/1 这次 `Duplicate entry '...' for key '__mo_cpkey_col'` 的直接炸点在 **TN/TAE 的 `bmsql_stock` Freeze 期 dedup**，不是 `bmsql_district ... FOR UPDATE`。

更准确地说，这是 **hidden composite PK (`__mo_cpkey_col`) 在 merge / transfer / object rewrite 之后，dedup 候选按 PK 找、delete masking 只按 rowid 消** 导致的漏消问题。

## 关键证据

1. 原始日志里真正报错的是 `bmsql_stock`
   - `Explore-logs-dn.txt:550-558`
   - 在 `txnimpl/table.go:1228` 的 `Phase_Freeze` 报 duplicate
   - 报错前一刻同表正在做 flush / merge / prepare-commit

2. Freeze 顺序是先 transfer、后 dedup
   - `pkg/vm/engine/tae/txn/txnimpl/txndb.go:467-483`
   - `PrePreareTransfer(...)` 在前
   - `PrePrepareDedup(...)` 在后

3. dedup 候选按 PK 查，delete masking 按 rowid 消
   - 候选查找：`pkg/vm/engine/tae/txn/txnimpl/base_table.go:211-285`
   - dedup 主流程：`pkg/vm/engine/tae/txn/txnimpl/table.go:1208-1259`
   - delete masking：`pkg/vm/engine/tae/txn/txnimpl/table.go:1120-1188`
   - rowid-only `contains()`：`pkg/vm/engine/tae/txn/txnimpl/table.go:1676-1764`

4. delete transfer 会把 tombstone 重定向到新 rowid
   - `pkg/vm/engine/tae/txn/txnimpl/table.go:235-380`
   - `pkg/vm/engine/tae/txn/txnimpl/table.go:461-560`

5. CN 不会像单列 PK 那样提前拦住 hidden composite PK
   - `pkg/vm/engine/disttae/txn.go:1071-1075`
   - `__mo_cpkey_col` 会跳过 write-time PK check

## 根因串联

TPCC `NewOrder` 里重复命中同一条 `bmsql_stock` 只是触发器，不是根因归属。

真正的问题链路是：

1. 同一事务里，同一 logical row 的 composite PK 被多次更新。
2. 前一个版本在 statement 边界或 commit 前被 merge / transfer 到新对象、新 rowid。
3. tombstone 也被 transfer 到 **新 rowid**。
4. Freeze dedup 再按 PK 回看候选时，仍可能命中旧版本对应的数据候选。
5. 但 delete masking 只拿 tombstone rowid 去消候选 rowid；旧 rowid 与新 rowid 对不上时，候选漏消。
6. 最终同一个 `__mo_cpkey_col` 留下未被 mask 的候选，在 `Phase_Freeze` 报 1062。

## 这次修复做了什么

代码修改点：

- `pkg/vm/engine/tae/txn/txnimpl/table.go`
- `pkg/vm/engine/tae/txn/txnimpl/cpkey_mask.go`
- `pkg/vm/engine/tae/txn/txnimpl/cpkey_trace.go`

修复策略：

1. **不改普通单列 PK 路径**
2. **只在 hidden composite PK 的 data-table precommit dedup 路径补偿**
3. 在原有 `findDeletes()` 的 rowid-based masking 之后，再用 **当前事务本地 tombstone 里的 PK 列** 做一次补偿 masking
4. 这层补偿同时覆盖：
   - in-memory tombstone node
   - transfer / sink 后注册到 `tableSpace.stats` 的本地 tombstone 对象

这样即使 tombstone 已经被 transfer 到新 rowid，只要它仍表示“本事务已经删过这个 cpkey 的旧版本”，Freeze dedup 就不会再把这个历史版本误当成活跃 duplicate。

## 新增/保留的日志点

如果问题还复现，优先 grep 下面这些日志：

1. `TN-COMPOSITE-PK-FINDDELETES-UNMASKED`
   - 说明 rowid-based `findDeletes()` 之后仍残留候选

2. `TN-COMPOSITE-PK-LOCAL-TOMBSTONE-MASK`
   - **本次新增**
   - 说明本地 tombstone PK 补偿 masking 是否实际吃掉了候选

3. `TN-DEDUP-DUPLICATE-DETAIL`
   - 最终 duplicate 时会带上 duplicate pk / rowid 以及前后快照

如果你后面再给我日志，最有价值的是：

- 这三个 tag 的完整窗口
- 同一 txn 的 `tn.handle.commit.error`
- 同时段 `bmsql_stock` 的 flush / merge / prepare-commit 日志

## 当前验证

已确认：

- `go test ./pkg/vm/engine/tae/txn/txnimpl -run 'TestCompositePKLocalDeleteMasker' -count=1`
- `go test -tags manual ./pkg/vm/engine/tae/db/test -run 'TestManualCompositePKUpdateAfter(Merge|TransferBeforeDedup)' -count=1`

## 备注

之前围绕 `bmsql_district FOR UPDATE` freshness 的那条修复方向，不是这次 `__mo_cpkey_col` duplicate 的主修复面。
