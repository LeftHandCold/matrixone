# ADR：以 TAE Object 而不是 SQL Partition 作为生命周期执行边界

> 状态：Accepted
>
> 日期：2026-07-25
>
> 适用范围：MatrixOne Issue #24552 / #24853
>
> 规范设计：[TAE 对象级数据生命周期概要设计](issue-24552-24853-tae-object-lifecycle-overview-design-cn.md)

## 背景

MatrixOne 需要为普通表提供原生 TTL 和可恢复归档。早期方案要求用户先按生命周期列建立 SQL Range Partition，再以完整分区作为封存、归档和退休单元。

当前 MO Partition Service 使用隐藏物理子表承担路由；`Redefine` 仍包含临时表、`INSERT SELECT`、Drop 和 Rename 的整表迁移路径。大量数据库、表和按天分区会把对象生命周期问题放大为 Catalog 表数量、路由和 DDL 问题。非分区表、已有其他分区键的表及迟到数据也无法自然适配。

TAE 数据已经以不可变 Object 组织，Object 具有行数、大小、ZoneMap 和稳定身份；现有 `mergesort.DoMergeAndWrite` 提供流式读写、排序和 transfer-map 原语。

## 决策

1. Lifecycle Scanner、Job 和最终事务以**有界的 exact TAE Object set**为执行边界；单个超限 Object 使用 streaming，不假装还能拆成更小 Object set。
2. SQL Partition 不是功能依赖。已有分区只能作为候选发现和物理对齐优化。
3. 不新增第二套 Merge Engine，也不修改普通 Merge 的候选、Level、Overlap、Small、目标大小或调度策略。
4. Mixed Object 使用独立的 **Lifecycle Rewrite Executor**：
   - 过期行进入 Discard/Archive Sink；
   - 存活行使用稳定的 Merge sort/write 原语写回 ObjectIO；
   - 最终通过独立、可重放的 Lifecycle Commit 协议替换源对象。
5. Whole Object、Mixed Object、TTL 和 Archive 使用同一 Catalog、Lifecycle Snapshot、Attempt/Commit Control、Feature Guard、commit receipt 和资源预算模型；Archive child 另有 Attempt/Cleanup Root。
6. 首个 GA 用与现有 user/branch Snapshot 行为隔离的 `kind='lifecycle'` table-only Snapshot 保护精确 physical table generation；只有 Snapshot 提交、数据 flush、GC metadata-visible 且旧 GC cycle drained 后才能选择 Object。correctness 再由 exact physical object MVCC CAS、Feature Guard 和 durable receipt 闭环。
7. Archive 的长期 owner 使用 stable logical table identity/owner generation，TAE commit 使用 physical table generation + exact Object identity。首个 GA 在 owner-transfer 协议通过前拒绝会替换 physical table ID 的 `ALTER TABLE ... COPY`。
8. 第一次外部 PUT 前必须创建 system-owned Archive Attempt Root；TTL/Archive 都必须保留 system Attempt/Commit Control 和 final transaction identity。Archive 从属于 logical owner/account incarnation，DROP 只写 owner tombstone，provider cleanup 由后台 Sweeper 完成。
9. Object Index 和 scheduler 只覆盖显式 Binding，不日常扫描集群几十万张普通表。首个 release profile 最多认证 500～1000 张绑定表，并同时执行 Object Index、backlog、Snapshot retained bytes、Job 和外部对象硬上限。
10. TN scope reservation 只减少与普通 Merge 的冲突，是可后置优化；持续冲突到阈值进入 `CONFLICT_BLOCKED`，不修改普通 Merge 策略。

## 为什么不选择其他方案

### 强制 SQL Range Partition

- 不能覆盖普通非分区表；
- 对海量表产生隐藏子表和 DDL 放大；
- 生命周期列未必等于业务分区键；
- 迟到数据仍需额外协议；
- 当前 Partition 模块不是对象级 GC、事务和归档所有权系统。

### 在普通 Merge 中直接执行 Lifecycle

- 会让未启用 Feature 的表进入 Policy 和 Archive 分支；
- 外部上传、重试和 Manifest 状态会污染 TN 后台 Merge；
- 无法保持普通 Merge 的故障域和行为稳定。

### 复制一套 Merge Engine

- 重复排序、writer、transfer、schema 演进和 tombstone 逻辑；
- 修复需要维护两套实现；
- 长期数据格式和正确性会漂移。

### 先用大批量 SQL DELETE 作为正式实现

- 会产生大量 tombstone 和后续 Merge 放大；
- 行级事务、WAL/RPC 和索引更新难以支撑 TB 级；
- 可以用于小规模实验或对照测试，不能作为 Commercial GA 主路径。

## 实施顺序

```text
Read-only Planner/Object Index/Dry-run
  -> Export-only Parquet + verification
  -> tagged commit/table-only Snapshot-GC gate/Attempt Control/Archive Root/Feature Guard/resource protocol
  -> Whole Object TTL/Archive
  -> Mixed Object Lifecycle Rewrite Executor
  -> direct-readable Archive/Restore/Purge (Commercial GA)
  -> optional restore-required Deep Archive Profile
  -> reservation、聚簇建议等优化
```

前两步可以在不退休活动数据的情况下先验证功能和成本。只要开始从活动表移除数据，tagged commit/replay、table-only Snapshot-GC gate、system retained commit control、Feature Guard 和资源上限就属于正确性前提；Archive 还必须增加 pre-PUT Root 与 immutable Profile。不得以“后续优化”为由跳过。

“归档可恢复”是 Commercial GA 的必要条件，但“恢复前必须由 provider thaw”不是。首个 GA 从 direct-readable Parquet/ZSTD payload 恢复到新表；Deep Archive 只在客户 provider 和成本收益明确后增加，不改变本 ADR 的对象级执行边界。

首个 GA 的可恢复性从属于源表/租户：DROP TABLE/DATABASE/ACCOUNT 后不再承诺 Archive Restore。Legal Hold/WORM、DROP 后保留、跨租户 Transfer 和 Archive Backup/DR 不在范围；Backup/PITR/Snapshot Restore/Clone/Branch/DR 对 Lifecycle 表必须双向 fail closed。该收敛方案只有在六项 GA P0、真实 1/10 TiB 数据路径、`50 -> 200 -> 500 -> 1000` 分阶段放量和完整 Gate E 证据通过后才是 Commercial GA，因此决策状态为 Conditional Go，不表示当前实现已经 GA。

## 影响

正面影响：

- 非分区表和 TB 级表使用统一机制；
- 普通 Merge 策略不受 Lifecycle Policy 侵入；
- Job、事务和失败范围有明确上限；
- 可以根据 whole/mixed 比例解释实际重写成本。

代价：

- Mixed Object 必须读取并重写存活行；
- 需要 tagged Lifecycle Commit、table-only Snapshot GC-visible gate、system Attempt Control/Root/owner tombstone 和 Feature Guard；
- table-only Lifecycle Snapshot 仍可能保留该表与当前 Job 无关的 Merge 旧版本，必须按 exclusive retained bytes 限额并完成 10 TiB sustained-Merge 验证；
- 首个 GA 拒绝所有物化隐藏索引表和 archive-unaware Backup/DR；
- 首个 GA 拒绝 Lifecycle-bound 表的 ALTER COPY，直至 logical owner/physical source transfer 协议通过；
- Object Index 必须处理 generation、断档恢复和 obsolete GC；
- 首个 release profile 有绑定表、索引行、并发和积压认证上限，不承诺无上限通用 GA；
- 生命周期列不聚簇时重写放大会较高。

## 重新评估条件

只有同时满足以下条件才重新评估“SQL Partition 作为可选 fast path”，且不会改变本 ADR 的默认对象级边界：

- Partition 成为可证明独立的物理对象集合而不是隐藏子表放大；
- partition retirement、index、CDC、GC 和 DDL 已有完整原子协议；
- 大量表/分区 Catalog 与调度基准证明优于对象级路径；
- 非分区表仍由对象级路径完整支持。
