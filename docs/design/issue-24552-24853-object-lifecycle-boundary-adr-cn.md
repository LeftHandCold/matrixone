# ADR：以 TAE Object 作为生命周期执行边界

> 状态：Accepted（2026-07-28 按 Mixed Rewrite 代码评估修订）
>
> 日期：2026-07-25
>
> 适用范围：MatrixOne Issue #24552 / #24853
>
> 规范设计：[TAE 对象级数据生命周期概要设计](issue-24552-24853-tae-object-lifecycle-overview-design-cn.md)
>
> 实现级设计：[TAE Object Lifecycle 详细设计索引](tae-object-lifecycle/README.md)

## 背景

MatrixOne 需要为普通表提供原生 TTL 和可恢复归档。早期方案要求用户先按生命周期列
建立 SQL Range Partition，再以完整分区作为封存、归档和退休单元。

当前 MO Partition Service 使用隐藏物理子表承担路由；`Redefine` 仍包含临时表、
`INSERT SELECT`、Drop 和 Rename 的整表迁移路径。大量数据库、表和按天分区会把对象
生命周期问题放大为 Catalog 表数量、路由和 DDL 问题。非分区表、已有其他分区键的表
及迟到数据也无法自然适配。

TAE 数据已经以不可变 Object 组织，Object 具有行数、大小、ZoneMap 和稳定身份。现有
Reader、mergesort、Object Writer、两阶段 RowID transfer、Object MVCC、WAL 和 GC
已经形成完整数据路径。Lifecycle 应复用这些原语，但不能把远端 Archive I/O 塞进
普通 Merge scheduler 和事务。

## 决策

1. Lifecycle Scanner、Job 和最终事务以有界的 exact TAE Object set 为执行边界。
   单个合法大 Object 使用 streaming，不能假装还能拆成更小 Object set。
2. SQL Partition 不是功能依赖。已有分区只能作为候选发现和物理对齐优化。
3. 不修改普通 Merge 的候选、Level、Overlap、Small、目标大小或调度策略。新增独立
   Lifecycle Rewrite Executor；它复用当前 mergesort/Object Writer/transfer txn
   entry，而不是复制第二套存储引擎。
4. Whole Object 使用独立、版本化的 `OpCommitLifecycle`：
   - Archive 先由固定 Snapshot 的高层 Reader 导出并全量重读校验；
   - 最终短事务重新校验 table/schema generation、Feature Guard、source
     reservation/GC protection、exact Object、Footer digest 和 Tombstone delta；
   - Dataset/Receipt 与 Object 退休在同一正常租户事务提交；
   - 只提交 Object DropIntent，物理源文件仍由现有 TAE GC 回收。
5. 少量 Mixed Object 使用一个普通可写 SI 事务：
   - Reader、Archive 校验、`Relation.Delete(RowID + delete key)` 和 Dataset/Receipt
     使用同一 Snapshot/事务；
   - 复用现有 MVCC、Tombstone、RowID transfer、1PC/2PC 和事务恢复；
   - 受 rows、bytes、affected blocks、Tombstone 和 wall-time 硬预算限制。
6. 中/大 Mixed Object 由 Lifecycle Rewrite Executor 一次完成：
   - 固定 Snapshot 下读取 source Object；
   - 到期可见行同步进入 Parquet/ZSTD，或在 TTL 模式丢弃；
   - 存活行复用 `DoMergeAndWrite` 写成 normal TAE Object；
   - snapshot-deleted 和 expired 行在 transfer map 中为 `NoTransfer`；
   - 最终事务原子发布 Dataset/Receipt、创建 live Object、退休 source Object并提交
     两阶段 Tombstone transfer。
   大规模普通 DELETE 仍然禁止。只有 Rewrite 资源或重复放大超过 release profile 时
   才进入 `RESOURCE_BLOCKED`/`MIXED_LAYOUT_BLOCKED`。
7. Archive 的长期 owner 使用 stable logical table identity/owner generation，TAE
   commit 使用 physical table generation + exact Object identity。owner-transfer
   协议通过前拒绝会替换 physical table ID 的 `ALTER TABLE ... COPY`。
8. 第一次外部副作用前必须创建 system-owned Cleanup Root。副作用包括 Archive PUT、
   TAE live staging Object 和 transfer booking。TTL/Archive 都保留 system
   Attempt/Commit Control 和 final transaction identity。DROP 只写 owner
   tombstone，后台 Sweeper 负责异步清理。
9. Scanner 只覆盖显式 Binding，不日常扫描集群几十万张普通表。当前 Relation
   Metadata/PartitionState 是权威 Object 集合；每个 Binding 只持久化 O(1) 分页
   cursor/watermark 和有界 Candidate，不建设“每 Object 一行”的 Catalog Object
   Index。`CollectObjectList`和可选 packed Summary 只做加速。
10. TN exact source reservation 是普通 Merge/Lifecycle 的并发准入，不是可后置
    correctness 优化：scheduler skip 只是优化，普通 Merge 与 Lifecycle 的最终 TN
    admission 才是线性化边界。
11. 首个 GA 不创建长生命周期 table-only Lifecycle Snapshot。Whole 使用有界只读
    Snapshot，小 Mixed 使用有界可写 SI；Rewrite 使用当前
    `gc.SyncProtectionManager`保护 exact data/tombstone 文件。保护是内存态、可续租，
    TN restart/失效后 final 必须 abort/replan。

## 为什么不选择其他方案

### 强制 SQL Range Partition

- 不能覆盖普通非分区表；
- 对海量表产生隐藏子表和 DDL 放大；
- 生命周期列未必等于业务分区键；
- 迟到数据仍需额外协议；
- 当前 Partition 模块不是对象级 GC、事务和归档所有权系统。

### 在普通 Merge 中直接执行 Lifecycle

- 普通 Merge 会等待 Provider PUT/readback；
- 未启用表也会承受 Lifecycle 分支和回归风险；
- Merge retry、TaskService 和 Archive Cleanup Root 的所有权模型不同；
- Provider 故障会污染正常压缩和前台写放大。

### 复制一套独立 Merge 存储实现

- 会重复排序、writer、transfer、schema 演进和 tombstone 逻辑；
- 修复需要维护两套实现，数据格式和正确性会漂移；
- 本决策只新增独立的 Lifecycle 调度和编排，底层 rewrite 原语必须来自现有 Merge。

### 不实现 Mixed Rewrite

- 生命周期列未必是 physical sort/cluster key，不能假设 Mixed 永远只有一个小尾部；
- 普通 Merge 可能继续把不同生命周期时间的行聚到同一个 Object；
- 高度乱序 TB 表会永久积压，不能满足“一步到位”的 GA 目标；
- 因此 GA 必须实现 Rewrite，但实现为独立 executor 并复用现有 Merge 原语。

### 对所有 Mixed 数据使用大批量 SQL DELETE

- 会产生大量 tombstone 和后续 Merge 放大；
- 行级事务、WAL/RPC 和索引更新难以支撑 TB 级；
- 小 Mixed 尾部可以复用普通 DELETE，但必须有事务一致性证明和硬预算；
- 中/大 Mixed 必须 Rewrite，不能用拆小 Job 绕过 rolling budget。

## 实施顺序

```text
Read-only Planner/分页 Discovery/Dry-run
  -> Export-only Parquet + verification
  -> Feature Guard/Profile/Attempt Root/resource protocol
  -> OpCommitLifecycle + reservation/protection P0
  -> Whole Object TTL/Archive
  -> small Mixed writable-SI Row DELETE
  -> Mixed Rewrite + transfer/WAL/replay
  -> direct-readable Restore/Purge
  -> 1/10 TiB、1000 表和故障放量认证 (Commercial GA)
  -> optional restore-required Deep Archive Profile
  -> optional packed Summary、聚簇建议等优化
```

前两步可以在不退休活动数据的情况下验证功能和成本。只要开始退休 Object，
`OpCommitLifecycle`/WAL/replay、exact Object CAS、reservation/protection、system
retained Cleanup Root、Feature Guard 和资源上限就属于正确性前提。Archive 还必须
增加 pre-side-effect Root、immutable Profile、全量 readback 和 Restore。

小 Mixed 必须证明 Reader/Archive/DELETE 位于同一个可写 SI 事务；Rewrite 必须证明
`source_visible = expired + live`、transfer 映射和 create/drop 原子性。不得以
“后续优化”为由跳过。

“归档可恢复”是 Commercial GA 的必要条件，但“恢复前必须由 provider thaw”不是。
GA 从 direct-readable Parquet/ZSTD payload 恢复到新表；Deep Archive 只在客户
provider 和成本收益明确后增加。

归档可恢复性从属于源表/租户：DROP TABLE/DATABASE/ACCOUNT 后不再承诺 Archive
Restore。Legal Hold/WORM、DROP 后保留、跨租户 Transfer 和 Archive Backup/DR 不在
范围；Backup/PITR/Snapshot Restore/Clone/Branch/DR 对 Lifecycle 表必须双向 fail
closed。

只有八项 P0、真实 1/10 TiB 数据路径、`50 -> 200 -> 500 -> 1000` 分阶段放量和
全部 GA Gate 通过后才是 Commercial GA。当前决策是 Conditional Go，不表示实现
已经 GA。

## 影响

正面影响：

- 非分区表和 TB 级表使用统一机制；
- 普通 Merge 策略和 Provider 故障域不受 Lifecycle 侵入；
- Job、事务和失败范围有明确上限；
- 高度 Mixed 数据不再永久依赖大量 Tombstone；
- 不新增逐 Object Catalog Index、Logtail 双写或 replay。

代价：

- 生命周期列高度乱序时会产生 Rewrite 放大；超过认证预算会明确阻断，不承诺
  Archive Lag SLO；
- 需要独立 Lifecycle opcode、reservation/protection、system Cleanup Root、
  immutable Profile 和 Feature Guard；
- 小 Mixed DELETE 仍会产生 Tombstone、WAL、Logtail 和后续 Merge 放大；
- Whole Snapshot、小 Mixed SI、Rewrite protection/staging 都必须有硬上限；
- 首个 GA 拒绝所有物化隐藏索引表和 archive-unaware Backup/DR；
- 首个 GA 拒绝 Lifecycle-bound 表的 ALTER COPY；
- Discovery cursor 必须处理 Merge 跳位、断档恢复、full cycle 和 Candidate GC；
- release profile 有绑定表、Candidate、并发和积压认证上限，不承诺无上限通用 GA。

## 重新评估条件

只有同时满足以下条件才重新评估“SQL Partition 作为可选 fast path”，且不会改变本
ADR 的默认对象级边界：

- Partition 成为可证明独立的物理对象集合而不是隐藏子表放大；
- partition retirement、index、CDC、GC 和 DDL 已有完整原子协议；
- 大量表/分区 Catalog 与调度基准证明优于对象级路径；
- 非分区表仍由对象级路径完整支持。
