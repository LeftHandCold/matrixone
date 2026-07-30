# MatrixOne Issue #24853：分层数据生命周期行业调研与历史方案记录

> **非规范文档，禁止作为实现规格。** 本文只保留行业调研、术语、早期 Partition 方案及其 Review 演进记录。文中所有以 SQL Range Partition、TN partition generation fence、Phase 1/2 或 C+ 分区方案为前提的“推荐”“必须”“Go/No-Go”均为已经被取代的历史结论。
>
> Issue #24552 / #24853 当前唯一规范架构来源是 [MatrixOne TAE 对象级数据生命周期概要设计](issue-24552-24853-tae-object-lifecycle-overview-design-cn.md)，架构边界由 [ADR：以 TAE Object 而不是 SQL Partition 作为生命周期执行边界](issue-24552-24853-object-lifecycle-boundary-adr-cn.md) 固化。实现、测试、交付和 Commercial GA 评审只能引用这两份文档。
>
> 状态：历史调研稿，非规范、非实现输入
>
> 复核日期：2026-07-25
>
> MatrixOne 原始分析基线：`main@0d7eeb38b43b6b89f746b0a634662349a4b01de2`
>
> Review 后复核基线：`main@e936d9757027325a8270b754938840ac7c8f8289`
>
> MatrixOne Docs 基线：`main@b38d52fd99f899846015f0ffc0dfacd8f12da10a`
>
> 目标 Issue：[#24853](https://github.com/matrixorigin/matrixone/issues/24853)

## 1. 当前有效结论与本文用途

当前有效结论只有以下几项：

1. 历史版本保留、业务生命周期和在线物理放置是三个独立平面；
2. 不允许云 Bucket Lifecycle 直接迁移或删除活动 TAE Object；
3. Archive Payload 与活动 Object 物理分离，但首个 GA 的 Dataset 所有权从属于 stable logical table owner generation/account incarnation；DROP 后不承诺恢复；
4. 当前规范以有界 exact TAE Object set 为 Job 和事务边界，不依赖 SQL Partition；
5. 普通 Merge 策略保持不变；TTL 小 Mixed 才复用有界普通 DELETE，任意 Archive Mixed 与中/大
   TTL Mixed 都由独立 Lifecycle Rewrite Executor 处理；
6. Source Pin 首个 GA使用与现有 user/branch 行为隔离的 `kind='lifecycle'` table-only Snapshot，保护 exact physical table generation，并在选 Object 前通过 flush gate 与 GC metadata-visible/old-cycle-drained gate；不新增 exact-object ref；
7. 每个 TTL/Archive child 都有 system-retained Attempt/Commit Control；Archive 第一次外部 PUT 前另建 Cleanup Root。DROP 只写 owner tombstone，provider cleanup 由后台 Sweeper 完成；
8. Commercial GA 必须包含 TTL、direct-readable archive、恢复到新表和不可逆 Purge；Legal Hold/WORM、DROP 后保留、Archive Backup/DR 和 restore-required deep archive 不属于首个 GA；
9. 未实现 archive-aware 语义的 Backup/PITR/Snapshot Restore/Clone/Branch/DR 必须 fail closed；不能静默恢复缺少已归档历史行的表；
10. Object Index 和 scheduler 只覆盖显式 Binding，不扫描集群几十万张普通表；首个 release profile
    以账户 Guard=1000 与 Lifecycle-only 集群 activation slot=1000限制启用规模，并同时限制
    index/backlog/retained bytes/Job/外部对象；active coexistence 不得影响未绑定表普通 MO；
11. 收敛后方案是 Conditional Go：六项 GA P0、1/10 TiB、Stage 4 与 Gate E 的实现、故障测试证据完成后才能称为 Commercial GA。

本文后续章节用于理解行业背景、早期设计为什么被否决以及哪些所有权原则仍可复用。任何与当前规范冲突的内容均以对象级概要设计和 ADR 为准。

## 2. 首先纠正 Issue 中的语义混合

### 2.1 三个时间轴不是一回事

| 平面 | 时间依据 | 用户问题 | MO 当前能力 | 推荐语义 |
|---|---|---|---|---|
| 历史版本保留 | commit/drop timestamp | “能否读取昨天被更新或删除的数据？” | Snapshot、PITR、GC 引用保护 | `HISTORY RETENTION`，可提供 Snowflake 兼容别名 |
| 业务数据生命周期 | 业务时间列或业务条件 | “两年前的订单是否仍属于当前表？” | 当前无原生 TTL；#24552 正在请求 | `EXPIRE`、`ARCHIVE` |
| 在线物理放置 | 对象创建时间、访问热度、工作负载 | “直接查询时应使用什么成本/性能等级？” | 对象存储 + CN 内存/磁盘/远端缓存 | `HOT`/`ONLINE_COLD` 访问契约 |

例如一行 `event_ts='2024-01-01'` 的数据可能今天才被补录。按业务生命周期它应立即归档；按对象创建时间它仍是新对象；按 PITR 它又必须保留今天写入后的历史版本。三个时间轴不可互换。

### 2.2 Fail-safe 不是 Archive

Snowflake Fail-safe 和 BigQuery fail-safe 都是历史版本窗口之后的平台灾难恢复保护，不是用户可查询、可自行恢复的业务归档层。将 MO 的 `ARCHIVE` 描述为“类似 Fail-safe，但可由用户恢复”，会混淆以下边界：

- 谁可以发起恢复；
- 保留时长能否配置；
- 数据是否属于当前逻辑表；
- 是否用于合规留存；
- 是否可以按业务谓词选择数据；
- 是否计入历史版本保留成本。

MO 应把历史保护和业务归档分别建模，只在 GC 引用图上汇合。

### 2.3 最短留存和最晚删除也不是一回事

“至少保留 730 天”和“第 730 天必须删除”是相反方向的约束：

- `MINIMUM RETENTION`：730 天内不得删除，超过后允许删除；
- `MAXIMUM RETENTION`：超过 730 天不得继续保留，常见于隐私和数据最小化要求；
- `LEGAL HOLD`：在 hold 解除前不得删除；
- Snapshot、PITR、Data Branch 可能继续引用活动表的旧对象。

如果产品承诺“730 天必须物理删除”，就必须同时限制或级联清理超过该期限的 Snapshot、PITR、Data Branch、备份和 legal hold。否则最多只能承诺“从当前表逻辑不可见，物理回收受历史引用阻塞”。

## 3. 当前 MO 能力与真实边界

本节代码事实最初在 `main@0d7eeb38` 确认，并已复核到 `main@e936d975`。代码链接仍固定到原始 commit，以保持行号稳定；两个基线间与本方案相关的分区 metadata、Drop、Snapshot/PITR GC、TaskService 和对象 soft-delete 契约没有发生改变。

当前 MatrixOne Docs 已覆盖 Storage Hierarchy、Snapshot Read 和 PITR Tool，但没有定义业务数据 lifecycle policy、云归档状态机或 Restore Job 协议。因此这些文档能说明现有历史保护和存储分层基础，不能作为 #24853 已具备产品语义的证据。

### 3.1 可复用能力

#### Snapshot/PITR 已经进入对象 GC 判定

- `mo_snapshots` 和 `mo_pitr` 已记录 cluster/account/database/table 级范围和稳定对象 ID。
- `SnapshotInfo.GetTS` 会在 cluster、account、database、table 各层选择最早的适用 PITR 时间，因此最宽的适用窗口会保护对象。
- `AccountToTableSnapshots` 将不同层级引用映射到表。
- `ObjectIsSnapshotRefers` 和 `MakeSnapshotAndPitrFineFilter` 在物理 GC 前判断对象是否仍被 Snapshot/PITR 引用。

代码证据：

- [`pkg/frontend/predefined.go`](https://github.com/matrixorigin/matrixone/blob/0d7eeb38b43b6b89f746b0a634662349a4b01de2/pkg/frontend/predefined.go#L131-L167)
- [`pkg/vm/engine/tae/logtail/snapshot.go`](https://github.com/matrixorigin/matrixone/blob/0d7eeb38b43b6b89f746b0a634662349a4b01de2/pkg/vm/engine/tae/logtail/snapshot.go#L229-L262)
- [`pkg/vm/engine/tae/db/gc/v3/exec_v1.go`](https://github.com/matrixorigin/matrixone/blob/0d7eeb38b43b6b89f746b0a634662349a4b01de2/pkg/vm/engine/tae/db/gc/v3/exec_v1.go#L373-L479)

这部分适合复用为 lifecycle 的“历史引用是否允许物理回收”判定，但不应复用 `mo_pitr` 作为 lifecycle policy 表。

当前 GC 保护集合不会因为 lifecycle Job 中只有一个 `source_snapshot_ts` 字段就自动 pin 住源对象。当前规范已经选择严格复用 system-managed Lifecycle Snapshot，并要求数据 flush gate 与 GC snapshot-metadata-visible/old-cycle-drained gate；不再采用历史方案的 exact `lifecycle_source_ref`。

#### Data Branch 已通过系统 Snapshot 保护历史

当前 Data Branch 会创建 `kind='branch'` 的系统管理 Snapshot，保护 LCA 侧历史，直到对应分支子树被回收。也就是说，即使 lifecycle 已从当前表归档或过期一批数据，Data Branch 仍可能让原活动对象长期不能 GC。

代码与设计证据：

- [`docs/design/data_branch_protect_snapshot.md`](./data_branch_protect_snapshot.md)
- [实现 PR #24313](https://github.com/matrixorigin/matrixone/pull/24313)

因此生命周期必须展示 `logical_expired_bytes`、`history_pinned_bytes` 和 `physically_reclaimed_bytes`，不能只展示“已归档行数”。

#### TaskService 可作为调度基础

当前 TaskService 支持注册系统 Task Executor，SQL Task 也已经具备 cron、条件、重试、超时等能力。生命周期执行可复用调度和持久化框架，但不应直接实现成一条周期 SQL：

- 任务需要稳定 ID、policy generation、lease/fencing；
- 要记录对象级进度和外部存储请求；
- 进程崩溃后必须从中间阶段恢复；
- 云端恢复可能持续数小时甚至数天，不能绑定普通 SQL session；
- 重试不能重复发布归档，也不能重复删除活动数据。

代码证据：

- [`pkg/cnservice/server_task.go`](https://github.com/matrixorigin/matrixone/blob/0d7eeb38b43b6b89f746b0a634662349a4b01de2/pkg/cnservice/server_task.go#L293-L381)
- [`pkg/taskservice/task_runner.go`](https://github.com/matrixorigin/matrixone/blob/0d7eeb38b43b6b89f746b0a634662349a4b01de2/pkg/taskservice/task_runner.go#L317-L333)

#### 对象级 Soft Delete 和分区删除可作为退休原语候选

当前 `txnTable.SoftDeleteObject` 会把对象级 soft-delete 写入当前 CN 事务，TN 在 precommit 阶段把对象的 `deleteat` 设置为事务提交时间；range/list 分区也已经支持 `ALTER TABLE ... DROP PARTITION`。这两条路径比“按行生成海量 tombstone”更接近归档后的活动数据退休需求。

代码证据：

- [`txnTable.SoftDeleteObject`](https://github.com/matrixorigin/matrixone/blob/0d7eeb38b43b6b89f746b0a634662349a4b01de2/pkg/vm/engine/disttae/txn_table.go#L2108-L2145)
- [`HandleSoftDeleteObject`](https://github.com/matrixorigin/matrixone/blob/0d7eeb38b43b6b89f746b0a634662349a4b01de2/pkg/vm/engine/tae/rpc/handle.go#L1142-L1180)
- [`partitionservice.Service.DropPartitions`](https://github.com/matrixorigin/matrixone/blob/0d7eeb38b43b6b89f746b0a634662349a4b01de2/pkg/partitionservice/service.go#L209-L249)

但它们目前只是候选原语，不是现成 lifecycle 事务：

- 没有把 archive Manifest 发布与对象/分区退休组合起来的已验证协议；
- snapshot transaction 明确禁止 `SoftDeleteObject`，所以“读取旧 Snapshot”和“在当前事务退休对象”必然是两个阶段；
- 长时间复制期间发生的 INSERT/UPDATE/DELETE、tombstone 和 merge 必须由 TN generation fence 和稳定提交水位处理；
- 直接 soft-delete 基础对象可能让 secondary/fulltext/vector index 留下悬空条目；
- range/list 分区可以删除，不代表当前已经存在“分区只读/封存”的写入栅栏。

最值得验证的 MVP 路径是：给时间 range 分区增加 TN 权威 generation/fence，以分区为选择和并发隔离单元，归档成功后在短事务中发布 Manifest 并删除分区。对象级路径应在上述协议和索引一致性验证完成后再开放。

当前代码进一步证明 CN 本地 sealed flag 不足以保证正确性：

- 分区 catalog 只有描述和映射，没有 lifecycle state/generation：[`partitionservice/types.go`](https://github.com/matrixorigin/matrixone/blob/e936d9757027325a8270b754938840ac7c8f8289/pkg/partitionservice/types.go#L31-L48)；
- `PartitionInsert.Prepare` 会读取并缓存分区 metadata，随后按该路由写物理子表：[`insert_partition.go`](https://github.com/matrixorigin/matrixone/blob/e936d9757027325a8270b754938840ac7c8f8289/pkg/sql/colexec/insert/insert_partition.go#L65-L143)；
- Snapshot/非 RC 事务可能继续使用已经缓存的 table delegate，不检查新版本：[`disttae/txn.go`](https://github.com/matrixorigin/matrixone/blob/e936d9757027325a8270b754938840ac7c8f8289/pkg/vm/engine/disttae/txn.go#L2384-L2422)。

因此 CN admission 只能提前报错和减少浪费；旧路由、跨 CN 事务和已经 prepare 的写入必须在 TN pre-commit 通过 generation fence 最终裁决。

#### Stage 可做临时人工方案

当前 MO 支持把查询结果导出为 Stage 上的 Parquet，再通过外部流程管理：

```sql
SELECT *
FROM events
WHERE event_ts < '2024-01-01'
INTO OUTFILE 'stage://archive/events_%d.parquet'
FORMAT 'parquet';
```

测试证据：[`test/distributed/cases/stage/export_format.sql`](https://github.com/matrixorigin/matrixone/blob/0d7eeb38b43b6b89f746b0a634662349a4b01de2/test/distributed/cases/stage/export_format.sql#L119-L177)。

这可以作为 Phase 0 的手工 workaround，但 Stage 当前是外部导入导出和凭据抽象，不是 TAE 内部对象的所有权、引用计数、GC 与 crash-consistency 抽象。

### 3.2 当前缺失的关键能力

#### ObjectStorage 接口没有 lifecycle 能力

当前 `ObjectStorage` 只有：

- `List`
- `Stat`
- `Exists`
- `Write`
- `Read`
- `Delete`

没有：

- storage class/profile；
- server-side copy/rewrite；
- object tag；
- restore/rehydrate；
- restore status；
- object version/generation/CAS；
- lifecycle event；
- checksum/完整性查询。

代码证据：[`pkg/fileservice/object_storage.go`](https://github.com/matrixorigin/matrixone/blob/0d7eeb38b43b6b89f746b0a634662349a4b01de2/pkg/fileservice/object_storage.go#L139-L189)。

其中 `Delete(ctx, keys...)` 只接收 key，没有统一的 version/generation match 条件。因此不可变 key 能避免旧 runner 覆盖新 payload，却不能单独阻止旧 runner 删除对象；archive GC 还需要 catalog 中不可逆的 delete intent、禁止重新引用和 key 永不复用。

AWS 适配器的 `PutObjectInput` 和 `CreateMultipartUploadInput` 当前也没有设置 `StorageClass` 或归档标签。S3FS 还会根据 Endpoint 选择 AWS、MinIO、COS、OSS、HDFS 或本地磁盘适配器，因此新能力不能假定所有后端都支持同一种操作。

#### 当前对象定位不能表达“另一个桶/存储 profile”

`ObjectName` 是 Segment UUID、序号和字符串组成的固定 60 字节标识；`Location` 是 `ObjectName + Extent + Rows + BlockID`。其中没有：

- table ID；
- bucket/profile ID；
- storage class；
- object generation；
- lifecycle state。

S3FS 只是把文件路径映射为 `<keyPrefix>/<filePath>`。因此：

1. 不能只修改现有 `Location` 就把对象移到另一个 bucket/profile；
2. 当前对象名不是按表组织的，无法用 provider prefix rule 精确选择一张表；
3. 如果直接对共享前缀配置规则，可能把 checkpoint、GC、catalog/logtail 或其他内部对象一起沉降；
4. 如果依赖 tag，需要修改所有写路径并解决多云 tag 语义、异步传播和 catalog 一致性。

代码证据：

- [`pkg/objectio/name.go`](https://github.com/matrixorigin/matrixone/blob/0d7eeb38b43b6b89f746b0a634662349a4b01de2/pkg/objectio/name.go#L27-L53)
- [`pkg/objectio/location.go`](https://github.com/matrixorigin/matrixone/blob/0d7eeb38b43b6b89f746b0a634662349a4b01de2/pkg/objectio/location.go#L47-L69)
- [`pkg/fileservice/s3_fs.go`](https://github.com/matrixorigin/matrixone/blob/0d7eeb38b43b6b89f746b0a634662349a4b01de2/pkg/fileservice/s3_fs.go#L246-L252)

#### 任意 `AGE BY column` 目前不能低成本判断

当前 `ObjectStats` 的固定格式只保存：

- object name；
- extent；
- row/block count；
- **一个 sort-key ZoneMap**；
- object size/original size；
- reserved flags/merge level。

所以此前设想的 `object.max(event_time) < cutoff` 只在 lifecycle 列正好能由当前 sort-key ZoneMap 表达时成立。对任意时间列，系统只能：

1. 读取对象内 metadata；
2. 扫描对象；
3. 为 lifecycle 列增加新的热端统计目录；
4. 或要求 lifecycle 列与时间分区/排序键对齐。

归档 MVP 应把第 4 种进一步收窄为时间 range 分区，只处理完整分区，并为归档 Manifest 保存后续恢复所需的列级 min/max。仅与 sort key 对齐仍不足以定义分区封存和 DML 写入栅栏，应留到对象级协议成熟后。

代码证据：[`pkg/objectio/object_stats.go`](https://github.com/matrixorigin/matrixone/blob/0d7eeb38b43b6b89f746b0a634662349a4b01de2/pkg/objectio/object_stats.go#L55-L59) 和 [`SortKeyZoneMap`](https://github.com/matrixorigin/matrixone/blob/0d7eeb38b43b6b89f746b0a634662349a4b01de2/pkg/objectio/object_stats.go#L236-L238)。

#### 对象 metadata 与 payload 在同一个对象里

`LoadObjectMetaByExtent` 在 meta cache miss 后，仍需通过 FileService 从原对象指定 extent 读取 metadata。若把整个对象转入 restore-required 存储层：

- cache 命中时 planner 可能暂时正常；
- cache miss 后连 block/column metadata 都不可读；
- 无法可靠判断查询需要恢复哪些对象；
- 集群重启或 cache 驱逐后行为会改变。

因此深归档必须在在线存储中保留独立 Manifest/metadata，不能只依赖被冻结对象内的 metadata。

代码证据：

- [`pkg/objectio/cache.go`](https://github.com/matrixorigin/matrixone/blob/0d7eeb38b43b6b89f746b0a634662349a4b01de2/pkg/objectio/cache.go#L257-L281)
- [`pkg/objectio/reader.go`](https://github.com/matrixorigin/matrixone/blob/0d7eeb38b43b6b89f746b0a634662349a4b01de2/pkg/objectio/reader.go#L129-L153)

#### Cache 会绕过底层对象状态

S3FS 的读顺序会先检查 vector cache、memory cache、disk cache、remote cache，之后才访问对象存储。如果 provider 在 MO 不知情的情况下把对象转入归档：

- 某个 CN cache 命中时查询成功；
- 另一个 CN 或 cache miss 时查询失败；
- provider 看到的 GET 并不包含 MO cache hit，基于访问时间的自动分层会把“对用户仍然很热”的对象误判为冷；
- lifecycle 状态改变时当前没有跨 CN cache invalidation 路径。

代码证据：

- [`S3FS.Read`](https://github.com/matrixorigin/matrixone/blob/0d7eeb38b43b6b89f746b0a634662349a4b01de2/pkg/fileservice/s3_fs.go#L521-L669)
- [`S3FS.Delete`](https://github.com/matrixorigin/matrixone/blob/0d7eeb38b43b6b89f746b0a634662349a4b01de2/pkg/fileservice/s3_fs.go#L1210-L1237)

生命周期检查必须位于 cache 之前，并把 policy generation/lifecycle generation 纳入缓存有效性。

#### Merge、DML、Tombstone 和索引都假定活动对象可读

TAE merge 会扫描源数据对象或 tombstone 对象、写入新对象，再在事务中 soft-delete 源对象。merge 读还显式使用 `SkipAllCache`，所以“缓存中还有副本”不能拯救被冻结的对象。

代码证据：[`pkg/vm/engine/tae/tables/jobs/mergeobjects.go`](https://github.com/matrixorigin/matrixone/blob/0d7eeb38b43b6b89f746b0a634662349a4b01de2/pkg/vm/engine/tae/tables/jobs/mergeobjects.go#L240-L309)。

由此产生的约束是：

- restore-required 对象不能继续作为普通活动对象参与 merge；
- `UPDATE`/`DELETE` 影响归档单元时必须失败、先恢复，或在归档前锁定/重写；
- tombstone 不一定有业务时间列，不能独立按 `event_ts` 归档；
- secondary/fulltext/vector index 不能被 provider 规则独立归档；应随逻辑归档失效、归档其依赖关系，或恢复时重建；
- `ObjectStats.reserved` 的高位已经用于 merge level，不能直接偷用来保存 tier。

### 3.3 `mo_pitr` 和 `mo_stages` 不能直接复用为 policy 表

`mo_pitr.pitr_length` 是 `tinyint unsigned`，前端校验范围是 1–100，单位支持 h/d/mo/y。它表达的是滚动历史窗口，不适合表达精确的 730 天业务归档、多个 tier 阈值、policy generation、恢复状态和 storage profile。

`mo_stages` 只保存 URL、credentials、status、created time 和 comment，没有内部对象所有权、能力声明、版本、引用和 GC 状态。

代码证据：

- [`mo_pitr`](https://github.com/matrixorigin/matrixone/blob/0d7eeb38b43b6b89f746b0a634662349a4b01de2/pkg/frontend/predefined.go#L143-L167)
- [`pitr.go`](https://github.com/matrixorigin/matrixone/blob/0d7eeb38b43b6b89f746b0a634662349a4b01de2/pkg/frontend/pitr.go#L325-L339)
- [`mo_stages`](https://github.com/matrixorigin/matrixone/blob/0d7eeb38b43b6b89f746b0a634662349a4b01de2/pkg/frontend/predefined.go#L219-L229)

结论是：可以复用 PITR 的引用判定和 Stage 的连接解析经验，但 lifecycle 需要独立 catalog 和内部 storage profile。

## 4. MO 旧 Retention 实现提供的反面经验

MO 曾在 [PR #18306](https://github.com/matrixorigin/matrixone/pull/18306) 实现：

```sql
CREATE TABLE ... WITH RETENTION PERIOD n DAY;
```

该实现不是行级 TTL，而是：

1. 在 `mo_retention` 保存数据库名、表名和一个 deadline；
2. cron task 扫描所有租户；
3. deadline 到期后先删除 `mo_retention` 行，再执行 `DROP TABLE`。

这套代码随后通过 [Issue #22255](https://github.com/matrixorigin/matrixone/issues/22255) / [PR #22261](https://github.com/matrixorigin/matrixone/pull/22261) 在 2025 年被整体移除。Issue 和 PR 没有说明产品层面的移除原因，因此不能臆测原因；但旧代码和历史 Bug 已经暴露了新设计必须规避的问题：

- catalog 使用 `(database_name, table_name)`，rename 和重建不稳健；
- 只有一个 deadline，没有 policy generation 和执行流水；
- task 内用字符串拼 DDL；
- 旧 executor 在 `ReadRows` callback 内遇到 `DROP TABLE` 错误只返回 `false`，外层事务函数仍返回 `nil`，可能提交“retention 记录已删除、表仍存在”的状态；
- 外键阻止 `DROP TABLE` 时出现过记录被清掉但表未删除的问题：[#18647](https://github.com/matrixorigin/matrixone/issues/18647)；
- 出现过跨租户强制执行不生效：[#18651](https://github.com/matrixorigin/matrixone/issues/18651)；
- retention BVT 多次出现时序脆弱和 Snapshot/restore 交互问题：[#18886](https://github.com/matrixorigin/matrixone/issues/18886)、[#19161](https://github.com/matrixorigin/matrixone/issues/19161)；
- `mo_retention` 升级缺失曾导致表无法 DROP：[#19313](https://github.com/matrixorigin/matrixone/issues/19313)。

新 lifecycle 不能退回“catalog 一行 + cron SQL + 最终 DDL”的实现模式，必须有对象级 journal、幂等键、lease/fencing、事务发布和故障恢复。

## 5. 2026 年主流方案对比

### 5.1 数据库/分析系统

| 系统 | 当前方案 | 对 MO 的启示 |
|---|---|---|
| Snowflake | Storage Lifecycle Policy 基于行表达式每日执行；官方只说明大操作按小批次、可跨多日完成，没有公开其 micro-partition 重写/删除算法；可归档到 COOL/COLD 或直接过期；归档行不直接查询，通过带 `WHERE` 的 `CREATE TABLE ... FROM ARCHIVE OF` 新建表恢复；COLD 最长可 48 小时 | 不臆测其内部扫描/对象复制实现；恢复到新表、强制范围、热端 archive metadata、DML 锁、临时双份存储都值得借鉴 |
| Databricks Delta | Public Preview；依赖外部 S3 lifecycle 和手工同步的 `delta.timeUntilArchived`；只允许能排除归档文件的查询，其余早失败；`MERGE/UPDATE/DELETE` 需先恢复 | 是“provider lifecycle 做事实源”的风险样本：策略漂移、`_delta_log` 误归档、改长阈值不能自动回热、只靠有限 file stats |
| Elasticsearch ILM | 对 rollover 后的 immutable backing index 做 hot/warm/cold/frozen/delete；searchable snapshot 前通常 force-merge；不能直接处理 write index | 生命周期单元应先封存，并在归档前 compact；不应对任意可变行或当前写对象直接沉降 |
| Tiger/Timescale | 以时间 chunk 为单位异步迁移到对象存储；tiered chunk 不可 INSERT/UPDATE/DELETE；catalog 保存引用；引用归零后还延迟 14 天再 hard delete 以支持 PITR | “封存 chunk + catalog reference + delayed hard delete”与 MO 最匹配 |
| ClickHouse | TTL 可删除、移动、rollup，实际在 merge 中执行；官方建议 TTL 列与 partition key 对齐，从而整分区删除 | MO 归档 MVP 应先要求生命周期列与 range 分区对齐；对象级重写后置 |
| BigQuery | 90 天未修改后自动转长期存储计价，不降低性能/可用性；物理层对用户隐藏 | 在线 HOT/COLD 可以优先做自动优化，而不是暴露云厂商物理 class |
| Redshift RA3/RG | SSD 保存热块、S3 保存冷块，按 block temperature、age 和 workload 自动放置 | MO 的 HOT 更接近 CN cache/QoS，未必需要用户指定一个“热桶” |
| Oracle Heat Map/ADO | 跟踪 block 修改与 segment 访问统计，按 row/segment/tablespace policy 做压缩、In-Memory 驱逐或 segment 级 tablespace 迁移；storage tiering 只支持 segment scope | 热度应是数据库自身可观测信号；物理迁移应以完整 segment/partition 为单位，不能把 row-level policy 等同于任意对象跨介质 |
| Apache Doris | `STORAGE POLICY + cooldown_ttl` 把数据放远端；不支持 Unique MOW、backup 等组合，已冷却数据不会因 policy 改长自动回迁 | 原地分层会向 DML/备份/策略变更传播大量限制，第一版不宜承诺全表型透明兼容 |
| Apache Iceberg | 只有当数据文件不再被任何可 time-travel/rollback 的 Snapshot 引用时才删除；过短 orphan retention 会误删在途文件并损坏表 | 完整合规产品最终需要统一引用图；首个 MO GA 通过拒绝 archive-unaware Snapshot/PITR/Branch 和排除 Legal Hold 来收敛范围，staging cleanup 仍必须有 Root 与 grace/quiescence |

对 Snowflake 的 DROP 语义必须单独纠正：

- Snowflake 的 Lifecycle Job 每日运行并按小批次处理，运行时会锁该表的 UPDATE/DELETE/MERGE，但允许 INSERT/COPY；
- 归档行不能直接查询，通过 `CREATE TABLE ... FROM ARCHIVE OF` 恢复到新表；
- Snowflake `DROP TABLE` 不是立即物理删除：表先保留在 Time Travel，适用窗口内 `UNDROP TABLE` 会连同 Archive 数据一起恢复；进入 Fail-safe 后 Archive 仍可能由 Snowflake Support 恢复；
- Snowflake 不把 COOL/COLD Archive 复制到 failover 目标，官方明确说明 failover 后目标账户不可使用源账户 Archive。

MO 首个 GA 选择更简单但不同的契约：DROP TABLE/DATABASE/ACCOUNT 后即放弃 Archive Restore SLA，并由 system Sweeper 异步级联清理。这是为减少 DROP、租户删除和合规所有权协议而做的**有意差异**，不能描述为“与 Snowflake DROP 语义接近”，也不能宣传成独立七年合规归档。

### 5.2 云对象存储的“归档”并非统一语义

| 云服务 | 在线低成本层 | 深归档语义 | 重要限制 |
|---|---|---|---|
| AWS S3 | Standard-IA、Glacier Instant Retrieval、Intelligent-Tiering 的在线层可直接 GET | Glacier Flexible/Deep Archive 和 Intelligent-Tiering archive access 需 Restore；恢复副本可能是临时的 | 小于 128 KB 默认不迁移；30/90/180 天最短时长；异步执行；提前删除和每对象请求有成本 |
| Google Cloud Storage | Nearline、Coldline、Archive 都可低延迟直接读 | 名称为 Archive，但不需要离线恢复，通常毫秒访问 | Archive 有 365 天最短时长和 retrieval fee |
| Azure Blob | Hot/Cool/Cold 在线 | Archive 离线；standard 最长约 15 小时，high priority 小对象可低于 1 小时 | 高优先级账户级吞吐约 10 GiB/h；任务不可取消；180 天最短时长；回热后可能被旧 lifecycle 立即再次归档 |
| Aliyun OSS | IA 在线；Archive 可选择约 1 分钟 restore 或开启直接读 | Cold Archive/Deep Cold Archive 需 restore | Archive Direct Read 是 bucket 能力且 retrieval fee 更高；不同层最小大小/保留时长不同 |
| Tencent COS | IA 在线；Archive/Deep Archive 需恢复 | Archive 约 1–12 小时，Deep Archive 约 12–48 小时，取决于优先级 | 恢复请求约 100 QPS；小于 64 KB 不做 lifecycle 转换；90/180 天最短时长 |

所以 MO 不能把用户 SQL 中的 `ARCHIVE` 直接映射成 provider 的同名 storage class。正确的抽象应是访问能力，例如：

```text
ACTIVE_LOW_LATENCY
ACTIVE_DIRECT_READ
OFFLINE_RESTORE_REQUIRED
```

再由管理员定义的 storage profile 将这些意图映射到当前云的具体实现。

## 6. 对原候选方案的漏洞复核

| 原设想 | 严重度 | 为什么可能失效 | 修正 |
|---|---:|---|---|
| Snowflake 不暴露归档层，MO 可据此差异化 | 高 | 2025-11 已 GA Storage Lifecycle Policies | 改为兼容其清晰语义，并在跨云、可解释成本、异步作业上差异化 |
| 一个 `HOT → COLD → ARCHIVE` 状态机即可 | 高 | 三个阶段混合了 cache、活动表物理介质和逻辑归档；历史版本还有独立状态 | 拆成历史、在线放置、归档/过期三个平面 |
| 当前 TAE Object 可直接换 storage class | 阻断 | ObjectName/Location 没有 profile；metadata 与 payload 同对象；merge、DML、索引仍需要读取 | 深归档复制到独立归档数据集；在线 direct-read tier 后置 |
| 用对象 `max(event_time)` 判断 | 阻断 | 当前热端只有一个 sort-key ZoneMap，任意列不具备该统计；即使能判断，也没有对象封存写栅栏 | MVP 限定完整时间 range 分区；对象级 lifecycle compaction 后置 |
| 复制 Snapshot 后再退休当前对象即可保证一致 | 阻断 | 两阶段之间的 UPDATE/DELETE、tombstone 或 merge 可能让归档副本恢复出已删除行，或让退休集合失效；仅校验基础对象 ID 不够 | MVP 用 TN fence 封存时间分区并等待稳定提交水位；后续方案必须验证基础对象、tombstone 和索引的统一 generation/read set |
| `SoftDeleteObject` 已经让 Manifest + 退休可直接实现 | 高 | 当前有事务原语，但没有与 lifecycle catalog、外部副本校验、分区写栅栏和索引更新集成，也没有端到端故障测试 | 把它作为实现候选而非已完成能力；优先验证完整 range 分区路径 |
| 用 Bucket Lifecycle 自动迁移当前对象 | 阻断 | 表对象不是按表前缀组织；provider 策略异步且会漂移；可能误归档内部对象 | MO catalog 是唯一事实源，provider 只作为显式作业执行器 |
| 用 last-access 自动识别冷热 | 高 | CN cache hit 不会触达 provider，provider 看到的访问时间不等于用户查询热度 | 只用于 cache/advisor，不作为数据可见性或深归档正确性依据 |
| 复用 Stage 保存归档对象 | 高 | Stage 凭据和用户生命周期不等于内部对象所有权、引用、GC；删除 Stage 可破坏数据 | 新建 system-managed storage profile；Stage 仅做导入导出 |
| 复用 `mo_pitr` 保存 tier retention | 高 | 语义不同，字段范围有限，没有 tier/profile/job/version | 新 lifecycle catalog；只复用历史引用判定 |
| `RESTORE TABLE events FROM ARCHIVE` 恢复原表 | 高 | 主键冲突、重复行、当前 schema、约束、并发写入和部分失败难定义 | MVP 只允许恢复到新表且必须带 predicate |
| 归档后立即节省全部活动存储 | 高 | PITR、Snapshot、Branch、Fail-safe 式窗口会继续 pin 原对象；复制阶段还会双份计费 | dry-run 和监控显式展示 duplicate/pinned/reclaimed bytes |
| 改大 policy 阈值可自动回热 | 中 | 云 provider 不会反向执行旧对象恢复，Databricks/Doris 均有类似限制 | policy version 化；已迁移数据只通过显式 restore/migration 回迁 |
| 删除超过归档期限的数据即可合规 | 高 | Snapshot/Branch/legal hold 可能继续保留原活动对象 | 区分最低留存、最大留存、hold；强制删除需处理整个引用图 |

## 7. 历史阶段比较过的四种实现路径

### 方案 A：直接使用云 Bucket Lifecycle

优点：

- 实现量最小；
- provider 负责异步迁移；
- 短期容易看到存储账单下降。

缺点：

- MO 不知道对象真实状态；
- 策略与 catalog 可能漂移；
- 无法安全按 table/业务时间选择当前随机命名对象；
- 不能保证 checkpoint、logtail、index 等内部对象不被误处理；
- cache、merge 和正常查询会出现非确定行为；
- provider delete 绕过 MO Snapshot/PITR/Branch 引用判断。

**结论：不可作为原生 Feature；最多可作为运维明确隔离的数据集上的实验能力。**

### 方案 B：扩展 TAE，当前对象原地跨层

需要新增：

- locator/storage profile 间接层；
- 所有对象存储 adapter 的 copy/transition/restore/status 能力；
- catalog lifecycle state；
- lifecycle-aware cache；
- merge/DML/index/tombstone 的全链路状态处理；
- metadata sidecar；
- 跨版本兼容和 GC。

优点是理论上可以透明查询在线 COLD，少一次逻辑归档复制。缺点是 blast radius 最大，而且 restore-required 对象会侵入核心读写路径。

**结论：适合后续实现 `ACTIVE_DIRECT_READ` 在线层，不适合作为深归档 MVP。**

### 方案 C+：MO 管理的独立 Archive Dataset

做法：

1. 通过 TN commit fence 封存符合条件的完整 range 分区；
2. 在稳定 Snapshot TS 上物化已经应用 tombstone 的逻辑行集；
3. 写入具有独立 `archive_dataset_id` 的不可变、版本化 archive payload；
4. 完成强 checksum、schema digest、Manifest root 和恢复读取校验；
5. 在一个 MO 事务中发布热端 Manifest，并退休活动分区及其索引；
6. 原活动对象继续由现有 Snapshot/PITR/Branch 规则保护和 GC；
7. 归档数据通过异步 Restore Job 物化到隐藏 staging table，校验后原子发布为新表。

优点：

- 深归档不进入正常 TAE 读、merge 和 DML 路径；
- catalog 可保持权威；
- 可同时服务 TTL、归档和合规观察；
- 容易支持多云不同恢复语义；
- Crash 后优先产生可回收 orphan 或临时双份，不产生无副本窗口。

代价：

- 需要归档写入和活动表退休的事务协议；
- 混合对象需要读写放大；
- 需要独立 Manifest、Job 和 GC；
- 历史窗口内存在双份存储。

**结论：有条件推荐，只有 P0 协议门槛通过后才能进入生产实现。**

这里的“同一 MO 事务”是 P0 prototype gate，不是当前代码已经提供的高层 API。底层 `SoftDeleteObject` 和 `DROP PARTITION` 提供了原语候选；原型必须证明 Manifest 发布、partition mapping 删除、物理分区/索引退休和 commit-unknown reconciliation 形成一个可判定结果。Job 状态不是正确性事实源，事务结果必须能从 Manifest 与分区 catalog 推导。

若无法证明该原子性，唯一安全降级是先发布 Manifest、保留活动分区可读，再由后续事务退休分区。该模式会暂时产生逻辑双份，但不会产生零副本；绝不能先删活动分区，再尝试发布 Manifest。

### 方案 D：SQL Task + Stage Parquet

这是当前即可实施的 workaround：

1. 周期查询旧分区；
2. 导出 Parquet 到独立 Stage；
3. 外部校验；
4. 手工或任务 DELETE，或对 range/list 分区执行 DROP PARTITION；
5. 需要时 LOAD 到新表。

它不能提供原生 exactly-once、引用保护、schema evolution、自动 restore 和成本可见性。

**结论：可作为 Phase 0 使用指南，不应包装成已完成的原生 lifecycle。**

## 8. 已废弃的 Partition-first 推荐架构

> 本节是早期方案记录，不得实现。当前 Catalog、Job 和 source-ref schema 见对象级规范设计。

### 8.1 三平面结构

```text
                    +-------------------------------+
                    | Management Control Planes     |
                    | Lifecycle + Placement + Cost  |
                    +---------------+---------------+
                                    |
              +---------------------+---------------------+
              |                                           |
    +---------v----------+                      +---------v----------+
    | Active Data Plane  |                      | Archive Data Plane |
    | TAE objects        | -- copy + verify --> | payload + manifest |
    | HOT / ONLINE_COLD  | <-- restore to new --| direct/offline     |
    +---------+----------+                      +---------+----------+
              |                                           |
              +---------------------+---------------------+
                                    |
                    +---------------v---------------+
                    | Reference / GC Plane          |
                    | Snapshot / PITR / Branch      |
                    | Restore Job / Hold / Backup   |
                    +-------------------------------+
```

### 8.2 用户可见状态

建议把用户语义定义为访问契约，而不是厂商 class。下表是跨平面的用户视图，不表示这些状态由同一个 policy 或状态机驱动：

| 状态 | 是否属于当前表 | 正常 SQL 是否直接读 | 是否允许普通 DML | 说明 |
|---|---|---|---|---|
| `HOT` | 是 | 是 | 是 | 活动数据；高 cache/QoS 目标 |
| `ONLINE_COLD` | 是 | 是 | 是，但可能有更高读成本 | 仅映射到支持直接读的存储 profile |
| `ARCHIVED` | 否 | 否 | 否 | 独立归档数据集，需要 restore |
| `LOGICALLY_EXPIRED` | 否 | 否 | 否 | 已从当前表移除，原对象可能仍被历史引用 |
| `PURGED` | 否 | 否 | 否 | 该 archive dataset 的 payload 已确认删除；源 TAE 对象是否回收由独立的历史引用和 TAE GC 决定 |

“HOT”在 MO 中主要代表缓存/QoS，而不是另一个 durable 主存。MO 本身已经把持久数据放在对象存储，CN 本地 SSD/内存更多是缓存。

### 8.3 建议新增 Catalog

名称仅作设计示例：

#### `mo_data_lifecycle_policies`

- `policy_id`
- `policy_name`
- `owner_account_id`
- `age_column_id`
- `archive_after`
- `purge_eligible_after`
- `archive_storage_profile_id`
- `purge_mode`：MVP 只允许 minimum-retention + best-effort physical purge
- `version`
- `status`
- `created_at` / `modified_at`

`cold_after` 不属于业务数据 lifecycle policy。`ONLINE_COLD` 是活动数据的物理放置问题，后续应进入独立 `mo_storage_placement_policies`；Phase 1/2 不把两者放进同一个状态机。

#### `mo_data_lifecycle_bindings`

- `policy_id`
- `scope_type`
- `account_id` / `database_id` / `table_id`
- `effective_version`
- `bound_at`

绑定必须依赖稳定 ID，name 只用于展示。rename、drop/recreate 不能让旧 policy 意外绑定到新对象。

#### `mo_archive_manifests`

- `archive_dataset_id`
- `owner_account_id`
- `source_database_id` / `source_table_id`
- 源 database/table/partition 的归档时展示名称
- `source_partition_id` / partition boundary / partition generation
- 捕获的 table/schema lifecycle generation
- `policy_id` / `policy_version`
- binding ID / binding version
- `source_snapshot_ts`
- 完整 archive snapshot schema / schema digest
- `predicate/cutoff`
- `row_count` / `logical_bytes` / `physical_bytes`
- policy/query 列 min/max
- verified Manifest root / verifier version / verified time
- `storage_profile_id`
- `payload_format_version` / `reader_version`
- region / KMS key ID / KMS key version
- `state`
- `archived_at` / `purge_eligible_at`

`archive_dataset_id` 是独立于源表生命周期的稳定身份。源表 rename、DROP、同名重建或不兼容 schema 变更后，已发布 archive dataset 仍能被枚举、审计和恢复。

Manifest 在发布时固化 `archived_at`、`purge_eligible_at`、policy version 和 retention semantics。后续 policy 修改不能静默重算已经发布 dataset 的删除期限；缩短期限必须 dry-run，并通过显式管理员操作生成新版本。

#### `mo_archive_objects`

- `archive_dataset_id`
- `object_id`
- dataset/job/epoch scoped immutable URI；MVP key 全局不复用，content hash 单独记录
- provider generation/version
- strong checksum / manifest root membership
- size / row count
- storage state
- restore state
- last verified time

#### `mo_lifecycle_jobs`

- `job_id`
- `parent_batch_id`
- `idempotency_key`
- `policy_id` / generation
- `target_table_id`
- `target_partition_id` / captured partition generation
- captured table/schema lifecycle generation 和 binding version
- `job_type`
- `state`
- `lease_owner` / `lease_epoch`
- step / object/chunk progress / executor epoch
- retry count / next retry
- last error
- requested/cancelled/completed time

一次 policy scan 可以生成包含多个候选分区的 coordinator/batch，但一个 archive child job 只能处理一个 partition。每个 child job 对应一个 `archive_dataset_id` 和一次 publish/retire 事务，不能把多个大分区放入同一个 all-or-nothing Job。

#### `mo_lifecycle_source_refs`

- `source_ref_id`
- `job_id` / `archive_dataset_id`
- `source_database_id` / `source_table_id` / `source_partition_id`
- `source_snapshot_ts`
- captured partition、table/schema lifecycle generation
- `state`
- `created_at` / `released_at`

`lifecycle_source_ref` 是归档复制期间对源 TAE Snapshot 的持久化 pin。实现可以复用系统管理 Snapshot，但最终必须进入现有 TAE GC 能识别的引用集合；只在 publish 成功、显式 unseal/cancel 完成或 terminal cleanup 后释放。记录本身不直接删除，而是保留 `RELEASED` 终态供 reconciliation 和审计。

#### `mo_archive_references`

- `reference_id`
- `archive_dataset_id`
- `reference_kind`
- `owner_id`
- `generation`
- `created_at` / `expires_at`
- `state`

引用数量只能作为派生缓存，不能作为删除的唯一事实源。Crash、重试和重复消息都可能使单一 `reference_count` 漂移。

#### `mo_archive_legal_holds`

- `hold_id`
- `archive_dataset_id` 或明确适用范围
- 创建人、授权角色和原因
- `created_at` / optional `expires_at`
- 解除人、解除原因和解除时间

Legal hold 是独立审计记录，不能被普通 policy 更新、DROP 或 GC 静默覆盖。

### 8.4 Provider 能力模型

不要给 `ObjectStorage` 强行增加所有后端都必须实现的方法。建议引入可探测的可选能力接口或独立 LifecycleStorage：

```text
Capabilities:
  DirectRead
  RestoreRequired
  ServerSideCopy
  ConditionalWrite / GenerationCAS
  Transition
  Restore
  RestoreStatus
  EventNotification
  Checksum
  MinObjectSize
  MinStorageDuration
  RetrievalPriority
```

storage profile 必须由系统管理员管理，不直接复用用户 Stage credential。policy 绑定时先做 capability validation，不能运行到第 90 天才发现 provider 不支持目标动作。

### 8.5 Archive Ownership、DROP、Backup/DR

- Active partition 由 table/partition lifecycle catalog 拥有；
- 正在复制的源 Snapshot 由 `lifecycle_source_ref` 拥有；它必须被 TAE GC 识别，Job 结束前不能只靠内存 lease 保护；
- 已发布 payload 由 archive dataset Manifest 拥有；未发布 payload 由 lifecycle job 临时拥有；
- Manifest 进入 `PURGE_PENDING` 后，archive payload 的不可逆删除所有权转移给 archive GC；普通 Job、policy 更新或新 reference 不能再撤销；
- hidden restore staging 由 restore job 拥有，发布成功后新表接管 active objects；
- provider restore 临时副本不是权威副本，任务结束后必须回收；
- 普通 `DROP TABLE` 只删除活动表，不自动删除已经发布的 archive dataset；
- archive 通过稳定 `archive_dataset_id` 继续枚举和恢复；
- 删除归档使用独立、可审计的 `DROP ARCHIVE DATASET <stable-id>` 一类命令；
- `DROP ... INCLUDING ARCHIVES` 必须先 dry-run，并检查显式 reference edge 与 legal hold；
- database/account 删除必须明确选择 archive 迁移、继续保留或显式级联删除，不能依赖名称前缀；
- archive catalog、storage profile、KMS metadata 和 reference/hold 必须纳入 Backup/DR；payload 是否跨区复制由 profile 明确声明，并计入成本；
- KMS key rotation 与疑似泄露必须有 rekey/restore-copy-drop 流程，不能假定深归档对象可以原地快速重加密。

### 8.6 独立状态机

Policy、partition、Manifest、provider object 和 restore job 不能共用一个状态字段：

```text
Partition: ACTIVE(g) -> SEALING(g+1) -> SEALED(g+1) -> RETIRED
           SEALING/SEALED(g+1) -- explicit abort before publish --> ACTIVE(g+2)
Manifest:  WRITING -> VERIFYING -> VERIFIED_NOT_PUBLISHED -> PUBLISHED
           -> PURGE_PENDING -> PURGED
Object:    UPLOADING -> VERIFYING -> VERIFIED
           -> DELETE_INTENT -> DELETING -> DELETED
           DELETING -> DELETE_FAILED_MANUAL -> DELETING
Access:    DIRECT_READ -> TRANSITIONING -> RESTORE_REQUIRED
           -> REHYDRATING -> DIRECT_READ
SourceRef: PROTECTING -> PINNED -> RELEASE_PENDING -> RELEASED
Restore:   REQUESTED -> REHYDRATING -> MATERIALIZING -> VALIDATING
           -> READY_TO_PUBLISH -> PUBLISHED
```

失败、取消和接管状态还需区分 `FAILED_RETRYABLE`、`FAILED_TERMINAL`、`CANCEL_REQUESTED` 与 stale epoch。任务状态用于调度和观察，Manifest/partition catalog 才是数据正确性的事实源。

每个非终态必须定义 deadline、重试上限、接管条件和 terminal failure；不能存在依赖某个失联 CN 或永不返回 provider 请求才能离开的状态。

## 9. 已废弃的 Partition-first 执行协议

> 本节中的 seal、partition generation、TN partition fence 和一分区一 Job 已由 system Lifecycle Snapshot、tagged Lifecycle Commit Entry、Attempt Root 和一 Object group 一 Job 取代。

### 9.1 数据单元选择

第一版只选择满足以下条件的单元：

1. 表已经由用户显式定义 range partition，MVP 不隐式创建、滚动或重组分区；
2. lifecycle 列是非空的 DATETIME/TIMESTAMP，并与 range 分区边界对齐；
3. 整个分区的上界 `<= cutoff`；
4. scan 时没有进行中的 merge、DDL 或另一个 lifecycle job，且后续通过 lifecycle generation 而不是长时间表锁检测 DDL；
5. policy generation 与任务捕获的一致；
6. partition/table、job 和文件数量没有超过硬上限。

当前 MO 没有 lifecycle state 或 partition generation。`PartitionInsert.Prepare` 会读取并缓存分区路由，Snapshot/非 RC 事务也可能继续使用已经缓存的 table delegate；因此 CN admission gate 只能做快速失败优化，不能保证封存正确性。

MVP 必须引入 TN 权威 commit fence：所有可能写入物理分区的 INSERT、UPDATE、DELETE、MERGE、LOAD、CDC、index build 和后台 merge，在 pre-commit 都要携带并校验预期 partition generation。Fence 的 wire identity 可以设计为“物理 partition table ID + generation”，或“logical table ID + partition ID + generation”；具体形式必须由 P0 原型结合现有写请求和冲突模型确定，不能只在文档中假设。

封存等待的是 TN fence 对应的稳定提交水位，而不是枚举或等待所有 CN 本地事务。Fence 之前成功提交的写入必须全部进入 `source_snapshot_ts`，fence 之后的旧 generation 写入必须 abort/retry。

若 `min <= cutoff < max`，这是混合单元。第一版应跳过；后续可以：

- 通过 SPLIT/REORGANIZE PARTITION 形成完整到期分区；或
- 进入显式 lifecycle compaction，把到期和未到期行重写到不同对象，再使用经过验证的对象级退休协议。

不能把混合对象整个归档，否则会让仍应在线的数据消失。

Late arrival 必须有显式语义：MVP 默认返回 sealed-partition 错误；可选能力可以把迟到数据路由到独立 quarantine/late-arrival 表，但不能静默重开旧分区。对其他 ACTIVE 分区的写入可以继续。

归档 copy 可能持续数小时，候选选择时检查一次 DDL 不构成保护。MVP 必须同时满足：

- seal 事务在把 partition 置为 `SEALING(g+1)` 时建立持久化、TAE GC 可识别的 `lifecycle_source_ref`；稳定提交水位确定后将它绑定到 `source_snapshot_ts`，两者之间不能出现源对象失去 active/reference 保护的窗口；
- table/schema/partition lifecycle generation 和 policy/binding version 在 Job 创建时捕获；相关 DDL 或 policy 变更必须推进对应 generation/version；
- 纯 metadata schema DDL 不持有数小时表锁，可以完成并推进 table/schema lifecycle generation；旧 Job 的最终 publish CAS 因此失败，然后执行 cleanup、unseal 或重新规划；
- index build、schema rewrite 或其他会写入目标物理分区的 DDL 仍受 TN fence 约束，不能绕过 `SEALED` 状态完成；调用方必须等待归档结束或先显式 cancel；
- `DROP TABLE`、`TRUNCATE`、`DROP/REORGANIZE PARTITION` 以及 database/account 删除遇到非终态 lifecycle Job 时，MVP 默认拒绝并返回阻塞 Job；管理员显式 cancel 后，catalog 事务必须阻止旧 publish、unseal 到新 generation、把 source ref 置为 `RELEASE_PENDING`，并把未发布 payload 所有权转给 orphan GC，随后即可重试 DROP；
- “DROP 后继续完成归档”或“DROP 隐式级联取消”都不是 MVP 的默认语义；若后续提供，必须使用独立显式命令和审计记录。

cancel/DROP 控制路径不能同步等待远端 orphan Delete；远端不可用时只会形成有上限、可告警的 orphan backlog，不应把 DDL 永久卡住。因此正确性不依赖持有数小时 DDL 锁：源 Snapshot 由持久化 reference 保护，元数据竞争由 generation/version 在最终事务中裁决。

### 9.2 Archive 的安全顺序

推荐顺序：

1. dry-run 计算 policy version、cutoff、候选完整分区、rows/bytes/files、pinned bytes 和目标 profile；
2. 创建一次 coordinator/batch；为每个候选 partition 创建一个 child Job，语义幂等键为 `(table_id, policy_version, partition_id, partition_generation, action)`；
3. 每个 child Job 独立分配一个 `archive_dataset_id`，一次只处理一个 partition；
4. 通过 TN commit fence 将该分区从 `ACTIVE(g)` 切到 `SEALING(g+1)`，并在同一封存操作中建立 TAE GC 可识别的 `lifecycle_source_ref`；
5. 等待 fence 对应的稳定提交水位，在一个 metadata 事务中记录 `source_snapshot_ts`、把 source ref 从 `PROTECTING` 置为 `PINNED`，并将 partition 切到 `SEALED(g+1)`；
6. 从一致性 Snapshot 物化已经应用 tombstone 的逻辑行集；
7. 写入 dataset/job/epoch scoped、全局不复用的 immutable archive payload；
8. 完成 container、强 checksum、schema、行数和 Manifest root 校验，把每个 object 标记为 `VERIFIED`；
9. 将 Manifest 写为 `VERIFIED_NOT_PUBLISHED`，固化 verified root；
10. 执行 conditional transactional publish：在一个短 MO 事务中校验全部 CAS 前置条件，发布 Manifest，退休 partition mapping、物理分区及其索引，并把 `lifecycle_source_ref` 置为 `RELEASE_PENDING`；
11. commit 后由 reconciliation 从 Manifest/partition catalog 推导结果，将 source ref 收敛到 `RELEASED`，再更新 Job 状态；
12. 未发布或 stale epoch payload 由 orphan GC 在 grace period 后回收。

第 10 步不是普通的“发布 + DROP”事务。它必须在同一事务快照中同时断言：

```text
partition.state == SEALED
AND partition.generation == captured g+1
AND table/schema lifecycle generation == captured generation
AND policy/binding version == captured version
AND job.executor_epoch == current epoch
AND manifest.state == VERIFIED_NOT_PUBLISHED
AND manifest.root == verified root
AND source_ref.state == PINNED
AND source_ref.source_snapshot_ts == manifest.source_snapshot_ts
AND manifest object set/count exactly matches verified root
AND every object referenced by manifest.state == VERIFIED
AND every object referenced by manifest.access == DIRECT_READ
```

任一条件不成立，整个事务必须 abort，不得发布 Manifest，也不得退休 partition 或索引。例如旧 Job 在 `SEALED(g+1)` 完成校验后，管理员已经显式 unseal 到 `ACTIVE(g+2)`，旧 publish 请求即使晚到也只能 CAS 失败，不能退休已经恢复写入的 `g+2` 分区。事务提交结果未知时，恢复器只能从 Manifest、partition catalog 和 generation 推导结果，不能以 Job 状态猜测。

如果复制失败，可以保留 `SEALED(g+1)` 重试，或在确认没有 Manifest 发布和退休动作后显式 unseal 到 `ACTIVE(g+2)`。Generation 不能回退或复用，旧 runner 和旧路由仍然必须被 TN 拒绝。不能在不做审计的情况下自动解封，因为调用方可能已经依赖“该时间分区不再可写”的语义。

核心不变量：

> 在活动数据被逻辑退休之前，必须至少存在一份已经校验且可由 catalog 找回的归档副本。

允许暂时双份，不允许出现零副本。

MVP 的原子边界固定为：

```text
one policy scan
  -> one bounded coordinator/batch
  -> one child job per partition
  -> one archive dataset per child job
  -> one conditional publish/retire transaction per partition
```

不能让一个 archive Job 跨多个大分区做 all-or-nothing publish。这样事务大小、失败范围、source pin、幂等键、恢复范围和接管成本都有明确上限。

### 9.3 并发语义

可借鉴 Snowflake 的保守策略：

- TN fence 对目标分区的 INSERT、UPDATE、DELETE、MERGE、LOAD、CDC、index build 和后台 merge 执行统一 generation 校验；
- 其他 ACTIVE 分区的 INSERT 可以继续；目标 SEALED 分区默认拒绝 late arrival；
- 若候选分区、对象、tombstone 或索引 generation 在封存前发生变化，当前 job 必须重算，不能继续退休旧选择集；
- 同一 table/policy/partition generation 只允许一个 active executor，CN failover 通过 lease epoch 防止旧 runner 推进 catalog；
- CN 层锁和 admission 只减少无效工作，最终正确性由 TN pre-commit fence 保证。

### 9.4 Archive Payload 格式

不能默认把“当前 ObjectIO 文件”当作长期归档格式。外层应定义稳定、版本化的 Archive Container，内部 columnar payload 再通过 ADR 选择：

- **ObjectIO**：恢复进 MO 成本低，但必须承诺跨版本 reader compatibility；
- **Parquet + 完整 schema manifest**：生态和长期可读性更好，但需验证全部 MO 类型、默认值、约束和精度映射；
- **其他版本化格式**：只有具备稳定 reader dispatch、升级和 golden archive 测试后才可加入。

Container 至少保存：

- 完整 MO schema、类型 ID 和 schema digest；
- predicate、Snapshot TS、partition ID/boundary/generation；
- 每文件强 checksum、size、row count、列 min/max；
- 由文件摘要构成的 Manifest root；
- payload writer/reader version；
- compression/encryption 和 KMS key version；
- job ID、executor epoch 和 idempotency key。

对象 key 必须使用 dataset/job/epoch scoped 的全局唯一 immutable identity，例如 `<archive_dataset_id>/<job_id>/<object_id>-<content-hash>`，不能原地覆盖，也不能在删除后复用。MVP 不做跨 dataset 的纯 content-addressed key 去重，content hash 只用于完整性校验；未来只有在建立全局 archive object identity、跨 dataset reference edge 和原子删除 CAS 后才能开放去重。Direct-readable archive MVP 在源分区退休前应完整重读目标 payload，重算 hash 并执行最小 restore/read drill；如果后续希望用 provider 认可的客户端强 checksum 代替每次全量重读，必须用单独 ADR 证明其端到端等价性，并保留周期 scrub 和跨版本 restore drill。

在 container、类型矩阵、reader dispatch、golden archive 和升级兼容测试确定前，不应承诺多年级的合规可恢复性。Deep Archive transition 只能发生在 direct-readable payload 完成上述校验之后。

### 9.5 TaskService 与外部副作用 Fencing

TaskService runner epoch、heartbeat 和 CAS 适合做外层调度，但不能自动 fence 已经发给 provider 的 multipart upload、copy、transition、restore 或 delete。

upload、copy、transition 和 restore 等非破坏性 step 必须使用 `(job_id, executor_epoch, step, object_id)` 记录条件状态迁移。旧 runner 即使在发现 lease 失效前完成了 provider 请求，也只能留下可 reconciliation 的 immutable object 或重复请求，不能推进新 epoch 的 Manifest。跨云正确性不能依赖所有 provider 都有 CAS，应以不可变 key、catalog 条件更新和幂等 reconciliation 收敛。

immutable key 只解决重复上传和覆盖，不能让 stale runner 的 Delete 自动安全。当前 `ObjectStorage.Delete(ctx, keys...)` 只有 key，没有统一的 provider version/generation 前置条件；删除必须使用下面的独立协议。

### 9.6 不可逆 Archive Delete 协议

Archive dataset 只有在 catalog 事务中满足以下删除谓词时，才能从可引用的 `PUBLISHED` 原子进入不可逆的 `PURGE_PENDING`：

```text
purge_eligible_at reached
AND archive reference edges == 0
AND restore/backup/DR/transition/migration/rekey reference edges == 0
AND legal hold == 0
AND hard-delete grace reached
AND manifest.state == PUBLISHED
AND every object.state == VERIFIED
AND no nonterminal provider step
```

创建 reference/hold 的事务必须同时断言 Manifest 仍为 `PUBLISHED`，不能是 `PURGE_PENDING`/`PURGED`；这样用一次 dataset 级 CAS 关闭“GC 检查引用为零后，新 restore 又增加引用”的 TOCTOU 窗口。Manifest 进入 `PURGE_PENDING` 后，object 才能逐个从 `VERIFIED` 条件更新为 `DELETE_INTENT -> DELETING`。这一 object 状态迁移同样不可撤销：

- 禁止新增 reference，也禁止 cancel 后重新引用同一个 immutable key；
- 重新保留的可靠决策必须在 Manifest 进入 `PURGE_PENDING` 前建立 reference。进入后不能恢复原 dataset；若 provider 对象尚可读，只能把它 best-effort 复制到新的 immutable key 并建立新的 object/Manifest，且这不会撤销或延缓旧 key 的删除；
- stale runner 和新 runner 只能对同一个、已冻结的 immutable key 重复 Delete，因此重复调用才是安全幂等；
- provider 支持 version ID、generation match 或 conditional delete 时，必须删除 Manifest 记录的具体版本；不支持时依赖“key 永不复用 + 不可逆 catalog 状态”保证旧 Delete 不会命中新数据；
- 单个对象必须在 provider 返回 not-found 或通过确认读取证明不存在后进入 `DELETED`；
- 只有 Manifest 的所有对象都进入 `DELETED`，Manifest 才能从 `PURGE_PENDING` 进入 `PURGED`。

Delete 请求结果未知时保持 `DELETING` 并重试同一个 key/version；不能回退到 `VERIFIED`，也不能仅凭 TaskService lease epoch 判断删除没有发生。这是 archive ownership/GC 的 P0 不变量。

每次 provider Delete 必须有 deadline、重试上限和退避。超过自动重试上限后进入不可重新引用的 `DELETE_FAILED_MANUAL` 告警态，Manifest 保持 `PURGE_PENDING`；人工重试仍只能恢复到 `DELETING` 并删除同一个冻结 key/version，不能回到 `PUBLISHED`。这样 provider 永久不可用不会产生无限同步等待，同时也不会谎报 `PURGED`。

## 10. Restore 设计

### 10.1 第一版只恢复到新表

不建议：

```sql
RESTORE TABLE events FROM ARCHIVE ...;
```

建议：

```sql
EXPLAIN RESTORE
  FROM ARCHIVE DATASET 'archive-dataset-uuid'
  WHERE event_ts >= '2024-01-01'
    AND event_ts <  '2024-02-01';

CREATE RESTORE JOB restore_events_202401
  FROM ARCHIVE DATASET 'archive-dataset-uuid'
  INTO events_restored_202401
  WHERE event_ts >= '2024-01-01'
    AND event_ts <  '2024-02-01'
  MAX_FILES 100000
  MAX_BYTES 10 TB
  PRIORITY STANDARD;

SHOW RESTORE JOB restore_events_202401;
```

`FROM ARCHIVE DATASET <stable-id>` 是权威入口。源表仍存在时可以提供 `FROM ARCHIVE OF <table>` 便捷语法，由 catalog 解析为 dataset ID；源表 DROP 后仍可通过 `SHOW ARCHIVE DATASETS` 和稳定 ID 恢复，不能依赖名称复活入口。

理由：

- 原表可能已经出现相同主键；
- 当前 schema 可能已增删列；
- 原表可能继续写入；
- restore-required provider 需要数小时，不能占用普通 session；
- 部分恢复失败时，新表可保持不可见/RESTORING，成功后再原子发布；
- 取消任务可能无法取消云端费用，必须在 Job 状态中明确提示。

Restore 必须使用系统拥有的隐藏 staging table：

```text
REQUESTED
  -> REHYDRATING
  -> MATERIALIZING
  -> VALIDATING
  -> READY_TO_PUBLISH
  -> PUBLISHED
```

并单独支持 `CANCEL_REQUESTED`、`FAILED_RETRYABLE` 和 `FAILED_TERMINAL`。Provider 的临时恢复副本不能直接成为已发布表的数据源；必须把数据复制到正常可读的 MO-managed active profile，校验行数、schema、hash 和抽样查询后，再以短事务原子发布新表。部分恢复、stale runner 和失败 staging 对用户不可见，并由 restore job 或 grace-period GC 回收。

权限检查基于 archive dataset owner 和 restore 发起人。行级安全、masking、index、constraint、publication/subscription 等派生定义不从 payload 盲目继承。

### 10.2 `WHERE` 必须强制

Snowflake 已强制 restore 使用 `WHERE`，并允许用 `EXPLAIN` 估算文件和字节。MO 也应强制：

- 至少有可利用 archive min/max 的谓词；
- 先输出 estimated files/bytes、provider retrieval SLA、费用类别；
- 允许账户级最大 files/bytes/concurrency；
- 超过阈值需要显式管理员批准。

### 10.3 Schema evolution

建议第一版以“独立、可逆”为默认规则：

- 新表默认使用 archive Snapshot 时保存的完整 schema，不依赖源表仍然存在；
- 用户若要映射到源表当前 schema，必须显式选择 `MAP TO CURRENT SCHEMA` 一类选项；
- 映射前检查新增列填充值、删除列处理、type change 是否无损、default/generated column、collation、时区和 decimal 精度；
- 不兼容映射在发起 provider restore 前早失败；
- index、constraint、row policy、publication、subscription 等派生对象默认不从 archive payload 盲目恢复，需要按显式选项或当前定义重建。

## 11. Cache、Merge、Index 与 Tombstone

### 11.1 Cache gate

任何活动对象的 storage state 改变前后都要：

1. catalog 生成新的 lifecycle generation；
2. CN 在 cache read 之前检查 generation/state；
3. 跨 CN 广播 invalidation，或让旧 generation cache key 自然失效；
4. `ARCHIVED` 对象即使 cache 仍有 bytes，也不能通过正常活动表路径返回；
5. restore 成功后使用新的对象/generation，不能复活旧 cache identity。

### 11.2 Merge

- `ARCHIVED` payload 不参与普通 TAE merge；
- `ONLINE_COLD` 只有在 `DirectRead=true` 且 merge cost/QoS 允许时才可参与；
- 生命周期迁移前先 compact 小对象，避免云端 per-object 请求和最小计费放大；
- lifecycle compaction 与普通 merge 共用对象 ownership/fencing，不能并发重写同一源对象。

### 11.3 Tombstone

归档单位必须同时考虑数据对象和对这些行生效的 tombstone：

- 不能按 tombstone 自身创建时间推导业务行年龄；
- restore 必须得到 archive cutoff Snapshot 上的逻辑行集，而不是简单复制原始数据对象后忽略删除；
- 最安全的是在固定 Snapshot 上物化“已应用 tombstone 的归档快照”；
- 若选择保存原始对象 + tombstone，则 Manifest 必须记录依赖图，复杂度和长期兼容成本更高。

第一版推荐物化逻辑快照。

### 11.4 Index

- archive payload 以基础行数据为真；
- secondary/fulltext/vector index 不单独深归档；
- 活动表对应索引在逻辑退休事务中同步更新；
- restore 到新表后按最终发布的 restore schema 和显式 index 选项重建；
- restore dry-run 要估算重建成本，而不仅是对象恢复成本。

## 12. GC 与引用模型

必须把源 TAE 对象与 archive payload 分成两个引用域和两条删除谓词，不能把所有引用合并成一个跨平面计数器。

源 TAE 引用域包括：

```text
active table
snapshot
pitr
branch protect snapshot
backup / replica / fork / ISCP
lifecycle_source_ref
```

源对象继续由现有 TAE GC watermark 和其可识别的历史引用保护。新增 `lifecycle_source_ref` 必须接入这套 GC 判定，或者严格复用系统管理 Snapshot；只在归档 child Job 发布成功、显式 unseal/cancel 完成或 terminal cleanup 后释放。

```text
Source TAE object deletable =
  dropped from active table
  AND no Snapshot/PITR/Branch/Backup/replica/fork/ISCP reference
  AND no lifecycle_source_ref
  AND existing TAE GC watermark allows deletion
```

Archive 引用域包括：

```text
published archive dataset/reference
restore job
archive backup / DR replica
transition / migration / rekey job
legal hold
```

这些关系必须保存为可审计的显式 reference edge。`reference_count` 只能作为派生指标或查询加速缓存，不能作为 GC 的唯一事实源；reconciliation 必须能从边集合重建 count 并检测漂移。Legal hold 使用独立记录和授权流程，普通 policy、DROP 和 task retry 不能删除或覆盖它。

```text
Archive payload provider-delete allowed =
  purge_eligible_at reached
  AND no archive dataset/reference edge
  AND no restore/backup/DR/transition/migration/rekey reference edge
  AND no legal hold
  AND hard-delete grace reached
  AND object is irreversibly marked DELETING
```

`DELETING` 必须由第 9.6 节的 catalog CAS 产生，不能先调用 provider Delete 再补状态。Archive payload 使用独立 mark-and-sweep GC，不能让当前 TAE GC 依赖远端 provider 可用性。

两个引用域默认不互相 pin：

- Archive Manifest 发布成功并释放 `lifecycle_source_ref` 后，不再因为 Archive Manifest 的存在而 pin 源 TAE 对象；
- Snapshot/PITR/Branch 只保护其历史视图需要的源 TAE 对象，不会无条件 pin archive payload；
- 若 Backup/DR 同时复制源对象和 archive payload，必须分别建立两个域内的引用边，不能用一个含义不明的全局 count。

否则会产生无法解释的跨平面永久引用和持续存储增长。建议参考 Timescale 的做法：引用归零后仍设置 hard-delete grace period。Iceberg 的经验也说明，过短 orphan retention 会把仍在写入或提交中的文件误判为 orphan。

## 13. SQL 语义建议

### 13.1 不采用累计时长列表

以下写法容易产生歧义：

```sql
RETENTION (
  HOT '7d',
  COLD '90d',
  ARCHIVE '730d'
)
```

`COLD '90d'` 究竟表示：

- 绝对年龄到 90 天？
- 在 COLD 再停留 90 天？
- 7–90 天区间？

建议使用绝对年龄阈值：

```sql
CREATE DATA LIFECYCLE POLICY events_lifecycle
  AGE BY event_ts
  ARCHIVE AFTER INTERVAL '90' DAY
  PURGE ELIGIBLE AFTER INTERVAL '730' DAY
  USING ARCHIVE STORAGE PROFILE mo_archive_default;

ALTER TABLE events
  ADD DATA LIFECYCLE POLICY events_lifecycle ON (event_ts);
```

明确语义：

- `[0, 90d)`：仍属于活动表；其 cache/介质由独立 storage placement policy 管理；
- `[90d, 730d)`：ARCHIVED；
- `>=730d`：归档 payload 具备 purge eligibility，进入 best-effort 回收判定；
- 原活动对象何时物理删除仍受历史引用影响。

MVP 不在 data lifecycle policy 中提供 `MOVE TO ONLINE_COLD`。ONLINE_COLD 是活动数据的物理放置策略，应在后续独立 `STORAGE PLACEMENT POLICY` 中定义，避免重新把三个平面混回一个状态机。

`PURGE ELIGIBLE AFTER` 表示“在此之前不得删除；到期后允许进入 best-effort 物理回收”，不承诺某一时刻前必然删除。Maximum retention 是后续独立合规模式，需要更高权限并处理 Snapshot、PITR、Branch、Backup、replica 和 legal hold 冲突。

### 13.2 Snowflake 兼容层

可考虑兼容 Snowflake 的 policy 对象和绑定方式，但不建议第一版实现任意 Boolean SQL expression。第一版可接受：

```sql
AGE BY <timestamp_column>
```

以后再扩展为受限谓词或 Snowflake 风格：

```sql
AS (event_ts DATETIME)
RETURNS BOOLEAN -> event_ts < ...
```

`DATA_RETENTION_TIME_IN_DAYS` 若实现，只应映射到历史版本保留平面，不能解释为 HOT 时长。

### 13.3 观察与 dry-run

至少需要：

```sql
SHOW DATA LIFECYCLE FOR TABLE events;
EXPLAIN DATA LIFECYCLE FOR TABLE events;
SHOW DATA LIFECYCLE JOBS;
SHOW ARCHIVES FOR TABLE events;
SHOW ARCHIVE DATASETS;
SHOW ARCHIVE DATASET 'archive-dataset-uuid';
SHOW ARCHIVE OBJECTS FOR TABLE events WHERE ...;
```

输出应包括：

- effective policy 和 generation；
- eligible / mixed / blocked 单元；
- eligible rows/bytes；
- rewrite bytes；
- archive bytes；
- temporary duplicate bytes；
- Snapshot/PITR/Branch pinned bytes；
- physically reclaimed bytes；
- provider min duration 和 early-deletion penalty bytes；
- restore-required files/bytes；
- 最近任务、错误和下一次重试。

## 14. 已废弃的 Partition-first 分阶段建议

> 本节 Phase 编号只用于保存历史 Review 上下文，不代表当前交付顺序。当前交付使用对象级规范中的 capability gates。

### Phase 0：明确现状和人工 Workaround

- 文档化 `SELECT ... INTO OUTFILE ... FORMAT 'parquet'` + 校验 + 手工删除流程；
- 明确它不是 exactly-once 原生归档；
- 给 #24853 和 #24552 补充统一术语，纠正 Snowflake 前提。

### Phase 1：Lifecycle Core + Native Expire

范围：

- 只支持 table scope；
- 只支持单个非空时间列；
- 必须与 range 分区边界对齐；
- 只处理完整封存分区；
- TN commit fence、partition generation 和稳定提交水位；
- late arrival 默认拒绝，可观察但不静默重开分区；
- policy/binding/job catalog；
- dry-run、fencing、对象级 journal；
- table/schema/partition lifecycle generation 与 conditional retire；
- partitions/table、jobs/account 和 metadata rows 硬上限；
- 逻辑 expire 与物理 GC 指标；
- 与 Snapshot/PITR/Branch 联合测试，包括读取已经 DROP 的历史 partition。

这一阶段可先解决 #24552 的安全子集，不涉及云深归档。

### Phase 2：MO-managed Online Archive

- 建立 archive payload + hot Manifest；
- 独立 `archive_dataset_id` 和版本化 Archive Container；
- seal 时建立 TAE GC 可识别的 `lifecycle_source_ref`，覆盖长时间 copy 与 DDL/GC 并发；
- 一分区一 child Job、一 archive dataset、一次 conditional transactional publish/retire；
- publish 事务 CAS partition、table/schema、policy/binding、executor epoch、Manifest root/state 和全部 object verification；
- archive object 使用不可逆 `DELETE_INTENT -> DELETING` 协议，关闭新增引用/Delete 竞态；
- copy → full verify → conditional transactional publish/retire P0 prototype gate；
- 强制 predicate 的 hidden staging → validate → atomic publish 新表恢复；
- archive snapshot schema 默认恢复，当前 schema 映射为显式选项；
- explicit reference edge、legal hold 和 archive mark-and-sweep GC；
- archive/restore cost 和进度；
- 先使用始终可直接读取的 profile 验证协议。

### Phase 3：Restore-required Deep Archive

- AWS/COS/OSS/Azure provider capability adapter；
- restore event + 有界 polling；
- priority、quota、deadline、cancel 状态；
- 大规模文件恢复的 backpressure；
- cache gate 和跨 CN invalidation；
- archive catalog/profile/KMS 的 Backup/DR 与周期 restore drill；
- 故障注入覆盖所有中间状态。

### Phase 4：ONLINE_COLD 与自动热度优化

- 对支持 direct read 的 provider class 做活动对象在线 COLD；
- lifecycle-aware merge QoS；
- 基于 MO 查询统计的 cache/advisor；
- 可选自动放置，用户声明性能/成本意图而不是厂商 class；
- account/database 默认继承；
- rollup/downsample；
- legal hold 与 maximum-retention 强制模式。

## 15. 历史 Partition 方案的故障与兼容测试

> 本节含有仍可复用的故障场景，但 partition fence/partition generation 验收已经失效。当前 Commercial GA 验收只以对象级规范为准。

### 15.1 正确性

- cutoff 边界、时区、NULL、未来时间、late arrival；
- whole-unit 和 mixed-unit；
- fence 前写入全部进入 archive Snapshot，fence 后旧 generation 提交成功数严格为零；
- 无主键 INSERT、UPDATE、DELETE、MERGE、LOAD、CDC、index build 和后台 merge 使用同一 TN generation 协议；
- policy 修改、删除、改名、table rename、drop/recreate；
- 旧 Job 校验完成后 unseal 到 `ACTIVE(g+2)`，晚到 publish 必须整体 CAS 失败；
- publish 前并发改变 table/schema lifecycle generation、policy/binding version、executor epoch、Manifest root/state 或任一 object verification，退休成功数必须为零；
- 一次 scan 可选择多个分区，但每个 child Job、archive dataset 和 publish/retire 事务只能覆盖一个 partition；
- DML/merge 与 lifecycle 同时操作相同对象；
- secondary/fulltext/vector index；
- tombstone 已应用后的归档结果；
- schema add/drop/type change；
- restore 主键冲突隔离；
- 多租户权限和 storage profile 隔离。

### 15.2 历史引用

- 无 Snapshot/PITR/Branch；
- table/database/account/cluster 多层 PITR；
- 多个不同窗口，以最早保护点为准；
- user Snapshot；
- Branch Protect Snapshot；
- Snapshot/Branch 在归档前、归档中、归档后创建/删除；
- 慢速 copy 期间并发 schema/index DDL、DROP/TRUNCATE/REORGANIZE、table/database/account DROP，验证 generation CAS、默认阻塞/cancel 语义和 `lifecycle_source_ref` 无保护空窗；
- `lifecycle_source_ref` 在 CN crash、TN leader change、executor takeover 和 commit unknown 后仍被 TAE GC 识别，并且只在 publish、显式 unseal/cancel 或 terminal cleanup 后释放；
- cancel/DROP 只等待 catalog fencing、unseal 和 source-ref ownership transfer，不等待远端 orphan Delete；
- Snapshot/PITR 在活动 partition DROP 后仍能读取其历史对象；
- archive purge 与 history pin 冲突；
- legal hold 与 maximum retention 冲突。

### 15.3 Crash consistency

逐点注入崩溃：

1. Job 创建前后；
2. payload 部分写入；
3. payload 完整但未校验；
4. 已校验但 Manifest 未发布；
5. Manifest 与活动退休事务提交前；
6. 事务提交成功但 Job 状态未更新；
7. provider transition/restore 已提交但状态未知；
8. restore 部分完成；
9. archive 多对象部分删除；
10. CN owner 失联，新 owner 以更高 lease epoch 接管；
11. TN leader change、commit unknown 和 seal command/已 prepare 事务竞争；
12. stale runner 已提交 provider 请求，但失去 catalog 推进权限；
13. 引用归零检查与并发新增 restore/reference 竞争；
14. object 已进入 `DELETE_INTENT/DELETING` 后旧、新 runner 重复 Delete，provider 返回成功、not-found、超时或结果未知；
15. provider key 被删除后尝试重新引用或复用同名 key；

每个点都要证明：

- 不出现零副本；
- 不重复发布；
- 重复 Delete 只能命中同一个冻结 key/version，不能删除新对象或已重新引用的数据；
- 重试可收敛；
- orphan 最终可回收；
- 等待有 deadline；
- backlog 和 metadata 增长有上限。

### 15.4 云语义

至少使用 fake provider 覆盖：

- direct-read archive；
- restore-required archive；
- restore 临时副本过期；
- transition 延迟；
- restore 不可取消；
- event 丢失后 polling 收敛；
- request throttling；
- min object size；
- min storage duration 和 early-delete；
- object generation/CAS 冲突；
- credentials/KMS key rotation。

### 15.5 容量和可观测性

必须设置并测试硬限制，而不只是展示指标：

- max lifecycle partitions per table；
- max archive files/bytes/duration per job；
- max concurrent jobs per account/cluster；
- target archive file size 和 max provider request QPS；
- max outstanding restore requests / restore staging bytes；
- max orphan bytes/objects 和 orphan grace duration；
- completed job/step、Manifest 和 reference metadata retention；
- hard backlog circuit breaker。

超过上限时 fail closed，不能静默扩大扫描或恢复范围。至少分别展示 `logical_expired_bytes`、`archive_verified_bytes`、`history_pinned_bytes`、`physically_reclaimed_bytes`、`orphan_bytes`、`restore_pending_bytes` 和预计 storage/retrieval cost。

## 16. MO 可以比业界做得更好的地方

### 16.1 把“逻辑过期”和“物理省钱”同时展示

Snowflake 已公开提示归档后至少还会因为 Time Travel + Fail-safe 保留双份存储。MO 应进一步展示具体 blocker：

```text
logical archived:       10 TB
archive payload:         6 TB
history pinned source:   8 TB
physically reclaimed:    2 TB
blocked by:
  PITR account_30d:      5 TB
  branch br_2025q4:      3 TB
```

这比只显示“已归档 10 TB”更真实。

由于 MO 的 durable data 本来就在对象存储，经济收益不能只比较 Standard 与 Glacier/Archive 的单价。每次 dry-run 应估算：

```text
net saving =
  standard storage saved
  - archive storage
  - Snapshot/PITR/Branch pinned duplicate storage
  - transition/retrieval/request cost
  - temporary restore copy
  - early-deletion/minimum-duration charge
  - restore compute and index rebuild cost
```

Phase 2 进入生产前，应使用真实客户表验证净节省、年度预计 restore 次数、平均 archive 文件大小、restore P50/P95/P99、历史引用导致的双份时间和 index rebuild 成本。若不能证明正向净收益，就不应只为了“有冷热分层”而开启归档。

### 16.2 Restore 从一开始就是异步、可恢复作业

Snowflake COLD restore 可长达 48 小时，Databricks 仍要求用户自己调 S3 restore。MO 可以提供：

- `EXPLAIN RESTORE`；
- 强制 predicate；
- 文件/字节 quota；
- Job ID；
- 状态、进度和预计完成时间；
- provider event + polling fallback；
- crash-safe resume；
- 明确提示取消不等于取消云端费用。

### 16.3 跨云使用能力契约

GCS Archive 可以毫秒直读，AWS/Azure Deep Archive 需要数小时恢复，OSS Archive 还可以按 bucket 打开 direct read。MO 若只暴露：

```text
ACTIVE_DIRECT_READ
OFFLINE_RESTORE_REQUIRED
```

就能在不同云上保持一致 SQL 语义，同时把具体 storage class 留给 profile 管理。

### 16.4 Policy 变更先做影响分析

任何缩短/延长阈值、切换 profile 的操作先输出：

- 已经不可逆或需显式 restore 的数据；
- 预计 rewrite/transition bytes；
- early-deletion penalty；
- Snapshot/PITR/Branch blocker；
- 预计任务数量和完成时间。

未经显式确认，不自动把已经深归档的数据“视为在线”。

### 16.5 生命周期列与物理组织联动

创建 policy 时可给出建议或拒绝不安全配置：

```text
Lifecycle column event_ts is not the partition/sort key.
Estimated mixed-object rewrite ratio: 92%.
Recommendation: partition by day on event_ts before enabling archive.
```

这把 ClickHouse、Timescale、Elastic 的“封存分区/chunk/index”经验产品化，而不是把读写放大留到后台才暴露。

## 17. 历史方案当时尚未解决的问题

以下内容记录 Partition-first 方案当时的未决问题，不是当前决策清单：

1. 版本化 Archive Container 内部 payload 选择 ObjectIO、Parquet 还是其他格式？长期 reader compatibility 如何承诺？
2. TN fence 的稳定身份使用物理 partition table ID 还是 logical table + partition ID？如何与现有冲突检测和 commit TS 建立稳定水位？
3. Manifest 发布、partition mapping 删除和物理 partition/index 退休能否在当前事务框架中形成单一可判定提交？
4. Maximum-retention 合规模式与 Snapshot/PITR/Branch/Backup/replica/legal hold 谁优先，由谁有权强制清理？
5. archive data 是否参与备份、跨区复制、Data Branch 和 clone？由谁承担副本费用？
6. ONLINE_COLD 是否要求普通查询完全透明，还是需要 session/query opt-in？
7. policy 切换 storage profile 后，历史 archive 是否原地迁移？
8. KMS key rotation 和密钥疑似泄露时，归档数据如何 rekey？
9. hidden restore staging 使用何种系统表身份和原子 rename/publish 原语？失败清理如何与用户同名建表竞争？
10. account/database 默认 policy 的继承、覆盖和 policy generation 如何定义？

### 17.1 Review 处理决策记录

| Review 意见 | 处理 | 原因 |
|---|---|---|
| 方案 C 升级为 C+，生产实现有条件通过 | 采纳 | 原分析稿方向正确，但缺 TN fence、独立 identity、原子提交和 restore staging 四个闭环 |
| CN admission 改为 TN commit fence | 采纳正确性要求；保留 wire key 决策 | 当前 CN 会缓存分区路由；最终 fence 必须在 TN。具体使用物理 partition table ID 还是 logical table/partition ID，需要 P0 原型验证 |
| `cold_after` 移出 lifecycle policy | 采纳 | ONLINE_COLD 属于活动数据 storage placement，不属于业务归档/过期状态机 |
| 独立 archive dataset、snapshot schema、reference edge、legal hold | 采纳 | 否则源表 DROP/schema 变化或计数漂移会破坏恢复与 GC 入口 |
| 版本化 container 和全量重读 | MVP 采纳；保留等价校验扩展 | Direct-readable MVP 在源退休前全量重读。未来只有 provider 认可的客户端强 checksum 被 ADR 证明端到端等价后，才可替代逐对象全量重读 |
| TaskService epoch 不等于 provider side-effect fencing | 采纳 | 外部请求可能在旧 runner 失去 lease 前完成；非破坏性请求由 immutable key、step journal 和 catalog 条件更新收敛，Delete 使用独立不可逆协议 |
| Hidden staging 和原子发布 | 采纳 | 部分 restore 不能对用户可见，provider 临时恢复副本也不能成为已发布表的长期数据源 |
| `PURGE AFTER` 改为 purge eligibility | 采纳 | MVP 只能承诺 minimum retention + best-effort reclaim；maximum retention 是独立合规模式 |
| Restore-required deep archive 当前不进入首个 GA | 采纳 | Restore 是 GA 必需能力，但首个 GA 直接读取已发布 Parquet/ZSTD payload；provider thaw、轮询、临时副本和跨云差异进入独立可选 Profile |

### 17.2 二轮 Review 处理决策记录

| Review 意见 | 处理 | 落地位置与判断 |
|---|---|---|
| publish/retire 缺完整 CAS 前置条件 | 采纳，P0 | 第 9.2 节定义 conditional transactional publish，原子校验 partition、table/schema、policy/binding、executor epoch、Manifest root/state 和全部 object verification；任何失败均不得退休 |
| immutable key 不能保证 stale runner Delete 安全 | 采纳，P0 | 当前 `ObjectStorage.Delete` 只有 key；第 9.6 节增加不可逆 `DELETE_INTENT -> DELETING`、reference 创建反向 CAS、provider version 优先和 key 永不复用 |
| 长时间 copy 缺 DDL 协调和 source pin | 采纳，P0 | 第 8.3、9.1、9.2 节增加 TAE GC 可识别的 `lifecycle_source_ref`、table/schema lifecycle generation 和 DROP/cancel 语义，不使用数小时表锁 |
| GC 需要拆成两条删除谓词 | 采纳，P1 | 第 12 节拆分 Source TAE 与 Archive payload 引用域，明确两者默认不交叉 pin |
| 一分区一原子 Job | 采纳，P1 | 一次 scan 生成有界 batch，每个 partition 一个 child Job、一个 archive dataset 和一次 conditional publish/retire 事务 |
| maximum retention 与 GC 术语不一致 | 采纳，P2 | 当前策略统一使用 `purge_eligible_at`；maximum-retention 明确标记为未来合规模式、非 MVP |

自审进一步固定四项失败路径：

- `PROTECTING -> PINNED` 与 `SEALING -> SEALED` 必须在同一 metadata 事务完成，避免 partition 已封存但 Snapshot pin 尚未生效；
- publish 或 cancel 只把 source ref 转为 `RELEASE_PENDING`，由 reconciliation 幂等收敛到可审计的 `RELEASED`；
- cancel/DROP 不同步等待 provider orphan Delete；Delete 自动重试耗尽后停在不可重新引用的人工处理态，不能谎报 `PURGED`。
- MVP 禁止跨 dataset 复用纯 content-addressed key；否则旧 Delete 可能命中新引用。key 使用 dataset/job/epoch scoped 唯一身份，content hash 仅用于校验。

### 17.3 Commercial GA 范围收敛决策记录

这一轮决策取代 17.1/17.2 中关于独立 Archive、exact source ref、Legal Hold、隐藏索引 handler 和 Archive Backup/DR 的产品结论；早期条目仅用于说明风险是如何被发现的。

| 收敛项 | 当前决策 |
|---|---|
| Archive ownership | 从属于 stable logical table owner generation/account incarnation；TAE CAS 使用 physical table generation + exact Object；DROP 后不承诺 Restore |
| Source protection | 独立 `kind='lifecycle'` table-only Snapshot；commit -> flush -> GC visible/old-cycle drained -> select Object |
| Commit reconciliation | TTL/Archive 都有 system retained Attempt Control，冻结 final transaction identity/request digest/immutable result |
| External ownership | Archive 第一次 PUT 前建立 system Attempt Root；DROP 写 Owner Registry tombstone，Sweeper 异步清理 |
| Dependency serialization | table Feature Guard + account/database scope Guard；首次创建和 bind 由唯一键/CAS 关闭竞态 |
| Index | 首个 GA 拒绝所有物化隐藏索引表，只允许不产生隐藏表的 Base PK |
| Backup/DR | Payload/Catalog 不复制；Backup/PITR/Snapshot Restore/Clone/Branch/DR 对 Lifecycle owner fail closed |
| Wire/replay | 在原始 `PrecommitWriteCmd.EntryList` 增加 tagged Lifecycle Entry，进入相同 WAL/1PC/2PC/retry 链 |
| Profile identity | Dataset/Root 冻结 profile version、namespace 和对象 identity；credential rotation 独立 |
| 长冲突/大 Object | 分别进入 `CONFLICT_BLOCKED` 和 oversize-object streaming/`OVERSIZE_BLOCKED` |
| ALTER COPY | logical owner/physical source transfer 未实现前，对 Lifecycle-bound 表 fail closed |
| Scale | 只索引/调度显式 Binding；账户 Guard=1000、集群 activation slot=1000、1/10 TiB、`1/2/4/8` child 并发和 index/backlog/retained-byte 硬上限；active coexistence 过线即暂停新 claim |
| GA 判定 | 六项 P0、1/10 TiB、Stage 4 与 Gate E 证据完成后 Conditional Go；当前代码仍不是 Commercial GA |

## 18. 功能定位、业务场景与行业术语

### 18.1 这个功能主要解决什么问题

一句话：**把“已经不常访问、但还不能删除的业务数据”从当前活动数据中安全地分离出来，降低长期存储成本，同时保留可审计、可估算、可恢复的能力。**

它回答的是三个业务问题：

1. 哪些数据仍属于当前业务表，应该正常查询和修改？
2. 哪些数据已经很少访问，但还要保留，应该转入低成本在线层或归档层？
3. 归档数据需要时，能否知道恢复范围、耗时、费用，并恢复成可用的新表？

它不是以下功能的替代品：

| 容易混淆的功能 | 它真正解决的问题 | 与本 Feature 的区别 |
|---|---|---|
| Backup/备份 | 系统、数据库或文件发生故障后，从另一份副本恢复 | 备份关注灾难恢复，不改变当前表中哪些业务行可见；生命周期归档关注业务数据的长期保存和成本 |
| Snapshot/快照 | 在某个时间点得到一致的数据视图 | 快照通常是历史引用和读取视图，不等于低成本归档，也不自动提供跨多年版本的业务恢复目录 |
| PITR | 将数据库恢复或读取到某个历史时间点 | PITR 是历史版本窗口；它保护 UPDATE/DELETE 之前的状态，不等于按业务时间列把旧订单归档 |
| Cache/缓存 | 用内存、SSD 或远端缓存降低访问延迟 | 缓存可以丢失和重建，不是数据的权威副本；归档必须有独立、可校验的持久副本 |
| Bucket Lifecycle | 云厂商根据对象年龄、标签等自动转移或删除对象 | 它不了解 MO 的表、Snapshot、tombstone、索引和 GC 引用，不能直接作为 MO 的事实源 |
| Fail-safe | 平台在历史窗口后提供的受限灾难恢复保护 | 通常不可由普通用户按业务谓词查询，不能当作用户可管理的 Archive |

### 18.2 典型业务场景

| 场景 | 数据特点 | 适合的生命周期策略 | 例子 |
|---|---|---|---|
| 审计、操作和安全日志 | 追加为主，访问频率随时间快速下降，保存期限由合规要求决定 | 近期开在线，过期后进入归档，按时间范围恢复到新表 | 保留 2 年登录日志，审计调查时恢复某个租户某个月的数据 |
| IoT、设备和监控时序数据 | 数据量大、时间列明确、旧数据很少逐行修改 | 按日/月 range 分区；近期明细在线，历史明细归档，长期只保留聚合结果或原始数据 | 最近 30 天秒级指标在线，31 天至 2 年的原始点位归档 |
| 订单、交易和履约记录 | 当前订单需要低延迟读写，历史订单需要查询、对账或监管留存 | 当前分区保持活动；封存后的历史分区归档；恢复到临时分析表 | 订单完成 90 天后归档，客服调查时恢复指定日期和订单范围 |
| 多租户 SaaS | 租户规模、留存要求和付费等级不同 | policy 绑定稳定的 account/table ID；storage profile 和配额按租户隔离 | 企业租户保留 7 年，普通租户保留 1 年，不能因为表 rename 误绑定到新表 |
| 数据分析和成本治理 | 低频历史数据占用大部分对象存储，偶尔需要全量分析 | 先 dry-run 估算可归档字节、重复副本和恢复成本，再按分区批量执行 | 财务系统只在月末访问三年前的明细，平时不需要在线占用高成本层 |
| 数据删除和隐私合规 | 要求到期后确实删除，而不是继续保留副本 | 未来 maximum-retention 合规模式（非 MVP），并同时处理 PITR、Snapshot、Branch、Backup 和 legal hold | 用户注销后按法规删除个人数据；若仍有 legal hold，必须明确显示删除被阻塞 |

最适合的共同特征是：**时间列明确、数据能够按 range 分区、封存后很少更新、可以接受异步恢复。**

不适合直接启用的场景包括：高频 UPDATE/MERGE 的 OLTP 当前表、没有时间组织且大量对象混合冷热数据的表、要求所有查询永远透明且毫秒级访问深归档的场景、以及尚未定义备份/快照/法务保留优先级的合规场景。

### 18.3 最重要的生命周期术语

| 术语 | 含义 | 在 MO 方案中的理解 |
|---|---|---|
| Data Lifecycle，数据生命周期 | 数据从产生、活跃、归档到删除的业务过程 | lifecycle policy 定义活动/归档/过期条件；在线介质和 cache 由独立 storage placement policy 定义 |
| Retention，保留 | 数据在某个范围内必须继续存在的时间约束 | 需要区分历史保留、业务归档保留和最大删除期限 |
| TTL（Time To Live） | 到达时间条件后自动过期的机制 | TTL 更接近逻辑 expire；它不天然意味着要先复制到 Archive |
| Expire/Logical Expire，逻辑过期 | 从当前业务表中不可见，但底层副本可能暂时存在 | 逻辑过期和物理删除之间可能被 PITR、Snapshot、Branch 或 hold 拉开很长时间 |
| Purge，物理清除 | 在所有引用和保留约束消失后真正删除数据 | purge 不是“到期立即删”，必须经过引用图和 grace period 判定 |
| Tiering，分层 | 按访问性能、成本或恢复能力把数据放到不同层 | MO 应暴露访问能力，不直接暴露每个云厂商的同名 storage class |
| HOT | 高性能、持续访问的活动数据状态 | 主要表示活动表和高缓存/QoS目标，不一定表示另一个持久 bucket |
| ONLINE_COLD | 仍可直接读取，但成本或延迟更低的数据层 | 只有 provider 支持 DirectRead 且 MO 能保持 merge/DML/cache 一致性时才适合透明提供 |
| ARCHIVED | 已从当前活动表分离，需要通过归档目录访问的数据 | 推荐默认恢复到新表，不进入普通 TAE 查询路径 |
| Direct Read | 对象处于低成本层，但 GET 可以直接读取 | GCS Archive、部分 OSS 配置可能属于此类，不能与 AWS Deep Archive 的恢复语义混为一谈 |
| Restore/再水合（Rehydrate） | 把离线归档对象临时或永久恢复到可读层 | 通常是异步操作，有 SLA、优先级、配额和检索费用 |
| Minimum Retention | 数据至少保留多久，期限内不得删除 | “至少 730 天”不代表第 730 天必须删除 |
| Maximum Retention | 数据最多保留多久，超过后必须进入删除流程 | 需要处理 Snapshot、PITR、Backup 和 legal hold 等冲突 |
| Legal Hold，法务保留 | 因诉讼、审计或监管调查而暂时禁止删除 | hold 优先于普通 purge，解除后才重新计算可删除时间 |

### 18.4 存储引擎和归档架构术语

| 术语 | 含义 | 为什么重要 |
|---|---|---|
| Object Storage，对象存储 | 以 key/value 对象形式保存大文件的持久存储，如 S3、OSS、COS、GCS | MO 的 TAE 对象最终落在对象存储，生命周期不能只改 CN 本地缓存状态 |
| Storage Class/Profile | 云厂商对对象的成本、延迟、最短存储时间和恢复能力组合 | MO 应用 storage profile 把 `ACTIVE_DIRECT_READ`、`OFFLINE_RESTORE_REQUIRED` 映射到具体云能力 |
| Provider Lifecycle | S3 等服务按对象创建时间、标签或前缀自动迁移/删除 | provider 不知道 MO 的逻辑引用，最多作为受 MO 控制的执行器 |
| Archive Dataset | 具有稳定 ID、独立于源表生命周期的逻辑归档集合 | 源表 DROP/rename 后仍可审计和恢复，并独立拥有 Manifest、payload、引用和 hold |
| Archive Payload | 归档数据本身，可能是 ObjectIO、Parquet 或版本化 container | 必须有长期格式和 schema 兼容承诺，不能默认当前内部文件格式永久不变 |
| Manifest | 描述归档数据由哪些对象组成、来源 Snapshot、schema、统计和状态的目录 | Manifest 是热端的权威索引，使恢复前不必读取深归档对象的 metadata |
| Metadata Sidecar | 与数据 payload 分离、保持在线的元数据副本 | 用于列 min/max、行数、checksum 和 restore 规划，避免 cache miss 后无法读取原对象 metadata |
| Immutable，封存/不可变 | 数据单元封存后不再允许普通 DML 修改 | 只有不可变单元才能安全复制、校验并在短事务中退休 |
| Sealed Partition，封存分区 | TN fence 之后旧 generation 写入无法提交，并已形成稳定提交水位的分区 | 这是 MO 当前缺失、但归档 MVP 必须新增的权威并发隔离状态 |
| Partition Pruning，分区裁剪 | 根据查询条件跳过不可能命中的分区 | 让按时间恢复和归档只处理必要文件，也是控制扫描成本的基础 |
| Compaction/Merge，合并重写 | 把多个小对象或混合数据重写为更适合查询/归档的大对象 | 生命周期不能绕过 merge；归档前通常要先形成完整封存单元 |
| Tombstone，删除标记 | MVCC/列存系统中表示某些行或对象已被删除的记录 | 归档必须在固定 Snapshot 上应用 tombstone，否则可能恢复出已删除行 |
| Soft Delete Object，对象软删除 | 记录对象的 delete timestamp，先从活动可见性中退休，稍后由 GC 物理删除 | MO 已有候选原语，但仍需和 Manifest、索引、tombstone 一起验证 |
| Generation/版本号 | 对象、policy 或 lifecycle 状态的单调版本 | 防止复制旧对象、过期 cache 或旧任务在新状态上继续提交 |
| CAS（Compare-And-Swap） | 只有对象版本仍等于预期版本时才允许更新 | 可用于 provider generation、Manifest 发布和任务接管的冲突检测 |

### 18.5 历史、恢复和可靠性术语

| 术语 | 含义 | 与本 Feature 的边界 |
|---|---|---|
| Snapshot/Time Travel | 在某个一致时间点读取数据或保留一个历史视图 | 是生命周期的历史引用来源，不是业务归档策略本身 |
| PITR（Point-In-Time Recovery） | 按时间点恢复数据库或读取历史版本 | 保护的是变更前历史，可能阻止已归档源对象被 GC |
| Data Branch | 基于历史状态形成可继续使用的数据分支 | Branch 的保护 Snapshot 也可能 pin 住源对象，必须纳入归档成本和 GC 展示 |
| GC（Garbage Collection） | 回收已经没有任何逻辑或历史引用的数据 | lifecycle 只能产生“可回收”状态，不能绕过 Snapshot/PITR/Branch 引用直接删对象 |
| Pinned Bytes | 因 Snapshot、PITR、Branch、Backup 或 hold 暂时不能删除的字节数 | 必须和 logical archived bytes、physically reclaimed bytes 分开显示 |
| Orphan Object | 已写入但没有成功发布到 Manifest 的孤儿对象 | 通过幂等 key 和 grace period 由 orphan GC 最终清理，不能立即删除以免误删在途对象 |
| Control Plane | policy、binding、job、quota、审计和成本状态 | 决定“应该做什么”，不直接承载大批量数据读取 |
| Data Plane | 活动 payload、归档 payload 和 restore 产生的数据 | 承载实际数据复制、校验、读取和写入 |
| Reference/GC Plane | Snapshot、PITR、Branch、Backup、Restore Job、hold 的引用关系 | 决定“现在能不能物理删除” |
| Async Job | 脱离普通 SQL session、可重试和可查询状态的后台任务 | 适合数小时级归档/恢复，不应绑定客户端连接生命周期 |
| Idempotency，幂等 | 同一操作重复执行，结果等价于执行一次 | 任务重试必须使用稳定 idempotency key，避免重复发布或重复删除 |
| Lease/Fencing | 用租约和递增 epoch 保证同一资源只有一个有效执行者 | CN 故障转移后，旧 executor 即使恢复也不能继续提交旧 generation |
| TN Commit Fence | 在事务最终提交位置校验 partition generation 的权威写屏障 | CN 旧路由可以继续运行，但 fence 后旧 generation 的写入无法提交，因此可证明分区已经封存 |
| Stable Commit Watermark | Fence 之前所有允许提交的写入均已完成的稳定时间点 | `source_snapshot_ts` 必须覆盖该水位，不能通过枚举 CN 本地事务近似得到 |
| Hidden Staging Table | Restore 期间由系统拥有、尚未向用户发布的临时目标表 | 只有数据、schema 和 hash 校验完成后才原子发布，避免用户读到部分恢复结果 |
| Dry-run/Explain | 只计算候选数据、字节数、阻塞引用、恢复时间和费用，不改变数据 | 让管理员在不可逆迁移前知道真实影响 |
| Backpressure，反压 | 当 provider、网络、内存或 restore 配额不足时主动限制生产速度 | 防止归档/恢复把 CN、对象存储连接或任务元数据打爆 |
| Exactly-once | 从业务效果看只产生一次结果 | 外部对象存储通常不能提供跨系统 exactly-once，MO 应通过幂等发布和可回收 orphan 达到等价效果 |
| At-least-once | 任务可能重复执行，但最终通过幂等收敛 | 更符合异步云 API 的现实，不能把“请求发过一次”当成“状态已提交” |

### 18.6 成本、可用性和恢复目标术语

| 术语 | 含义 | 设计时要看什么 |
|---|---|---|
| Retrieval Fee | 从归档层读取或恢复数据的费用 | `EXPLAIN RESTORE` 应估算文件数、字节数和费用类别，而不是只报存储容量 |
| Minimum Storage Duration | 云层要求对象至少存放一段时间，否则仍可能收费 | 归档期限不能只看业务天数，还要校验 provider 的 30/90/180/365 天约束 |
| Early Deletion Fee | 未达到最短存储时长就删除或迁移产生的费用 | policy 缩短、purge 或 profile 切换前要给出成本提示 |
| Duplicate Bytes | 复制校验期间活动副本和归档副本同时存在的字节数 | 归档成功不等于立即节省全部存储；这是正常的过渡成本 |
| RPO（Recovery Point Objective） | 故障后最多能接受丢失多长时间的数据 | lifecycle archive 的 Snapshot TS 和异步复制延迟会影响 RPO，不能只看任务状态 |
| RTO（Recovery Time Objective） | 故障后要求多久恢复可用 | Deep Archive 的恢复可能需要小时级甚至更久，应把 SLA 暴露给业务方 |
| SLA | 对延迟、可用性、恢复时间等作出的服务承诺 | `ONLINE_COLD` 和 `OFFLINE_RESTORE_REQUIRED` 必须有不同 SLA，不能统一承诺普通查询延迟 |

### 18.7 用一句话区分最容易混淆的概念

```text
TTL       = 到期后从当前逻辑表中过期
Archive   = 把低频但仍需保留的数据放入物理分离、但从属于源表/租户的数据集
Backup    = 为系统故障准备另一份恢复副本
Snapshot  = 某个时间点的一致视图或历史引用
PITR      = 按时间点回到历史状态
Purge     = owner 存在时到正常清理时间、或 owner DROP 后，在无 Restore/read lease 时物理删除
Restore   = 把 Archive 数据重新变成可查询数据，通常是异步作业
Tiering   = 按成本/性能/恢复能力改变数据放置，不等价于删除
```

### 18.8 COOL/COLD 到底是什么：一个客户 A 的典型例子

先强调：**COOL/COLD 不是所有云厂商都统一遵循的标准术语。** 在 Snowflake Storage Lifecycle Policies 中，它们是 Snowflake 定义的归档层名称：

- `COOL`：归档后的数据仍然比较容易取回，适合偶尔访问但不要求长期处于最高性能层的数据；Snowflake 文档给出的最短归档期是 90 天。
- `COLD`：比 COOL 更便宜，但取回速度更慢；Snowflake 文档给出的最短归档期是 180 天，恢复最长可能达到 48 小时，并限制单次恢复的文件数。

在 AWS、Azure、GCS、OSS、COS 中，同样叫“冷”或“归档”的层可能分别代表直接读取、需要 restore、不同的最短存储时长和不同的取回费用。因此 MO 不应把 SQL 中的 `COOL`/`COLD` 直接硬编码成某个云厂商 class。下面两个名称只用于解释 provider 能力，并不表示当前规范会把 `ONLINE_COLD` 暴露为 Lifecycle 状态：

```text
ACTIVE_DIRECT_READ       = 成本较低，但普通 SQL 仍可直接读取
OFFLINE_RESTORE_REQUIRED  = 查询前必须提交异步恢复任务
```

#### 客户 A：工业设备 SaaS 的设备日志

客户 A 为工厂管理设备，MatrixOne 中每天写入大量设备事件、告警和传感器明细。客户的实际需求不是“永远把所有数据放在最快的存储上”，而是：

1. 最近 30 天的数据用于实时运维，查询频繁，必须正常读写；
2. 30 天到 180 天的数据偶尔用于趋势分析和客服排障，仍保留在活动表中，由 CN cache 自适应冷热；
3. 180 天到 2 年的数据很少访问，但合同和安全审计要求保留；
4. 2 年以后通常可以删除；若调查尚未完成，用户应在归档进入删除前先 Restore 到新表。本首个 GA 不提供 Legal Hold。

可以配置成如下逻辑（具体云 class 由管理员的 storage profile 决定）：

| 数据年龄 | MO 逻辑状态 | 客户体验 | 背后动作 |
|---|---|---|---|
| 0–30 天 | `ACTIVE` | 正常查询、UPDATE、告警分析 | 活动表；CN cache 会保留热点 |
| 30–180 天 | `ACTIVE` | 仍可普通查询；低频页自然退出 cache | 活动表；不引入 Lifecycle `ONLINE_COLD` 状态 |
| 180 天–2 年 | `ARCHIVED` / 类 COLD | 当前表不可直接查；提交带时间和设备条件的 Restore Job，恢复到新表 | 复制、校验后从活动表逻辑退休，归档 payload 异步保存 |
| 2 年以后 | `PURGE_ELIGIBLE` | 一般不可见 | 等当前 Archive Restore/read lease 结束后进入不可逆物理清除；普通 PITR/Snapshot/Backup 不保护 Archive |

#### 客户 A 什么时候真正使用 Archive

例如客户 A 在 2026 年 7 月发现某批设备在 2025 年 2 月出现过异常。运维人员不需要把两年的所有日志全部恢复，而是执行一个带范围的恢复任务：

```sql
EXPLAIN RESTORE
  FROM ARCHIVE OF device_events
  WHERE device_id IN ('A-1007', 'A-1032')
    AND event_ts >= '2025-02-01'
    AND event_ts <  '2025-03-01';

CREATE RESTORE JOB restore_device_events_202502
  FROM ARCHIVE OF device_events
  INTO device_events_202502
  WHERE device_id IN ('A-1007', 'A-1032')
    AND event_ts >= '2025-02-01'
    AND event_ts <  '2025-03-01';
```

恢复完成后，客户把 `device_events_202502` 与当前设备表、工单表关联，完成故障定位。Archive Restore Job 结束后释放 access lease；恢复得到的新表按普通 MO 表管理，调查结束后由客户删除。

#### 客户 A 得到的实际好处

- **成本下降**：两年前的绝大多数数据不再长期占用高成本在线层；同时可以看到仍被 PITR、Snapshot 或 Branch pin 住的源对象，避免误判节省金额。
- **在线性能更稳定**：当前运维查询主要面对近 30 天活动数据，历史长尾不会持续扩大 hot cache、merge 和统计开销。
- **生命周期更容易解释**：可以回答“当前表保留多久、归档正常保留多久、何时允许 purge、是否有 Restore lease 阻止删除”，而不是只说对象存储已经变冷；但这不等于 Legal Hold/WORM 合规证明。
- **恢复范围可控**：按设备、时间和其他归档元数据恢复，先估算文件数、字节数和费用，避免一次性恢复数十 TB。
- **不会把日常查询绑在云厂商差异上**：对客户只有活动数据和归档数据两种业务可见性；AWS 的数小时 thaw 和 GCS 的直接读取由 Archive Profile 屏蔽。

这个例子也说明了为什么当前规范以有界 exact TAE Object set 为执行边界，并把恢复固定为独立新表，而不要求客户先建立时间 Range Partition。如果客户 A 要求对两年归档数据继续毫秒级 UPDATE，这项能力就不适合启用；这部分数据应继续留在活动表。若客户要求 DROP 租户后仍不可删除地保留七年，本首个 GA 同样不适用。

## 19. 当前规范入口

本文不再提供最终实现建议。当前决定是：

- 以有界 exact TAE Object set 而不是 SQL Partition 为执行边界；
- 使用 Lifecycle Rewrite Executor，不新增或修改普通 Merge Engine；
- Commercial GA 覆盖 TTL、direct-readable archive、恢复到独立新表和从属 owner 的不可逆 Purge；
- Source Pin 使用独立 table-only Lifecycle Snapshot + GC visible gate；每个 child 使用 system Attempt Control，Archive 第一次 PUT 前使用 system Attempt Root；
- Archive owner 使用 stable logical table identity，TAE retirement 使用 physical table/object identity；首个 GA 拒绝 Lifecycle-bound 表的 ALTER COPY；
- Archive 从属于源表/租户，DROP 后不承诺恢复；Legal Hold/WORM、DROP 后保留和 Archive Backup/DR 不在首个 GA；
- Backup/PITR/Snapshot Restore/Clone/Branch/DR 对 Lifecycle 表 fail closed；
- 首个 GA 拒绝所有物化隐藏索引表，能力创建/bind/final commit CAS 同一 Feature Guard；
- restore-required deep archive 是首个 GA 后的可选 Archive Profile，不阻塞 GA；
- CDC、FK、Publication/Subscription、Fulltext、Vector 和外部插件不在首个 GA 支持矩阵；
- Object Index/scheduler 只覆盖显式 Binding；账户 Guard=1000、Lifecycle-only集群activation
  slot=1000、1/10 TiB 和同表/库/账户/集群 `1/2/4/8` child 并发；active coexistence 超线
  必须暂停新claim而不是影响普通MO；
- 首个 GA 不开放 Archive Small Mixed，也不跨 external transaction 复用 verified staging；
  任意 Archive Mixed 走单源 Rewrite，明确aborted后一律 cleanup 并以 fresh attempt 重做；
- Lifecycle-owned final transaction使用TN shard复制、WAL replay且有硬上限的Terminal
  Journal；COMMITTED与退休事务原子记录，ABORTED只有durable ACK后才是明确终态，同一
  external txn终态后不得再次进入storage Commit；普通事务不访问该Journal；
- tagged Entry replay、table-only GC visible gate、Control/Root/owner cleanup、Feature Guard、资源硬上限、不可逆 Purge、Stage 4、升级降级和 TB 级长稳全部是 GA 门禁。

唯一规范请阅读：

1. [TAE 对象级数据生命周期概要设计](issue-24552-24853-tae-object-lifecycle-overview-design-cn.md)
2. [ADR：以 TAE Object 而不是 SQL Partition 作为生命周期执行边界](issue-24552-24853-object-lifecycle-boundary-adr-cn.md)

## 20. 参考资料

### MatrixOne

- [Issue #24853](https://github.com/matrixorigin/matrixone/issues/24853)
- [Issue #24552：Native table TTL](https://github.com/matrixorigin/matrixone/issues/24552)
- [PR #18306：旧 Retention 实现](https://github.com/matrixorigin/matrixone/pull/18306)
- [Issue #22255：移除旧 Retention](https://github.com/matrixorigin/matrixone/issues/22255)
- [PR #22261：移除旧 Retention](https://github.com/matrixorigin/matrixone/pull/22261)
- [FileService AI Skill](../ai-skills/fileservice.md)
- [Storage Engine AI Skill](../ai-skills/storage-engine.md)
- [Backup/Restore AI Skill](../ai-skills/backup-restore.md)
- [Snapshot Read 设计文档](https://github.com/matrixorigin/docs/blob/b38d52fd99f899846015f0ffc0dfacd8f12da10a/design/mo/backup/20240419-YANGGMM-snapshot_read_introduction.md)
- [PITR Tool 设计文档](https://github.com/matrixorigin/docs/blob/b38d52fd99f899846015f0ffc0dfacd8f12da10a/design/mo/backup/20240808-YANGGMM-pitr-tool-introduction.md)
- [Storage Hierarchy 设计文档](https://github.com/matrixorigin/docs/blob/b38d52fd99f899846015f0ffc0dfacd8f12da10a/design/mo/tnservice/20230308-aptend-ref_storage_hierarchy.md)

### 数据库和数据平台

- [Snowflake Storage Lifecycle Policies](https://docs.snowflake.com/en/user-guide/storage-management/storage-lifecycle-policies)
- [Snowflake 2025-11-07 GA Release Note](https://docs.snowflake.com/en/release-notes/2025/other/2025-11-07-storage-lifecycle-policies-ga)
- [Snowflake CREATE STORAGE LIFECYCLE POLICY](https://docs.snowflake.com/en/sql-reference/sql/create-storage-lifecycle-policy)
- [Snowflake Retrieve Archived Data](https://docs.snowflake.com/en/user-guide/storage-management/storage-lifecycle-policies-retrieving-archived-data)
- [Snowflake Lifecycle Billing](https://docs.snowflake.com/en/user-guide/storage-management/storage-lifecycle-policies-billing)
- [Snowflake DROP TABLE](https://docs.snowflake.com/en/sql-reference/sql/drop-table)
- [Databricks Delta Archival Support](https://docs.databricks.com/aws/en/optimizations/archive-delta)
- [Elasticsearch ILM](https://www.elastic.co/docs/manage-data/lifecycle/index-lifecycle-management)
- [Elasticsearch Searchable Snapshot](https://www.elastic.co/docs/reference/elasticsearch/index-lifecycle-actions/ilm-searchable-snapshot)
- [Tiger/Timescale Data Tiering](https://www.tigerdata.com/docs/use-timescale/latest/data-tiering/about-data-tiering)
- [Tiger/Timescale Tiered Data References and PITR](https://www.tigerdata.com/docs/use-timescale/latest/data-tiering/tiered-data-replicas-forks)
- [ClickHouse TTL](https://clickhouse.com/docs/guides/developer/ttl)
- [BigQuery Pricing / Long-term Storage](https://cloud.google.com/bigquery/pricing)
- [BigQuery Time Travel and Fail-safe](https://docs.cloud.google.com/bigquery/docs/time-travel)
- [Amazon Redshift Managed Storage](https://docs.aws.amazon.com/redshift/latest/mgmt/managing-cluster-considerations.html)
- [Oracle Heat Map](https://docs.oracle.com/en/database/oracle/oracle-database/26/refrn/HEAT_MAP.html)
- [Oracle Automatic Data Optimization](https://docs.oracle.com/en/database/oracle/oracle-database/26/vldbg/time-based-info.html)
- [Apache Doris Remote Storage](https://doris.apache.org/docs/3.x/table-design/tiered-storage/remote-storage/)
- [Apache Iceberg Maintenance](https://iceberg.apache.org/docs/latest/maintenance/)

### 对象存储

- [AWS S3 Archived Objects](https://docs.aws.amazon.com/AmazonS3/latest/userguide/archived-objects.html)
- [AWS S3 Lifecycle Transition Considerations](https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-transition-general-considerations.html)
- [AWS S3 Intelligent-Tiering](https://docs.aws.amazon.com/AmazonS3/latest/userguide/intelligent-tiering-overview.html)
- [Google Cloud Storage Classes](https://docs.cloud.google.com/storage/docs/storage-classes)
- [Azure Blob Archive Rehydration](https://learn.microsoft.com/en-us/azure/storage/blobs/archive-rehydrate-overview)
- [Alibaba Cloud OSS Storage Classes](https://www.alibabacloud.com/help/en/oss/user-guide/oss-overview)
- [Alibaba Cloud OSS Archive Direct Read](https://www.alibabacloud.com/help/en/oss/user-guide/archive-direct-reading)
- [Tencent COS Lifecycle](https://cloud.tencent.com/document/product/436/17028)
- [Tencent COS Archive Restore](https://cloud.tencent.com/document/product/436/32430)
