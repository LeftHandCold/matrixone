# MatrixOne Issue #24853：分层数据生命周期方案复核与设计建议

> 状态：方案评估稿，不代表已进入实现
>
> 复核日期：2026-07-23
>
> MatrixOne 基线：`main@0d7eeb38b43b6b89f746b0a634662349a4b01de2`
>
> MatrixOne Docs 基线：`main@b38d52fd99f899846015f0ffc0dfacd8f12da10a`
>
> 目标 Issue：[#24853](https://github.com/matrixorigin/matrixone/issues/24853)

## 1. 结论先行

Issue #24853 反映的用户需求是真实且重要的，但 Issue 对问题的定义和最初方案需要做较大修正。

核心结论如下：

1. **Issue 中关于 Snowflake 的关键前提已经过时。** Snowflake 已于 2025-11-07 将 Storage Lifecycle Policies 正式 GA，当前已支持基于行条件的归档/过期、`COOL`/`COLD` 两种归档层、归档元数据查询和 `CREATE TABLE ... FROM ARCHIVE OF ... WHERE ...` 恢复。因此 MO 的差异化不能再建立在“Snowflake 不提供显式归档策略”上。
2. **历史版本保留、业务数据生命周期和物理冷热放置必须拆成三个独立平面。** `DATA_RETENTION_TIME_IN_DAYS`、PITR、Snapshot 解决的是历史版本；TTL/归档解决的是当前表中哪些业务行还应可见；HOT/COLD 解决的是在线数据的缓存、介质和访问 SLA。把三者放入一个 `RETENTION(HOT, COLD, ARCHIVE)` 语法会产生不可解释的冲突。
3. **当前 MO 有可复用的基础能力，但不存在可以直接拼起来完成该 Feature 的“现成积木”。** 可以复用 Snapshot/PITR 的 GC 引用判定、TaskService、FileService、Stage 导入导出和对象存储适配器；但当前对象定位、对象元数据、缓存路径、merge、tombstone、索引和云存储接口都没有 lifecycle 状态与 restore 能力。
4. **不建议让云厂商 Bucket Lifecycle 直接管理当前 TAE 对象，也不建议第一版把当前 TAE 对象原地转入深归档。** 这两种方案存在数据不可读、metadata 也被冻结、缓存绕过状态、merge/DML 失败、策略漂移以及误归档内部对象等严重风险。
5. **推荐主方案是“MO 管理的逻辑归档数据集 + 热端 Manifest + 可恢复异步作业”。** 先把符合条件且已经封存的数据单元复制到独立、由 MO 管理的归档数据集，校验成功后再在一个 MO 事务中发布归档 Manifest 并从活动表逻辑移除。深归档数据不进入正常 TAE 查询路径，恢复默认写入新表。
6. **第一版必须收窄能力。** 只支持表级、单调时间列、与 range 分区边界对齐、完整封存分区；只支持新表恢复；不支持任意 SQL 表达式、仅按 sort key 选择对象、last-access 作为正确性依据、透明访问深归档、同表恢复和账户级继承。
7. **MO 可以在“可解释性和跨云一致性”上做得比主流方案更好。** 关键是提供 lifecycle dry-run、历史引用导致的 pinned bytes、物理回收进度、恢复文件数/字节数/预计费用、明确的云能力契约，以及可中断、可重试、有 fencing 的异步任务。

建议将 #24853 与 [#24552 Native table TTL](https://github.com/matrixorigin/matrixone/issues/24552) 共享同一个 lifecycle 执行内核，但保持不同的用户语义：TTL 是逻辑过期动作，归档是“先保存到归档数据集，再逻辑过期”。

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

本节只描述当前 `main@0d7eeb38` 可从代码和文档确认的事实。

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
- 长时间复制期间发生的 INSERT/UPDATE/DELETE、tombstone 和 merge 必须通过 sealed state 或 generation/read-set 冲突检测处理；
- 直接 soft-delete 基础对象可能让 secondary/fulltext/vector index 留下悬空条目；
- range/list 分区可以删除，不代表当前已经存在“分区只读/封存”的写入栅栏。

最值得验证的 MVP 路径是：给时间 range 分区增加显式 sealed 状态，以分区为选择和并发隔离单元，归档成功后在短事务中发布 Manifest 并删除分区。对象级路径应在上述协议和索引一致性验证完成后再开放。

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
| Snowflake | Storage Lifecycle Policy 基于行表达式，每日增量执行，可归档到 COOL/COLD 或直接过期；归档行不直接查询，通过带强制 `WHERE` 的 `CREATE TABLE ... FROM ARCHIVE OF` 新建表恢复；COLD 最长可 48 小时 | Issue 的 Snowflake 前提已过时；恢复到新表、强制谓词、热端 archive metadata、DML 锁、临时双份存储都值得借鉴 |
| Databricks Delta | Public Preview；依赖外部 S3 lifecycle 和手工同步的 `delta.timeUntilArchived`；只允许能排除归档文件的查询，其余早失败；`MERGE/UPDATE/DELETE` 需先恢复 | 是“provider lifecycle 做事实源”的风险样本：策略漂移、`_delta_log` 误归档、改长阈值不能自动回热、只靠有限 file stats |
| Elasticsearch ILM | 对 rollover 后的 immutable backing index 做 hot/warm/cold/frozen/delete；searchable snapshot 前通常 force-merge；不能直接处理 write index | 生命周期单元应先封存，并在归档前 compact；不应对任意可变行或当前写对象直接沉降 |
| Tiger/Timescale | 以时间 chunk 为单位异步迁移到对象存储；tiered chunk 不可 INSERT/UPDATE/DELETE；catalog 保存引用；引用归零后还延迟 14 天再 hard delete 以支持 PITR | “封存 chunk + catalog reference + delayed hard delete”与 MO 最匹配 |
| ClickHouse | TTL 可删除、移动、rollup，实际在 merge 中执行；官方建议 TTL 列与 partition key 对齐，从而整分区删除 | MO 归档 MVP 应先要求生命周期列与 range 分区对齐；对象级重写后置 |
| BigQuery | 90 天未修改后自动转长期存储计价，不降低性能/可用性；物理层对用户隐藏 | 在线 HOT/COLD 可以优先做自动优化，而不是暴露云厂商物理 class |
| Redshift RA3/RG | SSD 保存热块、S3 保存冷块，按 block temperature、age 和 workload 自动放置 | MO 的 HOT 更接近 CN cache/QoS，未必需要用户指定一个“热桶” |
| Oracle Heat Map/ADO | 跟踪 block 修改与 segment 访问统计，按 row/segment/tablespace policy 做压缩、In-Memory 驱逐或 segment 级 tablespace 迁移；storage tiering 只支持 segment scope | 热度应是数据库自身可观测信号；物理迁移应以完整 segment/partition 为单位，不能把 row-level policy 等同于任意对象跨介质 |
| Apache Doris | `STORAGE POLICY + cooldown_ttl` 把数据放远端；不支持 Unique MOW、backup 等组合，已冷却数据不会因 policy 改长自动回迁 | 原地分层会向 DML/备份/策略变更传播大量限制，第一版不宜承诺全表型透明兼容 |
| Apache Iceberg | 只有当数据文件不再被任何可 time-travel/rollback 的 Snapshot 引用时才删除；过短 orphan retention 会误删在途文件并损坏表 | MO 必须把 Snapshot/PITR/Branch/Job/Legal Hold 统一纳入引用图，并给 orphan GC 足够 grace period |

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
| 复制 Snapshot 后再退休当前对象即可保证一致 | 阻断 | 两阶段之间的 UPDATE/DELETE、tombstone 或 merge 可能让归档副本恢复出已删除行，或让退休集合失效；仅校验基础对象 ID 不够 | MVP 先封存时间分区并排空在途写；后续方案必须验证基础对象、tombstone 和索引的统一 generation/read set |
| `SoftDeleteObject` 已经让 Manifest + 退休可直接实现 | 高 | 当前有事务原语，但没有与 lifecycle catalog、外部副本校验、分区写栅栏和索引更新集成，也没有端到端故障测试 | 把它作为实现候选而非已完成能力；优先验证完整 range 分区路径 |
| 用 Bucket Lifecycle 自动迁移当前对象 | 阻断 | 表对象不是按表前缀组织；provider 策略异步且会漂移；可能误归档内部对象 | MO catalog 是唯一事实源，provider 只作为显式作业执行器 |
| 用 last-access 自动识别冷热 | 高 | CN cache hit 不会触达 provider，provider 看到的访问时间不等于用户查询热度 | 只用于 cache/advisor，不作为数据可见性或深归档正确性依据 |
| 复用 Stage 保存归档对象 | 高 | Stage 凭据和用户生命周期不等于内部对象所有权、引用、GC；删除 Stage 可破坏数据 | 新建 system-managed storage profile；Stage 仅做导入导出 |
| 复用 `mo_pitr` 保存 tier retention | 高 | 语义不同，字段范围有限，没有 tier/profile/job/version | 新 lifecycle catalog；只复用历史引用判定 |
| `RESTORE TABLE events FROM ARCHIVE` 恢复原表 | 高 | 主键冲突、重复行、当前 schema、约束、并发写入和部分失败难定义 | MVP 只允许恢复到新表且必须带 predicate |
| 归档后立即节省全部活动存储 | 高 | PITR、Snapshot、Branch、Fail-safe 式窗口会继续 pin 原对象；复制阶段还会双份计费 | dry-run 和监控显式展示 duplicate/pinned/reclaimed bytes |
| 改大 policy 阈值可自动回热 | 中 | 云 provider 不会反向执行旧对象恢复，Databricks/Doris 均有类似限制 | policy version 化；已迁移数据只通过显式 restore/migration 回迁 |
| 删除超过归档期限的数据即可合规 | 高 | Snapshot/Branch/legal hold 可能继续保留原活动对象 | 区分最低留存、最大留存、hold；强制删除需处理整个引用图 |

## 7. 四种可选实现路径

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

### 方案 C：MO 管理的独立逻辑归档数据集

做法：

1. 在一致性 Snapshot TS 上选择符合条件的封存数据单元；
2. 写入独立归档 payload；
3. 校验 checksum、size、row count 和可读性；
4. 在热端 catalog 发布 Manifest；
5. 同一 MO 事务中将相应数据从活动表逻辑移除；
6. 原活动对象继续由现有 Snapshot/PITR/Branch 规则保护和 GC；
7. 归档数据通过异步 Restore Job 恢复到新表。

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

**结论：推荐主方案。**

这里的“同一 MO 事务”是需要新增和验证的 lifecycle commit protocol，不是当前代码已经提供的高层 API。底层 `SoftDeleteObject` 和 `DROP PARTITION` 提供了原语候选；对 MVP 而言，完整 range 分区比任意排序对象更安全，因为分区边界、写入栅栏、索引和 DDL 冲突都更容易定义。若这条事务路径无法证明 Manifest 发布与分区删除原子提交，则方案 C 不能进入生产，只能退回“归档已发布、活动数据稍后退休”的可见双份状态，绝不能先删活动数据。

### 方案 D：SQL Task + Stage Parquet

这是当前即可实施的 workaround：

1. 周期查询旧分区；
2. 导出 Parquet 到独立 Stage；
3. 外部校验；
4. 手工或任务 DELETE，或对 range/list 分区执行 DROP PARTITION；
5. 需要时 LOAD 到新表。

它不能提供原生 exactly-once、引用保护、schema evolution、自动 restore 和成本可见性。

**结论：可作为 Phase 0 使用指南，不应包装成已完成的原生 lifecycle。**

## 8. 推荐架构

### 8.1 三平面结构

```text
                    +-------------------------------+
                    | Lifecycle Control Plane       |
                    | Policy / Binding / Job / Cost |
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

建议把用户语义定义为访问契约，而不是厂商 class：

| 状态 | 是否属于当前表 | 正常 SQL 是否直接读 | 是否允许普通 DML | 说明 |
|---|---|---|---|---|
| `HOT` | 是 | 是 | 是 | 活动数据；高 cache/QoS 目标 |
| `ONLINE_COLD` | 是 | 是 | 是，但可能有更高读成本 | 仅映射到支持直接读的存储 profile |
| `ARCHIVED` | 否 | 否 | 否 | 独立归档数据集，需要 restore |
| `LOGICALLY_EXPIRED` | 否 | 否 | 否 | 已从当前表移除，原对象可能仍被历史引用 |
| `PURGED` | 否 | 否 | 否 | 所有归档 payload 和不再被引用的源对象均已物理回收 |

“HOT”在 MO 中主要代表缓存/QoS，而不是另一个 durable 主存。MO 本身已经把持久数据放在对象存储，CN 本地 SSD/内存更多是缓存。

### 8.3 建议新增 Catalog

名称仅作设计示例：

#### `mo_data_lifecycle_policies`

- `policy_id`
- `policy_name`
- `owner_account_id`
- `age_column_id`
- `cold_after`
- `archive_after`
- `purge_after`
- `storage_profile_id`
- `retention_semantics`：minimum / maximum / best-effort
- `version`
- `status`
- `created_at` / `modified_at`

#### `mo_data_lifecycle_bindings`

- `policy_id`
- `scope_type`
- `account_id` / `database_id` / `table_id`
- `effective_version`
- `bound_at`

绑定必须依赖稳定 ID，name 只用于展示。rename、drop/recreate 不能让旧 policy 意外绑定到新对象。

#### `mo_archive_manifests`

- `archive_id`
- `source_table_id`
- `policy_id` / `policy_version`
- `source_snapshot_ts`
- `schema_version`
- `predicate/cutoff`
- `row_count` / `logical_bytes` / `physical_bytes`
- policy/query 列 min/max
- `storage_profile_id`
- `payload_format_version`
- `state`
- `created_at` / `expires_at`
- `reference_count` / `legal_hold`

#### `mo_archive_objects`

- `archive_id`
- `object_id`
- `uri` 或 profile 内 key
- provider generation/version
- checksum
- size / row count
- storage state
- restore state
- last verified time

#### `mo_lifecycle_jobs`

- `job_id`
- `idempotency_key`
- `policy_id` / generation
- `target_table_id`
- `job_type`
- `state`
- `lease_owner` / `lease_epoch`
- object/chunk progress
- retry count / next retry
- last error
- requested/cancelled/completed time

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

## 9. Lifecycle 执行协议

### 9.1 数据单元选择

第一版只选择满足以下条件的单元：

1. lifecycle 列是非空的 DATETIME/TIMESTAMP；
2. lifecycle 列与 range 分区边界对齐；
3. 分区已经进入由系统强制的 sealed 状态，不接受新的 INSERT/UPDATE/DELETE；
4. 整个分区的上界 `<= cutoff`；
5. 没有进行中的 merge、DDL 或另一个 lifecycle job；
6. policy generation 与任务捕获的一致。

当前 MO 没有该 lifecycle sealed 状态，必须新增 DML admission gate，并等待已经进入事务的写入完成后才能捕获归档 Snapshot。不能把“最近没有写入”当成封存。

若 `min <= cutoff < max`，这是混合单元。第一版应跳过；后续可以：

- 通过 SPLIT/REORGANIZE PARTITION 形成完整到期分区；或
- 进入显式 lifecycle compaction，把到期和未到期行重写到不同对象，再使用经过验证的对象级退休协议。

不能把混合对象整个归档，否则会让仍应在线的数据消失。

### 9.2 Archive 的安全顺序

推荐顺序：

1. 创建带幂等键的 Job，并申请 table/partition lease；
2. 在短事务中把目标分区从 `ACTIVE` 切到 `SEALING`，阻止新 DML 进入；
3. 等待所有已获准的在途写和 merge 完成，再切到 `SEALED`；
4. 捕获 `source_snapshot_ts`、policy version、分区定义以及基础对象/tombstone/index generation；
5. 从一致性 Snapshot 读取已经封存的分区；
6. 写入内容寻址或 job-scoped 的 archive payload；
7. 校验 checksum、size、row count，并做最小可读性检查；
8. 将 Manifest 写为 `VERIFIED_NOT_PUBLISHED`；
9. 在一个短 MO 事务中发布 Manifest，并删除分区或逻辑退休活动对象，同时处理索引；
10. commit 后将任务标记为 `ARCHIVED`；
11. 未发布 payload 由 orphan GC 在 grace period 后回收。

如果复制失败，可以保留 `SEALED` 重试，或在确认没有 Manifest 发布和退休动作后显式 unseal。不能在不做审计的情况下自动解封，因为调用方可能已经依赖“该时间分区不再可写”的语义。

核心不变量：

> 在活动数据被逻辑退休之前，必须至少存在一份已经校验且可由 catalog 找回的归档副本。

允许暂时双份，不允许出现零副本。

### 9.3 并发语义

可借鉴 Snowflake 的保守策略：

- lifecycle commit 窗口阻止影响候选单元的 `UPDATE`、`DELETE`、`MERGE`；
- `INSERT` 可以继续，但只属于捕获 Snapshot 之后的新 generation；
- 若候选对象在归档期间被 merge/replace，当前 job 必须基于 generation 检测冲突并重算，不能继续退休旧选择集；
- 同一 table/policy generation 只允许一个 active executor，CN failover 通过 lease epoch fencing。

### 9.4 Archive Payload 格式

不能默认把“当前 ObjectIO 文件”当作长期归档格式。需要单独 ADR 决定：

- **ObjectIO**：恢复进 MO 成本低，但必须承诺跨版本 reader compatibility；
- **Parquet + 完整 schema manifest**：生态和长期可读性更好，但需验证全部 MO 类型、默认值、约束和精度映射；
- **版本化 archive container**：内部包含 columnar payload、schema、统计和 checksum，工作量最大但契约最清楚。

在 payload 格式和升级兼容测试确定前，不应承诺多年级的合规可恢复性。

## 10. Restore 设计

### 10.1 第一版只恢复到新表

不建议：

```sql
RESTORE TABLE events FROM ARCHIVE ...;
```

建议：

```sql
EXPLAIN RESTORE
  FROM ARCHIVE OF events
  WHERE event_ts >= '2024-01-01'
    AND event_ts <  '2024-02-01';

CREATE RESTORE JOB restore_events_202401
  FROM ARCHIVE OF events
  INTO events_restored_202401
  WHERE event_ts >= '2024-01-01'
    AND event_ts <  '2024-02-01'
  MAX_FILES 100000
  MAX_BYTES 10 TB
  PRIORITY STANDARD;

SHOW RESTORE JOB restore_events_202401;
```

理由：

- 原表可能已经出现相同主键；
- 当前 schema 可能已增删列；
- 原表可能继续写入；
- restore-required provider 需要数小时，不能占用普通 session；
- 部分恢复失败时，新表可保持不可见/RESTORING，成功后再原子发布；
- 取消任务可能无法取消云端费用，必须在 Job 状态中明确提示。

### 10.2 `WHERE` 必须强制

Snowflake 已强制 restore 使用 `WHERE`，并允许用 `EXPLAIN` 估算文件和字节。MO 也应强制：

- 至少有可利用 archive min/max 的谓词；
- 先输出 estimated files/bytes、provider retrieval SLA、费用类别；
- 允许账户级最大 files/bytes/concurrency；
- 超过阈值需要显式管理员批准。

### 10.3 Schema evolution

建议第一版采用与 Snowflake 接近、可解释的规则：

- 新表使用源表当前 schema；
- 归档前不存在的新增列恢复为 NULL；
- 已删除列默认不暴露，但 archive manifest 保留原 schema，供管理员审计或未来 raw export；
- type change 必须有显式兼容矩阵，不兼容则 job 早失败；
- index、constraint、publication、subscription 等派生对象不从 archive payload 盲目恢复，按当前定义重建或显式排除。

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
- restore 到新表后按当前 schema 和 index 定义重建；
- restore dry-run 要估算重建成本，而不仅是对象恢复成本。

## 12. GC 与引用模型

建议统一维护以下引用：

```text
active table
snapshot
pitr
branch protect snapshot
backup / replica / fork
archive manifest
restore job
legal hold
```

物理删除条件至少是：

```text
retention deadline reached
AND active reference = 0
AND snapshot/pitr/branch/backup reference = 0
AND restore job reference = 0
AND legal hold = 0
AND orphan/grace deadline reached
```

需要区分两类物理对象：

1. **原活动对象**：继续由当前 TAE GC 和 Snapshot/PITR/Branch 引用判定保护；
2. **归档 payload**：由 archive catalog reference、restore job、backup/DR、hold 和 archive retention 保护。

建议参考 Timescale 的做法：引用归零后仍设置 hard-delete grace period。Iceberg 的经验也说明，过短 orphan retention 会把仍在写入或提交中的文件误判为 orphan。

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
  MOVE TO ONLINE_COLD AFTER INTERVAL '7' DAY
  ARCHIVE AFTER INTERVAL '90' DAY
  PURGE AFTER INTERVAL '730' DAY
  USING STORAGE PROFILE mo_archive_default;

ALTER TABLE events
  ADD DATA LIFECYCLE POLICY events_lifecycle ON (event_ts);
```

明确语义：

- `[0, 7d)`：HOT；
- `[7d, 90d)`：ONLINE_COLD；
- `[90d, 730d)`：ARCHIVED；
- `>=730d`：归档 payload 可进入 purge 判定；
- 原活动对象何时物理删除仍受历史引用影响。

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

## 14. 分阶段落地建议

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
- sealed state、在途写排空和分区级 DML admission；
- policy/binding/job catalog；
- dry-run、fencing、对象级 journal；
- 逻辑 expire 与物理 GC 指标；
- 与 Snapshot/PITR/Branch 联合测试。

这一阶段可先解决 #24552 的安全子集，不涉及云深归档。

### Phase 2：MO-managed Online Archive

- 建立 archive payload + hot Manifest；
- copy → verify → transactional publish/retire；
- 强制 predicate 的新表恢复；
- schema evolution；
- archive/restore cost 和进度；
- 先使用始终可直接读取的 profile 验证协议。

### Phase 3：Restore-required Deep Archive

- AWS/COS/OSS/Azure provider capability adapter；
- restore event + 有界 polling；
- priority、quota、deadline、cancel 状态；
- 大规模文件恢复的 backpressure；
- cache gate 和跨 CN invalidation；
- 故障注入覆盖所有中间状态。

### Phase 4：ONLINE_COLD 与自动热度优化

- 对支持 direct read 的 provider class 做活动对象在线 COLD；
- lifecycle-aware merge QoS；
- 基于 MO 查询统计的 cache/advisor；
- 可选自动放置，用户声明性能/成本意图而不是厂商 class；
- account/database 默认继承；
- rollup/downsample；
- legal hold 与 maximum-retention 强制模式。

## 15. 必须通过的故障与兼容测试

### 15.1 正确性

- cutoff 边界、时区、NULL、未来时间、late arrival；
- whole-unit 和 mixed-unit；
- policy 修改、删除、改名、table rename、drop/recreate；
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
10. CN owner 失联，新 owner 以更高 lease epoch 接管。

每个点都要证明：

- 不出现零副本；
- 不重复发布；
- 不重复删除；
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

## 17. 尚需产品/架构决策的问题

以下问题未决前不建议进入完整实现：

1. 归档 payload 的长期格式是 ObjectIO、Parquet 还是新 container？
2. MVP 是否强制用户按 lifecycle 时间列分区，还是允许后台 lifecycle compaction？
3. `PURGE AFTER` 是 best-effort、minimum-retention 还是 maximum-retention？
4. maximum-retention 与 Snapshot/PITR/Branch/legal hold 谁优先？
5. archive data 是否参与备份、跨区复制、Data Branch 和 clone？由谁承担副本费用？
6. ONLINE_COLD 是否要求普通查询完全透明，还是需要 session/query opt-in？
7. policy 切换 storage profile 后，历史 archive 是否原地迁移？
8. KMS key rotation 和密钥疑似泄露时，归档数据如何 rekey？
9. 长时间 restore 完成后，新表何时可见，失败表如何清理？
10. account/database 默认 policy 的继承、覆盖和 policy generation 如何定义？

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
| 数据删除和隐私合规 | 要求到期后确实删除，而不是继续保留副本 | 使用 maximum retention，并同时处理 PITR、Snapshot、Branch、Backup 和 legal hold | 用户注销后按法规删除个人数据；若仍有 legal hold，必须明确显示删除被阻塞 |

最适合的共同特征是：**时间列明确、数据能够按 range 分区、封存后很少更新、可以接受异步恢复。**

不适合直接启用的场景包括：高频 UPDATE/MERGE 的 OLTP 当前表、没有时间组织且大量对象混合冷热数据的表、要求所有查询永远透明且毫秒级访问深归档的场景、以及尚未定义备份/快照/法务保留优先级的合规场景。

### 18.3 最重要的生命周期术语

| 术语 | 含义 | 在 MO 方案中的理解 |
|---|---|---|
| Data Lifecycle，数据生命周期 | 数据从产生、活跃、低频访问、归档到删除的完整过程 | policy 定义每个阶段的进入条件、可见性、可写性和最终回收条件 |
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
| Archive Payload | 归档数据本身，可能是 ObjectIO、Parquet 或版本化 container | 必须有长期格式和 schema 兼容承诺，不能默认当前内部文件格式永久不变 |
| Manifest | 描述归档数据由哪些对象组成、来源 Snapshot、schema、统计和状态的目录 | Manifest 是热端的权威索引，使恢复前不必读取深归档对象的 metadata |
| Metadata Sidecar | 与数据 payload 分离、保持在线的元数据副本 | 用于列 min/max、行数、checksum 和 restore 规划，避免 cache miss 后无法读取原对象 metadata |
| Immutable，封存/不可变 | 数据单元封存后不再允许普通 DML 修改 | 只有不可变单元才能安全复制、校验并在短事务中退休 |
| Sealed Partition，封存分区 | 系统明确禁止新的 INSERT/UPDATE/DELETE，并排空在途写入的分区 | 这是 MO 当前缺失、但归档 MVP 必须新增的并发隔离状态 |
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
Archive   = 把低频但仍需保留的数据放入独立长期数据集
Backup    = 为系统故障准备另一份恢复副本
Snapshot  = 某个时间点的一致视图或历史引用
PITR      = 按时间点回到历史状态
Purge     = 在所有引用、保留期和 legal hold 消失后物理删除
Restore   = 把 Archive 数据重新变成可查询数据，通常是异步作业
Tiering   = 按成本/性能/恢复能力改变数据放置，不等价于删除
```

### 18.8 COOL/COLD 到底是什么：一个客户 A 的典型例子

先强调：**COOL/COLD 不是所有云厂商都统一遵循的标准术语。** 在 Snowflake Storage Lifecycle Policies 中，它们是 Snowflake 定义的归档层名称：

- `COOL`：归档后的数据仍然比较容易取回，适合偶尔访问但不要求长期处于最高性能层的数据；Snowflake 文档给出的最短归档期是 90 天。
- `COLD`：比 COOL 更便宜，但取回速度更慢；Snowflake 文档给出的最短归档期是 180 天，恢复最长可能达到 48 小时，并限制单次恢复的文件数。

在 AWS、Azure、GCS、OSS、COS 中，同样叫“冷”或“归档”的层可能分别代表直接读取、需要 restore、不同的最短存储时长和不同的取回费用。因此 MO 不应把 SQL 中的 `COOL`/`COLD` 直接硬编码成某个云厂商 class，而应表达成访问能力：

```text
ACTIVE_DIRECT_READ       = 成本较低，但普通 SQL 仍可直接读取
OFFLINE_RESTORE_REQUIRED  = 查询前必须提交异步恢复任务
```

#### 客户 A：工业设备 SaaS 的设备日志

客户 A 为工厂管理设备，MatrixOne 中每天写入大量设备事件、告警和传感器明细。客户的实际需求不是“永远把所有数据放在最快的存储上”，而是：

1. 最近 30 天的数据用于实时运维，查询频繁，必须正常读写；
2. 30 天到 180 天的数据偶尔用于趋势分析和客服排障，仍希望可以直接查，但可以接受更高延迟；
3. 180 天到 2 年的数据很少访问，但合同和安全审计要求保留；
4. 2 年以后通常可以删除，但正在调查的设备或法务 hold 不能删除。

可以配置成如下逻辑（具体云 class 由管理员的 storage profile 决定）：

| 数据年龄 | MO 逻辑状态 | 客户体验 | 背后动作 |
|---|---|---|---|
| 0–30 天 | `HOT` | 正常查询、UPDATE、告警分析 | 活动表和高性能缓存 |
| 30–180 天 | `ONLINE_COLD` / 类 COOL | 普通查询仍能执行，但延迟和成本可能更高 | 转到支持 DirectRead 的低成本层，仍由 MO Manifest 管理 |
| 180 天–2 年 | `ARCHIVED` / 类 COLD | 当前表不可直接查；提交带时间和设备条件的 Restore Job，恢复到新表 | 复制、校验后从活动表逻辑退休，归档 payload 异步保存 |
| 2 年以后 | `PURGE_CANDIDATE` | 一般不可见 | 检查 PITR、Snapshot、Branch、Backup 和 legal hold 后物理清除 |

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

恢复完成后，客户把 `device_events_202502` 与当前设备表、工单表关联，完成故障定位。调查结束后，如果没有 legal hold，新表和归档引用可以按策略清理。

#### 客户 A 得到的实际好处

- **成本下降**：两年前的绝大多数数据不再长期占用高成本在线层；同时可以看到仍被 PITR、Snapshot 或 Branch pin 住的源对象，避免误判节省金额。
- **在线性能更稳定**：当前运维查询主要面对近 30 天活动数据，历史长尾不会持续扩大 hot cache、merge 和统计开销。
- **合规更容易解释**：可以回答“当前表保留多久、归档保留多久、何时允许 purge、什么原因阻止删除”，而不是只说对象存储已经变冷。
- **恢复范围可控**：按设备、时间和其他归档元数据恢复，先估算文件数、字节数和费用，避免一次性恢复数十 TB。
- **不会把日常查询绑在云厂商差异上**：对客户仍然是 `HOT`、`ONLINE_COLD`、`ARCHIVED` 这些稳定语义，AWS 的数小时恢复和 GCS 的直接读取由管理员 profile 屏蔽。

这个例子也说明了为什么第一版应优先支持“时间 range 分区 + 追加或少更新数据 + 新表恢复”。如果客户 A 的设备事件表每天都在随机 UPDATE 过去两年的记录，或者要求对两年归档数据继续毫秒级 UPDATE，那么这项能力就不适合直接启用，应该先做数据分区、冷热数据拆表或保留在在线层。

## 19. 最终建议

建议将 #24853 的目标重新表述为：

> 为 MatrixOne 增加独立于 Snapshot/PITR 的数据生命周期策略。策略首先支持按时间对齐的封存 range 分区进行逻辑过期和 MO-managed archive；归档数据由热端 Manifest 管理，通过异步、可估算、带谓词的新表 Restore Job 恢复。在线冷热放置使用跨云访问能力契约，不直接暴露或依赖云厂商 Bucket Lifecycle 作为事实源。

推荐优先级：

1. 先交付 Lifecycle Core、时间 range 分区 sealed state 和严格子集的 Native TTL；
2. 再交付 MO-managed archive 和新表 restore；
3. 再接入 restore-required 云归档；
4. 最后做活动对象 ONLINE_COLD 和自动热度优化。

不建议把 Issue 原语法直接实现，因为它会在 API 层固化错误的三个平面混合，并承诺当前存储引擎尚无法可靠实现的透明语义。

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
