# MatrixOne TAE 对象级数据生命周期概要设计

> 关联 Issue：[matrixorigin/matrixone#24552](https://github.com/matrixorigin/matrixone/issues/24552)、[matrixorigin/matrixone#24853](https://github.com/matrixorigin/matrixone/issues/24853)
>
> 状态：首个受限 Commercial GA 的唯一概要设计；不表示当前代码已经具备该能力
>
> 评审结论：**Conditional Go**。第22节十项P0、1/10 TiB认证、故障矩阵和
> 分阶段放量门禁全部通过后，才允许发布Commercial GA
>
> 实现级详细设计：[TAE Object Lifecycle 详细设计索引](tae-object-lifecycle/README.md)

## 1. 结论

首个 Commercial GA 采用以下方案：

```text
只扫描显式绑定表
  -> 当前 TAE Metadata + 分页 Discovery 发现候选
  -> 每个 child 选择有界 source Object set
  -> 按路径使用高层 exact-object Reader 或 Lifecycle Rewrite host
  -> Whole、小 Mixed DELETE 或 Mixed Rewrite
  -> Archive 写 Parquet/ZSTD 并全量重读校验
  -> 在正常 MO 事务中原子发布 Dataset 并退休活动数据
  -> Restore 时恢复到独立新表
```

活动数据退休分为三条路径：

1. **Whole Object**
   - Object 中所有物理行都能由可信 ZoneMap 证明已经到期；
   - TTL 不读取 payload，最终事务执行严格 Object 退休；
   - Archive 使用高层 Reader 导出 Snapshot 逻辑可见行；
   - 最终短事务使用tagged `LifecycleCommitEntry`再校验exact Object和Tombstone delta；
   - 只提交正常 Object DropIntent，不直接删除 TAE 文件；
   - 原文件继续由现有 TAE GC 异步物理回收。

2. **少量 Mixed Object**
   - 使用普通、可写的 SI 内部事务固定 Snapshot；
   - 高层 Reader 读取到期行、`RowID` 和 PK；
   - Archive 在同一事务生命周期内完成 PUT 和全量重读校验；
   - 使用现有 `Relation.Delete(RowID + delete key)`；
   - Dataset 元数据和 DELETE 在同一正常事务中提交；
   - 复用现有 MVCC、Tombstone、RowID transfer、WAL、1PC/2PC 和 commit-unknown 恢复。

3. **中/大量 Mixed Object**
   - 不执行大规模普通 DELETE；
   - 独立 Lifecycle Rewrite Executor 单次读取双输出；
   - 到期行写 Archive（TTL 则丢弃），存活行写新的正常 TAE Object；
   - 最终短事务复用现有 Merge 的 create/drop/transfer/WAL/GC 闭环；
   - 超出 source/live/transfer/staging 预算才进入 `RESOURCE_BLOCKED` 或
     `MIXED_LAYOUT_BLOCKED`，且源数据保持可见。

这个方案的核心取舍是：

- 优先交付稳定、可恢复、可运维的正式 GA；
- 不把 Provider I/O 和 Lifecycle Policy 塞进普通 Merge；
- 不修改普通 Merge 的候选、物理排序、transfer 格式和 GC 策略；
- 只为实际执行中的 exact source set 增加短期 reservation admission；
- 不依赖 SQL Partition；
- 不在普通 SELECT 中增加隐藏 TTL 条件；
- Reader 负责执行已选中的有界任务，不负责每天扫描全表发现到期数据；
- 对不能安全、低成本处理的数据布局明确 fail closed。

## 2. Commercial GA 产品范围

### 2.1 支持矩阵

| 能力 | 首个 Commercial GA |
|---|---|
| Policy scope | 仅显式表级 Binding |
| 生命周期列 | `NOT NULL DATE/DATETIME/TIMESTAMP` |
| TTL Whole Object | 支持 |
| TTL 少量 Mixed Object | 支持，普通 RowID DELETE，受硬预算限制 |
| Archive Whole Object | 支持 |
| Archive 少量 Mixed Object | 支持，普通 RowID DELETE，受硬预算限制 |
| 无显式 Primary Key 的普通表 | 支持，复用 MO 持久化 fake PK 作为 DELETE key |
| 中/大量 Mixed Object | 支持独立 Lifecycle Rewrite，受硬预算限制 |
| Archive 格式 | Parquet + ZSTD |
| Archive 可访问性 | Provider 上直接可读，但不透明参与普通 SQL 查询 |
| Restore | 必须支持；恢复到独立新表 |
| DROP TABLE/ACCOUNT | 放弃 Restore，后台异步清理 Archive |
| 普通 Merge | 候选/排序/GC不变；跳过正在执行的exact reservation |
| SQL Partition | 不依赖 |
| ONLINE_COLD | 不实现 |
| Restore-required Deep Archive | 不实现 |
| Legal Hold/WORM/maximum retention | 不实现 |
| Account/Database Policy 继承 | 不实现 |
| Archive-aware Backup/DR | 不实现 |
| 隐藏二级/唯一索引表 | 不支持绑定 |
| CDC/FK/Publication/Fulltext/Vector/插件 | 不支持绑定 |

首个 GA 是“能力边界受限，但边界内具备生产质量”，不是 Preview 改名。支持范围内必须完成故障恢复、升级、容量、清理和 Restore；不能用“后续优化”解释数据不一致。

### 2.2 Policy 模式

一张表的一个 Binding 只选择一种活动数据退出方式：

```text
TTL:
  lifecycle_column
  expire_after

ARCHIVE:
  lifecycle_column
  archive_after
  archive_retention
  archive_profile_version
```

首个 GA 不允许同一 Binding 同时配置多级 HOT/COLD/ARCHIVE。原因是 MO 活动数据已经位于对象存储，增加透明 ONLINE_COLD 对当前客户的收益不足以覆盖查询路由、缓存、费用和兼容性复杂度。

Archive Policy 的含义是：

```text
到达 archive_after
  -> 数据具备归档资格
  -> 成功归档并退休活动副本
  -> 按生命周期时间从归档资格点计算 archive_retention
  -> 到达 purge_eligible_at 后具备异步删除资格
  -> 异步 Purge
```

`archive_retention` 是最短保留语义，不是“到期必须删除”的 maximum retention。DROP owner 可以按产品契约提前放弃 Archive Restore；因此首个 GA 不能宣传为 Legal Hold、WORM 或七年不可删除的合规归档。

Dataset 的删除资格按以下公式冻结：

```text
purge_eligible_at =
  max(
    max_lifecycle_value + archive_after + archive_retention,
    publish_commit_ts + minimum_publish_grace
  )
```

这表示 `archive_retention` 是从数据具备归档资格到删除资格之间的最短逻辑窗口，
不是从 Job 实际 publish 时重新起算。`minimum_publish_grace` 保证严重积压时刚发布的
Dataset 也不会立即删除；后台积压只会把实际删除推迟，不会提前删除。

### 2.3 时间语义

每个 child Job 固定：

```text
evaluation_time_utc
cutoff = evaluation_time_utc - expire_after/archive_after
expired = lifecycle_column < cutoff
```

约束如下：

- 等于 cutoff 的行不算到期；
- `evaluation_time_utc` 在 child 创建时冻结，retry 不原地改变；
- `expire_at` 表示开始具备处理资格，不承诺到点瞬间不可见；
- 只有最终事务提交后，新 SELECT 才不再看到这些行；
- Job 失败、冲突、超限或 Provider 不可用时，源数据继续可见；
- 迟到写入且时间已经到期的数据由后续 child 处理；
- 首个 GA 不支持 NULL 生命周期列和 calendar month/year 语义。

### 2.4 可交付效果与成本边界

MO 的活动 TAE Object 本来就在 S3/OSS/COS 一类对象存储上，所以 Archive 的价值
不能简单宣传成“从昂贵磁盘搬到便宜对象存储”：

- **TTL** 删除到期数据，不保留第二份 Payload，原始存储节省最直接；
- **Archive** 用 Parquet/ZSTD 替代活动 TAE 副本，主要移除活动表中的历史扫描、
  Tombstone/Merge/GC、Catalog/索引和 Cache 干扰，并提供独立 Restore 边界；
- 如果 Archive Profile 指向更低单价、仍支持直接 GET 的存储类型，还能获得介质
  价差；Provider 没有分层时，这部分收益为零；
- TAE 与 Parquet 都是压缩列式格式，不能承诺“归档后压缩率必然显著提高”；压缩率
  只按真实数据 benchmark 计算；
- Archive 还会产生 PUT、全量 readback、GET、请求数、最低保存期和流量费用，Dry-run
  必须展示净成本，而不是只展示归档文件大小。

因此 GA 的效果口径是“降低活动数据面负担并提供可恢复的离线历史”，不是保证每个
Provider 都降低 raw object bytes。只有客户的实际 Profile 价格、压缩结果、访问频率
和 Restore 成本模型显示净收益时，才推荐启用 Archive。

### 2.5 两个 Issue 的覆盖关系

| Issue 诉求 | 本 GA 的覆盖 |
|---|---|
| #24552 按时间自动清理历史数据 | 提供 Whole、小 Mixed DELETE 和中/大 Mixed Rewrite；只有资源或重复放大超限才 blocked |
| #24853 Storage Lifecycle Policy | 提供表级 Archive、独立 Profile、Purge 和 Restore 新表 |
| #24853 多级 HOT/COOL/COLD | 不做透明多级在线路由；MO Active 数据本来就在对象存储 |
| #24853 Deep Archive | 不进入本 GA；需要 Provider restore job 的存储类型另行设计 |

因此这是两个 Issue 的可商用安全子集，不应在产品文案中扩张为已经完整兼容
Snowflake Storage Lifecycle Policies。

## 3. 典型业务场景

最适合的场景是时间基本有序、以 INSERT 为主的大事实表：

- 日志、Trace、Metric；
- IoT 设备事件；
- 订单事件流水；
- 审计事件；
- 交易明细历史。

例如客户每天写入 1 TiB 日志，按 `event_time` 基本有序：

```text
最近 30 天：保留在活动表
30 天以前：归档为 Parquet/ZSTD
归档保留 2 年
调查历史事件时：RESTORE 到独立新表
```

如果 Object 按时间形成，cutoff 通常只穿过一个或少量边界 Object：

```text
旧 Object 旧 Object 旧 Object | 边界 Object | 新 Object 新 Object
            Whole            |   Mixed     |     未到期
```

这时绝大部分数据走 Whole Object 快速退休，边界尾部走少量 DELETE 或 Rewrite。

成本较高、且不承诺严格 Archive Lag SLO 的布局是：

- 生命周期列高度乱序、每个 Object 都同时包含新旧时间；
- 持续高频 UPDATE/DELETE；
- 大量超宽 PK；
- 依赖隐藏索引、CDC、FK 或 Publication；
- 要求 Archive 透明在线查询；
- 要求不可删除的合规留存。

这些表仍可由 Rewrite 安全处理，但必须经过 Dry-run、rewrite amplification 和
resource budget 评估；超过已认证边界时 fail closed。

## 4. 术语

| 术语 | 定义 |
|---|---|
| Active data | 普通 SELECT 可见、由正常 TAE Object/Tombstone/MVCC 管理的数据 |
| Source Object | 本 child 读取并准备退休的当前 TAE Data Object |
| Whole Object | 可信 Metadata 能证明该 Object 所有物理行都已到期 |
| Mixed Object | Object 同时包含到期和未到期物理行，或 Metadata 无法证明 Whole |
| Exact-object Reader | 只读取冻结 source Object set、应用指定 Snapshot Tombstone 的高层 Reader |
| Lifecycle Rewrite | 到期行写 Archive/丢弃、存活行写新 TAE Object，再原子替换 source Object |
| Source Reservation | TN 内存中的 exact Object 短租约，负责普通 Merge/Lifecycle 准入线性化；仍不替代 final exact CAS |
| Source Protection | 复用 GC SyncProtection 保护source Data/Tombstone，并在Rewrite时预保护未来live/booking staging文件 |
| Dataset | 一次原子发布的 Archive 逻辑单元 |
| Payload | Dataset 引用的不可变 Parquet 文件 |
| Manifest | Dataset 的 schema、行数、范围、Payload identity、checksum 和 root |
| Attempt Control | system-owned 的 Job/事务收敛记录 |
| Cleanup Root | 第一次 Archive PUT、TAE live staging 或 transfer booking 前创建的 system-owned 外部对象所有权记录 |
| Conditional Lifecycle Retire | 通过同一commit payload中的tagged `LifecycleCommitEntry`提交；Object、generation、Guard、reservation/protection或Tombstone条件不匹配时必须整体abort |
| `MIXED_LAYOUT_BLOCKED` | 数据布局导致持续 Rewrite 放大超过 release profile，系统停止退休但保留源数据 |
| `RESOURCE_BLOCKED` | source/live/transfer/staging 或 Provider 资源超过硬限额 |
| `CONFLICT_BLOCKED` | 同一 source set 持续与 Merge/DML 冲突，当前 generation 停止重试并等待重新规划 |
| `COMMIT_UNKNOWN` | 最终事务结果尚未确定；不得释放所有权或清理可能已发布的数据 |
| `MANUAL_RECONCILE_REQUIRED` | 自动对账超过运维时限后的告警态；不代表事务已失败，也不转移资源所有权 |

Archive 不是 Snapshot：

- Snapshot 表示同一 TAE 数据在历史时间点的可见性；
- Archive 是独立 Parquet Dataset；
- Manifest 不永久 pin 源 TAE Object；
- 普通 Snapshot/PITR 不自动保护 Archive Payload；
- 首个 GA 对 Snapshot/PITR/Backup/Clone/Branch 与 Lifecycle Binding 使用互斥准入。

## 5. 为什么不依赖 SQL Partition

SQL Partition 是逻辑组织和优化能力，不是 TAE 物理文件边界。

即使表定义了 Partition：

- 底层仍由 ObjectIO Object、Block 和 Tombstone 管理；
- 普通 Merge 仍可能改变 Object 集合；
- 删除逻辑 Partition 不等于直接删除一组独立存储目录；
- 不能把对象存储 prefix 当作事务一致性边界。

Lifecycle 因而直接基于 TAE Object 和逻辑可见行实现：

```text
Policy/Binding
  -> Object Metadata
  -> exact source Object set
  -> Reader/DELETE/Lifecycle Rewrite/LifecycleCommitEntry
  -> existing TAE GC
```

SQL Partition 可以提高时间局部性，但不是正确性前提，也不是 Lifecycle Catalog 的所有权单位。

## 6. 可复用的 MO 能力

### 6.1 Reader

[`pkg/vm/engine/types.go`](../../pkg/vm/engine/types.go) 中：

- `Relation` 与打开它的事务绑定；
- `Ranges` 返回 `RelData`；
- `BuildReaders` 接受调用方指定的 `RelData`；
- `Reader.Read` 以 Batch 流式输出。

[`pkg/vm/engine/disttae/txn_table.go`](../../pkg/vm/engine/disttae/txn_table.go) 已能：

- 从 `PartitionState` 枚举 Snapshot 可见 Object；
- 使用 `ObjListRelData` 表示指定 Object/Block；
- 让 `BuildReaders` 按调用方提供的范围构造 Reader。

[`pkg/vm/engine/readutil/reader.go`](../../pkg/vm/engine/readutil/reader.go) 已支持：

- 应用 Snapshot 可见 Tombstone；
- 输出逻辑可见行；
- 请求 `__mo_rowid`；
- 使用现有 RemoteDataSource/ObjectIO/Cache 路径。

Lifecycle 不自建第二套 ObjectIO Scanner。新增的 `ScanExactObjectSnapshot` 只是把以下输入固定下来：

```text
source_snapshot_ts
exact persisted RelData
table schema/version
TombstoneData
requested columns
```

它必须排除表级 in-memory rows，只输出指定持久化 source Object set 的逻辑可见行。

### 6.2 普通 DELETE

现有 `Relation.Delete` 接收包含 `RowID + delete key` 的 Batch，并走普通事务提交。
`delete key` 是普通 PK、复合 PK，或无显式 PK 表的持久化 fake PK。Lifecycle 必须
从同一 Relation/TableDef 解析实际 delete key，不能自行构造或只凭用户 schema
猜测。TAE 已具备：

- Tombstone RowID 去重；
- 并发 DELETE/UPDATE 冲突检测；
- Merge 后 RowID transfer；
- transfer page 不存在时返回冲突；
- Tombstone Object、WAL、Logtail、Merge/Vacuum 和 GC。

小 Mixed 路径只调用这一能力，不自行写 Tombstone 文件，不自行重放 DELETE。
fake PK、复合/varlen PK 都计入实际 encoded bytes 预算；Reader 不能可靠输出实际
delete key 的表必须在 Binding 准入时 fail closed。

### 6.3 Object Soft Delete 与 GC

现有 `SoftDeleteObject` 会在事务中为 Object 写 DeleteAt，之后由 TAE GC 异步删除文件。

当前 `HandleSoftDeleteObject` 对 `OkExpectedEOB` 返回成功，这种宽松幂等语义不能用于 Lifecycle。Lifecycle 必须新增独立的严格变体，且不能改变普通 Merge 已有调用者的行为。

Lifecycle 永远不直接调用 FileService 删除源 TAE Object：

```text
final transaction
  -> SoftDeleteObject
  -> checkpoint/logtail
  -> existing TAE GC
  -> provider physical delete
```

### 6.4 正常事务与 commit-unknown 恢复

Dataset/Receipt Catalog写入、普通Row DELETE和tagged `LifecycleCommitEntry`必须放在同一个
正常MO分布式事务中：

- 兼容时可走 1PC；
- 跨 participant 时走 2PC；
- 复用现有事务状态查询和 unknown-commit resolver；
- 不自建 participant apply watermark；
- 不使用 executor 内存状态判断提交结果。

### 6.5 现有 Merge create/drop/transfer

现有 `DoMergeAndWrite`、`HandleMergeEntryInTxn` 和 `mergeObjectsEntry` 已提供：

- Snapshot visible rows读取和普通Object Writer；
- source row到destination row的transfer map；
- source Object DropIntent和new live Object Catalog create；
- Prepare前两阶段Tombstone transfer；
- rollback staging清理、WAL/replay和GC衔接。

Lifecycle Rewrite以独立host把到期行加入deletes bitmap，使其在transfer map中保持
`api.NoTransfer`；存活行继续由现有writer/transfer闭环处理。普通Merge算法和worker
不执行Archive Provider I/O。

## 7. 不进入首个 GA 的实现

首个 GA 明确不实现：

- 修改普通 Merge 策略；
- 长生命周期 table-only Lifecycle Snapshot；
- 每Object持久化Catalog Index；
- 普通 SELECT 隐藏 TTL 过滤；
- Archive Delete Vector；
- ONLINE_COLD；
- Restore-required Deep Archive。

这些能力不是“代码写完前临时关闭”，而是不属于本 GA support matrix。

## 8. 总体架构

```text
                         +----------------------+
SQL DDL ---------------->| Policy / Binding     |
                         | Feature Guard        |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
PartitionState/Metadata
------------------------>| Paged Discovery      |
                         | cursor + candidates  |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
                         | Planner / Dry-run    |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
                         | Scheduler / Quota    |
                         +----------+-----------+
                                    |
                                    v
                     +--------------+--------------+
                     | Lifecycle Child Executor    |
                     +--------------+--------------+
                                    |
              +---------------------+---------------------+
              |                     |                     |
              v                     v                     v
        +----------------------+          +----------------------+
        | Whole Object         |          | Small Mixed Object   |
        | exact Reader/metadata|          | writable SI Reader   |
        +----------+-----------+          +----------+-----------+
              |                     |                     |
              |                     |                     v
              |                     |          +----------------------+
              |                     |          | Relation.Delete      |
              |                     |          | RowID + delete key    |
              |                     |          +----------+-----------+
              |                     |
              |                     v
              |          +----------------------+
              |          | Mixed Rewrite        |
              |          | expired -> Archive   |
              |          | live -> TAE Objects  |
              |          +----------+-----------+
              |                     |
              v                     v
        +----------------------+          +----------------------+
        | ArchiveStore         |          | Merge create/drop/    |
        | Parquet/ZSTD/Verify  |          | survivor transfer     |
        +----------+-----------+          +----------+-----------+
                   \                                 /
                    \                               /
                     v                             v
                     +-----------------------------+
                     | normal final transaction    |
                     | Dataset/Receipt + retirement|
                     +--------------+--------------+
                                    |
                     +--------------+--------------+
                     |                             |
                     v                             v
             existing TAE GC               Restore Service
                                           hidden staging table
```

### 8.1 组件职责

| 组件 | 职责 |
|---|---|
| Policy/Binding | 保存表级动作、生命周期列、阈值和 Profile version |
| Feature Guard | 关闭 DDL/依赖创建与 Binding/final commit 的首次创建竞态 |
| Object Discovery | 分页读取当前TAE Metadata，只持久化每表cursor和有界Candidate |
| Planner | 计算 cutoff、Whole/Mixed 候选、alignment ratio 和成本 |
| Scheduler | 只调度 Binding Registry，执行公平性和硬预算 |
| Exact Reader | 按固定 Snapshot 和 exact RelData 输出逻辑可见 Batch |
| Archive Writer | 写 Parquet/ZSTD、checksum、Manifest 和 sidecar |
| Rewrite Executor | 单次读取分流Archive行和live TAE Object，复用mergesort |
| Reservation/Protection | 协调exact Object普通Merge并保护source Data/Tombstone文件 |
| Finalizer | 执行短事务、条件校验、Dataset publish和活动数据退休 |
| Attempt/Cleanup Registry | 管理 Job、事务和外部对象的 system-owned 所有权 |
| Sweeper | 收敛 staging、DROP cascade、Purge 和迟到 PUT |
| Restore Service | 校验 Dataset，写隐藏 staging table，原子发布新表 |

Job Executor运行在CN TaskService worker。TN增加reservation、source protection校验
和tagged `LifecycleCommitEntry` handler；TN不运行跨云复制或Lifecycle Planner，普通
Merge worker不执行Lifecycle任务。

## 9. Catalog 与身份模型

### 9.1 Policy 和 Binding

Binding 至少冻结：

```text
account_incarnation
logical_database_id
logical_table_id
physical_database_id
physical_table_id
physical_table_generation
schema_version
schema_digest
lifecycle_column_id/type
action_mode
duration
archive_retention
archive_profile_id/version
guard_version/digest
binding_version
active_child_generation/attempt_id/executor_epoch
state
```

长期 Archive owner 使用 logical table identity；TAE retirement 使用当前 physical table identity。`ALTER TABLE ... COPY` 可能更换物理表，因此 final transaction 必须同时验证 logical owner 和 physical generation。

### 9.2 Feature Guard

每张发生 Lifecycle 或相关依赖控制面操作的表有一行权威 Guard：

```text
table_id
version
schema_generation
binding_generation
dependency_bits
owner_state
digest
```

以下操作必须懒创建或 CAS 同一唯一 Guard 行：

- Lifecycle bind/unbind/change；
- ALTER、TRUNCATE、DROP；
- CREATE/DROP INDEX；
- 创建/删除 CDC、FK、Publication、Fulltext、Vector 和插件依赖；
- Backup/PITR/Snapshot/Clone/Branch 准入。

双方第一次操作不能先检查“Guard 不存在”后跳过写入。双方都尝试插入相同唯一键，以唯一键冲突关闭首次创建竞态。

Guard 不进入普通 INSERT、UPDATE、DELETE、SELECT 和 Merge 热路径。

### 9.3 Archive Profile

Profile 是不可变、版本化的存储身份：

```text
profile_id
profile_version
storage_namespace_id
provider_type
endpoint_identity
bucket/container
immutable_prefix
capabilities
encryption/KMS identity digest
credential_generation
```

Dataset 和 Cleanup Root 必须冻结：

```text
profile_id
profile_version
storage_namespace_id
manifest key/version
payload key/version list
```

规则：

- endpoint/bucket/prefix 不能对已发布版本原地修改；
- Credential 可以轮换，但不能改变 namespace identity；
- KMS key identity 变化必须创建新 Profile version，不能让历史 Dataset 静默改用新 key；
- 存在 Dataset、Root 或未完成清理时禁止删除 Profile metadata；
- 已被 Root/Dataset 引用的 Profile version 和 cleanup credential handle 保存在
  system-retained registry，不随 DROP ACCOUNT 删除；
- owner DROP 后 credential 只保留 Restore 已被 fence 后所需的 list/delete 权限，
  直到所有 Root 清理完成；secret 本身由现有密钥系统托管，不复制进 Manifest；
- provider 不支持 version ID 时，必须使用全局唯一、永不复用的 immutable key；
- 缺少稳定 PUT/GET/HEAD/LIST/DELETE 和 checksum 能力的 Profile 不能退休源数据。

如果客户在 Provider 侧撤销 credential、删除 bucket 或修改外部策略，MO 只能让
Cleanup 进入 `DELETE_FAILED`，或让 Restore 返回 `LIFECYCLE_ARCHIVE_UNAVAILABLE`
并记录健康告警；Dataset 本身仍保持 `PUBLISHED`，不能声称已经清理或可 Restore。GA
合同必须把外部 namespace/credential 的可用性责任写清。

### 9.4 Dataset 和 Manifest

Dataset 至少保存：

```text
dataset_id
account_incarnation
logical_table_id
source_schema_version/digest
source_snapshot_ts
evaluation_time_utc/cutoff
source_object_ids/digest
row_count
uncompressed_bytes
payload_count/bytes
manifest_key/version/root
archive_profile_identity
min/max lifecycle value
purge_eligible_at
state/version
publish_transaction_id/commit_ts
```

Manifest 是不可变内容，包含：

- 完整 MO schema 和 column ID；
- Parquet field mapping；
- Payload 精确 key/version/size/checksum；
- 每个文件和 row group 的行数、min/max/null count；
- Dataset content root；
- KMS/key generation；
- source snapshot、cutoff 和 source Object digest。

Catalog Dataset 行是可见性真相；Manifest sidecar 是长期校验和 DROP 后清理依据，不能替代 Catalog 做正常 Restore 路由。

## 10. Object Discovery 与候选发现

### 10.1 不创建每 Object Catalog Index

当前TAE `PartitionState`/Relation Metadata是Snapshot下当前Object集合权威；
`CollectObjectList`提供Object create/delete变化；GC metadata服务已删除Object和
回收水位，不是当前Active Object目录。

Lifecycle只持久化：

```text
每Binding一行scan cursor/watermark
有界近期Candidate/Child Job
```

不为每个Object创建Catalog行，不在Logtail replay callback同步写Lifecycle状态。
按平均128 MiB估算，1000张1 TiB表约820万Object，1000张10 TiB表约8190万
Object；逐Object Catalog Index会带来不可接受的checkpoint/replay/stale backlog。

### 10.2 分页扫描

新增`ScanLifecycleObjectMetadataPage`：

- 复用PartitionState按Object ID有序枚举；
- 每页只加载有限Object的生命周期列footer；
- 生命周期列是physical sort/cluster key时直接复用`ObjectStats.SortKeyZoneMap`；
- 持久化`after_object_id/cycle_id/state_version`；
- cursor和本页Candidate在同一事务提交；
- Merge生成的新Object若落在cursor之前，下一full cycle一定发现；
- `CollectObjectList`只做增量加速，不能替代full cycle。

可选packed Discovery Summary存于共享对象存储，每表Catalog只保存Root/version。
它只优化footer I/O；损坏或丢失时分页重建，Executor/final transaction不依赖它。

### 10.3 保守分类

GA 生命周期列必须 `NOT NULL`。Whole 判定：

```text
whole_expired =
  row_count > 0
  AND null_count == 0
  AND zonemap_initialized
  AND zonemap_version_supported
  AND zonemap_type_exact
  AND max_non_null < cutoff
```

其他情况：

```text
min_non_null >= cutoff
  -> not due

min_non_null < cutoff <= max_non_null
  -> Mixed candidate

stats missing/unknown/truncated/inconsistent
  -> Mixed candidate, Reader 验证
```

Discovery只能形成Candidate。Executor必须在执行Snapshot重新取得exact Object
identity和Metadata；final transaction再做条件校验。

## 11. Job、批次与调度

### 11.1 一次扫描与 child

```text
one policy scan
  -> one coordinator batch
  -> one child per bounded source set
  -> one Dataset/Receipt
  -> one atomic final transaction
```

首个 GA：

- 小Mixed DELETE child只允许一个source Object；
- Rewrite child默认最多16个source Object，并受source/live/transfer/staging上限；
- Whole child可以包含多个Object，但同时受object count、source bytes、payload bytes和final-entry bytes限制；
- 一个 child 失败不回滚其他 child；
- retry 不改变原 child 的 `evaluation_time_utc`；
- source set 变化时终结旧 child并创建新 generation。

### 11.2 初始并发

初始 release profile：

| Scope | Active child 上限 |
|---|---:|
| 同一表 | 1 |
| 同一数据库 | 2 |
| 同一账户 | 4 |
| 集群 | 8 |

Scheduler 必须同时限制：

- due/running/retry Job 数量和 bytes；
- Discovery cursor/Candidate/Packed Summary backlog；
- Archive upload/readback 并发；
- active Rewrite/reservation/source protection和transfer bytes；
- active SI Mixed transaction 数量；
- active Reader/SI snapshot retained bytes 和 GC lag；
- staging/published/external object 数量；
- Tombstone rolling bytes/rows；
- Merge/Tombstone backlog；
- Cleanup、Restore 和 Purge 的最低资源份额。

### 11.3 状态

```text
PLANNED
  -> REGISTERED
  -> READING
  -> UPLOADING       # Archive only
  -> VERIFIED
  -> REWRITING       # medium/large Mixed only
  -> FINALIZING
  -> COMMITTED

failure before final txn:
  -> ABORTED / RETRYABLE
  -> MIXED_LAYOUT_BLOCKED / RESOURCE_BLOCKED / CONFLICT_BLOCKED

final txn result unknown:
  -> COMMIT_UNKNOWN

automatic reconciliation exceeds operational age:
  -> MANUAL_RECONCILE_REQUIRED
  -> ownership and transaction identity remain unchanged
```

每个等待状态必须有：

- owner；
- deadline；
- lease/epoch；
- 重试上限；
- 终态；
- 增长上限。

超时是停止、fence、告警和 reconciliation 触发器，不是猜测事务失败的证据。

每次事务状态查询和 reconciliation 都使用独立 deadline 与有界退避，不能让一个
`COMMIT_UNKNOWN` 占住 worker、锁或执行槽。超过自动对账时限只进入
`MANUAL_RECONCILE_REQUIRED` 并暂停该 scope 的新 retirement；它仍保留 Root、
source snapshot timestamp、事务身份和 staging，直到事务服务给出权威结果或完成
经过审计的人工处置。

`MIXED_LAYOUT_BLOCKED`、`RESOURCE_BLOCKED` 和 `CONFLICT_BLOCKED` 是当前 child
generation 的终态：

- 旧 child 不在原 generation 上无限重试；
- 先清理未发布 staging，并明确回滚/收敛事务；
- 用户改善布局、增加 grace period、修改 Binding，或运维显式 re-evaluate 后，Planner 创建新的 child generation；
- 旧 executor epoch 不能提交到新 generation；
- blocked summary 有数量、bytes、oldest age 和保留上限。

## 12. Reader 契约

### 12.1 Exact-object Reader

新增窄接口：

```text
ScanExactObjectSnapshot(
    relation,
    source_snapshot_ts,
    exact_object_stats[],
    columns[],
    filter,
    on_batch,
) -> ScanReport, error
```

实现要求：

- 复用 `ObjListRelData`、RemoteDataSource、SimpleReader 和 TombstoneData；
- exact RelData 不包含 table-level in-memory row；
- 只读取 source set；
- Snapshot Tombstone 必须应用到输出；
- 支持请求 `__mo_rowid`、PK、生命周期列和全部业务列；
- Reader 输出明确区分 Archive 业务投影与 DELETE control projection；
- `__mo_rowid` 和 fake PK 只服务退休，不写入 Archive Payload；
- 用户显式 PK/复合 PK 本来就是业务列，只在 Payload 中保留一份；
- Reader error、Object missing、checksum error 和 context cancel 一律上抛；
- `ScanReport` 返回 requested/reached Object、Block、physical rows、visible rows 和
  Tombstone coverage；
- 只有 exact Object/Block 全部 reached 的 0 visible rows 才是合法空结果；
- Object missing、短读、未消费到 EOF 或 coverage 不完整的空结果必须失败。

### 12.2 Batch 所有权

首个 GA callback 串行：

```text
Reader owns Batch/Vector
  -> callback may synchronously consume
  -> callback must copy data retained after return
  -> callback returns
  -> Reader may reuse/clean Batch
```

要求：

- callback 不异步持有借用 Vector；
- Writer backpressure 在 callback 内同步传播；
- success/error/cancel/panic 路径 Batch 只释放一次；
- Writer 在返回前完成复制或序列化；
- Reader Close、Writer Abort 和 multipart cleanup 均有唯一 Owner。

并行 Reader 只有在串行正确性、顺序无关 root、内存上限和取消测试完成后才能启用；不属于 GA 必要条件。

### 12.3 覆盖性校验

每次 Archive 记录：

```text
source_visible_rows
source_visible_content_root
archive_rows
archive_content_root
```

必须满足：

```text
archive_rows == source_visible_rows
archive_content_root == source_visible_content_root
```

root 使用稳定 schema column ID、NULL、类型和规范编码计算，不能依赖 Batch 切分、Reader 并行度或 Parquet 文件边界。
root 只覆盖 Manifest 冻结的业务列，不包含 `__mo_rowid` 或内部 fake PK。Restore 到
无显式 PK 的新表时由正常 MO writer 生成新的 fake PK，不能把源表内部 delete key
暴露为用户列。

首个 GA 的 canonical row order 固定为：

```text
source Object ID ascending
  -> Block ID ascending
  -> physical row offset ascending
  -> skip Snapshot-invisible rows
```

Writer 按此顺序分配逻辑 row ordinal，Manifest 冻结 Payload/row-group ordinal range；
readback 按 Manifest 顺序重算 cryptographic root。未来如果启用并行 Reader，必须在
hash 前恢复 canonical ordinal，或使用包含 ordinal 的 Merkle leaf；不能用简单 XOR/
SUM 代替有序、抗碰撞的内容证明。

Provider 写完后必须通过 `Open(version)` 全量重读 Payload 并重新计算 root。只比较 PUT 返回 ETag 不满足 GA。

## 13. Whole Object 路径

### 13.1 Whole TTL

Whole TTL 不读取 payload：

```text
Planner 证明 whole_expired
  -> create Attempt Control
  -> final short transaction
       CAS Guard/Binding/owner generation
       LifecycleCommitEntry(WHOLE_TTL, exact Object)
       insert Receipt
  -> commit
  -> existing TAE GC
```

最终条件：

```text
physical table/generation unchanged
exact Object ID/current state unchanged
Object has no prior DropIntent
Object Metadata still proves max < cutoff
Guard/Binding/owner version unchanged
attempt epoch is current
```

Object-not-found 不能视为成功。只有一致性读取确认相同 attempt 的 Receipt 已提交，Reconciler 才能把重复请求判定为已完成。

### 13.2 Whole Archive

导出阶段不持有表锁：

```text
create Attempt Control
  -> claim exact source Objects
  -> capture source_snapshot_ts
  -> register source Data/Tombstone protection
  -> exact Reader
  -> create Cleanup Root
  -> Parquet PUT
  -> provider full readback
  -> VERIFIED
```

正常路径在第一次 PUT 前创建 Cleanup Root。特殊情况下，`ScanReport` 证明所有 exact
Object/Block 都已完整扫描，但 Snapshot 下 `visible_rows == 0`：

- 不创建空 Parquet、Manifest 或 Dataset；
- 不进入外部 PUT，因此只需要 system-owned Attempt Control；
- final transaction写入`EMPTY_ARCHIVE` Receipt并提交同一tagged entry；
- coverage 不完整时绝不能走该分支。

最终短事务：

```text
CAS Guard/Binding/owner generation
  -> validate exact source Objects
  -> validate reservation/source protection
  -> publish Dataset/Manifest/Receipt
  -> LifecycleCommitEntry
       phase-1/phase-2 scan Tombstone in (source_snapshot_ts, prepare_ts]
       any Tombstone targets archived row -> abort
       DropIntent source Object
  -> commit
```

不能只做Object CAS：

```text
Reader 导出行 R
  -> 用户 DELETE R 提交 Tombstone
  -> Data Object identity 仍可能不变
  -> 仅做 Object CAS 会发布仍含 R 的 Archive
  -> Restore 时复活已删除行
```

新协议不依赖长/短SQL表锁。现有Merge transfer entry从`source_snapshot_ts`到
`prepare_ts`执行两阶段Tombstone扫描；Whole没有destination mapping，因此任何
Snapshot后删除都会使final transaction冲突。Prepare后仍在运行的DELETE查到
`NoTransfer`也必须冲突。INSERT不在frozen source set，不受影响。

发现任何指向已导出 Whole Object 的新 Tombstone：

```text
abort final transaction
  -> Dataset 保持未发布
  -> 清理 staging
  -> replan/re-export
  -> 超过冲突期限后 CONFLICT_BLOCKED
```

首个 GA 不用 Archive Delete Vector 修补已导出 Payload。

### 13.3 Tagged `LifecycleCommitEntry`

Whole和Rewrite共用`PrecommitWriteCmd.EntryList`中的版本化
`LifecycleCommitEntry` tag，不复用普通`OpCommitMerge`，也不在commit前发送独立
`TxnOperator.Write`。Dataset/Receipt仍走普通Catalog workspace写入；tag由
`Transaction.lifecycleCommit`这条独立、最多一条、不可变的commit-control通道持有，
不进入普通`txn.writes`，不参与workspace dump/compact/sort/GC。`genWriteReqs`先编码
普通Entry，再把tag直接追加到同一个可重放`PrecommitWriteCmd.EntryList`。生产finalizer
必须同时持有实际写入workspace的Catalog pair：Archive为Dataset+Receipt，TTL为Receipt；
pair缺失或no-op时禁止control和请求。空Entry编码只允许私有单测用于证明tag不会被CN
workspace过滤。

协议冻结：

```text
mode = WHOLE_TTL | WHOLE_ARCHIVE | MIXED_REWRITE_TTL |
       MIXED_REWRITE_ARCHIVE | EMPTY_ARCHIVE
attempt/executor/reservation/source-protection identity
logical/physical/schema/Binding/Guard generation
source snapshot/cutoff/exact ObjectStats digest
source/expired/live row conservation
Dataset/Manifest/Receipt roots
nested existing MergeCommitEntry + source_set_digest
created_layout_digest + transfer_mapping_digest
entry digest
```

TN iterator在普通Entry转换前识别tag，handler复用现有`mergeObjectsEntry`的
create/drop/两阶段transfer/WAL/replay。每个内部TAE retry generation从immutable
Booking重建私有entry；旧generation的Catalog node、TransferTable和entry指针不得
复用。每代在任何`SoftDeleteObject/CreateNonAppendableObject`之前，必须先取得
generation-local `(attempt_id, entry_digest)`唯一BUILDING slot；失败后整代事务
rollback，禁止在已部分修改的同一代中重新Build。`HandleCommit`栈上唯一的
`LifecycleReplayBudget`向全部retry generation传递同一绝对deadline、最大代数和
累计I/O/CPU/delta预算，不建立无界全局memo。Route不使用不存在的TopologyGeneration，
只冻结ServiceID/ShardID/ReplicaID/protocol version；发送前在txn锁外权威刷新并重新解析
Address，身份/capability变化或不能唯一解析目标shard即fail closed。老TN由capability gate阻止接收新tag；
capability未全集群启用时只允许Dry-run/Export-only。

## 14. 少量 Mixed Object 路径

### 14.1 为什么使用普通 DELETE

高层 Reader 已经能输出逻辑可见行和 `RowID`。少量到期行无需重写整个 Object：

```text
Reader expired rows
  -> Archive/Discard
  -> Relation.Delete(RowID + delete key)
  -> existing Tombstone/MVCC/Merge/GC
```

这避免：

- 新 live Object；
- transfer map 生成；
- mergesort adapter；
- 新 Merge commit entry；
- 修改普通 Merge；
- 在查询路径增加 TTL filter。

### 14.2 固定 Snapshot 的可写事务

不能使用 `CloneSnapshotOp` 执行 Mixed DELETE。当前 `Relation.Delete` 明确拒绝 Snapshot Operator。

首个 GA 必须创建一个普通、可写的 SI 内部事务：

```text
begin normal writable SI transaction
  -> source_snapshot_ts = txn.SnapshotTS
  -> exact-object Reader
  -> filter expired visible rows
  -> collect RowID + delete key
  -> TTL: build DELETE
  -> Archive: PUT + full readback verify
  -> Relation.Delete in bounded batches
  -> insert Dataset/Receipt or TTL Receipt
  -> commit same transaction
```

禁止：

- 使用默认悲观 RC 执行多条独立语句；
- 先用 Snapshot Operator SELECT，再用另一个当前事务 DELETE；
- Reader 和 DELETE 使用不同 source snapshot；
- Archive Dataset 先发布、DELETE 后提交；
- DELETE 成功后异步补写 Dataset。

外部 PUT/readback 必须全部发生在第一次 `Relation.Delete` 之前。开始写 DELETE
workspace 后禁止任何 ArchiveStore I/O；剩余步骤只能是有界 DELETE batch、正常
MO 存储/Catalog 写和 commit。这样 SI transaction 虽然存活时间较长，但不会持有
行写锁等待跨云归档请求。

### 14.3 长事务风险与硬边界

Mixed Archive 的可写 SI 事务跨越外部 I/O，只能服务小尾部。

每个 Mixed child 必须同时满足有限配置：

```text
source_object_count
source_compressed_bytes
expired_rows
actual RowID/delete-key encoded bytes
affected_blocks
archive_payload_bytes
transaction_wall_time
provider single-I/O deadline
workspace/WAL estimate
```

任一预测或运行时指标超过 release profile：

```text
abort transaction
  -> no active row retired
  -> no Dataset published
  -> cleanup staging
  -> replan as Lifecycle Rewrite
```

所有这些值必须是有限 hard limit；默认无限值非法。认证报告必须冻结实际 release
profile。单个 source Object 超过小 Mixed 上限时必须流式切换到 Lifecycle Rewrite，
不能因为一个当前合法 Object 大于普通 child target 而永久 blocked。只有 Rewrite
估算本身超过 spill、staging、transfer 或 Provider 的硬预算时，才进入
`RESOURCE_BLOCKED`；Whole Object 仍可以流式 Archive。

事务 context 必须由 Job deadline 派生。不能依赖 TN 默认 zombie timeout 作为资源控制。

### 14.4 并发 UPDATE/DELETE

Archive 和 DELETE 基于同一 SI Snapshot：

- 用户先提交相同 RowID Tombstone：Lifecycle Tombstone 去重/冲突失败，Dataset 不发布；
- Lifecycle 先提交：用户后续操作按普通 MO 可见性和冲突语义执行；
- UPDATE 的 delete-old/insert-new 与 Lifecycle DELETE 冲突时，Lifecycle fail closed；
- Merge 抢先替换 Object：现有 RowID transfer 成功则继续，transfer 缺失或链超限则事务 abort；
- 持续冲突超过期限后进入 `CONFLICT_BLOCKED`，不承诺 Archive Lag SLO。

这些行为必须通过真实 TAE 并发测试证明，不能只依赖接口名称推断。

### 14.5 中/大 Mixed Rewrite

超过普通DELETE预算的Mixed不再永久blocked：

```text
Acquire exact Object reservation
  -> register exact Data/Tombstone source protection
  -> Snapshot Operator at S streaming read
       snapshot-deleted -> neither output
       expired          -> Parquet/ZSTD（TTL则discard）
       live             -> normal TAE Object writer
  -> full Archive readback + row/transfer conservation
  -> short final transaction:
       Dataset/Receipt
       LifecycleCommitEntry
       source DropIntent
       create live Objects
       transfer post-S live-row deletes
```

现有`DoMergeAndWrite`接收`Snapshot Tombstone bitmap UNION expired bitmap`：

- expired row不写新Object，mapping为`api.NoTransfer`；
- live row写新Object并生成正常destination mapping；
- post-S delete命中live row时transfer；
- post-S delete命中expired/archived row时整个final transaction abort并re-export。

普通Merge不执行Provider I/O。只在exact source reservation期间跳过/拒绝对应Object；
reservation丢失时final exact CAS fail closed。

## 15. 为什么首个 GA 不需要长期 Lifecycle Snapshot

本方案在 Archive 验证完成前不退休源数据：

- Reader/Rewrite前先取得exact reservation，普通Merge跳过对应Object；
- exact source Data/Tombstone文件由现有GC SyncProtection保护并续租；
- Rewrite在同一个不可扩BloomFilter中预注册deterministic live/booking filename
  range，防止final前被orphan GC删除；
- reservation/protection丢失：Reader取消或final validation abort；
- Mixed Reader 完成后发生 Merge：普通 DELETE transfer 或冲突；
- CN crash：源数据未被 final transaction 退休，Cleanup Root 负责 staging；
- final transaction 失败：Dataset 不发布，活动数据保持可见。

因此不新增长期table-only Snapshot。保护单位是当前child的exact Data/Tombstone
文件，且TN Prepare必须验证protection仍有效。

小Mixed可写SI事务仍可能推进不了GC watermark；Rewrite构建阶段不持有可写事务，
但占用reservation/protection/staging/transfer资源。必须分别按以下维度admission：

```text
small Mixed:
  txn wall time + workspace + tombstone + GC lag

Rewrite:
  source protection age + source/live/archive bytes +
  transfer bytes + staging objects
```

- 超过 soft limit 降低并发和 child bytes；
- 超过hard limit在进入final transaction前cancel Reader/txn/Rewrite；
- cancel 后先得到明确 aborted，再释放 Root staging；
- 已进入 `COMMIT_UNKNOWN` 的 final transaction 仍按第 18 节对账，不能为了降
  retained bytes 猜测释放所有权。

首个 GA 不增加：

- table-only Snapshot kind；
- GC metadata-visible gate；
- old GC cycle drain；
- 每Object持久化source-ref Catalog行；
- Snapshot pinned-byte accounting。

现有GCTTL不是正确性证明；正确性来自已注册且Prepare仍有效的SyncProtection。
任何读取错误、短读、checksum mismatch、Object消失、protection续租失败或TN
restart都必须终止旧attempt或让final fail closed。

## 16. Archive 格式与存储

### 16.1 文件格式

统一使用：

- Apache Parquet；
- ZSTD；
- 目标文件默认 256 MiB，可在 256–512 MiB 之间调优；
- 稳定 MO column ID/field ID；
- 完整类型、NULL、时区、Decimal 精度和 schema version；
- row-group min/max/null count；
- content checksum 和 Dataset root；
- encryption/KMS identity digest；
- provider object version（Provider支持时）；否则使用 immutable key + size + SHA-256。

不使用 CSV，因为 CSV 不能无损表达 MO 类型、NULL、时区、Decimal 和长期 schema contract。

不规定“一天一个文件”或“每月重写合并”：

- child 内流式累计到目标文件大小；
- 月/年范围通过 Dataset Catalog 和 Manifest 集合选择；
- 不为了目录整齐复制历史 Payload。

### 16.2 ArchiveStore

新增独立能力接口，不要求所有现有 FileService backend 实现：

```text
ArchiveStore
  PutStream(immutable_key, checksum)
  Open(exact key/version)
  StatVersion(exact key/version)
  ListAttemptObjects(immutable_prefix)
  ListAttemptUploads(immutable_prefix)
  AbortMultipart(upload_identity)
  Delete(exact key/version)
```

GA Profile 必须证明：

- PUT 后可按 exact identity 重读；
- LIST/HEAD 和 multipart enumeration 一致性满足 Cleanup Root 对账；
- multipart 可以枚举或显式 abort；
- Delete 可重试；
- immutable key 永不覆盖；
- Credential rotation 后仍能访问旧 namespace。

### 16.3 不可变 key

```text
/<namespace>/<account-incarnation>/<table>/<dataset>/<attempt>/<content-hash>
```

key 不包含可复用的 Job 序号作为唯一身份。旧 executor 迟到 PUT 只能写入自己的 immutable attempt prefix，不能覆盖新 attempt。

## 17. Attempt Control 与 Cleanup Root

### 17.1 创建时机

所有 child 在进入执行前创建 system-owned Attempt Control。

Archive child 在第一次 Provider PUT 前创建 system-owned Cleanup Root；TTL/Archive
Rewrite 在第一次 TAE live staging Object 或 transfer booking write 前也创建 Root：

```text
REGISTERED
  -> UPLOADING
  -> VERIFIED
  -> FINALIZING
       -> PUBLISHED -> DELETE_PENDING -> DELETING
                                      -> CLEANED | DELETE_FAILED
       -> POST_COMMIT_CLEANUP -> TRANSFERRED -> bounded audit GC
       -> DELETE_PENDING              # explicitly aborted
```

不能等到 Dataset publish 时才创建 Root，否则：

```text
PUT staging
  -> DROP ACCOUNT
  -> tenant Job 被删除
  -> publish 未发生
  -> staging 永久泄漏
```

Root 先冻结 immutable attempt namespace/prefix，再允许创建 Provider 或 TAE shared
FileService 对象。取得 upload identity 后，
必须在发送第一个 part 前持久化；如果 executor 在 multipart create 返回与 Root 更新
之间 crash，Sweeper 通过 `ListAttemptUploads(prefix)` 发现并 abort。每个 PUT complete
后先把 exact key/version/checksum 追加到 Root，再开始下一个 Payload；Root row 过大
时使用按 attempt 分页的 child-object rows，不能退化为单行无限数组热点。

### 17.2 Root 内容

```text
account incarnation
logical/physical table identity
job/child/attempt/executor epoch
deterministic prefix
profile/version/namespace
manifest key/version/root
payload exact key/version list
multipart identities
state/version/access_generation
lease/deadline
first/last I/O time
final transaction identity/result
owner tombstone identity
```

Attempt Control/Root 由 system account retained registry 所有，不随 tenant cluster-table cleanup 一起消失。
Registry 按 `account_incarnation/table_id/attempt_id` 分片或聚簇，并维护可分页的
`next_action_at` 索引；Scheduler/Sweeper 禁止用一行全局 cursor 或每轮扫描全部
历史 Root。

### 17.3 唯一 Owner

| 资源 | 创建 Owner | 终态 Owner |
|---|---|---|
| Reader Batch | Reader | Reader |
| Parquet buffer | Archive Writer | Archive Writer |
| multipart upload | Attempt Root | Writer/Sweeper |
| staging Payload | Cleanup Root | Dataset Root 或 Sweeper |
| Published Payload | Dataset + Root | Purge/owner-drop Sweeper |
| SI transaction | Mixed Executor | txn client/unknown resolver |
| TTL attempt | Attempt Control | Reconciler |
| Restore staging table | Restore Attempt | Restore cleanup |
| source Object reservation | Lifecycle attempt | TN reservation manager/TTL |
| source GC protection | Lifecycle attempt | TN SyncProtection manager/TTL |
| live TAE staging Object | Cleanup Root | final transaction 或 Sweeper |
| transfer booking/page | final transaction | TAE txn entry/replay/rollback |
| Discovery scan state/Candidate | Scheduler epoch | Planner/Reconciler |

任何资源不能同时由 executor defer 和 Sweeper 无条件删除。

`DELETE_FAILED` 是可运维故障态，不是“对象已经不存在”：

- Root 和 exact key/version 列表继续保留；
- 每次 Provider Delete/HEAD/LIST 使用独立 deadline 和有界退避；
- 自动重试耗尽后释放 worker slot，保留告警和人工恢复入口；
- 后续恢复清理由新的 Sweeper generation CAS 接管；
- 未确认全部 Payload 消失前，Dataset 不能进入 `PURGED`。

### 17.4 迟到 PUT

旧 executor 可能在第一次清理后迟到完成 PUT。`CLEANED` tombstone 必须保留到：

```text
max provider I/O deadline
  + multipart abort convergence
  + quiescence window
```

窗口内发现迟到对象时，Sweeper 重新进入删除流程；不能复用该 key。

## 18. 最终事务与 commit unknown

Dataset/Receipt 属于 tenant 业务事务，Cleanup Root/Attempt 属于 system-retained
registry。首个 GA 不要求一个事务跨 account 同时修改两者；Root 是外部对象的
write-ahead ownership record，不是 Dataset 的可见性真相。

提交前顺序固定为：

```text
allocate normal tenant transaction identity
  -> CAS Root VERIFIED -> FINALIZING
       freeze transaction identity/final-entry digest/executor epoch
  -> wait Root CAS committed
  -> submit tenant final transaction
```

只有 Root 的 `FINALIZING` 持久化成功后，才允许提交可能退休源数据的事务。Tenant
最终事务必须 CAS tenant-owned Binding/active child generation/attempt/epoch，以
fence stale executor。Root 进入 `FINALIZING` 后，Sweeper 在事务结果权威明确前
永远不能删除 Payload。

Root/Attempt 的 system-registry 事务必须先提交并释放全部锁，之后 tenant final
transaction 才能获取正常 txn/Guard/Dataset 锁；两类事务之间不跨调用持锁。Root CAS
成功但 tenant transaction 超时，只能明确 abort tenant txn，再由 Reconciler 把 Root 推进到
`DELETE_PENDING`，不能在锁等待路径同步清理 Provider。

该协议有意避免跨 owner 2PC：

- tenant 最终事务原子保证 Dataset/Receipt 与活动数据退休；
- system Root 在事务前已经取得全部外部对象所有权；
- response lost 只让 Root 停在 `FINALIZING`，不会误删；
- Reconciler 按 transaction identity 查询结果，再把 Root 收敛为 `PUBLISHED`、
  `TRANSFERRED` 或 `DELETE_PENDING`；
- 禁止先提交 tenant 事务、再 best-effort 创建 Root。

TTL Whole 没有外部 Payload，由 system-owned Attempt Control 在提交前进入
`FINALIZING`。TTL Rewrite 已写 live staging/transfer booking，必须像 Archive 一样
先把 Cleanup Root 置为 `FINALIZING`。两者都冻结相同 transaction
identity/entry digest/epoch；不能因为 tenant Job row 随 DROP 消失而把 unknown 当作
aborted。

### 18.1 Whole Archive final transaction

同一事务包括：

```text
CAS Feature Guard/Binding/active child attempt/owner
CAS Dataset VERIFIED_NOT_PUBLISHED -> PUBLISHED
insert immutable Receipt
LifecycleCommitEntry(WHOLE_ARCHIVE, exact source Object)
```

`EMPTY_ARCHIVE` 不写 Dataset，只提交匹配的 zero-visible Receipt 和
`LifecycleCommitEntry(WHOLE_ARCHIVE, exact source Object)`；其余 Guard、owner、
attempt、reservation、source protection 和 exact Object
条件不放宽。

### 18.2 Mixed Archive final transaction

同一普通 SI 事务包括：

```text
Relation.Delete(RowID + delete key)
CAS Feature Guard/Binding/active child attempt/owner
publish Dataset/Manifest
insert Receipt
```

### 18.3 TTL final transaction

```text
Whole:
  CAS Guard/Binding/active child attempt/owner
  LifecycleCommitEntry(WHOLE_TTL, exact source Object)
  insert Receipt

Mixed:
  Relation.Delete(RowID + delete key)
  CAS Guard/Binding/active child attempt/owner
  insert Receipt
```

### 18.4 Mixed Rewrite final transaction

```text
CAS Guard/Binding/active child attempt/owner
CAS Dataset VERIFIED_NOT_PUBLISHED -> PUBLISHED    # Archive only
insert immutable Receipt
LifecycleCommitEntry(
  MIXED_REWRITE,
  exact source Objects,
  staged live Objects,
  transfer booking,
  row-conservation roots
)
```

TTL Rewrite 不写 Dataset，但仍必须在同一事务提交 Receipt、source retirement、live
Object publish 和 transfer。任何一项失败都整体 abort。

### 18.5 结果判定

```text
txn service 明确 committed
  + 正常一致性事务读到匹配 Receipt
  + 非空 Archive 时读到匹配 Dataset
    -> 非空 Archive: CAS Root FINALIZING -> PUBLISHED
    -> Rewrite: live/range child -> TAE_OWNED;
                booking child -> DELETE_PENDING
    -> TTL Rewrite: CAS Root FINALIZING -> POST_COMMIT_CLEANUP;
                    booking删除后 -> TRANSFERRED
    -> TTL Whole/EMPTY_ARCHIVE: CAS Attempt Control FINALIZING -> COMMITTED
    -> COMMITTED

txn service 明确 aborted
    -> Archive/Rewrite: CAS Root FINALIZING -> DELETE_PENDING
    -> TTL Whole/EMPTY_ARCHIVE: CAS Attempt Control FINALIZING -> ABORTED
    -> ABORTED，允许清理未发布 staging

txn service 仍 in-doubt
    -> COMMIT_UNKNOWN
    -> Root/Attempt Control 保持 FINALIZING
    -> 保留适用的 Cleanup Root 和 staging
    -> 继续查询相同 transaction identity

automatic reconciliation exceeds operational age
    -> MANUAL_RECONCILE_REQUIRED
    -> 释放 worker/执行槽，但不释放资源所有权
    -> 暂停该 scope 的新 retirement 并告警
```

禁止：

- 看到暂时缺失的 Catalog row 就报 corruption；
- response lost 后创建新 final transaction 猜测重试；
- 超时后自动删除可能已经发布的 Payload；
- 使用 Root 最新 MVCC row timestamp 代替不可变 publish commit identity。

Reconciler 每次只进行有 deadline 的事务状态查询和一致性 Catalog 读取。事务服务
明确 committed 后，还必须在正常一致性事务中读到匹配的 Receipt，非空 Archive
还要读到匹配 Dataset，再推进 Root/Attempt Control；如果 Archive owner 已经明确
DROP，则可以直接把 Root 推进到 `DELETE_PENDING`。明确 aborted 后才允许清理
未发布 staging。事务服务长期无权威结果时，系统保持 fail closed，由 hard quota
阻止新的不确定项继续增长。

Receipt 至少记录：

```text
intent/attempt/entry digest
transaction identity
participant set
source object/root
dataset/manifest/root
retirement mode
commit timestamp
protocol/capability version
```

## 19. DROP、Purge 与 Restore lease

### 19.1 产品契约

Archive 从属于源表和租户：

- DROP TABLE/ACCOUNT 后不承诺 Restore；
- 不支持 Archive Transfer；
- 不在 DROP 同步路径等待 Provider；
- 不保留 Legal Hold；
- 后台依据 owner tombstone 异步删除 Archive；
- 这是与 Snowflake UNDROP/Fail-safe 的有意差异。

### 19.2 Owner tombstone

DROP 主事务增加轻量本地 Catalog 写：

```text
DROP TABLE
  -> table owner tombstone

DROP ACCOUNT
  -> account incarnation owner tombstone
```

Owner tombstone 必须在同一 DROP Catalog 事务中写入，并且先于 tenant Lifecycle rows 变得不可枚举。它不枚举 Payload，不调用 Provider。Sweeper 通过 system-owned Root 和 owner tombstone 清理。

### 19.3 删除谓词

源 TAE Object：

```text
SoftDelete committed
AND existing Snapshot/PITR/Branch/Backup/ISCP reference absent
AND existing TAE GC watermark permits
```

Archive Payload：

```text
(
  purge_eligible_at reached
  OR owner dropped
)
AND no active Restore/read lease
AND Cleanup Root CAS to DELETE_PENDING/DELETING
```

进入 `DELETING`：

- 状态不可撤销；
- 禁止新增 Restore lease；
- stale/new runner 重复 Delete 是幂等；
- provider 支持 version ID 时按具体版本删除；
- 全部对象确认删除后 Dataset 才能进入 `PURGED`；
- 需要继续保留时只能复制到新的 immutable Dataset，不得复活原 key。

### 19.4 Restore lease

Restore 获取 lease 与 Root 的 `DELETE_PENDING` CAS 同一 `access_generation`：

```text
ACTIVE/PUBLISHED
  -> acquire bounded lease(attempt_id, executor_epoch, expires_at)

DELETE_PENDING/DELETING
  -> reject new lease
```

Restore 必须在每个 Provider 读取批次和最终发布前续租并校验
`attempt_id/executor_epoch/access_generation`。续租失败或 lease 到期后：

- 立即停止新的 Payload 读取；
- 不允许发布 staging table；
- 释放本地资源并由 Restore Attempt 清理 staging；
- stale executor 不能复活旧 lease。

Sweeper 只等待未过期且 generation 匹配的 lease；lease 到期或明确释放后，才能
CAS 进入删除。这样 Restore crash 不会永久阻塞 Purge，Purge 也不会在有效 Restore
期间删除 Payload。

owner DROP 会提高 `access_generation` 并 fence 已存在 Restore attempt。已有 Restore 可以完成当前 I/O，但最终发布新表前必须重新 CAS owner state；看到 owner dropped 后只能删除 staging、释放 lease，不能在 DROP 后发布新表。

## 20. Restore

### 20.1 用户语义

Archive 不透明参与原表查询。用户执行：

```text
RESTORE ARCHIVE
  FOR TABLE db.source
  FROM <time range or dataset list>
  TO TABLE db.restored_name;
```

Restore 创建独立新表，不把历史数据原地合并回源表。

### 20.2 流程

```text
resolve immutable Dataset set
  -> acquire Restore lease
  -> validate owner/Profile/schema compatibility
  -> Open exact Manifest/Payload versions
  -> full checksum/root verification
  -> create hidden staging table
  -> stream Parquet into normal MO writer
  -> verify row count/content root
  -> atomic publish/rename as requested new table
  -> release lease
```

任一失败：

- 新表名不可见；
- staging table 由 Restore Attempt 唯一清理；
- Dataset 保持不变；
- 重试使用新的 Restore attempt，不覆盖旧 staging。

### 20.3 Schema contract

Restore 使用 Manifest 冻结的源 schema：

- column ID、顺序、类型、NULL、default 和 collation 必须可解释；
- 当前版本不支持的历史类型返回明确错误；
- 不把未知类型降级成字符串；
- 新表可以由用户显式选择名称和 database；
- 不自动附加源表当前索引、FK、CDC 或 Publication。

### 20.4 Backup/DR 限制

首个 GA 不复制 Archive Payload 到 Backup/DR。

对 Lifecycle-bound 表：

- 普通 Snapshot/PITR/Backup/Clone/Branch 创建与 Restore 使用 Feature Guard 互斥；
- 未实现 archive-aware restore 的操作在执行前拒绝；
- DR/failover 后 Archive Catalog/Payload 不可用时返回明确 unsupported/unavailable；
- 不能返回“恢复成功但历史行缺失”的表。

## 21. 普通 Merge 与 DML 并发

### 21.1 普通 Merge

不修改普通Merge候选/排序/重写/GC策略：

- Planner/Candidate不reservation；
- Executor开始实际I/O前claim exact source Object；
- Scheduler跳过reservation source；
- 普通Merge final handler以短`MergeAdmissionTicket`关闭“检查后、DropIntent安装前”
  的窗口，ticket释放后由Object MVCC接管；
- Merge不等待远端Archive I/O；
- 已运行Merge和claim由TN reservation manager线性化；
- reservation丢失后Lifecycle exact Object CAS abort/replan；
- Rewrite复用现有create/drop/两阶段RowID transfer；
- Discovery通过分页full cycle和有界`CollectObjectList`增量加速。

Lifecycle 不直接处理 Merge 已删除的旧 Object，也不阻止现有 GC 删除它们。Lifecycle 只对执行 Snapshot 上选中的当前有效数据负责。

### 21.2 INSERT

INSERT 产生新 Object：

- 不修改已冻结 source set；
- 迟到旧数据进入下一次 Discovery cycle/Job；
- final transaction 不获取长时间表锁，也不把新 Object 纳入旧 attempt；
- Job 不承诺在 `expire_at` 瞬间捕获迟到数据。

### 21.3 UPDATE/DELETE

少量冲突由普通事务 abort/retry 处理。

Whole Archive和Mixed Rewrite对导出后Tombstone使用：

```text
source reservation/protection
  + merge phase-1/phase-2 delta transfer
  + expired row NoTransfer conflict
  + exact source DropIntent
```

持续高频 UPDATE/DELETE：

- 可能反复 re-export；
- 超过 conflict age/attempt budget 后 `CONFLICT_BLOCKED`；
- 活动数据保持完整；
- 不承诺 Archive Lag SLO；
- 必须告警并展示冲突 Object、Tombstone rows/bytes 和建议。

## 22. Commercial GA 十项 P0

### P0-1：Reader Batch 所有权

证明串行 callback 下：

- Batch/Vector 借用边界清晰；
- retained data 已复制；
- Close/Cancel/Error/Panic exactly-once；
- 无异步悬挂 Vector；
- 无 multipart、buffer 和 goroutine 泄漏。
- `ScanReport` 能区分完整 0 visible rows 与 Object missing/短读/未到 EOF。
- Rewrite单次读取中Archive Sink同步消费borrowed Batch，不能异步悬挂Vector。

### P0-2：Mixed 可写 SI Snapshot

证明：

- 使用普通可写 SI transaction，不是 Snapshot Operator；
- Reader、Archive 和 DELETE 使用同一 Snapshot；
- RC statement snapshot advance 不参与；
- 并发 DELETE/UPDATE 导致冲突而非发布旧数据；
- commit unknown 保留 staging 和事务身份；
- transaction/provider/workspace 均受硬 deadline。

### P0-3：Tagged Lifecycle Entry、WAL 与 Replay

证明：

- table/physical generation、exact Object 和 Guard CAS；
- Object-not-found 不被当作成功；
- tag位于可重放commit payload，老TN由capability gate阻止接收；
- nested MergeCommitEntry、entry/transfer/Receipt digest一致；
- duplicate Prepare、`ErrTAENeedRetry`多generation、WAL replay 和 response lost幂等；
- 外部逻辑attempt冻结绝对deadline/累计预算，每个内部generation重建私有entry；
- `LogTxnEntry`成功是runtime Owner转移点，前后故障无slab/page/TransferDels泄漏；
- 复用create/drop/transfer command后重启Object集合正确；
- 普通宽松 SoftDelete 行为不变。

### P0-4：Source reservation 与 GC protection

证明：

- Acquire与普通/用户强制Merge final admission线性化；
- Merge admission ticket覆盖检查reservation到安装全部source DropIntent，所有退出
  路径exactly-once释放，且按table/object分片；
- reservation过期、续租失败和TN restart均让旧attempt fail closed；
- GC SyncProtection覆盖exact source Data和所需Tombstone文件；
- Rewrite冻结的live/booking range也在首次外部写前进入同一protection；
- register与运行中GC cycle、renew与CleanupExpired无删除窗口；
- TN Prepare验证protection仍有效；
- reservation/protection不写WAL、不replay，丢失只影响进度。

### P0-5：第一次外部副作用前 Cleanup Root

证明：

- Root 先于所有 PUT/multipart、TAE live staging Object 和 transfer booking；
- multipart create 与 upload identity 持久化之间 crash 时可按 deterministic prefix 枚举并收敛；
- DROP TABLE/ACCOUNT 不丢失清理 Owner；
- Root `FINALIZING` 必须先冻结并持久化 transaction identity、entry digest 和 executor epoch，tenant final transaction 后发；
- tenant final transaction CAS Binding/active child attempt/epoch，stale executor 不能提交；
- Root 不参与 tenant 2PC 时，committed/aborted/unknown 三种结果都能由 Reconciler 唯一收敛，且没有“已退休但无 Root”或“unknown 被误清理”路径；
- old executor 迟到 PUT 仍会被发现；
- `COMMIT_UNKNOWN` 不清理可能已发布 Payload；
- Profile/namespace/key/version identity 冻结，cleanup credential handle 在 owner DROP
  后仍由 system registry 保留。

### P0-6：Rewrite 行守恒、transfer 与并发 Tombstone

证明：

- `source_visible = expired + live`；
- Archive readback rows/root等于expired；
- created TAE Object rows和non-NoTransfer mapping等于live；
- Snapshot前Tombstone不进入任一输出；
- Snapshot后delete命中live row时transfer；
- Snapshot后delete命中expired row时整个transaction abort；
- Prepare后并发delete查到NoTransfer必须冲突；
- transfer phase-1/phase-2任何错误都向上返回；
- staging live Object/booking在abort/unknown/commit三种结果下Owner唯一。

### P0-7：Mixed DELETE/Rewrite 资源硬预算

预算至少覆盖：

- actual RowID/delete-key bytes；
- rows、blocks、source/archive/live bytes；
- workspace/WAL/Logtail；
- rolling Tombstone bytes；
- transfer memory/disk和created Object count；
- reservation/source protection count/age；
- Merge/Vacuum backlog；
- transaction/provider duration；
- active snapshot-exclusive retained bytes/GC lag；
- account/cluster fair-share。

超限必须停止退休，不允许通过拆成无限小 Job 持续制造无界 backlog。

### P0-8：Restore 原子发布

证明：

- immutable Manifest/schema/checksum；
- exact version read；
- hidden staging table；
- 全量验证；
- 新表原子可见；
- failure/cancel/restart 无半张表和 staging 泄漏；
- Purge/owner DROP 与 Restore lease CAS 正确。

### P0-9：CN commit-control 不得丢失

证明：

- Lifecycle tag不作为`bat == nil`的普通`txn.writes Entry`进入workspace；
- `Transaction.lifecycleCommit`最多一条、payload deep-copy后不可变；
- Archive必须有Dataset+Receipt、TTL必须有Receipt；production control不存在合法空Catalog
  pair，`genWriteReqs`在最终workspace中按逻辑identity验证exact pair；缺失、空Entry或
  只有无关write均返回`ErrLifecycleCatalogPairMissing`且不发请求；
- 普通workspace dump/compact/sort/GC和只读判定不会过滤或改写control/pair；control设置后
  状态机进入SEALED，禁止局部statement rollback或继续外部write；
- `genWriteReqs`在普通Entry后恰好追加一次tag；私有编码helper可测试空Entry，production
  finalizer不得发送control-only；
- 同digest同payload重复设置幂等；冻结身份仅为ServiceID/ShardID/ReplicaID/protocol
  version，发送前权威refresh，identity/capability变化或多候选shard时fail closed；
- Dataset/Receipt普通Catalog写与tag属于同一外部事务，任一缺失整体不能提交。

`Transaction`仅对Lifecycle final transaction增加：

```text
OPEN -> SEALED -> COMMITTING -> TERMINAL
              \-> POISONED -> full rollback -> TERMINAL
```

Seal后，外部Write/statement/snapshot/adjust类入口必须poison；只有Commit的未导出内部
helper可在COMMITTING执行既有merge/dump/transfer。普通transaction、普通Merge和普通查询
不进入该状态机。

### P0-10：内部 TAE generation 的并发与资源 Owner

证明：

- generation-local slot在任何TAE Catalog mutation和runtime page安装前线性化；
- 只有BUILDING owner可以执行`SoftDeleteObject`、`CreateNonAppendableObject`和
  phase-1；重复请求只做有界等待或复用终态；
- Drop/Create Catalog node在API成功后立即属于整个内部TAE事务，不由builder局部撤销；
- TransferTable/slab/page/TransferDels在`LogTxnEntry`前属于builder，成功后原子转交
  txn entry；Root-owned物理文件始终只由Cleanup Root删除；
- 构建失败把slot置FAILED并回滚整个generation，禁止在同一已污染事务内重建；
- G2使用新的内部事务、slot、Catalog node、TransferTable和entry；
- `HandleCommit`级ReplayBudget对所有generation共享绝对deadline、最大代数和累计
  预算，retry不能刷新预算或形成无界memo；
- 重叠Commit由现有TxnService串行；terminal后迟到duplicate必须在Booking I/O前经过deadline、
  Lifecycle admission、reservation/protection和exact-source preflight，已提交source不exact
  时返回`LIFECYCLE_RECONCILE_REQUIRED`并由Receipt收敛；仅在实证storage可并行执行同一
  external txn时才增加有界registry。

十项P0之外，Feature Guard、Profile identity、Discovery scale、DROP cleanup、
升级capability和support matrix也是GA必备条件；它们不能因为不在编号中被降级
为可选项。

## 23. 资源和规模

### 23.1 目标

- 常见单表：1 TiB；
- 认证单表：10 TiB；
- 显式绑定表：最多 1000；
- 集群普通表可达几十万，但不进入 Lifecycle 日常扫描。

### 23.2 必须有界的积累项

每项记录 `count + bytes + oldest_age`：

- Binding/Guard/Discovery cursor/Packed Summary；
- due/running/retry/blocked Jobs；
- Attempt Control/Cleanup Root；
- source reservation/protection；
- TAE live staging Object/transfer booking；
- staging/Payload/multipart；
- Dataset/Manifest/Receipt；
- `COMMIT_UNKNOWN`；
- Mixed SI transaction/workspace；
- active Reader/SI snapshot-exclusive retained bytes 和 GC lag；
- Tombstone Object/WAL/Logtail；
- Restore staging/lease；
- Purge/orphan/delete backlog。

每项必须定义：

- quota owner；
- soft action；
- hard action；
- recovery/reconcile path；
- terminal GC watermark。

只限制 executor 内存而允许 Catalog 或外部对象无限增长，不满足 GA。

最低清理规则：

- 每个 Binding 只保留 O(1) 的 scan cursor/watermark 和有界 Candidate/child Job；
- 可选 packed Discovery Summary 只保留当前 root、一个构建中 root 和有界旧版本，
  且其丢失不影响正确性；
- 已知 aborted 的 Attempt 在 staging 清理、I/O quiescence 和审计保留期结束后分页
  GC；
- `CLEANED` Root 在迟到 PUT 窗口结束后分页 GC；
- `PURGED` Dataset/Manifest/Receipt 压缩成有界 audit tombstone；只有 final
  transaction 已收敛，且超过最大 txn retry/WAL replay/滚动降级窗口和产品审计保留期
  后才分页 GC；
- owner tombstone 至少保留到该 account incarnation/table identity 的所有 Root、
  Dataset 和 Restore Attempt 都进入可回收终态，并超过迟到 executor/I/O 窗口；
  account incarnation 永不复用；
- blocked Job 只保留有界明细和聚合统计，re-evaluate 创建新 generation；
- `COMMIT_UNKNOWN`/`MANUAL_RECONCILE_REQUIRED` 不按年龄自动 GC；达到 hard quota
  时暂停对应 scope 的新 retirement，避免不确定所有权继续增长；
- `DELETE_FAILED` 不丢弃 key/version，达到 hard quota 时暂停新 Archive publish，
  Cleanup/Purge 仍保留最低资源。

所有保留期、page size、hard quota 和暂停 scope 必须在 release profile 中冻结，
默认无限值非法。

### 23.3 Tombstone 成本

最低原始成本近似：

```text
expired mixed rows × (24B RowID + encoded delete-key bytes)
```

每天 1 TiB、平均行宽 200B、delete key 8B：

| Mixed 到期比例 | 最低 Tombstone 原始量 |
|---:|---:|
| 1% | 约 1.6 GiB/天 |
| 10% | 约 16 GiB/天 |
| 100% | 约 164 GiB/天 |

实际还有 WAL、Logtail、Tombstone Object、Merge/Vacuum 和读放大。因此即使 1% 也不能自动视为安全，必须同时看 rolling rate 和 backlog。

### 23.4 Oversize Object

当前 Object 最大 rows/blocks/bytes 可能显著高于平均 128 MiB。

- Whole Archive 使用 streaming，不把整个 Object 放入内存；
- Mixed Rewrite使用streaming；一个当前合法单Object不能因普通child target而永久blocked；
- 普通小Mixed DELETE超限时转Rewrite，不在长SI transaction中硬撑；
- 不声称能把一个 Object 拆成更小的原子 source Object；
- 认证覆盖当前`8192 rows/block × 256 blocks`、3 GiB Object size limit、
  单Block varlen、transfer spill和file数。

## 24. 可观测性与运维

### 24.1 Dry-run

```text
DRY RUN LIFECYCLE FOR TABLE db.t
```

至少输出：

- cutoff/evaluation time；
- Whole/Mixed/not-due Object 数和 bytes；
- `alignment_ratio`；
- 预计 Archive read/write/readback bytes；
- 预计 Mixed expired rows、RowID/delete-key bytes 和 blocks；
- 预计 Tombstone/WAL/Logtail；
- 预计Rewrite source/live/archive/transfer bytes和write amplification；
- lifecycle column与physical sort/cluster key是否对齐；
- oversized/unsupported Object；
- Provider 请求和费用估算；
- 预计 blocked 原因；
- Feature Guard 不支持项。

### 24.2 SHOW

```text
SHOW LIFECYCLE FOR TABLE db.t
SHOW LIFECYCLE JOBS
SHOW ARCHIVE DATASETS FOR TABLE db.t
SHOW LIFECYCLE BLOCKERS
```

显示：

- effective Binding/Profile version；
- next action/backlog age；
- Job/attempt/transaction identity；
- source/archive bytes；
- Dataset/Payload/root 状态；
- conflict/tombstone/merge backlog；
- Cleanup/Restore/Purge 状态；
- 最近错误和用户建议。

### 24.3 Kill switch

支持：

- cluster/account/table 暂停新 Job；
- 禁止进入 `FINALIZING`；
- 允许正在提交的事务按正常协议收敛；
- Cleanup、Reconcile、Restore 和 Purge 保留最低资源；
- 不通过 kill switch 回滚已经提交的 Dataset 或 Object retirement。

## 25. 代码改动边界

### 25.1 新增为主

建议新增：

- `pkg/lifecycle/catalog`：Policy、Binding、Guard、Scan State、Candidate、Job、Dataset、Receipt；
- `pkg/lifecycle/planner`：Metadata 分类、Dry-run、cost/budget；
- `pkg/lifecycle/executor`：Whole/小Mixed/Rewrite child、Reader callback、finalizer；
- `pkg/lifecycle/rewrite`：split classifier、Archive Sink、TAE live writer、transfer report；
- `pkg/lifecycle/archive`：ArchiveStore、Parquet Writer、Manifest/root；
- `pkg/lifecycle/ownership`：Attempt Control、Cleanup Root、Owner Tombstone、Sweeper；
- `pkg/lifecycle/restore`：Dataset resolve、lease、staging publish；
- `pkg/lifecycle/observability`：metrics、SHOW、审计和 Runbook。

### 25.2 现有路径的窄改动

| 位置 | 改动 |
|---|---|
| `pkg/vm/engine/disttae` | exact-object Reader/Metadata page；内部可写SI封装；Lifecycle rewrite host |
| `proto/api.proto` | 版本化tagged `LifecycleCommitEntry`及payload字段 |
| `pkg/catalog`、`pkg/vm/engine/tae/rpc` | tagged entry route、bounded validation、错误原样返回 |
| TAE Merge/txn/catalog | exact reservation admission；复用create/drop/两阶段transfer |
| TAE GC | 复用SyncProtection并在Lifecycle Prepare验证 |
| SQL/frontend | table-level DDL、Guard、Dry-run/SHOW/RESTORE、DROP owner tombstone |
| TaskService | Lifecycle scheduler/lease/epoch |
| system Catalog | retained Attempt/Cleanup/Owner registry |

### 25.3 明确不改

- 普通Merge selection/physical sort key；
- 普通`DoMergeAndWrite` host语义；
- transfer map 格式；
- 普通 Reader 语义；
- 普通 SELECT 过滤；
- 现有用户 Snapshot loader；
- 现有 TAE GC 删除谓词；
- 所有 FileService backend 的统一接口；
- 未绑定表的DML/Merge Catalog写；reservation map为空时只允许廉价空检查。

## 26. 故障与验收矩阵

### 26.1 正确性

- Whole TTL max `< cutoff`；
- cutoff 等值；
- ZoneMap 缺失/截断/版本未知；
- Snapshot 前已有 Tombstone；
- 完整扫描但 0 visible rows 与 Object missing/短读；
- Archive Reader rows/root 与 Parquet readback 一致；
- Mixed 无到期行、少量到期、预算边界和超限；
- Mixed Rewrite的0/1/max live row、NoTransfer和row conservation；
- 无 PK、普通 PK、复合/varlen PK 的准入和 DELETE；
- schema digest/type/NULL/timezone/Decimal；
- Restore Dataset range 和重复 Dataset 去重。

### 26.2 并发

- Whole vs Merge，双方先后 Prepare/Commit；
- Whole Archive 导出期间 DELETE/UPDATE；
- reservation 获取前/后普通 Merge、CN Merge 和用户强制 Merge；
- protection 注册、续租、TN restart 和过期后的 final Prepare；
- TN delta validation 与并发 Tombstone Prepare；
- Mixed vs DELETE/UPDATE；
- Mixed vs 一次/多次 Merge transfer；
- Rewrite 的 survivor/expired 行并发 UPDATE/DELETE；
- transfer page 过期；
- Binding vs DDL/CDC/Index 首次创建；
- DROP TABLE/ACCOUNT vs PUT/finalize/Restore/Purge。

### 26.3 Crash points

逐点注入：

- Attempt/Root commit 前后；
- multipart create/part/complete 前后；
- Manifest PUT/verify 前后；
- Root/Attempt `FINALIZING` commit 后、tenant final submit 前；
- reservation/protection 获取、续租、过期、释放前后；
- tagged Lifecycle entry parse/validate、builder和`LogTxnEntry`前后；
- live staging Object/transfer booking 创建前后；
- Relation.Delete workspace/write/prepare 前后；
- Catalog participant prepare 前后；
- TN WAL append 前后；
- 1PC/2PC decision 前后；
- response lost；
- committed 后立即 DROP owner；
- Sweeper Delete 前后；
- Restore staging create/write/verify/publish 前后。

### 26.4 Replay 和升级

- duplicate Prepare；
- `ErrTAENeedRetry`；
- TN restart/replay；
- CN executor epoch 接管；
- 旧 executor 迟到 PUT；
- 新 CN/旧 TN、新 TN/旧 CN；
- capability 未全员开启；
- 升级中只运行 Dry-run/Export-only；
- 降级时禁止新 retirement，但继续 Cleanup/Reconcile/Restore/Purge。

### 26.5 性能

- 1000 Binding Registry scan；
- 1000 表分页 Metadata Discovery full cycle 调度；
- 百万级当前 Object 的分页扫描、Candidate 硬上限和可选 Summary rebuild；
- 1 TiB 真实表；
- 10 TiB 单表持续 Merge；
- Whole 3 GiB streaming；
- Rewrite 最大允许 source object 和几乎全部行存活；
- Whole/Mixed 活跃事务在持续 Merge 下的 snapshot-exclusive retained bytes 与 GC lag；
- 每日 1 TiB 下 1% Mixed rolling Tombstone；
- Provider 限速、超时、抖动和读回成本；
- 前台 SELECT/INSERT/UPDATE/DELETE P95/P99；
- reservation 冲突率、保护续租失败率和 transfer 开销；
- Cleanup/Restore 在 Archive backlog 下不饥饿。

## 27. 分阶段实现和 GA 门禁

分阶段是实现和放量顺序，不是使用不安全协议交付 Preview。

| Gate | 能力 | 允许的数据副作用 |
|---|---|---|
| Gate A | Binding、Guard、Metadata Planner、Dry-run | 无 PUT、无退休 |
| Gate B | Discovery、Exact Reader、Export-only、Parquet/Manifest | 只写 staging/export，不退休 |
| Gate C | Attempt/Cleanup、reservation/protection、tagged Lifecycle entry和Rewrite原型 | 测试环境退休 |
| Gate D | Whole、小Mixed DELETE、Mixed Rewrite、十项P0、故障矩阵 | 受控试点 |
| Gate E | 1/10 TiB、升级、运维、成本、客户试点 | Commercial GA |

放量：

```text
50 -> 200 -> 500 -> 1000 bound tables
```

每一级观察：

- correctness invariant；
- oldest backlog age；
- conflict/blocked ratio；
- final transaction retry/unknown；
- Discovery Candidate/Summary/Catalog增长；
- reservation/protection/transfer/staging增长；
- Tombstone/Merge/Vacuum backlog；
- staging/orphan/delete backlog；
- Restore 成功率；
- Provider 错误率和费用；
- 前台 P95/P99。

任何数据不变量失败立即停止扩大。容量越界暂停新 Job，但 Cleanup、Reconcile、Restore 和 Purge 继续运行。

## 28. 架构决策记录

1. **不做 ONLINE_COLD**：活动数据已经在对象存储，收益不足以覆盖透明查询和缓存复杂度。
2. **Archive 使用 Parquet/ZSTD**：类型、压缩、跨版本读取和生态兼容优于 CSV。
3. **不依赖 SQL Partition**：Partition 不是 TAE 物理文件和事务边界。
4. **Reader执行、Discovery调度**：每表cursor和有界Candidate，不建每Object Catalog Index。
5. **普通Merge算法不变**：只增加正在执行exact source的reservation admission。
6. **Whole快速、小Mixed DELETE**：低成本路径继续复用现有能力。
7. **大Mixed使用Rewrite**：expired进Archive/丢弃，live进新TAE Object，不写海量Tombstone。
8. **独立Executor/tagged commit-control**：Provider I/O不进入普通Merge；CN使用
   workspace之外的单条commit-control把tag放入正常可重放commit payload，老TN对新协议
   fail closed。
9. **source/staging protection**：复用GC SyncProtection保护source和Rewrite
   pre-commit staging，不增加长期table Snapshot。
10. **两阶段Tombstone transfer**：live row transfer，archived row并发删除使final abort。
11. **最终短事务**：Build不持有写事务，final禁止Provider I/O。
12. **小Mixed使用普通可写SI事务**：不能把Snapshot SELECT和当前DELETE拼接。
13. **复用Merge Entry/WAL/GC**：不复制new Object replacement和物理删除闭环。
14. **Archive从属于owner**：DROP后不承诺Restore，异步清理，不宣传合规归档。
15. **Restore到独立新表**：不修改普通SELECT，也不把历史行原地混回源表。
16. **Packed Summary只是hint**：不让派生发现数据成为安全单点。
17. **Commercial GA以证据为准**：文档闭环不等于当前代码已经生产可用。

## 29. 最终判断

| 能力 | 结论 |
|---|---|
| 高层 Reader Archive/Export-only | Go |
| Discovery/Dry-run | Go |
| direct-readable Archive + Restore 新表 | Conditional Go |
| Whole TTL | Conditional Go |
| Whole Archive Retire | Conditional Go，必须关闭reservation/protection/Tombstone P0 |
| 小规模 Mixed RowID DELETE | Conditional Go，必须使用可写 SI 和硬预算 |
| 大规模 Mixed 普通 DELETE | No-Go |
| Mixed Object Rewrite | Conditional Go，首个GA正式能力，必须关闭row conservation/transfer P0 |
| 查询时隐藏 TTL | No-Go |
| ONLINE_COLD/Deep Archive | 不在首个 GA |
| 首个受限 Commercial GA | 十项P0、Gate E和分阶段认证完成后Conditional Go |

这套方案复用了MO已有Reader、DELETE、mergesort、Object Writer、两阶段
Tombstone transfer、事务恢复和GC。新增代码集中在Lifecycle控制面、Archive
Catalog、独立Rewrite Executor、CN commit-control/tagged entry、Cleanup、Restore和
资源调度；普通Merge
只增加exact reservation admission，符合“约1000张绑定表、TB级、稳定可靠、
尽量不增加MO内核回归风险”的目标。
