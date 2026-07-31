# MatrixOne TAE Object Lifecycle Commercial GA 实现设计

> 本文是首个 Commercial GA 的总实现规范。全局范围和不变量见 [README.md](README.md)，
> 精确Catalog、接口、状态机、测试和代码任务以README列出的01–08单一职责子设计为准。
>
> 当前协议结论：**Conditional Go**。Whole/Mixed Object算法冻结，Gate A/B可开发；
> [07-p0-ga-test-matrix-cn.md](07-p0-ga-test-matrix-cn.md)汇总的Lifecycle P0，以及既有
> Cleanup/格式/升级安全门禁完成前，不能宣布协议和Commercial GA完成。

## 1. 交付边界

实现新增 Lifecycle 控制面、Archive I/O、thin Object retire entry和Restore；复用现有：

- TAE Metadata、ObjectStats、ZoneMap和MVCC；
- `BlockDataReadNoCopy`、`DoMergeAndWrite`、BlockWriter和TransferTable；
- `SoftDeleteObject`、`CreateNonAppendableObject`、Merge txn entry；
- 普通MO事务、WAL、Replay、checkpoint和GC；
- Stage、FileService、TaskService和普通INSERT/DDL。

不修改普通SELECT、普通DML、Merge候选/排序、WAL格式语义和GC删除谓词。

## 2. 产品接口

SQL语法可按Parser风格调整，但语义固定：

```sql
ALTER TABLE db.t SET LIFECYCLE (
  COLUMN created_at,
  EXPIRE AFTER INTERVAL '90' DAY,
  ACTION ARCHIVE,
  STAGE existing_stage
);

ALTER TABLE db.t UNSET LIFECYCLE;
SHOW LIFECYCLE FOR TABLE db.t;
SHOW LIFECYCLE JOBS;
SHOW LIFECYCLE DATASETS FOR TABLE db.t;

RESTORE ARCHIVE DATASET '<dataset-id>' TO TABLE db.restored_t;
PURGE ARCHIVE DATASET '<dataset-id>';
```

`expire_at`是开始具备处理资格的时间，不承诺到点瞬间消失。失败时源数据继续可见。
Archive不参与原表在线查询；Restore始终创建新表。

绑定表必须拒绝当前不支持的CDC、FK、Publication、隐藏索引、Snapshot/PITR/Backup/
Clone/Branch和插件组合。Phase 1还拒绝逻辑分区表、物理Partition child，以及未经部署认证
或启用对象Versioning的Archive Stage。

普通查询、DML和Merge在未绑定路径不读取Lifecycle Catalog。可能冲突的表级管理DDL只复用
已有`mo_tables`锁并执行一次索引化Binding存在性查询，不跨feature-row barrier；release
gate关闭后仍可能有待UNSET/清理的Binding，因此不增加CN本地开关缓存来省掉这条低频
控制面路径。

## 3. 最小Catalog

通过正常bootstrap/upgrade新增Lifecycle表，不修改已有MO系统表列定义。

### 3.1 Binding

每个物理表最多一行：

```text
binding_id
account_id
database_id
logical_table_id
physical_table_id
binding_generation
schema_digest
lifecycle_column_id
action                 DELETE | ARCHIVE
expire_interval
late_arrival_grace
stage_id nullable
stage_identity_digest nullable
scan_cursor
state                  ACTIVE | PAUSED | BLOCKED | DISABLING
version
```

Policy、Lifecycle列、Stage身份、physical table或有效schema发生语义变化时，
`binding_generation`递增。Binding不保存active attempt、逐Object索引、Candidate或滚动统计环。
Binding的`schema_digest`是源表final fence，覆盖源Column ID及读取/分类语义，不用于比较
Restore新表的目标Column ID。

### 3.2 Dataset

Archive成功发布一行：

```text
dataset_id
binding_id / binding_generation
logical_table_id / source_physical_table_id
source_snapshot_ts
evaluation_time / effective_cutoff
source_set_digest
schema_descriptor_digest
lifecycle_min / lifecycle_max
root_id / attempt_id
manifest_key / manifest_sha256
content_hash / row_count / logical_bytes
stage_id / stage_identity
purge_eligible_at
state                  PUBLISHED | DELETE_PENDING | DELETING | PURGED
version
access_generation
restore_lease_id nullable
restore_deadline nullable
publish_txn_id
```

Dataset本身是Archive发布权威，不增加Archive Receipt。`version`用于所有状态/lease CAS；
`stage_id`提供Stage DROP/ALTER索引化引用检查。Dataset不保留ERROR状态：发布前错误不产生
Dataset，Restore错误保存在Attempt，PUBLISHED Dataset继续可Restore/Purge。
`schema_descriptor_digest`验证Manifest历史descriptor的canonical bytes；Restore按结构字段
创建目标表，不持久化第二个restore schema摘要。

### 3.3 TTL Receipt

用于SHOW、审计和commit-unknown只读对账：

```text
receipt_id
binding_id / binding_generation
source_snapshot_ts
evaluation_time / effective_cutoff
source_set_digest
expired_rows
retired_bytes
root_id nullable
publish_txn_id
```

### 3.4 Cleanup Root

system account持有，一次attempt一行：

```text
root_id / attempt_id
mode                    ARCHIVE_WHOLE | ARCHIVE_REWRITE | TTL_REWRITE
owner_account_id / logical_table_id / physical_table_id
executor_epoch
worker_lease_deadline
archive_namespace_identity / credential_handle
archive_prefix / manifest_key / manifest_digest
tae_namespace_identity / segment_id / booking_prefix / ordinal_upper_bound
source_set_digest
final_txn_id nullable
state
state_version
cleanup_after
temporary_cleanup_done
last_list_at / quiescence_since
last_error
```

只有Archive或Rewrite产生Provider、live staging、booking副作用时创建Root。Whole TTL和
TTL小Mixed不为事务终态额外创建Root，避免把Root变成Terminal Journal。

状态：

```text
REGISTERED -> UPLOADING -> VERIFIED -> FINALIZING

REGISTERED/UPLOADING/VERIFIED
  -> attempt失败、超时、租约失效或owner消失
  -> DELETE_PENDING

FINALIZING -> PUBLISHED | DELETE_PENDING | COMMIT_UNKNOWN
COMMIT_UNKNOWN -> PUBLISHED | DELETE_PENDING
PUBLISHED -> Archive Dataset Purge、owner消失或TTL Rewrite临时资源收敛 -> DELETE_PENDING
DELETE_PENDING -> DELETING -> CLEANED
```

`COMMIT_UNKNOWN -> DELETE_PENDING`只允许在普通MO权威确认事务abort且不存在matching
Dataset/TTL Receipt时发生。Root完整转换、Owner和删除前置条件只在
[04-cleanup-root-reconcile-cn.md](04-cleanup-root-reconcile-cn.md)定义。

不建立Root Object明细表。Manifest或确定性prefix是删除枚举来源。发布后Dataset控制
逻辑可见性和Restore/Purge；Root继续承担Payload物理删除，Dataset Purge不直接访问Provider。

### 3.5 Restore Attempt与Chunk Receipt

Restore Attempt保存Dataset、隐藏表、lease、deadline、`next_chunk_ordinal`、
`restored_rows`、nullable `verified_content_hash`和状态，不保存可继续计算的SHA内部
状态。Chunk Receipt使用：

```text
PRIMARY KEY (restore_id, chunk_ordinal)
chunk_digest
file_ordinal / row_group_ordinal
row_count
logical_bytes
canonical_content_hash
```

首版单个Restore串行推进。数据INSERT、Receipt和Attempt进度在同一普通事务提交；同ordinal
相同digest幂等，不同digest是corruption。最终Hash按Receipt ordinal使用02的固定聚合公式
重建，`verified_content_hash`只在最终发布事务中一次性写入。AUTO_INCREMENT全局最大正值
只保存在经过Archive full readback验证的Manifest中；每个Restore Chunk又验证最终MO
vectors的canonical hash，不在Receipt中维护第二套max聚合。

## 4. Stage合同

Binding、Dataset和Root冻结：

```text
stage_id, provider, canonical endpoint, region,
bucket/container, immutable prefix, storage class, encryption/KMS identity,
credential handle
```

Storage location有引用时不得原地修改。credential可轮换，但稳定handle必须在账户删除、
服务重启、Restore和Sweeper中仍可解析。首个GA不接受只存在于tenant行中的inline secret。

每次物理写使用不可覆盖key：

```text
<stage-prefix>/lifecycle/<root-id>/<attempt-id>/payload-<ordinal>-<write-id>.parquet
<stage-prefix>/lifecycle/<root-id>/<attempt-id>/manifest-<digest>.json
```

attempt和prefix永不复用；Manifest只引用已full readback验证的write-id。worker租约或
SyncProtection失效时不接管原attempt，而是清理旧Root并创建新Root。Stage必须由运维
认证Provider侧 incomplete multipart回收规则；正常错误路径仍主动Abort multipart。

Archive Stage还必须来自部署管理的Lifecycle认证记录/allowlist，并使用专用、Versioning
关闭的Bucket/Container。通用FileService不增加Versioning查询或version ID删除接口。
运维在仍有Lifecycle引用时开启Versioning属于不受支持的配置漂移；首个GA不建设自动漂移
状态机，发现后撤销Stage认证、暂停新Archive，并通过Provider运维工具清理历史版本。

## 5. Discovery与调度

Scheduler维护内存中的Active Binding registry，只扫描显式Binding。每次读取一页当前
TAE Metadata，Candidate有数量/bytes/TTL上限，crash后允许重扫。

这里删除的是“把每个Object再复制到Lifecycle Catalog”的持久Object Index，不是删除
MO已有Metadata索引。当前`PartitionState`已经维护：

```text
dataObjectsNameIndex
tombstoneObjectsNameIndex
dataObjectTSIndex
```

Lifecycle直接从这些当前可见Metadata发现Object。GC metadata包含历史删除和回收信息，
不是当前活动Object集合的权威来源，不能替代`PartitionState`。

当前`GetNonAppendableObjectStats`会把全表ObjectStats一次性收集到slice，Lifecycle不得在
百万Object表上直接调用它。Gate A必须增加只读、有界的分页接口，例如：

```go
type LifecycleObjectCursor struct {
    SnapshotTS      types.TS
    LastObjectName  objectio.ObjectNameShort
    Wrapped         bool
}

type LifecycleObjectPage struct {
    Objects    []objectio.ObjectEntry
    Next       LifecycleObjectCursor
    EndOfCycle bool
}

ScanLifecycleObjects(
    ctx context.Context,
    state *logtailreplay.PartitionState,
    cursor LifecycleObjectCursor,
    maxObjects int,
    maxMetaBytes uint64,
) (LifecycleObjectPage, error)
```

接口从现有B-tree seek/iterator开始，最多返回`maxObjects/maxMetaBytes`，不会构造全表slice。
cursor只是进度hint：

- 一个cycle固定Metadata snapshot；
- snapshot已stale或Merge改变Object集合时，重新开始当前cycle；
- 到末尾后必须wrap，避免新Object或排序在cursor之前的Object永久漏扫；
- `full_scan_interval`到期必须强制从头开启新cycle；
- `last_full_scan_at`超过SLO必须告警并停止继续放量；
- Candidate和cursor丢失可重建；
- final transaction始终以实时Metadata和exact source CAS为准。

该接口及其分页/重启/百万Object基准是Gate A的交付物，不需要WAL、Replay或Catalog逐Object
行。只有实测证明现有Metadata分页在1000绑定表认证负载下仍达不到Discovery SLO，才重新
评估可丢失的派生summary；即使增加summary，也不能成为退休正确性的事实源。

分类：

```text
max(time_col) <= cutoff     -> Whole hint
min <= cutoff < max        -> Mixed hint
min > cutoff               -> Not due
metadata缺失/不可信         -> Reader classification
```

`PartitionState`只负责有界列出当前Object。若Lifecycle列是sort key，可直接用
`ObjectStats.SortKeyZoneMap()`；否则按ObjectLocation有界range-read metadata ZoneMap area，
只加载该Column ID对应的物理seqnum，不读取数据行。metadata requests/bytes也受page硬上限。

final transaction从不信任cursor或Candidate。

初始并发：

```text
per table 1, per database 2, per account 4, cluster child 8,
cluster Rewrite 1, provider read/write分别限流
```

1000张绑定表是配置与认证上限，不实现分布式activation slot。

普通Merge不查询Lifecycle。Merge抢先时Lifecycle exact CAS失败；Lifecycle不要求Merge等待。

## 6. Source Snapshot与GC保护

Child冻结`source_snapshot_ts=S`。Lifecycle只执行一次Tombstone选择，返回S时Snapshot
Reader输入和对应exact identities；SyncProtection保护同一集合。只有有效RowID ZoneMap能
证明不相交时才排除Tombstone Object，unknown/legacy/截断/解码异常一律保守纳入，无法
解析则attempt失败，超限则`RESOURCE_BLOCKED`。

选择exact Data/Tombstone文件后：

1. 注册现有GC `SyncProtection`；
2. 注册成功后重新Stat并验证全部文件identity；
3. Reader、Provider、Rewrite和finalization共享创建时冻结的绝对deadline，首期不续长attempt；
4. final Prepare携带并验证job ID；
5. deadline到期、TN重启或保护丢失时fail closed；
6. 不在原staging上恢复attempt；
7. 不再读取source后释放；`COMMIT_UNKNOWN`不无限续租。

SyncProtection是可失效执行租约，不是长期Snapshot或新GC引用表。

## 7. Reader与Archive

### 7.1 Whole

按Object ID、Block ordinal、Row offset读取S时可见行。

### 7.2 Mixed物理合同

严格单source Object。`LifecycleRewriteHost.LoadNextBatch`返回完整物理Block，保持原始
Block顺序、行顺序和offset；D/E只写入delete bitmap：

```text
D snapshot-deleted -> no output / NoTransfer
E expired          -> Archive或discard / NoTransfer
L live             -> DoMergeAndWrite输出并产生destination
```

禁止先抽取L再组成新Batch。生命周期分类只在CN完成，TN不重算TTL表达式。

### 7.3 内存

读取Block前根据Object metadata/column extents保守估算：

```text
source vectors + merge output + parquet buffer +
transfer slab实际allocator capacity + safety margin
```

估算失败或超过认证上限，在读取前进入`RESOURCE_BLOCKED`。单Object流式处理不等于允许
任意oversize单Block。

### 7.4 格式与验证

Parquet/ZSTD文件保存size、SHA-256、ordinal、row count和必要min/max。Manifest保存文件集、
完整版本化逻辑schema descriptor、descriptor digest、canonical encoder version、content
hash和总行数。Manifest顶层包含`manifest_format_version=1`，Reader必须先按它选择parser，
未知版本fail closed。descriptor至少能重建稳定列顺序、列名、源Column ID、MO类型、
width/scale、nullability、charset/collation和AUTO_INCREMENT属性；Manifest还保存每个
AUTO_INCREMENT列在归档数据中的最大正值并由full readback复核。Phase 1不恢复PK、索引、FK、
CDC、Publication、默认表达式、权限或策略。

descriptor中的Column ID明确命名为`source_column_id`，只用于lineage和源schema fence；
Restore目标列由普通DDL分配新ID，结构校验忽略源ID，不增加第二个持久
`restore_schema_digest`。

canonical encoder使用明确的row/column/type/null/length framing。一个Chunk严格等于一个
Manifest Parquet Row Group；按`(file_ordinal, row_group_ordinal)`升序展平，从0开始连续
生成全局`chunk_ordinal`。Restore不得按运行时bytes、Batch或worker版本重新切分。
Manifest必须保存`total_chunk_count`、`dataset_content_hash`和`hash_formula_version`。
`Dataset.content_hash`等于Manifest Hash，是按ordinal聚合
`chunk_ordinal/row_count/canonical_content_hash`的版本化SHA-256。source writer与full
readback decoder必须得到相同chunk和Dataset hash。Restore根据Chunk Receipt重建聚合结果，
不持久化SHA内部状态、不重新扫描隐藏表或全部Payload。readback失败不得进入final transaction。

每个Row Group还必须满足`max-restore-chunk-rows`和
`max-restore-chunk-logical-bytes`。`logical_bytes`按未压缩canonical encoder bytes计算；
Writer在越界前flush，单行自身超限则`RESOURCE_BLOCKED`。Manifest保存每个Row Group的
声明值，full readback和Restore按实际解码结果复核，不能使用压缩size替代。

## 8. Cleanup Root write-ahead

第一次Provider multipart/PUT、TAE live staging或external booking前必须创建Root，并预先
冻结两套namespace和可枚举范围。

- Archive按root prefix清理；
- live staging使用root-scoped唯一segment/range；
- external booking复用现有Merge codec，但通过Lifecycle-only path allocator在写前取得
  root-scoped不可变key；
- 不定义Lifecycle Booking V1；
- 写成功但进程未登记单个key时，prefix LIST仍可发现；
- 最大I/O窗口后再次LIST，迟到PUT重置quiescence；
- CLEANED tombstone保留到quiescence结束。

## 9. Whole退休

Whole Archive完成readback后，Whole TTL完成复核后，Finalizer创建短普通事务：

```text
insert Dataset or TTL Receipt
append thin LifecycleCommitEntry
commit immediately
```

Whole允许有界多源。所有source共享S、按Object ID排序；上限同时受Object数、source bytes、
Tombstone delta、wire bytes、文件数和wall time限制。任一source冲突则全事务abort。

## 10. Mixed Rewrite

`DoMergeAndWrite`生成live Object、CreatedObjs顺序和TransferTable。Lifecycle不得重新排序、
合并或修补mapping。

Finalizer将以下内容放入同一事务：

```text
Dataset/TTL Receipt
source Object drop
created live Object publish
survivor transfer
thin retire entry
```

复用现有external booking和Merge txn entry。Lifecycle wrapper只增加：

- `CollectDeletesFrom = source_snapshot_ts`；
- exact source identity；
- Root-owned物理staging；
- bounded Tombstone visitor；
- Archive missing mapping时abort；
- Dataset/TTL元数据原子发布。

## 11. Thin retire entry

普通Catalog DML不能表达TAE Object create/drop/transfer，因此在现有可重放commit payload中
增加一个内部tagged entry。V1使用`api.Entry.EntryType=7`和payload field=11；
`Entry.bat=nil`。它独立于普通空Batch，不暴露为SQL Write API。

V1字段：

```text
protocol_version / mode
root_id / attempt_id
dataset_id XOR receipt_id
database_id / logical_table_id / physical_table_id
binding_generation / schema_digest
source_snapshot_ts
data source complete ObjectStats bytes
source_set_digest
created ObjectStats list（Rewrite）
external booking locations / transfer mapping digest（Rewrite）
delta limits / final prepare deadline / merge level
```

SyncProtection job ID复用现有`PrecommitWriteCmd.SyncProtectionJobId`，不在thin entry重复。
entry和`source_set_digest`只包含要退休的Data Object。Snapshot Reader使用的Tombstone
Object只属于CN/SyncProtection protection set，TN不得把它加入SoftDelete集合。

Archive事务必须包含Dataset普通写，TTL事务必须包含TTL Receipt普通写。Finalizer私有持有
TxnOperator；workspace仅增加一个普通事务恒为nil、不会参与dump/compact/sort的
`lifecycleCommitControl`可选指针。`genWriteReqs`在普通entries后追加它，Finalizer随后立即
Commit。不增加公共Transaction状态机、Pair Token、Terminal Journal或Restore entry。

TN在任何mutation前验证entry结构、物理table/schema、限额、protection和exact source；
TN不查询tenant Lifecycle Catalog。source identity不一致、Drop Intent/EOB或物理table/schema
不匹配均abort。EOB不代表本attempt已提交。

TN限额校验只拒绝畸形、伪造或超过发布硬上限的entry，必须发生在Booking I/O和TAE mutation
前；它不是资源繁忙型admission，不增加Lifecycle permit或`RESOURCE_BUSY`重试语义。合法
entry继续复用普通Merge/TXN资源路径。

retirement上线采用两步发布：

1. 先把unknown Entry/version在Batch解析前fail closed的安全解析部署到全部TN，只开放
   Export-only；
2. 再发布会产生V1 entry的CN；全部CN/TN完成升级后才打开
   `lifecycle-retirement-enabled`。

具备安全解析但不支持V1的TN必须返回typed unsupported且没有TAE mutation；更老TN不在
retirement兼容集合内，发布控制面禁止路由。不为此建设HAKeeper capability协议。降级前
关闭retirement并等待`FINALIZING/COMMIT_UNKNOWN`收敛。

## 12. Post-S Tombstone

从`S`扫描到Prepare：

```text
命中L且mapping有效      -> transfer到新RowID
Archive命中E/NoTransfer -> abort
Whole Archive有命中     -> abort
TTL命中E                -> 可忽略；首版允许保守abort
越界/nil mapping/错误    -> abort
```

visitor按单source Object处理，必须有内部deadline和可执行的内存/工作量上界。phase错误不得
吞掉。Prepare后旧RowID事务依赖现有TAE冲突语义。

## 13. WAL、Replay与GC

- WAL/Replay记录普通TAE source drop、created live Object和Tombstone；
- Provider Payload/Manifest不进入TAE WAL；
- external booking供现有Merge Prepare/retry重建运行时transfer；
- 不承诺Replay恢复历史transfer page；
- committed后旧TAE文件由现有GC删除；
- Lifecycle不直接Delete源Object文件。

普通MO的retry、duplicate Commit和事务终态语义原样复用。Lifecycle不修复通用事务问题。

## 14. 结果对账

Root `VERIFIED -> FINALIZING`使用本Root的`root_id/attempt_id/executor_epoch/state_version`
CAS。它只决定worker ownership，不锁整张Binding；多个Root可以FINALIZING。

```text
matching Dataset/TTL Receipt存在 -> PUBLISHED
明确事务失败且matching记录不存在 -> DELETE_PENDING
结果或Catalog可见性不确定         -> COMMIT_UNKNOWN
```

EOB或Drop Intent不能单独触发Root清理。任一`COMMIT_UNKNOWN` Root暂停该Binding全部新
retirement，不为精确overlap建设Object列表；达到数量/bytes上限暂停Lifecycle并告警。
长期未知由运维处理。

## 15. 管理路径依赖与DDL fence

最终实现采用已有Catalog行组成的薄fence：

1. `SET LIFECYCLE`和表DDL复用`mo_tables`行锁；
2. 只有`SET LIFECYCLE`在持有表锁后更新一次system account的`LIFECYCLE` feature row，
   形成与scope级依赖发布之间的write barrier；普通表DDL只做索引化Binding lookup；
3. Snapshot/PITR/Publication/Clone/Branch创建先跨feature-row barrier，再查询目标scope
   Binding；CDC复用PITR准入；Lifecycle retirement gate开启时物理Backup全局fail closed；
4. 绑定表的不兼容DDL直接拒绝，DROP在同一barrier下删除Binding；
5. Finalizer重新校验Binding generation、physical table、schema digest、Lifecycle列和exact
   source Object；
6. 未绑定表不创建Feature Guard、active-attempt或dependency行，普通查询/DML/Merge不访问
   该barrier。

`SET LIFECYCLE`锁顺序固定为`mo_tables -> feature row`；只需要全局scope的管理操作仅取得
feature row，普通表DDL不取得feature row。
若测试暴露普通MO通用事务/DDL缺陷，走公共Issue，不为Lifecycle增加分布式状态机。

## 16. TTL小Mixed

仅TTL允许。使用固定SI事务读取RowID和真实PK/fake PK，按现有`Relation.Delete`删除，并在
同事务写TTL Receipt。rows、预计Tombstone bytes、affected blocks、事务时长和backlog任一
超限就改走Rewrite或`MIXED_LAYOUT_BLOCKED`，不得无限拆分重试。

这是可关闭的性能优化，不是Whole/Rewrite核心GA前置。Gate F未通过时关闭该路径，所有TTL
Mixed走Rewrite或Blocked。

## 17. Restore与Purge

Dataset一次最多一个Restore lease。初始化先使用一个普通事务原子完成Dataset lease CAS、
hidden table CREATE和Restore Attempt INSERT，禁止隐藏表先于Attempt Owner提交：

```text
CAS lease + CREATE hidden + INSERT IMPORTING Attempt in one ordinary transaction
-> read/verify Manifest and files
-> serial chunked normal INSERT + Receipt/Attempt progress in the same transaction
-> rebuild ordered content hash from Chunk Receipts
-> verify schema/rows/content hash
-> ordinary transaction:
     CAS matching unexpired Dataset lease
     CAS Attempt IMPORTING -> PUBLISHING/lease/chunk progress/rows/verified hash
     verify exact hidden-name + database ID + table ID
     ValidateAutoColumnOffset + SetOffset(archived positive max, same TxnOperator)
       for AUTO_INCREMENT columns
     atomic rename/publish
     Attempt.verified_content_hash = recomputed dataset_content_hash
     Attempt DONE + clear lease + increment Dataset.version
```

Chunk事务不更新Hash；最终发布事务验证Receipt严格覆盖
`0..Manifest.total_chunk_count-1`，并一次性写入`verified_content_hash`。每个Chunk必须在
转换成最终MO vectors后再用canonical encoder重算Hash，不能Hash Parquet中间表示后直接
INSERT。`SetOffset`参数是已恢复最大值本身；类型上限保持allocator耗尽语义，不做`max+1`。

`PUBLISHING`只允许作为上述最终普通事务内部的CAS中间值，不单独提交；事务回滚后仍是
`IMPORTING`，提交后直接是`DONE`。owner丢失或响应未知时按一致性Catalog身份对账：
target名称映射到同一table ID则停止
清理；hidden名称仍精确映射且target不映射时，允许清理事务与迟到发布事务通过普通WW
conflict决胜；两个身份均不匹配或矛盾时fail closed。失败清理使用一个短普通事务CAS非DONE
Attempt，并确认Catalog中当前名称仍是
`__mo_lifecycle_restore_<restore-id>`且database/table ID完全匹配，再按隐藏名DROP。禁止
仅凭`staging_table_id`删除；CAS或身份校验失败先重读Attempt，`DONE`或目标名已映射到相同
table ID时立即停止。cleanup `ErrTxnUnknown`时不盲目重试DROP，重新做身份对账。
重读到`FAILED + hidden absent + target不映射该ID`可确认清理已提交；非终态且hidden精确
存在才允许重新发起完整清理事务，其余矛盾组合fail closed。

每次GET前检查Attempt固定deadline，每个Chunk事务CAS Attempt lease/deadline/ordinal，
最终发布再CAS Dataset lease。Purge只有在Dataset没有有效lease时才能CAS到
`DELETE_PENDING`并递增access generation；显式Purge遇有效lease返回
`RESTORE_IN_PROGRESS`，后台Purge延迟重试。进入`DELETE_PENDING`后不存在允许继续读取的
旧Restore。Restore发布和Purge写同一Dataset行，任何竞态只能一方提交。Sweeper异步删除，
Purge事务不等待lease或Provider。

DROP沿用普通MO语义，不等待Provider，也不保证DROP后Restore。后台从现有Binding/Dataset/
Root状态发现孤儿并清理，不建立owner tombstone。

TTL Receipt、PURGED Dataset、CLEANED Root和终态Restore记录按审计窗口分页回收；每类记录
都有rows/bytes hard cap，达到cap只暂停Lifecycle。精确保留条件见
[01-product-sql-catalog-cn.md](01-product-sql-catalog-cn.md)。

## 18. 错误与资源

主要状态：

```text
RESOURCE_BLOCKED
CONFLICT_BLOCKED
MIXED_LAYOUT_BLOCKED
COMMIT_UNKNOWN
ARCHIVE_VERIFY_FAILED
STAGE_CREDENTIAL_UNAVAILABLE
RESTORE_LEASE_EXPIRED
CLEANUP_FAILED
```

所有Provider I/O有deadline/retry budget；所有队列有rows/bytes上限。Lifecycle通过
Scheduler/CN并发、单源Rewrite、entry解码前硬上限和active-coexistence门禁
约束资源。首个GA不增加TN Lifecycle专用permit，也不承诺比普通Merge更强的TN资源保证。
认证负载触发公共Merge资源缺陷时，记录/修复公共MO Issue或降低认证上限，不能用Lifecycle
私有状态机掩盖。

Mixed Rewrite受`live_logical_bytes/expired_logical_bytes`上限和账户/集群固定窗口source
bytes预算约束；Restore按Dataset logical bytes限制账户/集群active staging总量。预算由
现有Lifecycle Coordinator管理，不增加Catalog Slot；Coordinator切换时Rewrite预算保守
关闭到下一窗口，Restore计数从Attempt/Dataset重建。

## 19. P0测试

必须覆盖：

1. 完整物理Block输入、D/E/L交错和mapping producer唯一性；
2. Archive-before-retire与Dataset/Object mutation原子性；
3. Stage位置不可变、credential轮换和服务重启；
4. Root-before-side-effect、写后登记前crash、迟到PUT；
5. 各SQL类型canonical hash、Manifest版本、Chunk有序聚合和full readback；
6. 最大Object、oversize Block、Rewrite amplification/window bytes和Restore staging拒绝；
7. Restore原子初始化、中断、同ordinal相同/不同digest、chunk重放和Purge active-lease拒绝；
8. S前/S后DELETE、NoTransfer、Whole Archive并发DELETE；
9. attempt deadline到期、SyncProtection丢失和TN重启；
10. 相同/重叠/不相交source的并发final transaction；
11. final response lost、matching Dataset优先和EOB不误清理；
12. 普通Merge抢先、CN/TN crash、WAL replay和GC；
13. DDL fence最后Gate的DROP/TRUNCATE/ALTER/UNSET竞态；
14. AUTO_INCREMENT最大正值readback、发布SetOffset、类型上限和overflow；
15. final txn内部Attempt CAS/SetOffset/Rename/DONE/lease释放各点crash、发布/清理竞争和
    cleanup unknown；
16. feature off和无Binding的普通MO回归。

## 20. 实施顺序与代码边界

```text
A Catalog/Binding/Discovery
B Exact Reader/canonical encoder/Parquet format prototype
C Cleanup Root/Stage identity/full readback/Export-only/Sweeper
D thin entry + Whole exact retire
E single-source Rewrite + post-S Tombstone
F TTL small Mixed
G Restore/Purge lease
H DDL fence tests and minimal implementation
I 1/10 TiB, 30-day soak, 50→1000 rollout
```

预期代码边界：

```text
pkg/frontend or pkg/sql/compile       SQL/Binding/DDL final Gate
pkg/vm/engine/disttae/lifecycle       Discovery/Reader/Rewrite/Finalizer
pkg/vm/engine/tae/rpc                 thin entry dispatch
pkg/vm/engine/tae/tables/txnentries   narrow Lifecycle wrapper
pkg/fileservice / Stage adapter       Archive provider
pkg/taskservice                       Scheduler
```

每个Gate的Definition of Done：

- 单元、故障和并发测试通过；
- 明确资源Owner、deadline和hard cap；
- feature off不启动Lifecycle调度或数据路径；未绑定表的普通查询/DML/Merge零访问，相关
  表级管理DDL只复用`mo_tables`锁并执行一次索引化Binding lookup；
- `git diff --check`和Markdown检查通过；
- 设计与实现没有恢复本README已删除的协议；
- Gate H之前退休能力仅在受控无不兼容DDL环境验证，不能宣布GA。
