# MatrixOne TAE Object Lifecycle Commercial GA 实现设计

> 本文是首个 Commercial GA 的唯一实现规范。全局范围和不变量见 [README.md](README.md)。

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
Clone/Branch和插件组合。未绑定表不读取Lifecycle Catalog。

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
action                 TTL | ARCHIVE
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

### 3.2 Dataset

Archive成功发布一行：

```text
dataset_id
binding_id / binding_generation
logical_table_id / source_physical_table_id
source_snapshot_ts
source_set_digest
schema_digest
root_id / attempt_id
manifest_key / manifest_sha256
content_hash / row_count / logical_bytes
stage_identity
purge_eligible_at
state                  PUBLISHED | DELETE_PENDING | DELETING | PURGED | ERROR
access_generation
restore_lease_id nullable
restore_deadline nullable
publish_txn_id
version
```

Dataset本身是Archive发布权威，不增加Archive Receipt。

### 3.3 TTL Receipt

用于SHOW、审计和commit-unknown只读对账：

```text
receipt_id
binding_id / binding_generation
source_snapshot_ts
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
owner_account_id / logical_table_id / physical_table_id
executor_epoch
archive_namespace_identity / credential_handle
archive_prefix / manifest_key / manifest_digest
tae_namespace_identity / segment_id / booking_prefix / ordinal_upper_bound
source_set_digest
final_txn_id nullable
state
state_version
cleanup_after
last_list_at / quiescence_since
last_error
```

状态：

```text
REGISTERED -> UPLOADING -> VERIFIED -> FINALIZING
FINALIZING -> PUBLISHED | DELETE_PENDING | COMMIT_UNKNOWN
DELETE_PENDING -> DELETING -> CLEANED
```

不建立Root Object明细表。Manifest或确定性prefix是删除枚举来源。

### 3.5 Restore Attempt与Chunk Receipt

Restore Attempt保存Dataset、隐藏表、lease、deadline、进度和状态。Chunk Receipt使用
`(restore_id, chunk_ordinal, chunk_digest)`唯一键，跟对应普通INSERT同事务提交。

## 4. Stage合同

Binding、Dataset和Root冻结：

```text
stage_id, provider, canonical endpoint, region,
bucket/container, immutable prefix, encryption/KMS identity,
credential handle
```

Storage location有引用时不得原地修改。credential可轮换，但稳定handle必须在账户删除、
服务重启、Restore和Sweeper中仍可解析。首个GA不接受只存在于tenant行中的inline secret。

归档key固定为：

```text
<stage-prefix>/lifecycle/<root-id>/<attempt-id>/payload-<ordinal>.parquet
<stage-prefix>/lifecycle/<root-id>/<attempt-id>/manifest.json
```

attempt和prefix永不复用。

## 5. Discovery与调度

Scheduler维护内存中的Active Binding registry，只扫描显式Binding。每次读取一页当前
TAE Metadata，Candidate有数量/bytes/TTL上限，crash后允许重扫。

分类：

```text
max(time_col) <= cutoff     -> Whole hint
min <= cutoff < max        -> Mixed hint
min > cutoff               -> Not due
metadata缺失/不可信         -> Reader classification
```

final transaction从不信任cursor或Candidate。

初始并发：

```text
per table 1, per database 2, per account 4, cluster child 8,
cluster Rewrite 1, provider read/write分别限流
```

1000张绑定表是配置与认证上限，不实现分布式activation slot。

普通Merge不查询Lifecycle。Merge抢先时Lifecycle exact CAS失败；Lifecycle不要求Merge等待。

## 6. Source Snapshot与GC保护

Child冻结`source_snapshot_ts=S`，选择exact Data/Tombstone文件后：

1. 注册现有GC `SyncProtection`；
2. 注册成功后重新Stat并验证全部文件identity；
3. Reader期间周期续租；
4. final Prepare携带并验证job ID；
5. 续租失败、TN重启或保护丢失时fail closed；
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
schema digest、canonical encoder version、content hash和总行数。

canonical encoder使用明确的row/column/type/null/length framing。source streaming hash与
full readback decoder hash必须一致。readback失败不得进入final transaction。

## 8. Cleanup Root write-ahead

第一次Provider multipart/PUT、TAE live staging或external booking前必须创建Root，并预先
冻结两套namespace和可枚举范围。

- Archive按root prefix清理；
- live staging使用root-scoped唯一segment/range；
- external booking复用现有Merge codec，但key位于root-scoped prefix；
- 不定义Lifecycle Booking V1；
- 写成功但进程未登记单个key时，prefix LIST仍可发现；
- 最大I/O窗口后再次LIST，迟到PUT重置quiescence；
- CLEANED tombstone保留到quiescence结束。

## 9. Whole退休

Whole Archive完成readback后，Whole TTL完成复核后，Finalizer创建短普通事务：

```text
insert Dataset or TTL Receipt
append thin LifecycleRetireEntry
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
增加一个内部tagged entry。它独立于普通空Batch，不暴露为SQL Write API。

V1字段：

```text
protocol_version / mode
binding_id / binding_generation
logical_table_id / physical_table_id / schema_digest / lifecycle_column_id
source_snapshot_ts
source ObjectStats bytes/digest/is_tombstone list
source_set_digest
created ObjectStats list（Rewrite）
existing external booking locations/digest（Rewrite）
root_id / attempt_id / manifest_digest
SyncProtection job ID
delta limits / absolute prepare deadline
```

Archive事务必须包含Dataset普通写，TTL事务必须包含TTL Receipt普通写。Finalizer私有持有
TxnOperator，追加entry后立即Commit；不增加公共Transaction状态机、Pair Token、Terminal
Journal或Restore entry。

TN在任何mutation前验证字段、限额、protection和exact source。source identity不一致、
Drop Intent/EOB、binding/table字段不合法均abort。EOB不代表本attempt已提交。

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

EOB或Drop Intent不能单独触发Root清理。`COMMIT_UNKNOWN`阻止相同source新retirement；
达到数量/bytes上限暂停Lifecycle并告警。长期未知由运维处理。

## 15. DDL fence：最后实现Gate

本合同保留，但不阻塞前述Reader、Export、Whole/Mixed P0：

1. 先复现普通Merge与DROP/TRUNCATE/ALTER并发；
2. 若发现用户可见通用错误，单独提MO Issue并修公共路径；
3. 再测试Lifecycle按旧schema归档、finalization期间DDL的真实结果；
4. 只有Lifecycle语义仍有缺口时，为绑定表实现薄fence：
   - Finalizer获取现有`mo_tables`逻辑行锁；
   - 重新读取Binding generation、physical table、schema digest和Lifecycle列；
   - 不兼容DDL对已绑定表拒绝，或在同一锁内更新/删除Binding；
5. 不新增Feature Guard、active-attempt字段或分布式DDL状态机。

同表Scheduler默认finalization并发1。若最终采用Binding write CAS，它只串行短final
transaction；上传、readback和Rewrite不持有该行。

## 16. TTL小Mixed

仅TTL允许。使用固定SI事务读取RowID和真实PK/fake PK，按现有`Relation.Delete`删除，并在
同事务写TTL Receipt。rows、预计Tombstone bytes、affected blocks、事务时长和backlog任一
超限就改走Rewrite或`MIXED_LAYOUT_BLOCKED`，不得无限拆分重试。

## 17. Restore与Purge

Dataset一次最多一个Restore lease：

```text
acquire lease with fixed deadline
-> create hidden staging table
-> read/verify Manifest and files
-> chunked normal INSERT + chunk Receipt
-> verify schema/rows/content hash
-> ordinary DDL atomic rename/publish
```

每次GET/chunk前验证lease。Purge将Dataset CAS到`DELETE_PENDING`并禁止新lease；已有lease
只在固定deadline内续租，超时Restore abort并清理隐藏表。Sweeper异步删除，Purge事务不等待。

DROP沿用普通MO语义，不等待Provider，也不保证DROP后Restore。后台从现有Binding/Dataset/
Root状态发现孤儿并清理，不建立owner tombstone。

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

所有Provider I/O有deadline/retry budget；所有队列有rows/bytes上限；TN admission fail-fast，
不等待资源。Lifecycle资源池不能占用普通事务和Merge保底配额。

## 19. P0测试

必须覆盖：

1. 完整物理Block输入、D/E/L交错和mapping producer唯一性；
2. Archive-before-retire与Dataset/Object mutation原子性；
3. Stage位置不可变、credential轮换和服务重启；
4. Root-before-side-effect、写后登记前crash、迟到PUT；
5. 各SQL类型canonical hash和full readback；
6. 最大Object、oversize Block拒绝和并发资源上限；
7. Restore中断、chunk重放、Purge lease drain；
8. S前/S后DELETE、NoTransfer、Whole Archive并发DELETE；
9. SyncProtection续租失败和TN重启；
10. 相同/重叠/不相交source的并发final transaction；
11. final response lost、matching Dataset优先和EOB不误清理；
12. 普通Merge抢先、CN/TN crash、WAL replay和GC；
13. DDL fence最后Gate的DROP/TRUNCATE/ALTER/UNSET竞态；
14. feature off和无Binding的普通MO回归。

## 20. 实施顺序与代码边界

```text
A Catalog/Binding/Discovery
B Exact Reader/Parquet/full readback/Export-only
C Cleanup Root/Stage identity/Sweeper
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
- feature off与未绑定表无新Catalog访问；
- `git diff --check`和Markdown检查通过；
- 设计与实现没有恢复本README已删除的协议；
- Gate H之前退休能力仅在受控无不兼容DDL环境验证，不能宣布GA。
