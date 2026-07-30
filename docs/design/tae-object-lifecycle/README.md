# MatrixOne TAE Object Lifecycle 详细设计索引

> 关联 Issue：[matrixorigin/matrixone#24552](https://github.com/matrixorigin/matrixone/issues/24552)、
> [matrixorigin/matrixone#24853](https://github.com/matrixorigin/matrixone/issues/24853)
>
> 上位设计：[MatrixOne TAE 对象级数据生命周期概要设计](../issue-24552-24853-tae-object-lifecycle-overview-design-cn.md)
>
> 状态：Commercial GA 实现级详细设计，当前代码尚未实现这些能力
>
> 决策：**Conditional Go**。本文档集定义 P0 协议和开发边界，不代表 P0 已通过或产品已经 GA。

## 1. 文档目标

这组文档把概要设计拆成可以直接分配给开发者的实现合同，回答以下问题：

- 对外提供哪些 SQL、状态和错误语义；
- 新增哪些 Catalog 表，谁创建、更新和清理每一行；
- Object Discovery 如何复用现有 Metadata，而不创建每 Object Catalog 行；
- Reader、Parquet Writer、conditional Lifecycle retire、Mixed Rewrite 和小 Mixed
  DELETE 的具体接口；
- Archive PUT、最终事务、commit unknown、DROP、Purge、Restore 如何收敛；
- 每个等待、租约、重试和积累项的上限；
- P0 原型必须证明什么，Commercial GA 必须通过哪些测试和放量门禁；
- 代码应按什么顺序、修改哪些文件、如何验证。

本文档集坚持四条工程约束：

1. 不修改普通 Merge 的候选算法、排序、普通重写语义、普通transfer格式和 GC
   谓词；Lifecycle使用独立Booking V1，只为正在执行的exact source Object增加窄
   reservation admission。
2. 不把 SQL Partition 当成数据安全边界。
3. 退休活动数据之前必须先证明 TTL 可安全删除，或 Archive 已可完整恢复。
4. 任何不确定、超限、版本不兼容或并发条件不满足的情况都 fail closed，源数据继续可见。

## 2. 文档列表

| 文档 | 唯一负责的内容 |
|---|---|
| [01-product-sql-contract-cn.md](01-product-sql-contract-cn.md) | 产品边界、SQL、权限、时间语义、支持矩阵和用户可见错误 |
| [02-catalog-state-machine-cn.md](02-catalog-state-machine-cn.md) | Catalog DDL、身份、版本、状态机、事务边界和资源 Owner |
| [03-object-index-planner-scheduler-cn.md](03-object-index-planner-scheduler-cn.md) | Object Discovery、分页游标、候选分类、Dry-run、Job 切分、调度和硬配额 |
| [04-reader-archive-format-cn.md](04-reader-archive-format-cn.md) | exact 逻辑 Reader、Rewrite borrowed Batch 合同、Parquet/ZSTD、Manifest、root 和 readback |
| [05-strict-object-retire-protocol-cn.md](05-strict-object-retire-protocol-cn.md) | Whole/Mixed Rewrite、CN commit-control、reservation/source protection、tagged entry、TN Prepare、transfer、WAL/replay和升级 |
| [06-mixed-delete-transaction-cn.md](06-mixed-delete-transaction-cn.md) | 小 Mixed 的可写 SI 事务、普通 DELETE、预算和并发冲突 |
| [07-attempt-cleanup-reconcile-cn.md](07-attempt-cleanup-reconcile-cn.md) | Attempt/Root、immutable key、commit unknown、迟到 PUT 和 Sweeper |
| [08-drop-purge-restore-cn.md](08-drop-purge-restore-cn.md) | DROP cascade、Purge、Restore lease、隐藏 staging table 和限制矩阵 |
| [09-observability-operations-cn.md](09-observability-operations-cn.md) | 指标、SHOW、告警、kill switch、容量模型和运维 Runbook |
| [10-p0-test-ga-acceptance-cn.md](10-p0-test-ga-acceptance-cn.md) | P0 证明义务、故障/并发/升级/规模测试和 GA 放量门禁 |
| [11-implementation-plan-cn.md](11-implementation-plan-cn.md) | 可执行代码任务、文件边界、依赖 Gate、认证顺序和 Review Owner |

## 3. 规范优先级

遇到冲突时按以下顺序解释：

1. 本 README 的全局不变量和术语；
2. 上表中“唯一负责”的详细设计文档；
3. 上位概要设计；
4. [对象执行边界 ADR](../issue-24552-24853-object-lifecycle-boundary-adr-cn.md)；
5. Review 输入和调研文档。

详细设计之间不得复制同一状态机或接口的第二份定义。其他文档只能引用负责该内容的文档。

如果实现必须偏离本文档：

```text
先修改负责该内容的详细设计
  -> 更新受影响的 P0/GA 测试
  -> 完成架构 Review
  -> 再修改代码
```

不能在代码中用 feature flag 偷渡尚未评审的第二套语义。

## 4. Commercial GA 固定范围

### 4.1 支持

- 只有显式表级 Binding；
- 生命周期列为 `NOT NULL DATE/DATETIME/TIMESTAMP`；
- TTL Whole Object；
- Archive Whole Object；
- 在硬预算内的小 Mixed Object 普通 RowID DELETE；
- 中/大 Mixed Object 使用独立 Lifecycle Rewrite；
- 无显式主键的普通表，使用 MO 已持久化 fake PK 作为 delete key；
- Parquet/ZSTD direct-readable Archive；
- 全量 readback 校验；
- Restore 到独立新表；
- DROP TABLE/DATABASE/ACCOUNT 后异步放弃并清理 Archive；
- 约 500～1000 张绑定表，1 TiB 常见单表，10 TiB 认证目标。

### 4.2 不支持

- 普通 SELECT 隐藏 TTL filter；
- ONLINE_COLD；
- restore-required Deep Archive；
- SQL Partition 依赖；
- 大 Mixed 普通 DELETE；
- 在普通 Merge worker 中执行 Lifecycle/Provider I/O；
- 修改普通 Merge 的候选、物理排序或 GC 策略；
- CDC、FK、Publication、Fulltext、Vector、插件和隐藏二级/唯一索引表；
- Lifecycle-aware Snapshot/PITR/Backup/Clone/Branch/DR；
- Legal Hold、WORM、maximum retention；
- DROP 后保证保留 Archive；
- Account/Database Policy 继承。

不支持项必须在 DDL 或操作开始前明确拒绝，不能静默产生不完整结果。

## 5. 唯一数据流

```text
ALTER TABLE ... SET LIFECYCLE
  -> existing mo_tables row lock串行化准入
  -> create Binding-scoped Feature Guard
  -> Binding ACTIVE
  -> Object Discovery 分页扫描当前 TAE Metadata
  -> Planner 形成有界 Candidate/Child
  -> Child Executor reserve exact Object 并注册 source protection
       |
       +-- Whole TTL
       |     -> final transaction
       |     -> Receipt + tagged LifecycleCommit in one commit payload
       |
       +-- Whole Archive
       |     -> exact Reader
       |     -> Root -> Parquet PUT -> readback -> VERIFIED
       |     -> Dataset + Receipt + tagged LifecycleCommit
       |
       +-- small Mixed TTL/Archive
       |     -> one writable SI transaction
       |     -> exact Reader
       |     -> optional Root/Parquet/readback
       |     -> Relation.Delete(RowID + delete key)
       |     -> optional Dataset + Receipt
       |
       +-- medium/large Mixed TTL/Archive
             -> one source Object per child
             -> LifecycleRewriteHost 复用 Merge block reader/writer
             -> one-pass split:
                  snapshot-deleted -> no output / NoTransfer
                  expired -> Archive/discard
                  live -> new TAE Objects + transfer destination
             -> verify outputs
             -> Dataset/Receipt + tagged LifecycleCommit
             -> source DropIntent + survivor-only transfer

final transaction response lost
  -> Attempt/Root FINALIZING
  -> normal Txn GetStatus + consistent Receipt reconciliation
  -> COMMITTED or ABORTED

Object retired
  -> existing TAE checkpoint/logtail/GC
  -> physical source object delete
```

## 6. 全局安全不变量

以下不变量适用于所有文档和代码：

### I-1 Archive-before-retire

非空 Archive 只有同时满足以下条件才允许退休源行：

```text
all payload PUT complete
AND all payload full readback complete
AND archive_rows == expired_visible_rows
AND source_visible_rows == expired_visible_rows + live_visible_rows
AND archive_content_root == verified expired-row root
AND created TAE Object rows == live_visible_rows
AND transfer map covers every live row exactly once
AND immutable Manifest VERIFIED
AND Root state == FINALIZING for the exact final transaction
```

### I-2 One final transaction

Dataset/Receipt 和活动数据退休必须在同一个 tenant 正常事务中提交。不能先删除源数据再 best-effort 发布 Dataset。

### I-3 Unknown is not aborted

`COMMIT_UNKNOWN`、超时、CN crash、TaskService lease 丢失都不能被解释成事务 aborted。结果未知期间：

- Root/Attempt Control 保留；
- Payload 不删除；
- live staging和external transfer booking不删除；
- 不创建新的 final transaction 重做相同退休；
- 不释放会导致错误 Restore/Purge 的所有权。

source protection 应尽力续租；若因 TN restart/TTL 失效，也只能让未提交 final
transaction失败，不能据此判断已经提交但响应丢失的事务结果。

### I-4 Discovery is a hint

Scan cursor、Candidate、可选 packed Summary、ZoneMap 分类和 Dry-run 只负责少读。
最终事务必须重新校验：

- 物理 table ID/schema generation；
- exact Object identity/state/stats digest；
- Binding/Guard/active attempt generation；
- reservation/protection token；
- Whole/Rewrite 的 Tombstone delta、row-conservation 和 transfer 条件。

### I-5 Existing GC owns TAE file deletion

Lifecycle 只提交 Object DropIntent 或普通 Tombstone。源 TAE 文件只能由现有 GC 物理删除。

### I-6 Immutable external identity

Archive key 永不覆盖、永不复用。Profile 的 namespace identity 不可原地修改；credential rotation 不得改变 namespace。

### I-7 One resource, one owner

Batch、txn、multipart、staging Payload、staging live TAE Object、Dataset、
Restore staging table和Discovery scan state/Candidate在任何时刻只有一个清理 Owner。
Owner转移必须持久化并CAS。

普通Merge的temporary `BookingLoc`可以在Prepare读取后删除；Lifecycle booking不可以。
它由Root持有到final transaction结果明确，之后才作为temporary child单独清理。

### I-8 All waits and growth are bounded

锁、Provider I/O、Reader、事务、重试、Discovery/Job backlog、Root、Tombstone、
transfer、retained bytes和staging object/table都必须有：

- 上限；
- 计量；
- 拒绝或暂停动作；
- 可达终态；
- 运维可见性。

### I-9 Ordinary Merge remains independent

Lifecycle只对实际执行中的exact source set申请短期reservation。普通Merge跳过或
拒绝该exact set，但不等待远端Archive、不读取Policy、不改变候选和排序。
final提交前reservation丢失时Lifecycle通过TN validation/exact Object CAS
abort/replan；final结果未知时不能启动第二次提交。旧TAE文件仍只能由现有GC删除。

### I-10 Capability before retirement

Safety Release先在所有TN部署“unknown EntryType在Batch解析前fail closed”，并把
capability从TN heartbeat经HAKeeper/ClusterDetails传播到`metadata.TNService`；
retirement在该版本永久关闭。Retirement Release中，只有全部相关CN/TN支持同一tagged
Lifecycle commit协议版本，且发送前
冻结的`ServiceID + ShardID + ReplicaID + protocol version`仍匹配，才允许真正退休数据。
Address发送前从权威cluster snapshot重新解析；不新造`TopologyGeneration`。refresh失败、
capability变化或不能唯一解析一个target shard都fail closed。Capability是发送fence，
unknown Entry parser是最后进程安全边界；缺任一层都不允许retirement。否则只允许：

- DDL；
- Object Discovery；
- Dry-run；
- Export-only；
- Restore 已有且兼容的 Dataset。

### I-11 Survivor-only transfer

Lifecycle Rewrite 必须启用现有 transfer 机制，但 transfer 只服务仍留在活动表的
`live_visible` 行：

```text
snapshot-deleted -> no output, NoTransfer
expired-visible  -> Archive/discard, NoTransfer
live-visible     -> new TAE Object, exactly one transfer destination
```

transfer 永远不修改 Archive Payload。`S` 后并发 DELETE 命中 live 行时转移到新
RowID；找不到有效destination（包括expired、snapshot-deleted、`NoTransfer`、
nil Block map或越界）时，整个attempt统一abort/rebuild。首个GA不区分Archive和TTL
的NoTransfer策略；TTL冗余DELETE忽略留作后续优化。不能受普通 Merge 的
`MO_COMMENT_NO_DEL_HINT` 配置影响。

### I-12 Root owns Lifecycle physical staging

Lifecycle 的 live staging Object 和 external transfer booking 在 final transaction
结果权威确定前由 Cleanup Root 唯一拥有。复用的 Merge transaction entry只是
Catalog/transfer 的借用者：

- Prepare/Rollback/`ErrTAENeedRetry` 不能删除 Root 资产；
- committed 且 matching Receipt 可见后，live staging 才转为 `TAE_OWNED`；
- aborted 后才进入 `DELETE_PENDING`；
- unknown 时保持原状态。

Lifecycle Rewrite 每个 child 只允许一个 source Object，并且一律使用 immutable
external booking；不允许 inline transfer 或多 source Rewrite。

### I-13 Whole proof 与 Booking V1

Whole child允许最多64个source，但每个source必须有同序、包含Object ID和ObjectStats
digest的`SourceLayoutProof`；`source_set_digest`覆盖source与proof全集。Rewrite仍严格单源。

Lifecycle Booking V1原样运输`DoMergeAndWrite`生成的TransferTable：编码每Block
实际物理行数和稀疏live destination，零mapping Block也不能省略；未出现的source
slot统一重建为`NoTransfer`。文件必须绑定Root child、TAE namespace、source layout
和`CreatedObjs` layout。它不编码D/E业务语义，也不允许重新生成或排序mapping。

### I-14 Rewrite 内存和读取前准入

Rewrite在`DoMergeAndWrite`创建dense slab前取得task级峰值memory token，并在每次
`BlockDataReadNoCopy`前按metadata column extent取得Block子token。估算未知、溢出或
超过`max_certified_block_read_bytes`时必须在payload读取前fail closed。3 GiB
Object仅在由多个认证Block组成时承诺streaming；首个GA不承诺任意oversize单Block/行。

### I-15 退化路径按真实副作用建 Root

attempt可预分配Root/segment/range/booking名称并加入SyncProtection，但不预创建
Catalog child。首次E行PUT前创建Archive child，首次L行交给writer前创建TAE range，
只有`live > 0`才生成booking；`visible == 0`没有Root，`live == 0`退化Whole时没有
TAE range/booking。

### I-16 WAL 不恢复历史 transfer page

WAL/Replay恢复source DropIntent、新live Object和final transaction Catalog/Tombstone，
不恢复已提交事务的历史运行时transfer page。Booking V1只服务当前final transaction
的Prepare/retry；TN restart后旧RowID事务缺页必须RW/WW conflict，不能静默成功。

### I-17 Retry generation 与 runtime Owner

Lifecycle tag必须位于可重放的`PrecommitWriteCmd.EntryList`中，但不能混入普通
`txn.writes`：CN使用独立、单值、immutable `LifecycleCommitControl`，在普通
workspace完成dump/compact/sort后由`genWriteReqs`原样追加。外部逻辑attempt冻结
entry bytes/digest、Root、Booking、绝对deadline和累计预算。Archive finalizer必须先
写Dataset+Receipt、TTL必须先写Receipt；内部adapter在逻辑写成功加入当前workspace后
返回txn-bound opaque `LifecycleCatalogPairToken`，Seal一次性消费。production不存在合法
control-only transaction，也不在workspace dump后重扫逻辑行或读取Object。

Lifecycle finalizer按需分配context；`nil`就是普通事务概念上的OPEN，Seal全部校验成功后
才分配初始SEALED context并进入：
`nil(OPEN) -> SEALED -> COMMITTING -> TERMINAL`；Seal后
任何外部workspace mutation转`POISONED`并只能full rollback。Commit内部使用专用helper
继续既有merge/dump/transfer；普通transaction context恒为nil，不分配Lifecycle对象。

每个内部TAE generation在任何SoftDelete/Create前，先从ephemeral TxnMemo取得唯一
`BUILDING` slot。Catalog node在对应API成功后归整个txn；slab/page/TransferDels在
`txn.LogTxnEntry`前归builder、成功后归txn entry；任何runtime cleanup都不得删除
Root-owned staging。失败的generation整体rollback，不能原地重建；G2使用全新slot和
私有entry。

累计retry budget由一次`HandleCommit`调用栈持有并传给G1/G2，普通路径不创建该对象。
重叠Commit由TxnService串行；terminal后迟到duplicate在Booking I/O前经deadline、admission
和exact-source preflight收敛，已退休source返回`LIFECYCLE_RECONCILE_REQUIRED`。V1禁止
增加进程全局无界replay memo；只有实证storage并行执行同一external txn时才加入有界registry。
TN admission只TryAcquire，busy在创建内部TAE txn前立即返回`RESOURCE_BUSY`，不排队。
普通Merge的同类注册前缺口由[#26445](https://github.com/matrixorigin/matrixone/issues/26445)
跟踪。

## 7. 权威数据与派生数据

| 数据 | 权威性 | 丢失后的结果 |
|---|---|---|
| Binding/Feature Guard | 权威 | 禁止执行或提交 Lifecycle |
| 当前 Relation/PartitionState/TAE Catalog | 当前 source Object 权威 | 无法校验则 fail closed |
| Scan state/Candidate/Packed Summary | 派生 hint | 重建或重新分页扫描，源数据不受影响 |
| Dataset Catalog + immutable Receipt | Archive 可见性权威 | 不允许猜测发布状态 |
| Manifest/Payload | Archive 内容权威 | 校验失败则 Dataset 不可恢复 |
| Attempt/Cleanup Root | 外部副作用所有权权威 | 不允许在 Root 前 PUT/live staging/booking write |
| TaskService task/epoch | 投递 hint | 从 Catalog lease 重新接管 |
| 普通 Merge/GC metadata | TAE 旧版本回收权威 | Lifecycle 不复制其职责 |

Feature Guard只属于active或仍在收敛的Lifecycle表；两类所有者合计受同一个
`max-bound-tables=1000`硬上限约束。未绑定表的普通DDL不创建Guard，卡住的Unbind也不能
通过持续绑定新表造成Guard无界增长。

## 8. 核心身份

所有接口和表使用以下身份，不用可重用名称代替：

```text
account_identity =
  account_id + account_incarnation

logical_owner_identity =
  account_identity + logical_database_id + logical_table_id

physical_source_identity =
  physical_database_id + physical_table_id + schema_generation

child_identity =
  job_id + child_generation

attempt_identity =
  child_identity + attempt_id + executor_epoch

archive_namespace_identity =
  profile_id + profile_version + storage_namespace_id

dataset_identity =
  dataset_id + manifest_root
```

数据库名、表名、Profile 名只用于显示。Rename 不改变 logical ID；TRUNCATE/ALTER COPY 必须改变 physical identity 或 Guard generation。

## 9. 术语

| 术语 | 解释 |
|---|---|
| Active data | 普通 SELECT 可见、由 TAE Object/MVCC/Tombstone 管理的数据 |
| Whole Object | Metadata 能严格证明 Object 所有物理行均到期 |
| Mixed Object | 同一 Object 同时有到期和未到期行，或 Metadata 无法证明 Whole |
| Exact Reader | 只读调用方给出的持久化 Object/Block 集合，并应用指定 Snapshot Tombstone 的 Reader |
| Conditional Lifecycle Retire | 通过同一commit payload中的tagged Lifecycle entry提交，任一准入条件不匹配即整体abort |
| Lifecycle Rewrite | 到期行写 Archive/丢弃、存活行写新 TAE Object，并在短事务中整体替换 source Object |
| Source Reservation | TN 内存中的 exact Object 短租约，线性化普通 Merge/Lifecycle 准入；不替代 final CAS |
| Source Protection | 复用 GC SyncProtection 保护 source Data/Tombstone 以及 Rewrite final 前未入 Catalog 的 live/booking staging 文件 |
| Dataset | 一次原子发布的 Archive 逻辑单元 |
| Payload | Dataset 引用的不可变 Parquet 文件 |
| Manifest | Dataset schema、Payload exact identity、统计、checksum 和 content root |
| Attempt Control | system-owned 的执行和 final transaction 对账记录 |
| Cleanup Root | 第一次真实Archive PUT、TAE live staging或transfer booking副作用前创建的system-owned所有权记录；只预分配ID不算创建 |
| Receipt | 与退休同事务提交的不可变成功证据 |
| Commit unknown | final transaction 已发送，但客户端不能确定 committed/aborted |
| Tombstone delta | Archive source Snapshot 之后新提交、且指向 source Object 的删除记录 |
| Owner tombstone | DROP owner 的轻量 Catalog 事实，不是 TAE 行 Tombstone |
| Archive Profile | 不可变、版本化的外部存储 namespace 身份和可轮换 credential 引用 |

## 10. 开发和 Review 规则

- 协议实现先写 P0 failing tests，再写实现；
- 新增的 Catalog 状态必须有迁移、重启、接管和清理测试；
- protobuf 新 tag 不复用旧值；
- 普通 `SoftDeleteObject`、Merge、SELECT 和 GC 行为必须有回归测试；
- Provider fake 必须能注入 response lost、迟到 PUT、短读、checksum mismatch、LIST lag 和 Delete failure；
- 任何 panic/error/cancel path 都按 Q1-Q3 检查：
  - Q1：谁负责收尾？
  - Q2：谁通知等待者？
  - Q3：等待何时结束？
- 每个实现 PR 必须引用 [P0 与 GA 验收](10-p0-test-ga-acceptance-cn.md) 中对应 case ID。
