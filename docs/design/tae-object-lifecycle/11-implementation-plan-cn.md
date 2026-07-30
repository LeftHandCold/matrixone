# 实施计划、代码边界与交付 Gate

> 本文把概要设计拆成可直接领取的开发任务。分阶段只表示依赖和放量顺序，不允许把
> 缺少安全协议的数据退休路径当作 Preview 交付。

## 1. 交付目标

最终 Commercial GA 同时交付：

```text
TTL:
  Whole Object retire
  + small Mixed DELETE
  + medium/large Mixed Rewrite

Archive:
  direct-readable Parquet/ZSTD
  + Whole Object retire
  + small Mixed DELETE
  + medium/large Mixed Rewrite
  + Restore to a new table
  + Purge/owner-drop async cleanup
```

不在 GA 内：

- ONLINE_COLD；
- restore-required Deep Archive；
- Legal Hold/WORM/maximum retention；
- DROP 后保证归档仍可恢复；
- archive-aware Backup/PITR/DR；
- CDC、Publication、FK、隐藏二级/唯一索引、Fulltext、Vector 和插件表。

以上依赖必须在Bind与依赖创建两侧获取相同table DDL lock并双向检查；只有已绑定或
仍在收敛的表使用Feature Guard，未绑定普通表不创建Guard。

## 2. 不可破坏的边界

1. 普通 Merge 的 selector、level、sort-key overlap、small/vacuum、目标 Object 大小
   和调度频率不变。
2. 未绑定表的普通DML、查询和Merge不进行Lifecycle Catalog查询，不写
   Discovery/Candidate，不创建reservation/protection。仅不兼容DDL/依赖创建在已有
   `mo_tables`行锁内查询Binding；无Binding时不创建Guard并立即回到普通路径。
3. Planner/Candidate 不是权威；当前 Relation Metadata 和 TN Prepare exact CAS 才是
   Object 权威。
4. Archive Verify 完成前绝不退休源行。
5. Dataset/Receipt 与活动数据退休必须在同一个 tenant transaction。
6. 外部副作用前必须有 system-owned Cleanup Root。
7. 普通 TAE GC 只看到正常 DropIntent；Lifecycle 不直接删除 source Object 文件。
8. Lifecycle是`PrecommitWriteCmd.EntryList`中的显式tag，必须随commit payload在每个
   TN retry generation完整重放；capability gate阻止老TN收到该tag。
9. Mixed Rewrite 复用现有 Merge 原语，不 fork 一份 mergesort/Object Writer/transfer
   实现。

## 3. 推荐代码布局

```text
pkg/lifecycle/
  catalog/          # Binding、Guard、Dataset、Receipt、scan state、Candidate、runtime stats
  control/          # system Attempt、Cleanup Root、owner tombstone、Reconcile
  discovery/        # metadata page scanner、classification、optional summary
  planner/          # cutoff、batch、path、budget
  scheduler/        # fairness、quota、child claim
  reader/           # exact Reader adapter、projection、canonical root
  archive/          # Parquet/ZSTD、Manifest、Provider readback
  executor/         # Whole、小 Mixed、Rewrite 编排
  restore/          # hidden staging、chunk receipt、atomic publish
  sweeper/          # payload/live staging/orphan cleanup
  observability/    # metrics、trace、invariant checker

pkg/vm/engine/disttae/
  lifecycle_scan.go
  lifecycle_rewrite.go
  lifecycle_commit.go

pkg/vm/engine/tae/db/merge/
  reservation.go

pkg/vm/engine/tae/rpc/
  handle_lifecycle.go

pkg/vm/engine/tae/tables/txnentries/
  lifecycle.go       # 组合/复用现有 merge create/drop/transfer entry
```

目录名是建议边界，可以按仓库 package cycle 调整；协议和 Owner 边界不能因目录调整
而合并。

## 4. Gate S：Safety Release

Gate S先于任何会退休数据的代码发布。该生产版本中`retirement-enabled`不可打开，只
交付协议安全底座和只读/导出能力。

### S1. Unknown Entry fail-closed

在`ParseEntryList/handleRequests`进入`apiEntryToWriteEntry`和
`ProtoBatchToBatch`之前校验EntryType：

```text
known ordinary Entry -> require valid Batch -> existing path
known Lifecycle Entry -> route to Lifecycle parser
unknown Entry/version -> UNSUPPORTED_TXN_ENTRY
```

禁止panic、禁止忽略unknown tag后提交同一事务的其他Entry。Safety Release中的旧/新
混部测试必须证明unknown numeric enum和nil Batch只返回错误。

### S2. Capability权威传播

实现链路：

```text
TN supported LifecycleCommitVersion
  -> TNStoreHeartbeat
  -> HAKeeper TNStoreInfo
  -> ClusterDetails.TNStore
  -> metadata.TNService
  -> clusterservice authoritative refresh
```

需要修改：

```text
proto/logservice.proto
  TNStoreHeartbeat.lifecycle_commit_protocol_version = 12
  TNStoreInfo.lifecycle_commit_protocol_version = 12
  TNStore.lifecycle_commit_protocol_version = 12

proto/metadata.proto
  TNService.lifecycle_commit_protocol_version = 10

pkg/tnservice/store_heartbeat.go
pkg/pb/logservice state update + HAKeeper RSM/ClusterDetails
pkg/clusterservice/cluster.go
```

缺字段、heartbeat过期、Replica变化和传播层缺值一律为unsupported。CN只冻结
`ServiceID + ShardID + ReplicaID + protocol_version`，Address发送前权威重解析。

### S3. Safety Release exit

- 全部允许作为未来降级目标的TN均已具备unknown Entry fail-closed；
- capability从heartbeat到CN读回有golden/integration test；
- Discovery/Dry-run/Export-only可用；
- retirement kill switch不可绕过；
- 混部、TN迁移、heartbeat过期都不发送Lifecycle tag。

## 5. Gate A：Catalog、Feature Guard 与只读 Discovery

Gate A 没有 Provider PUT 和活动数据退休。

### A1. Bootstrap/upgrade Catalog

实现：

- Archive Profile/version 和 immutable storage namespace；
- Feature Guard；
- Binding；
- Discovery Scan State；
- 有界 Candidate；
- 每Binding固定hour/day bucket ring的Rewrite runtime stats；
- Dataset/Receipt；
- system Attempt/Cleanup Root/Root Object；
- owner tombstone；
- Restore Attempt/chunk Receipt。

要求：

- 所有唯一键包含 account incarnation；
- Binding 以 logical table identity 唯一；
- Candidate ID 使用确定性 digest；
- Root 与 tenant cluster table 分属 system retained/tenant plane；
- Archive Rewrite Root分别冻结Archive Provider和TAE FileService namespace/encryption
  identity，Root Object按kind只能选择对应identity；
- upgrade 可重复执行；
- downgrade 不删除未知状态和 Root；
- system 表均有分页键，禁止一行全局 cursor。
- Root Object主键固定为`(root_id, object_kind, kind-local ordinal)`，不同kind不共享
  全局ordinal分配器。
- Dataset/Receipt/Restore chunk只写transaction ID，不在原事务内填写CommitTS；
- Archive Root包含nullable `observed_commit_ts`，只由committed Reconciler从权威
  Txn GetStatus写入；该值未知时禁止Purge。

验收：

- bootstrap、upgrade、rollback fence 测试；
- 未绑定普通表DDL不创建Guard；Bind/依赖并发由同一`mo_tables`行锁串行；
- DROP ACCOUNT 后 Root/owner tombstone 仍可枚举；
- Profile version 被 Dataset/Root 引用时不能删除或重指 namespace。

### A2. Feature Guard 双向准入

所有相关DDL使用同一个现有`mo_tables`逻辑行排他锁；Guard只存在于ACTIVE Binding或
仍有child/unknown需要收敛的表：

```text
Lifecycle bind/unbind
CDC create/drop
Publication create/drop
FK create/drop
hidden index create/drop
TRUNCATE/ALTER COPY/DROP
```

Bind：

1. 获取table DDL lock并持有到事务终态；
2. 检查authoritative dependency；
3. 无不兼容依赖时插入Binding+Guard；
4. 与DDL在同一事务提交。

依赖/DDL：

1. 获取同一table DDL lock；
2. 读取Binding；
3. 无Binding时不创建/读取/CAS Guard，执行现有普通路径；
4. 有ACTIVE/reconciling Binding时读取Guard并拒绝或fence；
5. 多表操作按logical table identity排序加锁。

不能在锁外先做“没有Binding”快照检查。`max-bound-tables`按权威Guard行数计数，包含
ERROR/PAUSED等未解除Binding和DISABLING/reconciling owner；达到1000后新Bind fail
closed。Bind使用cluster-scoped capacity lock串行容量准入，锁顺序固定为
`capacity -> logical table identity`，并在锁内分页计算最多1000行的Guard权威计数、持有
到Bind事务终态。该锁只进入Bind控制面，不进入普通DDL、DML、查询或Merge；获取失败、
超时或计数不确定时拒绝新Bind。
Unbind只有在active child和unknown final transaction收敛后才能删除Guard，且旧Guard
删除前禁止同一logical table重新Bind。DROP TABLE在owner tombstone/fence与Catalog删除
同事务成立后可删除tenant Guard，Root继续负责异步Provider cleanup。

### A3. 分页 Metadata Discovery API

在 DistTAE 增加只读接口：

```go
type LifecycleObjectMetadata struct {
    Stats            objectio.ObjectStats
    StatsDigest      [32]byte
    LifecycleZoneMap objectio.ZoneMap
    RowCount         uint64
    BlockCount       uint32
    CompressedBytes  uint64
}

func ScanLifecycleObjectMetadataPage(
    ctx context.Context,
    rel engine.Relation,
    snapshot types.TS,
    lifecycleSeqnum uint16,
    afterObjectID []byte,
    maxObjects int,
    maxFooterBytes int64,
) (items []LifecycleObjectMetadata, next []byte, complete bool, err error)
```

实现约束：

- 从当前 Relation Metadata/PartitionState 分页；
- 每页先检查 object count/footer bytes/deadline；
- 不构造全表 slice 后再截断；
- Object ID 顺序在同一 Snapshot 稳定；
- ZoneMap 缺失、截断、类型未知只能分类为 `NEEDS_SCAN`；
- Merge 导致 cursor 前出现新 Object 时，由下一 full cycle 保证最终发现；
- `CollectObjectList`仅更新优先队列/watermark，断档时回退 full cycle。

### A4. Planner 与 Candidate

分类：

```text
max < cutoff                    -> WHOLE
min >= cutoff                   -> NOT_DUE
otherwise / metadata unknown    -> MIXED_NEEDS_SCAN
```

Planner 按 release profile 决定：

```text
WHOLE
SMALL_MIXED_DELETE
MIXED_REWRITE
RESOURCE_BLOCKED
MIXED_LAYOUT_BLOCKED
```

Candidate 每表、每账户和集群总量均有硬上限。达到上限暂停该 scope 的 Discovery，
不能扫描更多再丢弃结果。

Rewrite成本写入每Binding固定hour/day bucket ring，而不是Binding权威行或逐Object
Index：

```text
amplification =
  attempted_source_bytes
  / max(committed_retired_expired_bytes, 1)
```

failed/aborted attempt也计attempted和实际aborted read/write bytes；只有final commit
增加committed retired expired bytes。bucket CAS、rollover、restart恢复和阈值阻断
必须有并发测试。

Gate A exit：

- 1000 Binding 不访问任何未绑定表；
- 百万 Object 表 Catalog 仍只有 O(1) scan state + 有界 Candidate；
- crash 前后 cursor/Candidate 无丢页或错误退休；
- Dry-run 报告 Whole/Mixed/bytes 和估算放大；
- feature disabled 时普通 DML/Merge/GC 无新增写。

## 6. Gate B：Exact Reader、Archive 与 Export-only

Gate B 可以写 staging/export，但不退休活动数据。

### B1. Exact Reader

实现 `ScanExactObjects`：

- 输入持久化 exact ObjectStats/digest；
- 绑定固定 Snapshot；
- 排除 table workspace/in-memory row；
- 应用 Snapshot-visible Tombstone；
- 串行 callback；
- Batch borrowed ownership exactly once；
- Object/Block coverage 到 EOF；
- stable canonical root。

先完成 fault injection：

- Object missing；
- block short read/checksum；
- callback error/panic/cancel；
- Batch reuse/double clean；
- 0 visible complete；
- Object metadata extent未知/溢出/认证上限±1；
- 实现可供Rewrite复用的保守Block峰值估算器，Reader在payload读取前准入；
- 3 GiB多Block Object streaming；
- 未认证oversize Block/varlen row在payload读取前拒绝。

### B2. Archive Writer

实现：

- Parquet + ZSTD；
- field ID 和 MO type/schema digest；
- multipart bounded pipeline；
- deterministic immutable key；
- Manifest V1；
- full Provider readback；
- payload/root/content root 校验。

ETag、HEAD size、Footer 或 sample 不能替代全量 readback。

### B3. Cleanup Root observer

ArchiveStore 每次副作用前：

```text
Root committed
Root Object ALLOCATED committed
then PUT/multipart
```

每次 multipart create、part complete、PUT response、provider version 都通过 observer
写回 Root。observer 持久化失败时停止后续 I/O，把所有权交给 Sweeper。

Gate B exit：

- Export-only 可恢复解码且 root 一致；
- 任一 crash 后没有不可枚举外部对象；
- stale writer 使用 new attempt/new prefix；
- Provider 429/5xx/timeout 下 memory/goroutine 有界；
- 不存在活动数据 DropIntent/DELETE。

## 7. Gate C：Reservation、GC Protection 与 Lifecycle Wire

Gate C 先在测试表上验证协议，不对客户表开放。

### C1. Exact source reservation

在 TN 增加 in-memory manager：

```go
type LifecycleReservationToken struct {
    AttemptID     uuid.UUID
    ExecutorEpoch uint64
    TableID       uint64
    Generation    uint64
    ReservedMode  LifecycleCommitMode
    ObjectDigest  [32]byte
    SourceLayouts []SourceLayoutProof
    ExpiresAt     time.Time
}

ReserveLifecycleSources(...)
RenewLifecycleReservation(...)
ValidateLifecycleReservation(...)
ReleaseLifecycleReservation(...)
IsReservedByOther(...)
BeginMergeAdmission(...)
EndMergeAdmission(...)
```

接入点：

- Merge scheduler 在选择时跳过 reserved Object；
- TN `OpCommitMerge` final admission 在 manager shard 中取得短
  `MergeAdmissionTicket`；它覆盖检查 reservation 到安装全部 source DropIntent 的
  窗口；
- DropIntent/txn entry安装后释放 ticket，由 Object MVCC 接管互斥；
- CN/user forced Merge 最终也走相同 TN admission；
- tagged Lifecycle entry校验自己的 token。

reservation 不写 WAL。TN restart 后 token 丢失，Lifecycle final 必须失败并 replan；
不能从 Job row 猜测恢复。

普通 Merge ticket 的 success/error/panic 路径都 exactly-once 释放。manager 必须按
table/object 分片；禁止单全局 mutex。ticket 不覆盖 `DoMergeAndWrite` 或 commit
等待，因此不会把 Lifecycle lease 变成普通 Merge 长锁。

### C2. Exact source GC protection

复用 `pkg/vm/engine/tae/db/gc/v3/sync_protection.go`：

1. capture `source_snapshot_ts`；
2. 枚举 exact source Data Object 和相关 Tombstone Object filename；
3. Rewrite预分配Root/segment/range/booking确定性名称，但不创建Root/child；
4. 把source文件和全部未来可能生成的live/booking文件名加入同一BloomFilter；
5. GC cycle 空闲时注册；
6. 注册成功后才允许读取或写staging；
7. build 期间续租；
8. TN Prepare 调用 `ValidateSyncProtection`；
9. commit/abort 明确后释放。

GC 正在运行时注册失败必须 retry。deadline 只能触发停止和告警，不能把
`COMMIT_UNKNOWN`猜成 aborted。

当前manager不支持扩展已有BloomFilter；Writer越过已冻结range必须在下一次物理写前
停止，不能写完后补保护。

### C3. 可重放 tagged wire

在 `proto/api.proto` 增加：

```text
api.Entry.EntryType.LifecycleCommit = 7
api.Entry.lifecycle_commit_payload = 11
LifecycleCommitEntry V1
LifecycleCommitMode:
  WHOLE_TTL
  WHOLE_ARCHIVE
  MIXED_REWRITE_TTL
  MIXED_REWRITE_ARCHIVE
```

Entry 冻结：

- protocol/version/mode；
- account/table/schema/Binding/Guard generation；
- attempt/executor/reservation/protection identity和expected filename-set digest；
- source Snapshot、exact source Object/digest和TN生成的同序
  `repeated SourceLayoutProof`；
- created live Object及`created_layout_digest`；
- immutable external transfer booking exact identity/layout digest；
- Archive Dataset/Manifest/root；
- source/expired/live counts 和 conservation roots；
- deterministic entry digest。

Finalizer不调用独立`TxnOperator.Write`，也不能把`bat=nil` tag混入普通`txn.writes`。
它通过唯一adapter写入Dataset/Receipt普通Catalog DML，并设置一个单值、深拷贝、
immutable `LifecycleCommitControl`。普通workspace完成dump/compact/sort后，
`genWriteReqs`把control原样追加到`PrecommitWriteCmd.EntryList`。TN必须在普通Entry
转换前拦截该tag。Finalizer预选绝对
deadline `D`，把它同时冻结到Root并设为commit context deadline，生成的
`TxnCommitRequest.DeadlineUnixNano`必须等于`D`。外部逻辑attempt还冻结entry
bytes/digest、Root、Booking、最大内部generation和累计预算；每个TAE generation从
Booking重建私有Catalog node、TransferTable和txn entry，严禁复用上一代指针。

生产finalizer没有合法control-only路径：Archive adapter必须先把Dataset+Receipt、
TTL adapter必须先把Receipt加入当前workspace，成功后返回包内不可构造、绑定txn ID、
attempt、pair digest和workspace generation的`LifecycleCatalogPairToken`。Finalizer调用
`SealLifecycleCommit(token, control)`一次性消费token；缺失、跨txn、statement rollback后
失效、workspace generation变化、重复消费或digest不一致均返回
`ErrLifecycleCatalogPairMissing`，禁止生成TxnRequest。

`genWriteReqs`只要求已消费token仍绑定当前txn且最终ordinary Entry非空，不在workspace
merge/dump后按逻辑主键扫描Dataset/Receipt。Lifecycle Cluster Table行可由现有workspace
转换为Object Entry；final commit不得为重新证明pair而读取FileService/Object。强制flush
和threshold下的原子性由opaque token、Seal后禁止statement rollback、现有workspace合同
和端到端测试证明。空ordinary Entry下append control仅保留为私有编码helper测试。

路由不使用不存在的`TopologyGeneration`。Finalizer和`genWriteReqs`在绝对deadline内、
在`txn.Lock`外刷新权威cluster snapshot，冻结/比较
`ServiceID + ShardID + ReplicaID + protocol_version`；Address每次发送前重新解析。身份、
capability变化、refresh失败或不能唯一解析一个source TN shard都fail closed/replan。

Whole的`source_objects`与`source_layouts`必须一一对应、同按Object ID排序，
`source_set_digest`覆盖两者；Rewrite两者基数都严格为1。每条proof再次包含Object ID和
ObjectStats digest，禁止proof跨Object错绑。

reservation冻结初始mode，只允许Rewrite退化为同类Whole/Empty；禁止Whole升级Rewrite
和TTL/Archive互换。

Lifecycle external booking使用独立Booking V1 envelope，冻结magic/version、Root
child/TAE namespace binding、每Block actual rows、原始sparse destination、
`CreatedObjs` layout、长度和digest。零mapping Block仍有record，未出现
的source slot重建为`NoTransfer`。Booking不编码D/E业务分类，也不得直接使用会省略
全空Block和Root/layout binding的普通`TransferHashPage.Marshal()`。

唯一CN/TN链路：

```text
Lifecycle finalizer adapter
  -> ordinary Dataset/Receipt Catalog writes
  -> internal adapter returns txn-bound LifecycleCatalogPairToken
  -> SealLifecycleCommit consumes token: nil(OPEN) -> allocate SEALED context
  -> Commit: SEALED -> COMMITTING
  -> ordinary workspace dump/compact/sort
  -> genWriteReqs appends tag without toPBEntry
  -> PrecommitWriteCmd.EntryList
  -> ParseEntryList/iterator intercept before apiEntryToWriteEntry
  -> HandleCommitLifecycle
```

实现文件：

```text
pkg/vm/engine/disttae/types.go
  LifecycleCommitControl/opaque CatalogPairToken/route identity
  optional *LifecycleFinalizeContext

pkg/vm/engine/disttae/txn.go
  internal Dataset/Receipt adapter token issue
  SealLifecycleCommit + external mutation poison checks
  commit-only internal merge/dump/transfer helpers
  rollback/finalize lifecycle

pkg/vm/engine/disttae/tools.go
  consumed token/current txn validation + authoritative route refresh
  exact single-shard tag append

pkg/catalog/tuplesParse.go（或等价iterator入口）
  preserve/intercept LifecycleCommit tag

pkg/vm/engine/tae/rpc/handle_lifecycle.go
  parse/validate/register
```

context/control/token不进入普通dump/compact/sort/PK dedup/statement offset/Batch GC；
一次txn最多一条control，Seal是一次性本地线性化点。payload在workspace dump和commit
retry后必须逐字节不变。普通txn的`lifecycleFinalize == nil`，`nil`就是概念上的OPEN且
不分配对象；只有Lifecycle Seal全部校验成功后才创建初始SEALED context并执行
`nil(OPEN) -> SEALED -> COMMITTING -> TERMINAL`，
SEALED/COMMITTING发生外部mutation时进入POISONED，只能full rollback。不得增加独立
opcode，也不得把可选字段塞入`OpCommitMerge`。

WAL/Replay只恢复source DropIntent、新live Object和final transaction的
Tombstone/Catalog状态，不恢复已提交事务的历史运行时transfer page。External
Booking只用于同一final transaction的duplicate Prepare/NeedRetry重建page；TN
restart后旧事务缺页必须RW/WW conflict。

### C4. TN Prepare 顺序

```text
validate absolute request deadline
pre-scan max-one Lifecycle tag
TryAcquire bounded Lifecycle commit-admission permit
  busy -> RESOURCE_BUSY before GetOrCreateTxnWithMeta
validate protocol/capability/digest
validate authoritative frozen ServiceID/ShardID/ReplicaID route
validate table/schema/Guard generation
validate reservation token
validate SyncProtection expected filename-set digest
validate exact current source Objects
if source is no longer exact: return LIFECYCLE_RECONCILE_REQUIRED before Booking I/O
validate one-to-one SourceLayoutProof list
validate created live Objects/checksum
claim generation-local slot before any TAE txn mutation
stream-validate Booking V1, CreatedObjs layout and mapping bounds/count/digest
bounded/cancelable collect and validate Tombstone delta
builder owns decoded runtime resources
register Root-owned merge create-drop-transfer txn entry
LogTxnEntry success atomically transfers runtime Owner to txn entry
append WAL through normal transaction path
```

任一条件失败，整个 transaction abort。
admission permit由一次`HandleCommit`持有并覆盖其所有internal retry generation；实现以
`defer`或等价的单一terminal cleanup保证在任一parse/preflight/Booking/TAE失败、commit
完成或deadline退出时exactly-once释放。不得按G1/G2重复获取。首个GA没有admission
waiter：TryAcquire失败立即返回，CN scheduler带有界jitter重新调度。普通Commit没有
Lifecycle tag时不触碰该permit。
generation slot存放在internal txn的ephemeral TxnMemo：

```text
NEW -> BUILDING -> REGISTERED
                -> FAILED -> whole generation rollback
```

只有BUILDING owner可以执行Booking decode、SoftDelete/Create、phase-1和LogTxnEntry。
SoftDelete/Create成功后Catalog node立即归整个txn，builder不得手动撤销；runtime资源
在Log前归builder、成功后归txn entry。follower有界等待/读取同一结果，不执行mutation。
当前`LogTxnEntry`用故障注入冻结“error在append前、nil在append后”，不新增receipt/CAS。
普通Merge的[#26445](https://github.com/matrixorigin/matrixone/issues/26445)独立修复，
Lifecycle Gate C只要求local builder/slot闭环，不强绑普通Merge改造。

`HandleCommit`预扫描max-one Lifecycle tag并创建唯一
`LifecycleReplayBudget`，同一指针传给G1/G2的`handleRequests`。它持有绝对deadline、
generation count、累计booking bytes、delta rows/bytes和CPU时间；每次generation及I/O
前先消费预算，超限terminal abort。budget随HandleCommit调用栈销毁，不建立进程全局
Map。同一internal txn内的重复注册通过generation slot成为follower。V1复用TxnService
对overlapping Commit的串行：terminal后迟到duplicate不依赖永久registry，而是在Booking
I/O前依次检查deadline、Lifecycle bounded admission、route、reservation/protection和
exact source。已提交source不exact时返回`LIFECYCLE_RECONCILE_REQUIRED`，由CN正常读
Root/Receipt收敛。只有Gate C实证发现同一external txn可并行进入storage HandleCommit时，
才增加有hard cap/TTL/terminal/deadline回收的registry。

Gate C exit：

- 1PC、2PC、duplicate Prepare、`ErrTAENeedRetry`多generation、response lost全矩阵通过；
- 私有空Entry编码helper能保留control；production路径永不允许control-only；
- production finalizer没有有效txn-bound token时不生成任何request；
- Dataset/Receipt force flush成Object Entry仍原子提交，final commit不重读Object或扫描
  逻辑行；
- route refresh不持有txn.Lock，ServiceID/ShardID/ReplicaID/capability或唯一shard不满足时
  fail closed；
- ordinary txn context恒为nil；Lifecycle state machine拒绝全部外部mutation，但
  COMMITTING的内部merge/dump/transfer可完成；
- generation slot在第一次TAE mutation前唯一，失败后同代不能重建；
- retry不延长绝对deadline/累计预算，budget有明确HandleCommit Owner，G2不复用G1
  可变指针；
- overlapping duplicate Commit由现有TxnService串行；迟到duplicate在Booking I/O前按
  deadline/source preflight收敛，并受Lifecycle admission硬限流；
- admission busy在创建内部TAE txn前立即返回，无TN waiter/queue；
- unknown EntryType在Batch解析前unsupported，不panic；capability端到端传播缺一层即
  unsupported；
- `LogTxnEntry`前后逐点故障注入无runtime资源泄漏或Root物理误删；
- old/new CN/TN 混部只允许 Dry-run/Export-only；
- TN restart 丢 reservation/protection 后不会退休；
- ordinary `OpCommitMerge`回归通过；
-现有GC最终回收已提交DropIntent，未提交source不受Lifecycle直接Delete。

## 8. Gate D：三条退休执行路径

### D1. Whole Object

Whole TTL：

```text
Metadata max < cutoff
  -> reservation/protection
  -> short final txn
  -> Receipt + tagged LifecycleCommit(WHOLE_TTL)
```

Whole Archive：

```text
reservation/protection
  -> Exact Reader at S
  -> Parquet/ZSTD + readback VERIFIED
  -> Root FINALIZING with txn identity/digest
  -> Dataset + Receipt + tagged LifecycleCommit(WHOLE_ARCHIVE)
```

final 只写 DropIntent；但由于没有new live Object/transfer phase，Prepare必须执行独立
的有界`ValidateNoPostSnapshotDelete(S, prepareTS)`。发现post-S删除或验证超限就
abort/re-export。FileService source delete交给existing GC。

### D2. 小 Mixed DELETE

只服务经认证的小尾部：

```text
one bounded writable SI transaction
  -> exact Reader at txn Snapshot
  -> optional Archive PUT/readback
  -> Relation.Delete(RowID + actual delete key)
  -> Dataset/Receipt in same txn
```

必须共享普通 DELETE 的单 PK、复合 PK、fake PK 编码。预测超限时在第一次 Delete 前
完整 rollback，并重plan为 Rewrite。

### D3. 中/大 Mixed Rewrite

每个child严格只有一个source Object，Build阶段不持有writable transaction：

1. 获取 reservation/protection；
2. 只预分配Root ID、segment/range/booking名称和protection set，不创建Root child；
3. 以 `source_snapshot_ts`逐 block 读取；
4. 得到 Snapshot delete bitmap `D`；
5. 计算 expired bitmap `E`；
6. Archive模式首次E行准备PUT前创建Root/Archive child，再按block/row顺序同步写入；
7. 把 `D ∪ E`交给现有 `DoMergeAndWrite`；
8. 首次L行、把Batch返回给mergesort前创建Root（若无）并提交
   `TAE_LIVE_SEGMENT_RANGE`；只有live row写入normal TAE Object；
9. live row 产生 destination，`D/E`保持 `api.NoTransfer`；
10. `TransferTable`只能使用本次`DoMergeAndWrite`的返回值；禁止Lifecycle重建、
    修改、排序或合并destination mapping；
11. actual writer只消费range内ordinal，Sync后按原始顺序追加exact child并冻结
    ObjectStats/checksum/`created_layout_digest`；
12. 只有`live > 0`才按Booking V1生成immutable external booking；每页在FileService
    write前动态分配Root child；inline transfer禁止；
13. Archive full readback 后进入短 final transaction。

当前 `PrepareNewWriter()`没有 error 返回值，禁止在其中做 Catalog transaction。
P0先证明 writer name 可由`segmentID + ordinal`稳定枚举，crash时 Sweeper遍历有限
range；证明不了时增加共享`BeforeCreateObject(name) error` hook，不允许写后才取得
所有权。

首个GA不允许多source Rewrite、substream merge或Rewrite排序spill。若现有host不能
证明单source内部按block/row递增，Lifecycle host显式按block ordinal驱动
`BlockDataReadNoCopy`。输入第二个source必须在任何Root/FileService/Provider副作用
前拒绝。

进入`DoMergeAndWrite`前必须取得覆盖dense slab、最大认证source Block、mergesort
output、Object Writer/index、Archive encoder、TN booking copy和安全余量的task父
memory token。每次`BlockDataReadNoCopy`前再以metadata column extent保守估算并取得
Block子token；估算未知、溢出或超过`max_certified_block_read_bytes`时，在payload
读取前fail closed。

Build 完成硬不变量：

```text
source_physical = snapshot_deleted + expired + live
source_visible  = expired + live
archive_rows    = expired                 # Archive
new_live_rows   = live
each live row has exactly one destination
snapshot_deleted/expired are NoTransfer
```

`source_visible==0`走EMPTY_ARCHIVE/Whole退休空Object；`expired==0 && live>0`结束为
no-op并清理staging；`live==0 && expired>0`必须把final mode切为Whole，Archive走
与TTL相同的独立post-S delete validator。禁止提交`createdObjs==0`的
`MIXED_REWRITE_*` entry。
`visible==0`不创建Root；`live==0`不得创建TAE range/booking；Planner预测为Rewrite
不能成为预创建空child的理由。退化路径必须有独立Root crash matrix。

新 live Object 仍按表的 physical sort/cluster key 使用现有 mergesort。不能按 lifecycle
column伪装成sorted。output level按普通Merge的晋级规则从source level计算，不能默认
为0。

### D4. Tombstone transfer

复用现有Merge两阶段算法，但必须通过Lifecycle wrapper参数化：

- `CollectDeletesFrom`必须等于`source_snapshot_ts`，不能使用几分钟后才创建的final
  transaction `StartTS`；
- phase 1收集`(source_snapshot_ts, collect_ts]`；
- phase 2收集`(collect_ts, prepare_ts.Prev()]`，覆盖Prepare并发窗口；
- phase 1使用request ctx；由于`PrepareCommit()`没有caller ctx，phase 2使用txn
  entry在首次构造时冻结的TN内部绝对deadline，并继续消费同一个visitor预算；
- duplicate Write/Prepare不得重置deadline或rows/bytes/blocks预算，禁止无界
  `context.Background()`；
- delta rows/encoded bytes/affected blocks都有hard limit；
- 任何scan/transfer error向上返回，不能吞掉；
- survivor delete transfer 到新 RowID；
- 任一post-S Tombstone遇到nil Block map、`NoTransfer`、越界或其他missing mapping，
  整个Lifecycle final transaction统一abort/rebuild；
- transfer page missing/expired/digest mismatch 均 fail closed；
- 首个GA不区分Archive/TTL/snapshot-deleted的NoTransfer策略；TTL冗余DELETE忽略只
  作为后续优化。

`LifecycleRewriteHost.DoTransfer()`固定true，不受普通Merge comment影响；但只有live
行有destination。运行时transfer page、destination语义和WAL不fork第二套实现；
外部持久化载体按本设计使用独立Booking V1 envelope。

外部transfer booking只复用必要的destination原语，不复用普通Merge的文件codec和
Prepare即删语义。实现独立Booking V1 writer/streaming validator，并保持
`writeTransferMapsToS3/marshalTransferMaps`普通路径不变：

- 普通Merge仍使用random temp key和load-and-delete；
- Lifecycle通过Root分配deterministic page key并在write前提交child；
- Lifecycle TN Prepare只读和校验，不删除，且不原地改写request；
- duplicate Prepare/`ErrTAENeedRetry`可重新打开同一immutable page；
- `SetTransferTable`后CN host唯一拥有producer slab，Booking VERIFIED或任一失败路径
  exactly-once Release；TN每个内部TAE generation首次注册时解码独立TransferTable，
  移交txn entry后由其exactly-once Release，移交前验证失败/cancel则由decoder释放；
  同generation duplicate命中memo，并发失败者只释放自己的私有decode；
- 在任何SoftDelete/Create前claim TxnMemo generation slot；只有BUILDING owner可以
  继续，失败后整代rollback且同代禁止重建；
- SoftDelete/Create成功后Catalog node归整个TAE txn，builder不得局部撤销；
- `txn.LogTxnEntry`成功是TN runtime Owner线性化点；此前builder失败或并发注册失败者
  释放slab/page/TransferDels，成功后只由txn entry释放；
- destination校验使用整个entry共享的一张全局bitmap，跨page重复/缺口都拒绝；
- 协议聚合digest只保留`source_set_digest/created_layout_digest/
  transfer_mapping_digest/entry_digest`四种名称和共享canonical codec；文件SHA、
  Object/layout proof和Protection digest保持各自显式名称，不能作为聚合digest别名；
- final结果明确后由Root child Sweeper删除booking。

复用的Merge txn entry增加构造期物理Owner：

```text
ordinary Merge    -> CreatedObjectOwnedByMergeEntry
Lifecycle Rewrite -> CreatedObjectOwnedByCleanupRoot
```

Lifecycle rollback/NeedRetry只回滚Catalog、transfer page和内存，不能物理删除
live/booking。committed + Receipt后live child转`TAE_OWNED`；aborted后Root清理；
unknown保持。Transfer dense slab admission按physical block slots计算，并把当前
mpool panic路径改成Lifecycle可返回error的checked allocation。slot数来自已认证的
`schema.Extra.BlockMaxRows/ObjectMaxBlocks`，token按allocator实际4 MiB/16 MiB量化
容量计费，不按默认常量或live rows估算。

代码任务边界：

```text
pkg/vm/engine/tae/mergesort/task.go
  add checked transfer-slab allocation path

pkg/vm/engine/tae/tables/table_scan.go
  add bounded/cancelable Tombstone delta visitor

pkg/vm/engine/tae/tables/txnentries/mergeobjects.go
  parameterize collect-from/context/delta limits
  Lifecycle wrapper uses strict post-S missing-mapping abort
  parameterize PhysicalCreatedObjectOwner
  preserve upstream #26333 phase-2 error propagation

pkg/vm/engine/tae/model/pages.go
  keep ordinary transfer codec compatible
  add versioned Lifecycle Booking V1 envelope/streaming validator

pkg/vm/engine/tae/iface/txnif/memo.go
  add ephemeral generation-local Lifecycle slot map
  do not serialize slots into WAL memo

pkg/vm/engine/tae/rpc/handle.go
  pre-scan max-one tag before internal TAE txn creation
  TryAcquire Lifecycle permit; busy returns RESOURCE_BUSY without waiter
  own LifecycleReplayBudget across internal retry generations
  reject unknown EntryType before Batch conversion

pkg/vm/engine/tae/rpc/handle_lifecycle.go
  claim slot before TAE mutation
  build/register Lifecycle-local txn entry

pkg/vm/engine/disttae/lifecycle_rewrite.go
  single-source host
  DoTransfer always true
  Root-aware immutable booking writer

pkg/vm/engine/disttae/types.go
pkg/vm/engine/disttae/txn.go
pkg/vm/engine/disttae/tools.go
  optional LifecycleFinalizeContext + opaque CatalogPairToken
  single immutable LifecycleCommitControl
  append control after ordinary workspace processing
```

这些改动必须保持普通Merge constructor的默认行为和wire字节兼容。不能直接让普通
Merge使用Lifecycle Root、strict missing-mapping规则或delta limit。

### D5. Final result/Root

明确 committed：

```text
Txn GetStatus == COMMITTED
AND consistent read at snapshot >= commit_ts sees matching Receipt
AND Archive sees matching Dataset/Manifest root
```

然后：

- Archive Root在同一system CAS持久化权威`observed_commit_ts = commit_ts`并转
  `PUBLISHED`；
- Rewrite live/range child -> `TAE_OWNED`；
- Rewrite booking child -> `DELETE_PENDING`；
- TTL Rewrite Root -> `POST_COMMIT_CLEANUP`，booking全部删除后 -> `TRANSFERRED`；
- normal TAE/WAL/GC 接管 live/source Object。
- runtime transfer page仍由现有transfer table TTL管理，不是WAL replay对象。

明确 aborted 才允许 Root -> `DELETE_PENDING`。仍 unknown 时保留
reservation/protection、Root 和 staging，释放 worker slot并进入 Reconciler。
Dataset/Receipt/Restore chunk不在原事务中填写CommitTS；Root的`observed_commit_ts`
为NULL时禁止Purge，不能用worker wall clock或永久依赖Txn GetStatus替代持久化。

Gate D exit：

- 十项 P0 中 Reader、SI、wire、CN commit-control、generation slot、
  reservation/protection、Root、Rewrite、budget全部
  通过；
- Whole/small Mixed/Rewrite 每条路径都有 crash matrix；
- Whole Prepare/commit后旧RowID事务（含TN restart）必须冲突；Rewrite旧事务只能
  成功transfer或冲突，不能静默提交；
- producer属性测试证明`DoMergeAndWrite`生成mapping与writer输出顺序一致，Booking
  round-trip保持TransferTable逐项相同；
- `DELETE`提交于`(source_snapshot_ts, finalTxn.StartTS]`时仍被收集并正确transfer；
- post-S Tombstone命中nil map/`NoTransfer`/越界时Dataset、source退休和new live
  publish整体abort；
- source visible row 不出现缺失、重复或未归档即删除；
- 普通 Merge/SELECT/DML/GC 回归在阈值内。

## 9. Gate E：Restore、Purge、DROP 与 Reconcile

### E1. Restore

```text
select immutable Dataset set
  -> acquire access_generation lease
  -> create hidden staging table
  -> read/verify each Manifest/Payload
  -> bounded insert transaction per chunk
  -> immutable chunk Receipt
  -> full row/root verification
  -> atomic publish as a new table
```

要求：

- 不覆盖已有目标表；
- payload/Manifest/schema/root 任一不符不发布；
- response lost 依赖 Receipt 恢复；
- AUTO_INCREMENT 水位正确推进；
- cancel/worker loss 后 staging 可清；
- Purge 与 Restore lease CAS 同一 access generation。

### E2. DROP

保持普通 DROP 主流程：

- DROP TABLE/DATABASE/ACCOUNT 在本地事务写有界 owner tombstone；
- 不等待 Provider；
- 产品合同明确 DROP 后不再保证 Archive Restore；
- system Sweeper 按 account incarnation/table generation 异步清理。

不把 Archive payload 数量放大到 DROP 事务。

### E3. Purge/Cleanup

进入 `DELETING` 前：

```text
owner dropped OR purge_eligible_at reached
AND no Restore/read lease
AND final txn not in-doubt
AND CAS access_generation
```

正常retention Purge还要求matching Root的`observed_commit_ts`非NULL且minimum publish
grace已到；owner dropped按产品契约放弃Restore，可以绕过retention/grace，但仍不能
绕过in-doubt final transaction或active lease。

`DELETING`不可逆，不允许新 reference。按 exact key/version 删除；Provider 支持 version
ID/CAS 时按具体版本删除。全部 HEAD/LIST 确认不存在后 Root/Dataset 才进入终态。

Gate E exit：

- Restore round-trip root/row/type一致；
- Purge/Restore/DROP竞态只胜一方；
- account drop后所有Root最终可枚举清理；
- unavailable Backup/PITR/DR入口在执行前明确拒绝；
- Provider credential失效只进入`DELETE_FAILED`，不丢证据。

## 10. Gate F：运维、认证与 GA

### F1. Release profile

冻结至少：

```text
bound tables: 500 -> 1000 staged rollout
child concurrency: table 1 / database 2 / account 4 / cluster 8
Rewrite concurrency: table 1 / cluster 1 default / 4 certified hard max
Discovery page object/footer-byte limit
Candidate table/account/cluster limit
Reader batch/memory limit
max certified Block read bytes and pre-DoMergeAndWrite task memory token
small Mixed rows/ratio/delete-key/WAL limit
Rewrite single-source/live/dense-transfer/external-booking/delta/staging limit
Rewrite 24h/7d attempted/committed/aborted bytes and amplification threshold
Provider I/O/deadline/concurrency limit
Root/Object/orphan/backlog limit
reservation/protection lease and renew interval
```

任何默认无限值非法。

### F2. Observability

交付：

- `SHOW LIFECYCLE`、JOBS、DATASETS、RESTORE；
- Discovery cursor/lag/Candidate；
- execution path、expired/live/transfer；
- reservation/protection 状态；
- Root/staging/orphan/unknown；
- Rewrite amplification；
- Tombstone/Merge/GC backlog；
- Provider requests/bytes/cost；
- invariant checker 和 kill switch。

### F3. 认证矩阵

必须完成：

- 当前最大 rows/block、blocks/object、bytes/object；
- single block max varlen；
- 1 TiB 常见表全流程；
- 10 TiB 单表持续 Insert/Merge 七天；
- 高时间局部性、1%边界 Mixed、高度乱序三种 layout；
- “几乎全部存活”的最大 Rewrite；
- 1000 Binding、公平调度和无全局热点；
- 真实支持的 S3/OSS/COS/S3-compatible Provider；
- rolling upgrade/downgrade fence；
- 30 天 soak；
- Restore/Purge/DROP/GC/2PC chaos。

放量：

```text
Safety Release:
  retirement permanently disabled
  -> parser/capability/Discovery/Dry-run/Export-only certification

Retirement Release:
  Whole: 50 -> 200 -> 500 -> 1000 bound tables
  -> Small Mixed independent gate
  -> Rewrite independent gate
```

任一数据不变量失败立即关闭新 retirement，继续 Cleanup/Reconcile，不扩大下一阶段。
降级前关闭retirement并等待`FINALIZING/COMMIT_UNKNOWN`收敛；禁止降到缺少Safety
Release unknown-entry parser的版本。

## 11. 开发依赖图

```text
S1 Safe parser ─┐
S2 Capability  ─┼── Retirement feature gate
                │
A1 Catalog ─────┼── A2 Guard
             ├── A3 Discovery ── A4 Planner
             └── B3 Root

B1 Reader ────── B2 Archive

C1 Reservation ─┐
C2 Protection  ─┼── C3 Wire ── C4 TN Prepare
B3 Root        ─┘

A4 + B1 + C4 ───── D1 Whole
A4 + B1 + B2 ───── D2 Small Mixed
B2 + C4 + merge primitives ── D3/D4 Rewrite

D1/D2/D3 + E1 Restore + E2/E3 Cleanup ── F GA certification
```

可以并行开发 Catalog、Reader/Archive、reservation/protection，但第一个会退休数据的
测试必须等 C1～C4 和 Root 协议全部完成。

## 12. Code Review Owner

| 变更 | 必须 Review |
|---|---|
| SQL/Binding/Guard/support matrix | Frontend + Catalog |
| Exact Reader/delete projection | DistTAE + Transaction |
| Archive/Manifest/Provider | FileService + Restore |
| reservation/SyncProtection | TAE Merge + GC |
| CN commit-control/tagged entry/TN Prepare/WAL/replay | DistTAE + TAE Transaction + TxnService |
| Rewrite/transfer | TAE Merge + MVCC |
| Attempt/Root/Sweeper | TaskService + FileService |
| scale/kill switch/runbook | SRE + Release |

协议、MVCC、GC 和 replay 变更不能仅由 Lifecycle 模块自审。

## 13. 每个任务的 Definition of Done

每个任务提交必须包含：

1. 正常、失败、cancel、panic、restart/epoch 路径；
2. 资源 Owner 和终态；
3. 所有等待的 deadline/通知方/终止条件；
4. 有界增长和 hard limit；
5. unit + race + package integration；
6. fault injection；
7. metrics/trace/error contract；
8. 升级/降级行为；
9. 对普通 DML/Merge/GC 的回归证据；
10. 对应设计章节同步更新。

只有实现和[GA 验收矩阵](10-p0-test-ga-acceptance-cn.md)全部关闭后，才能把
Conditional Go 改为 Commercial GA。
