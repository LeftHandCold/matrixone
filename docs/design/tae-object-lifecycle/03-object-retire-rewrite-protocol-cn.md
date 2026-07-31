# 03 Whole Retire与Mixed Rewrite协议详细设计

## 1. 唯一执行路线

```text
Whole TTL       -> exact Object retire
Whole Archive   -> Archive verify -> exact Object retire
TTL small Mixed -> bounded Relation.Delete
TTL large Mixed -> single-source Rewrite
Archive Mixed   -> single-source Rewrite
```

TTL small Mixed是可关闭的Gate F性能优化；未启用或认证失败时，TTL Mixed统一走Rewrite或
`MIXED_LAYOUT_BLOCKED`。不修改普通Merge候选、排序、writer、Level、WAL或GC。

## 2. Build阶段

### Whole

Whole Archive读取S时可见行并写Payload；Whole TTL只需重新确认分类和source。
Whole允许有界多源，所有source共享S并按Object ID排序。最终上限由：

```text
source object count
source bytes
tombstone delta rows/bytes
wire bytes
wall time
```

共同决定，不把“64”冻结为永久常量。

首个发布的保守Release Profile为：单个Whole child最多64个Data Object、累计source
origin bytes最多4 GiB，任一先到即切分；Mixed始终严格单源。Scheduler只在同一有界
Metadata page内合并连续Whole候选，因此一次child只创建一个Dataset/Root并执行一次完整
source-set exact CAS。4 GiB/64是可经认证下调的初始上限，不是持久格式合同；Manifest
chunk数、Tombstone protection、wire bytes和30分钟attempt deadline仍可更早阻止该批次。

### Mixed

严格单source Object。`DoMergeAndWrite`是以下内容的唯一producer：

- live Object；
- `CreatedObjs`顺序；
- TransferTable destination；
- writer输出布局。

Lifecycle不得重新排序、合并或修补mapping。

Phase 1只认证普通CN Merge当前实际生产的默认物理布局：
`BlockMaxRows=8192`、`ObjectMaxBlocks=256`。Lifecycle Rewrite在读取源Block之前检查
`SchemaExtra`；非默认布局返回`RESOURCE_BLOCKED`，不修改普通Merge producer，也不在
Lifecycle中实现第二套布局算法。未来若公共Merge原生支持参数化布局，再单独放开认证范围。

在调用writer前根据source rows/blocks/bytes和当前schema参数计算created Object与booking
page的保守upper bound，预分配唯一segment ID并写入Root。Lifecycle writer只能在该
segment/range内创建对象；超过upper bound立即失败，不在写后扩张Root cleanup范围。

输出Object Level不自创规则：thin entry携带单source当前`ObjectStats.GetLevel()`，
`CreateNonAppendableObject`沿用普通Merge的Level晋级/大对象判断，避免把高Level source
错误降回L0或修改普通Merge策略。

## 3. external booking复用边界

复用现有TransferTable编码和TN读取逻辑，不增加Booking V1。但当前
`writeTransferInfoToS3`内部生成随机临时文件名且成功后才返回位置，不满足Root
write-ahead所有权。

新增Lifecycle-only选项，不改变普通Merge默认行为：

```go
type TransferBookingWriteOptions struct {
    ForceExternal bool
    PathAllocator func(pageOrdinal uint32) (string, error)
    Owner         TransferBookingOwner // MergeTxn or LifecycleRoot
}
```

Lifecycle要求：

- `ForceExternal=true`；
- PathAllocator在写入前返回`root/attempt/booking-ordinal-write-id`；
- namespace已在Root持久化；
- 返回的BookingLoc顺序与写入page顺序一致；
- rollback只清理runtime资源，不删除Root-owned文件；
- 明确abort后由Root Sweeper删除。

`DoMergeAndWrite`返回后、Root进入`VERIFIED`前，Lifecycle必须按已持久化Root再次验证
实际输出所有权：每个`CreatedObjectStats.ObjectName.SegmentId`等于Root预分配segment，
Object ordinal唯一且小于`ordinal_upper_bound`；每个实际booking location都位于Root的
`booking_prefix`之下且至少存在一个external booking。任一不匹配立即失败并由该Root
异步清理，禁止把越界文件带入final transaction。这个校验只约束Lifecycle writer输出，
不改变普通Merge的路径或文件命名。

普通Merge传nil options，行为和临时文件命名完全不变。

Created Object物理Owner也使用默认不变的窄选项：

```go
type CreatedObjectPhysicalOwner uint8

const (
    CreatedObjectOwnedByTxnEntry CreatedObjectPhysicalOwner = iota // 普通Merge
    CreatedObjectOwnedByLifecycleRoot                              // Lifecycle
)
```

Lifecycle txn entry rollback只撤销Catalog/runtime，不直接Delete Root-scoped live文件；Root在
事务明确abort后清理。普通Merge保持现有`PrepareRollback`物理删除行为。

## 4. Finalizer

Finalizer私有创建普通`TxnOperator`：

```text
Archive:
  insert Dataset
  append thin LifecycleCommitEntry
  Commit immediately

TTL:
  insert TTL Receipt
  append thin LifecycleCommitEntry
  Commit immediately
```

不把TxnOperator返回通用SQL执行流程，不引入公共FinalizeContext、Pair Token或终态Journal。
Dataset/TTL普通写失败时不追加entry；entry构造失败时整个事务rollback。

CN提交链只增加一个薄的可选字段：

```go
// ordinary transaction: always nil
// Lifecycle Finalizer: set once, then Commit immediately
lifecycleCommitControl *LifecycleCommitControl
```

它不放入`txn.writes`，不参与workspace dump/compact/sort。`genWriteReqs`先生成普通
Dataset/TTL Receipt entries，再在编码`PrecommitWriteCmd`前追加该entry；生产finalizer
不允许control-only事务。普通entries为空或缺少对应Catalog写时返回
`ErrLifecycleCatalogWriteMissing`，不发送请求。

该字段只提供包内setter，整个TxnOperator由Finalizer私有持有，因此不增加公共
OPEN/SEALED/POISONED状态机。rollback/commit终态随workspace释放指针。

## 5. Thin LifecycleCommitEntry

tag进入可被现有commit request重放的`PrecommitWriteCmd.EntryList`，但不能伪装成空Batch
普通DML。V1冻结：

```proto
message Entry {
  enum EntryType {
    // existing 0..6 unchanged
    LifecycleCommit = 7;
  }
  // existing fields 1..10 unchanged
  LifecycleCommitEntry lifecycle_commit = 11;
}
```

`Entry.bat`保持nil，任何路径都必须先按`entry_type`dispatch，不能先调用
`ProtoBatchToBatch`。payload字段：

```proto
message LifecycleCommitEntry {
  uint32 protocol_version = 1;
  RetireMode retire_mode = 2;
  string root_id = 3;
  string attempt_id = 4;
  string dataset_id = 5;
  string receipt_id = 6;
  uint64 database_id = 7;
  uint64 logical_table_id = 8;
  uint64 physical_table_id = 9;
  uint64 binding_generation = 10;
  bytes schema_digest = 11;
  timestamp.Timestamp source_snapshot_ts = 12;
  bytes source_set_digest = 13;
  repeated bytes data_source_object_stats = 14;
  repeated bytes created_object_stats = 15;
  repeated string transfer_booking_locations = 16;
  bytes transfer_mapping_digest = 17;
  int64 final_prepare_deadline_unix_nano = 18;
  uint64 max_delta_rows = 19;
  uint64 max_delta_bytes = 20;
  uint32 max_delta_blocks = 21;
  int32 merge_level = 22;
}
```

Archive entry必须且只能携带`dataset_id`，TTL entry必须且只能携带`receipt_id`。Archive和
所有Rewrite必须携带`root_id`；TTL Whole没有外部副作用，可以不创建Root。
`binding_id`、Lifecycle column和Manifest digest由同一普通事务中的Binding CAS、
Dataset/Receipt及Cleanup Root负责，不在TN退休entry中复制。

V1不包含D/E业务值、SourceLayoutProof、destination bitmap或第二份mapping。
SyncProtection job ID复用现有`PrecommitWriteCmd.SyncProtectionJobId`，不在entry内重复。

Digest统一使用SHA-256和固定domain separator：

```text
source_set_digest =
  SHA256("matrixone/lifecycle/data-sources/v1" ||
         对data_source_object_stats按Object ID排序后
         逐项拼接complete fixed-length ObjectStats bytes)

transfer_mapping_digest =
  SHA256("MO-LIFECYCLE-TRANSFER-v1" ||
         uint32_be(created_object_count) ||
         created_object_stats按DoMergeAndWrite输出顺序逐项编码(
           uint32_be(length) || bytes) ||
         uint32_be(source_block_count) ||
         decoded TransferTable按source block ordinal/source row offset逐项编码)
```

TN使用现有booking codec解码后重算`transfer_mapping_digest`。该digest防传输损坏或错配，
不宣称TN能重新证明TTL业务分类；mapping业务正确性来自唯一producer
`DoMergeAndWrite`及属性测试。

`data_sources[]`只包含本事务要退休的Data Object；字段号3在schema中`reserved`，V1
编码器不能产生该字段。Snapshot Reader
使用的Tombstone Object只存在于CN选择结果和`SyncProtectionJobId`对应的protection set，
不进入entry、`source_set_digest`或任何Drop集合。`protection_set_digest`只参与
SyncProtection job identity，不扩展为TN业务字段。Transfer digest中的整数固定big-endian，
所有变长数组和bytes必须带长度framing；source digest只拼接固定长度的完整ObjectStats。

## 6. 旧TN与滚动升级P0

现有未知Entry可能继续进入普通Batch解析。必须先交付安全解析：

```text
inspect EntryType/protocol version
-> known normal Entry: existing path
-> known LifecycleCommit: Lifecycle handler
-> unknown EntryType/version: typed unsupported error
-> only after dispatch may parse Batch
```

要求：

- 未完成全量升级时retirement默认关闭；
- Export-only只作为测试/认证模式验证Writer与Provider，不是Phase 1生产能力；生产release
  关闭时不创建新Root，也不执行Provider PUT；
- 第一阶段先把“未知Entry在Batch解析前返回unsupported”补丁部署到全部TN；
- 第二阶段才允许能够生成`LifecycleCommit=7`的新CN上线；
- 新CN→已具备安全解析但不支持V1的TN，在任何Batch解析/mutation前失败；
- 没有安全解析补丁的更老TN不在retirement滚动兼容集合中，发布控制面禁止路由；
- 旧CN不会产生Lifecycle entry；
- 新TN接受正常旧CN请求；
- 降级前关闭retirement并等待FINALIZING/COMMIT_UNKNOWN收敛；
- 不建设HAKeeper capability协议；使用版本发布开关和集群升级检查。

## 7. TN Handler顺序

```text
1. dispatch EntryType/version before Batch parsing
2. validate serialized size/count/deadline
3. validate physical table/schema identity和entry结构；TN不查询tenant Lifecycle Catalog
4. 使用现有`PrecommitWriteCmd.SyncProtectionJobId`接口验证deterministic job ID和lease
5. resolve and compare exact Data ObjectStats
6. decode existing external booking（Rewrite）
7. scan bounded post-S Tombstone delta
8. Whole: 对每个data source执行`SoftDeleteObject(..., false)`
9. Rewrite: 对唯一data source执行`SoftDeleteObject(..., false)` +
   CreateNonAppendableObject
10. install existing transfer runtime
11. LogTxnEntry
12. normal Prepare/Commit/WAL
```

任一检查失败整个事务abort。Object not found、Drop Intent/EOB不能当成本attempt成功。
第2步只拒绝畸形、伪造或超过发布硬上限的entry，发生在Booking I/O和TAE mutation之前；
它不是TN资源繁忙型admission。V1不增加Lifecycle专用permit或`RESOURCE_BUSY`重试语义，
合法entry继续使用普通Merge/事务资源路径。

资源Owner按现有事务边界分开：

| 资源 | `LogTxnEntry`前 | 成功后/rollback |
|---|---|---|
| Root-scoped物理live/booking文件 | Cleanup Root | commit后live交TAE；booking仍由Root清理 |
| `SoftDeleteObject/CreateNonAppendableObject`产生的Catalog node | API成功后立即归当前TAE txn | 由整个txn commit/rollback |
| TransferTable/slab/runtime page/TransferDels | Lifecycle builder | `LogTxnEntry`成功后归txn entry |

builder在注册前失败时只释放runtime资源，不删除Root文件，也不手工撤销已经归TAE txn的
Catalog node；该代final transaction必须整体rollback，不能在同一个已部分mutation的txn
内重新build。现有普通Merge的注册前清理问题不在本功能顺便重构。

`ErrTAENeedRetry`由现有commit payload重新dispatch同一个immutable entry，并从external
booking为新的内部TAE txn重建私有runtime对象；禁止复用上一内部txn的指针，但不增加
Lifecycle replay state machine。

## 8. Exact Source Identity

每个data source至少比较：

```text
physical_table_id
Object ID
complete ObjectStats bytes or canonical digest
source_set_digest
```

重叠source由现有Object Drop Intent/MVCC保证最多一个事务成功。不增加Binding
active-attempt claim。不同Root允许同时FINALIZING。TN不得从SyncProtection job枚举
Tombstone Object并把它们附加到退休集合。

## 9. post-S Tombstone

普通Merge以final transaction StartTS为增量起点；Lifecycle必须从`S`开始：

```text
CollectDeletesFrom = source_snapshot_ts
CollectDeletesTo   = prepare_ts
```

处理：

- 命中L且mapping有效：transfer到新RowID；
- Archive命中E/NoTransfer：abort；
- Whole Archive命中任意source row：abort；
- TTL命中E：语义上冗余，可忽略；
- Phase 1首版可统一对NoTransfer abort，换取更少分支；
- nil map、越界、decoder错误：abort。

Whole只需要回答“是否存在post-S DELETE”：复用现有Tombstone选择和读取路径，固定
`maxRows=1`，发现第一行立即abort，不物化完整Tombstone Batch。普通Merge继续使用
`maxRows=0`的原有完整扫描语义。

Rewrite visitor按单source处理，有rows/bytes/blocks和内部deadline。超限进入
`CONFLICT_BLOCKED`或`RESOURCE_BLOCKED`。

建议抽取Lifecycle专用有界接口，不改变普通Merge默认扫描：

```go
type TombstoneDeltaBudget struct {
    MaxRows   uint64
    MaxBytes  uint64
    MaxBlocks uint32
    Deadline  time.Time
}

VisitLifecycleTombstoneDelta(
    sourceObject types.Objectid,
    from, to types.TS,
    budget TombstoneDeltaBudget,
    visit func(rowid types.Rowid, pk []byte) error,
) error
```

实现按单Tombstone Object/Batch及时release，累计预算包含变长PK。`PrepareCommit`没有调用方
context时使用entry中的absolute deadline，而不是`context.Background()`无限等待。现有
BigDelete hint命中时可直接abort；测试大量不超过hint阈值的小事务累计路径。

## 10. 普通DELETE与Prepare后事务

S前DELETE归D，不进入Archive/live。S后L DELETE经transfer。Prepare后仍引用旧RowID的
用户事务依赖现有TAE RW/WW冲突，必须测试而不重新设计。

INSERT不写入旧source Object，不属于当前attempt；后续cycle处理。

## 11. TTL Small Mixed

仅`ACTION DELETE`可以走这条路径；Archive Mixed禁止使用。一个child：

```text
open bounded internal writable transaction at fixed snapshot S
-> exact Reader只读取目标source Object的RowID + real/fake PK + lifecycle column
-> 重算E集合
-> Relation.Delete(E RowID, delete key)
-> insert TTL Receipt
-> commit
```

必须证明Read和Delete使用同一个固定Snapshot。MO默认RC逐语句推进Snapshot时不能直接拼接
独立SELECT和DELETE；Gate F需要使用现有可写SI能力，或在内部TxnOperator上冻结Snapshot。

准入在打开事务前估算，执行中再次计费：

```text
expired rows
estimated Tombstone bytes = rows * (RowID bytes + delete-key bytes + overhead)
affected blocks
source Object bytes
current table Tombstone/Merge backlog
transaction wall time
```

任一超限立即rollback，改走单源Rewrite或进入`MIXED_LAYOUT_BLOCKED`。不把大量E拆成无限
小事务，因为这会留下持续Tombstone/WAL/Logtail放大。Provider I/O不进入这条事务。

并发Merge/DELETE/UPDATE依赖现有SI、RowID transfer和冲突语义；Lifecycle只增加source
identity复核和TTL Receipt原子写，必须在Gate F故障测试中证明。

## 12. WAL、Replay与GC

- TAE WAL记录source DropIntent、created live Object和Tombstone；
- Provider Payload/Manifest不进入WAL；
- existing booking供Prepare/retry重建runtime transfer；
- 不承诺Replay恢复历史transfer page；
- commit后源物理文件由现有GC删除；
- Lifecycle不直接Delete源TAE Object。

Lifecycle复用普通MO 1PC/2PC、NeedRetry和duplicate Commit语义。通用问题在公共实现修复。

## 13. DDL Gate

thin entry始终携带Binding/table/schema identity，但“读取后比较”不是最终互斥证明。
Gate H必须通过并发测试选择：

- finalizer持有`mo_tables`行锁到事务终态；或
- finalizer和不兼容DDL write-CAS同一Binding行。

在此之前P0退休只运行于无不兼容DDL的受控测试环境。
