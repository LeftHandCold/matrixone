# Whole Object 与 Strict Retire 协议详细设计

> 本文唯一负责 Whole TTL/Archive、final table lock、protobuf wire、TN Prepare、
> exact Object CAS、Tombstone delta、WAL/replay、commit retry 和滚动升级。

## 1. 问题

当前 CN `txnTable.SoftDeleteObject`：

- 构造 Object ID Batch；
- 使用内部 `SOFT_DELETE_OBJECT` Entry；
- `toPBEntry` 仍编码成普通 `api.Entry_Delete`；
- `file_name` 中放 soft-delete 暗号；
- TN `HandleSoftDeleteObject` 调用 TAE `SoftDeleteObject`。

当前 TN 对 `DropObjectByID` 返回 `OkExpectedEOB` 时记录 warning 并返回成功。这种语义适合普通 Merge 的重复清理，但 Lifecycle 不能区分：

```text
同一个 attempt 已经提交
```

和：

```text
普通 Merge 抢先替换了 source Object
另一个 Lifecycle attempt 已经 DropIntent
Object 根本不属于当前 table/schema generation
```

因此新增独立窄协议，保留普通 SoftDelete 原样。

## 2. 不新增完整 Lifecycle Commit

Strict Retire 只负责 Whole Object DropIntent 的条件校验。它不承载：

- Mixed live Object rewrite；
- transfer map；
- Archive Manifest/Payload；
- Provider I/O；
- Dataset Catalog 业务逻辑；
- Scheduler/Root 状态。

Dataset/Receipt/Guard 是同一普通 tenant transaction 的正常 Catalog 写。TN Strict entry 只成为该事务的一个写 participant。

最终持久 replay 继续使用现有 Object Drop transaction entry：

```text
Strict wire entry
  -> TN Prepare validation
  -> existing TAE DropObjectEntry
  -> existing command/WAL/checkpoint/replay
```

## 3. Protobuf

修改：

```text
proto/api.proto
pkg/pb/api/api.pb.go        # 由 make generate 生成
```

新增 enum 值，不复用：

```proto
message Entry {
    enum EntryType {
        Insert = 0;
        Delete = 1;
        Update = 2;
        Alter = 3;
        SpecialDelete = 4;
        DataObject = 5;
        TombstoneObject = 6;
        StrictObjectRetire = 7;
    }

    // existing fields 1..10 remain unchanged
    StrictObjectRetirePayload strict_object_retire = 11;
}
```

新增：

```proto
message StrictRetireObject {
    bytes object_id = 1;
    bytes object_stats = 2;
    bytes object_stats_digest = 3;
}

message StrictObjectRetirePayload {
    enum Mode {
        ModeUnknown = 0;
        TTL = 1;
        ARCHIVE = 2;
        EMPTY_ARCHIVE = 3;
    }

    uint32 protocol_version = 1;
    Mode mode = 2;
    bytes attempt_id = 3;
    uint64 executor_epoch = 4;

    bytes account_incarnation = 5;
    uint64 logical_table_id = 6;
    uint64 physical_table_id = 7;
    uint64 schema_generation = 8;
    uint32 tae_schema_version = 9;
    bytes schema_digest = 10;

    bytes binding_id = 11;
    uint64 binding_generation = 12;
    uint64 guard_version = 13;
    bytes guard_digest = 14;

    uint32 lifecycle_column_seqnum = 15;
    uint32 lifecycle_type = 16;
    bytes cutoff_canonical = 17;
    timestamp.Timestamp evaluation_time_utc = 18;
    timestamp.Timestamp source_snapshot_ts = 19;
    bool require_no_new_tombstone = 20;

    repeated StrictRetireObject objects = 21;
    bytes source_object_digest = 22;
    uint64 source_visible_rows = 23;
    bytes source_content_root = 24;
    bytes dataset_id = 25;
    bytes manifest_root = 26;

    bytes entry_digest = 27;
}
```

说明：

- `Entry.table_id/database_id` 与 payload physical ID 必须一致；
- `object_stats` 使用当前固定 binary `ObjectStats`；
- `cutoff_canonical` 使用 lifecycle type 的内部编码；
- TTL 的 Archive 字段为空；
- ARCHIVE 的 Dataset/root/rows 必填；
- EMPTY_ARCHIVE 要求 rows=0，Dataset/Manifest为空；
- `entry_digest` 是除自身字段外 canonical protobuf bytes 的 SHA-256；
- repeated Object 必须按 object ID 严格升序；
- V1 最多 64 个 Object；
- 整个 serialized entry hard max 512 KiB。

协议 version：

```text
STRICT_OBJECT_RETIRE_V1 = 1
```

## 4. CN 内部 Entry

修改：

```text
pkg/vm/engine/disttae/types.go
pkg/vm/engine/disttae/txn_table.go
pkg/vm/engine/disttae/tools.go
```

新增内部类型：

```go
const STRICT_OBJECT_RETIRE EntryType = ...

type Entry struct {
    // existing fields
    strictRetire *api.StrictObjectRetirePayload
}
```

新增 Relation 窄接口，不能改变所有普通调用方语义：

```go
type LifecycleRelation interface {
    StrictRetireObjects(
        ctx context.Context,
        payload *api.StrictObjectRetirePayload,
    ) error
}
```

实现只：

1. 校验当前 txn 不是 Snapshot Operator；
2. 校验 payload 和 Relation physical ID；
3. 深拷贝 payload；
4. append typed internal Entry；
5. 不构造 Object ID Batch；
6. 不使用 `file_name` 暗号。

`toPBEntry`：

- `entry_type = StrictObjectRetire`；
- `strict_object_retire = payload`；
- `bat = nil`；
- 不落入普通 Insert/Delete 路径。

## 5. TN 解析

修改：

```text
pkg/catalog/entry.go             # ParseEntryList 对新 enum保留 typed api.Entry
pkg/vm/engine/tae/rpc/handle.go
pkg/vm/engine/tae/rpc/lifecycle_retire.go
```

`handleRequests` 必须先按 enum 分流：

```go
case *api.Entry:
    switch req.EntryType {
    case api.Entry_StrictObjectRetire:
        err = h.HandleStrictObjectRetire(ctx, txn, txnMeta, req)
    default:
        wr := h.apiEntryToWriteEntry(...)
        ...
    }
```

禁止先转 `WriteReq` 再通过 `file_name` 识别。

未知 enum/protocol：

- 返回明确 `ErrUnsupportedProtocol`；
- 整个事务 abort；
- 不能跳过 Entry；
- 不能把它当普通 Delete；
- 不能返回 success。

## 6. TN Prepare 校验顺序

`HandleStrictObjectRetire` 对 entry 执行以下固定顺序。

### 6.1 Wire 和 digest

```text
protocol_version == V1
mode valid
attempt ID length valid
entry size/object count bounded
objects sorted/unique
entry.table_id == payload.physical_table_id
entry_digest recompute matches
source_object_digest recompute matches
archive fields consistent with mode
require_no_new_tombstone == (mode == ARCHIVE)
```

任何失败返回 non-retryable protocol error。

### 6.2 Table/schema

在 TAE txn：

```text
database exists by Entry.database_id
table exists by physical_table_id
latest visible schema version == tae_schema_version
computed schema digest == payload.schema_digest
lifecycle column seqnum exists/type matches
table is normal data table
```

`schema_generation`、Binding/Guard version由 tenant Catalog CAS关闭跨 DDL竞态；TN 不自行读取 tenant Guard 表。TAE schema version/digest提供物理 Catalog第二道检查。

### 6.3 Exact Object

每个 source Object：

```text
object exists in this TAE table
object is data Object, not tombstone Object
object create committed and visible to final txn
object has no committed DeleteNode
object has no DropIntent from another txn
current ObjectStats bytes/digest exactly match
ObjectStats object ID/rows/blocks/size valid
footer object ID/rows/blocks match ObjectStats
lifecycle column null_count == 0
lifecycle ZoneMap exact/supported
lifecycle max < cutoff
```

Object-not-found、已有 DropIntent、stats mismatch、ZoneMap不再 Whole 都返回条件冲突，整个事务 abort。

禁止把 `OkExpectedEOB` 转 success。

### 6.4 Tombstone readiness

ARCHIVE 必须检查：

```text
(source_snapshot_ts, final_validation_ts]
```

内所有指向 source Object 的 committed Tombstone。

现有 `TombstoneRangeScanByObject` 会物化 Batch，代码中还记录了 1 GiB 固定缓冲风险；
`WaitTombstoneObjectCommitted` 可能无 context 阻塞。Strict Prepare 不直接调用该组合。

新增：

```go
type TombstoneDeltaResult struct {
    Found           bool
    RowsSeen        uint64
    Overflow        bool
    HistoryComplete bool
}

func HasCommittedObjectTombstoneInRange(
    ctx context.Context,
    table *catalog.TableEntry,
    objectID *objectio.ObjectId,
    fromExclusive types.TS,
    toInclusive types.TS,
    maxRows uint64,
    maxBytes uint64,
) (TombstoneDeltaResult, error)
```

实现要求：

- 覆盖内存和持久化 Tombstone Object；
- 检查 Tombstone apply watermark 是否至少到 `toInclusive`；
- 检查可查询 Tombstone history/GC low watermark 没有越过 `fromExclusive`；
- watermark 未到时立即返回 retryable `ErrTAENeedRetry`，不做无界等待；
- history低水位已越过`fromExclusive`时返回`HistoryComplete=false`，不能返回“无Tombstone”；
- 使用 caller context；
- 找到第一条 relevant Tombstone即可 `Found=true` 早停；
- 只为诊断累计 bounded rows/bytes；
- 达到 bound返回 `Overflow=true`；
- read/checksum/metadata error fail closed；
- 不构造全量 Batch。

ARCHIVE：

```text
Found OR Overflow OR !HistoryComplete -> LIFECYCLE_TOMBSTONE_CHANGED
```

TTL 不需要该检查，因为所有物理行已经过期，DELETE/UPDATE 的旧版本退休不会使 Archive 复活；普通 MVCC/write conflict仍必须通过测试。

### 6.5 注册 DropIntent

所有校验成功后，按 object ID顺序调用新的严格 TAE helper：

```go
func (tbl *txnTable) StrictSoftDeleteObject(
    id *types.Objectid,
    expectedStatsDigest [32]byte,
    attemptID [16]byte,
    entryDigest [32]byte,
) error
```

它最终仍调用 `DropObjectEntry`，但重复语义为：

```text
same TAE txn + same attempt/entry/object already registered
  -> success no-op

same TAE txn + different digest
  -> protocol conflict

other txn DropIntent / committed drop / object missing
  -> conflict
```

同一 TAE txn 的 dedup memo：

```text
(attempt_id, entry_digest, object_id)
```

只存在 txn 生命周期，不替代 durable Receipt。

如果第 N 个 Object失败：

- handler返回 error；
- `handleRequests` rollback整个 txn；
- 前 N 个 DropIntent不提交；
- Dataset/Receipt/Guard写也不提交。

## 7. final validation timestamp

Whole Archive finalizer使用普通悲观 RC tenant transaction：

```text
allocate txn, do not write
  -> persist Root FINALIZING with txn ID
  -> acquire exclusive table lock
  -> force normal RC snapshot refresh after lock
  -> final_validation_ts = refreshed txn SnapshotTS
  -> read/CAS Guard/Binding/Dataset
  -> append Strict entry
  -> commit
```

必须证明：

```text
所有在 table lock 前已经进入的 UPDATE/DELETE
  -> 已提交或回滚
  -> final_validation_ts 能看到其 Tombstone

所有在 table lock 后发起的 UPDATE/DELETE
  -> 被锁阻塞到 final txn结束
```

不能在 Root CAS 前拿 table lock；不能用导出时 `source_snapshot_ts` 作为 final txn Snapshot。

如果现有 `LockTableWithContext` 返回 retry/refresh要求：

- 使用现有 RC retry机制重新开始 final tenant transaction；
- Root 中改写新的 txn ID 必须 CAS `FINALIZING` 且旧 txn已明确 aborted；
- 不能在旧 txn result unknown时换 txn ID。

## 8. Whole TTL

流程：

```text
Planner Whole candidate
  -> current Relation metadata exact recheck
  -> Attempt Control FINALIZING(txn ID, entry digest)
  -> tenant final txn
       CAS Guard/Binding/active attempt
       insert Receipt
       StrictObjectRetire(mode=TTL)
  -> commit
```

不做 payload Reader，不创建 Cleanup Root，不拿 Archive table lock。

最终 entry仍校验 footer `max < cutoff`，因为 Index只是 hint。

并发结果：

| 并发 | 预期 |
|---|---|
| Merge先替换 Object | Strict conflict，TTL replan |
| Strict先DropIntent | Merge按现有冲突 abort/replan |
| 用户DELETE同一行 | 最终逻辑仍删除；不得出现未删除行 |
| 用户UPDATE | old row删除/new row正常插入或其中一方冲突 |
| TRUNCATE/DDL | Guard/table/schema冲突，整事务abort |

## 9. Whole Archive

### 9.1 导出阶段

```text
normal read-only transaction at source_snapshot_ts
  -> exact Object metadata recheck
  -> Exact Reader all visible rows
  -> all Payload writer close/PUT complete
  -> freeze ScanReport/source content root
  -> close read transaction
  -> Provider full readback/Manifest VERIFIED
```

导出阶段不拿表锁。Merge 可替换 source Object；读失败则 attempt失败，读成功后 final CAS仍可能 abort。

### 9.2 Root finalizing

```text
allocate tenant final txn ID
  -> system txn CAS Root:
       VERIFIED -> FINALIZING
       final_txn_id
       final_entry_digest
       executor_epoch
  -> wait system commit
```

Root CAS失败则 rollback尚未写入的 tenant txn，不拿表锁。

### 9.3 短 final transaction

固定锁序：

```text
table write lock
  -> Feature Guard
  -> Binding/active attempt
  -> Dataset/Receipt rows
  -> TN StrictObjectRetire Prepare
```

禁止 finalizer 在拿 table lock前持有：

- Guard/Binding row lock；
- Dataset unique key lock；
- Root system transaction lock；
- Provider connection/PUT；
- Reader Batch。

锁参数：

```text
lock wait deadline = 30 seconds
post-lock final txn deadline = 60 seconds
no Provider I/O under lock
```

同一 tenant txn：

1. CAS Guard/Binding/active attempt；
2. insert immutable Dataset `PUBLISHED`；
3. insert immutable Receipt；
4. append Strict entry；
5. commit。

### 9.4 新 Tombstone

示例：

```text
Reader导出 R
  -> 用户DELETE R提交
  -> source data Object identity可能不变
  -> 不检查delta会把R留在Archive
  -> Restore复活R
```

因此：

- CN可做 bounded preflight，但不是安全权威；
- TN Strict Prepare必须做 `(source_snapshot, final_validation]` delta；
- 发现 delta整事务abort；
- Dataset插入和Receipt回滚；
- Root在明确 aborted后 `DELETE_PENDING`；
- 重新导出新 attempt；
- 达到冲突上限 `CONFLICT_BLOCKED`。

## 10. 表锁覆盖证明

P0 必须列举所有能为普通表产生 source RowID Tombstone 的路径：

- SQL DELETE；
- SQL UPDATE；
- distributed delete/merge-delete；
- internal table mutation；
- TRUNCATE；
- CDC/插件/隐藏索引 handler（Binding已拒绝）；
- Merge transfer产生的 Tombstone处理。

每条路径必须满足至少一项：

```text
A. 与 Lifecycle exclusive table lock冲突；
B. 在 final txn Snapshot前提交，并被TN delta扫描覆盖；
C. 与 exact Object DropIntent发生TAE事务冲突；
D. Binding准入明确拒绝该路径。
```

未分类路径使 P0-4 不通过。

普通后台 Merge不依赖 SQL LockService，按 C 处理。

## 11. `ErrTAENeedRetry`

当前 TN commit路径遇到 `ErrTAENeedRetry` 会：

```text
rollback/release current TAE txn
start new TAE txn at original snapshot
re-run handleRequests with original PrecommitWriteCmd payload
commit again
```

Strict entry要求：

- payload immutable；
- digest相同；
- handler不依赖进程内递增 cursor；
- 重试前的 DropIntent已随 txn rollback；
- Root/tenant transaction ID不变；
- 重试不得再次 PUT/Verify；
- retry count/deadline复用现有 txn限制；
- permanent strict condition failure不能包装成 NeedRetry。

## 12. 1PC、2PC 与 response lost

### 12.1 1PC

一个 TN shard时，Dataset/Receipt Catalog和源表可能仍在同一 shard。即便走1PC，Receipt与DropIntent必须原子。

### 12.2 2PC

如果 participant不止一个：

- Strict entry参与正常 Prepare；
- Dataset/Receipt participant参与同一 txn；
- 任一 strict条件失败，所有 participant abort；
- 不新增 Lifecycle coordinator。

### 12.3 Response lost

客户端 commit返回 unknown：

- 不重发新的 txn ID；
- txn client/LockService unknown resolver持有锁清理；
- Attempt/Root保持 FINALIZING；
- Reconciler查询原 txn `GetStatus`；
- committed后在一致性事务读匹配Receipt/Dataset；
- aborted后才清理staging。

Object missing不能作为 response lost已提交的替代证据，因为可能是 Merge。

## 13. Txn status 查询

现有 `proto/txn.proto` 已有 `TxnMethod.GetStatus`。新增受控 helper：

```go
type FinalTxnResolution struct {
    Status   txn.TxnStatus
    CommitTS timestamp.Timestamp
}

func ResolveFinalTxnStatus(
    ctx context.Context,
    snapshot txn.CNTxnSnapshot,
) (FinalTxnResolution, error)
```

要求：

- 使用 Root/Attempt冻结的 txn ID、TN shard/participant snapshot；
- 每次调用独立10秒 deadline；
- ACTIVE/unknown返回in-doubt，不猜 abort；
- TN unreachable返回retryable；
- committed必须再读Receipt/Dataset；
- aborted不得存在匹配Receipt；存在则P0 invariant failure。

若现有 txn client没有暴露安全 helper，P0先实现该窄接口，不让 Lifecycle自己手工拼散落 RPC。

## 14. WAL/replay

Strict wire payload只在 Precommit阶段存在。成功校验后使用现有：

```text
catalog.TableEntry.DropObjectEntry
txnTable.txnEntries
collectCmd
Object MVCC command
WAL
checkpoint/logtail
catalog replay
```

不新增：

- Lifecycle WAL opcode；
- Manifest replay；
- Root replay；
- provider side effect replay。

必须测试：

- prepare后TN crash；
- WAL append前后crash；
- commit后response前crash；
- checkpoint前后replay；
- duplicate Prepare；
- source Object已有DropIntent；
- replay后DropAt和Receipt一致可见。

## 15. 普通 SoftDelete 回归边界

以下代码行为不改：

```text
txnTable.SoftDeleteObject
makeSoftDeleteFileName
HandleSoftDeleteObject
OkExpectedEOB -> warning + success
Merge/flush调用者
```

新 Strict handler不得复用 `HandleSoftDeleteObject` 的吞错逻辑。回归测试必须证明普通 Merge原有case不改变。

## 16. Capability 与滚动升级

Strict entry绑定新的 MO protocol version：

```text
MORPC protocol >= version containing STRICT_OBJECT_RETIRE_V1
```

复用现有：

- `runtime.MOProtocolVersion`；
- QueryService Get/SetProtocolVersion；
- 集群升级最终 protocol version推进机制。

Lifecycle Coordinator在每次创建 retirement child和final前检查：

```text
cluster final protocol version >= required
AND all active CN/TN have completed target binary rollout
AND no downgrade fence
```

协议未 ready：

- Binding可创建；
- Index/Dry-run可运行；
- Export-only可运行；
- Strict entry不得发送；
- Job显示 `CAPABILITY_NOT_READY`。

旧 TN收到未知 enum必须abort；这是一道最后保险，不是正常升级流程。

降级：

- 有 ACTIVE Binding不等于不能降，但 retirement必须先kill switch；
- 有 FINALIZING/COMMIT_UNKNOWN必须先收敛；
- 有已提交Strict Drop的集群只能降到仍认识其现有WAL Object Drop格式的版本；
- 因最终WAL复用现有Object Drop，不需要旧版本理解Strict wire历史。

## 17. 限额

每个 Strict entry：

```text
objects            <= 64
serialized bytes   <= 512 KiB
footer reads       <= 64
footer concurrency <= 4
TN Prepare wall    <= 30 seconds
tombstone rows     early exit on first match
tombstone diag     <= 1 million rows / 64 MiB metadata
```

达到任何限额：

- 返回retryable/blocked明确错误；
- 不部分提交；
- 不扩大entry；
- Planner拆成新 child，但不拆单个Object。

## 18. P0 测试矩阵

Wire：

- V1 round-trip/deterministic digest；
- unknown enum/version；
- duplicate/reordered Object；
- 512 KiB边界；
- old TN fail closed。

Strict condition：

- object missing；
- stats changed；
- schema version/digest changed；
- object已有DropIntent（同txn同digest/同txn异digest/其他txn）；
- lifecycle max越过cutoff；
- null count异常；
- data/tombstone object混淆。

Tombstone：

- source Snapshot前 Tombstone不触发；
- 区间下界exclusive/上界inclusive；
- memory/persisted Tombstone；
- watermark未到返回NeedRetry；
- history low watermark越过source Snapshot时fail closed/re-export；
- first-match early exit；
- overflow fail closed；
- context cancel不挂起。

并发：

- Archive export后DELETE/UPDATE；
- lock前在途DML；
- lock后新DML；
- Merge先/后；
- TRUNCATE/ALTER/DROP；
- 64 Object中第N个冲突全回滚。

Txn/replay：

- 1PC/2PC；
- ErrTAENeedRetry；
- duplicate Prepare；
- response lost；
- TN crash/WAL replay/checkpoint；
- Receipt/Dataset/DropAt原子可见。

回归：

- 普通 Merge SoftDelete ExpectedEOB仍保持；
- 未绑定表DML/SELECT/Merge无新增逻辑；
- Strict feature gate关闭时不产生新Entry。
