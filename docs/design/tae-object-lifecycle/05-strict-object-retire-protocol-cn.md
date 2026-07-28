# Lifecycle Object Retire 与 Mixed Rewrite 协议详细设计

> 文件名为兼容已有链接保留 `strict-object-retire`。
>
> 本文唯一负责 Whole Object 退休、Mixed Rewrite、source reservation/protection、
> CN 构建器、`OpCommitLifecycle` wire、TN Prepare、两阶段 Tombstone transfer、
> WAL/replay、commit unknown、滚动升级以及与普通 Merge 的协调。

## 1. 结论

Lifecycle 不把归档逻辑塞进普通 Merge，也不复制第二套 Merge/G C。

大 Mixed 的正式路径是一个独立的、对象级 Archive-aware Merge：

```text
claim exact source Objects
  -> 固定 source_snapshot_ts
  -> 注册 exact source GC protection
  -> streaming read source Objects once
       expired visible rows -> Parquet/ZSTD
       live visible rows    -> new normal TAE Objects
       snapshot-deleted rows-> neither output
  -> verify Archive and live output
  -> short normal distributed transaction
       Dataset/Receipt
       exact source validation
       create new live Objects
       DropIntent old source Objects
       install transfer pages
  -> commit
  -> existing TAE GC deletes old source files
```

三个退休策略：

| 分类 | 活动数据处理 |
|---|---|
| Whole | 不写新 live Object，直接严格 DropIntent |
| 小 Mixed | 普通 `Relation.Delete`，见 06 |
| 中/大 Mixed | Lifecycle Rewrite，过期行不写入新 Object |

Rewrite 不为到期行创建 Tombstone。旧 Object 整体 DropIntent；未到期行已经在新
TAE Object 中。这样避免大规模 RowID/PK Tombstone、WAL、Logtail 和后续 Vacuum
放大。

## 2. 为什么这是当前 MO 的最优实现

### 2.1 可直接复用

现有代码已经完成普通 Merge 最困难的闭环：

```text
pkg/vm/engine/tae/mergesort
  DoMergeAndWrite
  source-row -> destination-row TransferTable

pkg/vm/engine/disttae/merge.go
  CN fixed-snapshot BlockDataReadNoCopy
  normal TAE Object writer

pkg/vm/engine/tae/tables/jobs/mergeobjects.go
  HandleMergeEntryInTxn
  SoftDelete source Object
  CreateNonAppendableObject

pkg/vm/engine/tae/tables/txnentries/mergeobjects.go
  phase-1/phase-2 Tombstone transfer
  transfer page
  rollback staging object cleanup
  WAL command

pkg/vm/engine/tae/db/gc/v3
  checkpoint/logtail/Snapshot/PITR/ISCP filtering
  final FileService delete
```

`mergesort` 的 deletes bitmap 已有准确语义：

```text
deleted input row
  -> 不写 output
  -> TransferTable 对应项保持 api.NoTransfer
```

Lifecycle 只需把：

```text
snapshot tombstone bitmap
```

扩展为：

```text
snapshot tombstone bitmap UNION lifecycle-expired bitmap
```

即可让现有 Merge writer只写存活行，并为存活行生成 transfer map。

### 2.2 不选择的实现

不在普通 Merge worker 中执行 Provider PUT/readback：

- Provider I/O 可能数分钟；
- 失败需要 Cleanup Root、Manifest 和 commit-unknown 对账；
- 普通 Merge 不能依赖归档 namespace、credential 或外网；
- 未绑定表不应增加这类故障面。

不对大 Mixed 执行普通 DELETE：

- 到期行越多，Tombstone/WAL/Logtail 越大；
- 旧 Object 仍需后续 Merge/Vacuum 重写；
- 相当于先写删除日志，再做一次 Rewrite。

不让 Lifecycle直接物理删除旧 TAE 文件：

- DropIntent/checkpoint/logtail/GC 是当前唯一回收权威；
- Lifecycle 不复制 Snapshot/PITR/ISCP/GC watermark 逻辑。

## 3. 普通 Merge 的代码事实

当前 Merge：

- Scheduler 遍历当前 `IterDataItem()`，按 level、Object size、sort-key overlap 和
  Tombstone/Vacuum 成本选对象；
- 不读取业务生命周期列，也不知道 90 天 cutoff；
- 按表的 physical primary/cluster sort key 排序，不按任意 lifecycle column；
- 创建新 Object 后在事务内 SoftDelete 源 Object；
- 源文件稍后由现有 GC 删除。

因此“数据已经 90 天”不能推出“普通 Merge 不会碰它”。若生命周期列与 physical
sort key 不一致，Merge 可能把不同日期重新混入 Object。

本协议解决正确性和一次 Rewrite 成本；首个 GA 不改变普通 Merge 的分组或排序。
高度乱序表可能持续产生 Mixed，必须由 Dry-run、rewrite amplification budget 和
Lag support level 明确展示。

## 4. Source Reservation

### 4.1 目的

Final exact Object CAS 足以保证不错误退休，但如果没有 reservation：

```text
Lifecycle 读/写 4 GiB
  -> 普通 Merge A+B->C 先提交
  -> Lifecycle final CAS abort
```

会反复浪费跨云 I/O。Reservation 负责进度和降低浪费，不替代 CAS。

### 4.2 TN 内存结构

新增：

```text
pkg/vm/engine/tae/db/merge/lifecycle_reservation.go
```

接口：

```go
type LifecycleReservationToken struct {
    AttemptID      types.Uuid
    ExecutorEpoch  uint64
    ReservationGen uint64
    PhysicalTableID uint64
    SourceDigest   [32]byte
    ExpireAt       time.Time
}

type LifecycleReservationManager interface {
    Acquire(
        ctx context.Context,
        tableID uint64,
        objects []objectio.ObjectStats,
        attemptID types.Uuid,
        executorEpoch uint64,
        ttl time.Duration,
    ) (LifecycleReservationToken, error)

    Renew(ctx context.Context, token LifecycleReservationToken, ttl time.Duration) error
    Release(ctx context.Context, token LifecycleReservationToken) error
    Validate(token LifecycleReservationToken, objects []objectio.ObjectStats) error
    IsReserved(tableID uint64, objectID objectio.ObjectId) bool

    BeginMergeAdmission(
        tableID uint64,
        objects []objectio.ObjectStats,
    ) (MergeAdmissionTicket, error)
    EndMergeAdmission(ticket MergeAdmissionTicket)
}
```

内部索引：

```text
by (table ID, object ID) -> attempt/generation/expire
by attempt               -> exact source set
short merge admission     -> exact source set while DropIntent is being installed
```

### 4.3 线性化

`Acquire` 在 TN：

1. 校验 source Object 当前存在、非 appendable、无 DropIntent；
2. 校验 ObjectStats/digest；
3. 在同一 manager mutex 下检查重复/冲突 reservation；
4. 插入 exact set 并返回 generation token。

普通 Merge：

- Scheduler gather 时跳过 reservation 中的 Object，减少无效任务；
- `HandleMergeEntryInTxn` 进入时调用 `BeginMergeAdmission`；
- 在同一 manager shard 内检查 Lifecycle reservation，并安装一个短
  `MergeAdmissionTicket`；
- ticket 保持到 source Object DropIntent/merge txn entry 已全部安装，随后释放；
- ticket 释放后由 Object MVCC/DropIntent 拒绝新的 Lifecycle `Acquire`；
- success/error/panic 都 exactly once `EndMergeAdmission`，不能泄漏 merge claim；
- 用户强制 `mergeobjects` 也必须经过最终检查，不能绕过；
- 已经开始的普通 Merge 如果在 reservation 后到达 final handler，返回可重试冲突
  并清理其新 staging Object。

Lifecycle final handler：

- 只接受与 token 相同 attempt/generation/source digest；
- token 过期、缺失、TN restart 后丢失一律 abort/replan；
- 不能看到 Object 仍存在就忽略 token 缺失。

这样关闭了“普通 Merge 检查 map 通过、Lifecycle 随后 Acquire、Merge 尚未安装
DropIntent”的 check/use 窗口。manager 按 table/object shard，不能使用一个集群
全局 mutex。正常 Merge ticket 只覆盖 TN Catalog mutation 的短临界区，不覆盖
`DoMergeAndWrite`、Provider I/O 或事务提交等待。

ticket是manager中的逻辑claim，不是一直持有manager mutex。`Begin`在shard mutex下
插入claim后立即解锁，SoftDelete/Catalog调用发生在锁外；`End`再次短暂加锁删除。
因此fail-fast rejection不等待持ticket方的I/O，也不形成
`manager mutex -> TAE Catalog lock -> manager mutex`环。

### 4.4 生命周期

Reservation 是有 TTL 的 TN 内存状态：

```text
ACQUIRED -> RENEWED* -> RELEASED
ACQUIRED -> EXPIRED
TN restart -> LOST
```

不写 WAL、不 replay。原因：

- 丢失只导致本 attempt final fail closed；
- exact Object CAS 仍是数据安全边界；
- TN restart 后尚未完成的普通 Merge 由正常事务/WAL恢复，不依赖 ephemeral ticket；
- 避免每个活跃 Object reservation 进入 Catalog/checkpoint/replay；
- Attempt/Cleanup Root 负责持久外部副作用。

默认：

```text
TTL             = 2 minutes
renew interval  = 30 seconds
max claim age   = 60 minutes
```

Release profile 可降低，不可设置无限。

## 5. Exact Source GC Protection

Reservation 阻止 Merge 替换 source Object，但不保护：

- DROP/TRUNCATE 后的源文件；
- source Snapshot 所需的旧 Tombstone Object；
- TN 重启后的 reservation 空窗。

复用现有 `gc.SyncProtectionManager`，新增 Lifecycle wrapper，不创建长期 table
Snapshot。

### 5.1 保护集合

在 source Snapshot `S` 收集：

```text
exact source Data Object filenames
exact Tombstone Object filenames needed to evaluate those source Objects at S
Rewrite deterministic live segment ordinal range filenames
Rewrite deterministic transfer booking page filenames
```

实现必须复用 Snapshot Reader/Tombstone loader 的同一枚举规则。若当前代码无法在
读取前精确判断“相关 Tombstone”，先保守加入该表在 `S` 可见的全部 Tombstone
Object；不得为了减少 Bloom 大小漏保护。保护文件数、Bloom bytes 和
snapshot-exclusive retained bytes 都进入 admission，超限时不开始 build。

生成 deterministic BloomFilter 并注册：

```text
job_id   = lifecycle/<attempt-id>/<executor-epoch>
valid_ts = now + protection_ttl
```

顺序：

```text
Acquire source reservation
  -> open short Snapshot reader at S
  -> enumerate exact data+tombstone files
  -> Rewrite: allocate Cleanup Root and freeze live segment/booking ranges
  -> build one BloomFilter covering source + future staging filenames
  -> RegisterSyncProtection
  -> close discovery txn
  -> create Snapshot Operator/read exact Objects at S
```

`RegisterSyncProtection` 在 GC cycle 正在执行时会拒绝。Lifecycle 退避重试；不能
假装已保护。

### 5.2 续租和 Prepare 校验

- CN worker按现有 Publication worker模式续 `valid_ts`；
- 任一次续租失败立即 cancel Reader/Writer；
- TN `OpCommitLifecycle` Prepare 调用
  `ValidateSyncProtection(job_id, prepare_ts)`；
- TN restart 后 protection 丢失，final transaction abort；
- response unknown 后 protection 不因 worker lease丢失立即 unregister，直到原
  transaction 明确 committed/aborted；
- committed 后 source files 已有正常 DropIntent/transfer protection，可以
  unregister；
- aborted 后先清理 staging owner，再 unregister。

### 5.3 限制

当前 SyncProtection 是 BloomFilter，可能误保护额外文件，但不能漏掉列入集合的文件。
它不能原地扩展BloomFilter，所以Rewrite的live/booking命名范围必须在首次注册前冻结；
Writer不得越过range后再补保护。
GA 测试必须证明：

- GC register/cycle 线性化；
- renew 与 CleanupExpired 竞态；
- TN restart 后 final fail closed；
- protection 集合包含 source Snapshot 所需 Tombstone；
- protection 集合包含全部实际live staging和external booking；
- build超过当前GCTTL时未发布staging不被orphan GC删除；
- history low watermark 已越过 `S` 时不尝试猜测 delta，直接 re-export。

## 6. Build 阶段

Build 阶段不持有可写事务，不持有 SQL table lock，不调用 TN final mutation。

### 6.1 LifecycleRewriteHost

新增：

```text
pkg/lifecycle/rewrite/
  host.go
  classifier.go
  archive_sink.go
  live_writer.go
  transfer.go
  report.go
```

核心类型：

```go
type RewriteSpec struct {
    AttemptID         types.Uuid
    ExecutorEpoch     uint64
    PhysicalTableID   uint64
    SchemaGeneration  uint64
    SchemaDigest      [32]byte
    SourceSnapshotTS  types.TS
    Cutoff             CutoffValue
    LifecycleSeqnum    uint16
    SourceObjects      []ExactObjectRef
    Mode               LifecycleCommitMode
    TargetObjectSize   uint32
    Limits             RewriteLimits
}

type RewriteReport struct {
    SourcePhysicalRows uint64
    SourceVisibleRows  uint64
    SnapshotDeletedRows uint64
    ExpiredRows        uint64
    LiveRows           uint64
    ExpiredBytes       uint64
    LiveBytes          uint64
    ReachedObjects     uint64
    ReachedBlocks      uint64
    ArchiveRoot        [32]byte
    LiveRoot           [32]byte
    SourceDigest       [32]byte
    TransferDigest     [32]byte
    CreatedObjects     []objectio.ObjectStats
    Complete           bool
}
```

### 6.2 单次读取双输出

`LifecycleRewriteHost.LoadNextBatch`：

1. 复用 CN Merge 的 `BlockDataReadNoCopy` 在 `S` 读取 block；
2. 得到 Snapshot Tombstone bitmap `D`；
3. 对 `!D` 的行计算 lifecycle predicate；
4. 将到期可见行同步写 Archive Sink；
5. 构造 `E = expired visible row bitmap`；
6. 返回原 Batch 和 `D UNION E` 给现有 `DoMergeAndWrite`；
7. `DoMergeAndWrite` 只把存活行写入新 normal TAE Object；
8. 到期行在 transfer map 中保持 `api.NoTransfer`。

伪代码：

```go
bat, snapshotDeletes, release, err := host.readBlock(ctx, block)
if err != nil {
    return nil, nil, nil, err
}

expired := classifyExpired(bat, snapshotDeletes, spec.Cutoff)
if spec.Mode.HasArchive() {
    if err := host.archive.AppendSelected(ctx, bat, expired); err != nil {
        release()
        return nil, nil, nil, err
    }
}

rewriteDeletes := snapshotDeletes.Clone()
rewriteDeletes.Or(expired)
host.report.Observe(bat, snapshotDeletes, expired)
return bat, rewriteDeletes, release, nil
```

Archive append 必须在 `LoadNextBatch` 返回前完成，不得异步保留 borrowed Vector。
它按04定义写入source-ordinal substream，不能把mergesort callback到达顺序当作
Archive行序。Batch/release exactly-once规则继续由04定义。

### 6.3 物理排序

新 live Object 仍按表的现有 physical primary/cluster sort key 调用
`DoMergeAndWrite`：

- 不按 lifecycle column伪装成 sorted；
- 不破坏现有 sort-key ZoneMap 和查询假设；
- 未定义 sort key 时走现有 reshape；
- 生命周期列与 physical sort key不一致时，未来普通 Merge 仍可能重新形成 Mixed。

### 6.4 Whole

Whole TTL：

- Metadata 和 final TN 再校验 `max < cutoff`；
- 不读 payload；
- 不创建 Archive 或 live Object；
- transfer table为空。

Whole Archive：

- 读取所有 Snapshot visible rows到 Archive；
- `live_rows == 0`；
- 不创建 live Object；
- source Object整体 DropIntent。

Snapshot visible rows为零时不创建空 Archive Dataset，提交 `EMPTY_ARCHIVE` Receipt。

### 6.5 Mixed TTL

```text
expired -> discard
live    -> new TAE Object
```

仍计算 expired row count和 source/live conservation，但不写 Parquet。

### 6.6 Mixed Archive

```text
expired -> Parquet/ZSTD
live    -> new TAE Object
```

Archive Writer 完成后执行全量 provider readback。TAE live Object使用现有
Object Writer sync/checksum；P0 额外验证 row conservation和 transfer mapping。

## 7. Build 完成不变量

进入 final transaction 前必须同时满足：

```text
reservation valid
source protection valid
ReachedObjects == requested Objects
ReachedBlocks == requested Blocks
Complete == true
SourceVisibleRows == ExpiredRows + LiveRows
sum(CreatedObject.Rows) == LiveRows
count(non-NoTransfer mappings) == LiveRows
all snapshot-deleted rows map to NoTransfer
all expired rows map to NoTransfer
all live rows map to one valid destination
```

Archive mode额外：

```text
archive readback rows == ExpiredRows
archive readback root == ArchiveRoot
Manifest root/digest VERIFIED
Cleanup Root owns all payload/manifest/live staging objects
```

TTL mode额外：

```text
archive payload count == 0
dataset_id/manifest_root empty
```

任何不变量失败：

- 不进入 final；
- source保持当前可见；
- Cleanup Root清理 archive和新 live staging；
- release reservation/protection only after cleanup ownership明确转移。

## 8. Cleanup Root 扩展

第一次外部副作用前创建 Root。对 Rewrite，“外部副作用”包括：

```text
Archive namespace PUT/multipart
TAE shared FileService new live Object write
transfer booking file write
```

Root Object type：

```text
ARCHIVE_PAYLOAD
ARCHIVE_MANIFEST
ARCHIVE_SIDECAR
TAE_LIVE_STAGING_OBJECT
TAE_TRANSFER_BOOKING
```

### 8.1 在现有 `PrepareNewWriter` 前取得所有权

当前 `mergesort.MergeTaskHost.PrepareNewWriter()` 不返回 `error`，因此不能在该
callback 内临时执行一个可能失败的 Root Catalog transaction。为避免修改所有普通
Merge host，Lifecycle Rewrite 在调用 `DoMergeAndWrite` 前预注册一个有界
write-ahead ownership envelope：

1. 生成 attempt 专属 `segmentID`；
2. 冻结 `ordinal ∈ [0, max_created_live_objects)`，协议硬上限 `< 255`；
3. 在 system transaction 插入一个
   `TAE_LIVE_SEGMENT_RANGE/ALLOCATED` Root child，记录 segmentID、ordinal 上限和
   FileService namespace；
4. transaction committed 后才进入 `DoMergeAndWrite`；
5. Lifecycle host 的 `PrepareNewWriter`只使用该 segmentID和递增ordinal，不执行
   Catalog I/O；
6. 每次 `Sync` 后追加 exact `TAE_LIVE_STAGING_OBJECT/VERIFIED` child，冻结
   ObjectStats/checksum；
7. crash发生在FileService write与exact child之间时，Sweeper按range计算全部可能
   Object name并逐个`Stat`/删除；
8. writer ordinal 到达 range 上限时，必须在创建下一 writer 前停止 Rewrite。

这只增加一个 write-ahead range row，而不是每个 attempt 固定预插254行。P0必须证明
`ConstructWriterWithSegmentID`的输出严格落在该可枚举范围，且 segmentID 不被其他
attempt复用；若证明不了，必须给mergesort增加显式、向后兼容的
`BeforeCreateObject(name) error` hook，不能退化为写后补Root。

当前`pkg/objectio/ioutil/writer.go`中该constructor直接调用
`objectio.BuildObjectName(segmentID, num)`，所以这个方案在现有代码上可实现；测试
仍需冻结该命名合同，防止未来writer改名后绕过Root/Protection。

Transfer booking 不在 `PrepareNewWriter` callback 内生成。每个 booking page 都使用
deterministic attempt/page ordinal，并在 FileService write 前单独提交
`TAE_TRANSFER_BOOKING/ALLOCATED` child。

### 8.2 Transfer booking 不能沿用 Prepare 即删

当前 CN Merge 的`writeTransferMapsToS3`使用随机临时文件，
`marshalTransferMaps`在TN Prepare读取后立即删除`BookingLoc`。这对 Lifecycle
不满足 duplicate Prepare、`ErrTAENeedRetry`和commit-unknown的可重读要求。

实现必须抽取共享 codec，同时保持两种Owner策略：

```text
ordinary Merge:
  existing random temp name
  existing load-and-delete behavior

Lifecycle:
  deterministic attempt/page key
  Root child committed before write
  TN Prepare load without delete
  duplicate Prepare/retry can reopen exact immutable booking
  delete only after final txn is authoritatively committed or aborted
```

Lifecycle handler不得调用会修改原request并删除文件的旧helper。它使用不可变
`booking key/version/digest`重新构造transfer maps，并校验每页root。结果明确后：

- committed：live/range child转`TAE_OWNED`，booking child单独
  `DELETE_PENDING -> DELETED`；
- aborted：booking与所有未发布staging一起删除；
- unknown：booking保持`VERIFIED`，不删除。

Lifecycle handler的错误defer不得调用普通Merge
`merge.CleanUpUselessFiles(req, fs)`；该函数会删除`BookingLoc`和created Object，
而这些资源已经归Cleanup Root所有，且`ErrTAENeedRetry`仍需复用。TN错误路径只回滚
本地Catalog/txn entry并返回错误，物理staging由Root在权威transaction结果明确后
收敛。

TTL Rewrite Root只有在全部booking确认删除后才能进入`TRANSFERRED`；此前进入
`POST_COMMIT_CLEANUP`。Archive Root可以进入`PUBLISHED`，但其temporary booking
child仍由Sweeper清理；不得把整个Root置为`DELETE_PENDING`而误删Archive Payload。

提交成功时：

- Archive Payload/Manifest 由 Root 保持清理 Owner，Dataset 提供可见引用；
- TAE live/range Root child标为`TAE_OWNED`，由TAE Catalog/GC接管；
- transfer booking由Root在事务结果明确后单独删除；
- Archive Root进入`PUBLISHED`等待Purge/owner drop；
- 无Archive的TTL Rewrite Root先进入`POST_COMMIT_CLEANUP`，booking清理完成后进入
  `TRANSFERRED`，只保留短期审计身份。

提交 aborted时：

- Archive对象按 07 删除；
- TAE live staging按现有 Merge cleanup方式删除；
- transfer booking删除；
- source对象不由 Root删除。

结果 unknown时禁止删除上述任何可能已提交对象。

## 9. Wire 协议

### 9.1 独立 opcode

在当前 `OpFaultInject = 2017` 后新增：

```proto
OpCommitLifecycle = 2018;
```

不把 optional lifecycle字段塞进 `OpCommitMerge`。原因：

- 老 TN 会忽略未知 protobuf字段并按普通 Merge提交，违反 fail closed；
- 新 opcode 在老 TN 返回 `unknown write op`；
- capability gate 能明确阻止滚动升级期间退休。

路由复用：

```text
txnOp.Write
  -> pkg/txn/storage/tae/write.go
  -> HandleCommitLifecycle
  -> same TAE txn identified by TxnMeta
```

### 9.2 Protobuf

```proto
message LifecycleSourceObject {
    bytes object_stats = 1;
    bytes object_stats_digest = 2;
}

message LifecycleReservation {
    bytes attempt_id = 1;
    uint64 executor_epoch = 2;
    uint64 reservation_generation = 3;
    int64 expire_at_unix_nano = 4;
    bytes source_digest = 5;
}

message LifecycleCommitEntry {
    enum Mode {
        MODE_UNKNOWN = 0;
        WHOLE_TTL = 1;
        WHOLE_ARCHIVE = 2;
        MIXED_REWRITE_TTL = 3;
        MIXED_REWRITE_ARCHIVE = 4;
        EMPTY_ARCHIVE = 5;
    }

    uint32 protocol_version = 1;
    Mode mode = 2;

    uint64 database_id = 3;
    uint64 physical_table_id = 4;
    uint64 logical_table_id = 5;
    bytes account_incarnation = 6;

    uint64 schema_generation = 7;
    uint32 tae_schema_version = 8;
    bytes schema_digest = 9;
    bytes binding_id = 10;
    uint64 binding_generation = 11;
    uint64 guard_version = 12;
    bytes guard_digest = 13;

    uint32 lifecycle_column_seqnum = 14;
    uint32 lifecycle_type = 15;
    bytes cutoff_canonical = 16;
    timestamp.Timestamp evaluation_time_utc = 17;
    timestamp.Timestamp source_snapshot_ts = 18;

    repeated LifecycleSourceObject source_objects = 19;
    bytes source_digest = 20;
    uint64 source_visible_rows = 21;
    uint64 snapshot_deleted_rows = 22;
    uint64 expired_rows = 23;
    uint64 live_rows = 24;
    bytes archive_root = 25;
    bytes live_root = 26;

    bytes dataset_id = 27;
    bytes manifest_root = 28;
    bytes receipt_digest = 29;

    MergeCommitEntry merge = 30;
    LifecycleReservation reservation = 31;
    string source_protection_job_id = 32;
    int64 source_protection_valid_ts = 33;

    bytes transfer_digest = 34;
    bytes entry_digest = 35;
}
```

`merge`：

- `MergedObjs` 必须与 `source_objects` 完全一致；
- `CreatedObjs` 是新 live TAE ObjectStats；
- `Booking/BookingLoc` 使用现有 transfer encoding；
- `StartTs` 等于 `source_snapshot_ts`；
- `Err` 必须为空；
- `Level` 保留现有 Object level规则。

### 9.3 Wire 限额

```text
protocol_version          = 1
source objects            <= 64（Rewrite默认 <= 16）
created objects           < 255
inline transfer rows      < 500,000
serialized entry          <= 1 MiB
booking files/bytes       <= release profile
```

所有 repeated source按 Object ID升序。Digest基于 canonical protobuf（排除自身
digest字段）。

## 10. CN Finalizer

Finalizer只在 Build `VERIFIED` 后开始：

```text
allocate normal tenant txn
  -> system txn CAS Root VERIFIED -> FINALIZING
       freeze final_txn_id
       freeze LifecycleCommitEntry digest
       freeze receipt/dataset/manifest identity
  -> tenant txn CAS Guard/Binding/active attempt
  -> tenant txn insert Dataset/Receipt（Archive）
     or TTL Receipt
  -> txnOp.Write(OpCommitLifecycle, exact entry)
  -> commit
```

Dataset/Receipt和 `OpCommitLifecycle` 必须由一个不可拆分的 finalizer API加入同一
`TxnOperator`。任何一步写入 workspace失败都 rollback同一 transaction。

禁止：

- 先提交 Dataset再退休；
- 先退休再异步补 Dataset；
- response lost后用新 txn ID重发；
- final transaction执行 Provider I/O；
- reservation过期后只看 Object仍存在就提交。

## 11. TN Handler

新增：

```text
pkg/txn/storage/tae/write.go
pkg/vm/engine/tae/rpc/handle_lifecycle.go
pkg/vm/engine/tae/tables/txnentries/lifecycle_objects.go
```

### 11.1 解析和 digest

1. opcode只接受 `LifecycleCommitEntry`;
2. protocol/version/capability匹配；
3. canonical重新计算 `entry_digest`;
4. mode字段组合合法；
5. source/created/booking行数和大小有界；
6. nested `MergeCommitEntry` 与顶层 identity一致。

未知字段版本、越界、digest mismatch均返回明确错误，不降级普通 Merge。

### 11.2 Identity/Guard

TN校验：

```text
account incarnation
physical/logical table IDs
TAE schema version/digest
source Snapshot <= current prepare
Binding/Guard generation proof
attempt/executor epoch
```

Binding/Guard/Receipt是同一正常事务的 Catalog写。Finalizer必须先在 tenant
workspace中完成对应 CAS；TN lifecycle txn memo记录 expected receipt digest，
Prepare前由 lifecycle finalizer adapter确认 matching Catalog write已经加入同一
TxnOperator。缺失或不同 digest则整个 txn abort。

### 11.3 Reservation/protection

```text
ReservationManager.Validate(token, source)
SyncProtectionManager.ValidateSyncProtection(jobID, prepareTS)
```

任一失败 abort，不自动重新 acquire。

### 11.4 Exact source Object

对每个 source：

```text
exists in physical table
is persisted data Object
create committed and visible
no DropIntent
ObjectStats bytes/digest exact match
not appendable
source ordering/digest exact
```

Object missing或已有 DropIntent不能视为幂等成功。只有一致性 Receipt对账能判断原
attempt已提交。

Whole TTL额外从 lifecycle column Metadata再次证明：

```text
valid, non-truncated ZoneMap
null_count == 0
max < cutoff
```

### 11.5 Row conservation/transfer

Rewrite：

```text
sum source visible       == source_visible_rows
expired + live           == source_visible_rows
sum CreatedObject.Rows   == live_rows
non-NoTransfer mappings  == live_rows
transfer digest          == recomputed digest
```

TN不重读 Archive provider，不信任 Archive ETag。Archive readback证明由 Dataset/
Manifest/Root状态和同事务 Receipt保证；TN只校验冻结 digest组合。

## 12. 复用 Merge transaction entry

新增 Lifecycle wrapper，但内部复用现有 `mergeObjectsEntry`：

```go
type LifecycleMergeOptions struct {
    CollectDeletesFrom types.TS
    ExpectedExpiredRows uint64
    FailOnDeleteOfNoTransfer bool
    ReservationToken LifecycleReservationToken
}

func HandleLifecycleMergeEntryInTxn(
    ctx context.Context,
    txn txnif.AsyncTxn,
    req *api.LifecycleCommitEntry,
    transfer *mergesort.TransferTable,
    rt *dbutils.Runtime,
) error
```

内部：

1. 以不修改request、不删除Root资产的loader读取immutable booking；
2. `rel.SoftDeleteObject` source；
3. `rel.CreateNonAppendableObject` created live Object；
4. 构造 `mergeObjectsEntry`；
5. phase-1 Tombstone扫描起点使用 `source_snapshot_ts`，不是 final txn startTS；
6. prepare transfer pages；
7. `txn.LogTxnEntry` 复用 create/drop/transfer/WAL command；
8. Lifecycle wrapper只保存validation/digest memo，不创建第二套 Object WAL格式。

这个wrapper不拥有物理文件cleanup权，不能触发普通CN Merge handler的
`CleanUpUselessFiles` defer。Lifecycle request在duplicate Prepare和retry中保持
immutable，booking location不能被loader原地截短。

普通 `HandleMergeEntryInTxn` 继续使用：

```text
CollectDeletesFrom = txn.GetStartTS()
FailOnDeleteOfNoTransfer = existing behavior
```

Lifecycle不改变普通调用者。

## 13. 并发 Tombstone 语义

### 13.1 source Snapshot 前

Reader应用 `S` 前 Tombstone：

- 不进 Archive；
- 不进 live Object；
- transfer为 `NoTransfer`；
- 计入 `snapshot_deleted_rows`。

### 13.2 `S` 后、Prepare 前

现有 Merge两阶段扫描：

```text
phase 1: (S, collect_ts]
phase 2: (collect_ts, prepare_ts.Prev()]
```

若 Tombstone命中 live row：

- transfer到新 live RowID；
- 正常用户 DELETE/UPDATE语义保持。

若 Tombstone命中 expired/archived row：

- mapping为 `NoTransfer`；
- Lifecycle final transaction返回冲突并整体 abort；
- Dataset不发布，旧 source不退休；
- staging由 Root清理；
- re-export包含新的 Snapshot可见性。

这是 Archive 不复活并发删除的关键条件。

### 13.3 Prepare 后

Transfer page对 expired row没有 destination。一个更早开始、在 Lifecycle Prepare
后尝试提交的用户删除：

- transfer lookup得到 `NoTransfer`；
- 返回普通事务冲突；
- 不允许静默提交一个无法映射的删除。

一个 Lifecycle commit之后才开始的用户事务已看不到退休行。

### 13.4 INSERT

新 INSERT不在 frozen source set：

- 不影响本 child conservation；
- 新 row留在当前表；
- 迟到且已过期的数据由后续 cycle处理。

## 14. 1PC、2PC、retry 和 response lost

### 14.1 1PC/2PC

`OpCommitLifecycle` 是正常 `TxnOperator.Write`：

- 单 participant满足条件时可1PC；
- Dataset/Receipt或其他 Catalog participant存在时走2PC；
- 所有 participant同一 Txn ID和commit decision；
- Lifecycle不自建 commit coordinator。

### 14.2 重复 Write/Prepare

TAE txn memo key：

```text
(txn ID, attempt ID, entry digest)
```

规则：

- same txn/same digest重复到达：返回同一注册结果；
- same txn/different digest：fatal transaction error；
- different txn/same attempt：只有旧 txn明确 aborted且 Root CAS新 txn后才允许；
- source Object missing不作为duplicate success。

### 14.3 `ErrTAENeedRetry`

Retry必须：

- 从原始 request bytes重新解析同一 Lifecycle entry；
- 使用同一 final Txn ID语义和 entry digest；
- 重新校验 reservation/protection；
- 不重新执行 Build或Provider PUT；
- 旧 TAE txn的DropIntent/created catalog node随rollback清理；
- staging live/Archive仍由Root持有。

### 14.4 Response lost

```text
txn service committed
AND consistent tenant read sees matching Receipt/Dataset
  -> COMMITTED

txn service aborted
  -> ABORTED

status unknown
  -> COMMIT_UNKNOWN
```

`COMMIT_UNKNOWN`：

- 不释放/覆盖Root；
- 不删除live staging或Archive；
- 不启动新final txn；
- 尝试保持source protection；续租失败也不能推断aborted；
- 由正常 txn status helper和一致性Receipt读取收敛。

## 15. WAL、Replay 与 GC

### 15.1 WAL

最终 Object变化继续由现有 merge transaction command表示：

```text
dropped source Object IDs
created live Object IDs
```

Replay不需要重放 Provider I/O、reservation或Scanner。Lifecycle Dataset/Receipt由
普通 Catalog DML WAL恢复。

### 15.2 Transfer page

Transfer map encoding和TN内存transfer page沿用现有机制。外部`BookingLoc`的Owner
和删除时机按8.2节改为Root管理，不能沿用普通Merge的Prepare即删。Lifecycle transfer
map允许`NoTransfer`表示到期行不存在destination；这与现有pre-deleted row语义一致。

### 15.3 GC

commit前：

- source protection阻止列入集合的Data/Tombstone文件删除；
- reservation降低source被Merge替换概率。

commit后：

- source Object已有正常 DropIntent；
- source protection解除；
- existing checkpoint/logtail/GC判断何时物理删除；
- Lifecycle不追踪GC delete completion作为Dataset发布条件；
- Archive Payload由Lifecycle Sweeper，不由TAE GC管理。

## 16. 普通 Merge 回归边界

允许的窄改动：

1. Scheduler从 reservation manager读取 exact Object skip hint；
2. `HandleMergeEntryInTxn` admission拒绝被其他 attempt reservation的source；
3. 抽取可配置 `collectDeletesFrom` 的内部 constructor；
4. Lifecycle调用同一 create/drop/transfer实现。

明确不改：

- L0/Ln/vacuum选对象算法；
- physical sort key；
- target object size默认；
- `DoMergeAndWrite` 普通 host行为；
- transfer map wire格式；
- 普通 Merge level提升；
- GC删除谓词；
- 未绑定表任何 Catalog写。

reservation map为空时，只允许一次廉价空检查；性能测试必须证明未绑定表
Merge P95/P99无显著回归。

## 17. 资源上限

每个 Rewrite child：

```text
source objects             <= 16 default, 64 hard protocol
source bytes               <= 4 GiB default
created live objects       <= 32 default, <255 hard
transfer memory/disk       <= 512 MiB default
archive staging bytes      <= source expired estimate × profile
TAE live staging bytes     <= source live estimate × profile
attempt wall time          <= 60 minutes
reservation TTL            = 2 minutes, renew 30 seconds
source protection TTL      = 20 minutes, renew 5 minutes
protected filenames        <= release profile
protection Bloom bytes      <= 8 MiB default
snapshot-exclusive bytes   <= account/cluster hard limit
Provider single I/O        <= 2 minutes
final transaction          <= 60 seconds
```

单个当前合法 Object可以达到：

```text
8192 rows/block × 256 blocks = 2,097,152 rows
Object writer size limit      = 3 GiB
```

GA必须支持该边界的 streaming，不分配整 Object Batch，也不能因默认4 GiB child
限制把合法单 Object永久重试。

## 18. 错误和终态

| 条件 | 结果 |
|---|---|
| reservation conflict/lost | replan；达到期限 `CONFLICT_BLOCKED` |
| source protection register/renew失败 | cancel build；不final |
| source Object被Merge替换 | exact validation失败；replan |
| Archive/live writer失败 | cleanup staging；源不变 |
| post-S delete命中expired row | final abort；re-export |
| post-S delete命中live row | transfer到新RowID |
| transfer missing/overflow | final abort；`RESOURCE_BLOCKED`或replan |
| final txn明确aborted | cleanup staging |
| final txn unknown | 保留全部owner；Reconcile |
| GC删除已commit旧source | 正常，不由Lifecycle干预 |

## 19. P0 测试矩阵

### 19.1 Split 正确性

- 0/1/max rows；
- Whole、tiny Mixed、50/50、几乎全过期、几乎全存活；
- Snapshot已有 Tombstone；
- lifecycle值等于cutoff；
- NULL/类型/ZoneMap异常；
- Archive rows + live rows == source visible rows；
- expired永不出现在live Object；
- live永不出现在Archive；
- transfer每个live row恰好一个destination。

### 19.2 Merge 复用

- 有/无 physical sort key；
- PK、cluster by、fake PK；
- output 0/1/多 Object；
- created object count逼近 `api.NoTransfer`；
- inline/external booking；
- rollback清理全部created Object/booking；
- transfer phase-1/phase-2每个错误都向上返回并abort。

### 19.3 并发

- ordinary Merge在Acquire前/后/Build中/final前提交；
- user-forced Merge不能绕过reservation admission；
- reservation expire/renew/TN restart；
- DELETE/UPDATE在S前、phase1、phase2、Prepare后；
- delete archived row必须conflict/re-export；
- delete live row必须transfer；
- INSERT late old/new row；
- DROP/TRUNCATE/schema/binding generation变化。

### 19.4 GC protection

- register与GC cycle并发；
- data和tombstone文件都受保护；
- renew失败；
- CleanupExpired；
- unregister前后checkpoint watermark；
- TN restart protection lost；
- history low watermark越过S。

### 19.5 事务和恢复

- 1PC/2PC；
- duplicate Write/Prepare；
- `ErrTAENeedRetry`；
- CN/TN crash在每个Create/Drop/transfer/catalog write之间；
- commit response lost；
- Receipt存在/缺失/digest mismatch；
- WAL replay后active source/live Object集合正确；
- rolling upgrade老TN明确拒绝2018 opcode。

### 19.6 普通路径回归

- 未绑定表普通Merge selection/task/result不变；
- reservation为空/少量/大量的并发访问；
- normal Merge SoftDelete ExpectedEOB语义不被Lifecycle改写；
- 普通SELECT/DML/GC无Lifecycle分支；
- 10 TiB持续Merge + Rewrite soak，无无界reservation/transfer/staging增长。

## 20. 决策记录

1. Lifecycle“接管 Merge”仅指 exact source Object 的专用 Rewrite，不接管整张表。
2. Provider I/O永不进入普通 Merge worker和final transaction。
3. Reservation线性化普通Merge/Lifecycle的source准入；final
   exact Object/generation/digest CAS仍是不可省略的数据安全边界。
4. Reservation不持久化，TN restart必须让旧attempt fail closed。
5. 复用SyncProtection保护exact Data/Tombstone文件，不增加长期table-only Snapshot。
6. 到期行使用`NoTransfer`，不生成逐行Tombstone。
7. 存活行复用现有Merge create/drop/两阶段transfer/WAL/GC。
8. 使用独立`OpCommitLifecycle`，防止老TN把新协议当普通Merge执行。
9. 首个GA不改变普通Merge的physical layout策略；乱序表的持续成本由budget和SLO限制。
