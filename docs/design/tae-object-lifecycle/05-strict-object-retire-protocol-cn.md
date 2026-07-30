# Lifecycle Object Retire 与 Mixed Rewrite 协议详细设计

> 文件名为兼容已有链接保留 `strict-object-retire`。
>
> 本文唯一负责 Whole Object 退休、Mixed Rewrite、source reservation/protection、
> CN 构建器、tagged Lifecycle commit wire、TN Prepare、两阶段 Tombstone transfer、
> WAL/replay、commit unknown、滚动升级以及与普通 Merge 的协调。

## 1. 结论

Lifecycle 不把归档逻辑塞进普通 Merge，也不复制第二套 Merge/GC。

这里的“复用 Merge”有严格边界：

- `DoMergeAndWrite` 是新 live Object、`CreatedObjs` 顺序和
  source-row -> destination-row `TransferTable` 的唯一 producer；
- Lifecycle 只提供固定 Snapshot 的输入 Batch 和 `D UNION E` delete bitmap，禁止
  重新生成、修改、排序、压缩或合并 destination mapping；
- final transaction 复用现有 create/drop/transfer/WAL/Replay/GC 原语，但通过薄
  Lifecycle wrapper增加 Snapshot 起点、exact source CAS、Root ownership、Dataset/
  Receipt 原子发布和有界 Tombstone delta；
- 不直接调用普通 `Relation.MergeObjects`、`HandleCommitMerge` 或未参数化的
  `mergeObjectsEntry`，也不建设第二套排序、Object writer、transfer 或 Object WAL。

大 Mixed 的正式路径是一个独立的、对象级 Archive-aware Merge：

```text
claim exactly one source Object
  -> 固定 source_snapshot_ts
  -> 注册 exact source GC protection
  -> streaming read the source Object once
       expired visible rows -> Parquet/ZSTD
       live visible rows    -> new normal TAE Objects
       snapshot-deleted rows-> neither output
  -> verify Archive and live output
  -> short normal distributed transaction
       Dataset/Receipt
       exact source validation
       create new live Objects
       DropIntent old source Object
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

`TransferTable`必须直接取自同一次`DoMergeAndWrite`调用。现有 merger/reshape在写入
destination row的同一循环中记录当前位置，并在writer `Sync`顺序中追加
`CreatedObjs`；因此`CreatedObjs`顺序是mapping中`ObjIdx`的组成部分，不是可重新排序
的展示字段。Lifecycle只允许序列化、校验和重建这张表，不允许调用
`UpdateMappingAfterMerge`或实现另一套destination算法。

但不能直接调用普通 `Relation.MergeObjects`、普通
`HandleCommitMerge` 或未参数化的 `mergeObjectsEntry`。当前普通entry会在
`PrepareRollback`异步物理删除全部`createdObjs`，phase 2仍使用
`context.Background()`。早期Tombstone scan error吞错已由
[PR #26333](https://github.com/matrixorigin/matrixone/pull/26333)修复，后续不得回退；
剩余deadline/工作量预算缺口由
[Issue #26377](https://github.com/matrixorigin/matrixone/issues/26377)独立跟踪。
`NewMergeObjectsEntry -> txn.LogTxnEntry`之间的注册前资源Owner缺口已经由真实TAE
故障注入确认，并由
[Issue #26445](https://github.com/matrixorigin/matrixone/issues/26445)独立跟踪。
这些普通Merge行为仍未提供Lifecycle所需的Root ownership、有界delta和caller
deadline。Lifecycle只复用排序、写Object、transfer page、create/drop WAL与Replay
核心，并使用本文定义的wrapper和options。

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

### 2.3 Object Rewrite 十项 P0 决策

| Review P0 | 固定决策 |
|---|---|
| RWT-P0-1 | live staging/booking由Cleanup Root拥有；Lifecycle rollback不物理删除 |
| RWT-P0-2 | phase 1/2使用有界、可取消、错误不吞的Tombstone delta visitor |
| RWT-P0-3 | transfer machinery强制开启，但只给live survivor建立destination |
| RWT-P0-4 | Lifecycle只允许immutable external booking，禁止inline |
| RWT-P0-5 | 一个Rewrite child严格一个source Object；资源按物理slot admission |
| RWT-P0-6 | TN验证identity和结构守恒；CN承担TTL/D分类，不做第二次业务值全扫 |
| RWT-P0-7 | Lifecycle tagged entry必须进入可重放Commit payload；每代TAE txn从immutable bytes重新构造私有entry |
| RWT-P0-8 | `LogTxnEntry`成功是runtime资源Owner转移点；注册前失败由builder幂等清理，Root物理文件不动 |
| RWT-P0-9 | CN使用独立、单值、不可变的commit-control通道携带tag；普通workspace不得过滤、合并或dump它 |
| RWT-P0-10 | 每代必须在任何TAE txn mutation前取得唯一BUILDING slot；失败后整代rollback，禁止同代重建 |

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
    AttemptID       types.Uuid
    ExecutorEpoch   uint64
    ReservationGen  uint64
    PhysicalTableID uint64
    ReservedMode    LifecycleCommitMode
    SourceSetDigest [32]byte
    SourceLayouts   []SourceLayoutProof
    ExpireAt        time.Time
}

type SourceLayoutProof struct {
    ObjectID         objectio.ObjectId
    ObjectStatsDigest [32]byte
    BlockCount        uint32
    PhysicalRows      uint64
    BlockLayoutDigest [32]byte
}

type LifecycleReservationManager interface {
    Acquire(
        ctx context.Context,
        tableID uint64,
        mode LifecycleCommitMode,
        objects []objectio.ObjectStats,
        attemptID types.Uuid,
        executorEpoch uint64,
        ttl time.Duration,
    ) (LifecycleReservationToken, error)

    Renew(ctx context.Context, token LifecycleReservationToken, ttl time.Duration) error
    Release(ctx context.Context, token LifecycleReservationToken) error
    Validate(
        token LifecycleReservationToken,
        mode LifecycleCommitMode,
        objects []objectio.ObjectStats,
    ) error
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

1. 验证source Object已经按Object ID canonical升序，拒绝空集合、重复ID和mode超限；
2. 对每个source校验当前存在、非appendable、无DropIntent和ObjectStats/digest；
3. 由TN当前Catalog/ObjectStats为每个source生成一条`SourceLayoutProof`；
4. 在同一 manager mutex 下检查重复/冲突 reservation；
5. 插入exact set并返回带全部layout proof的generation token。

`SourceLayouts`与canonical source set严格一一对应，并按Object ID同序。
每条`SourceLayoutProof`再次携带Object ID和ObjectStats digest，防止proof错配到另一
Object。`SourceSetDigest`覆盖按序排列的
`(ObjectID, ObjectStats bytes/digest, SourceLayoutProof canonical bytes)`全集。
`SourceLayoutProof`是TN对物理布局的短期证明：它不包含业务值，也不证明TTL分类。
final Prepare必须用每个Object的当前metadata重新计算并逐项匹配；reservation丢失、
proof数量/顺序/内容不匹配都abort。`MIXED_REWRITE_*`要求长度恰好为1，
`WHOLE_*`允许协议上限内多源。

token冻结`ReservedMode`。final mode只允许以下单向转换：

```text
WHOLE_TTL             -> WHOLE_TTL
WHOLE_ARCHIVE         -> WHOLE_ARCHIVE | EMPTY_ARCHIVE
MIXED_REWRITE_TTL     -> MIXED_REWRITE_TTL | WHOLE_TTL
MIXED_REWRITE_ARCHIVE -> MIXED_REWRITE_ARCHIVE | WHOLE_ARCHIVE | EMPTY_ARCHIVE
```

不允许Whole反向升级Rewrite，不允许TTL/Archive互换。`expired == 0`是no-op，不产生
final entry，不能借“mode转换”提交。

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

- 只接受与 token 相同attempt/generation/`source_set_digest`；
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
  -> Rewrite: preallocate deterministic Root/segment/range/booking IDs and freeze name ranges
  -> build one BloomFilter covering source + future staging filenames
  -> RegisterSyncProtection
  -> close discovery txn
  -> create Snapshot Operator/read exact Objects at S
```

这里的`preallocate`只计算并冻结确定性身份，不创建Root或Root Object行。Root仍严格在
第一次真实外部副作用前创建；这样`visible == 0`时可以完全没有Root，同时
SyncProtection的BloomFilter又能在读取前覆盖未来可能出现的live/booking文件名。

`RegisterSyncProtection` 在 GC cycle 正在执行时会拒绝。Lifecycle 退避重试；不能
假装已保护。

### 5.2 续租和 Prepare 校验

- CN worker按现有 Publication worker模式续 `valid_ts`；
- 任一次续租失败立即 cancel Reader/Writer；
- TN Lifecycle entry Prepare 调用
  `ValidateSyncProtection(job_id, prepare_ts, expected_set_digest)`；
- TN restart 后 protection 丢失，final transaction abort；
- response unknown 后 protection 不因 worker lease丢失立即 unregister，直到原
  transaction 明确 committed/aborted；
- committed 后 source files 已有正常 DropIntent/transfer protection，可以
  unregister；
- aborted 后先清理 staging owner，再 unregister。

校验不能只证明“job ID存在且未过期”。token还必须冻结并验证
`expected_set_digest`，覆盖source Data/Tombstone filenames、live segment range和
booking range；否则调用方传入另一个仍有效job ID也可能通过。

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
    SourceObject       ExactObjectRef
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
    ReachedObject      bool
    ReachedBlocks      uint64
    ArchiveRoot        [32]byte
    LiveRoot           [32]byte
    SourceSetDigest       [32]byte
    TransferMappingDigest [32]byte
    CreatedObjects     []objectio.ObjectStats
    Complete           bool
}
```

`RewriteSpec`只能有一个source Object。Planner、wire和TN handler都必须重复校验，
不能把“默认1个”实现成可配置到16/64的soft limit。

### 6.2 单次读取双输出

`LifecycleRewriteHost.LoadNextBatch`：

1. 复用 CN Merge 的 `BlockDataReadNoCopy` 在 `S` 读取 block；
2. 得到 Snapshot Tombstone bitmap `D`；
3. 对 `!D` 的行计算 lifecycle predicate；
4. 将到期可见行同步写 Archive Sink；
5. 构造 `E = expired visible row bitmap`；
6. 返回原 Batch 和 `D UNION E` 给现有 `DoMergeAndWrite`；
7. `DoMergeAndWrite` 只把存活行写入新 normal TAE Object；
8. 到期行在 transfer map 中保持 `api.NoTransfer`；
9. 原样接收该次调用生成的`TransferTable`和按writer Sync顺序生成的
   `CreatedObjs`，不得做后处理重排。

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
它按04定义的单source block/row顺序同步写入 Archive Writer。Batch/release
exactly-once规则继续由04定义。

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

Rewrite build结束后的退化规则：

```text
source_visible_rows == 0:
  EMPTY_ARCHIVE/WHOLE_TTL退休空Object；不创建Dataset，也不创建Root

expired_rows == 0 AND live_rows > 0:
  no-op；不退休source，不发布Dataset
  若已出现live写入，Root清理live staging；不生成booking

live_rows == 0 AND expired_rows > 0:
  final mode切换为WHOLE_TTL/WHOLE_ARCHIVE
  不创建TAE live segment/range child，不生成external booking
  不允许以createdObjs==0的MIXED_REWRITE entry提交
  TTL/Archive都必须执行13.4的独立post-S delete validator
```

这样不会因为普通Merge entry在`createdObjs==0`时跳过transfer phase而漏掉Archive
并发删除校验。Root child的创建由“实际发生的第一类副作用”驱动，而不是由Planner
预测的初始mode驱动：

```text
attempt开始:
  只预分配Root ID、segment/range/booking名称和protection set

Archive首次出现E行、准备PUT前:
  创建Root（可同时冻结Archive和TAE namespace identity）
  创建ARCHIVE_PAYLOAD child

首次出现L行、把Batch交给mergesort/writer前:
  若Root不存在则先创建Root
  持久化TAE_LIVE_SEGMENT_RANGE child

live > 0且Rewrite完成:
  才生成external booking及其child
```

Archive计划为Rewrite但最终`live == 0`时，父Root允许保持`ARCHIVE_REWRITE`以保留已冻结
的双namespace identity，但不得存在TAE range/live/booking child。TTL全过期且无
Archive副作用时不创建Root。所有退化路径都必须按实际child集合进入可达终态。

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
ReachedObject == true
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
Merge host，Lifecycle先冻结确定性命名；只有分类实际发现第一条L行时，才在把该
Batch返回给`DoMergeAndWrite`的writer之前，提交一个有界write-ahead ownership
envelope：

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

如果最终没有L行，不创建range child；如果先创建range后实际writer没有产生Object，
range仍由Root从`VERIFIED -> DELETE_PENDING -> DELETED`收敛，禁止标为
`TAE_OWNED`。`live == 0`退化Whole的标准路径不会创建range。

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

普通Merge codec可以继续复用底层destination编码原语，但Lifecycle必须使用独立、
版本化的Booking V1 envelope，不能把现有`TransferHashPage.Marshal()`直接当成协议。
Lifecycle不需要在Booking中再次编码D/E业务分类；缺少mapping统一表示
`api.NoTransfer`。但现有codec会省略全部`NoTransfer` slot和全空Block，且没有冻结
Root、source layout、`CreatedObjs`顺序、实际物理行数和完整文件digest，无法区分
“合法无mapping”与“Booking被截断/错绑”，也不满足immutable retry ownership。
两种Owner策略：

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

### 8.3 Lifecycle Transfer Booking V1

Booking V1是`DoMergeAndWrite`产出的`TransferTable`的不可变运输格式，不是第二套
mapping算法，也不携带TTL分类语义。每个immutable booking文件由canonical header、
block records和trailer组成：

```text
Header:
  magic                         = "MOLTBK01" fixed 8 bytes
  codec_version                 = uint16(1)
  flags                         = uint16(0), unknown bit rejected
  root_id                       = 16 bytes
  root_object_ordinal           = uint32, kind-local
  tae_storage_namespace_digest  = 32 bytes
  source_object_id              = canonical types.ObjectidSize(18) bytes
  source_object_stats_digest    = 32 bytes
  source_layout_digest          = 32 bytes
  created_layout_digest         = 32 bytes
  first_source_block            = uint32
  source_block_count            = uint32
  block_record_count            = uint32

BlockRecord, repeated by increasing physical block ordinal:
  block_ordinal                 = uint32
  actual_rows                   = uint32
  live_mapping_count            = uint32
  LiveMapping[live_mapping_count]:
    source_row_offset           = uint32
    destination_object_ordinal  = uint8
    destination_block_ordinal   = uint16
    destination_row_offset      = uint32

Trailer:
  payload_length                = uint64
  canonical_payload_sha256      = 32 bytes
```

所有整数使用big-endian且不插入alignment padding；字段顺序、定长/变长字段长度前缀
和digest排除域固定如上。destination字段宽度与`api.TransferDestPos`一致；
`ObjIdx == api.NoTransfer(255)`禁止出现在LiveMapping。
`canonical_payload_sha256`覆盖从magic到`payload_length`的全部字节，不包含自身；
`payload_length`等于header加全部BlockRecord的字节数，不含trailer；
Root Object和wire中的`sha256`必须等于“完整文件（包含trailer）”SHA-256，两者不能
互相替代。未知version/flag、长度溢出、trailing bytes或digest不匹配都拒绝。

覆盖规则：

- 只编码每个Block的`actual_rows`物理slot，不编码`BlockMaxRows`未使用尾部；
- `actual_rows`和Block顺序来自冻结的`SourceLayoutProof`，destination只来自原始
  `TransferTable`；encoder把二者一一配对但不推导或改变mapping；
- 每个source Block恰有一个BlockRecord；没有任何destination的Block也必须以
  `live_mapping_count == 0`存在；
- `LiveMapping`必须是`DoMergeAndWrite`输出的原始mapping，按
  `source_row_offset`严格升序且source offset不重复；未出现的source slot统一重建为
  `api.NoTransfer`；
- destination必须落在exact `CreatedObjs`布局；所有Block的mapping总数必须等于
  `live_rows`和`sum(CreatedObject.Rows)`；
- destination object ordinal必须小于CreatedObjs数量且小于`api.NoTransfer(255)`，
  block/row ordinal必须落在对应created Object的实际布局内；
- TN必须为整个`LifecycleCommitEntry`分配一张全局destination bitmap，不能按booking
  page分别分配；destination按`CreatedObjs` producer顺序和各Block实际行数展平，
  每读取一条mapping先校验bounds，再以test-and-set拒绝跨page/同page重复，全部page
  结束后必须满足`set bits == live_rows == sum(CreatedObject.Rows)`且不存在缺口；
  该检查只证明Booking结构自洽，不重新证明TTL分类、source row为何应当存活或mapping
  的业务语义；bitmap按`ceil(live_rows/8)`计入TN booking memory token，并且只能在
  `live_rows`、created layout和hard limit全部通过checked arithmetic后分配；
- page的Block范围连续、互不重叠，全部page完整覆盖proof中的全部Block；
- booking引用冻结`root_id + TAE_TRANSFER_BOOKING kind-local ordinal +
  TAE namespace digest`，不能借用另一Root下内容相同的文件。

最大标准单Object的2,097,152个live mapping按每项
`4 + 1 + 2 + 4 = 11` bytes编码，约22 MiB；加header、256个BlockRecord和trailer后
仍低于32 MiB默认Booking hard limit。实现必须用checked arithmetic在任何分配/写入
前证明该上界；不得把上述字段实现为4个`uint32`导致最大合法Object必然越过限额。

`LifecycleTransferBooking`的key/version/size/完整文件SHA与上述header binding必须全部
匹配。TN先流式校验envelope，再构造运行时transfer page；不得先分配信任payload长度
的无界buffer。解码后重新编码必须得到完全相同的canonical bytes、mapping和
`created_layout_digest`。该格式是Lifecycle V1 wire的一部分，普通Merge codec和
临时`BookingLoc`字节保持兼容、不受影响。

### 8.3.1 V1 Digest 唯一命名与公式

V1只使用以下四个**协议聚合digest**，禁止再引入
`created_object_order_digest/transfer_layout_digest/transfer_digest`等同义字段：

```text
source_set_digest
created_layout_digest
transfer_mapping_digest
entry_digest
```

`object_stats_digest`、`block_layout_digest`、`canonical_payload_sha256`、完整Booking
文件`sha256`和`source_protection_set_digest`仍然存在，但它们分别属于Object证明、
文件checksum或Protection identity，不得作为上述四种聚合digest的别名。

共同编码规则：

- 哈希算法固定`SHA-256`；
- domain separator为下列ASCII字节并包含结尾`\x00`；
- 所有整数big-endian，定长字段直接写入，变长字段使用`uint32 length + bytes`；
- repeated字段严格按协议指定顺序聚合，禁止依赖Go map或protobuf未知字段顺序；
- unknown field、重复field或non-canonical编码直接拒绝，不能先归一化后接受；
- digest字段自身在计算对应digest时写全零。

公式固定为：

```text
source_set_digest =
  SHA256("MO-LIFECYCLE-SOURCE-SET-V1\x00"
    || source_count:u32
    || for each source in ObjectID order:
         object_id
         len(object_stats):u32 || object_stats
         object_stats_digest
         block_count:u32
         physical_rows:u64
         block_layout_digest)

created_layout_digest =
  SHA256("MO-LIFECYCLE-CREATED-LAYOUT-V1\x00"
    || created_count:u32
    || for each CreatedObj in exact DoMergeAndWrite producer order:
         object_ordinal:u32
         len(object_stats):u32 || canonical_object_stats
         block_count:u32
         rows:u64
         for each block in physical block ordinal order:
           block_ordinal:u32
           actual_rows:u32)

transfer_mapping_digest =
  SHA256("MO-LIFECYCLE-TRANSFER-MAPPING-V1\x00"
    || source_set_digest
    || created_layout_digest
    || booking_count:u32
    || for each booking in ordinal order:
         ordinal:u32
         first_source_block:u32
         source_block_count:u32
         live_mapping_count:u64
         canonical_payload_sha256)

entry_digest =
  SHA256("MO-LIFECYCLE-COMMIT-ENTRY-V1\x00"
    || deterministic LifecycleCommitEntry bytes with entry_digest zeroed)
```

`canonical_payload_sha256`仍按8.3节覆盖单个booking canonical payload；
`transfer_mapping_digest`负责跨booking page聚合，因此同一page换序、漏page、重复page
或跨page destination重复都不能通过。`entry_digest`覆盖
`source_set_digest/created_layout_digest/transfer_mapping_digest`、Root identity、
Receipt/Dataset identity和全部CAS字段。CN encoder、TN decoder和离线round-trip测试
必须共用同一codec包，禁止各自复制公式。

内存所有权固定为：

```text
DoMergeAndWrite
  -> SetTransferTable把CN slab唯一所有权交给Lifecycle host
  -> Booking encoder只读，不修改mapping
  -> immutable Booking write + readback/digest VERIFIED
  -> host在success/error/cancel任一路径exactly-once Release CN slab

TN Booking decoder
  -> 每个内部TAE generation首次注册时重建该generation私有TransferTable
  -> 同generation duplicate命中memo，不重复共享/注册第二个可变entry
  -> validation失败/cancel时decoder exactly-once Release
  -> validation成功后原子移交所有权给txn entry
  -> txn Prepare/Rollback exactly-once Release
```

Root只拥有immutable Booking文件，不拥有CN/TN内存；duplicate Prepare不得共享可变
`TransferTable`实例，也不得让Booking loader原地消费request字段。

V1不维护两套ordinal：`LifecycleTransferBooking.ordinal ==
root_object_ordinal == Booking header.root_object_ordinal`，共同表示
`TAE_TRANSFER_BOOKING` kind内的page ordinal。Payload/live/range的ordinal属于各自
kind，可以重复使用相同数值。

### 8.4 Created Object 物理所有权模式

复用的 Merge txn entry必须显式携带：

```go
type PhysicalCreatedObjectOwner uint8

const (
    CreatedObjectOwnedByMergeEntry PhysicalCreatedObjectOwner = iota + 1
    CreatedObjectOwnedByCleanupRoot
)
```

唯一语义：

| 调用者 | Owner | `PrepareRollback` |
|---|---|---|
| 普通 Merge | `MergeEntry` | 保持现有行为，删除created files |
| Lifecycle Rewrite | `CleanupRoot` | 只回滚Catalog node、transfer page和内存；禁止物理Delete |

Lifecycle构造entry时必须固定
`PhysicalCreatedObjectOwner=CreatedObjectOwnedByCleanupRoot`，且不能被调用者覆盖。
`PrepareRollback`、普通error defer、duplicate Prepare和`ErrTAENeedRetry`都只能释放
本事务的内存/Catalog状态。Root child按以下权威事实转移：

```text
txn committed + matching Receipt visible:
  TAE_LIVE_SEGMENT_RANGE/TAE_LIVE_STAGING_OBJECT -> TAE_OWNED

final txn service authoritatively reports ABORTED:
  VERIFIED -> DELETE_PENDING

txn unknown:
  remain VERIFIED; no physical delete
```

这关闭以下数据文件丢失窗口：

```text
first Prepare -> ErrTAENeedRetry -> rollback deletes live file
-> retry reuses ObjectStats -> Catalog commits pointer to missing file
```

实现应把物理Owner作为构造期只读字段并写单测；不能根据Root的事后查询临时判断，
否则rollback遇到Catalog不可用时会重新产生误删窗口。

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
- TAE live staging由Root在权威aborted后删除，普通Merge entry不删除；
- transfer booking删除；
- source对象不由 Root删除。

结果 unknown时禁止删除上述任何可能已提交对象。

## 9. Wire 协议

### 9.1 可重放的 tagged commit entry

V1不使用独立`TxnOperator.Write(OpCommitLifecycle)`。Lifecycle必须作为
`PrecommitWriteCmd.EntryList`中的显式tag，随正常commit request一起发送和重放：

```proto
message Entry {
    enum EntryType {
        // 0..6 保留当前已有值。
        LifecycleCommit = 7;
    }

    // 当前已有字段保持不变。
    bytes lifecycle_commit_payload = 11;
}
```

唯一合法组合是：

```text
entry_type                 = LifecycleCommit
lifecycle_commit_payload  = canonical LifecycleCommitEntry bytes
bat                        = nil
file_name                  = empty
database_id/table_id       = payload中的权威物理表路由
```

该tag不能作为普通`txn.writes Entry`加入workspace：当前`genWriteReqs`会跳过
`bat == nil || bat.IsEmpty()`，workspace merge/compact也会删除nil Batch，
`toPBEntry`自身要求Batch存在。CN必须使用9.1.1节的独立commit-control通道，在普通
Entry完成merge/dump/compact之后原样追加tag。TN iterator必须在普通
`apiEntryToWriteEntry`之前识别该tag，解析后调用`HandleCommitLifecycle`。未知版本、
非法字段组合或能力不匹配必须fail closed，绝不能把它按普通Insert/Delete Entry解释。

采用commit payload而不是独立Write，是因为当前`ErrTAENeedRetry`会新建内部TAE
transaction并只重放commit request中的payload。单独提前发送的Write不会自然进入新
generation，可能出现Dataset/Receipt重放成功、Object retirement却缺失。tagged entry
则在每个内部generation都从同一不可变payload和Booking重新构造。

Capability gate必须在CN发起退休前确认exact目标TN支持
`LifecycleCommit Entry V1`；滚动升级中未满足时只允许Planner/Export，不允许退休。
`database_id/table_id`必须路由到`source physical_table_id`所属权威TN shard，不能复用
调试Merge中“取第一个TN”的辅助路径。V1不新造当前MO没有权威来源的
`TopologyGeneration`。冻结的路由身份只能是`ServiceID + ShardID + ReplicaID`；
`TxnServiceAddress`不是身份，必须在发送前从权威cluster snapshot重新解析。Lifecycle
capability绑定`ServiceID + ShardID + ReplicaID + protocol_version`。该值不能只存在于
Lifecycle本地配置，权威链固定为：

```text
TN binary supported protocol
  -> TNStoreHeartbeat
  -> HAKeeper TNStoreInfo
  -> ClusterDetails
  -> metadata.TNService
  -> CN authoritative cluster refresh
```

旧节点缺少字段时按`unsupported/0`处理；Heartbeat过期、Replica变化或任一传播层缺值
立即撤销能力。实施必须同步修改`proto/logservice.proto`的heartbeat/store info、
`proto/metadata.proto`的TNService、TN heartbeat producer、HAKeeper state/ClusterDetails
和`clusterservice.newTNService`，不能用进程本地feature flag冒充权威能力。
V1字段冻结为：

```text
TNStoreHeartbeat.lifecycle_commit_protocol_version = field 12
TNStoreInfo.lifecycle_commit_protocol_version      = field 12
TNStore.lifecycle_commit_protocol_version          = field 12
metadata.TNService.lifecycle_commit_protocol_version = field 10
```

这些字段分别属于不同message，field number不跨message共享；升级前必须执行proto
compatibility/golden测试，禁止复用reserved字段或改变现有字段编号。

route refresh可能等待HAKeeper，绝不能在`txn.Lock`内进行。Finalizer和`genWriteReqs`
都必须在绝对deadline下先调用可返回错误的authoritative refresh，再持锁比较冻结身份；
任一身份或capability变化均返回`LIFECYCLE_ROUTE_CHANGED`并整体abort/replan。若当前
拓扑不能唯一解析source到一个TN shard（包括service有多个候选shard），V1 fail closed，
不得向全部shard广播Lifecycle tag。

Capability只是减少错误发送，旧协议节点的最后安全边界必须独立成立：TN在解析
`api.Entry.Bat`之前先校验EntryType。unknown EntryType或已知Lifecycle Entry的unknown
protocol version返回明确`UNSUPPORTED_TXN_ENTRY`，不得进入
`apiEntryToWriteEntry/ProtoBatchToBatch`，不得panic，也不得跳过tag后继续提交普通Entry。
这个通用fail-closed parser必须先在Safety Release部署到所有允许作为降级目标的TN。
重试不得改写payload、attempt identity或绝对deadline。

### 9.1.1 CN commit-control 通道

V1在`disttae.Transaction`中增加一个单值、事务本地的commit control，而不是把它混入
普通`writes []Entry`：

```go
type LifecycleCommitControl struct {
    Payload             []byte // canonical bytes；Set时深拷贝，此后只读
    EntryDigest         [32]byte
    CatalogPairDigest   [32]byte // CN-local finalizer proof，不是第五种wire聚合digest
    DatabaseID          uint64
    PhysicalTableID     uint64
    TargetServiceID     string
    TargetShardID       uint64
    TargetReplicaID     uint64
    ProtocolVersion     uint32
}

// unexported；只能由Catalog adapter在成功追加逻辑写后创建。
type lifecycleCatalogPairToken struct {
    txnID               []byte
    attemptID           [16]byte
    datasetID           []byte   // TTL为空
    receiptDigest       [32]byte
    manifestRoot        [32]byte // TTL为全零
    pairDigest          [32]byte
    workspaceGeneration uint64
    consumed            bool
}

type LifecycleFinalizeState uint8

const (
    LifecycleFinalizeSealed LifecycleFinalizeState = iota + 1
    LifecycleFinalizeCommitting
    LifecycleFinalizePoisoned
    LifecycleFinalizeTerminal
)

type LifecycleFinalizeContext struct {
    control LifecycleCommitControl
    token   lifecycleCatalogPairToken
    state   LifecycleFinalizeState
}

type Transaction struct {
    // existing ordinary DML/DDL workspace
    writes []Entry

    // ordinary transaction always nil; allocated only by successful Seal
    lifecycleFinalize *LifecycleFinalizeContext
}
```

Catalog adapter和唯一Seal接口：

```go
func (a *ArchiveCatalogAdapter) InsertDatasetAndReceipt(
    txn *Transaction, ...,
) (*lifecycleCatalogPairToken, error)

func (a *TTLCatalogAdapter) InsertReceipt(
    txn *Transaction, ...,
) (*lifecycleCatalogPairToken, error)

func (txn *Transaction) SealLifecycleCommit(
    token *lifecycleCatalogPairToken,
    control LifecycleCommitControl,
) error
```

合同：

- 只允许Lifecycle finalizer adapter调用；普通Relation/DML不能直接构造；
- adapter先在同一tenant transaction完成Archive的Dataset+Receipt或TTL的Receipt普通
  Catalog DML；只有普通write API成功把完整逻辑写加入当前workspace后，才能返回opaque
  token。token类型和constructor均不导出，普通Relation/DML不能伪造；没有pair的no-op
  不调用本API，production不存在合法control-only transaction；
- Archive adapter任一行写入失败都不签发token，并把整个final transaction标记为必须
  full rollback；不能保留已加入workspace的Dataset后重试Receipt或改走TTL token；
- finalizer先完成Dataset/Receipt普通Catalog写，再根据payload中的binding generation、
  Dataset/Manifest root和Receipt digest计算
  `CatalogPairDigest`。canonical输入固定为
  `"MO-LIFECYCLE-CATALOG-PAIR-V1\x00" || physical_table_id:u64be ||
  binding_id:(u32be length + bytes) || binding_generation:u64be ||
  dataset_id:(u32be length + bytes) || manifest_root:32bytes ||
  receipt_digest:32bytes || entry_digest:32bytes`后取SHA-256；TTL的Dataset ID长度为0、
  Manifest root为32字节零值。字段值必须来自adapter刚加入workspace的Catalog mutation和
  同一payload，不能由调用方再传一份未验证副本。该值只证明CN finalizer配对，不进入
  wire四种聚合digest；
- refresh/route/capability校验先在锁外完成；在`txn.Lock`下只比较其结果、验证
  token的txn ID、attempt、workspace generation、未消费状态、
  `CatalogPairDigest == token.pairDigest`以及token与payload identity，并深拷贝`Payload`；
- adapter发出token后若发生statement rollback、workspace generation改变、跨txn传递或
  full rollback，token立即失效；Seal原子把`consumed`置true，重复消费即使bytes相同也
  返回fatal transaction error；
- `workspaceGeneration`只验证“签发到Seal”窗口。Seal成功后的commit内部
  merge/dump/compact可以改变workspace布局，不使已消费token失效；任何外部mutation仍按
  状态机把transaction置为POISONED；
- 第一次设置把transaction标成非只读；
- `lifecycleFinalize == nil`就是概念上的`OPEN`；成功时才分配
  `LifecycleFinalizeContext{state: SEALED}`并原子执行`nil(OPEN) -> SEALED`，保存
  token+control。具体枚举不定义`LifecycleFinalizeOpen`，禁止为了表示普通事务OPEN而
  预分配context；
  此后finalizer立即调用同一
  `TxnOperator.Commit`，不得再把transaction交回通用SQL执行流程；
- `SEALED`/`COMMITTING`收到任何外部workspace mutation时原子转为`POISONED`，后续
  只能完整rollback；不能只撤销Catalog写而保留control；
- Seal是本地一次性线性化点；同token或不同token重复调用均返回fatal transaction error。
  网络层duplicate Commit由immutable payload和TN协议幂等处理，不能通过重复Seal实现；
- 一次transaction最多一个control，禁止slice和后写覆盖；
- context/control/token不进入statement offset、workspace size、PK dedup、dump、compact、sort、
  `toPBEntry`、普通Batch cleanup或CN staging GC；
- 整体rollback/finalize把state置为`TERMINAL`并清除context，只释放该CN本地
  immutable bytes，不触发
  Provider/Root物理删除；
- transaction被删除后control/token不可复用；
- 普通transaction的`lifecycleFinalize == nil`，不分配Lifecycle对象、不增加锁、不访问
  Lifecycle Catalog、不改变原错误码和statement行为。公共mutation入口只增加一个nil
  fast path；非nil时才进入以下状态机。

`genWriteReqs`的固定顺序：

```text
merge/dump/compact ordinary txn.writes
  -> encode ordinary api.Entry
  -> if lifecycleFinalize != nil:
       require context.state == COMMITTING
       require consumed token belongs to this txn/attempt/payload
       require ordinary EntryList non-empty
       resolve authoritative route outside txn.Lock
       require ServiceID + ShardID + ReplicaID + capability still match
       require exactly one target shard
       append exactly one api.Entry{
         entry_type: LifecycleCommit,
         database_id/table_id: frozen physical route,
         lifecycle_commit_payload: immutable Payload,
         bat: nil,
       }
  -> require EntryList non-empty
  -> encode one PrecommitWriteCmd
```

底层`appendLifecycleControlForTest`必须能够在空ordinary Entry列表中编码control，以证明
`bat=nil`不会被helper静默丢弃；它只用于编码单测，不能被production commit调用。
production `genWriteReqs`在context/token缺失、token不属于当前txn、未消费、失效或ordinary
Entry列表为空时返回`ErrLifecycleCatalogPairMissing`，不得发送请求。它禁止在workspace
merge/dump后重新查找Dataset/Receipt逻辑行：Lifecycle Cluster Table可以被正常workspace
转换为Object Entry，在commit路径重读Object会引入FileService I/O和第二套DML证明器。
逻辑pair的真实性由opaque adapter token、Seal后禁止statement rollback、同事务普通
workspace合同和端到端原子性测试共同保证。输出tag bytes必须与Root冻结的canonical bytes
逐字节相同，workspace dump、merge、sort和commit retry都不能重新marshal或改变它。

### 9.1.2 Lifecycle finalizer 事务状态机

此状态机只在`lifecycleFinalize != nil`的内部Lifecycle final transaction生效；普通用户
transaction保持当前OPEN路径，不增加Merge、查询或普通DML语义。

```text
nil(OPEN) --SealLifecycleCommit/allocate--> SEALED --Commit only--> COMMITTING --> TERMINAL
   |                                             |
   +-- ordinary full rollback                    +-- external mutation --> POISONED
                                                                      |
                                                                      +-- full rollback --> TERMINAL
```

- `nil(OPEN)`沿用当前普通transaction状态，不是Lifecycle枚举值；Seal所有校验和deep copy
  成功后才发布非nil context，Seal失败不能留下半初始化context；
- 只有`Commit(ctx)`可执行`SEALED -> COMMITTING`；它调用专用内部
  `incrStatementIDForLifecycleCommit`和commit dump/transfer helpers，不能通过公共外部
  mutation入口绕过状态检查；
- 外部`WriteBatch`、`WriteFile`及locked variants、`StartStatement`、`EndStatement`、
  `IncrStatementID(false)`、`AdvanceSnapshot`、`RollbackLastStatement`、
  `UpdateSnapshotWriteOffset`、`Adjust`和`SetHaveDDL`在SEALED/COMMITTING时都必须
  poison。返回`error`的入口返回`ErrLifecycleTxnSealed`；void入口只标记POISONED，随后
  `Commit`返回`ErrLifecycleTxnPoisoned`；
- Commit内部的workspace merge/dump、Tombstone transfer和`IncrStatementID(true)`必须走
  未导出的commit helper，允许COMMITTING而不允许任何并发外部mutation；
- 一旦POISONED，禁止重开、重seal或同一TxnOperator重试；只允许完整`Rollback`。请求
  已发出后的unknown仍按Root/Receipt reconcile处理，不能用Rollback删除Root资产。
- `genWriteReqs`/route refresh在请求发出前失败时，`COMMITTING -> POISONED`，调用方执行
  full Rollback；请求已发出后若response lost，进入`TERMINAL`的unknown finalize，仅释放
  CN context/control/token内存，Root/Booking/Payload继续由Reconciler决定，绝不能回滚
  物理资产。

实现修改范围固定为：

```text
pkg/vm/engine/disttae/types.go  # Transaction字段和immutable control
pkg/vm/engine/disttae/txn.go    # Set/commit/rollback/finalize生命周期
pkg/vm/engine/disttae/tools.go  # genWriteReqs最后追加control
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
    bytes source_set_digest = 5;
    repeated SourceLayoutProof source_layouts = 6;
    LifecycleCommitEntry.Mode reserved_mode = 7;
}

message SourceLayoutProof {
    bytes object_id = 1;
    bytes object_stats_digest = 2;
    uint32 block_count = 3;
    uint64 physical_rows = 4;
    bytes block_layout_digest = 5;
}

message LifecycleTransferBooking {
    uint32 ordinal = 1;
    string immutable_key = 2;
    string provider_version = 3;
    bytes sha256 = 4;
    uint64 size_bytes = 5;
    uint32 first_source_block = 6;
    uint32 source_block_count = 7;
    uint64 live_mapping_count = 8;
    bytes transfer_mapping_digest = 9;
    bytes created_layout_digest = 10;
    bytes root_id = 11;
    uint32 root_object_ordinal = 12;
    bytes tae_storage_namespace_digest = 13;
    uint32 booking_codec_version = 14;
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
    bytes source_set_digest = 20;
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

    bytes transfer_mapping_digest = 34;
    bytes entry_digest = 35;
    repeated LifecycleTransferBooking transfer_bookings = 36;
    bytes source_protection_set_digest = 37;
}
```

`merge`：

- `MergedObjs` 必须与 `source_objects` 完全一致；
- `CreatedObjs` 是新 live TAE ObjectStats；
- `Booking`和inline transfer字段必须为空；
- `transfer_bookings`按ordinal连续递增，冻结每个external booking的exact
  key/version/SHA/size、source block range、live mapping数、transfer mapping digest
  和`CreatedObjs` layout digest，并绑定Root child/TAE namespace/codec version；
- booking payload严格使用8.3节Lifecycle Transfer Booking V1；不得直接使用会省略
  全空Block和Root/layout binding的普通`TransferHashPage.Marshal()`；
- 所有booking的source block range必须连续、无重叠、完整覆盖
  唯一`SourceLayoutProof.block_count`；
- `reservation.source_set_digest == entry.source_set_digest`，且reservation
  `source_layouts`与entry `source_objects`逐项同序；
- `reservation.reserved_mode -> entry.mode`符合4.3节单向转换矩阵；
- `source_protection_set_digest`必须与TN manager当前job冻结值完全相同；
- `StartTs` 等于 `source_snapshot_ts`；
- `Err` 必须为空；
- `Level` 必须按普通Merge的晋级规则从唯一source Object计算并冻结；不能默认传0，
  造成高level Object降级后重新进入高频Merge。

### 9.3 Wire 限额

```text
protocol_version          = 1
source objects            == 1 for Rewrite；<= 64 for Whole
source layout proofs      == source objects，一一对应且同序
created objects           < 255
inline transfer bytes     == 0
external booking          required for Rewrite
serialized entry          <= 1 MiB
booking files/bytes       <= bounded release profile
```

所有 repeated source按 Object ID升序。Digest严格使用8.3.1节的唯一公式；
`source_set_digest`同时覆盖全部source和全部layout proof。即使只有一个source，
wire仍保留repeated字段以支持Whole和版本兼容；TN按mode执行严格基数校验。
Lifecycle不保留“少于50万行走inline”的第二条路径，
避免entry大小、retry和Owner语义分叉。

## 10. CN Finalizer

Finalizer只在 Build `VERIFIED` 后开始：

```text
allocate normal tenant txn
  -> choose absolute final_prepare_deadline D and bind commit context deadline = D
  -> refresh authoritative TN route/capability outside txn.Lock
  -> freeze ServiceID + ShardID + ReplicaID + protocol_version
  -> system txn CAS Root VERIFIED -> FINALIZING
       freeze final_txn_id
       freeze canonical LifecycleCommitEntry bytes/digest
       freeze final_prepare_deadline = D
       freeze max retry generations/cumulative budget profile
       freeze receipt/dataset/manifest identity
  -> tenant txn CAS Guard/Binding/active attempt
  -> tenant txn insert Dataset/Receipt（Archive）
     or TTL Receipt
  -> internal adapter returns txn-bound LifecycleCatalogPairToken
  -> SealLifecycleCommit consumes token
       atomically enters SEALED
  -> workspace.Commit builds one replayable PrecommitWriteCmd.EntryList
  -> commit
```

Dataset/Receipt普通Catalog写和LifecycleCommitControl必须由一个不可拆分的finalizer
API加入同一`TxnOperator`。内部adapter只在逻辑写成功进入当前workspace后返回opaque、
txn-bound token；Seal验证token/payload digest并一次性消费。缺token、失效token、跨txn
token、control或digest不一致均返回`ErrLifecycleCatalogPairMissing`，不得生成request。
任何一步写入workspace失败都rollback同一transaction；缺少control时Dataset/Receipt也
不得单独提交。finalizer seal后必须立即进入同一`TxnOperator.Commit`，不得执行或接受
更多普通statement。`workspace.Commit()`生成的
`TxnCommitRequest.DeadlineUnixNano`必须精确等于Root已冻结的`D`，不允许使用fallback
deadline或在发送时重新延长。

禁止：

- 先提交 Dataset再退休；
- 先退休再异步补 Dataset；
- 把Lifecycle作为commit前的独立`TxnOperator.Write`发送；
- response lost后用新 txn ID重发；
- final transaction执行 Provider I/O；
- reservation过期后只看 Object仍存在就提交。

## 11. TN Handler

新增：

```text
proto/api.proto
pkg/catalog/tuplesParse.go（或等价Entry iterator扩展）
pkg/vm/engine/tae/rpc/handle.go
pkg/vm/engine/tae/rpc/handle_lifecycle.go
pkg/vm/engine/tae/tables/txnentries/lifecycle_objects.go
```

### 11.1 解析和 digest

1. iterator只在`EntryType=LifecycleCommit`时接受`lifecycle_commit_payload`；
2. protocol/version/capability匹配；
3. canonical重新计算 `entry_digest`;
4. mode字段组合合法；
5. source/created/booking行数和大小有界；
6. nested `MergeCommitEntry` 与顶层 identity一致。

`MIXED_REWRITE_*`额外要求`expired_rows > 0`且`live_rows > 0`；
`createdObjs==0`或`expired_rows==0`直接拒绝，不能在TN临时猜测应退化为何种模式。

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
SyncProtectionManager.ValidateSyncProtection(jobID, prepareTS, expectedSetDigest)
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
matching SourceLayoutProof exists at same ordinal
current block count/physical rows/block-layout digest match that proof
```

Object missing或已有 DropIntent不能视为幂等成功。只有一致性 Receipt对账能判断原
attempt已提交。

Whole TTL额外从 lifecycle column Metadata再次证明：

```text
valid, non-truncated ZoneMap
null_count == 0
max < cutoff
```

### 11.5 CN/TN 信任边界与行守恒

Rewrite：

```text
source_physical_rows     == snapshot_deleted + expired + live
source_visible_rows      == expired + live
sum CreatedObject.Rows   == live
non-NoTransfer mappings  == live
every mapping destination is inside exact CreatedObjs layout
every source offset appears at most once
every destination is unique and all created rows are covered
transfer_mapping_digest + created_layout_digest == recomputed digests
```

TN独立执行的防御性校验：

- exact source Object identity/Stats和一一对应的`SourceLayoutProof`集合；
- source物理行数、block布局、计数等式和mapping边界；
- created ObjectStats、行数、level和digest；
- external booking exact identity、size/SHA、page/layout digest；
- reservation/protection/attempt/entry generation；
- `S` 后Tombstone delta：有mapping则使用现有transfer，无mapping、`NoTransfer`、
  nil Block map或越界一律使整个Lifecycle final transaction abort。

其中`source_physical_rows`来自TN当前Object metadata；`snapshot_deleted/expired/live`
计数来自CN并参与算术守恒，Booking只携带`DoMergeAndWrite`实际产生的live mapping。
TN必须为整个txn entry建立一个全局destination bitmap，拒绝跨Booking page的重复
destination或created row缺口；不能每个page分别建bitmap。这是传输格式
的防御性结构检查，不是另一套Merge证明。mapping的业务正确性来自复用同一个
`DoMergeAndWrite` producer、`CreatedObjs`顺序冻结、producer属性测试和Booking
编解码round-trip测试。destination唯一性不单列新的架构P0，也不要求修改普通Merge。

TN明确不执行：

- 重新计算TTL表达式，或逐行重建D/E分类；
- 根据D/E分类重新生成、修补、排序或合并destination mapping；
- 第二次读取全部source业务列；
- 读取Provider中的Parquet并逐行分类。

TTL分类结果由CN承担，其信任模型与普通SQL DELETE predicate相同。正确性由
classifier单测、属性测试、Archive full readback和1/10 TiB源/归档/live对账认证。
TN不重读Archive provider，也不信任ETag；它只校验Root/Manifest/Receipt冻结的
digest组合。设计不得宣称TN独立证明source/destination业务语义；该信任边界与普通
Merge/SQL DELETE predicate一致。

## 12. 复用 Merge transaction entry

新增 Lifecycle wrapper，但内部复用现有 `mergeObjectsEntry`的
create/drop/transfer page/WAL核心：

```go
type TombstoneDeltaLimits struct {
    MaxRows   uint64
    MaxBytes  uint64
    MaxBlocks uint32
    Deadline  time.Time
}

type LifecycleMergeOptions struct {
    CollectDeletesFrom          types.TS
    PhysicalCreatedObjectOwner  PhysicalCreatedObjectOwner
    DeltaLimits                 TombstoneDeltaLimits
    ReservationToken           LifecycleReservationToken
    ExpectedCreatedLayoutDigest  [32]byte
    ExpectedTransferMappingDigest [32]byte
}

func HandleLifecycleMergeEntryInTxn(
    ctx context.Context,
    txn txnif.AsyncTxn,
    req *api.LifecycleCommitEntry,
    transfer *mergesort.TransferTable,
    rt *dbutils.Runtime,
) error
```

内部必须先取得generation-local唯一执行slot，再发生任何TAE transaction mutation：

```go
type LifecycleGenerationSlotState uint8

const (
    LifecycleSlotNew LifecycleGenerationSlotState = iota
    LifecycleSlotBuilding
    LifecycleSlotRegistered
    LifecycleSlotFailed
)

type LifecycleGenerationSlotKey struct {
    AttemptID  types.Uuid
    EntryDigest [32]byte
    // internal TAE txn identity由拥有该slot map的TxnMemo隐含。
}
```

slot map属于当前internal TAE txn的ephemeral `TxnMemo`，不写WAL、不进入Memo序列化，并
随该internal txn commit/rollback销毁。唯一状态机：

```text
NEW --atomic claim--> BUILDING --LogTxnEntry success--> REGISTERED
                         |
                         +-- any error --> FAILED --> whole generation rollback
```

规则：

- 在wire/digest/大小的无副作用校验后、Booking decode和任何
  `SoftDeleteObject/CreateNonAppendableObject`前claim；
- 只有`BUILDING` owner可以decode Booking、修改Catalog、安装runtime page和Log entry；
- follower看到`BUILDING`时只能在当前absolute deadline内有界等待，或立即返回
  `LIFECYCLE_GENERATION_BUILD_IN_PROGRESS`；不能成为第二个builder；
- follower看到`REGISTERED`返回同一逻辑注册结果，不取得entry/runtime指针；
- follower看到`FAILED`返回同一terminal generation error；
- owner中途失败后，当前internal TAE txn必须整体rollback；禁止把slot重置为NEW并在已
  部分修改的同一txn中重建；
- `ErrTAENeedRetry`创建G2时使用新的internal txn、新TxnMemo和新slot；G1 slot和资源
  不得迁移到G2。

取得唯一slot后的执行顺序：

1. 创建`LifecycleEntryBuilder`；它独占decoder私有TransferTable、尚未移交的runtime
   transfer pages、`TransferDelsMap`和临时buffer；
2. 以不修改request、不删除Root资产的loader读取immutable booking；
3. `rel.SoftDeleteObject`成功后，Drop Catalog node立即归整个TAE txn所有；
4. `rel.CreateNonAppendableObject`成功后，Create Catalog node立即归整个TAE txn所有；
5. 按wire冻结顺序创建live Object，校验`created_layout_digest`和原始
   `transfer_mapping_digest`；禁止重排或重建mapping；
6. 构造带`CreatedObjectOwnedByCleanupRoot`的 `mergeObjectsEntry`；
7. phase-1 Tombstone扫描起点使用 `source_snapshot_ts`，不是 final txn startTS；
8. 使用共享预算的有界Lifecycle delta visitor执行phase 1并prepare transfer pages；
9. 任一post-S Tombstone若找不到有效destination，包括nil Block map、
   `api.NoTransfer`和越界，都返回typed error并使整个final transaction abort；
10. 调用`txn.LogTxnEntry`；返回成功才把内存/runtime资源Owner从builder移交给txn
    entry，并把slot置为`REGISTERED`；
11. 注册前失败由builder释放slab、page、TransferDels和decoder buffer，但不手动撤销
    Drop/Create Catalog node；Catalog node只由整个TAE txn rollback；
12. txn entry负责后续phase 2、rollback和exactly-once runtime cleanup，并复用
    create/drop/transfer/WAL command；Lifecycle wrapper不创建第二套Object WAL格式。

当前`LogTxnEntry`的可返回错误发生在append之前，append之后没有可返回错误的步骤。
V1应使用故障注入测试冻结以下all-or-nothing合同，不增加registration receipt/CAS：

```text
error -> entry未append，runtime Owner仍是builder
nil   -> entry已append，runtime Owner已转给txn entry
```

若未来通用`LogTxnEntry`在append后增加可失败步骤，必须先修改该通用合同，不能让
Lifecycle在ambiguous返回下猜Owner。

这个wrapper不拥有物理文件cleanup权，不能触发普通CN Merge handler的
`CleanUpUselessFiles` defer。Lifecycle request在duplicate Prepare和retry中保持
immutable，booking location不能被loader原地截短。

实现伪代码：

```go
slot, role, err := txn.GetMemo().ClaimLifecycleSlot(attemptID, entryDigest)
if err != nil || role != LifecycleSlotBuilder {
    return slot.WaitOrResult(ctx)
}

builder := NewLifecycleEntryBuilder(...)
defer builder.AbortIfUnregistered()

entry, err := builder.BuildAndPreparePhase1(ctx)
if err != nil {
    slot.Fail(err)
    return err
}
if err = txn.LogTxnEntry(tableID, entry); err != nil {
    slot.Fail(err)
    return err
}
slot.PublishRegistered(func() {
    builder.MarkRegistered() // 与slot发布位于同一不可失败临界区
})
return nil
```

普通Merge目前存在同类注册前资源残留，已由
[Issue #26445](https://github.com/matrixorigin/matrixone/issues/26445)跟踪。Lifecycle
Gate C直接要求Lifecycle-local builder/slot经过故障注入验证，不依赖普通Merge修复
何时合入。不得假设普通事务rollback会调用尚未Log的entry。

普通 `HandleMergeEntryInTxn` 继续使用：

```text
CollectDeletesFrom = txn.GetStartTS()
PhysicalCreatedObjectOwner = CreatedObjectOwnedByMergeEntry
```

普通Merge原有“全删Block可跳过”等语义保持不变；Lifecycle严格缺图abort规则只能在
wrapper内生效，不能为了Lifecycle修改普通Merge结果。

### 12.1 有界、可取消的 Tombstone delta visitor

现有 `TombstoneRangeScanByObject`会把区间内匹配Tombstone累积成完整Batch，没有
rows/bytes/blocks上限；设计基线的普通Merge phase 2还使用
`context.Background()`。基线吞错已由#26333修复，但Lifecycle仍禁止直接调用这条
无界、无caller deadline路径。普通Merge的独立现有风险跟踪在
[Issue #26377](https://github.com/matrixorigin/matrixone/issues/26377)。

新增或抽取：

```go
func VisitTombstoneRangeByObject(
    ctx context.Context,
    table *catalog.TableEntry,
    objectID objectio.ObjectId,
    start, end types.TS,
    limits TombstoneDeltaLimits,
    visit func(TombstoneDelta) error,
) (TombstoneDeltaReport, error)
```

要求：

- 按Tombstone Object/Batch流式读取，不构造完整区间Batch；
- 每次累加rows、encoded bytes、affected source blocks；
- limits按整个Lifecycle final transaction聚合，不按source Object分别重置；
- phase 1使用request ctx，并受TN固定release profile和绝对Prepare deadline双重限制；
- `PrepareCommit()`接口没有caller ctx，phase 2从txn entry冻结的绝对deadline创建内部
  bounded ctx，并继续消费phase 1剩余的rows/bytes/blocks/time预算；不得裸用
  `context.Background()`，也不得因客户端断连任意取消已经进入Prepare的事务；
- TN在首次构造entry时把deadline钳制为
  `min(attempt remaining deadline, now + certified final transaction limit)`；
  duplicate Write/Prepare必须复用同一txn entry，不能重置deadline或预算；TN restart
  后旧事务按正常恢复结果处理，不能靠重建entry无限续期；
- `WaitTombstoneObjectCommitted`也必须可取消/有deadline；
- 任一scan/transfer/error原样向上返回，不能吞掉；
- 超限返回typed error，不先安装部分可提交transfer page；
- 已安装的临时page由txn rollback删除，Root物理文件不删除。

初始认证profile：

```text
delta rows              <= 1,000,000
delta affected blocks   <= 256
delta encoded bytes     <= 32 MiB
phase 1 + phase 2       <= final transaction 60s deadline
```

32 MiB不是用户可任意放大的配置。实现必须从当前70 MiB `MaxWalSize`扣除
Lifecycle entry、Catalog command、create/drop command、transfer page和安全余量后
计算release profile；若编码变化导致上界不再成立，启动时拒绝retirement。

超限/超时：

```text
abort final txn
source remains visible
Dataset/Receipt not published
Root keeps staging until authoritative abort
-> CONFLICT_BLOCKED or RESOURCE_BLOCKED
```

不能用更长的`context.Background()`让TN Prepare无限等待，也不能在失败后自动立刻
重试同一source形成饥饿循环。

### 12.2 Transfer slab admission

当前mergesort按物理slot分配dense `[]TransferDestPos`，不是按live行数分配；分配
失败会panic。Lifecycle不得硬编码`objectio.BlockMaxRows/ObjectMaxBlocks`，而要冻结并
校验实际`schema.Extra.BlockMaxRows`和`schema.Extra.ObjectMaxBlocks`。准入计算为：

```text
requested slots =
  checked(block_count * certified schema.Extra.BlockMaxRows)

allocator charged capacity =
  requested slots <=   524,288 -> 4 MiB slab对应容量
  requested slots <= 2,097,152 -> 16 MiB slab对应容量
  otherwise                    -> checked exact allocation

CN dense slab charged bytes（按allocator量化后容量，不按requested/live rows）
+ max certified decoded source Block vectors
+ mergesort output Batch/Object Writer/index buffer
+ Archive encoder buffer
+ TN booking load/copy and detached page bytes
+ serialization/decompression/metadata overhead
+ safety margin
```

计算，不使用`live_rows`估算。`block_count`、每Block实际行数和schema Extra必须同时
满足release profile；非认证布局、乘法溢出或Object超过
`schema.Extra.ObjectMaxBlocks`时在读数据和分配前fail closed。文档中的默认约22 MiB
Booking估算只适用于`8192 rows/block × 256 blocks/object`认证profile，不代表所有
合法表。

dense slab在`DoMergeAndWrite`内部创建merger时、首次
`BlockDataReadNoCopy`之前分配，因此必须在调用`DoMergeAndWrite`前取得覆盖上述峰值
的task-level memory token；不能在读完Block后补做admission。每次Block读取前再按
Object metadata的column extent `OriginSize`做checked uint64求和，取得该Block的
source/decode子令牌；无法可靠估算、求和溢出或超过
`max_certified_block_read_bytes`时，在读取payload前返回
`LIFECYCLE_OVERSIZE_UNSUPPORTED/LIFECYCLE_REWRITE_RESOURCE_EXCEEDED`。

首个GA单源Object、cluster Rewrite并发默认1。实现还必须为Lifecycle调用链提供checked
slab allocation并把mpool失败返回error，沿Owner规则cleanup。不能只在最外层recover
任意panic后宣称安全；recover只可作为最后的进程保护，不能替代可返回error的分配
接口。普通Merge的独立现有panic风险跟踪在
[Issue #26376](https://github.com/matrixorigin/matrixone/issues/26376)。

## 13. 并发 Tombstone 语义

“Lifecycle Rewrite永远启用transfer”的准确含义是**survivor-only transfer
mandatory**：

```go
func (*LifecycleRewriteHost) DoTransfer() bool {
    return true
}
```

这个返回值只负责启用现有transfer machinery，不表示每一行都有destination：

```text
D = snapshot-deleted -> NoTransfer
E = expired-visible  -> NoTransfer
L = live-visible     -> exactly one destination
```

它不受普通 Merge table comment或`MO_COMMENT_NO_DEL_HINT`影响，也从不修改Archive
Payload。若关闭transfer，`S`后对L行的DELETE/UPDATE无法映射到新Object，会把已删除
行复活。

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

若 Tombstone找不到有效destination，包括命中expired、snapshot-deleted、
`api.NoTransfer`、nil Block map或越界：

- 整个Lifecycle final transaction abort；
- Dataset/Receipt不发布，source Object不退休；
- Root在权威aborted后清理staging；
- 后续以新Snapshot重新分类和构建，达到冲突期限后进入`CONFLICT_BLOCKED`，不无限
  重试。

这条fail-closed规则不区分TTL与Archive，也不要求TN识别D/E。TTL expired DELETE从
业务结果看虽然是冗余的，但首个GA不为这项优化增加D/E wire分类和多套
`NoTransfer`策略；待GA后有明确吞吐证据时再单独设计。

该简化只作用于`S`后的Tombstone处理；CN在Snapshot `S`构建Archive/live输出时仍
必须按6.2节区分D/E/L，否则无法决定哪些行归档、丢弃或保留。

### 13.3 Prepare 后

Transfer page对 expired row没有 destination。一个更早开始、在 Lifecycle Prepare
后尝试提交的用户删除：

- transfer lookup得到 `NoTransfer`；
- 返回普通事务冲突；
- 不允许静默提交一个无法映射的删除。

一个 Lifecycle commit之后才开始的用户事务已看不到退休行。

还必须显式证明旧事务窗口：

```text
用户事务先读取旧RowID
  -> Lifecycle Whole/Rewrite Prepare并commit
  -> 用户事务随后DELETE/commit
  -> transfer page可用时按正常规则处理；不存在/已过期时必须RW/WW conflict
```

TN restart后不保证恢复已提交事务的运行时transfer page，因此缺页也只能让旧用户事务
冲突重试，不能静默成功。反向时序中，若旧用户DELETE已在Lifecycle Prepare前进入
可见/prepare区间，Whole validator或Rewrite delta visitor必须发现并按本章
语义处理。

### 13.4 Whole 的并发删除

Whole TTL/Archive都不创建new live Object，普通Merge entry可能因`createdObjs==0`提前跳过
transfer phase。因此它必须在final Prepare执行独立且同样有界的：

```text
ValidateNoPostSnapshotDelete(
  exact source Object,
  (source_snapshot_ts, prepare_ts),
  TombstoneDeltaLimits
)
```

发现任何命中source的post-S Tombstone都abort/rebuild；超限或无法完成验证也abort。
首个GA对Whole TTL同样fail closed，不利用“DELETE与TTL退休结果相同”做特殊优化。

### 13.5 INSERT

新 INSERT不在 frozen source set：

- 不影响本 child conservation；
- 新 row留在当前表；
- 迟到且已过期的数据由后续 cycle处理。

## 14. 1PC、2PC、retry 和 response lost

### 14.1 1PC/2PC

tagged Lifecycle entry属于正常commit payload：

- 单participant满足条件时可1PC；
- Dataset/Receipt或其他Catalog participant存在时走2PC；
- 所有participant共享同一外部Txn ID、entry bytes和commit decision；
- Dataset/Receipt存在而Lifecycle tag缺失，或反向缺失，都必须使整个事务失败；
- Lifecycle不自建commit coordinator，也不在commit前单独发送Write。

### 14.2 重复 Write/Prepare

外部逻辑attempt memo key：

```text
(txn ID, attempt ID, entry digest)
```

规则：

- same txn/same digest重复到达：返回同一逻辑注册结果，但绝不能返回某个旧内部
  generation的entry、Catalog node或TransferTable指针；
- same txn/different digest：fatal transaction error；
- different txn/same attempt：只有旧 txn明确 aborted且 Root CAS新 txn后才允许；
- source Object missing不作为duplicate success；
- 同一内部generation并发decode时只有一个builder可以完成`LogTxnEntry`；失败者释放
  全部私有runtime资源，不删除Root文件。

### 14.3 `ErrTAENeedRetry`

必须区分两层身份：

```text
external logical attempt:
  external txn ID + attempt ID + canonical entry bytes/digest
  Root/Booking identities
  absolute final_prepare_deadline
  max internal generations
  cumulative I/O/CPU/delta budget

internal TAE execution generation:
  G1, G2, ...
  private Catalog nodes + TransferTable + txn entry + runtime pages
```

Finalizer预先选择并在Root冻结绝对deadline `D`，再以同一`D`作为commit context
deadline；`TxnCommitRequest.DeadlineUnixNano`是TN收到的唯一wire来源，且必须等于
Root中的`D`。retry不得重新计算`now + 60s`；每代可以重新验证剩余预算，但所有
generation共享同一个绝对deadline、最大generation数以及累计I/O/CPU/delta预算。

累计预算的唯一Owner是一次`HandleCommit`调用栈上的
`LifecycleReplayBudget`，不是generation entry，也不是全局无界memo：

```go
type LifecycleReplayBudget struct {
    AbsoluteDeadline       time.Time
    MaxGenerations         uint32
    GenerationCount        uint32
    MaxBookingBytes        uint64
    CumulativeBookingBytes uint64
    MaxDeltaRows           uint64
    CumulativeDeltaRows    uint64
    MaxDeltaBytes          uint64
    CumulativeDeltaBytes   uint64
    MaxCPU                 time.Duration
    CumulativeCPU          time.Duration
    Terminal               bool
}
```

`HandleCommit`先无副作用预扫描commit payload：没有Lifecycle tag时保持普通路径；恰好
一个tag时，从`TxnCommitRequest.DeadlineUnixNano`和release profile创建一个budget
实例。该指针传给G1和每次`handleRequests` retry generation：

```text
HandleCommit
  -> pre-scan max-one Lifecycle tag
  -> create one LifecycleReplayBudget
  -> before G1: ConsumeGeneration()
  -> handleRequests(..., budget)
  -> ErrTAENeedRetry
       -> before Start G2: ConsumeGeneration()
       -> handleRequests(..., same budget)
```

每次Booking read/decode和Tombstone visitor都必须在副作用前reserve/累计对应budget；
超过任一上限或绝对deadline立即terminal fail closed。`HandleCommit`返回后budget随栈
销毁，没有跨transaction全局Map。

同一次`HandleCommit`内部的retry generation只使用这一份预算；同一internal txn内的
重复注册由generation slot变成follower，不执行第二份Booking I/O、delta scan或
Catalog mutation。V1不增加Lifecycle全局duplicate registry：当前TxnService已串行重叠
Commit，但不会永久保存终态。迟到duplicate的安全与成本边界固定为：

```text
TxnService absolute request deadline gate
  -> storage HandleCommit pre-scan max-one Lifecycle tag
  -> TryAcquire Lifecycle commit permit
       busy: return RESOURCE_BUSY before creating internal TAE txn
  -> parse/tag/route/capability preflight
  -> reservation + protection + exact source preflight
  -> only then Booking read/decode and TAE mutation
```

已提交attempt的source不再exact时，TN必须在Booking I/O前返回
`LIFECYCLE_RECONCILE_REQUIRED`；CN以正常一致性读Root/Receipt收敛，不能把它当作
corruption或重新导出。已明确aborted而source仍exact的请求只有在同一绝对deadline、
attempt/reservation仍有效且调用方结果unknown时才可重新尝试。超过deadline由TxnService
拒绝。admission不设waiter、不在TN commit路径排队；TryAcquire失败立即返回
`RESOURCE_BUSY`，CN scheduler带有界jitter重新调度。CN在创建final transaction前也执行
同口径调度限流，TN permit是最后硬边界。permit
由一次`HandleCommit`独占并覆盖其全部G1/G2 retry generation；在parse、preflight、Booking、
TAE mutation、terminal abort或正常返回的任一出口都必须exactly-once释放。它不随单个
generation释放或重取，避免retry绕过并发上限。

Gate C只需证明没有两个storage `HandleCommit`会并行执行同一external txn；若该证明
失败，才增加key为`external txn ID + CommitSequence + attempt ID + entry_digest`、具有
hard cap、TTL、terminal和deadline回收的专用registry。禁止以普通路径增加进程全局
无界Map。

每次retry必须：

- 从commit request中的原始tagged entry bytes重新解析同一Lifecycle entry；
- 使用同一external Txn ID、attempt ID和entry digest；
- 从immutable Booking构造全新的私有TransferTable、Catalog node和txn entry；
- 重新校验reservation/protection和当代剩余预算；
- 不重新执行Build或Provider PUT；
- G1 rollback后只清理G1 Catalog/runtime状态，任何G1指针不得交给G2；
- staging live/Archive仍由Root持有；
- 超过绝对deadline、最大generation或累计预算后fail closed，不再重试。

HandleCommit-local replay context只保存不可变bytes/digest、Root identity、budget和
最终逻辑结果；不得缓存可变entry指针。这样`ErrTAENeedRetry`重放Dataset/Receipt时，
完整Lifecycle retirement也必然在新TAE txn中重新注册。

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

Archive Parquet/Manifest字节**不进入TAE WAL**；它们由Archive Provider、Cleanup
Root和Manifest checksum管理。WAL只记录final transaction对活动TAE Catalog的
原子变化：

最终 Object变化继续由现有 merge transaction command表示：

```text
dropped source Object IDs
created live Object IDs
```

Replay不需要重放 Provider I/O、reservation或Scanner。Lifecycle Dataset/Receipt由
普通 Catalog DML WAL恢复。

这样crash边界保持与普通Merge一致：

```text
commit前crash:
  old source remains active
  Root owns and later cleans staging

commit后、response前crash:
  WAL/replay restores old source DropIntent + new live Object Active
  Receipt reconciliation confirms publish

checkpoint/GC后:
  existing TAE GC physically deletes old source
```

Lifecycle不新建第二套WAL/Replay引擎；tagged Lifecycle entry只在Prepare时验证额外的
Archive/分类/Owner条件，成功后复用create/drop/transfer transaction command。

### 15.2 Transfer page

WAL/Replay恢复source DropIntent、新live Object以及final transaction产生的
Tombstone/Catalog状态；它**不承诺重建已提交事务的历史运行时transfer page**。

Lifecycle Transfer Booking V1用于同一final transaction的duplicate
Write/Prepare、`ErrTAENeedRetry`和TN在Prepare期间重建运行时page。它的Owner和删除
时机按8.2节由Root管理，不能沿用普通Merge的Prepare即删。事务提交后运行时page仍按
现有transfer table TTL管理；TN restart后旧用户事务无法取得page时，必须按现有
RW/WW conflict语义重试，不能把缺页当删除成功，也不能声称由WAL replay恢复page。

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
source objects             == 1 protocol invariant
source bytes               <= current legal single Object maximum
each decoded block estimate <= max_certified_block_read_bytes
max certified block read    = 256 MiB GA candidate，认证只可保持或调低
task peak memory token      covers slab + block + output + archive + TN copy + margin
created live objects       <= 32 default, <255 hard
transfer dense memory      <= physical slots based admission
external booking bytes     <= 32 MiB default, certified hard limit
tombstone delta bytes      <= 32 MiB
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
Rewrite cluster concurrency  = 1 default, 4 certified hard max
```

单个当前合法 Object可以达到：

```text
8192 rows/block × 256 blocks = 2,097,152 rows
Object writer size limit      = 3 GiB
```

GA必须支持由“每个Block都在认证读取上限内”组成的3 GiB Object streaming，不分配
整Object Batch，也不能因默认4 GiB child限制把这种合法单Object永久重试。首个GA
不承诺任意合法256 MiB单行或任意无法预估解压峰值的Block；它们必须在payload读取前
明确进入`LIFECYCLE_OVERSIZE_UNSUPPORTED/RESOURCE_BLOCKED`，直到真正
block-streaming Reader完成认证。

## 18. 错误和终态

| 条件 | 结果 |
|---|---|
| reservation conflict/lost | replan；达到期限 `CONFLICT_BLOCKED` |
| source protection register/renew失败 | cancel build；不final |
| source Object被Merge替换 | exact validation失败；replan |
| Archive/live writer失败 | cleanup staging；源不变 |
| post-S delete命中live row | transfer到新RowID |
| post-S delete无mapping/NoTransfer/nil map/越界 | final abort；rebuild或`CONFLICT_BLOCKED` |
| transfer损坏/overflow | final abort；`RESOURCE_BLOCKED`或replan |
| CN control缺有效token/被过滤/route identity变化 | 发送前abort；Dataset/Receipt不提交 |
| SEALED事务收到外部workspace mutation | 置POISONED；只允许完整rollback |
| generation slot BUILDING follower | 有界等待/返回同一结果；不得重复mutation |
| generation builder失败 | slot FAILED；整代TAE txn rollback；同代禁止重建 |
| overlapping duplicate Commit | TxnService串行；迟到请求按deadline/source preflight收敛 |
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
- inline booking一律在side effect前拒绝；
- external booking duplicate load/digest/size/version；
- Booking V1 unknown version/flag、缺Block、零mapping Block、actual rows和尾部slot；
- booking Root/ordinal/namespace错绑、完整文件SHA和canonical payload digest不一致；
- producer TransferTable编解码round-trip后mapping和`CreatedObjs`顺序完全一致；
- Lifecycle不得调用mapping重排、修补或重新生成helper；
- ordinary owner rollback删除created Object；
- Lifecycle owner rollback不删Root live/booking，权威aborted后由Sweeper删除；
- `ErrTAENeedRetry`后复用同一staging，最终Catalog引用文件仍存在；
- transfer phase-1/phase-2每个错误都向上返回并abort；
- phase-1、runtime page install、`LogTxnEntry`之前/之中/成功之后逐点注入失败；
- Drop/Create API成功后Catalog node只由整个txn rollback，不由builder手动撤销；
- Log前失败释放slab/page/TransferDels/decoder状态，不删除Root文件；
- `LogTxnEntry`成功后只由txn entry释放runtime资源，不double free；
- 普通Merge #26445共享修复或Lifecycle独立builder回归测试。

### 19.3 并发

- ordinary Merge在Acquire前/后/Build中/final前提交；
- user-forced Merge不能绕过reservation admission；
- reservation expire/renew/TN restart；
- DELETE/UPDATE在S前、phase1、phase2、Prepare后；
- post-S delete命中Archive/TTL expired或snapshot-deleted槽位都必须abort/rebuild；
- post-S delete命中nil Block map、`NoTransfer`和越界都必须abort；
- delete live row必须transfer；
- Whole Archive/TTL post-S delete validator；
- Whole Archive/TTL Prepare/commit后旧RowID DELETE必须冲突，含TN restart；
- 旧DELETE先prepare时Whole validator必须发现；
- delta rows/bytes/blocks/deadline达到limit-1/limit/limit+1；
- phase2 cancel/error不能被吞掉；
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
- duplicate tagged entry/Prepare；
- control穿过workspace dump/compact/merge/sort后payload bytes和digest完全不变；
- 底层空Entry编码helper能生成control，证明`bat=nil`不被静默丢弃；production
  `genWriteReqs`对空ordinary Entry或无有效token返回`ErrLifecycleCatalogPairMissing`且不发请求；
- Dataset/Receipt在force flush和workspace threshold下转换为Object Entry后仍与tag原子
  提交；commit路径不重读Object、不扫描逻辑行；
- token跨txn、statement rollback后、workspace generation变化或第二次消费均发送前拒绝；
- control只冻结ServiceID/ShardID/ReplicaID/protocol version；发送前权威refresh重解析
  Address和capability，identity变化、多候选shard或refresh失败均发送前拒绝；
- old TN/unknown EntryType在Batch解析前返回unsupported，不调用
  `apiEntryToWriteEntry/ProtoBatchToBatch`，进程不panic；
- capability从TN heartbeat经HAKeeper、ClusterDetails到TNService传播；任一层缺值或过期
  都按unsupported；
- route refresh断言不在`txn.Lock`内等待；
- ordinary transaction的`lifecycleFinalize`恒为nil，不分配context、不改变原错误码；
- Seal后所有外部Workspace mutation入口分别验证：error入口返回
  `ErrLifecycleTxnSealed`，void入口置POISONED，Commit只允许完整rollback；Commit内部
  merge/dump/transfer仍可在COMMITTING完成；
- rollback/finalize释放CN control但不删除Root/provider对象；
- G1 `ErrTAENeedRetry`后G2仍包含完整Dataset/Receipt和Lifecycle retire；
- G2不复用G1 entry、Catalog node、TransferTable或runtime page；
- retry/restart不延长`DeadlineUnixNano`，达到generation/累计预算上限后终止；
- generation slot在SoftDelete/Create前只有一个`BUILDING` owner；
- follower分别在BUILDING/REGISTERED/FAILED状态有界返回，不执行Catalog mutation；
- builder在SoftDelete后、Create第N个后、phase1/page/Log前失败均使整代rollback，同代
  不能重建；
- `LogTxnEntry` error发生在append前、nil发生在append后，合同用故障注入冻结；
- G1/G2各自使用不同TxnMemo slot，不继承slot状态或runtime指针；
- HandleCommit-local replay budget在G1/G2累计booking/delta/CPU，limit-1/limit/limit+1；
- overlapping duplicate Commit由TxnService mutex串行，只有一个storage HandleCommit执行；
- terminal后迟到duplicate：deadline后被拒绝；已提交source不exact时不读Booking、不mutation、
  返回`LIFECYCLE_RECONCILE_REQUIRED`；
- Lifecycle commit admission饱和时在创建内部TAE txn前立即`RESOURCE_BUSY`；无waiter，
  permit跨G1/G2一次获取并exactly-once释放；
- 只有实验证明storage HandleCommit可并行执行同一external txn时，才测试并启用有界
  registry；
- Dataset/Receipt存在但Lifecycle tag缺失，以及反向缺失，整个事务都失败；
- CN/TN crash在每个Create/Drop/transfer/catalog write之间；
- commit response lost；
- Receipt存在/缺失/digest mismatch；
- WAL replay后active source/live Object集合正确；
- rolling upgrade capability gate阻止老TN收到LifecycleCommit tag，新TN拒绝未知版本。

### 19.6 TN信任边界与资源

- Whole多source的proof数量/顺序/Object ID/stats/layout任一变化都fail closed；
- Rewrite proof数量严格为1；
- CN计数不满足物理/可见/created rows算术守恒时TN拒绝；
- Booking mapping越界、source offset重复、跨page destination重复或created row缺口时
  TN全局bitmap拒绝；
- digest四个唯一名称、domain separator、canonical endian/order和zeroed-self字段的
  CN/TN/离线golden vector一致；
- TN不重读TTL业务列或Provider Payload；
- TN不根据D/E分类重新生成或修补destination mapping；
- classifier属性测试覆盖所有支持时间类型、cutoff边界和D/E/L完备性；
- 单源最大blocks/rows/bytes和几乎全部live的dense transfer峰值；
- slab requested slots跨4 MiB/16 MiB量化边界，token按allocator capacity计费；
- 非默认`schema.Extra.BlockMaxRows/ObjectMaxBlocks`在认证范围内正确计费，范围外读取
  前拒绝；
- metadata extent估算在limit-1/limit/limit+1、溢出和未知时读取前fail closed；
- 3 GiB多Block Object通过；超认证单Block/单行不进入Reader；
- 第二个source在Root/FileService/Provider side effect前拒绝；
- Rewrite并发1/2/4下CN+TN+detached page峰值；
- mpool分配失败返回error，不panic进程，Root/source所有权收敛。

### 19.7 普通路径回归

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
8. 使用`PrecommitWriteCmd.EntryList`中的tagged LifecycleCommit entry，确保每一代
   TAE retry完整重放；capability gate防止老TN误解析。
9. 首个GA不改变普通Merge的physical layout策略；乱序表的持续成本由budget和SLO限制。
10. Lifecycle Rewrite严格单源、一律external booking，默认集群并发1。
11. transfer只映射live survivor；首个GA对任何post-S `NoTransfer` DELETE统一
    abort/rebuild，不区分Archive/TTL/D。TTL冗余删除忽略是GA后的可选优化。
12. Cleanup Root拥有Lifecycle物理staging；Merge entry rollback不删除Root资产。
13. TN证明identity、计数、mapping边界和Booking完整性；CN与现有
    `DoMergeAndWrite` producer承担TTL/D分类和destination业务语义，不在Prepare二次
    全扫业务值或重建mapping。
14. Whole允许最多64个source，每个source都有同序`SourceLayoutProof`；Rewrite仍
    严格单源。
15. Lifecycle Booking使用独立V1 envelope；普通Merge codec保持兼容。
16. task-level内存令牌在`DoMergeAndWrite`前取得，未认证Block在payload读取前拒绝。
17. WAL/Replay不恢复历史运行时transfer page；TN restart后的旧事务缺页必须冲突。
18. 外部逻辑attempt与内部TAE execution generation分离；每代从immutable Booking构造
    私有entry，绝对deadline和累计预算不因retry续期。
19. `txn.LogTxnEntry`成功是runtime资源Owner线性化点；注册前由builder释放，注册后由
    txn entry释放，任何一方都不删除Root-owned物理文件。
20. CN tag使用独立单值commit-control，不进入普通workspace Entry处理；在
    `genWriteReqs`最后原样追加。
21. generation slot在第一次TAE txn mutation前决出唯一builder；Catalog node由整个
    txn持有，builder只清理尚未移交的runtime资源。
22. 累计retry预算由`HandleCommit`调用栈唯一持有并传入每代；重叠Commit复用TxnService
    串行，迟到Commit先经deadline/source preflight和Lifecycle admission；只有实证并行
    storage执行才增加有界共享registry；禁止进程全局无界replay memo。
23. production control必须消费Catalog adapter签发的txn-bound opaque pair token；
    workspace dump后不重扫逻辑行，空Entry编码只作为私有单测，不能成为提交语义。
24. 路由冻结ServiceID/ShardID/ReplicaID/protocol version，Address发送前刷新；不新增
    没有权威来源的TopologyGeneration，不能唯一定位shard即fail closed。
25. Lifecycle final transaction按需分配`LifecycleFinalizeContext`并使用
    OPEN/SEALED/COMMITTING/POISONED/TERMINAL状态机；普通transaction context恒为nil。
26. unknown EntryType必须在Batch解析前fail closed；capability经TN heartbeat、HAKeeper和
    cluster metadata权威传播，Safety Release全员ready前retirement保持关闭。
27. Lifecycle TN admission只使用TryAcquire；busy在内部TAE txn创建前立即返回，禁止commit
    路径排队。
28. Dataset/Receipt/Restore chunk只写transaction ID；真实CommitTS由Reconciler观测并
    持久化到Archive Root，未知时禁止Purge。
29. Feature Guard只属于已绑定或仍在收敛的表；首次Bind/依赖竞态复用现有
    `mo_tables`行锁，未绑定普通DDL不创建Guard。
