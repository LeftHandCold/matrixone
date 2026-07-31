# 02 Object Discovery、Exact Reader与Archive格式详细设计

## 1. Discovery事实源

当前可见Object集合只以当前Snapshot的`PartitionState`/TAE Catalog为准。GC metadata含
历史删除信息，不是活动集合事实源。

MO已有`dataObjectsNameIndex`、`tombstoneObjectsNameIndex`和`dataObjectTSIndex`。
Lifecycle不建立第二份持久Object Index。

## 2. 分页接口

禁止直接调用会构造全表slice的`GetNonAppendableObjectStats`。新增只读接口：

```go
type LifecycleObjectCursor struct {
    SnapshotTS     types.TS
    LastObjectName objectio.ObjectNameShort
    Wrapped        bool
}

type LifecycleObjectPage struct {
    Objects    []objectio.ObjectEntry
    Next       LifecycleObjectCursor
    EndOfCycle bool
    MetaBytes  uint64
}

func ScanLifecycleObjects(
    ctx context.Context,
    state *logtailreplay.PartitionState,
    cursor LifecycleObjectCursor,
    maxObjects int,
    maxMetaBytes uint64,
) (LifecycleObjectPage, error)
```

实现要求：

- 基于现有B-tree seek/iterator；
- `maxObjects`或`maxMetaBytes`任一达到即返回；
- 不预构造全表ObjectStats；
- 只返回S时可见、committed、non-appendable Data Object；
- appendable Object等待现有flush/merge变为non-appendable，不由Lifecycle强制封口；
- Tombstone Object只进入Snapshot/protection解析，不作为退休Candidate；
- Snapshot stale时返回类型化错误并重开cycle；
- cursor只作为hint，不参与正确性；
- 完整cycle到末尾必须wrap；
- `full_scan_interval`到期强制从头开始，防止持续Merge导致饥饿；
- 每个Binding记录`last_full_scan_at`；
- 任何Object从进入到期区间到被再次观察的最大延迟不超过
  `max(full_scan_interval, scheduler_backlog_slo)`，超限告警并停止扩大放量。

## 3. Lifecycle列Metadata分类

`PartitionState`负责列出当前Object，不代表`ObjectStats`已经内嵌任意列统计。当前
`ObjectStats.SortKeyZoneMap()`只覆盖物理sort key。分类分两条路径：

```text
Lifecycle列 == 当前物理sort key
  -> 直接使用ObjectStats.SortKeyZoneMap

Lifecycle列 != sort key
  -> 根据ObjectLocation有界range-read Object metadata/ZoneMap area
  -> 只加载lifecycle column seqnum对应的Block ZoneMap
  -> 聚合Object min/max
```

Binding保存稳定Column ID；每个cycle在固定schema digest下解析为物理seqnum。映射失败或
schema变化立即停止该page并重新规划。

metadata read不是数据行扫描，但仍必须计入：

```text
max_metadata_reads_per_page
max_metadata_read_bytes_per_page
provider metadata-read concurrency
deadline/cache hit ratio
```

任何上限命中都结束当前page并保存cursor，不退化为一次性全表I/O。ZoneMap area缺失、
类型不匹配、min/max截断或校验失败时，标记`READER_CLASSIFY`，由Exact Reader读取。

对于Lifecycle列有可信ZoneMap的Object：

```text
max <= cutoff            Whole hint
min > cutoff             Not due
min <= cutoff < max      Mixed hint
ZoneMap缺失/截断/异常     Reader classify
```

ZoneMap不能证明Snapshot delete、NULL、列类型转换或精确到期行数。分类只决定读哪些Object。

Candidate只存在于有界内存队列：

```text
binding identity
snapshot TS
ObjectStats clone/digest
hint mode
estimated rows/bytes
evaluation_time / effective_cutoff / evaluation_timezone
deadline
```

crash后丢失并重扫。final transaction从不信任Candidate。

## 4. 调度

只扫描显式ACTIVE Binding。初始上限：

```text
per table child       1
per database          2
per account           4
cluster child         8
cluster Rewrite       1
```

每轮按账户公平队列调度；单个大表不能占满所有child。Provider 429、内存不足、cleanup
backlog和unknown Root达到阈值时只暂停Lifecycle。

TaskService只负责一个有界Coordinator和child租约，不保存Object正确性：

```text
Coordinator tick
-> 分页读取ACTIVE Binding
-> 按account/database/table限额发child
-> child: DISCOVER -> PROTECT -> READ/WRITE -> VERIFY -> FINALIZE
-> terminal: SUCCEEDED | RETRYABLE | BLOCKED | FAILED | UNKNOWN
```

Candidate可随child丢失；Root和Dataset才是外部副作用/发布权威。Task lease丢失不会让
新worker接管旧attempt：有Root的旧attempt进入清理，新worker重新Discovery。单表child
并发1是Scheduler限额，不是Catalog锁。

普通Merge连续抢先时，child按冲突次数和wall time退避；超过上限进入`CONFLICT_BLOCKED`，
不继续消耗Provider/Rewrite资源，也不要求普通Merge等待。用户DML继续正常运行。

## 5. Source Snapshot与SyncProtection

Child冻结`source_snapshot_ts=S`。`source_set`只包含将被退休的Data Object；
`protection_set`包含这些Data Object以及S时Reader会消费的Tombstone Object：

```text
visible data source objects
-> SelectLifecycleSnapshotTombstones一次性选择S时可能相关的Tombstone Objects
-> 选择结果作为既有Snapshot Reader可能消费的物理Tombstone保守超集交给SyncProtection
-> 固定ObjectStats/name/digest
-> source_set_digest只覆盖Data
-> protection_set_digest覆盖Data + Tombstone
```

Snapshot Reader继续复用同一`PartitionState`和MO现有可见性逻辑，包括内存Tombstone；
Lifecycle不再实现第二套Reader输入注入。Protection Selector只负责保护Reader可能读取的
物理Tombstone文件，允许假阳性，不允许假阴性。

两组身份不得在后续wire中重新合并：

- `data_sources[]`和`source_set_digest`只描述本次要退休的Data Object；
- Tombstone Object只属于Snapshot Reader和SyncProtection job；
- TN finalizer只能对`data_sources[]`执行`SoftDeleteObject(..., false)`；
- Tombstone Object绝不进入Lifecycle retire entry或source Drop集合。

Protection Selector必须是现有Snapshot Reader物理输入的保守超集，不能维护一个更窄的
Lifecycle可见性规则。建议接口：

```go
func SelectLifecycleSnapshotTombstones(
    ctx context.Context,
    snapshot types.TS,
    sources []objectio.ObjectStats,
    limits TombstoneSelectionLimits,
) ([]ExactTombstoneObject, error)
```

只有已初始化、类型正确、未损坏的RowID ZoneMap能够证明与全部source Object范围不相交时，
才允许排除Tombstone Object。ZoneMap缺失、legacy未初始化、截断、解码失败或RowID范围
异常时，不调用`RowidPrefixEq`，而是保守纳入Reader/protection输入；加载完整metadata仍
无法解析则当前attempt失败。保守纳入后超过files/bytes上限返回`RESOURCE_BLOCKED`，不能
继续归档。

新Tombstone在S之后出现，不属于S时Reader输入，由final Prepare的post-S visitor处理。

protection文件数和编码Bloom Filter bytes受Lifecycle独立上限；Bloom Filter
false positive只会多保护文件，不影响正确性。达到上限就缩小Whole batch，Mixed单源仍
超限则`RESOURCE_BLOCKED`。

保护流程：

1. 注册现有GC SyncProtection；
2. 注册成功后重新Stat全部exact文件；
3. 验证文件名、ObjectStats和source set digest；
4. 首期不续长attempt，Reader context与Protection使用同一个冻结绝对deadline；
5. final Prepare再次验证job ID；
6. deadline到期、TN重启或保护丢失立即停止attempt；
7. 不复用原staging恢复attempt；
8. Reader/final不再使用source后释放；
9. COMMIT_UNKNOWN不保留或延长源Object protection。

释放调用必须脱离已经取消的worker context，但使用Lifecycle本地的短硬超时；不得用无
deadline的后台context等待GC owner。释放失败只记录并由现有SyncProtection TTL收敛，
不能延长attempt或阻塞普通MO。

`SyncProtectionJobID`由`attempt_id + protection_set_digest`确定性生成，同一attempt不得换用
另一个有效job ID。它仍是当前进程内可失效租约，不变成持久GC引用。

## 6. Exact Reader接口

Whole Reader：

```go
ReadWholeObject(
    ctx context.Context,
    snapshot types.TS,
    source objectio.ObjectStats,
    projection []uint16,
    consume func(*batch.Batch) error,
) error
```

Mixed Rewrite Host：

```go
type LifecycleRewriteHost interface {
    mergesort.MergeTaskHost
    SourceSnapshotTS() types.TS
    ClassifyBlock(*batch.Batch, *nulls.Nulls) (expired *nulls.Nulls, err error)
    ArchiveExpired(*batch.Batch, *nulls.Nulls) error
}
```

Reader callback首版串行。Batch借用和release exactly once；callback返回后不得持有vector。

## 7. Mixed物理输入合同

`LoadNextBatch`必须返回完整原始物理Block：

- Object、Block和row offset顺序不变；
- D为S时已删除；
- E为S时可见且到期；
- L为S时可见且未到期；
- `D ∪ E`作为`DoMergeAndWrite`的delete bitmap；
- 不先抽取L重建Batch。

Archive只编码E；TTL丢弃E；`DoMergeAndWrite`只输出L并生成TransferTable。

分类完成后计算：

```text
rewrite_amplification =
  live_logical_bytes / max(expired_logical_bytes, 1)
```

`live_logical_bytes`和`expired_logical_bytes`都使用canonical逻辑字节口径，不能混用Parquet
压缩bytes或Object物理size。超过release profile的`max-rewrite-amplification`时，不进入
final transaction，返回`MIXED_LAYOUT_BLOCKED`；Archive/Rewrite已经产生的Root staging
按Cleanup协议异步回收。该限制防止Lifecycle列与Object布局严重不相关时每天重写几乎全部
live data。

每个Mixed source在Reader启动前还按exact ObjectStats size向Scheduler记账，受账户/集群
固定窗口的rewrite source bytes预算约束；已经开始读取的source无论成功、blocked或abort
都计入本窗口，不能通过失败重试绕过预算。具体Owner和failover语义见06。

## 8. 读取前内存准入

在`BlockDataReadNoCopy`之前重新读取Block metadata/column extents，并按实际schema参数
估算。首个认证实现冻结为：

```text
estimated_peak_bytes =
  2 * source_block_logical_bytes
  + 64 MiB Archive chunk/encoder budget
  + 16 MiB transfer slab budget
  + 16 MiB safety margin
```

估算未知、溢出或超过`max_certified_block_read_bytes`时，在读取前返回
`RESOURCE_BLOCKED`。默认`max_certified_block_read_bytes=256 MiB`，因此在该保守公式下
单Block source logical bytes最多80 MiB。3 GiB Object只有在由多个认证Block组成时承诺
streaming。这个额外metadata读取和预算只在Lifecycle Rewrite Host非nil时执行；普通Merge
保持原路径，不增加读取或准入。

## 9. Schema Descriptor

Manifest必须保存可创建新表的版本化逻辑schema，不只有digest：

```text
schema_format_version
reader_min_version
columns[] {
  ordinal
  source_column_id
  name
  mo_type
  type_parameters
  width
  scale
  nullable
  charset
  collation
  auto_increment
}
schema_descriptor_digest
```

AUTO_INCREMENT运行数据不属于结构descriptor，也不进入`schema_descriptor_digest`。
Manifest单独保存：

```text
auto_increment_stats[] {
  source_column_id
  ordinal
  name
  mo_integer_type
  has_positive_value
  archived_max_positive_value_uint64
}
```

descriptor和Archive Payload只包含用户逻辑列，不包含RowID、fake PK、commit TS或隐藏索引
内部列。Mixed Rewrite仍按现有Merge schema处理live TAE行；Archive writer使用独立的
用户列projection，两者不能混用列ordinal。

`source_column_id`只用于来源追踪和归档时的源schema fence。Restore通过普通DDL创建新表，
目标列由MO分配新的Column ID；目标结构按ordinal/name/type/nullability等Phase 1恢复字段
校验，不要求目标Column ID等于`source_column_id`，也不新增第二个持久
`restore_schema_digest`。

Phase 1 Restore只恢复列结构和数据，不恢复PK、UNIQUE/CHECK/FK、二级索引、CDC、
Publication、插件、权限和源表默认表达式。`auto_increment`属性可以恢复：加载期间禁用
自动生成；Archive Writer按列统计已归档、严格大于0值的最大值（与现有
`incrservice`处理显式值的口径一致），Provider full readback从最终MO逻辑值重新计算并
校验`archived_max_positive_value_uint64`。Restore最终发布事务先使用现有
`ValidateAutoColumnOffset`校验目标整数类型，再对新表调用
`incrservice.SetOffset(max, txnOp)`，使下一次分配大于已恢复最大正值；若最大值已经等于该
整数类型上限，则保持allocator耗尽/后续INSERT返回out-of-range，禁止执行`max+1`造成
overflow。没有正值（仅NULL、0或负值）时不调用`SetOffset`，沿用新表初始offset。

Parquet物理映射是一个版本化合同。Archive Writer和Restore Reader实现必须位于同一
Lifecycle package，并由双向golden test证明一致；不能在不同模块各自演化不兼容映射。
Phase 1优先采用确定、无损、容易跨版本恢复的物理表示：

| MO类型族 | Parquet物理/逻辑类型 |
|---|---|
| BOOL | BOOLEAN |
| 有符号/无符号整数 | INT32/INT64 + signedness logical annotation |
| FLOAT/DOUBLE | FLOAT/DOUBLE |
| DECIMAL | UTF8规范字符串；precision/scale保存在descriptor |
| DATE/TIME/DATETIME/TIMESTAMP | UTF8规范字符串；scale/时区语义保存在descriptor |
| CHAR/VARCHAR/TEXT/JSON/UUID | UTF8规范字符串；精确MO类型保存在descriptor |
| ENUM | UINT32；枚举值语义保存在descriptor |
| BINARY/VARBINARY/BLOB | BYTE_ARRAY |

不在合同和golden test矩阵中的类型，Bind或Archive开始前fail closed。这里的字符串映射是
Phase 1显式、版本化的持久格式，不是遇到未知类型时的隐式降级；Manifest descriptor和
canonical hash仍保存、验证精确MO类型语义。

## 10. Canonical逻辑编码

hash输入必须有framing：

```text
row_begin
column_ordinal
type_tag
null_tag
value_length
value_bytes
row_end
```

覆盖NULL、CHAR尾空格、DECIMAL scale、TIMESTAMP时区、JSON canonical form、浮点NaN/
signed zero和binary。Encoder版本写入Manifest，unknown版本Restore fail closed。

稳定行序：

- Whole：Object ID、Block ordinal、Row offset；
- Mixed：单source Block ordinal、Row offset；
- File/Row Group：单调ordinal。

一个Chunk严格等于Manifest中的一个Parquet Row Group。Manifest先按`file_ordinal`排序文件，
再按文件内`row_group_ordinal`排序Row Group，展平后从0开始连续分配全局
`chunk_ordinal`。这个边界由Archive Writer冻结；Restore必须直接使用Manifest中的边界和
ordinal，禁止按运行时`chunk-bytes`、INSERT Batch大小或worker版本重新切分、合并Chunk。

每个chunk保存：

```text
chunk_ordinal
file_ordinal
row_group_ordinal
row_count
logical_bytes
canonical_content_hash
chunk_digest
```

`logical_bytes`是该Row Group所有行经过同一canonical encoder及其framing后的未压缩字节数，
不是Parquet压缩大小、文件大小或内存allocator用量。它与`row_count`共同构成Archive Writer
和Restore共享的可恢复容量合同。

公式冻结为：

```text
canonical_content_hash =
  SHA256(
    0xD1
    || uint16_be(canonical_encoder_version=1)
    || schema_digest[32]
    || ordered canonical row bytes)

chunk_digest =
  ArchiveAggregateHash(
    schema_digest = 32 bytes zero,
    chunks = [this chunk])

Manifest.dataset_content_hash =
  ArchiveAggregateHash(
    schema_digest,
    ordered chunks)

ArchiveAggregateHash(schema_digest, chunks) =
  SHA256(
    "matrixone/lifecycle/archive-dataset/v1"
    || uint16_be(hash_formula_version=1)
    || schema_digest[32]
    || uint64_be(len(chunks))
    || for each chunk in ordinal order:
         uint64_be(chunk_ordinal)
         || uint64_be(row_count)
         || uint64_be(logical_bytes)
         || canonical_content_hash[32])

Dataset.content_hash = Manifest.dataset_content_hash
```

canonical row bytes严格按第10节的row/column/type/null/value-length framing；所有整数使用
big-endian定长编码。Phase 1固定`file_ordinal == chunk_ordinal`且
`row_group_ordinal == 0`，这两个字段由Manifest shape单独验证，不重复进入派生
`chunk_digest`。Archive Writer和Provider full readback使用同一公式；Restore可在CN crash
后按`chunk_ordinal`读取Receipt重建最终`Manifest.dataset_content_hash`，不持久化SHA内部
状态，也不重新扫描隐藏表或全部Payload。`hash_formula_version=1`对应以上domain separator
和聚合字段；unknown版本Restore必须fail closed。

## 11. Parquet与Manifest

首个GA的Dataset Purge时间不要求Writer维护逐行Lifecycle min/max；Finalizer使用冻结的
`effective_cutoff + purge_interval`作为保守`purge_eligible_at`。它可能延后回收，但不会
早于对象内最年轻归档行的保留期限，也不增加新的聚合状态。

Parquet使用ZSTD。每个文件记录：

```text
ordinal
immutable key
size
SHA-256
row_count
logical_bytes
row_groups[] {
  chunk_ordinal
  row_group_ordinal
  row_count
  logical_bytes
  canonical_content_hash
}
min/max（可选）
```

Manifest顶层必须先记录：

```text
manifest_format_version = 1
```

Reader必须先按该字段选择parser；缺失、0或未知版本在读取任何可变长度集合前fail closed。
Manifest记录Dataset/Root/Attempt、schema descriptor、AUTO_INCREMENT归档最大值、文件集、
`total_chunk_count`、`dataset_content_hash`、`hash_formula_version`、总行数、
source snapshot/evaluation time/cutoff/source set digest、Lifecycle列min/max、Stage
identity和加密信息。`total_chunk_count`必须等于所有文件Row Group数量之和，合法
`chunk_ordinal`范围严格为`0..total_chunk_count-1`。每个文件的`row_count`和
`logical_bytes`必须分别等于其Row Group对应字段之和。

Writer按`target_payload_file_bytes`流式切分，不按“每天一个文件”或“月底全量合并”。
默认值在Provider/Restore认证后冻结，并同时受`max_payload_files_per_dataset`和单次Root
bytes上限约束。每个Row Group对应一个Restore chunk，并受`max_chunks_per_dataset`硬上限；
Writer在产生超限Manifest前必须缩小source batch或返回`RESOURCE_BLOCKED`。Phase 1不做
Archive compaction；需要合并文件时作为独立优化设计。

Phase 1两个上限统一冻结为4096，因为一个Payload严格只有一个Row Group。Writer必须在
第4097个Payload PUT之前拒绝，Manifest parser与Restore还要重复校验；不能等全部Payload
写完后才发现Manifest超限。

每个Row Group还必须同时满足：

```text
row_count <= max-restore-chunk-rows
logical_bytes <= max-restore-chunk-logical-bytes
```

两个上限都是认证配置，启动时必须大于0且不能运行时自动放宽。Writer在追加下一行会使任一
上限超出时，先flush当前非空Row Group，再把该行写入新Row Group；等于上限合法。若单行
canonical logical bytes已经超过上限，返回`RESOURCE_BLOCKED`，禁止进入final transaction，
源数据保持活动可见，Root异步清理此前已产生的staging。

Provider full readback必须根据实际解码结果重新计算每个Row Group的`row_count`和
`logical_bytes`并与Manifest比较。Restore在GET/解码前检查Manifest声明的两个值不超过
当前release profile上限；unknown、缺失、0值或超限一律fail closed。Restore实际解码和
普通INSERT仍使用现有CN内存/事务预算，不能把高压缩率后的物理size当作准入依据。

Writer接口：

```go
type ArchiveWriter interface {
    AppendSelected(
        ctx context.Context,
        physicalBlock *batch.Batch,
        expired *nulls.Nulls,
    ) error
    FinishPayloads(ctx context.Context) (files []ArchiveFile, stats ArchiveStats, err error)
    WriteAndReadbackManifest(
        ctx context.Context,
        manifest ManifestV1,
    ) (key string, sha256 [32]byte, err error)
    Close() error
}
```

Owner合同：

- `physicalBlock`由Reader借用，`AppendSelected`返回前不得保留vector引用；
- Writer拥有Parquet/压缩/checksum buffer并在`Close` exactly-once释放；
- remote key在PUT前由Root namespace allocator产生；
- Writer错误只关闭本地资源并推进Root清理，不在回调内猜测Delete远端文件；
- `FinishPayloads`完成所有Payload PUT与full readback后，才能生成Manifest；
- Manifest自身readback成功后才返回可进入final transaction的结果。

## 12. Stage Adapter、不可变Key与multipart

Lifecycle复用现有`mo_stages`作为用户入口，但Reader/Restore/Sweeper不能每次重新解释可变URL：

```go
type FrozenArchiveTarget struct {
    StageID            uint64
    Provider           string
    CanonicalEndpoint  string
    Region             string
    BucketOrContainer  string
    ImmutablePrefix    string
    StorageClass       string
    EncryptionIdentity []byte
    CredentialHandle   string
}

ResolveLifecycleStage(
    ctx context.Context,
    accountID uint32,
    stageID uint64,
) (FrozenArchiveTarget, error)
```

Bind时解析并校验一次，Dataset/Root保存冻结结果。Restore和Sweeper使用冻结target + 最新
credential handle解析结果，不重新读取Stage URL决定namespace。

`ResolveLifecycleStage`还必须查询部署管理的Lifecycle Stage认证记录/allowlist。Phase 1
只接受专用、对象Versioning关闭的Bucket/Container；当前通用`ObjectStorage`不提供Bucket
Versioning查询或version ID删除，因此无法证明上述条件时必须拒绝Archive Binding。认证记录
属于部署配置，不新增Lifecycle Catalog状态机。

Stage DDL只在控制面检查Lifecycle引用：

- 有Binding/Dataset/Root/Restore引用时拒绝修改endpoint/region/bucket/prefix/storage
  class/encryption；
- credential可轮换，但handle identity不变；
- 有Dataset承诺Restore时拒绝DROP Stage；
- `DROP ACCOUNT`仍按普通MO完成，system Root通过部署管理handle继续清理；
- inline-only `stage_credentials`不允许用于Lifecycle。

Binding和Dataset都持久化`stage_id`并提供索引化引用路径；历史Dataset检查不能解析
`stage_identity_blob`后做全表扫描。Root按冻结namespace/credential handle清理，不依赖
当前Stage行仍存在。

Provider adapter必须通过PUT/GET/HEAD/LIST/Delete、multipart Abort、限流/超时和加密认证。
Phase 1不接受需要异步thaw才能GET的Deep Archive target。

专用Bucket在仍有Binding/Dataset/Root时不得由运维开启Versioning。Lifecycle首个GA不建设
运行时Bucket漂移检测系统：该变更属于破坏受支持部署合同的外部配置。运维发现漂移后必须
撤销Stage认证并暂停新Archive；已有Dataset的历史版本由Provider运维工具清理，Root在重新
认证或人工确认物理清理前不能因该异常路径被宣称完成。之后可单独扩展Provider adapter的
version ID能力，但不修改Phase 1通用FileService接口。

每次物理写使用：

```text
payload-<ordinal>-<write-id>.parquet
manifest-<manifest-digest>.json
```

禁止覆盖固定`payload-N`或`manifest.json`。Manifest只引用full readback已验证的key。
worker/SyncProtection丢失后创建新Root/attempt/prefix，旧Root只清理。

Phase 1 Stage必须配置Provider侧`AbortIncompleteMultipartUpload`规则，并在Bind时验证或由
运维认证；否则该Stage不能用于Archive。FileService正常错误仍主动Abort。

## 13. Full readback

所有文件PUT完成后从Provider重新GET：

- 重新计算文件SHA-256；
- 使用Parquet decoder和同一canonical encoder计算每个chunk hash及Dataset聚合hash；
- 校验schema descriptor/digest；
- 校验文件集合、ordinal和总行数。

随后生成canonical Manifest bytes，使用digest命名PUT，再从Provider GET并验证Manifest
SHA-256。只有Payload和Manifest都完成readback，Root才能进入VERIFIED。

HEAD、ETag或本地buffer hash不能替代full readback。任何失败Root进入`DELETE_PENDING`。
