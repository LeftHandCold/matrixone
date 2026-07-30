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
-> 同一选择结果同时交给Snapshot Reader和SyncProtection
-> 固定ObjectStats/name/digest
-> source_set_digest只覆盖Data
-> protection_set_digest覆盖Data + Tombstone
```

禁止Protection Selector和Snapshot Reader分别维护两套Tombstone筛选规则。建议接口：

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

protection文件数、编码Bloom Filter bytes和续租数量受Lifecycle独立上限；Bloom Filter
false positive只会多保护文件，不影响正确性。达到上限就缩小Whole batch，Mixed单源仍
超限则`RESOURCE_BLOCKED`。

保护流程：

1. 注册现有GC SyncProtection；
2. 注册成功后重新Stat全部exact文件；
3. 验证文件名、ObjectStats和source set digest；
4. 读取期间续租；
5. final Prepare再次验证job ID；
6. 续租失败、TN重启或保护丢失立即停止attempt；
7. 不复用原staging恢复attempt；
8. Reader/final不再使用source后释放；
9. COMMIT_UNKNOWN不无限续租。

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

## 8. 读取前内存准入

根据Object metadata/column extents和实际schema参数估算：

```text
source vectors
+ merge output buffer
+ parquet encoder buffer
+ transfer slab allocator capacity
+ checksum/readback buffer
+ safety margin
```

估算未知、溢出或超过`max_certified_block_read_bytes`时，在读取前返回
`RESOURCE_BLOCKED`。3 GiB Object只有在由多个认证Block组成时承诺streaming。

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

descriptor和Archive Payload只包含用户逻辑列，不包含RowID、fake PK、commit TS或隐藏索引
内部列。Mixed Rewrite仍按现有Merge schema处理live TAE行；Archive writer使用独立的
用户列projection，两者不能混用列ordinal。

`source_column_id`只用于来源追踪和归档时的源schema fence。Restore通过普通DDL创建新表，
目标列由MO分配新的Column ID；目标结构按ordinal/name/type/nullability等Phase 1恢复字段
校验，不要求目标Column ID等于`source_column_id`，也不新增第二个持久
`restore_schema_digest`。

Phase 1 Restore只恢复列结构和数据，不恢复PK、UNIQUE/CHECK/FK、二级索引、CDC、
Publication、插件、权限和源表默认表达式。`auto_increment`属性可以恢复：加载期间禁用
自动生成，完成后把新表计数器推进到大于已恢复最大值的安全位置。

Parquet物理映射由一个版本化registry集中实现，Archive Writer和Restore Reader必须引用
同一registry，禁止各自维护switch：

| MO类型族 | Parquet物理/逻辑类型 |
|---|---|
| BOOL | BOOLEAN |
| 有符号/无符号整数 | INT32/INT64 + signedness logical annotation |
| FLOAT/DOUBLE | FLOAT/DOUBLE |
| DECIMAL | FIXED_LEN_BYTE_ARRAY + DECIMAL(precision, scale) |
| DATE | INT32 + DATE |
| TIME/DATETIME/TIMESTAMP | INT64 +对应MICROS annotation，MO语义由descriptor补充 |
| CHAR/VARCHAR/TEXT/JSON/UUID/ENUM | BYTE_ARRAY；文本类使用UTF8，精确MO类型保存在descriptor |
| BINARY/VARBINARY/BLOB | BYTE_ARRAY |

不在registry和golden test矩阵中的类型，Bind或Archive开始前fail closed，不能退化为字符串。

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

Archive与Restore的可重启内容摘要以Parquet Row Group为chunk边界。每个chunk保存：

```text
chunk_ordinal
file_ordinal
row_group_ordinal
row_count
canonical_content_hash
chunk_digest
```

公式冻结为：

```text
canonical_content_hash =
  SHA256("mo-lifecycle-chunk-content/v1" || ordered canonical row bytes)

chunk_digest =
  SHA256("mo-lifecycle-chunk/v1"
         || chunk_ordinal || file_ordinal || row_group_ordinal
         || row_count || canonical_content_hash)

Dataset.content_hash =
  SHA256("mo-lifecycle-dataset-content/v1"
         || ordered framing(
              chunk_ordinal,
              row_count,
              canonical_content_hash))
```

所有整数使用canonical big-endian定长编码，字节串带长度framing。Archive Writer和Provider
full readback使用同一公式；Restore可在CN crash后按`chunk_ordinal`读取Receipt重建最终
`Dataset.content_hash`，不持久化SHA内部状态，也不重新扫描隐藏表或全部Payload。

## 11. Parquet与Manifest

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
  canonical_content_hash
  chunk_digest
}
min/max（可选）
```

Manifest记录Dataset/Root/Attempt、schema descriptor、文件集、content hash、总行数、
source snapshot/evaluation time/cutoff/source set digest、Lifecycle列min/max、Stage identity
和加密信息。

Writer按`target_payload_file_bytes`流式切分，不按“每天一个文件”或“月底全量合并”。
默认值在Provider/Restore认证后冻结，并同时受`max_payload_files_per_dataset`和单次Root
bytes上限约束。每个Row Group对应一个Restore chunk，并受`max_chunks_per_dataset`硬上限；
Writer在产生超限Manifest前必须缩小source batch或返回`RESOURCE_BLOCKED`。Phase 1不做
Archive compaction；需要合并文件时作为独立优化设计。

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
