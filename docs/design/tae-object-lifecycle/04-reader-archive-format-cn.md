# Exact Reader、Archive 格式与校验详细设计

> 本文唯一负责 Exact Reader 接口、Batch 所有权、Archive Writer、Parquet/ZSTD 映射、
> Manifest、content root 和 Provider 全量 readback。

## 1. 目标

Lifecycle Reader 必须同时满足：

- 复用 MO 现有 Reader/Tombstone/ObjectIO/Cache；
- 只读取调用方给出的持久化 source Object set；
- 在一个固定事务 Snapshot 上输出逻辑可见行；
- 不把表级 in-memory rows 混入；
- 支持 Archive 业务列和 Mixed DELETE control 列；
- 能证明完整读到了所有请求 Object/Block；
- 串行、流式、有背压、有内存上限；
- 任何短读、Object missing、checksum error、cancel 都失败；
- Archive 内容不依赖 Batch 大小、文件切分或 CN 并行度。

## 2. 现有代码复用

主要复用：

```text
pkg/vm/engine/types.go
  engine.Relation
  engine.Reader
  engine.RelData

pkg/vm/engine/disttae/snapshot_scan.go
  Snapshot reader construction
  Tombstone application
  reusable Batch callback

pkg/vm/engine/disttae/txn_table.go
  BuildReaders
  ObjListRelData
  __mo_rowid projection

pkg/vm/engine/readutil/reader.go
  Object/block read
  snapshot-visible Tombstone filtering
```

不直接复用 `pkg/frontend/export.go` 的导出执行器，因为当前 Parquet export：

- 依赖 `MysqlResultSet`；
- 按前端结果类型映射；
- 单个 Parquet 文件在内存中累计完整 bytes 后再写；
- 不提供 source/readback content root；
- 不提供 Root/multipart 所有权回调。

可以抽取和复核其中的 `parquet-go` 类型映射，但 Lifecycle Writer 必须是独立的 streaming library。

## 3. 新接口

实现位置：

```text
pkg/vm/engine/disttae/lifecycle_scan.go
pkg/vm/engine/disttae/lifecycle_scan_test.go
pkg/lifecycle/reader/
  scan.go
  projection.go
  report.go
  canonical.go
```

接口：

```go
type ExactObjectRef struct {
    Stats       objectio.ObjectStats
    StatsDigest [32]byte
}

type ExactScanMode uint8

const (
    ScanAllVisible ExactScanMode = iota + 1
    ScanLifecycleExpired
)

type ExactScanSpec struct {
    PhysicalTableID  uint64
    SchemaGeneration uint64
    SchemaDigest     [32]byte
    SnapshotTS       types.TS
    Objects          []ExactObjectRef

    Mode              ExactScanMode
    LifecycleSeqnum   uint16
    Cutoff            types.Datetime
    BusinessColumns   []uint16
    IncludeRowID      bool
    // 必须复用普通 DELETE 的真实键编码：
    // 单 PK、复合 PK encoded key，或无 PK fake key。
    DeleteKeyProjection DeleteKeyProjection

    MaxBatchRows       int
    MaxBatchBytes      int64
}

type BorrowedBatch struct {
    Batch             *batch.Batch
    BusinessVecs      []int
    RowIDVec           int
    DeleteKeyVecs      []int
    ObjectOrdinalVec   int
    BlockOrdinalVec    int
    RowOffsetVec       int
}

type ScanConsumer func(context.Context, BorrowedBatch) error

type ScanReport struct {
    RequestedObjects       uint64
    ReachedObjects         uint64
    RequestedBlocks        uint64
    ReachedBlocks          uint64
    PhysicalRows           uint64
    SnapshotVisibleRows    uint64
    SelectedVisibleRows    uint64
    SelectedBusinessBytes  uint64
    DeleteKeyBytes         uint64
    AffectedBlocks         uint64
    TombstoneRowsApplied   uint64
    Complete               bool
    CanonicalRoot          [32]byte
}

func ScanExactObjects(
    ctx context.Context,
    rel engine.Relation,
    spec ExactScanSpec,
    consume ScanConsumer,
) (ScanReport, error)
```

`rel` 必须绑定到 SnapshotTS 与 `spec.SnapshotTS` 完全相同的事务；不允许函数内部创建新的 RC statement Snapshot。

## 4. 输入校验

调用前检查：

```text
len(objects) in [1, configured max]
objects sorted by Object ID ascending
no duplicate Object ID
all ObjectStats valid
stats digest matches canonical ObjectStats bytes
all objects are persisted data Object
no mem block marker
schema/table identity matches opened Relation
BusinessColumns unique and visible
LifecycleSeqnum type matches Binding
DeleteKeyProjection 与 Relation TableDef 以及普通 DELETE 的实际预处理结果一致
MaxBatchRows/Bytes within release profile
```

如果调用方没有排序，返回 `LIFECYCLE_INVALID_SOURCE_ORDER`，不在 Reader 内静默重排并改变 source digest。

## 5. Exact RelData 构造

为每个 ObjectStats 按 block ordinal 生成 exact block list：

```text
Object 0 block 0..N
Object 1 block 0..N
...
```

构造 `ObjListRelData` 时：

- 只包含这些 Object 的持久化 BlockInfo；
- 不调用会附加当前 workspace/in-memory row 的 table-level Ranges；
- 不包含 source set 之外的 Object；
- 固定一个 Reader，首个 GA 不并行；
- Tombstone policy 使用 Snapshot-visible policy；
- reader filter 只用于生命周期行选择，不能用 ZoneMap 跳过 coverage 统计。

ZoneMap 可以决定整块“不选任何到期行”，但 Reader 仍必须把该 block 标记为 reached，且必须证明 filter 使用的 block metadata对应 exact source block。

## 6. 行选择

### 6.1 Whole Archive

```text
Mode = ScanAllVisible
```

Reader 输出 source Snapshot 下所有逻辑可见业务行。Snapshot 已删除行不进入 Archive。

### 6.2 Mixed TTL/Archive

```text
Mode = ScanLifecycleExpired
selected = visible AND lifecycle_value < cutoff
```

Reader 必须统计：

- Object 物理行；
- Snapshot 逻辑可见行；
- 到期逻辑可见行；
- 到期行涉及的 block；
- RowID/delete key encoded bytes。

未到期行不进入 callback，但计入 `SnapshotVisibleRows`，用于 Mixed ratio 和预算。

### 6.3 NULL

Binding 已拒绝 nullable 生命周期列。如果底层读到 NULL：

- 认为 schema/data invariant 失败；
- 立即停止；
- 不把 NULL 当到期；
- 不退休任何行。

## 7. Batch 所有权

唯一合同：

```text
Reader owns Batch and all Vector memory.
Consumer may use them synchronously until callback returns.
Consumer must copy any retained bytes before return.
After return Reader may reset, reuse or clean the Batch.
```

Reader：

- 每次只调用一个 consumer；
- 不并行 callback；
- callback 返回 error 后不再调用；
- success/error/cancel/panic 都关闭底层 Reader；
- Batch/Vector cleanup exactly once；
- `Reader.Close` error 不覆盖更早的主错误，但作为 joined diagnostic 返回；
- callback panic 转成内部 error，执行 cleanup 后重新进入 Job failure path。

Consumer：

- 不把 Vector 指针发给异步 goroutine；
- Writer 必须在返回前完成序列化或复制；
- backpressure 直接阻塞 callback；
- 不调用 Batch.Clean；
- 不修改 RowID/delete key vector。

默认：

```text
MaxBatchRows  = 8,192
MaxBatchBytes = 64 MiB
```

单行 varlen 超过 64 MiB 时允许一个 oversize row batch，但受单行 256 MiB hard limit；超过则 Binding/Job fail closed。

## 8. Coverage 证明

Reader 为每个 exact block 维护：

```text
REQUESTED -> OPENED -> EOF_REACHED
```

`ReachedBlocks` 只在 block Reader 明确 EOF 后递增。以下不是完整：

- Object open 返回 not found；
- Batch 为空但未到 EOF；
- context deadline；
- checksum/short read；
- consumer 提前停止；
- reader close 前仍有未消费 block；
- Tombstone loader 失败；
- metadata rows/blocks 与实际不一致。

成功条件：

```text
ReachedObjects == RequestedObjects
AND ReachedBlocks == RequestedBlocks
AND Complete == true
```

`SelectedVisibleRows == 0` 只有在上述条件全部满足时才是合法空结果。

Whole Archive 的 source read transaction 只持有到：

```text
Exact Reader全部Object/Block到EOF
AND所有consumer callback返回
AND所有Payload writer close/PUT complete
AND source ScanReport/content root冻结
```

此后立即结束只读事务，再执行Provider full readback和Manifest PUT；这些步骤不能继续
访问源TAE。最终Strict CAS仍可能因Merge/DML变化而abort。这样避免把远端GET校验时间
算进source Snapshot pin。Mixed Archive不能采用该优化，因为Reader和DELETE必须保留
同一个writable SI事务。

## 9. Canonical row order

首个 GA 固定：

```text
Object ID ascending
  -> Block ordinal ascending
  -> physical row offset ascending
  -> 跳过 Snapshot 不可见行
  -> 跳过 Mixed 未到期行
```

为每个选中行分配从 0 开始的 `dataset_row_ordinal`。ordinal 只进 hash/Manifest row-group range，不作为用户列写入 Payload。

## 10. Canonical value encoding

算法版本：

```text
MO_LIFECYCLE_CANONICAL_V1
```

每个 cell：

```text
column_id uvarint
type_id uvarint
null_marker 1 byte
value_length uvarint
canonical_value bytes
```

规则：

- NULL：length 0，不编码默认值；
- signed/unsigned integer：固定宽度 big-endian；
- bool：0/1；
- float：IEEE bits；所有 NaN 规范化为单一 quiet NaN，`-0` 保留；
- Decimal：precision/scale + fixed-width two's-complement；
- DATE/DATETIME/TIMESTAMP/TIME：MO 内部整数 UTC/canonical value；
- CHAR/VARCHAR/TEXT/JSON：Vector 中实际 bytes，不补空格、不改 Unicode；
- BINARY/VARBINARY/BLOB：原 bytes；
- UUID：16 bytes；
- ENUM：稳定 numeric value；
- BIT：声明 bit width + raw bits。

不支持的内部/复合类型在 Binding 准入时拒绝。

row hash：

```text
Hrow = SHA256(
  "MO-LIFECYCLE-ROW-V1"
  || schema_digest
  || dataset_row_ordinal
  || encoded cells in column ID order
)
```

Dataset root 使用 streaming Merkle accumulator：

```text
leaf   = SHA256(0x00 || Hrow)
parent = SHA256(0x01 || left || right)
empty  = SHA256("MO-LIFECYCLE-EMPTY-V1" || schema_digest)
```

奇数节点使用明确 promotion rule：未配对节点提升到下一层，不复制自身。Manifest 记录 algorithm/version/row count。

## 11. Parquet schema

### 11.1 通用规则

- Parquet schema name：`matrixone_lifecycle_v1`；
- field ID 使用稳定 MO column ID；
- field name保存导出时用户列名；
- Manifest 保存完整 MO type/width/scale/collation/default；
- Parquet 只包含业务列；
- `__mo_rowid` 和 internal fake PK 不写 Payload；
- 用户显式 PK 本来是业务列，只写一份；
- compression：ZSTD level 3；
- row group target：64 MiB uncompressed；
- file target：256 MiB compressed；
- file hard max：512 MiB compressed；
- 单文件 row count 不超过 Parquet/reader library 安全限制。

### 11.2 类型映射

| MO type | Parquet physical/logical |
|---|---|
| BOOL | BOOLEAN |
| INT8/16/32 | INT32 + signed logical width |
| INT64 | INT64 |
| UINT8/16/32 | INT32/INT64 + unsigned logical width |
| UINT64 | FIXED_LEN_BYTE_ARRAY(8) + MO unsigned annotation |
| FLOAT32 | FLOAT |
| FLOAT64 | DOUBLE |
| DECIMAL64/128/256 | FIXED_LEN_BYTE_ARRAY + DECIMAL(precision,scale) |
| DATE | INT32 + DATE |
| TIME | INT64 + TIME_MICROS |
| DATETIME | INT64 + MO_DATETIME_MICROS annotation |
| TIMESTAMP | INT64 + TIMESTAMP_MICROS(isAdjustedToUTC=true) |
| CHAR/VARCHAR/TEXT | BYTE_ARRAY + UTF8 |
| JSON | BYTE_ARRAY + JSON + MO JSON version |
| BINARY/VARBINARY/BLOB | BYTE_ARRAY |
| UUID | FIXED_LEN_BYTE_ARRAY(16) + UUID |
| ENUM | INT32 + MO enum dictionary metadata |
| BIT | FIXED_LEN_BYTE_ARRAY + bit width metadata |

首个 GA拒绝：

- internal `TS/RowID/BlockID` 用户投影；
- tuple/row；
- array/vector；
- geometry 未定义版本；
- 依赖插件自定义序列化的类型。

类型支持是按整张表判断，因为 Restore 需要完整业务 schema；不能只因生命周期列受支持就接受其他不可恢复列。

## 12. Streaming Archive Writer

新增：

```text
pkg/lifecycle/archive/
  store.go
  writer.go
  parquet_schema.go
  canonical.go
  manifest.go
  verify.go
  keys.go
```

### 12.1 ArchiveStore

不扩大全部 `fileservice.ObjectStorage` 接口。新增适配层：

```go
type ImmutableObjectIdentity struct {
    Key             string
    ProviderVersion string
    Size            int64
    SHA256          [32]byte
}

type UploadObserver interface {
    OnMultipartCreated(ctx context.Context, uploadID string) error
    OnPutCompleted(ctx context.Context, id ImmutableObjectIdentity) error
}

type ArchiveStore interface {
    ProduceImmutable(
        ctx context.Context,
        key string,
        expectedMaxBytes int64,
        observer UploadObserver,
        produce func(context.Context, io.Writer) error,
    ) (ImmutableObjectIdentity, error)

    OpenExact(ctx context.Context, id ImmutableObjectIdentity) (io.ReadCloser, error)
    StatExact(ctx context.Context, id ImmutableObjectIdentity) error
    ListAttemptObjects(ctx context.Context, prefix string) iter.Seq2[ObjectInfo, error]
    ListAttemptUploads(ctx context.Context, prefix string) iter.Seq2[MultipartInfo, error]
    AbortMultipart(ctx context.Context, upload MultipartInfo) error
    DeleteExact(ctx context.Context, id ImmutableObjectIdentity) error
}
```

`ProduceImmutable` 内部可以用 `io.Pipe` 连接 Parquet producer 与 provider multipart uploader，但它拥有：

- pipe 两端；
- producer/uploader goroutine；
- cancel；
- first error；
- join；
- multipart abort handoff。

调用方不自行启动未跟踪 goroutine。

### 12.2 Root-before-PUT

Archive Attempt Control 在 Reader 开始前已经存在。Cleanup Root 可以 lazy 创建，以支持完整扫描后 0 selected rows：

```text
first selected Batch
  -> create/commit Cleanup Root REGISTERED
  -> allocate Root Object row ALLOCATED
  -> wait both commits visible
  -> ProduceImmutable
```

在 Root/Object row commit 前：

- 不创建 multipart；
- 不发第一个 part；
- 不调用 Provider PUT。

0 selected rows且 ScanReport complete：

- 不创建 Root；
- 不写空 Parquet/Manifest；
- Archive final transaction使用 `EMPTY_ARCHIVE` Receipt；
- 仍按对应 Whole/Mixed retirement条件执行。

### 12.3 文件切分

Writer 在 Batch/row-group边界检查 counting writer：

```text
compressed bytes >= 256 MiB
OR uncompressed row group >= 64 MiB
OR file rows >= release max
```

达到条件：

1. close 当前 Parquet footer；
2. 等待 PUT complete；
3. Root Object row记录 exact key/size/SHA；
4. 分配下一个 ordinal/key；
5. 继续消费。

不按天固定一个文件，也不每月重写合并。时间范围由 Manifest row-group stats 和 Dataset catalog 表达。

## 13. Immutable key

```text
<profile immutable prefix>/
  accounts/<account-incarnation-hex>/
  tables/<logical-table-id>/
  datasets/<dataset-id>/
  attempts/<attempt-id>/
  payload-000000.parquet
  payload-000001.parquet
  manifest.json
```

规则：

- attempt/dataset ID 为随机 UUID，不复用；
- ordinal 在同一 attempt只分配一次；
- key 不因 retry 改名；
- retry 前先由 Root/Object row + HEAD/LIST 判定前一次是否 complete；
- 已存在且 identity 匹配则复用；
- 已存在但 size/SHA 不匹配是 P0 corruption，不覆盖；
- 新 attempt使用新 prefix。

## 14. Manifest

格式：

```text
UTF-8 RFC 8785 canonical JSON
manifest_format = matrixone.lifecycle.manifest
manifest_version = 1
```

顶层至少包含：

```json
{
  "format": "matrixone.lifecycle.manifest",
  "version": 1,
  "dataset_id": "...",
  "attempt_id": "...",
  "account_incarnation": "...",
  "logical_table_id": "0",
  "source_physical_table_id": "0",
  "source_schema_generation": "0",
  "source_schema_version": "0",
  "source_schema_digest": "...",
  "source_snapshot_ts": "...",
  "evaluation_time_utc": "...",
  "cutoff_utc": "...",
  "source_objects": [],
  "source_object_digest": "...",
  "columns": [],
  "payloads": [],
  "source_visible_rows": "0",
  "archive_rows": "0",
  "content_root_algorithm": "MO_LIFECYCLE_MERKLE_V1",
  "source_content_root": "...",
  "archive_content_root": "...",
  "lifecycle_min": "...",
  "lifecycle_max": "...",
  "profile_identity": {},
  "encryption": {
    "mode": "SSE_KMS",
    "key_identity": "...",
    "profile_encryption_digest": "...",
    "provider_result": "..."
  },
  "created_at": "..."
}
```

Canonical JSON 标量规则：

- 所有 unsigned/signed 64-bit ID、计数、字节数和 generation 使用无前导零的十进制字符串；
- UUID 使用 lowercase canonical UUID string；
- TS、digest、root、checksum 和 canonical value bytes 使用固定 lowercase hex；
- 时间使用 UTC、固定 6 位小数的 RFC 3339 字符串；
- enum 使用 schema 中固定的大写字符串；
- 禁止 JSON 浮点数、`NaN/Infinity` 和依赖语言精度的 64-bit JSON number；
- 解析后必须拒绝数值不规范、大小写不规范和重复 JSON key。

这样 table ID 或 row count 超过 `2^53-1` 时，Go、Java、Rust 和 JavaScript
实现仍计算相同 RFC 8785 bytes/Manifest root。

每个 source Object：

```text
object ID
ObjectStats digest
blocks/physical rows
snapshot-visible rows
selected rows
```

每个 payload：

```text
ordinal
exact key/provider version
size/SHA-256
row count
first/last dataset row ordinal
row group count
row-group lifecycle min/max/null count
Parquet schema fingerprint
```

Manifest 不保存：

- credential/secret；
- presigned URL；
- RowID；
- fake PK（除非它是用户显式业务列）；
- 可变 Stage name作为存储身份。

### 14.1 传输和静态加密

首个 GA 不自研新的客户端数据加密格式，使用 provider server-side encryption，但
以下条件是 Binding/Profile 准入和每次 PUT/HEAD/readback 的硬条件：

```text
endpoint uses authenticated TLS
AND profile encryption mode in (provider-managed SSE, customer-managed KMS)
AND provider capability probe confirms requested mode
AND PUT response encryption identity matches Profile
AND Root/Dataset/Manifest encryption digest matches
```

Manifest `encryption` 固定保存：

```json
{
  "mode": "SSE_KMS",
  "key_identity": "...",
  "profile_encryption_digest": "...",
  "provider_result": "..."
}
```

规则：

- 不保存 secret、data key、credential 或 presigned URL；
- `key_identity` 是 canonical KMS ARN/key ID/等价 provider identity，不是别名的可变解析结果；
- 改成另一 KMS key identity 必须通过 `ADD VERSION` 创建新 Profile version；
- provider 在同一 key identity 下轮换材料时，必须保证历史对象仍可解密；
- 每个 Payload/Manifest 的 provider encryption result 进入 Root Object 诊断字段或 Manifest；
- GET/readback/Restore 必须校验冻结的 encryption digest，不能静默降级为无加密写；
- HTTP、跳过证书校验、无法确认服务端加密的 provider 配置不允许用于 GA Archive；
- 本地 Mixed/Restore spill 使用 CN 受控临时加密，密钥不持久化，文件由 attempt Owner 清理。

如果未来增加客户端 envelope encryption，必须升级 Manifest version、定义 data-key
生命周期和 Restore/Purge 协议，不能复用本节字段假装兼容。

Manifest 自身：

1. 先在内存中构造 canonical JSON；上限 16 MiB；
2. 计算 SHA-256/manifest root；
3. Root Object row ALLOCATED；
4. immutable PUT；
5. full readback canonical parse；
6. root相同后 Root `VERIFIED`。

Payload 数量使 Manifest 超过 16 MiB 时，child 在写 Payload 前就必须由预测上限阻止；不能写完后才发现 Manifest 过大。

## 15. 全量 readback

每个 Payload PUT complete 后：

```text
OpenExact
  -> read entire object to EOF
  -> verify size + SHA-256
  -> parse Parquet footer/schema
  -> decode every row in Manifest order
  -> recompute row count/min/max/null count/content root
```

必须满足：

```text
payload identity matches Root Object
archive_rows == selected source rows
archive_content_root == Reader source root
all payload ordinal ranges contiguous
all Parquet field IDs/types match Manifest
```

禁止只比较：

- ETag；
- HEAD size；
- Parquet footer；
- sample rows；
- provider checksum header。

readback 使用 Provider exact identity，不从 CN write buffer/cache返回未落远端数据。

## 16. Writer 与 Reader 失败协议

| 失败点 | Owner/动作 |
|---|---|
| Reader before Root | Attempt Control abort；无外部对象 |
| Root commit失败 | Reader/txn cancel；禁止 PUT |
| multipart create后 observer持久化失败 | uploader停止；Sweeper按 prefix列举/abort |
| producer失败 | ArchiveStore cancel uploader；Root DELETE_PENDING |
| uploader失败 | cancel producer；Root保留 multipart/object证据 |
| PUT response lost且同一writer epoch仍有效 | Root保持 UPLOADING；HEAD/LIST/readback reconcile |
| PUT期间worker/epoch丢失 | 旧Root清理；new attempt/new prefix，不原地接管 |
| readback失败 | Root DELETE_PENDING；不发布 Dataset |
| root mismatch | attempt终止 + P0告警；不复用 key |
| context cancel | first error驱动双方退出；join后返回 |
| panic | recover为 error；唯一 Owner执行收敛 |

ArchiveStore 返回前必须保证内部 goroutine 全部退出或 Owner 已持久转移给 Sweeper。

## 17. Schema evolution

Binding ACTIVE 时首个 GA拒绝 schema change，因此一个 Dataset 内只有一个 schema digest。

解除 Binding 后重新绑定：

- 新 Binding generation可使用新 schema；
- 新 Dataset保存新 schema；
- Restore 时间区间跨 schema generation时：
  - schema digest相同：允许；
  - 仅新增带可确定常量 default 的 nullable列：首个 GA仍拒绝跨 generation自动合并；
  - 其他：要求按 schema generation分别 Restore到不同目标表。

这样 Restore 不做隐式类型转换或丢列。

## 18. 内存和 I/O 上限

每个 Archive child：

```text
Reader Batch             <= 64 MiB
Parquet encoding buffers <= 128 MiB
Merkle accumulator       <= 64 levels
Manifest in memory       <= 16 MiB
in-flight multipart      <= 4 parts
multipart part           = 16 MiB
per I/O deadline         = 2 minutes
full child wall time     <= 2 hours
```

Writer 必须使用 memory semaphore；达到上限向 Reader callback施加背压，不继续拉取 Batch。

## 19. 测试要求

Reader unit：

- exact Object/Block顺序；
- 排除 in-memory rows；
- Snapshot Tombstone；
- Mixed cutoff；
- hidden RowID/fake PK projection；
- empty complete vs empty short read；
- callback error/panic/cancel；
- Batch reuse/double clean；
- oversize varlen row；
- checksum/Object missing。

Canonical/root：

- Batch size变化 root不变；
- Parquet file切分变化 root不变；
- NULL/Decimal/timezone/NaN/-0；
- column name变但 ID/schema digest处理正确；
- row顺序变化 root变化；
- empty root稳定；
- corruption单 bit可检测。

ArchiveStore：

- multipart identity在first part前持久化；
- response lost后HEAD/LIST收敛；
- producer/uploader互相失败无goroutine leak；
- late complete PUT；
- existing key identity mismatch不覆盖；
- full readback绕过write cache；
- provider LIST lag和Delete failure。

集成：

- 真实 S3/OSS/COS compatible provider；
- 512 MiB file与多文件 Dataset；
- 3 GiB source Object streaming；
- 1 TiB Archive长跑；
- CN kill在每个 Root/Object状态；
- readback后再退休，反向顺序测试必须失败。
