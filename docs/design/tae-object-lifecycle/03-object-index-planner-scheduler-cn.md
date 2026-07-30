# Object Discovery、Planner 与 Scheduler 详细设计

> 文件名为兼容已有链接保留 `object-index`，但本设计不再创建“每个 TAE Object
> 一行”的持久化 Catalog Object Index。
>
> 本文唯一负责候选发现、分页游标、可选派生 Summary、Whole/Mixed 分类、
> Dry-run、Job 切分、TaskService 接管和资源配额。

## 1. 设计结论

TAE 当前 Metadata 已经记录当前表的 Object 集合、ObjectStats、创建/删除时间和
可见性。Lifecycle 不复制一份新的权威 Object 目录。

首个 Commercial GA 使用：

```text
Binding Registry
  -> 当前 TAE PartitionState/ObjectStats（权威）
  -> 按表、按页读取 lifecycle column Metadata
  -> 持久化 table scan cursor/watermark（每表 O(1)）
  -> 持久化有界 Candidate/Child Job（只保存要执行的批次）
  -> Executor 在 source Snapshot 和 final transaction 再校验
```

明确不使用：

```text
mo_catalog 中每个 Object 一行的长期 Object Index
Logtail replay callback 同步维护 Lifecycle Index
GC metadata 作为 Lifecycle 当前对象权威
每天对集群所有表做 Reader 全表扫描
```

原因：

- 一百万个有效 Object 就会产生一百万行 Catalog 数据、索引、Checkpoint 和
  replay 成本；
- Merge 会持续执行 `A+B -> C`，逐 Object 派生行会形成高频删除、插入和 stale
  backlog；
- 多个 Planner/Indexer 会争用相同表/时间索引；
- 派生 Index 丢失并不应影响数据安全，却会被实现成新的恢复单点；
- TAE PartitionState 已经是当前 Object 身份和可见性的权威来源。

## 2. 现有能力与缺口

### 2.1 当前对象集合

现有：

```text
pkg/vm/engine/disttae/txn_table.go
  ForeachVisibleObjects
  GetNonAppendableObjectStats

pkg/vm/engine/disttae/object_list.go
  CollectObjectList

pkg/vm/engine/disttae/logtailreplay/object_list.go
  CollectObjectList
  CollectSnapshotObjectList
```

可以取得：

- Snapshot 下可见 Data/Tombstone Object；
- `ObjectStats`、Object ID、创建/删除时间；
- 从一个 TS watermark 到另一个 TS 的 Object 变化。

Lifecycle 只处理当前可见、持久化、非 appendable Data Object。Merge 已经
SoftDelete 的旧 Object 不进入候选；它们继续由现有 GC 管理。

### 2.2 生命周期列 Metadata

现有 `Relation.GetColumMetadataScanInfo` 能从 Object footer 读取任意列：

```text
Object name/location
row count
null count
origin/compressed size
column ZoneMap
level
create/delete TS
```

但当前接口一次构造全表结果，没有分页，也会为生命周期列不是物理 sort key 的
情况加载所有 Object footer。

GA 新增窄分页接口：

```go
type LifecycleMetaPageSpec struct {
    SnapshotTS       types.TS
    AfterObjectID    types.Objectid
    Limit            uint32
    LifecycleSeqnum  uint16
}

type LifecycleObjectMeta struct {
    Stats            objectio.ObjectStats
    CreateTS         types.TS
    DeleteTS         types.TS
    LifecycleZoneMap objectio.ZoneMap
    NullCount        uint64
    RowCount         uint64
    MetadataDigest   [32]byte
}

type LifecycleMetaPage struct {
    Objects      []LifecycleObjectMeta
    LastObjectID types.Objectid
    EndOfCycle   bool
}

func ScanLifecycleObjectMetadataPage(
    ctx context.Context,
    rel engine.Relation,
    spec LifecycleMetaPageSpec,
) (LifecycleMetaPage, error)
```

接口要求：

- 复用 PartitionState 的 Object-name ordered iterator；
- 只在当前 Snapshot 读取 `Limit` 个 Object；
- 只加载生命周期列所需 footer；
- 生命周期列就是实际 physical sort/cluster key 时直接复用 `ObjectStats.SortKeyZoneMap`；
- 不先构造全表 Slice/Batch 再截断；
- Object ID 顺序稳定，相同 Snapshot 下无重复；
- Merge 导致源 Object 消失时不报数据损坏，下一页/下一 cycle 以当前权威对象为准；
- footer missing、checksum 或类型错误返回错误，不猜测 Whole。

## 3. 持久化 Scan State

每个 Binding 一行：

```sql
CREATE TABLE mo_catalog.mo_lifecycle_scan_state (
    binding_id                 BINARY(16) NOT NULL,
    binding_generation        BIGINT UNSIGNED NOT NULL,
    physical_table_id         BIGINT UNSIGNED NOT NULL,
    schema_generation         BIGINT UNSIGNED NOT NULL,
    cycle_id                   BIGINT UNSIGNED NOT NULL,
    cycle_snapshot_ts         VARBINARY(16) NOT NULL,
    after_object_id            BINARY(16) NULL,
    cycle_started_at           TIMESTAMP(6) NOT NULL,
    cycle_completed_at         TIMESTAMP(6) NULL,
    collect_watermark_ts       VARBINARY(16) NOT NULL,
    scan_state                 TINYINT UNSIGNED NOT NULL,
    state_version              BIGINT UNSIGNED NOT NULL,
    next_scan_at               TIMESTAMP(6) NOT NULL,
    last_error_code            INT NULL,
    last_error                 VARCHAR(1024) NULL,
    updated_at                 TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (binding_id)
);
```

`scan_state`：

```text
IDLE
  -> SCANNING
  -> CYCLE_COMPLETE
  -> IDLE

任意状态
  -> REBUILD_REQUIRED
  -> SCANNING
```

这张表规模是绑定表数量，不是 Object 数量。1000 张绑定表约 1000 行。

CAS：

```text
binding_id
binding_generation
state_version
```

Binding generation、physical table 或 schema generation 变化时：

- 当前 cursor 作废；
- 已持久化但未 claim 的候选作废；
- 新 cycle 从空 cursor 开始；
- 正在 final transaction 的 child 由自身 generation CAS 决胜。

## 4. 分页 Cycle

### 4.1 开始

Scanner 创建短只读事务：

```text
S = txn.SnapshotTS
CAS scan_state:
  IDLE/CYCLE_COMPLETE -> SCANNING
  cycle_id += 1
  cycle_snapshot_ts = S
  after_object_id = NULL
```

每页使用新的短只读事务时，不要求继续读取历史 `S`。实现选择二者之一：

1. 整个 page 使用一个短事务，并将该页的 Snapshot 写入候选；
2. cycle 固定 `S`，但必须由现有 Snapshot Operator 明确读 `S`。

GA 推荐每页一个当前短 Snapshot，原因是 Discovery 只是 hint，不值得长期 pin
cycle Snapshot。`cycle_snapshot_ts` 只记录 cycle 起点用于观测，不作为 final
correctness。

### 4.2 Page CAS

每页：

```text
read current Binding/scan state
  -> scan at most page_limit objects after cursor
  -> classify metadata
  -> insert bounded candidates/jobs
  -> CAS after_object_id/state_version
```

Cursor 和本页 Candidate 插入放在同一个 Catalog 事务。事务失败：

- cursor 不前进；
- 重读该页；
- Candidate 使用 deterministic candidate key 去重。

Candidate key：

```text
SHA256(
  binding_id
  binding_generation
  physical_table_id
  source_object_id
  metadata_digest
  cutoff_bucket
)
```

### 4.3 Merge 期间的 cursor

Object iterator 按 Object ID 前进。可能发生：

```text
cursor = B
Merge A+B -> Z
```

若 `Z > B`，本 cycle 后续能看到；若 `Z <= B`，本 cycle 可能看不到。到达尾部后
cursor 归零，下一 cycle 一定重新枚举当前对象。

因此：

- 单 cycle 不承诺完整；
- `max_full_cycle_interval` 是发现延迟上限的一部分；
- final transaction 从不相信“本 cycle 没看到”代表对象不存在；
- cycle 不因 Merge 重启或回滚。

### 4.4 Watermark 增量

`CollectObjectList(from, to)` 可用于优先发现新建/新删除 Object，减少刚完成 Merge
的新 Object 等到下一 full cycle 的时间：

```text
from = collect_watermark_ts.Next()
to   = current short txn SnapshotTS
```

增量结果只进入内存优先队列或有界 Candidate 表。它不能替代 full cycle：

- 未发生物理变化的 Object 会随 cutoff 推进而从 not-due 变成 due；
- 增量日志不能回答任意生命周期列的未来到期时间；
- checkpoint/logtail gap 时直接回到 full cycle，不回放自建日志。

## 5. Candidate 是有界工作，不是 Object Index

Candidate/Child Job 只保存已经进入近期执行窗口的数据：

```text
candidate_id
binding/generation
source object ID/stats/digest
metadata snapshot TS
lifecycle ZoneMap digest
classification
estimated source/expired/live bytes
cutoff/evaluation time
next_action_at
state/version
```

硬约束：

```text
candidate rows per table        <= 10,000
candidate rows per account      <= 100,000
candidate rows cluster-wide     <= release profile
candidate age                   <= 7 days unless BLOCKED
```

达到上限：

- Scanner 停止为该表生成新 Candidate；
- cursor 可以暂停，不能丢弃已插入 work；
- Cleanup/Reconcile/Restore 不受影响；
- 指标和 SHOW 显示 `DISCOVERY_BACKPRESSURE`。

Candidate 完成、stale 或 replan 后保留短审计期再物理清理。它不永久记录所有 Object。

## 6. 可选 Packed Discovery Summary

如果 1000 张绑定表的真实 footer I/O 证明分页 cycle 仍不满足成本目标，可以启用
派生 Summary；它不是 GA 正确性前提。

格式：

```text
one immutable packed segment
  -> table/binding generation
  -> source watermark
  -> sorted records:
       object ID
       object stats digest
       lifecycle min/max/null count
       next estimated action time
```

规则：

- Segment 存在共享 Object Storage，不在 `mo_catalog` 每 Object 写行；
- Catalog 每表只保存 Root/version/watermark；
- 增量变化写新的 immutable delta segment；
- 后台 compaction 只合并 Summary，不修改 TAE Object；
- Summary 丢失、损坏或落后时回退分页扫描；
- Executor 和 final transaction不读取 Summary 作安全判断；
- Summary 文件由 Lifecycle Cleanup Root 管理，不交给 TAE GC；
- 不进入 Logtail replay callback；
- 不允许 Summary 更新阻塞普通 Merge/DML。

它解决的是 footer I/O，不是“记录所有当前对象”的新权威目录。

## 7. Metadata 分类

令：

```text
cutoff = evaluation_time_utc - policy_duration
```

支持类型使用 MO 内部 canonical encoding 比较，不做字符串或时区本地比较。

### 7.1 Whole

只有全部满足才分类 Whole：

```text
ZoneMap initialized
AND type/version supported
AND max is not truncated
AND null_count == 0
AND physical row_count > 0
AND max_value < cutoff
```

`max == cutoff` 不到期。

Whole 是 Planner hint。Executor 仍在 source Snapshot 复核，final transaction 仍校验
exact Object/generation。

### 7.2 Not due

```text
min_value >= cutoff
```

这表示该 Object 当前没有可由 ZoneMap 判定到期的物理行。下一次检查时间可以用
`min_value + duration` 估算，但：

- 最长 24 小时必须重新确认一次；
- Binding/schema 变化立即失效；
- 估算不形成数据安全承诺。

### 7.3 Mixed candidate

以下任一成立：

```text
min < cutoff <= max
ZoneMap unknown/truncated
null count or row count inconsistent
metadata version unsupported
```

Executor exact scan 后计算真实：

```text
expired_visible_rows
live_visible_rows
expired/live bytes
affected blocks
transfer map estimate
```

再选择：

```text
expired == 0
  -> STALE/NOT_DUE

live == 0
  -> Whole path

small delete budgets all pass
  -> SMALL_MIXED_DELETE

otherwise rewrite budgets pass
  -> MIXED_REWRITE

otherwise
  -> RESOURCE_BLOCKED
```

`MIXED_LAYOUT_BLOCKED` 不再表示“功能永久实现不了”，只用于布局/成本超出当前 release
profile；大 Mixed 正常优先走 Lifecycle Rewrite。

### 7.4 Empty visible Object

Metadata row count大于零但 Snapshot 下 visible rows 可能为零。只有 Exact Reader
完整到 EOF 的 `ScanReport` 能证明：

```text
requested objects/blocks == reached objects/blocks
AND snapshot_visible_rows == 0
AND complete == true
```

Discovery 不能把 Tombstone 估算当成空对象证明。

## 8. Layout Audit

Dry-run 和绑定前 audit 输出：

```text
whole_due_bytes / due_bytes
mixed_due_bytes / due_bytes
expired/live ratio histogram
lifecycle column == physical sort/cluster key
estimated daily rewrite bytes
estimated daily archive bytes
estimated normal DELETE/tombstone bytes
```

产品分级：

| 布局 | 功能 | Lag/成本承诺 |
|---|---|---|
| lifecycle 列与 physical sort/cluster key 对齐 | 全部支持 | 可认证日级 Archive Lag |
| 时间基本有序但未声明为 sort key | 全部支持 | 按实测认证 |
| 高度乱序 | Rewrite 保证安全，资源超限 fail closed | 不承诺严格 Lag SLO |

不能声称一次 Rewrite 会永久修复乱序布局。普通 Merge 仍按表的物理 sort key 工作，
未来可能重新形成 Mixed。首个 GA 不修改普通 Merge 的分组/排序算法；长期降低 Rewrite
成本可另行增加 lifecycle-aware physical layout optimization，但不能成为正确性依赖。

## 9. Job 切分

一次 policy scan：

```text
one coordinator
  -> N independent child jobs
  -> each child one final transaction
  -> Archive child one Dataset
```

### 9.1 Whole child

允许合并多个 Whole Object，但同时满足：

```text
source objects          <= 64
source compressed bytes <= 8 GiB
expected output objects <= 32
final wire bytes        <= 512 KiB excluding external transfer map
```

TTL Whole 可只做 Metadata/final transaction。Archive Whole 使用 streaming Reader。

### 9.2 Small Mixed child

一个 child 只处理一个 source Object，使用普通可写 SI DELETE，并受
[06-mixed-delete-transaction-cn.md](06-mixed-delete-transaction-cn.md) 全部预算约束。

### 9.3 Rewrite child

一个 child **只处理一个** source Object：

```text
source objects          == 1
source compressed bytes <= current legal Object hard limit
source rows             <= release profile
created live objects    < api.NoTransfer (255)
transfer bytes          <= release profile
attempt wall time       <= 60 minutes
```

单源是首个 GA 的协议不变量，不是可调 soft limit。它把 dense transfer slab、
Tombstone delta、失败范围和 Root staging 上限约束在一个当前合法 Object 内。单个
合法 TAE Object 即使超过普通 child 目标也必须支持 streaming；不能要求把 source
Object 再拆成更小原子对象，也不能通过把多个 Object 合并成一个 Rewrite child
提高吞吐。吞吐由独立 child 调度和认证后的 Rewrite 并发控制。

## 10. Scheduler

### 10.1 绑定表 Registry

Scheduler 查询：

```text
Binding state = ACTIVE
AND next_scan_at/job next_action_at <= now
```

不枚举未绑定表，不调用集群全表 Catalog scan。

### 10.2 初始并发

```text
per table    child = 1
per database child = 2
per account  child = 4
cluster      child = 8
```

子类额外上限：

```text
active small Mixed SI txn
active Rewrite: per table 1, cluster 1 default, 4 certified hard max
active source protection
active Provider PUT/readback
active Restore/Purge/Cleanup
```

Cleanup、Reconcile 和 Restore 保留独立最低并发，不能被 Archive backlog 饿死。

### 10.3 TaskService 与 Catalog lease

TaskService 只负责投递。执行权由 Catalog：

```text
attempt_id
executor_epoch
lease_expire_at
state_version
```

CAS 决定。旧 executor 不能凭 TaskService 状态发布 Dataset、续 source reservation
或删除 Payload。

## 11. Admission Budgets

在 claim 前检查：

```text
candidate/job backlog
active reader/rewrite bytes
source protection count/age
TAE staging object bytes/count
archive staging bytes/count
Rewrite physical slots/dense transfer reservation
max certified decoded Block estimate and task peak memory token
external booking bytes/pages
Tombstone delta rows/bytes/blocks
Provider request/egress budget
small Mixed tombstone rolling bytes
Merge/Tombstone/Vacuum backlog
GC lag
```

Soft limit：

- 降低 page、child bytes 或并发；
- 延后 next action；
- 不影响正常 DML。

Hard limit：

- 不 claim 新 child；
- pre-final child 安全 cancel；
- 已进入 `FINALIZING/FINAL_RETRYABLE/COMMIT_UNKNOWN` 的事务只做对账或受限 final retry；
- Cleanup/Reconcile 保留资源。

所有积累项必须有 finite release-profile 值；`0 = unlimited` 非法。

## 12. 与普通 Merge 的交互

Discovery 不跟踪 Merge 已删除旧 Object，也不等待 GC。

```text
Object A+B -> C

TAE 当前 Metadata:
  A/B deleted or not visible
  C current

Lifecycle:
  stale Candidate(A/B) executor recheck失败后丢弃
  next page/cycle发现 C
```

只有 Executor claim 的 exact source set 与 Merge 有窄协调：

- Planner 不 reservation；
- Candidate 排队不 reservation；
- Executor 开始实际 Reader/Rewrite 前向 TN claim；
- claim 期间普通 Merge 跳过 exact Object；
- 已在运行的 Merge 与 claim 由 TN admission 线性化；
- reservation 丢失不构成错误提交依据，final exact CAS 仍是安全边界。

详细协议见
[05-strict-object-retire-protocol-cn.md](05-strict-object-retire-protocol-cn.md)。

## 13. Blocked 与终止

### 13.1 `RESOURCE_BLOCKED`

原因：

- Rewrite single-source/live/dense-transfer/external-booking/delta/staging超过硬限额；
- Provider 预算不足；
- source protection 无法取得或续租；
- 单对象超过已认证 streaming 边界。

动作：

- 不删除源行；
- 记录 measured cost；
- 指数退避到上限；
- 达到 max blocked age 后停止自动重试，等待配置或数据布局变化。

### 13.2 `MIXED_LAYOUT_BLOCKED`

只用于：

- 生命周期列高度乱序导致持续 Rewrite 成本超过 release profile；
- repeated rewrite amplification 超过表级预算；
- 当前产品限制无法处理该类型/布局。

它是 fail-closed，不代表 archived。

判断不依赖逐Object Index，而读取02定义的每Binding固定大小runtime stats：

```text
rewrite_amplification =
  attempted_source_bytes
  / max(committed_retired_expired_bytes, 1)
```

`attempted`包含成功、失败、conflict和abort的Rewrite source bytes，防止失败任务不计
成本后无限重试；分母只计final transaction已commit的退休到期字节。Scheduler同时看
24h/7d rolling source/retired/aborted read/write bytes和consecutive blocked count。
hour/day bucket ring大小固定，重启后从Catalog恢复，不因进程重启清零。默认阈值必须
在1/10 TiB认证后冻结，超过后终止当前child generation并等待显式`RECHECK`或窗口
自然滚动，不能继续占用Rewrite队列。

### 13.3 `CONFLICT_BLOCKED`

同一 source set 持续发生：

- reservation conflict；
- exact Object 被 Merge 替换；
- post-snapshot Tombstone 命中已归档行；
- Guard/schema/owner generation 变化。

达到 conflict age/attempt 上限后终止当前 child generation，下一次 Scanner 从当前
TAE Metadata 重新规划。

## 14. 测试要求

### 14.1 Discovery

- 0/1/百万 Object 的分页，不构造全量结果；
- cursor page commit 前/后 crash；
- Merge 在 cursor 前后生成新 Object；
- Object ID 位于 cursor 前，下一 cycle 必须发现；
- `CollectObjectList` gap 回退 full cycle；
- lifecycle 列是/不是 sort key；
- footer missing、truncated ZoneMap、NULL/inconsistent count；
- Candidate hard limit 和公平性；
- 重启只恢复 scan state/job，不 replay 每 Object Index。

### 14.2 分类

- `max < cutoff` Whole；
- `max == cutoff` not Whole；
- min/max 跨 cutoff；
- Exact scan 将 metadata Mixed 修正为 Whole/empty/not-due；
- tiny Mixed 选 DELETE；
- medium/large Mixed 选 Rewrite；
- Rewrite 超预算 `RESOURCE_BLOCKED`。
- 单Block metadata estimate未知/溢出/超过认证上限时，payload读取前
  `LIFECYCLE_OVERSIZE_UNSUPPORTED -> RESOURCE_BLOCKED`；
- 24h/7d rewrite amplification超阈值时`MIXED_LAYOUT_BLOCKED`，重启不能清零。

### 14.3 Merge/GC

- A/B Candidate 排队后 Merge 为 C；
- Reader claim 前/中/后普通 Merge；
- stale Candidate 永不 retire C 或消失对象；
- GC 删除 A/B 不触发 Lifecycle 自建清理；
- 未绑定表的 Merge/GC 路径无 Lifecycle Catalog 写；
- reservation map 为空时性能无可测回归。

## 15. 决策记录

1. 当前 TAE Metadata 是 Object 权威，GC metadata 不是当前查询目录。
2. 不创建每 Object Catalog 行，避免 replay、热点和 stale backlog。
3. 每表 O(1) cursor + 有界 Candidate 满足恢复和调度。
4. Full cycle 负责最终发现，`CollectObjectList` 只做增量加速。
5. Packed Summary 是可重建性能优化，不是 GA 正确性依赖。
6. Lifecycle Rewrite 解决大 Mixed 的退休；它不自动保证未来物理时间局部性。
7. 普通 Merge 只感知正在执行的 exact reservation，不感知 Policy/Scanner。
8. Rewrite成本使用每Binding固定bucket ring统计，不建立逐Object Catalog Index，
   也不更新权威Binding CAS行。
