# Object Index、Planner 与 Scheduler 详细设计

> 本文唯一负责派生 Object Index、候选分类、Dry-run、Job 切分、调度、TaskService 接管和硬配额。

## 1. 设计结论

Lifecycle 不新增权威 Object 目录，也不复用 GC metadata 作为当前数据目录。

复用现有能力：

- `Relation.CollectObjectList(ctx, from, to, bat, mp)`：
  - `from` 为空时，从 PartitionState 列举 `to` Snapshot 的当前可见 Object；
  - `from/to` 非空时，从当前 PartitionState 或历史 Checkpoint 返回 create/delete delta；
- `objectio.ObjectStats`：
  - Object ID、rows、blocks、compressed/origin bytes；
- `objectio.FastLoadObjectMeta`：
  - 按 exact Object location 读取生命周期列 footer/ZoneMap/null count；
- 当前 Relation/PartitionState：
  - Executor 和 final transaction 的权威 source Object 状态。

新增 Object Index 只回答：

```text
“哪个绑定表的哪些当前 Object，最值得在什么时间重新检查？”
```

它不回答：

```text
“这个 Object 现在一定可以删除吗？”
```

## 2. 为什么不直接每天全表 Reader

假设目标 Object 约 128 MiB：

```text
1 TiB table  ~= 8,192 objects
10 TiB table ~= 81,920 objects
1000 x 1 TiB ~= 8.2 million objects
```

每天对所有绑定表执行全表 Reader 会：

- 读取所有 payload，而不是只读取到期边界；
- 重复应用 Tombstone；
- 消耗 CN cache、GET、网络和 CPU；
- 与普通查询争用；
- 让“没有新数据到期”的表仍产生同等成本。

每天调用 `GetColumMetadataScanInfo` 扫全部 footer 也不理想。Object Index 将 footer 成本移到：

- 首次 Binding backfill；
- 新 Object create delta；
- stats/version 变化后的精确 refresh；
- 显式 `RECHECK DRY RUN`。

## 3. Indexer 代码边界

新增：

```text
pkg/lifecycle/objectindex/
  index.go
  backfill.go
  catchup.go
  footer.go
  classify.go
  reconcile.go
  limits.go
```

现有 disttae 增加一个窄 helper：

```go
type LifecycleObjectMeta struct {
    Stats          objectio.ObjectStats
    CreateTS       types.TS
    DeleteTS       types.TS
    ColumnZoneMap  objectio.ZoneMap
    ColumnNulls    uint64
    ColumnType     types.Type
}

func LoadExactObjectColumnMeta(
    ctx context.Context,
    fs fileservice.FileService,
    stats objectio.ObjectStats,
    columnSeqnum uint16,
) (LifecycleObjectMeta, error)
```

实现位置建议：

```text
pkg/vm/engine/disttae/lifecycle_object_meta.go
```

该 helper：

- 只读调用方给定 Object；
- 使用 `stats.Location()` + `FastLoadObjectMeta`；
- 验证 footer object name、rows、blocks 与 ObjectStats 一致；
- 返回 lifecycle column 的 Object-level ZoneMap/null count；
- 不枚举整张表；
- 不写 PartitionState；
- 不缓存 Lifecycle 自定义状态。

现有 `GetColumMetadataScanInfo` 保持行为不变。

## 4. Index generation 协议

### 4.1 创建

Binding DDL 提交后：

```text
binding.state = ENABLING
binding.index_generation = previous + 1
  -> create INDEX_BACKFILL Job Control
```

Backfill Job 在一个只读事务中取得：

```text
physical table identity
schema generation/version/digest
lifecycle column seqnum/type
snapshot_ts = txn.SnapshotTS()
```

然后调用：

```go
rel.CollectObjectList(ctx, types.TS{}, snapshotTS, batch, mp)
```

只保留：

- `is_tombstone = false`；
- `delete_at` 为空；
- physical table identity 匹配；
- ObjectStats 可解析。

### 4.2 分页

`CollectObjectList` 当前返回 Batch，不提供 server-side page token。首个 GA 不允许单个调用无界增长，因此 P0 要新增分页 wrapper：

```go
type ObjectListCursor struct {
    SnapshotTS types.TS
    LastObject objectio.ObjectNameShort
}

func CollectSnapshotDataObjectsPage(
    ctx context.Context,
    rel engine.Relation,
    snapshotTS types.TS,
    after objectio.ObjectNameShort,
    maxObjects int,
    maxBytes int64,
) (objects []objectio.ObjectEntry, next ObjectListCursor, done bool, err error)
```

实现复用 PartitionState 的 ordered object iterator；历史 Snapshot 无法直接分页时，内部仍按 object ID 过滤和停止，不能先构造全量 Batch。

初始上限：

```text
max_objects_per_page = 1,000
max_encoded_page     = 16 MiB
max_footer_workers   = 4 per table
```

footer 结果按 object ID 排序后，以普通 Catalog transaction 每批最多 500 行写 Index。

### 4.3 Backfill 和增量并发

Backfill 冻结 `snapshot_ts = W0`。增量 catchup 使用：

```text
last_applied = W0
target = new read transaction SnapshotTS
CollectObjectList(last_applied.Next(), target)
```

因为现有区间语义是闭区间 `[from,to]`，必须使用 `Next()` 防止重复边界。

对每条 delta：

- create：
  - 精确加载 footer；
  - upsert 仅允许相同 object ID + stats digest；
  - digest 不同视为 generation/state conflict，不覆盖；
- delete：
  - CAS 当前 Index row `CURRENT/CLAIMED -> DELETED_HINT`；
  - row 不存在允许记录 bounded missing-delta metric，不创建假 Object；
- create+delete 同窗口：
  - 按 create/delete TS 排序；
  - 最终状态为 `DELETED_HINT`。

catchup 连续追到：

```text
target - last_applied <= 5 seconds
AND no uncommitted page
```

才允许发布 READY。

### 4.4 READY CAS

system Job Control 和 tenant Binding 分两步收敛：

1. system Job Control 标记 index generation `CATCHUP_COMPLETE(Wn)`；
2. tenant transaction重新读取 Guard/Binding/table：
   - Binding 仍 `ENABLING`；
   - binding/index/schema generation 未变；
   - physical table ID 未变；
3. CAS Binding：

```text
state = ACTIVE
index_generation = G
index_watermark = Wn
version + 1
```

4. system Job Control 标记 `READY_ACKED`。

如果第 2 步失败，G 永远不能用于调度，Index GC 分页删除它。

### 4.5 重启与断档

Indexer 重启时只扫描：

```text
task_kind in (INDEX_BACKFILL, INDEX_CATCHUP)
AND state not terminal
AND next_action_at <= now
LIMIT 1000
```

接管：

```text
lease expired
  -> executor_epoch + 1
  -> resume from persisted object cursor/watermark
```

如果 `CollectObjectList` 返回 stale read 且所需 Checkpoint 已不可用：

- 当前 generation 进入 `REBUILD_REQUIRED`；
- 创建新 generation 和新 W0；
- 旧 generation不参与调度；
- 不尝试猜测缺失 delta。

### 4.6 不接 Logtail callback

首个 GA 明确不在普通 Logtail replay callback 中同步写 Object Index，原因：

- 所有 CN 都可能消费同一表 Logtail，容易重复写和热点；
- replay 与 Lifecycle Catalog transaction 会形成新的恢复耦合；
- 未绑定表不应付出判断/写入成本；
- Index 不是实时正确性条件。

使用有水位轮询：

```text
active Binding index catchup every 5 minutes
due policy scan every 60 minutes
```

用户可触发 `RECHECK`，但仍受配额。

## 5. Index 一致性规则

Index 行写入要求：

```text
index physical_table_id == Binding physical_table_id
index schema_generation == Binding schema_generation
object stats ID == row object_id
footer object ID == row object_id
footer rows/blocks == ObjectStats rows/blocks
ZoneMap type == lifecycle column type
null_count <= row_count
```

不满足：

- 行标记 `METADATA_INVALID`；
- 不分类 Whole；
- Binding error count/bytes 累积；
- 超过表级阈值后暂停调度并告警。

Index 缺失或 stale 最多导致延迟处理，不能导致错误退休。

## 6. 候选分类

### 6.1 Canonical cutoff encoding

Planner 将 `cutoff` 编码为生命周期列内部类型的 canonical bytes，再与 ZoneMap 比较。禁止把值格式化成字符串再比较。

### 6.2 Whole

```text
row_count > 0
AND lifecycle_null_count == 0
AND zonemap_status == EXACT_SUPPORTED
AND max_value < cutoff
```

ObjectStats 的 sort-key ZoneMap 只在生命周期列确实是该 sort key 且 type/version 匹配时可用。其他列必须读取 footer column ZoneMap。

### 6.3 Not due

```text
zonemap exact
AND min_value >= cutoff
```

`next_action_at` 可按 `min_value + duration` 计算；上限不超过 24 小时重新确认一次，防止 Binding/schema 变化长期不被观察。

### 6.4 Mixed candidate

```text
min_value < cutoff <= max_value
OR zonemap missing/uninitialized/truncated/unsupported
OR metadata inconsistency
```

Mixed candidate 不直接决定 DELETE。Executor 读取 exact Object 后计算：

```text
physical_rows
visible_rows
expired_visible_rows
expired_ratio
delete_key_bytes
affected_blocks
estimated_tombstone_bytes
```

再做硬预算准入。

### 6.5 Empty visible Object

Metadata row_count > 0 但 Snapshot 下 visible rows 可能为 0。只有 Exact Reader 的完整 `ScanReport` 能证明 empty；Index 不能把 Tombstone 估算当成空对象证明。

## 7. Planner

新增：

```text
pkg/lifecycle/coordinator/
  planner.go
  candidate.go
  dryrun.go
  grouping.go
  cost.go
  compatibility.go
```

输入：

```go
type PlanRequest struct {
    BindingID          uuid.UUID
    BindingGeneration uint64
    EvaluationTimeUTC time.Time
    MaxObjects         int
    MaxSourceBytes     int64
}
```

输出：

```go
type ChildPlan struct {
    ChildGeneration     uint64
    Mode                ChildMode
    EvaluationTimeUTC   time.Time
    Cutoff              types.Datetime
    PhysicalTableID     uint64
    SchemaGeneration    uint64
    Objects             []ObjectCandidate
    SourceObjectDigest  [32]byte
    EstimatedRows       uint64
    EstimatedBytes      uint64
    Reason              string
}
```

`ChildMode`：

```text
WHOLE_TTL
WHOLE_ARCHIVE
MIXED_TTL
MIXED_ARCHIVE
MIXED_PROBE
```

Planner 不打开 payload Reader。它只使用 Binding、READY Index 和预算。

## 8. Job 切分

### 8.1 一次 policy scan

```text
one POLICY_SCAN coordinator
  -> page due Index rows for one Binding
  -> create bounded child plans
  -> atomically persist child Job Control rows
  -> advance binding.next_scan_at
```

一分区概念不参与。一个 child 对应一个原子退休事务。

### 8.2 Whole child

初始 release profile：

```text
max source objects       = 64
max compressed bytes     = 8 GiB
max estimated rows       = 100 million
max strict entry bytes   = 512 KiB
max Archive payload      = 16 GiB
```

Grouping：

- 按 lifecycle max、object ID 排序；
- 只组合同一 physical table/schema generation；
- 不跨 Binding；
- 不跨 retention/cutoff；
- source Object digest 冻结有序 object ID + stats digest。

### 8.3 Mixed child

首个 GA：

```text
exactly one source Object
```

ObjectStats 即使显示 3 GiB 也可用 streaming Reader 做 probe，但只有满足 Mixed 事务预算才允许 DELETE。否则进入 `MIXED_LAYOUT_BLOCKED`。

### 8.4 Source set 改变

如果 Executor 权威检查发现：

- Object missing；
- stats digest 变化；
- physical/schema generation 变化；
- Merge 已替换 Object；

旧 child 终结为 retryable conflict，Planner 创建：

```text
child_generation + 1
new attempt ID
new source digest
```

不能修改旧 child 的 source list 后继续使用原 Dataset/Root。

## 9. Dry-run

`RECHECK ... DRY RUN` 输出：

| 字段 | 含义 |
|---|---|
| binding/index generation | 数据新鲜度 |
| index watermark/lag | Object delta 延迟 |
| active bytes/rows | 当前表规模 |
| whole due objects/bytes | 可低成本退休 |
| mixed objects/bytes | 需要 payload probe |
| estimated expired ratio | 基于 ZoneMap/采样的估计，明确标为 estimate |
| blocked reason | 依赖、metadata、预算、capability |
| estimated Archive bytes | 使用该表历史实际压缩比；无历史时给区间 |
| PUT/GET/readback/delete requests | Provider 请求成本 |
| active TAE bytes removable | 退休成功后的估计 |
| retained/merge/tombstone headroom | 当前安全余量 |
| expected first action time | 调度时间，不是完成 SLA |

Dry-run 不能宣称：

- Mixed 精确到期行数，除非显式 bounded probe；
- Provider 账单一定下降；
- Archive 压缩率一定优于 TAE；
- cutoff 到点即完成。

## 10. Scheduler

新增：

```text
pkg/lifecycle/coordinator/
  scheduler.go
  fair_queue.go
  admission.go
  task_dispatch.go
  leases.go
  quotas.go
```

### 10.1 单个 coordinator

集群只有一个逻辑 Lifecycle Coordinator lease：

- TaskService cron 每分钟唤醒；
- system Job Control/Catalog lease 决定唯一 owner；
- 不为 1000 张表创建 1000 个 cron task；
- 每轮从 Binding Registry 按 `next_scan_at` 分页 1000 行；
- 使用 account/database/table fair queue。

### 10.2 TaskService 边界

TaskService：

- 投递 generic child task；
- worker heartbeat；
- worker crash 后重新分配；
- `Allocate` epoch 作为额外观察值。

TaskService 不负责：

- Provider PUT/Delete 幂等；
- final transaction fencing；
- Catalog state truth；
- Root ownership；
- commit unknown 判定。

Worker 启动第一步必须 CAS Job Control：

```text
state is runnable
AND lease expired/unowned
AND executor_epoch expected
  -> lease_owner_cn = current CN
  -> executor_epoch + 1
  -> lease_expire_at = now + 60s
```

每 20 秒 heartbeat。连续 60 秒未续约后可以重新调度，但 Archive
`REGISTERED/UPLOADING` 不能在同一attempt原地接管：旧Root进入清理，新worker使用
new attempt/new prefix。只有完整`VERIFIED`后才允许新epoch接手finalize；
`FINALIZING`只做原txn对账。Provider单次I/O deadline不超过2分钟，旧executor的
immutable prefix由旧Root和quiescence协议收敛。

### 10.3 初始并发

| Scope | active child |
|---|---:|
| table | 1 |
| database | 2 |
| account | 4 |
| cluster | 8 |

独立保底槽：

```text
cleanup/reconcile >= 2 of cluster workers
restore           <= 2
mixed writable SI <= 2
archive upload    <= 4
```

Cleanup/Reconcile 不得因 Archive backlog 饿死。

## 11. Admission budgets

初始硬限制：

| 项 | 表级 | 账户级 | 集群级 |
|---|---:|---:|---:|
| active child | 1 | 4 | 8 |
| active Mixed SI | 1 | 2 | 2 |
| active Reader source bytes | 8 GiB | 32 GiB | 64 GiB |
| active upload bytes | 16 GiB | 64 GiB | 128 GiB |
| Job backlog rows | 10,000 | 100,000 | 1,000,000 |
| Object Index rows | table actual objects × 1.2 | 10 million | 100 million |
| obsolete Index rows | current rows × 0.2 | 2 million | 20 million |
| COMMIT_UNKNOWN | 1 | 4 | 16 |
| staging external objects | 10,000 | 100,000 | 1,000,000 |

这些是首个 release safety profile，不是性能目标。认证可调低；调高必须重跑容量/故障门禁。

### 11.1 Retained bytes admission

现有普通 Reader/SI transaction 可能间接阻止 GC watermark。每个 child 记录：

```text
snapshot age
source Object bytes
table checkpoint/GC lag
snapshot-exclusive retained bytes（能观测时）
```

策略：

```text
soft limit:
  reduce new child bytes/concurrency

hard limit:
  cancel pre-final Reader/Mixed transaction
  wait explicit rollback
  clean staging through Root

COMMIT_UNKNOWN:
  do not cancel ownership
  pause new retirement for scope
```

### 11.2 Backlog admission

任何下游积压达到 hard limit：

- 不创建新的同类 Job；
- 已在 final transaction 的 Job继续对账；
- Cleanup/Purge 保留资源；
- Binding 显示 `PAUSED_BY_QUOTA` 原因但不改变用户配置；
- 低于 low watermark 后自动恢复。

## 12. Fairness

Scheduler 使用分层 deficit round-robin：

```text
cluster
  -> account
    -> database
      -> table
```

cost 单位：

```text
max(
  source_bytes / 128 MiB,
  estimated_provider_requests / 16,
  estimated_tombstone_bytes / 1 MiB
)
```

一个 10 TiB 表不能长期占满 8 个集群槽。相同 scope 内：

1. Cleanup/Reconcile；
2. Restore；
3. TTL；
4. Archive；
5. Index Backfill；
6. Dry-run。

用户手工 `RECHECK` 提高可见优先级，但不能绕过硬配额。

## 13. Blocked 与终止

### 13.1 `MIXED_LAYOUT_BLOCKED`

触发：

- expired rows/ratio 超预算；
- delete key bytes 超预算；
- source Object/transaction duration 超预算；
- affected blocks/tombstone backlog 超预算。

动作：

- 不 DELETE；
- Archive staging 未发布则 Root 进入 `DELETE_PENDING`；
- child generation 终态；
- Index row 回到 CURRENT 并记录 blocked reason；
- 只有 Binding change、显式 RECHECK 或布局变化后创建新 generation。

### 13.2 `CONFLICT_BLOCKED`

触发：

```text
same logical due range conflicts for >= 24 hours
OR 8 final conflicts
OR 3 complete re-exports
```

动作同样 fail closed。该状态不承诺 lag SLO。

### 13.3 Provider/metadata error

- retryable Provider error：指数退避 1m、5m、30m、2h，最多 8 次；
- checksum/root mismatch：不自动覆盖同 key，终止 attempt 并 P0 告警；
- metadata invalid：不分类 Whole，最多 bounded Mixed probe；
- Catalog invariant failure：cluster retirement kill switch。

## 14. 普通 Merge 的交互

Indexer/Planner 不 reservation 普通 Merge source set。

```text
Index says Object A current
  -> Merge commits A -> B
  -> Index delta later marks A deleted/adds B
```

在 delta 到来前：

- Planner 可能创建包含 A 的 child；
- Executor 权威检查发现 A missing/changed；
- child abort/replan；
- Lifecycle 不读取 GC 已删除的 A，不延长其所有权；
- existing GC 正常回收 A。

可选 reservation 只能是减少重复导出的 hint，不能让 Merge 等待，也不进入首个 GA correctness。

## 15. 测试要求

单元测试：

- backfill + concurrent create/delete delta；
- inclusive TS 边界和 `Next()`；
- stale Checkpoint 触发新 generation；
- footer mismatch 不分类 Whole；
- unknown ZoneMap 只进入 Mixed；
- 1000-page cursor crash/resume；
- old executor 不能 READY 新 generation；
- Job grouping 不突破 object/bytes/entry limits；
- fair queue 和 cleanup minimum share；
- quota high/low watermark；
- TaskService duplicate delivery只产生一个 Catalog lease owner。

集成测试：

- CN 重启后从 Catalog watermark 恢复；
- 普通 Merge 高频替换 Object，Index 最终收敛且无错误 retire；
- 1000 Binding 不扫描未绑定表；
- 10 TiB metadata 模型下单轮内存、SQL rows 和 page time 有界；
- 删除 Index generation 后可以完全重建且不影响活动数据。
