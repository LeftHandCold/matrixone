# 可观测性、容量与运维详细设计

> 本文唯一负责配置、指标、SHOW、日志、告警、kill switch、成本模型、Runbook和放量运维。

## 1. 运维目标

运维人员必须在不查询Provider原始bucket、不读取源码的情况下回答：

- 哪些表绑定了Lifecycle；
- 哪些数据即将/已经到期；
- Whole/Mixed比例和blocked原因；
- 每个Job处于哪一步、谁拥有、何时超时；
- 有没有final transaction结果未知；
- staging/published/Purge对象由哪个Root负责；
- Provider上传、readback、Restore和Delete是否收敛；
- Tombstone、Merge、GC和retained bytes是否接近上限；
- Archive是否真的节省总成本；
- 什么开关可以安全停止扩大故障；
- 如何恢复而不误删数据。

## 2. 配置

配置统一放在：

```text
[lifecycle]
enabled = true
retirement-enabled = false
archive-upload-enabled = true
restore-enabled = true
purge-enabled = true
cleanup-enabled = true
reconcile-enabled = true
```

GA发布时 `retirement-enabled` 只有通过cluster capability和stage gate后才打开。

主要release profile：

```text
max-bound-tables                  = 1000
policy-scan-interval              = 1h
index-catchup-interval            = 5m
coordinator-page-size             = 1000
table-concurrency                 = 1
database-concurrency              = 2
account-concurrency               = 4
cluster-concurrency               = 8
cleanup-min-workers               = 2
small-mixed-cluster-concurrency   = 2
rewrite-cluster-concurrency       = 1
rewrite-certified-hard-concurrency = 4
max-certified-block-read-bytes    = 256MiB
restore-cluster-concurrency       = 2
provider-io-timeout               = 2m
final-lock-timeout                = 30s
final-txn-timeout                 = 60s
automatic-reconcile-age           = 24h
manual-reconcile-probe-interval   = 1h
root-quiescence-window            = 24h
```

数值与各协议文档的hard limit一致。启动时校验：

- hard >= soft；
- timeout/lease/quiescence关系合法；
- cleanup/reconcile不能同时关闭并开启retirement；
- retention/grace非负；
- 配置超过已认证profile时拒绝启动retirement。

256 MiB是首个GA的候选认证上限，表示4.1节保守估算后的单个Block
source/decode峰值，不是单行或压缩extent大小承诺。1/10 TiB、最大varlen和并发峰值
认证可把发布值调低；未经新一轮认证不得调高。

## 3. Kill switch

### 3.1 层级

```text
cluster
account
binding/table
operation kind
```

operation kind：

```text
INDEX
PLAN
UPLOAD
RETIRE_WHOLE
RETIRE_MIXED
RESTORE
PURGE
CLEANUP
RECONCILE
```

### 3.2 安全行为

`retirement-enabled=false`：

- 不创建新的Whole/Mixed final transaction；
- Reader/Uploading可安全cancel并由Root cleanup；
- 已进入FINALIZING/COMMIT_UNKNOWN不cancel、不清理，继续Reconcile；
- Cleanup/Reconcile继续；
- Restore/Purge按独立开关。

`archive-upload-enabled=false`：

- 不开始新PUT；
- in-flight I/O由context deadline收敛；
- 已VERIFIED可等待；
- 不影响Purge/Delete。

`cleanup-enabled=false`：

- 仅用于Provider Delete事故短时止血；
- Root/对象持续积累，必须高优告警；
- retirement默认自动关闭，避免继续制造staging；
- Reconcile仍运行。

`reconcile-enabled=false`：

- retirement强制关闭；
- FINALIZING/unknown保持；
- 不允许生产长期关闭。

P0 invariant checker可自动触发cluster retirement kill switch，但不能自动关闭Cleanup/Reconcile。

## 4. SHOW 输出

### 4.1 `SHOW LIFECYCLE FOR TABLE`

至少：

```text
binding_id
state/generation
mode
lifecycle column
action duration/retention
profile name/version/namespace
schema/physical generation
discovery cycle/state/cursor/watermark/lag
candidate rows/bytes/backpressure
next scan
active child/attempt/epoch/state
whole due objects/bytes
mixed due objects/bytes
blocked objects/bytes/reason
rewrite 24h/7d attempted/retired/aborted bytes and amplification
last success/error
quota pause reason
```

### 4.2 `SHOW LIFECYCLE JOBS`

至少：

```text
job/child/attempt ID
task kind/state
evaluation time/cutoff
source Object count/bytes/digest short form
snapshot TS
executor CN/epoch/lease expiry
execution path: WHOLE / SMALL_MIXED_DELETE / MIXED_REWRITE
rows read/expired/live/deleted
live staging Object count/bytes
transfer entries/booking files/bytes
reservation/protection status and expiry
payload count/bytes
retry/conflict age
final txn ID/status
deadline/next action
last error
```

### 4.3 `SHOW LIFECYCLE DATASETS`

至少：

```text
dataset ID/state
source table/schema generation
lifecycle min/max
rows/payload bytes/count
manifest root
publish commit time
purge eligible time
profile identity
active Restore leases
Root cleanup state
```

### 4.4 `SHOW RESTORE ARCHIVE JOB`

至少：

```text
restore ID/state
Dataset count/bytes
target db/table
staging table ID
current payload/row group
restored rows/expected
root status
chunk txn status
lease expiry
last error
```

普通用户输出不含：

- credential ref；
- raw endpoint secret/query string；
- full internal transaction payload；
- presigned URL。

## 5. Metrics

指标前缀：

```text
mo_lifecycle_
```

### 5.1 Binding/Discovery

```text
bindings{state,mode}
discovery_cycles{state}
discovery_page_seconds
discovery_cursor_age_seconds
discovery_collect_lag_seconds
candidate_rows{state,class}
candidate_bytes{state,class}
candidate_backpressure{scope}
discovery_metadata_invalid_total{reason}
policy_scan_seconds
policy_scan_candidates{class}
```

### 5.2 Job

```text
jobs{state,kind}
job_age_seconds{state,kind}
job_source_bytes{kind}
job_rows{kind,result}
job_retries_total{kind,reason}
job_conflict_age_seconds{kind}
blocked_objects{reason}
blocked_bytes{reason}
```

### 5.3 Reader/Archive

```text
reader_seconds{mode,result}
reader_bytes{mode}
reader_rows{mode,kind}
reader_incomplete_total{reason}
archive_put_bytes{provider}
archive_get_verify_bytes{provider}
archive_payloads{state}
archive_compression_ratio
archive_root_mismatch_total
provider_requests_total{provider,op,result}
provider_latency_seconds{provider,op}
```

### 5.4 Retirement/Txn

```text
strict_retire_objects{mode,result}
strict_prepare_seconds{phase}
strict_conflicts_total{reason}
tombstone_delta_found_total
tombstone_delta_overflow_total
tombstone_delta_rows
tombstone_delta_bytes
tombstone_delta_blocks
tombstone_delta_scan_seconds{phase,result}
mixed_delete_rows
mixed_delete_key_bytes
mixed_tombstone_estimated_bytes
mixed_txn_seconds{result}
final_txn{status,mode}
commit_unknown_age_seconds
```

### 5.5 Root/Cleanup

```text
roots{state}
root_objects{state,kind}
staging_bytes
multipart_uploads{state}
cleanup_backlog_objects
cleanup_backlog_bytes
cleanup_oldest_age_seconds
delete_failed{provider,reason}
late_put_total
quiescence_roots
```

### 5.6 Restore

```text
restore_jobs{state}
restore_bytes{result}
restore_rows{result}
restore_chunk_txn{status}
restore_lease_count{state}
restore_staging_tables{state}
restore_seconds{result}
```

### 5.7 资源

```text
active_children{scope}
reader_memory_bytes
writer_memory_bytes
rewrite_task_memory_reserved_bytes
rewrite_block_estimated_bytes
rewrite_block_admission_rejected_total{reason}
small_mixed_delete_spill_bytes
rewrite_dense_transfer_reserved_bytes
rewrite_external_booking_bytes
rewrite_source_objects
rewrite_attempted_source_bytes{window}
rewrite_committed_retired_expired_bytes{window}
rewrite_aborted_read_bytes{window}
rewrite_aborted_write_bytes{window}
rewrite_amplification_ratio{window}
rewrite_consecutive_blocked
txn_workspace_bytes{kind}
snapshot_age_seconds{kind}
snapshot_retained_bytes{kind}
tae_gc_lag_seconds
merge_backlog_objects
tombstone_backlog_bytes
catalog_control_rows{table,state}
```

## 6. Label基数

Prometheus label禁止：

- table ID/name；
- account ID/name；
- job/attempt/dataset/root ID；
- object key/ID；
- raw error message。

允许低基数：

- state/kind/mode/result/reason；
- provider type；
- rollout stage。

具体identity放结构化日志和SHOW。账户级计量进入内部usage表，不做metrics label。

## 7. Structured log

统一event：

```text
lifecycle.binding.changed
lifecycle.index.generation
lifecycle.job.transition
lifecycle.reader.completed
lifecycle.archive.object
lifecycle.root.transition
lifecycle.final_txn
lifecycle.reconcile
lifecycle.cleanup
lifecycle.restore
lifecycle.invariant.failure
```

公共字段：

```text
service/CN ID
account incarnation short
logical/physical table ID
binding/job/attempt/root/dataset/restore ID
generation/epoch/state version
from/to state
source digest/manifest root short
txn ID/status/commit TS
duration/rows/bytes
error code
```

禁止记录credential和业务行值。

## 8. Trace

一个child root span：

```text
LifecycleChild
  DiscoveryRevalidate
  SourceReservation
  SourceProtection
  ExactReader
  ArchivePayload[n]
    Put
    Readback
  RewriteLiveObject[n]
  TransferBooking
  Manifest
  RootFinalizing
  FinalTxn
    CatalogCAS
    tagged LifecycleCommitEntry / RelationDelete
    Commit
  Reconcile
```

Provider object ordinal可以span attribute，full key不进入普通trace。

## 9. Alerts

### P0 page

- Archive root mismatch > 0；
- committed txn缺Receipt/Dataset；
- aborted txn有Receipt；
- Dataset PUBLISHED无Root；
- Root PUBLISHED无Dataset且owner未DROP；
- DELETING出现新lease；
- 未知/不支持的tagged Lifecycle协议被跳过或当普通Merge提交；
- Rewrite row-conservation/transfer root不匹配；
- Rewrite source object count不等于1或出现inline transfer；
- Whole source/layout proof数量、顺序或identity不一致；
- Booking V1 version/Root/namespace/actual-row/D-E-L覆盖不一致；
- Lifecycle rollback/NeedRetry后Root live file缺失；
- Tombstone delta超限后仍提交，或phase scan error被吞掉；
- protection失效后仍成功退休source；
- staging上传发生在Root创建前；
- 超认证Block在payload读取前未被拒绝，或未取得task memory token即进入Rewrite；
- activity retired但Archive未VERIFIED；
- invariant checker failure。

动作：自动关闭retirement，保留Cleanup/Reconcile。

### P1 page/ticket

- COMMIT_UNKNOWN > 10分钟；
- MANUAL_RECONCILE_REQUIRED > 0；
- DELETE_FAILED > 1小时；
- POST_COMMIT_CLEANUP/temporary booking > 1小时；
- cleanup oldest > 6小时；
- Root/staging bytes > 80% hard limit；
- Restore staging > 24小时；
- GC lag/retained bytes > hard；
- Tombstone backlog > hard；
- capability在升级窗口外不一致。

### P2 warning

- Discovery full-cycle/cursor lag > 30分钟；
- blocked比例 > 10%；
- Archive cost model连续7天净负收益；
- compression ratio异常；
- provider latency/error升高；
- Job lag > policy目标。

## 10. 成本模型

### 10.1 TTL

```text
monthly_saving =
  removed_active_bytes * active_storage_price
  - delete/request cost
  - additional Tombstone/Merge cost for Mixed
```

Whole TTL最接近纯节省；Mixed必须计入写放大。

### 10.2 Archive

```text
monthly_net =
  removed_TAE_active_bytes * active_storage_price
  + estimated_active_scan/cache/merge reduction
  - archive_payload_bytes * archive_storage_price
  - PUT/readback/HEAD/LIST cost
  - expected Restore GET/egress/compute
  - minimum storage duration/early delete fee
  - operational reserve
```

`active_storage_price == archive_storage_price`时，介质价差为0。收益主要来自：

- 活动扫描减少；
- Merge/Tombstone/GC压力减少；
- Catalog/cache工作集减少；
- TAE和Parquet真实压缩差。

不能假设Parquet/ZSTD一定比TAE小。压缩比使用客户真实历史Dataset：

```text
actual_archive_payload_bytes / source_visible_logical_bytes
```

## 11. Capacity dashboard

必须同时展示：

```text
绑定表/Index
  current/obsolete rows
  catchup backlog

Job
  due/running/blocked/unknown
  source bytes

External
  staging/published/delete-failed bytes
  object/multipart count

TAE
  Mixed Tombstone rolling bytes
  Merge backlog
  GC lag/retained bytes

Restore
  staging bytes/table count
  active leases/chunks
```

任一积累项没有增长上限时，不允许打开下一rollout stage。

## 12. Runbook

### 12.1 `COMMIT_UNKNOWN`

1. 不手工Delete Root/Payload；
2. 确认final txn ID/digest；
3. 查询Txn GetStatus；
4. committed：等待服务水位>=commit TS，读Receipt/Dataset；
5. aborted：确认无Receipt/Dataset；
6. unknown：保持Root FINALIZING，检查TN/LockService resolver；
7. 24小时后进入manual，不猜测；
8. 同表retirement保持暂停。

### 12.2 Root `DELETE_FAILED`

1. 确认owner/Purge事实和无lease；
2. 检查Profile namespace没有被重指；
3. 修复credential只授予原namespace；
4. 用HEAD/LIST审计exact prefix；
5. CAS新Sweeper epoch；
6. 重试Delete；
7. 两次确认不存在后CLEANED；
8. 不删除Root证据直到retention。

### 12.3 `MIXED_LAYOUT_BLOCKED`

1. 查看重复 Rewrite 的 source/live bytes 和 rewrite amplification；
2. 对比24h/7d attempted source bytes、committed retired expired bytes及
   aborted read/write bytes；
3. 查看生命周期列与 physical sort/cluster key 的相关性；
4. 查看 Tombstone/Merge backlog 和迟到写入分布；
5. 建议增加晚到 grace 或改善 event-time 局部性；
6. 不直接调大 hard limit，也不通过重启清空rolling bucket；
7. 数据布局变化后显式 `RECHECK` 创建新 generation。

### 12.4 `RESOURCE_BLOCKED`

1. 查看 source、live staging、dense transfer、external booking、Tombstone delta 和
   Provider分项预算；
2. 查看Block metadata extent估算、`max_certified_block_read_bytes`和task/Block
   memory token；未知/溢出必须发生在payload读取前；
3. 确认是否为单个合法大Object；若是，区分“多Block均已认证的3 GiB Object”和
   “未认证oversize Block/row”，不能无限重试；
4. 确认 Root 已拥有全部 staging，source 仍可见；
5. 释放集群资源或调整经认证的 release profile；
6. 显式 `RECHECK`，禁止直接把 blocked child 改回 running。

### 12.5 Root mismatch

1. cluster retirement kill switch；
2. 保留source数据、Root、Payload和日志；
3. 禁止overwrite key；
4. 对比Reader canonical root、payload SHA、decoder/type mapping；
5. 修复并新attempt/new prefix；
6. 原attempt cleanup需保留审计副本到事故结论。

### 12.6 Restore卡住

1. 查看current chunk txn；
2. unknown先Txn/Receipt对账；
3. lease快到期则由同epoch续租；
4. worker失联则fence epoch并接管；
5. staging count/receipt不一致不发布；
6. cancel后等待unknown收敛再DROP staging。

### 12.7 Provider事故

```text
PUT/GET失败:
  disable archive-upload/restore
  keep cleanup/reconcile if Delete/Status正常

Delete异常:
  disable cleanup
  automatically disable new retirement/upload
  keep reconcile

namespace误配置:
  freeze Profile
  no credential rotation across namespace
```

## 13. 升级/放量

Stage：

```text
0: 5 internal tables, Dry-run/Export-only
1: 50 bindings
2: 200 bindings
3: 500 bindings
4: 1000 bindings
```

每Stage至少运行：

- 7天稳定窗口；
- 一次CN/TN rolling restart；
- 一次Provider故障注入；
- 一次commit response lost；
- 一次Restore/Purge；
- 无P0 invariant failure；
- 资源低于70% hard limit；
- Cleanup/Reconcile能追平。

Stage 4前必须完成1TiB常见/10TiB认证。

## 14. SLO和错误预算

内部初始目标：

```text
Discovery cycle p99     < 30 min
due Whole start p99     < 6 h
Cleanup normal p99      < 24 h
commit unknown auto     < 10 min for 99.9%
Restore RPO             exact published Dataset
Restore publish atomic  100%
data invariant failure  0
```

Lifecycle lag可有错误预算；数据不变量没有错误预算。任何“已退休但不可恢复”是release blocker。
