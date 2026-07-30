# 06 可观测性、容量与运维详细设计

## 1. 配置

发布配置必须有明确默认值和hard cap：

```text
lifecycle-enabled
lifecycle-retirement-enabled          # 全量升级后才开启
max-bound-tables
scan-page-objects
scan-page-meta-bytes
scan-metadata-reads-per-page
scan-metadata-read-bytes-per-page
full-scan-interval
candidate-count/bytes
child-concurrency
rewrite-concurrency
max-rewrite-amplification
rewrite-budget-window
max-rewrite-source-bytes-per-account/window
max-rewrite-source-bytes-per-cluster/window
provider-read/write-concurrency
provider-bytes-per-second
target-payload-file-bytes
max-payload-files-per-dataset
max-chunks-per-dataset
max-restore-chunk-rows
max-restore-chunk-logical-bytes
max-certified-block-read-bytes
max-source-objects-per-whole
max-protection-files/bloom-bytes
max-final-wire-bytes
max-delta-rows/bytes/blocks
max-source-conflicts-per-child
max-conflict-wall-time
root-unknown-count/bytes
cleanup-backlog-count/bytes
published-dataset-count/metadata-bytes
archive-payload-bytes-per-account
restore-concurrency/deadline
restore-attempt/chunk-receipt-count
max-active-restore-staging-bytes-per-account
max-active-restore-staging-bytes-per-cluster
terminal-metadata-retention
```

配置关系必须启动时校验，不能自动放宽hard cap。
`max-restore-chunk-rows`和`max-restore-chunk-logical-bytes`必须由普通INSERT事务的内存、
wire/WAL和时长认证结果反推，并受release profile hard cap约束；用户不能把它们调高到
未经认证的范围。

运行时配置可以调低Writer目标，但已有Dataset所需值超过当前配置时，Restore返回
`RESOURCE_BLOCKED`并报告所需rows/logical bytes，不能标记corruption。运维可在release
hard cap内恢复配置。同一Manifest reader/version的后续兼容release不得降低其已认证Restore
hard cap；不满足该条件的降级必须在仍有对应Dataset时拒绝。

## 2. Metrics

低基数标签仅使用account bucket、mode、state、reason，不使用table/object/root ID。

```text
lifecycle_bindings
lifecycle_scan_pages_total
lifecycle_scan_objects_total
lifecycle_scan_metadata_reads/bytes
lifecycle_scan_metadata_cache_hits
lifecycle_full_scan_age_seconds
lifecycle_candidates{mode}
lifecycle_jobs{state,reason}
lifecycle_reader_bytes
lifecycle_archive_put/readback_bytes
lifecycle_archive_verify_failures
lifecycle_rewrite_source/live/expired_bytes
lifecycle_rewrite_amplification
lifecycle_rewrite_window_source_bytes{scope}
lifecycle_final_txn{result}
lifecycle_post_snapshot_delta_rows
lifecycle_roots{state}
lifecycle_root_bytes{state}
lifecycle_cleanup_backlog_bytes
lifecycle_restore{state}
lifecycle_restore_bytes
lifecycle_restore_staging_bytes{scope}
lifecycle_restore_chunk_rows/logical_bytes
lifecycle_provider_errors{operation,reason}
lifecycle_resource_rejections{resource}
```

`lifecycle_resource_rejections`只统计Scheduler/CN/Reader/Provider/entry-hard-limit拒绝，不表示
存在TN Lifecycle专用permit或`RESOURCE_BUSY` final retry。

## 3. SHOW

`SHOW LIFECYCLE FOR TABLE`至少显示：

- action/cutoff/Stage；
- Binding generation/state；
- last page/last full scan；
- eligible/blocked估算；
- active child和最后错误；
- Archive/TTL累计结果。

`SHOW LIFECYCLE JOBS`显示child、source count/bytes、mode、阶段、deadline和blocked reason。

`SHOW LIFECYCLE DATASETS`显示Dataset、时间范围、rows/bytes、Stage、purge time、lease和状态。

## 4. Structured log与Trace

每个child/root/restore状态变化写结构化日志：

```text
binding_id, root_id, attempt_id, dataset_id, restore_id
account_id, logical/physical_table_id
phase, old_state, new_state, reason
source_objects/bytes, expired/live rows
provider operation/key hash/bytes/duration/retry
txn_id/result, source_snapshot_ts, cutoff
deadline, resource budget/used
```

日志可以带ID，Metrics不能。不得输出Stage明文credential、完整URL query或用户数据。

Trace至少包含`discovery.page`、`metadata.zonemap`、`source.protect`、`reader.object`、
`archive.put/readback`、`rewrite.merge`、`final.commit`、`root.reconcile/delete`和
`restore.chunk/publish`。每个span受采样和数量上限，不能为每行建span。

## 5. Alerts

P0 page：

- content hash/readback不一致；
- matching Dataset与Root identity不一致；
- COMMIT_UNKNOWN超过SLO或hard cap；
- PUBLISHED Dataset对应Root进入删除；
- Restore hash不一致；
- ordinary MO回归超过stop threshold。

P1 ticket：

- full scan age超SLO；
- cleanup backlog增长；
- Provider 429/credential错误；
- Archive Stage认证失效或运维发现Versioning配置漂移；
- Rewrite/TTL blocked持续；
- terminal metadata接近cap。

## 6. Kill switch

```text
pause new discovery
pause new archive
pause retirement
pause restore
pause purge
```

Kill switch不强制清理FINALIZING/COMMIT_UNKNOWN，也不取消已进入普通事务Prepare的操作。
关闭retirement后Export-only仍可单独运行。

## 7. 普通MO隔离

feature off：

- 无Lifecycle对象分配；
- 无Catalog查询；
- 无Scheduler任务；
- 无Provider I/O；
- 普通Merge默认参数和代码路径不变；
- unknown Entry安全解析只增加可测的常数分支。

feature开启但目标表无Binding：

- 普通查询、DML和Merge仍为零Lifecycle Catalog访问；
- 可能冲突的DDL至多执行一次按`(account_id, physical_table_id)`的索引化Binding lookup；
- 不创建Binding、Guard、Candidate、Root或其他Lifecycle元数据。

Active coexistence必须测DML、查询、Merge、checkpoint、GC、logtail吞吐和P99。阈值在Gate I
前冻结，建议初始stop threshold：

```text
throughput regression > 5%
p99 regression > 10%
TN/CN memory regression > 5%
Merge/GC backlog持续增长
```

## 8. 成本与容量口径

SHOW/metrics至少区分：

```text
retired_active_logical_bytes
archive_payload_physical_bytes
archive_put/get/list/delete_requests
restore_read/egress_bytes
rewrite_source/live/expired_bytes
cleanup_orphan_bytes
```

成本报告计算：

```text
Archive成本 =
  Payload存储费
  + PUT/LIST/GET/Delete请求费
  + full readback费
  + Restore取回/流量/计算费

活动面收益（估算） =
  退休Object bytes
  + 对应Metadata/Merge/checkpoint/GC/cache工作集减少
```

如果Stage与活动TAE使用相同存储类别，不能宣称每GB介质单价下降；收益主要来自活动工作集
退休。压缩率是Parquet编码结果，不是COLD/ARCHIVE名称带来的额外收益。

## 9. Rewrite与Restore容量保险丝

### 9.1 Mixed Rewrite

Mixed Rewrite受三个独立条件约束：

```text
live_logical_bytes / max(expired_logical_bytes, 1)
  <= max-rewrite-amplification

sum(exact source ObjectStats size for one account in current fixed window)
  <= max-rewrite-source-bytes-per-account/window

sum(exact source ObjectStats size for cluster in current fixed window)
  <= max-rewrite-source-bytes-per-cluster/window
```

source bytes在Reader启动前由现有Lifecycle Coordinator按exact ObjectStats size预占并立即
计费；attempt后续成功、blocked或abort都不返还，因为普通MO已经承担了读取/Rewrite压力。
amplification在单源分类完成后检查，超限时不进入final transaction，Root staging异步清理，
任务进入`MIXED_LAYOUT_BLOCKED`或等待更多行到期。

这三个计数器是现有单active Coordinator拥有的内存固定窗口预算，不是Catalog Slot或数据
正确性协议。Coordinator切换时，新owner在当前窗口剩余时间内把cluster Rewrite预算视为
已耗尽，到下一个固定窗口再重新开放；宁可短暂停止Rewrite，也不通过failover绕过hard cap。
账户bucket只为本窗口实际出现的绑定账户创建，窗口切换时整体回收，条目数受
`max-bound-tables`约束。Whole退休和普通Merge不访问这些计数器。

### 9.2 Restore staging

Restore启动准入使用`Dataset.logical_bytes`作为保守核算值：

```text
sum(Dataset.logical_bytes for active RUNNING/VERIFYING/PUBLISHING attempts in account)
  + requested Dataset.logical_bytes
  <= max-active-restore-staging-bytes-per-account

cluster对应总和
  <= max-active-restore-staging-bytes-per-cluster
```

RESTORE命令由现有Lifecycle Coordinator完成准入后才执行05中的初始化普通事务。Coordinator
重启时从现有Restore Attempt和Dataset索引按page rows/bytes cap分页重建计数；每轮有deadline，
失败后有界退避并保持暂停新Restore。这个计数不预分配持久Slot，不进入普通DML路径。Attempt
`DONE`后表已转交用户Catalog，不再计入Lifecycle staging；`ABORTED`只有在隐藏表确认DROP后
才释放核算值。实际TAE物理bytes仍作为观测指标，若持续高于logical bytes认证系数则Gate I
Stop Ship并降低上限。

初始化前的内存reservation由发起Coordinator唯一持有：初始化事务明确失败时立即释放；提交
成功后由active Attempt接管核算；结果unknown时保留reservation并按05的Dataset/Attempt/
hidden identity对账，不能先释放后接受第二个Restore。Coordinator崩溃后不恢复纯内存
reservation，实际已提交的初始化由active Attempt重建，实际未提交的reservation自然消失。

### 9.3 Cleanup backlog与Binding容量

`cleanup-backlog-count/bytes`到达hard cap后，暂停所有会创建Root的新任务，包括Archive
Whole、Archive Rewrite和TTL Rewrite；Whole TTL和已关闭的TTL small Mixed没有Root副作用，
可按独立事务预算决定是否继续。已有Sweeper和COMMIT_UNKNOWN对账继续运行。

`max-bound-tables`是发布认证的全集群配置上限，允许另设每账户配额；它不是预分配Slot或
跨租户事务不变量。超过认证值拒绝新Binding或停止扩大放量，不能恢复Cluster Activation
Slot协议。

## 10. Runbook

### COMMIT_UNKNOWN

1. 暂停该表新retirement；
2. 一致性查询matching Dataset/TTL Receipt；
3. 检查普通事务/TAE source状态；
4. 能证明published则Root PUBLISHED；
5. 能证明aborted且无publication则DELETE_PENDING；
6. 不能证明则保留、告警，禁止人工直接Delete Payload。

### Cleanup失败

检查credential handle、Stage位置、Provider规则和prefix；恢复凭据后重试。不能修改Root
namespace指向其他bucket。

若部署认证被撤销或运维发现专用Bucket被开启Versioning，暂停该Stage的新Archive。首个GA
不自动枚举/删除历史version；由Provider运维工具完成清理并恢复认证，在此之前相关Root
保持`DELETING`并告警，不能仅按current key不可见宣称`CLEANED`。

### Full scan饥饿

检查last full scan、cursor stale次数、Merge churn和账户队列；必要时降低page并发但强制
wrap，不允许通过跳过旧Object隐藏问题。

## 11. 放量

```text
50 -> 200 -> 500 -> 1000 bindings
```

每阶段至少覆盖稳定期、Provider限流、Rewrite+TTL+Restore并发和普通MO对照。任何数据
不变量失败立即关闭retirement并停止扩大。
