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
provider-read/write-concurrency
provider-bytes-per-second
target-payload-file-bytes
max-payload-files-per-dataset
max-chunks-per-dataset
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
restore-concurrency/chunk-bytes/deadline
restore-attempt/chunk-receipt-count
terminal-metadata-retention
```

配置关系必须启动时校验，不能自动放宽hard cap。

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
lifecycle_final_txn{result}
lifecycle_post_snapshot_delta_rows
lifecycle_roots{state}
lifecycle_root_bytes{state}
lifecycle_cleanup_backlog_bytes
lifecycle_restore{state}
lifecycle_restore_bytes
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

## 9. Runbook

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

## 10. 放量

```text
50 -> 200 -> 500 -> 1000 bindings
```

每阶段至少覆盖稳定期、Provider限流、Rewrite+TTL+Restore并发和普通MO对照。任何数据
不变量失败立即关闭retirement并停止扩大。
