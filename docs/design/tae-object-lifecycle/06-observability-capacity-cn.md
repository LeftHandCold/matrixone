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
max-active-cleanup-roots
cleanup-backlog-bytes-observed
published-dataset-count/metadata-bytes
archive-payload-bytes-per-account
restore-concurrency/deadline
restore-attempt/chunk-receipt-count
max-active-restore-staging-bytes-per-account
max-active-restore-staging-bytes-per-cluster
terminal-metadata-retention
```

配置关系必须启动时校验，不能自动放宽hard cap。
Phase 1冻结`max-payload-files-per-dataset = max-chunks-per-dataset = 4096`；
Writer在写第4097个Payload前返回`RESOURCE_BLOCKED`，Manifest parser和Restore再次校验。
因为Phase 1每个Payload严格只有一个Row Group，这一个上限同时约束Payload文件、
Manifest集合、Chunk Receipt和Restore聚合内存；后续若支持一个文件多个Row Group，再拆成
两个独立配置。
Manifest控制元数据另行冻结`max-manifest-bytes = 16 MiB`、
`max-schema-columns = 4096`和字符串/对象key上限。Reader必须先通过Stage/FileService
`StatFile`取得大小并完成上限检查，再执行exact bounded read；禁止用`Size=-1`把未知大小的
Manifest完整载入内存。以上是V1持久格式认证常量，不能由租户或运行时配置放宽。
`max-restore-chunk-rows`和`max-restore-chunk-logical-bytes`必须由普通INSERT事务的内存、
wire/WAL和时长认证结果反推，并受release profile hard cap约束；用户不能把它们调高到
未经认证的范围。

`max-certified-block-read-bytes`默认256 MiB。Lifecycle Rewrite在读取Block前按02中的
保守公式`2 * source logical bytes + 96 MiB`准入；未知、溢出或超过上限时返回
`RESOURCE_BLOCKED`。该检查不进入普通Merge路径。

`rewrite-concurrency`首个GA在每个Lifecycle coordinator CN上固定为1。它是Task executor
中的本地、fail-fast semaphore：只包围Mixed Rewrite build/finalize，不进入TN、普通Merge
或普通事务；并发已满时在Root和外部副作用创建前返回`RESOURCE_BLOCKED`，由后续metadata
扫描重试。认证后最多放宽到4，不能由租户SQL修改。

运行时配置可以调低Writer目标，但已有Dataset所需值超过当前配置时，Restore返回
`RESOURCE_BLOCKED`并报告所需rows/logical bytes，不能标记corruption。运维可在release
hard cap内恢复配置。同一Manifest reader/version的后续兼容release不得降低其已认证Restore
hard cap；不满足该条件的降级必须在仍有对应Dataset时拒绝。

`max-active-cleanup-roots`只统计`REGISTERED/UPLOADING/VERIFIED/FINALIZING/
COMMIT_UNKNOWN/DELETE_PENDING/DELETING`，以及尚未清完booking/live staging的
`PUBLISHED` Root。已经健康发布且`temporary_cleanup_done=true`的Archive Root是长期
Dataset物理Owner元数据，不算异常backlog；否则4096上限会让10 TiB表在约8万Object中的
前4096个后永久停摆。健康PUBLISHED Root数量/metadata bytes单独监控，并由Dataset
Purge/retention收敛。

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

这里的`pause retirement`指不再启动新的child；已经进入执行的child可能完成当前Archive、
readback或final transaction。Kill switch不强制清理FINALIZING/COMMIT_UNKNOWN，也不取消
已进入普通事务Prepare的操作。需要Backup时必须保持gate关闭并等待in-flight指标归零。
Export-only只用于测试/认证Writer与Provider，不作为Phase 1生产模式。关闭retirement release
后不创建新Root或执行Provider PUT，只继续收敛已有Root和终态元数据。

## 7. 普通MO隔离

feature off：

- 不创建新的Lifecycle attempt、Dataset、Restore或staging；
- Coordinator cron仍只为历史Cleanup Root执行有界reconcile/sweep，避免关闭开关后遗留外部
  对象永久泄漏；过期Restore隐藏表清理与有界终态元数据压缩也继续，Binding调度和新
  Restore执行跳过；
- 除上述历史Root清理外无新的Provider PUT/readback；
- 普通查询、DML、Merge、checkpoint、GC和logtail无Lifecycle Catalog访问；
- 表级管理DDL仍执行一次索引化Binding检查，但不取得集群级feature-row写锁；
- Snapshot/PITR/Publication等scope级发布才使用既有feature-row barrier；
- 普通Merge语义、默认参数、候选、排序、writer、WAL和GC不变；每个Block只增加一次
  `lifecycleReadBudget == nil`快速分支，feature-off开销由Gate I对照基准验证；
- unknown Entry安全解析只增加可测的常数分支。

TaskService声明Coordinator并发1；runner handoff若仍产生重复tick，本地只允许一个run占有
游标，重复tick直接跳过而不排队，不让取消后的任务等待另一轮长扫描。

feature开启但目标表无Binding：

- 普通查询、DML和Merge仍为零Lifecycle Catalog访问；
- 可能冲突的DDL至多执行一次按`(account_id, physical_table_id)`的索引化Binding lookup；
- 表级DDL与`SET LIFECYCLE`复用既有`mo_tables`行锁，不被其他账户DDL全局串行；
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
sum(Dataset.logical_bytes for active IMPORTING/PUBLISHING attempts in account)
  + requested Dataset.logical_bytes
  <= max-active-restore-staging-bytes-per-account

sum(Dataset.logical_bytes for active IMPORTING/PUBLISHING attempts in cluster)
  + requested Dataset.logical_bytes
  <= max-active-restore-staging-bytes-per-cluster
```

RESTORE不转发给TaskService Coordinator，也不创建持久Slot或CN本地reservation。05中的
初始化普通事务在任何Dataset lease、隐藏表或Attempt副作用之前，短暂更新并锁定现有
`mo_feature_registry('LIFECYCLE')`行；随后在同一`TxnExecutor`中完成以下动作：

1. 读取release gate，缺失或关闭时fail-closed；
2. 从system-owned、尚未`CLEANED`的Cleanup Root中有界枚举可能拥有活动Restore的账户，
   并显式加入当前账户；
3. 切换statement account context，读取每个账户`IMPORTING/PUBLISHING` Attempt对应的
   `Dataset.logical_bytes`，完成账户和全集群overflow/hard-cap检查；
4. 通过后才执行Dataset lease CAS、创建隐藏表和插入Attempt，并与准入检查一起提交。

枚举账户数hard cap为1024，初始化事务绝对deadline为30秒，feature-row lock wait上限为5秒；
查询失败、身份异常、溢出或达到任一上限都在首次副作用前返回`RESOURCE_BLOCKED`或明确错误。
所有CN的Restore初始化都锁同一现有feature row，因此“检查容量”和“创建持久Attempt”串行；
事务提交后由Attempt计入下一次核算，事务失败则不留下reservation，响应丢失继续由普通MO事务
结果和05中的Attempt身份对账处理。锁只覆盖短初始化事务，Chunk导入、发布和普通查询、DML、
Merge均不访问该行。

活动账户集合依赖既有所有权不变量：每个可Restore Dataset都有未`CLEANED`的system-owned Root，
有效Restore lease阻止其Purge/Cleanup；当前账户仍显式加入以保证自身用量不会因元数据异常漏算。
`DONE`后表已转交用户Catalog，不再计入Lifecycle staging；`FAILED`只有在隐藏表确认DROP后才释放
核算值。实际TAE物理bytes仍作为观测指标，若持续高于logical bytes认证系数则Gate I Stop Ship
并降低上限。

### 9.3 Cleanup backlog与Binding容量

创建Root前由system-owned Root仓库统计未进入`CLEANED`的active Root；
`max-active-cleanup-roots`发布默认值为4096，到达hard cap后暂停所有会创建Root的新任务，
包括Archive Whole、Archive Rewrite和TTL Rewrite。Payload/booking/staging bytes由单Root
认证上限乘以active Root cap形成保守增长上界，并继续作为观测与Stop Ship指标，不建设
逐对象明细表或分布式bytes Slot。Whole TTL和已关闭的TTL small Mixed没有Root副作用，可按
独立事务预算决定是否继续。已有Sweeper和COMMIT_UNKNOWN对账继续运行。

Whole调度不能退化成“一Object一Dataset/Root”。首个Release Profile在同一Metadata page
内按最多64个source且累计origin bytes最多4 GiB聚合，一个batch只产生一个Dataset/Root/
final transaction；Mixed仍单源。Object数、source bytes、Manifest chunk数、protection集合、
wire bytes或wall time任一达到上限都提前切分或`RESOURCE_BLOCKED`。

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
