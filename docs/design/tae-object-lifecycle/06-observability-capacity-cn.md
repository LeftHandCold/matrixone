# 06 可观测性、容量与运维详细设计

## 1. 配置

首个GA唯一运行时release gate是`mo_feature_registry.enabled`；`scope_spec`只保存认证过的
`archive_stages`。下面其余数值是代码中的Release Profile认证常量或运维Stop-Ship阈值，
不是首版租户SQL/runtime knob，也不为它们新建配置状态机：

```text
mo_feature_registry.enabled           # 全量升级后才由运维打开
max-bound-tables
scan-page-objects
scan-page-meta-bytes
scan-metadata-reads-per-page
scan-metadata-read-bytes-per-page
scan-binding-pages-per-run
full-scan-interval
candidate-count/bytes
child-concurrency
rewrite-concurrency
cluster-child-concurrency
cleanup-sweep-budget
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
observed-restore-staging-bytes-per-cluster-stop-threshold
terminal-metadata-retention
```

Release Profile关系由构造/启动校验和测试冻结，不能自动放宽。`max-delta-rows`使用公共scanner的N+1模式在
收集过程中停止；`max-delta-bytes`在公共Merge scanner返回单source Batch后校验，是防御性
拒绝线而非新的私有内存管理器，其峰值必须连同MO公共scanner的结构上限纳入认证。
Coordinator每轮最多读取4个Binding账户分页；每个分页最多覆盖64个account，因此即使大量
tenant完全没有Binding，一次cron也至多执行256个tenant Binding探测，随后从现有内存cursor
继续。这个上限只约束控制面扫描，不建立Account/Object Index或持久调度状态。
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
`cluster-child-concurrency`首发固定为2；`cleanup-sweep-budget`固定为1分钟，慢Provider只延后
Lifecycle维护页，不建立TN permit或分布式资源状态。

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

首个GA只实现能从现有执行点直接、无额外状态取得的低基数指标，不再建设第二套监控状态机。
标签只使用mode/result/operation/resource，不使用account/table/object/root ID：

```text
mo_lifecycle_jobs_total{mode,result}
mo_lifecycle_active_jobs{mode}
mo_lifecycle_observed_full_scan_age_seconds
mo_lifecycle_objects_total{operation}
mo_lifecycle_bytes_total{operation}
  # provider_write/provider_read
  # rewrite_source_pressure/rewrite_estimated_expired_pressure/
  # rewrite_estimated_live_pressure/retired_source
mo_lifecycle_root_transitions_total{from,to}
mo_lifecycle_final_transactions_total{mode,result}
mo_lifecycle_restore_total{operation,result}
mo_lifecycle_resource_rejections_total{resource}
mo_lifecycle_provider_errors_total{operation}
mo_lifecycle_active_cleanup_roots
mo_lifecycle_reserved_cleanup_bytes
```

`observed_full_scan_age_seconds`是当前有界调度页中观察到的最大值；未完成过full scan的Binding
按Unix epoch计算，因而会立即触发超龄告警。Root/Dataset/Restore backlog、最老工作age和
Restore staging logical bytes直接使用01中已有索引做有界运维查询；普通DML/查询P99、
CN/TN内存、Merge/GC/logtail backlog复用MO现有指标，不复制Lifecycle版本。

`resource_rejections_total`只统计Scheduler/CN/Reader/Provider/entry-hard-limit拒绝，不表示
存在TN Lifecycle专用permit或`RESOURCE_BUSY` final retry。

## 3. SHOW

`SHOW LIFECYCLE FOR TABLE`显示action、state、expire/purge days、Stage ID、Binding
generation和更新时间；扫描cursor、eligible/blocked估算及累计值通过metrics/日志诊断。

`SHOW LIFECYCLE JOBS`显示Root ID、mode、state、cleanup time和last error。

`SHOW LIFECYCLE DATASETS`显示Dataset ID、state、rows/bytes、purge time和Manifest key。

JOBS和DATASETS均使用默认/最大1000行的`LIMIT ... OFFSET ...`有界翻页，且
`OFFSET + LIMIT <= 1,000,000`；排序包含唯一ID tie-breaker。它是live Catalog的
best-effort诊断接口，并发变更时不承诺跨页一致快照；更丰富字段继续由现有metrics、日志和
受控Catalog运维查询提供，不扩大首期SHOW实现。

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
continue purge and cleanup
```

这里的`pause retirement`指不再启动新的child；已经进入执行的child可能完成当前Archive、
readback或final transaction。Kill switch不强制清理FINALIZING/COMMIT_UNKNOWN，也不取消
已进入普通事务Prepare的操作。普通Backup不以Lifecycle gate或in-flight drain为前置条件，
但这只说明Backup创建路径不被Lifecycle阻断，不代表其产物具备Lifecycle恢复能力。物理
Backup可能包含Lifecycle Catalog、Stage和release gate，却不包含外部Archive Payload。
Export-only只用于测试/认证Writer与Provider，不作为Phase 1生产模式。关闭retirement release
后不创建新Root或执行Provider PUT，只继续Purge、收敛已有Root和终态元数据。

因此`enabled=false`不是物理Backup Restore的隔离开关：本节定义的历史Root cleanup仍会
运行。含Lifecycle状态的物理Backup Restore在Phase 1 unsupported；恢复环境必须在任何
Coordinator/Sweeper tick前禁用Lifecycle任务，并隔离原Archive namespace的删除凭据。完整
Backup/DR兼容由独立PR实现，不在Lifecycle运行时增加cluster identity或恢复状态机。

## 7. 普通MO隔离

feature off：

- 不创建新的Lifecycle attempt、Dataset、Restore或staging；
- Coordinator cron仍只为历史Cleanup Root执行有界reconcile/sweep，避免关闭开关后遗留外部
  对象永久泄漏；过期Restore隐藏表清理与有界终态元数据压缩也继续，Binding调度和新
  Restore执行跳过；
- 除上述历史Root清理外无新的Provider PUT/readback；
- 普通查询、DML、Merge、checkpoint、GC和logtail无Lifecycle Catalog访问；
- 表级管理DDL仍执行一次索引化Binding检查，但不取得集群级feature-row写锁；
- Snapshot/PITR创建、Clone/Data Branch和普通同集群Publication/Subscription不访问
  Lifecycle feature row；Snapshot/PITR Restore仅在破坏性提交前执行Archive scope
  fail-closed检查，并要求先关闭、drain Lifecycle数据任务；
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
retired_source_pressure_bytes
archive_payload_physical_bytes
provider_read/write_bytes
rewrite_source/estimated_expired_pressure_bytes
cleanup_orphan_bytes
```

`retired_source_pressure_bytes`与Rewrite使用`max(ObjectStats.OriginSize, ObjectStats.Size, 1)`
口径，不宣称是精确canonical logical bytes。Provider请求数、Restore egress和账单继续读取
现有Provider/FileService监控，不在Lifecycle内复制请求计数状态。

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
source_pressure_bytes / max(estimated_expired_pressure_bytes, 1)
  <= max-rewrite-amplification

sum(exact source ObjectStats size for one account in current fixed window)
  <= max-rewrite-source-bytes-per-account/window

sum(exact source ObjectStats size for cluster in current fixed window)
  <= max-rewrite-source-bytes-per-cluster/window
```

source bytes在Reader启动前由现有Lifecycle Coordinator按exact ObjectStats size预占并立即
计费；attempt后续成功、blocked或abort都不返还，因为普通MO已经承担了读取/Rewrite压力。
`estimated_expired_pressure_bytes`按到期行比例估算，不是精确logical bytes；固定窗口的
source pressure bytes hard cap负责覆盖宽窄行偏斜，不为精确放大率增加逐行统计状态。
amplification在单源分类完成后检查，超限时不进入final transaction，Root staging异步清理，
任务进入`MIXED_LAYOUT_BLOCKED`或等待更多行到期。

这三个计数器是现有单active Coordinator拥有的内存固定窗口预算，不是Catalog Slot或数据
正确性协议。Coordinator切换后由新owner重新开始本地窗口计数，不为跨重启精确配额引入
持久状态或整日blackout；因此它是运行期限流和认证边界，不是跨owner的强事务不变量。
账户bucket只为本窗口实际出现的绑定账户创建，窗口切换时整体回收，条目数受
`max-bound-tables`约束。Whole退休和普通Merge不访问这些计数器；反复重启下的资源表现由
active-coexistence和故障注入门禁验证，不能因此修改普通Merge。

### 9.2 Restore staging

Restore启动准入使用`Dataset.logical_bytes`作为保守核算值：

```text
sum(Dataset.logical_bytes for active IMPORTING/PUBLISHING attempts in account)
  + requested Dataset.logical_bytes
  <= max-active-restore-staging-bytes-per-account
```

RESTORE不转发给TaskService Coordinator，也不创建持久Slot或CN本地reservation。05中的
初始化普通事务在任何Dataset lease、隐藏表或Attempt副作用之前，只锁当前账户现有的
`mo_account`行，再统计当前账户`IMPORTING/PUBLISHING` Attempt对应的
`Dataset.logical_bytes`。账户cap因此是精确的同事务准入；不同账户不互相串行，也不访问
Lifecycle feature row或枚举全体tenant。通过后才在同一普通事务执行Dataset lease CAS、
创建隐藏表和插入Attempt。

全集群staging bytes只有发布认证、监控和Stop-Ship阈值，不是启动准入配置或跨账户事务强不变量；不为精确cluster
quota增加Slot表或全局锁。初始化事务绝对deadline为30秒、账户行lock wait上限为5秒；
查询失败、账户消失、溢出或账户cap耗尽都在首次副作用前fail closed。事务提交后由Attempt
计入下一次本账户核算，事务失败不留下reservation，响应丢失继续由普通MO事务结果和05中的
Attempt身份对账处理。Chunk导入、发布和普通查询、DML、Merge均不访问账户锁。

`DONE`后表已转交用户Catalog，不再计入Lifecycle staging；`FAILED`只有在隐藏表确认DROP后才释放
核算值。实际TAE物理bytes与全集群logical bytes作为观测指标，若持续高于认证系数或cluster
配置边界则Gate I Stop Ship
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

`max-bound-tables=1000`是发布认证的全集群硬上限。所有SET已经由system account中现存的
Lifecycle feature row串行，因此在该短事务内用system account精确COUNT Binding（更新已有
Binding时排除自身）并拒绝第1001张表；这不是预分配Slot、持久counter或reconcile协议，
也不进入Scheduler、普通DDL、DML或查询路径。`MaxBindingsPerRun`仍只是单轮调度上限，不能
替代SET准入。发布控制面继续在50/200/500/1000阶段记录实际Binding总量并停止继续放量。

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
