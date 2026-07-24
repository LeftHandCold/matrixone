# MatrixOne TAE 对象级数据生命周期概要设计

> 目标 Issue：[Native table TTL #24552](https://github.com/matrixorigin/matrixone/issues/24552)、[Tiered data lifecycle #24853](https://github.com/matrixorigin/matrixone/issues/24853)
>
> 状态：生产目标概要设计，可用于架构评审和分阶段实现
>
> 设计日期：2026-07-24
>
> 代码复核基线：`534aa8cb894bb303cd74a8e8fa0b80f84922c63b`
>
> 本文取代早期分析稿中“以 SQL Range Partition 为主要执行边界”的实现建议，但保留其三平面、归档所有权、GC 引用和不可逆删除协议。

## 1. 最终结论

MO 应建设一套独立的 **TAE 对象级数据生命周期引擎**，同时服务于：

- #24552：过期行从当前表中自动消失，即原生 TTL；
- #24853：过期行先写入可验证、可恢复的归档数据集，再从当前表中消失。

推荐方案的核心不是“再实现一套 Merge”，也不是“给现有 Merge 加一个过期判断”，而是：

1. 以一组有上限、精确标识的不可变 TAE Object 为一个原子 Job；
2. 用增量对象索引计算到期时间，不按天全库扫描数据；
3. 复用现有 `mergesort.DoMergeAndWrite` 的读、排序、写 ObjectIO 和 transfer-map 生成能力；
4. 通过新的 `LifecycleRewriteHost` 将过期行送入 `DiscardSink` 或 `ArchiveSink`，将仍然存活的行写回正常 TAE Object；
5. 新增独立的 `LifecycleCommitEntry`，在一个短事务内原子完成：
   - 发布已校验的 Archive Manifest；
   - 注册新 TAE Object；
   - 退休精确的旧 TAE Object；
   - 提交索引、CDC、约束等依赖变更；
6. 普通 Merge 的候选选择、Level、Overlap、Small、目标大小、资源估算和调度策略全部保持不变；
7. 只在 TAE 通用多 scope 调度器上增加一个带租期的外部对象 reservation，用于降低 Lifecycle 与后台 Merge 的重复重写；reservation 不是正确性条件，最终正确性仍由事务 CAS 保证。

生命周期状态只有：

```text
TTL:      ACTIVE -> EXPIRED
Archive:  ACTIVE -> ARCHIVED -> PURGED
```

`ONLINE_COLD` 不属于这套生命周期引擎。CN 内存/磁盘缓存、远端缓存和活动 ObjectIO 的介质选择属于独立的在线放置策略。MO 不应为了一个收益不明确的 “COOL” 名称，把缓存、Merge、FileService 和查询路径一起复杂化。

该设计不依赖 SQL Partition。SQL Partition 可以继续作为用户的数据组织和路由功能，但不是 TTL/Archive 的正确性边界。

## 2. 用户最终获得什么

### 2.1 TTL

用户可以定义：

```sql
CREATE TABLE logs (
    id BIGINT,
    event_time TIMESTAMP NOT NULL,
    payload JSON
) TTL event_time + INTERVAL 7 DAY;
```

系统保证：

- `event_time` 超过七天的行最终从当前表中不可见；
- 不要求用户每天执行大批量 `DELETE`；
- 表没有分区也能工作；
- 表中同时包含新旧数据的 Object 会被增量重写；
- 完全过期的 Object 在没有行级依赖时可以直接退休；
- Snapshot/PITR 引用仍可能延迟底层旧 Object 的物理删除，但不会让过期行重新出现在当前表。

### 2.2 Archive

用户可以定义：

```sql
CREATE DATA LIFECYCLE POLICY archive_orders
    ON COLUMN order_time
    ARCHIVE AFTER INTERVAL 90 DAY
    TO ARCHIVE PROFILE saudi_archive
    PURGE AFTER INTERVAL 7 YEAR
    NULLS KEEP
    CDC ROW_DELETE;

ALTER TABLE finance.orders
    SET DATA LIFECYCLE POLICY archive_orders;
```

系统保证：

- 满 90 天的行先写成带 schema、统计信息、校验和及加密信息的归档数据集；
- 归档校验成功且最终事务提交后，这些行才从当前表消失；
- 归档文件不要求可被 MO 直接在线查询；恢复是显式异步操作；
- 满七年只表示进入可清理时间，仍需同时满足无引用、无法律保留和 grace period；
- 恢复默认生成新表，不直接把历史数据混回正在写入的原表。

典型客户效果是：沙特金融客户把最近 90 天订单留在活动表，七年合规记录进入归档数据集。日常查询、Merge、统计信息、备份和缓存不再处理七年全部历史；审计时按时间范围恢复到独立表。

如果底层云只有一种对象存储价格，归档仍可降低活动表的 Merge、扫描、元数据、缓存和备份成本，但不能承诺显著降低原始存储单价。只有归档 Profile 映射到更便宜的存储类别时，才会额外降低每 GB 成本。Parquet/ZSTD 是标准化和减少读取放大的手段，压缩率不是分层节省的主要来源。

## 3. 术语和语义边界

| 术语 | 本文含义 |
|---|---|
| TAE Object | MO 活动表中不可变的列式数据对象，不等于 S3/OSS/COS 的“一个业务分区” |
| Mixed Object | 生命周期列的值跨越 cutoff，既包含过期行又包含存活行的 Object |
| Cutoff | 本次 Job 固定的过期分界值，例如 `evaluation_time - 90 DAY` |
| TTL / Expire | 从当前逻辑表移除过期行，不保留独立业务归档副本 |
| Archive | 先创建独立归档数据集，校验并发布后再从当前表移除 |
| Archive Dataset | 与源表生命周期解耦、具有稳定 ID 的逻辑归档数据集 |
| Manifest | 描述归档文件、schema、行数、统计、校验和、加密和引用关系的热端元数据 |
| Source Pin | 在归档/重写期间阻止 TAE GC 删除源 Snapshot 可见对象的持久化引用 |
| Reservation | 降低 Lifecycle 与 TN Merge 同时选择相同对象概率的短租约；不承担正确性 |
| CAS | Compare-And-Swap，提交时只有所有版本、状态和对象条件仍匹配才允许成功 |
| Tombstone | TAE 对数据行删除的记录；Merge 后需要正确转移到新对象 |
| Transfer Map | 源对象行位置到新对象行位置的映射 |
| Archive Profile | 对云存储能力、存储类别、恢复 SLA、最低保留期和计费参数的声明 |

以下三个平面必须独立：

| 平面 | 解决的问题 | 示例 |
|---|---|---|
| 历史版本 | 已更新/删除的数据还能否按历史时刻读取 | Snapshot、PITR、Time Travel |
| 业务生命周期 | 哪些业务行还属于当前表 | TTL、Archive、Purge、Legal Hold |
| 在线放置 | 当前可查询对象放在哪里、如何缓存 | CN cache、远端 cache、活动对象存储 |

归档不是 Snapshot；归档 Manifest 默认不永久 pin 源 TAE Object。Snapshot/PITR 也不自动 pin Archive Payload。

Archive Dataset 只保存 `source_snapshot_ts` 下当前可见、满足生命周期谓词的逻辑行，不复制这批行的所有历史版本。更早版本是否继续存在，仍由 Snapshot/PITR 平面决定。

## 4. 为什么不使用 SQL Partition

MO 当前 Partition 模块不是“假的”，但它是 SQL 逻辑路由和隐藏子表实现，当前不适合作为大规模生命周期底座：

- Partition Service 为每个分区创建物理子表；
- `Redefine` 当前采用临时表、`INSERT SELECT`、Drop、Rename 的整表迁移路径；
- 大量数据库、表和按天分区会将对象规模问题放大为 Catalog 表数量、路由和 DDL 问题；
- 生命周期列与已有业务分区键未必一致；
- 迟到数据可能进入新 Object，却属于很早的业务时间；
- 一个 TB 表不能依赖“一次 Drop 整个分区”才能正确归档。

相关代码：

- [`pkg/partitionservice/service.go`](../../pkg/partitionservice/service.go) 负责分区元数据和路由；
- [`pkg/partitionservice/storage.go`](../../pkg/partitionservice/storage.go) 创建物理分区表；
- 同文件的 `Redefine` 使用 `insert into temporary_table select * from old_table`。

因此生命周期执行边界应是“当前 Snapshot 下精确的一组 TAE Object”，而不是 SQL Partition。已有分区如果恰好与 cutoff 对齐，可以成为候选发现的优化信息，但不能成为功能依赖。

## 5. 当前 MO 代码能够复用什么

### 5.1 可复用

1. **Object 级统计**

   `ObjectStats` 已包含对象名、行数、块数、对象大小和 sort key ZoneMap；对象 footer 包含各列 ZoneMap。生命周期列是 sort key 时无需读 footer，否则只需在对象进入索引时读一次 footer。

   代码：

   - [`pkg/objectio/object_stats.go`](../../pkg/objectio/object_stats.go)
   - [`pkg/objectio/writer.go`](../../pkg/objectio/writer.go)
   - [`pkg/vm/engine/disttae/txn_table.go`](../../pkg/vm/engine/disttae/txn_table.go) 的 `GetColumMetadataScanInfo`

2. **Merge 的流式重写原语**

   [`pkg/vm/engine/tae/mergesort/task.go`](../../pkg/vm/engine/tae/mergesort/task.go) 定义了 `MergeTaskHost` 和 `DoMergeAndWrite`。现有 merger/reshaper 已能：

   - 按 sort key 排序或 reshape；
   - 跳过 delete mask 中的行；
   - 写出新的 ObjectIO；
   - 为实际写出的存活行生成 transfer map。

3. **多对象 scope 冲突控制**

   [`pkg/vm/engine/tae/db/merge/executor.go`](../../pkg/vm/engine/tae/db/merge/executor.go) 将源对象作为 multi-scope task 调度；[`pkg/vm/engine/tae/db/dispatcher.go`](../../pkg/vm/engine/tae/db/dispatcher.go) 会拒绝本 TN 内 scope 冲突。

4. **TaskService 接管**

   [`pkg/taskservice/task_service.go`](../../pkg/taskservice/task_service.go) 在任务重新分配时增加 epoch，并在完成时按 epoch 更新。可以复用其 cron、任务持久化和 executor 接管，但不能把 task epoch 当作对象存储副作用的 fence。

5. **Snapshot/PITR GC 引用**

   [`pkg/vm/engine/tae/logtail/snapshot.go`](../../pkg/vm/engine/tae/logtail/snapshot.go) 和 [`pkg/vm/engine/tae/db/gc/v3/exec_v1.go`](../../pkg/vm/engine/tae/db/gc/v3/exec_v1.go) 已能让表级 Snapshot 阻止源对象被 GC。

6. **Parquet 基础**

   [`pkg/iceberg/write`](../../pkg/iceberg/write) 已有 MO Batch 到 Parquet、row group 和统计信息的实现。生命周期可复用类型转换和 row-group 逻辑，但必须新增流式 `ArchiveStore` 输出，不能用可能把整文件留在内存中的缓冲输出。

### 5.2 不能直接复用

1. **现有 Merge 事务条目**

   过期行在 transfer map 中是 `NoTransfer`。现有 [`mergeobjects.go`](../../pkg/vm/engine/tae/tables/txnentries/mergeobjects.go) 在转移并发 tombstone 时，若目标行为 `NoTransfer` 会报错。生命周期必须定义“并发删除发生在过期行上”时是重试、跳过还是同步归档删除，不能把现有 Merge Entry 原样复用。

   当前 `MergeTaskHost.GetCommitEntry()` 的返回类型固定为 `*api.MergeCommitEntry`。`LifecycleRewriteHost` 可以把它作为 `DoMergeAndWrite` 所需的**进程内输出载体**，随后将 created objects/transfer table 转入 `LifecycleCommitEntry`；不能把这个临时对象直接作为最终 RPC，也不能调用现有 `NewMergeObjectsEntry` 提交。若实现时抽出中性的 `RewriteResult`，只允许做接口解耦，不改变普通 Merge 行为。

2. **TN 内存 scope 作为 fence**

   scope 只覆盖一个 TN 进程，不持久化，不覆盖 CN 手工 Merge、DDL 和 TN 重启。它只能减少冲突，不能证明提交安全。

3. **当前 ObjectStorage 接口**

   [`pkg/fileservice/object_storage.go`](../../pkg/fileservice/object_storage.go) 只有 List、Stat、Exists、Write、Read、Delete，没有 storage class、version ID、restore/thaw、条件删除和 provider checksum。不能用它承诺深归档语义。

4. **直接 SoftDeleteObject**

   只退休 Base Object 不会自动清理隐藏索引表，也不一定产生 CDC 所需的逐行 delete。生命周期不能绕过 SQL 删除的依赖语义。

5. **把固定 ObjectStats 布局继续加字段**

   `ObjectStats` 是固定二进制布局。生命周期索引是可重建派生数据，不应为它改变所有对象的固定格式和兼容边界。

## 6. 方案选择

### 6.1 方案 A：直接修改普通 Merge

做法：Merge 读取数据时判断 TTL，顺手丢弃或归档过期行。

拒绝原因：

- 生命周期策略会进入最核心、最高频的 Merge 路径；
- 每次 Merge 都需要查询 policy、archive provider 和任务状态；
- Archive 外部 I/O 会拉长 TN 事务和 Merge task；
- 失败语义从“内部重写失败”变成“跨云副作用 + 重写失败”；
- 难以保证关闭 Feature 后普通 Merge 行为完全不变。

### 6.2 方案 B：复制一套 Merge

做法：复制 merger、writer、transfer 和 commit 代码，改造成生命周期 Merge。

拒绝原因：

- 排序、对象大小、schema 演进、tombstone 转移和 writer 修复需要维护两份；
- 两套代码会逐渐产生数据格式和正确性差异；
- 测试面几乎翻倍。

### 6.3 方案 C：独立 Lifecycle Engine，组合稳定原语

采用该方案：

- 独立控制面、Job、Manifest、ArchiveStore 和提交协议；
- 新 `LifecycleRewriteHost` 组合 `DoMergeAndWrite`；
- 普通 Merge 策略零改动；
- 通用 scope dispatcher 只增加可过期 reservation；
- 正确性由新的事务 Entry 和 CAS 负责。

这是“加一层并复用基础能力”，而不是“把 Feature 塞进 Merge”。

## 7. 总体架构

```text
  SQL Policy / Binding
           |
           v
  Lifecycle Object Index <--- Object create/delete logtail + one-time backfill
           |
       due rows only
           v
  Policy Scanner / Fair Scheduler ---> one bounded child Job per object group
           |
           +--> persistent source pin
           +--> optional TN scope reservation
           |
           v
  LifecycleRewriteHost
     | expired rows                 | live rows
     +--> Discard / ArchiveSink     +--> mergesort.DoMergeAndWrite
                |                            |
                v                            v
       Parquet staging payload       new TAE Object + transfer map
                \                            /
                 \                          /
                  v                        v
             verify payload and dependency deltas
                           |
                           v
              conditional distributed transaction
              - publish Manifest
              - register new objects
              - retire exact source objects
              - update index/CDC/FK dependencies
                           |
                           v
                 reconcile + release source pin
```

组件职责：

| 组件 | 职责 |
|---|---|
| Policy Manager | 解析策略、继承规则、版本和 provider capability |
| Object Indexer | 为活动对象建立生命周期列 min/max/null 和 next action 派生索引 |
| Scanner | 只扫描到期索引项，生成 dry-run 或执行计划 |
| Coordinator | 建立 source pin，将 batch 拆成独立 child Job，管理引用计数 |
| Job Executor | 读取固定 Snapshot、输出活动对象/归档文件/依赖 delta |
| ArchiveStore | 提供版本化、可校验、可恢复的云归档能力 |
| Commit Handler | 执行对象级 CAS、tombstone 协调和原子 retirement |
| Reconciler | 处理接管、commit unknown、staging orphan 和 provider eventual consistency |
| Purger | 按不可逆删除协议清理 Archive Payload |
| Restore Service | thaw、验证、写隐藏 staging 表并原子发布为新表 |

Lifecycle Job Executor 运行在 CN TaskService worker，而不是塞入 TN 后台 Merge task：CN 负责跨云归档、Parquet 和正常分布式事务；TN 只提供 scope reservation、对象级 PrepareCommit 校验和 TAE transaction entry。长时间 copy/export 不保持 MO 事务，只有建 pin 和最终 publish/retire 是短事务。

## 8. 元数据模型

所有系统表均按 account 分区或聚簇，并具有 schema version。名称为设计名，最终 DDL 可按 MO 系统表规范调整。

### 8.1 Policy 与 Binding

`mo_lifecycle_policies`：

- `policy_id`
- `account_id`
- `name`
- `action`：`EXPIRE` / `ARCHIVE`
- `column_id`、`column_type`
- `interval`
- `null_semantics`
- `archive_profile_id`
- `purge_after`
- `cdc_mode`
- `version`
- `state`

`mo_lifecycle_bindings`：

- `table_id`
- `policy_id`
- `policy_version`
- `binding_version`
- `table_schema_version`
- `lifecycle_generation`
- `commit_seq`
- `next_scan_at`
- `state`

所有影响生命周期语义的 DDL、Policy 变更和显式取消都增加 `lifecycle_generation`，只增不减。

Policy 可以定义在 account、database 或 table；优先级为 `table > database > account`。继承结果在建表或上级 Policy 变更时物化为每表一条 effective binding，日常 Scanner 不逐层解析继承，也不遍历所有数据库。`mo_catalog` 等系统表默认禁止绑定。

生命周期表达式限定为确定性的单列时间边界：

```text
lifecycle_column < fixed_cutoff
```

不允许 UDF、`now()` 以外的非确定函数、跨表子查询或任意布尔表达式进入 Object Index。每次 scan 先固定 UTC `evaluation_ts` 和 cutoff，Job 重试继续使用同一个值，避免边执行边移动边界。

### 8.2 Object Index

`mo_lifecycle_object_index` 每个当前对象一行：

- account/database/table/object ID；
- 对象 create/drop version 或 fingerprint；
- policy/binding version；
- 生命周期列 `min_value`、`max_value`、`null_count`；
- `rows`、`origin_size`；
- `next_action_at`；
- `state`：`ACTIVE`、`CLAIMED`、`OBSOLETE`；
- `index_epoch`、`last_verified_at`。

该索引按 `(next_action_at, account_id)` 聚簇。它只用于发现候选，不是正确性真相：

- 索引落后时只会晚做或多产生一个失败 Job；
- 最终事务必须重新验证源对象的真实 MVCC 身份；
- 对象被普通 Merge 替换后，旧索引行标记 `OBSOLETE`，新对象计算新 deadline。

建立方式：

1. Policy 启用时做一次限速 footer backfill；
2. 后续消费对象 create/drop 变更增量维护；
3. 生命周期列是 sort key 时直接使用 `ObjectStats.SortKeyZoneMap()`；
4. 不是 sort key 时只为新对象读取一次 footer；
5. Policy 版本变化后按新版本增量重建。

增量消费者持久化 logtail checkpoint。重启后从 checkpoint 重放；检测到 logtail gap 时，按 account/table shard 限速对账当前对象列表。后台还要做分片轮转 reconciliation，保证“漏事件”不会让新对象永远没有 deadline，但它扫描的是对象目录和 footer 缺口，不是每天读取全部业务数据。

支持的生命周期列首期及正式默认范围为 `DATE`、`DATETIME`、`TIMESTAMP` 和显式声明为 epoch 的整数列。ZoneMap 对候选发现可以保守；只有能证明 `max < cutoff` 时才走整对象快速路径，否则一律按 Mixed Object 读取验证。

### 8.3 Job、Dataset 和 Payload

`mo_lifecycle_jobs` 记录：

- `job_id`、`coordinator_id`
- exact source object set 及 digest
- `source_snapshot_ts`
- `cutoff`、predicate digest
- policy/binding/schema/generation
- executor、executor epoch、attempt
- 状态、deadline、progress、错误
- created object / dataset / dependency delta 摘要

TaskService 分配新 executor 后，runner 必须先用 TaskService epoch 条件认领 `mo_lifecycle_jobs`，成功写入 authoritative `executor_epoch` 后才能产生任何外部 key。TaskService epoch 负责“谁应执行”，Job row CAS 负责“谁还能发布”；两者不能合并成一个内存判断。

一个 child Job 对应一个独立 `archive_dataset_id`，不以源 table ID 作为永久身份，也不与其他 Job 做 all-or-nothing 发布。面向用户的月/年归档视图由 collection/super-manifest 聚合多个 Dataset，不重写其 payload。

`mo_archive_manifests` 记录 schema digest、payload root、行数、min/max/null、压缩、KMS、provider version 和状态。

`mo_archive_payloads` 每个不可变文件一行，记录 key、content hash、provider version ID、etag、storage class、字节数、验证和删除状态。

`mo_archive_references` 用显式边表达 Restore、Backup、DR 等对 dataset/payload 的引用；Legal Hold 使用独立表，不能伪装成一个很大的 retention 值。

## 9. 调度和规模

### 9.1 不做全库每日扫描

每天扫描全部数据和 footer 的成本不可接受。本设计的日常查询是：

```text
WHERE next_action_at <= now()
  AND state = 'ACTIVE'
ORDER BY next_action_at
LIMIT bounded_batch
```

以 128 MiB 平均对象估算，1 TiB 表约 8192 个 Object Index 行，不是 1 TiB 数据扫描，也不是每天为每张表建立一个新分区。对象 Index 的维护成本与对象产生/退休数量相关，而不是与全库行数相关。

### 9.2 一组对象一个原子 Job

一次 policy scan 的关系是：

```text
one scan
  -> one coordinator
  -> N bounded child jobs
  -> one archive dataset per child
  -> one publish/retire transaction per child
```

一个 Job 不跨多个大表，也不把整个 TB 表放进一个 all-or-nothing 事务。

保守默认上限：

- source bytes：1 GiB；
- source rows：100 万；
- source objects：64；
- archive target file：256 MiB；
- output files：16；
- output ObjectIO 数量必须小于 `api.NoTransfer` 保留值；
- 单 account 并发 Job：2；
- 以上均可配置，但扩大上限必须通过内存和 transfer-map 基准。

调度采用 account/table 公平队列，并分别限制：

- source read bytes/s；
- active ObjectIO write bytes/s；
- archive write bytes/s；
- 内存、spill 和 in-flight bytes；
- Job、payload、orphan、delete backlog 数量。

### 9.3 与普通 Merge 的关系

普通 Merge 保持原逻辑：

```text
普通 Merge scheduler
  -> 原候选算法
  -> 原 Level/Overlap/Small
  -> 原目标大小和资源估算
  -> 原 task
```

新增的是通用 dispatcher reservation：

- Lifecycle 在输出外部文件前，用 exact source object scopes 获取短租约；
- dispatcher 的 conflict check 同时检查 running task 和未过期 reservation；
- reservation 包含 owner、job epoch、lease deadline，只能按 owner/epoch 续租和释放；
- reservation 丢失、过期或 TN 重启都不影响正确性；
- reservation map 为空时保留快速路径；
- 普通 Merge 不读取 policy，不访问 Lifecycle Catalog，不改变候选策略。

CN 手工 Merge 不受 TN 内存 reservation 完全覆盖，仍可能竞争；最终对象 MVCC CAS 保证只有一个事务成功。

reservation 通过新的 TN lifecycle scope RPC 获取、续租和释放。它不占住一个工作线程，不持有事务或表锁。连续冲突的 Job 缩小 object set、指数退避并暴露 conflict metric；系统宁可延迟归档，也不能为了追求进度放松最终 CAS。

## 10. 数据执行路径

每个 Job 固定：

- `source_snapshot_ts`
- exact source objects
- `cutoff`
- schema/policy/binding/generation
- archive profile
- dependency plan

`LifecycleRewriteHost.LoadNextBatch`：

1. 在固定 Snapshot 读取源批次；
2. 应用该 Snapshot 已可见的 tombstone；
3. 计算生命周期谓词；
4. 将过期行加入 overlay delete mask；
5. TTL 将过期行交给 `DiscardSink`；
6. Archive 将过期行交给 `ArchiveSink` 和 DependencyDeltaSink；
7. 返回原 Batch + 合并后的 delete mask 给 `DoMergeAndWrite`；
8. `DoMergeAndWrite` 只对存活行排序、写新 ObjectIO 并建立 transfer map。

所有 Sink 都是流式、有上限且可取消的。任一 Sink 失败，整个 Job 不进入 `VERIFIED_NOT_PUBLISHED`。

Job 在进入 `VERIFIED_NOT_PUBLISHED` 前验证：

```text
source_visible_rows = live_rows + expired_rows
live transfer mappings = live_rows
dependency input rows = expired_rows
archive manifest rows = expired_rows        # Archive only
source/expired streaming digest = verified manifest digest
```

Archive Payload 必须通过 `ArchiveStore` 重新打开并完整读取校验，不能只相信上传成功、ETag 或 executor 本地 hash。未来只有 provider 给出可证明等价的不可变 checksum 契约后，才允许把全量重读改成服务端校验。

### 10.1 整对象快速路径

条件：`max_value < cutoff`，且 ZoneMap 对该列是精确可证明的。

TTL：

- 没有隐藏索引、行级 CDC、FK cascade 或插件依赖时，不读取 payload，最终事务直接退休对象；
- 存在行级依赖时仍需扫描一次，以生成依赖删除 delta。

Archive：

- 即使整个对象过期，也要读取已经应用 tombstone 的逻辑行；
- 输出 Parquet 并校验后退休源对象；
- 不是简单复制原 ObjectIO，因为归档格式、schema contract、加密、已删除行和长期兼容性不同。

### 10.2 Mixed Object 路径

条件：`min_value < cutoff <= max_value`，或者统计信息不能严格证明整对象过期。

系统只读一次：

- 过期行写 Archive/Discard 和依赖 delta；
- 存活行写新 TAE Object；
- 最终原子替换旧对象。

这确实有读写放大，但只发生在边界混合 Object，不是每天把全表重写一遍。建议用户按生命周期列作为 clustering/sort key，MO 也可在 dry-run 中展示 `alignment_ratio` 和预计重写放大。

### 10.3 无过期行和全过期

- 读取验证后无过期行：Job 成功结束，不产生 commit；
- 全过期且需要重写：新活动 Object 列表可以为空；
- 新插入的迟到数据产生新 Object，由后续增量索引再次捕获；
- `NULLS KEEP` 为默认语义；显式 `NULLS EXPIRE` 才允许过期 NULL。

## 11. Archive 格式与存储

### 11.1 文件格式

统一使用：

- Apache Parquet；
- ZSTD；
- 目标文件默认 256 MiB，可在 256–512 MiB 范围调优；
- 有稳定 MO column ID/field ID、完整 MO type 和 schema version；
- 文件级和 row-group 级 min/max/null count；
- content hash 和 Manifest Merkle/root digest；
- KMS key ID、加密版本和 provider object version。

每个 Dataset 还写一个不可变、版本化并签名的 Archive Container sidecar，包含 dataset ID、源身份、schema、payload 列表和 root。MO Catalog 的 Manifest row 是可见性真相，sidecar 在最终事务前只是 staging；它同时为 Catalog 灾难后的审计/重建提供外部证据。Backup/DR 必须一起保护 Lifecycle Catalog、Reference、Legal Hold 和 Container 索引，不能只备份 Parquet payload。

不使用 CSV：它丢失类型、NULL、时区、Decimal 精度、嵌套结构和高效谓词下推信息。

不每天固定生成一个小文件，也不每月重写一个月全部 payload。一个 child Dataset 内可以流式累积到目标文件大小；历史目录的月/年视图通过 super-manifest 合并，不复制 payload。

当前 `pkg/iceberg/write` 尚未覆盖的 MO 类型在补齐编码器前必须被 Policy admission 拒绝；不能把类型降级成字符串。正式声明支持 Archive 时，所有可持久化的内置 MO 类型要么有无损编码，要么在产品文档中明确列为不支持。

### 11.2 ArchiveStore

新增独立的能力接口，不修改所有 FileService backend 都必须实现的 `ObjectStorage`：

```text
ArchiveStore
  PutStream(immutable_key, checksum, storage_class)
  Open(version)
  StatVersion()
  VerifyChecksum()
  RequestRestore()
  RestoreStatus()
  DeleteVersion(condition)
  Capabilities()
  CostProfile()
```

Capability 至少声明：

- 是否可立即读取；
- 是否需要异步 thaw；
- restore latency；
- 是否支持 version ID 和条件删除；
- storage class；
- 最小保存时长、提前删除费用、最小对象大小；
- checksum、server-side copy 和 KMS 能力。

逻辑上只有一个 `ARCHIVED` 状态。标准对象类和深归档类只是不同 Archive Profile：

- `ONLINE_ARCHIVE`：可直接读 Parquet，恢复较快；
- `RESTORE_REQUIRED_ARCHIVE`：先由 provider thaw，恢复慢且可能收费。

这不是再造 `COOL/COLD` 业务状态，Policy、Manifest、引用和删除协议完全相同。

### 11.3 不可变 key

key 至少包含：

```text
account / archive_dataset_id / job_id / executor_epoch /
object_group_digest / file_sequence / content_hash
```

禁止覆盖和复用旧 key。重复上传相同内容可以由 Reconciler 识别，但 stale runner 不能写入新 runner 的对象。

## 12. Source Pin 与 GC

长期只用表级 Snapshot 会在慢归档期间 pin 住这张表上与本 Job 无关的 Merge 旧版本，TB 表不能接受。因此正式实现新增 `mo_lifecycle_source_refs`，按 exact source object 保存：

- account/database/table/object identity；
- object create version/fingerprint；
- `source_snapshot_ts`；
- job ID、executor epoch；
- ref generation、state、deadline。

deadline 用于告警和接管，不能像 lease 一样自动让 source ref 失效。只有确认 Job terminal/commit 结果后的状态迁移才能释放 GC 引用。

TAE GC 在现有 Snapshot/PITR 判断之外查询已加载的 lifecycle source-ref set。建 pin 使用“宽保护桥接到精确保护”的协议：

1. Coordinator 在同一事务创建 Job、exact source refs 和系统管理的表级桥接 Snapshot：

   ```text
   mo_snapshots.kind = 'lifecycle'
   level = 'table'
   ts = source_snapshot_ts
   ```

   `source_snapshot_ts` 是该事务的真实 Snapshot TS；事务先验证 exact source objects 在此时刻可见且 fingerprint 匹配，任一对象已被 Merge/DDL 替换就整体放弃并重选，不能给不存在或错误版本的对象补 ref。

2. 等待 GC/logtail 消费端确认 `source_ref_generation` 已可见；
3. 只有收到确认后，才删除表级桥接 Snapshot；
4. Job 使用 exact refs 继续读取和归档；
5. 若确认超时，Job 不读取源对象，保留或安全删除桥接 Snapshot 后重试。

这使“引用创建到 GC 可见”的窗口由现有 Snapshot 覆盖，同时让长 Job 最终只 pin 精确对象。Phase 0 可以先保留桥接 Snapshot 完成安全原型，但 TB 级 Preview/GA 前必须启用 exact refs，不能长期用整表 Snapshot 代替。

释放规则：

- publish/retire 已确定成功；
- 或明确 cancel/unbind；
- 或 terminal failure 完成 staging cleanup；
- commit unknown 必须先 reconcile；
- exact object 的最后一个 Job 引用结束。

最终事务只把 exact source ref 标记为 `RELEASE_PENDING`。异步 Reconciler 确认提交状态后再删除；允许短暂多 pin，禁止提前释放形成 GC 窗口。

两条 GC 谓词必须独立：

```text
Source TAE Object deletable =
  dropped from active table
  AND no Snapshot/PITR/Branch/Backup/ISCP reference
  AND no lifecycle exact-object source reference
  AND existing TAE GC watermark allows deletion

Archive Payload deletable =
  purge_eligible_at reached
  AND no archive/restore/backup/DR reference
  AND no legal hold
  AND grace period reached
  AND payload is irreversibly marked DELETING
```

## 13. 原子提交协议

### 13.1 为什么需要新协议

外部 Parquet 上传不能加入 MO 事务，也不能依靠 TaskService epoch 撤销。正确顺序必须是：

```text
write immutable staging
  -> full verification
  -> VERIFIED_NOT_PUBLISHED
  -> short conditional MO transaction
  -> PUBLISHED + source retired atomically
```

新增：

- `api.LifecycleCommitEntry`
- `OpCommitLifecycle`
- CN `CommitLifecycle`
- TN `HandleCommitLifecycle`
- TAE `lifecycleObjectsEntry`

最终 wire/事务语义不能复用或扩展 `MergeCommitEntry`。前述 `DoMergeAndWrite` 进程内适配不属于最终提交协议。

### 13.2 最终事务内容

同一个正常分布式事务中：

1. 条件更新 Job：

   ```text
   job_id == captured job
   executor_epoch == current epoch
   state == VERIFIED_NOT_PUBLISHED
   ```

2. 条件更新 Binding 的 `commit_seq`（不能只做 Snapshot read）：

   ```text
   policy_version == captured
   binding_version == captured
   lifecycle_generation == captured
   table_schema_version == captured
   commit_seq == captured
     -> commit_seq = captured + 1
   ```

   这会与并发 DDL/Policy Job 形成真实的 write-write conflict；条件影响行数不是 1 时立即 abort。

3. 条件发布 Manifest：

   ```text
   state == VERIFIED_NOT_PUBLISHED
   root == verified root
   all payloads == VERIFIED
   ```

4. 附加 `OpCommitLifecycle`：

   - exact source object identity、create/drop version 和 digest；
   - source Snapshot TS；
   - cutoff、predicate digest；
   - created live ObjectStats；
   - transfer table；
   - dependency delta locations；
   - archive dataset ID 和 root；
   - job ID、executor epoch、schema/policy/generation。

5. TN PrepareCommit 验证：

   - 每个源对象仍是当前可见且版本完全相同；
   - 没有遗漏需要处理的并发 tombstone；
   - created object 和 transfer table 完整；
   - 依赖更新计划完整；
   - 输出对象数量不与 `NoTransfer` sentinel 冲突。

6. 原子提交：

   - 注册新活动对象；
   - 退休 exact source objects；
   - 转移 live-row tombstone；
   - 提交隐藏索引、CDC 和约束 delta；
   - 发布 Manifest 和 Job success。

任一条件失败，整个事务 abort，不能出现“Manifest 已发布但源数据还在”或“源对象已退休但 Manifest 不可用”。

### 13.3 Commit unknown

网络超时时不能根据 executor 本地状态判断成功或失败。Reconciler 按以下事实判断：

- Manifest 是否已按 dataset/root 发布；
- source object set 是否已被该 `job_id` 的 commit 退休；
- created objects 是否已注册；
- Job epoch 和 terminal state；
- dependency delta 是否提交。

确认未提交才允许重试；确认已提交只做后处理；无法确认时继续持有 source pin 和 staging，不得猜测。

## 14. 并发语义

### 14.1 普通 Merge

- Lifecycle reservation 优先降低重复选择；
- reservation 丢失时，普通 Merge 和 Lifecycle 都可以执行；
- 最终对 exact source object 做 MVCC CAS；
- 一个提交成功，另一个 abort 并重建 Object Index；
- 不修改普通 Merge 的数据选择和结果。

### 14.2 INSERT 与迟到数据

新写入产生新对象，不影响已经捕获的 source object set。迟到旧数据会计算出已到期的 `next_action_at`，进入下一轮 Job。

`REJECT EXPIRED ON INSERT` 可以作为独立 Policy 选项，不能成为后台 TTL 正确性的前提。

### 14.3 UPDATE/DELETE 与 Tombstone

Job 记录 `source_snapshot_ts`，最终事务检查从 Snapshot 到 Prepare 之间的新 tombstone：

- tombstone 指向存活行：按 transfer map 转移到新 TAE Object；
- TTL 中 tombstone 指向已过期行：无需 transfer，确认不会产生依赖残留；
- Archive 中 tombstone 指向已导出的过期行：本次 Job abort 并重导出，避免归档复活用户在导出期间删除的行。

后续如果引入 Archive Delete Vector，可优化为不重导 payload，但不能在没有该能力时静默忽略并发删除。

### 14.4 DDL、DROP 与 Policy 变更

不持有数小时表锁。所有相关操作增加或改变 schema/lifecycle generation：

- ALTER schema/index；
- DROP/TRUNCATE/REORGANIZE；
- DROP TABLE/DATABASE/ACCOUNT；
- Policy bind/unbind/change。

最终事务 CAS generation。DROP 默认：

1. 将未发布 Job 标记 cancel；
2. 使 generation 失效；
3. 有界等待 executor 停止并释放 reservation；
4. source pin 在 terminal reconcile 后释放。

产品可另加 `DROP ... CASCADE ARCHIVE JOBS`，但默认不能让旧 Job 在表已改变后继续发布。

## 15. 索引、CDC、外键和插件

这是生产实现不能绕过的部分。

### 15.1 隐藏索引表

MO 的普通/唯一索引可能存储在隐藏表中。只 SoftDelete Base Object 会留下索引行，甚至造成唯一键永远冲突。

新增 `LifecycleDependencyHandler`：

```text
Plan(table_def, policy)
ConsumeExpiredRows(batch)
FlushDelta()
Validate()
Commit(txn)
Rollback()
```

内置普通索引和唯一索引 handler 复用 SQL `multi_update` 的 key projection 规则，但输出有上限、可 spill 的 dependency delta，由最终事务一起提交。

Delta 必须携带隐藏索引行的精确身份或等价 MVCC 条件，不能只按 index key 做无条件删除；否则归档期间并发插入的同 key 新行可能被误删。唯一性检查、Base Row retirement 和隐藏表删除使用同一个 commit timestamp。

整对象 O(1) retirement 只有在 Dependency Plan 明确证明无行级消费者时才能使用。

### 15.2 CDC

Policy 必须声明 CDC 语义：

- 表没有 active CDC：无额外动作；
- `CDC ROW_DELETE`：对已过期行生成 CDC 可消费的逐行删除，正式默认；
- `CDC RANGE_RETIRE`：只有 MO CDC 协议和下游显式支持范围 retirement event 时才可选。

存在 active CDC 却没有兼容 handler 时，Policy 绑定失败，不能静默让下游继续保留已过期数据。

`ROW_DELETE` 事件与 Base Object retirement 使用同一个 commit timestamp；Reconciler 必须能以 `job_id` 证明事件已提交，不能在事务外补发后直接标记成功。

### 15.3 外键

- 默认 `RESTRICT`：过期父行仍被引用时 Job 失败并给出计数；
- 显式 `CASCADE`：Dependency Planner 生成有上限的跨表 DAG，并在同一事务提交；
- 不能在 engine 层绕过 SQL 外键语义。

### 15.4 Fulltext、Vector 和外部插件

每种插件必须注册 lifecycle handler。未注册的索引类型使 Policy admission 失败。正式交付前，当前产品声明支持的所有内置索引类型必须有 handler 或明确禁止与 Lifecycle 同时使用，不能把数据不一致留给用户发现。

## 16. Archive 发布、引用和删除

### 16.1 状态机

Job：

```text
PLANNED -> PINNED -> RUNNING -> VERIFIED_NOT_PUBLISHED
        -> COMMITTING -> SUCCEEDED
        \-> RETRY_WAIT / CANCELING / FAILED_TERMINAL
```

Dataset：

```text
STAGING -> VERIFIED_NOT_PUBLISHED -> PUBLISHED
                                      |
                                      v
                                  DELETING -> PURGED
```

Payload：

```text
UPLOADING -> VERIFIED -> DELETE_INTENT -> DELETING -> DELETED
```

### 16.2 删除所有权

`PURGE AFTER INTERVAL 7 YEAR` 的时间基准是 lifecycle column，不是“归档成功后再保存七年”。对一个 payload：

```text
purge_eligible_at =
  max(
    max_lifecycle_value_in_payload + purge_interval,
    published_at + provider_minimum_storage_duration
  )
```

使用 payload 中的最大业务时间，保证其中每一行都达到最短保存期。Writer 按 purge deadline bucket 组文件，避免一个很新的行让大量老数据长期无法清理。该语义只是 minimum retention eligibility；“到七年必须物理删除”的 maximum-retention 合规模式需要同时约束 Snapshot、PITR、Backup、DR 和 Legal Hold，作为单独策略提供。

不可变 key 只能防止重复覆盖，不能防止 stale runner 删除对象。Purger 必须先在 MO 中原子执行：

```text
references == 0
AND purge_eligible_at reached
AND legal_hold == false
AND grace period reached
AND state == VERIFIED
  -> state = DELETE_INTENT / DELETING
```

进入 `DELETING` 后：

- 状态不可撤销；
- 禁止新增 reference；
- 禁止取消后重新引用同一个 key；
- 如需恢复保留，只能复制到新 immutable key；
- provider 支持 version ID 时删除具体版本；
- stale/new runner 重复 Delete 才是安全幂等；
- 所有 payload 确认删除后，Manifest 才能进入 `PURGED`。

## 17. Restore

恢复是 TaskService 异步 Job：

1. 用户指定 archive dataset 和必须有界的时间/业务谓词；
2. Manifest min/max 先裁剪 payload；
3. 深归档 Profile 先 `RequestRestore` 并轮询有界状态；
4. 读取 Parquet 到按归档 schema 创建的隐藏 staging MO 表；
5. 校验行数、schema digest、文件 hash 和 Manifest root；
6. 原子 rename/publish 为新表；
7. Dataset 仍保持 `PUBLISHED`，Restore 只是新增引用和恢复记录。

默认接口：

```sql
RESTORE ARCHIVE DATASET 'dataset-id'
    WHERE order_time >= '2025-01-01'
      AND order_time <  '2025-02-01'
    INTO finance.orders_restore_202501;
```

不默认恢复到原表，原因是：

- 原表 schema 可能已经变化；
- 主键/唯一键可能冲突；
- CDC、外键和业务写入并发复杂；
- 新表更容易验证、审计和删除。

全量恢复大数据集需要显式 `FULL` 和容量/费用确认。

## 18. 故障恢复与所有权

每种资源都有唯一 owner 和 terminal cleanup：

| 资源 | Owner | 成功后 | 失败/接管后 |
|---|---|---|---|
| Bridge Snapshot | source-ref generation | GC 确认 exact refs 后释放 | 超时保留或安全重试 |
| Exact source refs | child Job/object | final reconcile 后释放 | terminal cleanup 后释放 |
| TN reservation | job + executor epoch | final txn 后释放 | lease 到期 |
| 新 TAE Object staging | child Job | commit 后转 table 所有 | orphan GC |
| Archive Payload staging | dataset + job epoch | Manifest 发布后转 dataset 所有 | staging GC |
| Dependency Delta | child Job | commit 后转目标表所有 | orphan GC |
| Restore staging table | Restore Job | rename 后转用户所有 | Drop staging |
| Published Payload | Archive Dataset | Purge 后删除 | Reconciler 继续维护 |

Staging GC 也不能只按文件年龄删除。它必须同时确认 owner Job 已 terminal、该 executor epoch 已失效、attempt deadline 与 orphan grace 均已超过、没有已发布 Manifest 引用；随后以和正式 Purge 相同的 `DELETE_INTENT -> DELETING` 协议删除。仍在上传或 commit unknown 的 key 不属于 orphan。

旧 executor 已提交给 provider 的迟到上传仍可能在第一次 Delete 后出现。因此 cleanup tombstone 不能马上删除：

- provider 有 versioning 时列举并删除该 immutable key 的全部未引用版本；
- 在 quiescence window 内持续 Stat/List，发现迟到版本就再次删除；
- `DELETED` tombstone 至少保留到最大 attempt deadline、provider request timeout 和 grace 都过去；
- provider 无法提供 version/list/一致 Stat 时，Profile 必须声明该限制并采用更长隔离窗口，不能宣称已经完成强删除。

所有外部操作必须有 deadline：

- object read/write；
- multipart upload；
- provider thaw/status；
- reservation acquire/renew；
- Job attempt；
- DROP cancel wait；
- cleanup 和 Delete retry。

禁止在等待 provider、对象 I/O、Task 接管或用户确认时持有表锁。

容量准入按最坏瞬时占用计算：

```text
peak transient bytes =
  pinned source bytes
  + rewritten live ObjectIO bytes
  + archive staging bytes
  + dependency/CDC delta bytes
  + retry/orphan allowance
```

超过 account 或 cluster budget 时不启动新 Job，而不是执行到中途再靠 OOM 或磁盘写满限流。

### 18.1 安全、租户与审计

- Archive key 必须带不可伪造的 account/dataset 身份，所有 Catalog 查询强制 tenant filter；
- 系统归档 credential 由服务管理，不复用用户 Stage 临时凭据；
- Policy 管理、Restore、Legal Hold 和 Purge 使用独立权限；
- 创建/变更 Policy、发布 Dataset、恢复、加解 Hold、进入 DELETE_INTENT 和 provider Delete 都写审计日志；
- Manifest 保存 KMS key ID 和加密版本，密钥轮换不能要求重写所有 payload；删除 KMS key 前必须检查引用；
- DROP 源表默认不删除独立 Archive Dataset；必须显式 `CASCADE ARCHIVES` 且经过 retention/hold 检查；
- Policy unbind 只停止新归档，不删除已发布 Dataset。

## 19. SQL、可解释性与成本

### 19.1 Dry-run

绑定和执行前支持：

```sql
EXPLAIN DATA LIFECYCLE FOR finance.orders;
```

输出：

- effective policy 和来源；
- due object/row/bytes；
- whole-object 与 mixed-object 数量；
- `alignment_ratio`；
- 预计活动表回收字节；
- 预计 active rewrite、archive write、index/CDC write 字节；
- provider storage/restore/early-delete 费用；
- Snapshot/PITR 可能 pin 的字节；
- 不兼容索引、CDC、FK 和类型；
- 预计 Job 数和完成时间。

若 provider 无廉价 storage class，必须明确显示：

```text
raw storage price saving: 0 or unknown
operational isolation saving: measurable but workload-dependent
```

不能把“数据从一个同价 bucket 复制到另一个 bucket”描述成存储降本。

### 19.2 运行观测

系统表和指标至少包括：

- effective policy/version/generation；
- due、claimed、running、retry bytes；
- archived、active reclaimed、source pinned bytes；
- whole/mixed/alignment ratio；
- estimated/actual rewrite amplification；
- source/index/CDC/archive bytes；
- Job progress、attempt、epoch、deadline；
- staging orphan、delete backlog、oldest age；
- Restore thaw ETA、读取字节和费用。

## 20. 代码改动边界

### 20.1 新增为主

建议新增：

- `pkg/vm/engine/tae/lifecycle`：RewriteHost、transaction entry、source validation；
- `pkg/lifecycle/catalog`：Policy/Binding/Job/Manifest/Reference；
- `pkg/lifecycle/scheduler`：indexer、scanner、coordinator、reconciler；
- `pkg/lifecycle/archive`：Parquet container、ArchiveStore、provider adapters；
- `pkg/lifecycle/dependency`：index/CDC/FK/plugin handlers；
- `pkg/pb/api`：版本化 `LifecycleCommitEntry`；
- SQL parser/planner/executor 的 Policy、TTL、Dry-run、Restore 入口。

### 20.2 对现有核心路径的最小改动

| 现有模块 | 改动 | 不改什么 |
|---|---|---|
| `tae/mergesort` | 必要时抽出可组合的小接口；优先零行为改动 | 排序、reshape、writer 和普通 Merge 调用语义 |
| `tae/db/dispatcher` | 增加有租期 external reservation 和空表快速路径 | 普通 Merge 候选和优先级 |
| `tae/rpc` | 新增 lifecycle opcode/handler | MergeCommitEntry 语义 |
| `disttae` | 新 lifecycle relation/txn 调用 | 普通 DML/Merge API |
| Snapshot frontend | 增加桥接 Snapshot `kind='lifecycle'` 的系统管理和用户隐藏规则 | 用户 Snapshot 语义 |
| TaskService | 注册 lifecycle task code/runner | epoch 的通用语义 |
| FileService | 不破坏 ObjectStorage；旁路增加 ArchiveStore | 活动 ObjectIO 读写 |
| GC/logtail | 新增 exact-object lifecycle source refs 和可见 generation ACK；新增 archive purger | Snapshot/PITR 原判定，两种删除谓词不混合 |

### 20.3 明确禁止

- 不在普通 Merge callback 中查询 lifecycle policy；
- 不让 Merge 直接调用 ArchiveStore；
- 不用 SQL Partition ID 作为 Job 主键或原子边界；
- 不在活动 TAE Object 上配置云 Bucket Lifecycle；
- 不把原始 ObjectIO 当无版本契约的七年归档格式；
- 不使用用户 Stage credential 作为系统长期归档所有权；
- 不让一个 Job 跨整张 TB 表；
- 不以 TaskService epoch 代替对象/事务 CAS；
- 不在没有 dependency handler 时直接 SoftDeleteObject；
- 不允许 stale runner 撤销 `DELETING` 或复用 payload key。

## 21. 交付阶段与正式门槛

阶段是实现顺序，不是永久阉割的产品版本。所有阶段从第一天使用同一套最终 Catalog、Job、Entry、Manifest、引用和错误模型。

| 阶段 | 实现内容 | 进入下一阶段条件 |
|---|---|---|
| Phase 0 | Object Index、dry-run、桥接 Snapshot、scope reservation、Lifecycle Entry 原型 | crash/race 下证明无错删、无提前 GC、无双发布 |
| Phase 1 | exact-object source refs、Native TTL、whole/mixed path、普通/唯一索引、CDC、DDL 并发 | 生产数据安全矩阵与 TB 级 pin 放大测试通过 |
| Phase 2 | Parquet Archive、Manifest 原子发布、标准对象类、恢复到新表 | provider fake/real 故障矩阵和成本压测通过 |
| Phase 3 | 深归档 thaw、version delete、Legal Hold、Backup/DR 引用、全部内置插件 | 完整生产验收 |

正式 Feature GA 必须完成 Phase 0–3 中与已声明能力相关的全部门槛。可以先交付受控 Preview，但不能用 “MVP” 名义绕过索引、CDC、GC、接管和删除安全。

## 22. 验收矩阵

### 22.1 正确性

- 无过期、部分过期、全部过期；
- lifecycle 列是/不是 sort key；
- NULL、时区边界、DST、精度、cutoff 等值；
- 迟到 INSERT；
- 无 PK、PK、普通索引、唯一索引；
- CDC on/off、ROW_DELETE、RANGE_RETIRE capability；
- FK RESTRICT/CASCADE；
- Fulltext/Vector/plugin admission；
- schema evolution 和归档 schema restore。

### 22.2 并发

- TN 普通 Merge；
- CN 手工 Merge；
- INSERT/UPDATE/DELETE 和新增 tombstone；
- ALTER/DROP/TRUNCATE；
- Policy change/unbind；
- Task epoch 接管；
- reservation 到期、TN restart；
- Snapshot/PITR/Branch 创建和删除。
- exact source-ref checkpoint/ACK 与 GC 同时推进。

### 22.3 Crash point

对每个点做 kill/restart：

- bridge Snapshot/source-ref 事务前后；
- GC ACK 前后和 bridge Snapshot 删除前后；
- 每个 payload multipart；
- payload complete/verify；
- 新 TAE Object 写完；
- dependency delta 写完；
- Manifest `VERIFIED_NOT_PUBLISHED`；
- final txn before/after prepare；
- commit response 丢失；
- source pin release；
- DELETE_INTENT、provider delete、PURGED；
- Restore thaw、staging load、rename。

### 22.4 资源与性能

- 10 万表/大量空表时不全库扫描；
- TB 表 Job 拆分和公平性；
- foreground P95/P99 扫描、写入和 Merge 影响；
- transfer map、Parquet writer、dependency delta 内存上限；
- provider 限流、慢读、慢写和 eventual consistency；
- orphan 和 delete backlog 不无限增长；
- 同价对象存储时真实收益不被高估。

### 22.5 滚动升级

- Lifecycle 由 cluster capability gate 控制；
- 所有 TN/CN 支持新 opcode 后才能启用执行；
- 旧节点收到新 Entry 明确拒绝，不能忽略；
- Manifest/container 有版本号和兼容读取器；
- 回滚只停止新 Job，已发布 Dataset 仍可恢复和清理。

## 23. 主要风险与控制

| 风险 | 结果 | 控制 |
|---|---|---|
| Mixed Object 比例高 | 重写放大大 | index dry-run、sort key 建议、bounded job |
| 与 Merge 持续冲突 | 重复导出、饥饿 | reservation + stable-object 偏好 + CAS/retry |
| 慢 copy 遇到 GC | 源对象消失 | persistent exact-object source refs |
| exact ref 尚未传播就释放宽保护 | GC 错删源对象 | bridge Snapshot + generation ACK + chaos test |
| 并发删除被归档复活 | 数据语义错误 | expired-row tombstone 变化时 abort/re-export |
| 隐藏索引残留 | 错查或唯一冲突 | dependency handler 同事务 |
| CDC 下游不删除 | 双边不一致 | ROW_DELETE 默认、无 handler 拒绝绑定 |
| stale runner 删除新对象 | 永久丢失 | immutable key + irreversible delete state + version delete |
| 同价云存储无字节降本 | ROI 不成立 | capability/cost dry-run，分离运维收益 |
| 深归档恢复时间不可控 | SLA 违约 | Profile 声明、异步 Restore、ETA 和容量预检 |
| 一个 Job 过大 | OOM、长事务 | rows/bytes/objects/files 硬上限 |
| 长 Job 使用整表 Snapshot | 无关旧版本被 pin | Snapshot 只做建 pin 桥接，ACK 后切到 exact refs |

## 24. 架构决策

本设计的最终选择是：

1. 不使用 SQL Partition 作为生命周期底座；
2. 不实现生命周期 `ONLINE_COLD` 状态；
3. 不修改普通 Merge 策略；
4. 重用 Merge 的流式排序/写对象原语，但使用独立 Host 和事务 Entry；
5. 用增量 Object Index 代替每日全库数据扫描；
6. 以 bounded exact object set 作为原子 Job；
7. 用桥接 Snapshot 安全建立 exact-object source refs，用对象 MVCC + Catalog CAS 决定提交；
8. Archive 用 typed Parquet/ZSTD 和独立 Manifest，不复制原 ObjectIO 充当长期格式；
9. 索引、CDC、FK 和插件是提交协议的一部分，不是后补功能；
10. Archive Payload 删除采用不可逆状态机和具体 version delete；
11. 恢复默认进入新表；
12. 分阶段实现，但正式交付以生产级闭环为唯一目标。

这套方案的主要价值是：它把 Feature 加在 TAE Object 生命周期之上，最大限度隔离普通 Merge；同时没有用“隔离”换取数据正确性漏洞。即使客户已经把活动数据放在 S3/OSS/COS，MO 仍能通过缩小活动数据集、减少后台重写和查询面，并在 provider 支持时使用更低价归档类别，实现可测量、可解释的降本。
