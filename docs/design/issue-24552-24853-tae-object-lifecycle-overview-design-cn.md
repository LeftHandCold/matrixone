# MatrixOne TAE 对象级数据生命周期概要设计

> 目标 Issue：[Native table TTL #24552](https://github.com/matrixorigin/matrixone/issues/24552)、[Tiered data lifecycle #24853](https://github.com/matrixorigin/matrixone/issues/24853)
>
> 状态：Issue #24552 / #24853 当前唯一规范概要设计；定义 Commercial GA 目标和强制门禁，不代表当前代码已经具备该能力
>
> 设计日期：2026-07-24
>
> 代码复核基线：`54b63cb8ebfc2169e457b4cab3ee09e1ac12b562`
>
> 架构决策：[以 TAE Object 而不是 SQL Partition 作为生命周期执行边界](issue-24552-24853-object-lifecycle-boundary-adr-cn.md)
>
> 本文取代早期分析稿中的全部实现、Phase 和 Go/No-Go 建议；早期文档只保留为行业调研与历史记录。

## 1. 最终结论

MO 应建设独立的 **TAE 对象级 Lifecycle Service 与 Lifecycle Rewrite Executor**，同时服务于：

- #24552：过期行从当前表中自动消失，即原生 TTL；
- #24853：过期行先写入可验证、可恢复的归档数据集，再从当前表中消失。

推荐方案的核心不是“再实现一套 Merge”，也不是“给现有 Merge 加一个过期判断”，而是：

1. 以一组有上限、精确标识的不可变 TAE Object 为一个原子 Job；
2. 用增量对象索引计算到期时间，不按天全库扫描数据；
3. 先实现只读 Planner/Object Index/Dry-run 和 Export-only Archive，用真实表验证候选选择、文件格式和成本；
4. Whole Object 直接进入有界退休协议；Mixed Object 通过 `LifecycleRewriteExecutor` 将过期行送入 `DiscardSink` 或 `ArchiveSink`，将仍存活行写回正常 TAE Object；
5. Rewrite Executor 可以复用现有 `mergesort.DoMergeAndWrite` 的读、排序、写 ObjectIO 和 transfer-map 生成能力，但不是第二套 Merge Engine，也不进入普通 Merge scheduler；
6. 新增独立、可版本化和可重放的 `LifecycleCommitIntent` / `LifecycleCommitEntry`，在一个短事务内原子完成：
   - 发布已校验的 Archive Manifest；
   - 注册新 TAE Object；
   - 退休精确的旧 TAE Object；
   - 提交 GA 支持矩阵内的普通/唯一索引依赖变更；
7. 普通 Merge 的候选选择、Level、Overlap、Small、目标大小、资源估算和调度策略全部保持不变；
8. TAE 通用多 scope reservation 只用于降低 Lifecycle 与后台 Merge 的重复重写，可以在协议原型之后实现；它不是正确性条件，最终正确性由事务 CAS 和 durable receipt 保证。

生命周期状态只有：

```text
TTL:      ACTIVE -> EXPIRED
Archive:  ACTIVE -> ARCHIVED -> PURGED
```

`ONLINE_COLD` 不属于这套 Lifecycle Service。CN 内存/磁盘缓存、远端缓存和活动 ObjectIO 的介质选择属于独立的在线放置策略。MO 不应为了一个收益不明确的 “COOL” 名称，把缓存、Merge、FileService 和查询路径一起复杂化。

该设计不依赖 SQL Partition。SQL Partition 可以继续作为用户的数据组织和路由功能，但不是 TTL/Archive 的正确性边界。

### 1.1 Commercial GA 支持矩阵

Commercial GA 是“能力边界受限但承诺生产质量”，不是 Preview 改名。GA 必须支持：

| 能力 | GA 决策 |
|---|---|
| Policy scope | 只支持 table scope |
| Lifecycle column | `NOT NULL DATE/DATETIME/TIMESTAMP`；显式单位、无溢出的 epoch integer |
| TTL | Whole Object + Mixed Object |
| 普通/唯一索引 | 专用 dependency handler 通过一致性验收后支持 |
| Direct-readable Archive | 支持 |
| Restore-required Deep Archive | 支持至少一个客户实际 provider，并通过真实故障矩阵 |
| Restore | 显式异步 thaw，恢复到独立新表 |
| Purge | minimum-retention eligibility、Legal Hold、不可逆 version delete |

首个 GA 明确拒绝：

- NULL lifecycle column；
- active CDC；
- FK 父表或子表；
- Publication/Subscription；
- Fulltext、Vector、异步索引和外部插件；
- account/database Policy 继承；
- 任意表达式、UDF 和 subquery；
- 同表恢复、透明查询时自动 thaw；
- maximum-retention 准时物理删除承诺。

拒绝能力必须在 Policy bind 和最终事务的 `dependency_fingerprint` CAS 两次检查。未来每增加一种能力，都通过独立 ADR、handler 和验收矩阵进入支持列表。

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
    PURGE ELIGIBLE AFTER INTERVAL 7 YEAR;

ALTER TABLE finance.orders
    SET DATA LIFECYCLE POLICY archive_orders;
```

系统保证：

- 满 90 天的行先写成带 schema、统计信息、校验和及加密信息的归档数据集；
- 归档校验成功且最终事务提交后，这些行才从当前表消失；
- 归档文件不要求可被 MO 直接在线查询；恢复是显式异步操作；
- 满七年只表示进入可清理时间，仍需同时满足无引用、无法律保留和 grace period；
- 恢复默认生成新表，不直接把历史数据混回正在写入的原表。

若 `saudi_archive` 是 restore-required Profile，用户查看归档数据的流程是：

```text
RESTORE ARCHIVE ... INTO new_table
  -> MO 向 provider 申请 thaw
  -> 有界轮询 RestoreStatus
  -> 解冻后导入 hidden staging table
  -> 校验并原子发布新表
  -> 用户 SELECT 新表
```

冻结文件不支持透明 `SELECT`。MO 必须在执行前展示预计恢复字节、provider 费用和 SLA；用户显式确认后才发起可能收费且不可取消的 thaw。

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

### 6.3 方案 C：Lifecycle Service + Rewrite Executor，组合稳定原语

采用该方案：

- 独立控制面、Job、Manifest、ArchiveStore 和提交协议；
- 新 `LifecycleRewriteHost` 组合 `DoMergeAndWrite`；
- 普通 Merge 策略零改动；
- 通用 scope dispatcher 只增加可过期 reservation；
- 正确性由新的事务 Entry 和 CAS 负责。

这是“加一层并复用基础能力”，而不是“把 Feature 塞进 Merge”，也不是创建新的 Merge Engine。

### 6.4 功能优先的实现切片

可以先实现可见功能，但必须区分“不会删活动数据的原型”和“开始承担数据正确性的写路径”：

| 切片 | 能做什么 | 是否允许退休活动数据 |
|---|---|---|
| Read-only Planner | 扫描表/Object 元数据、计算 cutoff、展示候选与成本 | 否 |
| Export-only | 读取候选 Object、输出 Parquet/Container、全量重读校验 | 否 |
| Protocol Foundation | Commit Intent/Receipt、exact ref、GC ACK、admission、hard budget | 只用于故障原型 |
| Whole Object | 对完全过期 Object 做 TTL/Archive | 是，协议门禁通过后 |
| Mixed Object | 过期行归档/丢弃，存活行写回 ObjectIO | 是，GA 必需 |
| Deep Archive/Restore | transition/thaw、staging restore、原子发布 | 是，GA 必需 |

Planner 可以先直接通过当前 `GetColumMetadataScanInfo` 分析一张白名单表；规模验证后再切换到增量 Object Index。Export-only 也可以先不复用 mergesort。真正退休活动数据之前，必须完成 replay、source ref、dependency admission 和资源上限，不能用“后续再优化”替代。

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
              - update supported index dependencies
                           |
                           v
                 reconcile + release source pin
```

组件职责：

| 组件 | 职责 |
|---|---|
| Policy Manager | 解析 table-scope 策略、版本和 provider capability |
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
- `archive_profile_id`
- `purge_eligible_after`
- `required_capability_version`
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

Commercial GA 只支持 table-scope Policy，一张表一条独立 Binding。account/database 继承不进入首个 GA，避免一次 Policy 变更同步更新海量表。`mo_catalog` 等系统表默认禁止绑定。

生命周期表达式限定为确定性的单列时间边界：

```text
lifecycle_column < fixed_cutoff
```

不允许用户表达式中的 UDF、`now()`、其他非确定函数、跨表子查询或任意布尔表达式进入 Object Index；当前时间只能由系统固定的 `evaluation_ts` 提供。时间语义固定为：

- `evaluation_ts` 取创建 Job 事务的 HLC timestamp，转换为 UTC 后持久化；
- cutoff 等值不算过期，谓词固定为 `value < cutoff`；
- `TIMESTAMP` 按 UTC instant 比较；
- `DATETIME` 和 `DATE` 在 Policy 创建时固定声明的 timezone/calendar 中计算，再转换为 UTC cutoff，不读取执行 Job 时的 session timezone；
- Month/Year interval 使用日历运算，月末取目标月最后一个合法日期，闰日按目标年最后合法日期；
- epoch integer 必须声明秒/毫秒/微秒/纳秒单位，并在 bind 时验证不会溢出；
- 同一 Job 的 retry 永久复用原 `evaluation_ts` 和 cutoff，重新规划才产生新值。

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

`OBSOLETE` 不是永久墓碑。Indexer 维护 `applied_logtail_lsn/index_generation`；只有对象 drop 已被持久 checkpoint 覆盖、新对象 create 已处理、所有 Scanner cursor 都超过该 generation 后，才按 account 公平、有限 batch 删除旧索引行。Indexer 不可用、版本未知或发现 corruption 时：

- 停止创建新 Lifecycle Job；
- 不影响普通 DML、查询、Merge；
- 从当前 Object catalog 按 shard 重建新 generation；
- 新旧 generation digest 对比成功后原子切换；
- 旧 generation 由独立 watermark GC 回收。

支持的生命周期列范围为 `NOT NULL DATE/DATETIME/TIMESTAMP` 和显式声明为 epoch 的整数列。ZoneMap 对候选发现可以保守；只有统计版本已知且能严格证明 whole-expired 时才走整对象快速路径，否则一律按 Mixed Object 读取验证。

Indexer 从所有 data block 聚合 object `row_count/null_count/min/max`，并校验 block row/null 总和。任何不一致都标记对象 `SCAN_REQUIRED` 并报警，不能用默认零值进入 whole-object fast path。

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

`mo_archive_datasets` 是 Dataset 可见性和生命周期状态的唯一权威行，至少记录：

- `archive_dataset_id`、account、source database/table、collection ID；
- source Snapshot、schema/policy/binding/lifecycle generation；
- lifecycle range、rows/bytes、manifest root、archive profile；
- `published_at`、`purge_eligible_at`、reference/hold summary；
- `state`：`STAGING`、`VERIFIED_NOT_PUBLISHED`、`PUBLISHED`、`PURGE_PENDING`、`PURGED`；
- dataset row version 和 access generation。

最终 publish/retire 事务必须原子写入可按 source table/range 检索的 `PUBLISHED` Dataset 行。Restore 和 Purge 以该行为事实源，不能依赖异步任务先更新 Collection 才发现 Dataset。

`mo_archive_collections` 为同一 table/policy generation 提供稳定 `collection_id`，按 lifecycle range 和 dataset shard 分页维护：

- `commit_seq`、reference summary、legal-hold state/generation；
- range min/max、dataset count、logical rows/bytes；
- page cursor、page root 和 collection root；
- collection-level Reference/Legal Hold；
- 部分 Purge 后的审计摘要；
- Backup/DR checkpoint 和 catalog compaction generation。

Restore 先按 collection range 裁剪 dataset page，再按 Manifest/payload min-max 裁剪文件，禁止一次把数万 Dataset 全部装入内存。Super-manifest 更新使用 copy-on-write page 和 root CAS，不重写已发布 Dataset。

Collection page/root 是 Dataset 权威行之上的分页聚合索引。Page 更新可以在最终事务后由幂等 Indexer 完成，但 Restore 必须同时扫描“已 `PUBLISHED`、尚未进入当前 collection root”的有界增量 Dataset；Indexer 持久化 high-watermark 并对账，不能让 collection lag 把已经从活动表退休的数据隐藏。

Collection 权威行另有 `commit_seq`、reference summary 和 legal-hold state/generation。Dataset publish、Dataset Purge CAS、Collection Reference/Legal Hold 的创建和释放都必须在同一事务条件更新并递增 `collection.commit_seq`。Publish 继承当时有效的 Collection Hold；Purge 同时断言 Dataset 与 Collection 都无 Reference/Hold。这样并发 Hold、Publish 和 Purge 必然至少有一方 CAS 失败，不需要在一个事务中展开数万个 Dataset edge。

`mo_archive_manifests` 记录 schema digest、payload root、行数、min/max/null、压缩、KMS 和 provider version；发布后内容不可变。Dataset row 持有 Manifest root 和生命周期状态，Manifest 不能独立进入与 Dataset 冲突的 `PUBLISHED/PURGE_PENDING` 状态。

`mo_archive_payloads` 每个不可变文件一行，记录 key、content hash、provider version ID、etag、storage class、字节数、验证和删除状态。

`mo_archive_references` 用显式边表达 Restore、Backup、DR 等对 dataset/payload 的引用；Legal Hold 使用独立表，不能伪装成一个很大的 retention 值。

控制面 metadata 也有独立 retention/GC：

- terminal Job 在 source ref、staging 和 commit unknown 全部收敛后，按 account/batch 归档或删除；
- Commit Receipt 至少保留到 source ref 释放、Dataset 进入终态且审计保留期结束，再将 digest 汇总进 collection audit root；
- 已 Purged Dataset/Manifest 保留有界审计摘要，不永久保留全部 payload 行；
- Collection page 只有新 root 已被 Backup/DR checkpoint 覆盖、无 cursor/reference 指向时才能回收；
- `DELETE_FAILED_MANUAL`、in-doubt Receipt 和 Legal Hold 相关 metadata 禁止自动 GC；
- metadata rows/bytes、删除 batch 和速率进入 account/cluster hard budget。

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

调度使用两类 profile：

| Profile | 适用条件 | 目标 |
|---|---|---|
| Whole-object streaming | 整对象全部过期、无未支持依赖、不需要 live transfer | 提高 source rows/objects 上限，持续形成 256–512 MiB Archive Payload |
| Mixed/dependency | 需要 live rewrite、transfer 或索引 delta | 小批次，严格控制行数、transfer 和 Prepare |

Planner 联合估算 source bytes/rows/blocks、expired rows、live rewrite bytes、archive compressed bytes、target files、tombstone 和 dependency delta。不能只用 `source bytes` 间接推断其他资源。

每个 Job 和每个 final transaction 分别设置硬上限：

| 资源 | 强制控制 |
|---|---|
| source objects/blocks/rows/bytes | scan 前估算，运行时达到任一上限立即停止追加 |
| transfer slots/bytes | 独立预算，不按 source bytes 推断 |
| new/concurrent tombstones | rows、bytes、objects 上限 |
| dependency delta | rows、bytes、files、target tables 上限 |
| archive/live output | writer memory、spill/temp bytes、文件数和文件最小/最大值 |
| created/source metadata | ref count、fingerprint bytes、entry bytes 上限 |
| WAL/RPC | Intent 序列化后 hard limit，发送前测量 |
| Prepare | deadline、CPU budget、可执行操作白名单 |
| staging/orphan | account/cluster bytes、objects 和 oldest-age 水位 |

起始实验配置可以使用 `1 GiB source / 100 万 rows / 64 objects / 16 output files / 256 MiB archive target`，但这不是 GA 承诺。GA 默认值必须由 32 B、256 B、4 KiB 行宽和真实 WAL/RPC/内存基准确定。output ObjectIO 数量始终小于 `api.NoTransfer` 保留值。

dependency delta 使用对象化 staging；final transaction 只携带有界 locations、digests 和 counts，不携带无限逐行列表。任何运行时预算超限都执行：

```text
abort current attempt
keep source visible and pinned
split/replan into smaller job
```

调度采用 account/table 公平队列，并分别限制：

- source read bytes/s；
- active ObjectIO write bytes/s；
- archive write bytes/s；
- 内存、spill 和 in-flight bytes；
- Job、payload、orphan、delete backlog 数量。

Restore、TTL、Archive、Purge 使用独立队列和最低份额，避免大租户或大 Restore 饿死小租户 TTL。TaskService 不可用时只积累 backlog，绝不转为前台同步删除。

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
6. Archive 将过期行交给 `ArchiveSink`；
7. TTL 和 Archive 都按 Dependency Plan 将过期行交给 `DependencyDeltaSink`；
8. 返回原 Batch + 合并后的 delete mask 给 `DoMergeAndWrite`；
9. `DoMergeAndWrite` 只对存活行排序、写新 ObjectIO 并建立 transfer map。

所有 Sink 都是流式、有上限且可取消的。任一 Sink 失败，Job 不进入 `READY_TO_COMMIT`，Archive Dataset 不进入 `VERIFIED_NOT_PUBLISHED`。

Job 在进入 `READY_TO_COMMIT` 前验证：

```text
source_visible_rows = live_rows + expired_rows
live and expired source-row identities are disjoint and cover every visible row
live transfer mappings = live_rows
dependency input rows = expired_rows        # plan 有 handler 时；否则为 0
archive manifest rows = expired_rows        # Archive only
expired streaming digest = verified manifest data digest  # Archive only
```

Archive Payload 必须通过 `ArchiveStore` 重新打开并完整读取校验，不能只相信上传成功、ETag 或 executor 本地 hash。未来只有 provider 给出可证明等价的不可变 checksum 契约后，才允许把全量重读改成服务端校验。

### 10.1 整对象快速路径

Commercial GA 的生命周期列必须 `NOT NULL`。整对象快速路径的保守判定是：

```text
whole_expired =
  row_count > 0
  AND null_count == 0
  AND zonemap_initialized
  AND zonemap_version_supported
  AND zonemap_type_exact_for_lifecycle_type
  AND max_non_null < cutoff
```

空对象交给普通对象清理，不产生空 Archive Dataset。ZoneMap 缺失、未初始化、截断、类型/统计版本未知、row/null count 不一致或 cutoff 等值时，一律进入 Mixed scan，不能直接退休。

最终模型若以后开放 NULL，公式必须是：

```text
NULLS KEEP:
  whole_expired =
    null_count == 0
    AND zonemap_initialized
    AND max_non_null < cutoff

NULLS EXPIRE:
  whole_expired =
    (row_count > 0 AND null_count == row_count)
    OR (
      zonemap_initialized
      AND max_non_null < cutoff
    )
```

这些 NULL 模式在独立正确性矩阵通过前不进入 GA SQL。

TTL：

- dependency plan 证明没有行级消费者时，不读取 payload，最终事务直接退休对象；
- 普通/唯一索引 handler 要求行级 key 时仍需扫描生成对象化 dependency delta；
- 未支持 CDC、FK、Publication 或插件由 admission 直接拒绝，不能在运行时跳过。

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
- 运行时发现生命周期列含 NULL，视为 schema/data contract violation，Job fail closed 并阻止后续执行。

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
  TransitionVersion(target_class)
  TransitionStatus()
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

Commercial GA Archive Profile 必须提供稳定 version identity、不可覆盖的唯一 key、可验证 checksum 和按具体 version 的删除；缺少这些能力的 provider 只能用于 Export-only 实验，不能绑定可退休源数据的 GA Policy。

逻辑上只有一个 `ARCHIVED` 状态。标准对象类和深归档类只是不同 Archive Profile：

- `ONLINE_ARCHIVE`：可直接读 Parquet，恢复较快；
- `RESTORE_REQUIRED_ARCHIVE`：先由 provider thaw，恢复慢且可能收费。

这不是再造 `COOL/COLD` 业务状态，Policy、Manifest、引用和删除协议完全相同。

`RESTORE_REQUIRED_ARCHIVE` 是 GA 能力，不是后续实验。Provider adapter 必须实现：

- 写入或 transition 到目标 storage class 的确定结果；
- event 丢失时可用 polling 收敛；
- thaw request 幂等键、状态、临时可读截止时间；
- request 不可取消时的费用和 UI 语义；
- provider throttle、unknown、not-found、KMS 和 credential rotation；
- 至少一次真实 provider 的 archive/restore/purge drill。

每个 transition/thaw request 携带 payload version ID、`access_generation` 和稳定 request ID。Provider event 与 polling 结果只有同时匹配三者才能推进 access state；旧 attempt 的迟到事件只能记审计日志，不能覆盖新 generation。

若 provider transition 产生新的 version identity，结果必须先完成 checksum/size 验证，再用 `(payload_id, old_version, access_generation)` CAS 写入新 version。CAS 成功前旧 verified version 仍是权威版本；CAS 失败的新 version 只能进入 orphan cleanup，不能被 Manifest、Restore 或 Purge 引用。CAS 成功后，新 version 成为唯一权威版本，旧 version 作为 dataset 所有的 superseded version 使用同一不可逆删除协议清理；新 version 未完成验证前禁止删除旧 version。

Deep Archive 的安全顺序固定为：

```text
upload to direct-readable staging class
  -> reopen and full verify
  -> publish Dataset + retire source
  -> transition the exact verified version to deep class
  -> poll until RESTORE_REQUIRED
```

Transition 失败时保留 direct-readable 已发布副本并重试，不能删除后重传。除非 provider checksum 契约已经单独证明与完整重读等价，否则禁止把未重读校验的 payload 直接写入不可读 deep class 后退休源数据。

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

2. 等待所有可能删除该 Object 的 owning TN/GC shard 持久确认 `source_ref_generation` 已可见；
3. 只有收到确认后，才删除表级桥接 Snapshot；
4. Job 使用 exact refs 继续读取和归档；
5. 若确认超时，Job 不读取源对象，保留或安全删除桥接 Snapshot 后重试。

ACK 不是“某个 TN 读过一次表”。一个有效 ACK 同时证明：

1. exact refs 已提交、已进入可重放 checkpoint；
2. owning TN 已加载目标 generation；
3. 该 owner 上未看见新 refs 的旧 GC cycle 已结束，或 watermark 被隔离在 ref generation 之前；
4. checkpoint/GC watermark 不会越过被保护 object；
5. TN 重启时 loader 在 GC 恢复 delete 前完成；
6. shard 迁移后新旧 owner 都完成 handoff/ACK。

ACK generation 和 owner set 持久化在 Lifecycle Catalog，可分页观察。GC source-ref filter 固定 fail-closed：

```text
loader not ready          -> retain and alarm
generation gap            -> retain and alarm
catalog/checkpoint error  -> retain and alarm
unknown ref version       -> retain and alarm
owner handoff incomplete  -> retain and alarm
all required ACKs ready   -> evaluate normal GC predicates
```

Fail-closed 作用于已经激活 lifecycle ref generation 的 account/table/shard；未启用 Lifecycle 的 scope 继续使用原 GC 路径。任何错误都不能转换成“空引用集合”。这使“引用创建到 GC 可见”的窗口由现有 Snapshot 覆盖，同时让长 Job 最终只 pin 精确对象。协议原型可以暂时保留桥接 Snapshot，但 TB 级 Preview/GA 前必须启用 exact refs。

滚动升级/降级屏障：

- 所有可能承担 GC owner 的 TN 报告 exact-ref capability 后才能 bind/execute；
- exact refs 激活后，旧 TN 不得加入 owner set；
- 停止新 Job 不等于可以降级；
- 降级前重新建立表级宽保护，等待全部 owner ACK，再释放 exact refs；
- 无法恢复宽保护时禁止降级，宁可多保留。

释放规则：

- publish/retire 已确定成功；
- 或明确 cancel/unbind 已增加 generation、fence 全部 attempt、确认没有在途 source read/commit unknown，并完成 terminal staging 所有权转移；
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
  parent dataset is irreversibly PURGE_PENDING
  AND dataset purge_eligible_at reached
  AND no archive/restore/backup/DR reference
  AND no legal hold
  AND grace period reached
  AND payload is DELETE_INTENT or DELETING
```

## 13. 原子提交协议

### 13.1 为什么需要新协议

外部 Parquet 上传不能加入 MO 事务，也不能依靠 TaskService epoch 撤销。正确顺序必须是：

```text
write immutable staging
  -> full verification
  -> dataset VERIFIED_NOT_PUBLISHED + job READY_TO_COMMIT
  -> short conditional MO transaction
  -> dataset PUBLISHED + source retired + job SUCCEEDED atomically
```

新增：

- 版本化 `api.LifecycleCommitIntent` / `LifecycleCommitEntry`
- `OpCommitLifecycle`
- CN `CommitLifecycle`
- TN `HandleCommitLifecycle`
- TAE `lifecycleObjectsEntry`
- `mo_lifecycle_commit_receipts`

最终 wire/事务语义不能复用或扩展 `MergeCommitEntry`。前述 `DoMergeAndWrite` 进程内适配不属于最终提交协议。

`LifecycleCommitIntent` 必须完整进入 `TxnCommitRequest.Payload` 和事务 WAL 可重放信息，不能引用 executor 内存。稳定身份为：

```text
intent_id = hash(job_id, attempt, lifecycle_generation, object_group_digest)
```

Intent 至少包含：

- wire/capability version；
- job ID、attempt、executor epoch；
- source Snapshot、exact object create/drop/fingerprint digest；
- schema、policy、binding、lifecycle generation；
- `dependency_fingerprint`；
- cutoff、predicate 和 immutable export intent digest；
- created live Object locations/stats/root；
- transfer locations/digest/count；
- dependency delta locations/digest/count；
- archive dataset/container/payload versions/root；
- source/expired/live rows、objects、bytes 和序列化 budget。

同一个 `intent_id` 的重复 Handle/Prepare/Replay 只能得到同一个对象注册和 retirement 结果；内容 digest 不同则返回不可重试冲突。

### 13.2 最终短事务

导出阶段只冻结 source Snapshot/object fingerprint、schema/policy generation、dependency fingerprint 和 export intent digest，不捕获 `commit_seq`。完整读取和校验完成后才开启最终短事务，读取当前 `commit_seq` 并执行：

同一个正常分布式事务中：

1. 条件更新 Job：

   ```text
   job_id == captured job
   executor_epoch == current epoch
   job.state == READY_TO_COMMIT
   ```

2. 条件更新 Binding 的 `commit_seq`（不能只做 Snapshot read）：

   ```text
   policy_version == captured
   binding_version == captured
   lifecycle_generation == captured
   table_schema_version == captured
   commit_seq == value read in this short transaction
     -> commit_seq = value + 1
   ```

   这会与并发 Lifecycle commit/DDL/Policy 形成真实 write-write conflict；条件影响行数不是 1 时只重试短事务。只有 source/schema/policy/dependency fingerprint 改变时，才废弃已校验 export 并重新规划。

3. 条件发布 Manifest：

   ```text
   dataset.state == VERIFIED_NOT_PUBLISHED
   manifest.root == verified root
   every payload.state == VERIFIED
   collection.commit_seq == captured collection commit_seq
     -> collection.commit_seq = value + 1
   ```

   Dataset row 在该事务中进入 `PUBLISHED` 并继承当前有效的 Collection Reference/Legal Hold。Collection page/root 可以异步聚合，但权威 Dataset 行必须立即可按 source/range 发现。

4. 附加 `OpCommitLifecycle`：

   - 携带完整、序列化后仍在 hard limit 内的 `LifecycleCommitIntent`；
   - transfer/dependency 使用对象化 locations，不携带无限逐行列表。

5. TN PrepareCommit 验证：

   - 每个源对象仍是当前可见且版本完全相同；
   - source Snapshot、schema/policy/binding/generation 仍匹配；
   - `dependency_fingerprint` 仍匹配 GA support matrix；
   - 没有遗漏需要处理的并发 tombstone；
   - created object、transfer 和 dependency locations 的 digest/count 完整；
   - Intent/WAL/RPC/Prepare budget 未超限；
   - 输出对象数量不与 `NoTransfer` sentinel 冲突。

   Prepare 只能执行确定、幂等、有界的本地 metadata 校验和 txn entry 注册；禁止远端 ArchiveStore I/O、全量 payload/source 扫描、provider polling、无 deadline wait。必须使用事务 request deadline，禁止 `context.Background()`；任一 collect/transfer/validation 错误原样上抛，禁止返回 `nil`。

6. 原子提交：

   - 注册新活动对象；
   - 退休 exact source objects；
   - 转移 live-row tombstone；
   - 提交支持矩阵内的隐藏索引 delta；
   - 发布 Manifest、Job success 和 durable commit receipt。

任一条件失败，整个事务 abort，不能出现“Manifest 已发布但源数据还在”或“源对象已退休但 Manifest 不可用”。

### 13.3 Replay、Receipt 与 Commit unknown

CN 将 Catalog normal writes 和每个 TN 的 Lifecycle Intent 一起放入同一个分布式事务。每个 participant 按稳定 request order 重放；Catalog row 和 TAE Object 变更都只在全局 commit 后可见。`ErrTAENeedRetry` 重建 TAE txn 时必须从原始 `TxnCommitRequest` 重放同一 Intent，不允许重新读取 mutable Job 状态拼请求。

最终事务涉及多个 participant 时必须走完整 2PC。只有 Catalog、Lifecycle Intent 和 Receipt 确认落在同一个兼容 participant，且 replay/commit-unknown 语义与 2PC 等价时，才允许 1PC 优化；任何优化都不得省略 Intent、Receipt 或 durable commit result。集群 capability 未全部升级到兼容 wire/replay 版本时，不得启动会退休源对象的 Job。

`mo_lifecycle_commit_receipts` 在同一事务插入：

- intent ID/digest；
- job/attempt/generation；
- source/created/dependency/dataset roots；
- commit timestamp；
- format/capability version。

TAE WAL/txn entry 同时记录 intent ID 和 digest，用于检测同一 Intent 的重复 replay。Receipt 对用户不可改，按 collection 分页保留并进入 Backup/DR。

网络超时时不能根据 executor 本地状态判断成功或失败。Reconciler 按以下事实判断：

- Receipt 是否按 intent ID/digest 可见；
- Manifest 是否按 dataset/root 发布；
- source/created/dependency roots 是否与 Receipt 匹配；
- 事务服务是否仍报告 in-doubt；
- TAE intent registry/WAL 是否存在冲突结果。

判断规则：

```text
receipt visible + all roots match  -> committed，做后处理
transaction durably aborted        -> retry same/replanned intent
transaction still in-doubt         -> wait/reconcile
receipt/root mismatch              -> terminal corruption alarm
```

无法确认时继续持有 source pin 和 staging，不得猜测。故障测试必须覆盖 before Prepare、during Prepare、WAL append 前后、after commit、response lost 和重复 replay，证明不重复注册对象、发布 Manifest 或提交索引 delta。

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
4. DROP 可以完成逻辑 Catalog 变更，不同步等待远端 Delete、thaw 或 commit-unknown 收敛；
5. source pin 在 terminal reconcile 后异步释放；无法判定时继续多保留物理对象。

产品可另加 `DROP ... CASCADE ARCHIVE JOBS`，但默认不能让旧 Job 在表已改变后继续发布。

## 15. 索引、CDC、外键和插件

Commercial GA 使用拒绝优先的 support matrix：

| 表能力 | GA 行为 |
|---|---|
| 无二级依赖的普通表 | 允许 |
| 普通/唯一索引 | 对应 handler 通过后允许 |
| active CDC | 拒绝 |
| FK 父表或子表 | 拒绝 |
| Publication/Subscription | 拒绝 |
| Fulltext/Vector/异步索引/外部插件 | 拒绝 |

准入执行两次：

1. Policy bind 时解析 TableDef、系统 Catalog 和外部依赖，生成版本化 `dependency_fingerprint`；
2. 最终短事务重新计算并 CAS fingerprint，阻止 Job 运行期间新增 Index、CDC、FK、Publication 或插件。

任何未知依赖、Catalog 读取失败或 handler version 不匹配都 fail closed。

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

首个 GA 拒绝 active CDC。Object retirement 不天然等价于 CDC 逐行 delete，不能让下游继续保留已过期行。

未来开放前必须用独立 ADR 定义 replayable lifecycle delete log、commit timestamp、下游 capability 和 Receipt 对账；不得在事务外补发后直接标记成功。

### 15.3 外键

首个 GA 拒绝 FK 父表和子表。`RESTRICT/CASCADE/SET NULL` 都是跨表行级语义，不能在 engine 层绕过，也不能靠整对象 retirement 推导。

### 15.4 Publication、Fulltext、Vector 和外部插件

Publication 是 MO 将数据库/表发布给其他账户并由 Subscription 消费的数据共享能力。首个 GA 拒绝已经发布的源表和 Subscription 表，避免源端归档后订阅端是否应同步删除的语义不明确。

Fulltext、Vector、异步索引和外部插件同样拒绝。未来每种能力必须注册 lifecycle handler 并通过独立一致性验收后才能进入 support matrix；未注册永远是 bind/commit 拒绝，不是运行时静默跳过。

## 16. Archive 发布、引用和删除

### 16.1 状态机

Job：

```text
PLANNED -> PINNED -> RUNNING -> READY_TO_COMMIT
        -> COMMITTING -> SUCCEEDED
        \-> RETRY_WAIT / CANCELING / FAILED_TERMINAL
```

Dataset：

```text
STAGING -> VERIFIED_NOT_PUBLISHED -> PUBLISHED
                                      |
                                      v
                              PURGE_PENDING -> PURGED
```

Payload：

```text
UPLOADING -> VERIFIED -> DELETE_INTENT -> DELETING -> DELETED
                                           |
                                           v
                              DELETE_FAILED_MANUAL
```

Payload 的删除所有权状态与 provider access 状态分列保存，不能互相覆盖：

```text
DIRECT_READABLE -> TRANSITIONING -> RESTORE_REQUIRED
                                      |
                                      v
                               THAW_REQUESTED
                                      |
                                      v
                               THAWED_UNTIL(ts)
```

Restore Job 使用 `PLANNED -> REQUESTING_THAW -> WAITING_THAW -> LOADING_STAGING -> VERIFYING -> PUBLISHED/FAILED`。Dataset 始终保持自己的 `PUBLISHED/PURGE_PENDING` 状态，不能用 `RESTORING` 覆盖删除互斥判断。

### 16.2 删除所有权

`PURGE ELIGIBLE AFTER INTERVAL 7 YEAR` 的时间基准是 lifecycle column，不是“归档成功后再保存七年”。对一个 payload：

```text
purge_eligible_at =
  max(
    max_lifecycle_value_in_payload + purge_interval,
    published_at + provider_minimum_storage_duration
  )
```

使用 payload 中的最大业务时间，保证其中每一行都达到最短保存期。Writer 按 purge deadline bucket 组文件，避免一个很新的行让大量老数据长期无法清理。该语义只是 minimum retention eligibility；“到七年必须物理删除”的 maximum-retention 合规模式需要同时约束 Snapshot、PITR、Backup、DR 和 Legal Hold，作为单独策略提供。

不可变 key 只能防止重复覆盖，不能防止 stale runner 删除对象。Purger 首先在一个 MO 事务中执行 Dataset CAS：

```text
dataset.state == PUBLISHED
AND reference_edges == 0
AND legal_hold == false
AND collection.reference_summary == 0
AND collection.legal_hold == false
AND collection.commit_seq == captured collection commit_seq
AND dataset.purge_eligible_at reached
AND grace period reached
AND no restore/reference creation in progress
AND no transition/thaw operation in progress
AND every payload.state == VERIFIED
  -> dataset.state = PURGE_PENDING
  -> collection.commit_seq = value + 1
```

Reference/Legal Hold 创建事务必须反向断言 `dataset.state == PUBLISHED`。Reference 创建与 Purge CAS 竞争时只有一个事务成功；一旦 Dataset 进入 `PURGE_PENDING`，不能新增引用、取消删除或回到 `PUBLISHED`。

Dataset 成功冻结后，每个 Payload 独立 CAS：

```text
dataset.state == PURGE_PENDING
AND payload.state == VERIFIED
AND key/version == frozen identity
  -> payload.state = DELETE_INTENT
  -> payload.state = DELETING
```

进入 Payload `DELETING` 后：

- 状态不可撤销；
- 如需恢复保留，只能复制到新 immutable key；
- GA provider 必须按冻结的具体 version identity 删除，禁止只按 key 删除“当前版本”；
- stale/new runner 重复 Delete 才是安全幂等；
- provider 永久失败或无法证明结果时进入 `DELETE_FAILED_MANUAL`，Dataset 保持 `PURGE_PENDING` 并告警；
- 所有 payload 确认 `DELETED` 后，Dataset 才能进入 `PURGED`。

Archive Container sidecar 自身也是一个带 version/hash 的 Payload，并且最后删除；在数据文件部分删除或人工处理期间，仍保留冻结 key/version 清单作为审计和重复 Delete 的事实源。

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

Commercial GA 的 Restore schema contract：

- 使用 Archive Container 保存的 snapshot schema，不自动映射到源表当前 schema；
- 所有列类型必须无损解码，任何类型/版本未知时 staging 整体失败；
- 重建在 GA support matrix 中声明支持的普通/唯一索引，并在发布前与全量重建 digest 对比；
- default 和 comment 从 snapshot schema 复制；
- 只允许 bind 时已经验证为确定性、可重建的 generated column；恢复后重新计算并校验；
- auto-increment 创建独立 sequence，起点高于恢复数据最大值，不连接源表 sequence；
- 不复制 ACL、FK、CDC、Publication/Subscription、Partition、Fulltext、Vector 或插件状态；
- staging schema、row count、data digest、index digest 全部成功后才能原子 rename/publish；
- 任一失败回滚/清理隐藏 staging，不能留下部分可见表。

Restore 在创建 reference 时反向 CAS Dataset 仍为 `PUBLISHED`；若 Dataset 已进入 `PURGE_PENDING`，Restore 必须失败。Deep Archive 的 thaw 临时副本有 provider expiry，MO 必须在 expiry 前完成有界导入，否则重新申请，不能把临时 thaw 副本作为已发布表的长期底层文件。

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
| Restore reference | Restore Job/attempt | 新表发布或明确不再 resume 后释放 | retry 继续持有，terminal cleanup 后释放 |
| Provider thaw temp copy | Provider request/version | staging load 完成后等待 provider expiry | 不主动当作永久副本，过期后重新申请 |
| Published Payload | Archive Dataset | Purge 后删除 | Reconciler 继续维护 |

Staging GC 也不能只按文件年龄删除。它必须同时确认 owner Job 已 terminal、该 executor epoch 已失效、attempt deadline 与 orphan grace 均已超过、没有已发布 Manifest 引用；随后以和正式 Purge 相同的 `DELETE_INTENT -> DELETING` 协议删除。仍在上传或 commit unknown 的 key 不属于 orphan。

旧 executor 已提交给 provider 的迟到上传仍可能在第一次 Delete 后出现。因此 cleanup tombstone 不能马上删除：

- provider 有 versioning 时列举并删除该 immutable key 的全部未引用版本；
- 在 quiescence window 内持续 Stat/List，发现迟到版本就再次删除；
- `DELETED` tombstone 至少保留到最大 attempt deadline、provider request timeout 和 grace 都过去；
- provider 无法提供 version/list/一致 Stat 时，Profile 必须声明该限制并采用更长隔离窗口，不能宣称已经完成强删除，也不能进入包含 Purge 承诺的 Commercial GA。

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
  + dependency/index delta bytes
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

### 18.2 错误状态和接管责任

| 状态 | 自动重试 | 接管者 | 是否 terminal | 是否继续 pin/block |
|---|---|---|---|---|
| `RETRY_WAIT` | 有界退避 | TaskService 新 executor | 否 | source ref 保留 |
| `COMMIT_UNKNOWN` | 只允许 reconcile | Reconciler | 否 | source ref/staging 保留 |
| `CANCELING` | 等待有界 stop | Coordinator/Reconciler | 否 | terminal cleanup 前保留 |
| `FAILED_TERMINAL` | 否 | 管理员确认/cleanup | 是 | 活动源未退休；cleanup 后释放 |
| `GC_REF_BLOCKED` | loader 修复后 | TN GC owner | 否 | fail-closed，阻止受影响 GC 和新 Job |
| `INDEX_REBUILD_REQUIRED` | 重建 generation | Indexer | 否 | 停止新 Job，不影响普通表 |
| `RESTORE_WAIT_THAW` | polling/backoff | Restore executor | 否 | Dataset reference 保留 |
| `DELETE_FAILED_MANUAL` | 否 | 运维/Purger | Payload terminal | Dataset 保持 `PURGE_PENDING` |

所有 retry 都有 attempt 和 elapsed deadline；自动重试耗尽转 terminal/manual 并告警。`COMMIT_UNKNOWN` 不允许用户强制标记失败后释放 source ref。

`COMMIT_UNKNOWN` 超过 transaction recovery SLO 后进入人工恢复队列，但状态仍保持 in-doubt：运维工具查询 transaction service、Receipt 和各 TN intent registry，只能写入“已提交”或“已确认 abort”的可证明结果，不能提供“强制释放 pin”按钮。

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
- 预计 active rewrite、archive write、index/dependency write 字节；
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
- source/index/archive bytes；
- Job progress、attempt、epoch、deadline；
- staging orphan、delete backlog、oldest age；
- Restore thaw ETA、读取字节和费用。

### 19.3 Commercial SLO、Backlog 与公平性

每个发布 SKU/Archive Profile 必须在 GA release artifact 中固定并验证：

- cutoff 到逻辑退休的 target lag 和 maximum supported backlog age；
- account/cluster 的 scan、rewrite、archive、restore、purge QPS/bytes/s；
- foreground P95/P99、普通 Merge backlog、TN memory/WAL 的自动降速和暂停阈值；
- provider 故障时 staging/orphan bytes、objects 和 oldest-age circuit breaker；
- oldest pending、pinned bytes、retry age、GC generation gap 和 delete manual 告警；
- TTL、Archive、Restore、Purge 的 weighted fairness 和最低份额。

大租户不能饿死小租户，Restore 不能饿死 TTL。TaskService/provider 不可用时数据保持活动可见，只增加 backlog。GA 门禁使用固定硬件、并发和 workload 生成可重复报告，不在设计中承诺未经测量的固定百分比。

## 20. 代码改动边界

### 20.1 新增为主

建议新增：

- `pkg/vm/engine/tae/lifecycle`：RewriteHost、transaction entry、source validation；
- `pkg/lifecycle/catalog`：Policy/Binding/Job/Receipt/Collection/Manifest/Reference；
- `pkg/lifecycle/scheduler`：indexer、scanner、coordinator、reconciler；
- `pkg/lifecycle/archive`：Parquet container、ArchiveStore、provider adapters；
- `pkg/lifecycle/dependency`：普通/唯一索引 handler 和未支持依赖 admission；
- `pkg/pb/api`：版本化 `LifecycleCommitIntent` / `LifecycleCommitEntry`；
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

Feature 使用 global/account/table 三层 capability gate。表没有有效 Binding 时：

- Object create/delete consumer 不读取生命周期列 footer；
- 普通 Merge 不查询 Lifecycle Catalog；
- 普通 DML/查询不生成 Lifecycle Intent；
- GC 只有在 cluster 已激活 exact-ref capability 时加载 source-ref filter，loader 异常仍 fail closed；
- scheduler、commit、source-ref、archive/restore、purger 分别有 kill switch，停止新工作不能跳过在途 reconciliation。

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

## 21. Capability Gates 与 Commercial GA

Gate 是内部实现和验收顺序，不是对外永久阉割的产品 Phase。所有 Gate 使用同一最终 Catalog、Intent、Receipt、source ref、Manifest、Reference 和错误模型。

| Gate | 能力 | Exit criteria |
|---|---|---|
| Gate A：Read-only | 单表 metadata Planner、Dry-run、Export-only Parquet/Container | 不退休活动数据；候选、行数和 digest 与全表基准一致 |
| Gate B：Safety Protocol | replayable Intent/Receipt、bridge/exact ref、GC ACK、dependency admission、hard budget、whole/mixed fault prototype | P0-2～P0-6 的 kill/replay/GC/超限矩阵全部通过 |
| Gate C：TTL GA Candidate | table-scope、NOT NULL 时间列、whole/mixed TTL、验收后的普通/唯一索引 | P0-1～P0-6 关闭，1/10 TiB 与 7 天 chaos/soak 通过 |
| Gate D：Archive GA Candidate | direct-readable + restore-required Profile、双层 Purge、Collection、Restore 新表、Backup/DR | P0-7 和全部 P1 关闭，fake/real provider archive/restore/purge drill 通过 |
| Gate E：Commercial GA | 支持矩阵、SLO、Runbook、升级/降级、reconciliation/audit 工具 | 客户试点完成一次 archive、deep restore、reference/hold 和 purge drill；发布评审签字 |

任何 Gate 未通过都不能通过减少故障测试、放宽 budget 或关闭 fail-closed 来换取发布进度。Commercial GA 必须完成 Gate A–E；CDC、FK、Publication、Fulltext、Vector 和插件因为明确不在 GA support matrix，不要求实现 handler，但必须证明两次 admission 都会拒绝。

### 21.1 进入实现前的详细设计包

概要设计之后必须拆分并分别评审：

1. Lifecycle Commit Intent、replay、WAL、Receipt；
2. exact source ref、GC bridge/ACK、owner handoff、升级降级；
3. dependency handler 与 GA support matrix；
4. Archive Dataset/Collection/Manifest/Payload/Reference/Purge；
5. direct/deep ArchiveStore 和 Restore staging/schema/publish；
6. Object Index generation/backfill/reconciliation/obsolete GC；
7. resource budget、scheduler、公平性和 SLO；
8. disaster recovery、audit/reconciliation 工具和 Runbook。

## 22. 验收矩阵

### 22.1 正确性

- 无过期、部分过期、全部过期；
- lifecycle 列是/不是 sort key；
- NOT NULL admission；全 NULL/部分 NULL schema 或坏数据必须拒绝/fail closed；
- 时区边界、DST、闰日、月末、epoch 单位/溢出、精度、cutoff 等值；
- 迟到 INSERT；
- 无 PK、PK、普通索引、唯一索引；
- active CDC、FK、Publication/Subscription、Fulltext/Vector/plugin 的 bind 和 final CAS 拒绝；
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
- Archive Dataset `VERIFIED_NOT_PUBLISHED`、Job `READY_TO_COMMIT`；
- final txn before/during/after Prepare；
- WAL append 前后、Intent 重放和 Receipt 写入；
- commit response 丢失；
- source pin release；
- Reference/Restore 创建与 Dataset `PURGE_PENDING` CAS 竞争；
- Payload DELETE_INTENT、DELETING、DELETE_FAILED_MANUAL、provider delete、Dataset PURGED；
- Deep Archive transition/thaw、event 丢失、polling、临时副本 expiry、staging load、rename。

### 22.4 资源与性能

- 10 万表/大量空表时不全库扫描；
- 1 TiB/10 TiB 表，32 B/256 B/4 KiB 行宽；
- whole/mixed `0%/50%/100%`，tombstone `0%/1%/20%`；
- Job 拆分和多租户/TTL/Restore 公平性；
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
- exact refs 激活后旧 TN 不得成为 GC owner；
- 降级前重建宽保护并等待所有 owner ACK；
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
| CDC/FK/Publication/插件语义缺失 | 外部或派生状态不一致 | bind + final fingerprint 两次拒绝 |
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
4. 不新增 Merge Engine；Lifecycle Rewrite Executor 可重用流式排序/写对象原语，但使用独立 Host、Intent 和事务 Entry；
5. 用增量 Object Index 代替每日全库数据扫描；
6. 以 bounded exact object set 作为原子 Job；
7. 用桥接 Snapshot 安全建立 exact-object source refs，用对象 MVCC + Catalog CAS 决定提交；
8. Archive 用 typed Parquet/ZSTD 和独立 Manifest，不复制原 ObjectIO 充当长期格式；
9. 普通/唯一索引由同事务 handler 处理；CDC、FK、Publication 和插件在首个 GA 双重拒绝；
10. Dataset 使用 `PURGE_PENDING`，Payload 使用不可逆状态机和具体 version delete；
11. direct-readable 与 restore-required deep archive 都进入 GA，恢复默认进入新表；
12. 按 capability gate 实现，但只有完整 Gate E 才能称为 Commercial GA。

这套方案的主要价值是：它把 Feature 加在 TAE Object 生命周期之上，最大限度隔离普通 Merge；同时没有用“隔离”换取数据正确性漏洞。即使客户已经把活动数据放在 S3/OSS/COS，MO 仍能通过缩小活动数据集、减少后台重写和查询面，并在 provider 支持时使用更低价归档类别，实现可测量、可解释的降本。

## 25. Commercial Review 规范闭环索引

下表表示设计要求已经进入唯一规范，不表示代码或测试已经通过：

| Review 项 | 规范落点 |
|---|---|
| P0-1 唯一权威方案 | 本文状态、历史分析稿降级、Object-boundary ADR |
| P0-2 NULL/ZoneMap fast path | 10.1 |
| P0-3 replay/Receipt/Prepare | 13 |
| P0-4 exact ref/GC fail-closed | 12 |
| P0-5 dependency closure | 1.1、15 |
| P0-6 hard budgets | 9.2、13.2、18 |
| P0-7 Dataset/Payload Purge | 16 |
| P1-1 Whole/Mixed profile | 9.2 |
| P1-2 Object Index GC | 8.2 |
| P1-3 Policy inheritance | 1.1、8.1：GA 只支持 table scope |
| P1-4 late `commit_seq` | 13.2 |
| P1-5 Collection/super-manifest | 8.3 |
| P1-6 Restore schema contract | 17 |
| P1-7 SLO/fairness | 19.3 |
| P1-8 time/calendar semantics | 8.1 |

实现进入 Commercial GA 的唯一判据是 Gate E 及其测试证据，不是“文档中已经描述”。
