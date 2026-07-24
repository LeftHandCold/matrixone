# MatrixOne TAE 对象级数据生命周期概要设计

> 目标 Issue：[Native table TTL #24552](https://github.com/matrixorigin/matrixone/issues/24552)、[Tiered data lifecycle #24853](https://github.com/matrixorigin/matrixone/issues/24853)
>
> 状态：Issue #24552 / #24853 当前唯一规范概要设计；定义 Commercial GA 目标和强制门禁，不代表当前代码已经具备该能力
>
> 方案评审结论：Conditional Go；六项 GA P0、Gate E 与实现/故障测试证据全部关闭后，才允许发布 Commercial GA
>
> 设计日期：2026-07-25
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
6. 在现有 `PrecommitWriteCmd.EntryList` 中新增有 tag、可版本化和可重放的 `LifecycleCommitEntry`，在一个短事务内原子完成：
   - Archive Job 发布已校验的 Manifest；
   - 注册新 TAE Object；
   - 退休精确的旧 TAE Object；
   - 写入 durable Receipt；
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
| 索引 | 只允许不物化隐藏索引表的 Base Table Primary Key；任何 `IndexDef.IndexTableName` 非空的普通/唯一索引及 Fulltext/Vector/异步索引均拒绝 |
| Direct-readable Archive | 必须支持；Parquet/ZSTD payload 位于 MO 可直接读取的普通 S3/OSS/COS 兼容对象存储 |
| Restore | 必须支持；异步读取已发布归档，导入隐藏 staging，校验后发布为独立新表 |
| Restore-required Deep Archive | 不属于首个 GA 门槛；作为独立可选 Archive Profile，在客户 provider 和成本收益明确后另行实现和验收 |
| Archive ownership | Archive 从属于源 table generation 和 account incarnation；DROP TABLE/DATABASE/ACCOUNT 后不再承诺 Archive Restore |
| Purge | 正常到期或 owner DROP 触发的不可逆 exact-identity delete；不包含 Legal Hold/WORM/maximum retention |
| Backup/DR | 不复制 Archive；未实现 archive-aware restore 的 Backup/PITR/Snapshot Restore/Clone/Branch/DR 操作必须 fail closed |

首个 GA 明确拒绝：

- NULL lifecycle column；
- 所有物化为隐藏表的普通/唯一索引；
- active CDC；
- FK 父表或子表；
- Publication/Subscription；
- Fulltext、Vector、异步索引和外部插件；
- Legal Hold、WORM、maximum-retention 合规承诺；
- DROP 后保留 Archive、跨账户 Archive Transfer；
- account/database Policy 继承；
- 任意表达式、UDF 和 subquery；
- 同表恢复、归档数据透明参与普通查询；
- 在独立 Deep Archive capability gate 通过前绑定 `RESTORE_REQUIRED_ARCHIVE` Profile；
- 为生命周期表执行未实现 archive-aware 语义的 Backup/PITR/Snapshot Restore/Clone/Branch/DR。

这些拒绝能力必须由每表唯一 `FeatureGuard` 行在创建能力、Policy bind 和最终事务三处 CAS，不能使用“先读当前没有依赖”的非串行化检查。未来每增加一种能力，都通过独立 ADR、协议和验收矩阵进入支持列表。

这是首个 GA 的正式产品契约，而不是暂时实现细节：

- Archive Payload 与活动 Object 物理分离，但所有权不独立于源表和租户；
- `DROP TABLE/DATABASE/ACCOUNT` 沿用 MO 的删除含义，并级联放弃相应 Archive 的恢复能力；
- 本能力不能宣传为“七年不可删除”的合规归档；用户需要此类保证时必须等待 Legal Hold/WORM/独立归档所有权能力；
- DROP 主事务只增加本地、常数级 Catalog tombstone，不等待对象存储；外部 Archive 由 system-owned Sweeper 异步删除。

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
- system Lifecycle Snapshot 会在 Job 明确收敛前延迟底层旧 Object 的物理删除，但不会让过期行重新出现在当前表；普通 Snapshot/PITR 与 Lifecycle 的组合在首个 GA 准入时拒绝。

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
- 满七年只表示正常情况下进入可清理时间，仍需满足没有进行中的 Restore/read lease 和 grace period；
- 恢复默认生成新表，不直接把历史数据混回正在写入的原表。

首个 GA 的 `saudi_archive` 是 direct-readable Profile。用户查看归档数据的流程是：

```text
RESTORE ARCHIVE ... INTO new_table
  -> MO 按 Manifest 直接读取精确 payload version
  -> 导入 hidden staging table
  -> 校验并原子发布新表
  -> 用户 SELECT 新表
```

归档 payload 不透明参与普通查询，但 Restore Service 可以直接读取它，不需要先向 provider 申请 thaw。MO 必须在执行前展示预计恢复文件数、读取字节、费用和 ETA，并对任务设置 predicate、容量和并发上限。

未来可选的 `RESTORE_REQUIRED_ARCHIVE` Profile 复用相同 Restore 接口，只在读取 payload 前增加 provider thaw、状态轮询和临时可读期限管理。该能力不影响首个 GA 的“归档数据必须能够恢复”承诺。

典型客户效果是：沙特金融客户把最近 90 天订单留在活动表，较老订单进入从属于该表的归档数据集。日常查询、Merge、统计信息和缓存不再处理全部历史；审计时按时间范围恢复到独立表。若客户需要即使误删表或租户后仍保证保存七年，本 GA 不满足该合规要求。

如果底层云只有一种对象存储价格，归档仍可降低活动表的 Merge、扫描、元数据和缓存成本，但不能承诺显著降低原始存储单价。只有归档 Profile 映射到更便宜的存储类别时，才会额外降低每 GB 成本。Parquet/ZSTD 是标准化和减少读取放大的手段，压缩率不是分层节省的主要来源。

## 3. 术语和语义边界

| 术语 | 本文含义 |
|---|---|
| TAE Object | MO 活动表中不可变的列式数据对象，不等于 S3/OSS/COS 的“一个业务分区” |
| Mixed Object | 生命周期列的值跨越 cutoff，既包含过期行又包含存活行的 Object |
| Cutoff | 本次 Job 固定的过期分界值，例如 `evaluation_time - 90 DAY` |
| TTL / Expire | 从当前逻辑表移除过期行，不保留独立业务归档副本 |
| Archive | 先创建独立归档数据集，校验并发布后再从当前表移除 |
| Archive Dataset | 具有稳定 ID、物理上与活动 Object 分离，但所有权从属于源 table generation/account incarnation 的逻辑归档数据集 |
| Manifest | 描述归档文件、schema、行数、统计、校验和、加密和引用关系的热端元数据 |
| Lifecycle Snapshot | system-owned 表级 Snapshot；在归档/重写期间阻止 TAE GC 删除其可见的源 Object |
| Attempt/Cleanup Root | 在第一次外部 PUT 前写入 system-account retained registry 的持久所有权记录；负责 staging、已发布 payload 和 DROP 后清理 |
| Owner Registry/Tombstone | system retained registry 中 account/database/table 的 `ACTIVE/DROPPED` 权威行；DROP 将其 CAS 为 tombstone，通知 Sweeper 放弃 Restore 并级联清理 Archive |
| Access Generation | Dataset 上串行化 Restore/read lease 与 `DELETE_PENDING` 的递增版本 |
| Reservation | 降低 Lifecycle 与 TN Merge 同时选择相同对象概率的短租约；不承担正确性 |
| CAS | Compare-And-Swap，提交时只有所有版本、状态和对象条件仍匹配才允许成功 |
| Tombstone | TAE 对数据行删除的记录；Merge 后需要正确转移到新对象 |
| Transfer Map | 源对象行位置到新对象行位置的映射 |
| Archive Profile | 对云存储能力、存储类别、恢复 SLA、最低保留期和计费参数的声明 |

以下三个平面必须独立：

| 平面 | 解决的问题 | 示例 |
|---|---|---|
| 历史版本 | 已更新/删除的数据还能否按历史时刻读取 | Snapshot、PITR、Time Travel |
| 业务生命周期 | 哪些业务行还属于当前表 | TTL、Archive、Purge |
| 在线放置 | 当前可查询对象放在哪里、如何缓存 | CN cache、远端 cache、活动对象存储 |

归档不是 Snapshot。Lifecycle Snapshot 只保护在途 Job 的源 Object；Job 明确提交或终止后释放，不由 Archive Manifest 永久持有。普通 Snapshot/PITR 也不自动保护 Archive Payload。

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

   Data Branch 已在 [`pkg/frontend/data_branch_snapshot.go`](../../pkg/frontend/data_branch_snapshot.go) 中演示“业务系统元数据与保护 Snapshot 同事务写入”。这证明 system-owned Lifecycle Snapshot 可以沿现有模式实现。但 [`pkg/frontend/check_snapshot_flushed.go`](../../pkg/frontend/check_snapshot_flushed.go) 的 `CheckSnapshotFlushed` 只证明 Base/Index Table 的 `flushTS >= snapshotTS`，不证明 GC loader 已把 `mo_snapshots` 记录装入保护集合；Lifecycle 仍需新增 GC metadata-visible gate。

6. **DROP 的轻量系统表 Hook**

   [`pkg/sql/compile/ddl.go`](../../pkg/sql/compile/ddl.go) 已在 DROP TABLE 主路径中以 system tenant 更新 Merge Settings 和 Data Branch 元数据。Lifecycle 复用相同扩展点写一个常数级 owner tombstone，不在 DROP 中执行 provider Delete 或等待后台 Job。

7. **Parquet 基础**

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
| Protocol Foundation | tagged Commit Entry/Receipt、Lifecycle Snapshot/GC visible gate、Attempt Root、Feature Guard、hard budget | 只用于故障原型 |
| Whole Object | 对完全过期 Object 做 TTL/Archive | 是，协议门禁通过后 |
| Mixed Object | 过期行归档/丢弃，存活行写回 ObjectIO | 是，GA 必需 |
| Direct-readable Archive/Restore | 直接读取已发布 payload、staging restore、原子发布 | 是，GA 必需 |
| Restore-required Deep Archive Profile | transition/thaw、临时可读副本和 provider 状态收敛 | 否；首个 GA 后按客户需求独立验收 |

Planner 可以先直接通过当前 `GetColumMetadataScanInfo` 分析一张白名单表；规模验证后再切换到增量 Object Index。Export-only 也可以先不复用 mergesort。真正退休活动数据之前，必须完成 replay、Snapshot/GC gate、Attempt Root、Feature Guard 和资源上限，不能用“后续再优化”替代。

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
           +--> system Lifecycle Snapshot + GC visible gate
           +--> system Attempt/Cleanup Root
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
             verify payload and rewrite result
                           |
                           v
              conditional distributed transaction
              - publish Manifest
              - register new objects
              - retire exact source objects
              - write durable Receipt
                           |
                           v
                 reconcile + release Lifecycle Snapshot
```

组件职责：

| 组件 | 职责 |
|---|---|
| Policy Manager | 解析 table-scope 策略、版本和 provider capability |
| Object Indexer | 为活动对象建立生命周期列 min/max/null 和 next action 派生索引 |
| Scanner | 只扫描到期索引项，生成 dry-run 或执行计划 |
| Coordinator | 创建 Lifecycle Snapshot；Archive Job 另建 Attempt Root；通过 GC gate 后将 batch 拆成独立 child Job |
| Job Executor | 读取固定 Snapshot、输出活动对象和归档文件 |
| ArchiveStore | 提供版本化、可校验、可恢复的云归档能力 |
| Commit Handler | 执行对象级 CAS、tombstone 协调和原子 retirement |
| Reconciler | 处理接管、commit unknown、staging orphan 和 provider eventual consistency |
| Purger | 按不可逆删除协议清理 Archive Payload |
| Restore Service | 读取已发布归档、验证、写隐藏 staging 表并原子发布为新表；可选 Profile 在读取前扩展 thaw |

Lifecycle Job Executor 运行在 CN TaskService worker，而不是塞入 TN 后台 Merge task：CN 负责跨云归档、Parquet 和正常分布式事务；TN 只提供 scope reservation、对象级 PrepareCommit 校验和 TAE transaction entry。长时间 copy/export 不保持 MO 事务，只有创建 Snapshot/Root 和最终 publish/retire 是短事务。

## 8. 元数据模型

所有表均具有 schema version。租户可见 Policy/Binding/Job/Dataset 可按 account 分区或聚簇；Attempt/Cleanup Root、Owner Tombstone 和其冻结的 Profile 身份必须属于 system account retained registry，不能被现有 DROP ACCOUNT 的 cluster-table 清理删除。名称为设计名，最终 DDL 可按 MO 系统表规范调整。

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

### 8.2 Feature Guard 与 Archive Profile 身份

`dependency_fingerprint` 不能只在最终事务里“重新读一次”。这无法与并发首次创建 CDC、Publication、FK、Index 或插件形成 write-write conflict。首个 GA 新增每表唯一权威行 `mo_table_feature_guards`：

- 唯一键：`(account_incarnation, table_id)`；
- `guard_version`、`table_schema_version`、`lifecycle_generation`；
- 已启用 feature bitmap 和版本化 digest；
- Lifecycle Binding ID/version/state；
- 最后修改事务信息。

对 account/database scope 的 Backup、PITR、Snapshot、Clone/Branch 或 DR，不能靠“先枚举当前 Lifecycle 表”关闭与并发 bind 的竞态。另设 system retained `mo_lifecycle_scope_guards`：

- 唯一键：`(account_incarnation, scope_type, scope_id, scope_generation)`；
- `guard_version/commit_seq`；
- `lifecycle_owner_count/owner_root`：system-owned unique table-generation owner edge 的事务摘要；edge 在有效 Binding、在途 Job 或未清理 Dataset/Root 存在期间保留；
- archive-unaware Backup/PITR/Snapshot/Clone/Branch/DR protection bitmap。

Lifecycle bind/unbind、最后一个 Root 清理以及 scope 保护创建/删除必须 CAS 同一 scope row，并在同一事务创建/删除唯一 owner edge。创建 account/database 保护要求 `lifecycle_owner_count == 0 && owner_root == empty`；Lifecycle bind 要求 protection bitmap 为空，并原子增加 edge/count/root。unbind 后只要还有 Job/Dataset/Root 就不能删除 edge。Reconciler 从 edge 重算 summary；不一致时 fail closed，不能把 count 当成可漂移的缓存。首次 row/edge 创建同样依赖唯一键冲突。这样正常 scope admission 是 O(1) Guard 更新，不扫描/锁住海量表，又不会漏掉与并发首次 bind 的竞态。

以下操作必须在自己的正常 Catalog 事务中懒创建或 CAS 同一 Guard 行：

- Lifecycle bind/unbind 和最终 publish/retire；
- CREATE/DROP INDEX；
- 创建/删除 CDC、Publication/Subscription、FK、Fulltext、Vector、异步索引或插件依赖；
- 将表加入/移出 table-scope Backup、PITR、用户 Snapshot Restore、Clone、Branch 或 DR 保护关系；
- ALTER、TRUNCATE 和 DROP TABLE。DROP DATABASE/ACCOUNT 不枚举并更新每张表 Guard，而是 CAS system owner registry 的 database/account 行。

首次操作不能先判断“Guard 不存在”后跳过写入。双方都尝试插入同一个唯一键，唯一键冲突后重新读取并 CAS，才能关闭“Lifecycle bind 与 CDC create 同时看到不存在”的竞态。Guard 不进入普通 INSERT/UPDATE/DELETE、查询或 Merge 路径；未绑定 Lifecycle 的表也只在上述低频 DDL/控制面操作发生时创建。

升级时不能假定 Guard 已包含历史依赖。允许 Lifecycle bind 前必须满足：

1. 所有会创建/删除受控能力的 CN/控制面已具备 table/scope Guard capability；
2. bind 在一致性事务中扫描 TableDef 与现有 CDC/FK/Publication/Index/Backup/PITR/Snapshot/Clone/Branch/DR 元数据，写入初始 digest；
3. Guard 写入与这些能力的新建路径通过唯一键/CAS 串行化；
4. reconcile 对 Guard 与真实 Catalog 做分片校验，发现漂移立即停止新 Job，未知状态 fail closed。

只更新 final commit 而未改完整能力创建链路时，Lifecycle 只能运行 Read-only/Export-only，不能退休数据。

首个 GA 的 Archive Profile 是不可变、版本化的存储身份：

```text
(profile_id, profile_version, storage_namespace_id,
 endpoint, bucket/container, immutable_prefix)
```

- 已发布的 `profile_version` 不允许原地改 endpoint、bucket/container 或 prefix；
- Credential/KMS rotation 使用独立 `credential_generation`，不得改变 namespace identity；
- Dataset 与 Attempt/Cleanup Root 都冻结完整身份、Manifest/Payload key 和 provider version（若支持）；versionless Profile 冻结 hash/etag 与永不复用的 key；
- 存在未 `CLEANED` Root 时禁止删除冻结 Profile 元数据或删除凭据可达性；
- 变更存储位置必须创建新 `profile_version`，旧版本只供 Restore/Purge/Sweeper 使用。

这样管理员修改 Profile 不会让多年后的 Restore 找错对象，也不会让 Sweeper 删除另一个 namespace 的同名 key。

### 8.3 Object Index

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

### 8.4 Job、Attempt Root、Dataset 和 Payload

`mo_lifecycle_jobs` 记录：

- `job_id`、`coordinator_id`
- exact source object set 及 digest
- `source_snapshot_ts`
- `cutoff`、predicate digest
- policy/binding/schema/generation
- executor、executor epoch、attempt
- 状态、deadline、progress、错误
- created object / dataset / rewrite result 摘要

TaskService 分配新 executor 后，runner 必须先用 TaskService epoch 条件认领 `mo_lifecycle_jobs`，成功写入 authoritative `executor_epoch`。TaskService epoch 负责“谁应执行”，Job/Root CAS 负责“谁还能写外部对象和发布”；两者不能合并成一个内存判断。

#### System-owned Attempt/Cleanup Root

在**第一次外部 PUT 之前**，Coordinator 必须在 system account retained registry 创建 `mo_lifecycle_cleanup_roots`。不能等 Manifest publish 时才创建，否则 `staging upload -> DROP ACCOUNT -> tenant Job 被删` 会产生永久 orphan。

Root 至少冻结：

- account ID/incarnation、database/table ID、table/lifecycle generation；
- job ID、attempt、executor epoch；
- Lifecycle Snapshot ID/TS；
- `profile_id/profile_version/storage_namespace_id` 与 credential generation reference；
- deterministic immutable object prefix；
- attempt lease、I/O deadline、quiescence deadline；
- Archive Container/Manifest key、provider version、root；
- Payload key/version page root、rows/bytes；
- final intent ID/digest、Receipt digest mirror；Root row 的 MVCC commit timestamp 作为提交顺序；
- `access_generation`、state、row version。

Root 状态机：

```text
REGISTERED -> UPLOADING -> VERIFIED -> PUBLISHED
                                \       |
                                 \      v
                                  -> DELETE_PENDING -> DELETING -> CLEANED
```

约束：

1. Archive Job 的 Lifecycle Snapshot、Job 和 `REGISTERED` Root 在同一正常事务创建；事务提交后还要通过数据 flush 与 GC metadata-visible 两道 gate。TTL Job 不产生外部 Archive PUT，只需同事务创建 Snapshot 与 Job，不需要空 Root。
2. Runner 只有 CAS `REGISTERED/UPLOADING + executor_epoch + state_version` 成功后才能执行 PUT；每个已完成 multipart/PUT 的精确 key/version/checksum 都写入 Root 的有界 payload page 或不可变 Container。
3. 最终 publish/retire 事务把 Root 从 `VERIFIED` 原子转为 `PUBLISHED`，表示外部对象所有权从 attempt staging 转交给从属 Dataset；Root 本身继续作为 DROP/租户删除后的 cleanup root。
4. 失败且明确未发布的 attempt 进入 `DELETE_PENDING`。`COMMIT_UNKNOWN` 禁止进入删除。
5. `CLEANED` 不是立即删除记录。tombstone 至少保留到最大 I/O deadline、provider request timeout 和 quiescence window 全部结束；旧 executor 的迟到版本在窗口内继续被发现和删除。

Crash 可能发生在 provider 已接受 PUT、Root 尚未记下 identity 之间。因此 Sweeper 不能只删 Journal 中的对象，还必须能按 Root 的 deterministic prefix 枚举该 attempt 的全部对象身份、终止残留 multipart upload，并反复对账到 quiescence。不能可靠列举对象/multipart 的 Profile 不允许进入可执行 Archive GA。

key 还必须包含 account incarnation，避免账户 ID 或表 ID 复用后，旧 Sweeper 命中新租户对象。

协议原型必须证明 tenant Lifecycle 事务可以把 tenant Dataset/Binding、system Root/Owner Registry 和 TAE Entry 放入同一 1PC/2PC。若当前 SQL 执行上下文不能原子写 system-owned relation，就应新增 TN/Catalog 可识别的 system registry write entry；禁止把 Root `PUBLISHED` 或 Owner CAS 降级成事务后的异步补写，否则会重新打开 DROP/commit-unknown 窗口。

一个 child Job 对应一个 `archive_dataset_id`，不与其他 Job 做 all-or-nothing 发布。Dataset ID 是稳定路由 ID，但所有权明确从属于 `(account_incarnation, source_table_id, source_table_generation)`；它不是可以脱离源表长期存在的独立合规对象。面向用户的月/年归档视图由 collection/super-manifest 聚合多个 Dataset，不重写 payload。

`mo_archive_datasets` 是 Dataset 可见性和生命周期状态的唯一权威行，至少记录：

- `archive_dataset_id`、account incarnation、source database/table/generation、collection ID；
- source Snapshot、schema/policy/binding/lifecycle generation；
- lifecycle range、rows/bytes、manifest root、冻结的 archive profile identity；
- `published_at`、`purge_eligible_at`、active access lease count；
- `state`：`STAGING`、`VERIFIED_NOT_PUBLISHED`、`PUBLISHED`、`DELETE_PENDING`、`PURGED`；
- dataset row version 和 access generation。

最终 publish/retire 事务必须原子写入可按 source table/range 检索的 `PUBLISHED` Dataset 行。Restore 和 Purge 以该行为事实源，不能依赖异步任务先更新 Collection 才发现 Dataset。

`mo_archive_collections` 为同一 table/policy generation 提供稳定 `collection_id`，按 lifecycle range 和 dataset shard 分页维护：

- `commit_seq`、active-access summary；
- range min/max、dataset count、logical rows/bytes；
- page cursor、page root 和 collection root；
- 部分 Purge 后的审计摘要；
- catalog compaction generation。

Restore 先按 collection range 裁剪 dataset page，再按 Manifest/payload min-max 裁剪文件，禁止一次把数万 Dataset 全部装入内存。Super-manifest 更新使用 copy-on-write page 和 root CAS，不重写已发布 Dataset。

Collection page/root 是 Dataset 权威行之上的分页聚合索引。Page 更新可以在最终事务后由幂等 Indexer 完成，但 Restore 必须同时扫描“已 `PUBLISHED`、尚未进入当前 collection root”的有界增量 Dataset；Indexer 持久化 high-watermark 并对账，不能让 collection lag 把已经从活动表退休的数据隐藏。

Collection 只是从属 Dataset 的分页聚合索引，不提供独立所有权、Legal Hold 或 Backup/DR root。Dataset publish、Restore/read lease 和 Dataset 删除 CAS 必须条件更新 `collection.commit_seq` 或等价 access generation，保证“开始 Restore”和“开始不可逆删除”只有一方成功。

`mo_archive_manifests` 记录 schema digest、payload root、行数、min/max/null、压缩、KMS 和 provider version；发布后内容不可变。Dataset row 持有 Manifest root 和生命周期状态，Manifest 不能独立进入与 Dataset 冲突的 `PUBLISHED/DELETE_PENDING` 状态。

`mo_archive_payloads` 每个不可变文件一行，记录 key、content hash、provider version ID、etag、storage class、字节数、验证和删除状态。

`mo_archive_access_leases` 只表达首个 GA 内部 Restore/read Job 的短期引用。它也属于 system retained registry，并在 Root 保存 active lease summary；不能只存在会被 DROP ACCOUNT 清掉的租户 Catalog。Lease 包含 Restore attempt/executor epoch、I/O deadline、access generation 和 `ACTIVE/FENCED/RELEASED` 状态；它不是 Legal Hold、Backup 或跨租户永久引用。

Lease 获取短事务必须同时：

- CAS Dataset/Root 仍为 `PUBLISHED`；
- 递增 Root `access_generation/active_lease_count`；
- 条件递增 account/database/table Owner Registry `commit_seq` 且 state 仍为 `ACTIVE`。

因此 Restore 首次获取与 owner DROP 会产生真实 write-write conflict。owner DROP 已提交后，Sweeper fence Restore executor；待 Restore publish 事务明确 committed/aborted，或 executor 已失效且最大 read I/O deadline 结束后，将 Lease 置为 `RELEASED` 并递减 Root count。DROP 主事务不等待该过程；卡住只形成可告警的 cleanup backlog，不阻塞用户 DDL。

`mo_lifecycle_owner_registry` 同样属于 system account retained registry。Lifecycle bind 懒创建 account-incarnation、database ID/generation 和 table ID/generation 三层唯一权威行，状态初始为 `ACTIVE`，包含 `owner_version/commit_seq`：

- 最终 publish/retire 必须条件递增相关 account/database/table owner `commit_seq`；
- DROP TABLE 只 CAS table owner 为 `DROPPED`；
- DROP DATABASE 只 CAS database owner 为 `DROPPED`；
- DROP ACCOUNT 只 CAS account-incarnation owner 为 `DROPPED`，不枚举海量 table Guard；
- 首次 bind 与 DROP 同时发现 owner row 不存在时，依靠相同唯一键插入冲突关闭竞态；
- `DROPPED` 行就是有界 owner tombstone，与 DROP 的本地 Catalog 变更同事务提交，不等待 provider；
- Sweeper 从 tombstone 查找 Root，不依赖反复查询“租户是否还存在”；
- owner DROP 覆盖 `purge_eligible_at`；等已有 Restore/read lease 收敛后，CAS 相同 `access_generation` 进入 `DELETE_PENDING`，此后禁止新 lease。

这样 DROP ACCOUNT 只多写一个常数级 system row，final Lifecycle commit 又会与它产生真正的 write-write conflict；不需要扫描或锁住租户下所有 Lifecycle 表。

控制面 metadata 也有独立 retention/GC：

- terminal Job 在 Snapshot、staging 和 commit unknown 全部收敛后，按 account/batch 归档或删除；
- Commit Receipt 至少保留到 Snapshot 释放、Dataset/Root 进入终态且审计保留期结束，再将 digest 汇总进 collection audit root；
- 已 Purged Dataset/Manifest 保留有界审计摘要，不永久保留全部 payload 行；
- Collection page 只有新 root 已生效、无 cursor/access lease 指向时才能回收；
- `DELETE_FAILED_MANUAL` 和 in-doubt Receipt/Root 禁止自动 GC；
- Root 只有 `CLEANED` 且 quiescence window 结束后才能压缩为有界审计 tombstone；
- Owner Registry 的 `DROPPED` tombstone 只有关联 Root/Lease/Job/Receipt 全部终结且 quiescence 结束后才能分页压缩/回收；account incarnation 和 table generation 永不复用；
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
  -> one Lifecycle Snapshot per child
  -> one Attempt Root per Archive child
  -> one archive dataset per Archive child
  -> one publish/retire transaction per child
```

一个 Job 不跨多个大表，也不把整个 TB 表放进一个 all-or-nothing 事务。每个 child 的 Snapshot/Root/commit-unknown 独立收敛；一个冲突或卡住的 child 不得延长其他 child 的 Snapshot 生命周期。

调度使用两类 profile：

| Profile | 适用条件 | 目标 |
|---|---|---|
| Whole-object streaming | 整对象全部过期、不需要 live transfer | 提高 source rows/objects 上限，持续形成 256–512 MiB Archive Payload |
| Mixed rewrite | 需要 live rewrite 和 transfer | 小批次，严格控制行数、transfer 和 Prepare |

Planner 联合估算 source bytes/rows/blocks、expired rows、live rewrite bytes、archive compressed bytes、target files 和 tombstone。不能只用 `source bytes` 间接推断其他资源。

每个 Job 和每个 final transaction 分别设置硬上限：

| 资源 | 强制控制 |
|---|---|
| source objects/blocks/rows/bytes | scan 前估算，运行时达到任一上限立即停止追加 |
| transfer slots/bytes | 独立预算，不按 source bytes 推断 |
| new/concurrent tombstones | rows、bytes、objects 上限 |
| archive/live output | writer memory、spill/temp bytes、文件数和文件最小/最大值 |
| created/source metadata | object count、fingerprint bytes、entry bytes 上限 |
| WAL/RPC | tagged Entry 序列化后 hard limit，发送前测量 |
| Prepare | deadline、CPU budget、可执行操作白名单 |
| staging/orphan | account/cluster bytes、objects 和 oldest-age 水位 |

起始实验配置可以使用 `1 GiB source / 100 万 rows / 64 objects / 16 output files / 256 MiB archive target`，但它只是多对象 batch 上限，不得成为单对象无法处理的死循环。当前默认上限可达到：

```text
8192 rows/block * 256 blocks/object = 2,097,152 rows/object
bytes/object <= 3 GiB
```

对应 [`pkg/objectio/const.go`](../../pkg/objectio/const.go)、[`pkg/vm/engine/tae/options/types.go`](../../pkg/vm/engine/tae/options/types.go) 和 [`pkg/objectio/writer.go`](../../pkg/objectio/writer.go)。一个 Object 已是退休 CAS 的最小边界；单个超限 Object 不能再通过“拆 source object set”解决，必须进入 oversize-object streaming：

- 一个 attempt 只处理该 Object；
- 输入、Parquet、live rewrite、transfer 和 tombstone 均流式或 spill，不把全对象装入内存；
- final transaction 只携带 created object/transfer location 的有界 root 和 count；
- 达到 spill/file/entry hard limit 时进入明确 `OVERSIZE_BLOCKED`，不无限 retry；
- GA 认证覆盖版本允许的最大 rows、blocks、bytes、单 block varlen、transfer/tombstone、spill 和文件数；
- Mixed Object 还必须覆盖“几乎全部行存活”的最大 live rewrite，而不是只测 3 GiB 顺序归档读。

GA 默认值必须由 32 B、256 B、4 KiB 行宽和真实 WAL/RPC/内存基准确定。output ObjectIO 数量始终小于 `api.NoTransfer` 保留值。

transfer/tombstone 使用对象化 staging；final transaction 只携带有界 locations、digests 和 counts，不携带无限逐行列表。多对象 Job 运行时预算超限时执行：

```text
abort current attempt
keep source visible and Snapshot protected
split/replan into smaller job
```

单对象已经不能再拆时执行 oversize streaming；若仍超过认证硬上限，进入 `OVERSIZE_BLOCKED` 并保留活动数据，不得反复创建相同失败 Job。

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

reservation 通过新的 TN lifecycle scope RPC 获取、续租和释放。它不占住一个工作线程，不持有事务或表锁。连续冲突的 Job 缩小 object set、指数退避并暴露 conflict metric；超过配置的最大冲突时间后进入 `CONFLICT_BLOCKED`，不再承诺该表的 Archive Lag SLO。系统宁可延迟归档，也不能为了追求进度修改普通 Merge 策略、持有长表锁或放松最终 CAS。

## 10. 数据执行路径

每个 Job 固定：

- `source_snapshot_ts`
- exact source objects
- `cutoff`
- schema/policy/binding/generation
- archive profile

`LifecycleRewriteHost.LoadNextBatch`：

1. 在固定 Snapshot 读取源批次；
2. 应用该 Snapshot 已可见的 tombstone；
3. 计算生命周期谓词；
4. 将过期行加入 overlay delete mask；
5. TTL 将过期行交给 `DiscardSink`；
6. Archive 将过期行交给 `ArchiveSink`；
7. 返回原 Batch + 合并后的 delete mask 给 `DoMergeAndWrite`；
8. `DoMergeAndWrite` 只对存活行排序、写新 ObjectIO 并建立 transfer map。

所有 Sink 都是流式、有上限且可取消的。任一 Sink 失败，Job 不进入 `READY_TO_COMMIT`，Archive Dataset 不进入 `VERIFIED_NOT_PUBLISHED`。

Job 在进入 `READY_TO_COMMIT` 前验证：

```text
source_visible_rows = live_rows + expired_rows
live and expired source-row identities are disjoint and cover every visible row
live transfer mappings = live_rows
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

- Feature Guard 证明没有隐藏索引表或其他行级消费者时，不读取 payload，最终事务直接退休对象；
- 任何物化为隐藏表的普通/唯一索引以及 CDC、FK、Publication 或插件由 admission 直接拒绝，不能在运行时跳过。

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

每个 Dataset 还写一个不可变、版本化并签名的 Archive Container sidecar，包含 dataset ID、owner identity、schema、payload 精确 key/version 列表和 root。MO Catalog 的 Manifest row 是可见性真相，sidecar 在最终事务前只是 staging；publish 后 Root 保留其精确 identity，供租户 Catalog 已被 DROP 时的 Sweeper 清理。首个 GA 不承诺用 sidecar 重建丢失的 Dataset Catalog，也不把 Archive 纳入 Backup/DR。

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
  ListAttemptObjects(immutable_prefix)  # key + optional provider version
  AbortMultipart(upload_identity)
  VerifyChecksum()
  DeleteVersion(condition)
  Capabilities()
  CostProfile()

RestoreRequiredArchiveStore extends ArchiveStore
  TransitionVersion(target_class)
  TransitionStatus()
  RequestRestore()
  RestoreStatus()
```

首个 GA 只要求 `ArchiveStore` 核心接口。Capability 至少声明：

- 是否可立即读取；
- 是否支持 version ID 和条件删除；
- storage class；
- 最小保存时长、提前删除费用、最小对象大小；
- checksum、server-side copy 和 KMS 能力。

Commercial GA Archive Profile 必须提供不可覆盖且永不复用的唯一 key、可验证 checksum、attempt-prefix object/multipart enumeration 和精确删除身份。Provider 支持 version ID/CAS 时必须按具体 version 删除；不支持 versioning 时只能使用 globally unique immutable key + hash/etag，且该 key 永不承载其他 Dataset 的合法对象。缺少这些能力的 provider 只能用于 Export-only 实验，不能绑定可退休源数据的 GA Policy。

逻辑上只有一个 `ARCHIVED` 状态。标准对象类和深归档类只是不同 Archive Profile：

- `DIRECT_READABLE_ARCHIVE`：Restore Service 可直接读 Parquet，不表示归档数据透明参与普通查询；
- `RESTORE_REQUIRED_ARCHIVE`：先由 provider thaw，恢复慢且可能收费。

这不是再造 `COOL/COLD` 业务状态，Policy、Manifest、引用和删除协议完全相同。

首个 Commercial GA 只开放 `DIRECT_READABLE_ARCHIVE`。归档数据通过异步 Restore 进入新表，但 Restore Service 直接 `Open(version)` 读取 payload，不经过 provider thaw。

`RESTORE_REQUIRED_ARCHIVE` 是首个 GA 后的独立可选 Profile。只有客户实际云厂商、目标 storage class、可量化价差和恢复 SLA 明确后才实现；它不能成为 direct-readable Archive/Restore 发布的前置条件。该 Profile 的 Provider adapter 还必须实现：

- 写入或 transition 到目标 storage class 的确定结果；
- event 丢失时可用 polling 收敛；
- thaw request 幂等键、状态、临时可读截止时间；
- request 不可取消时的费用和 UI 语义；
- provider throttle、unknown、not-found、KMS 和 credential rotation；
- 至少一次真实 provider 的 transition/thaw/restore/purge drill。

每个 transition/thaw request 携带 payload version ID、`access_generation` 和稳定 request ID。Provider event 与 polling 结果只有同时匹配三者才能推进 access state；旧 attempt 的迟到事件只能记审计日志，不能覆盖新 generation。

若 provider transition 产生新的 version identity，结果必须先完成 checksum/size 验证，再用 `(payload_id, old_version, access_generation)` CAS 写入新 version。CAS 成功前旧 verified version 仍是权威版本；CAS 失败的新 version 只能进入 orphan cleanup，不能被 Manifest、Restore 或 Purge 引用。CAS 成功后，新 version 成为唯一权威版本，旧 version 作为 dataset 所有的 superseded version 使用同一不可逆删除协议清理；新 version 未完成验证前禁止删除旧 version。

可选 Deep Archive Profile 的安全顺序固定为：

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
account_incarnation / archive_dataset_id / job_id / attempt / executor_epoch /
object_group_digest / file_sequence / content_hash
```

禁止覆盖和复用旧 key。重复上传相同内容可以由 Reconciler 识别，但 stale runner 不能写入新 runner 的对象。每个 PUT 必须已经有可定位该 deterministic prefix 的 system-owned Root；不能先写后补登记。

## 12. Lifecycle Snapshot 与 GC

首个 GA 不新增 exact-object source ref，也不改普通 Merge/GC 的对象引用模型。每个 child Job 使用现有 GC 能识别的 system-owned **table-level Lifecycle Snapshot** 保护源数据。代价是慢 Job 可能额外保留该表在 Snapshot 之后由持续 Merge 产生的旧版本；这是用更高、可测的空间成本换取更小的内核改动面。

正确顺序固定为：

```text
create system Lifecycle Snapshot + Job
  + REGISTERED Attempt Root             # Archive only
  -> transaction committed
  -> data flush gate
  -> GC snapshot-metadata-visible gate
  -> select exact source Objects at source_snapshot_ts
  -> read/rewrite/upload/verify
  -> final publish/retire transaction reaches definite result
  -> release Lifecycle Snapshot
```

具体要求：

1. Snapshot 使用 `mo_snapshots.kind='lifecycle'`、table scope 和真实 `source_snapshot_ts`，对用户隐藏；Archive Job 的 Snapshot、Job 和 Root 必须同事务成功或失败，TTL Job 的 Snapshot 与 Job 必须同事务成功或失败。
2. `CheckSnapshotFlushed` 只作为第一道 gate，证明 Base Table 的 `flushTS >= snapshotTS`。首个 GA 拒绝隐藏索引表，因此无需等待隐藏 Index Table；若以后开放索引 handler，必须扩展对应 flush 集合。
3. 新增只读 `CheckLifecycleSnapshotProtected(snapshot_id, snapshot_ts, owner_epoch)` 或等价内部协议。每个可能删除源 Object 的 GC owner 必须确认：
   - 已从可重放 Catalog/checkpoint 加载该 Snapshot；
   - 当前保护集合包含精确 Snapshot ID/TS；
   - 在加载前冻结 protection set 的旧 GC cycle 已结束；
   - owner restart/shard handoff 后，新 owner 在恢复 delete 前重新满足同一条件。
4. Object Index 在 gate 前只能提供候选 hint。两道 gate 未全部通过前，Planner 不得形成权威 source Object set，Executor 不得读源对象或执行外部 PUT。超时只停止 Job并告警，不能降级为“假定已保护”。
5. gate 后在 `source_snapshot_ts` 选择对象，并用 Job CAS `PLANNED -> PINNED` 持久化 exact object identity/fingerprint/digest；Executor 只有看到 `PINNED` 和匹配 epoch 后才能读取/PUT。Snapshot 负责可读性，object CAS 负责防止并发 Merge/DDL 后错误退休。

其时序证明是：

```text
S = Lifecycle Snapshot commit
F = data flush gate passed
G = all GC owners see S, and every pre-S protection-set cycle has drained
O = source Object selection

required order: S < F < G < O
```

`O` 发生时不存在未看到 S 的旧 GC cycle；`G` 之后启动的 GC cycle必须包含 S。此后即使普通 Merge 替换并 drop 了源 Object，GC 仍因 S 保留其 Snapshot 可见版本，直至明确释放。实现必须用 GC/Merge/Snapshot 并发 chaos test 证明该偏序，不能只依赖通常存在的 GC grace period。

Snapshot 的 deadline 是运维 SLO，不是会自动失效的 lease：

- deadline 可以触发停止新 I/O、fence executor、告警和 reconciliation；
- 只有 final transaction **明确 committed**，或 cancel/terminal cleanup **明确 aborted/not-started** 后才能释放；
- `COMMIT_UNKNOWN` 无论超时多久都保留 Snapshot，以及 Archive Job 的 Attempt Root 和可能已发布 payload；
- owner DROP 先通过 Feature Guard fence 新 publish；已经进入 final transaction 的 attempt 仍须按 transaction service 结果收敛，不能因 DROP 或 deadline 猜测 abort。

System Snapshot 的空间控制不是“超过预算只暂停新 Job”：

- 持续统计每个 Job 的 `snapshot_exclusive_retained_bytes`，即若无该 Lifecycle Snapshot 本可被现有 GC 回收的额外旧版本；
- 软限额停止同表/同租户新 Job；
- 硬限额 fence/cancel 当前 executor，停止新 PUT；等待 final transaction 明确 committed/aborted 后，分别完成 publish 后处理或 attempt cleanup，再释放 Snapshot；
- 不能通过提前删 Snapshot 来降低 pinned bytes；
- GA 必须覆盖 `10 TiB table + sustained normal Merge`，测量 Snapshot 独占保留量、前台 P95/P99、Merge backlog、fence 到释放时间和 cleanup backlog。

两条 GC 谓词必须独立：

```text
Source TAE Object deletable =
  dropped from active table
  AND no Snapshot/PITR/Branch/Backup/ISCP reference
  AND existing TAE GC watermark allows deletion

Archive Payload deletable =
  (
    owner exists AND dataset purge_eligible_at reached
    OR owner tombstone committed
  )
  AND no active Restore/read lease
  AND grace/quiescence condition reached
  AND dataset/root is irreversibly DELETE_PENDING
  AND payload is DELETE_INTENT or DELETING
```

普通 Snapshot/PITR 不保护 Archive Payload；Archive Dataset 也不继续保护源 TAE Object。两者没有跨平面永久引用。

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

当前 [`pkg/vm/engine/tae/rpc/handle.go`](../../pkg/vm/engine/tae/rpc/handle.go) 会把每个 `TxnCommitRequest.Payload[].CNRequest.Payload` 无条件解析为 `api.PrecommitWriteCmd`，`ErrTAENeedRetry` 也从原始 commit requests 重建事务。因此首个 GA 只采用一种 wire 方案：

1. 在 [`proto/api.proto`](../../proto/api.proto) 的 `PrecommitWriteCmd.EntryList.Entry` 增加明确 tag，例如 `LifecycleCommit = 7`；
2. Entry 增加版本化、长度有界的 `lifecycle_commit_payload`，内容为 `api.LifecycleCommitEntry`；
3. `ParseEntryList` 识别该 tag 后返回 typed entry，不能把它误解析为普通 DML Batch；
4. TN iterator、Prepare/Commit、TAE WAL/txn entry、rollback 和 replay 全部识别同一个版本；
5. 未声明 capability 的旧 CN/TN 收到该 tag 必须明确拒绝，不能忽略。

最终 wire/事务语义不能复用 `MergeCommitEntry`，也不能增加一个绕开 `PrecommitWriteCmd` 解析链的孤立 opcode。前述 `DoMergeAndWrite` 进程内适配不属于最终提交协议。

版本化 `LifecycleCommitEntry` 必须完整进入原始 `TxnCommitRequest.Payload` 和事务 WAL 可重放信息，不能引用 executor 内存。稳定身份为：

```text
intent_id = hash(job_id, attempt, lifecycle_generation, object_group_digest)
```

Entry 至少包含：

- wire/capability version；
- job ID、attempt、executor epoch；
- Lifecycle Snapshot ID/TS、exact object create/drop/fingerprint digest；
- schema、policy、binding、lifecycle generation；
- Feature Guard key/version/digest；
- cutoff、predicate 和 immutable export intent digest；
- created live Object locations/stats/root；
- transfer locations/digest/count；
- archive dataset、Attempt Root、Container/payload/profile versions/root；
- source/expired/live rows、objects、bytes 和序列化 budget。

同一个 `intent_id` 的重复 Handle/Prepare/Replay 只能得到同一个对象注册和 retirement 结果；内容 digest 不同则返回不可重试冲突。

### 13.2 最终短事务

导出阶段只冻结 Lifecycle Snapshot/object fingerprint、schema/policy generation、Feature Guard version/digest 和 export intent digest，不捕获 `commit_seq`。完整读取和校验完成后才开启最终短事务，读取当前 `commit_seq` 并执行：

同一个正常分布式事务中：

1. 条件更新 Job：

   ```text
   job_id == captured job
   executor_epoch == current epoch
   job.state == READY_TO_COMMIT
   attempt_root.state == VERIFIED                   # Archive only
   attempt_root.executor_epoch == current epoch     # Archive only
   ```

2. 条件更新同一张表的 Feature Guard、Binding 与 system Owner Registry（不能只做 Snapshot read）：

   ```text
   guard.key == (account_incarnation, table_id)
   guard.version/digest == captured
   guard declares no unsupported feature
   account/database/table owner.state == ACTIVE
   each owner.commit_seq == captured
     -> each owner.commit_seq = value + 1
   policy_version == captured
   binding_version == captured
   lifecycle_generation == captured
   table_schema_version == captured
   commit_seq == value read in this short transaction
     -> commit_seq = value + 1
   ```

   这会与并发 Lifecycle commit/DDL/Policy/owner DROP 形成真实 write-write conflict；条件影响行数不是 1 时只重试短事务。只有 source/schema/policy/Feature Guard/owner identity 改变时，才废弃已校验 export 并重新规划。

3. Archive Job 条件发布 Manifest 并转交 Root 所有权；TTL Job 跳过本步：

   ```text
   dataset.state == VERIFIED_NOT_PUBLISHED
   manifest.root == verified root
   every payload.state == VERIFIED
   frozen profile/namespace/exact object identities == verified identities
   attempt_root.state == VERIFIED
     -> attempt_root.state = PUBLISHED
   attempt_root.intent/receipt digest mirror == final digest
   collection.commit_seq == captured collection commit_seq
     -> collection.commit_seq = value + 1
   ```

   Dataset row 在该事务中进入 `PUBLISHED`。Collection page/root 可以异步聚合，但权威 Dataset 行必须立即可按 source/range 发现。

4. 在原始 `PrecommitWriteCmd.EntryList` 附加 tagged `LifecycleCommitEntry`：

   - 携带完整、序列化后仍在 hard limit 内的 Entry；
   - transfer/tombstone 使用对象化 locations，不携带无限逐行列表。

5. TN PrepareCommit 验证：

   - 每个源对象仍是当前可见且版本完全相同；
   - Lifecycle Snapshot、schema/policy/binding/generation 仍匹配；
   - Feature Guard version/digest 仍匹配 GA support matrix；
   - 没有遗漏需要处理的并发 tombstone；
   - created object、transfer 和 tombstone locations 的 digest/count 完整；
   - Entry/WAL/RPC/Prepare budget 未超限；
   - 输出对象数量不与 `NoTransfer` sentinel 冲突。

   Prepare 只能执行确定、幂等、有界的本地 metadata 校验和 txn entry 注册；禁止远端 ArchiveStore I/O、全量 payload/source 扫描、provider polling、无 deadline wait。必须使用事务 request deadline，禁止 `context.Background()`；任一 collect/transfer/validation 错误原样上抛，禁止返回 `nil`。

6. 原子提交：

   - 注册新活动对象；
   - 退休 exact source objects；
   - 转移 live-row tombstone；
   - Archive Job 发布 Manifest 并完成 Root handoff；
   - 写入 Job success 和 durable commit receipt。

任一条件失败，整个事务 abort，不能出现“Manifest 已发布但源数据还在”或“源对象已退休但 Manifest 不可用”。

### 13.3 Replay、Receipt 与 Commit unknown

CN 将 Catalog normal writes 和每个 TN 的 tagged Lifecycle Entry 一起放入同一个分布式事务。每个 participant 按稳定 request order 重放；Catalog row 和 TAE Object 变更都只在全局 commit 后可见。`ErrTAENeedRetry` 重建 TAE txn 时必须从原始 `TxnCommitRequest` 重放同一 Entry，不允许重新读取 mutable Job 状态拼请求。

最终事务涉及多个 participant 时必须走完整 2PC。只有 Catalog、tagged Lifecycle Entry 和 Receipt 确认落在同一个兼容 participant，且 replay/commit-unknown 语义与 2PC 等价时，才允许 1PC 优化；任何优化都不得省略 Entry、Receipt 或 durable commit result。集群 capability 未全部升级到兼容 wire/replay 版本时，不得启动会退休源对象的 Job。

`mo_lifecycle_commit_receipts` 在同一事务插入：

- intent ID/digest；
- job/attempt/generation；
- source/created/transfer/tombstone/dataset roots；
- commit timestamp；
- format/capability version。

TAE WAL/txn entry 同时记录 intent ID 和 digest，用于检测同一 Entry 的重复 replay。Receipt 对用户不可改，按 collection 分页有界保留；首个 GA 不承诺它进入 Backup/DR。

网络超时时不能根据 executor 本地状态判断成功或失败。Receipt 对账复用 MO 正常事务恢复和一致性读，不自建 participant apply watermark：

判断规则：

```text
txn service 明确 committed
  + 正常一致性事务读到匹配的 Receipt/Manifest/Root
    -> committed，做后处理

txn service 明确 aborted
    -> aborted，清理 attempt 或重新规划

txn service 仍 in-doubt
    -> 保留 Snapshot、Archive Attempt Root（如有）和 staging，继续 reconcile

txn service 已 committed
  + 一致性读取达到 commit_ts 后仍发现 root/digest 冲突
    -> terminal corruption alarm
```

owner DROP 可能在 final transaction 已提交后删除 tenant Job/Dataset/Receipt。为使这条合法路径可判定，final transaction 必须把 intent/Receipt digest 同步镜像到 system Root，并使用 Root/tombstone 的 MVCC commit timestamp 判断先后：

```text
txn service 明确 committed
  + Root == PUBLISHED 且 intent/receipt digest 匹配
  + matching owner DROPPED tombstone.commit_ts > lifecycle commit_ts
    -> committed-for-cleanup
       tenant Receipt/Manifest 缺失是预期 cascade，不报 corruption
```

Root mirror 只用于收敛与删除，不恢复被 DROP 的业务 Catalog。非一致性读取暂时看不到 Receipt/Manifest，或者某个 participant 尚未通过正常事务/logtail 可见，均只能 WAIT/RETRY，不能报 corruption。无法确认时继续持有 Snapshot 和 Root，不得猜测。故障测试必须覆盖 1PC/2PC、重复 Prepare、before/during Prepare、WAL append 前后、`ErrTAENeedRetry`、after commit、response lost、commit 后立即 DROP TABLE/ACCOUNT、滚动升级/降级和重复 replay，证明不重复注册对象、发布 Manifest 或错误清理 payload。

TTL 没有 Archive Root。若 transaction service 明确返回 TTL final transaction committed，且 owner tombstone 的 MVCC commit timestamp 晚于该 transaction commit timestamp，则 tenant Receipt 因 DROP 缺失同样视为 committed-for-cleanup；此规则只允许释放 Snapshot/清理 staging，不恢复 owner 或重放 retirement。

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

持续 UPDATE/DELETE 的 mixed Object 可能让 Archive 永久 abort/re-export。首个 GA 不增加长时间 handoff/fence 或 Archive Delete Vector；超过最大冲突时间后 Job 进入 `CONFLICT_BLOCKED`，释放未发布 staging，并只在 final transaction 明确未开始/aborted 后释放 Lifecycle Snapshot。活动数据保持完整可见，但该表不再享有 Archive Lag SLO。

### 14.4 DDL、DROP 与 Policy 变更

不持有数小时表锁。所有相关操作增加或改变 schema/lifecycle generation：

- ALTER schema/index；
- DROP/TRUNCATE/REORGANIZE；
- DROP TABLE/DATABASE/ACCOUNT；
- Policy bind/unbind/change。

这些操作都 CAS `FeatureGuard`；最终事务同时 CAS Guard 和 Binding generation。首个 GA 的 DROP 契约固定为：

1. DROP TABLE 在自己的 Catalog 事务中 CAS table Feature Guard 和 table Owner Registry；DROP DATABASE/ACCOUNT 只 CAS 对应 database/account-incarnation Owner Registry，不枚举全部 table Guard；
2. 同一事务把 system retained registry 的对应 Owner Registry 行置为 `DROPPED` tombstone；
3. DROP 按现有 MO 逻辑完成，不同步等待 executor stop、远端 Delete、Restore、provider 或 `COMMIT_UNKNOWN`；
4. Coordinator/Reconciler 观察 tombstone 后 fence 未进入 final transaction 的 executor，并停止新 PUT；
5. 已进入 final transaction 的 attempt 先按 transaction service + 一致性 Receipt 规则确认 committed/aborted；
6. Sweeper fence system retained registry 中已有的 Restore/read lease；在 Restore publish 明确收敛或最大 read I/O deadline 结束后释放 lease，再 CAS `access_generation`，将 Root/Dataset 置为 `DELETE_PENDING` 并异步清理；
7. owner tombstone 提交后，`purge_eligible_at` 不再阻止级联删除；进入 `DELETE_PENDING` 后禁止新 Restore/read lease；
8. Lifecycle Snapshot 只有在 final result 明确、在途源读结束后释放；无法判定时宁可多保留。

这里的“不等待”只排除 Lifecycle 自建的长期锁、executor/provider 等待和无限期 `COMMIT_UNKNOWN` 等待。DROP 对 Guard/Owner Registry 的 CAS 仍属于普通 MO 事务，允许按现有事务语义发生写冲突、重试、死锁检测或 statement timeout；它不能绕过一个正在提交的 final transaction，也不能因为追求 DDL 立即返回而取消上述串行化点。

这不会增加 `CASCADE ARCHIVES` 选项：首个 GA 的 Archive 天生从属于 owner，普通 DROP 就表示放弃恢复能力。用户若需要 DROP 后仍保留 Archive，应等待独立归档所有权能力，而不是让首个 GA 的 DROP 主路径承担合规语义。

## 15. 索引、CDC、外键和插件

Commercial GA 使用拒绝优先的 support matrix：

| 表能力 | GA 行为 |
|---|---|
| 无隐藏索引表的普通表 | 允许 |
| Base Table 内建 Primary Key，且没有独立 `IndexTableName` | 允许 |
| 物化为隐藏表的普通/唯一索引 | 拒绝 |
| active CDC | 拒绝 |
| FK 父表或子表 | 拒绝 |
| Publication/Subscription | 拒绝 |
| Fulltext/Vector/异步索引/外部插件 | 拒绝 |

准入不是“两次读取然后比较”，而是所有相关创建/删除操作、Policy bind/unbind 和最终短事务都写同一个 `FeatureGuard` 唯一行。创建不支持能力时若发现有效 Binding 就拒绝；Lifecycle bind 时若发现不支持能力就拒绝；两者首次并发创建时由唯一键冲突和 Guard CAS 决出唯一顺序。

任何未知依赖、Catalog 读取失败或 Guard version/digest 不匹配都 fail closed。

### 15.1 隐藏索引表

MO 的普通/唯一索引可能存储在隐藏表中。只 SoftDelete Base Object 会留下索引行，甚至造成唯一键永远冲突。

首个 GA 不实现这一 handler，直接拒绝任何 `TableDef.Indexes` 中 `IndexTableName` 非空的表。这个边界覆盖 [`pkg/sql/plan/build_ddl.go`](../../pkg/sql/plan/build_ddl.go) 生成的普通/唯一隐藏索引表，以及 [`pkg/sql/compile/ddl.go`](../../pkg/sql/compile/ddl.go) 的对应 handler。Base Table Primary Key 只有在不产生独立隐藏表时才允许。

CREATE INDEX 和 DROP INDEX 即使当时没有 Lifecycle Binding，也必须懒创建/CAS Guard；Policy 已绑定时 CREATE INDEX 拒绝，用户必须先停止并 unbind Lifecycle，等待在途 Job 收敛，再创建索引。这样首个 GA 不需要在一次对象退休事务中同步 Base Object 与隐藏索引表，显著降低 kill/replay/unique-conflict 风险。

未来若业务必须支持隐藏普通/唯一索引，需要单独 P0 原型和 ADR，证明：

- expired row 到隐藏索引行的精确 MVCC 身份；
- Base retirement 与 hidden-table delta 同 commit timestamp；
- kill/replay 和 `ErrTAENeedRetry` 不重复删除；
- 并发同 key INSERT/UPDATE 不被旧 delta 误删；
- Restore 全量重建与 digest 校验。

在这些证据完成前，不得把 handler 重新放回 GA 支持矩阵。

### 15.2 CDC

首个 GA 拒绝 active CDC。Object retirement 不天然等价于 CDC 逐行 delete，不能让下游继续保留已过期行。

未来开放前必须用独立 ADR 定义 replayable lifecycle delete log、commit timestamp、下游 capability 和 Receipt 对账；不得在事务外补发后直接标记成功。

### 15.3 外键

首个 GA 拒绝 FK 父表和子表。`RESTRICT/CASCADE/SET NULL` 都是跨表行级语义，不能在 engine 层绕过，也不能靠整对象 retirement 推导。

### 15.4 Publication、Fulltext、Vector 和外部插件

Publication 是 MO 将数据库/表发布给其他账户并由 Subscription 消费的数据共享能力。首个 GA 拒绝已经发布的源表和 Subscription 表，避免源端归档后订阅端是否应同步删除的语义不明确。

Fulltext、Vector、异步索引和外部插件同样拒绝。未来每种能力必须注册 lifecycle handler 并通过独立一致性验收后才能进入 support matrix；未注册永远是 bind/commit 拒绝，不是运行时静默跳过。

## 16. Archive 发布、访问租约和删除

### 16.1 状态机

Job：

```text
PLANNED -> PINNED -> RUNNING -> READY_TO_COMMIT
        -> COMMITTING -> SUCCEEDED
        \-> RETRY_WAIT / CANCELING / CONFLICT_BLOCKED
                         / OVERSIZE_BLOCKED / FAILED_TERMINAL
```

Dataset：

```text
STAGING -> VERIFIED_NOT_PUBLISHED -> PUBLISHED
                                      |
                                      v
                              DELETE_PENDING -> PURGED
```

Payload：

```text
UPLOADING -> VERIFIED -> DELETE_INTENT -> DELETING -> DELETED
                                           |
                                           v
                              DELETE_FAILED_MANUAL
```

可选 Deep Archive Profile 的 provider access 状态必须与 Payload 删除所有权状态分列保存，不能互相覆盖。Direct-readable Profile 始终保持 `DIRECT_READABLE`：

```text
DIRECT_READABLE -> TRANSITIONING -> RESTORE_REQUIRED
       ^              |                  |
       +-- failure ---+                  v
                                  THAW_REQUESTED
                                          |
                                          v
                                  THAWED_UNTIL(ts)
                                          |
                                          +-- expiry/access_generation+1
                                              -> RESTORE_REQUIRED
```

首个 GA 的 Restore Job 使用：

```text
PLANNED -> ACQUIRING_LEASE -> LOADING_STAGING
        -> VERIFYING -> PUBLISHED / FAILED
```

可选 Deep Archive Profile 只在 `ACQUIRING_LEASE` 和 `LOADING_STAGING` 之间插入 `REQUESTING_THAW -> WAITING_THAW -> ACCESS_READY`。Dataset 始终保持自己的 `PUBLISHED/DELETE_PENDING` 状态，不能用 `RESTORING` 或 access state 覆盖删除互斥判断。

### 16.2 删除所有权

`PURGE ELIGIBLE AFTER INTERVAL 7 YEAR` 的时间基准是 lifecycle column，不是“归档成功后再保存七年”。对一个 payload：

```text
purge_eligible_at =
  max(
    max_lifecycle_value_in_payload + purge_interval,
    published_at + provider_minimum_storage_duration
  )
```

使用 payload 中的最大业务时间，保证正常 owner 存在时其中每一行都达到配置的最短保存期。Writer 按 purge deadline bucket 组文件，避免一个很新的行让大量老数据长期无法清理。该语义不是 Legal Hold、WORM 或 maximum-retention 合规承诺；显式 DROP owner 会覆盖该时间。

owner DROP 提前删除 provider 有 minimum storage duration 的对象可能产生 early-deletion charge。DROP 不为此等待或交互确认，但审计事件、异步 cleanup estimate 和计费指标必须显示预计/实际费用；这属于“DROP 放弃 Archive”的产品代价。

不可变 key 只能防止重复覆盖，不能防止 stale runner 删除对象。Purger/Sweeper 首先在一个 MO 事务中执行 Dataset/Root CAS：

```text
dataset/root.state == PUBLISHED
AND (
  owner is ACTIVE
    AND dataset.purge_eligible_at reached
    AND normal purge grace reached
  OR matching owner tombstone exists
    AND drop cleanup grace reached
)
AND access_generation == captured
AND active_restore_read_leases == 0
AND no provider access operation in progress
AND every payload.state == VERIFIED
  -> dataset/root.state = DELETE_PENDING
  -> access_generation = access_generation + 1
```

owner 仍 `ACTIVE` 的正常 Purge 必须同时 CAS tenant Dataset 和 system Root。owner 已 `DROPPED` 时，tenant Dataset/Job 可能已被现有 DROP ACCOUNT 清理，此时 system Root + Owner Tombstone + system Access Lease 是唯一 cleanup authority；Sweeper 不得因 tenant row 不存在而放弃清理，也不得把“不存在”当成 payload 已删除。若 tenant Dataset 仍存在可以同步更新，但 owner-drop cleanup 的正确性不依赖它。

Restore/read lease 获取事务必须反向断言 `state == PUBLISHED` 并 CAS 同一个 `access_generation`。Lease 创建与删除 CAS 竞争时只有一个事务成功；一旦进入 `DELETE_PENDING`，状态不可撤销，不能新增 lease、取消删除或回到 `PUBLISHED`。如果业务以后需要保留，只能在进入删除前完成 Restore，或者从仍可读的源复制到一个新 owner/new immutable key。

Dataset 成功冻结后，每个 Payload 独立 CAS：

```text
dataset/root.state == DELETE_PENDING
AND payload.state == VERIFIED
AND key/version == frozen identity
  -> payload.state = DELETE_INTENT
  -> payload.state = DELETING
```

进入 Payload `DELETING` 后：

- 状态不可撤销；
- 如需恢复保留，只能复制到新 immutable key；
- Provider 支持 version ID/CAS 时必须按冻结的具体 version 删除；versionless Profile 只能删除 Root 冻结且全局永不复用的 immutable key，禁止删除会被其他 Dataset 合法复用的“当前 key”；
- stale/new runner 重复 Delete 才是安全幂等；
- provider 永久失败或无法证明结果时进入 `DELETE_FAILED_MANUAL`，Dataset/Root 保持 `DELETE_PENDING` 并告警；
- 所有 payload 确认 `DELETED` 后，Dataset 才能进入 `PURGED`。

Archive Container sidecar 自身也是一个带 version/hash 的 Payload，并且最后删除；在数据文件部分删除或人工处理期间，Root 仍保留冻结 key/version 清单作为审计和重复 Delete 的事实源。全部对象确认删除后 Root 才进入 `CLEANED`，随后继续保留到 quiescence window 结束。

## 17. Restore

恢复是 TaskService 异步 Job：

1. 用户指定 archive dataset 和必须有界的时间/业务谓词；
2. Manifest min/max 先裁剪 payload；
3. 在同一 Catalog 事务中创建 system Restore access lease，CAS Dataset/Root 仍为 `PUBLISHED`、`access_generation` 未变化，并条件递增仍为 `ACTIVE` 的 account/database/table Owner Registry `commit_seq`；
4. direct-readable Profile 按 Manifest 打开精确 payload identity（provider version，或 versionless immutable key + hash/etag）；可选 Deep Archive Profile 先 `RequestRestore` 并轮询到有界的临时可读状态；
5. 流式读取 Parquet 到按归档 schema 创建的隐藏 staging MO 表；
6. 校验行数、schema digest、文件 hash 和 Manifest root；
7. 原子 rename/publish 为新表；
8. Dataset 仍保持 `PUBLISHED`，Restore 只持有有界 access lease 和恢复记录。

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
- 一个 Restore Job 只允许一个 schema digest；若谓词命中多个历史 schema version，`EXPLAIN RESTORE` 列出分组并要求拆成多个目标表，首个 GA 不做隐式 schema union/coercion；
- 所有列类型必须无损解码，任何类型/版本未知时 staging 整体失败；
- 只恢复 Archive Container 内的 Base Table schema/data；首个 GA 不归档物化隐藏索引表；
- Base Table Primary Key 在 staging 中全量验证；跨 Dataset 出现重复 key 时以 `RESTORE_UNIQUE_CONFLICT` 整体失败，不丢行也不静默去重；
- default 和 comment 从 snapshot schema 复制；
- 只允许 bind 时已经验证为确定性、可重建的 generated column；恢复后重新计算并校验；
- auto-increment 创建独立 sequence，起点高于恢复数据最大值，不连接源表 sequence；
- 不复制 ACL、FK、CDC、Publication/Subscription、Partition、Fulltext、Vector 或插件状态；
- staging schema、row count、data digest 全部成功后才能原子 rename/publish；
- 任一失败回滚/清理隐藏 staging，不能留下部分可见表。

若 owner tombstone 已提交，或 Dataset/Root 已进入 `DELETE_PENDING`，Restore access lease 创建必须失败并明确返回“源 owner 已删除/归档正在删除”，不能返回空表。Direct-readable Restore 不依赖 provider access state；可选 Deep Archive 的 thaw 临时副本有 provider expiry，MO 必须在 expiry 前完成有界导入，否则重新申请，不能把临时 thaw 副本作为已发布表的长期底层文件。

全量恢复大数据集需要显式 `FULL` 和容量/费用确认。

### 17.1 Backup、PITR、Snapshot、Clone、Branch 与 DR

首个 GA 不复制 Archive Payload 或其权威 Dataset Catalog 到 Backup/DR。为避免“恢复成功但缺少已退休历史行”，以下矩阵必须在操作执行前 fail closed：

| 操作 | Lifecycle-bound 或存在 `PUBLISHED` Dataset 的表 |
|---|---|
| system-owned Lifecycle Snapshot | 允许；仅内部源保护，不可被用户 Restore/Clone |
| 用户 Snapshot 创建/Restore | 拒绝 |
| PITR 创建、绑定或 Restore | 拒绝 |
| Backup 创建/Restore | 拒绝 |
| Clone / Data Branch | 拒绝作为 source 或 destination |
| 加入 Publication/Subscription | 拒绝 |
| 加入 DR/replication/failover scope | 拒绝 |
| DR target 上 `RESTORE ARCHIVE` | 明确返回 `ARCHIVE_UNAVAILABLE_ON_DR`，不得返回空结果或“恢复成功” |

对 database/account scope 的保护操作，使用 8.2 节 system scope Guard 与 owner edge/root 做 O(1) 串行化，不能采用“先枚举当前 Binding，再逐表检查”的 TOCTOU 方案。Table-scope 操作 CAS table Feature Guard。反向地，Lifecycle bind 同时 CAS account/database scope Guard 与 table Guard，拒绝已经处于上述保护关系中的 scope/table。

Archive Dataset Catalog/Root 丢失后的业务恢复、跨地域副本以及 failover 后 Archive 可用性不在首个 GA SLA。Runbook 必须明确区分：

- 活动表 Backup/DR 未执行，因为准入已拒绝；
- Archive 在 DR 目标不可用；
- system-owned Root 仅用于清理，不等于可从对象存储自动重建业务 Catalog。

以后实现 archive-aware Backup/DR 时，应通过新 capability/ADR 开放，不能把当前 fail-closed 改成静默 best effort。

## 18. 故障恢复与所有权

每种资源都有唯一 owner 和 terminal cleanup：

| 资源 | Owner | 成功后 | 失败/接管后 |
|---|---|---|---|
| Lifecycle Snapshot | child Job/Attempt Root | final result 明确后释放 | `COMMIT_UNKNOWN` 继续保留 |
| TN reservation | job + executor epoch | final txn 后释放 | lease 到期 |
| 新 TAE Object staging | child Job | commit 后转 table 所有 | orphan GC |
| Archive Payload staging | system Attempt Root | Manifest 发布后 Root 状态转 `PUBLISHED` | Root 驱动 staging cleanup |
| Restore staging table | Restore Job | rename 后转用户所有 | Drop staging |
| Restore access lease | Restore Job/attempt | 新表发布或明确不再 resume 后释放 | retry 继续持有，terminal cleanup 后释放 |
| Provider thaw temp copy（可选 Deep Archive） | Provider request/version | staging load 完成后等待 provider expiry | 不主动当作永久副本，过期后重新申请 |
| Published Payload | Dataset + system Cleanup Root | Purge/owner DROP 后删除 | Sweeper/Reconciler 继续维护 |

Staging GC 不能只按文件年龄或“租户 Job 查不到”删除。它从 system Root 出发，确认 final transaction 明确 aborted/not-started、executor 已 fenced、Root 仍未 `PUBLISHED`，再以正式 `DELETE_INTENT -> DELETING` 协议删除。仍在上传或 `COMMIT_UNKNOWN` 的 key 不属于 orphan。

旧 executor 已提交给 provider 的迟到上传仍可能在第一次 Delete 后出现。因此 cleanup tombstone 不能马上删除：

- provider 有 versioning 时列举并删除该 immutable key 的全部未引用版本；
- 在 quiescence window 内持续 Stat/List，发现迟到版本就再次删除；
- `DELETED` tombstone 至少保留到最大 attempt deadline、provider request timeout 和 grace 都过去；
- provider 无法提供 version/list/一致 Stat 时，Profile 必须声明该限制并采用更长隔离窗口，不能宣称已经完成强删除，也不能进入包含 Purge 承诺的 Commercial GA。

所有外部操作必须有 deadline：

- object read/write；
- multipart upload；
- 可选 Deep Archive 的 provider thaw/status；
- reservation acquire/renew；
- Job attempt；
- DROP cancel wait；
- cleanup 和 Delete retry。

禁止在等待 provider、对象 I/O、Task 接管或用户确认时持有表锁。

容量准入按最坏瞬时占用计算：

```text
peak transient bytes =
  snapshot-exclusive retained source bytes
  + rewritten live ObjectIO bytes
  + archive staging bytes
  + dependency/index delta bytes
  + retry/orphan allowance
```

超过 account 或 cluster soft budget 时不启动新 Job；运行中越过 hard budget 时必须 fence 当前 Job，按明确提交结果收敛后再释放 Snapshot，而不是执行到中途靠 OOM、磁盘写满或提前释放保护限流。

### 18.1 安全、租户与审计

- Archive key 必须带不可伪造的 account/dataset 身份，所有 Catalog 查询强制 tenant filter；
- 系统归档 credential 由服务管理，不复用用户 Stage 临时凭据；
- Policy 管理、Restore 和 Purge 使用独立权限；
- 创建/变更 Policy、发布 Dataset、恢复、owner DROP、进入 DELETE_INTENT 和 provider Delete 都写审计日志；
- Manifest 保存 KMS key ID 和加密版本，密钥轮换不能要求重写所有 payload；删除 KMS key 前必须检查引用；
- DROP 源表/租户按正式产品契约级联放弃 Archive Restore；Root/Sweeper 异步删除，不等待 provider；
- Policy unbind 只停止新归档，不删除已发布 Dataset。

### 18.2 错误状态和接管责任

| 状态 | 自动重试 | 接管者 | 是否 terminal | 是否继续保留/阻塞 |
|---|---|---|---|---|
| `RETRY_WAIT` | 有界退避 | TaskService 新 executor | 否 | Snapshot/Root 保留 |
| `COMMIT_UNKNOWN` | 只允许 reconcile | Reconciler | 否 | Snapshot/Root/staging 保留 |
| `CANCELING` | 等待有界 stop | Coordinator/Reconciler | 否 | terminal cleanup 前保留 |
| `CONFLICT_BLOCKED` | 管理员/数据组织变化后重新规划 | Coordinator | 是（当前 Job） | 活动源保持可见；不承诺 Archive Lag SLO |
| `OVERSIZE_BLOCKED` | 提高经认证上限或修复 streaming 后 | Coordinator | 是（当前 Job） | 活动源保持可见 |
| `FAILED_TERMINAL` | 否 | 管理员确认/cleanup | 是 | 活动源未退休；cleanup 后释放 |
| `GC_SNAPSHOT_GATE_BLOCKED` | loader/owner 修复后 | TN GC owner | 否 | 不选源对象、不执行 PUT |
| `INDEX_REBUILD_REQUIRED` | 重建 generation | Indexer | 否 | 停止新 Job，不影响普通表 |
| `RESTORE_WAIT_ACCESS`（仅可选 Deep Archive 使用 `WAIT_THAW`） | polling/backoff | Restore executor | 否 | Dataset access lease 保留 |
| `DELETE_FAILED_MANUAL` | 否 | 运维/Purger | Payload terminal | Dataset/Root 保持 `DELETE_PENDING` |

所有 retry 都有 attempt 和 elapsed deadline；自动重试耗尽转 terminal/manual 并告警。`COMMIT_UNKNOWN` 不允许用户强制标记失败后释放 Snapshot 或删除 Root。

`COMMIT_UNKNOWN` 超过 transaction recovery SLO 后进入人工恢复队列，但状态仍保持 in-doubt：运维工具查询 transaction service、Receipt 和各 TN intent registry，只能写入“已提交”或“已确认 abort”的可证明结果，不能提供“强制释放 Snapshot/清理 Root”按钮。

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
- 预计 active rewrite、archive write 字节；
- provider storage/restore/early-delete 费用；
- Lifecycle Snapshot 的当前/预计独占保留字节；
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
- archived、active reclaimed、Lifecycle Snapshot exclusive retained bytes；
- whole/mixed/alignment ratio；
- estimated/actual rewrite amplification；
- source/index/archive bytes；
- Job progress、attempt、epoch、deadline；
- staging orphan、delete backlog、oldest age；
- Restore access state、ETA、读取字节和费用；可选 Deep Archive 额外展示 thaw ETA 和临时副本期限。

### 19.3 Commercial SLO、Backlog 与公平性

每个发布 SKU/Archive Profile 必须在 GA release artifact 中固定并验证：

- cutoff 到逻辑退休的 target lag 和 maximum supported backlog age；
- account/cluster 的 scan、rewrite、archive、restore、purge QPS/bytes/s；
- foreground P95/P99、普通 Merge backlog、TN memory/WAL 的自动降速和暂停阈值；
- provider 故障时 staging/orphan bytes、objects 和 oldest-age circuit breaker；
- oldest pending、Snapshot exclusive retained bytes、retry age、GC gate gap 和 delete manual 告警；
- TTL、Archive、Restore、Purge 的 weighted fairness 和最低份额。

大租户不能饿死小租户，Restore 不能饿死 TTL。TaskService/provider 不可用时数据保持活动可见，只增加 backlog。持续 UPDATE/DELETE 导致反复冲突的表在阈值后进入 `CONFLICT_BLOCKED`，首个 GA 对该表不承诺 Archive Lag SLO，但必须告警并解释原因。GA 门禁使用固定硬件、并发和 workload 生成可重复报告，不在设计中承诺未经测量的固定百分比。

## 20. 代码改动边界

### 20.1 新增为主

建议新增：

- `pkg/vm/engine/tae/lifecycle`：RewriteHost、transaction entry、source validation；
- `pkg/lifecycle/catalog`：Policy/Binding/FeatureGuard/Job/Receipt/Collection/Manifest/AccessLease；
- `pkg/lifecycle/ownership`：system-owned Attempt/Cleanup Root、Owner Tombstone、Sweeper；
- `pkg/lifecycle/scheduler`：indexer、scanner、coordinator、reconciler；
- `pkg/lifecycle/archive`：Parquet container、ArchiveStore、provider adapters；
- `pkg/lifecycle/admission`：Feature Guard 和未支持依赖/Backup/DR fail-closed；
- `pkg/pb/api`：`PrecommitWriteCmd.EntryList` tagged、版本化 `LifecycleCommitEntry`；
- SQL parser/planner/executor 的 Policy、TTL、Dry-run、Restore 入口。

### 20.2 对现有核心路径的最小改动

| 现有模块 | 改动 | 不改什么 |
|---|---|---|
| `tae/mergesort` | 必要时抽出可组合的小接口；优先零行为改动 | 排序、reshape、writer 和普通 Merge 调用语义 |
| `tae/db/dispatcher` | 增加有租期 external reservation 和空表快速路径 | 普通 Merge 候选和优先级 |
| `tae/rpc` | 现有 Precommit parser/iterator 增加 tagged Lifecycle Entry handler | MergeCommitEntry 语义和原请求 retry 链 |
| `disttae` | 新 lifecycle relation/txn 调用 | 普通 DML/Merge API |
| Snapshot frontend | 增加 `kind='lifecycle'` 的系统管理、用户隐藏、flush gate | 其他用户 Snapshot 语义 |
| TaskService | 注册 lifecycle task code/runner | epoch 的通用语义 |
| FileService | 不破坏 ObjectStorage；旁路增加 ArchiveStore | 活动 ObjectIO 读写 |
| GC/logtail | 暴露 Snapshot metadata-visible + old-cycle-drained gate；不新增 exact refs | Snapshot/PITR 原删除谓词和普通 Merge |
| DDL/Account Drop | 低频路径 CAS Feature Guard、写 system owner tombstone | 不等待 provider、不改变普通 DROP 主体语义 |

Feature 使用 global/account/table 三层 capability gate。表没有有效 Binding 时：

- Object create/delete consumer 不读取生命周期列 footer；
- 普通 Merge 不查询 Lifecycle Catalog；
- 普通 DML/查询不生成 Lifecycle Entry；
- GC 继续使用现有 Snapshot 保护；Lifecycle 只在 metadata-visible gate 成功后选择对象；
- scheduler、commit、Snapshot、archive/restore、purger 分别有 kill switch，停止新工作不能跳过在途 reconciliation。

### 20.3 明确禁止

- 不在普通 Merge callback 中查询 lifecycle policy；
- 不让 Merge 直接调用 ArchiveStore；
- 不用 SQL Partition ID 作为 Job 主键或原子边界；
- 不在活动 TAE Object 上配置云 Bucket Lifecycle；
- 不把原始 ObjectIO 当无版本契约的七年归档格式；
- 不使用用户 Stage credential 作为系统长期归档所有权；
- 不让一个 Job 跨整张 TB 表；
- 不以 TaskService epoch 代替对象/事务 CAS；
- 不对存在隐藏索引表或未知依赖的表直接 SoftDeleteObject；
- 不允许 stale runner 撤销 `DELETING` 或复用 payload key。

## 21. Capability Gates 与 Commercial GA

Gate 是内部实现和验收顺序，不是对外永久阉割的产品 Phase。所有 Gate 使用同一最终 Catalog、tagged Entry、Receipt、Lifecycle Snapshot、Attempt Root、Manifest、Access Lease 和错误模型。

| Gate | 能力 | Exit criteria |
|---|---|---|
| Gate A：Read-only | 单表 metadata Planner、Dry-run、Export-only Parquet/Container | 不退休活动数据；候选、行数和 digest 与全表基准一致 |
| Gate B：Safety Protocol | tagged Entry/Receipt、Snapshot/GC gate、pre-PUT Root、immutable Profile、Feature Guard、hard budget、whole/mixed fault prototype | GA-P0-A～F 的原型和 kill/replay/GC/超限矩阵全部通过 |
| Gate C：TTL GA Candidate | table-scope、NOT NULL 时间列、无隐藏索引表、whole/mixed TTL | 六项 GA P0 实现关闭；1/10 TiB、oversize Object 与 7 天 chaos/soak 通过 |
| Gate D：Archive GA Candidate | direct-readable Profile、Root/Sweeper、不可逆 Delete、Restore 新表、Backup/DR fail-closed | fake/real provider direct archive/restore/owner-drop/purge drill 通过 |
| Gate E：Commercial GA | 支持矩阵、SLO、Runbook、升级/降级、reconciliation/audit 工具 | 客户试点完成 direct archive、Restore 新表、DROP cascade cleanup 和 purge drill；发布评审签字 |
| Optional Gate F：Restore-required Deep Archive | provider transition/thaw、临时副本期限、费用和跨云故障矩阵 | 客户 provider 与成本收益明确；独立 ADR 通过；至少一个真实 provider 完成 transition/thaw/restore/purge drill |

任何 Gate 未通过都不能通过减少故障测试、放宽 budget 或关闭 fail-closed 来换取发布进度。Commercial GA 必须完成 Gate A–E，不要求完成 Optional Gate F；隐藏索引表、CDC、FK、Publication、Fulltext、Vector、插件和 archive-unaware Backup/DR 因为明确不在 GA support matrix，不要求实现 handler，但必须证明 Feature Guard 在首次创建竞态和 final commit 都会拒绝。

### 21.1 进入实现前的详细设计包

概要设计之后必须拆分并分别评审：

1. tagged Lifecycle Commit Entry、Precommit parser/iterator、1PC/2PC replay、WAL、Receipt；
2. system Lifecycle Snapshot、flush/GC metadata-visible gate、释放时序和 10 TiB pinned-byte budget；
3. system Attempt/Cleanup Root、Owner Tombstone、迟到 PUT quiescence 和 Sweeper；
4. Feature Guard、immutable Archive Profile 与 GA support matrix；
5. Archive Dataset/Collection/Manifest/Payload/Access Lease/不可逆 Delete；
6. direct-readable ArchiveStore 和 Restore staging/schema/publish；
7. Backup/PITR/Snapshot/Clone/Branch/DR fail-closed；
8. Object Index generation/backfill/reconciliation/obsolete GC；
9. oversize streaming、resource budget、scheduler、公平性和 SLO；
10. audit/reconciliation 工具和 Runbook。

`RESTORE_REQUIRED_ARCHIVE` 另行增加 provider-specific ADR 和实现设计，不阻塞上述 Commercial GA 详细设计包。

## 22. 验收矩阵

### 22.1 正确性

- 无过期、部分过期、全部过期；
- lifecycle 列是/不是 sort key；
- NOT NULL admission；全 NULL/部分 NULL schema 或坏数据必须拒绝/fail closed；
- 时区边界、DST、闰日、月末、epoch 单位/溢出、精度、cutoff 等值；
- 迟到 INSERT；
- 无 PK、Base Table PK；隐藏普通/唯一索引表的 bind、并发 CREATE INDEX 和 final Guard CAS 拒绝；
- active CDC、FK、Publication/Subscription、Fulltext/Vector/plugin 的 bind、首次创建竞态和 final Guard CAS 拒绝；
- Backup/PITR/用户 Snapshot Restore/Clone/Branch/DR 双向准入拒绝，DR target Restore Archive 明确报不可用；
- schema evolution、跨多个 schema digest 的 Restore 拒绝/拆分、跨 Dataset Base PK 重复的原子失败。

### 22.2 并发

- TN 普通 Merge；
- CN 手工 Merge；
- INSERT/UPDATE/DELETE 和新增 tombstone；
- ALTER/DROP/TRUNCATE；
- Policy change/unbind；
- Task epoch 接管；
- reservation 到期、TN restart；
- 用户 Snapshot/PITR/Backup/Clone/Branch/DR 创建与 Lifecycle bind 首次并发；
- Lifecycle Snapshot Catalog commit、GC metadata load、旧 GC cycle 和普通 Merge 同时推进；
- Restore access lease 与 owner DROP/`DELETE_PENDING` 同时推进。

### 22.3 Crash point

对每个点做 kill/restart：

- Lifecycle Snapshot/Job/`REGISTERED` Root 事务前后；
- flush gate、GC metadata-visible gate 和旧 GC cycle drain 前后；
- Root `REGISTERED -> UPLOADING` 后、第一次 PUT 前后；
- 每个 payload multipart；
- payload complete/verify；
- 新 TAE Object 写完；
- Archive Dataset `VERIFIED_NOT_PUBLISHED`、Root `VERIFIED`、Job `READY_TO_COMMIT`；
- final txn before/during/after Prepare；
- WAL append 前后、tagged Entry 重放和 Receipt 写入；
- commit response 丢失；
- Snapshot release；
- Restore access lease 创建与 Dataset/Root `DELETE_PENDING` CAS 竞争；
- DROP TABLE/ACCOUNT 的 owner tombstone 提交前后、tenant Job/Catalog 已删除但 Root 仍存在；
- DROP DATABASE/ACCOUNT 与第一次 Lifecycle bind、scope Backup/PITR/Snapshot/Clone/Branch/DR 准入同时 CAS scope Guard/Owner Registry；
- Restore executor 异常退出或 tenant Catalog 被 DROP 后，system retained lease 仍可被 fence、收敛和回收；
- stale executor 在第一次 cleanup 后迟到 PUT，`CLEANED` tombstone/quiescence 再清理；
- Payload DELETE_INTENT、DELETING、DELETE_FAILED_MANUAL、provider delete、Dataset PURGED；
- direct-readable payload open/read、staging load、校验和 rename；
- 可选 Deep Archive Profile 另行覆盖 transition/thaw、event 丢失、polling 和临时副本 expiry。

### 22.4 资源与性能

- 10 万表/大量空表时不全库扫描；
- 1 TiB/10 TiB 表，32 B/256 B/4 KiB 行宽；
- 10 TiB 表持续普通 Merge 时的 `snapshot_exclusive_retained_bytes`、硬限额 fence 与明确结果后释放；
- whole/mixed `0%/50%/100%`，tombstone `0%/1%/20%`；
- 当前版本最大 rows/object、blocks/object、bytes/object、单 block varlen、几乎全存活 Mixed rewrite、spill/file hard limit；
- Job 拆分和多租户/TTL/Restore 公平性；
- foreground P95/P99 扫描、写入和 Merge 影响；
- transfer map、Parquet writer、tombstone staging 内存/磁盘上限；
- provider 限流、慢读、慢写和 eventual consistency；
- orphan 和 delete backlog 不无限增长；
- 同价对象存储时真实收益不被高估。

### 22.5 滚动升级

- Lifecycle 由 cluster capability gate 控制；
- 所有 TN/CN 支持新 tagged Entry wire/replay version 后才能启用执行；
- 旧节点收到新 Entry 明确拒绝，不能忽略；
- Manifest/container 有版本号和兼容读取器；
- 新 Lifecycle Snapshot gate capability 未全员可用前不得选源对象；
- 滚动降级前停止新 Job，并让所有在途 Snapshot/Root/commit-unknown 明确收敛；
- 回滚只停止新 Job，已发布 Dataset 仍可恢复和清理。

## 23. 主要风险与控制

| 风险 | 结果 | 控制 |
|---|---|---|
| Mixed Object 比例高 | 重写放大大 | index dry-run、sort key 建议、bounded job |
| 与 Merge/高频更新持续冲突 | 重复导出、永久饥饿 | reservation + CAS/retry；到阈值进入 `CONFLICT_BLOCKED`，不承诺 Lag SLO |
| 慢 copy 遇到 GC | 源对象消失 | system Lifecycle Snapshot + flush gate + GC metadata-visible/old-cycle-drained gate |
| 长 Job 的表级 Snapshot 过度保留 | 对象存储增长 | exclusive retained bytes budget；硬限额 fence 当前 Job；10 TiB sustained-Merge soak |
| 并发删除被归档复活 | 数据语义错误 | expired-row tombstone 变化时 abort/re-export |
| 隐藏索引残留 | 错查或唯一冲突 | 首个 GA 拒绝所有隐藏索引表；Feature Guard 关闭首次 CREATE 竞态 |
| CDC/FK/Publication/插件语义缺失 | 外部或派生状态不一致 | 所有能力创建 + bind + final commit CAS 同一 Guard |
| 首次 PUT 后 DROP ACCOUNT | staging 永久泄漏 | PUT 前 system Attempt Root；DROP owner tombstone；Sweeper |
| Snapshot 只 flush 未被 GC loader 看见 | GC 错删源对象 | 独立 GC metadata-visible + old-cycle-drained gate |
| 普通 Backup/DR 静默缺历史 | “恢复成功”但数据不完整 | support matrix 执行前 fail closed，DR target 显式 unavailable |
| Profile 被改指另一个 bucket | Restore 找不到或 Purge 误删 | Dataset/Root 冻结 versioned namespace identity，credential 独立轮换 |
| stale runner 删除新对象 | 永久丢失 | immutable key + irreversible delete state + exact-identity delete |
| 同价云存储无字节降本 | ROI 不成立 | capability/cost dry-run，分离运维收益 |
| direct-readable Restore 资源失控 | 前台抖动、恢复失败 | predicate、文件/字节/并发上限、ETA 和容量预检 |
| 可选深归档恢复时间不可控 | SLA 违约 | 独立 Profile/Gate、provider SLA、thaw 状态机和费用预检 |
| 单个 Object 超过普通 Job 上限 | 无限 split/retry 或 OOM | oversize-object streaming + `OVERSIZE_BLOCKED` + 最大边界认证 |

## 24. 架构决策

本设计的最终选择是：

1. 不使用 SQL Partition 作为生命周期底座；
2. 不实现生命周期 `ONLINE_COLD` 状态；
3. 不修改普通 Merge 策略；
4. 不新增 Merge Engine；Lifecycle Rewrite Executor 可重用流式排序/写对象原语，但使用独立 Host 和 tagged 事务 Entry；
5. 用增量 Object Index 代替每日全库数据扫描；
6. 以 bounded exact object set 作为原子 Job；单个超限 Object 使用 streaming，不假装还能拆 Object；
7. 首个 GA 用 system table-level Lifecycle Snapshot 保护源对象，通过 flush + GC metadata-visible gate 后才选择对象；不新增 exact refs；
8. Archive 用 typed Parquet/ZSTD 和独立 Manifest，不复制原 ObjectIO 充当长期格式；
9. 首个 GA 拒绝隐藏普通/唯一索引表、CDC、FK、Publication、插件和 archive-unaware Backup/DR；table 依赖 CAS table Guard，account/database 保护 CAS scope Guard；
10. 第一次外部 PUT 前创建 system Attempt Root；DROP 写 owner tombstone并异步级联清理；
11. Dataset 从属于源 table/account；使用 `DELETE_PENDING`、Payload 不可逆状态机和 exact-identity delete；不包含 Legal Hold/WORM；
12. Profile 的 namespace identity 不可变且版本化，credential rotation 与存储身份分离；
13. 首个 GA 必须包含 direct-readable archive 和恢复到新表；restore-required deep archive 是 Optional Gate F，不阻塞 GA；
14. 按 capability gate 实现，完成 Gate A–E 才能称为 Commercial GA；Optional Gate F 通过后才能开放对应 Deep Archive Profile。

这套方案的主要价值是：它把 Feature 加在 TAE Object 生命周期之上，最大限度隔离普通 Merge；同时没有用“隔离”换取数据正确性漏洞。即使客户已经把活动数据放在 S3/OSS/COS，MO 仍能通过缩小活动数据集、减少后台重写和查询面，并在 provider 支持时使用更低价归档类别，实现可测量、可解释的降本。

## 25. Commercial Review 规范闭环索引

下表表示设计要求已经进入唯一规范，不表示代码或测试已经通过：

| 收敛后 GA P0 | 规范闭环 | 实现证据要求 |
|---|---|---|
| GA-P0-A Lifecycle wire/replay | 13：`PrecommitWriteCmd.EntryList` tagged Entry，进入原始 WAL/retry 链 | 1PC/2PC、重复 Prepare、`ErrTAENeedRetry`、response lost、滚动升级/降级 |
| GA-P0-B immutable Profile identity | 8.2、11：Dataset 与 Root 冻结 profile version/namespace/exact object identity | Profile 改名、credential rotation、versioned/versionless Restore/Purge、误 namespace 删除测试 |
| GA-P0-C serializable Feature Guard | 8.2、13.2、15、17.1：table 依赖 CAS table Guard；account/database 保护 CAS scope Guard/owner edge | CDC/Index/Backup/DR 等与首次 bind 并发，唯一键/CAS 只能一方成功 |
| GA-P0-D pre-PUT Root 与 owner cleanup | 8.4、14.4、16、18：Root 在首个 PUT 前；DROP tombstone；迟到 PUT quiescence | system/tenant/TAE 同事务原型、upload 各点 kill、DROP TABLE/ACCOUNT、租户 Journal 消失、stale executor 迟到上传 |
| GA-P0-E Snapshot/GC 可见性与释放 | 5.1、12：commit -> flush -> GC visible/old-cycle drain -> object selection；deadline 非 lease | Merge/GC/Snapshot chaos、`COMMIT_UNKNOWN`、10 TiB sustained Merge、硬限额 fence |
| GA-P0-F Backup/DR fail-closed | 1.1、8.2、17.1：逐项支持矩阵、双向 Guard、DR target 显式 unavailable | Backup/PITR/Snapshot/Clone/Branch/DR 创建与 bind 的并发拒绝；无静默缺行 |

之前 Review 中仍然有效的正确性要求没有因范围收敛而消失：

| 要求 | 规范落点 |
|---|---|
| NULL/ZoneMap whole-object fast path | 10.1 |
| hard budgets 与 bounded transaction | 9.2、13.2、18 |
| Dataset/Root/Payload 不可逆删除 | 16 |
| one child Job / one Snapshot / one final transaction；Archive child 另有一个 Root/Dataset | 9.2 |
| Object Index generation 与 obsolete GC | 8.3 |
| table-only Policy、time/calendar semantics | 1.1、8.1 |
| late `commit_seq` 与 exact source object CAS | 13.2 |
| Restore staging/schema/atomic publish | 17 |
| oversize single-object streaming | 9.2、22.4 |
| 高频冲突进入 `CONFLICT_BLOCKED` | 9.3、14.3、18.2 |
| SLO、公平性和真实 provider drill | 19.3、21、22 |

方案级最终判断：

- Read-only Planner / Dry-run / Export-only：**Go**；
- Safety Protocol 原型：**Conditional Go**，先实现并证明 GA-P0-A～F；
- TTL/Archive 数据退休 Preview：六项 P0 原型和故障矩阵通过后 **Go**；
- 首个 Commercial GA：按本收敛支持矩阵为 **Conditional Go**，只有 Gate E 与全部实现/测试证据通过后才能发布；
- restore-required Deep Archive：不阻塞 GA，仍为 Optional Gate F。

因此，“文档方案已闭环”不等于“当前代码已经 Commercial GA”。实现进入 Commercial GA 的唯一判据是 Gate E、六项 P0 证明义务和验收报告，而不是文档中已经描述。
