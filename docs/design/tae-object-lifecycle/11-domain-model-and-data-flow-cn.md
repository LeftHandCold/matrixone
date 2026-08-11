# Lifecycle Phase 1 实现方案定稿总结

> 对应实现 PR：[#26655](https://github.com/matrixorigin/matrixone/pull/26655)
>
> 对应产品 Issue：[#24853](https://github.com/matrixorigin/matrixone/issues/24853)
>
> 状态：**Implementation Freeze Candidate / Commercial GA Conditional Go**。
> 本文只总结 PR #26655 已经实现的能力和原理，不把未来规划写成当前能力。

## 1. 一句话结论

Phase 1 在显式绑定表上，以 TAE Object 为处理单位完成 TTL 和 Archive：Whole Object 直接精确
退休，Mixed Object 复用现有 Merge 重写存活行；Archive 在源 Object 退休前将过期行写成
Parquet/ZSTD 并完成 Provider 全量回读校验；最终使用一个普通 MO 事务原子发布 Dataset、
新 live Object 和源 Object 退休。归档数据可按单个 Dataset，或按源表和时间范围，恢复到
一个独立新表。

该实现覆盖 Issue #24853 的首期 TTL/Archive/Restore 主线，但不实现中间 `COLD`/
`ONLINE_COLD` 查询层，也不复制第二套 Merge、事务、WAL、Replay 或 GC。

## 2. 设计目标和约束

实现遵循四条固定原则：

1. Lifecycle 必须能长期管理 500～1000 张认证绑定表和 TB 级数据，不是一次性 Export Demo；
2. 普通 Query、DML、Merge、checkpoint、WAL、Replay、GC 不访问 Lifecycle Catalog，不增加
   Lifecycle 状态机；
3. MO 通用事务、Merge、WAL、Replay、GC 问题由公共实现负责，Lifecycle 不建设私有补偿；
4. Lifecycle 只解决自己新增的正确性和所有权问题：外部 Payload、源 Object 精确退休、
   Restore、Purge、故障清理和资源隔离。

因此当前实现明确没有引入：

- Lifecycle Terminal Journal；
- Pair Token、Binding active-attempt claim、全局 Feature Guard；
- 第二套 Merge/Transfer/WAL/Replay/GC；
- TN Lifecycle 专用 permit 或分布式 Slot；
- 持久 Candidate/Object Index、逐对象 Cleanup 明细表；
- Lifecycle 私有事务执行器或通用 exactly-once 框架。

## 3. 用户入口和产品语义

### 3.1 策略管理

```sql
ALTER TABLE db.events SET LIFECYCLE (
    COLUMN created_at,
    EXPIRE AFTER INTERVAL 90 DAY,
    ACTION ARCHIVE TO STAGE archive_stage,
    PURGE ELIGIBLE AFTER INTERVAL 365 DAY
);

ALTER TABLE db.events PAUSE LIFECYCLE;
ALTER TABLE db.events RESUME LIFECYCLE;
ALTER TABLE db.events UNSET LIFECYCLE;
```

Phase 1 只接受显式表级 Binding，以及 `NOT NULL DATE/DATETIME/TIMESTAMP` Lifecycle 列。
`EXPIRE AFTER` 表示最短数据年龄，不承诺在阈值到达的瞬间完成退休；实际延迟受 Metadata
扫描周期、Object 布局、系统负载、资源上限和 Archive Provider 状态影响。

### 3.2 查询和诊断

```sql
SHOW LIFECYCLE FOR TABLE db.events;
SHOW LIFECYCLE DATASETS FOR TABLE db.events LIMIT 100;
SHOW LIFECYCLE JOBS LIMIT 100;
SHOW LIFECYCLE RESTORES LIMIT 100;
```

其中 `JOBS` 主要展示 Archive/Rewrite Attempt 与 Cleanup Root，不是普通 TaskService 的无界
历史表；`DATASETS`、`JOBS`、`RESTORES` 等集合视图有界并支持分页参数。

### 3.3 Restore

```sql
-- 恢复一个内部发布单元
RESTORE ARCHIVE DATASET '<dataset-id>' TO TABLE history.events_part;

-- 恢复源表在一个时间范围内的归档数据
RESTORE ARCHIVE TABLE db.events
BETWEEN '2025-01-01' AND '2025-04-01'
TO TABLE history.events_q1;
```

范围 Restore 的 Phase 1 语义是明确的半开区间 `[from,to)`，不是 SQL `BETWEEN` 常见的双闭
区间。Restore 始终发布一个独立新表，不覆盖源表，也不自动写回当前源表 schema。

## 4. 总体架构

```text
ALTER TABLE ... SET LIFECYCLE
             │
             ▼
       Binding（策略与扫描游标）
             │
             ▼
Scheduler 只分页扫描已绑定表的当前 TAE Metadata
             │
             ├── Whole TTL ───────────────┐
             ├── Whole Archive ──┐        │
             └── Mixed Rewrite ──┼────────┤
                                │        │
                       Payload + Manifest│
                       Provider full readback
                                │        │
                                ▼        ▼
                   普通 MO final transaction
                   Dataset/TTL Receipt
                   + 新 live Object（Mixed）
                   + exact source Object retire
                                │
                 ┌──────────────┴──────────────┐
                 ▼                             ▼
       现有 WAL/Replay/checkpoint/GC     Cleanup Root/Sweeper
                                               │
                                               ▼
                                      外部 Payload 物理清理

Dataset/Manifest
       │
       ▼
Restore Attempt + 固定 Chunk Receipt
       │
       ▼
隐藏 staging table ──原子 Rename──> 独立新表
```

## 5. 核心领域对象

| 对象 | 粒度与 Owner | 作用 |
|---|---|---|
| Binding | tenant Catalog，一张绑定表一条 | 保存策略、Lifecycle 列、Binding generation、物理表/schema fence 和有界扫描游标 |
| Archive Attempt | 一次 Object 处理 | 以 `root_id + attempt_id` 标识本次 worker 和不可复用 namespace |
| Cleanup Root | system Catalog，一次有副作用 Attempt 一条 | Archive Payload、external booking、TAE staging 的唯一清理 Owner |
| Payload | Archive Stage 中的 Parquet/ZSTD 文件 | 保存实际归档行数据 |
| Manifest | 一个 Dataset 的不可变 JSON 目录 | 保存 schema、文件、Chunk、hash、行数、逻辑字节和 Restore 元数据 |
| Dataset | tenant Catalog，一次成功 Archive 发布一条 | Archive 的用户可见性、Restore、保留和 Purge 单元 |
| TTL Receipt | tenant Catalog，一次成功 TTL 退休一条 | TTL 幂等、对账和审计，不产生外部 Payload |
| Restore Attempt | tenant Catalog，一次 Restore 一条 | 冻结 Dataset 集合、范围、lease、隐藏表身份、进度和 deadline |
| Chunk Receipt | tenant Catalog，一个固定 Row Group 一条 | 以 `(restore_id, chunk_ordinal)` 保证分块导入幂等和断点续跑 |

一张表通常会随时间产生多个 Dataset。Dataset 不是“一张表的所有历史”，而是一次有界
Archive final transaction 成功发布的数据单元；时间范围 Restore 会把多个 Dataset 组合成
一个冻结的 Restore Attempt。

## 6. Discovery 与精确 Reader

Scheduler 不建设持久 Object Index，也不扫描集群全部表。它只枚举显式 Binding，并从现有
PartitionState/TAE Metadata 做有界分页：Candidate 只在内存中存在，崩溃后允许重扫，final
transaction 从不信任 Candidate。

每次处理固定 `source_snapshot_ts=S`，Reader 必须返回完整 ScanReport：

- 源 Object 的每个 Block ordinal 恰好读取一次；
- 原始 Block 顺序、行顺序和 row offset 不变；
- `D + E + L == ObjectStats.Rows()`；
- D 表示 S 时已经删除，E 表示已过期，L 表示仍存活。

Provider full readback 只能证明“写出的归档文件完整”，ScanReport 用于证明“源 Object 没有
漏读”。ScanReport 只在 CN 内存使用，不进入 wire 或 Catalog。

读取源 Data/Tombstone Object 前复用 MO 现有 SyncProtection：注册后重新 Stat exact 文件，
处理期间续租，final Prepare 再验证。续租失败或 TN 重启导致 Protection 丢失时，当前 Attempt
fail closed；它不是跨重启持久 Snapshot。

## 7. Whole Object 路线

Whole 表示源 Object 中所有当前可见行都已过期。

### 7.1 Whole TTL

```text
exact source set
→ post-S Tombstone 检查
→ 普通 final transaction
→ TTL Receipt + exact source Object retire
```

Whole TTL 不写外部文件，也不为普通事务结果建立 Lifecycle 私有 Journal。

### 7.2 Whole Archive

```text
exact Reader
→ Parquet/ZSTD Payload
→ Manifest
→ Provider full readback + hash/schema/row count 验证
→ 普通 final transaction
→ Dataset + exact source Object retire
```

Whole 支持有界多源：当前 Release Profile 最多 64 个 source Object、累计最多 4 GiB，任一
上限先到即切分。整个 source set 使用同一个 S；任一 source identity 冲突，整批 abort。

## 8. Mixed Object 路线

Mixed 表示同一 Object 中同时存在过期行和存活行。Phase 1 严格一次处理一个 source Object：

```text
读取完整原始物理 Block
→ D：S 时已删除
→ E：已过期，TTL 删除或写 Archive
→ L：仍存活
→ D∪E 作为 DoMergeAndWrite 的 delete bitmap
→ DoMergeAndWrite 只输出 L
→ 复用现有 CreatedObjs、TransferTable、external booking、Create/Drop/Transfer
→ final transaction 原子发布 Dataset/Receipt、新 L Object，并退休源 Object
```

Lifecycle 不先过滤 L 再拼新 Batch，不自行计算 destination mapping，也不复制 Merge writer。
`DoMergeAndWrite` 是 Created Object 和 Transfer mapping 的唯一 producer。

TTL 小 Mixed 的 `Relation.Delete` 优化没有在本 PR 交付；Phase 1 统一进入单源 Rewrite，或在
写放大/资源条件不满足时延后处理。

## 9. 并发 DML 与 exact retirement

Archive/Rewrite 可能持续数分钟，因此 final transaction 不能只比较 Object ID。thin
Lifecycle retire entry 携带并校验：

```text
binding generation
logical/physical table identity
schema digest + Lifecycle column identity
source snapshot ts
完整 data source ObjectStats identity
source_set_digest
SyncProtection job identity
```

`data_sources[]` 只包含要退休的 Data Object。Reader 使用的 Tombstone Object 只属于
`protection_set`，绝不进入 `SoftDeleteObject` 集合。

从 S 到 Prepare 之间的新 DELETE 使用现有 Tombstone/Transfer 语义收敛：

- Whole Archive 命中任意 source 行：abort；
- Mixed Archive 命中 L：通过 TransferTable 转移到新 RowID；
- Mixed Archive 命中 E/NoTransfer：保守 abort，重新归档；
- TTL 路径可以采用相同的保守 abort。

最终依靠现有 Object Drop Intent、MVCC 冲突和 exact Object CAS 保证重叠 source 最多一个事务
成功。不增加 Binding active-attempt 锁；不相交任务在数据上允许成功，表级 final fence 可能
使其顺序提交。

## 10. Archive 格式与内容完整性

### 10.1 为什么是 Parquet + ZSTD

Parquet 保留列类型、NULL、Decimal、Timestamp、JSON、二进制值和稳定 Row Group；ZSTD 是
Parquet 内使用的通用无损压缩算法，用更低对象存储容量和 I/O 换取可控 CPU。相较 CSV，
该组合避免转义、NULL、编码和类型恢复歧义，也适合按列批量 readback/Restore。

### 10.2 不可变对象与 Manifest

每次 Attempt 使用不可复用的 Root/Attempt namespace，Payload key 带唯一 write identity，
Manifest key 带内容 digest。旧 worker 的迟到 PUT 不能覆盖已验证、已发布的文件。

Manifest V1 保存：

- 版本化逻辑 Schema Descriptor；
- canonical encoder/hash formula version；
- 文件和 Row Group 的稳定 ordinal、key、size、SHA-256；
- row count、logical bytes、Dataset content hash；
- Lifecycle 列范围和 AUTO_INCREMENT 最大值；
- Root/Attempt/Dataset 与冻结 Stage identity。

每个 Row Group 同时受最大行数和解码后 logical bytes 上限约束。单行本身超过认证上限时，
Archive 返回 `RESOURCE_BLOCKED`，源 Object 保持可见，不发布无法恢复的 Dataset。

## 11. 原子发布与失败语义

Archive 只有在 Payload PUT、Manifest 写入和 Provider full readback 全部成功后，才进入短 final
transaction。该普通 MO 事务原子完成：

```text
Dataset 或 TTL Receipt
+ Mixed 新 live Object
+ TransferTable/booking
+ thin retire control
+ exact source Object retirement
= 一个普通事务提交
```

因此：

- Archive/readback 失败：源数据继续可见，Root 异步清理；
- source CAS 冲突：Dataset 不发布，失败 Root 异步清理；
- commit 明确成功：matching Dataset/Receipt 可见，Root 进入 PUBLISHED；
- commit 明确失败：Root 进入 DELETE_PENDING；
- `ErrTxnUnknown`：Root 保持 COMMIT_UNKNOWN，保留 Payload，不猜测 abort。

Reconciler 以 matching Dataset/TTL Receipt 为发布权威。长期不能确认时暂停相关 Lifecycle
工作并告警，不建设比普通 MO 更强的 exactly-once。

## 12. Cleanup Root 与外部对象回收

第一次 Provider PUT/multipart、TAE staging 或 external booking 前，必须先持久化 system-owned
Cleanup Root。Root 冻结 Archive namespace、TAE/FileService namespace、credential handle、
不可复用 prefix 和保守 `reserved_cleanup_bytes`。

主要状态为：

```text
REGISTERED → UPLOADING → VERIFIED → FINALIZING → PUBLISHED
      │            │           │          │
      └────────────┴───────────┴──────────┴→ DELETE_PENDING

FINALIZING → COMMIT_UNKNOWN
DELETE_PENDING → DELETING → CLEANED
```

Root 是所有未发布外部副作用的唯一 Owner；final commit 成功后的 live TAE Object 交回现有
WAL/checkpoint/GC，Root 继续负责 Archive Payload 的最终物理删除。

Cleanup 使用 Manifest 精确删除；Manifest 尚未形成时按 Root/Attempt prefix LIST/Delete。
Delete 后必须等待 quiescence，再次 LIST；发现迟到 PUT 会重新删除并重置静默期。Provider
故障只延迟当前 Root，不误标 CLEANED；单轮 sweep 有总时间预算和分页，避免一个慢 Provider
长期饿死整个 backlog。

## 13. Restore 与 Purge

### 13.1 单 Dataset Restore

单 Dataset Restore 读取并验证 Dataset/Manifest，使用一个普通事务同时完成：

```text
CAS Dataset Restore lease
+ CREATE 隐藏 staging table
+ INSERT Restore Attempt（含精确 database/name/table identity）
```

一个 Parquet Row Group 固定对应一个 Restore Chunk。每个 Chunk 的数据 INSERT、Chunk Receipt
和 `next_chunk_ordinal/restored_rows` 在同一普通事务提交。崩溃或响应丢失后按 Receipt 继续，
不会重复导入。

最终按 ordinal 验证 Receipt 连续性和聚合内容 hash，然后在普通事务中原子 Rename 隐藏表、
标记 Attempt DONE、释放 lease，并推进 AUTO_INCREMENT 水位。

### 13.2 时间范围 Restore

范围 Restore 不是扩大一次 Object 退休事务，而是组合已经发布的 Dataset：

1. 按 tenant、源逻辑表和时间重叠条件查询 PUBLISHED Dataset；
2. 最多选择 4096 个 Dataset，且必须具有一致的 schema、Lifecycle 列和 Stage generation；
3. 在第一次隐藏表副作用前，把有序 Dataset ID、选择 digest、`[from,to)`、Chunk 总数和逻辑
   字节数冻结到 Restore Attempt；
4. 初始化事务同时为全部选中 Dataset 加 lease、创建隐藏表并插入 Attempt；
5. 按 Dataset/Manifest 固定顺序把 Row Group 展平为全局 `chunk_ordinal`，边界 Chunk 在转成
   canonical rows 后精确过滤 `[from,to)`；
6. Resume 优先读取 Attempt 中冻结的 Dataset 集合，后续新发布的重叠 Dataset 不会混入；
7. 最终把所有过滤后 Receipt 聚合校验并发布为一个新表。

Range Restore 是同步但可重入、可断点续跑的 Phase 1 能力。deadline 根据选中逻辑字节数
计算，最少 24 小时、最多 7 天；CN 本地并发默认 1，并使用账户 Restore staging bytes 准入。

### 13.3 Purge

Dataset 到达 `purge_eligible_at` 后可自动或手工进入 Purge。Purge 与 Restore 通过 Dataset
行上的 lease/CAS 互斥：存在有效 Restore lease 时拒绝或延后；进入 DELETE_PENDING 后禁止
新 Restore。Purge 事务不等待 Provider，只触发 Root 异步物理清理。

Restore 发布与隐藏表清理都 CAS 同一个 Attempt，并校验 staging database ID、hidden name
和 table ID，禁止旧 worker 仅凭 Rename 后不变的 table ID 删除已发布新表。

## 14. Catalog 与普通 MO 影响边界

实现新增五张 tenant Lifecycle 表：

```text
mo_lifecycle_bindings
mo_lifecycle_datasets
mo_lifecycle_ttl_receipts
mo_lifecycle_restore_attempts
mo_lifecycle_restore_chunks
```

以及一张 system-owned cluster table：

```text
mo_lifecycle_cleanup_roots
```

这些表只被 Lifecycle Scheduler、控制面、Restore/Purge 和 Cleanup 使用。未绑定表没有 Binding
行，不进入 Scheduler；普通查询、DML、普通 Merge、WAL、Replay、checkpoint、GC、logtail
均不访问这些表。

低频管理路径只做必要的窄 fence：

- SET/UNSET 与不兼容表级 DDL 复用现有 `mo_tables` 行锁；
- FK DDL 在已有 parent 元数据事务路径中锁 parent `mo_tables` 行并 probe Binding；
- Stage 被 Binding 或非 PURGED Dataset 引用时，禁止 ALTER/DROP/REMOVE；
- 不使用跨租户全局 feature-row barrier；Lifecycle feature row 只保存本功能 release/config。

## 15. 资源隔离和有界性

当前实现的关键有界点包括：

- 只扫描显式 Binding，Metadata/Candidate 分页且仅在内存存在；
- Scheduler 的 cluster/account/database/table child 并发均有上限；
- Archive child 首发并发为低值，不增加 TN 专用 permit；
- Mixed 严格单 source，Whole source 数量和总字节有上限；
- Reader Block、Payload、Row Group rows/logical bytes、Manifest、wire、Tombstone delta、
  external booking 都有限制；
- Mixed Rewrite 有写放大和账户/集群窗口 source bytes 预算；
- Root unknown、cleanup backlog、reserved cleanup bytes、sweep wall time 有硬边界；
- Restore 有 CN 本地并发、账户 staging bytes、Dataset/Chunk/Manifest 数量和 deadline 上限；
- 资源不足只暂停、延后或拒绝 Lifecycle，不在 TN 中等待资源，不改变普通 Merge 配额。

Phase 1 不为“全集群 1000 Binding”建设分布式 Slot。1000 是首发认证人口和运维上限，
Scheduler/控制面本身保持有界。

## 16. Release、升级和故障注入

Lifecycle release gate fresh bootstrap 默认关闭。新 thin retire entry 改变 CN→TN wire，因此发布
顺序固定为：

```text
先完成全部 TN 升级和协议验证
→ 再升级/启用 CN Lifecycle
→ 最后显式打开 release gate
```

旧 TN、未知 protocol/version 必须在 Batch 解析和任何 TAE mutation 前 fail closed。当前实现
不增加 HAKeeper capability 协议，以部署准入保证 TN-first。

故障注入点覆盖 Archive Payload/Manifest PUT、full readback、final commit 前后、Cleanup
LIST/Delete、Restore initialize/chunk/publish 等关键边界。默认关闭时只保留常量级分支，不
改变普通路径；它用于验证：

- 失败时源数据仍可见；
- Dataset 不会提前发布；
- unknown 不会误删 Payload；
- Chunk 重试不重复导入；
- 迟到 PUT 最终被二次 LIST/Delete 收敛；
- CN/TN 重启后 Owner、lease 和 Cleanup 仍可恢复。

## 17. 已实现范围与明确不支持范围

### 17.1 PR #26655 已实现

- 显式 Binding 与后台有界调度；
- TTL Whole、Archive Whole；
- Archive Mixed 和 TTL Mixed 单源 Rewrite；
- Parquet/ZSTD、Manifest V1、full readback、canonical hash；
- exact source retirement、post-S Tombstone、SyncProtection；
- Cleanup Root、commit unknown fail-closed、迟到 PUT quiescence；
- 单 Dataset Restore 到新表；
- 按源表和 `[from,to)` 时间范围的多 Dataset Restore 到新表；
- Restore Chunk 幂等、断点续跑、AUTO_INCREMENT 水位；
- Dataset lease/Purge、Stage fence、必要 DDL fence；
- Release gate、升级 fail-closed、指标、SHOW 和故障注入。

### 17.2 Phase 1 明确不支持

- `COLD`/`ONLINE_COLD` 查询层和 Deep Archive；
- 分区表及其 physical child；
- 已绑定源表上的二级/唯一/全文/向量索引和 FK 共存；
- Restore 回原表或覆盖当前表；
- 恢复源表完整 PK、索引、FK、Check、CDC/Publication 等依赖；
- DROP 源表后继续承诺 Archive Restore；
- Archive-aware Snapshot/PITR Restore、Backup/DR；
- CDC/CCPR 退休事件完整性；
- Legal Hold、WORM、maximum retention。

Snapshot/PITR 创建和历史 GC 保护继续使用 MO 现有 MVCC/GC；Clone/Data Branch 只复制活动
数据且不继承 Binding/Dataset/Payload；普通 Publication 读取活动视图。Lifecycle 不向这些
已存在的普通管理路径增加全局 barrier。

## 18. 当前验证结论与定稿口径

当前实现已经覆盖：

- fresh bootstrap、默认关闭和周期任务空跑；
- TTL、Whole Archive/Restore、Mixed Rewrite/Restore；
- 流式数据、UPDATE/DELETE 逐行守恒；
- post-S Whole/Mixed DELETE/UPDATE；
- 范围边界、空范围、冻结 Dataset 集合和 Resume；
- wide types、NULL、JSON、BLOB、DECIMAL、时间类型和 AUTO_INCREMENT；
- Provider 429/503、readback、Cleanup LIST/Delete、迟到 PUT；
- final commit unknown、CN FINALIZING 崩溃；
- Restore initialize/chunk/publish 故障与重试；
- CN 切换、TN 重启、Unit/Race/BVT/静态检查。

因此可以把 PR #26655 作为 Phase 1 实现定稿候选，不再扩大协议面。后续只接受测试和 Debug
发现的 Lifecycle 自身正确性、资源泄漏、不可终止或普通 MO 回归修复。

Commercial GA 仍保持 Conditional Go：合并代码不等于完成商用认证。对外 GA 前仍需完成
1/10 TiB 实体规模、4096/4097 Dataset 边界、长期 soak、50→1000 绑定表 active-coexistence、
真实 Provider 长时故障，以及新 CN/旧 TN 发布门禁验证。若认证暴露 MO 公共 Merge/事务问题，
优先走公共 Issue/修复或降低 Lifecycle 认证上限，不在 Lifecycle 内建设私有补偿系统。
