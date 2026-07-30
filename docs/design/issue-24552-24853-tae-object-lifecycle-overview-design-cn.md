# MatrixOne TAE 对象级数据生命周期概要设计

> 关联 Issue：[matrixorigin/matrixone#24552](https://github.com/matrixorigin/matrixone/issues/24552)、
> [matrixorigin/matrixone#24853](https://github.com/matrixorigin/matrixone/issues/24853)
>
> 实现级设计：[docs/design/tae-object-lifecycle/README.md](tae-object-lifecycle/README.md)
>
> 决策：**Conditional Go**。
>
> 产品阶段：**Phase 1 Commercial GA子集**。它满足#24552核心TTL，并交付#24853的表级
> Archive/Restore/Purge，不包含ONLINE_COLD、Deep Archive和account/database继承，不能
> 宣称完整关闭#24853。

## 1. 要解决的客户问题

客户的大表通常同时包含：

- 仍在高频查询和更新的近期数据；
- 已经过业务保留期、可以直接删除的数据；
- 很少查询但因审计、投诉或历史分析仍要保留的数据。

MO 的活动数据已经位于 S3、OSS、COS 或兼容对象存储。Lifecycle 的主要收益不是“换一种
压缩算法”，而是：

1. 把不再需要在线执行引擎能力的数据从活动 TAE 集合中退休；
2. 减少普通查询、Merge、checkpoint、GC、Metadata和缓存管理的活动数据规模；
3. 可选地把归档文件放到客户 Stage 对应的更低价存储层；
4. 用 direct-readable Parquet/ZSTD 保留跨系统访问和恢复能力。

如果客户的对象存储没有更便宜的存储层，Lifecycle仍能减少活动数据库管理成本，但外部
Payload本身未必显著降低每GB存储单价。产品不能把压缩率包装成分层收益。

### 1.1 典型场景

客户 A 在沙特运营支付或订单系统：

- 0～90 天：在线表正常查询、更新和风控；
- 90 天以后：Archive 到客户指定 Stage，在线表不再包含这些行；
- 偶发调查：`RESTORE ARCHIVE`恢复到一张独立新表，调查结束后删除新表；
- 达到客户规定的最终期限：异步 Purge Archive。

Lifecycle不提供`ONLINE_COLD`。归档数据默认不参与普通SQL查询，查询前先Restore。

## 2. 产品范围

首个 Commercial GA 支持：

- 显式表级 Binding；
- TTL和Archive；
- Whole Object直接退休；
- TTL小Mixed普通DELETE；
- Archive Mixed和中大TTL Mixed单源Rewrite；
- Parquet/ZSTD、full readback和Restore新表；
- 500～1000张绑定表，常见1 TiB、认证10 TiB单表。

不支持：

- 逻辑分区表和物理Partition child（Phase 1在Bind时拒绝）；
- 查询时TTL过滤、ONLINE_COLD、Deep Archive；
- Archive Mixed大批量普通DELETE；
- CDC/FK/Publication/隐藏索引/插件；
- Lifecycle-aware Backup/PITR/DR；
- Legal Hold/WORM；
- DROP后继续承诺Restore。

## 3. 为什么不依赖 SQL Partition

当前方案直接面向 TAE Object：

- SQL Partition不是物理隔离和GC所有权边界；
- 普通Merge可能跨输入Object生成新Object；
- Lifecycle需要处理未分区表和已有TB级表；
- Object identity、ObjectStats、MVCC和GC才是实际数据文件生命周期。

Phase 1并不因为“不依赖SQL Partition”就自动支持分区表：当前Binding只冻结一个
`physical_table_id`，因此逻辑分区表和物理Partition child在Bind时直接拒绝。未来支持时，
一个逻辑Binding必须显式展开为多个物理child；Partition可作为Planner hint，但不进入
当前安全协议。

## 4. 总体架构

```text
                         +--------------------------+
ALTER TABLE ...          | Lifecycle Binding       |
SET LIFECYCLE ---------->| policy/column/stage     |
                         +------------+-------------+
                                      |
                                      v
                         +--------------------------+
                         | Metadata Discovery       |
                         | only bound tables        |
                         +------------+-------------+
                                      |
                       Whole / TTL small / Rewrite
                                      |
             +------------------------+----------------------+
             |                                               |
             v                                               v
 +----------------------+                         +----------------------+
 | Exact Reader         |                         | LifecycleRewriteHost |
 | Archive Writer       |                         | existing merge core  |
 | Parquet/ZSTD         |                         | D/E/L split           |
 +----------+-----------+                         +----------+-----------+
            |                                                |
            +----------------------+-------------------------+
                                   v
                       +--------------------------+
                       | short final transaction  |
                       | Dataset/TTL record        |
                       | thin retire control       |
                       | exact Object CAS          |
                       +-------------+------------+
                                     |
                       +-------------+-------------+
                       |                           |
                       v                           v
             existing TAE WAL/GC         Cleanup Root/Sweeper
             active object lifecycle     external temporary files
```

普通查询、DML、Merge、WAL、Replay和GC不理解Lifecycle Policy。

## 5. Object处理算法

### 5.1 Discovery

Scheduler只遍历显式Binding。它分页读取当前TAE Metadata，利用Object/Block ZoneMap、
行数、大小和时间列min/max形成有界内存Candidate：

- `max(lifecycle_column) <= cutoff`：Whole候选；
- `min <= cutoff < max`：Mixed候选；
- `min > cutoff`：未到期。

Metadata分类只是hint。Reader和final transaction必须重新验证。

`ObjectStats`只直接包含sort key ZoneMap。Lifecycle列不是sort key时，Planner通过
ObjectLocation有界range-read该列的Object metadata/Block ZoneMap并聚合min/max；这仍是
metadata I/O，不是全行扫描，但必须受requests/bytes/deadline限制。

不建设逐Object Lifecycle Catalog Index，因为MO的`PartitionState`已经维护当前Object的
name/TS B-tree；再复制一份会同时引入Merge更新、replay、热点和一致性成本。Lifecycle
需要新增的是基于现有B-tree的有界分页Discovery API，而不是调用会物化全表结果的
`GetNonAppendableObjectStats`。Cursor只在Binding保存O(1)进度hint，丢失或snapshot stale
时从头重扫；每个cycle最终wrap。GC metadata不是当前可见Object集合的权威来源。

### 5.2 Whole Object

```text
固定S
-> 注册现有GC SyncProtection
-> 重新Stat exact源文件
-> exact Reader读取可见行
-> Archive时写Parquet并full readback
-> 短事务exact CAS源Object
-> 发布Dataset/TTL Receipt并SoftDeleteObject
-> 现有TAE GC异步物理删除
```

Whole允许有界多源，但所有source共享同一个S，按Object ID稳定排序，并在一个事务中对完整
source set做exact CAS。任一冲突，整个batch失败。

### 5.3 Mixed Object

Mixed严格一次一个source Object。Reader交给`DoMergeAndWrite`的必须是完整原始Block：

```text
D = S时已删除
E = S时可见且已过期
L = S时可见且未过期

delete bitmap = D ∪ E
Archive output = E
new live TAE Object = L
```

`DoMergeAndWrite`是CreatedObjs顺序和TransferTable的唯一producer。Lifecycle不预先过滤L、
不重排行、不重建destination mapping。

Archive Mixed一律Rewrite。TTL小Mixed在严格预算内可走普通`Relation.Delete`，超限走Rewrite。

### 5.4 普通Merge并发

Lifecycle不接管整张表Merge，也不修改普通Merge选择算法。普通Merge先替换source时，
Lifecycle final exact CAS失败并重新扫描；Lifecycle先提交时，普通Merge沿用现有Object
Drop Intent/MVCC冲突。

SyncProtection只防GC删除当前attempt仍在读取的源文件，不阻止普通Merge。

## 6. Archive格式与内容验证

Archive使用Parquet/ZSTD。Manifest保存：

```text
dataset_id
root_id / attempt_id
versioned logical schema descriptor
schema_descriptor_digest
canonical_encoder_version
hash_formula_version
total_chunk_count
dataset_content_hash
file ordinal/key/size/SHA-256/row_count/logical_bytes
row-group ordinal/row_count/logical_bytes/chunk hash
total row_count
必要的min/max
stage namespace identity
encryption/KMS identity
```

稳定行序：

- Whole：Object ID、Block ordinal、Row offset；
- Mixed：单源Block ordinal、Row offset；
- 文件和Row Group使用单调ordinal。

逻辑值编码包含row、column ordinal、type、null和length framing，覆盖DECIMAL、
TIMESTAMP、JSON、CHAR、NaN等类型。Archive Writer产生的chunk hash及Dataset有序聚合
hash必须与Provider full readback后的decoder结果相等。

不使用Merkle Tree或逐Cell持久化hash。

逻辑schema descriptor保存稳定列顺序、列名/`source_column_id`、MO类型、width/scale、
nullability、charset/collation和必要的AUTO_INCREMENT属性。源Column ID只用于lineage；
Restore新表由普通DDL分配新ID并按结构字段校验，不新增第二个restore schema digest。
Phase 1 Restore只恢复列结构和数据，不恢复PK、二级索引、FK、CDC、Publication、
默认表达式、权限或策略。

一个Chunk严格等于Manifest中的一个Parquet Row Group。文件和Row Group分别按ordinal排序，
展平后从0开始连续分配全局`chunk_ordinal`；Restore不得按运行时bytes、Batch或worker版本
重新切分。每个Chunk保存ordinal、row count和canonical content hash；Manifest保存
`total_chunk_count`、`dataset_content_hash`和`hash_formula_version`。Dataset内容Hash按
ordinal聚合这些Receipt字段。Archive Writer、full readback和Restore使用同一公式，CN
crash后可从Chunk Receipt重建，不持久化SHA内部状态，也不重新扫描隐藏表或全部Payload。

每个Row Group必须同时满足认证的最大Restore行数和未压缩canonical logical bytes上限；
Writer在越界前flush，单行自身超限则`RESOURCE_BLOCKED`，禁止退休源数据。Manifest保存
每个Row Group的`logical_bytes`，Restore在GET/解码前后分别校验声明值和实际值。

## 7. Stage与外部对象

复用现有Stage概念，不建立完整Archive Profile系统。Binding、Dataset和Cleanup Root冻结：

```text
stage_id
provider
canonical endpoint
region
bucket/container
immutable prefix
storage class
encryption/KMS identity
credential handle
```

首个GA只接受IAM Role、workload identity、部署管理credential alias或system secret handle。
Root不保存明文secret。Storage location在仍有Binding/Dataset/Root/Restore引用时不能原地
修改；credential可以轮换但handle必须长期可解析。

Phase 1 Archive Stage还必须是部署认证/allowlist中的专用、Versioning关闭的
Bucket/Container。通用FileService不扩展Bucket Versioning查询或version ID删除。运维开启
Versioning属于破坏受支持部署合同的外部配置；首个GA不建设自动漂移状态机，发现后暂停该
Stage并由Provider运维工具清理历史版本。

## 8. Cleanup Root

第一次外部副作用前创建一条system-owned Root：

```text
root_id / attempt_id
owner account/table
Archive namespace + deterministic prefix
TAE/FileService namespace + segment/range/booking upper bound
manifest key/digest
state/version
cleanup_after
```

一条Root覆盖一个attempt，不建立逐对象明细。所有key包含`root_id/attempt_id`且不复用：

- Payload使用`payload-<ordinal>-<write-id>.parquet`；
- Manifest使用`manifest-<digest>.json`；
- Manifest只引用full readback已验证的不可变key；
- Manifest存在时按Manifest清理；
- Manifest尚未生成时按prefix LIST；
- TAE staging按预分配namespace/segment范围清理；
- 最大I/O窗口后再次LIST；
- 发现迟到PUT就重新计算quiescence。

Root在final结果前拥有Payload、booking和live staging；commit成功后live Object交给现有
TAE，Dataset控制Archive逻辑可见性，Root继续负责Payload物理删除。pre-final失败、
COMMIT_UNKNOWN收敛、Dataset Purge和owner消失的完整转换以详细设计04为准。

## 9. Final transaction

Archive在readback成功后、TTL在分类完成后创建一个短普通MO事务：

```text
ordinary transaction
  -> insert Dataset（Archive）或TTL Receipt
  -> append one thin Lifecycle retire entry
  -> commit immediately
```

thin entry只表达普通Catalog DML无法表达的TAE Object mutation：

- mode；
- source snapshot TS；
- Binding/table/schema identity；
- exact source ObjectStats set；
- Mixed CreatedObjs和现有external booking引用；
- Root/attempt/manifest identity；
- bounded Tombstone delta参数。

它不携带第二套Merge mapping证明，不用于Restore，也不创建通用事务状态机。

TN使用现有create/drop/transfer transaction entry、WAL和Replay。Provider文件不进入TAE WAL。

滚动升级先部署unknown Entry/version在Batch解析前fail closed的安全解析；全体CN/TN升级完成
前只允许Export-only，retirement默认关闭。这里使用发布开关和集群升级准入，不增加
HAKeeper Lifecycle capability协议。

## 10. 并发DELETE

普通Merge默认从final transaction StartTS收集Tombstone，不足以覆盖Lifecycle在S读取、
数分钟后提交的窗口。Lifecycle wrapper必须从S收集到Prepare：

- post-S DELETE命中L：transfer到新RowID；
- Archive命中E或任何NoTransfer：abort整个final transaction；
- Whole Archive只要命中source：abort；
- TTL命中E是冗余删除，可忽略；首版允许统一abort以降低分支复杂度。

扫描必须有内部deadline和工作量/内存上限。不能依赖客户端context强行取消Prepare。

## 11. Commit unknown

Lifecycle不建设Terminal Journal，也不承诺比普通MO更强的exactly-once：

```text
明确成功
  -> matching Dataset/TTL Receipt可见
  -> Root PUBLISHED

明确失败
  -> source保持可见
  -> Root DELETE_PENDING
  -> 后续fresh attempt

ErrTxnUnknown
  -> Root COMMIT_UNKNOWN
  -> 不删除staging
  -> 不启动相同source的新retirement
  -> 一致性读取matching Dataset/TTL Receipt和source状态
```

若长期不能确认，停止该表、告警和人工处理；unknown Root有硬上限。不能因为EOB、Drop
Intent或timeout就删除Root，matching Dataset永远优先解释为已发布。

## 12. DDL fence的最终定位

DDL fence保留在GA设计中，但排在实现最后：

- 当前没有证据证明普通MO Merge/DDL存在通用数据正确性Bug；
- 普通MO已经有`mo_tables`行锁、TAE Table MVCC和Object Drop Intent；
- Lifecycle新增了“旧schema外部Payload + Binding + Dataset”的跨系统语义，最终仍需验证。

Gate顺序：

1. 完成Reader、Archive、Cleanup、Whole/Mixed和exact source CAS；
2. 建立普通Merge与DROP/TRUNCATE/ALTER并发测试基线；
3. 建立Lifecycle finalization并发DDL测试；
4. 若复现普通MO通用Bug，提Issue并修复公共路径；
5. 否则只给绑定表实现薄fence：复用现有`mo_tables`行锁，重新校验Binding generation、
   physical table ID、schema digest和Lifecycle列。

不增加Feature Guard、Binding active-attempt字段或DDL分布式状态机。Scheduler默认单表
finalization并发1；Root-local CAS只决定本Root由哪个worker推进。两个Root可以同时
FINALIZING，重叠source最终由exact Object CAS决定。

## 13. Restore与Purge

Restore只恢复到独立新表：

```text
Dataset获取有期限lease
-> 解析Manifest并验证文件
-> 创建隐藏staging table
-> 串行分块普通INSERT
-> 数据/Chunk Receipt/Attempt进度同一普通事务
-> 按Receipt ordinal重建content hash
-> 校验schema/row count/content hash
-> 普通事务CAS匹配且未过期的Dataset lease
-> CAS Attempt状态、lease、Chunk/row进度和隐藏表精确身份
-> 一次性写Attempt.verified_content_hash
-> 原子发布新表、Attempt DONE并清除lease
```

Chunk事务不更新Hash；最终发布前必须证明Receipt严格连续覆盖Manifest声明的全部Chunk。
失败清理禁止按`staging_table_id`直接DROP；必须在同一普通事务中CAS非DONE Attempt，并确认
当前名称仍是该Restore的隐藏名且database/table ID完全匹配，再按隐藏名DROP。Rename后的
正式表保持原table ID，因此任一身份不匹配都必须停止清理。
Restore不使用tagged entry。Purge只更新Dataset状态：

```text
PUBLISHED + no active Restore lease -> DELETE_PENDING
  -> 禁止新Restore
  -> Sweeper删除Payload
  -> PURGED
```

显式Purge遇到有效lease返回`RESTORE_IN_PROGRESS`；后台Purge延迟重试。Purge事务不等待
lease或Provider。Restore发布和Purge写同一Dataset行，只能一方提交；进入
`DELETE_PENDING`后不存在继续读取的旧Restore。

DROP TABLE/DATABASE/ACCOUNT沿用普通MO业务语义，不等待Provider。Lifecycle后台根据缺失owner
和Root/Dataset状态异步清理，不建设owner tombstone或DROP专用状态机。

## 14. 资源与规模

初始并发：

```text
单表child             1
单数据库              2
单账户                4
集群Lifecycle child   8
集群Rewrite           1（认证后最多4）
单Dataset Restore     1
```

硬上限至少覆盖：

- binding count（配置和认证边界，不是分布式slot协议）；
- Candidate数量和Metadata页；
- Reader/encoder/transfer峰值内存；
- Provider PUT/readback并发与bytes；
- single Object rows/blocks/bytes；
- Tombstone delta；
- staging、Root unknown、cleanup backlog；
- Restore chunk和隐藏表。

Scheduler/CN admission失败只暂停Lifecycle。首个GA不增加TN Lifecycle专用permit，也不
承诺比普通Merge更强的TN资源保证；若认证负载触发公共Merge资源问题，关联公共Issue或
降低认证上限后重测，不建设Lifecycle私有补偿状态机。

## 15. Commercial GA门禁

必须完成：

- D/E/L交错、全D/E、全L；
- post-S INSERT/UPDATE/DELETE；
- 普通Merge抢先和exact CAS失败；
- SyncProtection续租失败/TN重启；
- PUT/readback失败和迟到PUT；
- CN/TN crash、response lost和WAL replay；
- DROP、Stage credential轮换、Restore/Purge竞态；
- 最大合法Object和未认证oversize Block拒绝；
- 1 TiB功能认证、10 TiB容量认证；
- 30天soak；
- 50 → 200 → 500 → 1000表放量；
- Lifecycle active coexistence下普通DML、Merge、checkpoint、GC、logtail吞吐与P99对照。

DDL fence验证是最后一个协议Gate，但仍是Commercial GA签署项。

## 16. 最终决策

| 能力 | 结论 |
|---|---|
| Metadata Discovery / Dry-run / Export-only | Go |
| Whole exact retire | Conditional Go |
| TTL小Mixed DELETE | Conditional Go |
| 单源Mixed Rewrite | Conditional Go |
| Parquet/ZSTD + full readback | Go |
| Restore新表 / Purge lease | Conditional Go |
| DDL fence | 设计保留，最后验证和实现 |
| Commercial GA | 全部P0和规模门禁完成后Go |

方案的核心不是建设一套新的存储引擎，而是：

> 用Reader和现有Merge生成归档与存活数据，用thin entry原子退休exact Object，用普通
> WAL/Replay/GC维护活动数据，用Cleanup Root维护外部副作用。
