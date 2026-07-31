# MatrixOne TAE Object Lifecycle 详细设计索引

> 关联 Issue：[matrixorigin/matrixone#24552](https://github.com/matrixorigin/matrixone/issues/24552)、
> [matrixorigin/matrixone#24853](https://github.com/matrixorigin/matrixone/issues/24853)
>
> 上位设计：[MatrixOne TAE 对象级数据生命周期概要设计](../issue-24552-24853-tae-object-lifecycle-overview-design-cn.md)
>
> 状态：**Conditional Go**。Object 路线可以进入 P0 开发；完成本文定义的正确性、
> 故障、TB 级和放量门禁后，才能宣布 Commercial GA。

## 1. 设计目标

首个 GA 必须同时满足：

1. 可长期管理 500～1000 张显式绑定表和 TB 级数据，不是一次性 Export Demo；
2. 未绑定表不进入 Lifecycle 扫描，不增加 Catalog 行、事务状态机或查询过滤；
3. 不复制第二套 Merge、WAL、Replay、GC 或通用事务执行器；
4. Lifecycle 自己负责外部归档、源 Object 精确退休、Restore 和资源隔离；
5. MO 已有的通用事务问题由公共实现负责，Lifecycle 不建立私有补偿系统。

## 2. 权威文档

本目录采用“一份索引 + 一份总规范 + 一份需求追踪矩阵 + 九份单一职责子设计”，
恢复开发所需细节，但不恢复旧重型协议：

| 文档 | 唯一职责 |
|---|---|
| 本 README | 产品范围、全局不变量、删除项和 Gate 顺序 |
| [commercial-ga-implementation-design-cn.md](commercial-ga-implementation-design-cn.md) | Commercial GA总实现合同和跨文档数据流 |
| [00-requirements-traceability-cn.md](00-requirements-traceability-cn.md) | #24552/#24853原始需求、Phase 1覆盖和后续范围 |
| [01-product-sql-catalog-cn.md](01-product-sql-catalog-cn.md) | Phase 1产品、SQL、Catalog字段、审计回收和DDL合同 |
| [02-discovery-reader-archive-cn.md](02-discovery-reader-archive-cn.md) | Metadata分页、Reader、schema descriptor、Parquet和full readback |
| [03-object-retire-rewrite-protocol-cn.md](03-object-retire-rewrite-protocol-cn.md) | Whole/Mixed、thin entry、booking、post-S DELETE和升级 |
| [04-cleanup-root-reconcile-cn.md](04-cleanup-root-reconcile-cn.md) | Root状态机、Owner、commit unknown、迟到PUT和物理清理 |
| [05-restore-purge-drop-cn.md](05-restore-purge-drop-cn.md) | Restore新表、Purge lease、DROP和限制矩阵 |
| [06-observability-capacity-cn.md](06-observability-capacity-cn.md) | 配置、指标、告警、隔离和放量 |
| [07-p0-ga-test-matrix-cn.md](07-p0-ga-test-matrix-cn.md) | 基础安全门禁、协议P0全集、全路径故障测试和GA门禁 |
| [08-implementation-plan-cn.md](08-implementation-plan-cn.md) | Gate、包边界、PR边界和Definition of Done |
| [09-commercial-ga-runbook-cn.md](09-commercial-ga-runbook-cn.md) | Release gate、kill switch、故障处置、1/10 TiB与30天soak证据 |

子设计不得复制另一子设计的第二份状态机。上位概要负责产品效果；ADR只记录决策理由；
行业调研和Review输入都不是实现规范。

## 3. Commercial GA 固定范围

### 3.1 支持

- 显式表级 Binding；
- `NOT NULL DATE/DATETIME/TIMESTAMP` Lifecycle 列；
- TTL Whole Object；
- Archive Whole Object；
- TTL 小 Mixed 的有界 `Relation.Delete`（可关闭优化，首个发布默认关闭）；
- Archive Mixed 和中/大 TTL Mixed 的单源 Object Rewrite；
- Parquet/ZSTD direct-readable Archive；
- Provider full readback；
- Restore 到独立新表；
- DROP 后异步放弃 Restore 并清理归档；
- 常见单表 1 TiB，认证目标单表 10 TiB；
- 50 → 200 → 500 → 1000 张绑定表分阶段放量。

### 3.2 不支持

- `ONLINE_COLD`、查询时隐藏 TTL 行；
- Restore-required Deep Archive；
- 逻辑分区表和物理Partition child（Phase 1在Bind时直接拒绝）；
- Archive Mixed 普通 Row DELETE；
- 修改普通 Merge 候选、排序、writer、WAL 或 GC；
- CDC、FK、Publication、Fulltext、Vector、插件和隐藏索引表；
- Lifecycle-aware Snapshot/PITR/Backup/Clone/Branch/DR；
- Legal Hold、WORM、maximum retention；
- DROP 后继续保证 Restore；
- 恢复回原表或自动适配原表当前 schema。

不支持能力在开始前 fail closed，不能静默产生不完整结果。

## 4. 唯一 Object 路线

```text
显式 Binding
  -> Scheduler只扫描绑定表的当前TAE Metadata
  -> 有界Candidate（仅内存，丢失可重扫）
  -> 固定source_snapshot_ts
  -> 注册现有GC SyncProtection并重新验证exact文件
       |
       +-- Whole TTL
       |     -> 短final transaction
       |     -> TTL Receipt + exact Object retire
       |
       +-- Whole Archive
       |     -> exact Reader
       |     -> Parquet/ZSTD + full readback
       |     -> Dataset + exact Object retire，同一事务
       |
       +-- TTL小Mixed（可关闭优化，首个发布默认关闭）
       |     -> 未单独通过Gate F认证时统一进入单源Rewrite或Blocked
       |     -> 认证启用后才允许有界SI事务
       |     -> RowID/delete key + Relation.Delete
       |     -> TTL Receipt，同一事务
       |
       +-- Archive Mixed / 中大TTL Mixed
             -> 严格单source Object
             -> BlockDataReadNoCopy读取完整物理Block
             -> D(snapshot deleted) ∪ E(expired)作为delete bitmap
             -> DoMergeAndWrite只输出L(live)
             -> 复用现有CreatedObjs、TransferTable和external booking
             -> Dataset/TTL Receipt + 新Object发布 + 源Object退休，同一事务

提交成功 -> 现有TAE WAL/Replay/checkpoint/GC负责活动Object
外部临时文件 -> Cleanup Root和Sweeper负责
Restore -> Manifest校验 -> 隐藏staging新表 -> 普通INSERT -> 原子改名发布
```

Whole不是“一Object一Dataset”。首个Release Profile在同一有界Metadata page内最多合并
64个Whole source、累计最多4 GiB（任一先到即切分），一个batch只创建一个Dataset/Root并
对完整source set做exact CAS；Mixed始终严格单source。该上限可在1/10 TiB认证后下调，
不会成为Manifest持久格式的一部分。

Lifecycle 不先过滤出 L 再拼 Batch，也不自己计算 destination mapping。
`DoMergeAndWrite` 是存活行位置映射的唯一 producer。

## 5. 全局安全不变量

### I-1 Archive before retire

只有 Payload PUT、full readback、文件 SHA-256、versioned schema descriptor/digest、
逻辑内容 hash、总行数和Manifest自身readback全部验证成功，才允许进入退休源数据的
final transaction。

### I-2 One atomic final transaction

Archive Dataset 或 TTL Receipt，与源 Object 退休、Mixed 新 Object 发布和 transfer，
必须在一个普通 MO 事务中原子提交。失败时源数据继续可见。

### I-3 Exact source wins

final transaction 不相信 Candidate。每个 source identity 至少覆盖：

```text
physical_table_id
object_id
ObjectStats bytes/digest
source_set_digest
```

任一不匹配、已存在 Drop Intent 或 Object 不可见，整个事务失败。EOB 不是“本 attempt
已经成功”的证据。

退休entry的`data_sources[]`只包含Data Object；S时Reader使用的Tombstone Object只属于
Snapshot Reader和SyncProtection的`protection_set`，绝不进入source digest或
`SoftDeleteObject`集合。

### I-4 Full physical Block contract

Mixed Rewrite 输入保持原始 Object、Block 和 row offset 顺序。D/E 只通过 delete bitmap
排除，L 由现有 Merge writer输出。Lifecycle 不重排、修补或重建 TransferTable。

### I-5 Post-S DELETE is closed

增量 Tombstone 从 `source_snapshot_ts=S` 扫描到 Prepare：

- 命中 L：通过现有 transfer 转到新 RowID；
- Archive 命中 E/NoTransfer：整个事务 abort，fresh attempt重新归档；
- TTL 命中 E 在语义上可忽略；首个实现允许保守地统一 abort；
- Whole Archive 命中任意 post-S DELETE：abort。

扫描必须有内部 deadline 和可执行的内存/工作量上界。

### I-6 Existing MO owns common transaction semantics

Lifecycle 复用普通 MO 当前生产支持的悲观事务、单 TN 1PC、`ErrTAENeedRetry`、WAL、
Replay 和 GC，不承诺比普通 MO 更强的 exactly-once，也不建立终态 Journal、私有事务
执行器或第二套 Replay。未来公共 MO 支持 multi-TN/2PC 后，复跑同一原子性矩阵；
Lifecycle 不提前实现私有 2PC。

### I-7 Root before side effect

第一次 Provider PUT/multipart、TAE live staging 或 external booking 写入前，必须已持久化
一条 system-owned Cleanup Root。Root 冻结 Archive 与 TAE 两套 namespace、稳定凭据
handle和确定性 prefix；不建立逐对象明细表。

### I-8 Unknown is not aborted

有Root的attempt遇到`ErrTxnUnknown`时，Root保持`COMMIT_UNKNOWN`，不删除staging，并暂停
该Binding的新retirement。Reconciler只读matching Dataset/TTL Receipt和当前source状态；
仍不能确认时保持暂停并告警，不猜测结果。首个GA不为“只阻塞重叠source”建立Object列表。

Whole TTL和TTL小Mixed没有Provider、live staging或booking副作用，不为commit unknown专门
创建Root；它们沿用普通MO事务结果，并用TTL Receipt + exact source状态做幂等重扫。

### I-9 Restore/Purge is leased

Purge 只能在Dataset没有有效Restore lease时CAS进入`DELETE_PENDING`；显式Purge遇到有效
lease返回`RESTORE_IN_PROGRESS`，后台Purge延迟重试。进入`DELETE_PENDING`后不存在仍被
允许读取的旧Restore，Sweeper再异步删除Payload。Purge事务不等待Provider。

固定Row Group Chunk同时受认证行数和未压缩logical bytes上限，保证发布的Dataset可以由
单个普通INSERT事务恢复。隐藏表发布/清理都CAS同一Attempt；清理必须校验隐藏名称、
database ID和table ID，禁止仅凭Rename后仍不变的table ID执行DROP。

隐藏表使用大小写不敏感的精确保留形状
`__mo_lifecycle_restore_<32位十六进制restore-id>`。frontend仅做O(32)名称检查，拒绝
用户访问以及CREATE/RENAME到该命名空间；同前缀但非该形状的历史用户表不受影响。内部SQLExecutor继续复用普通
CREATE/INSERT/RENAME/DROP，不查询Lifecycle Catalog、不引入新权限或表类型。Manifest不
恢复源PK/索引，Chunk在逻辑内容Hash验证后复用现有`incrservice.InsertValues`生成目标普通
表自带的fake PK，再由同一普通事务调用`Relation.Write`。

Dataset lease、隐藏表CREATE和Restore Attempt INSERT必须在第一个普通事务中原子提交。
AUTO_INCREMENT归档最大正值写入Manifest并经full readback验证；最终Rename/DONE事务先
校验目标类型上限，再复用现有`incrservice.SetOffset(max, txnOp)`推进新表水位。
`PUBLISHING`只允许作为最终普通事务内部的CAS中间值，不单独提交；owner丢失或响应未知时
按目标名/隐藏名与table ID的一致性身份决定停止清理或参与普通事务DROP竞争，不增加
publish Journal。SQL重试若发现`DONE` Attempt的target名称仍精确映射原
`staging_table_id`，直接按发布成功收敛；目标表已经DROP时旧DONE不阻止同名新Restore。

### I-10 Ordinary MO stays ordinary

未绑定表：

- 无 Lifecycle Catalog 行；
- 不进入 Scheduler；
- 普通查询、DML和Merge不访问Lifecycle Catalog或增加状态机；
- 普通 Merge/WAL/Replay/GC算法不变；
- 不访问 Stage、Dataset、Root或Lifecycle admission。

可能与Lifecycle冲突的表级DDL只复用既有`mo_tables`行锁并执行一次索引化Binding
存在性查询，不取得集群级feature-row写锁。release gate关闭后Binding仍可能处于
PAUSED/BLOCKED并需要UNSET或DROP收敛，因此不为“feature off”另建CN缓存或第二套开关
状态。Snapshot/PITR/Publication等scope级空集合发布才跨既有feature-row barrier。上述
低频控制面检查不能扩散到普通查询、DML或Merge热路径。

### I-11 All growth is bounded

Reader内存、Provider I/O、staging bytes、external booking、Tombstone delta、Root unknown、
cleanup backlog、Restore staging、Job和Rewrite并发都有硬上限。Mixed Rewrite还受live/
expired写放大和账户/集群固定窗口source bytes预算限制；Restore按Dataset logical bytes
事务化限制账户active staging，全集群总量只作为监控/Stop-Ship阈值。达到上限只暂停
Lifecycle。首个GA通过Scheduler/CN并发、
单请求硬上限和active-coexistence门禁约束资源，不增加TN Lifecycle专用permit或比普通
Merge更强的资源协议。

### I-12 DDL fence is management-path only

DDL fence 不进入查询、DML或Merge热路径。实现只作用于`SET LIFECYCLE`和可能创建不兼容
依赖/改变表身份的管理操作：

1. 表DDL与`SET LIFECYCLE`复用现有`mo_tables`行锁；表DDL随后只做索引化Binding
   lookup或DROP detach，不写feature-row；
2. `SET LIFECYCLE`在持有表锁后更新一次system account中已经存在的`LIFECYCLE`
   `mo_feature_registry`行，关闭与scope级依赖发布的首次Binding空集合竞态；
3. Snapshot、PITR、Publication和Clone/Branch创建跨同一个feature-row barrier，再按索引
   查询目标scope中是否存在Binding；
4. CDC依赖PITR，因此复用PITR准入；物理Backup不是Archive-aware，因此只有release gate
   已关闭、全集群不存在Binding、非`PURGED` Dataset和未收敛Cleanup Root时才允许执行；
5. 已绑定表的TRUNCATE、ALTER、CREATE INDEX等不兼容DDL fail closed；DROP TABLE在同一
   `mo_tables`锁事务中删除Binding，DROP DATABASE在原数据库DDL事务中按database identity
   补删孤儿Binding；外部Payload按Cleanup Root异步回收；
6. Finalizer仍校验Binding generation、physical table、schema和exact source identity；
7. tenant异步upgrade期间，普通管理DDL仅把Lifecycle表的精确`ErrNoSuchTable`视为尚无
   Binding；Lifecycle命令仍fail closed。Cleanup Root的物理`account_id=0`和Restore表的
   同名哨兵列只兼容旧CN，不进入Lifecycle业务状态。

这个barrier复用已有单行，不为未绑定表创建Guard/Candidate/其他Catalog行，也不进入
普通表级DDL。feature关闭时跳过Binding/Restore调度和数据路径，但历史Cleanup Root仍由
Coordinator有界reconcile/sweep，避免关闭开关造成外部对象泄漏；仅scope级依赖发布和
`SET LIFECYCLE`使用屏障，普通查询、DML、Merge、checkpoint、GC和logtail永远不访问它。

接受的控制面代价仅限少量管理操作：新增FK为关闭父表首次Binding竞态会锁父表
`mo_tables`行；Clone/Branch等scope发布在其后台事务期间持有feature-row barrier；
`REMOVE @stage/...`在Provider删除期间持有对应Stage行锁。它们可能串行同一资源上的管理
操作，但不进入普通查询、DML或Merge，也不扩展成所有表DDL的全局锁。

## 6. 最小权威数据

| 数据 | Owner | 用途 |
|---|---|---|
| Binding | tenant Catalog | Policy、Lifecycle列、Stage和O(1)扫描游标 |
| Dataset | tenant Catalog | 已发布Archive的唯一可见性入口 |
| TTL Receipt | tenant Catalog | TTL退休结果和unknown对账 |
| Cleanup Root | system Catalog | 有外部/staging副作用的attempt和异步清理 |
| Restore Attempt/Chunk Receipt | Lifecycle Catalog | Restore租约和分块幂等 |
| 当前TAE Metadata | 现有TAE | source Object权威 |
| Manifest/Payload | Archive Stage | 归档内容权威 |

Candidate、统计窗口和 Object 列表都不是持久化权威数据。

## 7. 明确删除的旧设计

首个 GA 不实现：

- Lifecycle Terminal Journal；
- 公共 `LifecycleFinalizeContext`/Catalog Pair Token；
- generation BUILDING slot、G1/G2和专用 replay budget；
- Restore tagged entry；
- destination bitmap和TN业务语义重算；
- `SourceLayoutProof`和 Lifecycle Booking V1；
- HAKeeper Lifecycle capability传播；
- Feature Guard、Cluster Activation Slot；
- 持久 Candidate、rolling stats ring；
- account incarnation registry、owner tombstone和DROP专用状态机；
- Archive Receipt和Root Object逐对象明细表；
- Merkle Tree和完整 Archive Profile版本系统；
- TN Lifecycle专用permit、`RESOURCE_BUSY` final retry和私有资源状态机；
- 通用FileService version ID扩展和Bucket Versioning自动漂移状态机；
- Restore持久SHA内部状态、隐藏表全量重扫和第二个restore schema digest。

thin retire control、现有 external booking、普通事务、Dataset/TTL Receipt和Cleanup Root
已经足够表达首个 GA。

## 8. 实施优先级

```text
Gate A  最小Catalog + Binding + Metadata Discovery
Gate B  Exact Reader + canonical encoder + 固定Row Group Chunk + 可重启Hash + Parquet/ZSTD
Gate C  Cleanup Root + 认证Stage身份/凭据 + full readback + 测试/认证Export-only
Gate D  Whole exact retire + thin commit-control
Gate E  单源Mixed Rewrite + post-S DELETE
Gate F  TTL小Mixed DELETE（可关闭的性能优化）
Gate G  Restore/Purge lease
Gate H  管理路径依赖/DDL fence与滚动升级fail-closed
Gate I  1/10 TiB、30天soak、50→1000表放量
```

每个 Gate 都必须证明未绑定普通 MO 的吞吐、P99、内存、Merge、checkpoint、GC 和 logtail
没有不可接受回归。

Gate F未通过时关闭TTL small Mixed，TTL Mixed统一进入Rewrite或
`MIXED_LAYOUT_BLOCKED`；它不阻塞Whole/Rewrite核心GA。
