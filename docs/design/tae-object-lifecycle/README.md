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

本目录只保留两份权威文档，避免同一协议分散在十余份文件中产生版本漂移：

| 文档 | 唯一职责 |
|---|---|
| 本 README | 产品范围、全局不变量、删除项和 Gate 顺序 |
| [commercial-ga-implementation-design-cn.md](commercial-ga-implementation-design-cn.md) | Catalog、接口、Object协议、清理、Restore、测试和代码任务 |

上位概要设计负责解释产品效果和架构取舍。Review 输入不是实现规范。

## 3. Commercial GA 固定范围

### 3.1 支持

- 显式表级 Binding；
- `NOT NULL DATE/DATETIME/TIMESTAMP` Lifecycle 列；
- TTL Whole Object；
- Archive Whole Object；
- TTL 小 Mixed 的有界 `Relation.Delete`；
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
- SQL Partition 作为正确性边界；
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
       +-- TTL小Mixed
       |     -> 有界SI事务
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

Lifecycle 不先过滤出 L 再拼 Batch，也不自己计算 destination mapping。
`DoMergeAndWrite` 是存活行位置映射的唯一 producer。

## 5. 全局安全不变量

### I-1 Archive before retire

只有 Payload PUT、full readback、文件 SHA-256、schema digest、逻辑内容 hash 和总行数
全部验证成功，才允许进入退休源数据的 final transaction。

### I-2 One atomic final transaction

Archive Dataset 或 TTL Receipt，与源 Object 退休、Mixed 新 Object 发布和 transfer，
必须在一个普通 MO 事务中原子提交。失败时源数据继续可见。

### I-3 Exact source wins

final transaction 不相信 Candidate。每个 source identity 至少覆盖：

```text
physical_table_id
object_id
ObjectStats bytes/digest
is_tombstone
source_set_digest
```

任一不匹配、已存在 Drop Intent 或 Object 不可见，整个事务失败。EOB 不是“本 attempt
已经成功”的证据。

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

Lifecycle 复用普通 MO 的 1PC/2PC、`ErrTAENeedRetry`、WAL、Replay 和 GC，不承诺比普通
MO 更强的 exactly-once，也不建立终态 Journal、私有事务执行器或第二套 Replay。

### I-7 Root before side effect

第一次 Provider PUT/multipart、TAE live staging 或 external booking 写入前，必须已持久化
一条 system-owned Cleanup Root。Root 冻结 Archive 与 TAE 两套 namespace、稳定凭据
handle和确定性 prefix；不建立逐对象明细表。

### I-8 Unknown is not aborted

`ErrTxnUnknown` 时 Root 保持 `COMMIT_UNKNOWN`，不删除 staging，不对相同 source 发起
新的退休。Reconciler只读 matching Dataset/TTL Receipt和当前source状态；仍不能确认时
停止该表并告警，不猜测结果。

### I-9 Restore/Purge is leased

Purge 进入 `DELETE_PENDING` 后禁止新 Restore；已有 Restore 只能在固定 deadline 内续租。
Sweeper 等 lease 终止或 deadline，再异步删除 Payload。Purge 事务不等待 Provider。

### I-10 Ordinary MO stays ordinary

未绑定表：

- 无 Lifecycle Catalog 行；
- 不进入 Scheduler；
- 普通查询/DML不增加状态机；
- 普通 Merge/WAL/Replay/GC算法不变；
- 不访问 Stage、Dataset、Root或Lifecycle admission。

### I-11 All growth is bounded

Reader内存、Provider I/O、staging bytes、external booking、Tombstone delta、Root unknown、
cleanup backlog、Restore staging、Job和Rewrite并发都有硬上限。达到上限只暂停
Lifecycle，不占用普通 MO 的保底资源。

### I-12 DDL fence is retained but implemented last

DDL fence 是 Commercial GA 的防御性合同，不是 Reader、Export-only、Whole/Mixed Rewrite
原型的前置：

1. 先测试普通 MO Merge/Object mutation 与 DROP/TRUNCATE/ALTER 的既有并发语义；
2. 再测试 Lifecycle finalization 与 Binding/schema/physical table变化；
3. 若证明是普通 MO 通用 Bug，提公共 Issue并修复公共路径，Lifecycle直接复用；
4. 若只是 Lifecycle 的外部归档语义，才实现绑定表专用的薄 fence：
   复用现有 `mo_tables` 行锁，重新校验 Binding generation、physical table和schema；
5. 不引入 Feature Guard表、active-attempt锁或 DDL 专用分布式状态机。

在该 Gate 完成前，受控 P0 可以在“无并发不兼容 DDL”环境运行；Commercial GA 不能跳过
验证结果和最终决策。

## 6. 最小权威数据

| 数据 | Owner | 用途 |
|---|---|---|
| Binding | tenant Catalog | Policy、Lifecycle列、Stage和O(1)扫描游标 |
| Dataset | tenant Catalog | 已发布Archive的唯一可见性入口 |
| TTL Receipt | tenant Catalog | TTL退休结果和unknown对账 |
| Cleanup Root | system Catalog | attempt、外部副作用和异步清理 |
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
- Merkle Tree和完整 Archive Profile版本系统。

thin retire control、现有 external booking、普通事务、Dataset/TTL Receipt和Cleanup Root
已经足够表达首个 GA。

## 8. 实施优先级

```text
Gate A  最小Catalog + Binding + Metadata Discovery
Gate B  Exact Reader + Parquet/ZSTD + full readback + Export-only
Gate C  Cleanup Root + Stage身份/凭据 + Sweeper
Gate D  Whole exact retire + thin commit-control
Gate E  单源Mixed Rewrite + post-S DELETE
Gate F  TTL小Mixed DELETE
Gate G  Restore/Purge lease
Gate H  DDL fence验证与最小实现（最后）
Gate I  1/10 TiB、30天soak、50→1000表放量
```

每个 Gate 都必须证明未绑定普通 MO 的吞吐、P99、内存、Merge、checkpoint、GC 和 logtail
没有不可接受回归。
