# ADR：以 TAE Object 作为生命周期执行边界

> 状态：Accepted（2026-07-30 按 Commercial GA 简化基线修订）
>
> 适用范围：MatrixOne Issue #24552 / #24853
>
> 规范设计：[概要设计](issue-24552-24853-tae-object-lifecycle-overview-design-cn.md)、
> [Commercial GA实现设计](tae-object-lifecycle/commercial-ga-implementation-design-cn.md)

## 背景

早期方案要求用户先按时间列建立SQL Range Partition，再以完整分区归档。该方案不能覆盖
未分区表、已有其他分区键的表和TB级存量表，也会把数据生命周期问题放大为Catalog、路由
和DDL问题。

TAE已经使用不可变Object组织数据，现有Reader、Merge writer、RowID transfer、Object
MVCC、WAL和GC形成完整闭环。Lifecycle应复用这些原语。

## 决策

1. Lifecycle以当前TAE Metadata中的有界exact Object set为执行边界，不依赖SQL Partition。
2. 普通Merge算法不变。Merge抢先时Lifecycle exact CAS失败，不要求Merge等待Lifecycle。
3. Whole Object完成验证后直接`SoftDeleteObject`，物理删除仍归现有GC。
4. Mixed严格单source Object，读取完整物理Block：
   - D和E进入`DoMergeAndWrite`的delete bitmap；
   - E进入Archive或TTL discard；
   - L由现有writer输出；
   - TransferTable只由`DoMergeAndWrite`产生。
5. Archive Mixed一律Rewrite；仅TTL小Mixed可在硬预算内使用普通`Relation.Delete`。
6. Archive必须完成Provider full readback，Dataset与源Object退休在一个普通MO事务中提交。
7. 使用thin tagged retire entry表达TAE create/drop/transfer；不复制Merge、事务执行器、
   WAL、Replay或GC。
8. 读取期间使用现有GC SyncProtection。它是可失效租约，不是持久Snapshot协议。
9. 外部副作用由一条system-owned Cleanup Root管理，使用确定性namespace/prefix，不建立
   逐Object明细。
10. Restore只恢复到独立新表；Purge与Restore通过有期限lease协调。

## DDL fence决策

DDL fence保留为最后一个Commercial GA Gate，不作为Reader、Export、Whole/Mixed原型前置：

1. 先验证普通MO Merge/Object mutation与DROP/TRUNCATE/ALTER的既有行为；
2. 若发现通用MO Bug，提公共Issue并修公共路径；
3. 只有Lifecycle的Binding/schema/外部Payload语义仍有缺口时，才对绑定表增加薄fence；
4. 薄fence复用现有`mo_tables`行锁并重新校验Binding generation、physical table、
   schema digest和Lifecycle列；
5. 不增加Feature Guard、Binding active-attempt字段或DDL分布式状态机。

Root-local CAS只决定本Root的worker ownership。相同或重叠source最终由TAE exact Object
CAS互斥；同表Scheduler默认finalization并发1。

## 明确不选择

- SQL Partition作为正确性边界；
- 在普通Merge worker中执行Provider I/O；
- Lifecycle自己生成或修补destination mapping；
- 大Mixed普通DELETE；
- 查询时隐藏TTL行或ONLINE_COLD；
- Terminal Journal、专用FinalizeContext/Pair Token；
- Lifecycle Booking V1、SourceLayoutProof或destination bitmap；
- Feature Guard、Cluster Slot、持久Object Index/Candidate；
- Restore tagged entry或第二套WAL/Replay。

## 影响

正面影响：

- 未绑定表不进入Lifecycle路径；
- 可覆盖已有普通TB级表；
- 活动Object仍由成熟TAE闭环维护；
- Lifecycle失败只浪费本attempt工作，源数据继续可见。

代价：

- 时间列乱序会产生Mixed Rewrite和写放大；
- 普通Merge抢先可能导致Lifecycle重做；
- SyncProtection丢失会中止attempt；
- Commit unknown可能暂停单表并需要运维收敛；
- DDL fence必须在GA前通过最后Gate定案。

## 重新评估条件

只有出现以下条件才重新讨论执行边界：

- TAE提供成熟、持久且低成本的生命周期原生对象标记；
- Partition成为可证明的物理所有权和GC边界；
- 普通Merge提供可插拔但不影响普通负载的Lifecycle producer接口；
- 客户数据高度乱序使单源Rewrite在认证规模下不可接受。
