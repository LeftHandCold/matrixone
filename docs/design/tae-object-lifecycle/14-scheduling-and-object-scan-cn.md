# Lifecycle Phase 1 调度与 Object 扫描原理

> 对应实现：PR [#26655](https://github.com/matrixorigin/matrixone/pull/26655)
>
> 本文只说明 Lifecycle 如何被周期唤醒、如何轮转 Binding、如何分页扫描 Object、如何发现
> 持续新增的 Object，以及 cursor、full scan 和最终数据正确性之间的关系。

## 1. 核心结论

Lifecycle 不是“每张表每天创建一个独立 Job”，也不是“每分钟只处理两张表”。Phase 1 使用
一个全集群 Coordinator：

```text
每分钟第15秒尝试唤醒一次Coordinator
  → 分页装载一批ACTIVE Binding
  → 同时最多运行2个Binding child
  → 一个child完成后，排队的下一个Binding立即补位
  → 每张表本次最多读取一页、64个Object
  → 持久化该表自己的Object cursor
  → 本轮未完成的表以后从cursor继续
```

Cron 只是“闹钟”，Coordinator run 才是真正持续执行的任务。一次 run 可以跨越多分钟，期间
不需要等下一次 cron 才让后面的表开始工作。

## 2. 五个容易混淆的概念

| 概念 | 含义 |
|---|---|
| Cron tick | TaskService 每分钟第15秒尝试唤醒一次 Coordinator |
| Coordinator run | 一次实际调度运行，可以持续多分钟，最长约35分钟 |
| Binding child | 一张绑定表本轮的发现和处理工作 |
| Object page | 一张表从 cursor 后读取的最多64个可见、非appendable Object |
| Full scan | 一张表从 Object B-tree 开头一直分页推进到表尾的完整周期 |

“每分钟一次”不等于“每张表每分钟完整扫描一次”；“并发2”也不等于“每分钟只允许处理两张
表”。

## 3. Coordinator 如何启动

Phase 1 注册一个 TaskService cron：

```text
task id: tae_object_lifecycle
cron:    15 * * * * *
```

触发时间示例：

```text
12:00:15
12:01:15
12:02:15
```

不是每15秒执行一次。

Coordinator task 和本地 run slot 都限制并发为1：

- 上一轮未结束时，不启动第二个并发 Coordinator；
- 重复 tick 不会叠加另一批 Binding child；
- 一轮结束后，后续 tick 才能启动下一轮；
- 单轮最大执行时间约35分钟，超时后尚未完成或仍在排队的 child 被取消，后续轮转重试。

即使 release gate 关闭，Coordinator 仍会有界处理既有 Cleanup Root、过期 Restore 隐藏表和
终态元数据；关闭状态不会扫描 Binding 或创建新的 retirement。

## 4. 一轮如何选择 Binding

当前 Release Profile 为：

```text
Binding page size          = 64
max Binding pages/run      = 4
max Bindings/run hard cap  = 1000
```

由于一轮最多读取4页、每页最多64条，因此正常一次 run 实际最多装载约256个 Binding。超过
这个数量时，Coordinator 保存一个内存 Binding cursor，下一轮从后面的 tenant/Binding 继续。

Binding 顺序按账户做简单公平轮转，避免同一账户的多张表长期占满队首。执行并发限制为：

```text
cluster  = 2
account  = 4
database = 2
table    = 1
```

因为 cluster 上限最小，所以全集群实际同时最多有2个 Binding child；同一张表始终最多一个。

Binding cursor 是 Coordinator 的内存调度提示。CN 重启后它可以丢失并从头分页，最多产生重复
调度，不影响退休正确性。每张表自己的 Object cursor 则持久化在 Binding Catalog 中。

## 5. “并发2”如何工作

假设本轮装载了50张表，每张表处理5秒：

```text
12:00:15  Coordinator启动
12:00:15  表1、表2开始
12:00:20  表1、表2完成，表3、表4立即开始
12:00:25  表3、表4完成，表5、表6立即开始
...
```

表3、表4不需要等到12:01:15。所有已装载的 Binding child 都属于同一次 Coordinator run，
只是在两个执行槽后排队。

如果表1、表2各运行两分钟：

```text
12:00:15  表1、表2开始，表3～表50排队
12:01:15  上一轮仍在运行，不启动新Coordinator
12:02:15  上一轮仍在运行，不启动新Coordinator
12:02:30  表1、表2完成，表3、表4立即开始
```

因此 cron 频率不是处理吞吐。近似吞吐由下面的关系决定：

```text
一批Binding完成时间 ≈ 所有Binding一页处理时间之和 ÷ 2个并发
```

## 6. 每张表如何分页扫描 Object

### 6.1 复用现有 Object B-tree

Lifecycle 不建设持久 Object Index。它直接复用 PartitionState 已有的
`dataObjectsNameIndex`，该 B-tree 按 `ObjectNameShort` 字节排序。

真实 key 不是全局递增序号：

```text
ObjectNameShort = Segment UUID（16字节）+ Object Num（2字节）
```

当前新 Segment 使用 UUIDv7，正常情况下具有近似时间顺序；但多 CN 时钟、同一时间窗口、
Rewrite以及一个Segment内的Object Num都不构成可依赖的全局连续创建序号。Lifecycle分页
合同只依赖B-tree key顺序，正确性不能假设“后来创建的Object一定在cursor后”。

因此文档中的“Object 1～64、65～128”只表示 B-tree 分页位置，不表示 Object 按创建时间
连续编号。

### 6.2 单页合同

一张表每次被调度，只从 B-tree 读取一页：

```text
从cursor之后Seek
→ 最多64个Object
→ Metadata最多8 MiB
→ Discovery最多30秒
```

只返回当前页面快照中可见、非appendable的 Data Object：

- appendable Object 仍由普通 MO 写入和 flush，不进入 Lifecycle；
- flush 成不可变 Object 后，后续扫描才可能看到；
- 已经在旧事务中退休、当前快照不可见的 Object 不进入候选。

64 是“单张表、单次分页”的最大值，不跨表凑数：

```text
表A cursor后只有2个Object  → 只读2个并结束
表B cursor后有100个Object → 读前64个并保存cursor
```

不会使用表A的剩余62个额度去读取表B。

## 7. Object cursor 保存什么

每个 Binding 独立持久化：

```text
scan_snapshot_ts
scan_last_object_name
scan_wrapped
last_full_scan_at
```

其中最关键的是 `scan_last_object_name`，它表示 B-tree 书签：

```text
第一次：First()，读取最多64个，保存最后一个ObjectNameShort
下一次：Seek(last ObjectNameShort)，跳过该key，从后面继续
```

Cursor 的作用是限制每轮 Metadata、内存和执行时间，避免大表每次都从头开始。它不是数据
正确性的权威，也不表示“前64个 Object 已经成功退休”。

## 8. Scan、Candidate 和真正处理之间的关系

一页 Metadata 被分类后，只在当前 CN 内存产生有界 Candidate/Object plan：

```text
Object page
→ ZoneMap/必要Metadata分类
→ 内存planInputs
→ Whole有界合并或Mixed单源plan
→ 当前Binding child立即处理
```

没有持久 Candidate 表、Object任务队列或第二套 Object Catalog。

当前实现先持久化页面 cursor，再处理该页的 Object plan。因此：

- child 成功：下次自然从下一页继续；
- 处理 deferred、失败或在副作用前崩溃：cursor 不回退，该 Object 等下一次 full scan重新发现；
- 已产生 Payload/staging/booking 后崩溃：Cleanup Root负责对账和清理；
- 重复发现同一个 Object 不会重复退休，final transaction仍执行 exact source CAS。

这是一项明确取舍：Discovery 允许重复或延后，正确性只由 final transaction 决定，不为
Candidate 引入持久状态机。

## 9. 什么是一次 full scan

Full scan 是一张表从 B-tree 开头推进到当次看到的表尾：

```text
首次调度：从B-tree开头读第1页
后续调度：从Object cursor继续
...
读到表尾：scan_wrapped=true，记录full scan完成
```

它可以跨越很多次 Coordinator run。24小时不是未完成扫描的超时时间：

```text
第1天扫到30%
第2天继续从30%扫到60%
第3天继续到表尾
```

未完成的 full scan 不因为超过24小时而从头重启，否则大表尾部可能永久饥饿。

Full scan 也不是一个跨多天的长事务快照。每个 Binding child/Object Attempt 使用当时的普通
事务快照，处理具体 source Object 时再固定 `source_snapshot_ts`、SyncProtection和exact
source identity。这样不长期 pin 一个表级快照。

## 10. 不满足过期条件的 Object 怎么办

假设某页包含64个 Object：

```text
40个尚未过期 → 跳过
10个已经过期 → Archive/TTL
14个尚未过期 → 跳过
```

Cursor 仍推进到页面最后一个 Object。下一次轮到该表时，从下一页继续，不立即回头重查未
过期 Object。

未过期 Object 会在下一次 full scan 从头扫描时重新判断。`EXPIRE AFTER 90 DAY` 是每次
Object Attempt 的动态 cutoff：

```text
cutoff = evaluation time in frozen timezone
       - expire_after_days
       - late_arrival_grace_days
```

它是最短年龄阈值，不是“第90天准点执行”的定时器。

## 11. 扫描期间新增 Object 怎么办

新写入数据首先位于 appendable Object，普通 MO flush 后才成为 Lifecycle 候选。因为 B-tree
按 UUID key 而不是创建时间排序，新 Object 在 key 空间中可能出现在 cursor 前，也可能在
cursor 后。

### 11.1 新 Object 排在 cursor 后

如果它在下一页快照中已经可见，本轮 full scan 的后续分页可能直接看到它：

```text
cursor = K64
新Object key > K64
→ 后续Seek(K64)可能读取到新Object
```

### 11.2 新 Object 排在 cursor 前

当前 full scan 不回头：

```text
cursor = K64
新Object key < K64
→ 本轮不会看到
→ 下一轮full scan从B-tree开头时看到
```

### 11.3 到达表尾后才产生

它同样等待下一轮 full scan。由此可见，cursor 是变化集合上的有界 keyset pagination，
不是新增 Object change feed；周期性从头扫描是发现 UUID key 空间中迟到插入的必要条件。

## 12. Full scan 周期的冻结语义

为了兼顾 Archive lag 和普通 MO 稳定性，Phase 1 应冻结为：

```text
新建/重新配置Binding
→ 下一次调度立即开始full scan

full scan尚未完成
→ 每次轮到该表继续推进一页
→ 不因24小时到期而重置cursor

full scan已经完成
→ 在24小时内跳过Object discovery
→ 满24小时后从B-tree开头开始下一轮
```

因此常规新 Object 的最坏发现延迟约为：

```text
剩余full scan时间
+ 最多24小时完整扫描间隔
+ Binding排队/Provider/资源deferred时间
```

这不是精确 SLA。`full_scan_age` 超过认证阈值必须告警。

## 13. 当前 PR 的实现差异

当前代码已经定义：

```text
lifecycleFullScanInterval = 24 hours
```

但正常扫描到表尾后会持久化 `scan_wrapped=true`；下一次调用 Discovery 时，现有逻辑把
`Wrapped` 直接解释为“立即从头开始”，优先于24小时判断。因此当前实际效果是：

```text
full scan完成
→ 下一次该Binding被调度
→ 很快从头开始下一轮
```

这不会产生错误退休，但会使未过期 Object 被频繁重复扫描，在500～1000张绑定表下增加
不必要的 PartitionState/Metadata 压力。

本文件冻结的目标是“完成后等待24小时”。PR定稿前需要使实现与此合同一致，或明确选择
持续扫描并删除24小时语义；不能同时保留两个相互矛盾的口径。结合“最小影响普通 MO”的
目标，首选完成后等待24小时。

## 14. 50张表与500张表示例

### 14.1 50张表

50条 Binding 可以在一次 run 中全部装载：

```text
表1、表2开始
→ 谁先结束，表3先补位
→ 另一个结束，表4补位
→ 直到表50或35分钟预算结束
```

每张表本次最多推进一页、64个 Object。后面的表不等待下一分钟 tick，只等待两个执行槽。

### 14.2 500张表

正常需要至少两轮 Binding 装载：

```text
第1轮装载约256张
第2轮从内存Binding cursor继续装载剩余约244张
随后Binding cursor wrap，从表集合开头重新轮转
```

每个被调度的 Binding 仍只推进自己的一页。如果一张表有 `N` 个当前可见 Object，忽略删除
和并发变化，完成一轮至少需要：

```text
ceil(N / 64) 次Binding调度机会
```

实际耗时还取决于 Whole Archive、Mixed Rewrite、Provider readback 和并发冲突，不能用 cron
一分钟简单换算。

## 15. 数据增长速度超过扫描速度

系统要稳定收敛，必须满足：

```text
Lifecycle扫描和处理可退休Object的长期速度
>
新增并flush成可退休Object的长期速度
```

如果不满足：

- cursor仍持续向后推进，但 full scan age/Archive lag增长；
- UUID key插入到cursor前的Object等待下一轮，可能长期得不到处理；
- 源数据继续留在活动表，不会因为落后而被错误退休；
- 资源上限只让Lifecycle deferred，不抢占普通MO保底资源。

这是Commercial GA容量门禁，不属于需要私有事务协议解决的问题。1/10 TiB、50→1000表和
持续写入认证必须证明full scan能在目标SLO内稳定完成；不能完成时应降低认证绑定数量/
并发写入规模、改善按时间聚簇的数据布局，或在公共资源能力允许后提高Lifecycle并发。

## 16. Cursor 与数据正确性的边界

Cursor 只保证扫描效率，不保证 exactly-once：

```text
Binding/Object cursor
→ 可重复、可丢失提示、允许延后发现

Candidate/Object plan
→ 仅内存、有界、崩溃可重扫

final transaction
→ Binding generation/schema/physical table fence
→ exact source ObjectStats/source-set CAS
→ Dataset/TTL Receipt与retirement原子提交
```

因此：

- Cursor 重复：最多重复发现，不会双重退休；
- Cursor 暂时跳过：下一轮 full scan重新发现；
- 普通 Merge 或 DML 先改变 source：final exact CAS失败，源数据保持普通可见性；
- Root 出现后崩溃：Root/Reconciler/Sweeper负责副作用，不依赖 cursor推断提交结果。

正确性不依赖 Object UUID 是否按时间递增，也不依赖每轮使用同一个长事务快照。

## 17. 故障、开关和重启

### Coordinator/CN 重启

- 内存 Binding cursor和排队 child丢失；
- 下次 Coordinator可以从Binding集合开头重新分页；
- 每张表持久 Object cursor仍在Catalog中；
- 还没有副作用的内存Candidate直接丢弃；
- 已经创建副作用的Attempt由Cleanup Root接管。

### Release gate关闭

- 不再开始新的Binding/Object retirement；
- 已在执行中的单个 Object允许按现有事务/错误路径结束；
- Cleanup、COMMIT_UNKNOWN对账和过期Restore清理继续；
- 普通Query、DML、Merge不访问该gate。

### Deferred或冲突

资源、布局、Provider或source CAS导致的 deferred/失败不会回退cursor，也不会将源数据标记为
成功。对应 Object在下一轮 full scan重新评估；有Root时按Root状态机清理。

## 18. 可观测性与验收

调度至少需要观察：

```text
active Binding child / mode
job success / deferred / error
last_full_scan_at
observed_full_scan_age_seconds
Binding/Object cursor进度
retired source bytes
rewrite amplification/budget
Provider latency/error
cleanup backlog/unknown Root
```

验收必须覆盖：

1. 0、1、63、64、65和多页 Object；
2. 50、256、257、500、1000张 Binding；
3. 2并发下同一次 run 中后续 Binding即时补位；
4. run 跨越多个 cron tick，不出现重叠 Coordinator；
5. full scan超过24小时仍从原cursor继续；
6. full scan完成后24小时内不重复扫描；
7. 新 Object key分别插入cursor前、cursor后和表尾完成后；
8. appendable→flush的可见性；
9. CN重启导致内存Binding cursor丢失；
10. cursor已推进后Object处理deferred/失败；
11. 持续写入速度低于、等于和高于处理速度；
12. active-coexistence期间普通MO吞吐、P99、内存、GC、Merge和checkpoint无不可接受回归。

## 19. 最终决策

Phase 1 继续采用：

```text
单个全集群cron
+ 有界Binding分页和内存公平轮转
+ 每表持久ObjectName cursor
+ 复用PartitionState Object B-tree
+ Candidate仅内存
+ 每表一页最多64 Object
+ 完成后24小时full scan周期
+ final exact CAS作为唯一正确性门
```

不增加每表 cron、Object创建时间索引、持久 Candidate队列或分布式调度状态机。该方案的
代价是新 Object发现为最终一致，归档延迟受full scan周期和backlog影响；收益是普通 MO
热路径零 Lifecycle Catalog访问，并将资源和协议复杂度限制在Lifecycle控制面。
