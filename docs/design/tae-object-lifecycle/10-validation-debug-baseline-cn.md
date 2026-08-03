# TAE Object Lifecycle 验证与 Debug 基线

> 基线更新：2026-08-03
> 状态：**Phase 1 功能开发基本完成，进入验证与 Debug；Commercial GA 仍为 Conditional Go**。
>
> 本文是完成核心开发后的唯一执行跟踪文件。后续测试、故障注入、问题分类、容量调优和
> 发布判定以本文为入口；它不替代01～09的协议细节，也不得借验证之名恢复已经删除的重型设计。

## 1. 固定目标

1. Lifecycle必须能长期管理500～1000张显式绑定表和TB级数据，不是一次性Demo；
2. 普通查询、DML、Merge、WAL、Replay、checkpoint和GC不进入Lifecycle私有状态机；
3. 优先复用MO公共事务、Merge、WAL、Replay和GC；公共MO缺陷走公共修复或认证边界；
4. Lifecycle只解决自己新增的Archive、Dataset、Restore、Purge、Cleanup和资源隔离问题；
5. 验证阶段以发现Bug、修复已证明Bug和调小不安全上限为主，不继续扩大功能范围。

## 2. 已冻结的Phase 1主链

```text
显式ALTER TABLE Binding
  -> Scheduler只分页扫描Binding和现有TAE Metadata
  -> Whole exact Reader / Mixed单源Rewrite
  -> Parquet/ZSTD + Provider full readback + canonical hash
  -> 普通MO事务原子写Dataset或TTL Receipt并退休source Object
  -> live Object交给现有TAE WAL/Replay/GC
  -> 外部Payload、booking和临时staging由Cleanup Root异步收敛
  -> RESTORE ARCHIVE恢复到独立新表
```

固定实现边界：

- 不建设Object Index；使用PartitionState/Object Metadata有界分页和O(1) cursor；
- Whole允许有界多source，Mixed严格单source；
- Mixed复用`BlockDataReadNoCopy + DoMergeAndWrite`，后者是Created Object和TransferTable的
  唯一producer；
- Object退休使用`SoftDeleteObject`，物理删除交给普通TAE GC；
- 不增加第二套Merge、WAL、Replay、GC或通用事务执行器；
- Archive必须完成PUT、full readback和内容验证后才能进入final transaction；
- Restore使用固定Row Group Chunk、Chunk Receipt、隐藏表和普通DDL原子发布；
- `COMMIT_UNKNOWN`保持fail-closed，不猜测、不误删可能已经发布的Payload。

## 3. 明确不进入Phase 1的能力

- `ONLINE_COLD`、Restore-required Deep Archive、Legal Hold、WORM；
- 逻辑分区表、二级/唯一索引、FK、Fulltext、Vector和插件表；
- CDC/CCPR退休事件完整性；
- Lifecycle-aware Backup/DR和物理Backup Restore兼容；
- Archive-aware Snapshot/PITR Restore；
- Clone/Branch继承Binding、Dataset或Archive Payload；
- account/database Policy继承；
- 为Lifecycle私建Terminal Journal、Feature Guard、Binding claim、TN permit、Booking V1、
  SourceLayoutProof或持久Candidate。

这些边界不是“以后永远不做”，而是不能成为首个Phase 1 GA的隐含承诺。

## 4. 进入验证前的基线清理

| 项目 | 结论 | 处理 |
|---|---|---|
| Data Branch Clone锁序 | 旧Lifecycle Barrier删除后遗留了普通MO锁序调整 | 恢复main原锁序，不改变Lifecycle功能 |
| PITR两次Lifecycle Restore probe | 不是重复：一次检查历史源scope，一次检查当前目标scope | 保留两次检查，不做错误删减 |
| `CREATE TABLE ... LIFECYCLE` | Parser未交付该语法 | Phase 1只承诺建表后`ALTER TABLE ... SET LIFECYCLE` |
| Dry-run | 没有独立生产SQL入口 | 定义为测试/认证流程，不宣称生产能力 |
| Export-only | 只有隔离测试/认证用途 | 不作为Phase 1生产模式 |
| TTL small Mixed `Relation.Delete` | Gate F可选路径尚未交付 | 本版默认关闭，TTL Mixed统一Rewrite或Blocked |
| 权限 | 当前实现限制为管理员控制面 | Phase 1文档按admin-only冻结，不在清理阶段放宽代码 |
| 索引准入 | 代码拒绝带二级/唯一索引的源基表 | 文档与代码统一，不只写“隐藏索引表” |
| 滚动升级 | 没有自动HAKeeper capability判断 | 使用unknown-entry fail-closed、release gate和运维升级前置条件 |

## 5. Gate状态

| Gate | 当前状态 | 后续动作 |
|---|---|---|
| A Catalog/Binding/Discovery | 代码完成；fresh bootstrap缺陷已修复 | 在50重新fresh bootstrap后回归ALTER语法、Catalog upgrade、分页和1000 Binding上限 |
| B Reader/Archive格式 | 代码完成 | 类型矩阵、最大Block、Manifest golden和full readback故障测试 |
| C Cleanup Root/Stage | 代码完成 | Provider超时、迟到PUT、multipart和quiescence验证 |
| D Whole exact retire | 代码完成 | Merge抢先、post-S DELETE、commit unknown和TN restart |
| E Mixed单源Rewrite | 代码完成 | D/E/L、Transfer、booking、WAL/Replay和资源峰值验证 |
| F TTL small Mixed DELETE | 未交付、默认关闭 | 不阻塞核心GA；除非以后单独决定实现和认证 |
| G Restore/Purge | 代码完成 | Chunk重试、隐藏表发布/清理竞争、lease/Purge和大规模Restore |
| H DDL/升级边界 | 代码面完成 | 使用现有集成能力做真实锁、路由、滚动升级验证 |
| I 认证与发布 | 未完成 | 1/10 TiB、30天soak、50→1000表和真实Provider认证 |

禁止把“Gate A～H代码存在”表述成“Gate I认证已经完成”。

## 6. Q1/Q2/Q3基线

| 审查项 | 当前结论 | 验证重点 |
|---|---|---|
| Q1：每个资源只有一个有效Owner | 通过 | Root→TAE/Dataset转交、Restore隐藏表发布、失败清理exactly-once |
| Q2：每条等待链有终点 | 设计通过、运行条件验证 | Provider deadline、Cleanup预算、Reader长事务、unknown hard cap |
| Q3：每个积累项有上限 | 条件通过 | CN/TN峰值内存、Root/Chunk/staging上限和1000表长期消费率 |

任何新问题都必须先给出具体可达链路，再归类为：

1. Lifecycle自身Bug：本PR修复并补故障测试；
2. MO公共Bug：复现后提公共Issue/复用公共修复，不建设Lifecycle私有补偿；
3. 认证边界：降低并发、对象大小或支持矩阵后重新认证；
4. 后续能力：写入后续Issue，不阻塞Phase 1主链。

## 7. MO公共问题与认证边界

### 7.1 公共Merge Tombstone scanner峰值

公共scanner可能先物化完整Tombstone Batch，再检查Lifecycle的row/byte上限。宽PK场景可能
在返回`RESOURCE_BLOCKED`前产生较高TN瞬时内存。它属于普通Merge共享路径，不为Lifecycle
建设私有Tombstone Reader或TN permit。

处理：认证最大合法Object、PK宽度和并发；若触发不安全峰值，则Stop Ship、降低认证上限，
或等待/推动公共MO bounded scanner修复。

### 7.2 Reader长事务对MinTS/GC的影响

Archive child在读取、PUT、readback和finalization期间持有reader事务。它没有已证明的数据
错误，但可能延迟MinTS并增加GC、checkpoint和logtail retained bytes。

处理：首发集群child并发保持1～2；在active-coexistence和30天soak中测量。未达标时优先
减少单轮Object数量、缩短deadline或降低并发，不增加Lifecycle Snapshot/GC协议。

### 7.3 滚动升级和旧TN

新协议需要TN在Batch解析前识别或拒绝Lifecycle entry。代码有fail-closed解析和release gate，
但新CN→旧TN、降级、路由变化和response lost仍需要真实集群证据。

处理：先部署安全解析版本，确认全部CN/TN达到兼容下限后再开启retirement；不建设HAKeeper
Lifecycle capability传播。

### 7.4 Snapshot/PITR

Snapshot/PITR创建继续使用MO现有MVCC/GC保护旧TAE Object；Lifecycle只对当前可见Object执行
soft delete，物理GC仍由现有历史引用决定。Phase 1不恢复Archive Payload或Lifecycle状态。

处理：创建和历史保护允许共存；涉及Lifecycle Archive scope的逻辑Restore按支持矩阵
fail-closed。Cluster级逻辑Restore的公共完整性问题不在本功能内重做。

### 7.5 Backup/DR

普通物理Backup可能包含Lifecycle Catalog引用，却不复制外部Archive Payload；恢复环境如果
继续持有原Bucket删除凭据，还可能操作原集群Payload。

处理：Phase 1不修改普通Backup，但含Lifecycle状态的物理Backup Restore明确unsupported；
恢复环境必须隔离Lifecycle任务和原Archive凭据。完整兼容走独立Issue/PR。

### 7.6 CDC/CCPR

Object退休不是逐行DELETE事件，CDC/CCPR下游可能无法得到完整删除语义，而当前CDC本身也不
属于本功能的商用依赖。

处理：Lifecycle不查询、不阻断、不修改CDC Catalog和运行路径；Phase 1明确不保证两者兼容。

## 8. 验证与Debug执行顺序

### 阶段0：冻结与低并发回归

- 完成本文件第4节的清理；
- `git diff --check`、Markdown围栏/链接/占位符检查；
- 受影响包定向UT和race测试；
- Lifecycle、frontend、disttae、TAE rpc/txnentries、Catalog upgrade低并发完整回归；
- 固定代码基线，后续只接受测试证明的Bug修复和必要阈值调整。

### 阶段1：故障注入

- PUT、HEAD、readback、Manifest写入和迟到PUT；
- source Reader、SyncProtection续租、普通Merge抢先；
- Whole/Mixed final commit、response lost、TN restart和WAL replay；
- Root reconcile、cleanup LIST/DELETE、quiescence和Provider持续超时；
- Restore初始化、Chunk commit、发布、清理、Purge/lease竞争；
- 每个故障点检查源数据、Dataset、Root、staging和隐藏表Owner。

### 阶段2：规模与资源

- 最大Object、宽PK、varlen、BLOB、高压缩率Row Group；
- 1 TiB常见单表；
- 10 TiB认证单表；
- Provider 429/限流/慢请求；
- 内存、MinTS、checkpoint、GC、logtail、Merge backlog和外部对象数量。

### 阶段3：Active coexistence

- 50 → 200 → 500 → 1000 Binding逐级放量；
- 同时运行普通查询、DML、Merge、checkpoint、GC和logtail；
- 对照feature-off/无Binding基线，记录吞吐、P99、CPU、heap和retained bytes；
- 任一数据不变量失败或普通MO指标越界，立即停止扩大。

### 阶段4：长期运行与发布

- 30天真实elapsed soak；
- Cleanup和终态元数据消费率覆盖产生率，backlog无持续正斜率；
- 演练kill switch、告警、人工处理`COMMIT_UNKNOWN`和容量降级；
- 汇总Gate I证据后再决定Commercial GA。

测试必须低并发执行，禁止同时启动大量UT或race任务压垮开发机。真实集群联调按单独阶段
执行，不为了本功能在仓库内新建MO原本不存在的SQL集群测试框架。

## 9. Stop-Ship条件

出现任一项即停止放量并回到Debug：

- Archive未完成验证却退休源Object；
- Dataset可见但source未退休，或source退休但Dataset不可见；
- 新旧live Object双重可见或都不可见；
- Restore行数、schema、hash、Chunk或AUTO_INCREMENT水位错误；
- Cleanup删除`PUBLISHED`或`COMMIT_UNKNOWN` Payload；
- 普通MO panic、数据错误、不可恢复OOM或明确不可接受的P99/GC/Merge回归；
- Root、unknown、staging、Cleanup backlog达到硬上限且不能收敛；
- 认证范围内的数据永久进入不可Restore或不可Purge状态。

## 10. 决策日志

- 不恢复全局Lifecycle feature-row Barrier；
- 不修改普通Merge候选、排序、writer、WAL、Replay和GC语义；
- 不建设Object Index；
- 不为公共Tombstone scanner建设Lifecycle私有替代；
- 不接入CDC/CCPR；
- 不在本PR实现Lifecycle-aware Backup/DR；
- 不把Cluster逻辑Restore公共问题扩展成Lifecycle状态机；
- Gate F保持可选且默认关闭；
- Phase 1继续实现Issue #24853的online/HOT到Archive再到独立新表Restore；仅延后中间的
  COLD/ONLINE_COLD，不把产品收窄成TTL-only；
- Gate I证据未完成前，结论始终是Conditional Go。

## 11. Fresh bootstrap E2E缺陷记录

### 11.1 2026-08-03：首次Lifecycle cron导致CN退出

输入基线为`a8b4df3ea97102adc3624641704cdb8d1ae8a4f6`。50上的fresh LogService + TN + CN +
MinIO隔离集群确认了两个Lifecycle自身缺陷：

1. SYS fresh bootstrap只创建了五张tenant Lifecycle表，没有创建system-owned
   `mo_lifecycle_cleanup_roots`，也没有插入默认关闭的`LIFECYCLE` feature row；
2. TaskService传给Coordinator的context没有deadline。Cleanup Root查询报表不存在后，
   Coordinator仍继续执行Restore cleanup，最终在内部SQL事务校验处触发FATAL。

修复边界保持在Lifecycle内部：

- SYS fresh bootstrap显式创建`LifecycleClusterTableDefinitions`并插入默认`enabled=false`的
  Lifecycle feature row；普通tenant bootstrap不创建Cleanup Root；
- Coordinator入口为本次Task补充35分钟总deadline，覆盖30分钟Object attempt和维护余量；
- Cleanup Root的Catalog扫描未完成时立即结束本次tick，不再串联后续内部SQL；扫描完成后，
  单个Root、Restore cleanup或metadata cleanup错误仍按原逻辑聚合，避免一个Provider故障
  饿死其他维护；下一次cron继续重试；
- 不修改TaskService、普通事务、Merge、WAL、Replay或GC。

本地已增加fresh SYS bootstrap合同、无deadline Task context和首个维护错误短路测试。50上的
fresh bootstrap与主链E2E需要在新提交部署后重新执行，完成前Gate A仍保留该验证项。

### 11.2 2026-08-03：Lifecycle feature row初始化SQL被Planner拒绝

输入基线为`c4e0dca014e2559a1e76ae46b124602be0b5f0b3`。50上的第二次fresh bootstrap
确认Cleanup Root建表已经成功，但随后初始化`LIFECYCLE` feature row时失败。SQL使用：

```sql
ON DUPLICATE KEY UPDATE feature_code = feature_code
```

`feature_code`是`mo_feature_registry`的主键。MO Planner在编译ODKU时会拒绝任何主键
更新目标，即使右值仍是该列且fresh bootstrap运行时不存在重复键。因此该错误发生在SQL
规划阶段，SYS初始化事务回滚，尚未进入Lifecycle cron。

修复保持为一条fresh bootstrap和upgrade共用的幂等SQL，只把no-op更新列改为非主键：

```sql
ON DUPLICATE KEY UPDATE description = description
```

该写法沿用MO已有的非主键no-op ODKU能力；重复执行时不会修改已有`enabled`、
`scope_spec`或Stage配置。回归测试必须断言具体更新目标，不能只检查SQL包含
`ON DUPLICATE KEY`。本修复不修改系统表结构、普通事务、Merge、WAL、Replay或GC；50上的
fresh bootstrap与主链E2E仍需在新提交部署后重新执行。

### 11.3 2026-08-03：Release config误把ByteJson当作JSON文本

输入基线为`98b0b7f837b23b3a2019658ccc52a795c23fa3e6`。50上的fresh bootstrap、
Cleanup Root和默认关闭的feature row均已通过，两轮cron保持CN存活；但Coordinator读取
`mo_feature_registry.scope_spec`后报`invalid character '\x01'`。

`scope_spec`的SQL类型是JSON，内部SQL Executor返回`T_json` Vector，其中保存的是MO的
ByteJson存储编码。Lifecycle直接调用`GetStringAt`会把以类型码开头的存储字节交给标准JSON
Decoder。修复直接复用MO现有`types.DecodeJson(...).String()`读取方式，不修改查询、系统表、
通用JSON实现或Coordinator状态机。

Release config和Coordinator测试数据同步改为通过`AppendByteJson`构造真实`T_json` Vector，
不再用`T_varchar`掩盖编码差异。50上的默认关闭空跑与后续ALTER/TTL/Archive主链仍需在新
提交部署后继续验证。
