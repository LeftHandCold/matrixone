# 00 原始需求与Phase 1追踪矩阵

## 1. 目的

本表防止“简化协议”误删产品能力，也防止Phase 1被误称为完整关闭Issue #24853。

## 2. Issue #24552

| 原始需求 | Phase 1 | 设计入口 | 验收 |
|---|---|---|---|
| 原生时间TTL | 支持 | 01、03 | 07 |
| 自动后台执行 | 支持 | 02、08 | 07/06 |
| 避免昂贵全表DELETE | Whole retire + Mixed Rewrite | 03 | 07 |
| 日志/trace/metrics高吞吐 | 支持但需规模认证 | 06、08 | 1/10 TiB |

完成Phase 1可以满足Issue #24552的核心产品目标。

## 3. Issue #24853

| 原始愿景 | Phase 1决定 | 原因/后续 |
|---|---|---|
| HOT活动数据 | 使用现有TAE | 支持 |
| COLD透明在线查询 | 不实现 | 收益有限且增加查询/Merge复杂度 |
| ARCHIVE | direct-readable Parquet/ZSTD | 支持 |
| Deep Archive/thaw | 不实现 | 后续Provider能力 |
| SQL级存储位置 | 复用Stage | 支持表级 |
| 自动后台转换 | 支持 | Metadata Scheduler |
| Archive前完整校验 | 支持 | full readback |
| Restore | 恢复到独立新表 | 支持 |
| 到期Purge | 异步Purge | 支持 |
| Account/Database继承 | 不实现 | 后续Phase |
| Table Policy | 支持 | Phase 1 |
| 逻辑分区表 | 不实现 | Phase 1在Bind时拒绝；后续按物理child展开 |
| Time Travel/Fail-safe | 不替代现有Snapshot/PITR | 创建和保留可共存，复用MVCC/GC |
| Archive继续在线UPDATE | 不支持 | 应继续留在活动TAE |
| DROP后合规保留 | 不支持 | 无Legal Hold/WORM |

因此Phase 1是#24853的可商用子集，不是原Issue全部能力。

### 3.1 周边功能支持边界

| 功能 | Phase 1决定 | 边界 |
|---|---|---|
| Snapshot/PITR创建与历史保护 | 支持 | 旧Object由现有MVCC/GC引用保护；Lifecycle不增加专用GC协议 |
| Snapshot/PITR Restore Lifecycle Archive scope | 不支持，破坏性Restore提交前fail closed | Database/Table不恢复ARCHIVE Binding、Dataset、ARCHIVE Root或外部Payload；直接Account Restore存在任意Binding时拒绝；Cluster逻辑Restore不扩展TTL兼容；执行前关闭并drain Lifecycle数据任务 |
| Clone/Data Branch活动数据 | 支持 | 复制目标时间点活动数据；目标不继承Binding、Dataset或Payload |
| 普通同集群Publication/Subscription | 支持 | 订阅端直接读取发布者活动表，不需要退休事件 |
| CDC/CCPR | 不接入、不提供兼容性SLA | Lifecycle不修改其创建/运行路径，也不生成逐行退休事件 |
| 普通物理Backup创建 | 不修改、不阻断 | 外部Archive Payload不进入Backup |
| 含Lifecycle状态的物理Backup Restore | Phase 1不支持 | 可能恢复Catalog/Stage/Root却仍指向原namespace；启动任何Lifecycle任务前必须隔离任务和原namespace删除凭据 |

上述共存能力不使用跨租户Lifecycle feature-row barrier。Backup Restore兼容性属于后续独立
设计；仅把Lifecycle release gate设为disabled不能替代恢复隔离，因为历史Root cleanup仍需
在正常集群中运行。

## 4. 已确认的非功能需求

| 目标 | 设计措施 | 证据 |
|---|---|---|
| 500～1000绑定表 | 只扫描Binding、分页和公平调度 | 02/06/07 |
| 1 TiB常见、10 TiB认证 | streaming、硬上限和scale test | 02/07 |
| 普通MO稳定 | 普通查询/DML/Merge不访问Lifecycle；普通Merge算法不变 | 03/06 |
| Archive可恢复 | Manifest版本/schema descriptor、Chunk恢复上限、可重启Hash、原子隐藏表Owner、AUTO_INCREMENT水位和身份清理 | 02/05 |
| Merge并发安全 | exact CAS、单源Rewrite、现有transfer | 03/07 |
| 外部对象不泄漏/误删 | immutable key、Root/Sweeper | 04/07 |
| 重启与升级安全 | SyncProtection fail closed、旧TN拒绝 | 02/03/07 |
| Purge物理回收 | 仅认证的非Versioned专用Stage | 02/04/07 |
| 普通MO资源隔离 | Rewrite写放大/窗口bytes、Restore staging总量、cleanup backlog硬上限 | 02/06/07 |

## 5. 需求变更规则

增加ONLINE_COLD、Deep Archive、继承、Legal Hold或Lifecycle-aware Backup/DR与物理恢复
兼容时，必须建立独立Phase设计，不能把字段悄悄加入Phase 1状态机。
