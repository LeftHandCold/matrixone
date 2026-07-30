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
| Time Travel/Fail-safe | 不替代现有Snapshot/PITR | 独立产品平面 |
| Archive继续在线UPDATE | 不支持 | 应继续留在活动TAE |
| DROP后合规保留 | 不支持 | 无Legal Hold/WORM |

因此Phase 1是#24853的可商用子集，不是原Issue全部能力。

## 4. 已确认的非功能需求

| 目标 | 设计措施 | 证据 |
|---|---|---|
| 500～1000绑定表 | 只扫描Binding、分页和公平调度 | 02/06/07 |
| 1 TiB常见、10 TiB认证 | streaming、硬上限和scale test | 02/07 |
| 普通MO稳定 | 普通查询/DML/Merge不访问Lifecycle；普通Merge算法不变 | 03/06 |
| Archive可恢复 | schema descriptor、可重启Chunk Hash、full readback | 02/05 |
| Merge并发安全 | exact CAS、单源Rewrite、现有transfer | 03/07 |
| 外部对象不泄漏/误删 | immutable key、Root/Sweeper | 04/07 |
| 重启与升级安全 | SyncProtection fail closed、旧TN拒绝 | 02/03/07 |
| Purge物理回收 | 仅认证的非Versioned专用Stage | 02/04/07 |

## 5. 需求变更规则

增加ONLINE_COLD、Deep Archive、继承、Legal Hold或Backup/DR时，必须建立独立Phase设计，
不能把字段悄悄加入Phase 1状态机。
