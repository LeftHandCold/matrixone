# MatrixOne 联邦查询设计方案综合评审

**评审日期**：2026-03-31
**评审版本**：v1.0
**文档来源**：`doc/fq/` 目录下全部设计文档

---

## 1. 评审目的与范围

本评审文档对 `doc/fq/` 目录下以下全部设计文档进行体系化综合评审：

| 被评审文档 | 主要内容 |
|------------|----------|
| `federated_query_design.md` | 总体架构设计、TABLE_SCAN 主链路决策、控制面/执行面分层 |
| `federated_query_implementation_plan.md` | 实施拆解、阶段划分、代码改造点清单 |
| `federated_query_code_checklist.md` | 可执行 checklist、直接可开工的代码点与验收标准 |
| `federated_query_scope_replan.md` | 从保守 MVP 到完整 V1 的范围升级方案 |
| `federated_query_sql_catalog_interface_draft.md` | SQL 语法草案、catalog 元数据设计、Go 接口草案 |
| `federated_query_postgresql_addendum.md` | PostgreSQL 对设计的特殊挑战与影响分析 |

本评审不重复文档中已有的设计论证，而是从**架构正确性、工程可行性、完整性、风险**四个维度给出独立评审意见，并提出需要澄清或补充的关键问题。

---

## 2. 总体评价

**评审结论**：✅ **推荐采纳，设计方案工程上可行**

这套联邦查询设计文档体系的核心价值在于：

1. **三层对象模型分离**（Connection / External Catalog / Foreign Table）让关注点清晰隔离，Connection 承载 secret，Catalog 承载方言和 metadata，Foreign Table 承载执行，三者各司其职
2. **catalog-first 优于单表逐个接入**，更适合 Oracle/MySQL 企业级接入场景，与 Doris 的 external catalog 路线方向一致
3. **dialect adapter 作为一等公民**，避免方言差异污染核心引擎逻辑，MySQL/Oracle/PostgreSQL 各有独立 adapter
4. **复用 TABLE_SCAN 主链路而非新建 FEDERATED_SCAN 旁路**，避免了未来两套扫描体系并存的维护负担
5. **渐进式扩展策略**（Phase 1 跑通主链路 → Phase 2/3 打通真实数据库 → Phase 4/5 增强 pushdown 和可观测性），降低了架构风险

---

## 3. 核心架构决策评审

### 3.1 走 TABLE_SCAN 主链路而非新建 FEDERATED_SCAN 旁路

**评审意见**：✅ 正确，理由充分

**支撑依据**：
- `pkg/vm/engine/disttae/txn_database.go` 中 `db.relation()` 是合适的 foreign relation 接入点，foreign table 可以在 `newTxnTable()` 之前被拦截，返回 `foreignRelation`
- foreign table 在 planner/binder 层面仍表现为普通 TABLE_SCAN，可以最大化复用 filter、projection、join、agg、explain 的现有逻辑
- 避免了 external table 和 source table 两条"特殊表扫描路径"并存的问题
- 未来 foreign table 若要升级为普通表模型，成本更低

**工程建议**：拦截点必须严格放在 `db.relation()` 中，不能在 `txnTable` 内部塞 foreign 分支。`txnTable` 的职责必须保持单一。

### 3.2 CONNECTION + EXTERNAL CATALOG + FOREIGN TABLE 三层分离

**评审意见**：✅ 正确，相比 Doris 的 JDBC catalog 路线更适合 MatrixOne

**与 Doris 的区别**：文档明确指出不机械复制 Doris 的 JDBC 技术选型（因为 MO 是 Go 栈），而是借鉴其 catalog 对象模型、用 Go 原生驱动实现。这是合理的判断。

**三层职责建议**：

| 对象 | 核心职责 | 是否含 secret |
|------|----------|---------------|
| CONNECTION | host/port/user/password/TLS/connect_timeout/query_timeout | 是 |
| EXTERNAL CATALOG | 方言类型、metadata discovery、名字映射、cache、pool 策略、include/exclude | 否（引用 connection） |
| FOREIGN TABLE | 本地 schema 固化、scan 执行单元、pushdown 策略、fetch_size | 否（引用 catalog） |

### 3.3 ForceOneCN = true 的单 CN 执行

**评审意见**：✅ 正确，首版必须强制

**支撑依据**：
- `pkg/vm/engine/readutil/relation_data.go` 中 `UnmarshalRelationData()` 当前只支持 `BlockListRelData` 和 `EmptyRelationData`，不原生支持 foreign range 的序列化
- 多 CN foreign scan 会牵涉 readutil / remoterun / 远端 reader 生命周期 / 结果归并等多个模块的联动修改
- `ForceOneCN = true` 可以让首版聚焦在"MO 主链路接入"这一个核心问题上，不被分布式执行问题分散精力

**工程建议**：`node.Stats.ForceOneCN = true` 应该在 `query_builder.go` 的 scan 节点构造时直接设置，不需要等到 compile 阶段发现 foreign table 再补救。

### 3.4 imported foreign table schema 固化 + 显式 refresh

**评审意见**：✅ 正确，首版必须采用保守策略

**理由**：
- Oracle/MySQL 的类型系统和大写/小写/引号规则差异较大
- 自动跟随远端 schema 变化容易引入静默行为变化，在生产环境中极难调试
- schema 固化让 foreign table 的行为可预测、可解释
- 后续 refresh 机制提供了显式纠错路径

**补充建议**：schema drift 后的行为必须有明确定义：
- 如果远端列被删除：查询失败，给出 refresh 提示
- 如果远端类型变宽（如 int → bigint）：应该尝试兼容还是直接报错，需要明确定义策略

---

## 4. 需要澄清和补充的关键问题

### 问题 1：IMPORT FOREIGN SCHEMA 与 CREATE FOREIGN TABLE ... FROM CATALOG ... 的语义边界

**现状**：两份文档对这两种语法有不同描述：
- `implementation_plan.md` 强调 `IMPORT FOREIGN SCHEMA`
- `sql_catalog_interface_draft.md` 强调 `CREATE FOREIGN TABLE ... FROM CATALOG ...`

**需要明确**：

| 问题 | 建议 |
|------|------|
| 两种语法是并存还是包含关系？ | 建议 `IMPORT` 是 `CREATE ... FROM CATALOG` 的批量简化形式，后者支持单表导入 + 本地 schema 覆盖 |
| `IMPORT` 是否支持增量导入？ | 首版建议不支持，只做全量一次导入 |
| `IMPORT` 的本地目标 schema 如何命名？ | 建议显式指定 `INTO target_schema`，不允许省略 |
| 导入冲突时（如本地已存在同名表） | 建议报错而非覆盖，需要用户显式处理 |

### 问题 2：PostgreSQL 的 external catalog 粒度

**现状**：`postgresql_addendum.md` 正确识别出 PG 的 catalog 粒度与其他两者不同：
- MySQL：一个 catalog ≈ 一个 MySQL 实例（多 database）
- Oracle：一个 catalog ≈ 一个 service（多 schema）
- PostgreSQL：一个 catalog ≈ 一个 database（内部有多 schema）

**需要明确**：

| 问题 | 建议 |
|------|------|
| MO 的 external catalog 是否需要为 PG 单独适配 catalog.type = 'postgresql'？ | 必须适配，PG 的 database 是稳定一等边界，与 MySQL/Oracle 不同 |
| PG 的 schema 在 MO 侧如何暴露？ | 建议 catalog 内部按 `schema.table` 两级暴露，不平铺到 catalog 根级别 |
| `IMPORT FOREIGN SCHEMA` 对 PG 的语义？ | 对 PG 来说就是 import 某个 database 下的某个 schema |

### 问题 3：DialectAdapter 的 TranslatePredicate 返回 ok = false 的语义歧义

**现状**：

```go
TranslatePredicate(expr *plan.Expr, ctx TranslateContext) (sql string, ok bool, err error)
```

`ok = false` 既可能表示"谓词不能下推（能力不足）"，也可能表示"翻译时发生了错误"。

**需要明确**：

| 场景 | ok | err | 含义 |
|------|----|-----|------|
| 谓词复杂度超出下推能力 | false | nil | 谓词不能下推，走 MO 本地 residual filter |
| 谓词语义在目标方言中等价但翻译有 bug | false | != nil | 翻译出错，需要报错 |
| 目标方言原生不支持该谓词类型 | false | nil | 谓词不能下推，走 MO 本地 residual filter |

**建议**：在文档中明确这三类的区分，或者改为返回枚举 `PushdownResult{Pushed, NotPushed, Error}`，将"能力不足"和"翻译失败"显式分开。

### 问题 4：Ranges() 返回的 RelData 与未来 multi-CN 扩展

**现状**：`implementation_plan.md` 指出"首版不要求 FederatedRelData 被 remoterun 反序列化"。

**需要补充**：
- `Ranges()` 首版返回的"单一逻辑范围"应该实现 `engine.RelData` 接口，但可以不支持序列化
- 在 `pkg/vm/engine/readutil/relation_data.go` 中应预留 `FederatedRelData` 的注册点
- 避免未来做 multi-CN scan 时需要回头修改 `engine.RelData` 接口

**工程建议**：在 `engine.RelData` 的实现列表中，预留一个 `// TODO: FederatedRelData for multi-CN foreign scan` 的注释，说明这是一个已知扩展点。

### 问题 5：REFRESH 的语义边界

**现状**：文档中多处提到 `REFRESH FOREIGN TABLE` 和 `REFRESH EXTERNAL CATALOG`，但没有明确定义：

| 问题 | 建议 |
|------|------|
| REFRESH 是全量重刷还是增量更新？ | 首版建议明确为全量重刷，不做增量 |
| REFRESH 执行期间原查询是否被 block？ | 建议 REFRESH 执行期间新查询使用新 schema，block 期间原查询可继续使用旧 schema（copy-on-read） |
| REFRESH 后远端表被删除，foreign table 如何处理？ | 建议 foreign table 标记为 invalid 状态，后续查询报错 |
| REFRESH 是否需要加锁防止并发？ | 建议 REFRESH 获取 catalog 级别的写锁，防止并发 refresh |

### 问题 6：connector 连接池策略与 Session 生命周期

**现状**：`sql_catalog_interface_draft.md` 定义了 `connection_pool_min_size` 和 `connection_pool_max_size`，但没有定义：

| 问题 | 建议 |
|------|------|
| 连接池的生命周期是 per query / per session / per catalog？ | 建议 per catalog，session 级别复用 connection |
| connection 复用时 session state 如何处理？ | 建议 connector 层的 `Session` 接口自己管理 session state 初始化和清理，如每次 `Query()` 前执行必要的 `SET` 语句 |
| 连接泄漏如何防护？ | 建议 `Session` 必须实现 `Close()`，且 `Reader` 的 `Close()` 必须传播到 `Session.Close()` |

**建议**：在 `ExternalCatalog` 接口中增加 `NewSession(ctx context.Context) (Session, error)` 方法，让 catalog 自己管理 session 的创建和生命周期，而不是让 `foreignReader` 直接持有连接。

---

## 5. 落地优先级建议

基于 `scope_replan.md` 的"完整 V1"思路和评审意见，建议的实际开工优先级如下：

| 阶段 | 核心交付 | 关键里程碑 | 风险 |
|------|----------|------------|------|
| **Phase 0** | 控制面 DDL + catalog 元数据骨架 | mo_connections / mo_external_catalogs 建表；CONNECTION / CATALOG / FOREIGN TABLE 的 parser + DDL build 可跑通 | 低 |
| **Phase 1** | foreignRelation 骨架 + mock connector | SELECT * FROM foreign_table 可走通 TABLE_SCAN 主链路，mock 数据验证通过 | 低 |
| **Phase 2** | MySQL dialect adapter 打通 | 真实 MySQL 数据库 SELECT / projection / predicate pushdown 验证通过 | 中（MySQL 与 MO 类型映射需要仔细定义） |
| **Phase 3** | PostgreSQL dialect adapter | 验证 catalog 抽象对高差异关系库的通用性，处理 PG 特有的 schema / identifier / session 语义 | 中（PG 的 catalog 粒度决策需要提前冻结） |
| **Phase 4** | Oracle dialect adapter | 高差异方言验证，处理 Oracle 特有的 NUMBER / DATE / CLOB / 大小写引号规则 | 高（Oracle 差异最大，建议最后做） |
| **Phase 5** | pushdown 增强（LIMIT / ORDER BY） | 在 DialectAdapter.Capabilities() 中声明能力，安全扩展下推范围 | 中（需要严格的等价性验证） |
| **Phase 6** | explain / metrics / REFRESH 闭环 | 慢查询可定位，schema drift 可恢复 | 低 |

**不推荐并发**：不推荐 MySQL / PG / Oracle 三个 adapter 同时开发。应该严格按 MySQL → PG → Oracle 的顺序推进，每完成一个 adapter 并验证 catalog 抽象的通用性后再开始下一个。

---

## 6. 风险评估与应对

| 风险 | 等级 | 应对措施 |
|------|------|----------|
| 方言适配过于复杂导致延期 | 高 | 严格按 MySQL → PG → Oracle 顺序推进；PostgreSQL 不作为首批简化目标而应作为抽象验证 |
| schema drift 处理不当导致静默错误 | 高 | 首版必须显式失败，禁止静默兼容；DDL 变更场景的处理必须在阶段 4 明确 |
| Ranges() / RelData 序列化未来扩展 | 中 | 在 engine.RelData 接口层面预留扩展点注释，避免未来破坏性修改 |
| 多 dialect 混合查询语义不清晰 | 中 | 在 sql_catalog_interface_draft.md 中明确：JOIN 场景不下推，由 MO 本地执行器处理 |
| connection 资源泄露 | 中 | Reader 必须实现明确 close 语义；Session.Close() 必须传播到底层连接；增加 context cancel 场景的资源回收测试 |
| planner 误判 foreign table 代价 | 低 | 首版启发式 stats 保守设置；ForceOneCN = true 已减轻此类风险 |

---

## 7. 对 scope_replan.md 的采纳意见

`scope_replan.md` 将目标从"最小可落地 MVP"升级为"完整只读联邦查询 V1"，**评审意见：✅ 采纳升级方向，同时采纳其推荐的"保留硬边界"策略**。

具体而言：

**可以提升到一期的能力**（评审同意）：

- Oracle 同时进入一期目标（而非二批预留）
- PostgreSQL 纳入一期设计（作为抽象验证）
- ALTER CONNECTION / ALTER CATALOG
- REFRESH EXTERNAL CATALOG / REFRESH FOREIGN TABLE
- LIMIT / 简单 ORDER BY pushdown
- explain / metrics / timeout / connection test

**必须保留为二期/三期边界的能力**（评审建议坚持）：

- multi-CN foreign scan
- foreign DML / 跨源写入
- 跨源事务 / 统一 snapshot
- join pushdown / agg pushdown
- direct catalog.schema.table 原生执行

---

## 8. 对 sql_catalog_interface_draft.md 的补充建议

### 8.1 增加 DialectAdapter 的 Capabilities 结构

建议 `DialectAdapter` 的 `Capabilities()` 应该返回一个结构体，而不是简单的 bool 列表：

```go
type Capabilities struct {
    ProjectionPushdown bool
    PredicatePushdown  bool
    LimitPushdown      bool
    OrderByPushdown    bool
    // 后续扩展
}

func (d *MySQLAdapter) Capabilities() Capabilities {
    return Capabilities{
        ProjectionPushdown: true,
        PredicatePushdown:  true,
        LimitPushdown:      true,
        OrderByPushdown:    false, // MySQL 8.0 之前不支持 FETCH FIRST
    }
}
```

这样在 explain 和测试中更容易理解每个方言的能力边界。

### 8.2 增加 PostgreSQL 和 Oracle 的类型映射表

`sql_catalog_interface_draft.md` 提供了 Oracle/MySQL 的首批类型建议，但**建议补充**：

- **PostgreSQL 的首批类型映射表**（`postgresql_addendum.md` 已有轮廓但未落地到 interface draft）
- 明确"稳定双向映射"与"文本化保守降级"的分类原则
- 明确哪些类型首版直接 reject（而不是尝试降级）

### 8.3 增加多方言混合场景的明确限制

当前 interface draft 没有明确回答：

> 如果一条查询同时涉及 MySQL foreign table 和 Oracle foreign table（JOIN），会发生什么？

**建议在 interface draft 中明确**：
- 跨方言 JOIN 时，谓词不下推，由 MO 本地执行器处理
- 如果涉及 Oracle 的 `FETCH FIRST` 或 MySQL 的 `LIMIT`，不能同时下推
- 这种场景下的 query timeout 是指整个 MO 查询的超时，不仅仅是远端 SQL 执行超时

---

## 9. 结论

### 9.1 综合评分

| 维度 | 评分 | 说明 |
|------|------|------|
| 架构设计 | ⭐⭐⭐⭐⭐ | 核心决策（主链路、catalog-first、三层分离）论证充分且正确 |
| 工程可行性 | ⭐⭐⭐⭐ | 渐进式推进合理，部分边界需要提前冻结 |
| 完整性 | ⭐⭐⭐⭐ | 文档体系完整，核心问题已覆盖；PostgreSQL 适配需要进一步落地 |
| 可执行性 | ⭐⭐⭐⭐ | code_checklist 可直接作为开工依据 |
| 风险识别 | ⭐⭐⭐⭐ | 风险识别较全面，部分边界场景需要补充 |

### 9.2 最终建议

1. **采纳全部核心架构决策**：继续走 TABLE_SCAN 主链路，坚持 catalog-first，三层对象模型分离
2. **采纳 scope_replan.md 的范围升级**：目标从 MVP 升级为完整 V1，但保留 multi-CN / DML / 跨源事务的硬边界
3. **按 MySQL → PostgreSQL → Oracle 顺序推进 adapter**：PG 的加入是为了验证抽象通用性，不是为了赶工期同时做三个
4. **在继续编码之前，先冻结以下决策**：
   - `IMPORT FOREIGN SCHEMA` 与 `CREATE FOREIGN TABLE ... FROM CATALOG ...` 的语义边界
   - PG 的 catalog 粒度（一个 catalog = 一个 PG database）
   - REFRESH 的语义（全量 / 是否 block / 表删除后的状态）
   - `TranslatePredicate` 返回 `ok = false` 的三种场景区分
5. **后续需要补充的文档内容**：
   - PG 的类型映射表落地到 interface draft
   - schema drift 的显式失败策略明文化
   - multi-CN foreign scan 的已知扩展点注释（Ranges / RelData 层面）

---

*本评审文档生成于 2026-03-31，建议在正式开始编码前，由方案负责人对"需要澄清的 6 个关键问题"给出明确答复，并更新到对应设计文档中。*
