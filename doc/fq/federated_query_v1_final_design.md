# MatrixOne 联邦查询 V1 最终方案

## 1. 这份文档的定位

`doc/fq/` 目录里前面已经有多份设计、评审、重规划和补充文档。它们的作用分别不同：

- `federated_query_v1_executive_summary.md`：给管理层 / 评审会快速理解 V1 价值与边界的压缩版摘要
- `federated_query_design.md`：总体架构分析
- `federated_query_scope_replan.md`：从保守 MVP 升级到更完整 V1 的范围重规划
- `federated_query_sql_catalog_interface_draft.md`：SQL / metadata / 接口草案
- `federated_query_postgresql_addendum.md`：PG 特殊设计点
- `federated_query_code_checklist.md`：保守基线 checklist

而本文的目标更直接：

> **把当前已经讨论成熟的内容，收敛成一份“V1 冻结决策版方案”。**

也就是说，本文不再继续发散，而是回答：

- V1 到底做什么
- V1 不做什么
- 为什么这么定
- 方案的最终对象模型和执行模型是什么
- 数据源范围和实现顺序如何冻结

---

## 2. V1 的核心判断

### 2.1 当前主需求

当前最核心的业务需求是：

> **从外部数据库读数据，再交给 MatrixOne 计算引擎做统一计算。**

这意味着 V1 设计要优先优化下面这个主场景：

- 外部数据库负责提供数据
- MatrixOne 负责统一入口、查询规划、远端取数、以及本地计算执行

### 2.2 但架构不能被这个场景锁死

虽然“远端读数 + MO 计算”是当前一等主场景，但最终架构不应被它锁死成唯一模式。

V1 设计要同时满足两件事：

1. **先把当前主需求做到最好**
2. **不封死未来的合理演进**

后续可能的合理演进包括：

- 更强但仍保守的 pushdown
- direct `catalog.schema.table`
- 更丰富的 stats / optimizer 接入
- 更强的执行形态

所以，V1 的正确方向不是“只允许远端读 + 本地算”，而是：

> **先围绕这个主场景设计，但对象模型、planner 路由、catalog 抽象必须保留未来演进空间。**

---

## 3. V1 最终对象模型

V1 冻结为三层：

1. `CONNECTION`
2. `EXTERNAL CATALOG`
3. `IMPORTED FOREIGN TABLE`

### 3.1 CONNECTION

负责：

- host / port / user / password
- TLS / timeout
- 认证和 secret

### 3.2 EXTERNAL CATALOG

负责：

- 方言类型
- metadata discovery
- include / exclude
- identifier 规则
- metadata cache
- connection pool 参数
- test connection

### 3.3 IMPORTED FOREIGN TABLE

负责：

- 在 MO 中形成稳定的可查询对象
- 固化本地 schema
- 承接执行入口

这里要明确：

> **imported foreign table 持久化到 MO 的主要是对象定义、schema 固化信息和映射 metadata，不是把远端业务数据整张落盘到 MO 本地。**

真正的数据仍然在查询执行时按需从远端读取。

---

## 4. V1 最终执行模型

## 4.1 主路径冻结

V1 明确冻结为：

- `TABLE_SCAN -> Relation -> Reader`

也就是：

- 不长期维护独立的 `FEDERATED_SCAN` 旁路
- foreign table 在 planner 中仍表现为普通表扫描
- foreign 读取能力通过 `Relation` / `Reader` 接入现有主链路

## 4.2 运行时模型

V1 的核心执行方式是：

1. 用户在 MO 中写 SQL
2. planner/binder 仍按正常路径生成 `TABLE_SCAN`
3. 识别到 foreign table 后，走 `foreignRelation`
4. `foreignReader` 通过 connector 从远端取数
5. 安全可下推的部分在远端执行
6. 数据回到 MO 后，继续由 MO 计算引擎做 filter / join / agg / sort

### 4.3 计算分工

V1 的默认计算分工是：

- **远端**：projection、basic predicate、`LIMIT`、简单 `ORDER BY`（仅 capability 允许时）
- **MO 本地**：residual filter、JOIN、AGG、复杂表达式、复杂排序与后续算子

---

## 5. V1 最终范围

## 5.1 数据源范围

V1 的设计范围冻结为：

- MySQL
- PostgreSQL
- Oracle

但实现顺序冻结为：

1. MySQL
2. PostgreSQL
3. Oracle

原因：

- MySQL 最适合快速跑通第一条真实链路
- PostgreSQL 最适合验证 catalog/schema/type/session 抽象是否足够一般
- Oracle 最适合验证高差异企业级方言是否能被稳定承接

## 5.2 SQL / 控制面范围

V1 冻结支持：

- `CREATE CONNECTION`
- `ALTER CONNECTION`
- `DROP CONNECTION`
- `SHOW CREATE CONNECTION`
- `CREATE EXTERNAL CATALOG`
- `ALTER EXTERNAL CATALOG`
- `DROP EXTERNAL CATALOG`
- `SHOW CREATE CATALOG`
- `CREATE FOREIGN TABLE ... FROM CATALOG ...`
- `IMPORT FOREIGN SCHEMA`
- `REFRESH EXTERNAL CATALOG`
- `REFRESH FOREIGN TABLE`
- `SHOW CREATE TABLE <foreign table>`

同时冻结以下补充语义：

- `CREATE FOREIGN TABLE ... FROM CATALOG ...` 是单表导入标准语法
- `IMPORT FOREIGN SCHEMA` 是批量导入语法，复用单表导入主管线
- `IMPORT FOREIGN SCHEMA` 必须显式指定 `INTO target_schema`
- `IMPORT FOREIGN SCHEMA` 首版只做全量导入，不做增量同步
- `TEST CONNECTION` 或 catalog 创建时的真实建连校验应纳入 V1 控制面行为
- `TEST CONNECTION` 应尽量贴近未来真实查询执行 CN 的建连路径，而不是只验证 metadata 写入时的一次连通性

## 5.3 查询能力范围

V1 冻结支持：

- foreign 单表 `SELECT`
- local + foreign JOIN（本地执行）
- `AGG` over foreign table（本地聚合）
- projection pushdown
- basic predicate pushdown
- `LIMIT` pushdown
- 简单 `ORDER BY` pushdown（方言 capability 允许时）
- explain / metrics / timeout / cancel

补充冻结：

- `foreign + foreign JOIN` 在语义上允许通过 MO 本地执行器完成，但**不作为 V1 主打能力和性能承诺**
- 多方言混合查询（例如 MySQL + PG、MySQL + Oracle）不做跨源 pushdown，由 MO 本地统一完成 JOIN / AGG / residual filter

## 5.4 schema / refresh 范围

V1 冻结为：

- imported foreign table 在导入时固化本地 schema
- 不做自动 schema 同步
- schema drift 发生后显式失败
- 通过 `REFRESH` 修正

`REFRESH` 的 V1 语义冻结为：

- 首版统一按全量重刷理解
- refresh 只更新 metadata / imported schema
- refresh 期间同一 catalog / foreign table 的并发 refresh 需要串行化
- 已经开始执行的查询继续使用其已绑定 schema
- refresh 成功后的新查询使用新 metadata
- 远端对象丢失时，将 imported foreign table 标记为 invalid，并在查询时报错
- 远端类型定义一旦变化，即使看起来只是 widening / length 扩大，也先按 schema drift 处理，不做静默兼容
- 只有在 `REFRESH` 后新类型仍能稳定映射到 MO 类型时，才更新 imported schema；否则继续走 invalid / 显式失败路径

## 5.5 source-specific catalog 边界

为了避免 MySQL / PG / Oracle 混在一起时对象边界不清，V1 还需要冻结以下 source-specific 规则：

- MySQL：一个 external catalog 可以覆盖多个 remote database
- PostgreSQL：**一个 external catalog 对应一个 PG database**，catalog 内再暴露多个 schema
- Oracle：一个 external catalog 通常对应一个 service/connection scope，catalog 内暴露多个 schema

这条规则很关键，因为它直接影响：

- `IMPORT FOREIGN SCHEMA` 的语义
- namespace 抽象
- metadata discovery
- 权限与 show/create 的对象边界

---

## 6. V1 明确不做的内容

下面这些能力明确不进 V1：

- multi-CN foreign scan
- foreign `INSERT/UPDATE/DELETE`
- 跨源事务
- 统一 snapshot
- join pushdown
- 复杂 agg pushdown
- 复杂函数 pushdown
- 自动 schema 同步
- direct `catalog.schema.table` 原生执行

这些能力不是永远不做，而是：

> **不与 V1 主线绑定交付。**

---

## 7. V1 的关键工程原则

V1 明确遵守下面这些原则：

1. **主链路优先**
   - foreign query 必须复用现有 `TABLE_SCAN` 主路径

2. **单 CN 优先**
   - `ForceOneCN = true`
   - 优先在 scan 节点构造阶段就固化该属性

3. **metadata 与数据分离**
   - imported foreign table 落地的是 metadata，不是远端业务数据落盘

4. **capability-based pushdown**
   - 只有当方言可证明等价时才允许下推

5. **方言 adapter 一等公民**
   - MySQL / PG / Oracle 不能靠同一套 SQL 模板硬套

6. **先保正确，再谈性能**
   - 不允许为了更高 pushdown 覆盖率牺牲结果语义正确性

---

## 8. V1 关键接口冻结方向

这里不冻结所有代码细节，但冻结接口方向。

### 8.1 ExternalCatalog

接口层建议统一使用 `namespace` 抽象：

- MySQL：通常对应 remote database
- PostgreSQL：通常对应 remote schema
- Oracle：通常对应 remote schema

并由 `ExternalCatalog` 负责：

- metadata discovery
- connection pool 与 session 生命周期边界
- `NewSession()`
- `TestConnection()`
- stats 获取

V1 进一步冻结：

- 不要求跨 CN 的全局连接池语义
- connection pool 的拥有者是执行 CN 上的 `ExternalCatalog`
- 查询执行通过 `NewSession()` 借出 session，并在 reader/stream close 时回收到统一关闭路径

### 8.2 DialectAdapter

`DialectAdapter` 负责：

- metadata SQL
- identifier 规则
- 类型映射
- pushdown 翻译
- capability 暴露

`TranslatePredicate` 不再用含糊的 `ok bool` 表达，而应显式区分：

- 可下推
- 不可下推但可回退本地
- 翻译失败应报错

### 8.3 FederatedRelData

V1 允许使用最小 `FederatedRelData` 服务单 CN 本地 reader 构建，但：

- 不要求 V1 就支持 remoterun 反序列化
- 应在 `readutil` 一类注册点预留未来 multi-CN 扩展位

---

## 9. V1 成功标准

如果 V1 最终要算“成功交付”，至少应满足：

1. 能稳定连接 MySQL / PostgreSQL / Oracle
2. 能创建 connection / catalog / foreign table
3. foreign table 查询时，数据仍主要来自远端即时读取
4. MO 能在本地完成 join / agg / residual filter
5. 简单 pushdown 可生效，且结果正确
6. schema drift 有显式失败和 refresh 修复路径
7. explain / metrics / timeout / error mapping 可用
8. 不因为先做 V1 而把未来更强执行形态锁死

---

## 10. 最终结论

MatrixOne 联邦查询 V1 的最终方案可以归纳成一句话：

> **以 `CONNECTION + EXTERNAL CATALOG + IMPORTED FOREIGN TABLE` 为控制与对象模型，以 `TABLE_SCAN -> Relation -> Reader` 为执行主路径，以“远端读数 + MO 计算”为当前主场景，交付一个支持 MySQL / PostgreSQL / Oracle 的完整只读联邦查询 V1。**

这个 V1 不追求一次做完所有高复杂度能力，但它必须做到两点：

- 当前主需求能真正落地
- 长期架构不被错误锁死
