# MatrixOne 联邦查询可执行 Code Checklist

> 本文对应的是**保守可落地基线**，用于先把架构主链路稳稳接进去。
>
> 如果你希望把 Oracle/MySQL、refresh、更强 pushdown、更多首发能力直接纳入一期正式交付，请继续看 `doc/fq/federated_query_scope_replan.md`。
>
> 如果要把 PostgreSQL 也纳入规划，请继续看 `doc/fq/federated_query_postgresql_addendum.md`。
>
> 如果要直接看冻结后的 V1 执行版清单，请继续看 `doc/fq/federated_query_v1_checklist.md`。

本文把前面的设计稿、实施拆解稿和 review 结论进一步收敛成一份**可以直接拿来排期和开工**的 checklist。目标不是再讲一遍设计理念，而是回答：

- 第一批代码到底先改哪些目录
- 哪些约束必须先冻结，避免越做越散
- MySQL-first / Oracle-follow 的实现边界如何拆
- 每一阶段的“完成标准”是什么

---

## 1. 首批范围冻结

在开始写代码前，建议先把下面这些约束明确冻结：

- [ ] 首批只支持 `SELECT`
- [ ] 基线范围先用 `MySQL` 打通主链路；如果采用扩展 V1 规划，则顺序升级为 `MySQL -> PostgreSQL -> Oracle`
- [ ] 首批只支持 `CONNECTION + EXTERNAL CATALOG + IMPORTED FOREIGN TABLE`
- [ ] 执行面必须复用 `TABLE_SCAN -> Relation -> Reader`
- [ ] 首批强制 `ForceOneCN = true`
- [ ] 首批不做 multi-CN foreign scan
- [ ] 首批不做 write / DML pushdown
- [ ] 首批不做 join/agg pushdown
- [ ] 首批 imported foreign table 采用“本地 schema 固化 + 显式 refresh”
- [ ] 首批 connector 请求面只接受“已翻译 SQL + 少量执行参数”

如果这些约束没有冻结，后面 parser、catalog、engine、connector 会很快互相牵扯，范围失控。

---

## 2. 必须先拍板的工程规则

这些不是“实现细节”，而是所有代码改动都要遵守的根规则：

- [ ] 不新建长期 `FEDERATED_SCAN` 旁路 operator
- [ ] `foreign table` 在 planner 中仍绑定为普通 `TABLE_SCAN`
- [ ] `relkind` 显式新增 `SystemForeignRel = "f"`，禁止散落字面量
- [ ] `txnDatabase.relation()` 是 foreign relation 的主接入点，避免把 foreign 分支堆进 `txnTable`
- [ ] `BuildReaders()` 首版只允许 `1 real reader + N-1 empty reader`
- [ ] `Ranges()` 首版只返回单一“全表扫描”语义 range，不伪造 disttae block/object range
- [ ] `test_connection` 的定义要尽量贴近实际执行 CN，而不只是“建对象时连通”
- [ ] secret 只允许存放在 `connection` 对象，不允许落在 foreign table 定义里
- [ ] schema drift 发生时不允许静默兼容，必须失败或要求显式 refresh

---

## 3. 第一批代码改造顺序

推荐按下面的顺序推进，而不是多个面同时开花。

### 3.1 阶段 A：控制面 DDL 和元数据骨架

目标：先把对象模型立住，让 `CONNECTION / EXTERNAL CATALOG / FOREIGN TABLE` 能在系统里被创建和记录。

#### 目录与文件

- [ ] `pkg/sql/parsers/tree/`
- [ ] `pkg/sql/parsers/dialect/mysql/`
- [ ] `pkg/sql/plan/build.go`
- [ ] `pkg/sql/plan/build_ddl.go`
- [ ] `pkg/catalog/types.go`
- [ ] `pkg/frontend/predefined.go`
- [ ] `pkg/frontend/` 中负责 DDL 执行、权限与 show/create 的代码

#### 必做项

- [ ] 新增 `CREATE CONNECTION`
- [ ] 新增 `DROP CONNECTION`
- [ ] 新增 `CREATE EXTERNAL CATALOG`
- [ ] 新增 `DROP EXTERNAL CATALOG`
- [ ] 新增 `CREATE FOREIGN TABLE ... FROM CATALOG ...`
- [ ] 新增 `IMPORT FOREIGN SCHEMA`
- [ ] 冻结 `IMPORT FOREIGN SCHEMA` 与 `CREATE FOREIGN TABLE ... FROM CATALOG ...` 的语义边界
- [ ] 预留 `REFRESH EXTERNAL CATALOG`
- [ ] 预留 `REFRESH FOREIGN TABLE`
- [ ] 新增 `SystemForeignRel = "f"`
- [ ] 新增 `mo_catalog.mo_connections`
- [ ] 新增 `mo_catalog.mo_external_catalogs`
- [ ] imported foreign table 继续落到 `mo_tables/mo_columns`
- [ ] foreign table properties 中记录 `foreign_catalog_id/catalog_name/source_type/remote_schema/remote_table/pushdown_policy/fetch_size`
- [ ] `SHOW CREATE CONNECTION`
- [ ] `SHOW CREATE CATALOG`
- [ ] `SHOW CREATE TABLE <foreign table>`

#### 验收标准

- [ ] SQL 能成功 parse / build / execute
- [ ] catalog 中能看到 connection 和 external catalog 元数据
- [ ] `SHOW CREATE` 不泄露 secret
- [ ] imported foreign table 能落成 `relkind = SystemForeignRel`

### 3.2 阶段 B：执行骨架接入

目标：在不接真实 MySQL 的前提下，先让 foreign table 走通 `TABLE_SCAN -> Relation -> Reader` 主链路。

#### 目录与文件

- [ ] `pkg/vm/engine/disttae/txn_database.go`
- [ ] 新增 `pkg/vm/engine/federated/`

#### 建议包内文件

- [ ] `types.go`
- [ ] `relation.go`
- [ ] `reader.go`
- [ ] `connector.go`
- [ ] `catalog.go`
- [ ] `dialect.go`

#### 必做项

- [ ] 在 `txnDatabase.relation()` 中于 `newTxnTable()` 前识别 foreign table
- [ ] 返回 `foreignRelation`
- [ ] 实现 `Ranges()`
- [ ] 实现 `BuildReaders()`
- [ ] 实现 `Stats()`
- [ ] 实现最小 `QueryRequest`
- [ ] 提供 mock connector
- [ ] 确保 `ForceOneCN = true`
- [ ] 如果 compile 侧无法直接把本地 reader 数压成 1，则在 `BuildReaders()` 中退化为 `1 real + N-1 empty`

#### 验收标准

- [ ] 不接真实数据库时，mock connector 也能跑通 `SELECT * FROM foreign_table`
- [ ] compile / scope 路径不需要为 foreign table 复制第二套 scan 框架
- [ ] explain 中能看见 foreign table 标识

### 3.3 阶段 C：MySQL-first 打通真实链路

目标：先把一个最常见、差异最小的关系库打通，让整套框架从“能编译”进入“可真实查询”。

#### 目录与文件

- [ ] `pkg/vm/engine/federated/mysql/` 或等价目录
- [ ] `pkg/vm/engine/federated/dialect_mysql.go`
- [ ] `pkg/vm/engine/federated/catalog_mysql.go`

#### 必做项

- [ ] 选定 MySQL Go 驱动
- [ ] 实现 connection config 到 DSN 的转换
- [ ] 实现 `TestConnection`
- [ ] 实现 database/table discovery
- [ ] 实现列类型映射
- [ ] 实现 projection + basic predicate pushdown
- [ ] 支持 `FetchSize` / `Timeout`
- [ ] 远端错误映射到 `moerr`

#### MySQL 首批推荐支持的类型

- [ ] `int / bigint`
- [ ] `decimal`
- [ ] `varchar / text`
- [ ] `datetime / timestamp`
- [ ] `null`

#### MySQL 首批建议暂缓或保守处理

- [ ] `json`
- [ ] `bit`
- [ ] `enum/set`
- [ ] `unsigned` 的边界转换
- [ ] timezone 相关歧义类型

#### 验收标准

- [ ] `CREATE CONNECTION` + `CREATE EXTERNAL CATALOG` + `CREATE FOREIGN TABLE ... FROM CATALOG ...` 可以打通
- [ ] `SELECT *` / projection / basic filter / limit 可以执行
- [ ] query timeout、生效路径和错误信息可验证
- [ ] secret 不出现在日志和 `SHOW CREATE`

### 3.4 阶段 D：Schema drift / refresh 正式闭环

目标：把“导入后 schema 固化”从文档约束真正落成行为。

#### 必做项

- [ ] imported foreign table 创建时把远端 schema 固化写入 `mo_columns`
- [ ] 查询默认只按本地固化 schema 解释结果
- [ ] 远端缺列/类型不兼容时，查询显式失败
- [ ] 提供 `REFRESH FOREIGN TABLE` 或等价 refresh/import 逻辑
- [ ] 提供 `REFRESH EXTERNAL CATALOG` 或等价 metadata refresh 逻辑
- [ ] 定义 refresh 的 V1 语义：全量重刷、串行化同对象 refresh、远端缺失对象标 invalid

#### 验收标准

- [ ] 远端 schema 漂移不会静默改变查询含义
- [ ] refresh 前后行为差异可测试、可解释

### 3.5 阶段 E：Oracle 方言适配

目标：在不破坏 MySQL-first 主链的前提下，引入第二个高差异方言，验证 external catalog 架构是否成立。

#### 目录与文件

- [ ] `pkg/vm/engine/federated/oracle/` 或等价目录
- [ ] `pkg/vm/engine/federated/dialect_oracle.go`
- [ ] `pkg/vm/engine/federated/catalog_oracle.go`

#### 必做项

- [ ] 选定 Oracle Go 驱动
- [ ] 支持 `service_name` / `sid` 基本配置
- [ ] 实现 schema/table discovery
- [ ] 实现 quoted identifier 与大小写规则
- [ ] 实现 limit / fetch first 语法适配
- [ ] 实现 Oracle 类型映射矩阵
- [ ] 明确 pushdown deny-list

#### Oracle 必须重点单测的差异点

- [ ] `NUMBER`
- [ ] `DATE`
- [ ] `TIMESTAMP`
- [ ] `TIMESTAMP WITH TIME ZONE`
- [ ] `CLOB`
- [ ] 空字符串 / `NULL` 语义
- [ ] 大小写与引号

#### 验收标准

- [ ] Oracle 不被当成“换个 DSN 就行”的伪同构源
- [ ] 风险类型和风险表达式默认走保守路径
- [ ] 同一个 external catalog 框架下 MySQL/Oracle 都能接入

### 3.6 阶段 F：Explain、统计信息与可观测性

目标：让联邦查询可调试、可定位，而不是只能“能跑”。

#### 目录与文件

- [ ] `pkg/sql/plan/stats.go`
- [ ] `pkg/sql/plan/explain/`
- [ ] `pkg/frontend/` / `pkg/common/moerr`
- [ ] `pkg/vm/engine/federated/` 日志与 metrics

#### 必做项

- [ ] explain 中展示 foreign table / connector type / pushed predicates / residual predicates / `ForceOneCN`
- [ ] 增加 remote connect latency / remote query latency / rows fetched / timeout / remote error 等指标
- [ ] 统一错误码

#### 验收标准

- [ ] 慢查询可以区分“卡在远端”还是“卡在本地处理”
- [ ] 错误信息足以定位，但不泄露 secret

---

## 4. 关键代码清单

下面这张表更适合直接拿去做 issue / 子任务拆分。

| 优先级 | 代码点 | 建议动作 |
|--------|--------|----------|
| P0 | `pkg/catalog/types.go` | 定义 `SystemForeignRel = "f"` |
| P0 | `pkg/frontend/predefined.go` | 增加 `mo_connections`、`mo_external_catalogs` |
| P0 | `pkg/sql/plan/build_ddl.go` | 增加 connection / catalog / import foreign table DDL build |
| P0 | `pkg/frontend/` DDL 执行代码 | 落库、权限、show/create、secret 保护 |
| P1 | `pkg/vm/engine/disttae/txn_database.go` | 在 `newTxnTable()` 前拦截 foreign table |
| P1 | `pkg/vm/engine/federated/relation.go` | 实现 `Ranges/BuildReaders/Stats` |
| P1 | `pkg/vm/engine/federated/reader.go` | 实现 reader 生命周期与 close/cancel |
| P1 | `pkg/vm/engine/federated/connector.go` | 定义 `QueryRequest/Session/RowStream` |
| P2 | `pkg/vm/engine/federated/mysql/` | MySQL driver、metadata、type map、pushdown |
| P3 | `pkg/vm/engine/federated/oracle/` | Oracle driver、metadata、identifier、type map |
| P2/P3 | `pkg/sql/plan/explain/` | foreign explain 输出 |
| P2/P3 | `pkg/sql/plan/stats.go` | 启发式 foreign stats |

---

## 5. 测试 Checklist

### 5.1 DDL / Catalog

- [ ] `CREATE CONNECTION`
- [ ] `DROP CONNECTION`
- [ ] `CREATE EXTERNAL CATALOG`
- [ ] `DROP EXTERNAL CATALOG`
- [ ] `CREATE FOREIGN TABLE ... FROM CATALOG ...`
- [ ] `IMPORT FOREIGN SCHEMA`
- [ ] `SHOW CREATE CONNECTION`
- [ ] `SHOW CREATE CATALOG`
- [ ] `SHOW CREATE TABLE foreign_table`
- [ ] 非法 option / 缺失字段 / 重名 / 权限不足

### 5.2 执行链路

- [ ] `SELECT * FROM foreign_table`
- [ ] projection
- [ ] simple predicate pushdown
- [ ] residual predicate
- [ ] `LIMIT`
- [ ] local join foreign（仅本地执行）
- [ ] agg over foreign（仅本地聚合）

### 5.3 稳定性

- [ ] timeout
- [ ] cancel
- [ ] connector close
- [ ] connection pool 生命周期
- [ ] remote error -> `moerr` 映射
- [ ] schema drift 后失败与 refresh 恢复

### 5.4 方言差异

- [ ] MySQL 类型映射
- [ ] Oracle 类型映射
- [ ] 大小写与 quoted identifier
- [ ] limit 语法差异
- [ ] null / 空字符串语义差异

---

## 6. 明确暂缓的事项

这些能力建议明确放到后续阶段，避免干扰首批落地：

- [ ] direct `catalog.schema.table` 原生执行
- [ ] 多 CN foreign scan
- [ ] 写入类联邦能力
- [ ] join/agg pushdown
- [ ] 跨源统一 snapshot / 事务一致性
- [ ] 自动 schema 同步
- [ ] 高级函数/表达式翻译

---

## 7. 建议的实际开工方式

如果要真正开始编码，建议按下面节奏推进：

1. 先做 **阶段 A + 阶段 B**，不接真实外部数据库，只用 mock connector 跑通主链路。
2. 再做 **阶段 C**，用 MySQL 打通第一条真实链路。
3. 然后补 **阶段 D**，把 schema drift / refresh 做成正式行为。
4. 最后再做 **阶段 E**，引入 Oracle 方言适配，验证 catalog-first 架构能否支撑高差异源。

这个顺序的关键价值在于：先证明 **MO 主链路可接入**，再证明 **真实数据库可接入**，最后证明 **多方言模型成立**。
