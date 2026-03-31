# MatrixOne 联邦查询 V1 代码任务拆解

## 1. 这份文档的定位

`doc/fq/` 里现在已经有 3 类和 V1 实施相关的文档：

- `federated_query_v1_final_design.md`：回答 **V1 到底做什么**
- `federated_query_v1_checklist.md`：回答 **V1 需要完成哪些阶段和验收项**
- `federated_query_implementation_plan.md`：回答 **整体按什么模块推进**

而本文再往前走一步，专门回答：

> **如果现在就开始写代码，应该先改哪些文件、拆成哪些任务包、每个任务包的输出和验证口径是什么。**

所以，这份文档不是再讲理念，而是把 V1 checklist 继续压缩成可派发、可排期、可验收的代码任务。

---

## 2. 使用方式

建议把这份文档当成“实施 backlog”的起点：

1. 先看 `federated_query_v1_final_design.md`，确认语义冻结
2. 再看 `federated_query_v1_checklist.md`，确认阶段和边界
3. 最后按本文的任务包逐个推进

这份文档的默认原则是：

- **控制面先于执行面**
- **主链路优先于旁路**
- **正确性优先于 pushdown 覆盖率**
- **MySQL 先打通，再用 PostgreSQL 验抽象，再用 Oracle 验高差异方言**

---

## 3. 建议总顺序

推荐按下面顺序推进，而不是多线同时开很大：

1. Parser / AST / 语句分类
2. FE 自处理控制面 DDL
3. 系统 metadata object 与 bootstrap
4. foreign table DDL / relkind / `SHOW CREATE`
5. execution skeleton（mock connector）
6. MySQL 端到端
7. PostgreSQL 补齐通用抽象
8. Oracle 补齐高差异方言
9. refresh / invalid / metrics / timeout / explain 收口

原因很简单：

- 前 4 步把“对象模型”立住
- 第 5 步把“执行主路径”立住
- 后 3 步把“多数据源能力”补齐
- 最后一步把“可运维、可治理”补齐

---

## 4. 任务包拆解

## 4.1 任务包 A：控制面 SQL 的 parser / AST 骨架

### 目标

让 V1 所有控制面 SQL 至少先具备：

- 可 parse
- 有明确 AST
- 能进入后续 FE / planner 路由

### 主要文件

- `pkg/sql/parsers/dialect/mysql/mysql_sql.y`
- `pkg/sql/parsers/dialect/mysql/keywords.go`
- `pkg/sql/parsers/tree/stmt.go`
- `pkg/sql/parsers/tree/stmt_test.go`
- `pkg/sql/parsers/dialect/mysql/mysql_sql_test.go`
- 建议新增：
  - `pkg/sql/parsers/tree/connection.go`
  - `pkg/sql/parsers/tree/external_catalog.go`
  - 如有需要，扩展 `create.go` / `drop.go` / `show.go`

### 关键子任务

- 新增 / 扩展 `CONNECTION`、`CATALOG`、`FOREIGN`、`REFRESH` 等关键字
- 为以下语句定义 AST：
  - `CREATE/ALTER/DROP/SHOW CONNECTION`
  - `CREATE/ALTER/DROP/SHOW EXTERNAL CATALOG`
  - `TEST CONNECTION`
  - `CREATE FOREIGN TABLE ... FROM CATALOG ...`
  - `IMPORT FOREIGN SCHEMA`
  - `REFRESH EXTERNAL CATALOG`
  - `REFRESH FOREIGN TABLE`
- 在 `stmt.go` 中补 statement kind / query type 分类

### 依赖

- 无

### 交付物

- SQL 可以 parse 成稳定 AST
- FE / planner 可以识别这些语句类型

### 验证

- parser 单测通过
- statement kind 单测通过

---

## 4.2 任务包 B：控制面 FE 自处理路由

### 目标

让 `CONNECTION` 和 `EXTERNAL CATALOG` 先按独立 catalog object 落地，而不是伪装成普通表 DDL。

### 主要文件

- `pkg/frontend/self_handle.go`
- `pkg/frontend/mysql_cmd_executor.go`
- `pkg/frontend/stmt_kind.go`
- `pkg/frontend/authenticate.go`
- 可参考：
  - `pkg/frontend/connector.go`
  - `pkg/frontend/publication_subscription.go`
- 建议新增：
  - `pkg/frontend/connection.go`
  - `pkg/frontend/external_catalog.go`

### 关键子任务

- 在 `self_handle.go` 中增加语句分流
- 在 `mysql_cmd_executor.go` 中增加 handle 入口
- 为 `CONNECTION` 实现：
  - create
  - alter
  - drop
  - show create / show
  - test connection
- 为 `EXTERNAL CATALOG` 实现：
  - create
  - alter
  - drop
  - show create / show
  - refresh external catalog
- 补权限检查与事务分类
- `TEST CONNECTION` 尽量复用未来真实执行 CN 的建连路径

### 依赖

- 任务包 A

### 交付物

- FE 可以直接执行 connection / catalog 控制面语句

### 验证

- frontend 单测
- `SHOW CREATE` 结果稳定
- secret 不泄露

---

## 4.3 任务包 C：metadata object 与系统表 bootstrap

### 目标

让 `CONNECTION`、`EXTERNAL CATALOG`、foreign table relkind 在系统 metadata 里有正式表示。

### 主要文件

- `pkg/catalog/types.go`
- `pkg/frontend/predefined.go`
- `pkg/frontend/authenticate.go`
- `pkg/frontend/system_initialize.go`

### 关键子任务

- 在 `pkg/catalog/types.go` 中新增 / 冻结：
  - `SystemForeignRel = "f"`
  - foreign table 判断 helper
  - 新 system table name 常量
- 在 `predefined.go` 中新增：
  - `mo_catalog.mo_connections`
  - `mo_catalog.mo_external_catalogs`
- 在 `authenticate.go` 中补齐：
  - `sysWantedTables`
  - `predefinedTables`
  - `createSqls`
- 在初始化路径中确保新系统表能被 bootstrap

### 依赖

- 任务包 A

### 交付物

- system metadata 中有正式 connection / catalog 对象
- foreign table 有独立 relkind，而不是复用 external table relkind

### 验证

- 系统表初始化单测
- `mo_tables.relkind` 正确落值

---

## 4.4 任务包 D：foreign table DDL / relkind / SHOW CREATE

### 目标

把 foreign table 从“设计上的对象”变成“catalog 中正式存在的对象”。

### 主要文件

- `pkg/sql/plan/build.go`
- `pkg/sql/plan/build_ddl.go`
- `pkg/sql/plan/build_show.go`
- `pkg/sql/plan/build_show_util.go`
- 如需要新增 planner DDL 类型，还需涉及 protobuf / 生成代码

### 关键子任务

- 为 `CREATE FOREIGN TABLE ... FROM CATALOG ...` 增加 build 逻辑
- 为 `IMPORT FOREIGN SCHEMA` 增加 build 逻辑
- 为 `REFRESH FOREIGN TABLE` 选定统一路径：
  - 要么 FE 自处理
  - 要么 planner/compile DDL
  - **不要一半走 FE、一半走 DDL**
- 在 `build_ddl.go` 中把 foreign table 正确写成：
  - `TableType = SystemForeignRel`
  - `relkind = SystemForeignRel`
- 在 `build_show_util.go` 中让 `SHOW CREATE TABLE` 能正确回放 foreign table 建表语义

### 依赖

- 任务包 A
- 任务包 C

### 交付物

- foreign table 可正式建表并落入 `mo_tables / mo_columns`
- `SHOW CREATE TABLE` 正确

### 验证

- planner DDL 单测
- show create 单测

---

## 4.5 任务包 E：foreign relkind 的 planner guardrail

### 目标

确保 foreign table 是走 `TABLE_SCAN` 主路径的“特殊 relation”，而不是误入现有 external/source 特例，也不能误进 DML。

### 主要文件

- `pkg/sql/plan/query_builder.go`
- `pkg/sql/plan/build_insert.go`
- `pkg/sql/plan/build_constraint_util.go`
- `pkg/sql/plan/result_scan.go`

### 关键子任务

- 在 `query_builder.go` 中确认新 relkind **不会**被改写成 `EXTERNAL_SCAN`
- 明确 foreign table 仍保持 `Node_TABLE_SCAN`
- 在 insert / constraint / 其他 DML 入口中禁止 foreign table 写入
- 审查所有对 `SystemExternalRel` / `SystemSourceRel` 的分支，决定是否需要补 `SystemForeignRel`

### 依赖

- 任务包 C
- 任务包 D

### 交付物

- planner 对 foreign table 的路径和边界稳定

### 验证

- `SELECT` 正常
- `INSERT/UPDATE/DELETE/ALTER` 之类的越界能力显式报错

---

## 4.6 任务包 F：federated engine package 骨架

### 目标

把执行面最小公共骨架先搭出来，避免一开始就把 MySQL/PG/Oracle 逻辑散在 disttae 各处。

### 主要文件

- 新增目录：`pkg/vm/engine/federated/`
- 建议文件：
  - `types.go`
  - `relation.go`
  - `reader.go`
  - `catalog.go`
  - `connector.go`
  - `dialect.go`
  - `capability.go`
  - `expr_translate.go`

### 关键子任务

- 定义 `foreignRelation`
- 定义 `foreignReader`
- 定义最小 connector 抽象：
  - `SQL`
  - `FetchSize`
  - `Timeout`
- 定义最小 dialect adapter 抽象：
  - metadata discovery
  - identifier quoting
  - type mapping
  - basic predicate / limit / order pushdown
- 定义 capability 结构体

### 依赖

- 无强依赖，但最好在任务包 D 之后开始

### 交付物

- 一个可扩展的 federated engine 骨架

### 验证

- mock connector 可以被 foreign reader 调用

---

## 4.7 任务包 G：disttae relation 分流与最小执行闭环

### 目标

让 foreign table 真正从 `db.Relation()` 开始走向 `foreignRelation`，并通过 `TABLE_SCAN` 跑通最小查询。

### 主要文件

- `pkg/frontend/compiler_context.go`
- `pkg/vm/engine/disttae/txn_database.go`
- `pkg/vm/engine/disttae/txn_table.go`
- `pkg/sql/compile/compile.go`
- `pkg/sql/compile/scope.go`

### 关键子任务

- 在 `txnDatabase.relation()` 中于 `newTxnTable()` 前识别 foreign table
- foreign table 返回 `foreignRelation`
- `foreignRelation` 首版至少实现：
  - `GetTableDef`
  - `GetTableID`
  - `GetTableName`
  - `Stats`
  - `Ranges`
  - `BuildReaders`
- `Ranges()` 首版可退化为单一逻辑范围
- `BuildReaders()` 首版退化为：
  - `1 real + N-1 empty`
- compile / scope 继续走现有 `TABLE_SCAN` 主链路

### 依赖

- 任务包 C
- 任务包 D
- 任务包 F

### 交付物

- `SELECT * FROM foreign_table` 能在 mock connector 上跑通

### 验证

- engine / compile 联调单测
- 不复制第二套 scan 框架

---

## 4.8 任务包 H：V1 的 `ForceOneCN` 与 relData 策略

### 目标

把 V1 的单 CN 语义落实到代码任务，而不是只停留在文档描述。

### 主要文件

- `pkg/sql/plan/opt_misc.go`
- `pkg/sql/plan/stats.go`
- `pkg/sql/compile/compile.go`
- `pkg/sql/plan/explain/explain_node.go`
- 如未来需要跨 CN 序列化，再看：
  - `pkg/vm/engine/types.go`
  - `pkg/vm/engine/readutil/relation_data.go`
  - `pkg/sql/compile/remoterun.go`

### 关键子任务

- 优先在 scan 节点构造阶段或 planner 明确知道 foreign scan 时固化 `ForceOneCN = true`
- compile 阶段只作为兜底，不作为唯一设置点
- explain 中可以看见 foreign scan / `ForceOneCN` 信息
- **V1 先不要急着发明新的 `RelDataType`**
- 只要 single-CN 路径够用，就先不引入 federated relData 序列化

### 依赖

- 任务包 G

### 交付物

- foreign scan 的单 CN 语义稳定

### 验证

- explain 可见
- 不需要引入额外跨 CN reader 分发复杂度

---

## 4.9 任务包 I：MySQL adapter 端到端

### 目标

先打通第一个真实可用数据源。

### 主要文件

- 建议新增：
  - `pkg/vm/engine/federated/mysql/`
  - 或 `pkg/vm/engine/federated/dialect_mysql.go`

### 关键子任务

- 选定 MySQL 驱动
- DSN 构建
- `TestConnection`
- metadata discovery
- identifier 规则
- type mapping
- pushdown：
  - projection
  - basic predicate
  - `LIMIT`
  - simple `ORDER BY`
- 远端错误映射为 `moerr`

### 依赖

- 任务包 F
- 任务包 G
- 任务包 H

### 交付物

- MySQL foreign table 端到端可查询

### 验证

- MySQL 集成测试
- local + foreign JOIN 基本可用

---

## 4.10 任务包 J：PostgreSQL adapter 补齐通用抽象

### 目标

不是简单“再加一个 driver”，而是用 PG 检验这套抽象是否真的足够一般化。

### 主要文件

- 建议新增：
  - `pkg/vm/engine/federated/postgresql/`
  - 或 `dialect_postgresql.go`

### 关键子任务

- 选定 PG 驱动
- 冻结 `1 catalog = 1 PG database`
- schema discovery
- `search_path` 不影响 metadata discovery
- identifier / quoted identifier
- type mapping
- pushdown：
  - basic predicate
  - `LIMIT`
  - simple `ORDER BY`
- connection / catalog / runtime session 参数分层

### 依赖

- 任务包 I

### 交付物

- PG 适配不推翻现有对象模型

### 验证

- PG 集成测试
- mixed-dialect 本地执行边界正确

---

## 4.11 任务包 K：Oracle adapter 验证高差异方言

### 目标

验证这套抽象能承接 Oracle，而不是只适合 MySQL / PG。

### 主要文件

- 建议新增：
  - `pkg/vm/engine/federated/oracle/`
  - 或 `dialect_oracle.go`

### 关键子任务

- 选定 Oracle 驱动
- `service_name` / `sid`
- schema discovery
- identifier / quoted identifier / 大小写规则
- `FETCH FIRST` / `ROWNUM`
- Oracle type mapping
- 保守 pushdown deny-list
- 重点覆盖：
  - `NUMBER`
  - `DATE`
  - `TIMESTAMP`
  - `TIMESTAMP WITH TIME ZONE`
  - `CLOB`
  - 空字符串 / `NULL`

### 依赖

- 任务包 J

### 交付物

- Oracle 适配稳定可用，且高风险语义默认保守处理

### 验证

- Oracle 集成测试
- 高差异类型 / 语义专项测试

---

## 4.12 任务包 L：refresh / invalid / explain / metrics / timeout 收口

### 目标

让 V1 具备正式产品能力，而不是只有“能查”。

### 主要文件

- `pkg/frontend/` 中 catalog / connection 执行逻辑
- `pkg/vm/engine/federated/`
- `pkg/sql/plan/explain/`
- 相关 metrics / timeout / cancel 落点

### 关键子任务

- `REFRESH EXTERNAL CATALOG`
- `REFRESH FOREIGN TABLE`
- 同一对象的 refresh 串行化
- schema drift 显式失败
- invalid foreign table 状态
- explain foreign details
- remote connect/query latency metrics
- rows fetched / remote error / timeout metrics
- timeout / cancel
- secret 脱敏

### 依赖

- 任务包 I
- 任务包 J
- 任务包 K

### 交付物

- V1 的运维与治理闭环

### 验证

- refresh 行为测试
- 失败路径测试
- explain / metrics / timeout 测试

---

## 5. 测试任务建议单独立项

虽然每个任务包都应自带验证，但建议再单独立一个“测试补齐任务流”，避免最后集中补测试。

### 推荐测试落点

- parser：
  - `pkg/sql/parsers/dialect/mysql/mysql_sql_test.go`
  - `pkg/sql/parsers/tree/stmt_test.go`
- planner：
  - `pkg/sql/plan/build_ddl_test.go`
  - `pkg/sql/plan/build_show_util_test.go`
  - `pkg/sql/plan/build_test.go`
- frontend：
  - `pkg/frontend/authenticate_test.go`
  - `pkg/frontend/connector_test.go`
  - `pkg/frontend/mysql_cmd_executor_test.go`
- compile / execution：
  - `pkg/sql/compile/ddl_test.go`
  - `pkg/sql/compile/scope_test.go`
- engine / readutil：
  - `pkg/vm/engine/disttae/txn_table_test.go`
  - `pkg/vm/engine/readutil/relation_data_test.go`（仅当新增 relData 类型时）

---

## 6. 最后建议：按“任务包”而不是“按数据库”组织人力

如果一开始就按 MySQL / PG / Oracle 三条线拆人，容易出现：

- 控制面重复实现
- 抽象提前分叉
- 执行骨架不统一

更好的方式是：

1. 一组人先把 A-H 做完，立住统一主骨架
2. 再按 I / J / K 分 adapter
3. 最后统一做 L 和测试补齐

这样更符合 MatrixOne 当前这件事的真实风险结构：

> **最大的风险不是“驱动不会写”，而是“主对象模型和主执行路径没立稳”。**
