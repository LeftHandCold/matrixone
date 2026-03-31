# MatrixOne 联邦查询设计方案

## 1. 文档目标

本文基于当前 MatrixOne 代码实现，对联邦查询能力的可行接入方案做一次面向工程落地的设计分析，回答以下问题：

- 当前查询引擎主链路是什么，联邦查询最适合接在哪一层
- 现有 external/source/table function 等能力哪些可以复用，哪些不能直接复用
- 第一版联邦查询应该收敛哪些边界
- 推荐的元数据、planner、执行器、connector、pushdown、统计信息和安全设计是什么
- 为什么推荐走 `TABLE_SCAN -> Relation -> Reader` 主路径，而不是长期维护独立的旁路 operator

本文重点是“总体设计和架构判断”；具体实施拆解见配套文档 `doc/fq/federated_query_implementation_plan.md`，而面向 Oracle/MySQL 首批支持的 SQL、catalog 元数据与关键接口草案见 `doc/fq/federated_query_sql_catalog_interface_draft.md`。

如果你刚接触联邦查询，建议先看 `doc/fq/federated_query_beginner_guide.md`，先建立概念，再看本设计稿。

如果要把当前“保守 MVP”口径升级成“更完整的一期正式交付范围”，请继续看 `doc/fq/federated_query_scope_replan.md`。

如果要把 PostgreSQL 也正式纳入规划，请继续看 `doc/fq/federated_query_postgresql_addendum.md`。

还要提前说明一个总原则：**当前最核心的业务需求确实是“从外部数据库读数据，再交给 MO 计算引擎计算”，但设计方案不能被这一个场景永久锁死。** 更合理的做法是：

- 把“远端读数 + MO 计算”作为当前一等主场景
- 用它来决定第一波架构落点
- 但同时保留未来向更强 pushdown、direct `catalog.schema.table`、以及其他合理执行形态演进的空间

## 2. 当前代码架构概览

### 2.1 普通表查询主链路

当前普通表查询的主链路可概括为：

1. SQL 解析为 AST
2. `TxnCompilerContext` 解析库表、事务、快照、租户上下文
3. planner 生成 `TABLE_SCAN`
4. compile 阶段获取 `engine.Database` / `engine.Relation`
5. 扫描时由 `Relation.Ranges()` 生成分片描述
6. 再由 `Relation.BuildReaders()` 构造 reader
7. reader 输出 batch，继续进入 projection / filter / join / agg / sort

关键代码位置：

- SQL 解析：
  - `pkg/sql/parsers/dialect/mysql/`
  - `pkg/sql/parsers/tree/`
- 绑定与解析：
  - `pkg/frontend/compiler_context.go`
- 计划生成：
  - `pkg/sql/plan/build.go`
  - `pkg/sql/plan/query_builder.go`
- 编译执行：
  - `pkg/sql/compile/compile.go`
  - `pkg/sql/compile/scope.go`
- 引擎接口：
  - `pkg/vm/engine/types.go`

其中最关键的一点是：**普通表扫描最终依赖 `engine.Relation` 的 `Ranges()` 和 `BuildReaders()` 两个入口。**

### 2.2 当前已有的非普通表扫描路径

MatrixOne 当前已有两类“看起来像表、但不走普通 relation 扫描主线”的能力。

#### 2.2.1 External table

相关代码：

- `pkg/sql/plan/build_ddl.go`
- `pkg/sql/plan/query_builder.go`
- `pkg/sql/compile/compile.go`
- `pkg/sql/colexec/external/external.go`
- `pkg/catalog/types.go`

特点：

- external table 在 catalog 中仍然作为表存在
- `relkind` 标记为 external
- 文件访问参数主要编码在 `rel_createsql`
- planner 会把它绑定为 `Node_EXTERNAL_SCAN`
- 执行时走独立的 external reader 分发逻辑

这条路径比较适合“文件即表”的场景，不是远端数据库联邦查询的最终主线。

#### 2.2.2 Source

相关代码：

- `pkg/sql/plan/build_ddl.go`
- `pkg/sql/plan/query_builder.go`
- `pkg/sql/compile/compile.go`
- `pkg/sql/colexec/source/source.go`
- `pkg/catalog/types.go`

特点：

- source 也作为一种表对象存在
- 目前运行时实现明显以 Kafka 为主
- planner 会绑定为 `Node_SOURCE_SCAN`

这说明 MO 已经有“非普通存储对象也表现成表”的前例，但依旧不是关系型远端库联邦查询。

## 3. 现有代码给出的关键约束

### 3.1 compile 端不是“按表切 engine”

`pkg/sql/compile/compile.go` 中的 `handleDbRelContext()` 显示，compile 获取 relation 的方式是：

1. 从当前 session 绑定的 engine 打开 database
2. 再从该 database 打开 relation

也就是说，当前架构不是“每张表可以挂到不同 engine 实现”，而是：

> 先走当前引擎，再由引擎决定返回什么 relation。

与此对应，`pkg/vm/engine/disttae/txn_database.go` 中 `txnDatabase.Relation()` 最终会把 catalog 表解析成 `txnTable` 或相关 delegate。

这意味着，联邦查询真正可落地的接入点不是“额外注册一个新 engine 让 planner 自己切换”，而是：

- 在 disttae 的 relation 解析链路里识别 foreign table
- 返回一个新的 `foreignRelation`
- 让它实现 `engine.Relation`

### 3.2 多 CN 远端扫描不适合首版

这部分是 MVP 边界里最重要的结论。

远端 scope 在 remoterun 过程中会反序列化 `RelData`：

- `pkg/sql/compile/remoterun.go`
- `pkg/vm/engine/readutil/relation_data.go`

而当前 `readutil.UnmarshalRelationData()` 只明确支持：

- `BlockListRelData`
- `EmptyRelationData`

这说明远端 CN 扫描链路当前天然更偏向 block/object 语义。如果 foreign query 首版就试图：

- 自定义新的 split payload
- 在多 CN 间分发 foreign range
- 让远端 CN 自己建立远端数据库连接

则必须一起改动：

- `RelData` 序列化/反序列化链路
- remoterun payload
- 远端 reader 构造链路
- 连接生命周期与错误恢复策略

这会显著扩大首版复杂度。

因此，首版联邦查询建议直接采用：

- `ForceOneCN = true`
- 单 CN 执行 foreign scan
- 不在第一阶段支持多 CN 分片扫描

### 3.3 事务与 snapshot 语义无法天然统一

本地表扫描高度依赖当前事务和快照语义：

- `TxnCompilerContext` 会根据 snapshot 构造 clone txn
- `txnTable.Stats()` / `Rows()` / `Ranges()` / `BuildReaders()` 都与事务上下文紧耦合
- disttae 还要考虑 workspace、uncommitted writes、tombstone、snapshot write offset

而远端数据库：

- 有独立事务实现
- 有独立 snapshot 模型
- 不可能天然复用 MO 的 txn timestamp

因此联邦查询首版必须明确：

- **只支持只读**
- **不支持跨源强一致事务**
- **不承诺 MO 本地表和 foreign table 的统一 snapshot**

更准确的说法是：

> foreign table 的读取语义由远端连接器提供的 statement 级读语义决定；MO 本地表仍遵循 MO 自身事务和快照；跨源 join 不提供 2PC 或统一快照保证。

### 3.4 统计信息体系天然偏本地 block/object

`pkg/sql/plan/stats.go` 中大量统计逻辑依赖：

- block/object 数量
- zonemap
- NDV overlap
- shuffle range
- table row count 与 object row count

这些指标天然适合本地对象存储和 disttae 读路径，不适合直接拿来描述异构远端库。

因此 foreign query 首版不应以“高精度代价模型”为目标，而应：

- 提供启发式 stats
- 先保证正确性与可用性
- 后续再做 analyze、sampling、远端 explain 适配

### 3.5 Pushdown 不能一步做到“全能”

虽然 compile / reader 已经能传递：

- filter expr
- order by
- projection 结果列

但这并不意味着首版可以安全支持：

- 任意表达式下推
- join pushdown
- agg pushdown
- function pushdown
- order/limit 的无条件下推

核心风险在于：

- SQL 方言差异
- 类型差异
- null 语义差异
- collation / charset 差异
- decimal / timestamp 兼容性

因此首版推导原则应是：

> 能证明等价才下推；不能证明等价就留在 MO 本地执行。

## 4. 推荐总体方案

## 4.1 总结结论

**正式方案建议：foreign table 继续走 `TABLE_SCAN` 主路径，不新增长期独立的 `FEDERATED_SCAN` 主线。**

也就是说：

- planner 仍把 foreign table 当作表
- compile 仍通过 `engine.Database.Relation()` 获取 relation
- 由 disttae 针对 foreign table 返回一个新的 `foreignRelation`
- 再通过 `foreignRelation.BuildReaders()` 构造 `foreignReader`

这条路径最大的价值在于：

- 复用现有 scan 主框架
- 避免形成长期旁路
- 便于解释、优化、权限、统计和测试逐步收敛到一致模型

## 4.2 为什么不推荐长期维护独立 `FEDERATED_SCAN`

如果直接模仿 external/source 做一个新 operator，短期确实会更快，但长期问题明显：

- planner 会把 foreign table 永久视为特殊对象
- explain / stats / filter / join 语义更难统一
- 会出现两套“表扫描体系”
- 未来回归普通表模型成本更高

因此我的建议是：

- PoC 若追求极致速度，可短期做专用 scan 节点
- 但正式能力不要把自己锁死在旁路 operator 上

## 4.3 推荐架构分层

### 元数据层

原始方案中，这一层只有：

- `CREATE CONNECTION`
- `CREATE FOREIGN TABLE`

但如果把目标明确放到 **Oracle + MySQL 这类企业级关系库接入**，并参考 Doris 的 `ExternalCatalog / JdbcExternalCatalog / JdbcExternalTable` 路线，长期更优的建模应升级为：

- `CREATE CONNECTION`
- `CREATE EXTERNAL CATALOG`
- `IMPORT FOREIGN SCHEMA` 或 `CREATE FOREIGN TABLE ... FROM CATALOG ...`

也就是说，**Connection 负责 secret，Catalog 负责远端库/Schema 发现与方言能力，Foreign Table 负责执行面和本地治理。**

### 引擎适配层

在 `pkg/vm/engine/disttae/txn_database.go` 中识别 foreign table，返回 `foreignRelation`。

### 执行层

由 `foreignRelation` 实现：

- `Ranges()`
- `BuildReaders()`
- `Stats()`

并由 `foreignReader` 负责真正远端读取。

### Connector 层

新增统一 connector 抽象：

- driver
- session
- capabilities
- query request / row stream

首版只实现一个 driver。

### Pushdown 层

根据 connector capability，把一部分 projection / predicate 下推到远端，其余保留在 MO 本地执行。

## 4.4 参考 Doris 后的更优长期方案

在参考 Doris 的外部数据源做法后，我认为 MatrixOne 的联邦查询方案可以进一步优化为一个**双层架构**：

### 控制面：`CONNECTION + EXTERNAL CATALOG`

这一层负责：

- 连接信息与敏感配置管理
- 远端数据库/Schema/表发现
- identifier mapping
- metadata cache
- connection pool 策略
- test connection
- 数据源方言与函数规则

这与 Doris 中 `JdbcExternalCatalog` 的职责很接近。Doris 的代码里可以看到它把以下能力统一放在 catalog 层管理：

- JDBC 连接参数
- 连接池配置
- 远端数据库和表枚举
- 名称映射
- 连接测试
- 函数规则
- 不同数据库类型的统计信息获取策略

### 执行面：`FOREIGN TABLE` / imported table

这一层负责：

- 本地受控暴露哪些远端表
- 本地 schema override / 别名治理
- 权限边界
- `TABLE_SCAN -> Relation -> Reader` 执行复用

这样做的好处是：

1. 对客户更友好。客户往往不是只接一张表，而是接一个 Oracle schema 或一个 MySQL 实例下的一批表。
2. 对工程更友好。外部 catalog 负责“发现与映射”，foreign table 负责“稳定执行单元”，不会把所有复杂度都压进单表对象。
3. 对 MO 当前架构更友好。首版不必马上做完整的动态 `catalog.schema.table` 执行链路，可以先通过 import/pin 的方式把远端表纳入当前 `TABLE_SCAN` 主路径。

### 为什么不是机械照搬 Doris 的 JDBC Catalog

虽然 Doris 的 catalog-first 思路值得借鉴，但 MatrixOne 是 Go 代码栈，不适合机械复制“JDBC catalog”的实现路线。

对 MO 来说，更合理的是：

- 借鉴 Doris 的**建模方式**
- 但 connector 实现采用 **Go 原生驱动 / 方言 adapter**

例如：

- MySQL：优先用原生 MySQL 协议/驱动
- Oracle：优先用 Oracle 原生 Go 驱动（如 `godror` 一类能力模型）

也就是说，**要借鉴 Doris 的 catalog 设计，而不是照搬 JDBC 技术选型。**

### 对 MO 的现实落地建议

综合当前架构与 Doris 的经验，我建议 MatrixOne 的方案调整为：

#### 近期 MVP

- `CREATE CONNECTION`
- `CREATE EXTERNAL CATALOG`
- `IMPORT FOREIGN SCHEMA` / `CREATE FOREIGN TABLE ... FROM CATALOG ...`
- imported foreign table 继续走 `TABLE_SCAN`

#### 中长期

- 支持直接查询 `catalog.schema.table`
- binder / resolver / compile 正式支持 external catalog 原生对象
- imported foreign table 退化为“治理、别名和 schema 固定化”机制，而不是唯一访问方式

这比“只有 connection + foreign table”更适合 Oracle/MySQL 客户场景。

## 5. 元数据与 DDL 设计

## 5.1 Connection 对象

建议提供如下语义对象：

```sql
CREATE CONNECTION conn_mysql_01
TYPE = 'mysql'
OPTIONS (
  host = 'x.x.x.x',
  port = 3306,
  user = 'app',
  password = '***',
  database = 'sales',
  ssl_mode = 'required'
);
```

建议单独增加 catalog 表，例如：

- `mo_catalog.mo_connections`

推荐字段：

- `connection_id`
- `connection_name`
- `connection_type`
- `connection_options`
- `connection_status`
- `created_time`
- `owner`
- `comment`

其中：

- 敏感字段应密文存储
- `SHOW CREATE CONNECTION` 要做脱敏
- 连接对象与 foreign table 解耦，方便复用和轮换

### 为什么不建议把凭据直接塞进表属性

external table 允许把部分配置塞进 `rel_createsql`，但对远端数据库连接来说，这样做的问题更严重：

- 安全边界差
- 多表复用差
- 凭据轮换困难
- 审计与权限控制不清晰

因此 foreign query 需要“连接对象”和“表对象”分离。

## 5.2 External Catalog 对象

在 Oracle / MySQL 场景下，我建议新增：

```sql
CREATE EXTERNAL CATALOG mysql_sales
USING CONNECTION conn_mysql_01
TYPE = 'mysql'
OPTIONS (
  include_databases = 'sales,dim',
  metadata_cache_ttl = '300s',
  lower_case_meta_names = 'true',
  test_connection = 'true'
);
```

或对 Oracle：

```sql
CREATE EXTERNAL CATALOG oracle_hr
USING CONNECTION conn_oracle_01
TYPE = 'oracle'
OPTIONS (
  include_schemas = 'HR,CRM',
  metadata_cache_ttl = '300s',
  quoted_identifier = 'auto',
  test_connection = 'true'
);
```

建议 external catalog 负责以下通用能力：

- 远端 database/schema/table 的枚举
- include/exclude 过滤
- identifier mapping
- metadata cache
- connection test
- 连接池参数
- 数据源类型和函数规则

这部分是 Doris 风格里最值得借鉴的地方。

### 为什么 External Catalog 比只做 Foreign Table 更优

如果只做 `CREATE FOREIGN TABLE`，那么：

- 接一批 Oracle/MySQL 表时，需要逐张维护
- 远端 metadata 枚举和方言配置没有稳定挂载点
- connection pool、metadata cache、名字映射无处安放

而 external catalog 提供了一个稳定的“控制面”对象，更适合企业接入场景。

## 5.3 Foreign Table / Imported Table 对象

建议语义：

```sql
CREATE FOREIGN TABLE orders_ext (
  order_id bigint,
  customer_id bigint,
  amount decimal(18,2),
  created_at datetime
)
CONNECTION = 'conn_mysql_01'
REMOTE_SCHEMA = 'sales'
REMOTE_TABLE = 'orders'
OPTIONS (
  pushdown = 'basic',
  fetch_size = 4096
);
```

设计原则：

- foreign table 仍然是表
- 在 `mo_tables/mo_columns` 中落地
- schema 以 MO 侧定义为准
- 首版不做自动 schema 演化

在引入 external catalog 后，foreign table 更推荐作为**import 后的执行对象**，例如：

```sql
CREATE FOREIGN TABLE orders_ext
FROM CATALOG mysql_sales.sales.orders;
```

或：

```sql
IMPORT FOREIGN SCHEMA sales
FROM CATALOG mysql_sales
INTO ext_sales;
```

这样 imported foreign table 仍然可以复用当前执行主路径，同时又让 catalog 层统一承担 metadata discovery。

这里要特别强调：**imported foreign table 落到 MO 里的主要是对象定义、schema 固化信息和映射 metadata，而不是把远端业务数据整体持久化到 MO 本地表中。** 真正的数据读取仍然发生在查询执行时：先从远端数据库读，再进入 MO 计算引擎继续做 filter/join/agg/sort 等处理。

这两种语法的边界建议明确成：

- `CREATE FOREIGN TABLE ... FROM CATALOG ...`：单表导入的标准语法
- `IMPORT FOREIGN SCHEMA`：批量导入语法，本质上复用单表导入主管线

也就是说，`IMPORT FOREIGN SCHEMA` 不应发展成第二套独立实现，而应只是“批量生成 imported foreign table”的高层封装。

## 5.4 catalog 表达方式

建议做法：

- `mo_tables.relkind` 新增一种 foreign 类型，例如 `f`
- foreign table 非敏感元数据保存在 properties 或 `rel_createsql`
- 连接对象单独存储，只在表属性中引用 `connection_id` / `connection_name`
- external catalog 则作为独立 catalog object 或系统表对象存在

这与现有 external/source 的模式兼容，但又避免把敏感配置直接绑死在表定义里。

这里建议不要在代码中散落字面量 `'f'`，而是在 `pkg/catalog/types.go` 中显式定义：

```go
const SystemForeignRel = "f"
```

如果后续还要做 `util.TableIsXXX()` 一类判断，也建议同时补充对应 helper，避免后续 planner、frontend、engine 各自写一套判断逻辑。

## 5.5 `mo_connections` 与 external catalog 的系统表落盘方式

`CONNECTION` 对象建议像 `mo_stages` 一样，作为 `mo_catalog` 中的预定义系统表管理，而不是走普通 `CREATE TABLE` 语义。

推荐实现方式：

- 在 `pkg/frontend/predefined.go` 中定义 `mo_catalog.mo_connections`
- 在 frontend 启动/bootstrap 逻辑中保证其存在
- `CREATE CONNECTION` / `DROP CONNECTION` 走独立 DDL 执行路径
- foreign table 仅引用 connection，不直接持有 secret

这样做的优点是：

- 与现有 stage/cdc 等系统对象风格一致
- 权限与敏感字段管理更清晰
- 不会把 connection 生命周期误绑定到普通表 DDL 语义上

同理，`EXTERNAL CATALOG` 也建议采用独立 object metadata 管理，而不是塞进普通数据库表定义语义中。

另外，`test_connection` 的语义不应只理解为“控制面能连通”，更建议按**实际执行 CN 视角**验证：

- 至少保证执行扫描的 CN 能建立远端连接
- 如果未来支持多 CN，则应考虑按 CN 维度做健康校验或懒加载测试

这个判断是参考 Doris 在 catalog 层同时考虑控制面和执行面连通性的经验得出的。对 MO 来说，即便不照搬 FE/BE 双测，也应保证测试链路与真实执行路径尽量一致。

## 5.6 现有语法/计划模型对 external catalog 的支撑

一个重要观察是：MatrixOne 的语法树和计划对象其实已经为 catalog 级限定名预留了空间：

- `pkg/sql/parsers/tree/object_name.go` 支持 object name 最多三段：`catalog.schema.object`
- `pkg/sql/parsers/tree/table_name.go` 的 `TableName` 支持 `CatalogName`
- `pkg/pb/plan/plan.pb.go` 中 `ObjectRef` 已有 `ServerName/DbName/SchemaName/ObjName`

这意味着对 MO 而言：

- **语法层并不是只能做 `db.table`**
- external catalog 路线在 parser / plan model 上是有基础的

真正需要补的是：

- resolver 如何根据 `CatalogName/ServerName` 路由到外部 catalog manager
- compile 如何获取外部 relation
- 是否先走 imported table，再逐步开放 direct `catalog.schema.table`

## 6. Planner 与优化器建议

## 6.1 planner 继续绑定为 `TABLE_SCAN`

foreign table 在 binder / planner 阶段仍应表现成普通表：

- 仍返回 `ObjectRef + TableDef`
- 仍绑定为 `Node_TABLE_SCAN`

这样能够最大化复用：

- filter
- projection
- join
- agg
- explain
- compile/scope 主逻辑

## 6.2 首版默认 `ForceOneCN`

在 foreign scan 节点上直接设置：

- `node.Stats.ForceOneCN = true`

更具体地说，**如果 planner / query builder 在构造 scan 节点时就已经知道它是 foreign table，最好在那里直接固化 `ForceOneCN`；compile 阶段只作为兜底校验。**

理由：

- 避免 remoterun payload 扩展
- 避免多 CN 建连与远端切分复杂度
- 避免 result merge 和 connector 生命周期失控

这条策略非常适合 MVP。

进一步建议是：**首版不仅限制在单 CN，还尽量把本地 scan DOP 压到 1。**

原因是 `ForceOneCN` 解决的是“在哪个 CN 跑”的问题，但不天然等价于“这个 CN 内只起一个 reader”。如果 planner/compile 侧已有合适钩子，建议直接把 foreign scan 的本地 reader 并发也控制为 1；如果暂时不好统一控制，则在 `BuildReaders()` 中退化为“1 个真实 reader + N-1 个 empty reader”。

## 6.3 Pushdown 范围建议

首版仅建议支持：

- 列裁剪
- 简单谓词下推
- 可选的简单 limit 下推

不建议首版支持：

- join pushdown
- agg pushdown
- 函数 pushdown
- 默认开启的 order by pushdown

可下推谓词建议先限定为：

- `col = const`
- `col > const`
- `col >= const`
- `col < const`
- `col <= const`
- `col IN (...)`
- `col IS NULL`
- `col IS NOT NULL`
- `AND`
- 简单 `OR`

其余表达式在 MO 本地做 residual filter。

## 7. 执行层设计

## 7.1 `foreignRelation`

建议新增一个 relation 实现，首版重点实现：

- `GetTableDef()`
- `CopyTableDef()`
- `GetTableID()`
- `GetTableName()`
- `Stats()`
- `Ranges()`
- `BuildReaders()`

而写接口首版统一返回 `not supported`：

- `Write()`
- `Delete()`
- `AlterTable()`
- 其他修改性操作

## 7.2 `Ranges()` 的角色

对 foreign table 来说，`Ranges()` 首版不是为了做精细 block/object 切片，而是为了让 compile 主路径继续工作。

因此建议：

- 首版始终返回一个逻辑 split
- 或返回极少量 split
- 不伪造 disttae block 语义

未来若要增强，再按远端分区、主键范围或连接器自身 shard 能力切分。

实现上建议补充一个**仅限本地 single-CN 使用的最小 `RelData` 实现**，例如 `FederatedRelData`，只承载“单个逻辑全表范围/单个逻辑远端查询分片”的信息，而不要试图首版就适配远端 CN 序列化链路。

原因是当前 `readutil.UnmarshalRelationData()` 对 remoterun 的支持主要围绕 `BlockListRelData` / `EmptyRelationData`。因此首版 foreign scan 不应依赖“自定义 `RelData` 跨 CN 传输”；如果未来要做 remote CN foreign scan，再单独扩展：

- `readutil.UnmarshalRelationData()`
- remoterun payload
- remote-side reader build path

## 7.3 `BuildReaders()` 的角色

`BuildReaders()` 应完成：

1. 读取 foreign table metadata
2. 解析 connection 对象
3. 初始化 connector session
4. 解析可下推表达式和投影列
5. 构造远端查询请求
6. 返回 `foreignReader`

首版建议：

- 只返回 1 个实际 reader
- 其余 reader 返回 empty reader
- 让现有单 CN pipeline 平稳复用

推荐优先级是：

1. planner/compile 层直接把 foreign scan 本地 DOP 限制到 1
2. 若暂时无法完全统一控制，再由 `BuildReaders()` 保底返回“1 真 + N-1 空”

## 7.4 `foreignReader`

reader 的职责是：

- 流式拉取远端数据
- 做类型映射
- 组装 batch
- 明确上抛错误

推荐约束：

- 不要一次性全量拉取
- 支持 fetch size
- 支持 timeout / cancellation
- trace 和 metrics 要从首版开始就保留

同时建议把“表达式翻译”和“远端 SQL 生成”尽量收敛在 reader 之上的 pushdown 规划层，不要把 connector 抽象设计得过重。首版 connector 面向的输入建议尽量简单，最好是已经完成翻译后的远端 SQL 请求，而不是在 connector 层再暴露一套复杂的结构化表达式协议。

### 7.5 Oracle / MySQL 方言适配要求

如果 Oracle 和 MySQL 都是目标客户源，那么方案中必须把“方言 adapter”提升为一等公民，而不是简单把 connector 看成一个统一黑盒。

推荐最少拆成：

- `mysql dialect adapter`
- `oracle dialect adapter`

各自至少负责：

- metadata discovery SQL
- identifier quoting / 名称大小写规范
- row count / stats 获取 SQL
- test query
- 类型映射
- predicate / limit / order 的语法翻译

其中 MySQL 重点关注：

- `information_schema` 元数据
- `LIMIT` 语法
- unsigned / decimal / text / datetime 映射
- lower-case metadata 行为

其中 Oracle 重点关注：

- `ALL_TABLES` / `ALL_TAB_COLUMNS` 等元数据视图
- `SELECT 1 FROM dual` 测试语句
- `NUMBER / DATE / TIMESTAMP WITH TIME ZONE / CLOB`
- 大小写与 quoted identifier
- 空字符串等于 `NULL` 的语义差异
- `ROWNUM` / `FETCH FIRST` 语法差异

所以对客户支持而言，**Oracle 不能仅仅视为“再加一个 driver”**，而应视为“同一 external catalog 框架下的高差异方言适配器”。

### 7.6 Schema drift 与 refresh 策略

当前方案再补强一点会更稳：**首版 imported foreign table 应以“导入时固定 schema”为准，远端 schema 漂移不自动跟随。**

推荐策略：

- `CREATE FOREIGN TABLE ... FROM CATALOG ...` 或 `IMPORT FOREIGN SCHEMA` 时，把远端列定义固化进本地 `mo_tables/mo_columns`
- 查询时默认按本地 schema 解释结果
- 如果远端 schema 发生变化，由用户显式执行 refresh/import 操作修正

推荐新增或预留的控制语句：

- `REFRESH EXTERNAL CATALOG`
- `REFRESH FOREIGN TABLE`

V1 语义建议直接定义清楚：

- `REFRESH` 首版按全量重刷理解，不做增量 refresh
- 已经开始执行的查询继续沿用其已绑定的本地 schema
- refresh 成功后的新查询使用新 metadata
- 如果远端对象已不存在，则对应 imported foreign table 应进入 invalid 状态并在查询时报错

而首版不建议做：

- 自动 schema 同步
- 查询时隐式重载远端 schema
- 静默接受不兼容 schema 漂移

原因是对 Oracle/MySQL 而言，schema drift 很容易把类型映射、null 语义和列顺序兼容性搞乱。首版显式 refresh 比自动跟随更安全。

## 8. 统计信息与代价模型

首版 `foreignRelation.Stats()` 推荐采用启发式策略：

- 优先读 metadata cache
- 若远端能返回粗粒度 row count / size，则尽量利用
- 拿不到时给默认值

不要把首版目标定成“代价模型足够聪明”，因为当前 `stats.go` 明显围绕本地 block/object 设计。

后续可增强：

- `ANALYZE FOREIGN TABLE`
- stats TTL cache
- 远端 explain 采样
- 索引信息利用

## 9. 安全、权限与可观测性

### 权限建议

建议拆成两类权限：

- `USAGE ON CONNECTION`
- `SELECT ON FOREIGN TABLE`

这样可以避免“谁能查表就一定能拿到连接敏感信息”的问题。

### 凭据建议

- catalog 中密文存储
- CN 侧解密后使用
- `SHOW CREATE CONNECTION` 脱敏
- 日志、错误、trace 一律不打印 secret

### 错误体系建议

联邦查询新增错误不建议绕开 MatrixOne 现有错误码体系，建议对齐 `pkg/common/moerr` 的扩展方式，至少为以下场景预留明确分类：

- 远端连接失败
- 远端认证失败
- 远端查询失败
- 远端超时
- connector 或 pushdown 不支持

即使首版暂时先用已有 error wrapper，也应在设计上明确这些错误需要映射到可观测、可分类的 MO 错误体系中，而不是简单透传原始远端驱动错误字符串。

### 可观测性建议

首版就应增加：

- connection open latency
- remote query latency
- rows fetched
- bytes fetched
- pushdown hit ratio
- residual filter ratio
- timeout / error count

`EXPLAIN` 也建议显示：

- foreign scan
- connector type
- pushed predicates
- local residual predicates
- `ForceOneCN`

## 10. 首版范围与明确不做的事

### 跨源 JOIN / AGG 语义边界

这一点需要额外写清楚，避免后续理解偏差：

- **首版支持本地执行的跨源 JOIN/AGG**
- 这里的“支持”指的是：foreign scan 先把数据作为普通 batch 流送入 MO 执行器，再由 MO 本地 join/agg 算子处理
- **首版不支持 join pushdown / agg pushdown**
- **首版不提供跨源统一 snapshot**

因此像下面这类 SQL，在设计上是允许的：

```sql
SELECT l.id, f.amount
FROM local_table l
JOIN foreign_table f
  ON l.id = f.id;
```

但其语义边界是：

- join 在 MO 本地完成
- 远端仅承担 scan 和有限 pushdown
- 最终一致性语义由“本地表快照 + 远端 statement 级读取”共同决定
- 性能上应优先依赖 foreign 侧强过滤，而不是指望大结果集跨源 join

### 首版能力矩阵

| 能力 | 首版状态 | 说明 |
|------|----------|------|
| foreign 单表 `SELECT` | 支持 | 走 `TABLE_SCAN` 主路径 |
| local + foreign JOIN | 支持 | 仅本地执行，不做 join pushdown |
| `AGG` over foreign table | 支持 | 仅本地聚合 |
| foreign + foreign JOIN | 谨慎支持 | 语义上可本地执行，但不作为首版主打能力 |
| foreign 表写入 | 不支持 | 首版只读 |
| 多 CN foreign scan | 不支持 | 首版 `ForceOneCN` |
| 统一 snapshot / 2PC | 不支持 | 明确排除 |

### 首版要做

- 只读联邦查询
- `CREATE CONNECTION`
- `CREATE EXTERNAL CATALOG`
- `CREATE FOREIGN TABLE ... FROM CATALOG ...` 或 `IMPORT FOREIGN SCHEMA`
- `TABLE_SCAN` 主路径接入
- MySQL 方言 adapter
- PostgreSQL 纳入设计范围，用于校验 catalog/schema/type/session 抽象
- Oracle 方言 adapter 设计预留，且作为首批目标源之一
- projection + basic predicate pushdown
- MO 侧 residual filter
- 单 CN 执行

### 首版不做

这里的“首版”指**保守的最小可落地版本**，主要用于控制第一轮架构接入风险；不等于最终推荐的一期交付范围。若要采用更积极的一期规划，请参考 `doc/fq/federated_query_scope_replan.md`。

- `INSERT/UPDATE/DELETE` foreign table
- 跨源事务
- 统一 snapshot
- 多 CN foreign scan
- join pushdown
- agg pushdown
- 完整函数 pushdown
- 自动 schema 同步
- direct `catalog.schema.table` 原生执行（可作为第二阶段目标）

## 11. 风险判断

最大的风险不是“连不上远端”，而是范围失控。尤其要避免以下几类过早扩张：

1. 一版就做多 CN
2. 一版就做写入
3. 一版就做复杂 pushdown
4. 一版就做跨源事务一致性

这些方向都不是当前代码结构下最容易取得正收益的路径。

## 12. 最终结论

基于当前 MatrixOne 代码结构，联邦查询最合理的正式方案是：

- 把架构升级为 **`Connection + External Catalog + Imported Foreign Table`**
- 外部 catalog 负责 Oracle/MySQL/PostgreSQL 的 metadata discovery、连接池、名字映射、方言规则和连接测试
- imported foreign table 继续走 `TABLE_SCAN` 主路径
- 在 disttae relation 解析处返回新的 `foreignRelation`
- 通过 `foreignReader + dialect adapter` 接远端 connector
- 首版强制 `ForceOneCN`
- 首版只读，只做基础 pushdown，不做跨源事务一致性

一句话总结：

> 对 Oracle/MySQL 这类客户源，联邦查询不应只做单表 foreign table，而应演进为“catalog-first、执行仍走 TABLE_SCAN”的双层体系。
