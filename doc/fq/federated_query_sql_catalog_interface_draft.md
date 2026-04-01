# MatrixOne 联邦查询首批 SQL / Catalog / 接口草案

## 1. 文档目标

本文是在 `doc/fq/federated_query_design.md` 和 `doc/fq/federated_query_implementation_plan.md` 基础上的第三份实战草案文档，重点回答三类问题：

如果要看“保守 MVP 之外，哪些能力可以拉进更完整的一期范围”，请继续看 `doc/fq/federated_query_scope_replan.md`。

如果要看 PostgreSQL 的特殊设计点，请继续看 `doc/fq/federated_query_postgresql_addendum.md`。

- MySQL / PostgreSQL / Oracle 首批支持时，SQL 面应该长什么样
- `CONNECTION + EXTERNAL CATALOG + IMPORTED FOREIGN TABLE` 的 catalog 元数据该怎么落
- 关键 Go 接口应该先怎么拆，才能尽快做出可跑的 V1 主骨架

本文不再重复解释“为什么要做联邦查询”，而是直接给出首批可落地的设计拆分。

## 2. 目标范围

本文覆盖冻结后的 V1 首批目标范围：

- 目标源：`MySQL`、`PostgreSQL`、`Oracle`
- 查询类型：`SELECT`
- 执行形态：单 CN、`TABLE_SCAN` 主路径
- pushdown 范围：projection + basic predicate
- 控制面：`CONNECTION + EXTERNAL CATALOG`
- 执行面：`CREATE FOREIGN TABLE ... FROM CATALOG ...` 或 `IMPORT FOREIGN SCHEMA`

本文明确不覆盖：

- 远端写入
- 多 CN foreign scan
- direct `catalog.schema.table` 原生执行
- join/agg pushdown
- 跨源统一 snapshot

## 3. 总体对象模型

### 3.1 三类对象

推荐把首批联邦查询对象模型稳定在三层：

1. `CONNECTION`
2. `EXTERNAL CATALOG`
3. `FOREIGN TABLE`（建议作为 import 后的本地执行对象）

职责划分如下：

| 对象 | 作用 | 是否含 secret | 是否直接参与 scan |
|------|------|---------------|-------------------|
| `CONNECTION` | 保存连接、认证、TLS、超时等信息 | 是 | 否 |
| `EXTERNAL CATALOG` | 保存方言类型、metadata discovery 规则、名字映射、cache、pool 策略 | 否（引用 connection） | 否 |
| `FOREIGN TABLE` | 表示一个被本地导入/固定化后的远端表映射 | 否（引用 catalog） | 是 |

### 3.2 为什么要三层而不是两层

如果只有 `CONNECTION + FOREIGN TABLE`：

- 一个 MySQL 实例要接几十张表时，连接和 discovery 规则会散落在每张表上
- Oracle/MySQL 的大小写、名字映射、metadata cache、连接池策略都没有稳定挂载点
- 后面做 schema import 会很 awkward

而 `EXTERNAL CATALOG` 正好承接这些控制面能力。

## 4. SQL 设计草案

## 4.1 `CREATE CONNECTION`

### MySQL 示例

```sql
CREATE CONNECTION conn_mysql_sales
TYPE = 'mysql'
OPTIONS (
  host = '10.0.0.10',
  port = '3306',
  user = 'report_user',
  password = '***',
  ssl_mode = 'preferred',
  connect_timeout = '5s',
  query_timeout = '60s'
);
```

### Oracle 示例

```sql
CREATE CONNECTION conn_oracle_hr
TYPE = 'oracle'
OPTIONS (
  host = '10.0.0.20',
  port = '1521',
  service_name = 'orclpdb1',
  user = 'HR',
  password = '***',
  connect_timeout = '5s',
  query_timeout = '60s'
);
```

### 建议首版支持的通用字段

| 字段 | 必须 | 说明 |
|------|------|------|
| `type` | 是 | `mysql` / `oracle` |
| `host` | 是 | 远端地址 |
| `port` | 是 | 远端端口 |
| `user` | 是 | 认证用户 |
| `password` | 是 | 认证密码，密文存储 |
| `connect_timeout` | 否 | 建连超时 |
| `query_timeout` | 否 | 查询超时 |
| `ssl_mode` | 否 | MySQL 等源可用 |
| `service_name` | Oracle 必须 | Oracle service / SID 二选一 |
| `sid` | Oracle 可选 | Oracle 兼容模式 |

### 非首版但建议预留

- `session_init_sql`
- `proxy_url`
- `wallet_path`
- `charset`
- `role`

## 4.2 `CREATE EXTERNAL CATALOG`

### MySQL 示例

```sql
CREATE EXTERNAL CATALOG mysql_sales
USING CONNECTION conn_mysql_sales
TYPE = 'mysql'
OPTIONS (
  include_databases = 'sales,dim',
  metadata_cache_ttl = '300s',
  lower_case_meta_names = 'true',
  connection_pool_min_size = '1',
  connection_pool_max_size = '8',
  test_connection = 'true'
);
```

### Oracle 示例

```sql
CREATE EXTERNAL CATALOG oracle_hr
USING CONNECTION conn_oracle_hr
TYPE = 'oracle'
OPTIONS (
  include_schemas = 'HR,CRM',
  metadata_cache_ttl = '300s',
  quoted_identifier = 'auto',
  connection_pool_min_size = '1',
  connection_pool_max_size = '8',
  test_connection = 'true'
);
```

### 建议首版支持的 catalog 级字段

| 字段 | MySQL | Oracle | 说明 |
|------|-------|--------|------|
| `type` | 是 | 是 | 源类型，必须与 connection 匹配 |
| `using connection` | 是 | 是 | 引用 connection |
| `include_databases` | 是 | 否 | MySQL database 白名单 |
| `include_schemas` | 否 | 是 | Oracle schema 白名单 |
| `exclude_databases/schemas` | 可选 | 可选 | 黑名单 |
| `metadata_cache_ttl` | 是 | 是 | metadata cache TTL |
| `lower_case_meta_names` | 推荐 | 否 | MySQL 更常用 |
| `quoted_identifier` | 否 | 推荐 | Oracle 大小写/引号策略 |
| `test_connection` | 推荐 | 推荐 | 创建时主动校验 |
| `connection_pool_min_size` | 推荐 | 推荐 | 连接池参数 |
| `connection_pool_max_size` | 推荐 | 推荐 | 连接池参数 |

### Catalog 级能力建议

`EXTERNAL CATALOG` 首版至少承接：

- metadata discovery
- table/schema include/exclude
- identifier mapping
- metadata cache
- test connection
- connection pool config
- dialect type

## 4.3 `CREATE FOREIGN TABLE ... FROM CATALOG ...`

### 方式一：单表导入

```sql
CREATE FOREIGN TABLE orders_ext
FROM CATALOG mysql_sales.sales.orders;
```

Oracle 示例：

```sql
CREATE FOREIGN TABLE employees_ext
FROM CATALOG oracle_hr.HR.EMPLOYEES;
```

这种语法适合：

- 只接少量表
- 想手工控制本地表名
- 想逐张导入

### 方式二：带本地 schema 覆盖

```sql
CREATE FOREIGN TABLE orders_ext (
  order_id bigint,
  customer_id bigint,
  amount decimal(18,2),
  created_at datetime
)
FROM CATALOG mysql_sales.sales.orders
OPTIONS (
  pushdown = 'basic',
  fetch_size = '4096'
);
```

这种语法适合：

- 希望本地 schema 固定化
- 需要显式类型覆写
- 需要约束后续行为

## 4.4 `IMPORT FOREIGN SCHEMA`

### MySQL 示例

```sql
IMPORT FOREIGN SCHEMA sales
FROM CATALOG mysql_sales
INTO ext_sales;
```

### Oracle 示例

```sql
IMPORT FOREIGN SCHEMA HR
FROM CATALOG oracle_hr
INTO ext_hr;
```

建议首版行为：

- 读取远端 schema 内所有候选表
- 在本地目标 db/schema 下生成 imported foreign tables
- 每张 imported table 仍进入 `mo_tables/mo_columns`
- 不做持续自动同步，仅做一次 import

建议进一步明确两种语法的边界：

| 语法 | 建议定位 |
|------|----------|
| `CREATE FOREIGN TABLE ... FROM CATALOG ...` | 单表导入的标准语法，支持本地表名控制、列覆写、局部属性定制 |
| `IMPORT FOREIGN SCHEMA` | 批量导入语法，本质上是复用单表导入管线的批量简写 |

首版建议：

- `IMPORT FOREIGN SCHEMA` 必须显式指定 `INTO target_schema`
- `IMPORT FOREIGN SCHEMA` 只做全量导入，不做增量同步
- 与本地同名表冲突时直接报错，不隐式覆盖

### 为什么首版建议“一次导入”，而不是持续 auto-sync

因为持续 schema 同步会引入：

- ddl 监听
- schema drift 冲突处理
- 权限变化传播
- 本地类型覆写冲突

这些都不适合 V1 首阶段。

## 4.5 推荐补充语句

首版建议同步补上：

- `SHOW CREATE CONNECTION`
- `SHOW CREATE CATALOG`
- `SHOW CREATE TABLE <foreign table>`
- `DROP CONNECTION`
- `DROP EXTERNAL CATALOG`

同属 V1 范围，建议在控制面阶段一并规划：

- `ALTER CONNECTION`
- `ALTER EXTERNAL CATALOG`
- `REFRESH EXTERNAL CATALOG`
- `REFRESH FOREIGN TABLE`

## 4.6 Schema drift 策略

首版建议明确采用**本地 schema 固化 + 显式 refresh**模型：

- 导入时读取远端 schema，并写入本地 `mo_tables/mo_columns`
- 后续查询按本地固化 schema 执行
- 远端 schema 变化后，不自动影响本地 imported foreign table
- 用户需要显式执行 `REFRESH FOREIGN TABLE` 或重新 `IMPORT FOREIGN SCHEMA`

推荐理由：

- Oracle/MySQL 的类型和大小写差异较大
- 自动跟随远端 schema 很容易引入静默行为变化
- 本地 schema 固化更符合 MO 当前 `TABLE_SCAN` 主路径的稳定性需求

首版推荐的异常策略：

- 如果远端缺列/类型明显不兼容，查询失败并给出可 refresh 的提示
- 不允许静默重映射到不同列含义
- 即使是“看起来兼容”的类型变化，也先按 schema drift 处理，不做静默兼容
- 例如 `int -> bigint`、`varchar` 长度扩大、`decimal` 精度/scale 变化、`timestamp` 相关 flavor 变化，都要求显式 `REFRESH`
- `REFRESH` 后如果新类型仍能稳定映射到 MO 类型，则更新 imported schema；否则进入 invalid / 显式报错路径

## 4.7 `REFRESH` 的 V1 语义

V1 已引入 `REFRESH EXTERNAL CATALOG` / `REFRESH FOREIGN TABLE`，建议把行为直接定义清楚：

- 首版 `REFRESH` 统一按**全量重刷**理解，不做增量 refresh
- refresh 只更新 metadata / imported schema，不隐式修改用户 SQL
- refresh 期间应序列化同一 catalog / foreign table 的并发 refresh
- 已经开始执行的查询继续沿用其已绑定的本地 schema 解释结果
- refresh 成功后的新查询使用新 metadata
- 如果远端对象已不存在，则将对应 imported foreign table 标记为 invalid，并在查询时报错提示修复

## 5. Catalog 元数据设计草案

## 5.1 `mo_connections`

建议系统表：

```sql
mo_catalog.mo_connections (
  connection_id          bigint unsigned primary key,
  connection_name        varchar(256) unique,
  connection_type        varchar(32),
  connection_options     text,
  connection_status      varchar(32),
  owner                  bigint unsigned,
  creator                bigint unsigned,
  account_id             bigint unsigned,
  created_time           timestamp,
  comment                text
)
```

### 设计说明

- `connection_options` 存密文或受保护配置
- `connection_status` 可记录 active / invalid / disabled
- secret 不应进入 `SHOW CREATE` 明文

## 5.2 `mo_external_catalogs`

建议系统表：

```sql
mo_catalog.mo_external_catalogs (
  catalog_id             bigint unsigned primary key,
  catalog_name           varchar(256) unique,
  catalog_type           varchar(32),
  connection_id          bigint unsigned,
  catalog_options        text,
  metadata_cache_ttl     varchar(64),
  catalog_status         varchar(32),
  owner                  bigint unsigned,
  creator                bigint unsigned,
  account_id             bigint unsigned,
  created_time           timestamp,
  comment                text
)
```

### 设计说明

- `catalog_options` 保存 include/exclude/name mapping/pool/test_connection 等
- `connection_id` 指向 `mo_connections`
- catalog 与 connection 是 1:N 复用关系

## 5.3 imported foreign table 在现有 catalog 中的表达

首版建议 imported foreign table 不单独建新系统表，而是继续复用：

- `mo_tables`
- `mo_columns`

在 `mo_tables` 中：

- `relkind = SystemForeignRel`
- `rel_createsql` 或 properties 中记录 foreign 元信息

建议至少保存以下字段：

- `foreign_catalog_id`
- `foreign_catalog_name`
- `foreign_source_type`
- `remote_schema_name`
- `remote_table_name`
- `import_mode`
- `pushdown_policy`
- `fetch_size`

## 5.4 是否需要 `mo_foreign_table_mappings`

首版建议：**不新增**。

原因：

- imported foreign table 已经有 `mo_tables` 主记录
- 补充 properties 即可表达大多数映射关系
- 先减少 catalog 复杂度

如果未来要支持：

- schema import 差量刷新
- 多版本映射
- 复杂 column remap

再考虑单独 mapping 表更合适。

## 6. 关键 Go 接口草案

## 6.1 Connection 层

```go
type ConnectionConfig struct {
    Name           string
    Type           string
    Host           string
    Port           int
    User           string
    Password       string
    ConnectTimeout time.Duration
    QueryTimeout   time.Duration
    Extra          map[string]string
}
```

### 建议职责

- 从 `mo_connections` 读取配置
- 在运行时解密 secret
- 向 catalog manager / connector 提供统一配置对象

## 6.2 External Catalog 层

```go
type Capabilities struct {
    ProjectionPushdown bool
    PredicatePushdown  bool
    LimitPushdown      bool
    OrderByPushdown    bool
}

type ExternalCatalog interface {
    Name() string
    Type() string
    Capabilities() Capabilities
    TestConnection(ctx context.Context) error
    ListNamespaces(ctx context.Context) ([]string, error)
    ListTables(ctx context.Context, namespace string) ([]string, error)
    GetTableSchema(ctx context.Context, namespace, table string) ([]ColumnMeta, error)
    GetTableStats(ctx context.Context, namespace, table string) (*RemoteStats, error)
    NewSession(ctx context.Context) (Session, error)
}
```

### 建议职责

- metadata discovery
- metadata cache
- identifier mapping
- connection pool 与 session 生命周期管理
- 调用 dialect adapter 生成元数据 SQL

这里的 `namespace` 是接口层的统一叫法：

- MySQL 中通常对应 remote database
- PostgreSQL 中通常对应 remote schema（catalog 已绑定一个 database）
- Oracle 中通常对应 remote schema

并建议进一步冻结以下运行时边界：

- connection pool 的生命周期以 `ExternalCatalog` 为边界，而不是 per query 临时建池
- `NewSession(ctx)` 返回的是一次查询执行阶段使用的 session 句柄，由 catalog 统一完成建连、借还与必要的 session 初始化
- `Session.Close()` 必须负责归还连接到 pool 或关闭底层连接，不能依赖 GC
- `RowStream.Close()` / `foreignReader.Close()` 必须最终传播到 `Session.Close()`，避免连接泄漏

## 6.3 Dialect Adapter 层

```go
type PushdownStatus int

const (
    PushdownNotAvailable PushdownStatus = iota
    PushdownAvailable
)

type DialectAdapter interface {
    Name() string
    Capabilities() Capabilities
    BuildTestQuery() string
    BuildListSchemasSQL(opts CatalogOptions) string
    BuildListTablesSQL(schema string, opts CatalogOptions) string
    BuildDescribeTableSQL(schema, table string) string
    BuildRowCountSQL(schema, table string) string
    QuoteIdent(name string) string
    NormalizeRemoteName(name string) string
    TranslatePredicate(expr *plan.Expr, ctx TranslateContext) (sql string, status PushdownStatus, err error)
    TranslateLimit(limit uint64, hasResidual bool) (sql string, ok bool)
    MapRemoteType(col RemoteColumnMeta) (types.Type, error)
}
```

### 为什么 dialect adapter 是一等公民

对于 MySQL 和 Oracle，真正难的不是“建一个连接”，而是：

- metadata SQL 不同
- limit/order 语法不同
- 大小写和 identifier 规则不同
- 类型映射不同
- null/空字符串语义不同

所以 dialect adapter 必须一开始就是主角色。

这里建议显式区分三类情况：

- `status = PushdownAvailable, err = nil`：可以安全下推
- `status = PushdownNotAvailable, err = nil`：不能下推，回退到 MO 本地 residual filter
- `err != nil`：翻译逻辑失败或语义不明确，需要显式报错，而不是伪装成“不能下推”

## 6.4 Connector 执行层

```go
type QueryRequest struct {
    SQL       string
    FetchSize int
    Timeout   time.Duration
}

type Session interface {
    Query(ctx context.Context, req QueryRequest) (RowStream, error)
    Close() error
}

type RowStream interface {
    Next(ctx context.Context) (RowBatch, error)
    Close() error
}
```

### 设计原则

- connector 接口尽量简单
- projection/filter/order 的复杂决策尽量在 MO 侧完成
- session state 的初始化与清理由 `NewSession()` / `Session` 内部管理，不暴露给 planner
- 查询中断、reader close、stream close 都必须落到统一的资源回收路径
- connector 只负责执行远端 SQL 并返回流式结果

### 6.4.1 `test_connection` 与连接池位置

建议把这件事拆清楚：

- `test_connection`：用于创建或刷新 external catalog 时验证驱动可用、认证正确、远端可达
- 连接池：建议由实际执行查询的 CN 本地持有

原因是：

- 真正消耗远端连接的是查询执行 CN
- 即使控制面能连通，也不等于执行路径一定可用
- 未来如果扩展到多 CN，需要按执行节点治理连接池和驱动生命周期

因此首版虽然仍是单 CN，但对象模型上应默认“连接池归执行侧所有”。

## 6.5 执行侧对象

```go
type FederatedRelData struct {
    CatalogName  string
    RemoteSchema string
    RemoteTable  string
}

type foreignRelation struct {
    // imported table metadata
}

type foreignReader struct {
    // stream remote rows and fill MO batch
}
```

### 设计原则

- `FederatedRelData` 首版只服务于单 CN、本地 reader 构建
- 不要求首版支持 remoterun 反序列化
- future 多 CN 再扩展远端 payload
- 但建议在 `readutil/relation_data.go` 一类注册点为未来 `FederatedRelData` 的远端序列化预留明确扩展位

## 7. MySQL / PostgreSQL / Oracle 差异清单

| 项目 | MySQL | PostgreSQL | Oracle | 首版建议 |
|------|-------|------------|--------|----------|
| metadata schema | `information_schema` | `pg_catalog + information_schema` | `ALL_TABLES/ALL_TAB_COLUMNS` | dialect adapter 分开实现 |
| test query | `SELECT 1` | `SELECT 1` | `SELECT 1 FROM dual` | 必须分开 |
| catalog 粒度 | 常可覆盖多 database | 建议 1 catalog = 1 database | 常可覆盖一个 service 下多 schema | source-specific external catalog 语义 |
| limit | `LIMIT n` | `LIMIT n` / `OFFSET` | `FETCH FIRST n ROWS ONLY` / `ROWNUM` | adapter 控制 |
| identifier | 常见 lower-case | unquoted lower-case, quoted-sensitive | 常见 upper-case + quoted-sensitive | adapter 提供 normalize/quote |
| row count | `information_schema.tables` / statistics | `pg_class.reltuples` / stats | `ALL_TABLES.NUM_ROWS` | 允许不准，先启发式 |
| string/null semantics | 空字符串不等于 null | 标准 SQL 风格 | 空字符串会陷入 null 语义坑 | Oracle 单独重点防护 |
| numeric | `decimal`, unsigned | `numeric`, serial 家族 | `NUMBER` 灵活 | 分源映射 |
| timestamp | `datetime/timestamp` | `timestamp` / `timestamptz` | `DATE`, `TIMESTAMP`, `TIMESTAMP WITH TZ` | 分源映射 |
| 扩展类型 | `json`, `bit`, `enum/set` | `jsonb`, `uuid`, `bytea`, array | `CLOB`, `RAW`, `NUMBER` 变体 | 保守支持，超出范围显式 reject |

### 7.1 MySQL 首版建议

优先支持：

- metadata discovery
- `LIMIT`
- basic predicate pushdown
- decimal/varchar/datetime/int
- lower_case metadata handling

### 7.2 PostgreSQL 首版建议

优先支持：

- 一个 database 内的 schema discovery
- `LIMIT` / basic predicate / simple `ORDER BY`
- `numeric/boolean/text/timestamp/timestamptz/uuid/bytea`
- `search_path` 与 identifier 规则显式化

并建议：

- array / enum / domain / interval 首版保守处理
- `ILIKE` / regex / `NULLS FIRST/LAST` 通过 capability 控制

### 7.3 Oracle 首版建议

优先支持：

- metadata discovery
- identifier quoting / 大小写控制
- `NUMBER/DATE/TIMESTAMP WITH TIME ZONE`
- `FETCH FIRST` 或 `ROWNUM`
- 禁止或严格限制有风险的字符串/null 类下推

尤其建议：

- Oracle 首版默认比 MySQL 更保守
- 能本地算的先本地算
- 先保正确，再谈多 pushdown

## 7.4 多方言混合查询边界

如果一条查询同时涉及：

- MySQL foreign table
- PostgreSQL foreign table
- Oracle foreign table

则首版建议直接定义为：

- 各 foreign scan 只做各自安全的单表 pushdown
- JOIN / AGG / residual filter 统一回到 MO 本地执行器
- query timeout 以整个 MO 查询为准，而不是分别依赖各源默认超时
- 不承诺不同方言之间存在统一 snapshot

## 8. 首批里程碑建议

## M1：控制面跑通

交付：

- `CREATE CONNECTION`
- `CREATE EXTERNAL CATALOG`
- `SHOW CREATE CONNECTION`
- `SHOW CREATE CATALOG`
- metadata test connection

## M2：MySQL 全链路 V1 子里程碑

交付：

- `CREATE FOREIGN TABLE ... FROM CATALOG ...`
- `IMPORT FOREIGN SCHEMA`
- MySQL metadata discovery
- MySQL single-table query
- projection/basic predicate pushdown

## M3：PostgreSQL 方言 V1 子里程碑

交付：

- PostgreSQL metadata discovery
- PostgreSQL catalog 粒度验证
- PostgreSQL type mapping
- PostgreSQL basic query 与保守 pushdown

## M4：Oracle 方言 V1 子里程碑

交付：

- Oracle metadata discovery
- Oracle type mapping
- Oracle basic query
- Oracle 保守 pushdown

## M5：治理增强

交付：

- explain foreign details
- stats cache
- timeout / cancel / metrics
- 权限与脱敏完善

## 9. 推荐首批文件落点

### SQL / AST / Plan

- `pkg/sql/parsers/tree/`
- `pkg/sql/parsers/dialect/mysql/`
- `pkg/sql/plan/build.go`
- `pkg/sql/plan/build_ddl.go`

### Catalog / Frontend

- `pkg/catalog/types.go`
- `pkg/frontend/predefined.go`
- `pkg/frontend/`

### External catalog / dialect / execution

- `pkg/vm/engine/federated/catalog.go`
- `pkg/vm/engine/federated/connection.go`
- `pkg/vm/engine/federated/dialect_mysql.go`
- `pkg/vm/engine/federated/dialect_oracle.go`
- `pkg/vm/engine/federated/relation.go`
- `pkg/vm/engine/federated/reader.go`
- `pkg/vm/engine/disttae/txn_database.go`

## 10. 最后结论

如果目标客户明确包含 `Oracle + MySQL`，那么比“直接做单表 foreign table”更好的拆分是：

- **控制面先做 `CONNECTION + EXTERNAL CATALOG`**
- **执行面先做 imported foreign table，并继续复用 `TABLE_SCAN`**
- **MySQL 先打通链路，Oracle 在同一框架下补方言 adapter**

一句话总结：

> 对 Oracle/MySQL 首批支持而言，最优拆分不是“一个通用 driver + 一张 foreign table”，而是“catalog-first 的控制面 + imported table 的执行面 + 数据源方言 adapter”。 
