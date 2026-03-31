# MatrixOne 联邦查询实施拆解文档

> 如果下一步要把方案直接转成“按代码模块推进”的落地清单，请继续看 `doc/fq/federated_query_code_checklist.md`。
>
> 如果要把 V1 checklist 继续拆到“可直接派活的代码任务包”，请继续看 `doc/fq/federated_query_v1_impl_task_breakdown.md`。
>
> 如果要把当前保守 MVP 口径扩大成更完整的一期交付范围，请继续看 `doc/fq/federated_query_scope_replan.md`。
>
> 如果要把 PostgreSQL 也纳入一期设计，请继续看 `doc/fq/federated_query_postgresql_addendum.md`。
>
> 如果要直接看冻结后的 V1 最终决策版，请继续看 `doc/fq/federated_query_v1_final_design.md` 与 `doc/fq/federated_query_v1_checklist.md`。

## 1. 文档目的

本文是 `doc/fq/federated_query_design.md` 的配套实施文档，目标不是再解释“为什么这么做”，而是直接回答：

- 代码应该从哪里开始改
- 哪些模块先做，哪些模块后做
- 每个阶段的交付物是什么
- 每个阶段的风险点和验证方法是什么

本文以“首版做一个可落地的只读单 CN 联邦查询 MVP”为目标。更细的 Oracle/MySQL SQL 设计、catalog 元数据与关键接口草案见 `doc/fq/federated_query_sql_catalog_interface_draft.md`。

## 2. 实施总原则

首版联邦查询应遵循以下工程原则：

1. **主路径优先**：尽量接入 `TABLE_SCAN -> Relation -> Reader` 主链路
2. **单 CN 优先**：第一阶段固定 `ForceOneCN`
3. **只读优先**：不做 foreign table 写入
4. **正确性优先**：不会等价转换的表达式一律不下推
5. **catalog-first 优先**：控制面优先建 `CONNECTION + EXTERNAL CATALOG`
6. **执行面复用**：首版通过 imported foreign table 继续走现有 `TABLE_SCAN`
7. **最小 connector 面**：connector 接口尽量简单，方言复杂性留在 MO 侧 planner/adapter
8. **元数据解耦**：连接对象、catalog 对象、foreign table 对象分层

## 3. 推荐阶段划分

## 阶段 0：设计冻结与范围收敛

目标：

- 冻结首版范围
- 明确不做的能力
- 确认 foreign table 走 `TABLE_SCAN`

输出物：

- 本文档
- `doc/fq/federated_query_design.md`

建议冻结项：

- 首版仅支持 `SELECT`
- 首版强制单 CN
- 首版不做跨源事务一致性
- 首版控制面目标调整为 `CONNECTION + EXTERNAL CATALOG`
- 首版执行面先走 `CREATE FOREIGN TABLE ... FROM CATALOG ...` 或 `IMPORT FOREIGN SCHEMA`
- `IMPORT FOREIGN SCHEMA` 定位为批量导入语法，复用单表导入主管线
- 首批目标源明确包含 `MySQL` 和 `Oracle`

这一步不需要改代码，但非常关键。范围不收敛，后面代码很容易失控。

---

## 阶段 1：控制面元数据与 DDL 骨架

### 3.1 目标

让系统先具备“描述连接、外部 catalog、导入型 foreign table”的能力，而不是一上来写执行器。

### 3.2 推荐改动模块

#### 3.2.1 Parser / AST

建议检查并改造：

- `pkg/sql/parsers/tree/`
- `pkg/sql/parsers/dialect/mysql/`

需要新增或扩展的语法对象：

- `CreateConnection`
- `DropConnection`
- `ShowCreateConnection` 或复用 show 语法扩展
- `CreateExternalCatalog`
- `DropExternalCatalog`
- `ShowCreateCatalog` 或对应变体
- `CreateForeignTableFromCatalog`
- `ImportForeignSchema`

可选策略有两种：

1. 新增独立 AST 类型
2. 复用 `CREATE TABLE` 并扩展 table option / table kind

推荐：

- `CONNECTION` 用独立 AST
- `FOREIGN TABLE` 用独立 AST 或 `CREATE TABLE` 的明确变体

这样 binder/plan 语义更清晰。

#### 3.2.2 Plan / DDL Builder

重点文件：

- `pkg/sql/plan/build.go`
- `pkg/sql/plan/build_ddl.go`

需要新增：

- `buildCreateConnection(...)`
- `buildDropConnection(...)`
- `buildCreateForeignTable(...)`

这里的关键目标不是做运行时，而是把 metadata 正确写进 catalog，并把“catalog-first”控制面先立起来。

同时要尽早冻结 schema drift 策略：

- imported foreign table 首版按导入时 schema 固化
- 不做自动 schema 同步
- 后续通过显式 refresh/import 修正

#### 3.2.3 Catalog 元数据

重点文件：

- `pkg/catalog/types.go`
- `pkg/frontend/predefined.go`

建议改动：

- 在 `pkg/catalog/types.go` 中显式新增 `SystemForeignRel = "f"`
- 如有需要，补充 foreign table 判断 helper，避免后续各处散落字面量判断
- 新增 `mo_catalog.mo_connections` 的建表定义
- 新增 `mo_catalog.mo_external_catalogs`（或等价 catalog object 元数据）定义

建议 `mo_connections` 字段至少包含：

- id
- name
- type
- options
- status
- owner
- created_time
- comment

这里建议 `mo_connections` 的落盘方式直接参照 `mo_stages`：

- 在 `pkg/frontend/predefined.go` 中预定义
- 作为 `mo_catalog` 系统表 bootstrap
- 不通过普通 `CREATE TABLE` 间接创建

#### 3.2.4 Frontend 执行元数据 DDL

可参考 stage / cdc 的已有实现方式，重点关注：

- `pkg/frontend/predefined.go`
- `pkg/frontend/authenticate.go`
- `pkg/frontend/` 中 catalog 相关的 DDL 执行逻辑

如果 `CONNECTION` 最终不走 `CREATE TABLE` 语义，而是独立 catalog object，则需要新增对应的 frontend 执行入口。

推荐直接把它作为独立 catalog object 处理，而不是复用 `CREATE TABLE`。这样后续的权限、脱敏、drop/revoke 逻辑更自然。

对 `EXTERNAL CATALOG` 也建议采用同样策略，而不是伪装成普通 schema/table。

### 3.3 阶段 1 验收标准

- 能成功执行 `CREATE CONNECTION`
- 能成功执行 `DROP CONNECTION`
- 能成功执行 `CREATE EXTERNAL CATALOG`
- 能成功执行 `DROP EXTERNAL CATALOG`
- 能成功执行 `CREATE FOREIGN TABLE ... FROM CATALOG ...` 或 `IMPORT FOREIGN SCHEMA`
- imported foreign table 在 `mo_tables/mo_columns` 中落地
- connection object 在 `mo_connections` 中落地
- external catalog object 在系统 metadata 中落地
- `SHOW CREATE TABLE` / `SHOW CREATE CONNECTION` 能正确展示并脱敏
- `SHOW CREATE CATALOG` 能展示 catalog 级配置但不泄露 secret

---

## 阶段 2：imported foreign table 的 relation 解析与执行骨架

### 4.1 目标

让 imported foreign table 在 compile 和执行过程中真正走到 `TABLE_SCAN` 主路径，而不是停留在 metadata 层。

### 4.2 关键代码落点

#### 4.2.1 disttae relation 解析

核心文件：

- `pkg/vm/engine/disttae/txn_database.go`

当前逻辑是：

- 读取表项
- 创建 `txnTable`

这里需要扩展为：

- 识别 foreign table 的 `relkind`
- 返回 `foreignRelation`

建议不要在 `txnTable` 里塞大量 if/else foreign 分支，而是明确创建一个新实现。

实现拦截点建议固定在：

- `db.relation(...)`
- 且放在 `newTxnTable(...)` 之前

也就是说，先判断表项是否为 foreign table，再决定走 `newForeignRelation(...)` 还是原有 `newTxnTable(...)`。

这里的 foreign table 来源不再局限于“手工定义单表”，而是可以来自：

- `CREATE FOREIGN TABLE ... FROM CATALOG ...`
- `IMPORT FOREIGN SCHEMA`

#### 4.2.2 新 relation 实现

建议新增目录，例如：

- `pkg/vm/engine/federated/`

推荐文件划分：

- `types.go`：公共结构、metadata、capability 定义
- `relation.go`：`foreignRelation`
- `reader.go`：`foreignReader`
- `metadata.go`：表属性 / connection 解析
- `errors.go`：错误定义与包装

如果首版只做一个驱动，后续还可进一步拆成：

- `pkg/vm/engine/federated/mysql/`

### 4.3 `foreignRelation` 首版最小实现集

建议优先实现：

- `GetTableDef`
- `CopyTableDef`
- `GetTableID`
- `GetTableName`
- `GetDBID`
- `Stats`
- `Ranges`
- `BuildReaders`

建议首版统一 `not supported` 的接口：

- `Write`
- `Delete`
- `AddTableDef`
- `DelTableDef`
- `AlterTable`
- `UpdateConstraint`

这些行为在 planner/DDL 层也应提前拦住。

### 4.4 `Ranges()` 首版实现建议

建议非常保守：

- 返回一个逻辑 split
- `DataCnt()` 至少对 compile 友好
- 不尝试伪装成 disttae block/object 细粒度范围

如果确实需要一个明确的实现载体，建议新增**仅限本地 single-CN 使用的最小 `FederatedRelData`**：

- 实现 `engine.RelData`
- 只承载“单个逻辑范围/单个逻辑远端扫描任务”
- 首版不要求被 remoterun 反序列化

如果未来要做 remote CN foreign scan，则再单独扩展：

- `pkg/vm/engine/readutil/relation_data.go`
- `pkg/sql/compile/remoterun.go`
- remote-side reader build path

同时建议现在就把它作为已知扩展点写清楚：

- 在 `readutil/relation_data.go` 预留 `FederatedRelData` 的注册/序列化扩展位
- 避免未来为了 multi-CN foreign scan 再反向修改 `engine.RelData` 形状

### 4.5 `BuildReaders()` 首版实现建议

实现目标：

1. 读取 foreign table 配置
2. 读取 connection 配置
3. 创建 connector session
4. 解析 filter / projection
5. 创建唯一一个真实 reader
6. 其余 reader 返回 empty reader

这样可以与当前 `buildScanParallelRun` 兼容，同时不碰多 CN / remoterun 协议。

更推荐的实现顺序是：

1. 若 planner/compile 框架允许，直接把 foreign scan 的本地 reader 并发限制为 1
2. 作为兼容性保底，`BuildReaders()` 仍按“1 真 + N-1 空”返回

### 4.6 阶段 2 验收标准

- `SELECT * FROM foreign_table` 能进入 `TABLE_SCAN`
- compile 阶段不走 `EXTERNAL_SCAN` / `SOURCE_SCAN`
- 能生成 `foreignReader`
- 能返回基本 batch 数据
- `EXPLAIN` 至少能显示普通 scan 节点并保留 foreign 标识信息

---

## 阶段 3：external catalog manager 与 connector 抽象

### 5.1 目标

把远端访问和 metadata discovery 从 relation/reader 中抽离出来，形成可维护的 external catalog manager + connector 能力。

### 5.2 推荐抽象

建议新增一套简单接口：

```go
type Driver interface {
    Open(ctx context.Context, cfg ConnectionConfig) (Session, error)
    Capabilities() Capabilities
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

此外建议增加 catalog 级管理抽象，例如：

```go
type ExternalCatalog interface {
    ListNamespaces(ctx context.Context) ([]string, error)
    ListTables(ctx context.Context, namespace string) ([]string, error)
    GetTableSchema(ctx context.Context, namespace, table string) ([]ColumnMeta, error)
    TestConnection(ctx context.Context) error
    NewSession(ctx context.Context) (Session, error)
}
```

这样 connection/discovery 与 scan 执行职责不会全部堆在单个 relation 对象里。

这里建议统一用 `namespace` 作为接口层抽象：

- MySQL：通常对应 remote database
- PostgreSQL：通常对应 remote schema
- Oracle：通常对应 remote schema

### 5.2.1 首版 `QueryRequest` 简化原则

为了避免首版 connector 过度设计，建议 connector 层接收的请求尽量简单。推荐思路是：

- projection/filter/order 的可下推判定在 `foreignReader` 或 pushdown planner 完成
- connector 首版只接收已经翻译好的远端 SQL 与少量执行参数

例如：

```go
type QueryRequest struct {
    SQL       string
    FetchSize int
    Timeout   time.Duration
}
```

这样可以让 connector 接口在首版保持稳定，避免把 MO 内部表达式模型直接泄露到驱动层，同时保留最基本的执行调优参数。

### 5.3 能力协商字段建议

`Capabilities` 建议直接定义成结构体，而不是零散的隐式 bool：

```go
type Capabilities struct {
    ProjectionPushdown bool
    PredicatePushdown  bool
    LimitPushdown      bool
    OrderPushdown      bool
    AggPushdown        bool
    JoinPushdown       bool
}
```

首版建议至少覆盖：

- ProjectionPushdown
- PredicatePushdown
- LimitPushdown
- OrderPushdown
- AggPushdown
- JoinPushdown

尽管首版只用前几个能力位，但先把能力边界定义清楚，后续迭代更稳；更细粒度的函数级 capability 可以后续再扩展。

### 5.4 首批方言 adapter 选择

在引入 external catalog 之后，首批更合理的目标不再是“随便挑一个 connector”，而是：

- `mysql dialect adapter`
- `postgresql dialect adapter`
- `oracle dialect adapter`

原因是：

- MySQL 最适合作为第一条打通链路
- PostgreSQL 最适合校验 catalog/schema/session/type 抽象是否足够一般
- Oracle 仍然是高差异方言和客户目标源，必须保留

推荐实施顺序：

1. 先用 MySQL 跑通 catalog discovery + imported table + scan 主路径
2. 再补 PostgreSQL，验证通用关系库抽象
3. 最后补 Oracle 方言 adapter

这样可以在同一架构下同时满足“客户 Oracle 必须支持”和“工程风险可控”两个目标。

### 5.5 reader 与 connector 的职责边界

建议边界如下：

- reader：面向 MO batch 生命周期、上下文取消、投影和结果填充
- connector：面向远端连接、SQL 生成/执行、流式结果读取

不要把 SQL 翻译、连接池、网络错误处理全部堆进 `foreignReader`。

但与此同时，也不要为了“抽象漂亮”把 connector 设计成一套过重的结构化 DSL。首版更适合把复杂性留在 MO 侧，把 connector 稳定在“执行远端 SQL 并返回流式结果”的职责边界上。

### 5.5.1 Doris 给出的可借鉴能力点

参考 Doris 的 `JdbcExternalCatalog` / `JdbcExternalTable`，MatrixOne 的 external catalog 层建议从首版就考虑以下能力位：

- 连接池参数
- `test connection`
- include/exclude database/schema
- identifier mapping
- 函数规则/方言规则
- 不同数据库类型的 row count / metadata SQL

这些能力不一定都要在首版 fully implemented，但应该在对象模型上预留出来，否则后续 Oracle/MySQL 支持会很快失控。

### 5.5.2 `test_connection` 的执行视角

这一点建议在实现计划里明确：`test_connection` 不应仅在“元数据创建点”做校验，而应尽量站在实际查询执行 CN 的视角做验证。

首版建议：

- 创建 `EXTERNAL CATALOG` 时执行一次真实驱动建连测试
- 如果查询执行固定在当前 CN，则该测试即可视为执行路径校验
- 如果未来扩展到多 CN，需要把连接可达性和驱动可用性进一步下沉到 CN 维度治理

### 5.6 阶段 3 验收标准

- 使用真实或 mock connector 可以拿到远端结果
- reader 以流式方式生成 batch
- 连接失败、超时、远端 SQL 错误能明确上抛
- 连接关闭与 reader close 行为正确

---

## 阶段 4：方言适配、pushdown 规划与表达式翻译

### 6.1 目标

在保证正确性的前提下，让 foreign query 不只是“全表拉回再过滤”。

### 6.2 推荐模块

建议在 `pkg/vm/engine/federated/` 下新增：

- `pushdown.go`
- `expr_translate.go`
- `capability.go`
- `dialect_mysql.go`
- `dialect_pg.go`
- `dialect_oracle.go`

### 6.3 实现步骤

#### 第一步：列裁剪

先做 projection pushdown，因为它风险低、收益稳定。

输入来源：

- `TableDef`
- 扫描所需列
- projection list / filter 依赖列

输出：

- remote select column list
- 本地 batch 填充映射

#### 第二步：简单谓词下推

首版仅翻译：

- 比较表达式
- `IN`
- `IS NULL`
- `IS NOT NULL`
- `AND`
- 部分简单 `OR`

对于不能保证等价的表达式：

- 不要尝试降级翻译
- 直接留在本地做 residual filter

这里要特别强调 Oracle 与 MySQL 的差异：

- MySQL 可以自然支持 `LIMIT`
- Oracle 需要 `ROWNUM` / `FETCH FIRST`
- Oracle 的空字符串与 `NULL` 语义差异必须单独防护
- Oracle 标识符和大小写行为必须通过 dialect adapter 统一处理

如果把 PostgreSQL 也纳入一期，则还应明确：

- PostgreSQL 的 `ILIKE` / regex / `NULLS FIRST/LAST` 不能默认下推
- PostgreSQL 的 `search_path` 不应隐式影响 metadata discovery
- PostgreSQL 的 `jsonb/array/enum/domain` 首版应保守处理

#### 第三步：limit（可选）

如果远端 connector 行为明确，可追加简单 limit pushdown。

但如果 filter 存在 residual，或者 order 不能完整下推，则 limit pushdown 必须非常谨慎，避免结果被截断错误。

### 6.4 不建议首版做的 pushdown

- join pushdown
- aggregate pushdown
- window pushdown
- 函数 pushdown
- 排序下推默认开启

### 6.5 阶段 4 验收标准

- 能看见投影列数量明显减少
- 基础过滤条件只在远端执行一次
- 本地 residual filter 与远端 pushdown 结果一致
- 不支持的表达式仍返回正确结果

---

## 阶段 5：external catalog metadata cache、统计信息与优化器接入

### 7.1 目标

让 planner 至少具备“不过度离谱”的 foreign table 代价认知。

### 7.2 推荐实现策略

不要直接深改 `pkg/sql/plan/stats.go` 的主逻辑，而是先在 `foreignRelation.Stats()` 端提供尽可能稳定的启发式统计。

可用信息来源：

- 连接配置中的静态 hints
- external catalog metadata cache
- 远端系统表中的 row count
- connector 的 explain 结果
- 上次采样缓存

同时要与 schema drift 策略协同：

- imported table schema 以本地固化定义为准
- metadata cache 主要服务于 discovery / stats / refresh
- 不在普通查询路径中静默替换本地 schema

### 7.3 建议阶段能力

第一步：

- 给行数和平均行宽保守默认值
- 配合 `ForceOneCN = true`

第二步：

- 加入 TTL cache
- 支持手工 analyze foreign table

第三步：

- 用远端 explain 或统计系统增强 selectivity 估计

### 7.4 阶段 5 验收标准

- planner 不会把 foreign table 当超小表做激进错误选择
- explain 至少能看见基础估算值
- stats 缓存失效和刷新逻辑正常

---

## 阶段 6：安全、权限、catalog 生命周期与连接生命周期

### 8.1 目标

确保 foreign query 不会在“能跑起来”之后留下明显安全隐患。

### 8.2 权限模型

建议至少拆两层：

- `USAGE ON CONNECTION`
- `USAGE ON EXTERNAL CATALOG`
- `SELECT ON FOREIGN TABLE`

推荐检查点：

- 创建 connection 的 owner
- 创建/引用 external catalog 的 owner
- 使用 connection 的授权用户
- foreign table 是否允许跨租户引用 connection

### 8.3 凭据存储和展示

建议：

- catalog 中密文存储
- 运行时解密后短生命周期持有
- `SHOW CREATE CONNECTION` 脱敏
- 日志中绝不打印 password/token/DSN 原文

### 8.4 连接池与资源管理

首版建议先不做复杂全局池，但至少需要：

- session 级或 query 级连接复用
- context cancel 时及时关闭远端结果流
- reader close / pipeline exit 时释放连接

### 8.5 阶段 6 验收标准

- 未授权用户无法使用 connection
- 脱敏输出正确
- query cancel 不会泄露远端连接和 reader 资源

### 8.6 错误码与错误分层

联邦查询相关错误建议从首版开始对齐 `pkg/common/moerr` 体系，而不是直接把驱动层错误原样暴露给 SQL 层。

建议至少区分：

- foreign connection failed
- foreign authentication failed
- foreign query failed
- foreign timeout
- foreign capability not supported

实现上可以先使用现有 moerr wrapper 承载，再根据需要扩展专门错误码，但文档和测试里应当把这些错误场景单独列出来。

---

## 阶段 7：可观测性与 explain 增强

### 9.1 目标

让联邦查询出现慢、错、推不下去时能快速定位。

### 9.2 推荐指标

首版建议增加：

- remote connect latency
- remote query latency
- rows fetched
- bytes fetched
- pushdown predicate count
- residual predicate count
- timeout count
- remote error count

### 9.3 Explain 输出建议

建议在 explain 中体现：

- foreign table 标识
- connector type
- pushed predicates
- local residual predicates
- `ForceOneCN`

### 9.4 阶段 7 验收标准

- 慢查询时能看见远端耗时
- explain 能区分本地过滤和远端 pushdown
- 错误日志中信息足够定位但不泄露 secret

---

## 4. 具体文件级改造建议

这一节给出更直接的代码落点建议。

## 4.1 DDL / Parser / Plan

重点文件：

- `pkg/sql/parsers/tree/`
- `pkg/sql/parsers/dialect/mysql/`
- `pkg/sql/plan/build.go`
- `pkg/sql/plan/build_ddl.go`

建议动作：

- 新增 connection 相关 AST
- 新增 external catalog 相关 AST
- 新增 `CREATE FOREIGN TABLE ... FROM CATALOG ...` / `IMPORT FOREIGN SCHEMA`
- 在 `build.go` 中注册新 DDL 路由
- 在 `build_ddl.go` 中构建 connection / external catalog / imported foreign table plan

## 4.2 Catalog / Frontend

重点文件：

- `pkg/catalog/types.go`
- `pkg/frontend/predefined.go`
- `pkg/frontend/authenticate.go`
- `pkg/frontend/` 中负责 catalog DDL 和权限执行的相关代码

建议动作：

- 新增 `SystemForeignRel`
- 新增 `mo_connections`
- 新增 external catalog object metadata
- 执行 connection DDL
- 执行 external catalog DDL
- 校验 connection / foreign table 权限

## 4.3 Binder / Planner / Explain

重点文件：

- `pkg/frontend/compiler_context.go`
- `pkg/sql/plan/query_builder.go`
- `pkg/sql/plan/opt_misc.go`
- `pkg/sql/plan/stats.go`
- `pkg/sql/plan/explain/`

建议动作：

- imported foreign table 仍绑定为 `TABLE_SCAN`
- 优先在 scan 节点构造阶段设置 `ForceOneCN`，compile 侧仅作为兜底校验
- 给 explain 增加 foreign 标识
- 先接启发式 stats，别一开始深改代价模型

补充建议：

- 第二阶段开始评估如何利用 `TableName.CatalogName` / `ObjectRef.ServerName`
- 为未来 direct `catalog.schema.table` 查询预留 resolver 路由能力

补充建议：

- 如果 planner 在 scan 节点构造时已能识别 foreign table，应直接在那里把 `node.Stats.ForceOneCN = true` 固化
- 避免把 `ForceOneCN` 的正确性完全依赖到 compile 期补救

## 4.4 Engine / Execution

重点文件：

- `pkg/vm/engine/disttae/txn_database.go`
- 新增 `pkg/vm/engine/federated/`

建议动作：

- 在 `txnDatabase.Relation()` 中识别 foreign table
- 返回 `foreignRelation`
- 在新包中实现 relation / reader / connector / dialect / catalog manager 抽象

## 4.5 测试

建议重点覆盖：

- `pkg/sql/plan/`：DDL 和 plan 绑定测试
- `pkg/sql/compile/`：scan 构造和 `ForceOneCN` 测试
- `pkg/frontend/`：权限与 show/create 测试
- `pkg/vm/engine/federated/`：relation / reader / pushdown / mock connector 测试
- `test/distributed/cases/`：最终 SQL 集成测试

如果首版 CI 没有真实外部数据库依赖，建议：

- 单元测试和大部分执行测试使用 mock connector
- 真正外部数据库 e2e 先作为手工或单独 job

---

## 5. 推荐测试矩阵

## 5.1 DDL 测试

- `CREATE CONNECTION`
- `DROP CONNECTION`
- `CREATE EXTERNAL CATALOG`
- `DROP EXTERNAL CATALOG`
- `CREATE FOREIGN TABLE ... FROM CATALOG ...`
- `IMPORT FOREIGN SCHEMA`
- 重名、非法 option、缺失字段
- `SHOW CREATE CONNECTION`
- `SHOW CREATE CATALOG`
- `SHOW CREATE TABLE foreign_table`

## 5.2 查询语义测试

- `SELECT *`
- projection
- simple predicate pushdown
- residual predicate
- local table + foreign table join（仅本地执行）
- agg over foreign table（仅本地聚合）
- limit

## 5.3 类型测试

- int / bigint
- decimal
- varchar / text
- datetime / timestamp
- null
- timezone 差异
- Oracle `NUMBER` / `DATE` / `TIMESTAMP WITH TIME ZONE`
- MySQL unsigned / text / zero datetime 边界

## 5.4 错误与取消测试

- 连接失败
- 认证失败
- 远端 SQL 错误
- query timeout
- context cancel
- reader close

## 5.5 安全测试

- 无 connection 权限
- 无 foreign table 权限
- 脱敏展示
- secret 不落日志

---

## 6. 风险拆解与应对

## 6.1 风险一：需求膨胀

表现：

- 想同时支持多种 connector
- 想首版做多 CN
- 想首版做写入或事务一致性

应对：

- 阶段 0 明确冻结范围
- 所有非只读/非单 CN 的诉求后移

## 6.2 风险二：表达式翻译错误

表现：

- 结果错误但不易察觉
- 特别容易出现在 null、字符串比较、时间类型

应对：

- 采用 capability-based pushdown
- 不能严格证明等价时留在本地执行
- 把 residual filter 作为默认兜底，而不是 silent rewrite

## 6.3 风险三：连接资源泄露

表现：

- query cancel 后连接未释放
- 远端 result set 未关闭

应对：

- reader 必须实现明确 close 语义
- context cancel 统一透传到 connector
- 增加资源回收测试

## 6.4 风险四：planner 误判导致性能异常

表现：

- 把 foreign table 当小表
- 产生非常激进的 join order

应对：

- 首版启发式 stats 保守设置
- `ForceOneCN = true`
- 后续再迭代 stats cache 和 analyze

---

## 7. 最推荐的落地顺序

如果只从“尽快做出可用 MVP”角度看，建议严格按下面顺序推进：

1. 先做 `CONNECTION` / `FOREIGN TABLE` 元数据
2. 再做 disttae relation 解析返回 `foreignRelation`
3. 再做 mock connector + `foreignReader`
4. 跑通 `SELECT *`
5. 再加 projection pushdown
6. 再加 simple predicate pushdown
7. 再补 explain / metrics / 权限
8. 最后才考虑 stats 增强和多 connector

不要倒过来先做 pushdown 或多 CN。

---

## 8. 交付标准建议

我建议把联邦查询 MVP 的“完成标准”定义为：

- 可以创建 connection
- 可以创建 foreign table
- foreign table 走 `TABLE_SCAN`
- 单 CN 下可以正确查询远端数据
- 支持 projection + basic predicate pushdown
- 不支持的表达式在本地执行仍能保证正确性
- explain 能看见 foreign scan 关键信息
- 权限、脱敏、超时、取消、错误传播都可用

达到这些标准后，再讨论：

- 多 connector
- 更激进 pushdown
- analyze foreign table
- 多 CN 扫描

## 9. 结语

从当前 MatrixOne 代码结构看，联邦查询最稳妥的切入点不是再造一条旁路执行体系，而是把它接回现有表扫描主链路：

- planner 仍当它是表
- compile 仍获取 relation
- execution 仍消费 reader

工程上最关键的不是把能力一次做全，而是先把“元数据、relation、reader、基础 pushdown、权限与可观测性”这几个最核心的基础设施打牢。这样后续无论扩驱动、扩 pushdown，还是扩多 CN，都会有稳定基础。
