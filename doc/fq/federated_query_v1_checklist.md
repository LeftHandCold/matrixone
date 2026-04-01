# MatrixOne 联邦查询 V1 可执行 Checklist

本文是 `doc/fq/federated_query_v1_final_design.md` 的执行版配套文档。

如果需要把本文进一步拆成“可直接分配给工程师的文件 / 函数级任务”，请继续看 `doc/fq/federated_query_v1_impl_task_breakdown.md`。

目标很简单：

> **把 V1 最终方案拆成可以直接推进的任务清单。**

---

## 1. 先冻结的决策

在真正开工前，先确认下面这些点已经冻结：

- [ ] V1 主场景是“远端读数 + MO 计算”
- [ ] 架构不被该主场景锁死，未来保留更强 pushdown / direct catalog query 等演进空间
- [ ] 对象模型冻结为 `CONNECTION + EXTERNAL CATALOG + IMPORTED FOREIGN TABLE`
- [ ] 执行主路径冻结为 `TABLE_SCAN -> Relation -> Reader`
- [ ] V1 只做只读联邦，不做 DML / 跨源事务
- [ ] V1 强制 `ForceOneCN = true`
- [ ] V1 支持数据源为 `MySQL + PostgreSQL + Oracle`
- [ ] 实现顺序冻结为 `MySQL -> PostgreSQL -> Oracle`
- [ ] imported foreign table 持久化的是 metadata，不是远端业务数据落盘
- [ ] schema drift 策略冻结为“本地 schema 固化 + 显式 refresh”

---

## 2. Phase 0：冻结语义与对象模型

### 2.1 需要冻结的语义

- [ ] `CREATE FOREIGN TABLE ... FROM CATALOG ...` 是单表导入标准语法
- [ ] `IMPORT FOREIGN SCHEMA` 是批量导入语法，复用单表导入主管线
- [ ] `IMPORT FOREIGN SCHEMA` 必须显式 `INTO target_schema`
- [ ] `IMPORT FOREIGN SCHEMA` 首版只做全量导入
- [ ] 与本地同名表冲突时直接报错
- [ ] `REFRESH EXTERNAL CATALOG` / `REFRESH FOREIGN TABLE` 的 V1 语义冻结为全量重刷
- [ ] 同一 catalog / foreign table 的并发 refresh 需要串行化
- [ ] refresh 后远端缺失对象进入 invalid 状态
- [ ] 即使是表面兼容的类型变化（如 widening / length 扩大），也按 schema drift 处理并要求显式 refresh
- [ ] `foreign + foreign JOIN` 的口径冻结为：允许本地执行，但不作为 V1 主打能力
- [ ] 多方言混合查询的口径冻结为：不承诺跨源 pushdown，由 MO 本地统一做 JOIN / AGG / residual filter

### 2.2 需要冻结的接口原则

- [ ] `ExternalCatalog` 负责 metadata discovery、stats、`NewSession()`
- [ ] `DialectAdapter` 负责 metadata SQL、identifier、type mapping、pushdown 翻译
- [ ] `TranslatePredicate` 使用显式状态，而不是含糊的 `ok bool`
- [ ] `Capabilities` 使用结构体显式暴露能力位
- [ ] 接口层统一使用 `namespace` 抽象

### 2.3 Phase 0 验收

- [ ] 文档中所有 V1 关键语义无冲突
- [ ] SQL 草案、实现计划、最终方案对同一对象的定义一致

---

## 3. Phase 1：控制面 DDL 与 metadata object

### 3.1 涉及目录

- [ ] `pkg/sql/parsers/tree/`
- [ ] `pkg/sql/parsers/dialect/mysql/`
- [ ] `pkg/sql/plan/build.go`
- [ ] `pkg/sql/plan/build_ddl.go`
- [ ] `pkg/catalog/types.go`
- [ ] `pkg/frontend/predefined.go`
- [ ] `pkg/frontend/` DDL / 权限 / show-create 相关代码

### 3.2 必做项

- [ ] `CREATE CONNECTION`
- [ ] `ALTER CONNECTION`
- [ ] `DROP CONNECTION`
- [ ] `SHOW CREATE CONNECTION`
- [ ] `CREATE EXTERNAL CATALOG`
- [ ] `ALTER EXTERNAL CATALOG`
- [ ] `DROP EXTERNAL CATALOG`
- [ ] `SHOW CREATE CATALOG`
- [ ] `TEST CONNECTION` 或等价真实建连校验
- [ ] `TEST CONNECTION` 尽量复用真实执行 CN 的建连路径
- [ ] `CREATE FOREIGN TABLE ... FROM CATALOG ...`
- [ ] `IMPORT FOREIGN SCHEMA`
- [ ] `REFRESH EXTERNAL CATALOG`
- [ ] `REFRESH FOREIGN TABLE`
- [ ] `SHOW CREATE TABLE <foreign table>`
- [ ] 定义 `SystemForeignRel = "f"`
- [ ] 增加 `mo_catalog.mo_connections`
- [ ] 增加 `mo_catalog.mo_external_catalogs`
- [ ] imported foreign table 继续落到 `mo_tables/mo_columns`
- [ ] secret 不进入 foreign table 定义

### 3.3 验收标准

- [ ] 所有 DDL 能 parse / build / execute
- [ ] `SHOW CREATE` 不泄露 secret
- [ ] foreign table 能在 catalog 中正确落成 `relkind = SystemForeignRel`

---

## 4. Phase 2：执行骨架接入

### 4.1 涉及目录

- [ ] `pkg/vm/engine/disttae/txn_database.go`
- [ ] 新增 `pkg/vm/engine/federated/`

### 4.2 建议文件

- [ ] `types.go`
- [ ] `relation.go`
- [ ] `reader.go`
- [ ] `catalog.go`
- [ ] `connector.go`
- [ ] `dialect.go`
- [ ] `capability.go`
- [ ] `expr_translate.go`

### 4.3 必做项

- [ ] 在 `txnDatabase.relation()` 中于 `newTxnTable()` 前识别 foreign table
- [ ] 返回 `foreignRelation`
- [ ] 实现最小 `FederatedRelData`
- [ ] `Ranges()` 返回单一逻辑范围
- [ ] `BuildReaders()` 首版退化为 `1 real + N-1 empty`
- [ ] `Stats()` 提供保守启发式统计
- [ ] connector 支持 `SQL + FetchSize + Timeout`
- [ ] 保证 foreign table 继续走 `TABLE_SCAN`
- [ ] 优先在 scan 节点构造阶段固化 `ForceOneCN = true`
- [ ] 在 `readutil` 一类位置预留未来 `FederatedRelData` 扩展点

### 4.4 验收标准

- [ ] mock connector 能跑通 `SELECT * FROM foreign_table`
- [ ] compile / scope 不需要复制第二套 scan 框架
- [ ] explain 中能看见 foreign table 标识

---

## 5. Phase 3：MySQL adapter

### 5.1 必做项

- [ ] 选定 MySQL Go 驱动
- [ ] DSN 构建
- [ ] `TestConnection`
- [ ] metadata discovery
- [ ] identifier 规则
- [ ] type mapping
- [ ] projection pushdown
- [ ] basic predicate pushdown
- [ ] `LIMIT` pushdown
- [ ] 简单 `ORDER BY` pushdown（仅 capability 允许时）
- [ ] error -> `moerr` 映射

### 5.2 类型优先级

- [ ] `int / bigint`
- [ ] `decimal`
- [ ] `varchar / text`
- [ ] `datetime / timestamp`
- [ ] `null`

### 5.3 暂缓或保守处理

- [ ] `json`
- [ ] `bit`
- [ ] `enum/set`
- [ ] `unsigned` 边界

### 5.4 验收标准

- [ ] MySQL 全链路可跑通
- [ ] 结果正确优先于 pushdown 覆盖率

---

## 6. Phase 4：PostgreSQL adapter

### 6.1 必做项

- [ ] 选定 PostgreSQL Go 驱动
- [ ] 冻结“1 catalog = 1 PG database”语义
- [ ] schema discovery
- [ ] `search_path` 不影响 metadata discovery
- [ ] identifier / quoted identifier 规则
- [ ] type mapping
- [ ] `LIMIT` / basic predicate / simple `ORDER BY`
- [ ] PG session 参数分层（connection / catalog / runtime）

### 6.2 类型优先级

- [ ] `numeric`
- [ ] `boolean`
- [ ] `text`
- [ ] `timestamp`
- [ ] `timestamptz`
- [ ] `uuid`
- [ ] `bytea`

### 6.3 暂缓或保守处理

- [ ] array
- [ ] enum
- [ ] domain
- [ ] interval
- [ ] `ILIKE` / regex / `NULLS FIRST/LAST` 默认不直接下推

### 6.4 验收标准

- [ ] PostgreSQL 能验证 catalog/schema/type/session 抽象通用性
- [ ] 不因为 PG 加入而推翻前面对象模型

---

## 7. Phase 5：Oracle adapter

### 7.1 必做项

- [ ] 选定 Oracle Go 驱动
- [ ] `service_name` / `sid` 支持
- [ ] schema discovery
- [ ] identifier / quoted identifier / 大小写规则
- [ ] `FETCH FIRST` / `ROWNUM`
- [ ] Oracle type mapping
- [ ] 保守 pushdown deny-list

### 7.2 必测差异点

- [ ] `NUMBER`
- [ ] `DATE`
- [ ] `TIMESTAMP`
- [ ] `TIMESTAMP WITH TIME ZONE`
- [ ] `CLOB`
- [ ] 空字符串 / `NULL` 语义

### 7.3 验收标准

- [ ] Oracle 不被当成“换个 DSN 的 MySQL/PG”
- [ ] 高风险语义默认保守处理

---

## 8. Phase 6：Refresh、explain、metrics、治理闭环

### 8.1 必做项

- [ ] `REFRESH EXTERNAL CATALOG`
- [ ] `REFRESH FOREIGN TABLE`
- [ ] schema drift 显式失败
- [ ] invalid foreign table 状态
- [ ] explain foreign details
- [ ] remote connect/query latency metrics
- [ ] rows fetched / timeout / remote error metrics
- [ ] timeout / cancel
- [ ] secret 脱敏

### 8.2 验收标准

- [ ] refresh 能修复 schema drift
- [ ] 慢查询可以区分“远端慢”还是“本地算慢”
- [ ] 错误可定位但不泄露敏感信息

---

## 9. 语义测试与混合场景

### 9.1 必测查询语义

- [ ] foreign 单表 `SELECT`
- [ ] projection + basic predicate
- [ ] `LIMIT`
- [ ] 简单 `ORDER BY`
- [ ] local + foreign JOIN（本地执行）
- [ ] `AGG` over foreign table（本地聚合）

### 9.2 必测混合方言场景

- [ ] MySQL foreign + PostgreSQL foreign JOIN（本地执行）
- [ ] MySQL foreign + Oracle foreign JOIN（本地执行）
- [ ] 多方言混合场景下不误触发跨源 pushdown
- [ ] 多方言场景下 query timeout 仍以 MO 查询维度生效

### 9.3 必测治理场景

- [ ] `TEST CONNECTION`
- [ ] refresh 前后 schema drift 行为
- [ ] invalid foreign table 行为
- [ ] secret 不出现在日志和 `SHOW CREATE`

---

## 10. V1 明确不做

- [ ] multi-CN foreign scan
- [ ] foreign DML
- [ ] 跨源事务
- [ ] 统一 snapshot
- [ ] join pushdown
- [ ] 复杂 agg pushdown
- [ ] 复杂函数 pushdown
- [ ] 自动 schema 同步
- [ ] direct `catalog.schema.table`

---

## 11. 最终验收清单

### 11.1 架构层

- [ ] 主路径没有演变成长期 `FEDERATED_SCAN` 旁路
- [ ] imported foreign table 只持久化 metadata，不误导成数据落盘能力
- [ ] 对象模型在 MySQL / PostgreSQL / Oracle 上都成立

### 11.2 功能层

- [ ] control plane SQL 全部可用
- [ ] MySQL / PostgreSQL / Oracle 三个 adapter 全部可用
- [ ] foreign 单表查询可用
- [ ] local + foreign JOIN / `AGG` 可用（本地执行）
- [ ] mixed-dialect JOIN 保持本地执行边界
- [ ] pushdown 范围符合 capability 定义

### 11.3 运维层

- [ ] refresh 可用
- [ ] explain 可用
- [ ] metrics 可用
- [ ] timeout / cancel 可用
- [ ] secret 脱敏可用

### 11.4 边界层

- [ ] 未承诺 multi-CN / DML / 跨源事务
- [ ] 文档与实际实现边界一致
