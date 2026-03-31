# MatrixOne 联邦查询 PostgreSQL 设计补充

## 1. 为什么 PG 不能只是“再加一个 driver”

如果联邦查询准备同时面对 `MySQL / PostgreSQL / Oracle`，那么 PG 必须在设计阶段就被纳入，因为它会检验当前抽象是不是足够一般化。

PG 的难点不在“比 Oracle 更难”，而在于它会逼着我们把以下几个问题一次性想清楚：

1. `EXTERNAL CATALOG` 的边界到底是什么
2. schema 在对象模型里是不是一等概念
3. quoted identifier / 大小写折叠规则要不要按方言单独处理
4. session 级参数应该挂在 connection、catalog，还是 runtime session
5. 类型系统里哪些能做稳定映射，哪些必须保守降级

所以 PG 的价值不只是多支持一个数据库，而是帮助整个 external catalog 抽象更早“定型”。

---

## 2. PG 对当前对象模型的直接影响

## 2.1 external catalog 粒度

对 MySQL，可以把一个实例下的多个 database 作为 catalog 发现范围。

对 Oracle，可以把一个 service 下的多个 schema 作为 catalog 发现范围。

对 PostgreSQL，更建议采用：

- **一个 external catalog 对应一个 PG database**
- catalog 内部再暴露多个 schema

原因是 PG 原生并不擅长把跨 database 查询当作同一命名空间处理，真正稳定的一等边界是：

- database
- schema
- table

这意味着如果要支持 PG，设计文档里必须明确：

- `catalog` 对 PG 来说更接近“database handle”
- imported foreign table 的远端定位推荐是 `catalog.schema.table`

## 2.2 schema import 语义

PG 加入后，`IMPORT FOREIGN SCHEMA` 的语义会更自然，也更需要被写清楚：

```sql
IMPORT FOREIGN SCHEMA public
FROM CATALOG pg_sales
INTO mo_pg_sales;
```

这里至少要定义：

- import 的 schema 来源是 PG `schema`
- 本地导入目标 schema 如何命名
- `search_path` 会不会影响 metadata discovery（建议不会，统一按显式 schema 处理）

---

## 3. connection / catalog 级别需要补充的 PG 参数

如果文档要正式考虑 PG，connection 设计至少要预留下面这些参数：

- `database`
- `ssl_mode`
- `application_name`
- `connect_timeout`
- `query_timeout`
- `statement_timeout`
- `search_path`
- `timezone`

建议分层如下：

### 更适合放在 `CONNECTION`

- `host`
- `port`
- `user`
- `password`
- `database`
- `ssl_mode`
- `connect_timeout`

### 更适合放在 `EXTERNAL CATALOG`

- `include_schemas`
- `exclude_schemas`
- `metadata_cache_ttl`
- `test_connection`
- `connection_pool_min_size`
- `connection_pool_max_size`
- `default_search_path`
- `default_timezone`

### 更适合放在 runtime session / adapter

- `statement_timeout`
- `application_name`
- 每次 query 的 `SET LOCAL` / session init 行为

这样可以避免把 PG 的会话控制和静态连接配置混成一个层次。

---

## 4. metadata discovery 需要考虑什么

PG 不能只靠一个“通用 information_schema 查询模板”应付，因为实际需要的元数据常常要混合：

- `pg_namespace`
- `pg_class`
- `pg_attribute`
- `pg_type`
- `pg_index`
- `pg_constraint`
- `information_schema`

设计文档需要补清：

### 4.1 列发现

需要拿到：

- 列名
- 类型
- nullability
- 默认值
- ordinal position

### 4.2 主键 / 唯一键 / 索引

即便首版不深用优化器，也建议 metadata API 预留：

- primary key
- unique key
- index existence

因为 PG 的 `ORDER BY / LIMIT` pushdown、未来的 row estimate、甚至直接 explain 输出都会受这些信息影响。

### 4.3 行数与统计信息

PG 可以利用：

- `pg_class.reltuples`
- `pg_stat_*`
- `ANALYZE` 后的统计信息

这说明 design doc 里不应该把“remote stats”只写成后续可选项，而应在接口上预留。

---

## 5. identifier 与大小写规则

PG 会强迫文档把 identifier 规则写得更精确。

### PG 的核心规则

- 未加引号的标识符默认折叠为小写
- 加引号的标识符大小写敏感

这和：

- MySQL 常见的小写/文件系统相关行为
- Oracle 默认大写 + quoted identifier 敏感

都不同。

因此 `DialectAdapter` 至少要明确提供：

- `NormalizeIdentifier`
- `QuoteIdentifier`
- `CaseFoldingMode`

否则 imported foreign table、schema import、show create 很容易出现名字不一致。

---

## 6. 类型映射需要补哪些

如果 PG 纳入范围，类型映射表要显著扩充。

### 6.1 建议首批稳定支持

- `smallint / int / bigint`
- `numeric / decimal`
- `real / double precision`
- `varchar / text`
- `boolean`
- `date`
- `timestamp`
- `timestamptz`
- `bytea`
- `uuid`
- `json / jsonb`（如果先只做文本语义，也要写清楚）

### 6.2 建议首批保守处理

- array
- enum
- domain
- interval
- money
- geometric/network 类型

### 6.3 文档必须补充的原则

设计文档里要明确：

- 什么类型可以“稳定双向映射到 MO”
- 什么类型只允许“文本化/保守降级”
- 什么类型首版直接 reject

否则 PG 一加入，类型系统立刻会比 MySQL/Oracle 的交集复杂得多。

---

## 7. pushdown 语义要额外考虑什么

PG 加入后，pushdown 设计需要再补几条规则。

### 7.1 安全纳入首批的

- projection
- basic predicate
- `LIMIT`
- 简单 `ORDER BY`

### 7.2 需要 capability flag 的

- `ILIKE`
- `LIKE` 的转义差异
- regex
- `NULLS FIRST/LAST`
- boolean 表达式折叠
- `ANY/ALL`

### 7.3 暂不建议一期做主打的

- 复杂函数 pushdown
- `jsonb` 操作符下推
- array 操作符下推
- 聚合/窗口函数 pushdown

PG 的表达力很强，但这恰恰意味着不能把“PG 很强”误当成“首版什么都能推”。

---

## 8. 执行与一致性语义

PG 加入后，文档还要明确以下几点：

- 远端读取默认按 statement 级别一致性理解
- 不提供 MO 本地表与 PG 远端表之间的统一 snapshot
- 本地 + foreign JOIN 仍由 MO 本地执行器完成
- 如果未来要使用更强的 PG 事务隔离级别，需要单独设计 connector session 生命周期

也就是说，PG 不会改变当前“只读联邦、无统一事务”的大方向，但会让这部分语义描述必须写得更严格。

---

## 9. 对一期实现顺序的建议

如果把 PG 纳入设计，我建议的一期 adapter 顺序是：

1. MySQL
2. PostgreSQL
3. Oracle

原因是：

- MySQL 最利于快速打通主链路
- PG 最利于校验 catalog/schema/type/session 抽象是否足够一般
- Oracle 最利于校验高差异方言与企业级边界

这样做的好处是，Oracle 不会被拿来承担“顺便帮我们发现抽象设计问题”的额外负担。

---

## 10. 结论：PG 加入后，设计文档至少要补什么

如果现在决定把 PG 纳入联邦查询规划，我建议至少补这 6 件事：

1. **明确 PG 的 external catalog 粒度：一个 catalog 对应一个 PG database**
2. **把 schema 作为 catalog 内的一等对象来设计 import / refresh**
3. **补全 PG 的 connection/session 参数分层**
4. **补全 PG 的 metadata discovery 来源与接口**
5. **补全 identifier / type mapping / pushdown capability 矩阵**
6. **把一期实现顺序调整为 MySQL -> PostgreSQL -> Oracle**

如果只是在现有 MySQL/Oracle 文档上“顺手再加个 PG 名字”，那设计大概率还是不够稳。PG 真正的价值，是帮助我们更早验证 external catalog 与 dialect adapter 抽象是否足够通用。
