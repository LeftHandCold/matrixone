# MatrixOne 联邦查询范围重规划

## 1. 为什么要做这份重规划

前面的几份文档里，很多地方都用了“首版不做”“首批不支持”的表述。这些结论在**保守 MVP** 语境下是合理的，但并不等于：

- 这些能力永远不该做
- 这些能力一定要拖到很后面
- MatrixOne 当前架构完全承接不了

它们更多是在回答一个工程问题：

> 如果要先以最低风险证明“联邦查询能稳地接入 MO 主链路”，第一轮应该把范围收在哪里？

现在如果前提变成：

- 团队有时间
- 能接受更大的首发范围
- 目标不是最小 PoC，而是更完整的一期正式交付

那么范围确实应该重新规划。

同时要明确一个更底层的架构原则：

> **当前主需求是“外部读数 + MO 计算”，但最终设计不应该被这个场景锁死成唯一模式。**

也就是说：

- 一期设计当然应该优先把“远端读 + MO 算”做到最好
- 但对象模型、catalog 抽象、dialect adapter、planner 路由不应把未来更合理的演进路径直接封死

例如后续可能出现的合理演进包括：

- 更强但仍保守的 pushdown
- direct `catalog.schema.table`
- 更丰富的 stats / optimizer 接入
- 在明确收益和边界后引入新的执行形态

---

## 2. 为什么之前看起来“保守”

之前保守，不是因为这些能力没有价值，而是因为它们的工程耦合度差别很大。

### 2.1 一类是“可以做，只是当时为了先稳住主链路而后放”

这类能力其实完全可以被拉进一期，只是之前为了避免架构接入和语义扩散同时发生，先压后了：

- Oracle 与 MySQL 同时进入一期目标
- `REFRESH EXTERNAL CATALOG`
- `REFRESH FOREIGN TABLE`
- `ALTER CONNECTION`
- `ALTER EXTERNAL CATALOG`
- 更完整的 `SHOW CREATE`
- projection / predicate 之外的 `LIMIT`、`ORDER BY` pushdown
- explain / metrics / timeout / connection test 完整化

这类能力的特点是：

- 不要求推翻 `TABLE_SCAN -> Relation -> Reader` 主路径
- 不要求跨 CN 新协议
- 不要求跨源事务模型
- 主要是 connector、DDL、catalog metadata、dialect adapter 的扩展

换句话说：**它们是“工作量大”，但不是“方向错”。**

### 2.2 一类是“可以规划，但最好放在一期后半段”

这类能力不是不能做，但建议放在 foundation 打稳之后，否则会把前端、binder、权限、执行器一起拉复杂：

- direct `catalog.schema.table` 原生执行
- 更大范围的表达式 / 函数 pushdown
- 更积极的 remote stats / cost model
- foreign + foreign join 的主打能力化

这类能力的特点是：

- 不是简单多写几个 adapter 就能完成
- 需要 resolver、binder、planner、权限模型一起配合
- 对 explain、stats、可观测性要求更高

### 2.3 一类是“即使有时间，也不该轻易塞进第一波交付”

这类能力真正难的不是代码量，而是语义边界和系统耦合：

- multi-CN foreign scan
- foreign `INSERT/UPDATE/DELETE`
- 跨源事务
- 统一 snapshot
- join pushdown
- agg pushdown（尤其跨方言、复杂表达式）

这些能力之所以之前被排除，不是因为“不重要”，而是因为它们会牵涉到：

1. `readutil / remoterun / relation data` 的协议与序列化扩展
2. planner/compile 的分布式 reader 切分语义
3. 远端数据库事务、重试、提交、回滚、幂等与错误恢复
4. Oracle/MySQL 不同的 null、时间、函数、排序、limit 语义
5. “可否证明 pushdown 后结果与 MO 本地执行等价”这个本质问题

所以这类能力即使最终要做，也更适合作为**二期/三期能力**。

---

## 3. 新的总体建议：不要再以“最小 MVP”作为一期目标

如果团队现在愿意投入更多时间，我建议把目标从：

- “最小可落地读联邦 MVP”

升级为：

- **“完整只读联邦查询 V1”**

这个 V1 仍然坚持正确的技术边界：

- 仍然复用 `TABLE_SCAN -> Relation -> Reader`
- 仍然以 imported foreign table 为主
- 仍然不碰跨源事务与写入
- 仍然不把 multi-CN foreign scan 硬塞进第一波

但它应当明显强于最小 MVP。

---

## 4. 建议纳入一期正式交付的能力

下面这些能力，我建议直接拉进一期，而不是再挂在“后续再说”上。

### 4.1 数据源范围

- MySQL：一期必须完整支持
- PostgreSQL：建议直接纳入一期设计范围
- Oracle：仍然保留为一期目标，但实现顺序可以放在 MySQL/PG 之后

如果把 PG 纳入考虑，设计会更稳，而不是更乱。因为 PG 会提前暴露出：

- external catalog 到底对应“一个 PG cluster”还是“一个 PG database”
- schema 粒度是否是对象模型中的一等概念
- quoted identifier / 大小写折叠规则是否抽象正确
- `jsonb` / `uuid` / `bytea` / array / enum / interval 等类型是否有通用映射策略
- session 级参数（`search_path`、`TimeZone`、`statement_timeout` 等）放在哪一层管理

而 Oracle 则会继续暴露：

- 更强差异的 identifier 和 metadata 规则
- `NUMBER/DATE/TIMESTAMP WITH TIME ZONE/CLOB` 等类型模型
- `FETCH FIRST`、方言函数、大小写与引号策略

所以更合理的表述不是“只做 MySQL，然后 Oracle/PG 再说”，而是：

- **一期设计范围：MySQL + PostgreSQL + Oracle**
- **一期实现顺序：MySQL -> PostgreSQL -> Oracle**

这样 MySQL 用来尽快跑通主链路，PG 用来校验通用关系库抽象，Oracle 用来校验高差异方言抽象。

### 4.2 控制面能力

一期建议直接支持：

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
- `TEST CONNECTION` 或等价创建时测试语义

这组能力放到一期是值得的，因为它们共同决定了这个功能是不是“能交给客户用”，而不是只能做演示。

### 4.3 读查询能力

一期建议明确支持：

- foreign 单表 `SELECT`
- local + foreign JOIN（本地执行）
- `AGG` over foreign table（本地聚合）
- projection pushdown
- basic predicate pushdown
- `LIMIT` pushdown
- `ORDER BY` pushdown（限定在单表、简单列、方言可证明等价时）
- residual filter

这里最重要的变化是：

- 不必把 `LIMIT`/`ORDER BY` 一概放到后面
- 只要坚持 capability-based pushdown，就可以安全纳入一期

### 4.4 生命周期与可运维性

一期建议直接纳入：

- schema drift 显式失败
- refresh 闭环
- connection test
- timeout / cancel
- explain foreign 节点增强
- remote latency / rows fetched / timeout / error metrics
- `moerr` 错误码映射

没有这些内容，功能虽然“能跑”，但还不够像一个可交付系统能力。

---

## 5. 仍建议后移的能力

下面这些能力，即使现在不缺时间，我仍建议不要放进第一波主线交付。

### 5.1 multi-CN foreign scan

原因不是工作量，而是它会真正改到：

- relation data 序列化
- compile / scope 的 reader 切分
- remote reader 生命周期
- 结果归并与错误处理

如果第一波就做，会让“联邦查询是否能接入 MO 主路径”这个核心问题被分布式执行问题淹没。

### 5.2 写入与事务

包括：

- foreign `INSERT`
- foreign `UPDATE`
- foreign `DELETE`
- 跨源事务
- 统一 snapshot

这类能力如果没有明确业务必要性，不建议与一期读查询一起绑定推进。

### 5.3 激进 pushdown

包括：

- join pushdown
- 复杂 agg pushdown
- 复杂函数 pushdown
- 高风险表达式翻译

一期可以做的是：

- capability-based pushdown
- 可严格证明等价的简单场景

但不建议把“尽量多推”作为第一阶段 KPI。

---

## 6. 新的一期/二期/三期规划

## 6.1 一期：完整只读联邦查询 V1

这是我建议的新一期目标。

### 一期目标

- MySQL + Oracle 同时进入目标范围
- `CONNECTION + EXTERNAL CATALOG + IMPORTED FOREIGN TABLE`
- `ALTER` / `SHOW CREATE` / `REFRESH`
- imported foreign table schema 固化
- local + foreign JOIN
- `AGG` over foreign table
- projection / predicate / `LIMIT` / 简单 `ORDER BY` pushdown
- explain / metrics / timeout / cancel / connection test
- 权限、secret 管理、error mapping

### 一期不做

- multi-CN foreign scan
- foreign DML
- 跨源事务
- 统一 snapshot
- join pushdown
- 复杂 agg / 函数 pushdown

### 一期推荐代码推进顺序

1. control plane 和 metadata object
2. `foreignRelation / foreignReader` 主链路接入
3. MySQL adapter
4. PostgreSQL adapter
5. Oracle adapter
6. refresh / drift / observability
7. `LIMIT` / `ORDER BY` 等增强 pushdown

## 6.2 二期：分布式与动态解析增强

二期再做：

- multi-CN foreign scan
- direct `catalog.schema.table`
- 更完整 stats / cost model
- 更多表达式与函数 pushdown

这里的核心不再是“能不能联邦”，而是“联邦怎么更快、更自然”。

## 6.3 三期：事务型联邦能力

三期如果业务真的需要，再独立评估：

- 写入类联邦能力
- 跨源事务
- 更强一致性模型

这部分建议单独立项，不要和只读联邦混成一个项目。

---

## 7. 对现有 checklist 的修正理解

`doc/fq/federated_query_code_checklist.md` 仍然有价值，但要把它理解成：

- **最保守的落地基线**

而不是：

- **唯一合理的一期范围**

如果按照本重规划执行，那么有几项要从“后续”提升为“一期”：

| 能力 | 原先口径 | 新建议 |
|------|----------|--------|
| PostgreSQL 支持 | 未纳入 | 一期设计目标 |
| Oracle 支持 | 二批/预留 | 一期目标 |
| `ALTER CONNECTION/CATALOG` | 次优先级 | 一期目标 |
| `REFRESH EXTERNAL CATALOG` | 预留 | 一期目标 |
| `REFRESH FOREIGN TABLE` | 预留 | 一期目标 |
| `LIMIT` pushdown | 保守后放 | 一期目标 |
| 简单 `ORDER BY` pushdown | 保守后放 | 一期增强 |
| explain / metrics | 后段增强 | 一期目标 |

---

## 8. 最终建议

如果你现在的目标已经不是“先做个最小版本看通不通”，而是“做一个客户可用的一期版本”，那我建议：

1. **保留原来的架构判断不变**
   - 继续走 `TABLE_SCAN -> Relation -> Reader`
   - 继续坚持 `catalog-first`

2. **扩大一期范围**
   - Oracle 不再只是预留
   - refresh / alter / observability 直接进入一期
   - `LIMIT` / 简单 `ORDER BY` pushdown 直接进入一期

3. **仍然保留几个硬边界**
   - 不在第一波做 multi-CN foreign scan
   - 不在第一波做写入和跨源事务
   - 不在第一波做激进 join/agg pushdown

如果用一句话概括这次重规划，就是：

> **把目标从“最小可落地 MVP”提升为“完整只读联邦查询 V1”，但不要把分布式执行和事务型联邦一起绑进第一波。**
