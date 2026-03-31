# MatrixOne 联邦查询设计方案 Code Review

**Review 日期**: 2026-03-31
**Reviewer**: AI Assistant
**文档版本**: v2.0

---

## 1. 整体评价

**结论**: 当前方案在升级为 **`CONNECTION + EXTERNAL CATALOG + IMPORTED FOREIGN TABLE`** 之后，整体合理性比早期“只做 connection + foreign table”的版本更强，**推荐采纳当前路线**。

当前方案最重要的进步有两点：

1. 控制面从单表建模升级为 `catalog-first`
2. 执行面仍坚持 `TABLE_SCAN -> Relation -> Reader` 主路径

这意味着它既吸收了 Doris 在 external catalog 上的工程经验，又没有放弃 MatrixOne 当前最稳的执行复用路径。

---

## 2. 设计亮点

### 2.1 走 TABLE_SCAN 主链路而非新建 FEDERATED_SCAN 旁路

这是整个设计最关键的正确决策。

**理由**:
- 避免形成两套并行的"表扫描体系"
- 便于统一 explain、stats、filter、join 语义
- 未来如果要将 foreign table 能力回归普通表模型，成本更低
- 可以最大化复用现有 scan 主框架

**代码验证**:
```go
// pkg/vm/engine/disttae/txn_database.go
// 现有的 relation 解析链路确实适合作为接入点
func (db *txnDatabase) relation(ctx context.Context, name string, proc any) (engine.Relation, error) {
    // 在此处识别 foreign table，返回 foreignRelation
    tbl, err := newTxnTable(ctx, db, *item, ...)
    return tbl, nil
}
```

### 2.2 `catalog-first` 比“单表 foreign table”更适合 Oracle/MySQL

在 Oracle / MySQL 客户场景下，只做 `CONNECTION + FOREIGN TABLE` 有明显局限：

- 一批远端表的 discovery 规则无稳定挂载点
- 名字映射、大小写策略、metadata cache、connection pool 无处安放
- schema import / refresh 语义不自然

升级到：

- `CREATE CONNECTION`
- `CREATE EXTERNAL CATALOG`
- `CREATE FOREIGN TABLE ... FROM CATALOG ...` / `IMPORT FOREIGN SCHEMA`

之后，职责分层明显更合理。

### 2.3 元数据与连接凭据解耦

`CREATE CONNECTION` 与 `EXTERNAL CATALOG`、`FOREIGN TABLE` 分层的设计非常正确：

- 安全边界清晰：敏感凭据不直接绑死在表定义上
- 便于凭据轮换：更新连接信息不影响 imported table 定义
- 多 catalog / 多表可复用同一连接
- catalog 层可统一承接 discovery、mapping、pool 和 test_connection

### 2.4 首版范围控制合理

| 约束 | 评价 |
|------|------|
| 单 CN (`ForceOneCN = true`) | 正确，避免了 remoterun payload 扩展和多 CN 复杂性 |
| 只读 | 正确，规避了跨源事务一致性的复杂问题 |
| 不做 join/agg pushdown | 正确，降低了表达式翻译风险 |
| imported table 固化 schema，不做自动 schema 演化 | 正确，保持实现复杂度可控 |

### 2.5 正确识别了 readutil 序列化限制

设计方案正确识别了 `readutil.UnmarshalRelationData()` 只支持 `BlockListRelData` 和 `EmptyRelationData`，这为"首版单 CN"的决策提供了重要依据。

### 2.6 正确借鉴 Doris，但没有机械照搬 JDBC

这是当前版本一个很重要的优点。

当前方案借鉴了 Doris 的这些核心思想：

- external catalog 作为控制面对象
- 元数据发现与连接池配置挂在 catalog 层
- 不同数据库类型在 catalog/table 层有方言差异处理

但没有机械照搬 Doris 的 JDBC 技术栈，而是明确把 MatrixOne 的实现方向放在：

- Go 原生驱动
- dialect adapter
- imported foreign table 执行复用

这对 MatrixOne 来说是更合理的工程化取舍。

---

## 3. 需要进一步确认/补充的点

### 3.1 `relkind` 值需要明确定义

**现状**:
```go
// pkg/catalog/types.go
SystemOrdinaryRel     = "r"
SystemExternalRel     = "e"
SystemSourceRel       = "s"
SystemViewRel         = "v"
SystemMaterializedRel = "m"
// ...
```

**建议**:
```go
// 新增
SystemForeignRel = "f"
```

并在 `pkg/catalog/types.go` 中明确定义，而不是直接使用字面量 `'f'`，便于后续维护。

### 3.2 relation 解析拦截点实现位置

**设计方案**: 在 `txnDatabase.Relation()` 中识别 foreign table，返回 `foreignRelation`

**建议实现**:
```go
// pkg/vm/engine/disttae/txn_database.go
func (db *txnDatabase) relation(ctx context.Context, name string, proc any) (engine.Relation, error) {
    // 在 newTxnTable() 调用之前拦截
    if item.IsForeignTable() {
        return newForeignRelation(ctx, db, item)
    }
    return newTxnTable(ctx, db, item)
}
```

**避免**: 在 `txnTable` 内部积累大量 if/else foreign 分支，保持 txnTable 的职责单一。

### 3.3 `Ranges()` 首版实现策略

**问题**: `Ranges()` 在 MO 代码中被多处依赖，compile 阶段需要它来生成分片描述。

**建议**:
- 首版返回表示"全表扫描"的单一 range
- 不尝试伪造 disttae block/object 细粒度范围
- 需要实现 `engine.RelData` 接口或其简化版本，确保 compile 主路径正常工作

### 3.4 `BuildReaders()` 与 compile 框架兼容性

**建议**:
- 首版只返回 1 个真实 reader
- 其余 reader 返回 empty reader
- 让现有单 CN pipeline 平稳复用

**重点测试场景**:
1. `SELECT * FROM foreign_table` 单表查询
2. 带 `LIMIT n` 的查询
3. 带简单 `WHERE` 条件的查询

### 3.5 connector 抽象设计建议

**当前更合理的收敛方式**:
```go
type QueryRequest struct {
    SQL       string
    FetchSize int
    Timeout   time.Duration
}
```

**建议**:
- 保持 `SQL string` 为 connector 的核心输入
- 允许少量执行参数，例如 `FetchSize`、`Timeout`
- 不要把 `Expression`、`TableDef`、复杂 AST 直接暴露给 connector

**理由**: 保持 connector 接口稳定，避免首版过度设计，同时保留最小必需的执行调优能力。

### 3.6 external catalog object 落盘方式

**需要确认**:
- `mo_connections` 是否需要像 `mo_stages` 一样在 `mo_catalog` 中预定义
- `mo_external_catalogs` 是否也应作为独立系统对象元数据存在
- catalog 的 DDL 执行入口是新增还是复用现有框架

**参考实现**:
```go
// pkg/frontend/predefined.go 中的系统表定义
// pkg/catalog 功能表的建表 SQL
```

### 3.7 错误处理需要对齐 MO 错误码体系

**建议定义**:
```go
ErrForeignConnectionFailed  // 连接失败
ErrForeignQueryRejected     // 远端 SQL 错误
ErrForeignTimeout           // 超时
ErrForeignNotSupported      // 不支持的 pushdown
```

参考 `pkg/common/moerr` 中的错误定义方式。

### 3.8 跨源 JOIN 语义边界

**文档已识别**:
> foreign table 的读取语义由远端连接器提供；跨源 join 不提供 2PC 或统一快照保证。

**需要明确**:
- `SELECT local_table.*, f.* FROM local_table JOIN foreign_table` 的行为
- 是否在首版直接不支持跨源 JOIN

当前新版文档已经给出更合理的定义：

- 首版支持跨源 JOIN / AGG 的**本地执行**
- 不支持 join/agg pushdown
- 不提供统一 snapshot

这一定义合理，建议保留。

### 3.9 schema drift / refresh 策略需要作为正式规则保留

这是新版文档新增后，我认为非常正确的一点：

- imported foreign table 以导入时 schema 固化
- 不自动跟随远端 schema 漂移
- 通过 `REFRESH EXTERNAL CATALOG` / `REFRESH FOREIGN TABLE` 或重新 import 修正

这对于 Oracle/MySQL 特别重要，因为：

- Oracle 的大小写、quoted identifier、`NUMBER/DATE/TIMESTAMP` 兼容性复杂
- MySQL 也会有 unsigned、text、zero datetime 等边界

如果首版允许静默 schema drift，将极大增加错误概率。

### 3.10 `test_connection` 需要站在执行视角定义

这一点是当前方案还需要持续强调的工程细节：

- `test_connection` 不能只理解成“控制面建对象时能连上”
- 更合理的定义是：尽量验证**实际执行 CN** 是否具备驱动和连通性

这和 Doris 同时关注控制面与执行面连通性的经验是一致的。

---

## 4. 实施建议总结

| 优先级 | 建议事项 | 涉及文件/模块 |
|--------|----------|---------------|
| **高** | 明确 `SystemForeignRel = "f"` 在 types.go 中定义 | `pkg/catalog/types.go` |
| **高** | relation 解析拦截点放在 `db.relation()` 中，`newTxnTable()` 之前 | `pkg/vm/engine/disttae/txn_database.go` |
| **高** | `BuildReaders()` 首版只返回 1 个真实 reader | `pkg/vm/engine/federated/reader.go` |
| **中** | `Ranges()` 首版返回表示"全表扫描"的单一 range | `pkg/vm/engine/federated/relation.go` |
| **中** | connector 的 `QueryRequest` 保持简单，首版限定为 `SQL + 少量执行参数` | `pkg/vm/engine/federated/` |
| **中** | 确认 `mo_connections` / `mo_external_catalogs` 的系统对象落盘方式 | `pkg/catalog/`, `pkg/frontend/predefined.go` |
| **中** | 固化 schema drift / refresh 规则 | 文档与 `pkg/frontend/` DDL 逻辑 |
| **中** | 规定 `test_connection` 的执行视角 | external catalog / connector 设计 |
| **低** | direct `catalog.schema.table` 的二阶段支持方式 | resolver / binder 后续演进 |

---

## 5. 风险评估

| 风险 | 等级 | 应对措施 |
|------|------|----------|
| 需求膨胀（同时做多 connector、首版做多 CN） | 中 | 严格遵循阶段划分，范围冻结 |
| 表达式翻译错误（null、字符串比较、时间类型） | 高 | 采用 capability-based pushdown，不能严格证明等价时留在本地执行 |
| 连接资源泄露 | 中 | reader 必须实现明确 close 语义，context cancel 统一透传到 connector |
| planner 误判导致性能异常 | 低 | 首版启发式 stats 保守设置，`ForceOneCN = true` |
| schema drift 导致隐式行为变化 | 中 | 本地 schema 固化 + 显式 refresh |
| Oracle 方言差异低估 | 高 | 单独 dialect adapter，保守 pushdown |

---

## 6. 结论

这是一个**工程上合理且比初版更适合 Oracle/MySQL 客户需求**的联邦查询方案。

我对当前方案的总体判断是：

1. **控制面建模正确**：`CONNECTION + EXTERNAL CATALOG + IMPORTED FOREIGN TABLE` 明显优于早期双层模型
2. **执行面路线正确**：继续坚持 `TABLE_SCAN` 主路径，而不是长期旁路 operator
3. **MVP 边界正确**：只读、单 CN、保守 pushdown、schema 固化，都是稳妥选择
4. **Oracle/MySQL 路线合理**：MySQL 先打通、Oracle 作为高差异方言 adapter 跟进，是更可控的落地顺序

后续实施时，建议重点关注：

- `db.relation()` 拦截与 `foreignRelation` 接入
- external catalog object metadata 的定义
- schema drift / refresh 规则
- Oracle 方言 adapter 的保守实现边界
- `test_connection` 与执行路径的一致性

**推荐状态**: ✅ 当前方案通过 Review，建议按现有 catalog-first 版本推进实施
