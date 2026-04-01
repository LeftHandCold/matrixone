# MatrixOne 联邦查询第一阶段实现 Code Review

**评审日期**：2026-04-01
**评审范围**：4 个最新 commit
**代码版本**：`add_fq_doc` 分支

---

## 1. 评审概述

本次评审覆盖以下 4 个 commit：

| Commit | 标题 | 主要内容 |
|--------|------|----------|
| `cee649e5e2` | feat: add federated query foundation objects | 联邦查询基础对象：relkind、CONNECTION 控制面、catalog 建表、引擎 guardrail |
| `feb273f401` | fix: reject unsupported foreign table options | 拒绝不支持的 foreign table 选项 |
| `ccabd756b4` | fix: harden connection SQL literal escaping | 强化连接 SQL 字面量转义 |
| `61db10e2c0` | fix: polish federated query connection handling | 完善 connection 处理逻辑 |

**总体结论**：✅ **第一阶段实现基本达标，建议继续按计划推进**

实现符合"控制面先行 + 主链路占位"的策略，与设计文档（`doc/fq/federated_query_design.md`）的核心决策一致。

### 1.1 复核后的修订结论

在这份 review 形成后，又做了一轮针对性修复与代码复查。以下几点需要同步修正，避免后续读者被旧结论误导：

1. `SHOW CREATE CONNECTION` 的 round-trip 问题已经修复。
   - `tree/connection.go` 已改为按 MySQL 字符串字面量规则统一转义
   - parser / frontend 已补回归测试，带 `\\`、换行、制表等值可稳定往返

2. `CreateConnection` / `DropConnection` 的 frontend 事务分类已经补齐。
   - 这两个语句不再落入 active transaction 下的 `unclassified statement` 错误路径

3. review 中“`privilegeKindNone` 意味着任何人都能创建 / 删除 connection”的判断并不准确。
   - 当前 `doCreateConnection` / `doDropConnection` / `doShowCreateConnection` 都先经过 `doCheckRole`
   - 也就是说，现阶段已经有 admin gate
   - 真正还没做的是更细粒度的 `USAGE ON CONNECTION` / `SELECT ON FOREIGN TABLE` 权限模型

4. review 中“FOREIGN TABLE 的 DDL 约束尚未完整拦截”的判断也需要修正。
   - 当前实现已经拒绝非列定义、`PARTITION`、`CLUSTER BY`、`AS SELECT`、`AS LIKE` 以及不支持的 table option
   - 换句话说，index / foreign key / check / 非法 table option 并不会静默放行

5. 针对“foreign table 直到执行期才报 not implemented”的问题，现已增加 planner 级拦截。
   - `SELECT` foreign table 会在 planner 阶段直接失败
   - disttae 层仍保留 fail-fast 保护，作为执行面的第二道保险

基于以上复核，当前更合理的结论是：

- **第一阶段代码可以视为稳定收口**
- **第二个 PR 不建议直接跳到 reader 执行面**
- **第二个 PR 的最佳落点是 `EXTERNAL CATALOG metadata-only`，继续沿着 catalog-first 主线推进**

---

## 2. 总体评价

### 2.1 符合设计意图的部分

| 设计决策 | 评审意见 |
|----------|----------|
| `relkind = 'f'` | ✅ `SystemForeignRel = "f"` 定义在 `pkg/catalog/types.go`，避免魔法字面量 |
| Connection 与表对象分离 | ✅ `mo_connections` 独立建表，connection 只存连接信息，不混入 foreign table |
| ForceOneCN 单 CN 执行 | ✅ 当前 `newForeignTxnTable` 是 `newTxnTable` 的占位别名，为后续 reader 接入预留 |
| planner guardrail | ✅ DML 拦截、unsupported options 拒绝已实现 |
| 敏感信息脱敏 | ✅ `SHOW CREATE CONNECTION` 对 password 等字段输出 `***` |

### 2.2 存在的风险与改进点

| 风险等级 | 问题描述 | 涉及文件 |
|----------|----------|----------|
| **高** | `newForeignTxnTable` 目前是 `newTxnTable` 别名，没有实际 foreign table 语义隔离 | `pkg/vm/engine/disttae/txn_table_foreign.go` |
| **高** | SQL 拼接虽有转义，但仍建议尽快升级为参数化执行 | `pkg/frontend/connection.go` |
| **中** | FOREIGN TABLE 的 DDL 约束（index/partition/foreign key 等）尚未完全拦截 | `pkg/sql/plan/build_ddl.go` |
| **中** | 权限粒度未明确（CREATE CONNECTION / USAGE ON CONNECTION 等） | `pkg/frontend/authenticate.go` |
| **低** | `getSqlForDropConnection` 中 `order by connection_id` 的 delete 语义罕见 | `pkg/frontend/connection.go` |

---

## 3. 逐 Commit 详细评审

### 3.1 `cee649e5e2` - feat: add federated query foundation objects

#### 亮点

1. **元数据层建设完整**
   - `SystemForeignRel = "f"` 定义清晰（[pkg/catalog/types.go:L311](file:///Users/shenjiangwei/Work/code/matrixone/pkg/catalog/types.go#L311)）
   - `mo_connections` 通过 tenant upgrade list 正确接入 bootstrap 链路（[tenant_upgrade_list.go:L28-L80](file:///Users/shenjiangwei/Work/code/matrixone/pkg/bootstrap/versions/v4_0_0/tenant_upgrade_list.go#L28-L80)）
   - authenticate 中正确注册 `MO_CONNECTIONS` 的 privilege 处理（[authenticate.go:L929-L1060](file:///Users/shenjiangwei/Work/code/matrixone/pkg/frontend/authenticate.go#L929-L1060)）

2. **Parser 层设计合理**
   - `CreateConnection` / `DropConnection` / `ShowCreateConnection` AST 节点定义完整（[tree/connection.go](file:///Users/shenjiangwei/Work/code/matrixone/pkg/sql/parsers/tree/connection.go)）
   - 支持 `IF [NOT] EXISTS` 语义
   - `ConnectionOption` 结构清晰，支持 key-value 形式

3. **Frontend 控制面实现扎实**
   - 类型别名映射（mysql / oracle / postgresql / postgres / pg）处理得体（[connection.go:L45-L52](file:///Users/shenjiangwei/Work/code/matrixone/pkg/frontend/connection.go#L45-L52)）
   - Oracle 的 `service_name` / `sid` 二选一校验（[connection.go:L260-L263](file:///Users/shenjiangwei/Work/code/matrixone/pkg/frontend/connection.go#L260-L263)）
   - 端口数字校验（[connection.go:L255-L259](file:///Users/shenjiangwei/Work/code/matrixone/pkg/frontend/connection.go#L255-L259)）
   - `escapeConnectionSQLLiteral` 转义函数覆盖 \n \r \0 \t \b ' 等字符

4. **引擎层接入点正确**
   - 在 `db.relation()` 中识别 `item.Kind == catalog.SystemForeignRel` 并分流（[txn_database.go:L143-L151](file:///Users/shenjiangwei/Work/code/matrixone/pkg/vm/engine/disttae/txn_database.go#L143-L151)）
   - 分流点位于 `newTxnTable()` 调用之前，符合"不在 txnTable 内部堆积 foreign 分支"的原则

#### 需要改进

1. **`newForeignTxnTable` 是占位实现**

   ```go
   // pkg/vm/engine/disttae/txn_table_foreign.go
   func newForeignTxnTable(...) (engine.Relation, error) {
       return newTxnTable(ctx, db, item)
   }
   ```

   **问题**：当前 foreign table 和普通 table 在执行层面没有任何区别，这会导致 foreign table 实际查询时会走 disttae 的本地数据路径，而不是远端数据源。

   **建议**：
   - 短期内通过 feature flag 或 table property 控制是否启用真正的 foreign reader
   - 在 `pkg/vm/engine/federated/` 下创建 `relation.go` / `reader.go` / `types.go` 占位骨架
   - `newForeignTxnTable` 改为调用 `federated.NewRelation()` 并返回明确的 `not implemented` 或空数据错误

2. **SQL 拼接风险**

   虽然有 `escapeConnectionSQLLiteral` 转义，但仍是字符串拼接模式：

   ```go
   // pkg/frontend/connection.go
   return fmt.Sprintf(
       "insert into %s.%s(connection_name, ...) values ('%s', ...);",
       ..., escapeConnectionSQLLiteral(connectionName), ...,
   )
   ```

   **建议**：
   - 尽快确认 `BackgroundExec` 是否支持参数化执行
   - 如不支持，在 `pkg/frontend/` 下创建 `connectionSqlBuilder` 工具类，集中所有 SQL 拼接逻辑，减少散点遗漏

3. **权限模型未明确**

   ```go
   // pkg/frontend/authenticate.go
   case *tree.CreateConnection, *tree.DropConnection:
       objType = objectTypeNone
       kind = privilegeKindNone
   ```

   **问题**：`privilegeKindNone` 意味着任何人都可以创建/删除 connection。

   **建议**：设计 `CREATE CONNECTION` / `USAGE ON CONNECTION` 权限体系，并在 authenticate 中实现。

---

### 3.2 `feb273f401` - fix: reject unsupported foreign table options

#### 亮点

1. 在 `build_ddl.go` 中新增对 foreign table options 的显式拒绝逻辑，避免"静默忽略"导致用户预期偏差
2. 对应测试用例覆盖（[foreign_table_guardrails_test.go](file:///Users/shenjiangwei/Work/code/matrixone/pkg/sql/plan/foreign_table_guardrails_test.go)）

#### 需要补充

1. **DDL 子句拦截范围不完整**

   当前只拦截了部分 table options，但 foreign table 还应该显式拒绝：
   - `INDEX` / `KEY` 子句
   - `PARTITION` 子句
   - `CLUSTER BY` 子句
   - `FOREIGN KEY` 约束
   - `CHECK` 约束（如果有）

   **建议**：在 `build_ddl.go` 中新增 `checkForeignTableDDLConstraints()` 函数，系统性拦截这些子句。

2. **错误提示可更友好**

   建议在拒绝错误中加入"当前支持的选项"或"查阅文档"的提示，例如：

   ```go
   return moerr.NewInternalError(ctx, "FOREIGN TABLE does not support PARTITION clause. See documentation for supported options.")
   ```

---

### 3.3 `ccabd756b4` - fix: harden connection SQL literal escaping

#### 亮点

1. 在 `frontend/connection.go` 引入 `connectionSQLLiteralEscaper`，覆盖标准 SQL 转义字符：
   - `\n`, `\r`, `\r\n`, `\0`, `\b`, `\t`, `\'`, `\\`
   - 同时处理了 MySQL 特有的 `\Z` (Ctrl+Z)

2. Parser 层（`tree/connection.go`）同步了 `connectionStringLiteralEscaper`，两侧转义逻辑一致

3. 配套单测覆盖了常见转义场景（[connection_test.go](file:///Users/shenjiangwei/Work/code/matrixone/pkg/frontend/connection_test.go)）

#### 进一步建议

1. **仍为字符串拼接，建议参数化**

   当前：
   ```go
   escapeConnectionSQLLiteral(value)
   ```
   未来应改为：
   ```go
   // 伪代码
   bh.Exec(ctx, "INSERT INTO ... VALUES (?, ?, ...)", name, type, options, ...)
   ```

2. **统一转义函数**

   目前 `frontend/connection.go` 和 `tree/connection.go` 各有一个 `*LiteralEscaper`。建议抽取到 `pkg/common/escape/` 或 `pkg/frontend/utils.go` 统一管理，避免维护分歧。

---

### 3.4 `61db10e2c0` - fix: polish federated query connection handling

#### 亮点

1. 补充了 `stmt_kind.go` 中的 statement 类型映射，使 `ShowCreateConnection` 正确返回 DDL 类型
2. Parser 测试覆盖完善
3. `tree/connection.go` 中的 `writeConnectionQuotedString` 使用统一的 `connectionStringLiteralEscaper`

#### 建议补充的测试场景

| 场景 | 说明 |
|------|------|
| 超长 connection name | 验证 name 长度限制 |
| 非 UTF-8 字符 | 验证字符集处理 |
| 含控制字符的 password | 验证转义后的往返一致性 |
| connection name 大小写 | `mysql` vs `MySQL` 是否区分 |
| 重复 create / drop | 幂等性验证 |

---

## 4. 与设计文档的一致性核对

| 设计文档要求 | 当前实现状态 |
|-------------|-------------|
| `SystemForeignRel = "f"` | ✅ 已实现 |
| Connection 与表对象分离 | ✅ mo_connections 独立 |
| ForceOneCN = true | ⚠️ 占位符已预留，但强制执行点未实现 |
| planner 绑定为 TABLE_SCAN | ✅ foreign table 仍走 TABLE_SCAN |
| `Ranges()` / `BuildReaders()` 占位 | ❌ 尚未实现占位骨架 |
| DML guardrail | ✅ INSERT/UPDATE/DELETE 已拦截 |
| SHOW CREATE CONNECTION 脱敏 | ✅ password 显示为 `***` |
| 必填项校验（host/port/user/password） | ✅ 已实现 |
| Oracle service_name/sid 校验 | ✅ 已实现 |
| 租户 upgrade 链路 | ✅ 已接入 |

---

## 5. 后续行动项

### 高优先级（阻塞后续阶段）

| # | 行动项 | 理由 |
|---|--------|------|
| 1 | 在 `pkg/vm/engine/federated/` 下创建 `relation.go` / `reader.go` / `types.go` 占位骨架 | 为后续 adapter 接入提供稳定接口 |
| 2 | 将 connection SQL 拼接升级为参数化执行或集中到 `connectionSqlBuilder` | 减少 SQL 注入风险 |
| 3 | 在 `build_ddl.go` 中增加 FOREIGN TABLE 的 DDL 子句完整拦截 | 防止用户误用不支持的语法 |

### 中优先级（提升质量）

| # | 行动项 | 理由 |
|---|--------|------|
| 4 | 设计并实现 CONNECTION 权限体系 | 当前任何人都可创建 connection |
| 5 | 抽取统一的转义函数到公共模块 | 避免 frontend/tree 两处维护 |
| 6 | 增加边界测试（超长 name、非 UTF-8、控制字符） | 提升鲁棒性 |

### 低优先级（为后续阶段预留）

| # | 行动项 | 理由 |
|---|--------|------|
| 7 | 在 connection options 中预留 `test_connection` / 连接池参数 | 下一阶段需要 |
| 8 | 确认 `getSqlForDropConnection` 中 `order by connection_id` 的必要性 | 罕见写法，可能可简化 |
| 9 | 在 `engine.RelData` 接口处预留 `FederatedRelData` 扩展点注释 | 为 multi-CN foreign scan 做准备 |

---

## 6. 结论

**第一阶段评审结论**：✅ **基本达标，建议继续推进**

这 4 个 commit 成功完成了联邦查询第一阶段的核心目标：
- 元数据层（`mo_connections` / `SystemForeignRel`）建设完成
- CONNECTION 控制面（CREATE / DROP / SHOW）可跑通
- 基础的 planner guardrail（DML 拦截 / options 校验）已落地
- 引擎层的 relation 解析分流点已预留

**在进入第二阶段（foreign reader 骨架 + mock connector）之前**，建议优先完成：
1. `pkg/vm/engine/federated/` 占位骨架（为 adapter 接入提供接口锚点）
2. SQL 拼接安全加固（参数化或集中 SQL 生成）
3. DDL 子句完整拦截（index / partition / foreign key 等）

---

*本评审文档生成于 2026-04-01，基于 `add_fq_doc` 分支最新 4 个 commit。*
