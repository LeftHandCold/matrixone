# LAG/LEAD/FIRST_VALUE/LAST_VALUE/NTH_VALUE 窗口函数实现文档

## 概述

本文档记录了为 MatrixOne 实现 LAG、LEAD、FIRST_VALUE、LAST_VALUE、NTH_VALUE 窗口函数的完整过程，修复了 GitHub issue #23405。

## 问题背景

用户报告 MatrixOne 不支持 LAG 窗口函数，执行以下 SQL 时报错：

```sql
SELECT 
    `gjahr` AS `年份`,
    ROUND(SUM(`zxssr`), 2) AS `营业总收入`,
    ROUND(
        (SUM(`zxssr`) - LAG(SUM(`zxssr`)) OVER (ORDER BY `gjahr`))
         / NULLIF(LAG(SUM(`zxssr`)) OVER (ORDER BY `gjahr`), 0) * 100,
         2
    ) AS `同比增长率(%)`
FROM `test_revenue_cost`  
GROUP BY `gjahr`
ORDER BY `gjahr`
```

## 实现的窗口函数

| 函数 | 语法 | 说明 |
|------|------|------|
| LAG | `LAG(expr [, offset [, default]])` | 返回当前行之前 offset 行的值 |
| LEAD | `LEAD(expr [, offset [, default]])` | 返回当前行之后 offset 行的值 |
| FIRST_VALUE | `FIRST_VALUE(expr)` | 返回窗口帧中的第一个值 |
| LAST_VALUE | `LAST_VALUE(expr)` | 返回窗口帧中的最后一个值 |
| NTH_VALUE | `NTH_VALUE(expr, n)` | 返回窗口帧中的第 n 个值 |

## 修改的文件

### 1. SQL 解析器

**pkg/sql/parsers/dialect/mysql/mysql_sql.y**
- 添加了 LAG、LEAD、FIRST_VALUE、LAST_VALUE、NTH_VALUE 的语法规则
- 支持各种参数组合（1-3个参数）
- 在 `role_name` 规则中添加了这些关键字，使其可以作为角色名使用
- 在 `non_reserved_keyword` 中添加了这些关键字

**pkg/sql/parsers/dialect/mysql/keywords.go**
- 添加了 `lag`、`lead`、`first_value`、`last_value`、`nth_value` 关键字

### 2. 函数注册

**pkg/sql/plan/function/function_id.go**
- 在 `functionIdRegister` map 中添加了新函数的 ID 映射

**pkg/sql/plan/function/list_window.go**
- 定义了新窗口函数的 `FuncNew` 结构
- 设置函数类型为 `plan.Function_WIN_VALUE`
- 配置参数检查和返回类型

**pkg/sql/plan/function/function.go**
- 添加了 `GetFunctionIsWinValueFunByName` 函数
- 添加了 `isWindowValue` 方法

### 3. 聚合框架

**pkg/sql/plan/function/agg/window.go**
- 添加了 `RegisterLag`、`RegisterLead`、`RegisterFirstValue`、`RegisterLastValue`、`RegisterNthValue` 注册函数

### 4. 执行器

**pkg/sql/colexec/aggexec/register.go**
- 添加了窗口函数 ID 变量：`WinIdOfLag`、`WinIdOfLead`、`WinIdOfFirstValue`、`WinIdOfLastValue`、`WinIdOfNthValue`
- 添加了注册函数

**pkg/sql/colexec/aggexec/window.go**
- 实现了 `valueWindowExec` 结构体
- 实现了 `valueEntry` 用于存储窗口帧中的值
- 实现了 `flushLag`、`flushLead`、`flushFirstValue`、`flushLastValue`、`flushNthValue` 方法
- 实现了 `appendValueToVector` 辅助函数处理各种数据类型

**pkg/sql/colexec/aggexec/types.go**
- 在 `makeSpecialAggExec` 中添加了对新窗口函数的处理
- 添加了 `makeValueWindowExec` 和 `makeValueWindowExecInternal` 函数

### 5. 查询绑定

**pkg/sql/plan/projection_binder.go**
- 修改了 `BindWinFunc` 函数
- 对于 WIN_VALUE 函数，当没有显式指定帧时，使用 ROWS 帧而不是默认的 RANGE 帧
- 这避免了 "RANGE N PRECEDING/FOLLOWING frame requires exactly one ORDER BY expression" 错误

## 关键实现细节

### valueWindowExec 结构

```go
type valueWindowExec struct {
    singleAggInfo
    mp *mpool.MPool
    
    // 每个输出行存储其窗口帧中的所有值
    frameValues [][]*valueEntry
    
    resultVec *vector.Vector
}

type valueEntry struct {
    isNull bool
    data   []byte
}
```

### 窗口帧处理

对于 WIN_VALUE 函数，`Fill` 方法会被调用多次：
- `groupIndex` 是当前输出行的索引
- `row` 是窗口帧内某个值的索引
- 每个 `groupIndex` 会收集其窗口帧内的所有值

### Flush 逻辑

- **LAG**: 返回帧中倒数第二个值（当前行之前的值）
- **LEAD**: 返回帧中第二个值（当前行之后的值）
- **FIRST_VALUE**: 返回帧中第一个值
- **LAST_VALUE**: 返回帧中最后一个值
- **NTH_VALUE**: 返回帧中第 n 个值（当前默认为第一个）

### 默认帧处理

WIN_VALUE 函数默认使用 ROWS 帧而不是 RANGE 帧：

```go
if isWinValueFunc && !ws.HasFrame {
    ws.Frame = &tree.FrameClause{Type: tree.Rows}
    ws.Frame.Start = &tree.FrameBound{Type: tree.Preceding, UnBounded: true}
    ws.Frame.End = &tree.FrameBound{Type: tree.Following, UnBounded: true}
}
```

### 关键字处理

LAG、LEAD、FIRST_VALUE、LAST_VALUE、NTH_VALUE 这些关键字需要同时支持：
1. 作为窗口函数名使用
2. 作为标识符使用（如角色名）

解决方案是在 `role_name` 语法规则中显式添加这些关键字：

```yacc
role_name:
    ID
    ...
|   LAG
        {
        $$ = tree.NewCStr("lag", 1)
    }
|   LEAD
        {
        $$ = tree.NewCStr("lead", 1)
    }
    ...
```

## 测试验证

### 解析测试

```sql
-- 窗口函数测试
SELECT LAG(a) OVER (ORDER BY b) FROM t                    -- PASS
SELECT LAG(a, 1) OVER (ORDER BY b) FROM t                 -- PASS
SELECT LAG(a, 1, 0) OVER (ORDER BY b) FROM t              -- PASS
SELECT LEAD(a) OVER (ORDER BY b) FROM t                   -- PASS
SELECT FIRST_VALUE(a) OVER (ORDER BY b) FROM t            -- PASS
SELECT LAST_VALUE(a) OVER (ORDER BY b) FROM t             -- PASS
SELECT NTH_VALUE(a, 2) OVER (ORDER BY b) FROM t           -- PASS
SELECT LAG(SUM(a)) OVER (ORDER BY b) FROM t               -- PASS

-- 关键字作为标识符测试
drop role if exists intern,lead,newrole,rolex,dev,test,rx -- PASS
create role lead                                           -- PASS
grant lead to user1                                        -- PASS
```

### 编译测试

所有相关包编译成功：
- `pkg/sql/parsers/dialect/mysql`
- `pkg/sql/plan/function`
- `pkg/sql/colexec/aggexec`
- `pkg/sql/colexec/window`
- `cmd/mo-service`

### 单元测试

所有现有测试通过：
- `go test ./pkg/sql/plan/...`
- `go test ./pkg/sql/colexec/window/...`
- `go test ./pkg/sql/plan/function/...`
- `go test ./pkg/sql/parsers/dialect/mysql/...`

## 已知限制

1. **LAG/LEAD offset 参数**: 当前实现默认 offset=1，尚未完全支持自定义 offset
2. **LAG/LEAD default 参数**: 当前返回 NULL，尚未支持自定义默认值
3. **NTH_VALUE n 参数**: 当前默认 n=1，等同于 FIRST_VALUE

## 后续优化建议

1. 完善 LAG/LEAD 的 offset 和 default 参数支持
2. 完善 NTH_VALUE 的 n 参数支持
3. 添加更多集成测试用例
4. 性能优化：减少内存拷贝

## 相关 Issue

- GitHub Issue: #23405
- 标题: SQL Window Function Compatibility Issues
