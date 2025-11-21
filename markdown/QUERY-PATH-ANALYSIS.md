# 查询路径分析

## 查询执行流程

### 1. 查询入口
```
用户SQL查询
  ↓
compileQuery (compile.go:827)
  ↓
compilePlanScope (compile.go:873)
  ↓
compileDataSource (scope.go:1039)
  ↓
getRelData (scope.go:566)
```

### 2. getRelData 调用路径

**位置：** `pkg/sql/compile/scope.go:566`

```go
func (s *Scope) getRelData(c *Compile, blockExprList []*plan.Expr) error {
    // ...
    
    if s.NodeInfo.CNCNT == 1 {
        // 单CN场景
        s.NodeInfo.Data, err = c.expandRanges(
            ...,
            engine.Policy_CollectAllData,  // 包含 Policy_CollectCommittedPersistedData
            ...)
    } else {
        // 多CN场景
        commited, err = c.expandRanges(
            ...,
            engine.Policy_CollectCommittedPersistedData,  // 只收集已提交的持久化数据
            ...)
    }
}
```

### 3. expandRanges 调用路径

**位置：** `pkg/sql/compile/compile.go:4151`

```go
func (c *Compile) expandRanges(
    n *plan.Node, rel engine.Relation, db engine.Database, ctx context.Context,
    blockFilterList []*plan.Expr, policy engine.DataCollectPolicy, rsp *engine.RangesShuffleParam) (engine.RelData, error) {
    
    // 设置 PreAllocBlocks
    preAllocBlocks := 2
    if policy&engine.Policy_CollectCommittedPersistedData != 0 {
        // 如果 Policy_CollectCommittedPersistedData 为 true，设置更大的 PreAllocBlocks
        if !c.IsTpQuery() {
            if len(blockFilterList) > 0 {
                preAllocBlocks = 64
            } else {
                preAllocBlocks = int(n.Stats.BlockNum)
                if rsp != nil {
                    preAllocBlocks = preAllocBlocks / int(rsp.CNCNT)
                }
            }
        }
    }
    
    // 构建 RangesParam
    rangesParam := engine.RangesParam{
        BlockFilters:       blockFilterList,
        PreAllocBlocks:     preAllocBlocks,
        TxnOffset:          c.TxnOffset,
        Policy:             policy,  // 传入的 policy
        Rsp:                rsp,
        DontSupportRelData: false,
    }
    
    // 调用 rel.Ranges
    relData, err := rel.Ranges(newCtx, rangesParam)
    return relData, nil
}
```

### 4. Ranges 方法调用路径

**位置：** `pkg/vm/engine/disttae/txn_table.go:634`

```go
func (tbl *txnTable) Ranges(ctx context.Context, rangesParam engine.RangesParam) (data engine.RelData, err error) {
    // 判断使用哪个路径
    if len(rangesParam.BlockFilters) == 0 && rangesParam.PreAllocBlocks > 128 && !rangesParam.DontSupportRelData {
        // 条件满足：无 block filters，PreAllocBlocks > 128，支持 RelData
        // 使用 getObjList 路径（返回对象列表，而不是块列表）
        return tbl.getObjList(ctx, rangesParam)
    }
    // 否则使用 doRanges 路径（返回块列表）
    return tbl.doRanges(ctx, rangesParam)
}
```

## Policy 设置分析

### Policy 定义

**位置：** `pkg/vm/engine/types.go:725-735`

```go
type DataCollectPolicy uint64

const (
    Policy_CollectCommittedInmemData = 1 << iota      // 1
    Policy_CollectUncommittedInmemData               // 2
    Policy_CollectCommittedPersistedData              // 4
    Policy_CollectUncommittedPersistedData            // 8
    Policy_CollectCommittedData   = Policy_CollectCommittedInmemData | Policy_CollectCommittedPersistedData  // 5
    Policy_CollectUncommittedData = Policy_CollectUncommittedInmemData | Policy_CollectUncommittedPersistedData  // 10
    Policy_CollectAllData         = Policy_CollectCommittedData | Policy_CollectUncommittedData  // 15
)
```

### Policy 使用场景

#### 场景1：单CN场景（CNCNT == 1）

**位置：** `pkg/sql/compile/scope.go:572-586`

```go
if s.NodeInfo.CNCNT == 1 {
    s.NodeInfo.Data, err = c.expandRanges(
        ...,
        engine.Policy_CollectAllData,  // = 15 (包含所有数据)
        ...)
}
```

**Policy_CollectAllData = 15**，包含：
- `Policy_CollectCommittedInmemData` (1)
- `Policy_CollectUncommittedInmemData` (2)
- `Policy_CollectCommittedPersistedData` (4)
- `Policy_CollectUncommittedPersistedData` (8)

**所以 `Policy_CollectCommittedPersistedData` 为 true**

#### 场景2：多CN场景（CNCNT > 1）

**位置：** `pkg/sql/compile/scope.go:611-618`

```go
commited, err = c.expandRanges(
    ...,
    engine.Policy_CollectCommittedPersistedData,  // = 4 (只收集已提交的持久化数据)
    ...)
```

**Policy_CollectCommittedPersistedData = 4**，只包含：
- `Policy_CollectCommittedPersistedData` (4)

**所以 `Policy_CollectCommittedPersistedData` 为 true**

### 用户说的"不是快照读"

用户说 `Policy_CollectCommittedPersistedData` 不是 true，因为这不是快照读。

**可能的情况：**
1. 用户查询的场景中，Policy 可能不包含 `Policy_CollectCommittedPersistedData`
2. 或者用户查询的是未提交的数据，使用 `Policy_CollectUncommittedData`

**但根据代码：**
- 单CN场景使用 `Policy_CollectAllData`，包含 `Policy_CollectCommittedPersistedData`
- 多CN场景使用 `Policy_CollectCommittedPersistedData`

**所以正常情况下，`Policy_CollectCommittedPersistedData` 应该是 true**

## getObjList vs doRanges 选择条件

**位置：** `pkg/vm/engine/disttae/txn_table.go:635-639`

```go
if len(rangesParam.BlockFilters) == 0 && rangesParam.PreAllocBlocks > 128 && !rangesParam.DontSupportRelData {
    return tbl.getObjList(ctx, rangesParam)  // 使用 getObjList
}
return tbl.doRanges(ctx, rangesParam)  // 使用 doRanges
```

**选择条件：**
1. `len(rangesParam.BlockFilters) == 0`：无 block filters
2. `rangesParam.PreAllocBlocks > 128`：预分配块数 > 128
3. `!rangesParam.DontSupportRelData`：支持 RelData

**从日志看：**
- `bmsql_oorder` 使用了 `getObjList`（满足条件）
- `bmsql_order_line` 使用了 `doRanges`（不满足条件，可能是 PreAllocBlocks <= 128 或有 block filters）

## doRanges 中 pState 获取逻辑

**位置：** `pkg/vm/engine/disttae/txn_table.go:816-860`

```go
// 第816行：如果 Policy_CollectCommittedPersistedData 为 true，获取 pState
if rangesParam.Policy&engine.Policy_CollectCommittedPersistedData != 0 {
    if part, err = tbl.getPartitionState(ctx); err != nil {
        return
    }
}

// 第830行：执行 rangesOnePart（可能耗时）
if err = tbl.rangesOnePart(...); err != nil {
    return
}

// 第842行：如果 part == nil，重新获取 pState
if part == nil {
    if part, err = tbl.getPartitionState(ctx); err != nil {
        return
    }
}

// 第857行：创建 RelData，使用当前的 part
blklist := readutil.NewBlockListRelationData(
    0,
    readutil.WithPartitionState(part))
```

**关键问题：**
- 如果 `Policy_CollectCommittedPersistedData` 为 false，第816行的条件不满足，`part` 保持为 nil
- 第842行会重新获取 pState
- 但如果 `Policy_CollectCommittedPersistedData` 为 false，说明不需要收集已提交的持久化数据
- 那么获取 pState 的目的是什么？

**可能的原因：**
- 即使不需要收集已提交的持久化数据，仍然需要 pState 来处理 tombstone 或其他逻辑
- 或者代码逻辑有问题，应该根据实际需求决定是否获取 pState

## 总结

1. **查询路径：** `getRelData` -> `expandRanges` -> `rel.Ranges` -> `getObjList` 或 `doRanges`
2. **Policy 设置：**
   - 单CN：`Policy_CollectAllData`（包含 `Policy_CollectCommittedPersistedData`）
   - 多CN：`Policy_CollectCommittedPersistedData`
3. **getObjList vs doRanges：**
   - 取决于 `BlockFilters`、`PreAllocBlocks`、`DontSupportRelData`
4. **doRanges 中 pState 获取：**
   - 如果 `Policy_CollectCommittedPersistedData` 为 true，第816行获取
   - 否则第842行获取（如果 part == nil）









