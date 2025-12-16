# 多CN环境下 EXCEPT 查询数据不一致问题分析

## 概述

本文档详细分析了在多CN环境下执行 `EXCEPT` 查询时出现数据不一致问题的根本原因，包括执行路径、可取消节点、以及为什么 CN1 cancel 后 CN0 仍返回错误数据。

## 问题描述

### 问题现象

在 TPCC 测试中，频繁执行以下 SQL 查询：

```sql
(SELECT o_w_id, o_d_id, SUM(o_ol_cnt) 
 FROM bmsql_oorder 
 GROUP BY o_w_id, o_d_id)
EXCEPT
(SELECT ol_w_id, ol_d_id, COUNT(ol_o_id) 
 FROM bmsql_order_line 
 GROUP BY ol_w_id, ol_d_id);
```

**预期结果**：应该返回 0 行（数据一致时）。

**实际结果**：偶尔返回非 0 行，但过一段时间再查询又变成 0 行。

**问题特征**：
- 数据本身没有问题（后续查询正常）
- 只在多CN执行时出现
- 通常与网络错误或 CN cancel 相关
- 可能是中间状态未正确处理

### 相关 Issue

- GitHub Issue: https://github.com/matrixorigin/matrixone/issues/22727

---

## EXCEPT 查询执行路径分析

### 1. 整体执行流程

```
CN0 (协调节点)
  ├─ 编译查询，决定执行类型（单CN/多CN）
  ├─ 如果是多CN，获取 CN 列表（CN0, CN1）
  ├─ 为每个 CN 分配数据块（TableScan）
  │
  ├─ 左查询 (LEFT QUERY)
  │   ├─ CN0: TableScan → Aggregate → [发送到 CN0]
  │   └─ CN1: TableScan → Aggregate → Connector → [发送到 CN0]
  │
  ├─ 右查询 (RIGHT QUERY)
  │   ├─ CN0: TableScan → Aggregate → [发送到 CN0]
  │   └─ CN1: TableScan → Aggregate → Connector → [发送到 CN0]
  │
  └─ EXCEPT (Minus)
      ├─ buildHashTable: 从右查询构建哈希表
      └─ probeHashTable: 用左查询探测哈希表
```

### 2. CN1 执行路径（远程 CN）

CN1 作为远程 CN，执行路径如下：

```
CN1 接收请求 (RemoteRun)
  ├─ TableScan (扫描分配给 CN1 的数据块)
  │   ├─ collectTombstones: 收集墓碑（删除标记）
  │   └─ expandRanges: 收集数据块列表
  │
  ├─ Aggregate (GROUP BY + SUM/COUNT)
  │   └─ 处理 CN1 分配的数据块
  │
  └─ Connector (发送数据回 CN0)
      ├─ vm.ChildrenCall: 从子操作符获取 batch
      ├─ connector.ctr.sp.SendBatch: 发送到 spool
      └─ connector.Reg.Ch2: 通知 CN0 接收数据
```

### 3. 关键代码位置

#### 3.1 数据收集阶段

**文件**: `pkg/sql/compile/compile.go`

**函数**: `collectTombstones`
- **作用**：收集表的墓碑（删除标记）
- **多CN影响**：不同 CN 可能看到不同的墓碑状态（同步延迟）

**函数**: `expandRanges` / `getRelData`
- **作用**：收集数据块列表
- **多CN影响**：不同 CN 可能看到不同的块列表（新提交的块未同步）

#### 3.2 远程执行阶段

**文件**: `pkg/sql/compile/remoterunServer.go`

**函数**: `receiveMessageFromCnServerIfConnector`
- **作用**：CN1 接收来自 CN0 的远程执行请求
- **关键**：启动 pipeline 执行

#### 3.3 数据发送阶段

**文件**: `pkg/sql/colexec/connector/connector.go`

**函数**: `Connector.Call`
- **作用**：CN1 将处理后的数据发送回 CN0
- **关键代码**：
  ```go
  result, err := vm.ChildrenCall(connector.GetChildren(0), proc, connector.OpAnalyzer)
  // ... 处理 batch ...
  connector.ctr.sp.SendBatch(proc.Ctx, 0, result.Batch, nil)
  connector.Reg.Ch2 <- process.NewPipelineSignalToGetFromSpool(...)
  ```

#### 3.4 EXCEPT 操作阶段

**文件**: `pkg/sql/colexec/minus/minus.go`

**函数**: `Minus.Call`
- **状态1**: `buildingHashMap` - 构建右查询的哈希表
- **状态2**: `probingHashMap` - 用左查询探测哈希表
- **状态3**: `operatorEnd` - 操作结束

**函数**: `buildHashTable`
- **作用**：从右查询的所有 batch 构建哈希表
- **关键**：必须接收**所有**右查询的数据

**函数**: `probeHashTable`
- **作用**：用左查询的 batch 探测哈希表，返回不在右查询中的行
- **关键**：必须接收**所有**左查询的数据

---

## 可取消节点分析

### 1. 哪些节点可以被 Cancel？

在 CN1 的执行路径中，以下节点可能被取消：

#### 1.1 TableScan 阶段
- **位置**: `pkg/sql/compile/scope.go` - `getRelData` / `expandRanges`
- **取消条件**: 
  - Context 被取消 (`proc.Ctx.Done()`)
  - 网络错误导致数据收集失败
- **影响**: CN1 无法获取完整的数据块列表

#### 1.2 Aggregate 阶段
- **位置**: `pkg/sql/colexec/aggregate/aggregate.go`
- **取消条件**:
  - Context 被取消
  - 上游操作符返回 `vm.CancelResult`
- **影响**: CN1 无法完成聚合计算

#### 1.3 Connector 阶段 ⭐ **关键**
- **位置**: `pkg/sql/colexec/connector/connector.go` - `Call`
- **取消条件**:
  - Context 被取消（网络断开、超时等）
  - 返回 `vm.CancelResult` 模拟取消
- **影响**: **CN1 停止发送数据到 CN0**

### 2. Cancel 模拟代码

**文件**: `pkg/sql/colexec/connector/connector.go`

```go
func (connector *Connector) Call(proc *process.Process) (vm.CallResult, error) {
    result, err := vm.ChildrenCall(connector.GetChildren(0), proc, connector.OpAnalyzer)
    if err != nil {
        return result, err
    }

    // [TEST CODE] Simulate CN cancel: randomly return CancelResult
    // This simulates the scenario where a remote CN gets canceled during execution
    if connector.Reg != nil && connector.Reg.Ch2 != nil {
        // 30% probability to simulate cancel
        if rand.Float32() < 0.30 && result.Batch != nil && !result.Batch.IsEmpty() {
            logutil.Warnf("[TEST CODE] Simulating CN cancel on remote CN: returning CancelResult")
            return vm.CancelResult, nil
        }
    }

    // ... 正常发送逻辑 ...
}
```

**说明**：
- 在 `Connector.Call` 中，从子操作符获取 batch 后，有 30% 概率返回 `vm.CancelResult`
- 这模拟了 CN1 在发送数据过程中被取消的场景
- 返回 `CancelResult` 后，CN1 停止发送后续数据

---

## 为什么 CN1 Cancel 后 CN0 仍返回错误数据？

### 1. 问题根源

**核心问题**：CN0 在 CN1 cancel 后，**没有检测到数据不完整**，继续使用已接收的部分数据执行 EXCEPT 操作，导致结果错误。

### 2. 详细分析

#### 2.1 数据收集阶段的不一致性

**场景1：Tombstone 同步延迟**

**文件**: `pkg/sql/compile/compile.go` - `collectTombstones`

**问题**：不同 CN 看到的墓碑状态可能不同
- CN0 看到：某些行已被删除（有墓碑）
- CN1 看到：这些行还未删除（墓碑未同步）

**影响**：
- CN0 的 TableScan 跳过已删除的行
- CN1 的 TableScan 包含这些行
- 导致左右查询的数据集不一致

**模拟代码**（已添加）：
```go
// [TEST CODE] Simulate tombstone synchronization delay
if rand.Float32() < 0.3 { // 30% probability
    delay := time.Duration(10+rand.Intn(40)) * time.Millisecond
    logutil.Warnf("[TEST CODE] Simulating tombstone sync delay: %vms", delay.Milliseconds())
    time.Sleep(delay)
}
```

**场景2：Block 对齐延迟**

**文件**: `pkg/sql/compile/scope.go` - `getRelData`

**问题**：不同 CN 看到的数据块列表可能不同
- CN0 看到：包含新提交的块
- CN1 看到：不包含新提交的块（未同步）

**影响**：
- CN0 扫描了更多/更少的块
- CN1 扫描了不同数量的块
- 导致左右查询的数据集不一致

**模拟代码**（已添加）：
```go
// [TEST CODE] Simulate block alignment delay
if s.IsRemote && rand.Float32() < 0.25 { // 25% probability for remote CNs
    delay := time.Duration(20+rand.Intn(60)) * time.Millisecond
    logutil.Warnf("[TEST CODE] Simulating block sync delay: %vms", delay.Milliseconds())
    time.Sleep(delay)
}
```

#### 2.2 CN1 Cancel 后的数据流

**正常流程**：
```
CN1: TableScan → Aggregate → Connector → [发送所有 batch 到 CN0]
CN0: [接收所有 batch] → Minus.buildHashTable / probeHashTable
```

**CN1 Cancel 后的流程**：
```
CN1: TableScan → Aggregate → Connector → [发送部分 batch] → Cancel
CN0: [接收部分 batch] → Minus.buildHashTable / probeHashTable → ❌ 错误结果
```

**关键问题**：

1. **CN0 不知道 CN1 已 Cancel**
   - CN1 返回 `vm.CancelResult` 后，停止发送数据
   - 但 CN0 可能已经接收了部分数据
   - CN0 的 `PipelineSignalReceiver` 可能将 CN1 标记为 `done`，但**没有错误处理**

2. **EXCEPT 操作继续执行**
   - `Minus.buildHashTable` 只使用已接收的右查询数据构建哈希表
   - `Minus.probeHashTable` 只使用已接收的左查询数据探测哈希表
   - **缺少完整性检查**：没有验证是否接收了所有 CN 的所有数据

3. **数据不完整导致错误结果**
   - 如果 CN1 在发送右查询数据时 cancel，哈希表不完整
   - 如果 CN1 在发送左查询数据时 cancel，探测不完整
   - 最终返回的结果基于不完整的数据集

### 3. 代码层面的问题

#### 3.1 缺少 Cancel 检测

**文件**: `pkg/vm/process/process_spoolr.go` - `PipelineSignalReceiver.GetNextBatch`

**问题**：当远程 CN cancel 时，`ReceiverDone` 被设置为 `true`，但**没有错误传播**。

**当前逻辑**：
```go
if receiver.ReceiverDone {
    // 标记为 done，但继续处理其他 receiver 的数据
    // 没有检查是否所有 receiver 都正常完成
}
```

**应该的逻辑**：
```go
if receiver.ReceiverDone {
    // 检查是否是异常 cancel（非正常结束）
    if receiver.Err != nil {
        return nil, receiver.Err  // 传播错误
    }
    // 或者检查是否所有 receiver 都正常完成
    if !allReceiversDoneNormally {
        return nil, moerr.NewInternalError("incomplete data from remote CN")
    }
}
```

#### 3.2 EXCEPT 操作缺少完整性验证

**文件**: `pkg/sql/colexec/minus/minus.go`

**问题**：`buildHashTable` 和 `probeHashTable` 没有验证是否接收了所有数据。

**当前逻辑**：
```go
func (minus *Minus) buildHashTable(...) error {
    for {
        input, err := vm.ChildrenCall(...)
        if input.Batch == nil {
            break  // 假设所有数据已接收
        }
        // ... 构建哈希表 ...
    }
}
```

**应该的逻辑**：
```go
func (minus *Minus) buildHashTable(...) error {
    expectedBatches := getExpectedBatchCount()  // 从 CN 列表计算
    receivedBatches := 0
    for {
        input, err := vm.ChildrenCall(...)
        if input.Batch == nil {
            break
        }
        receivedBatches++
        // ... 构建哈希表 ...
    }
    if receivedBatches < expectedBatches {
        return moerr.NewInternalError("incomplete data: expected %d batches, got %d", 
            expectedBatches, receivedBatches)
    }
}
```

#### 3.3 网络错误处理不完善

**文件**: `pkg/queryservice/query_service.go` - `RequestMultipleCn`

**问题**：当某个 CN 返回错误时，错误被记录但**查询继续执行**。

**当前逻辑**（测试代码）：
```go
if err != nil {
    logutil.Warningf("[MULTI-CN] Error from CN %s: %v", addr, err)
    failedNodes = append(failedNodes, addr)
    // 继续处理其他 CN 的响应
}
```

**应该的逻辑**：
```go
if err != nil {
    // 对于 EXCEPT 等需要完整数据的操作，应该立即返回错误
    if requiresCompleteData {
        return moerr.NewInternalError("incomplete data from CN %s: %v", addr, err)
    }
    // 对于可以容忍部分失败的操作，可以继续
}
```

---

## 模拟 Bug 的方法

### 1. 强制多CN执行

**文件**: `pkg/sql/plan/stats.go` - `GetExecType`

**修改**：强制所有 AP 查询使用多CN执行
```go
func GetExecType(qry *plan.Query, txnHaveDDL bool, isPrepare bool) ExecType {
    // ... 原有逻辑 ...
    
    // Force all AP queries to use multi-CN execution
    if ret != ExecTypeTP {
        return ExecTypeAP_MULTICN
    }
    return ret
}
```

### 2. 模拟 Tombstone 同步延迟

**文件**: `pkg/sql/compile/compile.go` - `collectTombstones`

**代码**：30% 概率延迟 10-50ms
```go
if rand.Float32() < 0.3 {
    delay := time.Duration(10+rand.Intn(40)) * time.Millisecond
    time.Sleep(delay)
}
```

### 3. 模拟 Block 对齐延迟

**文件**: `pkg/sql/compile/scope.go` - `getRelData`

**代码**：25% 概率延迟 20-80ms（仅远程 CN）
```go
if s.IsRemote && rand.Float32() < 0.25 {
    delay := time.Duration(20+rand.Intn(60)) * time.Millisecond
    time.Sleep(delay)
}
```

### 4. 模拟 CN Cancel

**文件**: `pkg/sql/colexec/connector/connector.go` - `Call`

**代码**：30% 概率返回 `vm.CancelResult`
```go
if connector.Reg != nil && connector.Reg.Ch2 != nil {
    if rand.Float32() < 0.30 && result.Batch != nil && !result.Batch.IsEmpty() {
        return vm.CancelResult, nil
    }
}
```

### 5. 模拟网络错误

**文件**: `pkg/queryservice/query_service.go` - `RequestMultipleCn`

**代码**：30% 概率注入网络错误（但继续处理）
```go
if rand.Float32() < 0.30 {
    // 模拟网络错误
    err = moerr.NewInternalError("simulated network error")
    // 但继续处理其他 CN 的响应（模拟 bug）
}
```

---

## 修复建议

### 1. 添加完整性检查

在 `Minus.buildHashTable` 和 `Minus.probeHashTable` 中添加数据完整性验证：

```go
// 记录期望的 batch 数量（基于 CN 列表）
expectedBatches := calculateExpectedBatches(cnList, blockCount)

// 在接收数据时计数
receivedBatches := 0
for {
    input, err := vm.ChildrenCall(...)
    if input.Batch == nil {
        break
    }
    receivedBatches++
    // ... 处理 batch ...
}

// 验证完整性
if receivedBatches < expectedBatches {
    return moerr.NewInternalError("incomplete data: expected %d batches, got %d", 
        expectedBatches, receivedBatches)
}
```

### 2. 改进 Cancel 检测

在 `PipelineSignalReceiver.GetNextBatch` 中检测异常 cancel：

```go
if receiver.ReceiverDone {
    // 检查是否是异常结束
    select {
    case err := <-receiver.Err:
        if err != nil {
            return nil, moerr.NewInternalError("remote CN canceled: %v", err)
        }
    default:
        // 正常结束
    }
}
```

### 3. 改进错误处理

在 `RequestMultipleCn` 中，对于需要完整数据的操作，遇到错误立即返回：

```go
if err != nil {
    // 检查操作类型
    if requiresCompleteData(operation) {
        return moerr.NewInternalError("incomplete data from CN %s: %v", addr, err)
    }
    // 否则继续处理
}
```

### 4. 添加超时机制

为远程 CN 的数据接收添加超时：

```go
ctx, cancel := context.WithTimeout(proc.Ctx, 30*time.Second)
defer cancel()

// 在超时时间内等待所有数据
for !allDataReceived {
    select {
    case <-ctx.Done():
        return moerr.NewInternalError("timeout waiting for data from remote CN")
    case batch := <-receiverCh:
        // 处理 batch
    }
}
```

---

## 相关文件清单

### 核心执行文件

1. **`pkg/sql/compile/compile.go`**
   - `collectTombstones`: 收集墓碑
   - `expandRanges`: 收集数据块

2. **`pkg/sql/compile/scope.go`**
   - `getRelData`: 获取关系数据（包含块列表）

3. **`pkg/sql/colexec/connector/connector.go`**
   - `Connector.Call`: CN1 发送数据到 CN0（Cancel 模拟位置）

4. **`pkg/sql/colexec/minus/minus.go`**
   - `Minus.Call`: EXCEPT 操作主逻辑
   - `buildHashTable`: 构建右查询哈希表
   - `probeHashTable`: 探测左查询数据

5. **`pkg/vm/process/process_spoolr.go`**
   - `PipelineSignalReceiver.GetNextBatch`: 接收多个 CN 的数据

6. **`pkg/queryservice/query_service.go`**
   - `RequestMultipleCn`: 向多个 CN 发送请求

7. **`pkg/sql/colexec/dispatch/sendfunc.go`**
   - `sendBatchToClientSession`: 发送 batch 到远程 CN

### 配置和决策文件

8. **`pkg/sql/plan/stats.go`**
   - `GetExecType`: 决定执行类型（单CN/多CN）

9. **`pkg/sql/compile/compile.go`**
   - `compileQuery`: 编译查询，获取 CN 列表

---

## 总结

### 问题根本原因

1. **数据同步延迟**：不同 CN 看到的墓碑和块列表可能不同（同步延迟）
2. **Cancel 检测缺失**：CN0 无法检测到 CN1 的异常 cancel
3. **完整性验证缺失**：EXCEPT 操作没有验证是否接收了所有数据
4. **错误处理不完善**：网络错误被记录但查询继续执行

### 为什么 CN1 Cancel 后 CN0 仍返回错误数据？

1. CN1 cancel 后停止发送数据，但 CN0 已接收部分数据
2. CN0 的 `PipelineSignalReceiver` 将 CN1 标记为 `done`，但没有错误传播
3. EXCEPT 操作继续使用不完整的数据集执行
4. 缺少完整性检查，无法发现数据不完整
5. 最终返回基于不完整数据集的错误结果

### 修复方向

1. **添加完整性检查**：验证是否接收了所有 CN 的所有数据
2. **改进 Cancel 检测**：检测异常 cancel 并传播错误
3. **改进错误处理**：对于需要完整数据的操作，遇到错误立即返回
4. **添加超时机制**：防止无限等待

通过以上修复，可以确保 EXCEPT 操作在多CN环境下返回正确的结果，即使某个 CN 发生 cancel 或网络错误。







