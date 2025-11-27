# sendBatchToClientSession FailureModeStrict 错误分析

## 问题描述

执行包含子查询的SQL语句时，返回 `sendBatchToClientSession` 的 `FailureModeStrict` 错误：
```
remote receiver %s is already done, data loss may occur...
```

## SQL查询特征

该SQL查询包含以下特征：
1. **LEFT JOIN** 操作
2. **子查询在WHERE子句中使用IN**：`(t1.pro_survey_point_id, t1.number) IN (SELECT ...)`
3. **子查询包含GROUP BY和聚合函数**：`SELECT pd.pro_survey_point_id, max(pd.number) FROM ... GROUP BY pd.pro_survey_point_id`
4. **多个表连接**：主查询和子查询都涉及多个表的连接

## 错误发生的根本原因

### 1. 分布式执行模式

当SQL查询在分布式环境中执行时：
- 子查询可能需要在多个CN节点上执行
- 查询计划可能选择 `SendToAll` 模式来分发数据
- `SendToAll` 模式要求数据必须发送到**所有**远程接收者，以确保数据完整性

### 2. FailureModeStrict 机制

**代码位置**: `pkg/sql/colexec/dispatch/sendfunc.go:395-403`

```go
if wcs.ReceiverDone {
    if failureMode == FailureModeStrict {
        // Strict mode: receiver done indicates data loss
        // This happens when remote CN crashes or cancels
        return true, moerr.NewInternalError(ctx, fmt.Sprintf(
            "remote receiver %s is already done, data loss may occur. "+
                "This usually indicates the remote CN has failed or been canceled",
            receiverID))
    }
}
```

**关键点**：
- `SendToAll` 和 `Shuffle` 模式使用 `FailureModeStrict`
- 当远程接收者已经完成（`ReceiverDone=true`）时，必须返回错误以防止数据丢失
- 这是为了防止在分布式执行中，部分节点失败导致数据不完整

### 3. ReceiverDone 被设置为 true 的情况

**代码位置**: 
- `pkg/sql/colexec/types2.go:31-34` - 记录dispatch pipeline时发现已done
- `pkg/sql/colexec/types.go:90-91` - 取消pipeline时

可能的原因：

1. **远程CN节点崩溃或OOM**
   - 执行子查询时节点资源不足
   - 节点进程异常退出

2. **网络连接问题**
   - 网络中断导致连接断开
   - 超时导致连接关闭

3. **查询被取消**
   - 用户主动取消查询
   - 系统超时自动取消
   - 其他查询或操作导致上下文取消

4. **远程CN节点提前完成**
   - 在 `RecordDispatchPipeline` 时发现已经收到停止消息
   - 节点已经处理完数据并关闭了接收通道

### 4. 执行流程分析

```
1. 查询计划生成，选择 SendToAll 模式
   ↓
2. Dispatch 操作符准备远程接收者
   ↓
3. 开始发送数据批次到所有远程CN节点
   ↓
4. 某个远程CN节点出现问题（崩溃/取消/断开）
   ↓
5. ReceiverDone 被设置为 true
   ↓
6. 继续尝试发送数据到该节点
   ↓
7. sendBatchToClientSession 检测到 ReceiverDone=true
   ↓
8. 使用 FailureModeStrict 模式返回错误
   ↓
9. 查询失败，返回错误信息
```

## 为什么这个SQL特别容易触发

1. **复杂的子查询**：
   - 子查询包含GROUP BY和聚合函数，需要更多计算资源
   - 子查询在WHERE子句中使用IN，需要先执行子查询再过滤主查询

2. **多表连接**：
   - 主查询和子查询都涉及多个表的LEFT JOIN
   - 增加了执行复杂度和资源消耗

3. **分布式执行**：
   - 子查询可能需要在多个CN节点上并行执行
   - 增加了节点失败的概率

4. **数据量**：
   - 如果数据量较大，执行时间较长，增加了超时或资源耗尽的风险

## 解决方案建议

### 1. 检查集群状态
- 检查所有CN节点的健康状态
- 查看是否有节点崩溃、OOM或网络问题
- 检查节点日志中的错误信息

### 2. 优化查询
- 考虑将子查询重写为JOIN
- 添加适当的索引以加速查询
- 如果可能，减少数据扫描范围

### 3. 调整配置
- 增加查询超时时间
- 增加CN节点的内存配置
- 检查网络连接稳定性

### 4. 监控和诊断
- 查看具体是哪个CN节点出现问题
- 检查该节点的资源使用情况
- 查看是否有其他查询同时执行导致资源竞争

### 5. 代码层面（如果需要）
如果这是一个已知的bug，可以考虑：
- 在 `SendToAll` 模式下，当检测到部分节点失败时，是否应该重试或降级处理
- 改进错误恢复机制，允许部分节点失败时继续执行（如果业务允许）

## 相关代码文件

1. **错误处理核心逻辑**：
   - `pkg/sql/colexec/dispatch/sendfunc.go` - `sendBatchToClientSession` 函数
   - `pkg/sql/colexec/dispatch/sendfunc.go` - `sendToAllRemoteFunc` 函数

2. **ReceiverDone 设置**：
   - `pkg/sql/colexec/types2.go` - `RecordDispatchPipeline` 函数
   - `pkg/sql/colexec/types.go` - `cancelPipeline` 函数

3. **Dispatch 操作符**：
   - `pkg/sql/colexec/dispatch/dispatch.go` - Dispatch 操作符实现
   - `pkg/sql/colexec/dispatch/types.go` - Dispatch 数据结构

## 真正的问题原因（代码逻辑问题）

根据代码分析，如果**每次执行都是 `ReceiverDone=true`**，说明问题不是节点失败，而是代码逻辑问题：

### 问题根源

**关键代码逻辑** (`pkg/sql/colexec/types2.go:30-35`):
```go
// check if sender has sent a stop running message.
if v, ok := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key]; ok && v.alreadyDone {
    dispatchReceiver.Lock()
    dispatchReceiver.ReceiverDone = true
    dispatchReceiver.Unlock()
    return
}
```

**问题场景**：

1. **消息顺序问题**：
   - 如果 `Method_StopSending` 消息在 `Method_PrepareDoneNotifyMessage` 之前到达
   - `CancelPipelineSending` 会创建一个 `alreadyDone=true` 的记录（第83行）
   - 当后续 `RecordDispatchPipeline` 被调用时，发现已有 `alreadyDone=true` 的记录，直接设置 `ReceiverDone=true`

2. **残留记录问题**：
   - 如果之前的查询没有正确清理，map中可能残留 `alreadyDone=true` 的记录
   - 当新的查询使用相同的 `(session, streamID)` 组合时，会立即失败
   - `RemoveRelatedPipeline` 只在特定消息处理完成后才被调用（`remoterunServer.go:122`）

3. **StreamID 重复使用**：
   - 如果 streamID 被重复使用，而之前的记录没有被清理
   - 新查询会立即检测到 `alreadyDone=true` 并失败

### 可能的具体原因

1. **子查询执行顺序问题**：
   - 你的SQL包含复杂的子查询，可能触发多个远程pipeline
   - 某个pipeline提前完成并发送了 `StopSending` 消息
   - 但另一个pipeline的 `PrepareDoneNotifyMessage` 还没到达
   - 导致 `CancelPipelineSending` 先执行，创建了 `alreadyDone=true` 记录

2. **数据问题导致提前结束**：
   - 子查询可能因为数据问题（如空结果集）提前完成
   - 触发了 `StopSending` 消息
   - 但主查询的dispatch还在等待 `PrepareDoneNotifyMessage`

3. **清理逻辑不完整**：
   - 如果查询执行过程中出错，可能没有正确调用 `RemoveRelatedPipeline`
   - 导致map中残留了 `alreadyDone=true` 的记录
   - 下次使用相同的key时会立即失败

### 解决方案

#### 1. 检查是否有残留记录（调试）

在 `RecordDispatchPipeline` 中添加日志，查看是否每次都有残留记录：

```go
func (srv *Server) RecordDispatchPipeline(
	session morpc.ClientSession, streamID uint64, dispatchReceiver *process.WrapCs) {
	key := generateRecordKey(session, streamID)
	
	srv.receivedRunningPipeline.Lock()
	defer srv.receivedRunningPipeline.Unlock()
	
	// 添加日志
	if v, ok := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key]; ok {
		logutil.Warn("Found existing record in RecordDispatchPipeline",
			zap.Bool("alreadyDone", v.alreadyDone),
			zap.Uint64("streamID", streamID))
	}
	
	if v, ok := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key]; ok && v.alreadyDone {
		// ...
	}
}
```

#### 2. 修复建议：清理残留记录

在 `RecordDispatchPipeline` 中，如果发现 `alreadyDone=true` 的记录，应该先清理它，而不是直接设置 `ReceiverDone=true`：

```go
func (srv *Server) RecordDispatchPipeline(
	session morpc.ClientSession, streamID uint64, dispatchReceiver *process.WrapCs) {
	key := generateRecordKey(session, streamID)
	
	srv.receivedRunningPipeline.Lock()
	defer srv.receivedRunningPipeline.Unlock()
	
	// 如果发现残留的 alreadyDone 记录，清理它（可能是之前的查询留下的）
	if v, ok := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key]; ok && v.alreadyDone {
		// 检查是否是旧的记录（没有receiver，说明是之前查询留下的）
		if v.receiver == nil {
			// 清理残留记录，允许新查询继续
			delete(srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline, key)
		} else {
			// 如果receiver存在，说明是当前查询的，应该标记为done
			dispatchReceiver.Lock()
			dispatchReceiver.ReceiverDone = true
			dispatchReceiver.Unlock()
			return
		}
	}
	
	// 正常记录新的pipeline
	value := runningPipelineInfo{
		alreadyDone: false,
		isDispatch:  true,
		queryCancel: nil,
		receiver:    dispatchReceiver,
	}
	srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key] = value
}
```

#### 3. 检查消息顺序

在 `CancelPipelineSending` 中添加日志，确认是否在 `RecordDispatchPipeline` 之前被调用：

```go
func (srv *Server) CancelPipelineSending(
	session morpc.ClientSession, streamID uint64) {
	key := generateRecordKey(session, streamID)
	
	srv.receivedRunningPipeline.Lock()
	defer srv.receivedRunningPipeline.Unlock()
	
	// 添加日志
	logutil.Info("CancelPipelineSending called",
		zap.Uint64("streamID", streamID),
		zap.Bool("hasExistingRecord", ok))
	
	if v, ok := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key]; ok {
		v.cancelPipeline()
	} else {
		// 这里创建 alreadyDone=true 的记录
		srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key] = generateCanceledRecord()
	}
}
```

#### 4. 临时解决方案

如果这是紧急问题，可以尝试：
- 重启CN节点，清理所有残留记录
- 检查是否有查询超时设置过短
- 检查子查询是否因为数据问题提前返回空结果

## 总结

这个错误**不是节点失败导致的**，而是代码逻辑问题：
1. **消息顺序问题**：`StopSending` 消息可能在 `PrepareDoneNotifyMessage` 之前到达
2. **残留记录问题**：之前的查询可能留下了 `alreadyDone=true` 的记录
3. **清理不完整**：查询异常结束时可能没有正确清理map中的记录

建议先添加日志确认具体原因，然后根据实际情况修复代码逻辑。


