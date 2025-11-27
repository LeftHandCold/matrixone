# sendBatchToClientSession FailureModeStrict 错误修复总结

## 问题描述

执行包含子查询的SQL语句时，每次都会返回 `sendBatchToClientSession` 的 `FailureModeStrict` 错误：
```
remote receiver %s is already done, data loss may occur...
```

## 问题根本原因

这是一个**逻辑错误**：`StopSending` 消息被错误地用来取消 dispatch receiver。

### 执行流程

1. **主查询发送子查询到远程节点**
   - 创建多个 dispatch receiver，它们可能共享相同的 streamID（但不同的 session）
   - 每个 receiver 通过 `RecordDispatchPipeline` 注册

2. **子查询执行完成**
   - 子查询执行完成后，`sender.close()` 被调用
   - `close()` 发送 `Method_StopSending` 消息到远程节点

3. **StopSending 消息处理**
   - 远程节点收到 `StopSending` 消息，调用 `CancelPipelineSending`
   - **错误**：`CancelPipelineSending` 调用了 `cancelPipeline()`，对于 dispatch receiver 设置了 `ReceiverDone=true`
   - 这导致 dispatch receiver 被错误地标记为 done

4. **发送数据时报错**
   - 当主查询尝试发送数据到 dispatch receiver 时
   - 发现 `ReceiverDone=true`，在 strict 模式下返回错误

### 关键问题

**`StopSending` 消息的用途**：
- `StopSending` 消息是用来**停止发送数据**的
- 它不应该影响**接收数据**的 dispatch receiver
- Dispatch receiver 应该继续接收数据，直到数据发送完成

**错误的逻辑**：
- `CancelPipelineSending` 对 dispatch receiver 调用了 `cancelPipeline()`
- `cancelPipeline()` 设置了 `receiver.ReceiverDone = true`
- 这导致 dispatch receiver 被错误地标记为 done

## 修复方案

### 修复内容

在 `CancelPipelineSending` 中添加了对 `isDispatch` 的检查：

```go
func (srv *Server) CancelPipelineSending(
	session morpc.ClientSession, streamID uint64) {
	key := generateRecordKey(session, streamID)
	
	srv.receivedRunningPipeline.Lock()
	defer srv.receivedRunningPipeline.Unlock()
	
	if v, ok := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key]; ok {
		// Fix: StopSending message is used to stop sending data, not to cancel
		// dispatch receivers. Dispatch receivers are used to receive data and
		// should continue receiving until data sending is complete.
		// Only cancel non-dispatch pipelines (those that execute queries).
		if v.isDispatch {
			// Don't cancel dispatch receivers - they should continue receiving data
			return
		} else {
			// Only cancel non-dispatch pipelines (query execution pipelines)
			v.cancelPipeline()
		}
	} else {
		// Don't create canceled record - let RecordDispatchPipeline handle it
		// This can happen when StopSending arrives before PrepareDoneNotifyMessage
	}
}
```

### 修复要点

1. **不取消 dispatch receiver**：
   - 如果 `isDispatch=true`，直接返回，不调用 `cancelPipeline()`
   - Dispatch receiver 应该继续接收数据

2. **只取消非 dispatch pipeline**：
   - 只对 `isDispatch=false` 的 pipeline 调用 `cancelPipeline()`
   - 这些是执行查询的 pipeline，可以被取消

3. **不创建残留记录**：
   - 如果 map 中没有记录，不创建 `alreadyDone=true` 的记录
   - 让 `RecordDispatchPipeline` 正常处理注册

## 相关代码文件

1. **修复文件**：
   - `pkg/sql/colexec/types2.go` - `CancelPipelineSending` 函数
   - `pkg/sql/colexec/types.go` - `cancelPipeline` 函数

2. **错误检测**：
   - `pkg/sql/colexec/dispatch/sendfunc.go` - `sendBatchToClientSession` 函数

3. **消息处理**：
   - `pkg/sql/compile/remoterunServer.go` - `handlePipelineMessage` 函数

## 测试验证

修复后，执行包含子查询的SQL语句：
- ✅ 不再返回 `ReceiverDone=true` 错误
- ✅ Dispatch receiver 正常接收数据
- ✅ 查询可以正常完成

## 总结

这个问题的根本原因是 `StopSending` 消息被错误地用来取消 dispatch receiver。修复后，`StopSending` 消息只用于停止发送数据，不再影响接收数据的 dispatch receiver，从而解决了每次执行都报错的问题。

