# sendBatchToClientSession 错误根本原因分析（每次必现）

## 问题现象

每次执行包含子查询的SQL都返回错误：
```
remote receiver %s is already done, data loss may occur...
```

## 根本原因

这是一个**消息处理顺序的竞态条件**导致的固定逻辑问题。

### 执行流程分析

1. **主查询发送子查询到远程节点**
   - 调用 `sender.sendPipeline()` 发送子查询
   - 创建新的 stream，streamID 由 morpc 分配

2. **远程节点执行子查询**
   - 接收 `Method_PipelineMessage`
   - 执行子查询
   - 完成后发送 `PrepareDoneNotifyMessage` 通知主查询

3. **主查询等待通知**
   - Dispatch 操作符在 `waitRemoteRegsReady()` 中等待 `PrepareDoneNotifyMessage`
   - 收到后调用 `RecordDispatchPipeline` 记录 receiver

4. **子查询完成，发送 StopSending**
   - 子查询执行完成后，`sender.close()` 被调用
   - `close()` 调用 `waitingTheStopResponse()`，发送 `Method_StopSending` 消息

### 问题发生的时序

**关键代码** (`pkg/sql/colexec/types2.go:72-84`):
```go
func (srv *Server) CancelPipelineSending(
	session morpc.ClientSession, streamID uint64) {
	key := generateRecordKey(session, streamID)
	
	srv.receivedRunningPipeline.Lock()
	defer srv.receivedRunningPipeline.Unlock()
	
	if v, ok := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key]; ok {
		v.cancelPipeline()
	} else {
		// ⚠️ 关键问题：如果 map 中没有记录，创建一个 alreadyDone=true 的记录
		srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key] = generateCanceledRecord()
	}
}
```

**问题场景**：

1. 子查询执行完成，`sender.close()` 被调用
2. `close()` 发送 `StopSending` 消息到远程节点
3. **如果 `StopSending` 消息在 `PrepareDoneNotifyMessage` 之前到达**（或同时到达但先处理）
4. `CancelPipelineSending` 被调用，发现 map 中没有记录（因为 `RecordDispatchPipeline` 还没被调用）
5. 创建一个 `alreadyDone=true` 的记录
6. 然后 `PrepareDoneNotifyMessage` 到达，调用 `RecordDispatchPipeline`
7. `RecordDispatchPipeline` 发现 map 中已有 `alreadyDone=true` 的记录
8. 直接设置 `ReceiverDone=true` 并返回，**没有创建正常的 receiver 记录**

### 为什么每次必现

对于你的SQL查询，可能的原因：

1. **子查询执行很快**（比如数据量小或返回空结果集）
   - 子查询立即完成并发送 `StopSending`
   - 但主查询的 dispatch 还在等待 `PrepareDoneNotifyMessage`

2. **网络或处理顺序**
   - `StopSending` 和 `PrepareDoneNotifyMessage` 可能同时发送
   - 但由于网络延迟或处理顺序，`StopSending` 先到达

3. **子查询的特殊执行路径**
   - 你的SQL包含 `GROUP BY` 和聚合函数
   - 如果子查询因为数据问题（如空结果集）快速完成
   - 可能触发特殊的完成路径，导致 `StopSending` 先发送

## 修复方案

### 方案1：修复 CancelPipelineSending 逻辑（推荐）

在 `CancelPipelineSending` 中，如果发现 map 中没有记录，不应该创建 `alreadyDone=true` 的记录，而应该：
- 要么忽略（如果还没有注册）
- 要么等待一段时间再检查

```go
func (srv *Server) CancelPipelineSending(
	session morpc.ClientSession, streamID uint64) {
	key := generateRecordKey(session, streamID)
	
	srv.receivedRunningPipeline.Lock()
	defer srv.receivedRunningPipeline.Unlock()
	
	if v, ok := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key]; ok {
		v.cancelPipeline()
	} else {
		// 修复：如果还没有注册，说明 PrepareDoneNotifyMessage 还没到达
		// 不应该创建 alreadyDone 记录，而是应该忽略或延迟处理
		// 因为后续的 RecordDispatchPipeline 会正确处理
		// 如果确实需要取消，应该在 RecordDispatchPipeline 中检查上下文是否已取消
		return
	}
}
```

### 方案2：修复 RecordDispatchPipeline 逻辑

在 `RecordDispatchPipeline` 中，如果发现 `alreadyDone=true` 的记录，应该检查是否是残留记录：

```go
func (srv *Server) RecordDispatchPipeline(
	session morpc.ClientSession, streamID uint64, dispatchReceiver *process.WrapCs) {
	key := generateRecordKey(session, streamID)
	
	srv.receivedRunningPipeline.Lock()
	defer srv.receivedRunningPipeline.Unlock()
	
	if v, ok := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key]; ok && v.alreadyDone {
		// 修复：检查是否是残留记录（receiver 为 nil 说明是 CancelPipelineSending 创建的）
		if v.receiver == nil {
			// 这是 CancelPipelineSending 创建的残留记录，清理它
			delete(srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline, key)
		} else {
			// 这是正常的取消，设置 ReceiverDone
			dispatchReceiver.Lock()
			dispatchReceiver.ReceiverDone = true
			dispatchReceiver.Unlock()
			return
		}
	}
	
	// 正常记录
	value := runningPipelineInfo{
		alreadyDone: false,
		isDispatch:  true,
		queryCancel: nil,
		receiver:    dispatchReceiver,
	}
	srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key] = value
}
```

### 方案3：调整消息发送顺序

确保 `PrepareDoneNotifyMessage` 在 `StopSending` 之前发送，但这可能影响性能。

## 验证方法

添加日志确认问题：

```go
// 在 CancelPipelineSending 中
logutil.Info("CancelPipelineSending called",
	zap.Uint64("streamID", streamID),
	zap.Bool("hasExistingRecord", ok))

// 在 RecordDispatchPipeline 中
if v, ok := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key]; ok && v.alreadyDone {
	logutil.Warn("RecordDispatchPipeline found alreadyDone record",
		zap.Uint64("streamID", streamID),
		zap.Bool("hasReceiver", v.receiver != nil))
	// ...
}
```

## 总结

这是一个**消息处理顺序的竞态条件**：
- `StopSending` 消息在 `PrepareDoneNotifyMessage` 之前到达
- `CancelPipelineSending` 创建了 `alreadyDone=true` 的残留记录
- `RecordDispatchPipeline` 发现残留记录，错误地设置了 `ReceiverDone=true`

**推荐修复方案**：方案2（修复 RecordDispatchPipeline），因为它更安全，不会影响正常的取消逻辑。

