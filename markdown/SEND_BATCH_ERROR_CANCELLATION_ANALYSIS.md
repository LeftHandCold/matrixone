# 真正的取消（用户取消/超时）是否能被正确捕捉？

## 问题

修改后，`CancelPipelineSending` 不再取消 dispatch receiver（`isDispatch=true`）。那么，如果真正的取消发生（用户取消查询、超时等），dispatch receiver 的取消是否还能被正确捕捉？

## 真正的取消流程

### 1. 取消触发

当用户取消查询或发生超时时：
- `proc.Base.sqlContext.queryCancel()` 被调用
- Context 被取消：`proc.Ctx.Done()` 返回

### 2. Dispatch 操作符感知取消

在 `dispatch.go:171` 中，`waitRemoteRegsReady` 会检查 context 是否被取消：

```go
case <-proc.Ctx.Done():
    timeoutCancel()
    dispatch.ctr.prepared = true
    return true, nil
```

如果 context 被取消，`waitRemoteRegsReady` 会立即返回。

### 3. Dispatch.Reset() 被调用

当 pipeline 失败或取消时，`Dispatch.Reset()` 会被调用（`pkg/sql/colexec/dispatch/types.go:135`）：

```go
func (dispatch *Dispatch) Reset(proc *process.Process, pipelineFailed bool, err error) {
    if dispatch.ctr != nil {
        if dispatch.ctr.isRemote {
            // 向所有 remote receivers 发送错误
            for _, r := range dispatch.ctr.remoteReceivers {
                r.Err <- err
            }
            
            // 清理 UUID 映射
            uuids := make([]uuid.UUID, 0, len(dispatch.RemoteRegs))
            for i := range dispatch.RemoteRegs {
                uuids = append(uuids, dispatch.RemoteRegs[i].Uuid)
            }
            colexec.Get().DeleteUuids(uuids)
        }
    }
    // ... 清理本地 receivers ...
}
```

**关键点**：
- `Dispatch.Reset()` 会向所有 remote receivers 发送错误：`r.Err <- err`
- Receiver 会通过 `r.Err` channel 收到错误，并停止接收数据
- 调用 `colexec.Get().DeleteUuids(uuids)` 清理 UUID 映射

### 4. RemoveRelatedPipeline() 被调用

在 `remoterunServer.go:122` 中，当连接关闭时（`receiver.connectionCtx.Done()`），会调用 `RemoveRelatedPipeline()`：

```go
if receiver.messageTyp == pipeline.Method_PipelineMessage || receiver.messageTyp == pipeline.Method_PrepareDoneNotifyMessage {
    if err == nil {
        <-receiver.connectionCtx.Done()
    }
    colexec.Get().RemoveRelatedPipeline(receiver.clientSession, receiver.messageId)
}
```

**关键点**：
- 当连接关闭时（包括真正的取消导致的连接关闭），`RemoveRelatedPipeline()` 会被调用
- 这会清理 `fromRpcClientToRelatedPipeline` map 中的记录

## 关键问题：ReceiverDone 是否会被设置？

### 当前实现

修改后，`CancelPipelineSending` 不再取消 dispatch receiver：

```go
if v.isDispatch {
    // Don't cancel dispatch receivers - they should continue receiving data
    return
}
```

这意味着：
- **`StopSending` 消息不会设置 `ReceiverDone=true`**（这是正确的，因为 `StopSending` 是正常的完成流程）
- **真正的取消也不会通过 `CancelPipelineSending` 设置 `ReceiverDone=true`**

### 真正的取消如何被处理？

1. **通过 `r.Err` channel**：
   - `Dispatch.Reset()` 会向 receiver 发送错误：`r.Err <- err`
   - Receiver 会通过 `r.Err` 收到错误，并停止接收数据
   - 这是**主要的取消机制**

2. **通过 context 取消**：
   - 如果 context 被取消，`sendBatchToClientSession` 中的 `wcs.Cs.Write(ctx, msg)` 会失败
   - 这会返回错误，导致发送失败

3. **通过 `RemoveRelatedPipeline()`**：
   - 当连接关闭时，`RemoveRelatedPipeline()` 会被调用
   - 这会清理 map 中的记录，防止后续操作

### 潜在问题

**问题**：如果真正的取消发生，`ReceiverDone` 不会被设置，但 receiver 会通过 `r.Err` 收到错误。如果后续还有数据发送，`sendBatchToClientSession` 会检查 `ReceiverDone`，但此时 `ReceiverDone=false`。

**分析**：
- 如果真正的取消发生，`Dispatch.Reset()` 会向 receiver 发送错误
- Receiver 会通过 `r.Err` 收到错误，并停止接收数据
- 同时，`RemoveRelatedPipeline()` 会被调用，清理 map 中的记录
- 如果后续还有数据发送，`sendBatchToClientSession` 会尝试发送，但：
  - 如果 context 被取消，`wcs.Cs.Write(ctx, msg)` 会失败，返回错误
  - 如果连接已关闭，写入会失败，返回错误
  - 如果 receiver 已经通过 `r.Err` 收到错误，它可能已经停止接收，但 `ReceiverDone` 仍然是 `false`

**结论**：
- ✅ **真正的取消可以通过 `r.Err` channel 被正确捕捉**
- ✅ **真正的取消可以通过 context 取消被正确捕捉**
- ⚠️ **但是，`ReceiverDone` 不会被设置，这可能导致在 strict 模式下，如果后续还有数据发送，不会立即返回错误**

### 是否需要设置 ReceiverDone？

**分析**：
- 如果真正的取消发生，`Dispatch.Reset()` 会向 receiver 发送错误
- Receiver 会通过 `r.Err` 收到错误，并停止接收数据
- 如果后续还有数据发送，`sendBatchToClientSession` 会尝试发送，但：
  - 如果 context 被取消，`wcs.Cs.Write(ctx, msg)` 会失败，返回错误
  - 如果连接已关闭，写入会失败，返回错误
  - 这些错误会被正确传播

**结论**：
- ✅ **真正的取消可以通过其他机制（`r.Err`、context 取消、连接关闭）被正确捕捉**
- ✅ **不需要通过 `CancelPipelineSending` 设置 `ReceiverDone` 来捕捉真正的取消**
- ✅ **当前的实现是安全的**

## 总结

### ✅ 真正的取消可以被正确捕捉

1. **通过 `r.Err` channel**：
   - `Dispatch.Reset()` 会向 receiver 发送错误
   - Receiver 会通过 `r.Err` 收到错误，并停止接收数据

2. **通过 context 取消**：
   - 如果 context 被取消，`sendBatchToClientSession` 中的 `wcs.Cs.Write(ctx, msg)` 会失败
   - 这会返回错误，导致发送失败

3. **通过连接关闭**：
   - 当连接关闭时，`RemoveRelatedPipeline()` 会被调用
   - 这会清理 map 中的记录，防止后续操作

### ✅ 不需要设置 ReceiverDone

- 真正的取消可以通过其他机制（`r.Err`、context 取消、连接关闭）被正确捕捉
- 不需要通过 `CancelPipelineSending` 设置 `ReceiverDone` 来捕捉真正的取消
- 当前的实现是安全的

### ⚠️ 注意事项

- `StopSending` 消息不应该设置 `ReceiverDone=true`（这是正常的完成流程）
- 真正的取消应该通过 `r.Err` channel 和 context 取消来处理
- `ReceiverDone` 主要用于检测**远程节点失败**的情况，而不是正常的取消流程

## 建议

当前的实现是安全的，不需要额外的修改。真正的取消可以通过以下机制被正确捕捉：

1. ✅ `r.Err` channel（主要机制）
2. ✅ Context 取消
3. ✅ 连接关闭

如果未来需要更明确的取消检测，可以考虑：
- 在 `Dispatch.Reset()` 中设置 `ReceiverDone=true`（但这可能会影响正常的完成流程）
- 或者，保持当前的实现，因为其他机制已经足够

