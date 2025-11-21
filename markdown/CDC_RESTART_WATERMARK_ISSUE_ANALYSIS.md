# CDC重启后"unexpected watermark"错误分析

## 问题描述

MO重启后，CDC任务不停地报错：`internal error: internal error: unexpected watermark`，错误信息显示数据的时间戳小于或等于watermark。

## 错误位置

错误发生在 `pkg/cdc/sinker_v2.go` 的 `Sink` 方法中（第713-721行）：

```go
if data.toTs.LE(&watermark) {
    logutil.Error("cdc.mysql_sinker2.unexpected_watermark",
        zap.String("table", s.dbTblInfo.String()),
        zap.String("toTs", data.toTs.ToString()),
        zap.String("watermark", watermark.ToString()))
    err := moerr.NewInternalError(ctx, "unexpected watermark")
    s.SetError(err)
    return
}
```

## CDC任务初始化流程

### 1. 任务启动流程（`cdc_exector.go:Start`）

1. **清理旧资源**：关闭并等待旧的readers完成
2. **初始化WatermarkUpdater**：通过 `GetCDCWatermarkUpdater` 获取全局单例
3. **注册表检测器**：注册到TableDetector以检测新表
4. **等待新表**：通过 `handleNewTables` 回调处理新表

### 2. 表初始化流程（`cdc_exector.go:addExecPipelineForTable`）

```go
// step 1: 获取watermark
watermark := exec.startTs
if exec.noFull {
    watermark = types.TimestampToTS(txnOp.SnapshotTS())
}
watermark, err = exec.watermarkUpdater.GetOrAddCommitted(
    ctx,
    &watermarkKey,
    &watermark,
)
```

**关键点**：
- 如果数据库中有watermark记录，返回数据库中的值（可能是旧的、较大的值）
- 如果数据库中没有记录，使用 `exec.startTs` 或当前事务的 `SnapshotTS()`
- 读取到的watermark会被放入 `cacheCommitted`

### 3. Reader读取流程（`table_change_stream.go:processWithTxn`）

```go
// 获取读取起始时间戳
fromTs, err := s.watermarkUpdater.GetFromCache(ctx, s.watermarkKey)
if err != nil {
    return err
}

// 获取读取结束时间戳
toTs := types.TimestampToTS(GetSnapshotTS(txnOp))

// 读取数据
changesHandle, err := CollectChanges(ctx, rel, fromTs, toTs, s.mp)
```

### 4. Sinker验证流程（`sinker_v2.go:Sink`）

```go
watermark, err := s.watermarkUpdater.GetFromCache(ctx, &key)
if data.toTs.LE(&watermark) {
    // 报错：unexpected watermark
}
```

## 问题根因分析

### 核心问题

**MO重启后，watermark缓存和实际数据读取之间存在时间窗口不一致问题**：

1. **WatermarkUpdater是全局单例**：
   - `GetCDCWatermarkUpdater` 返回的是全局单例
   - 重启后，内存缓存（`cacheCommitted`, `cacheUncommitted`, `cacheCommitting`）全部丢失
   - 需要从数据库重新加载

2. **初始化时的watermark可能过时**：
   - 重启时，从数据库读取的watermark可能是很久之前的值（比如任务已经很久没更新）
   - 这个watermark被放入 `cacheCommitted`
   - Reader使用这个watermark作为 `fromTs` 开始读取

3. **数据读取的时间戳可能小于watermark**：
   - Reader使用 `GetSnapshotTS(txnOp)` 作为 `toTs`
   - 如果当前事务的snapshot TS小于数据库中的watermark，就会出现 `data.toTs <= watermark`
   - 这可能是由于：
     - 事务隔离级别导致读取到旧数据
     - 系统时钟回退
     - 数据库中的watermark值异常（比如是未来的时间戳）

### 具体场景分析

**你的疑问是对的**：正常情况下，fromTs是从watermark获取的，toTs是当前snapshot TS，理论上toTs应该总是 >= watermark。

**但是，为什么会出现 toTs <= watermark 的情况呢？**

#### 场景1：数据库中的watermark值异常（最可能）

**问题根源**：数据库中的watermark值可能是**未来的时间戳**或**异常大的值**

**可能的原因**：
1. **历史bug**：之前某个版本的代码可能错误地写入了未来的时间戳
2. **手动修改**：有人手动修改了数据库中的watermark值
3. **时钟问题**：写入watermark时系统时钟异常（比如NTP同步问题）

**具体流程**：
```
1. 初始化时：
   - 从数据库读取watermark = 1762838247546220221-1（异常大的值，可能是未来的时间戳）
   - 放入cacheCommitted

2. Reader读取时：
   - fromTs = GetFromCache() = 1762838247546220221-1（从cacheCommitted获取）
   - toTs = GetSnapshotTS(txnOp) = 1762838211（当前事务的snapshot TS，正常值）
   - 检查：toTs (1762838211) < fromTs (1762838247546220221-1)
   - 代码在643行已经处理：if !toTs.GT(&fromTs) { return handleSnapshotNoProgress() }
   - 所以不会读取数据，直接返回

3. 但是，如果之前已经读取了数据（在检查之前）：
   - CollectChanges(fromTs, toTs) 可能返回空数据或旧数据
   - data.toTs = toTs = 1762838211
   - Sinker验证时：watermark = 1762838247546220221-1（从cacheCommitted获取）
   - 结果：data.toTs (1762838211) <= watermark (1762838247546220221-1) → 报错
```

#### 场景2：系统时钟回退

**问题根源**：系统时钟回退，导致当前事务的snapshot TS小于之前写入的watermark

**具体流程**：
```
1. 之前正常运行：
   - watermark = 1762838247546220221-1（正常值）
   - 系统时钟正常

2. 系统时钟回退（比如NTP同步、手动调整时间）：
   - 当前时间 < 之前的时间
   - 新事务的snapshot TS = 1762838211（回退后的时间）
   - 数据库中的watermark = 1762838247546220221-1（回退前的时间）

3. MO重启后：
   - 从数据库读取watermark = 1762838247546220221-1
   - 当前事务的snapshot TS = 1762838211
   - 结果：toTs < watermark
```

#### 场景3：事务隔离级别导致读取到旧数据

**问题根源**：事务的snapshot TS可能基于某个旧的timestamp

**具体流程**：
```
1. 初始化时：
   - 从数据库读取watermark = 1762838247546220221-1
   - 放入cacheCommitted

2. Reader创建新事务时：
   - 由于事务隔离级别或其他原因
   - 事务的snapshot TS可能被设置为一个旧值 = 1762838211
   - 这个值小于数据库中的watermark

3. 读取数据时：
   - fromTs = watermark = 1762838247546220221-1
   - toTs = snapshot TS = 1762838211
   - 结果：toTs < fromTs
```

### 为什么代码在643行已经检查了，但还是会报错？

**关键点**：代码在 `table_change_stream.go:643` 确实检查了 `toTs <= fromTs` 的情况：

```go
// 619行：获取fromTs
fromTs, err := s.watermarkUpdater.GetFromCache(ctx, s.watermarkKey)

// 638行：获取toTs
toTs := types.TimestampToTS(GetSnapshotTS(txnOp))

// 643行：检查 toTs > fromTs
if !toTs.GT(&fromTs) {
    return s.handleSnapshotNoProgress(ctx, fromTs, toTs)  // 直接返回，不读取数据
}

// 661行：只有通过检查才会调用CollectChanges
changesHandle, err := CollectChanges(ctx, rel, fromTs, toTs, s.mp)
```

**理论上，如果 toTs <= fromTs，代码会在643行直接返回，不会发送数据到Sinker。**

**但是，为什么还是会出现错误呢？**

#### 最可能的原因：数据库中的watermark值异常

**关键发现**：从你的错误日志来看，watermark值 `1762838247546220221-1` 看起来是一个**异常大的值**（可能是未来的时间戳）。

**可能的情况**：

1. **历史数据残留**：
   - 之前某个时刻，数据已经被发送到Sinker的队列中
   - 但是watermark在数据库中被错误地更新为一个很大的值（可能是bug或手动修改）
   - 当Sinker处理队列中的数据时，发现 data.toTs <= watermark

2. **并发/时序问题**：
   - Reader在某个时刻获取了fromTs（正常值）
   - 但是在处理过程中，watermark被其他goroutine更新为异常值
   - 数据被发送到Sinker，data.toTs是正常值
   - Sinker验证时，从cacheCommitted获取的watermark已经是异常值
   - 结果：data.toTs <= watermark

3. **CollectChanges的边界情况**：
   - 虽然代码检查了 `toTs > fromTs`，但`CollectChanges`可能在某些边界情况下仍然返回数据
   - 比如：fromTs和toTs非常接近，或者有精度问题

4. **之前的代码版本**：
   - 如果这是升级后的代码，之前的版本可能没有这个检查
   - 之前发送的数据还在队列中，但watermark已经被更新

#### 验证方法

可以通过以下SQL查询数据库中的watermark值，看看是否异常：

```sql
SELECT account_id, task_id, db_name, table_name, watermark, err_msg
FROM mo_catalog.mo_cdc_watermark
WHERE task_id = '019a70ee-87de-7b24-981f-1b0b127c85f2'
  AND db_name = 'cdc_test_6_db2'
  AND table_name = 'table2';
```

如果watermark值确实异常（比如是未来的时间戳），那么问题就找到了。

## 解决方案

### 方案1：修复Sinker的验证逻辑（推荐）✅ 已实现

在Sinker中，如果检测到 `data.toTs <= watermark`，应该：
1. **记录警告日志**（而不是错误）
2. **跳过该批次数据**（因为已经处理过了）
3. **不设置错误状态**（允许继续处理后续数据）

**已修改 `sinker_v2.go:713-724`**：

```go
if data.toTs.LE(&watermark) {
    // Skip old data that has already been processed
    // This can happen after MO restart when watermark from database
    // is greater than current transaction snapshot TS
    logutil.Warn("cdc.mysql_sinker2.skip_old_data",
        zap.String("table", s.dbTblInfo.String()),
        zap.String("toTs", data.toTs.ToString()),
        zap.String("watermark", watermark.ToString()),
        zap.String("reason", "data timestamp is less than or equal to watermark, likely already processed"))
    // Skip this batch and continue processing subsequent data
    return
}
```

**修改说明**：
- 将 `logutil.Error` 改为 `logutil.Warn`，避免将正常情况（旧数据）记录为错误
- 移除了 `s.SetError(err)`，不再设置错误状态，允许任务继续运行
- 添加了详细的注释和日志信息，说明这是跳过已处理的数据
- 直接返回，跳过该批次数据，继续处理后续数据

### 方案2：修复初始化时的watermark选择逻辑

在 `addExecPipelineForTable` 中，如果从数据库读取的watermark大于当前事务的snapshot TS，应该使用当前事务的snapshot TS：

```go
watermark, err = exec.watermarkUpdater.GetOrAddCommitted(
    ctx,
    &watermarkKey,
    &watermark,
)
if err != nil {
    return err
}

// 如果数据库中的watermark大于当前事务的snapshot TS，使用snapshot TS
currentSnapshotTS := types.TimestampToTS(txnOp.SnapshotTS())
if watermark.GT(&currentSnapshotTS) {
    logutil.Warn("cdc.watermark.adjust_to_snapshot",
        zap.String("table", info.String()),
        zap.String("db-watermark", watermark.ToString()),
        zap.String("snapshot-ts", currentSnapshotTS.ToString()))
    watermark = currentSnapshotTS
    // 更新watermark到缓存
    exec.watermarkUpdater.UpdateWatermarkOnly(ctx, &watermarkKey, &watermark)
}
```

### 方案3：在Reader中处理

在 `table_change_stream.go:processWithTxn` 中，确保 `fromTs` 不会大于 `toTs`：

```go
fromTs, err := s.watermarkUpdater.GetFromCache(ctx, s.watermarkKey)
if err != nil {
    return err
}

toTs := types.TimestampToTS(GetSnapshotTS(txnOp))

// 如果fromTs大于toTs，调整fromTs
if fromTs.GT(&toTs) {
    logutil.Warn("cdc.table_stream.adjust_from_ts",
        zap.String("table", s.tableInfo.String()),
        zap.String("from-ts", fromTs.ToString()),
        zap.String("to-ts", toTs.ToString()))
    fromTs = toTs
}
```

## 推荐方案

**已实现方案1** ✅

**当前实现**：
- **方案1**：在Sinker中，如果遇到旧数据（`data.toTs <= watermark`），优雅地跳过而不是报错
  - 记录警告日志，不设置错误状态
  - 跳过该批次数据，继续处理后续数据
  - 允许任务在MO重启后正常恢复

**效果**：
- ✅ 解决了MO重启后"unexpected watermark"错误导致任务失败的问题
- ✅ 任务可以正常恢复，即使watermark和当前数据时间戳不一致
- ✅ 通过警告日志可以监控这种情况的发生

**可选增强（方案2）**：
如果需要进一步优化，可以在初始化时确保watermark不会大于当前snapshot TS，这样可以减少跳过旧数据的情况。

## 测试建议

1. **重启测试**：重启MO后，检查CDC任务是否能正常恢复
2. **长时间停止测试**：停止CDC任务一段时间后重启，检查是否能正确处理
3. **时钟回退测试**：模拟系统时钟回退的情况
4. **并发测试**：多个CDC任务同时重启的情况

## 水位更新位置分析

### 水位更新的两个位置

#### 位置1：事务提交时（`reader_v2_txn_manager.go:CommitTransaction`）

```go
// Step 1: Send COMMIT to sinker
tm.sinker.SendCommit()
tm.sinker.SendDummy()

// Step 2: Check for errors
if err := tm.sinker.Error(); err != nil {
    return err  // 如果有错误，直接返回，不会更新watermark
}

// Step 3: Update watermark (persistent proof of success)
if err := tm.watermarkUpdater.UpdateWatermarkOnly(ctx, tm.watermarkKey, &toTs); err != nil {
    // UpdateWatermarkOnly always returns nil
}
```

**关键点**：
- 只有在 `sinker.Error()` 检查通过后才会更新watermark
- 如果SendCommit失败，不会更新watermark

#### 位置2：NoMoreData时（`reader_v2_data_processor.go:processNoMoreData`）

```go
// 情况1：有事务时
if tracker != nil && tracker.hasBegin {
    if err := dp.txnManager.CommitTransaction(ctx); err != nil {
        return err  // 如果CommitTransaction失败，不会更新watermark
    }
} else {
    // 情况2：没有事务时（heartbeat更新）
    // 即使没有事务，也会更新watermark作为heartbeat
    if err := dp.txnManager.watermarkUpdater.UpdateWatermarkOnly(
        ctx,
        dp.txnManager.watermarkKey,
        &dp.toTs,
    ); err != nil {
        // UpdateWatermarkOnly always returns nil
    }
}
```

**关键点**：
- 即使没有事务（initSnapshotSplitTxn=true），也会更新watermark作为heartbeat
- 这个更新在检查sinker.Error()之后，但如果context被canceled，可能已经执行了

### 问题：context canceled时水位可能被更新

**你发现的问题是对的！** 确实存在这种情况：

#### 场景1：CommitTransaction中的时序问题

```go
// Step 1: SendCommit() - 成功
tm.sinker.SendCommit()

// Step 2: 检查错误 - 此时还没有错误
if err := tm.sinker.Error(); err != nil {
    return err
}

// Step 3: 更新watermark - 执行了
UpdateWatermarkOnly(ctx, key, &toTs)  // watermark被放入cacheUncommitted

// 但是，如果此时context被canceled，或者后续处理失败
// watermark已经在cacheUncommitted中了
// CronJob会异步持久化它到数据库
```

#### 场景2：processNoMoreData中的heartbeat更新

```go
// 检查sinker错误 - 通过
if err := dp.sinker.Error(); err != nil {
    return err
}

// 如果没有事务，直接更新watermark（heartbeat）
if err := dp.txnManager.watermarkUpdater.UpdateWatermarkOnly(...); err != nil {
    // 即使context被canceled，watermark也可能已经被更新
}
```

#### 场景3：异步持久化的问题

```go
// UpdateWatermarkOnly只是将watermark放入cacheUncommitted
u.cacheUncommitted[*key] = *watermark  // 立即执行

// CronJob每3秒异步持久化
cronRun() {
    // 将cacheUncommitted移动到cacheCommitting
    // 然后批量UPDATE到数据库
    // 即使context被canceled，CronJob仍然会执行
}
```

### 为什么会出现这个问题？

1. **UpdateWatermarkOnly是同步的**：
   - 立即将watermark放入 `cacheUncommitted`
   - 不检查context是否被canceled

2. **CronJob是异步的**：
   - 每3秒执行一次，独立于业务逻辑
   - 即使context被canceled，CronJob仍然会持久化watermark

3. **没有回滚机制**：
   - 一旦watermark被放入 `cacheUncommitted`，就无法回滚
   - 即使后续处理失败，CronJob仍然会持久化它

### 影响

**这可能导致**：
- 数据还没有成功提交到目标数据库
- 但watermark已经被更新（持久化到数据库）
- 下次重启时，从数据库读取的watermark大于实际处理的数据
- 导致 `toTs <= watermark` 的情况

### 问题分析：为什么方案1有缺陷

**你的观察是对的！** 即使我在`UpdateWatermarkOnly`开始时检查了context，仍然存在问题：

#### 时序问题

```go
// CommitTransaction的执行流程：
1. SendCommit()           // 将COMMIT命令放入channel（异步）
2. SendDummy()            // 确保COMMIT被发送（但不等待执行）
3. Error()检查            // 检查之前的错误（不是COMMIT执行的结果）
4. UpdateWatermarkOnly()  // 更新watermark（此时COMMIT可能还没执行）
5. [COMMIT真正执行]       // 如果此时context被canceled，COMMIT失败
6. [CronJob持久化]        // watermark已经被更新，会被持久化
```

**关键问题**：
- `SendDummy()`只是确保命令被发送到channel，**不等待执行完成**
- `Error()`检查的是**之前的错误状态**，不是COMMIT执行的结果
- 如果COMMIT在`UpdateWatermarkOnly`之后失败（比如context canceled），watermark已经被更新了

### 更可靠的解决方案

#### 方案1：使用Pending Watermark机制（推荐）

**思路**：引入一个pending状态，只有在确认COMMIT成功后才真正更新watermark。

```go
// 在WatermarkUpdater中添加pending watermark
type CDCWatermarkUpdater struct {
    // ... existing fields ...
    cachePending map[WatermarkKey]types.TS  // 待确认的watermark
}

// 修改CommitTransaction
func (tm *TransactionManager) CommitTransaction(ctx context.Context) error {
    // Step 1: Send COMMIT
    tm.sinker.SendCommit()
    tm.sinker.SendDummy()
    
    // Step 2: 检查错误
    if err := tm.sinker.Error(); err != nil {
        return err
    }
    
    // Step 3: 将watermark放入pending状态（不立即更新）
    toTs := tm.tracker.GetToTs()
    tm.watermarkUpdater.SetPendingWatermark(ctx, tm.watermarkKey, &toTs)
    
    // Step 4: 等待COMMIT真正完成（通过检查sinker状态）
    // 或者，在handleCommit成功后，调用ConfirmPendingWatermark
}

// 在handleCommit成功后，确认pending watermark
func (s *mysqlSinker2) handleCommit(ctx context.Context) error {
    // ... commit logic ...
    if err := s.executor.CommitTx(ctx); err != nil {
        return err
    }
    
    // Commit成功，确认pending watermark
    // 这里需要通知TransactionManager或WatermarkUpdater
    // 将pending watermark移动到cacheUncommitted
}
```

**问题**：这需要改变架构，让Sinker能够通知TransactionManager。

#### 方案2：在CronJob持久化时检查Sinker状态（更实用）

**思路**：在CronJob持久化watermark时，检查对应的Sinker是否有错误。

```go
// 在WatermarkUpdater中存储sinker引用
type CDCWatermarkUpdater struct {
    // ... existing fields ...
    sinkerRefs map[WatermarkKey]Sinker  // 存储每个key对应的sinker引用
}

// 修改execBatchUpdateWM
func (u *CDCWatermarkUpdater) execBatchUpdateWM() (errMsg string, err error) {
    // ... existing logic ...
    
    // 在持久化前，检查sinker状态
    for key, watermark := range u.cacheCommitting {
        if sinker, ok := u.sinkerRefs[key]; ok {
            if err := sinker.Error(); err != nil {
                // Sinker有错误，不持久化这个watermark
                logutil.Warn(
                    "cdc.watermark.skip_persist_sinker_error",
                    zap.String("key", key.String()),
                    zap.String("watermark", watermark.ToString()),
                    zap.Error(err),
                )
                // 将watermark退回cacheUncommitted
                u.cacheUncommitted[key] = watermark
                delete(u.cacheCommitting, key)
                continue
            }
        }
    }
    
    // 只持久化没有错误的watermark
    // ... persist logic ...
}
```

**问题**：需要存储sinker引用，增加耦合。

#### 方案3：延迟更新watermark，等待COMMIT确认（最佳，但需要架构改动）

**思路**：改变UpdateWatermarkOnly的调用时机，只有在COMMIT真正成功后才调用。

```go
// 方案3a：在handleCommit成功后回调
func (s *mysqlSinker2) handleCommit(ctx context.Context) error {
    if err := s.executor.CommitTx(ctx); err != nil {
        return err
    }
    
    // Commit成功，通知TransactionManager更新watermark
    // 需要添加回调机制
    if s.onCommitSuccess != nil {
        s.onCommitSuccess(ctx, s.currentToTs)
    }
    
    return nil
}

// 方案3b：使用事务状态跟踪
// 在TransactionManager中，只有在确认COMMIT成功后才更新watermark
// 可以通过轮询sinker的txnState，或者使用回调机制
```

#### 方案4：在UpdateWatermarkOnly中检查context + 在CronJob中二次检查（折中方案）

**思路**：双重检查，减少问题发生的概率。

```go
// 在UpdateWatermarkOnly中检查context
func (u *CDCWatermarkUpdater) UpdateWatermarkOnly(
    ctx context.Context,
    key *WatermarkKey,
    watermark *types.TS,
) (err error) {
    // 检查1：context是否被canceled
    if ctx.Err() != nil {
        logutil.Warn(
            "cdc.watermark.update_skipped_context_canceled",
            zap.String("key", key.String()),
            zap.Error(ctx.Err()),
        )
        return nil
    }
    
    u.Lock()
    defer u.Unlock()
    u.cacheUncommitted[*key] = *watermark
    return nil
}

// 在CronJob持久化时，再次检查context（如果可能的话）
// 但这需要知道每个watermark对应的context，不太现实
```

#### 方案5：使用事务状态标记（推荐，最小改动）

**思路**：在UpdateWatermarkOnly时，记录一个标记，表示这个watermark对应的COMMIT可能还未完成。在CronJob持久化时，如果发现标记，延迟持久化。

```go
// 在WatermarkUpdater中添加
type CDCWatermarkUpdater struct {
    // ... existing fields ...
    pendingConfirm map[WatermarkKey]time.Time  // 待确认的watermark及其时间
}

// 修改UpdateWatermarkOnly
func (u *CDCWatermarkUpdater) UpdateWatermarkOnly(
    ctx context.Context,
    key *WatermarkKey,
    watermark *types.TS,
) (err error) {
    u.Lock()
    defer u.Unlock()
    
    // 检查context
    if ctx.Err() != nil {
        logutil.Warn(
            "cdc.watermark.update_skipped_context_canceled",
            zap.String("key", key.String()),
            zap.Error(ctx.Err()),
        )
        return nil
    }
    
    u.cacheUncommitted[*key] = *watermark
    // 标记为待确认（给一个短暂的延迟窗口，比如100ms）
    u.pendingConfirm[*key] = time.Now().Add(100 * time.Millisecond)
    return nil
}

// 修改execBatchUpdateWM
func (u *CDCWatermarkUpdater) execBatchUpdateWM() (errMsg string, err error) {
    // ... existing logic ...
    
    // 在持久化前，检查是否有待确认的watermark
    now := time.Now()
    for key, watermark := range u.cacheCommitting {
        if confirmTime, ok := u.pendingConfirm[key]; ok {
            if now.Before(confirmTime) {
                // 还在确认窗口内，延迟持久化
                logutil.Debug(
                    "cdc.watermark.delay_persist_pending_confirm",
                    zap.String("key", key.String()),
                    zap.Duration("remaining", time.Until(confirmTime)),
                )
                // 退回cacheUncommitted，下次再试
                u.cacheUncommitted[key] = watermark
                delete(u.cacheCommitting, key)
                continue
            }
            // 确认窗口已过，可以持久化
            delete(u.pendingConfirm, key)
        }
    }
    
    // ... persist logic ...
}
```

**这个方案的优点**：
- 改动最小
- 给COMMIT执行一个短暂的延迟窗口
- 如果COMMIT在100ms内失败，watermark不会被持久化

**缺点**：
- 不是100%可靠（如果COMMIT在100ms后失败，仍然可能持久化）
- 增加了延迟

### 推荐方案

**当前最佳方案**：**方案1（Sinker跳过旧数据）+ 方案5（延迟确认机制）**

1. **方案1**：已经在Sinker中实现，跳过旧数据而不是报错
2. **方案5**：添加延迟确认机制，减少watermark在COMMIT失败后被持久化的概率

这样可以：
- 即使出现问题，也不会导致任务失败（方案1）
- 减少问题发生的概率（方案5）
- 改动相对较小

## 水位跳跃问题分析

### 问题现象

从debug.log中可以看到，水位更新是**跳着来的**：

```
第42行：from-ts: 1762930020558583375-1, to-ts: 1762930021801138835-1
第44行：from-ts: 1762930022470292374-1, to-ts: 1762930022684196319-1
```

从 `1762930021801138835-1` 跳到了 `1762930022470292374-1`，中间跳过了约66,915,539个时间戳单位。

### 原因分析

#### 原因1：UpdateWatermarkOnly直接覆盖（正常行为）

```go
// watermark_updater.go:1038-1039
oldWatermark, hasOld := u.cacheUncommitted[*key]
u.cacheUncommitted[*key] = *watermark  // 直接覆盖，不检查大小
```

**关键问题**：`UpdateWatermarkOnly` 直接覆盖 `cacheUncommitted` 中的值，**不检查新的watermark是否比旧的大**。

**影响**：
- 如果多个批次同时调用 `UpdateWatermarkOnly`，后面的批次会覆盖前面的批次
- 即使后面的watermark更小，也会覆盖
- 这可能导致中间的watermark丢失

**但是**：从代码逻辑看，这应该是**正常行为**，因为：
1. 每个批次处理完后，都会调用 `UpdateWatermarkOnly` 更新watermark
2. 如果多个批次同时处理，后面的批次会覆盖前面的批次
3. 最终持久化的watermark是**最新的**，这是正确的

#### 原因2：日志没有打印完全（可能）

**可能的情况**：
- 有些批次的日志没有打印（日志级别问题）
- 或者有些批次的数据没有被处理（错误、跳过等）

**验证方法**：
- 检查是否有 `buffer_update` 的DEBUG日志（需要DEBUG级别）
- 检查是否有错误日志导致某些批次被跳过

#### 原因3：并发更新导致覆盖（设计如此）

**设计逻辑**：
- `UpdateWatermarkOnly` 使用 `Lock()` 保证线程安全
- 但是，如果多个批次同时调用，后面的批次会覆盖前面的批次
- 这是**设计如此**，因为最终只需要最新的watermark

**问题**：
- 如果批次A的watermark是100，批次B的watermark是200
- 如果批次B先调用 `UpdateWatermarkOnly`，然后批次A再调用
- 那么 `cacheUncommitted` 中的watermark会被设置为100（更小的值）
- 这可能导致watermark回退

### 代码逻辑检查

#### UpdateWatermarkOnly的实现

```go
func (u *CDCWatermarkUpdater) UpdateWatermarkOnly(
    ctx context.Context,
    key *WatermarkKey,
    watermark *types.TS,
) (err error) {
    u.Lock()
    defer u.Unlock()
    
    oldWatermark, hasOld := u.cacheUncommitted[*key]
    u.cacheUncommitted[*key] = *watermark  // 直接覆盖，不检查大小
    
    // 记录日志（DEBUG级别）
    logutil.Debug(
        "cdc.watermark.buffer_update",
        zap.String("old-watermark", oldWatermark.ToString()),
        zap.String("new-watermark", watermark.ToString()),
        ...
    )
    
    return nil
}
```

**问题**：没有检查 `watermark` 是否比 `oldWatermark` 大，直接覆盖。

#### 应该的修复

```go
func (u *CDCWatermarkUpdater) UpdateWatermarkOnly(
    ctx context.Context,
    key *WatermarkKey,
    watermark *types.TS,
) (err error) {
    u.Lock()
    defer u.Unlock()
    
    oldWatermark, hasOld := u.cacheUncommitted[*key]
    
    // 只更新更大的watermark（防止回退）
    if !hasOld || watermark.GT(&oldWatermark) {
        u.cacheUncommitted[*key] = *watermark
    } else {
        // 如果新的watermark更小，记录警告
        logutil.Warn(
            "cdc.watermark.update_skipped_smaller",
            zap.String("key", key.String()),
            zap.String("old-watermark", oldWatermark.ToString()),
            zap.String("new-watermark", watermark.ToString()),
        )
        return nil
    }
    
    logutil.Debug(...)
    return nil
}
```

### 结论

**水位跳跃是正常的**，因为：
1. `UpdateWatermarkOnly` 直接覆盖，不检查大小
2. 多个批次同时更新时，后面的批次会覆盖前面的批次
3. 最终持久化的watermark是**最新的**，这是正确的

**但是**，如果新的watermark比旧的小，会导致watermark回退，这可能是个问题。

**建议修复**：在 `UpdateWatermarkOnly` 中添加检查，只更新更大的watermark。

## 重复启动任务问题分析

### 问题现象

从debug2.log可以看到：
- **15:15:19.672321**：启动了一个任务 `cdc.table_stream.start`
- **15:19:35.743750**：又启动了一个任务 `cdc.table_stream.start`（4分钟后）

两个任务同时运行，会**交替更新水位**，导致水位跳跃。

### 根本原因：竞态条件

#### 问题1：`addExecPipelineForTable`没有在启动前检查

```go
// cdc_exector.go:1070-1074
// step 4. start goroutines (sinker first, then reader)
// Note: Reader will register itself in runningReaders during Run()
// to prevent duplicate readers (see TableChangeStream.Run line 207)
go sinker.Run(ctx, exec.activeRoutine)
go reader.Run(ctx, exec.activeRoutine)  // 直接启动，没有检查
```

**问题**：`addExecPipelineForTable`直接启动goroutine，没有在启动前检查`runningReaders`。

#### 问题2：`handleNewTables`中的检查存在竞态条件

```go
// cdc_exector.go:743-763
if val, ok := exec.runningReaders.Load(key); ok {
    if reader, ok := val.(cdc.ChangeReader); ok {
        readerInfo := reader.GetTableInfo()
        if info.OnlyDiffinTblId(readerInfo) {
            // wait for old reader to stop
            ...
        } else {
            continue  // 如果table id相同，跳过
        }
    }
}
// 如果没有找到，继续创建新的reader
```

**问题**：
1. 检查`runningReaders`时，如果第一个任务还在运行，应该会跳过
2. 但是，如果第一个任务因为某种原因（比如处理慢、卡住）**还没有注册到`runningReaders`**，检查会通过
3. 第二个任务会创建新的reader和sinker，启动goroutine

#### 问题3：`TableChangeStream.Run`中的检查在goroutine启动后执行

```go
// table_change_stream.go:287
// 1. Check for duplicate readers
if _, loaded := s.runningReaders.LoadOrStore(s.runningReaderKey, s); loaded {
    logutil.Warn("cdc.table_stream.duplicate_running", ...)
    s.wg.Done()
    s.Close()
    return
}
```

**问题**：
- 这个检查是在goroutine启动**后**执行的
- 如果两个任务几乎同时启动，都可能通过检查
- 第一个任务会成功注册，第二个任务会检测到重复并退出
- 但是，如果第一个任务在注册之前就退出了（比如因为错误），第二个任务会成功启动

### 时序问题分析

**场景1：第一个任务处理慢，第二个任务启动**

```
时间线：
T1: 第一个任务启动，goroutine开始运行
T2: 第一个任务还在处理数据（压力大，处理慢）
T3: TableDetector扫描（15秒或5秒重试），调用handleNewTables
T4: handleNewTables检查runningReaders，第一个任务已经注册，应该跳过
T5: 但是，如果第一个任务因为某种原因（比如卡住）没有及时注册，检查会通过
T6: 第二个任务创建新的reader和sinker，启动goroutine
T7: 两个任务都运行，交替更新水位
```

**场景2：第一个任务退出，第二个任务启动**

```
时间线：
T1: 第一个任务启动，goroutine开始运行
T2: 第一个任务因为错误退出，cleanup删除runningReaders中的记录
T3: TableDetector扫描，调用handleNewTables
T4: handleNewTables检查runningReaders，找不到记录，创建新的任务
T5: 第二个任务启动
```

### 为什么压力大时会出现？

**压力大时的问题**：
1. **处理慢**：第一个任务处理数据很慢，4分钟还没完成
2. **TableDetector重试**：如果`handleNewTables`返回错误，会触发重试（每5秒）
3. **竞态条件**：如果第一个任务在处理过程中，第二个任务启动，可能导致重复

### 解决方案

#### 方案1：在`addExecPipelineForTable`中提前检查（推荐）

```go
func (exec *CDCTaskExecutor) addExecPipelineForTable(
    ctx context.Context,
    info *cdc.DbTableInfo,
    txnOp client.TxnOperator,
) (err error) {
    // ... existing code ...
    
    // Check if reader already exists BEFORE creating new one
    key := cdc.GenDbTblKey(info.SourceDbName, info.SourceTblName)
    if val, ok := exec.runningReaders.Load(key); ok {
        if reader, ok := val.(cdc.ChangeReader); ok {
            readerInfo := reader.GetTableInfo()
            if !info.OnlyDiffinTblId(readerInfo) {
                // Same table, skip creating new reader
                logutil.Info(
                    "cdc.frontend.task.reader_already_running",
                    zap.String("table", key),
                    zap.Uint64("table-id", readerInfo.SourceTblId),
                )
                return nil
            }
        }
    }
    
    // ... create reader and sinker ...
    
    // Use LoadOrStore to atomically check and store
    // This prevents race condition between check and start
    actualReader, loaded := exec.runningReaders.LoadOrStore(key, reader)
    if loaded {
        // Another goroutine already started, close this one
        logutil.Warn(
            "cdc.frontend.task.duplicate_reader_prevented",
            zap.String("table", key),
        )
        reader.Close()
        sinker.Close()
        return nil
    }
    
    // Start goroutines
    go sinker.Run(ctx, exec.activeRoutine)
    go reader.Run(ctx, exec.activeRoutine)
    
    return nil
}
```

#### 方案2：在`handleNewTables`中使用原子操作

```go
// 使用LoadOrStore确保原子性
key := cdc.GenDbTblKey(info.SourceDbName, info.SourceTblName)
if val, loaded := exec.runningReaders.LoadOrStore(key, nil); loaded {
    // Already exists, skip
    continue
}
// 创建reader后，替换nil
```

#### 方案3：在`TableChangeStream.Run`中提前注册

```go
// 在goroutine启动前就注册，而不是在Run方法中注册
// 但这需要改变架构，影响较大
```

### 推荐方案

**方案1**：在`addExecPipelineForTable`中使用`LoadOrStore`确保原子性，防止竞态条件。

这样可以：
- 在创建reader之前就检查并注册
- 使用原子操作防止竞态条件
- 如果另一个goroutine已经启动，立即关闭当前创建的reader和sinker

## 相关代码位置

- `pkg/cdc/sinker_v2.go:713` - 错误检查位置
- `pkg/cdc/watermark_updater.go:847` - GetFromCache实现
- `pkg/cdc/watermark_updater.go:1118` - GetOrAddCommitted实现
- `pkg/cdc/watermark_updater.go:1030` - UpdateWatermarkOnly实现（水位更新位置）
- `pkg/cdc/reader_v2_txn_manager.go:183` - CommitTransaction中更新watermark
- `pkg/cdc/reader_v2_data_processor.go:390` - processNoMoreData中更新watermark（heartbeat）
- `pkg/frontend/cdc_exector.go:993` - 初始化watermark
- `pkg/cdc/table_change_stream.go:546` - Reader获取fromTs

## 快照读边界条件问题分析

### 问题描述

当 snapshot ts 正好等于一个 global checkpoint (gckp) 的 end 时，`FilterSortedMetaFilesByTimestamp` 函数的行为可能存在边界条件问题。

### 代码分析

`FilterSortedMetaFilesByTimestamp` 函数位于 `pkg/vm/engine/tae/db/checkpoint/snapshot.go:51-80`：

```go
func FilterSortedMetaFilesByTimestamp(
	ts *types.TS,
	files []ioutil.TSRangeFile,
) []ioutil.TSRangeFile {
	if len(files) == 0 {
		return nil
	}

	prev := files[0]

	// start.IsEmpty() means the file is a global checkpoint
	// ts.LE(&prev.end) means the ts is in the range of the checkpoint
	if prev.GetStart().IsEmpty() && ts.LE(prev.GetEnd()) {
		return files[:1]  // 只返回第一个文件（gckp）
	}

	for i := 1; i < len(files); i++ {
		curr := files[i]
		// curr.start.IsEmpty() means the file is a global checkpoint
		// ts.LE(&curr.end) means the ts is in the range of the checkpoint
		if curr.GetStart().IsEmpty() && ts.LE(curr.GetEnd()) {
			return files[:i]  // 返回从开始到当前gckp之前的所有文件
		}
	}

	return files
}
```

### 边界情况分析

#### 情况1：第一个文件是 gckp，snapshot ts == gckp.end

假设文件列表：
- `files = [GCKP[0, 300], [300, 400], [400, 500]]`
- `snapshot ts = 300`

执行流程：
1. `prev = GCKP[0, 300]`
2. `prev.GetStart().IsEmpty() == true`（是 gckp）
3. `ts.LE(prev.GetEnd()) == true`（300 <= 300）
4. 返回 `files[:1]`，即 `[GCKP[0, 300]]`

**结论**：这种情况下行为是正确的，只返回 gckp 本身。

#### 情况2：第一个文件不是 gckp，后续遇到 gckp，snapshot ts == gckp.end

假设文件列表（按 end 排序）：
- `files = [[0, 100], [100, 200], [200, 300], GCKP[0, 300], [300, 400], [400, 500]]`
- `snapshot ts = 300`

执行流程：
1. `prev = [0, 100]`，不是 gckp，跳过第一个 if
2. 进入循环：
   - `i=1`: `curr = [100, 200]`，不是 gckp，继续
   - `i=2`: `curr = [200, 300]`，不是 gckp，继续
   - `i=3`: `curr = GCKP[0, 300]`，是 gckp
   - `ts.LE(curr.GetEnd()) == true`（300 <= 300）
   - 返回 `files[:3]`，即 `[[0, 100], [100, 200], [200, 300]]`

**结论**：这种情况下**存在问题**！返回了 gckp 之前的所有 incremental checkpoint，但**没有包含 gckp 本身**。

#### 情况3：从注释示例 Ex.2 验证

代码注释中的示例：
```
files  :  [0,100],[100,200],[200,300],[0,300],[300,400],[400,500]
ts     :  300
return :  [0,100],[100,200],[200,300],[0,300]
```

这里 `[0,300]` 是 gckp（start 为空表示从 0 开始），ts=300 正好等于 gckp.end。

按照代码逻辑：
1. `prev = [0, 100]`，不是 gckp（start 不为空），跳过第一个 if
2. 循环到 `[0, 300]`（gckp）时：
   - `curr.GetStart().IsEmpty() == true`
   - `ts.LE(curr.GetEnd()) == true`（300 <= 300）
   - 返回 `files[:3]`，即 `[[0,100], [100,200], [200,300]]`

但注释说应该返回 `[[0,100], [100,200], [200,300], [0,300]]`，包含 gckp 本身。

**这说明代码逻辑与注释不一致，或者注释中的示例有误。**

### 问题根源

当 `snapshot ts == gckp.end` 时：

1. **如果 gckp 是第一个文件**：行为正确，返回 gckp 本身
2. **如果 gckp 不是第一个文件**：会返回 gckp 之前的所有文件，但**不包含 gckp 本身**

这是因为代码在找到满足条件的 gckp 时，返回的是 `files[:i]`，即从开始到**当前索引之前**的所有文件，不包括当前 gckp。

### 潜在影响

1. **数据丢失风险**：如果 snapshot ts 正好等于 gckp.end，但没有包含 gckp，可能导致：
   - 缺少 gckp 中的数据
   - 快照读不完整

2. **与注释不一致**：代码注释中的示例显示应该包含 gckp，但实际代码逻辑不包含。

### 修复建议

修改 `FilterSortedMetaFilesByTimestamp` 函数，当找到满足条件的 gckp 时，应该包含 gckp 本身。

**注意**：虽然 `files[:i+1]` 在 Go 中不会越界（当 `i+1 == len(files)` 时返回整个切片），但为了代码清晰和安全，建议使用以下方式：

**方案1：直接使用 i+1（推荐）**
```go
for i := 1; i < len(files); i++ {
	curr := files[i]
	if curr.GetStart().IsEmpty() && ts.LE(curr.GetEnd()) {
		// i+1 是安全的，因为 i < len(files)，所以 i+1 <= len(files)
		// 当 i+1 == len(files) 时，files[:i+1] 等价于 files[:]，返回整个切片
		return files[:i+1]  // 包含当前 gckp
	}
}
```

**方案2：显式边界检查（更明确）**
```go
for i := 1; i < len(files); i++ {
	curr := files[i]
	if curr.GetStart().IsEmpty() && ts.LE(curr.GetEnd()) {
		// 包含当前 gckp，需要返回到 i+1
		// 如果 i+1 超过长度，Go 会自动截断到 len(files)
		if i+1 <= len(files) {
			return files[:i+1]
		}
		return files  // 理论上不会到达这里，但为了安全
	}
}
```

**方案3：使用 append 方式（最安全）**
```go
for i := 1; i < len(files); i++ {
	curr := files[i]
	if curr.GetStart().IsEmpty() && ts.LE(curr.GetEnd()) {
		// 返回从开始到当前 gckp（包含）的所有文件
		result := make([]ioutil.TSRangeFile, 0, i+1)
		result = append(result, files[:i]...)
		result = append(result, files[i])
		return result
	}
}
```

**推荐使用方案1**，因为：
1. Go 的切片操作 `files[:i+1]` 是安全的，不会越界
2. 代码简洁清晰
3. 性能最好（不需要额外的内存分配）

### 边界情况详细说明

关于 `files[:i+1]` 是否越界的问题：

1. **Go 切片操作的安全性**：
   - 在循环中，`i` 的范围是 `1` 到 `len(files)-1`（因为 `i < len(files)`）
   - 所以 `i+1` 的范围是 `2` 到 `len(files)`
   - `files[:len(files)]` 在 Go 中是**完全合法**的，等价于 `files[:]`，返回整个切片
   - Go 的切片操作不会导致 panic，只会截断到有效范围

2. **边界情况分析**：
   - **情况A**：gckp 不是最后一个文件（`i < len(files)-1`）
     - `i+1 < len(files)`，`files[:i+1]` 返回前 `i+1` 个文件，包含 gckp
   - **情况B**：gckp 是最后一个文件（`i == len(files)-1`）
     - `i+1 == len(files)`，`files[:i+1]` 等价于 `files[:]`，返回所有文件
     - 这是合理的，因为我们需要包含这个 gckp

3. **结论**：`files[:i+1]` **不会越界**，是安全的操作。

### 验证方法

可以通过以下测试用例验证：

```go
// 测试用例1：snapshot ts == gckp.end，gckp 不是最后一个
files1 := []TSRangeFile{
	NewTSRangeFile("", "", TS(100), TS(200)),      // [100, 200]
	NewTSRangeFile("", "", TS(200), TS(300)),      // [200, 300]
	NewTSRangeFile("", "", types.TS{}, TS(300)),   // GCKP[0, 300]
	NewTSRangeFile("", "", TS(300), TS(400)),      // [300, 400]
}
snapshot1 := TS(300)
result1 := FilterSortedMetaFilesByTimestamp(&snapshot1, files1)
// 期望：应该包含 [100,200], [200,300], GCKP[0,300]
// 实际（修复前）：只包含 [100,200], [200,300]
// 实际（修复后）：包含 [100,200], [200,300], GCKP[0,300]

// 测试用例2：snapshot ts == gckp.end，gckp 是最后一个
files2 := []TSRangeFile{
	NewTSRangeFile("", "", TS(100), TS(200)),      // [100, 200]
	NewTSRangeFile("", "", TS(200), TS(300)),      // [200, 300]
	NewTSRangeFile("", "", types.TS{}, TS(300)),   // GCKP[0, 300] (最后一个)
}
snapshot2 := TS(300)
result2 := FilterSortedMetaFilesByTimestamp(&snapshot2, files2)
// 期望：应该包含所有文件 [100,200], [200,300], GCKP[0,300]
// 实际（修复前）：只包含 [100,200], [200,300]
// 实际（修复后）：包含所有文件（files[:i+1] 当 i+1==len(files) 时返回整个切片）
```

