# MatrixOne CDC模块完整分析

## 目录

1. [模块概述](#模块概述)
2. [整体架构](#整体架构)
3. [核心组件详解](#核心组件详解)
4. [数据流和调用路径](#数据流和调用路径)
5. [关键原理](#关键原理)
6. [代码细节](#代码细节)
7. [状态管理](#状态管理)
8. [错误处理机制](#错误处理机制)

---

## 模块概述

### 功能定位

CDC (Change Data Capture) 是MatrixOne的实时数据复制功能，用于将源数据库的变更同步到目标MySQL兼容数据库。

### 核心特性

- **实时同步**：捕获INSERT、UPDATE、DELETE操作
- **多级支持**：Account、Database、Table级别复制
- **初始快照**：可选的完整数据快照
- **时间范围控制**：支持StartTs和EndTs
- **自动恢复**：内置重试机制
- **状态管理**：支持Pause、Resume、Restart

---

## 整体架构

### 架构图

```
┌─────────────────────────────────────────────────────────────┐
│                    Frontend Layer                            │
│  ┌──────────────────────────────────────────────────────┐   │
│  │         CDCTaskExecutor (任务执行器)                  │   │
│  │  - 任务生命周期管理                                    │   │
│  │  - 状态机管理                                         │   │
│  │  - 表扫描器注册                                       │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    CDC Core Layer                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │ TableScanner │  │WatermarkUpdtr│  │TableDetector │     │
│  │ (表扫描器)   │  │ (水位管理器)  │  │ (表检测器)   │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│              Data Pipeline (数据管道)                        │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │         TableChangeStream (变更流)                    │   │
│  │  - 定时轮询变更                                        │   │
│  │  - 事务管理                                           │   │
│  │  - 数据处理                                           │   │
│  └──────────────────────────────────────────────────────┘   │
│                            │                                 │
│        ┌───────────────────┼───────────────────┐            │
│        ▼                   ▼                   ▼            │
│  ┌──────────┐      ┌──────────────┐    ┌──────────┐       │
│  │ Change   │      │ DataProcessor│    │Transaction│       │
│  │ Collector│─────▶│ (数据处理器) │───▶│ Manager   │       │
│  │ (收集器) │      │              │    │ (事务管理)│       │
│  └──────────┘      └──────────────┘    └──────────┘       │
│                            │                                 │
│                            ▼                                 │
│                    ┌──────────────┐                         │
│                    │   Sinker     │                         │
│                    │  (数据下沉)   │                         │
│                    └──────────────┘                         │
│                            │                                 │
│                            ▼                                 │
│                    ┌──────────────┐                         │
│                    │   Executor   │                         │
│                    │  (SQL执行器)  │                         │
│                    └──────────────┘                         │
│                            │                                 │
│                            ▼                                 │
│                    ┌──────────────┐                         │
│                    │  Target DB   │                         │
│                    │  (目标数据库) │                         │
│                    └──────────────┘                         │
└─────────────────────────────────────────────────────────────┘
```

### 核心组件关系

```
CDCTaskExecutor
    ├── TableDetector (表检测器，检测新表)
    ├── WatermarkUpdater (水位管理器，全局单例)
    └── TableChangeStream (每个表一个)
            ├── ChangeCollector (变更收集器)
            ├── DataProcessor (数据处理器)
            ├── TransactionManager (事务管理器)
            └── Sinker (数据下沉器)
                    └── Executor (SQL执行器)
```

---

## 核心组件详解

### 1. CDCTaskExecutor (任务执行器)

**位置**: `pkg/frontend/cdc_exector.go`

**职责**:
- 管理CDC任务的完整生命周期
- 处理任务状态转换（Start/Pause/Resume/Restart）
- 注册表检测器，监听新表创建
- 为每个表创建执行管道

**关键方法**:
```go
Start()          // 启动任务
Pause()          // 暂停任务
Resume()         // 恢复任务
Restart()        // 重启任务
addExecPipelineForTable()  // 为表创建执行管道
```

**状态机**:
```
Idle → Starting → Running → Paused
  ↑       ↓         ↓         ↓
  └───────┴─────────┴─────────┘
```

### 2. TableDetector (表检测器)

**位置**: `pkg/cdc/table_scanner.go`

**职责**:
- 定期扫描数据库，检测新创建的表
- 维护订阅关系（哪些任务关注哪些表）
- 触发回调函数，通知新表创建

**工作原理**:
1. 每个CDC任务注册到TableDetector
2. TableDetector定期扫描系统表 `mo_tables`
3. 发现新表时，调用注册的回调函数
4. 回调函数创建TableChangeStream

**关键数据结构**:
```go
type TableDetector struct {
    Mp                   map[uint32]TblMap  // accountId -> {db.table -> DbTableInfo}
    Callbacks            map[string]TableCallback  // taskId -> callback
    SubscribedAccountIds map[uint32][]string  // accountId -> [taskIds]
    SubscribedDbNames    map[string][]string  // dbName -> [taskIds]
    SubscribedTableNames map[string][]string  // tableName -> [taskIds]
}
```

### 3. WatermarkUpdater (水位管理器)

**位置**: `pkg/cdc/watermark_updater.go`

**职责**:
- 管理每个表的watermark（水位线）
- 提供三层缓存架构
- 异步批量持久化watermark到数据库

**三层缓存架构**:
```
cacheUncommitted (未提交缓存)
    ↓ (每3秒由CronJob移动)
cacheCommitting (提交中缓存)
    ↓ (批量UPDATE到数据库)
cacheCommitted (已提交缓存) ←→ Database (mo_cdc_watermark表)
```

**一致性模型**:
- **允许滞后**：watermark可以滞后于实际进度（导致重复处理，可接受）
- **禁止超前**：watermark绝不能超前于已持久化的数据（会导致数据丢失）

**关键方法**:
```go
GetOrAddCommitted()      // 获取或添加已提交的watermark（初始化时使用）
GetFromCache()           // 从缓存获取watermark（读取时使用）
UpdateWatermarkOnly()    // 更新watermark（数据提交后调用）
UpdateWatermarkErrMsg()  // 更新错误信息
```

### 4. TableChangeStream (变更流)

**位置**: `pkg/cdc/table_change_stream.go`

**职责**:
- 定时轮询表变更（默认200ms）
- 协调整个数据处理流程
- 管理reader的生命周期

**工作流程**:
```
Run() 
  → calculateInitialDelay()  // 计算初始延迟
  → for循环:
      → processOneRound()     // 处理一轮变更
          → processWithTxn()  // 在事务中处理
              → CollectChanges()  // 收集变更
              → DataProcessor.ProcessChange()  // 处理变更
              → TransactionManager.Commit()    // 提交事务
```

**关键组件**:
- `ChangeCollector`: 从引擎收集变更数据
- `DataProcessor`: 处理变更数据，转换为SQL
- `TransactionManager`: 管理事务生命周期
- `Sinker`: 发送数据到下游

### 5. ChangeCollector (变更收集器)

**位置**: `pkg/cdc/reader_v2_change_collector.go`

**职责**:
- 从MatrixOne引擎收集表变更
- 区分Snapshot和Tail变更
- 按时间范围过滤数据

**变更类型**:
- `Snapshot`: 初始快照数据
- `TailWip`: 增量变更（进行中）
- `TailDone`: 增量变更（已完成）
- `NoMoreData`: 没有更多数据

### 6. DataProcessor (数据处理器)

**位置**: `pkg/cdc/reader_v2_data_processor.go`

**职责**:
- 处理不同类型的变更数据
- 累积TailWip/TailDone到AtomicBatch
- 决定何时开始事务
- 发送数据到Sinker

**处理逻辑**:
```go
ProcessChange(data *ChangeData)
  switch data.Type:
    case ChangeTypeSnapshot:
      → 直接发送到Sinker（可选：拆分事务）
    case ChangeTypeTailWip:
      → 累积到AtomicBatch（不发送）
    case ChangeTypeTailDone:
      → 累积到AtomicBatch
      → 如果达到阈值，发送到Sinker
    case ChangeTypeNoMoreData:
      → 发送所有累积的数据
```

**AtomicBatch**:
- 使用B-Tree按(TS, PK)排序
- 保证同一主键的变更按时间顺序处理
- 支持去重和合并

### 7. TransactionManager (事务管理器)

**位置**: `pkg/cdc/reader_v2_txn_manager.go`

**职责**:
- 管理事务生命周期（BEGIN/COMMIT/ROLLBACK）
- 双重安全检查（Tracker + Watermark）
- 确保数据一致性

**事务流程**:
```
BeginTransaction()
  → 创建TransactionTracker
  → Sinker.SendBegin()
  → 标记hasBegin = true

CommitTransaction()
  → Sinker.SendCommit()
  → WatermarkUpdater.UpdateWatermarkOnly()  // 关键：先更新watermark
  → 标记hasCommitted = true

RollbackTransaction()
  → Sinker.SendRollback()
  → 标记hasRolledBack = true
```

**双重安全检查**:
1. **Layer 1**: TransactionTracker（内存状态，快速检查）
2. **Layer 2**: Watermark（持久化状态，可靠检查）

### 8. Sinker (数据下沉器)

**位置**: `pkg/cdc/sinker_v2.go`

**职责**:
- 接收数据并转换为SQL命令
- 管理命令队列
- 执行SQL到目标数据库

**架构**:
```
Producer (Reader) 
    → Command Channel (无缓冲，提供背压)
        → Consumer Goroutine
            → Executor
                → Target Database
```

**命令类型**:
- `BeginCommand`: 开始事务
- `CommitCommand`: 提交事务
- `RollbackCommand`: 回滚事务
- `InsertBatchCommand`: 插入批次
- `InsertDeleteBatchCommand`: 插入+删除批次
- `FlushCommand`: 刷新缓冲区

**状态机**:
```
IDLE --SendBegin--> ACTIVE --SendCommit--> COMMITTED
                        │
                        └--SendRollback--> ROLLED_BACK
```

**关键方法**:
```go
Sink(data *DecoderOutput)  // 验证watermark，发送数据
SendBegin()                // 发送BEGIN命令
SendCommit()               // 发送COMMIT命令
SendRollback()             // 发送ROLLBACK命令
Error()                    // 获取错误状态
ClearError()               // 清除错误状态
```

---

## 数据流和调用路径

### 完整调用路径

```
1. 用户执行 CREATE CDC TASK
   ↓
2. Frontend解析SQL
   ↓
3. TaskService创建任务
   ↓
4. CDCTaskExecutor.Start()
   ├── 初始化WatermarkUpdater（全局单例）
   ├── 注册到TableDetector
   └── 等待新表或处理现有表
       ↓
5. TableDetector检测到新表
   ↓
6. 调用handleNewTables回调
   ↓
7. CDCTaskExecutor.addExecPipelineForTable()
   ├── 获取watermark（GetOrAddCommitted）
   ├── 创建Sinker
   ├── 创建TableChangeStream
   └── 启动goroutine
       ↓
8. TableChangeStream.Run()
   ├── 计算初始延迟
   └── 主循环（每200ms）:
       ↓
9. processOneRound()
   ├── 创建事务操作符
   ├── processWithTxn()
       ├── 获取fromTs（GetFromCache）
       ├── 获取toTs（当前snapshot TS）
       ├── CollectChanges(fromTs, toTs)
       │   └── 从引擎读取变更数据
       ├── 创建ChangeCollector
       ├── DataProcessor.SetTransactionRange()
       └── 处理变更:
           ↓
10. DataProcessor.ProcessChange()
    ├── 根据变更类型处理
    ├── 累积到AtomicBatch（TailWip/TailDone）
    └── 发送到Sinker（Snapshot/达到阈值）
        ↓
11. Sinker.Sink()
    ├── 验证watermark（data.toTs > watermark）
    ├── 构建Command
    └── 发送到Command Channel
        ↓
12. Sinker Consumer Goroutine
    ├── 从Channel接收Command
    ├── 构建SQL（SQLBuilder）
    └── Executor.ExecSQL()
        ↓
13. 执行SQL到目标数据库
    ↓
14. TransactionManager.CommitTransaction()
    ├── Sinker.SendCommit()
    └── WatermarkUpdater.UpdateWatermarkOnly()
        ↓
15. WatermarkUpdater异步持久化
    ├── cacheUncommitted → cacheCommitting
    └── 批量UPDATE到数据库（每3秒）
```

### 关键数据流

#### 1. 变更数据流

```
MatrixOne Engine
    ↓ (CollectChanges)
ChangeCollector
    ↓ (ChangeData)
DataProcessor
    ↓ (累积/转换)
AtomicBatch / DecoderOutput
    ↓ (Sink)
Sinker
    ↓ (Command)
Executor
    ↓ (SQL)
Target Database
```

#### 2. Watermark流

```
TransactionManager.CommitTransaction()
    ↓
WatermarkUpdater.UpdateWatermarkOnly()
    ↓
cacheUncommitted (立即)
    ↓ (CronJob每3秒)
cacheCommitting
    ↓ (批量UPDATE)
cacheCommitted + Database
    ↓ (下次读取)
GetFromCache() / GetOrAddCommitted()
```

#### 3. 错误流

```
任何组件发生错误
    ↓
设置错误状态（Sinker.SetError()）
    ↓
TableChangeStream捕获错误
    ↓
UpdateWatermarkErrMsg()
    ↓
errorMetadataCache (内存)
    ↓ (异步)
Database (mo_cdc_watermark.err_msg)
    ↓ (查询)
GetTableErrMsg()
    ↓
显示错误信息
```

---

## 关键原理

### 1. Watermark机制

**目的**: 记录每个表已处理到哪个时间点，支持断点续传。

**存储位置**: `mo_catalog.mo_cdc_watermark` 表

**表结构**:
```sql
CREATE TABLE mo_cdc_watermark (
    account_id BIGINT,
    task_id VARCHAR(36),
    db_name VARCHAR(64),
    table_name VARCHAR(64),
    watermark VARCHAR(64),  -- 时间戳字符串
    err_msg TEXT            -- 错误信息
);
```

**一致性保证**:
- **允许滞后**：watermark可以滞后（重启后可能重复处理，但不会丢失数据）
- **禁止超前**：watermark绝不能超前（会导致数据丢失）

**三层缓存设计原因**:
1. **性能优化**：避免每次更新都写数据库
2. **批量处理**：每3秒批量UPDATE，减少数据库压力
3. **崩溃恢复**：即使崩溃，最多丢失3秒的watermark更新（可接受）

### 2. 事务管理

**双重安全检查**:

1. **TransactionTracker（内存）**:
   - 快速检查事务状态
   - 跟踪BEGIN/COMMIT/ROLLBACK
   - 崩溃后丢失，不可靠

2. **Watermark（持久化）**:
   - 可靠的事务完成证明
   - 存储在数据库中
   - 崩溃后仍可用

**提交顺序（关键）**:
```
1. Sinker.SendCommit()        // 先提交到目标数据库
2. WatermarkUpdater.UpdateWatermarkOnly()  // 再更新watermark
```

**为什么这个顺序很重要**:
- 如果先更新watermark，但目标数据库提交失败，会导致数据丢失
- 先提交到目标数据库，即使watermark更新失败，最多重复处理（可接受）

### 3. AtomicBatch机制

**目的**: 处理同一主键的多次变更，保证顺序和去重。

**数据结构**:
```go
type AtomicBatch struct {
    Batches []*batch.Batch  // 原始批次
    Rows    *btree.BTreeG[AtomicBatchRow]  // 按(TS, PK)排序的B-Tree
}

type AtomicBatchRow struct {
    Ts     types.TS  // 时间戳
    Pk     []byte    // 主键
    Offset int       // 在批次中的偏移
    Src    *batch.Batch  // 源批次
}
```

**排序规则**:
1. 首先按TS升序
2. TS相同时，按PK升序

**处理逻辑**:
- TailWip: 累积到AtomicBatch，不发送
- TailDone: 累积到AtomicBatch，达到阈值时发送
- NoMoreData: 发送所有累积的数据

### 4. 表检测机制

**目的**: 自动检测新创建的表，无需手动添加。

**实现方式**:
1. 定期扫描 `mo_catalog.mo_tables` 表
2. 比较当前表和上次扫描的表列表
3. 发现新表时，调用注册的回调函数

**订阅关系**:
- Account级别: 订阅整个账户的所有表
- Database级别: 订阅特定数据库的所有表
- Table级别: 订阅特定表

### 5. 错误处理和重试

**错误分类**:
- **Retryable**: 可重试错误（网络超时、连接失败等）
- **Non-Retryable**: 不可重试错误（表不存在、类型不匹配等）

**重试机制**:
- 自动重试最多3次（MaxRetryCount = 3）
- 超过3次后，转换为Non-Retryable
- 错误信息存储在 `mo_cdc_watermark.err_msg`

**错误清除**:
- 成功处理数据后，自动清除错误
- 也可以通过 `resume cdc task` 手动清除

---

## 代码细节

### 1. 初始化流程

```go
// 1. 创建任务执行器
exec := NewCDCTaskExecutor(...)

// 2. 启动任务
exec.Start(ctx)
  ├── 获取WatermarkUpdater（全局单例）
  ├── 注册到TableDetector
  └── 等待新表

// 3. 检测到新表
handleNewTables()
  └── addExecPipelineForTable()
      ├── GetOrAddCommitted()  // 获取watermark
      ├── NewSinker()          // 创建Sinker
      ├── NewTableChangeStream()  // 创建Reader
      └── go reader.Run()      // 启动goroutine
```

### 2. 数据处理流程

```go
// TableChangeStream主循环
for {
    select {
    case <-tick.C:
        processOneRound()
            ├── GetTxnOp()           // 创建事务
            ├── GetFromCache()       // 获取fromTs
            ├── GetSnapshotTS()      // 获取toTs
            ├── CollectChanges()     // 收集变更
            └── ProcessChange()      // 处理变更
    }
}

// DataProcessor处理变更
ProcessChange(data)
    switch data.Type:
        case Snapshot:
            → 直接发送
        case TailWip:
            → 累积到AtomicBatch
        case TailDone:
            → 累积到AtomicBatch
            → 达到阈值时发送
        case NoMoreData:
            → 发送所有数据
```

### 3. 事务提交流程

```go
// TransactionManager提交
CommitTransaction()
    ├── Sinker.SendCommit()              // 1. 提交到目标数据库
    ├── UpdateWatermarkOnly()            // 2. 更新watermark
    └── tracker.MarkCommitted()          // 3. 标记已提交

// WatermarkUpdater更新
UpdateWatermarkOnly()
    └── cacheUncommitted[key] = watermark  // 立即更新内存

// CronJob持久化（每3秒）
cronRun()
    ├── cacheUncommitted → cacheCommitting
    └── 批量UPDATE到数据库
```

### 4. 错误处理流程

```go
// 任何组件发生错误
Sinker.SetError(err)
    └── err.Store(&err)  // 原子存储

// Reader检测错误
if err := sinker.Error(); err != nil {
    → 停止处理
    → UpdateWatermarkErrMsg()  // 记录错误
}

// 错误持久化
UpdateWatermarkErrMsg()
    ├── 解析错误类型（Retryable/Non-Retryable）
    ├── 更新errorMetadataCache
    └── 异步持久化到数据库
```

---

## 状态管理

### 任务状态机

```
Idle
  ↓ (Start)
Starting
  ↓ (成功)
Running
  ↓ (Pause)
Paused
  ↓ (Resume)
Starting
  ↓ (Restart)
Restarting
  ↓ (成功)
Running
```

### 事务状态机

```
IDLE
  ↓ (SendBegin)
ACTIVE
  ↓ (SendCommit)
COMMITTED
  ↓ (cleanup)
IDLE

ACTIVE
  ↓ (SendRollback)
ROLLED_BACK
  ↓ (cleanup)
IDLE
```

### Watermark状态

```
cacheUncommitted (未提交)
  ↓ (CronJob移动)
cacheCommitting (提交中)
  ↓ (数据库UPDATE成功)
cacheCommitted (已提交)
```

---

## 错误处理机制

### 错误类型

1. **Retryable Errors**:
   - 网络超时
   - 连接失败
   - 临时性错误

2. **Non-Retryable Errors**:
   - 表不存在
   - 类型不匹配
   - 权限错误

### 错误处理流程

```
错误发生
  ↓
Sinker.SetError()
  ↓
Reader检测到错误
  ↓
UpdateWatermarkErrMsg()
  ├── 解析错误类型
  ├── 更新重试计数
  └── 持久化到数据库
  ↓
GetTableErrMsg()查询
  ↓
显示错误信息
```

### 错误恢复

1. **自动恢复**: Retryable错误自动重试（最多3次）
2. **手动恢复**: 修复问题后，执行 `resume cdc task` 清除错误
3. **重启恢复**: 重启任务时，从watermark继续处理

---

## 总结

### 核心设计理念

1. **最终一致性**: Watermark允许滞后，保证不丢失数据
2. **双重安全**: Tracker + Watermark确保事务正确性
3. **异步批量**: Watermark批量更新，提高性能
4. **自动检测**: TableDetector自动发现新表
5. **优雅降级**: 错误不影响其他表的处理

### 关键优化点

1. **三层缓存**: 减少数据库写入压力
2. **批量处理**: AtomicBatch累积变更，批量发送
3. **无缓冲Channel**: 提供背压，防止内存溢出
4. **定时轮询**: 可配置的轮询频率，平衡延迟和性能

### 扩展性

- 支持多任务并发
- 支持多表并行处理
- 支持Account/Database/Table多级复制
- 支持MySQL和MatrixOne两种目标

---

## 相关文件索引

### 核心文件

- `pkg/frontend/cdc_exector.go`: 任务执行器
- `pkg/cdc/table_change_stream.go`: 变更流
- `pkg/cdc/watermark_updater.go`: 水位管理器
- `pkg/cdc/sinker_v2.go`: 数据下沉器
- `pkg/cdc/reader_v2_data_processor.go`: 数据处理器
- `pkg/cdc/reader_v2_txn_manager.go`: 事务管理器
- `pkg/cdc/table_scanner.go`: 表检测器

### 辅助文件

- `pkg/cdc/types.go`: 类型定义
- `pkg/cdc/util.go`: 工具函数
- `pkg/cdc/sql_builder.go`: SQL构建器
- `pkg/cdc/error_handler.go`: 错误处理
- `pkg/cdc/observability.go`: 可观测性








