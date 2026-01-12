# GC 模块代码问题与设计缺陷

## 1. 错误处理问题

### 1.1 错误被静默忽略

**位置**: `executor.go` - `Run` 方法

```go
defer func() {
    if err = DeleteObjects(ctx, exec.fs, canGCObjects); err != nil {
        //TODO: handle error
        err = nil  // 错误被静默忽略！
    }
}()
```

**问题**: 删除临时文件失败时错误被忽略，可能导致文件泄漏。

**建议**: 
- 记录错误日志
- 考虑重试机制
- 或者将清理任务加入后台队列

### 1.2 Panic 恢复被注释

**位置**: `diskcleaner.go` - `doReplayAndExecute`

```go
func (cleaner *DiskCleaner) doReplayAndExecute(ctx context.Context) (err error) {
    // defer func() {
    //     if err := recover(); err != nil {
    //         logutil.Error("GC-Replay-Panic", zap.Any("err", err))
    //     }
    // }()
```

**问题**: Panic 恢复代码被注释，如果发生 panic 会导致整个 GC 进程崩溃。

**建议**: 启用 panic 恢复，并添加适当的错误处理和告警。

### 1.3 错误处理不一致

**位置**: `checkpoint.go` 多处

```go
if err = c.fs.Delete(c.ctx, ioutil.MakeGCFullName(maxFile)); err != nil {
    logutil.Error(...)
    v2.GCErrorIOErrorCounter.Inc()
    return  // 有时返回错误
}

// 但在其他地方：
if err = c.fs.Delete(ctx, filesToDelete...); err != nil {
    logutil.Error(...)
    v2.GCErrorIOErrorCounter.Inc()
    return err  // 有时返回 err
}
```

**问题**: 错误处理方式不一致，有时返回空，有时返回错误。

**建议**: 统一错误处理策略，明确哪些错误应该中断流程，哪些可以继续。

---

## 2. 并发安全问题

### 2.1 锁粒度过大

**位置**: `checkpoint.go` - `checkpointCleaner`

```go
func (c *checkpointCleaner) Process(...) error {
    c.StartMutationTask("gc-process")  // 获取 mutation.Mutex
    defer c.StopMutationTask()
    // 整个 GC 过程都持有锁
    // ...
}
```

**问题**: 整个 GC 过程持有 `mutation.Mutex`，阻塞其他操作。

**建议**: 
- 细化锁粒度
- 将只读操作和写操作分离
- 考虑使用读写锁

### 2.2 潜在的死锁风险

**位置**: `checkpoint.go` 和 `snapshot.go`

```go
// checkpoint.go
c.backupProtection.Lock()
// ... 创建快照
c.backupProtection.Unlock()

// 然后
c.mutation.Lock()
// ... 使用快照
```

**问题**: 多个锁的获取顺序可能不一致，存在死锁风险。

**建议**: 
- 明确锁的获取顺序
- 使用 `defer` 确保锁释放
- 考虑使用 `sync.Map` 替代部分场景

### 2.3 原子操作与锁混用

**位置**: `diskcleaner.go`

```go
type DiskCleaner struct {
    step        atomic.Uint32
    replayError atomic.Pointer[error]
    runningCtx  atomic.Pointer[runningCtx]
    // ...
}
```

**问题**: 同时使用原子操作和锁，增加了代码复杂度和出错概率。

**建议**: 统一并发控制策略，要么全用锁，要么全用原子操作。

---

## 3. 代码重复问题

### 3.1 对象收集逻辑重复

**位置**: `window.go`, `checkpoint.go`, `exec_v1.go`

多处存在类似的对象收集逻辑：

```go
// window.go
for i := 0; i < bat.Vecs[0].Length(); i++ {
    stats := objectio.ObjectStats(bat.Vecs[0].GetBytesAt(i))
    // ...
}

// exec_v1.go
for i := 0; i < bat.Vecs[0].Length(); i++ {
    stats := objectio.ObjectStats(bat.Vecs[0].GetBytesAt(i))
    // ...
}
```

**建议**: 抽取公共函数，减少代码重复。

### 3.2 Bloom Filter 构建重复

**位置**: `window.go`, `checkpoint.go`

```go
// window.go
bf, err = BuildBloomfilter(ctx, Default_Coarse_EstimateRows, ...)

// checkpoint.go
bf, err := BuildBloomfilter(ctx, Default_Coarse_EstimateRows, ...)
```

**建议**: 考虑缓存 Bloom Filter 或统一构建入口。

---

## 4. 魔法数字和硬编码

### 4.1 硬编码的超时和大小

**位置**: 多处

```go
// deleter.go
var deleteTimeout = 10 * time.Minute
var deleteBatchSize = 1000
var deleteWorkerNum = 4

// checkpoint.go
time.Since(c.backupProtection.lastUpdateTime) > 20*time.Minute  // 硬编码 20 分钟

// exec_v1.go
const Default_Coarse_EstimateRows = 10000000
const Default_Coarse_Probility    = 0.00001
const Default_CanGC_TailSize      = 64 * malloc.MB
```

**问题**: 这些值应该是可配置的，而不是硬编码。

**建议**: 
- 将这些值移到配置结构中
- 提供合理的默认值
- 支持运行时调整

### 4.2 魔法数字

**位置**: `snapshot.go`

```go
hoursAgo := types.BuildTS(time.Now().UnixNano()-int64(3*time.Hour), 0)
```

**问题**: `3*time.Hour` 是魔法数字，含义不明确。

**建议**: 定义为常量并添加注释说明其用途。

---

## 5. 注释和文档问题

### 5.1 TODO 注释未处理

**位置**: 多处

```go
// executor.go
//TODO: handle error

// checkpoint.go
// TODO: seem to be a bug
delete(metaFiles, maxFile)

// TODO:Requires Physical Removal Policy
```

**问题**: 存在多个未处理的 TODO，表明代码不完整。

**建议**: 逐一处理这些 TODO，或者创建 issue 跟踪。

### 5.2 注释与代码不一致

**位置**: `checkpoint.go`

```go
// when call this function: at least one incremental checkpoint has been scanned
// 但实际上函数内部没有检查这个前置条件
```

**建议**: 添加断言或检查来验证前置条件。

---

## 6. 类型设计问题

### 6.1 过度使用 map

**位置**: `snapshot.go`

```go
type SnapshotMeta struct {
    objects      map[uint64]map[objectio.Segmentid]*objectInfo
    tombstones   map[uint64]map[objectio.Segmentid]*objectInfo
    tables       map[uint32]map[uint64]*tableInfo
    tableIDIndex map[uint64]*tableInfo
    tablePKIndex map[string][]*tableInfo
    // ...
}
```

**问题**: 
- 嵌套 map 结构复杂，难以维护
- 内存分配碎片化
- 遍历效率低

**建议**: 
- 考虑使用更扁平的数据结构
- 或者使用专门的索引结构

### 6.2 接口设计过于庞大

**位置**: `types.go` - `Cleaner` 接口

```go
type Cleaner interface {
    Replay(context.Context) error
    Process(context.Context, func(*checkpoint.CheckpointEntry) bool) error
    AddChecker(checker func(item any) bool, key string) int
    RemoveChecker(key string) error
    GetScanWaterMark() *checkpoint.CheckpointEntry
    GetCheckpointGCWaterMark() *types.TS
    GetScannedWindow() *GCWindow
    Stop()
    GetMinMerged() *checkpoint.CheckpointEntry
    DoCheck(context.Context) error
    GetPITRs() (*logtail.PitrInfo, error)
    SetTid(tid uint64)
    EnableGC()
    DisableGC()
    GCEnabled() bool
    GetMPool() *mpool.MPool
    GetSnapshots() (*logtail.SnapshotInfo, error)
    GetDetails(ctx context.Context) (map[uint32]*TableStats, error)
    Verify(ctx context.Context) string
    ISCPTables() (map[uint64]types.TS, error)
    SetBackupProtection(protectedTS types.TS)
    UpdateBackupProtection(protectedTS types.TS)
    RemoveBackupProtection()
    GetBackupProtection() (protectedTS types.TS, lastUpdateTime time.Time, isActive bool)
    GetTablePK(tableId uint64) string
}
```

**问题**: 接口包含 25+ 个方法，违反接口隔离原则。

**建议**: 
- 拆分为多个小接口
- 使用组合而非继承

---

## 7. 资源管理问题

### 7.1 内存泄漏风险

**位置**: `window.go`

```go
func (w *GCWindow) LoadBatchData(...) (bool, error) {
    // ...
    err := loader(ctx, w.fs, &w.files[0], bat, mp)
    // 如果 err != nil，w.files[0] 可能没有被正确处理
    w.files = w.files[1:]
    return false, nil
}
```

**问题**: 错误情况下资源可能没有被正确释放。

**建议**: 使用 defer 确保资源释放。

### 7.2 Batch 复用不当

**位置**: 多处

```go
bat := buffer.Fetch()
defer buffer.Putback(bat, mp)
// ...
bat.CleanOnlyData()  // 只清理数据，不清理结构
```

**问题**: `CleanOnlyData` 可能不够彻底，导致数据残留。

**建议**: 明确 Batch 复用的语义，确保清理彻底。
