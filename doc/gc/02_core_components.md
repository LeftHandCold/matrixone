# GC V3 核心组件详解

## 1. DiskCleaner - 磁盘清理器

### 1.1 结构定义

```go
type DiskCleaner struct {
    cleaner      Cleaner                      // 实际的清理器实现
    step         atomic.Uint32                // 当前状态步骤
    replayError  atomic.Pointer[error]        // 回放错误
    runningCtx   atomic.Pointer[runningCtx]   // 运行上下文
    processQueue sm.Queue                     // 处理队列
    onceStart    sync.Once                    // 启动控制
    onceStop     sync.Once                    // 停止控制
}
```

### 1.2 核心方法

| 方法 | 描述 |
|------|------|
| `GC(ctx)` | 触发普通 GC |
| `ForceGC(ctx, ts)` | 强制 GC 到指定时间戳 |
| `SwitchToWriteMode()` | 切换到写模式 |
| `SwitchToReplayMode()` | 切换到回放模式 |
| `Start()` | 启动清理器 |
| `Stop()` | 停止清理器 |

### 1.3 状态机

```
                    ┌──────────────────┐
                    │  StateStep_Write │
                    └────────┬─────────┘
                             │
              SwitchToReplayMode()
                             │
                             ▼
                    ┌──────────────────┐
                    │StateStep_Write2  │
                    │     Replay       │
                    └────────┬─────────┘
                             │
                      FlushQueue()
                             │
                             ▼
                    ┌──────────────────┐
                    │ StateStep_Replay │
                    └────────┬─────────┘
                             │
              SwitchToWriteMode()
                             │
                             ▼
                    ┌──────────────────┐
                    │StateStep_Replay2 │
                    │     Write        │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │  StateStep_Write │
                    └──────────────────┘
```

## 2. Cleaner 接口

```go
type Cleaner interface {
    // 核心方法
    Replay(context.Context) error
    Process(context.Context, func(*checkpoint.CheckpointEntry) bool) error
    Stop()
    
    // 水位线管理
    GetScanWaterMark() *checkpoint.CheckpointEntry
    GetCheckpointGCWaterMark() *types.TS
    GetScannedWindow() *GCWindow
    GetMinMerged() *checkpoint.CheckpointEntry
    
    // 检查器管理
    AddChecker(checker func(item any) bool, key string) int
    RemoveChecker(key string) error
    DoCheck(context.Context) error
    
    // 快照与PITR
    GetPITRs() (*logtail.PitrInfo, error)
    GetSnapshots() (*logtail.SnapshotInfo, error)
    
    // 备份保护
    SetBackupProtection(protectedTS types.TS)
    UpdateBackupProtection(protectedTS types.TS)
    RemoveBackupProtection()
    GetBackupProtection() (types.TS, time.Time, bool)
    
    // GC控制
    EnableGC()
    DisableGC()
    GCEnabled() bool
    
    // 其他
    SetTid(tid uint64)
    GetMPool() *mpool.MPool
    GetDetails(ctx context.Context) (map[uint32]*TableStats, error)
    Verify(ctx context.Context) string
    ISCPTables() (map[uint64]types.TS, error)
    GetTablePK(tableId uint64) string
}
```

## 3. checkpointCleaner - Checkpoint清理器

### 3.1 结构定义

```go
type checkpointCleaner struct {
    ctx           context.Context
    sid           string
    mp            *mpool.MPool
    fs            fileservice.FileService
    logDriver     wal.Store
    checkpointCli checkpoint.Runner
    deleter       *Deleter
    
    // 水位线
    watermarks struct {
        scanWaterMark         atomic.Pointer[checkpoint.CheckpointEntry]
        gcWaterMark           atomic.Pointer[checkpoint.CheckpointEntry]
        checkpointGCWaterMark atomic.Pointer[types.TS]
    }
    
    // 选项
    options struct {
        gcEnabled           atomic.Bool
        checkEnabled        atomic.Bool
        gcCheckpointEnabled atomic.Bool
    }
    
    // 配置
    config struct {
        canGCCacheSize          int
        maxMergeCheckpointCount int
        maxScanCheckpointCount  int
        estimateRows            int
        probility               float64
    }
    
    // 检查器
    checker struct {
        sync.RWMutex
        extras map[string]func(item any) bool
    }
    
    // 备份保护
    backupProtection struct {
        sync.RWMutex
        protectedTS    types.TS
        lastUpdateTime time.Time
        isActive       bool
    }
    
    // 可变状态
    mutation struct {
        sync.Mutex
        taskState struct {
            id        uint64
            name      string
            startTime time.Time
        }
        scanned      *GCWindow
        metaFiles    map[string]ioutil.TSRangeFile
        snapshotMeta *logtail.SnapshotMeta
        replayDone   bool
        backupProtectionSnapshot struct {
            protectedTS types.TS
            isActive    bool
        }
    }
}
```

### 3.2 配置选项

```go
// 可用的配置选项
WithCanGCCacheSize(size int)           // 可GC缓存大小
WithMaxMergeCheckpointCount(count int) // 最大合并Checkpoint数量
WithMaxScanCheckpointCount(count int)  // 最大扫描Checkpoint数量
WithGCProbility(probility float64)     // Bloom Filter误判率
WithEstimateRows(rows int)             // 预估行数
WithGCCheckpointOption(enable bool)    // 是否启用Checkpoint GC
WithCheckOption(enable bool)           // 是否启用检查
```

## 4. Deleter - 文件删除器

### 4.1 结构定义

```go
type Deleter struct {
    toDeletePaths   []string
    fs              fileservice.FileService
    deleteTimeout   time.Duration  // 默认 10 分钟
    deleteBatchSize int            // 默认 1000
    workerNum       int            // 默认 4
}
```

### 4.2 删除策略

- **批量删除**: 将文件分批处理，每批最多 `deleteBatchSize` 个文件
- **并发删除**: 使用 `workerNum` 个 worker 并发执行删除
- **超时控制**: 每批删除有 `deleteTimeout` 超时限制
- **错误处理**: 忽略 `ErrFileNotFound` 错误

### 4.3 配置方法

```go
SetDeleteBatchSize(cnt int)           // 设置批量大小
SetDeleteTimeout(duration time.Duration) // 设置超时时间
SetDeleteWorkerNum(num int)           // 设置worker数量
```

## 5. GCExecutor - GC执行器

### 5.1 结构定义

```go
type GCExecutor struct {
    buffer struct {
        isOwner bool
        impl    *containers.OneSchemaBatchBuffer
    }
    config struct {
        canGCCacheSize int
    }
    mp   *mpool.MPool
    fs   fileservice.FileService
    bm   bitmap.Bitmap
    sels []int64
}
```

### 5.2 执行流程

```go
func (exec *GCExecutor) Run(
    ctx context.Context,
    sourcer SourerFn,      // 数据源
    corseFilter FilterFn,  // 粗过滤
    fineFilter FilterFn,   // 细过滤
    finalCanGCSinker SinkerFn, // 最终可GC数据接收器
) (newFiles []objectio.ObjectStats, err error)
```

执行步骤：
1. 粗过滤：使用 Bloom Filter 快速筛选
2. 细过滤：检查快照和 PITR 引用
3. 输出：分离可GC和不可GC的对象

## 6. TableStats - 表统计信息

```go
type TableStats struct {
    SharedCnt  uint64  // 共享对象数量
    SharedSize uint64  // 共享对象大小
    TotalCnt   uint64  // 总对象数量
    TotalSize  uint64  // 总对象大小
}
```

用于统计每个账户的存储使用情况。
