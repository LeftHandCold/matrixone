# GC 执行器与过滤机制

## 1. GCExecutor 概述

`GCExecutor` 是 GC 模块的执行引擎，负责实际的过滤和数据处理操作。

## 2. 结构定义

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

## 3. 函数类型定义

```go
// 过滤函数：标记可GC的行
type FilterFn func(
    context.Context, 
    *bitmap.Bitmap,  // 输出：bit 1 表示可GC
    *batch.Batch, 
    *mpool.MPool,
) error

// 数据源函数：读取下一批数据
type SourerFn func(
    context.Context, 
    []string,        // 列名
    *plan.Expr,      // 过滤表达式
    *mpool.MPool, 
    *batch.Batch,    // 输出
) (bool, error)      // 返回是否结束

// 数据接收函数：处理输出数据
type SinkerFn func(context.Context, *batch.Batch) error
```

## 4. 核心执行流程

### 4.1 Run 方法

```go
func (exec *GCExecutor) Run(
    ctx context.Context,
    sourcer SourerFn,           // 数据源
    corseFilter FilterFn,       // 粗过滤
    fineFilter FilterFn,        // 细过滤
    finalCanGCSinker SinkerFn,  // 最终可GC数据接收器
) (newFiles []objectio.ObjectStats, err error)
```

**执行流程**：

```
┌─────────────────────────────────────────────────────────────┐
│                        Run 方法                              │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  1. 创建两个 Sinker                                          │
│     - cannotGCSinker: 存储不可GC的对象                       │
│     - canGCSinker: 存储可能可GC的对象                        │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  2. 第一轮过滤 (粗过滤)                                      │
│     doFilter(sourcer, corseFilter,                          │
│              cannotGCSinker, canGCSinker)                   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  3. 同步 canGCSinker                                         │
│     - 获取临时文件和内存表                                   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  4. 创建细过滤数据源                                         │
│     MakeLoadFunc(canGCMemTable, canGCObjects, ...)          │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  5. 第二轮过滤 (细过滤)                                      │
│     doFilter(fineSourcer, fineFilter,                       │
│              cannotGCSinker, finalCanGCSinker)              │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  6. 同步 cannotGCSinker                                      │
│     - 返回不可GC的对象列表                                   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  7. 删除临时文件                                             │
│     DeleteObjects(canGCObjects)                             │
└─────────────────────────────────────────────────────────────┘
```

### 4.2 doFilter 方法

```go
func (exec *GCExecutor) doFilter(
    ctx context.Context,
    sourcer SourerFn,
    filter FilterFn,
    cannotGCSinker SinkerFn,
    canGCSinker SinkerFn,
) error
```

**执行步骤**：

1. 从数据源读取一批数据
2. 按删除时间戳排序
3. 调用过滤函数，标记可GC的行
4. 分离可GC和不可GC的数据
5. 分别写入对应的 Sinker

## 5. CheckpointBasedGCJob

### 5.1 结构定义

```go
type CheckpointBasedGCJob struct {
    GCExecutor
    
    config struct {
        coarseEstimateRows int  // Bloom Filter 预估行数
        coarseProbility    float64  // Bloom Filter 误判率
        canGCCacheSize     int  // 可GC缓存大小
    }
    
    sourcer       engine.BaseReader
    snapshotMeta  *logtail.SnapshotMeta
    snapshots     *logtail.SnapshotInfo
    iscpTables    map[uint64]types.TS
    pitr          *logtail.PitrInfo
    ts            *types.TS
    globalCkpLoc  objectio.Location
    globalCkpVer  uint32
    checkpointCli checkpoint.Runner
    
    result struct {
        vecToGC    *vector.Vector        // 可删除的对象名
        filesNotGC []objectio.ObjectStats // 不可删除的对象
    }
}
```

### 5.2 默认配置

```go
const (
    Default_Coarse_EstimateRows = 10000000   // 1000万行
    Default_Coarse_Probility    = 0.00001    // 0.001% 误判率
    Default_CanGC_TailSize      = 64 * MB    // 64MB 缓存
)
```

### 5.3 Execute 方法

```go
func (e *CheckpointBasedGCJob) Execute(ctx context.Context) error
```

**执行步骤**：

1. 创建临时缓冲区
2. 构建粗过滤器（Bloom Filter）
3. 构建细过滤器（快照/PITR 检查）
4. 创建最终 Sinker
5. 调用 `Run` 执行过滤
6. 保存结果

## 6. 过滤器实现

### 6.1 粗过滤器 - Bloom Filter

```go
func MakeBloomfilterCoarseFilter(
    ctx context.Context,
    rowCount int,
    probability float64,
    buffer containers.IBatchBuffer,
    location objectio.Location,
    ckpVersion uint32,
    ts *types.TS,
    transObjects *map[string]map[uint64]*ObjectEntry,
    mp *mpool.MPool,
    fs fileservice.FileService,
) (FilterFn, error)
```

**过滤逻辑**：

1. 从全局 Checkpoint 读取所有对象
2. 构建 Bloom Filter
3. 对于每个待检查的对象：
   - 如果不在 Bloom Filter 中，标记为可GC
   - 记录到 `transObjects` 用于后续细过滤

### 6.2 细过滤器 - 快照/PITR 检查

```go
func MakeSnapshotAndPitrFineFilter(
    ts *types.TS,
    snapshots *logtail.SnapshotInfo,
    pitrs *logtail.PitrInfo,
    snapshotMeta *logtail.SnapshotMeta,
    transObjects map[string]map[uint64]*ObjectEntry,
    iscpTables map[uint64]types.TS,
    checkpointCli checkpoint.Runner,
) (FilterFn, error)
```

**过滤逻辑**：

1. 构建表存在性映射（从 snapshotMeta 和 catalog）
2. 获取每个表的快照和 PITR 时间戳
3. 对于每个对象：
   - 检查表是否仍存在
   - 检查是否被快照引用
   - 检查是否被 PITR 保护
   - 检查 ISCP 表约束

### 6.3 表存在性检查

```go
func buildTableExistenceMap(
    snapshotMeta *logtail.SnapshotMeta, 
    checkpointCli checkpoint.Runner,
) (map[uint64]bool, error)
```

合并两个来源的表信息：
- `snapshotMeta` 中的表
- `catalog` 中未删除的表

## 7. 最终 Sinker

```go
func MakeFinalCanGCSinker(
    vec *vector.Vector,
    mp *mpool.MPool,
) (SinkerFn, error)
```

**处理逻辑**：

1. 对于每个可GC的对象：
   - 如果有删除时间戳，添加到删除列表
   - 如果是非系统表且无删除时间戳，也添加到删除列表

## 8. Bloom Filter 构建

```go
func BuildBloomfilter(
    ctx context.Context,
    rowCount int,
    probability float64,
    columnIdx int,
    sourcer SourerFn,
    buffer containers.IBatchBuffer,
    mp *mpool.MPool,
    dataProcess ...func(*bloomfilter.BloomFilter, *vector.Vector, *mpool.MPool) error,
) (bf *bloomfilter.BloomFilter, err error)
```

**构建流程**：

1. 创建 Bloom Filter（指定行数和误判率）
2. 遍历数据源
3. 对每批数据调用 `dataProcess` 处理
4. 默认处理：直接添加到 Bloom Filter

### 8.1 数据处理函数

```go
func dataProcess(
    b *bloomfilter.BloomFilter, 
    vec *vector.Vector, 
    pool *mpool.MPool,
) error
```

将对象统计信息转换为对象名，然后添加到 Bloom Filter。

## 9. 性能优化

### 9.1 两阶段过滤

- **粗过滤**: 使用 Bloom Filter 快速排除大部分对象
- **细过滤**: 只对粗过滤后的候选对象进行精确检查

### 9.2 批量处理

- 数据按批次读取和处理
- 使用 Sinker 缓存中间结果
- 支持内存和文件混合存储

### 9.3 并行删除

- 删除操作使用多 worker 并发执行
- 支持配置 worker 数量和批量大小
