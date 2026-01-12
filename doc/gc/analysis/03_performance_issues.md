# GC 模块性能问题分析

## 1. I/O 性能问题

### 1.1 重复读取 Checkpoint 数据

**现状**:

```go
// 在 scanCheckpointsLocked 中读取
ckpReader, err := c.getCkpReader(ctx, ckp)

// 在 mergeCheckpointFilesLocked 中再次读取
for _, ckpEntry := range ckpEntries {
    data, err = logtail.LoadCheckpointEntriesFromKey(...)
}

// 在 doGCAgainstGlobalCheckpointLocked 中又读取
sourcer := window.MakeFilesReader(ctx, fs)
```

**问题**: 同一份数据被多次读取，增加 I/O 开销。

**影响**: 
- 增加 S3 请求次数和成本
- 延长 GC 执行时间

**建议**:
```go
type CheckpointCache struct {
    cache map[string]*CachedCheckpoint
    maxSize int
    lru *list.List
}

func (cc *CheckpointCache) Get(key string) (*CachedCheckpoint, bool) {
    // LRU 缓存实现
}
```

### 1.2 Bloom Filter 重复构建

**现状**:

```go
// 在 ExecuteGlobalCheckpointBasedGC 中构建
bf, err = BuildBloomfilter(ctx, Default_Coarse_EstimateRows, ...)

// 在 mergeCheckpointFilesLocked 中再次构建
bf, err := BuildBloomfilter(ctx, Default_Coarse_EstimateRows, ...)
```

**问题**: 相同数据的 Bloom Filter 被重复构建。

**影响**: 
- CPU 开销
- 内存分配压力

**建议**:
- 缓存 Bloom Filter
- 或者增量更新 Bloom Filter

### 1.3 临时文件过多

**现状**:

```go
// GCExecutor.Run 中
canGCSinker := exec.getSinker(...)  // 创建临时文件
// ...
canGCObjects, canGCMemTable := canGCSinker.GetResult()
// ...
DeleteObjects(ctx, exec.fs, canGCObjects)  // 删除临时文件
```

**问题**: 
- 两阶段过滤产生大量临时文件
- 增加 I/O 和存储开销

**建议**:
- 使用内存缓存替代小文件
- 合并多个小文件为大文件
- 使用流式处理避免中间文件

---

## 2. 内存性能问题

### 2.1 大量小对象分配

**现状**:

```go
// snapshot.go
objects := make(map[string]map[uint64]*ObjectEntry)
for name, tables := range objects {
    for tid, entry := range tables {
        // 每个 entry 都是独立分配
    }
}
```

**问题**: 
- 大量小对象导致 GC 压力
- 内存碎片化

**影响**: 
- Go GC 暂停时间增加
- 内存使用效率低

**建议**:
```go
// 使用对象池
var objectEntryPool = sync.Pool{
    New: func() interface{} {
        return &ObjectEntry{}
    },
}

// 或者使用 arena 分配
type ObjectArena struct {
    entries []ObjectEntry
    index   int
}
```

### 2.2 Batch 内存管理不当

**现状**:

```go
bat := buffer.Fetch()
defer buffer.Putback(bat, mp)
// ...
bat.CleanOnlyData()  // 只清理数据
```

**问题**: 
- Batch 复用时可能残留数据
- 内存池大小固定，可能不够用

**建议**:
```go
type AdaptiveBatchPool struct {
    pools []*sync.Pool  // 不同大小的池
    stats PoolStats     // 使用统计
}

func (p *AdaptiveBatchPool) Get(size int) *batch.Batch {
    // 根据大小选择合适的池
}
```

### 2.3 Map 预分配不足

**现状**:

```go
objects := make(map[string]map[uint64]*ObjectEntry)  // 无初始容量
```

**问题**: Map 频繁扩容导致内存分配和复制。

**建议**:
```go
// 根据历史数据预估容量
estimatedSize := c.getEstimatedObjectCount()
objects := make(map[string]map[uint64]*ObjectEntry, estimatedSize)
```

---

## 3. CPU 性能问题

### 3.1 排序开销

**现状**:

```go
// window.go
func (w *GCWindow) sortOneBatch(...) error {
    if err := mergeutil.SortColumnsByIndex(
        data.Vecs,
        ObjectTablePrimaryKeyIdx,
        mp,
    ); err != nil {
        return err
    }
    return nil
}
```

**问题**: 每个 Batch 都需要排序。

**建议**:
- 使用归并排序合并已排序的 Batch
- 或者使用堆排序进行流式处理

### 3.2 Bloom Filter 参数不优

**现状**:

```go
const (
    Default_Coarse_EstimateRows = 10000000   // 1000万
    Default_Coarse_Probility    = 0.00001    // 0.001%
)
```

**问题**: 
- 固定参数可能不适合所有场景
- 误判率过低导致 Bloom Filter 过大

**建议**:
```go
func OptimalBloomFilterParams(expectedItems int, targetFPR float64) (size int, hashFuncs int) {
    // 根据实际数据量动态计算最优参数
}
```

### 3.3 快照检查效率低

**现状**:

```go
func ObjectIsSnapshotRefers(...) bool {
    // 对每个对象都进行二分查找
    left, right := 0, len(snapshots)-1
    for left <= right {
        // ...
    }
}
```

**问题**: 每个对象都需要遍历快照列表。

**建议**:
```go
// 预处理快照为区间树
type SnapshotIntervalTree struct {
    root *IntervalNode
}

func (t *SnapshotIntervalTree) IsProtected(createTS, dropTS types.TS) bool {
    // O(log n) 查询
}
```

---

## 4. 并发性能问题

### 4.1 串行处理瓶颈

**现状**:

```go
// checkpoint.go - Process 方法
c.StartMutationTask("gc-process")
defer c.StopMutationTask()

// 整个过程串行执行
err, tryGC = c.tryScanLocked(ctx, memoryBuffer, checker)
err = c.tryGCLocked(ctx, memoryBuffer)
```

**问题**: GC 过程完全串行，无法利用多核。

**建议**:
```go
// 并行扫描多个 Checkpoint
func (c *checkpointCleaner) parallelScan(ctx context.Context, ckps []*CheckpointEntry) error {
    g, ctx := errgroup.WithContext(ctx)
    results := make(chan *ScanResult, len(ckps))
    
    for _, ckp := range ckps {
        ckp := ckp
        g.Go(func() error {
            result, err := c.scanOne(ctx, ckp)
            if err != nil {
                return err
            }
            results <- result
            return nil
        })
    }
    
    // 合并结果
    // ...
}
```

### 4.2 删除并发度不足

**现状**:

```go
var deleteWorkerNum = 4  // 固定 4 个 worker
```

**问题**: 
- Worker 数量固定
- 可能无法充分利用 I/O 带宽

**建议**:
```go
type AdaptiveDeleter struct {
    minWorkers int
    maxWorkers int
    currentWorkers int
    
    // 根据 I/O 延迟动态调整
    latencyTracker *LatencyTracker
}

func (d *AdaptiveDeleter) adjustWorkers() {
    avgLatency := d.latencyTracker.Average()
    if avgLatency < targetLatency && d.currentWorkers < d.maxWorkers {
        d.currentWorkers++
    } else if avgLatency > targetLatency && d.currentWorkers > d.minWorkers {
        d.currentWorkers--
    }
}
```

### 4.3 锁竞争

**现状**:

```go
// SnapshotMeta 使用单一 RWMutex
type SnapshotMeta struct {
    sync.RWMutex
    // 所有字段共享一把锁
}
```

**问题**: 读写操作都需要获取锁，高并发时竞争严重。

**建议**:
```go
type SnapshotMeta struct {
    objectsMu    sync.RWMutex
    objects      map[uint64]map[objectio.Segmentid]*objectInfo
    
    tablesMu     sync.RWMutex
    tables       map[uint32]map[uint64]*tableInfo
    
    // 分离不同数据的锁
}
```

---

## 5. 算法效率问题

### 5.1 去重效率低

**现状**:

```go
// 使用 map 去重
filesToGCSet := make(map[string]struct{})
for ... {
    filesToGCSet[file] = struct{}{}
}
```

**问题**: 大量字符串作为 key，内存和 CPU 开销大。

**建议**:
```go
// 使用 Bloom Filter 预过滤
type DeduplicatedSet struct {
    bf    *bloomfilter.BloomFilter
    exact map[string]struct{}
}

func (ds *DeduplicatedSet) Add(s string) bool {
    if ds.bf.Test(s) {
        // 可能存在，检查精确集合
        if _, ok := ds.exact[s]; ok {
            return false  // 已存在
        }
    }
    ds.bf.Add(s)
    ds.exact[s] = struct{}{}
    return true
}
```

### 5.2 快照分发效率低

**现状**:

```go
func (sm *SnapshotMeta) AccountToTableSnapshots(...) {
    for tid, info := range sm.tableIDIndex {
        // 对每个表都收集所有层级的快照
        // 然后排序去重
    }
}
```

**问题**: O(n * m) 复杂度，n 是表数量，m 是快照数量。

**建议**:
```go
// 预计算每个账户/数据库的快照
type PrecomputedSnapshots struct {
    byAccount  map[uint32][]types.TS
    byDatabase map[uint64][]types.TS
    byTable    map[uint64][]types.TS
}

func (ps *PrecomputedSnapshots) GetForTable(info *tableInfo) []types.TS {
    // 直接返回预计算结果
}
```

---

## 6. 性能监控不足

### 6.1 缺少详细的性能指标

**缺失的指标**:
- 每个阶段的耗时分布
- I/O 操作的延迟分布
- 内存使用峰值
- Bloom Filter 效率（命中率、误判率）

**建议**:
```go
type GCPerformanceMetrics struct {
    ScanDuration      prometheus.Histogram
    FilterDuration    prometheus.Histogram
    DeleteDuration    prometheus.Histogram
    MergeDuration     prometheus.Histogram
    
    IOLatency         prometheus.Histogram
    MemoryPeak        prometheus.Gauge
    BloomFilterHitRate prometheus.Gauge
}
```

### 6.2 缺少性能基准

**问题**: 没有性能基准测试，难以评估优化效果。

**建议**:
```go
func BenchmarkGCScan(b *testing.B) {
    // 标准化的性能测试
}

func BenchmarkGCFilter(b *testing.B) {
    // ...
}

func BenchmarkGCDelete(b *testing.B) {
    // ...
}
```
