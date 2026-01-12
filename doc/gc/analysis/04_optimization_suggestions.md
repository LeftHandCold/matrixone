# GC 模块优化建议汇总

## 1. 短期优化（低风险、高收益）

### 1.1 修复错误处理

**优先级**: P0

**改动点**:
```go
// executor.go - 修复错误被忽略的问题
defer func() {
    if deleteErr := DeleteObjects(ctx, exec.fs, canGCObjects); deleteErr != nil {
        logutil.Error("GC-Delete-Temp-Files-Error", zap.Error(deleteErr))
        // 记录到指标
        v2.GCErrorIOErrorCounter.Inc()
        // 不覆盖原始错误
        if err == nil {
            err = deleteErr
        }
    }
}()
```

**收益**: 避免文件泄漏，提高系统可靠性。

### 1.2 启用 Panic 恢复

**优先级**: P0

**改动点**:
```go
// diskcleaner.go
func (cleaner *DiskCleaner) doReplayAndExecute(ctx context.Context) (err error) {
    defer func() {
        if r := recover(); r != nil {
            logutil.Error("GC-Replay-Panic", 
                zap.Any("panic", r),
                zap.Stack("stack"))
            v2.GCPanicCounter.Inc()
            err = fmt.Errorf("GC panic: %v", r)
        }
    }()
    // ...
}
```

**收益**: 防止 GC 崩溃影响整个系统。

### 1.3 配置化硬编码值

**优先级**: P1

**改动点**:
```go
type GCConfig struct {
    // 删除相关
    DeleteTimeout     time.Duration `default:"10m"`
    DeleteBatchSize   int           `default:"1000"`
    DeleteWorkerNum   int           `default:"4"`
    
    // 备份保护
    BackupProtectionTimeout time.Duration `default:"20m"`
    
    // Bloom Filter
    BloomFilterEstimateRows int     `default:"10000000"`
    BloomFilterFPR          float64 `default:"0.00001"`
    
    // 缓存
    CanGCCacheSize int `default:"67108864"` // 64MB
    
    // 清理
    AobjDelTsMapRetention time.Duration `default:"3h"`
}
```

**收益**: 支持运行时调优，适应不同场景。

### 1.4 添加缺失的监控指标

**优先级**: P1

**改动点**:
```go
// 新增指标
var (
    GCBloomFilterHitRate = prometheus.NewGauge(...)
    GCBloomFilterFPRate  = prometheus.NewGauge(...)
    GCObjectProtectedRatio = prometheus.NewGauge(...)
    GCQueueDepth = prometheus.NewGauge(...)
    GCLatencyP99 = prometheus.NewHistogram(...)
)
```

**收益**: 更好的可观测性，便于问题诊断。

---

## 2. 中期优化（中等风险、中等收益）

### 2.1 引入 Checkpoint 缓存

**优先级**: P1

**设计**:
```go
type CheckpointCache struct {
    mu      sync.RWMutex
    cache   map[string]*CachedEntry
    maxSize int64
    curSize int64
    lru     *list.List
    lruMap  map[string]*list.Element
}

type CachedEntry struct {
    key       string
    data      *logtail.CKPReader
    size      int64
    lastUsed  time.Time
}

func (cc *CheckpointCache) Get(ctx context.Context, key string) (*logtail.CKPReader, error) {
    cc.mu.RLock()
    if entry, ok := cc.cache[key]; ok {
        cc.mu.RUnlock()
        cc.touch(key)
        return entry.data, nil
    }
    cc.mu.RUnlock()
    
    // 加载并缓存
    data, err := cc.load(ctx, key)
    if err != nil {
        return nil, err
    }
    cc.put(key, data)
    return data, nil
}
```

**收益**: 减少重复 I/O，提升性能 30-50%。

### 2.2 优化两阶段过滤

**优先级**: P2

**设计**:
```go
// 使用流式处理替代临时文件
type StreamingFilter struct {
    coarseFilter FilterFn
    fineFilter   FilterFn
    sinker       SinkerFn
    bufferSize   int
}

func (sf *StreamingFilter) Process(ctx context.Context, sourcer SourerFn) error {
    buffer := make([]*batch.Batch, 0, sf.bufferSize)
    
    for {
        bat, done, err := sourcer(ctx)
        if err != nil {
            return err
        }
        if done {
            break
        }
        
        // 粗过滤
        coarseBM := sf.coarseFilter(ctx, bat)
        
        // 立即进行细过滤（无需写入临时文件）
        fineBM := sf.fineFilter(ctx, bat, coarseBM)
        
        // 直接输出
        if err := sf.sinker(ctx, bat, fineBM); err != nil {
            return err
        }
    }
    return nil
}
```

**收益**: 减少临时文件 I/O，降低延迟。

### 2.3 并行化扫描

**优先级**: P2

**设计**:
```go
func (c *checkpointCleaner) parallelScanCheckpoints(
    ctx context.Context,
    ckps []*checkpoint.CheckpointEntry,
) (*GCWindow, error) {
    // 限制并发数
    sem := make(chan struct{}, runtime.NumCPU())
    
    results := make([]*partialWindow, len(ckps))
    g, ctx := errgroup.WithContext(ctx)
    
    for i, ckp := range ckps {
        i, ckp := i, ckp
        g.Go(func() error {
            sem <- struct{}{}
            defer func() { <-sem }()
            
            result, err := c.scanOneCheckpoint(ctx, ckp)
            if err != nil {
                return err
            }
            results[i] = result
            return nil
        })
    }
    
    if err := g.Wait(); err != nil {
        return nil, err
    }
    
    // 合并结果
    return mergePartialWindows(results), nil
}
```

**收益**: 充分利用多核，扫描速度提升 2-4 倍。

### 2.4 优化快照检查

**优先级**: P2

**设计**:
```go
// 使用区间树优化快照检查
type SnapshotIntervalTree struct {
    root *intervalNode
}

type intervalNode struct {
    start, end types.TS
    left, right *intervalNode
    maxEnd types.TS
}

func (t *SnapshotIntervalTree) Build(snapshots []types.TS) {
    // 构建区间树
    // 每个快照点创建一个 [snapshot, MaxTS] 的区间
}

func (t *SnapshotIntervalTree) IsProtected(createTS, dropTS types.TS) bool {
    // O(log n) 查询是否有快照落在 [createTS, dropTS) 区间内
    return t.queryOverlap(createTS, dropTS)
}
```

**收益**: 快照检查从 O(n) 降到 O(log n)。

---

## 3. 长期优化（高风险、高收益）

### 3.1 重构模块架构

**优先级**: P2

**设计**:
```go
// 拆分 SnapshotMeta
type SnapshotManager struct {
    objectStore  *ObjectStore
    tableStore   *TableStore
    pitrManager  *PitrManager
    iscpManager  *IscpManager
}

// 拆分 Cleaner 接口
type GCScanner interface {
    Scan(ctx context.Context, ckps []*CheckpointEntry) (*GCWindow, error)
}

type GCFilter interface {
    Filter(ctx context.Context, window *GCWindow) (*FilterResult, error)
}

type GCDeleter interface {
    Delete(ctx context.Context, files []string) error
}

type GCMerger interface {
    Merge(ctx context.Context, ckps []*CheckpointEntry) error
}

// 组合使用
type GCPipeline struct {
    scanner GCScanner
    filter  GCFilter
    deleter GCDeleter
    merger  GCMerger
}
```

**收益**: 
- 更好的可测试性
- 更清晰的职责划分
- 更容易扩展

### 3.2 实现增量 GC

**优先级**: P3

**设计**:
```go
// 增量 GC：只处理变化的部分
type IncrementalGC struct {
    lastGCTS types.TS
    deltaLog *DeltaLog
}

func (igc *IncrementalGC) Process(ctx context.Context) error {
    // 1. 获取自上次 GC 以来的变化
    deltas := igc.deltaLog.GetSince(igc.lastGCTS)
    
    // 2. 只处理变化的对象
    for _, delta := range deltas {
        if delta.Type == DeltaTypeDelete {
            if igc.canGC(delta.Object) {
                igc.markForDeletion(delta.Object)
            }
        }
    }
    
    // 3. 执行删除
    return igc.executeDeletes(ctx)
}
```

**收益**: 
- 大幅减少每次 GC 的工作量
- 更快的 GC 周期
- 更低的资源消耗

### 3.3 实现分布式 GC

**优先级**: P3

**设计**:
```go
// 分布式 GC 协调器
type DistributedGCCoordinator struct {
    nodeID    string
    etcd      *clientv3.Client
    workers   []*GCWorker
}

func (dgc *DistributedGCCoordinator) Start(ctx context.Context) error {
    // 1. 选举 leader
    if err := dgc.electLeader(ctx); err != nil {
        return err
    }
    
    // 2. 分配任务
    tasks := dgc.partitionWork()
    
    // 3. 分发到各节点
    for _, task := range tasks {
        dgc.assignTask(task)
    }
    
    // 4. 等待完成并合并结果
    return dgc.waitAndMerge(ctx)
}
```

**收益**: 
- 支持大规模集群
- 更高的 GC 吞吐量
- 更好的容错性

### 3.4 实现软删除机制

**优先级**: P2

**设计**:
```go
// 软删除：先标记，后清理
type SoftDeleteManager struct {
    markedFiles map[string]*MarkedFile
    retentionPeriod time.Duration
}

type MarkedFile struct {
    path      string
    markedAt  time.Time
    reason    string
}

func (sdm *SoftDeleteManager) MarkForDeletion(path string, reason string) {
    sdm.markedFiles[path] = &MarkedFile{
        path:     path,
        markedAt: time.Now(),
        reason:   reason,
    }
}

func (sdm *SoftDeleteManager) Cleanup(ctx context.Context) error {
    now := time.Now()
    var toDelete []string
    
    for path, marked := range sdm.markedFiles {
        if now.Sub(marked.markedAt) > sdm.retentionPeriod {
            toDelete = append(toDelete, path)
        }
    }
    
    // 执行实际删除
    return sdm.doDelete(ctx, toDelete)
}

func (sdm *SoftDeleteManager) Recover(path string) error {
    // 在保留期内可以恢复
    delete(sdm.markedFiles, path)
    return nil
}
```

**收益**: 
- 支持误删恢复
- 更安全的 GC 操作
- 便于问题排查

---

## 4. 优化路线图

### Phase 1: 稳定性优化（1-2 周）
- [ ] 修复错误处理问题
- [ ] 启用 Panic 恢复
- [ ] 配置化硬编码值
- [ ] 添加缺失的监控指标

### Phase 2: 性能优化（2-4 周）
- [ ] 引入 Checkpoint 缓存
- [ ] 优化两阶段过滤
- [ ] 并行化扫描
- [ ] 优化快照检查

### Phase 3: 架构优化（4-8 周）
- [ ] 重构模块架构
- [ ] 实现软删除机制
- [ ] 改进可测试性

### Phase 4: 高级特性（8-12 周）
- [ ] 实现增量 GC
- [ ] 分布式 GC 支持

---

## 5. 风险评估

| 优化项 | 风险等级 | 影响范围 | 回滚难度 |
|--------|----------|----------|----------|
| 错误处理修复 | 低 | 小 | 易 |
| Panic 恢复 | 低 | 小 | 易 |
| 配置化 | 低 | 中 | 易 |
| 监控指标 | 低 | 小 | 易 |
| Checkpoint 缓存 | 中 | 中 | 中 |
| 两阶段过滤优化 | 中 | 大 | 中 |
| 并行化扫描 | 中 | 大 | 中 |
| 快照检查优化 | 低 | 中 | 易 |
| 架构重构 | 高 | 大 | 难 |
| 增量 GC | 高 | 大 | 难 |
| 分布式 GC | 高 | 大 | 难 |
| 软删除 | 中 | 大 | 中 |

---

## 6. 测试建议

### 6.1 单元测试覆盖

```go
// 每个优化都需要对应的单元测试
func TestCheckpointCache(t *testing.T) {
    // 测试缓存命中
    // 测试缓存淘汰
    // 测试并发访问
}

func TestStreamingFilter(t *testing.T) {
    // 测试正常流程
    // 测试错误处理
    // 测试边界条件
}
```

### 6.2 集成测试

```go
func TestGCEndToEnd(t *testing.T) {
    // 创建测试数据
    // 执行 GC
    // 验证结果
}
```

### 6.3 性能基准测试

```go
func BenchmarkGCWithCache(b *testing.B) {
    // 对比有无缓存的性能差异
}

func BenchmarkGCParallel(b *testing.B) {
    // 测试不同并发度的性能
}
```

### 6.4 压力测试

```go
func TestGCUnderLoad(t *testing.T) {
    // 模拟高负载场景
    // 验证 GC 稳定性
}
```
