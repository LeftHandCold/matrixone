# GC 模块架构层面的问题

## 1. 模块耦合问题

### 1.1 GC 与 Checkpoint 强耦合

**现状**:

```go
type checkpointCleaner struct {
    checkpointCli checkpoint.Runner  // 直接依赖 checkpoint.Runner
    // ...
}

// 在 GC 过程中直接操作 Checkpoint
c.checkpointCli.UpdateCompacted(newCkp)
c.checkpointCli.GetAllGlobalCheckpoints()
c.checkpointCli.ICKPRange(...)
```

**问题**:
- GC 模块直接操作 Checkpoint 内部状态
- 两个模块的边界不清晰
- 难以独立测试和演进

**建议**:
- 定义清晰的接口边界
- GC 只通过接口与 Checkpoint 交互
- 考虑使用事件驱动解耦

### 1.2 SnapshotMeta 职责过重

**现状**:

```go
type SnapshotMeta struct {
    // 快照表对象管理
    objects    map[uint64]map[objectio.Segmentid]*objectInfo
    tombstones map[uint64]map[objectio.Segmentid]*objectInfo
    
    // PITR 管理
    pitr specialTableInfo
    
    // ISCP 管理
    iscp specialTableInfo
    
    // 表信息管理
    tables       map[uint32]map[uint64]*tableInfo
    tableIDIndex map[uint64]*tableInfo
    tablePKIndex map[string][]*tableInfo
    
    // 快照表 ID 管理
    snapshotTableIDs map[uint64]struct{}
    
    // 还有大量方法...
}
```

**问题**:
- 单一类承担了太多职责
- 违反单一职责原则
- 难以理解和维护

**建议**:
拆分为多个独立的管理器：
```go
type SnapshotObjectManager struct { ... }
type TableInfoManager struct { ... }
type PitrManager struct { ... }
type IscpManager struct { ... }
```

### 1.3 循环依赖风险

**现状**:

```
gc/v3 ──────────────► logtail (SnapshotMeta)
   │                       │
   │                       ▼
   └──────────────► checkpoint
                          │
                          ▼
                      logtail (CKPReader)
```

**问题**: 模块间存在复杂的依赖关系，可能导致循环依赖。

**建议**:
- 引入中间抽象层
- 使用依赖注入
- 明确模块层次

---

## 2. 状态管理问题

### 2.1 水位线管理分散

**现状**:

```go
type checkpointCleaner struct {
    watermarks struct {
        scanWaterMark         atomic.Pointer[checkpoint.CheckpointEntry]
        gcWaterMark           atomic.Pointer[checkpoint.CheckpointEntry]
        checkpointGCWaterMark atomic.Pointer[types.TS]
    }
    // ...
}
```

**问题**:
- 三个水位线的关系不明确
- 更新逻辑分散在多个方法中
- 容易出现不一致状态

**建议**:
```go
type WatermarkManager struct {
    mu sync.RWMutex
    scan       *checkpoint.CheckpointEntry
    gc         *checkpoint.CheckpointEntry
    checkpoint *types.TS
}

func (wm *WatermarkManager) UpdateAll(scan, gc *checkpoint.CheckpointEntry, ckp *types.TS) {
    wm.mu.Lock()
    defer wm.mu.Unlock()
    // 原子性更新所有水位线
    // 验证一致性约束
}
```

### 2.2 状态机不完整

**现状**:

```go
const (
    StateStep_Write StateStep = iota
    StateStep_Write2Replay
    StateStep_Replay
    StateStep_Replay2Write
)
```

**问题**:
- 状态转换逻辑分散
- 没有明确的状态转换图
- 缺少非法状态转换的保护

**建议**:
```go
type StateMachine struct {
    current State
    transitions map[State][]State  // 合法的状态转换
}

func (sm *StateMachine) Transition(to State) error {
    if !sm.isValidTransition(sm.current, to) {
        return ErrInvalidStateTransition
    }
    sm.current = to
    return nil
}
```

### 2.3 配置与状态混合

**现状**:

```go
type checkpointCleaner struct {
    // 配置
    config struct {
        canGCCacheSize          int
        maxMergeCheckpointCount int
        // ...
    }
    
    // 运行时状态
    options struct {
        gcEnabled           atomic.Bool
        checkEnabled        atomic.Bool
        // ...
    }
    
    // 可变状态
    mutation struct {
        scanned      *GCWindow
        metaFiles    map[string]ioutil.TSRangeFile
        // ...
    }
}
```

**问题**: 配置、选项、状态混在一起，难以区分。

**建议**:
```go
type GCConfig struct { ... }      // 不可变配置
type GCOptions struct { ... }     // 运行时选项
type GCState struct { ... }       // 可变状态
```

---

## 3. 流程设计问题

### 3.1 GC 流程过于复杂

**现状**:

```
Process()
  └── tryScanLocked()
        └── scanCheckpointsLocked()
              └── ScanCheckpoints()
  └── tryGCLocked()
        └── tryGCAgainstGCKPLocked()
              └── doGCAgainstGlobalCheckpointLocked()
                    └── ExecuteGlobalCheckpointBasedGC()
                          └── CheckpointBasedGCJob.Execute()
                                └── GCExecutor.Run()
                                      └── doFilter() x 2
              └── mergeCheckpointFilesLocked()
                    └── MergeCheckpoint()
        └── deleteStaleCKPMetaFileLocked()
        └── deleteStaleSnapshotFilesLocked()
```

**问题**:
- 调用层次过深（7+ 层）
- 方法命名不一致（有的带 Locked，有的不带）
- 难以追踪执行流程

**建议**:
- 扁平化调用结构
- 使用 Pipeline 模式
- 统一命名规范

### 3.2 两阶段过滤设计问题

**现状**:

```go
// 第一阶段：粗过滤
coarseFilter = MakeBloomfilterCoarseFilter(...)

// 第二阶段：细过滤
fineFilter = MakeSnapshotAndPitrFineFilter(...)

// 执行
exec.Run(sourcer, coarseFilter, fineFilter, sinker)
```

**问题**:
- 粗过滤后的数据需要写入临时文件再读取
- 增加了 I/O 开销
- 临时文件管理复杂

**建议**:
- 考虑流式处理，避免中间文件
- 或者使用内存缓存替代文件

### 3.3 缺少事务性保证

**现状**:

```go
// 删除文件
if err = c.deleter.DeleteMany(ctx, taskName, filesToGC); err != nil {
    return err
}

// 更新水位线
c.updateGCWaterMark(gckp)

// 如果这里失败，已删除的文件无法恢复
```

**问题**: GC 操作不是原子的，中间失败可能导致不一致状态。

**建议**:
- 实现两阶段提交
- 或者使用软删除 + 后台清理
- 添加恢复机制

---

## 4. 扩展性问题

### 4.1 过滤器不可扩展

**现状**:

```go
// 硬编码的过滤器
coarseFilter = MakeBloomfilterCoarseFilter(...)
fineFilter = MakeSnapshotAndPitrFineFilter(...)
```

**问题**: 无法动态添加新的过滤条件。

**建议**:
```go
type FilterChain struct {
    filters []Filter
}

func (fc *FilterChain) AddFilter(f Filter) {
    fc.filters = append(fc.filters, f)
}

func (fc *FilterChain) Apply(ctx context.Context, bat *batch.Batch) *bitmap.Bitmap {
    result := bitmap.New()
    for _, f := range fc.filters {
        f.Filter(ctx, result, bat)
    }
    return result
}
```

### 4.2 删除策略不可配置

**现状**:

```go
// 硬编码的删除逻辑
if err = c.fs.Delete(ctx, filesToDelete...); err != nil {
    // ...
}
```

**问题**: 无法支持不同的删除策略（如软删除、延迟删除）。

**建议**:
```go
type DeletionStrategy interface {
    Delete(ctx context.Context, files []string) error
    CanRecover() bool
    Recover(ctx context.Context, files []string) error
}

type ImmediateDeletion struct{}
type SoftDeletion struct{}
type DelayedDeletion struct{}
```

### 4.3 监控指标不完整

**现状**: 虽然有很多指标，但缺少一些关键指标：

**缺失的指标**:
- GC 队列深度
- 单次 GC 处理的对象数量分布
- Bloom Filter 命中率
- 快照保护的对象比例
- GC 延迟（从对象删除到实际回收的时间）

**建议**: 补充这些关键指标。

---

## 5. 可测试性问题

### 5.1 依赖注入不足

**现状**:

```go
func NewCheckpointCleaner(
    ctx context.Context,
    sid string,
    fs fileservice.FileService,
    logDriver wal.Store,
    checkpointCli checkpoint.Runner,
    opts ...CheckpointCleanerOption,
) Cleaner {
    cleaner := &checkpointCleaner{
        // 直接创建内部依赖
        deleter: NewDeleter(fs),
        mp: common.CheckpointAllocator,  // 全局变量
        // ...
    }
}
```

**问题**: 
- 内部依赖无法替换
- 使用全局变量
- 难以进行单元测试

**建议**:
```go
type CheckpointCleanerDeps struct {
    Deleter     Deleter
    MemPool     *mpool.MPool
    TimeSource  TimeSource
    // ...
}

func NewCheckpointCleaner(deps CheckpointCleanerDeps, ...) Cleaner {
    // ...
}
```

### 5.2 时间依赖

**现状**:

```go
// 直接使用 time.Now()
if time.Since(c.backupProtection.lastUpdateTime) > 20*time.Minute {
    // ...
}

hoursAgo := types.BuildTS(time.Now().UnixNano()-int64(3*time.Hour), 0)
```

**问题**: 无法在测试中控制时间。

**建议**:
```go
type TimeSource interface {
    Now() time.Time
}

type RealTimeSource struct{}
func (RealTimeSource) Now() time.Time { return time.Now() }

type MockTimeSource struct {
    current time.Time
}
func (m *MockTimeSource) Now() time.Time { return m.current }
```

### 5.3 文件系统依赖

**现状**: 直接依赖 `fileservice.FileService`。

**问题**: 测试需要真实的文件系统或复杂的 mock。

**建议**: 
- 使用内存文件系统进行测试
- 或者抽象出更简单的接口
