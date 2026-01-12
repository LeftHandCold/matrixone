# Checkpoint 清理器详解

## 1. 概述

`checkpointCleaner` 是 GC 模块的核心实现，负责管理整个 GC 生命周期，包括扫描、过滤、删除和合并操作。

## 2. 初始化

```go
func NewCheckpointCleaner(
    ctx context.Context,
    sid string,
    fs fileservice.FileService,
    logDriver wal.Store,
    checkpointCli checkpoint.Runner,
    opts ...CheckpointCleanerOption,
) Cleaner
```

### 2.1 默认配置

| 配置项 | 默认值 | 描述 |
|--------|--------|------|
| `canGCCacheSize` | 64MB | 可GC对象缓存大小 |
| `estimateRows` | 10,000,000 | Bloom Filter 预估行数 |
| `probility` | 0.00001 | Bloom Filter 误判率 |

## 3. 核心流程

### 3.1 Process - 主处理流程

```go
func (c *checkpointCleaner) Process(
    inputCtx context.Context,
    checker func(*checkpoint.CheckpointEntry) bool,
) (err error)
```

**执行步骤**：

1. **检查 GC 是否启用**
2. **获取备份保护快照**
   - 如果备份保护激活，跳过所有 GC 操作
3. **创建内存缓冲区** (16MB)
4. **执行扫描** (`tryScanLocked`)
5. **执行 GC** (`tryGCLocked`)

### 3.2 tryScanLocked - 扫描流程

```go
func (c *checkpointCleaner) tryScanLocked(
    ctx context.Context,
    memoryBuffer *containers.OneSchemaBatchBuffer,
    checker func(*checkpoint.CheckpointEntry) bool,
) (err error, tryGC bool)
```

**执行步骤**：

1. 获取当前扫描水位线
2. 获取待扫描的增量 Checkpoint 列表
3. 过滤不满足条件的 Checkpoint
4. 扫描 Checkpoint 并创建 GCWindow
5. 更新扫描水位线
6. 保存快照元数据和表信息

### 3.3 tryGCLocked - GC 流程

```go
func (c *checkpointCleaner) tryGCLocked(
    ctx context.Context,
    memoryBuffer *containers.OneSchemaBatchBuffer,
) (err error)
```

**执行步骤**：

1. **检查是否需要 GC**
   - 是否存在全局 Checkpoint
   - 是否有已扫描的窗口
   - GC 水位线是否小于最大全局 Checkpoint

2. **执行 GC** (`tryGCAgainstGCKPLocked`)

3. **清理过期文件**
   - 删除过期的 Checkpoint 元数据文件
   - 删除过期的快照文件

## 4. GC 执行详解

### 4.1 tryGCAgainstGCKPLocked

```go
func (c *checkpointCleaner) tryGCAgainstGCKPLocked(
    ctx context.Context,
    gckp *checkpoint.CheckpointEntry,
    memoryBuffer *containers.OneSchemaBatchBuffer,
) (err error)
```

**执行步骤**：

1. 获取 PITR 信息
2. 获取快照信息（包含备份保护时间戳）
3. 执行 GC 并获取可删除文件列表
4. 删除文件
5. 合并 Checkpoint 文件

### 4.2 doGCAgainstGlobalCheckpointLocked

```go
func (c *checkpointCleaner) doGCAgainstGlobalCheckpointLocked(
    ctx context.Context,
    gckp *checkpoint.CheckpointEntry,
    snapshots *logtail.SnapshotInfo,
    pitrs *logtail.PitrInfo,
    memoryBuffer *containers.OneSchemaBatchBuffer,
) ([]string, error)
```

**执行步骤**：

1. 获取 ISCP 表信息
2. 调用 `GCWindow.ExecuteGlobalCheckpointBasedGC`
3. 写入新的元数据文件到 WAL
4. 更新 GC 水位线
5. 合并快照元数据

## 5. Checkpoint 合并

### 5.1 mergeCheckpointFilesLocked

```go
func (c *checkpointCleaner) mergeCheckpointFilesLocked(
    ctx context.Context,
    checkpointLowWaterMark *types.TS,
    memoryBuffer *containers.OneSchemaBatchBuffer,
    snapshots *logtail.SnapshotInfo,
    pitrs *logtail.PitrInfo,
    gcFileCount int,
) (err error)
```

**执行步骤**：

1. 获取待合并的 Checkpoint 列表
2. 过滤超出水位线的 Checkpoint
3. 构建 Bloom Filter
4. 调用 `MergeCheckpoint` 合并
5. 更新使用量统计
6. 写入新文件到 WAL
7. 更新 Compacted Checkpoint
8. 删除旧的 Checkpoint 文件

### 5.2 MergeCheckpoint 函数

```go
func MergeCheckpoint(
    ctx context.Context,
    taskName string,
    sid string,
    ckpEntries []*checkpoint.CheckpointEntry,
    bf *bloomfilter.BloomFilter,
    end *types.TS,
    client checkpoint.Runner,
    pool *mpool.MPool,
    fs fileservice.FileService,
) (deleteFiles, newFiles []string, 
   checkpointEntry *checkpoint.CheckpointEntry, 
   ckpData *batch.Batch, err error)
```

**合并逻辑**：

1. 遍历所有待合并的 Checkpoint
2. 加载每个 Checkpoint 的数据
3. 使用 Bloom Filter 过滤被快照/PITR 引用的对象
4. 将引用的对象写入新的 Checkpoint 数据
5. 创建新的 Compacted Checkpoint 条目
6. 返回待删除的旧文件列表

## 6. 回放机制

### 6.1 Replay 方法

```go
func (c *checkpointCleaner) Replay(inputCtx context.Context) (err error)
```

**执行步骤**：

1. 列出 GC 目录下的所有 TSRange 文件
2. 找到最新的快照文件和账户文件
3. 读取账户表信息
4. 读取 GC 元数据文件并重建 GCWindow
5. 读取快照元数据
6. 更新扫描水位线
7. 从 Compacted Checkpoint 收集使用量信息

## 7. 水位线管理

### 7.1 扫描水位线

```go
func (c *checkpointCleaner) updateScanWaterMark(e *checkpoint.CheckpointEntry)
func (c *checkpointCleaner) GetScanWaterMark() *checkpoint.CheckpointEntry
```

记录已扫描的增量 Checkpoint 的结束位置。

### 7.2 GC 水位线

```go
func (c *checkpointCleaner) updateGCWaterMark(e *checkpoint.CheckpointEntry)
func (c *checkpointCleaner) GetGCWaterMark() *checkpoint.CheckpointEntry
```

记录已完成 GC 的全局 Checkpoint 的结束位置。

### 7.3 Checkpoint GC 水位线

```go
func (c *checkpointCleaner) updateCheckpointGCWaterMark(ts *types.TS)
func (c *checkpointCleaner) GetCheckpointGCWaterMark() *types.TS
```

记录 Checkpoint 合并后的水位线，用于 Checkpoint Runner 清理。

## 8. 检查器机制

### 8.1 添加检查器

```go
func (c *checkpointCleaner) AddChecker(
    checker func(item any) bool, 
    key string,
) int
```

添加自定义检查器，用于控制哪些 Checkpoint 可以被处理。

### 8.2 移除检查器

```go
func (c *checkpointCleaner) RemoveChecker(key string) error
```

移除指定的检查器。注意：至少保留一个检查器。

### 8.3 检查流程

```go
func (c *checkpointCleaner) checkExtras(item any) bool
```

1. 首先检查备份保护
2. 然后遍历所有自定义检查器
3. 所有检查器都返回 true 才允许处理

## 9. 文件清理

### 9.1 删除过期 Checkpoint 元数据

```go
func (c *checkpointCleaner) deleteStaleCKPMetaFileLocked() (err error)
```

删除时间范围不等于当前 GCWindow 的 Checkpoint 元数据文件。

### 9.2 删除过期快照文件

```go
func (c *checkpointCleaner) deleteStaleSnapshotFilesLocked() error
```

保留最新的快照文件和账户文件，删除旧的。

## 10. WAL 集成

```go
func (c *checkpointCleaner) appendFilesToWAL(files ...string) error
```

将 GC 元数据文件追加到 WAL，确保崩溃恢复时的一致性。
