# GC 窗口机制

## 1. GCWindow 概述

GCWindow 是 GC 模块的核心数据结构，用于管理一个时间范围内的所有对象信息。它是 GC 操作的基本单位。

## 2. 结构定义

```go
type GCWindow struct {
    dir string                      // GC文件存储目录
    mp  *mpool.MPool               // 内存池
    fs  fileservice.FileService    // 文件服务
    
    files []objectio.ObjectStats   // 对象统计信息列表
    
    tsRange struct {
        start types.TS             // 时间范围起始
        end   types.TS             // 时间范围结束
    }
}
```

## 3. 对象条目结构

```go
type ObjectEntry struct {
    stats    *objectio.ObjectStats  // 对象统计信息（128字节）
    createTS types.TS               // 创建时间戳
    dropTS   types.TS               // 删除时间戳（空表示未删除）
    db       uint64                 // 数据库ID
    table    uint64                 // 表ID
}
```

## 4. 核心方法

### 4.1 创建窗口

```go
func NewGCWindow(
    mp *mpool.MPool,
    fs fileservice.FileService,
    opts ...WindowOption,
) *GCWindow

// 可选配置
func WithWindowDir(dir string) WindowOption  // 设置存储目录
```

### 4.2 扫描 Checkpoint

```go
func (w *GCWindow) ScanCheckpoints(
    ctx context.Context,
    checkpointEntries []*checkpoint.CheckpointEntry,
    getCkpReader func(context.Context, *checkpoint.CheckpointEntry) (*logtail.CKPReader, error),
    processCkpData func(*checkpoint.CheckpointEntry, *logtail.CKPReader) error,
    onScanDone func() error,
    buffer *containers.OneSchemaBatchBuffer,
) (metaFile string, err error)
```

**流程说明**：
1. 遍历所有 Checkpoint 条目
2. 从每个 Checkpoint 读取对象信息
3. 收集对象到内存 map 中（按对象名和表ID去重）
4. 排序并写入到 S3 文件
5. 更新时间范围
6. 写入元数据文件

### 4.3 执行 GC

```go
func (w *GCWindow) ExecuteGlobalCheckpointBasedGC(
    ctx context.Context,
    gCkp *checkpoint.CheckpointEntry,      // 全局Checkpoint
    snapshots *logtail.SnapshotInfo,       // 快照信息
    pitrs *logtail.PitrInfo,               // PITR信息
    snapshotMeta *logtail.SnapshotMeta,    // 快照元数据
    iscpTables map[uint64]types.TS,        // ISCP表
    checkpointCli checkpoint.Runner,        // Checkpoint客户端
    buffer *containers.OneSchemaBatchBuffer,
    cacheSize int,
    estimateRows int,
    probility float64,
    mp *mpool.MPool,
    fs fileservice.FileService,
) ([]string, string, error)
```

**返回值**：
- `[]string`: 可以删除的文件列表
- `string`: 新的元数据文件名
- `error`: 错误信息

### 4.4 合并窗口

```go
func (w *GCWindow) Merge(o *GCWindow)
```

合并两个窗口的对象列表和时间范围。

### 4.5 读取窗口

```go
func (w *GCWindow) ReadTable(
    ctx context.Context, 
    name string, 
    fs fileservice.FileService,
) error
```

从 S3 文件读取并重建 GCWindow。

## 5. 数据存储格式

### 5.1 对象表 Schema

```go
var ObjectTableAttrs = []string{
    "stats",       // 对象统计信息 (varchar)
    "created_ts",  // 创建时间戳 (TS)
    "deleted_ts",  // 删除时间戳 (TS)
    "db_id",       // 数据库ID (uint64)
    "table_id",    // 表ID (uint64)
}
```

### 5.2 元数据文件格式

元数据文件存储在 `gc/` 目录下，文件名格式：
```
gc/meta_{start_ts}_{end_ts}.gc
```

内容为对象统计信息的列表。

## 6. GC 执行流程

```
┌─────────────────────────────────────────────────────────────┐
│                    ExecuteGlobalCheckpointBasedGC           │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  1. 创建 CheckpointBasedGCJob                               │
│     - 设置全局Checkpoint位置                                 │
│     - 设置快照和PITR信息                                     │
│     - 配置过滤参数                                           │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  2. 执行 Job                                                │
│     ┌─────────────────────────────────────────────────┐    │
│     │  2.1 构建 Bloom Filter (粗过滤器)                │    │
│     │      - 从全局Checkpoint读取所有对象              │    │
│     │      - 构建 Bloom Filter                        │    │
│     └─────────────────────────────────────────────────┘    │
│                              │                              │
│                              ▼                              │
│     ┌─────────────────────────────────────────────────┐    │
│     │  2.2 粗过滤                                      │    │
│     │      - 遍历 GCWindow 中的对象                    │    │
│     │      - 使用 Bloom Filter 检查是否在全局CKP中     │    │
│     │      - 不在则标记为可GC                          │    │
│     └─────────────────────────────────────────────────┘    │
│                              │                              │
│                              ▼                              │
│     ┌─────────────────────────────────────────────────┐    │
│     │  2.3 细过滤                                      │    │
│     │      - 检查快照引用                              │    │
│     │      - 检查PITR引用                              │    │
│     │      - 检查ISCP表                                │    │
│     └─────────────────────────────────────────────────┘    │
│                              │                              │
│                              ▼                              │
│     ┌─────────────────────────────────────────────────┐    │
│     │  2.4 输出结果                                    │    │
│     │      - vecToGC: 可删除的对象名列表               │    │
│     │      - filesNotGC: 不可删除的对象统计信息        │    │
│     └─────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  3. 写入新的元数据文件                                       │
│     - 更新 w.files 为不可GC的对象                           │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  4. 构建最终删除列表                                         │
│     - 使用 Bloom Filter 去重                                │
│     - 返回可删除的文件名列表                                 │
└─────────────────────────────────────────────────────────────┘
```

## 7. 对象收集逻辑

```go
func collectObjectsFromCheckpointData(
    ctx context.Context, 
    ckpReader *logtail.CKPReader, 
    objects map[string]map[uint64]*ObjectEntry,
)
```

从 Checkpoint 数据中收集对象信息：
- 按对象名分组
- 每个对象名下按表ID分组
- 记录创建和删除时间戳

## 8. 详情统计

```go
func (w *GCWindow) Details(
    ctx context.Context, 
    snapshotMeta *logtail.SnapshotMeta, 
    mp *mpool.MPool,
) (map[uint32]*TableStats, error)
```

统计每个账户的存储使用情况：
- 共享对象（被多个表引用）
- 独占对象
- 总大小和数量
