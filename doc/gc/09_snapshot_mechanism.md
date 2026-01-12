# 快照机制详解

## 1. 概述

快照（Snapshot）机制是 MatrixOne GC 模块的核心保护机制之一。它确保被快照引用的数据对象不会被垃圾回收删除，从而支持数据库的时间点恢复（PITR）和快照备份功能。

## 2. 核心数据结构

### 2.1 SnapshotInfo - 快照信息

```go
type SnapshotInfo struct {
    cluster  []types.TS              // 集群级别快照时间戳列表
    account  map[uint32][]types.TS   // 账户级别快照 (accountID -> 时间戳列表)
    database map[uint64][]types.TS   // 数据库级别快照 (dbID -> 时间戳列表)
    tables   map[uint64][]types.TS   // 表级别快照 (tableID -> 时间戳列表)
}
```

### 2.2 PitrInfo - PITR 信息

`PitrInfo` 是 `SnapshotInfo` 的别名，用于存储 PITR（Point-In-Time Recovery）配置：

```go
type PitrInfo = SnapshotInfo
```

### 2.3 SnapshotMeta - 快照元数据

```go
type SnapshotMeta struct {
    sync.RWMutex
    
    // mo_snapshots 表中的对象和墓碑
    objects    map[uint64]map[objectio.Segmentid]*objectInfo
    tombstones map[uint64]map[objectio.Segmentid]*objectInfo
    
    // 用于过滤已转移的墓碑
    aobjDelTsMap map[types.TS]struct{}
    
    // PITR 特殊表信息
    pitr specialTableInfo
    
    // ISCP (Incremental Snapshot Checkpoint) 特殊表信息
    iscp specialTableInfo
    
    // 表信息映射
    tables       map[uint32]map[uint64]*tableInfo  // accountID -> tableID -> tableInfo
    tableIDIndex map[uint64]*tableInfo             // tableID -> tableInfo
    tablePKIndex map[string][]*tableInfo           // pk -> tableInfo列表
    
    // 快照表ID集合
    snapshotTableIDs map[uint64]struct{}
}
```

### 2.4 tableInfo - 表信息

```go
type tableInfo struct {
    accountID uint32     // 账户ID
    dbID      uint64     // 数据库ID
    tid       uint64     // 表ID
    createAt  types.TS   // 创建时间戳
    deleteAt  types.TS   // 删除时间戳
    pk        string     // 主键
}
```

### 2.5 objectInfo - 对象信息

```go
type objectInfo struct {
    stats    objectio.ObjectStats  // 对象统计信息
    createAt types.TS              // 创建时间戳
    deleteAt types.TS              // 删除时间戳
}
```

### 2.6 specialTableInfo - 特殊表信息

```go
type specialTableInfo struct {
    tid        uint64                                    // 表ID
    objects    map[objectio.Segmentid]*objectInfo       // 数据对象
    tombstones map[objectio.Segmentid]*objectInfo       // 墓碑对象
}
```

## 3. 快照层级

MatrixOne 支持四个层级的快照：

| 层级 | 枚举值 | 描述 |
|------|--------|------|
| 集群 | `SnapshotTypeCluster` | 保护整个集群的所有数据 |
| 账户 | `SnapshotTypeAccount` | 保护指定账户的所有数据 |
| 数据库 | `SnapshotTypeDatabase` | 保护指定数据库的所有数据 |
| 表 | `SnapshotTypeTable` | 保护指定表的数据 |

### 3.1 快照继承关系

```
┌─────────────────────────────────────────────────────────────┐
│                    Cluster Snapshot                          │
│                    (保护所有数据)                             │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Account Snapshot                          │
│                    (保护账户下所有数据)                       │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Database Snapshot                          │
│                   (保护数据库下所有表)                        │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     Table Snapshot                           │
│                     (保护单个表)                              │
└─────────────────────────────────────────────────────────────┘
```

## 4. PITR 配置

### 4.1 PITR 单位

| 单位 | 常量 | 描述 |
|------|------|------|
| 年 | `PitrUnitYear` | "y" |
| 月 | `PitrUnitMonth` | "mo" |
| 日 | `PitrUnitDay` | "d" |
| 时 | `PitrUnitHour` | "h" |
| 分 | `PitrUnitMinute` | "m" |

### 4.2 PITR 层级

| 层级 | 常量 | 描述 |
|------|------|------|
| 集群 | `PitrLevelCluster` | 集群级别 PITR |
| 账户 | `PitrLevelAccount` | 账户级别 PITR |
| 数据库 | `PitrLevelDatabase` | 数据库级别 PITR |
| 表 | `PitrLevelTable` | 表级别 PITR |

### 4.3 PITR 时间计算

```go
func (sm *SnapshotMeta) GetPITR(...) (*PitrInfo, error) {
    // 根据 PITR 配置计算保护时间点
    var ts time.Time
    if unit == PitrUnitYear {
        ts = AddDate(gcTime, -val, 0, 0)
    } else if unit == PitrUnitMonth {
        ts = AddDate(gcTime, 0, -val, 0)
    } else if unit == PitrUnitDay {
        ts = gcTime.AddDate(0, 0, -val)
    } else if unit == PitrUnitHour {
        ts = gcTime.Add(-time.Duration(val) * time.Hour)
    } else if unit == PitrUnitMinute {
        ts = gcTime.Add(-time.Duration(val) * time.Minute)
    }
    pitrTS := types.BuildTS(ts.UnixNano(), 0)
    // ...
}
```

## 5. 核心方法

### 5.1 GetSnapshot - 获取快照信息

```go
func (sm *SnapshotMeta) GetSnapshot(
    ctx context.Context,
    sid string,
    fs fileservice.FileService,
    mp *mpool.MPool,
    extraClusterTS ...types.TS,  // 额外的集群级快照（如备份保护）
) (*SnapshotInfo, error)
```

**功能**：
1. 从 `mo_snapshots` 表读取所有快照记录
2. 按层级分类快照时间戳
3. 添加额外的集群级快照（如备份保护时间戳）
4. 对每个层级的快照进行排序

### 5.2 GetPITR - 获取 PITR 信息

```go
func (sm *SnapshotMeta) GetPITR(
    ctx context.Context,
    sid string,
    gcTime time.Time,
    fs fileservice.FileService,
    mp *mpool.MPool,
) (*PitrInfo, error)
```

**功能**：
1. 从 `mo_pitr` 表读取 PITR 配置
2. 根据配置计算保护时间点
3. 返回各层级的 PITR 时间戳

### 5.3 GetISCP - 获取 ISCP 信息

```go
func (sm *SnapshotMeta) GetISCP(
    ctx context.Context,
    sid string,
    fs fileservice.FileService,
    mp *mpool.MPool,
) (map[uint64]types.TS, error)
```

**功能**：
1. 从 `mo_iscp_log` 表读取增量快照检查点信息
2. 返回表ID到水位线的映射

### 5.4 AccountToTableSnapshots - 快照分发

```go
func (sm *SnapshotMeta) AccountToTableSnapshots(
    snapshots *SnapshotInfo,
    pitr *PitrInfo,
) (
    tableSnapshots map[uint64][]types.TS,
    tablePitrs map[uint64]*types.TS,
)
```

**功能**：
将各层级的快照分发到具体的表，返回每个表的快照列表和 PITR 时间戳。

**分发逻辑**：
1. 系统表（mo_database, mo_tables, mo_columns）获取所有快照的扁平化列表
2. 用户表获取：
   - 表级快照
   - 同数据库其他表的快照
   - 数据库级快照
   - 账户级快照
   - 集群级快照

### 5.5 Update - 更新快照元数据

```go
func (sm *SnapshotMeta) Update(
    ctx context.Context,
    fs fileservice.FileService,
    data *CKPReader,
    startts, endts types.TS,
    taskName string,
) error
```

**功能**：
1. 更新表信息（从 mo_tables 收集）
2. 收集快照表、PITR 表、ISCP 表的对象
3. 清理已删除的对象

### 5.6 MergeTableInfo - 合并表信息

```go
func (sm *SnapshotMeta) MergeTableInfo(
    snapshots *SnapshotInfo,
    pitr *PitrInfo,
) error
```

**功能**：
1. 检查每个表是否被快照或 PITR 引用
2. 删除不再被引用的已删除表
3. 清理过期的 aobjDelTsMap 记录

## 6. 对象引用检查

### 6.1 ObjectIsSnapshotRefers

```go
func ObjectIsSnapshotRefers(
    obj *objectio.ObjectStats,
    pitr, createTS, dropTS *types.TS,
    snapshots []types.TS,
) bool
```

**检查逻辑**：

```
┌─────────────────────────────────────────────────────────────┐
│                  ObjectIsSnapshotRefers                      │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  1. 无快照且无 PITR → 返回 false (可GC)                      │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  2. dropTS 为空 → 返回 true (对象未删除，不可GC)             │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  3. PITR 检查                                                │
│     如果 dropTS > pitr → 返回 true (被PITR保护)             │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  4. 快照检查 (二分查找)                                      │
│     如果存在 snapTS 满足:                                    │
│       createTS <= snapTS < dropTS                           │
│     → 返回 true (被快照保护)                                 │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  5. 返回 false (可GC)                                        │
└─────────────────────────────────────────────────────────────┘
```

### 6.2 isSnapshotRefers (表级别)

```go
func isSnapshotRefers(
    table *tableInfo, 
    snapVec []types.TS, 
    pitr *types.TS,
) bool
```

检查表是否被快照或 PITR 引用，逻辑与对象级别类似。

## 7. 持久化

### 7.1 SaveMeta - 保存快照元数据

```go
func (sm *SnapshotMeta) SaveMeta(
    name string, 
    fs fileservice.FileService,
) (uint32, error)
```

保存内容：
- 快照表对象信息
- PITR 表对象信息
- ISCP 表对象信息
- 墓碑信息

### 7.2 SaveTableInfo - 保存表信息

```go
func (sm *SnapshotMeta) SaveTableInfo(
    name string, 
    fs fileservice.FileService,
) (uint32, error)
```

保存内容：
- 表基本信息（accountID, dbID, tid, createAt, deleteAt, pk）
- 快照表 ID 列表
- PITR 表信息
- ISCP 表信息
- aobjDelTsMap

### 7.3 文件格式

| Block ID | 内容 |
|----------|------|
| 0 | 表基本信息 |
| 1 | 快照表 ID |
| 2 | aobjDelTsMap |
| 3 | PITR 表信息 |
| 4 | ISCP 表信息 |

## 8. 回放

### 8.1 ReadMeta - 读取快照元数据

```go
func (sm *SnapshotMeta) ReadMeta(
    ctx context.Context, 
    name string, 
    fs fileservice.FileService,
) error
```

### 8.2 ReadTableInfo - 读取表信息

```go
func (sm *SnapshotMeta) ReadTableInfo(
    ctx context.Context, 
    name string, 
    fs fileservice.FileService,
) error
```

### 8.3 Rebuild 系列方法

| 方法 | 功能 |
|------|------|
| `Rebuild` | 重建对象信息 |
| `RebuildTableInfo` | 重建表信息 |
| `RebuildTid` | 重建快照表 ID |
| `RebuildPitr` | 重建 PITR 表信息 |
| `RebuildIscp` | 重建 ISCP 表信息 |
| `RebuildAObjectDel` | 重建 aobjDelTsMap |

## 9. 与 GC 的集成

### 9.1 GC 流程中的快照检查

```go
// 在 exec_v1.go 中
func MakeSnapshotAndPitrFineFilter(...) (FilterFn, error) {
    // 获取每个表的快照和 PITR
    tableSnapshots, tablePitrs := snapshotMeta.AccountToTableSnapshots(
        snapshots, pitrs,
    )
    
    return func(ctx context.Context, bm *bitmap.Bitmap, bat *batch.Batch, mp *mpool.MPool) error {
        for i := 0; i < bat.Vecs[0].Length(); i++ {
            // 检查对象是否被快照引用
            if !ObjectIsSnapshotRefers(stats, pitr, &createTS, &deleteTS, sp) {
                bm.Add(uint64(i))  // 标记为可GC
            }
        }
        return nil
    }, nil
}
```

### 9.2 备份保护集成

```go
// 在 checkpoint.go 中
func (c *checkpointCleaner) GetSnapshotsLocked() (*logtail.SnapshotInfo, error) {
    var extraTS types.TS
    protectedTS, isActive := c.getBackupProtectionSnapshot()
    if isActive {
        extraTS = protectedTS
    }
    // 将备份保护时间戳作为额外的集群级快照
    return c.mutation.snapshotMeta.GetSnapshot(c.ctx, c.sid, c.fs, c.mp, extraTS)
}
```

## 10. 系统表

### 10.1 mo_snapshots 表

存储快照记录：

| 列 | 类型 | 描述 |
|-----|------|------|
| snapshot_id | uint64 | 快照ID |
| sname | varchar | 快照名称 |
| ts | int64 | 时间戳 |
| level | enum | 快照层级 |
| account_name | varchar | 账户名 |
| database_name | varchar | 数据库名 |
| table_name | varchar | 表名 |
| obj_id | uint64 | 对象ID |

### 10.2 mo_pitr 表

存储 PITR 配置：

| 列 | 类型 | 描述 |
|-----|------|------|
| pitr_id | uint64 | PITR ID |
| pitr_name | varchar | PITR 名称 |
| level | varchar | PITR 层级 |
| obj_id | uint64 | 对象ID |
| length | uint8 | 保留时长 |
| unit | varchar | 时间单位 |

### 10.3 mo_iscp_log 表

存储增量快照检查点：

| 列 | 类型 | 描述 |
|-----|------|------|
| account_id | uint32 | 账户ID |
| table_id | uint64 | 表ID |
| watermark | varchar | 水位线 |
| drop_at | TS | 删除时间 |

## 11. 注意事项

1. **快照排序**: 所有快照时间戳都按升序排序，便于二分查找
2. **去重**: 使用 `compute.SortAndDedup` 确保快照列表无重复
3. **系统表特殊处理**: 系统表获取所有快照的扁平化列表
4. **PITR 优先级**: 同一层级多个 PITR 配置时，取最早的时间点
5. **aobjDelTsMap 清理**: 3小时前的记录会被自动清理
