# Checkpoint 在备份中的作用

## 1. 概述

Checkpoint 是 MatrixOne 备份机制的核心，它提供了数据库在某个时间点的一致性快照。

## 2. Checkpoint 类型

### 2.1 增量 Checkpoint (Incremental)

```go
const ET_Incremental EntryType = 1
```

- 记录两个时间点之间的数据变更
- 包含新增和删除的对象信息
- 用于增量备份

### 2.2 全局 Checkpoint (Global)

```go
const ET_Global EntryType = 0
```

- 记录数据库的完整状态
- 包含所有活跃对象
- 用于全量备份和 GC

### 2.3 备份 Checkpoint (Backup)

```go
const ET_Backup EntryType = 2
```

- 专门为备份创建的特殊 Checkpoint
- LSN 设置为 0（因为恢复后没有 WAL）

## 3. 备份时的 Checkpoint 创建

### 3.1 ForceCheckpointForBackup

```go
func (db *DB) ForceCheckpointForBackup(
    ctx context.Context,
    ts types.TS,
) (location string, err error) {
    
    // 1. 强制创建增量 Checkpoint
    err = db.ForceCheckpoint(ctx, ts)
    
    // 2. 获取最新的增量 Checkpoint
    maxEntry := db.BGCheckpointRunner.MaxIncrementalCheckpoint()
    maxEnd := maxEntry.GetEnd()
    
    // 3. 强制刷盘
    start := maxEnd.Next()
    end := db.TxnMgr.Now()
    err = db.BGFlusher.ForceFlush(ctx, end)
    
    // 4. 创建特殊的备份 Checkpoint
    location, err = db.BGCheckpointRunner.CreateSpecialCheckpointFile(
        ctx, start, end,
    )
    
    return
}
```

### 3.2 获取备份所需的 Checkpoint

```go
func (s *runnerStore) GetAllCheckpointsForBackup(
    compact *CheckpointEntry,
) []*CheckpointEntry {
    
    ckps := make([]*CheckpointEntry, 0)
    var ts types.TS
    
    // 添加 Compacted Checkpoint
    if compact != nil {
        ts = compact.GetEnd()
        ckps = append(ckps, compact)
    }
    
    // 添加全局 Checkpoint
    g := s.MaxFinishedGlobalCheckpointLocked()
    if g != nil {
        if ts.IsEmpty() {
            ts = g.GetEnd()
        }
        ckps = append(ckps, g)
    }
    
    // 添加增量 Checkpoint
    pivot := NewCheckpointEntry(s.sid, ts.Next(), ts.Next(), ET_Incremental)
    iter := tree.Iter()
    if ok := iter.Seek(pivot); ok {
        for {
            e := iter.Item()
            if !e.IsFinished() {
                break
            }
            ckps = append(ckps, e)
            if !iter.Next() {
                break
            }
        }
    }
    
    return ckps
}
```

## 4. Checkpoint 数据读取

### 4.1 CKPReader

```go
func GetCheckpointReader(
    ctx context.Context,
    sid string,
    fs fileservice.FileService,
    location objectio.Location,
    version uint32,
) (*CKPReader, error) {
    
    reader := NewCKPReader(version, location, common.CheckpointAllocator, fs)
    if err := reader.ReadMeta(ctx); err != nil {
        return nil, err
    }
    return reader, nil
}
```

### 4.2 遍历 Checkpoint 数据

```go
ckpReader.ForEachRow(ctx,
    func(
        account uint32,
        dbid, tid uint64,
        objectType int8,
        objectStats objectio.ObjectStats,
        createAt, deletedAt types.TS,
        rowID types.Rowid,
    ) error {
        // 处理每个对象
        return nil
    },
)
```

## 5. Checkpoint 文件复制

### 5.1 复制流程

```go
func CopyCheckpointDir(
    ctx context.Context,
    srcFs, dstFs fileservice.FileService,
    dir string, 
    backup types.TS,
) ([]*taeFile, types.TS, error) {
    
    decoder := func(name string) ioutil.TSRangeFile {
        meta := ioutil.DecodeCKPMetadataName(name)
        meta.SetExt("")
        return meta
    }
    
    taeFiles, metaFiles, _, err := copyFileAndGetMetaFiles(
        ctx, srcFs, dstFs, dir, backup, decoder, true,
    )
    
    // 找到最小时间戳（用于 GC 目录复制）
    minTs := types.TS{}
    for i := len(metaFiles) - 1; i >= 0; i-- {
        ckpStart := metaFiles[i].GetStart()
        if ckpStart.IsEmpty() {
            minTs = *metaFiles[i].GetEnd()
            break
        }
    }
    
    return taeFiles, minTs, nil
}
```

### 5.2 文件过滤

```go
func copyFileAndGetMetaFiles(...) {
    for i, file := range mFiles {
        meta := decoder(file.Name)
        
        // 跳过备份时间点之后的文件
        if !backup.IsEmpty() {
            end := meta.GetEnd()
            if !end.IsEmpty() && end.GT(&backup) {
                continue
            }
            start := meta.GetStart()
            if !start.IsEmpty() && start.GE(&backup) {
                continue
            }
        }
        
        // 复制文件
        if doCopy || meta.IsAcctExt() || meta.IsSnapshotExt() {
            checksum, err = CopyFileWithRetry(ctx, srcFs, dstFs, file.Name, dir)
            taeFileList = append(taeFileList, &taeFile{...})
        }
    }
}
```

## 6. Checkpoint 重写

### 6.1 为什么需要重写

增量备份时，需要裁剪 Checkpoint 数据：
- 移除备份时间点之后的变更
- 处理 Tombstone 数据
- 生成新的一致性 Checkpoint

### 6.2 重写流程

```go
func ReWriteCheckpointAndBlockFromKey(...) {
    // Phase 1: 加载原始 Checkpoint
    ckpReader, err := GetCheckpointReader(ctx, sid, fs, loc, version)
    
    // Phase 2: 分析对象
    initData(&objectsData, ckputil.ObjectType_Data, objectio.SchemaData)
    initData(&tombstonesData, ckputil.ObjectType_Tombstone, objectio.SchemaTombstone)
    
    // Phase 3: 裁剪 Tombstone
    err = trimTombstoneData(ctx, fs, ts, &tombstonesData)
    
    // Phase 4: 重写数据对象
    for _, objectData := range objsData {
        // 读取数据
        ds := NewBackupDeltaLocDataSource(ctx, fs, ts, dsTombstone)
        bat, sortKey, err := blockio.BlockDataReadBackup(ctx, &blk, ds, nil, ts, fs)
        
        // 写入新文件
        writer, err := ioutil.NewBlockWriter(dstFs, name.String())
        _, err = writer.WriteBatch(objectData.data[0])
        blocks, extent, err := writer.Sync(ctx)
    }
    
    // Phase 5: 生成新 Checkpoint
    dataSinker := ckputil.NewDataSinker(...)
    dataSinker.Write(ctx, objectInfoMeta)
    dataSinker.Write(ctx, tombstoneInfoMeta)
    
    newData := NewCheckpointDataWithSinker(dataSinker, common.CheckpointAllocator)
    location, checkpointFiles, err := newData.Sync(ctx, dstFs)
}
```

## 7. Checkpoint 合并

### 7.1 MergeCkpMeta

```go
func MergeCkpMeta(
    ctx context.Context,
    sid string,
    dstFs fileservice.FileService,
    cnLocation, tnLocation objectio.Location,
    start, end types.TS,
) (string, error)
```

合并多个 Checkpoint 元数据为一个统一的备份 Checkpoint。

## 8. 备份 Checkpoint 的特殊处理

### 8.1 LSN 重置

```go
func (s *runnerStore) AddBackupCKPEntry(entry *CheckpointEntry) (success bool) {
    entry.entryType = ET_Incremental
    success = s.AddICKPFinishedEntry(entry)
    
    // 重置所有增量 Checkpoint 的 LSN
    it := s.incrementals.Iter()
    for it.Next() {
        e := it.Item()
        e.ckpLSN = 0
        e.truncateLSN = 0
    }
    return
}
```

### 8.2 恢复时的处理

```go
// 备份 Checkpoint 的 LSN 为 0
if ckpEntry.ckpLSN != 0 {
    if ckpEntry.ckpLSN < reader.maxLSN {
        // 正常 Checkpoint
    }
}
// LSN 为 0 表示是备份恢复的 Checkpoint
```
