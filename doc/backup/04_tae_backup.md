# TAE 数据备份

## 1. 概述

TAE (Transactional Analytical Engine) 数据备份是整个备份流程的核心，负责备份存储引擎中的所有数据文件。

## 2. 入口函数

```go
var backupTae = func(
    ctx context.Context,
    sid string,
    config *Config,
) error {
    fs := fileservice.SubPath(config.TaeDir, taeDir)
    return BackupData(ctx, sid, config.SharedFs, fs, "", config)
}
```

## 3. BackupData 流程

```go
func BackupData(
    ctx context.Context,
    sid string,
    srcFs, dstFs fileservice.FileService,
    dir string,
    config *Config,
) error
```

### 3.1 触发 Checkpoint

```go
sql := "select mo_ctl('dn','Backup','')"
res, err := exec.Exec(ctx, sql, opts)
```

这会触发 DN 节点执行 `HandleBackup`：

```go
func (h *Handle) HandleBackup(...) {
    // 1. 强制创建 Checkpoint
    location, err = h.db.ForceCheckpointForBackup(ctx, currTs)
    
    // 2. 获取所有 Checkpoint 信息
    entries := h.db.BGCheckpointRunner.GetAllCheckpointsForBackup(compactEntry)
    
    // 3. 返回 Checkpoint 位置列表
    resp.CkpLocation = locations
}
```

### 3.2 解析 Checkpoint 信息

```go
func getFileNames(ctx context.Context, retBytes [][][]byte) ([]string, error) {
    // 解析返回的 Checkpoint 位置字符串
    // 格式: "backupTime;trimString;ckp1:version;ckp2:version;..."
}
```

### 3.3 设置备份保护

```go
protectionMgr := newBackupProtectionManager(ctx, exec, opts)
defer protectionMgr.cleanup()

if !protectedTS.IsEmpty() && protectionMgr != nil {
    protectionMgr.start(protectedTS)
}
```

### 3.4 执行备份

```go
func execBackup(
    ctx context.Context,
    sid string,
    srcFs, dstFs fileservice.FileService,
    names []string,
    count int,
    ts types.TS,
    typ string,
    filesList *[]*taeFile,
    protectionMgr *backupProtectionManager,
) error
```

## 4. Checkpoint 数据加载

### 4.1 加载 Checkpoint 条目

```go
func LoadCheckpointEntriesFromKey(
    ctx context.Context,
    sid string,
    fs fileservice.FileService,
    location objectio.Location,
    version uint32,
    softDeletes *map[string]bool,
    baseTS *types.TS,
) ([]*objectio.BackupObject, *CKPReader, error)
```

### 4.2 遍历对象

```go
ckpReader.ForEachRow(ctx,
    func(account uint32, dbid, tid uint64, objectType int8,
         objectStats objectio.ObjectStats, createAt, deletedAt types.TS,
         rowID types.Rowid) error {
        
        // 判断是否需要备份
        if deletedAt.IsEmpty() && isAblk {
            return nil  // 未刷盘，不需要复制
        }
        
        bo := &objectio.BackupObject{
            Location: objectStats.ObjectLocation(),
            CrateTS:  createAt,
            DropTS:   deletedAt,
        }
        
        // 判断是否需要复制
        if baseTS.IsEmpty() || createAt.GE(baseTS) || commitAt.GE(baseTS) {
            bo.NeedCopy = true
        }
        
        locations = append(locations, bo)
        return nil
    },
)
```

## 5. 并行复制

### 5.1 并行度计算

```go
func getParallelCount(count int) int {
    if count > 0 && count < 512 {
        return count
    }
    cupNum := runtime.NumCPU()
    if cupNum < 8 {
        return 50
    } else if cupNum < 16 {
        return 80
    } else if cupNum < 32 {
        return 128
    } else if cupNum < 64 {
        return 256
    }
    return 512
}
```

### 5.2 并行复制实现

```go
func parallelCopyData(
    srcFs, dstFs fileservice.FileService,
    files map[string]*objectio.BackupObject,
    parallelCount int,
    gcFileMap map[string]string,
) ([]*taeFile, error) {
    
    jobScheduler := tasks.NewParallelJobScheduler(parallelCount)
    defer jobScheduler.Stop()
    
    // 为每个文件创建复制任务
    for n := range files {
        backupJobs[idx] = getJob(srcFs, dstFs, files[n])
        idx++
    }
    
    // 调度执行
    for n := range backupJobs {
        err := jobScheduler.Schedule(backupJobs[n])
    }
    
    // 等待完成
    for n := range backupJobs {
        ret := backupJobs[n].WaitDone()
    }
}
```

### 5.3 单文件复制

```go
func CopyFile(ctx context.Context, srcFs, dstFs fileservice.FileService, 
              name, dstDir string, newNames ...string) ([]byte, error) {
    
    // 读取源文件
    ioVec := &fileservice.IOVector{
        FilePath: name,
        Entries: []fileservice.IOEntry{{
            ReadCloserForRead: &reader,
            Offset:            0,
            Size:              -1,
        }},
    }
    err := srcFs.Read(ctx, ioVec)
    
    // 计算校验和并写入
    hasher := sha256.New()
    hashingReader := io.TeeReader(reader, hasher)
    
    dstIoVec := fileservice.IOVector{
        FilePath: newName,
        Entries: []fileservice.IOEntry{{
            ReaderForWrite: hashingReader,
            Offset:         0,
            Size:           -1,
        }},
    }
    err = dstFs.Write(ctx, dstIoVec)
    
    return hasher.Sum(nil), nil
}
```

## 6. Checkpoint 目录复制

```go
func CopyCheckpointDir(
    ctx context.Context,
    srcFs, dstFs fileservice.FileService,
    dir string, 
    backup types.TS,
) ([]*taeFile, types.TS, error) {
    
    // 复制所有 Checkpoint 文件
    taeFiles, metaFiles, _, err := copyFileAndGetMetaFiles(
        ctx, srcFs, dstFs, dir, backup, decoder, true,
    )
    
    // 找到最后一个全局 Checkpoint 的结束时间
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

## 7. GC 目录复制

```go
func CopyGCDir(
    ctx context.Context,
    srcFs, dstFs fileservice.FileService,
    dir string,
    backup, min types.TS,
) ([]*taeFile, error) {
    
    // 复制 GC 元数据文件
    taeFiles, metaFiles, files, err := copyFileAndGetMetaFiles(
        ctx, srcFs, dstFs, dir, backup, ioutil.DecodeGCMetadataName, false,
    )
    
    // 读取并复制 GC 窗口中的对象
    for _, metaFile := range metaFiles {
        window := gc.NewGCWindow(common.DebugAllocator, srcFs)
        err = window.ReadTable(ctx, metaFile.GetGCFullName(), srcFs)
        
        objects := window.GetObjectStats()
        for _, object := range objects {
            checksum, err = CopyFileWithRetry(ctx, srcFs, dstFs, 
                object.ObjectName().String(), "")
        }
    }
}
```

## 8. Checkpoint 重写

对于增量备份，需要重写 Checkpoint 以裁剪数据：

```go
func ReWriteCheckpointAndBlockFromKey(
    ctx context.Context,
    sid string,
    fs, dstFs fileservice.FileService,
    loc objectio.Location,
    lastCkpData *CKPReader,
    version uint32, 
    ts types.TS,
) (objectio.Location, objectio.Location, []string, error) {
    
    // 1. 加载 Checkpoint
    ckpReader, err := GetCheckpointReader(ctx, sid, fs, loc, version)
    
    // 2. 分析需要保留的对象
    initData(&objectsData, ckputil.ObjectType_Data, objectio.SchemaData)
    initData(&tombstonesData, ckputil.ObjectType_Tombstone, objectio.SchemaTombstone)
    
    // 3. 裁剪 Tombstone 数据
    err = trimTombstoneData(ctx, fs, ts, &tombstonesData)
    
    // 4. 重写数据对象
    err = insertBatchFun(objectsData, ...)
    
    // 5. 重写 Tombstone 对象
    err = insertBatchFun(tombstonesData, ...)
    
    // 6. 生成新的 Checkpoint
    newData := NewCheckpointDataWithSinker(dataSinker, common.CheckpointAllocator)
    location, checkpointFiles, err := newData.Sync(ctx, dstFs)
    
    return location, location, files, nil
}
```

## 9. 文件列表保存

```go
func saveTaeFilesList(ctx context.Context, Fs fileservice.FileService, 
                      taeFiles []*taeFile, backupTime, backupTS, typ string) error {
    
    // 保存文件列表
    lines, size := taeFileListToCsv(taeFiles)
    metas, err := ToCsvLine2(lines)
    err = writeFile(ctx, Fs, taeList, []byte(metas))
    
    // 保存汇总信息
    lines = [][]string{taeBackupTimeAndSizeToCsv(backupTime, backupTS, typ, size)}
    metas, err = ToCsvLine2(lines)
    return writeFile(ctx, Fs, taeSum, []byte(metas))
}
```
