# 增量备份

## 1. 概述

增量备份只备份指定时间点之后的数据变更，相比全量备份可以显著减少备份时间和存储空间。

## 2. 增量备份原理

```
全量备份 (T0)                    增量备份 (T1)
┌─────────────────┐              ┌─────────────────┐
│  Checkpoint 1   │              │  Checkpoint 3   │
│  [T0, T100]     │              │  [T200, T300]   │
├─────────────────┤              ├─────────────────┤
│  Checkpoint 2   │              │  Checkpoint 4   │
│  [T100, T200]   │              │  [T300, T400]   │
├─────────────────┤              └─────────────────┘
│  所有数据对象   │                    │
│  f1, f2, f3...  │                    │
└─────────────────┘                    ▼
                                 只备份 T200 之后
                                 新增/修改的对象
```

## 3. 实现机制

### 3.1 备份时间戳

```go
type Config struct {
    // ...
    BackupType string    // 备份类型
    BackupTs   types.TS  // 增量备份的基准时间戳
}
```

### 3.2 判断是否需要复制

```go
// 在 LoadCheckpointEntriesFromKey 中
ckpReader.ForEachRow(ctx,
    func(..., createAt, deletedAt types.TS, ...) error {
        commitAt := createAt
        if !deletedAt.IsEmpty() {
            commitAt = deletedAt
        }
        
        bo := &objectio.BackupObject{
            Location: objectStats.ObjectLocation(),
            CrateTS:  createAt,
            DropTS:   deletedAt,
        }
        
        // 判断是否需要复制
        if baseTS.IsEmpty() || 
           (!baseTS.IsEmpty() && (createAt.GE(baseTS) || commitAt.GE(baseTS))) {
            bo.NeedCopy = true
        }
        
        locations = append(locations, bo)
        return nil
    },
)
```

### 3.3 跳过已备份文件

```go
// 检查目标是否已存在
dstObj, err := fileservice.SortedList(dstFs.List(ctx, ""))
dstHave := make(map[string]bool)

if len(dstObj) != 0 && dstObj[0].Name == fileList {
    data, err := readFile(ctx, dstFs, fileList)
    objects := strings.Split(string(data), "\n")
    for _, object := range objects {
        dstHave[object] = true
    }
}

// 标记不需要复制的文件
for _, oName := range oNames {
    objName := oName.Location.Name().String()
    if dstHave[objName] {
        oName.NeedCopy = false
    }
}
```

## 4. Checkpoint 裁剪

### 4.1 裁剪流程

增量备份需要裁剪 Checkpoint 数据，移除基准时间点之前的变更：

```go
func ReWriteCheckpointAndBlockFromKey(
    ctx context.Context,
    sid string,
    fs, dstFs fileservice.FileService,
    loc objectio.Location,
    lastCkpData *CKPReader,
    version uint32, 
    ts types.TS,  // 裁剪时间点
) (objectio.Location, objectio.Location, []string, error)
```

### 4.2 Tombstone 裁剪

```go
func trimTombstoneData(
    ctx context.Context,
    fs fileservice.FileService,
    ts types.TS,
    objectsData *map[string]*objData,
) error {
    
    for name := range *objectsData {
        if !(*objectsData)[name].appendable {
            continue
        }
        
        // 加载 Tombstone 数据
        location := (*objectsData)[name].stats.ObjectLocation()
        bat, sortKey, err := ioutil.LoadOneBlock(ctx, fs, location, objectio.SchemaData)
        
        // 过滤掉裁剪时间点之后的删除记录
        deleteRow := make([]int64, 0)
        for v := 0; v < bat.Vecs[0].Length(); v++ {
            commitTs := types.TS{}
            commitTs.Unmarshal(bat.Vecs[len(bat.Vecs)-1].GetRawBytesAt(v))
            
            if commitTs.GT(&ts) {
                // 跳过裁剪时间点之后的记录
                continue
            }
            deleteRow = append(deleteRow, int64(v))
        }
        
        // 收缩 batch
        if len(deleteRow) != bat.Vecs[0].Length() {
            bat.Shrink(deleteRow, false)
        }
        
        (*objectsData)[name].data = append((*objectsData)[name].data, bat)
    }
    
    return nil
}
```

### 4.3 数据对象裁剪

```go
// 使用 BackupDeltaLocDataSource 应用 Tombstone
ds := NewBackupDeltaLocDataSource(ctx, fs, ts, dsTombstone)
blk := oData.stats.ConstructBlockInfo(uint16(0))
bat, sortKey, err := blockio.BlockDataReadBackup(ctx, &blk, ds, nil, ts, fs)
```

## 5. BackupDeltaLocDataSource

### 5.1 数据结构

```go
type BackupDeltaLocDataSource struct {
    ctx        context.Context
    fs         fileservice.FileService
    ts         types.TS
    ds         map[string]*objData
    tombstones []objectio.ObjectStats
    needShrink bool
}
```

### 5.2 获取 Tombstone

```go
func (d *BackupDeltaLocDataSource) GetTombstones(
    ctx context.Context, 
    bid *objectio.Blockid,
) (deletedRows objectio.Bitmap, err error) {
    
    deletedRows = objectio.GetNoReuseBitmap()
    
    // 处理 tombstones
    if len(d.tombstones) > 0 {
        buildDS(
            func(tombstone objectio.ObjectStats) (bool, error) {
                // 检查 ZoneMap 是否匹配
                if !tombstone.ZMIsEmpty() {
                    objZM := tombstone.SortKeyZoneMap()
                    if skip := !objZM.PrefixEq(bid[:]); skip {
                        return true, nil
                    }
                }
                
                // 加载并过滤 Tombstone 数据
                for id := uint32(0); id < tombstone.BlkCnt(); id++ {
                    bat, _, err := ioutil.LoadOneBlock(ctx, d.fs, location, objectio.SchemaData)
                    
                    // 过滤时间戳
                    deleteRow := make([]int64, 0)
                    for v := 0; v < bat.Vecs[0].Length(); v++ {
                        commitTs.Unmarshal(bat.Vecs[len(bat.Vecs)-1].GetRawBytesAt(v))
                        if commitTs.GT(&d.ts) {
                            continue
                        }
                        deleteRow = append(deleteRow, int64(v))
                    }
                }
                return true, nil
            },
            d.tombstones,
        )
    }
    
    // 应用 Tombstone 到 bitmap
    scanOp := func(onTombstone func(tombstone *objData) (bool, error)) error {
        return ForeachTombstoneObject(onTombstone, d.ds)
    }
    
    GetTombstonesByBlockId(bid, &deletedRows, scanOp, d.needShrink)
    
    return
}
```

## 6. 增量备份文件过滤

### 6.1 Checkpoint 文件过滤

```go
func copyFileAndGetMetaFiles(...) {
    for i, file := range mFiles {
        meta := decoder(file.Name)
        
        if !backup.IsEmpty() {
            start := meta.GetStart()
            end := meta.GetEnd()
            
            // 跳过备份时间点之后的文件
            if !end.IsEmpty() && end.GT(&backup) {
                continue
            }
            if !start.IsEmpty() && start.GE(&backup) {
                continue
            }
        }
        
        // 复制文件
        // ...
    }
}
```

## 7. 增量备份恢复

增量备份恢复需要：

1. 先恢复全量备份
2. 按时间顺序应用增量备份
3. 重放 Checkpoint 数据

## 8. 最佳实践

### 8.1 备份策略

```
周日: 全量备份
周一-周六: 增量备份

恢复时:
1. 恢复周日的全量备份
2. 依次应用周一到目标日期的增量备份
```

### 8.2 注意事项

1. 增量备份依赖全量备份，需要保留完整的备份链
2. 增量备份的基准时间戳必须准确
3. 恢复时需要按正确顺序应用增量备份
4. 定期进行全量备份以缩短恢复时间
