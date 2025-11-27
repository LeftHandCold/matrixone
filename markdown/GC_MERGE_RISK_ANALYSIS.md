# GC Merge 操作风险分析

## 问题描述

当太久没有merge checkpoint，导致ckp目录下checkpoint过多时，一次性merge这些checkpoint可能会出现**被快照引用的文件被GC**的风险。

## 核心流程分析

### 1. Merge Checkpoint 流程

#### 1.1 `mergeCheckpointFilesLocked` 函数流程

**位置**: `pkg/vm/engine/tae/db/gc/v3/checkpoint.go:822`

**关键步骤**:

1. **获取要merge的checkpoint列表** (第885行)
   ```go
   toMergeCheckpoint = c.getEntriesToMerge(checkpointLowWaterMark)
   ```
   - 通过 `getEntriesToMerge` 获取需要merge的checkpoint entries
   - 受 `maxMergeCheckpointCount` 限制

2. **构建Bloom Filter** (第920-929行)
   ```go
   window := c.GetScannedWindowLocked()
   sourcer := window.MakeFilesReader(ctx, c.fs)
   bf, err := BuildBloomfilter(
       ctx,
       Default_Coarse_EstimateRows,
       Default_Coarse_Probility,
       0,
       sourcer.Read,
       memoryBuffer,
       c.mp,
   )
   ```
   - **关键点**: Bloom filter是基于**整个scanned window**构建的，而不是只基于要merge的checkpoint
   - `scanned window`包含了所有已扫描的checkpoint中的文件

3. **调用MergeCheckpoint** (第931-944行)
   ```go
   deleteFiles, tmpNewFiles, newCkp, newCkpData, err = MergeCheckpoint(
       ctx,
       c.TaskNameLocked(),
       c.sid,
       toMergeCheckpoint,  // 只merge这些checkpoint
       bf,                 // 但使用整个scanned window的bloom filter
       &ckpMaxEnd,
       c.checkpointCli,
       c.mp,
       c.fs,
   )
   ```

#### 1.2 `MergeCheckpoint` 函数流程

**位置**: `pkg/vm/engine/tae/db/gc/v3/merge.go:40`

**关键步骤**:

1. **收集所有要删除的文件** (第54-115行)
   ```go
   for _, ckpEntry := range ckpEntries {
       // 加载checkpoint数据
       _, data, err = logtail.LoadCheckpointEntriesFromKey(...)
       
       // 添加checkpoint元文件到deleteFiles
       deleteFiles = append(deleteFiles, nameMeta)
       deleteFiles = append(deleteFiles, ckpEntry.GetLocation().Name().String())
       
       // 添加checkpoint中的所有locations到deleteFiles
       locations, err = logtail.LoadCheckpointLocations(ctx, sid, data)
       for name := range locations {
           deleteFiles = append(deleteFiles, name)
       }
       
       // 添加tableIDLocations到deleteFiles
       tableIDLocations := ckpEntry.GetTableIDLocation()
       for i := 0; i < tableIDLocations.Len(); i++ {
           deleteFiles = append(deleteFiles, location.Name().String())
       }
   }
   ```
   - **关键点**: 所有要merge的checkpoint中的文件都被添加到`deleteFiles`中

2. **使用Bloom Filter过滤对象** (第123-144行)
   ```go
   // merge objects referenced by snapshot and pitr
   for _, data := range datas {
       var objectBatch *batch.Batch
       objectBatch, err = data.GetCheckpointData(ctx)
       
       statsVec := objectBatch.Vecs[ckputil.TableObjectsAttr_ID_Idx]
       bf.Test(statsVec,
           func(exists bool, i int) {
               if !exists {
                   return  // 不在bloom filter中，跳过（不添加到merged checkpoint）
               }
               // 在bloom filter中，添加到merged checkpoint
               appendValToBatchForObjectListBatch(objectBatch, ckpData, i, pool)
           })
   }
   ```
   - **关键逻辑**: 
     - 如果对象**在bloom filter中**（即存在于scanned window中），则添加到merged checkpoint
     - 如果对象**不在bloom filter中**（即不存在于scanned window中），则**不添加**到merged checkpoint
   - **问题**: Bloom filter是基于整个scanned window构建的，而不是只基于要merge的checkpoint

3. **生成新的merged checkpoint** (第146-194行)
   - 只包含通过bloom filter过滤的对象
   - 旧的checkpoint文件会被删除

### 2. GC 流程

#### 2.1 `doGCAgainstGlobalCheckpointLocked` 函数

**位置**: `pkg/vm/engine/tae/db/gc/v3/checkpoint.go:1212`

**关键步骤**:

1. **执行GC** (第1256行)
   ```go
   filesToGC, metafile, err = scannedWindow.ExecuteGlobalCheckpointBasedGC(
       ctx,
       gckp,
       snapshots,  // 包含snapshot引用信息
       pitrs,
       c.mutation.snapshotMeta,
       iscp,
       c.checkpointCli,
       memoryBuffer,
       c.config.canGCCacheSize,
       c.config.estimateRows,
       c.config.probility,
       c.mp,
       c.fs,
   )
   ```
   - 使用`snapshots`来检查哪些文件被snapshot引用
   - 被snapshot引用的文件不会被GC

2. **删除文件** (第1172行)
   ```go
   if err = c.deleter.DeleteMany(
       ctx,
       c.TaskNameLocked(),
       filesToGC,
   ); err != nil {
       // ...
   }
   ```

## 风险分析

### 风险场景1: Bloom Filter范围不匹配

**问题描述**:

当一次性merge大量checkpoint时，存在以下不匹配：

1. **Bloom Filter的构建范围**: 基于整个`scanned window`，包含所有已扫描的checkpoint中的文件
2. **Merge的范围**: 只merge部分checkpoint（`toMergeCheckpoint`）

**具体场景**:

假设有以下checkpoint序列：
- ckp1: [t0, t100) - 包含文件 f1, f2
- ckp2: [t100, t200) - 包含文件 f2, f3
- ckp3: [t200, t300) - 包含文件 f3, f4
- ...
- ckp10: [t900, t1000) - 包含文件 f9, f10

**scanned window**包含: f1, f2, f3, ..., f10

**要merge的checkpoint**: ckp1, ckp2, ckp3 (假设maxMergeCheckpointCount=3)

**Bloom Filter**: 基于整个scanned window构建，包含 f1-f10

**Merge过程**:
1. 遍历ckp1, ckp2, ckp3中的所有对象
2. 对于每个对象，检查是否在bloom filter中
3. 如果对象在bloom filter中（即存在于scanned window中），添加到merged checkpoint
4. 如果对象不在bloom filter中，**不添加**到merged checkpoint

**潜在问题**:
- 如果某个文件f1在ckp1中被引用，但在ckp2-ckp10中不再被引用
- f1在scanned window中，所以会被bloom filter标记为存在
- 在merge ckp1-ckp3时，f1会被添加到merged checkpoint（因为它在bloom filter中）
- **但是**，如果f1被某个snapshot引用（snapshot时间戳在t0-t1000之间），它应该被保留
- 如果f1在merged checkpoint中被错误地处理，可能导致后续GC时被删除

### 风险场景2: Snapshot引用检查时机

**问题描述**:

在`MergeCheckpoint`函数中，**完全没有检查snapshot引用**。该函数甚至没有接收snapshots参数：

```go
func MergeCheckpoint(
    ctx context.Context,
    taskName string,
    sid string,
    ckpEntries []*checkpoint.CheckpointEntry,
    bf *bloomfilter.BloomFilter,  // 只有bloom filter
    end *types.TS,
    client checkpoint.Runner,
    pool *mpool.MPool,
    fs fileservice.FileService,
) (deleteFiles, newFiles []string, checkpointEntry *checkpoint.CheckpointEntry, ckpData *batch.Batch, err error)
```

**注意**: 函数签名中没有`snapshots`或`pitrs`参数！

Snapshot引用检查发生在：

1. **GC阶段**: 在`ExecuteGlobalCheckpointBasedGC`中检查snapshot引用
2. **FillUsageBatOfCompacted**: 在merge之后调用（`checkpoint.go:945`），但只用于更新usage统计，不影响merge逻辑

**具体场景**:

1. Merge阶段:
   - 使用bloom filter过滤对象
   - **不检查snapshot引用**
   - 生成merged checkpoint

2. GC阶段:
   - 检查snapshot引用
   - 决定哪些文件可以GC

**潜在问题**:
- 如果某个文件在merge时被错误地排除在merged checkpoint之外
- 后续GC时，即使该文件被snapshot引用，也可能因为不在merged checkpoint中而被GC

### 风险场景3: 大量Checkpoint一次性Merge

**问题描述**:

当太久没有merge checkpoint时，可能会有大量checkpoint需要merge：

1. **maxMergeCheckpointCount限制**: 虽然有`maxMergeCheckpointCount`限制，但如果这个值设置得较大，仍然可能一次性merge很多checkpoint

2. **时间跨度大**: 大量checkpoint可能跨越很长的时间范围，导致：
   - scanned window包含大量文件
   - bloom filter可能不够精确（false positive）
   - snapshot引用检查可能遗漏某些文件

**具体场景**:

假设有100个checkpoint需要merge，时间跨度从t0到t10000：
- scanned window可能包含数万个文件
- bloom filter的false positive率可能导致某些文件被错误地标记为存在或不存在
- 如果某个文件在早期checkpoint中被引用，但在后期不再被引用，可能在merge时被错误处理

## 关键代码位置

### 1. Bloom Filter构建
- **位置**: `pkg/vm/engine/tae/db/gc/v3/checkpoint.go:920-929`
- **问题**: 基于整个scanned window，而不是只基于要merge的checkpoint

### 2. Merge过滤逻辑
- **位置**: `pkg/vm/engine/tae/db/gc/v3/merge.go:123-144`
- **问题**: 只使用bloom filter，没有直接检查snapshot引用

### 3. Snapshot引用检查
- **位置**: `pkg/vm/engine/tae/db/gc/v3/exec_v1.go:409-425`
- **时机**: 在GC阶段，不在merge阶段

### 4. FillUsageBatOfCompacted
- **位置**: `pkg/vm/engine/tae/db/gc/v3/checkpoint.go:945-952`
- **作用**: 更新usage统计，不影响merge逻辑

## 建议的修复方案

### 方案1: 在Merge时也检查Snapshot引用

**修改位置**: `pkg/vm/engine/tae/db/gc/v3/merge.go:123-144`

**修改思路**:
- 修改`MergeCheckpoint`函数签名，添加`snapshots`和`pitrs`参数
- 在merge时，不仅使用bloom filter，还要检查snapshot引用
- 如果对象被snapshot引用，即使不在bloom filter中，也要添加到merged checkpoint

**修改后的函数签名**:
```go
func MergeCheckpoint(
    ctx context.Context,
    taskName string,
    sid string,
    ckpEntries []*checkpoint.CheckpointEntry,
    bf *bloomfilter.BloomFilter,
    end *types.TS,
    snapshots *logtail.SnapshotInfo,  // 新增
    pitrs *logtail.PitrInfo,          // 新增
    snapshotMeta *logtail.SnapshotMeta, // 新增，用于获取table级别的snapshot
    client checkpoint.Runner,
    pool *mpool.MPool,
    fs fileservice.FileService,
) (deleteFiles, newFiles []string, checkpointEntry *checkpoint.CheckpointEntry, ckpData *batch.Batch, err error)
```

**修改后的过滤逻辑**:
```go
// merge objects referenced by snapshot and pitr
for _, data := range datas {
    var objectBatch *batch.Batch
    objectBatch, err = data.GetCheckpointData(ctx)
    
    statsVec := objectBatch.Vecs[ckputil.TableObjectsAttr_ID_Idx]
    createTSVec := objectBatch.Vecs[ckputil.TableObjectsAttr_CreateTS_Idx]
    deleteTSVec := objectBatch.Vecs[ckputil.TableObjectsAttr_DeleteTS_Idx]
    tableIDVec := objectBatch.Vecs[ckputil.TableObjectsAttr_Table_Idx]
    
    // 获取table级别的snapshot和pitr信息
    tableSnapshots, tablePitrs := snapshotMeta.AccountToTableSnapshots(snapshots, pitrs)
    
    for i := 0; i < objectBatch.RowCount(); i++ {
        statsBytes := statsVec.GetBytesAt(i)
        stats := (*objectio.ObjectStats)(unsafe.Pointer(&statsBytes[0]))
        createTS := vector.GetFixedAtNoTypeCheck[types.TS](createTSVec, i)
        deleteTS := vector.GetFixedAtNoTypeCheck[types.TS](deleteTSVec, i)
        tableID := vector.GetFixedAtNoTypeCheck[uint64](tableIDVec, i)
        
        // 检查是否在bloom filter中
        inBloomFilter := bf.Test(statsBytes)
        
        // 检查是否被snapshot引用
        tableSnapVec := tableSnapshots[tableID]
        tablePitr := tablePitrs[tableID]
        isSnapshotRefers := logtail.ObjectIsSnapshotRefers(
            stats, tablePitr, &createTS, &deleteTS, tableSnapVec)
        
        // 如果在bloom filter中或被snapshot引用，添加到merged checkpoint
        if inBloomFilter || isSnapshotRefers {
            appendValToBatchForObjectListBatch(objectBatch, ckpData, i, pool)
        }
    }
}
```

**调用处也需要修改** (`checkpoint.go:931`):
```go
if deleteFiles, tmpNewFiles, newCkp, newCkpData, err = MergeCheckpoint(
    ctx,
    c.TaskNameLocked(),
    c.sid,
    toMergeCheckpoint,
    bf,
    &ckpMaxEnd,
    snapshots,        // 新增
    pitrs,            // 新增
    c.mutation.snapshotMeta, // 新增
    c.checkpointCli,
    c.mp,
    c.fs,
); err != nil {
    // ...
}
```

### 方案2: 限制Bloom Filter的范围

**修改位置**: `pkg/vm/engine/tae/db/gc/v3/checkpoint.go:920-929`

**修改思路**:
- 只基于要merge的checkpoint构建bloom filter，而不是整个scanned window
- 需要创建一个临时的GCWindow，只包含要merge的checkpoint

**伪代码**:
```go
// 创建临时window，只包含要merge的checkpoint
tempWindow := NewGCWindow(c.mp, c.fs)
for _, ckp := range toMergeCheckpoint {
    // 扫描checkpoint，添加到tempWindow
    tempWindow.ScanCheckpoint(ckp, ...)
}

// 基于tempWindow构建bloom filter
sourcer := tempWindow.MakeFilesReader(ctx, c.fs)
bf, err := BuildBloomfilter(...)
```

### 方案3: 分批Merge

**修改位置**: `pkg/vm/engine/tae/db/gc/v3/checkpoint.go:885`

**修改思路**:
- 限制每次merge的checkpoint数量
- 确保每次merge的checkpoint数量不超过某个阈值（如10个）
- 多次merge，而不是一次性merge所有checkpoint

**伪代码**:
```go
const maxMergeBatchSize = 10
for len(toMergeCheckpoint) > 0 {
    batchSize := min(maxMergeBatchSize, len(toMergeCheckpoint))
    batch := toMergeCheckpoint[:batchSize]
    toMergeCheckpoint = toMergeCheckpoint[batchSize:]
    
    // merge这一批checkpoint
    err = c.mergeCheckpointBatch(batch, ...)
}
```

## 总结

主要风险在于：

1. **Bloom Filter范围不匹配**: 基于整个scanned window构建，但只merge部分checkpoint
2. **缺少Snapshot引用检查**: Merge阶段不检查snapshot引用，只在GC阶段检查
3. **大量Checkpoint一次性Merge**: 可能导致时间跨度大，增加错误概率

建议采用**方案1**（在Merge时也检查Snapshot引用）作为主要修复方案，因为这样可以确保被snapshot引用的文件不会被错误地排除在merged checkpoint之外。

