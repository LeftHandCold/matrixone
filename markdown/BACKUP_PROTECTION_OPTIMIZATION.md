# 备份保护机制优化总结

## 会话时间
2025-11-25

## 修改概述

本次会话主要对备份保护机制进行了两个方面的优化：
1. **修复测试错误**：处理空 checkpoint 的保护逻辑和测试断言问题
2. **优化保护策略**：当 backup 保护激活时，完全跳过 checkpoint 元数据的 merge 和删除操作，但保持数据文件的正常 GC

## 详细修改

### 1. 修复空 Checkpoint 保护逻辑

#### 问题描述
在 `TestBackupProtectionCheckpointEdgeCases` 测试中，空的 `CheckpointEntry{}` 的 `end` 时间戳为 `0-0`，由于 `0-0.LE(100-0)` 为 true，导致空 checkpoint 被错误地保护，测试失败。

#### 修复方案
在 `checkBackupProtection` 函数中添加对空时间戳的检查：

**文件**: `pkg/vm/engine/tae/db/gc/v3/checkpoint.go`

```go
func (c *checkpointCleaner) checkBackupProtection(item any) bool {
    // ... 前面的检查逻辑 ...
    
    // For checkpoint entries, check if the end timestamp is less than or equal to protected timestamp
    endTS := ckp.GetEnd()
    // Empty/invalid timestamps should not be protected (allow GC)
    if endTS.IsEmpty() {
        return true
    }
    if endTS.LE(&protectedTS) {
        // ... 保护逻辑 ...
        return false
    }
    
    return true
}
```

**修改说明**：
- 空时间戳的 checkpoint 不应该被保护，直接允许 GC
- 只有有效的、在保护时间点之前的 checkpoint 才需要被保护

#### 修复测试断言错误

**文件**: `pkg/vm/engine/tae/db/gc/v3/backup_protection_test.go`

修复了 `TestBackupProtectionFilterCheckpoints` 测试中的断言错误：

```go
// 修复前
require.True(t, ts2.EQ(&ts2))  // 错误：自己和自己比较

// 修复后
end2 := finalFiltered[1].GetEnd()
require.True(t, end2.EQ(&ts2))  // 正确：比较第二个 checkpoint 的 end 时间戳
```

### 2. 优化备份保护策略

#### 问题描述
用户反馈：当开启了 backup 任务时，checkpoint 还会被 merge，希望 checkpoint 和 gc 元数据都不被 merge 或删除，保持现状。等 backup 完成后，GC 会清理和 merge 这些过期的元数据。

#### 优化方案
当 backup 保护激活时：
- **完全跳过 checkpoint 元数据的 merge 和删除操作**
- **保持数据文件的正常 GC**（数据文件仍然需要被清理）
- **保持水位的正常更新**

#### 修改详情

##### 2.1 `mergeCheckpointFilesLocked` - 跳过 Merge 操作

**文件**: `pkg/vm/engine/tae/db/gc/v3/checkpoint.go`

在函数开始时检查 backup 保护状态，如果激活则直接返回：

```go
func (c *checkpointCleaner) mergeCheckpointFilesLocked(
    ctx context.Context,
    checkpointLowWaterMark *types.TS,
    memoryBuffer *containers.OneSchemaBatchBuffer,
    snapshots *logtail.SnapshotInfo,
    pitrs *logtail.PitrInfo,
    gcFileCount int,
) (err error) {
    // Skip merge if backup protection is active
    // Use snapshot taken at GC start to ensure consistency
    protectedTS, isActive := c.getBackupProtectionSnapshot()
    if isActive {
        logutil.Info(
            "GC-Backup-Protection-Skip-Merge",
            zap.String("task", c.TaskNameLocked()),
            zap.String("protected-ts", protectedTS.ToString()),
        )
        return nil
    }
    
    // ... 原有的 merge 逻辑 ...
}
```

**关键点**：
- 使用 `getBackupProtectionSnapshot()` 获取保护状态快照（在 GC 开始时创建，确保一致性）
- 如果保护激活，直接返回，跳过整个 merge 操作
- 移除了之前过滤 checkpoint 的逻辑（不再需要，因为直接跳过 merge）

##### 2.2 `deleteStaleCKPMetaFileLocked` - 跳过删除操作

**文件**: `pkg/vm/engine/tae/db/gc/v3/checkpoint.go`

在函数开始时检查 backup 保护状态，如果激活则直接返回：

```go
func (c *checkpointCleaner) deleteStaleCKPMetaFileLocked() (err error) {
    // Skip deletion if backup protection is active
    // Use snapshot taken at GC start to ensure consistency
    protectedTS, isActive := c.getBackupProtectionSnapshot()
    if isActive {
        logutil.Info(
            "GC-Backup-Protection-Skip-Delete-CKP-Meta",
            zap.String("task", c.TaskNameLocked()),
            zap.String("protected-ts", protectedTS.ToString()),
        )
        return nil
    }
    
    // ... 原有的删除逻辑 ...
}
```

##### 2.3 `deleteStaleSnapshotFilesLocked` - 跳过删除操作

**文件**: `pkg/vm/engine/tae/db/gc/v3/checkpoint.go`

在函数开始时检查 backup 保护状态，如果激活则直接返回：

```go
func (c *checkpointCleaner) deleteStaleSnapshotFilesLocked() error {
    // Skip deletion if backup protection is active
    // Use snapshot taken at GC start to ensure consistency
    protectedTS, isActive := c.getBackupProtectionSnapshot()
    if isActive {
        logutil.Info(
            "GC-Backup-Protection-Skip-Delete-Snapshot",
            zap.String("task", c.TaskNameLocked()),
            zap.String("protected-ts", protectedTS.ToString()),
        )
        return nil
    }
    
    // ... 原有的删除逻辑 ...
}
```

##### 2.4 `tryGCAgainstGCKPLocked` - 保持数据文件正常 GC

**文件**: `pkg/vm/engine/tae/db/gc/v3/checkpoint.go`

**重要修改**：移除了之前添加的保护判断，让数据文件正常 GC：

```go
func (c *checkpointCleaner) tryGCAgainstGCKPLocked(
    ctx context.Context,
    gckp *checkpoint.CheckpointEntry,
    memoryBuffer *containers.OneSchemaBatchBuffer,
) (err error) {
    // ... 获取 snapshots ...
    
    filesToGC, err := c.doGCAgainstGlobalCheckpointLocked(
        ctx, gckp, snapshots, pitrs, memoryBuffer,
    )
    if err != nil {
        extraErrMsg = "doGCAgainstGlobalCheckpointLocked failed"
        return
    }
    
    // Delete files after doGCAgainstGlobalCheckpointLocked
    // TODO:Requires Physical Removal Policy
    // Note: Data files are GC'ed normally even when backup protection is active.
    // Only checkpoint metadata merge/delete is skipped (handled in mergeCheckpointFilesLocked).
    if err = c.deleter.DeleteMany(
        ctx,
        c.TaskNameLocked(),
        filesToGC,
    ); err != nil {
        extraErrMsg = fmt.Sprintf("ExecDelete %v failed", filesToGC)
        return
    }
    
    // ... 更新水位 ...
    
    // 正常调用 mergeCheckpointFilesLocked，但它内部会检查保护状态
    err = c.mergeCheckpointFilesLocked(
        ctx, &waterMark, memoryBuffer, snapshots, pitrs, len(filesToGC),
    )
    // ...
}
```

**关键点**：
- 数据文件正常 GC（删除），即使 backup 保护激活
- 水位正常更新
- `mergeCheckpointFilesLocked` 正常调用，但它内部会检查保护状态并直接返回

## 优化后的工作流程

### Backup 保护激活时的 GC 行为

1. **数据文件 GC**：✅ 正常执行
   - `doGCAgainstGlobalCheckpointLocked` 正常执行
   - `deleter.DeleteMany` 正常删除数据文件
   - 水位正常更新

2. **Checkpoint 元数据**：❌ 完全跳过
   - `mergeCheckpointFilesLocked`：直接返回，跳过 merge
   - `deleteStaleCKPMetaFileLocked`：直接返回，跳过删除
   - `deleteStaleSnapshotFilesLocked`：直接返回，跳过删除

3. **数据对象保护**：✅ 继续生效
   - 通过 `GetSnapshot` 传入 `protectedTS` 作为集群级别 snapshot
   - `ObjectIsSnapshotRefers` 会检查这个时间戳
   - 保护在备份时间点存在的对象（`createTS <= protectedTS < dropTS`）

### Backup 完成后的 GC 行为

1. 备份进程移除保护（通过 `mo_ctl` 命令）
2. GC 恢复正常工作
3. 之前被保护的 checkpoint 元数据会被正常 merge 和删除

## 优势

1. **简化逻辑**：不再需要过滤 checkpoint，直接跳过整个操作，代码更简洁
2. **保持数据文件 GC**：数据文件仍然正常清理，避免磁盘空间浪费
3. **完全保护元数据**：checkpoint 和 gc 元数据在 backup 期间完全不被修改
4. **水位正常更新**：GC 水位正常更新，不影响后续的 GC 决策

## 相关文件

### 修改的文件

- `pkg/vm/engine/tae/db/gc/v3/checkpoint.go`
  - `checkBackupProtection`：添加空时间戳检查
  - `mergeCheckpointFilesLocked`：添加保护检查，直接跳过 merge
  - `deleteStaleCKPMetaFileLocked`：添加保护检查，直接跳过删除
  - `deleteStaleSnapshotFilesLocked`：添加保护检查，直接跳过删除
  - `tryGCAgainstGCKPLocked`：移除保护判断，保持数据文件正常 GC

- `pkg/vm/engine/tae/db/gc/v3/backup_protection_test.go`
  - `TestBackupProtectionFilterCheckpoints`：修复断言错误

## 测试验证

所有测试用例应该通过：

```bash
# 运行 GC 备份保护测试
go test -v ./pkg/vm/engine/tae/db/gc/v3 -run TestBackupProtection

# 运行备份保护集成测试
go test -v ./pkg/backup -run TestBackupProtection
```

## 总结

本次优化主要解决了两个问题：
1. **空 checkpoint 保护逻辑错误**：通过添加空时间戳检查修复
2. **保护策略优化**：当 backup 保护激活时，完全跳过 checkpoint 元数据的 merge 和删除，但保持数据文件的正常 GC 和水位的正常更新

这样既保证了备份期间 checkpoint 元数据的稳定性，又不会影响数据文件的正常清理，是一个更合理的保护策略。


