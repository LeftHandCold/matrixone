# 备份保护机制实现总结

## 问题背景

在备份过程中，如果备份时间过长，后台的 GC（垃圾回收）操作可能会删除备份时间点的数据，导致备份数据损坏。具体问题包括：

1. **数据对象被删除**：GC 可能删除在备份时间点仍然存在的对象（`createTS <= backupTS < dropTS`）
2. **Checkpoint 文件被删除**：GC 可能删除备份时间点之前生成的 checkpoint 文件
3. **元数据文件被删除**：GC 可能删除 `shared/ckp` 和 `shared/gc` 目录下的元数据文件

## 解决方案

实现了一个备份保护机制，通过时间戳保护来防止 GC 删除备份所需的数据和文件。

### 核心设计

1. **单一保护时间戳**：使用一个保护时间戳（`protectedTS`）来定义保护边界
2. **跨节点通信**：备份进程和 GC 进程可能在不同机器上，通过 `mo_ctl` 命令进行通信
3. **自动过期机制**：如果保护时间戳长时间未更新（20分钟），GC 自动移除保护，防止备份进程异常导致 GC 永久阻塞
4. **定期更新**：备份过程中每 5 分钟更新一次保护时间戳，确保 GC 知道备份仍在进行

## 实现细节

### 1. 备份端实现 (`pkg/backup/tae.go`)

#### 1.1 设置保护

在 `execBackup` 函数中，备份开始时设置保护时间戳：

```go
var protectedTS types.TS
if !start.IsEmpty() {
    protectedTS = start
} else if !baseTS.IsEmpty() {
    protectedTS = baseTS
}

if !protectedTS.IsEmpty() {
    // 通过 mo_ctl 设置备份保护
    tsValue := protectedTS.ToString()
    sql := fmt.Sprintf("select mo_ctl('dn','DiskCleaner','add_checker.backup.%s')", tsValue)
    _, err := exec.Exec(ctx, sql, opts)
    // ...
}
```

#### 1.2 定期更新保护

启动一个定时器，每 5 分钟更新一次保护时间戳：

```go
updateTicker = time.NewTicker(5 * time.Minute)
updateTickerStop = make(chan struct{})
go func() {
    for {
        select {
        case <-updateTicker.C:
            // 更新备份保护
            tsValue := protectedTS.ToString()
            sql := fmt.Sprintf("select mo_ctl('dn','DiskCleaner','add_checker.backup.%s')", tsValue)
            _, err := exec.Exec(ctx, sql, opts)
            // ...
        case <-updateTickerStop:
            return
        }
    }
}()
```

#### 1.3 移除保护

备份完成后，在 defer 中移除保护：

```go
defer func() {
    if updateTicker != nil {
        updateTicker.Stop()
        close(updateTickerStop)
        // 通知 GC 移除备份保护
        sql := "select mo_ctl('dn','DiskCleaner','remove_checker.backup.')"
        _, err := exec.Exec(ctx, sql, opts)
        // ...
    }
}()
```

#### 1.4 元数据文件复制优化

修复了 `copyFileAndGetMetaFiles` 中的过滤逻辑，确保只复制备份时间点之前的文件：

```go
if !backup.IsEmpty() {
    start := meta.GetStart()
    end := meta.GetEnd()
    // 跳过 end 时间戳大于备份时间点的文件
    if !end.IsEmpty() && end.GT(&backup) {
        logutil.Infof("[Backup] skip file %v (end %v > backup %v)", file.Name, end.ToString(), backup.ToString())
        continue
    }
    // 保留原来的 start 检查
    if !start.IsEmpty() && start.GE(&backup) {
        logutil.Infof("[Backup] skip file %v (start %v >= backup %v)", file.Name, start.ToString(), backup.ToString())
        continue
    }
}
```

### 2. GC 端实现 (`pkg/vm/engine/tae/db/gc/v3/checkpoint.go`)

#### 2.1 保护状态管理

在 `checkpointCleaner` 中添加了保护状态结构：

```go
type backupProtection struct {
    protectedTS    types.TS
    lastUpdateTime time.Time
    isActive       bool
    mu             sync.RWMutex
}
```

#### 2.2 保护方法实现

实现了 `SetBackupProtection`、`UpdateBackupProtection`、`RemoveBackupProtection` 和 `GetBackupProtection` 方法：

```go
func (c *checkpointCleaner) SetBackupProtection(protectedTS types.TS) {
    c.backupProtection.mu.Lock()
    defer c.backupProtection.mu.Unlock()
    c.backupProtection.protectedTS = protectedTS
    c.backupProtection.lastUpdateTime = time.Now()
    c.backupProtection.isActive = true
}

func (c *checkpointCleaner) UpdateBackupProtection(protectedTS types.TS) {
    c.backupProtection.mu.Lock()
    defer c.backupProtection.mu.Unlock()
    if c.backupProtection.isActive {
        c.backupProtection.protectedTS = protectedTS
        c.backupProtection.lastUpdateTime = time.Now()
    }
}

func (c *checkpointCleaner) RemoveBackupProtection() {
    c.backupProtection.mu.Lock()
    defer c.backupProtection.mu.Unlock()
    c.backupProtection.isActive = false
    c.backupProtection.protectedTS = types.TS{}
}

func (c *checkpointCleaner) GetBackupProtection() (protectedTS types.TS, lastUpdateTime time.Time, isActive bool) {
    c.backupProtection.mu.RLock()
    defer c.backupProtection.mu.RUnlock()
    return c.backupProtection.protectedTS, c.backupProtection.lastUpdateTime, c.backupProtection.isActive
}
```

#### 2.3 自动过期检查

在 `Process` 方法中检查保护是否过期（20分钟未更新）：

```go
func (c *checkpointCleaner) Process(ctx context.Context) error {
    // ...
    // 检查备份保护是否过期
    protectedTS, lastUpdateTime, isActive := c.GetBackupProtection()
    if isActive {
        if time.Since(lastUpdateTime) > 20*time.Minute {
            logutil.Warn("backup protection expired, removing")
            c.RemoveBackupProtection()
        }
    }
    // ...
}
```

#### 2.4 Checkpoint 文件保护

在 `checkBackupProtection` 中检查 checkpoint 是否应该被保护：

```go
func (c *checkpointCleaner) checkBackupProtection(endTS *types.TS) bool {
    protectedTS, _, isActive := c.GetBackupProtection()
    if !isActive {
        return false
    }
    // 保护 end 时间戳 <= 保护时间戳的 checkpoint
    return endTS.LE(&protectedTS)
}
```

在 `filterCheckpoints` 中过滤掉被保护的 checkpoint：

```go
func (c *checkpointCleaner) filterCheckpoints(checkpoints []*checkpoint.CheckpointEntry) []*checkpoint.CheckpointEntry {
    protectedTS, _, isActive := c.GetBackupProtection()
    if !isActive {
        return checkpoints
    }
    
    filtered := make([]*checkpoint.CheckpointEntry, 0, len(checkpoints))
    for _, ckp := range checkpoints {
        endTS := ckp.GetEnd()
        // 跳过被保护的 checkpoint
        if endTS.LE(&protectedTS) {
            continue
        }
        filtered = append(filtered, ckp)
    }
    return filtered
}
```

#### 2.5 元数据文件保护

在 `deleteStaleSnapshotFilesLocked` 和 `tryGCAgainstGCKPLocked` 中检查保护：

```go
protectedTS, _, isActive := c.GetBackupProtection()
if isActive && thisTS.LE(&protectedTS) {
    // 跳过被保护的文件
    continue
}
```

#### 2.6 数据对象保护

通过将保护时间戳作为"假的"集群级别 snapshot 传入 `GetSnapshot`，确保 `ObjectIsSnapshotRefers` 会检查这个时间戳：

```go
func (c *checkpointCleaner) GetSnapshotsLocked(ctx context.Context) (*logtail.SnapshotInfo, error) {
    // ...
    protectedTS, _, isActive := c.GetBackupProtection()
    var extraTS types.TS
    if isActive {
        extraTS = protectedTS
    }
    return c.snapshotMeta.GetSnapshot(ctx, c.sid, c.fs, c.mpool, extraTS)
}
```

### 3. Snapshot 集成 (`pkg/vm/engine/tae/logtail/snapshot.go`)

修改 `GetSnapshot` 方法，支持传入额外的集群级别 snapshot（不落盘）：

```go
func (sm *SnapshotMeta) GetSnapshot(
    ctx context.Context,
    sid string,
    fs fileservice.FileService,
    mp *mpool.MPool,
    extraClusterTS ...types.TS, // 新增参数
) (*SnapshotInfo, error) {
    // ... 处理所有 snapshot 数据 ...
    
    // 添加额外的集群级别 snapshot（备份保护时间戳）
    for _, extraTS := range extraClusterTS {
        if !extraTS.IsEmpty() {
            snapshotInfo.cluster = append(snapshotInfo.cluster, extraTS)
        }
    }
    
    // 排序集群 snapshots（包含额外的 TS）
    sort.Slice(snapshotInfo.cluster, func(i, j int) bool {
        return snapshotInfo.cluster[i].LT(&snapshotInfo.cluster[j])
    })
    
    // ...
}
```

这样，`ObjectIsSnapshotRefers` 在检查对象是否被 snapshot 引用时，会考虑这个保护时间戳，从而保护在备份时间点存在的对象（`createTS <= protectedTS < dropTS`）。

### 4. 命令处理 (`pkg/vm/engine/tae/rpc/handle_debug.go`)

在 `HandleDiskCleaner` 中处理备份保护命令：

```go
func HandleDiskCleaner(ctx context.Context, db *DB, cmd string, args ...string) ([]byte, error) {
    // ...
    if strings.HasPrefix(cmd, "add_checker.backup.") {
        // 解析时间戳
        value := strings.TrimPrefix(cmd, "add_checker.backup.")
        protectedTS := types.StringToTS(value)
        
        cleaner := db.DiskCleaner.GetCleaner()
        _, _, isActive := cleaner.GetBackupProtection()
        if isActive {
            cleaner.UpdateBackupProtection(protectedTS)
        } else {
            cleaner.SetBackupProtection(protectedTS)
        }
        return []byte("backup protection set"), nil
    } else if cmd == "remove_checker.backup." {
        cleaner := db.DiskCleaner.GetCleaner()
        cleaner.RemoveBackupProtection()
        return []byte("backup protection removed"), nil
    }
    // ...
}
```

### 5. 接口定义 (`pkg/vm/engine/tae/db/gc/v3/types.go`)

在 `Cleaner` 接口中添加了备份保护方法：

```go
type Cleaner interface {
    // ... 其他方法 ...
    SetBackupProtection(protectedTS types.TS)
    UpdateBackupProtection(protectedTS types.TS)
    RemoveBackupProtection()
    GetBackupProtection() (protectedTS types.TS, lastUpdateTime time.Time, isActive bool)
}
```

## 保护范围

### 1. Checkpoint 文件保护

- **保护条件**：`endTS <= protectedTS` 的 checkpoint 文件不会被删除
- **实现位置**：`checkBackupProtection`、`filterCheckpoints`、`mergeCheckpointFilesLocked`

### 2. 元数据文件保护

- **保护条件**：`thisTS <= protectedTS` 的元数据文件不会被删除
- **实现位置**：`deleteStaleSnapshotFilesLocked`、`tryGCAgainstGCKPLocked`

### 3. 数据对象保护

- **保护条件**：`createTS <= protectedTS < dropTS` 的对象不会被删除
- **实现方式**：通过将 `protectedTS` 作为集群级别 snapshot 传入 `GetSnapshot`，`ObjectIsSnapshotRefers` 会检查这个时间戳

## 工作流程

1. **备份开始**：
   - 备份进程确定保护时间戳（`start` 或 `baseTS`）
   - 通过 `mo_ctl` 命令通知 GC 设置保护
   - 启动定时器，每 5 分钟更新一次保护时间戳

2. **备份进行中**：
   - GC 在 `Process` 中检查保护状态
   - 如果保护过期（20分钟未更新），自动移除保护
   - 在删除 checkpoint、元数据文件和数据对象时，检查保护时间戳
   - 被保护的文件和对象不会被删除

3. **备份完成**：
   - 备份进程停止定时器
   - 通过 `mo_ctl` 命令通知 GC 移除保护
   - GC 恢复正常工作，可以删除之前被保护的数据

## 关键特性

1. **单一时间戳保护**：只使用一个保护时间戳，简单高效
2. **跨节点通信**：通过 `mo_ctl` 实现备份进程和 GC 进程的通信
3. **自动过期**：20分钟未更新自动移除保护，防止备份进程异常导致 GC 永久阻塞
4. **定期更新**：每 5 分钟更新一次，确保 GC 知道备份仍在进行
5. **全面保护**：保护 checkpoint 文件、元数据文件和数据对象
6. **元数据文件优化**：只复制备份时间点之前的元数据文件，避免复制不必要的文件

## 测试建议

1. **正常备份流程**：验证备份过程中数据不被 GC 删除
2. **长时间备份**：验证保护时间戳定期更新机制
3. **备份异常**：验证 20 分钟后保护自动移除
4. **元数据文件**：验证只复制必要的 checkpoint 文件
5. **数据一致性**：验证备份后的数据可以正确恢复

## 相关文件

- `pkg/backup/tae.go` - 备份逻辑，设置/更新/移除保护
- `pkg/vm/engine/tae/db/gc/v3/checkpoint.go` - GC 逻辑，实现保护检查
- `pkg/vm/engine/tae/db/gc/v3/types.go` - Cleaner 接口定义
- `pkg/vm/engine/tae/db/gc/v3/mock_cleaner.go` - Mock 实现
- `pkg/vm/engine/tae/rpc/handle_debug.go` - mo_ctl 命令处理
- `pkg/vm/engine/tae/logtail/snapshot.go` - Snapshot 管理，支持额外 cluster snapshot
- `pkg/sql/plan/function/ctl/cmd_disk_cleaner.go` - mo_ctl 命令验证
- `pkg/vm/engine/cmd_util/type.go` - 常量定义

## 总结

备份保护机制通过时间戳保护、跨节点通信、自动过期和定期更新等特性，确保了备份过程中数据不会被 GC 删除，同时避免了备份进程异常导致的 GC 阻塞问题。实现简洁高效，保护范围全面，包括 checkpoint 文件、元数据文件和数据对象。

