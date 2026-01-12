# 备份保护机制

## 1. 概述

备份保护机制用于在数据库备份期间暂停 GC 操作，防止备份所需的数据被误删除。这是保证备份一致性和完整性的关键功能。

## 2. 数据结构

```go
type checkpointCleaner struct {
    // ... 其他字段
    
    // 备份保护
    backupProtection struct {
        sync.RWMutex
        protectedTS    types.TS   // 保护的时间戳
        lastUpdateTime time.Time  // 最后更新时间
        isActive       bool       // 是否激活
    }
    
    mutation struct {
        // ... 其他字段
        
        // 备份保护快照（GC开始时创建）
        backupProtectionSnapshot struct {
            protectedTS types.TS
            isActive    bool
        }
    }
}
```

## 3. API 接口

### 3.1 设置备份保护

```go
func (c *checkpointCleaner) SetBackupProtection(protectedTS types.TS)
```

**功能**：
- 设置保护时间戳
- 记录更新时间
- 激活保护状态

**使用场景**：
- 备份开始时调用
- 传入备份的时间点

### 3.2 更新备份保护

```go
func (c *checkpointCleaner) UpdateBackupProtection(protectedTS types.TS)
```

**功能**：
- 更新保护时间戳
- 刷新更新时间
- 仅在保护激活时有效

**使用场景**：
- 长时间备份时定期更新
- 防止保护过期

### 3.3 移除备份保护

```go
func (c *checkpointCleaner) RemoveBackupProtection()
```

**功能**：
- 清除保护时间戳
- 停用保护状态

**使用场景**：
- 备份完成后调用
- 备份失败后清理

### 3.4 获取备份保护状态

```go
func (c *checkpointCleaner) GetBackupProtection() (
    protectedTS types.TS, 
    lastUpdateTime time.Time, 
    isActive bool,
)
```

**功能**：
- 返回当前保护状态
- 用于监控和诊断

## 4. 保护机制实现

### 4.1 GC 开始时的快照

```go
func (c *checkpointCleaner) Process(inputCtx context.Context, ...) error {
    // 获取锁
    c.StartMutationTask("gc-process")
    defer c.StopMutationTask()
    
    // 检查备份保护状态并创建快照
    c.backupProtection.Lock()
    
    // 检查保护是否过期（20分钟未更新）
    if c.backupProtection.isActive && 
       time.Since(c.backupProtection.lastUpdateTime) > 20*time.Minute {
        // 自动移除过期的保护
        c.backupProtection.isActive = false
        c.backupProtection.protectedTS = types.TS{}
    }
    
    // 创建快照
    c.mutation.backupProtectionSnapshot.protectedTS = c.backupProtection.protectedTS
    c.mutation.backupProtectionSnapshot.isActive = c.backupProtection.isActive
    isBackupActive := c.backupProtection.isActive
    protectedTS := c.backupProtection.protectedTS
    
    c.backupProtection.Unlock()
    
    // 如果备份保护激活，跳过所有 GC 操作
    if isBackupActive {
        return nil
    }
    
    // 继续正常 GC 流程...
}
```

### 4.2 快照的使用

```go
func (c *checkpointCleaner) getBackupProtectionSnapshot() (
    protectedTS types.TS, 
    isActive bool,
) {
    // 使用 mutation 中的快照（GC 开始时创建）
    // 无需加锁，因为 mutation 已经被锁定
    return c.mutation.backupProtectionSnapshot.protectedTS,
           c.mutation.backupProtectionSnapshot.isActive
}
```

### 4.3 Checkpoint 检查

```go
func (c *checkpointCleaner) checkBackupProtection(item any) bool {
    protectedTS, isActive := c.getBackupProtectionSnapshot()
    
    // 如果保护未激活，允许 GC
    if !isActive {
        return true
    }
    
    // 检查是否是 Checkpoint 条目
    ckp, ok := item.(*checkpoint.CheckpointEntry)
    if !ok {
        return true  // 非 Checkpoint 项允许 GC
    }
    
    // 检查 Checkpoint 的结束时间戳
    endTS := ckp.GetEnd()
    if endTS.IsEmpty() {
        return true  // 无效时间戳允许 GC
    }
    
    // 保护时间戳之前的 Checkpoint 不能 GC
    if endTS.LE(&protectedTS) {
        return false  // 阻止 GC
    }
    
    return true  // 允许 GC
}
```

## 5. 快照信息集成

### 5.1 获取快照时添加保护时间戳

```go
func (c *checkpointCleaner) GetSnapshotsLocked() (*logtail.SnapshotInfo, error) {
    var extraTS types.TS
    protectedTS, isActive := c.getBackupProtectionSnapshot()
    if isActive {
        extraTS = protectedTS
    }
    
    // 将保护时间戳作为额外的快照时间点
    return c.mutation.snapshotMeta.GetSnapshot(
        c.ctx, c.sid, c.fs, c.mp, extraTS,
    )
}
```

### 5.2 效果

- 保护时间戳被当作一个虚拟的快照时间点
- 该时间点之前创建、之后删除的对象会被保护
- 确保备份时间点的数据完整性

## 6. 保护过期机制

### 6.1 自动过期

```go
// 在 Process 方法中检查
if c.backupProtection.isActive && 
   time.Since(c.backupProtection.lastUpdateTime) > 20*time.Minute {
    // 保护已过期，自动移除
    c.backupProtection.isActive = false
    c.backupProtection.protectedTS = types.TS{}
}
```

### 6.2 过期时间

- 默认过期时间：20 分钟
- 需要定期调用 `UpdateBackupProtection` 刷新

### 6.3 设计原因

- 防止备份进程异常退出后保护永久生效
- 避免 GC 被永久阻塞
- 平衡数据安全和存储回收

## 7. 并发安全

### 7.1 锁设计

```
┌─────────────────────────────────────────────────────────────┐
│                      锁层次结构                              │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  backupProtection.RWMutex                                   │
│  - 保护 protectedTS, lastUpdateTime, isActive              │
│  - 读写分离，支持并发读取                                    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  mutation.Mutex                                             │
│  - 保护 GC 执行过程                                         │
│  - 包含 backupProtectionSnapshot                            │
└─────────────────────────────────────────────────────────────┘
```

### 7.2 一致性保证

1. **GC 开始时创建快照**
   - 在获取 mutation 锁后创建
   - 整个 GC 过程使用同一快照

2. **备份保护操作不阻塞 GC**
   - 只获取 backupProtection 锁
   - 不等待正在进行的 GC

3. **GC 不阻塞备份保护操作**
   - 使用快照而非实时状态
   - 备份保护可随时更新

## 8. 使用示例

### 8.1 备份流程

```go
// 1. 开始备份
cleaner.SetBackupProtection(backupTS)

// 2. 执行备份（可能需要较长时间）
for {
    // 定期更新保护
    cleaner.UpdateBackupProtection(backupTS)
    
    // 执行备份操作...
    if backupComplete {
        break
    }
    
    time.Sleep(5 * time.Minute)
}

// 3. 备份完成
cleaner.RemoveBackupProtection()
```

### 8.2 监控保护状态

```go
protectedTS, lastUpdate, isActive := cleaner.GetBackupProtection()
if isActive {
    log.Info("Backup protection active",
        "protectedTS", protectedTS,
        "lastUpdate", lastUpdate,
        "age", time.Since(lastUpdate))
}
```

## 9. 日志记录

### 9.1 设置保护

```
GC-Backup-Protection-Set
  protected-ts: xxx
  last-update-time: xxx
```

### 9.2 更新保护

```
GC-Backup-Protection-Updated
  protected-ts: xxx
  last-update-time: xxx
```

### 9.3 移除保护

```
GC-Backup-Protection-Removed
```

### 9.4 过期移除

```
GC-Backup-Protection-Expired-Remove
  time-since-update: xxx
```

### 9.5 跳过 GC

```
GC-Backup-Protection-Skip-All-GC
  task: xxx
  protected-ts: xxx
```

### 9.6 阻止 Checkpoint GC

```
GC-Backup-Protection-Block-Checkpoint
  checkpoint-end-ts: xxx
  protected-ts: xxx
  checkpoint: xxx
```

## 10. 注意事项

1. **及时更新保护**
   - 长时间备份需要定期更新
   - 建议每 5-10 分钟更新一次

2. **及时移除保护**
   - 备份完成后立即移除
   - 避免阻塞 GC 过长时间

3. **监控保护状态**
   - 定期检查保护是否意外激活
   - 监控保护时长

4. **错误处理**
   - 备份失败时也要移除保护
   - 使用 defer 确保清理
