# 备份保护机制

## 1. 概述

备份保护机制确保在备份过程中，GC（垃圾回收）不会删除备份所需的数据文件。这是保证备份数据一致性的关键。

## 2. 保护机制架构

```
┌─────────────────────────────────────────────────────────────┐
│                    Backup Process                            │
│                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐   │
│  │    Start     │───▶│   Protect    │───▶│    Copy      │   │
│  │   Backup     │    │   (mo_ctl)   │    │    Data      │   │
│  └──────────────┘    └──────────────┘    └──────────────┘   │
│                             │                    │           │
│                             ▼                    ▼           │
│                      ┌──────────────┐    ┌──────────────┐   │
│                      │   Update     │    │   Cleanup    │   │
│                      │  (5 min)     │    │  Protection  │   │
│                      └──────────────┘    └──────────────┘   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    GC (Cleaner)                              │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │              backupProtection                         │   │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────────┐  │   │
│  │  │ protectedTS│  │lastUpdate  │  │   isActive     │  │   │
│  │  └────────────┘  └────────────┘  └────────────────┘  │   │
│  └──────────────────────────────────────────────────────┘   │
│                              │                               │
│                              ▼                               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │         checkBackupProtection()                       │   │
│  │    - 检查 Checkpoint 是否受保护                       │   │
│  │    - 阻止删除保护时间点之前的数据                     │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

## 3. 备份端保护管理器

### 3.1 数据结构

```go
type backupProtectionManager struct {
    ctx            context.Context
    exec           executor.SQLExecutor
    opts           executor.Options
    protectedTS    types.TS           // 保护的时间戳
    updateTicker   *time.Ticker       // 更新定时器
    updateStopChan chan struct{}      // 停止信号
    protectionSet  bool               // 保护是否已设置
}
```

### 3.2 创建管理器

```go
func newBackupProtectionManager(
    ctx context.Context, 
    exec executor.SQLExecutor, 
    opts executor.Options,
) *backupProtectionManager {
    return &backupProtectionManager{
        ctx:            ctx,
        exec:           exec,
        opts:           opts,
        updateStopChan: make(chan struct{}),
    }
}
```

### 3.3 启动保护

```go
func (mgr *backupProtectionManager) start(protectedTS types.TS) {
    mgr.protectedTS = protectedTS
    
    // 通过 mo_ctl 设置保护
    sql := buildBackupProtectionSQL(protectedTS)
    _, err := mgr.exec.Exec(mgr.ctx, sql, mgr.opts)
    
    mgr.protectionSet = true
    
    // 启动定期更新
    mgr.updateTicker = time.NewTicker(backupProtectionUpdateInterval)
    go mgr.updateLoop()
}
```

### 3.4 定期更新

```go
func (mgr *backupProtectionManager) updateLoop() {
    for {
        select {
        case <-mgr.updateTicker.C:
            mgr.updateProtection()
        case <-mgr.updateStopChan:
            return
        case <-mgr.ctx.Done():
            return
        }
    }
}

func (mgr *backupProtectionManager) updateProtection() {
    sql := buildBackupProtectionSQL(mgr.protectedTS)
    mgr.exec.Exec(mgr.ctx, sql, mgr.opts)
}
```

### 3.5 清理保护

```go
func (mgr *backupProtectionManager) cleanup() {
    // 先关闭通道，通知 goroutine 退出
    if mgr.updateTicker != nil {
        close(mgr.updateStopChan)
        mgr.updateTicker.Stop()
    }
    
    // 移除保护
    if mgr.protectionSet && mgr.exec != nil {
        sql := buildRemoveBackupProtectionSQL()
        mgr.exec.Exec(mgr.ctx, sql, mgr.opts)
    }
}
```

## 4. GC 端保护实现

### 4.1 保护状态存储

```go
type checkpointCleaner struct {
    // ... 其他字段
    
    backupProtection struct {
        sync.RWMutex
        protectedTS    types.TS      // 保护的时间戳
        lastUpdateTime time.Time     // 最后更新时间
        isActive       bool          // 是否激活
    }
    
    mutation struct {
        // ... 其他字段
        
        // GC 开始时的保护快照
        backupProtectionSnapshot struct {
            protectedTS types.TS
            isActive    bool
        }
    }
}
```

### 4.2 设置保护

```go
func (c *checkpointCleaner) SetBackupProtection(protectedTS types.TS) {
    c.backupProtection.Lock()
    defer c.backupProtection.Unlock()
    
    c.backupProtection.protectedTS = protectedTS
    c.backupProtection.lastUpdateTime = time.Now()
    c.backupProtection.isActive = true
}
```

### 4.3 更新保护

```go
func (c *checkpointCleaner) UpdateBackupProtection(protectedTS types.TS) {
    c.backupProtection.Lock()
    defer c.backupProtection.Unlock()
    
    if !c.backupProtection.isActive {
        return
    }
    
    c.backupProtection.protectedTS = protectedTS
    c.backupProtection.lastUpdateTime = time.Now()
}
```

### 4.4 移除保护

```go
func (c *checkpointCleaner) RemoveBackupProtection() {
    c.backupProtection.Lock()
    defer c.backupProtection.Unlock()
    
    c.backupProtection.isActive = false
    c.backupProtection.protectedTS = types.TS{}
}
```

### 4.5 检查保护

```go
func (c *checkpointCleaner) checkBackupProtection(item any) bool {
    // 使用 GC 开始时的快照
    protectedTS, isActive := c.getBackupProtectionSnapshot()
    
    if !isActive {
        return true  // 允许 GC
    }
    
    ckp, ok := item.(*checkpoint.CheckpointEntry)
    if !ok {
        return true
    }
    
    endTS := ckp.GetEnd()
    if endTS.IsEmpty() {
        return true
    }
    
    // 保护时间点之前的 Checkpoint 不能被 GC
    if endTS.LE(&protectedTS) {
        return false  // 阻止 GC
    }
    
    return true
}
```

## 5. mo_ctl 命令

### 5.1 设置保护命令

```go
func buildBackupProtectionSQL(protectedTS types.TS) string {
    tsValue := protectedTS.ToString()
    return fmt.Sprintf(
        "select mo_ctl('dn','DiskCleaner','%s%s')", 
        backupProtectionCmdPrefix, tsValue,
    )
}
```

### 5.2 移除保护命令

```go
func buildRemoveBackupProtectionSQL() string {
    return fmt.Sprintf(
        "select mo_ctl('dn','DiskCleaner','%s')", 
        backupProtectionRemoveCmd,
    )
}
```

### 5.3 命令处理

```go
func (h *Handle) HandleDiskCleaner(...) {
    switch op {
    case cmd_util.RemoveChecker:
        if key == cmd_util.CheckerKeyBackup {
            h.db.DiskCleaner.GetCleaner().RemoveBackupProtection()
            return nil, nil
        }
    case cmd_util.AddChecker:
        // ...
    }
    
    switch key {
    case cmd_util.CheckerKeyBackup:
        ts := types.StringToTS(value)
        cleaner := h.db.DiskCleaner.GetCleaner()
        _, _, isActive := cleaner.GetBackupProtection()
        if isActive {
            cleaner.UpdateBackupProtection(ts)
        } else {
            cleaner.SetBackupProtection(ts)
        }
    }
}
```

## 6. GC 一致性保证

### 6.1 快照机制

GC 开始时创建保护状态的快照：

```go
// 在 Process() 中
c.backupProtection.Lock()
c.mutation.backupProtectionSnapshot.protectedTS = c.backupProtection.protectedTS
c.mutation.backupProtectionSnapshot.isActive = c.backupProtection.isActive
c.backupProtection.Unlock()
```

### 6.2 使用快照

```go
func (c *checkpointCleaner) getBackupProtectionSnapshot() (types.TS, bool) {
    return c.mutation.backupProtectionSnapshot.protectedTS,
           c.mutation.backupProtectionSnapshot.isActive
}
```

这确保了：
- GC 执行期间使用一致的保护状态
- 备份期间的保护更新不会影响正在进行的 GC
- 避免竞态条件

## 7. 保护时间线

```
T0: 备份开始
    │
T1: 设置保护 (protectedTS = T0)
    │
T2: GC 开始，创建快照
    │  - snapshot.protectedTS = T0
    │  - snapshot.isActive = true
    │
T3: 备份更新保护 (protectedTS = T0)
    │  - 不影响正在进行的 GC
    │
T4: GC 检查 Checkpoint
    │  - 使用 snapshot.protectedTS
    │  - 保护 T0 之前的数据
    │
T5: GC 完成
    │
T6: 备份完成
    │
T7: 移除保护
```

## 8. 常量定义

```go
const (
    // 保护更新间隔
    backupProtectionUpdateInterval = 5 * time.Minute
    
    // mo_ctl 命令前缀
    backupProtectionCmdPrefix = "add_checker.backup."
    backupProtectionRemoveCmd = "remove_checker.backup."
)
```
