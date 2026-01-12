# 可靠性问题

## 1. 数据一致性问题

### 1.1 🔴 备份保护设置失败后继续备份

**位置**: `tae.go:backupProtectionManager.start()`

```go
func (mgr *backupProtectionManager) start(protectedTS types.TS) {
    // ...
    _, err := mgr.exec.Exec(mgr.ctx, sql, mgr.opts)
    if err != nil {
        logutil.Errorf("backup: failed to set backup protection: %v", err)
        // Continue backup even if protection setup fails, but log the error
        return  // 问题：保护失败后继续备份
    }
    // ...
}
```

**问题**:
- 如果 GC 保护设置失败，备份仍然继续
- GC 可能在备份过程中删除需要的文件
- 导致备份数据不完整或损坏

**建议**:
```go
func (mgr *backupProtectionManager) start(protectedTS types.TS) error {
    // ...
    _, err := mgr.exec.Exec(mgr.ctx, sql, mgr.opts)
    if err != nil {
        return moerr.NewInternalError(mgr.ctx, 
            "failed to set backup protection: %v", err)
    }
    // ...
    return nil
}

// 调用处
if err := protectionMgr.start(protectedTS); err != nil {
    return err  // 保护失败则中止备份
}
```

### 1.2 🔴 文件复制失败时静默跳过

**位置**: `tae.go:parallelCopyData()`

```go
if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
    // TODO: handle file not found, maybe GC
    fileMutex.Lock()
    skipCount++
    fileMutex.Unlock()
    return &tasks.JobResult{Res: nil}  // 问题：静默跳过
}
```

**问题**:
- 文件不存在时只增加计数，不报错
- 可能是 GC 删除了文件，说明保护失效
- 备份数据可能不完整

**建议**:
```go
if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
    // 文件被 GC 删除，这是严重错误
    logutil.Error("backup: file not found, possibly deleted by GC",
        zap.String("file", name))
    errC <- moerr.NewInternalError(context.Background(),
        "file %s not found, backup protection may have failed", name)
    return &tasks.JobResult{
        Err: err,
        Res: nil,
    }
}
```

### 1.3 🟠 Checkpoint 重写错误处理不当

**位置**: `tae.go:execBackup()`

```go
cnLocation, tnLocation, checkpointFiles, err = logtail.ReWriteCheckpointAndBlockFromKey(...)
for _, name := range checkpointFiles {
    dentry, err := dstFs.StatFile(ctx, name)
    if err != nil {
        return err
    }
    // ...
}
if err != nil {  // 问题：这个 err 检查位置错误
    return err
}
```

**问题**:
- `err` 检查在循环之后，但 `ReWriteCheckpointAndBlockFromKey` 的错误应该立即检查
- 如果重写失败，循环中的 `checkpointFiles` 可能是无效的

**建议**:
```go
cnLocation, tnLocation, checkpointFiles, err = logtail.ReWriteCheckpointAndBlockFromKey(...)
if err != nil {
    return err  // 立即检查错误
}
for _, name := range checkpointFiles {
    // ...
}
```

## 2. 容错问题

### 2.1 🔴 备份中断无法恢复

**现状**:
- 备份中断后需要重新开始
- 已复制的文件无法复用
- 大数据量备份风险高

**建议**:
```go
type BackupCheckpoint struct {
    BackupID      string
    Phase         BackupPhase
    CompletedFiles map[string]bool
    LastFileIndex int
    Timestamp     time.Time
}

func (b *BackupCheckpoint) Save(ctx context.Context, fs fileservice.FileService) error
func LoadBackupCheckpoint(ctx context.Context, fs fileservice.FileService, id string) (*BackupCheckpoint, error)

// 恢复备份
func ResumeBackup(ctx context.Context, checkpoint *BackupCheckpoint, config *Config) error {
    // 从断点继续
}
```

### 2.2 🟠 网络故障处理不足

**现状**:
- 只有简单的重试机制
- 没有指数退避
- 没有断路器

**建议**:
```go
type RetryConfig struct {
    MaxRetries     int
    InitialBackoff time.Duration
    MaxBackoff     time.Duration
    Multiplier     float64
}

func CopyFileWithRetry(ctx context.Context, srcFs, dstFs fileservice.FileService,
    name string, config RetryConfig) ([]byte, error) {
    
    backoff := config.InitialBackoff
    for i := 0; i < config.MaxRetries; i++ {
        checksum, err := CopyFile(ctx, srcFs, dstFs, name, "")
        if err == nil {
            return checksum, nil
        }
        
        if !isRetryable(err) {
            return nil, err
        }
        
        select {
        case <-ctx.Done():
            return nil, ctx.Err()
        case <-time.After(backoff):
            backoff = time.Duration(float64(backoff) * config.Multiplier)
            if backoff > config.MaxBackoff {
                backoff = config.MaxBackoff
            }
        }
    }
    return nil, ErrMaxRetriesExceeded
}
```

### 2.3 🟠 存储空间检查缺失

**现状**:
- 备份前不检查目标存储空间
- 可能在备份中途因空间不足失败

**建议**:
```go
func checkStorageSpace(ctx context.Context, dstFs fileservice.FileService, 
    estimatedSize int64) error {
    
    // 获取可用空间
    available, err := dstFs.GetAvailableSpace(ctx)
    if err != nil {
        logutil.Warn("cannot check available space", zap.Error(err))
        return nil  // 无法检查时继续
    }
    
    // 预留 10% 余量
    required := int64(float64(estimatedSize) * 1.1)
    if available < required {
        return moerr.NewInternalError(ctx,
            "insufficient storage space: available %d, required %d",
            available, required)
    }
    return nil
}
```

## 3. 并发安全问题

### 3.1 🟠 多备份并发冲突

**现状**:
- 没有防止多个备份同时运行
- 可能导致资源竞争和数据不一致

**建议**:
```go
var backupLock sync.Mutex
var activeBackup *BackupState

func Backup(ctx context.Context, ...) error {
    backupLock.Lock()
    if activeBackup != nil {
        backupLock.Unlock()
        return moerr.NewInternalError(ctx, 
            "another backup is in progress: %s", activeBackup.ID)
    }
    activeBackup = &BackupState{ID: uuid.New().String(), StartTime: time.Now()}
    backupLock.Unlock()
    
    defer func() {
        backupLock.Lock()
        activeBackup = nil
        backupLock.Unlock()
    }()
    
    // 执行备份
}
```

### 3.2 🟡 保护更新 goroutine 泄漏风险

**位置**: `tae.go:backupProtectionManager`

```go
func (mgr *backupProtectionManager) updateLoop() {
    for {
        select {
        case <-mgr.updateTicker.C:
            mgr.updateProtection()  // 如果这里阻塞很久
        case <-mgr.updateStopChan:
            return
        case <-mgr.ctx.Done():
            return
        }
    }
}
```

**问题**:
- 如果 `updateProtection()` 阻塞，goroutine 无法及时退出
- `cleanup()` 可能等待很久

**建议**:
```go
func (mgr *backupProtectionManager) updateProtection() {
    ctx, cancel := context.WithTimeout(mgr.ctx, 30*time.Second)
    defer cancel()
    
    sql := buildBackupProtectionSQL(mgr.protectedTS)
    _, err := mgr.exec.Exec(ctx, sql, mgr.opts)
    // ...
}
```

## 4. 数据完整性问题

### 4.1 🟠 校验和验证不完整

**现状**:
- 写入时计算校验和
- 但没有在备份完成后验证

**建议**:
```go
func verifyBackup(ctx context.Context, fs fileservice.FileService, 
    files []*taeFile) error {
    
    for _, file := range files {
        if !file.needCopy {
            continue
        }
        
        data, err := readFile(ctx, fs, file.path)
        if err != nil {
            return err
        }
        
        hash := sha256.Sum256(data)
        if !bytes.Equal(hash[:], file.checksum) {
            return moerr.NewInternalError(ctx,
                "checksum mismatch for file %s", file.path)
        }
    }
    return nil
}
```

### 4.2 🟡 元数据和数据不一致风险

**现状**:
- 先复制数据，后保存元数据
- 如果保存元数据失败，数据已复制但无法使用

**建议**:
- 使用两阶段提交
- 或者先写临时元数据，最后重命名

```go
func saveMetas(ctx context.Context, cfg *Config) error {
    // 写入临时文件
    tempFile := moMeta + ".tmp"
    err := writeFile(ctx, cfg.GeneralDir, tempFile, []byte(metas))
    if err != nil {
        return err
    }
    
    // 原子重命名
    return cfg.GeneralDir.Rename(ctx, tempFile, moMeta)
}
```

## 5. 超时处理问题

### 5.1 🟠 缺少全局超时

**现状**:
- 备份没有全局超时限制
- 可能无限期运行

**建议**:
```go
func Backup(ctx context.Context, ...) error {
    // 设置默认超时
    if _, ok := ctx.Deadline(); !ok {
        var cancel context.CancelFunc
        ctx, cancel = context.WithTimeout(ctx, 24*time.Hour)
        defer cancel()
    }
    // ...
}
```

### 5.2 🟡 单文件复制无超时

**现状**:
```go
func CopyFile(ctx context.Context, ...) ([]byte, error) {
    // 没有单文件超时
}
```

**建议**:
```go
func CopyFile(ctx context.Context, ...) ([]byte, error) {
    ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
    defer cancel()
    // ...
}
```
