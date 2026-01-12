# 优化建议汇总

## 1. 问题优先级矩阵

| 问题 | 严重性 | 修复难度 | 优先级 |
|-----|-------|---------|-------|
| 备份保护失败后继续备份 | 🔴 高 | 低 | P0 |
| 文件复制失败静默跳过 | 🔴 高 | 低 | P0 |
| 备份中断无法恢复 | 🔴 高 | 高 | P1 |
| defer 在循环中 | 🟠 中 | 低 | P1 |
| 缺少监控指标 | 🟠 中 | 中 | P1 |
| 并行度不够智能 | 🟠 中 | 中 | P2 |
| 小文件效率低 | 🟠 中 | 高 | P2 |
| 配置文件错误被忽略 | 🟡 低 | 低 | P2 |
| 缺少备份压缩 | 🟡 低 | 高 | P3 |
| 缺少备份加密 | 🟡 低 | 高 | P3 |

## 2. 短期优化方案 (1-2 周)

### 2.1 修复关键错误处理

```go
// 1. 备份保护失败时中止备份
func (mgr *backupProtectionManager) start(protectedTS types.TS) error {
    sql := buildBackupProtectionSQL(protectedTS)
    _, err := mgr.exec.Exec(mgr.ctx, sql, mgr.opts)
    if err != nil {
        return moerr.NewInternalError(mgr.ctx, 
            "failed to set backup protection: %v", err)
    }
    mgr.protectionSet = true
    // ...
    return nil
}

// 2. 文件不存在时报错
if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
    return nil, moerr.NewInternalError(ctx,
        "file %s not found, backup protection may have failed", name)
}

// 3. 修复 defer 在循环中的问题
for _, metaFile := range metaFiles {
    window := gc.NewGCWindow(common.DebugAllocator, srcFs)
    err = window.ReadTable(ctx, metaFile.GetGCFullName(), srcFs)
    if err != nil {
        window.Close()
        return nil, err
    }
    // 处理完成后立即关闭
    objects := window.GetObjectStats()
    // ... 处理
    window.Close()
}
```

### 2.2 添加基础监控指标

```go
var (
    backupDuration = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Namespace: "mo",
            Subsystem: "backup",
            Name:      "duration_seconds",
            Help:      "Backup duration in seconds",
            Buckets:   prometheus.ExponentialBuckets(1, 2, 20),
        },
        []string{"type", "status"},
    )
    
    backupFilesCopied = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Namespace: "mo",
            Subsystem: "backup",
            Name:      "files_copied_total",
            Help:      "Total number of files copied",
        },
        []string{"type"},
    )
    
    backupBytesCopied = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Namespace: "mo",
            Subsystem: "backup",
            Name:      "bytes_copied_total",
            Help:      "Total bytes copied",
        },
        []string{"type"},
    )
    
    backupErrors = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Namespace: "mo",
            Subsystem: "backup",
            Name:      "errors_total",
            Help:      "Total backup errors",
        },
        []string{"type", "error_type"},
    )
)

func init() {
    prometheus.MustRegister(backupDuration)
    prometheus.MustRegister(backupFilesCopied)
    prometheus.MustRegister(backupBytesCopied)
    prometheus.MustRegister(backupErrors)
}
```

### 2.3 改进错误信息

```go
// 添加上下文信息
func wrapError(ctx context.Context, err error, operation string, 
    details map[string]interface{}) error {
    
    if err == nil {
        return nil
    }
    
    msg := fmt.Sprintf("%s failed", operation)
    for k, v := range details {
        msg += fmt.Sprintf(", %s=%v", k, v)
    }
    
    return moerr.NewInternalError(ctx, "%s: %v", msg, err)
}

// 使用示例
if err != nil {
    return wrapError(ctx, err, "copy file", map[string]interface{}{
        "source": srcPath,
        "dest":   dstPath,
        "size":   size,
    })
}
```

## 3. 中期优化方案 (1-2 月)

### 3.1 实现断点续传

```go
type BackupCheckpoint struct {
    BackupID       string                 `json:"backup_id"`
    StartTime      time.Time              `json:"start_time"`
    Phase          BackupPhase            `json:"phase"`
    CompletedFiles map[string]FileStatus  `json:"completed_files"`
    TotalFiles     int                    `json:"total_files"`
    TotalBytes     int64                  `json:"total_bytes"`
    CopiedBytes    int64                  `json:"copied_bytes"`
}

type FileStatus struct {
    Checksum  string    `json:"checksum"`
    Size      int64     `json:"size"`
    CopiedAt  time.Time `json:"copied_at"`
}

func (c *BackupCheckpoint) Save(ctx context.Context, fs fileservice.FileService) error {
    data, err := json.Marshal(c)
    if err != nil {
        return err
    }
    return writeFile(ctx, fs, ".backup_checkpoint", data)
}

func LoadBackupCheckpoint(ctx context.Context, fs fileservice.FileService) (*BackupCheckpoint, error) {
    data, err := readFile(ctx, fs, ".backup_checkpoint")
    if err != nil {
        if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
            return nil, nil
        }
        return nil, err
    }
    
    var checkpoint BackupCheckpoint
    if err := json.Unmarshal(data, &checkpoint); err != nil {
        return nil, err
    }
    return &checkpoint, nil
}

func ResumeBackup(ctx context.Context, checkpoint *BackupCheckpoint, 
    config *Config) error {
    
    // 跳过已完成的文件
    for _, file := range files {
        if status, ok := checkpoint.CompletedFiles[file.path]; ok {
            file.needCopy = false
            file.checksum = status.Checksum
        }
    }
    
    // 继续备份
    return execBackup(ctx, ...)
}
```

### 3.2 优化并行复制

```go
type AdaptiveParallelCopier struct {
    minParallel    int
    maxParallel    int
    currentParallel int
    
    // 性能指标
    throughput     float64
    errorRate      float64
    
    mu sync.Mutex
}

func (c *AdaptiveParallelCopier) adjustParallel() {
    c.mu.Lock()
    defer c.mu.Unlock()
    
    // 根据吞吐量和错误率调整并行度
    if c.errorRate > 0.1 {
        // 错误率高，降低并行度
        c.currentParallel = max(c.minParallel, c.currentParallel/2)
    } else if c.throughput < targetThroughput && c.currentParallel < c.maxParallel {
        // 吞吐量低，增加并行度
        c.currentParallel = min(c.maxParallel, c.currentParallel*2)
    }
}

func (c *AdaptiveParallelCopier) Copy(ctx context.Context, 
    files []*objectio.BackupObject) ([]*taeFile, error) {
    
    // 使用 semaphore 控制并行度
    sem := semaphore.NewWeighted(int64(c.currentParallel))
    
    // 定期调整并行度
    go func() {
        ticker := time.NewTicker(10 * time.Second)
        defer ticker.Stop()
        for {
            select {
            case <-ctx.Done():
                return
            case <-ticker.C:
                c.adjustParallel()
                sem.Resize(int64(c.currentParallel))
            }
        }
    }()
    
    // ...
}
```

### 3.3 添加备份状态 API

```go
type BackupStatus struct {
    ID            string        `json:"id"`
    Type          string        `json:"type"`
    Status        string        `json:"status"`
    StartTime     time.Time     `json:"start_time"`
    EndTime       *time.Time    `json:"end_time,omitempty"`
    Progress      float64       `json:"progress"`
    FilesCopied   int64         `json:"files_copied"`
    TotalFiles    int64         `json:"total_files"`
    BytesCopied   int64         `json:"bytes_copied"`
    TotalBytes    int64         `json:"total_bytes"`
    Throughput    float64       `json:"throughput_mbps"`
    Error         string        `json:"error,omitempty"`
}

// SQL 接口
// SELECT * FROM mo_backup_status WHERE id = 'xxx';
// SELECT * FROM mo_backup_history ORDER BY start_time DESC LIMIT 10;
```

## 4. 长期优化方案 (3-6 月)

### 4.1 重构备份架构

```go
// 定义备份接口
type BackupEngine interface {
    // 获取需要备份的对象
    GetBackupObjects(ctx context.Context, ts types.TS) (ObjectIterator, error)
    
    // 创建一致性快照
    CreateSnapshot(ctx context.Context) (Snapshot, error)
    
    // 获取元数据
    GetMetadata(ctx context.Context) (*BackupMetadata, error)
}

// TAE 实现
type TAEBackupEngine struct {
    db *DB
}

// 备份执行器
type BackupExecutor struct {
    engine     BackupEngine
    storage    BackupStorage
    compressor Compressor
    encryptor  Encryptor
    reporter   ProgressReporter
}

func (e *BackupExecutor) Execute(ctx context.Context, config BackupConfig) error {
    // 1. 创建快照
    snapshot, err := e.engine.CreateSnapshot(ctx)
    if err != nil {
        return err
    }
    defer snapshot.Release()
    
    // 2. 获取对象列表
    objects, err := e.engine.GetBackupObjects(ctx, config.BackupTS)
    if err != nil {
        return err
    }
    
    // 3. 复制数据
    copier := NewParallelCopier(e.storage, config.Parallelism)
    if err := copier.Copy(ctx, objects, e.reporter); err != nil {
        return err
    }
    
    // 4. 保存元数据
    metadata, err := e.engine.GetMetadata(ctx)
    if err != nil {
        return err
    }
    return e.storage.SaveMetadata(ctx, metadata)
}
```

### 4.2 支持增量备份链

```go
type BackupChain struct {
    FullBackup    *BackupInfo
    Incrementals  []*BackupInfo
}

type BackupInfo struct {
    ID        string
    Type      BackupType
    BaseID    string  // 增量备份的基准
    StartTS   types.TS
    EndTS     types.TS
    Location  string
    Size      int64
    CreatedAt time.Time
}

func (c *BackupChain) Validate() error {
    // 验证备份链完整性
}

func (c *BackupChain) GetRestoreSequence() []*BackupInfo {
    // 返回恢复顺序
    result := []*BackupInfo{c.FullBackup}
    result = append(result, c.Incrementals...)
    return result
}
```

### 4.3 支持备份压缩和加密

```go
type BackupOptions struct {
    Compression CompressionConfig
    Encryption  EncryptionConfig
}

type CompressionConfig struct {
    Enabled   bool
    Algorithm string  // "gzip", "lz4", "zstd"
    Level     int
}

type EncryptionConfig struct {
    Enabled   bool
    Algorithm string  // "AES-256-GCM"
    KeySource string  // "kms", "file", "env"
    KeyID     string
}

func NewCompressedWriter(w io.Writer, config CompressionConfig) (io.WriteCloser, error) {
    switch config.Algorithm {
    case "gzip":
        return gzip.NewWriterLevel(w, config.Level)
    case "lz4":
        return lz4.NewWriter(w), nil
    case "zstd":
        return zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.EncoderLevel(config.Level)))
    default:
        return nil, fmt.Errorf("unsupported compression: %s", config.Algorithm)
    }
}
```

## 5. 实施路线图

```
Phase 1 (Week 1-2): 关键修复
├── 修复错误处理问题
├── 修复资源泄漏问题
└── 添加基础监控指标

Phase 2 (Week 3-4): 可靠性增强
├── 实现断点续传
├── 添加备份状态 API
└── 改进错误信息

Phase 3 (Month 2): 性能优化
├── 优化并行复制
├── 实现自适应并行度
└── 优化小文件处理

Phase 4 (Month 3-4): 架构重构
├── 抽象备份接口
├── 支持增量备份链
└── 添加压缩支持

Phase 5 (Month 5-6): 高级特性
├── 添加加密支持
├── 支持备份策略
└── 完善监控告警
```

## 6. 测试建议

### 6.1 单元测试

- 错误处理路径
- 边界条件
- 并发安全

### 6.2 集成测试

- 完整备份恢复流程
- 断点续传
- S3 备份

### 6.3 性能测试

- 不同数据量
- 不同并行度
- 网络延迟模拟

### 6.4 混沌测试

- 网络中断
- 磁盘故障
- 进程崩溃
