# 架构设计问题

## 1. 模块耦合问题

### 1.1 🟠 备份模块与 TAE 强耦合

**现状**:
```go
// tae.go
import (
    "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/gc/v3"
    "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
    "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
    // ...
)
```

**问题**:
- backup 包直接依赖 TAE 内部实现
- 难以支持其他存储引擎
- TAE 内部变更可能破坏备份功能

**建议**:
```go
// 定义备份接口
type BackupSource interface {
    GetCheckpoints() ([]CheckpointInfo, error)
    GetObjects(checkpoint CheckpointInfo) ([]ObjectInfo, error)
    ForceCheckpoint(ctx context.Context, ts types.TS) (string, error)
}

// TAE 实现该接口
type TAEBackupSource struct {
    db *DB
}

func (s *TAEBackupSource) GetCheckpoints() ([]CheckpointInfo, error) {
    // ...
}
```

### 1.2 🟠 通过 SQL 执行备份命令

**现状**:
```go
sql := "select mo_ctl('dn','Backup','')"
res, err := exec.Exec(ctx, sql, opts)
```

**问题**:
- 备份依赖 SQL 执行器
- 增加了不必要的复杂性
- 错误信息通过 JSON 传递，解析复杂

**建议**:
- 提供直接的 RPC 接口
- 或者使用专门的备份服务

## 2. 状态管理问题

### 2.1 🔴 缺少备份状态持久化

**现状**:
- 备份进度只在内存中
- 备份中断后无法恢复
- 无法查询历史备份状态

**建议**:
```go
type BackupState struct {
    ID           string
    StartTime    time.Time
    Status       BackupStatus  // Running, Completed, Failed, Cancelled
    Progress     float64
    FilesCopied  int64
    BytesCopied  int64
    TotalFiles   int64
    TotalBytes   int64
    Error        string
}

// 持久化到文件或数据库
func (s *BackupState) Save(ctx context.Context, fs fileservice.FileService) error
func LoadBackupState(ctx context.Context, fs fileservice.FileService, id string) (*BackupState, error)
```

### 2.2 🟠 备份保护状态分散

**现状**:
```
备份端: backupProtectionManager
   │
   ▼ (通过 SQL)
GC 端: checkpointCleaner.backupProtection
```

**问题**:
- 状态分散在两个地方
- 通过 SQL 同步，可能不一致
- 备份端崩溃后 GC 端保护可能残留

**建议**:
- 使用分布式锁或租约机制
- 保护状态应该有 TTL
- 定期心跳更新

## 3. 接口设计问题

### 3.1 🟡 Config 结构体职责过多

**现状**:
```go
type Config struct {
    Timestamp   types.TS
    GeneralDir  fileservice.FileService
    SharedFs    fileservice.FileService
    TaeDir      fileservice.FileService
    HAkeeper    logservice.BRHAKeeperClient
    Metas       *Metas
    Parallelism uint16
    BackupType  string
    BackupTs    types.TS
}
```

**问题**:
- 混合了配置、状态和依赖
- 难以测试和模拟

**建议**:
```go
// 分离配置
type BackupConfig struct {
    Parallelism uint16
    BackupType  string
    BackupTs    types.TS
}

// 分离依赖
type BackupDependencies struct {
    SrcFs    fileservice.FileService
    DstFs    fileservice.FileService
    HAkeeper logservice.BRHAKeeperClient
}

// 分离状态
type BackupContext struct {
    Config       BackupConfig
    Dependencies BackupDependencies
    Metas        *Metas
}
```

### 3.2 🟡 函数参数过多

**现状**:
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

**问题**:
- 参数过多，难以理解和维护
- 容易传错参数

**建议**:
```go
type ExecBackupParams struct {
    Ctx           context.Context
    SID           string
    SrcFs         fileservice.FileService
    DstFs         fileservice.FileService
    Checkpoints   []string
    Parallelism   int
    BackupTS      types.TS
    BackupType    string
    ProtectionMgr *backupProtectionManager
}

func execBackup(params ExecBackupParams) ([]taeFile, error)
```

## 4. 扩展性问题

### 4.1 🟠 不支持备份策略

**现状**:
- 只支持全量和增量两种模式
- 无法配置备份策略

**建议**:
```go
type BackupStrategy interface {
    ShouldBackup(object ObjectInfo) bool
    GetRetentionPolicy() RetentionPolicy
}

type FullBackupStrategy struct{}
type IncrementalBackupStrategy struct {
    BaseTS types.TS
}
type DifferentialBackupStrategy struct {
    LastFullBackupTS types.TS
}
```

### 4.2 🟠 不支持备份压缩

**现状**:
- 数据直接复制，无压缩
- 对于大数据量，存储和传输成本高

**建议**:
```go
type CompressionConfig struct {
    Enabled     bool
    Algorithm   string  // "gzip", "lz4", "zstd"
    Level       int
}

func CopyFileWithCompression(ctx context.Context, srcFs, dstFs fileservice.FileService,
    name string, compression CompressionConfig) ([]byte, error)
```

### 4.3 🟡 不支持备份加密

**现状**:
- 备份数据明文存储
- 不满足安全合规要求

**建议**:
```go
type EncryptionConfig struct {
    Enabled   bool
    Algorithm string  // "AES-256-GCM"
    KeySource string  // "kms", "file", "env"
    KeyID     string
}
```

## 5. 可观测性问题

### 5.1 🟠 缺少结构化指标

**现状**:
- 只有日志输出
- 无法集成监控系统

**建议**:
```go
var (
    backupDuration = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Name: "mo_backup_duration_seconds",
            Help: "Backup duration in seconds",
        },
        []string{"type", "status"},
    )
    
    backupFilesCopied = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "mo_backup_files_copied_total",
            Help: "Total number of files copied during backup",
        },
        []string{"type"},
    )
    
    backupBytesCopied = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "mo_backup_bytes_copied_total",
            Help: "Total bytes copied during backup",
        },
        []string{"type"},
    )
)
```

### 5.2 🟡 缺少追踪支持

**现状**:
- 无法追踪备份请求的完整链路

**建议**:
- 集成 OpenTelemetry
- 添加 span 和 trace

## 6. 版本兼容性问题

### 6.1 🟠 备份格式版本管理不完善

**现状**:
```go
const Version = "0823"
```

**问题**:
- 版本号含义不明确
- 没有版本兼容性检查
- 升级路径不清晰

**建议**:
```go
const (
    BackupFormatVersion = 2
    MinSupportedVersion = 1
)

type BackupHeader struct {
    FormatVersion int
    MOVersion     string
    CreateTime    time.Time
    Features      []string  // 使用的特性列表
}

func CheckCompatibility(header BackupHeader) error {
    if header.FormatVersion < MinSupportedVersion {
        return ErrVersionTooOld
    }
    if header.FormatVersion > BackupFormatVersion {
        return ErrVersionTooNew
    }
    return nil
}
```
