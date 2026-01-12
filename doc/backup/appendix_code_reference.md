# 附录：代码参考

## 1. 核心文件索引

### 1.1 pkg/backup 目录

| 文件 | 描述 | 关键函数 |
|-----|------|---------|
| `backup.go` | 备份入口和主流程 | `Backup()`, `backupTae()`, `saveMetas()` |
| `types.go` | 核心类型定义 | `Config`, `Metas`, `Meta`, `taeFile` |
| `tae.go` | TAE 数据备份 | `BackupData()`, `execBackup()`, `parallelCopyData()` |
| `fs.go` | 文件系统操作 | `setupFilesystem()`, `setupS3()`, `writeFile()` |
| `utils.go` | 工具函数 | `buildInfo()`, `getS3Config()` |

### 1.2 pkg/vm/engine/tae/logtail 目录

| 文件 | 描述 | 关键函数 |
|-----|------|---------|
| `backup.go` | Checkpoint 数据加载 | `LoadCheckpointEntriesFromKey()`, `ReWriteCheckpointAndBlockFromKey()` |
| `snapshot.go` | 快照管理 | `SnapshotMeta`, `GetSnapshot()` |
| `ckp_writer.go` | Checkpoint 写入 | `NewBackupCollector_V2()` |

### 1.3 pkg/vm/engine/tae/db 目录

| 文件 | 描述 | 关键函数 |
|-----|------|---------|
| `db.go` | 数据库操作 | `ForceCheckpointForBackup()` |
| `checkpoint/store.go` | Checkpoint 存储 | `GetAllCheckpointsForBackup()`, `AddBackupCKPEntry()` |
| `checkpoint/runner.go` | Checkpoint 运行器 | `CreateSpecialCheckpointFile()` |
| `gc/v3/checkpoint.go` | GC 清理器 | `SetBackupProtection()`, `checkBackupProtection()` |

### 1.4 pkg/vm/engine/tae/rpc 目录

| 文件 | 描述 | 关键函数 |
|-----|------|---------|
| `handle_debug.go` | 调试处理 | `HandleBackup()`, `HandleDiskCleaner()` |

## 2. 关键接口

### 2.1 Cleaner 接口

```go
type Cleaner interface {
    // 备份保护方法
    SetBackupProtection(protectedTS types.TS)
    UpdateBackupProtection(protectedTS types.TS)
    RemoveBackupProtection()
    GetBackupProtection() (protectedTS types.TS, lastUpdateTime time.Time, isActive bool)
    
    // 其他方法...
}
```

### 2.2 FileService 接口

```go
type FileService interface {
    Read(ctx context.Context, vector *IOVector) error
    Write(ctx context.Context, vector IOVector) error
    Delete(ctx context.Context, filePaths ...string) error
    List(ctx context.Context, dirPath string) ([]DirEntry, error)
    StatFile(ctx context.Context, filePath string) (*DirEntry, error)
}
```

### 2.3 BRHAKeeperClient 接口

```go
type BRHAKeeperClient interface {
    GetBackupData(ctx context.Context) ([]byte, error)
}
```

## 3. 重要常量

### 3.1 备份相关

```go
const Version = "0823"

const (
    moMeta       = "mo_meta"
    configDir    = "config"
    taeDir       = "tae"
    taeList      = "tae_list"
    taeSum       = "tae_sum"
    hakeeperDir  = "hakeeper"
    HakeeperFile = "hk_data"
)
```

### 3.2 保护相关

```go
const (
    backupProtectionUpdateInterval = 5 * time.Minute
    backupProtectionCmdPrefix = "add_checker.backup."
    backupProtectionRemoveCmd = "remove_checker.backup."
)
```

### 3.3 Checkpoint 类型

```go
const (
    ET_Global EntryType = iota
    ET_Incremental
    ET_Backup
    ET_Compacted
)
```

## 4. 数据流图

### 4.1 备份数据流

```
SQL: BACKUP DATABASE TO ...
         │
         ▼
    Backup() [backup.go]
         │
         ├──▶ backupBuildInfo()
         │
         ├──▶ backupConfigs()
         │
         ├──▶ backupTae()
         │         │
         │         ▼
         │    BackupData() [tae.go]
         │         │
         │         ├──▶ mo_ctl('dn','Backup','')
         │         │         │
         │         │         ▼
         │         │    HandleBackup() [handle_debug.go]
         │         │         │
         │         │         ├──▶ ForceCheckpointForBackup()
         │         │         │
         │         │         └──▶ GetAllCheckpointsForBackup()
         │         │
         │         ├──▶ LoadCheckpointEntriesFromKey()
         │         │
         │         ├──▶ backupProtectionManager.start()
         │         │
         │         ├──▶ parallelCopyData()
         │         │
         │         ├──▶ CopyCheckpointDir()
         │         │
         │         └──▶ CopyGCDir()
         │
         ├──▶ backupHakeeper()
         │
         └──▶ saveMetas()
```

### 4.2 保护数据流

```
backupProtectionManager.start()
         │
         ▼
    mo_ctl('dn','DiskCleaner','add_checker.backup.xxx')
         │
         ▼
    HandleDiskCleaner() [handle_debug.go]
         │
         ▼
    cleaner.SetBackupProtection()
         │
         ▼
    checkpointCleaner.backupProtection.isActive = true
```

## 5. 测试文件

| 文件 | 描述 |
|-----|------|
| `pkg/backup/backup_test.go` | 备份功能测试 |
| `pkg/backup/fs_test.go` | 文件系统测试 |
| `pkg/backup/types_test.go` | 类型测试 |
| `pkg/vm/engine/tae/db/gc/v3/backup_protection_test.go` | 备份保护测试 |

## 6. 相关 SQL 命令

### 6.1 备份命令

```sql
-- 本地备份
BACKUP DATABASE TO '/path/to/backup';

-- S3 备份
BACKUP DATABASE TO 's3://bucket/path' WITH ...;

-- 指定时间点备份
BACKUP DATABASE TO '/path' WITH backup_ts='xxx';
```

### 6.2 mo_ctl 命令

```sql
-- 触发备份
SELECT mo_ctl('dn', 'Backup', '');

-- 设置备份保护
SELECT mo_ctl('dn', 'DiskCleaner', 'add_checker.backup.xxx');

-- 移除备份保护
SELECT mo_ctl('dn', 'DiskCleaner', 'remove_checker.backup.');
```

## 7. 日志关键字

### 7.1 备份日志

```
backup: copy file
backup: start backup
backup: end backup
Force-Backup-CKP
```

### 7.2 保护日志

```
GC-Backup-Protection-Set
GC-Backup-Protection-Updated
GC-Backup-Protection-Removed
GC-Backup-Protection-Block-Checkpoint
```

### 7.3 Checkpoint 日志

```
Replay-Backup-AddFailed
GC-TRACE-MERGE-CHECKPOINT-FILES
ReWrite Checkpoint
```
