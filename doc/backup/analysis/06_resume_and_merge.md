# 断点续传与备份合并功能分析

## 1. 现有断点续传机制分析

### 1.1 现有实现

代码中已经有基础的"跳过已存在文件"逻辑：

```go
// tae.go:execBackup() 中
dstObj, err := fileservice.SortedList(dstFs.List(ctx, ""))
dstHave := make(map[string]bool)
if err != nil {
    return err
}
// 检查是否有 file_list 文件
if len(dstObj) != 0 && dstObj[0].Name == fileList {
    data, err := readFile(ctx, dstFs, fileList)
    if err != nil {
        return err
    }
    objects := strings.Split(string(data), "\n")
    for _, object := range objects {
        dstHave[object] = true
    }
}

// 标记已存在的文件不需要复制
for _, oName := range oNames {
    objName := oName.Location.Name().String()
    if dstHave[objName] {
        oName.NeedCopy = false  // 跳过已存在的文件
    }
}
```

### 1.2 现有实现的问题

#### 问题 1: 依赖 file_list 文件

```go
if len(dstObj) != 0 && dstObj[0].Name == fileList {
    // 只有当 file_list 是第一个文件时才读取
}
```

**问题**:
- 依赖 `file_list` 文件存在且是目录中的第一个文件
- 如果备份中断在保存 `file_list` 之前，已复制的文件无法被识别
- `file_list` 只在备份完成时才写入

#### 问题 2: 没有校验和验证

```go
if dstHave[objName] {
    oName.NeedCopy = false  // 直接跳过，不验证文件完整性
}
```

**问题**:
- 只检查文件名是否存在
- 不验证文件内容是否完整
- 可能跳过损坏的文件

#### 问题 3: Checkpoint 和 GC 目录没有断点续传

```go
// CopyCheckpointDir 和 CopyGCDir 没有检查已存在文件
func CopyCheckpointDir(...) ([]*taeFile, types.TS, error) {
    // 直接复制所有文件，没有跳过逻辑
}
```

## 2. 改进的断点续传方案

### 2.1 实时记录已复制文件

```go
// 定义进度文件
const backupProgressFile = ".backup_progress"

type BackupProgress struct {
    BackupID      string            `json:"backup_id"`
    StartTime     time.Time         `json:"start_time"`
    Phase         string            `json:"phase"`
    CopiedFiles   map[string]string `json:"copied_files"`  // path -> checksum
    TotalFiles    int               `json:"total_files"`
    CopiedBytes   int64             `json:"copied_bytes"`
    TotalBytes    int64             `json:"total_bytes"`
}

// 每复制完一个文件就更新进度
func (p *BackupProgress) AddFile(path string, checksum []byte, size int64) {
    p.CopiedFiles[path] = hex.EncodeToString(checksum)
    p.CopiedBytes += size
}

// 定期保存进度（每 100 个文件或每 1GB）
func (p *BackupProgress) SaveIfNeeded(ctx context.Context, fs fileservice.FileService) error {
    // 保存到 .backup_progress 文件
}
```

### 2.2 改进的文件跳过逻辑

```go
func shouldCopyFile(ctx context.Context, dstFs fileservice.FileService, 
    file *objectio.BackupObject, progress *BackupProgress) (bool, error) {
    
    name := file.Location.Name().String()
    
    // 1. 检查进度文件中是否已记录
    if savedChecksum, ok := progress.CopiedFiles[name]; ok {
        // 2. 验证文件是否存在
        entry, err := dstFs.StatFile(ctx, name)
        if err != nil {
            if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
                return true, nil  // 文件不存在，需要复制
            }
            return false, err
        }
        
        // 3. 验证文件大小
        expectedSize := file.Location.Extent().End() + objectio.FooterSize
        if entry.Size != int64(expectedSize) {
            logutil.Warn("file size mismatch, will re-copy",
                zap.String("file", name),
                zap.Int64("expected", int64(expectedSize)),
                zap.Int64("actual", entry.Size))
            return true, nil
        }
        
        // 4. 可选：验证校验和（对于关键文件）
        if file.IsCritical {
            actualChecksum, err := calculateChecksum(ctx, dstFs, name)
            if err != nil {
                return true, nil  // 无法验证，重新复制
            }
            if actualChecksum != savedChecksum {
                return true, nil  // 校验和不匹配，重新复制
            }
        }
        
        return false, nil  // 文件已存在且有效，跳过
    }
    
    return true, nil  // 未记录，需要复制
}
```

### 2.3 改进的并行复制

```go
func parallelCopyDataWithResume(
    ctx context.Context,
    srcFs, dstFs fileservice.FileService,
    files map[string]*objectio.BackupObject,
    parallelCount int,
    progress *BackupProgress,
) ([]*taeFile, error) {
    
    // 过滤需要复制的文件
    filesToCopy := make([]*objectio.BackupObject, 0)
    skippedFiles := make([]*taeFile, 0)
    
    for _, file := range files {
        needCopy, err := shouldCopyFile(ctx, dstFs, file, progress)
        if err != nil {
            return nil, err
        }
        
        if needCopy {
            filesToCopy = append(filesToCopy, file)
        } else {
            // 记录跳过的文件
            skippedFiles = append(skippedFiles, &taeFile{
                path:     file.Location.Name().String(),
                size:     int64(file.Location.Extent().End() + objectio.FooterSize),
                needCopy: false,
                ts:       file.CrateTS,
            })
        }
    }
    
    logutil.Info("backup resume check",
        zap.Int("total", len(files)),
        zap.Int("to_copy", len(filesToCopy)),
        zap.Int("skipped", len(skippedFiles)))
    
    // 复制需要的文件
    copiedFiles, err := parallelCopyData(srcFs, dstFs, filesToCopy, parallelCount, progress)
    if err != nil {
        return nil, err
    }
    
    // 合并结果
    return append(skippedFiles, copiedFiles...), nil
}
```

## 3. 备份合并功能

### 3.1 需求分析

将多个增量备份合并成一个全量备份：

```
全量备份 (T0)     增量备份 (T1)     增量备份 (T2)
    │                  │                  │
    ▼                  ▼                  ▼
┌─────────┐      ┌─────────┐      ┌─────────┐
│ Base    │  +   │ Delta1  │  +   │ Delta2  │
│ Objects │      │ Objects │      │ Objects │
└─────────┘      └─────────┘      └─────────┘
    │                  │                  │
    └──────────────────┴──────────────────┘
                       │
                       ▼
              ┌─────────────────┐
              │  Merged Full    │
              │  Backup (T2)    │
              └─────────────────┘
```

### 3.2 合并策略

```go
type BackupMergeStrategy int

const (
    // 保留最新版本的对象
    MergeStrategyLatest BackupMergeStrategy = iota
    // 保留所有版本（用于时间点恢复）
    MergeStrategyAll
)

type BackupMergeConfig struct {
    Strategy       BackupMergeStrategy
    SourceBackups  []string  // 源备份路径列表
    TargetPath     string    // 目标路径
    Parallelism    int
    VerifyChecksum bool
}
```

### 3.3 合并实现

```go
func MergeBackups(ctx context.Context, config BackupMergeConfig) error {
    // 1. 加载所有备份的元数据
    backups := make([]*BackupMetadata, 0, len(config.SourceBackups))
    for _, path := range config.SourceBackups {
        meta, err := LoadBackupMetadata(ctx, path)
        if err != nil {
            return err
        }
        backups = append(backups, meta)
    }
    
    // 2. 按时间排序
    sort.Slice(backups, func(i, j int) bool {
        return backups[i].Timestamp.LT(&backups[j].Timestamp)
    })
    
    // 3. 验证备份链完整性
    if err := validateBackupChain(backups); err != nil {
        return err
    }
    
    // 4. 构建最终对象列表
    finalObjects := buildFinalObjectList(backups, config.Strategy)
    
    // 5. 复制对象到目标
    if err := copyObjectsToTarget(ctx, finalObjects, config); err != nil {
        return err
    }
    
    // 6. 合并 Checkpoint
    if err := mergeCheckpoints(ctx, backups, config.TargetPath); err != nil {
        return err
    }
    
    // 7. 生成新的元数据
    return generateMergedMetadata(ctx, backups, config.TargetPath)
}

func buildFinalObjectList(backups []*BackupMetadata, 
    strategy BackupMergeStrategy) map[string]*ObjectInfo {
    
    objects := make(map[string]*ObjectInfo)
    
    for _, backup := range backups {
        for _, obj := range backup.Objects {
            switch strategy {
            case MergeStrategyLatest:
                // 后面的备份覆盖前面的
                if obj.DeleteTS.IsEmpty() {
                    objects[obj.Name] = obj
                } else {
                    // 对象已删除，从列表中移除
                    delete(objects, obj.Name)
                }
            case MergeStrategyAll:
                // 保留所有版本
                key := fmt.Sprintf("%s@%s", obj.Name, obj.CreateTS.ToString())
                objects[key] = obj
            }
        }
    }
    
    return objects
}
```

### 3.4 Checkpoint 合并

```go
func mergeCheckpoints(ctx context.Context, backups []*BackupMetadata, 
    targetPath string) error {
    
    // 获取最新的 Checkpoint
    latestBackup := backups[len(backups)-1]
    
    // 读取所有 Checkpoint 数据
    var allObjects []*ObjectEntry
    for _, backup := range backups {
        ckpData, err := loadCheckpointData(ctx, backup.CheckpointLocation)
        if err != nil {
            return err
        }
        allObjects = append(allObjects, ckpData.Objects...)
    }
    
    // 去重和合并
    mergedObjects := deduplicateObjects(allObjects)
    
    // 生成新的 Checkpoint
    newCkp := &CheckpointData{
        Start:   backups[0].StartTS,
        End:     latestBackup.EndTS,
        Objects: mergedObjects,
    }
    
    return writeCheckpoint(ctx, targetPath, newCkp)
}
```

## 4. 完整的断点续传 + 合并方案

### 4.1 备份状态机

```
                    ┌─────────────┐
                    │   INIT      │
                    └──────┬──────┘
                           │
                           ▼
                    ┌─────────────┐
         ┌─────────│  SCANNING   │
         │         └──────┬──────┘
         │                │
         │                ▼
         │         ┌─────────────┐
         │  ┌──────│  COPYING    │◄─────┐
         │  │      └──────┬──────┘      │
         │  │             │             │
         │  │  (失败)     │ (成功)      │ (恢复)
         │  │             │             │
         │  ▼             ▼             │
         │  ┌─────────────┐      ┌──────┴──────┐
         │  │   FAILED    │      │  FINALIZING │
         │  └─────────────┘      └──────┬──────┘
         │                              │
         │                              ▼
         │                       ┌─────────────┐
         └──────────────────────▶│  COMPLETED  │
                                 └─────────────┘
```

### 4.2 使用示例

```go
// 1. 开始新备份
backup, err := StartBackup(ctx, BackupConfig{
    Type:        BackupTypeFull,
    Destination: "s3://bucket/backup/2024-01-01",
    Parallelism: 128,
})

// 2. 备份中断后恢复
backup, err := ResumeBackup(ctx, "s3://bucket/backup/2024-01-01")
if err != nil {
    // 无法恢复，需要重新开始
    backup, err = StartBackup(ctx, ...)
}

// 3. 合并多个备份
err := MergeBackups(ctx, BackupMergeConfig{
    SourceBackups: []string{
        "s3://bucket/backup/full-2024-01-01",
        "s3://bucket/backup/incr-2024-01-02",
        "s3://bucket/backup/incr-2024-01-03",
    },
    TargetPath:  "s3://bucket/backup/merged-2024-01-03",
    Strategy:    MergeStrategyLatest,
    Parallelism: 128,
})
```

## 5. 实施建议

### 5.1 短期改进（1-2 周）

1. **改进现有跳过逻辑**:
   - 不依赖 `file_list` 文件
   - 直接检查目标目录中的文件
   - 添加文件大小验证

2. **添加进度文件**:
   - 实时记录已复制文件
   - 支持从进度文件恢复

### 5.2 中期改进（1 月）

1. **完整的断点续传**:
   - 支持所有阶段的恢复
   - 添加校验和验证
   - 支持 Checkpoint 和 GC 目录

2. **基础合并功能**:
   - 支持两个备份合并
   - 验证备份链完整性

### 5.3 长期改进（2-3 月）

1. **高级合并功能**:
   - 支持多个备份合并
   - 支持不同合并策略
   - 自动清理旧备份

2. **备份管理**:
   - 备份链管理
   - 自动合并策略
   - 保留策略


## 6. 已实现的功能

### 6.1 备份合并功能 ✅

备份合并功能已经完整实现，包括核心库和命令行工具。

#### 6.1.1 命令行工具 (mo_br merge)

代码位于 `mo-backup/pkg/backup/merge.go` 和 `mo-backup/pkg/run/run.go`。

**使用方法：**

```bash
# 文件系统备份合并
mo_br merge <target_backup_id> \
    --backup_dir filesystem \
    --path /data/backups \
    --parallelism 128

# S3 备份合并
mo_br merge <target_backup_id> \
    --backup_dir s3 \
    --endpoint s3.amazonaws.com \
    --access_key_id YOUR_KEY \
    --secret_access_key YOUR_SECRET \
    --bucket my-backup-bucket \
    --filepath /backups \
    --parallelism 128

# MinIO 备份合并
mo_br merge <target_backup_id> \
    --backup_dir s3 \
    --endpoint minio.example.com:9000 \
    --access_key_id minioadmin \
    --secret_access_key minioadmin \
    --bucket backups \
    --filepath /mo-backups \
    --is_minio \
    --parallelism 64
```

**参数说明：**

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `<target_backup_id>` | 目标备份ID，系统自动找到对应的全量备份并合并所有中间增量备份 | 必填 |
| `--backup_dir` | 备份目录类型，`s3` 或 `filesystem` | 必填 |
| `--path` | 文件系统备份路径 | - |
| `--endpoint` | S3 端点 | - |
| `--access_key_id` | S3 访问密钥ID | - |
| `--secret_access_key` | S3 访问密钥 | - |
| `--bucket` | S3 存储桶 | - |
| `--filepath` | S3 文件路径 | - |
| `--region` | S3 区域 | - |
| `--is_minio` | 是否为 MinIO | false |
| `--target_path` | 合并后备份的目标路径（可选） | 自动生成 |
| `--meta_path` | mo_br.meta 文件路径 | ./mo_br.meta |
| `--parallelism` | 并行复制文件数 | 128 |

**工作流程：**

```
1. 读取 mo_br.meta 获取所有备份记录
           │
           ▼
2. 根据 target_backup_id 构建备份链
   (自动找到最近的全量备份)
           │
           ▼
3. 按时间顺序合并文件列表
   (后面的备份覆盖前面的同名文件)
           │
           ▼
4. 并行复制文件到目标目录
           │
           ▼
5. 复制最新备份的 ckp 和 gc 目录
           │
           ▼
6. 生成合并后的元数据文件
   (tae_list, tae_sum, file_list)
           │
           ▼
7. 更新 mo_br.meta 添加新备份记录
```

**输出示例：**

```
========== Backup Merge Summary ==========
Merged ID:     01234567-89ab-cdef-0123-456789abcdef
Output Path:   /data/backups/merged-01234567-89ab-cdef-0123-456789abcdef
Total Files:   15234
Total Size:    1073741824 bytes
Copied:        15234 files
Skipped:       0 files
Duration:      2m30s
==========================================
```

#### 6.1.2 核心库

代码位于 `pkg/backup/merge.go`。

```go
// BackupMeta 表示单个备份的元数据
type BackupMeta struct {
    BackupTime string      // 备份创建时间
    BackupTS   types.TS    // 备份时间戳
    BackupType string      // "full" 或 "incremental"
    TotalSize  int64       // 所有文件的总大小
    Files      []*taeFile  // 备份中的所有文件
}

// MergeConfig 包含备份合并操作的配置
type MergeConfig struct {
    SourceBackups []string  // 要合并的备份路径列表（顺序：full, incr1, incr2, ...）
    TargetPath    string    // 合并后备份的目标路径
    Parallelism   int       // 并行复制操作数
    IsS3          bool      // 是否在 S3 上
    S3Option      []string  // S3 配置（如果 IsS3 为 true）
}

// MergeResult 包含合并操作的结果
type MergeResult struct {
    TotalFiles   int           // 合并后备份的总文件数
    TotalSize    int64         // 合并后备份的总大小
    CopiedFiles  int           // 实际复制的文件数
    SkippedFiles int           // 跳过的文件数（已存在）
    Duration     time.Duration // 合并操作耗时
}
```

#### 使用方式

```go
import "github.com/matrixorigin/matrixone/pkg/backup"

// 合并多个备份
config := &backup.MergeConfig{
    SourceBackups: []string{
        "/data/backup/full-2024-01-01",
        "/data/backup/incr-2024-01-02",
        "/data/backup/incr-2024-01-03",
    },
    TargetPath:  "/data/backup/merged-2024-01-03",
    Parallelism: 128,
    IsS3:        false,
}

result, err := backup.MergeBackups(ctx, config)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("合并完成: %d 个文件, %d 字节, 耗时 %v\n",
    result.TotalFiles, result.TotalSize, result.Duration)
```

#### 功能特点

1. **支持多种存储**：本地文件系统和 S3 存储
2. **并行复制**：使用 goroutine 池并行复制文件，提高效率
3. **智能去重**：自动合并文件列表，保留最新版本
4. **备份链验证**：验证备份时间戳顺序，确保链完整性
5. **增量复制**：跳过目标目录中已存在的文件
6. **元数据生成**：自动生成合并后的 tae_list 和 tae_sum 文件

#### 合并流程

```
1. 加载所有源备份的元数据 (tae_list, tae_sum)
           │
           ▼
2. 验证备份链完整性（时间戳顺序）
           │
           ▼
3. 合并文件列表（后面的备份覆盖前面的）
           │
           ▼
4. 并行复制文件到目标目录
           │
           ▼
5. 复制最新备份的 ckp 和 gc 目录
           │
           ▼
6. 生成合并后的元数据文件
```

### 6.2 与其他系统的对比

| 系统 | 工具 | 特点 |
|------|------|------|
| PostgreSQL | pg_combinebackup | 合并增量备份，支持并行 |
| MySQL | mysqlbackup --prepare | 应用增量日志到全量备份 |
| TiDB BR | 不支持合并 | 需要手动管理备份链 |
| MatrixOne | mo_br merge | 类似 pg_combinebackup，支持 S3 和本地文件系统 |

### 6.3 待实现功能

1. **断点续传增强**：备份中断后从上次位置继续，支持校验和验证
2. **--dry-run 参数**：预览合并操作而不实际执行
3. **自动清理**：合并后自动删除源备份
4. **SQL 接口**：通过 SQL 语句执行合并操作
