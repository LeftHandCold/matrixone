# 备份合并功能 (mo_br merge)

## 1. 功能概述

`mo_br merge` 命令用于将一个全量备份和多个增量备份合并成一个新的全量备份，类似于 PostgreSQL 的 `pg_combinebackup` 工具。

### 1.1 使用场景

- 增量备份链过长，恢复时间过长
- 需要清理旧的增量备份，但保留完整的恢复能力
- 定期整合备份，简化备份管理

### 1.2 工作原理

```
全量备份 (T0)     增量备份 (T1)     增量备份 (T2)
    │                  │                  │
    ▼                  ▼                  ▼
┌─────────┐      ┌─────────┐      ┌─────────┐
│  Full   │  +   │  Incr1  │  +   │  Incr2  │
│ Backup  │      │ Backup  │      │ Backup  │
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

## 2. 命令使用

### 2.1 基本语法

```bash
mo_br merge <target_backup_id> [flags]
```

用户只需提供目标备份 ID，系统会自动：
1. 从 `mo_br.meta` 中找到该备份
2. 找到最近的全量备份
3. 合并从全量备份到目标备份之间的所有备份

### 2.2 文件系统备份合并

```bash
mo_br merge 01234567-89ab-cdef-0123-456789abcdef \
    --backup_dir filesystem \
    --path /data/backups \
    --parallelism 128
```

### 2.3 S3 备份合并

```bash
mo_br merge 01234567-89ab-cdef-0123-456789abcdef \
    --backup_dir s3 \
    --endpoint s3.amazonaws.com \
    --access_key_id YOUR_ACCESS_KEY \
    --secret_access_key YOUR_SECRET_KEY \
    --bucket my-backup-bucket \
    --filepath /backups \
    --region us-east-1 \
    --parallelism 128
```

### 2.4 MinIO 备份合并

```bash
mo_br merge 01234567-89ab-cdef-0123-456789abcdef \
    --backup_dir s3 \
    --endpoint minio.example.com:9000 \
    --access_key_id minioadmin \
    --secret_access_key minioadmin \
    --bucket backups \
    --filepath /mo-backups \
    --is_minio \
    --parallelism 64
```

## 3. 参数说明

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `<target_backup_id>` | 目标备份ID（必填参数） | - |
| `--backup_dir` | 备份目录类型：`s3` 或 `filesystem` | - |
| `--path` | 文件系统备份路径 | - |
| `--endpoint` | S3 端点地址 | - |
| `--access_key_id` | S3 访问密钥 ID | - |
| `--secret_access_key` | S3 访问密钥 | - |
| `--bucket` | S3 存储桶名称 | - |
| `--filepath` | S3 文件路径（基础路径） | - |
| `--region` | S3 区域 | - |
| `--compression` | S3 压缩方式 | - |
| `--role_arn` | S3 角色 ARN | - |
| `--is_minio` | 是否为 MinIO | false |
| `--target_path` | 合并后备份的目标路径（可选） | 自动生成 |
| `--meta_path` | mo_br.meta 文件路径 | ./mo_br.meta |
| `--parallelism` | 并行复制文件数 | 128 |

## 4. 执行流程

```
┌─────────────────────────────────────────────────────────────┐
│ 1. 读取 mo_br.meta 获取所有备份记录                           │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. 根据 target_backup_id 构建备份链                          │
│    - 找到目标备份                                            │
│    - 找到最近的全量备份                                       │
│    - 收集中间所有增量备份                                     │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. 按时间顺序合并文件列表                                     │
│    - 读取每个备份的 tae_list                                 │
│    - 后面的备份覆盖前面的同名文件                              │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. 并行复制 tae 数据文件到目标目录                            │
│    - 使用 goroutine 池并行复制                               │
│    - 文件找不到时尝试其他备份                                  │
│    - 自动添加 tae/ 前缀到文件路径                             │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. 复制最新备份的 ckp、gc、config、hakeeper 目录              │
│    - 递归复制所有子目录和文件                                  │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│ 6. 复制 backup_meta 和 mo_meta 文件                          │
│    - 包括对应的 .sha256 校验文件                              │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│ 7. 生成合并后的元数据文件                                     │
│    - tae_list: 所有文件列表                                  │
│    - tae_sum: 备份摘要（标记为 full 类型）                    │
│    - file_list: 文件路径列表（用于断点续传）                   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│ 8. 更新 mo_br.meta 添加新的合并备份记录                       │
└─────────────────────────────────────────────────────────────┘
```

## 5. 输出示例

```
========== Backup Merge Summary ==========
Merged ID:     01937abc-def0-7123-4567-89abcdef0123
Output Path:   /data/backups/full-01937abc-def0-7123-4567-89abcdef0123
Total Files:   15234
Total Size:    1073741824 bytes
Copied:        15234 files
Skipped:       0 files
Duration:      2m30s
==========================================
```

注意：合并后的备份目录使用 `full-<uuid>` 格式命名，表示这是一个合成的全量备份。

## 6. FileService 类型说明

### 6.1 MO Server 与 mo-backup 的 FileService 使用

备份系统中，MO Server 和 mo-backup 工具使用不同的 FileService 类型：

| 写入者 | FileService | 文件类型 |
|--------|-------------|----------|
| MO Server | LocalFS (`forETL=false`) | TAE 目录下所有文件 |
| mo-backup | LocalETLFS (`forETL=true`) | 备份元数据、配置文件 |

### 6.2 文件分类

**MO Server 写入的文件（使用 LocalFS，带 checksum）：**
- `tae/*` - 所有 TAE 数据文件（对象文件）
- `tae/tae_list` - 文件列表
- `tae/tae_sum` - 备份摘要
- `tae/ckp/*` - Checkpoint 文件
- `tae/gc/*` - GC 文件

**mo-backup 写入的文件（使用 LocalETLFS，无 checksum）：**
- `backup_meta` - 备份元数据
- `mo_meta` - MO 元数据
- `config/*` - 配置文件
- `hakeeper/*` - HAKeeper 数据

### 6.3 Merge 命令的 FileService 使用

```go
// 读取源备份
targetTaeFs, _ := fs.SetupFilesystem(backupPath, false)  // LocalFS for TAE files
targetEtlFs, _ := fs.SetupFilesystem(backupPath, true)   // LocalETLFS for meta files

// 写入目标目录
dstTaeFs, _ := fs.SetupFilesystem(targetPath, false)     // LocalFS for TAE files
dstEtlFs, _ := fs.SetupFilesystem(targetPath, true)      // LocalETLFS for meta files
```

### 6.4 checksum not match 错误

如果遇到 `checksum not match` 错误，通常是因为使用了错误的 FileService 类型读取文件：

```
错误场景：用 LocalFS 读取 backup_meta（mo-backup 用 LocalETLFS 写入）
错误信息：internal error: checksum not match
```

**解决方案**：确保读取文件时使用与写入时相同类型的 FileService。

## 7. 代码位置

| 文件 | 说明 |
|------|------|
| `mo-backup/pkg/config/merge.go` | MergeConfig、BackupChain 配置结构体 |
| `mo-backup/pkg/backup/merge.go` | RunMerge() 核心合并逻辑 |
| `mo-backup/pkg/run/run.go` | mergeCmd 命令定义、initMergeCmd() |

## 8. 与其他系统对比

| 系统 | 工具 | 特点 |
|------|------|------|
| PostgreSQL | pg_combinebackup | 合并增量备份，支持并行 |
| MySQL | mysqlbackup --prepare | 应用增量日志到全量备份 |
| TiDB BR | 不支持合并 | 需要手动管理备份链 |
| **MatrixOne** | **mo_br merge** | 类似 pg_combinebackup，支持 S3 和本地文件系统 |

## 9. 注意事项

1. **备份链完整性**：合并前确保备份链完整，不要删除中间的增量备份
2. **存储空间**：合并操作需要额外的存储空间存放新的全量备份
3. **并行度设置**：根据网络带宽和存储 IOPS 调整 `--parallelism` 参数
4. **元数据文件**：确保 `mo_br.meta` 文件存在且包含所有备份记录

## 10. 后续优化计划

1. **--dry-run 参数**：预览合并操作而不实际执行
2. **自动清理**：合并后自动删除源备份选项
3. **进度显示**：实时显示合并进度百分比
4. **校验和验证**：复制后验证文件完整性
