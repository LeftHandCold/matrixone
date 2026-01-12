# 会话上下文传递文档

本文档用于在新会话中快速恢复上下文。

## 已完成的任务

### 任务 1: Backup 模块文档创建 ✅

在 `doc/backup/` 目录创建了完整的备份模块文档：

| 文件 | 内容 |
|------|------|
| README.md | 目录索引 |
| 01_architecture.md | 整体架构 |
| 02_data_structures.md | 数据结构 |
| 03_backup_flow.md | 备份流程 |
| 04_tae_backup.md | TAE 备份 |
| 05_checkpoint_backup.md | Checkpoint 备份 |
| 06_filesystem.md | 文件系统操作 |
| 07_backup_protection.md | 备份保护机制 |
| 08_incremental_backup.md | 增量备份 |
| 09_restore.md | 恢复流程 |
| 10_best_practices.md | 最佳实践 |
| 11_merge_command.md | 合并命令文档 |
| appendix_code_reference.md | 代码参考 |

### 任务 2: Backup 代码问题分析 ✅

在 `doc/backup/analysis/` 目录创建了分析文档：

| 文件 | 内容 |
|------|------|
| README.md | 分析概述 |
| 01_code_issues.md | 代码问题 |
| 02_architecture_issues.md | 架构问题 |
| 03_reliability_issues.md | 可靠性问题 |
| 04_performance_issues.md | 性能问题 |
| 05_optimization_summary.md | 优化总结 |
| 06_resume_and_merge.md | 断点续传与合并分析 |

### 任务 3: 备份合并功能实现 ✅

实现了 `mo_br merge` 命令。

**新增/修改的文件：**

```
mo-backup/
├── pkg/
│   ├── config/
│   │   └── merge.go          # MergeConfig, BackupChain 结构体
│   ├── backup/
│   │   └── merge.go          # RunMerge() 核心逻辑
│   └── run/
│       └── run.go            # 添加 mergeCmd, initMergeCmd(), initMergeConfig()
```

**核心函数：**

```go
// mo-backup/pkg/backup/merge.go
func RunMerge(conf *config.MergeConfig) error
func loadMobrMetaForMerge(conf *config.MergeConfig) (config.MobrMetas, error)
func buildBackupChain(metas config.MobrMetas, targetID string) (*config.BackupChain, error)
func setupSourceFileServicesForMerge(ctx context.Context, conf *config.MergeConfig, chain *config.BackupChain) ([]fileservice.FileService, error)
func setupTargetFileServiceForMerge(ctx context.Context, conf *config.MergeConfig, targetPath string) (fileservice.FileService, error)
func loadAndMergeFileListsForMerge(ctx context.Context, fsList []fileservice.FileService, chain *config.BackupChain) ([]*MergeTaeFile, map[string]int, error)
func copyMergedFilesParallel(ctx context.Context, srcFsList []fileservice.FileService, dstFs fileservice.FileService, files []*MergeTaeFile, fileSourceMap map[string]int, parallelism int) (*MergeResult, error)
func saveMergedMetaForMerge(ctx context.Context, dstFs fileservice.FileService, files []*MergeTaeFile, latestBackup *config.BackupChainItem) error
func updateMobrMetaForMerge(conf *config.MergeConfig, mergedID, mergedPath string, totalSize int64, startTime time.Time, backupTS string) error
```

**使用方法：**

```bash
# 文件系统
mo_br merge <backup_id> --backup_dir filesystem --path /data/backups

# S3
mo_br merge <backup_id> --backup_dir s3 \
    --endpoint s3.amazonaws.com \
    --access_key_id KEY \
    --secret_access_key SECRET \
    --bucket mybucket \
    --filepath /backups
```

## 关键代码位置

| 模块 | 路径 |
|------|------|
| 备份核心 | `pkg/backup/` |
| TAE 备份 | `pkg/vm/engine/tae/db/checkpoint/tae.go` |
| mo_br 工具 | `mo-backup/` |
| 合并功能 | `mo-backup/pkg/backup/merge.go` |
| 合并命令 | `mo-backup/pkg/run/run.go` (搜索 `mergeCmd`) |
| 合并配置 | `mo-backup/pkg/config/merge.go` |

## 待完成功能

1. **--dry-run 参数**：预览合并操作
2. **自动清理**：合并后删除源备份选项
3. **断点续传增强**：校验和验证
4. **进度显示**：实时百分比

## 编译验证

```bash
cd mo-backup
go build -o mo_br ./cmd/mo_br/
./mo_br merge --help
```
