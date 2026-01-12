# Backup 模块架构概述

## 1. 设计目标

MatrixOne Backup 模块的设计目标：

1. **数据一致性**: 确保备份数据在某个时间点的一致性
2. **最小化影响**: 备份过程对在线业务的影响最小化
3. **存储灵活性**: 支持多种存储后端（本地/S3）
4. **高效传输**: 支持并行复制，提高备份效率
5. **增量支持**: 支持基于 Checkpoint 的增量备份

## 2. 整体架构

```
┌─────────────────────────────────────────────────────────────────┐
│                        Backup Entry                              │
│                      (backup.go:Backup)                          │
└─────────────────────────────────────────────────────────────────┘
                                │
                ┌───────────────┼───────────────┐
                ▼               ▼               ▼
┌───────────────────┐ ┌─────────────────┐ ┌─────────────────────┐
│   Config Backup   │ │   TAE Backup    │ │  HAKeeper Backup    │
│ (backupConfigs)   │ │  (backupTae)    │ │ (backupHakeeper)    │
└───────────────────┘ └─────────────────┘ └─────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                      BackupData                                  │
│                    (tae.go:BackupData)                          │
└─────────────────────────────────────────────────────────────────┘
                                │
        ┌───────────────────────┼───────────────────────┐
        ▼                       ▼                       ▼
┌───────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Checkpoint   │     │  Parallel Copy  │     │ Backup          │
│  Collection   │     │  (parallelCopy) │     │ Protection      │
└───────────────┘     └─────────────────┘     └─────────────────┘
```

## 3. 核心组件

### 3.1 备份入口 (Backup)

`backup.go` 中的 `Backup` 函数是整个备份流程的入口：

```go
func Backup(
    ctx context.Context,
    sid string,
    bs *tree.BackupStart,
    cfg *Config,
) error
```

主要职责：
- 解析备份参数
- 设置目标文件系统（本地/S3）
- 协调各子模块执行备份
- 保存备份元数据

### 3.2 TAE 数据备份 (BackupData)

`tae.go` 中的 `BackupData` 函数负责 TAE 存储引擎数据的备份：

```go
func BackupData(
    ctx context.Context,
    sid string,
    srcFs, dstFs fileservice.FileService,
    dir string,
    config *Config,
) error
```

主要职责：
- 触发 Checkpoint 创建
- 收集需要备份的文件列表
- 并行复制数据文件
- 处理 Checkpoint 和 GC 元数据

### 3.3 备份保护管理器 (backupProtectionManager)

确保备份期间数据不被 GC 回收：

```go
type backupProtectionManager struct {
    ctx            context.Context
    exec           executor.SQLExecutor
    protectedTS    types.TS
    updateTicker   *time.Ticker
    protectionSet  bool
}
```

### 3.4 文件系统抽象 (FileService)

支持多种存储后端：

```go
// 本地文件系统
func setupFilesystem(ctx context.Context, path string, forETL bool) 
    (fileservice.FileService, string, error)

// S3 对象存储
func setupS3(ctx context.Context, s3 *s3Config, forETL bool) 
    (fileservice.FileService, string, error)
```

## 4. 备份类型

### 4.1 全量备份

备份数据库的完整状态，包括：
- 所有 Checkpoint 数据
- 所有数据对象文件
- 配置文件
- HAKeeper 状态

### 4.2 增量备份

基于指定时间戳的增量备份：
- 只备份指定时间点之后的变更
- 依赖 Checkpoint 机制
- 需要配合全量备份使用

## 5. 数据流向

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Source     │     │   Backup     │     │ Destination  │
│   Storage    │────▶│   Process    │────▶│   Storage    │
│   (TAE)      │     │              │     │ (Local/S3)   │
└──────────────┘     └──────────────┘     └──────────────┘
       │                    │                    │
       ▼                    ▼                    ▼
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│ - Objects    │     │ - Checksum   │     │ - tae/       │
│ - Checkpoints│     │ - Parallel   │     │ - config/    │
│ - GC Meta    │     │ - Protection │     │ - hakeeper/  │
└──────────────┘     └──────────────┘     └──────────────┘
```

## 6. 与其他模块的关系

### 6.1 与 Checkpoint 模块

- 备份触发强制 Checkpoint 创建
- 读取 Checkpoint 获取数据文件列表
- 复制 Checkpoint 元数据文件

### 6.2 与 GC 模块

- 设置备份保护时间戳
- 阻止 GC 删除备份所需的文件
- 备份完成后移除保护

### 6.3 与 FileService

- 抽象存储后端差异
- 提供统一的文件操作接口
- 支持重试和错误处理

## 7. 关键设计决策

### 7.1 基于 Checkpoint 的备份

选择基于 Checkpoint 而非 WAL 的备份策略：
- 简化备份逻辑
- 减少备份数据量
- 便于增量备份实现

### 7.2 并行复制

使用 Job Scheduler 实现并行文件复制：
- 提高大规模数据备份效率
- 可配置并行度
- 支持错误处理和重试

### 7.3 备份保护机制

通过 mo_ctl 命令设置 GC 保护：
- 定期更新保护时间戳
- 确保备份期间数据安全
- 自动清理保护状态
