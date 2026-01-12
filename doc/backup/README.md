# MatrixOne Backup 模块深度解析

本文档系列深入分析 MatrixOne 数据库的 Backup（备份）模块实现，帮助开发者理解备份系统的架构设计、核心组件和工作流程。

## 目录结构

1. [架构概述](01_architecture.md) - Backup 模块整体架构和设计理念
2. [核心数据结构](02_data_structures.md) - 关键数据结构和类型定义
3. [备份流程](03_backup_flow.md) - 完整的备份执行流程
4. [TAE 数据备份](04_tae_backup.md) - TAE 存储引擎数据备份机制
5. [Checkpoint 备份](05_checkpoint_backup.md) - Checkpoint 在备份中的作用
6. [文件系统操作](06_filesystem.md) - 文件系统抽象和 S3 支持
7. [备份保护机制](07_backup_protection.md) - GC 保护和数据一致性保障
8. [增量备份](08_incremental_backup.md) - 增量备份的实现原理
9. [恢复流程](09_restore.md) - 数据恢复的基本原理
10. [最佳实践](10_best_practices.md) - 使用建议和注意事项

## 模块概述

MatrixOne 的 Backup 模块位于 `pkg/backup` 目录，主要负责：

- **全量备份**: 备份整个数据库的完整状态
- **增量备份**: 基于 Checkpoint 的增量数据备份
- **多存储支持**: 支持本地文件系统和 S3 对象存储
- **并行备份**: 支持多线程并行复制数据文件
- **GC 保护**: 在备份期间保护数据不被垃圾回收

## 核心文件

```
pkg/backup/
├── backup.go          # 备份入口和主流程
├── types.go           # 核心类型定义
├── tae.go             # TAE 数据备份实现
├── fs.go              # 文件系统操作
└── utils.go           # 工具函数

pkg/vm/engine/tae/
├── logtail/backup.go  # Checkpoint 数据加载
├── db/checkpoint/     # Checkpoint 管理
└── db/gc/v3/          # GC 和备份保护
```

## 版本信息

当前备份版本: `0823`

## 快速开始

### 执行备份

```sql
-- 本地文件系统备份
BACKUP DATABASE TO '/path/to/backup';

-- S3 备份
BACKUP DATABASE TO 's3://bucket/path' 
WITH endpoint='...', access_key_id='...', secret_access_key='...';
```

### 备份产物

备份完成后会生成以下文件：

- `mo_meta` - 备份元数据文件
- `config/` - 配置文件目录
- `tae/` - TAE 数据文件目录
- `hakeeper/` - HAKeeper 数据目录

## 相关文档

- [GC 模块文档](../gc/README.md) - 垃圾回收机制
- [Checkpoint 文档](../checkpoint/README.md) - Checkpoint 机制
