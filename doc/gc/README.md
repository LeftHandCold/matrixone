# MatrixOne TAE GC V3 模块文档

本目录包含 MatrixOne 数据库 TAE 存储引擎 GC（Garbage Collection）V3 模块的详细技术文档。

## 文档索引

| 文档 | 描述 |
|------|------|
| [01_overview.md](./01_overview.md) | GC模块整体架构概述 |
| [02_core_components.md](./02_core_components.md) | 核心组件详解 |
| [03_gc_window.md](./03_gc_window.md) | GC窗口机制 |
| [04_checkpoint_cleaner.md](./04_checkpoint_cleaner.md) | Checkpoint清理器 |
| [05_gc_executor.md](./05_gc_executor.md) | GC执行器与过滤机制 |
| [06_data_flow.md](./06_data_flow.md) | 数据流与处理流程 |
| [07_backup_protection.md](./07_backup_protection.md) | 备份保护机制 |
| [08_metrics.md](./08_metrics.md) | 监控指标 |
| [09_snapshot_mechanism.md](./09_snapshot_mechanism.md) | 快照机制详解 |

## 代码分析与优化

| 文档 | 描述 |
|------|------|
| [analysis/01_code_issues.md](./analysis/01_code_issues.md) | 代码问题与设计缺陷 |
| [analysis/02_architecture_issues.md](./analysis/02_architecture_issues.md) | 架构层面的问题 |
| [analysis/03_performance_issues.md](./analysis/03_performance_issues.md) | 性能问题分析 |
| [analysis/04_optimization_suggestions.md](./analysis/04_optimization_suggestions.md) | 优化建议汇总 |

## 模块位置

```
pkg/vm/engine/tae/db/gc/v3/
```

## 核心文件

- `types.go` - 类型定义与常量
- `diskcleaner.go` - 磁盘清理器主入口
- `checkpoint.go` - Checkpoint清理器实现
- `window.go` - GC窗口管理
- `executor.go` - GC执行器
- `exec_v1.go` - 基于Checkpoint的GC任务
- `merge.go` - Checkpoint合并逻辑
- `deleter.go` - 文件删除器
- `check.go` - GC验证检查器
- `util.go` - 工具函数

## 相关模块

- `pkg/vm/engine/tae/logtail/snapshot.go` - 快照元数据管理
- `pkg/vm/engine/tae/db/checkpoint/` - Checkpoint 模块

## 版本信息

当前版本: V3 (`CurrentVersion = uint16(3)`)
