# GC V3 模块架构概述

## 1. 模块简介

GC（Garbage Collection）模块是 MatrixOne TAE 存储引擎的核心组件之一，负责清理不再需要的数据对象和元数据文件，回收存储空间。V3 版本是当前最新的 GC 实现，采用基于 Checkpoint 的增量式垃圾回收策略。

## 2. 设计目标

- **高效性**: 通过 Bloom Filter 进行粗粒度过滤，减少不必要的扫描
- **安全性**: 支持快照（Snapshot）和 PITR（Point-In-Time Recovery）保护
- **可靠性**: 支持备份保护机制，防止备份期间数据被误删
- **可扩展性**: 模块化设计，支持多种过滤策略和删除策略

## 3. 整体架构

```
┌─────────────────────────────────────────────────────────────────┐
│                        DiskCleaner                               │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                    Process Queue                         │    │
│  │  (JT_GCExecute, JT_GCReplay, JT_GCForce, JT_GCNoop)    │    │
│  └─────────────────────────────────────────────────────────┘    │
│                              │                                   │
│                              ▼                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                 CheckpointCleaner                        │    │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  │    │
│  │  │  GCWindow   │  │ SnapshotMeta│  │ BackupProtection│  │    │
│  │  └─────────────┘  └─────────────┘  └─────────────────┘  │    │
│  └─────────────────────────────────────────────────────────┘    │
│                              │                                   │
│                              ▼                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │              CheckpointBasedGCJob                        │    │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  │    │
│  │  │CoarseFilter │  │ FineFilter  │  │   FinalSinker   │  │    │
│  │  │(BloomFilter)│  │(Snapshot/   │  │  (Delete List)  │  │    │
│  │  │             │  │ PITR Check) │  │                 │  │    │
│  │  └─────────────┘  └─────────────┘  └─────────────────┘  │    │
│  └─────────────────────────────────────────────────────────┘    │
│                              │                                   │
│                              ▼                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                      Deleter                             │    │
│  │           (Concurrent Batch File Deletion)               │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

## 4. 核心概念

### 4.1 水位线（Watermark）

GC 模块维护三个关键水位线：

| 水位线 | 描述 |
|--------|------|
| `scanWaterMark` | 已扫描的增量 Checkpoint 的结束时间戳 |
| `gcWaterMark` | 已完成 GC 的全局 Checkpoint 的结束时间戳 |
| `checkpointGCWaterMark` | Checkpoint 合并后的水位线，用于 Checkpoint Runner 清理 |

### 4.2 GC 窗口（GCWindow）

GC 窗口是一个时间范围内所有对象的集合，包含：
- 时间范围 `[start, end]`
- 对象统计信息列表 `files []objectio.ObjectStats`

### 4.3 对象条目（ObjectEntry）

```go
type ObjectEntry struct {
    stats    *objectio.ObjectStats  // 对象统计信息
    createTS types.TS               // 创建时间戳
    dropTS   types.TS               // 删除时间戳
    db       uint64                 // 数据库ID
    table    uint64                 // 表ID
}
```

## 5. 运行模式

DiskCleaner 支持两种运行模式：

| 模式 | 状态值 | 描述 |
|------|--------|------|
| Write 模式 | `StateStep_Write` | 主节点模式，执行完整的 GC 流程 |
| Replay 模式 | `StateStep_Replay` | 从节点模式，仅回放 GC 元数据 |

模式切换通过 `SwitchToWriteMode()` 和 `SwitchToReplayMode()` 方法实现。

## 6. 任务类型

```go
const (
    JT_GCNoop            // 空操作，用于刷新队列
    JT_GCExecute         // 执行 GC
    JT_GCReplay          // 回放 GC 元数据
    JT_GCReplayAndExecute // 回放后执行
    JT_GCForce           // 强制 GC（指定时间戳）
)
```

## 7. 关键流程

### 7.1 GC 主流程

1. **扫描阶段（Scan）**: 扫描增量 Checkpoint，收集对象信息到 GCWindow
2. **过滤阶段（Filter）**: 使用 Bloom Filter 粗过滤 + 快照/PITR 细过滤
3. **删除阶段（Delete）**: 并发批量删除可回收的文件
4. **合并阶段（Merge）**: 合并 Checkpoint 文件，更新水位线

### 7.2 保护机制

- **快照保护**: 被快照引用的对象不会被删除
- **PITR 保护**: PITR 时间点之后的对象不会被删除
- **备份保护**: 备份期间暂停所有 GC 操作
