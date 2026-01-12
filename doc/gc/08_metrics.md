# GC 监控指标

## 1. 概述

GC 模块集成了丰富的 Prometheus 监控指标，用于监控 GC 的执行状态、性能和错误情况。

## 2. 指标分类

### 2.1 执行计数器

| 指标名 | 类型 | 描述 |
|--------|------|------|
| `GCCheckpointExecutionCounter` | Counter | Checkpoint GC 执行次数 |
| `GCCheckpointExecutionErrorCounter` | Counter | Checkpoint GC 执行错误次数 |
| `GCSnapshotExecutionCounter` | Counter | 快照 GC 执行次数 |
| `GCSnapshotExecutionErrorCounter` | Counter | 快照 GC 执行错误次数 |
| `GCMergeExecutionCounter` | Counter | 合并执行次数 |
| `GCMergeExecutionErrorCounter` | Counter | 合并执行错误次数 |

### 2.2 对象计数器

| 指标名 | 类型 | 描述 |
|--------|------|------|
| `GCObjectScannedCounter` | Counter | 扫描的对象数量 |
| `GCObjectDeletedCounter` | Counter | 删除的对象数量 |
| `GCObjectSkippedCounter` | Counter | 跳过的对象数量（可GC但未删除） |
| `GCObjectProtectedCounter` | Counter | 被保护的对象数量 |
| `GCTableScannedCounter` | Counter | 扫描的表数量 |
| `GCTableProtectedCounter` | Counter | 被保护的表数量 |

### 2.3 Checkpoint 计数器

| 指标名 | 类型 | 描述 |
|--------|------|------|
| `GCCheckpointMergedCounter` | Counter | 合并的 Checkpoint 数量 |
| `GCCheckpointDeletedCounter` | Counter | 删除的 Checkpoint 数量 |
| `GCCheckpointRowsScannedCounter` | Counter | 扫描的 Checkpoint 行数 |
| `GCCheckpointRowsMergedCounter` | Counter | 合并的 Checkpoint 行数 |

### 2.4 文件删除计数器

| 指标名 | 类型 | 描述 |
|--------|------|------|
| `GCDataFileDeletionCounter` | Counter | 删除的数据文件数量 |
| `GCMetaFileDeletionCounter` | Counter | 删除的元数据文件数量 |
| `GCCheckpointFileDeletionCounter` | Counter | 删除的 Checkpoint 文件数量 |
| `GCSnapshotFileDeletionCounter` | Counter | 删除的快照文件数量 |

### 2.5 错误计数器

| 指标名 | 类型 | 描述 |
|--------|------|------|
| `GCErrorIOErrorCounter` | Counter | IO 错误次数 |

## 3. 时间直方图

### 3.1 总体耗时

| 指标名 | 类型 | 描述 |
|--------|------|------|
| `TaskGCDurationHistogram` | Histogram | GC 任务总耗时 |
| `TaskGCScanDurationHistogram` | Histogram | 扫描阶段耗时 |
| `TaskGCMergeCheckpointDurationHistogram` | Histogram | Checkpoint 合并耗时 |

### 3.2 详细阶段耗时

| 指标名 | 类型 | 描述 |
|--------|------|------|
| `GCCheckpointTotalDurationHistogram` | Histogram | Checkpoint GC 总耗时 |
| `GCCheckpointScanDurationHistogram` | Histogram | Checkpoint 扫描耗时 |
| `GCCheckpointFilterDurationHistogram` | Histogram | Checkpoint 过滤耗时 |
| `GCCheckpointDeleteDurationHistogram` | Histogram | Checkpoint 删除耗时 |
| `GCSnapshotTotalDurationHistogram` | Histogram | 快照 GC 总耗时 |
| `GCSnapshotScanDurationHistogram` | Histogram | 快照扫描耗时 |
| `GCSnapshotCollectDurationHistogram` | Histogram | 快照收集耗时 |
| `GCSnapshotDeleteDurationHistogram` | Histogram | 快照删除耗时 |
| `GCMergeTotalDurationHistogram` | Histogram | 合并总耗时 |
| `GCMergeCollectDurationHistogram` | Histogram | 合并收集耗时 |
| `GCMergeWriteDurationHistogram` | Histogram | 合并写入耗时 |
| `GCMergeTableDurationHistogram` | Histogram | 表合并耗时 |

## 4. 资源使用指标

### 4.1 内存使用

| 指标名 | 类型 | 描述 |
|--------|------|------|
| `GCMemoryObjectsGauge` | Gauge | GC 窗口中的对象数量 |
| `GCMemoryBufferGauge` | Gauge | 内存缓冲区大小 |

### 4.2 队列状态

| 指标名 | 类型 | 描述 |
|--------|------|------|
| `GCQueuePendingGauge` | Gauge | 待处理任务数 |
| `GCQueueProcessingGauge` | Gauge | 正在处理的任务数 |
| `GCQueueCompletedGauge` | Gauge | 已完成任务数 |

## 5. 指标使用示例

### 5.1 在代码中记录指标

```go
// 记录执行次数
v2.GCCheckpointExecutionCounter.Inc()

// 记录错误
v2.GCCheckpointExecutionErrorCounter.Inc()

// 记录耗时
start := time.Now()
// ... 执行操作
v2.TaskGCDurationHistogram.Observe(time.Since(start).Seconds())

// 记录对象数量
v2.GCObjectDeletedCounter.Add(float64(len(filesToGC)))

// 设置 Gauge
v2.GCMemoryObjectsGauge.Set(float64(len(w.files)))
```

### 5.2 Prometheus 查询示例

```promql
# GC 执行成功率
rate(gc_checkpoint_execution_total[5m]) / 
(rate(gc_checkpoint_execution_total[5m]) + rate(gc_checkpoint_execution_error_total[5m]))

# GC 平均耗时
histogram_quantile(0.95, rate(task_gc_duration_seconds_bucket[5m]))

# 每分钟删除的对象数
rate(gc_object_deleted_total[1m])

# 被保护的对象比例
gc_object_protected_total / gc_object_scanned_total
```

## 6. 告警建议

### 6.1 错误告警

```yaml
# GC 执行错误率过高
- alert: GCExecutionErrorRate
  expr: rate(gc_checkpoint_execution_error_total[5m]) > 0.1
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "GC execution error rate is high"

# IO 错误
- alert: GCIOError
  expr: increase(gc_error_io_error_total[5m]) > 0
  for: 1m
  labels:
    severity: critical
  annotations:
    summary: "GC IO error detected"
```

### 6.2 性能告警

```yaml
# GC 耗时过长
- alert: GCDurationHigh
  expr: histogram_quantile(0.95, rate(task_gc_duration_seconds_bucket[5m])) > 300
  for: 10m
  labels:
    severity: warning
  annotations:
    summary: "GC duration is too high"

# 待删除对象积压
- alert: GCObjectBacklog
  expr: gc_memory_objects > 1000000
  for: 30m
  labels:
    severity: warning
  annotations:
    summary: "GC object backlog is growing"
```

### 6.3 资源告警

```yaml
# 内存使用过高
- alert: GCMemoryHigh
  expr: gc_memory_buffer > 1073741824  # 1GB
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "GC memory buffer is too large"
```

## 7. 监控面板建议

### 7.1 概览面板

- GC 执行次数趋势
- GC 错误率
- GC 平均耗时
- 对象删除速率

### 7.2 详细面板

- 各阶段耗时分布
- 对象扫描/删除/保护比例
- Checkpoint 合并统计
- 文件删除统计

### 7.3 资源面板

- 内存使用趋势
- 队列状态
- 并发 worker 使用情况

## 8. 日志与指标关联

GC 模块的日志和指标可以通过以下方式关联：

1. **任务名称**: 日志中的 `task` 字段对应一次 GC 执行
2. **时间戳**: 日志时间和指标时间可以对应
3. **错误信息**: 日志中的错误详情补充指标的错误计数

### 8.1 关键日志

```
GC-TRACE-PROCESS          # GC 主流程
GC-TRACE-SCAN             # 扫描阶段
GC-TRACE-TRY-GC-AGAINST-GCKP  # GC 执行
GC-TRACE-MERGE-CHECKPOINT-FILES  # Checkpoint 合并
GC-ExecDelete-Done        # 文件删除完成
```
