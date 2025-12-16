# GC Metrics 优化总结

## 概述

本次优化主要针对 GC (Garbage Collection) 相关的 Prometheus metrics 进行了清理和增强，删除了未使用的 metrics，添加了新的 checkpoint 统计指标，并完善了 duration metrics 的埋点。

## 优化内容

### 1. 删除未使用的 Metrics

#### 1.1 GC File Size Histogram（全部删除）
- **原因**: 所有 file size histogram metrics 都没有在代码中使用
- **删除的 metrics**:
  - `gcFileSizeHistogram` 及其所有变量
  - `GCDataFileSizeHistogram`
  - `GCTombstoneFileSizeHistogram`
  - `GCCheckpointFileSizeHistogram`
  - `GCMetaFileSizeHistogram`
  - `GCSnapshotFileSizeHistogram`

#### 1.2 GC File Deletion（部分删除）
- **删除**: `GCTombstoneFileDeletionCounter` - 未使用
- **删除**: `GCLastTombstoneDeletionGauge` - 未使用

#### 1.3 GC Duration（部分删除）
- **删除**: `GCMergeScanDurationHistogram` - 未使用

#### 1.4 GC Table（部分删除）
- **删除**: `GCTableDeletedCounter` - 未使用
- **删除**: `GCTableSkippedCounter` - 未使用

#### 1.5 GC Snapshot & PITR（全部删除）
- **原因**: 所有 snapshot 和 PITR 相关的 metrics 都没有被使用（代码中被注释掉）
- **删除的 metrics**:
  - `gcSnapshotCounter` 及其所有变量
  - `GCSnapshotClusterCounter`
  - `GCSnapshotAccountCounter`
  - `GCSnapshotDatabaseCounter`
  - `GCSnapshotTableCounter`
  - `gcPitrCounter` 及其所有变量
  - `GCPitrClusterCounter`
  - `GCPitrAccountCounter`
  - `GCPitrDatabaseCounter`
  - `GCPitrTableCounter`

#### 1.6 GC Memory（部分删除）
- **删除**: `GCMemoryCacheGauge` - 未使用

#### 1.7 GC Error（部分删除）
- **删除**: `GCErrorFileNotFoundCounter` - 未使用
- **删除**: `GCErrorPermissionDeniedCounter` - 未使用
- **删除**: `GCErrorTimeoutCounter` - 未使用
- **删除**: `GCErrorContextCanceledCounter` - 未使用
- **保留**: `GCErrorIOErrorCounter` - 实际使用中

#### 1.8 GC Alert（全部删除）
- **原因**: 用户反馈不需要 GC Alerts 功能
- **删除的 metrics**:
  - `gcAlertGauge` 及其所有变量
  - `GCAlertNoDeletionGauge`
  - `GCAlertHighMemoryGauge`
  - `GCAlertSlowExecutionGauge`
  - `GCAlertErrorRateGauge`
- **删除的代码**: 所有使用这些 alert metrics 的代码

### 2. 新增 Checkpoint 统计 Metrics

#### 2.1 Checkpoint 处理统计
- **Metric**: `mo_gc_checkpoint_total{action}`
  - `action="merged"` - 合并的 checkpoint 数量
  - `action="deleted"` - 删除的 checkpoint 数量

#### 2.2 Checkpoint 行数统计
- **Metric**: `mo_gc_checkpoint_rows_total{type}`
  - `type="merged"` - 合并的 checkpoint 行数
  - `type="scanned"` - 扫描的 checkpoint 行数（近似值）

#### 2.3 埋点位置
- **`mergeCheckpointFilesLocked()`**:
  - 记录合并的 checkpoint 数量: `len(toMergeCheckpoint)`
  - 记录合并的 checkpoint 行数: `newCkpData.RowCount()`
  - 记录删除的 checkpoint 数量: `len(toMergeCheckpoint)`

- **`scanCheckpointsLocked()`**:
  - 记录扫描的 checkpoint 行数（使用 checkpoint 数量作为近似值）

### 3. 完善 Duration Metrics 埋点

#### 3.1 新增 Merge Table Duration Metric
- **Metric**: `GCMergeTableDurationHistogram`
- **用途**: 区分 merge checkpoint 和 merge table 的耗时
- **埋点位置**: `doGCAgainstGlobalCheckpointLocked()` 中的 `MergeTableInfo` 操作

#### 3.2 所有 Duration Metrics 埋点位置

| Metric | 埋点位置 | 说明 |
|--------|---------|------|
| `GCCheckpointTotalDurationHistogram` | `Process()` | GC Checkpoint 总耗时 |
| `GCCheckpointFilterDurationHistogram` | `doGCAgainstGlobalCheckpointLocked()` | Soft GC 耗时（filter 阶段） |
| `GCMergeTotalDurationHistogram` | `mergeCheckpointFilesLocked()` | Merge Checkpoint 耗时 |
| `GCMergeTableDurationHistogram` | `doGCAgainstGlobalCheckpointLocked()` | Merge Table 耗时（新增） |
| `GCSnapshotTotalDurationHistogram` | `tryGCLocked()` | GC Snapshot 总耗时 |

### 4. Dashboard 更新

#### 4.1 删除的内容
- 删除了 `initGCAlertsRow()` 函数
- 删除了对 `mo_gc_file_size_bytes_sum` 的引用

#### 4.2 新增的内容
- **`initGCCheckpointStatsRow()`**: 显示 checkpoint 统计信息
  - Checkpoints Merged
  - Checkpoints Deleted
  - Checkpoint Rows Merged
  - Checkpoint Rows Scanned

- **`initGCDurationMainRow()`**: 主要操作的 duration metrics
  - GC Checkpoint Total Duration
  - GC Soft GC Duration (Filter)
  - GC Snapshot Total Duration

- **`initGCDurationMergeRow()`**: Merge 操作的 duration metrics
  - GC Merge Checkpoint Duration
  - GC Merge Table Duration

- **`initGCTimestampRow()`**: 时间戳信息
  - GC Last Execution Time
  - GC Last Deletion Time

#### 4.3 修复的问题
- 修复了 metric 名称：从 `mo_gc_duration_bucket` 改为 `mo_gc_duration_seconds_bucket`

## 文件变更

### 修改的文件

1. **`pkg/util/metric/v2/gc.go`**
   - 删除了未使用的 metrics 定义
   - 添加了新的 checkpoint 统计 metrics
   - 删除了 GC Alert 相关 metrics

2. **`pkg/vm/engine/tae/db/gc/v3/checkpoint.go`**
   - 删除了 GC Alert 相关的代码
   - 添加了 checkpoint 统计的埋点
   - 添加了 merge table duration 的埋点

3. **`pkg/util/metric/v2/dashboard/grafana_dashboard_gc.go`**
   - 删除了 GC Alerts row
   - 添加了 Checkpoint Statistics row
   - 添加了 Duration Metrics rows（分为 Main 和 Merge 两部分）
   - 修复了 metric 名称

### 新增的文件

1. **`scripts/setup-grafana-macos.sh`**
   - macOS 上安装和配置 Grafana、Prometheus 的脚本

2. **`monitoring/README.md`**
   - Grafana 和 Prometheus 的详细配置指南

3. **`monitoring/verify-metrics.sh`**
   - 验证 MatrixOne metrics 端点的脚本

4. **`monitoring/test-gc-queries.sh`**
   - 测试 GC metrics Prometheus 查询的脚本

5. **`monitoring/diagnose-grafana.sh`**
   - Grafana Dashboard 诊断脚本

6. **`monitoring/GRAFANA_SETUP.md`**
   - Grafana 配置和故障排查指南

7. **`monitoring/fix-grafana-variables.md`**
   - Grafana 变量配置指南

## 使用指南

### 重新创建 Dashboard

在修改了 dashboard 代码后，需要重新执行测试来更新 Grafana 中的 dashboard：

```bash
# 对于本地部署
go test -v -run TestCreateLocalDashboard ./pkg/util/metric/v2/dashboard/... -timeout 30s

# 对于 Cloud 部署
go test -v -run TestCreateCloudDashboard ./pkg/util/metric/v2/dashboard/... -timeout 30s

# 对于 K8S 部署
go test -v -run TestCreateK8SDashboard ./pkg/util/metric/v2/dashboard/... -timeout 30s
```

### 验证 Metrics

1. **检查 metrics 端点**:
   ```bash
   curl http://10.222.1.50:7001/metrics | grep mo_gc
   ```

2. **验证 Prometheus 查询**:
   ```bash
   # 在 Grafana Explore 中测试
   histogram_quantile(0.50, sum(rate(mo_gc_duration_seconds_bucket{type="checkpoint",phase="total"}[5m])) by (le))
   ```

3. **运行诊断脚本**:
   ```bash
   cd monitoring
   ./diagnose-grafana.sh http://10.222.1.50:9090 10.222.1.50:7001
   ```

## 保留的 Metrics（实际使用中）

### Execution Metrics
- `mo_gc_execution_total{type, status}` - GC 执行总数
  - `type`: checkpoint, merge, snapshot
  - `status`: success, error

### File Deletion Metrics
- `mo_gc_file_deletion_total{type, reason}` - 删除的文件总数
  - `type`: data, checkpoint, meta, snapshot
  - `reason`: expired, merged, stale

### Duration Metrics
- `mo_gc_duration_seconds{type, phase}` - GC 执行耗时（直方图）
  - `type`: checkpoint, merge, snapshot
  - `phase`: scan, filter, collect, write, delete, total, table

### Object Metrics
- `mo_gc_object_total{type, action}` - 对象统计
  - `action`: scanned, deleted, protected, skipped

### Table Metrics
- `mo_gc_table_total{type, action}` - 表统计
  - `action`: scanned, protected

### Memory Metrics
- `mo_gc_memory_bytes{type}` - 内存使用
  - `type`: buffer, objects

### Queue Metrics
- `mo_gc_queue_size{type}` - 队列大小
  - `type`: pending, processing, completed

### Error Metrics
- `mo_gc_error_total{type, error}` - 错误统计
  - `type`: file, operation
  - `error`: io_error

### Checkpoint Statistics（新增）
- `mo_gc_checkpoint_total{action}` - Checkpoint 处理统计
  - `action`: merged, deleted

- `mo_gc_checkpoint_rows_total{type}` - Checkpoint 行数统计
  - `type`: merged, scanned

### Timestamp Metrics
- `mo_gc_last_execution_timestamp{type}` - 最后执行时间
- `mo_gc_last_deletion_timestamp{type}` - 最后删除时间

## 注意事项

1. **NaN 值**: 某些 metrics（如 filter, merge table）可能返回 NaN，这是正常的，表示这些操作还没有执行或数据不足。

2. **时间窗口**: `histogram_quantile` 查询需要足够的时间窗口，建议至少选择 5-15 分钟的时间范围。

3. **变量配置**: Dashboard 需要配置 `$interval` 和 `$instance` 变量才能正常工作。

4. **数据积累**: 某些 metrics 需要等待 GC 运行几次后才能看到数据。

## 后续优化建议

1. 考虑添加更多细粒度的 checkpoint 统计（如 checkpoint 大小、合并效率等）
2. 如果 merge table 操作频繁，可以考虑添加更详细的 phase 分解
3. 考虑添加 GC 效率相关的 metrics（如清理率、压缩比等）

## 相关文档

- [Grafana 配置指南](./monitoring/GRAFANA_SETUP.md)
- [Grafana 变量配置](./monitoring/fix-grafana-variables.md)
- [Monitoring README](./monitoring/README.md)




