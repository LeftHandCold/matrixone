# GC v3 Module Optimization

## 概述

本次对MatrixOne GC v3模块进行了全面的代码重构和优化，主要目标是提高代码的可读性、可维护性和可扩展性。

## 主要改进

### 1. 常量集中管理 (`constants.go`)

**优化前问题：**
- 魔数分散在各个文件中
- 重复定义相同的常量
- 缺乏统一的命名规范

**优化后改进：**
- 将所有常量集中到 `constants.go` 文件
- 按功能分组组织常量（版本、内存、执行、列索引等）
- 添加详细的注释说明
- 统一命名规范，提高可读性

```go
// 示例：集中的常量定义
const (
    // Version constants
    CurrentVersion = uint16(3)
    ObjectTableVersion = 0
    ObjectTablePrimaryKeyIdx = 0
)

const (
    // Memory size constants
    DefaultInMemoryStagedSize = mpool.MB * 32
    DefaultBufferSize = mpool.MB * 16
)
```

### 2. 接口抽象 (`interfaces.go`)

**优化前问题：**
- 缺乏清晰的接口定义
- 组件间耦合度高
- 难以进行单元测试

**优化后改进：**
- 定义了完整的接口体系
- 支持不同的GC执行策略
- 提供了组合模式支持
- 便于测试和扩展

```go
// 示例：清晰的接口定义
type ObjectFilter interface {
    ShouldGC(ctx context.Context, obj ObjectReference) (bool, error)
    Name() string
    Priority() int
}

type GCExecutionStrategy interface {
    Execute(ctx context.Context, gcCtx *GCContext, window *GCWindow) (*FilterResult, error)
    Name() string
}
```

### 3. 结构化错误处理 (`errors.go`)

**优化前问题：**
- 错误处理不统一
- 缺乏错误分类和上下文信息
- 调试困难

**优化后改进：**
- 实现了结构化的错误类型系统
- 提供了详细的错误上下文
- 支持错误聚合和重试判断
- 统一的错误日志格式

```go
// 示例：结构化错误处理
type GCError struct {
    Code      GCErrorCode
    Message   string
    Cause     error
    Context   map[string]interface{}
    Timestamp time.Time
    TaskName  string
}

func NewCheckpointReadError(cause error, checkpointName string) *GCError {
    return NewGCError(ErrCodeCheckpointRead, "failed to read checkpoint", cause).
        WithContext("checkpoint", checkpointName)
}
```

### 4. 过滤器链重构 (`filters.go`)

**优化前问题：**
- 过滤逻辑复杂且难以理解
- 缺乏模块化设计
- 难以添加新的过滤规则

**优化后改进：**
- 实现了组合式过滤器设计
- 每个过滤器职责单一且清晰
- 支持链式构建和灵活组合
- 提供了批处理优化

```go
// 示例：清晰的过滤器实现
type SnapshotFilter struct {
    BaseFilter
    filterCtx *FilterContext
}

func (sf *SnapshotFilter) ShouldGC(ctx context.Context, obj ObjectReference) (bool, error) {
    snapshots := sf.filterCtx.TableSnapshots[obj.Table]
    pitr := sf.filterCtx.TablePITRs[obj.Table]
    
    isReferenced := logtail.ObjectIsSnapshotRefers(
        obj.Stats, pitr, &obj.CreateTS, &obj.DropTS, snapshots,
    )
    
    return !isReferenced, nil
}
```

### 5. 配置管理优化 (`config.go`)

**优化前问题：**
- 配置分散且难以管理
- 缺乏默认值和验证机制
- 运行时配置变更困难

**优化后改进：**
- 统一的配置结构体
- 完整的默认值系统
- 严格的配置验证
- 支持运行时配置更新

```go
// 示例：完整的配置管理
type GCConfig struct {
    // Core GC settings
    Enabled                     bool          `json:"enabled" yaml:"enabled"`
    CheckEnabled                bool          `json:"check_enabled" yaml:"check_enabled"`
    
    // Performance tuning
    CoarseEstimateRows          int           `json:"coarse_estimate_rows" yaml:"coarse_estimate_rows"`
    CoarseProbility             float64       `json:"coarse_probility" yaml:"coarse_probility"`
    
    // ... 更多配置项
}

func (gc *GCConfig) Validate() error {
    if gc.CoarseEstimateRows <= 0 {
        return NewConfigValidationError(
            fmt.Errorf("coarse_estimate_rows must be positive, got %d", gc.CoarseEstimateRows),
            "coarse_estimate_rows",
        )
    }
    // ... 更多验证逻辑
}
```

### 6. 公共工具函数 (`utils.go`)

**优化前问题：**
- 重复代码多
- 缺乏通用的数据提取和处理工具
- 资源管理不统一

**优化后改进：**
- 提供了专用的数据提取器
- 实现了资源池管理
- 添加了统计收集器
- 统一的工具函数库

```go
// 示例：专用的数据提取器
type BatchDataExtractor struct {
    *ObjectStatsExtractor
    *TimestampExtractor
    *TableIDExtractor
}

func (bde *BatchDataExtractor) ExtractObjectReference(bat *batch.Batch, rowIdx int) ObjectReference {
    stats := bde.ExtractStats(bat, rowIdx)
    createTS, deleteTS := bde.ExtractTimestamps(bat, rowIdx)
    dbID, tableID := bde.ExtractIDs(bat, rowIdx)
    
    return ObjectReference{
        Stats:    &stats,
        CreateTS: createTS,
        DropTS:   deleteTS,
        DB:       dbID,
        Table:    tableID,
    }
}
```

## 架构改进

### 模块化设计

```
gc/v3/
├── constants.go    # 常量定义
├── interfaces.go   # 接口定义
├── errors.go       # 错误处理
├── config.go       # 配置管理
├── filters.go      # 过滤器实现
├── utils.go        # 工具函数
├── types.go        # 类型定义（保留原有）
├── executor.go     # 执行器（保留原有）
├── checkpoint.go   # 检查点处理（保留原有）
├── window.go       # 窗口管理（保留原有）
└── ...            # 其他文件
```

### 分层架构

1. **接口层**: 定义统一的接口契约
2. **实现层**: 具体的功能实现
3. **工具层**: 通用工具和辅助函数
4. **配置层**: 配置管理和验证

## 使用示例

### 创建过滤器链

```go
errorHandler := NewErrorHandler("gc-task-001")
filterCtx := NewFilterContext(
    timestamp, accountSnapshots, pitr, 
    snapshotMeta, iscpTables, transObjects, errorHandler,
)

filter := NewFilterChainBuilder(errorHandler).
    WithLogic(FilterLogicAND).
    AddCheckpointFilter(bloomFilter).
    AddSnapshotFilter(filterCtx).
    AddISCPFilter(iscpTables).
    AddTimeBasedFilter(timestamp).
    Build()

processor := NewBatchFilterProcessor(filter, errorHandler)
err := processor.ProcessBatch(ctx, bitmap, batch, memoryPool)
```

### 配置管理

```go
config := DefaultGCConfig()
config.CoarseEstimateRows = 5000000
config.EnableDetailedLogging = true

if err := config.Validate(); err != nil {
    return err
}

configManager := NewConfigManager(config)
```

### 错误处理

```go
errorHandler := NewErrorHandler("gc-cleanup")

err := errorHandler.MeasureExecutionTime("cleanup_files", func() error {
    return deleteFiles(ctx, filesToDelete)
})

if err != nil {
    errorHandler.HandleError(err, "file_cleanup",
        zap.Int("file_count", len(filesToDelete)),
    )
}
```

## 性能优化

1. **资源池化**: 减少对象分配开销
2. **批处理**: 提高数据处理效率
3. **并发控制**: 支持可配置的并发度
4. **内存管理**: 统一的内存池管理

## 可维护性提升

1. **代码模块化**: 单一职责，高内聚低耦合
2. **接口驱动**: 便于测试和扩展
3. **配置化**: 运行时可调整行为
4. **可观测性**: 详细的日志和指标

## 向后兼容性

- 保留了所有现有的公共API
- 现有的调用代码无需修改
- 新功能通过可选参数提供

## 未来扩展

优化后的架构为以下扩展提供了良好的基础：

1. **插件化过滤器**: 支持动态加载过滤规则
2. **多策略执行**: 支持不同的GC策略
3. **指标收集**: 集成监控和告警
4. **分布式GC**: 支持集群级别的垃圾回收

## 总结

这次重构显著提升了GC v3模块的代码质量：

- **可读性**: 清晰的模块划分和命名规范
- **可维护性**: 模块化设计和统一的错误处理
- **可扩展性**: 接口驱动的架构设计
- **可靠性**: 完善的配置验证和错误处理
- **性能**: 资源池化和批处理优化

通过这些改进，GC模块变得更加健壮、高效，并为未来的功能扩展奠定了坚实的基础。 