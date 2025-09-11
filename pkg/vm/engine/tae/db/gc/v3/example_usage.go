// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gc

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

// ExampleOptimizedGCUsage demonstrates how to use the optimized GC system
func ExampleOptimizedGCUsage() {
	// 1. Create and configure the GC system
	config := DefaultGCConfig()
	config.CoarseEstimateRows = 5000000
	config.EnableDetailedLogging = true
	config.MaxConcurrentTasks = 4

	// Validate configuration
	if err := config.Validate(); err != nil {
		panic(err)
	}

	configManager := NewConfigManager(config)

	// 2. Create error handler for structured error handling
	errorHandler := NewErrorHandler("example-gc-task")

	// 3. Create resource pool for efficient resource management
	resourcePool := NewResourcePool()
	defer resourcePool.GetBatch() // This would be properly managed in real code

	// 4. Create a statistics collector
	statsCollector := NewStatisticsCollector()

	// 5. Example of using the new filter system
	exampleFilterUsage(errorHandler, statsCollector)

	// 6. Example of using batch data extraction
	exampleBatchProcessing(resourcePool, errorHandler)

	// 7. Example of using the configuration system
	exampleConfigurationUsage(configManager)

	// 8. Example of error handling
	exampleErrorHandling(errorHandler)

	// 9. Print statistics
	summary := statsCollector.GetSummary()
	_ = summary // Use summary as needed
	errorHandler.HandleInfo("GC execution completed")
}

func exampleFilterUsage(errorHandler *ErrorHandler, statsCollector *StatisticsCollector) {
	// Create mock data for demonstration
	ctx := context.Background()
	timestamp := types.BuildTS(time.Now().UnixNano(), 0)
	accountSnapshots := make(map[uint32][]types.TS)
	accountSnapshots[1] = []types.TS{timestamp}

	var pitr *logtail.PitrInfo
	var snapshotMeta *logtail.SnapshotMeta
	var iscpTables map[uint64]types.TS
	var transObjects map[string]map[uint64]*ObjectEntry

	// Create filter context
	filterCtx := NewFilterContext(
		&timestamp, accountSnapshots, pitr, snapshotMeta,
		iscpTables, transObjects, errorHandler,
	)

	// Build a comprehensive filter chain
	filterChain := NewFilterChainBuilder(errorHandler).
		WithLogic(FilterLogicAND).
		AddSnapshotFilter(filterCtx).
		AddISCPFilter(iscpTables).
		AddTimeBasedFilter(&timestamp).
		Build()

	// Create a mock object for filtering
	mockObject := ObjectReference{
		CreateTS: timestamp,
		DropTS:   types.TS{}, // Empty means not dropped
		DB:       1,
		Table:    100,
	}

	// Apply filter
	shouldGC, err := filterChain.ShouldGC(ctx, mockObject)
	if err != nil {
		errorHandler.HandleError(err, "filter_execution")
		return
	}

	// Record statistics
	if shouldGC {
		statsCollector.RecordObjectGCed(1024) // Mock size
	} else {
		statsCollector.RecordObjectSkipped(1024)
	}

	errorHandler.HandleInfo("Filter example completed")
}

func exampleBatchProcessing(resourcePool *ResourcePool, errorHandler *ErrorHandler) {
	// Get resources from pool
	extractor := resourcePool.GetExtractor()
	defer resourcePool.PutExtractor(extractor)

	// In real usage, you would have an actual batch
	// Here we just demonstrate the API
	errorHandler.HandleInfo("Batch processing example")
}

func exampleConfigurationUsage(configManager ConfigManager) {
	// Get current configuration
	config := configManager.GetConfig()
	originalCacheSize := config.CanGCCacheSize

	// Update configuration
	config.CanGCCacheSize = 128 * 1024 * 1024 // 128MB
	if err := configManager.UpdateConfig(config); err != nil {
		// Handle configuration error
		return
	}

	// Use configuration values
	cacheSize := configManager.GetCanGCCacheSize()
	maxTasks := configManager.GetMaxConcurrentTasks()

	// Restore original configuration for demo
	config.CanGCCacheSize = originalCacheSize
	configManager.UpdateConfig(config)

	// Log configuration usage
	_ = cacheSize
	_ = maxTasks
}

func exampleErrorHandling(errorHandler *ErrorHandler) {
	// Example of structured error handling
	operation := "example_operation"

	err := errorHandler.MeasureExecutionTime(operation, func() error {
		// Simulate some work
		time.Sleep(10 * time.Millisecond)
		return nil
	})

	if err != nil {
		// Error would be logged automatically
		return
	}

	// Example of warning handling
	errorHandler.HandleWarning("This is a warning message")

	// Example of info logging
	errorHandler.HandleInfo("Operation completed successfully")
}

// ExampleAdvancedGCUsage shows more advanced usage patterns
func ExampleAdvancedGCUsage() {
	// 1. Create a custom filter
	customFilter := &CustomFilter{
		BaseFilter: BaseFilter{
			name:     "CustomBusinessLogicFilter",
			priority: 10,
			enabled:  true,
		},
		businessRules: map[string]bool{
			"critical_table": false, // Never GC critical tables
		},
	}

	// 2. Create error collector for batch error handling
	errorCollector := NewErrorCollector("batch-operation")

	// Simulate multiple operations that might fail
	for i := 0; i < 5; i++ {
		err := simulateOperation(i)
		errorCollector.Add(err)
	}

	// Handle aggregated errors
	if errorCollector.HasErrors() {
		aggregatedErr := errorCollector.ToAggregatedError()
		// Handle the aggregated error
		_ = aggregatedErr
	}

	// 3. Use helper utilities
	objects := []ObjectReference{
		{CreateTS: types.BuildTS(1000, 0), Table: 1},
		{CreateTS: types.BuildTS(2000, 0), Table: 2},
		{CreateTS: types.BuildTS(1500, 0), Table: 3},
	}

	// Sort objects by creation time
	SortObjectsByCreateTime(objects)

	// Calculate total size
	totalSize := CalculateTotalSize(objects)
	_ = totalSize

	// Group by table
	groupedObjects := GroupObjectsByTable(objects)
	_ = groupedObjects
}

// CustomFilter demonstrates how to implement a custom filter
type CustomFilter struct {
	BaseFilter
	businessRules map[string]bool
}

func (cf *CustomFilter) ShouldGC(ctx context.Context, obj ObjectReference) (bool, error) {
	if !cf.enabled {
		return true, nil
	}

	// Apply custom business logic
	tableName := getTableName(obj.Table) // Mock function
	if canGC, exists := cf.businessRules[tableName]; exists {
		return canGC, nil
	}

	// Default behavior
	return true, nil
}

func getTableName(tableID uint64) string {
	// Mock implementation
	if tableID == 1 {
		return "critical_table"
	}
	return "regular_table"
}

func simulateOperation(i int) error {
	if i%2 == 0 {
		return nil // Success
	}
	return NewGCError(ErrCodeUnknown, "simulated error", nil)
}

// ExampleIntegrationWithExistingCode shows how to integrate with existing code
func ExampleIntegrationWithExistingCode(
	ctx context.Context,
	fs fileservice.FileService,
	mp *mpool.MPool,
) error {
	// 1. Create optimized GC job with new configuration
	config := DefaultGCConfig()
	config.CoarseEstimateRows = 10000000
	config.CoarseProbility = 0.00001

	gcJob := &CheckpointBasedGCJob{
		// Initialize with optimized components
		configManager:  NewConfigManager(config),
		errorHandler:   NewErrorHandler("integrated-gc-job"),
		resourcePool:   NewResourcePool(),
		statsCollector: NewStatisticsCollector(),
	}

	// 2. Use error handling wrapper
	return gcJob.errorHandler.MeasureExecutionTime("gc_execution", func() error {
		return gcJob.Execute(ctx)
	})
}

// Mock CheckpointBasedGCJob for demonstration
type CheckpointBasedGCJob struct {
	configManager  ConfigManager
	errorHandler   *ErrorHandler
	resourcePool   *ResourcePool
	statsCollector *StatisticsCollector
}

func (job *CheckpointBasedGCJob) Execute(ctx context.Context) error {
	// Mock implementation using optimized components
	job.statsCollector.RecordOperation("execute")

	// Use configuration
	cacheSize := job.configManager.GetCanGCCacheSize()
	_ = cacheSize

	// Use error handling
	return job.errorHandler.WrapWithRecovery("gc_execution", func() error {
		// Actual GC logic would go here
		return nil
	})
}
