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
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
)

// ExampleGCV4Usage 演示GC v4的基本用法
func ExampleGCV4Usage() error {
	ctx := context.Background()

	// 1. 创建配置
	config := DefaultConfig()
	config.DatabaseName = "mo_catalog"
	config.BatchSize = 500
	config.EnableMonitoring = true

	// 这里需要实际的engine实例
	// config.Engine = yourEngineInstance

	// 2. 创建元数据存储
	store, err := NewSystemTableMetadataStore(config)
	if err != nil {
		return fmt.Errorf("failed to create metadata store: %w", err)
	}

	// 3. 创建查询服务
	queryService := NewQueryService(store)

	// 4. 创建GC清理器
	// 这里需要实际的checkpoint runner
	// checkpointCli := yourCheckpointRunner
	// cleaner, err := NewSystemTableGCCleaner(ctx, config, checkpointCli)
	// if err != nil {
	//     return fmt.Errorf("failed to create GC cleaner: %w", err)
	// }

	// 5. 演示基本操作
	return demonstrateBasicOperations(ctx, store, queryService)
}

// demonstrateBasicOperations 演示基本操作
func demonstrateBasicOperations(ctx context.Context, store MetadataStore, queryService QueryService) error {
	// 1. 保存对象信息
	if err := demonstrateObjectOperations(ctx, store); err != nil {
		return fmt.Errorf("object operations failed: %w", err)
	}

	// 2. 保存快照信息
	if err := demonstrateSnapshotOperations(ctx, store); err != nil {
		return fmt.Errorf("snapshot operations failed: %w", err)
	}

	// 3. 管理水位线
	if err := demonstrateWatermarkOperations(ctx, store); err != nil {
		return fmt.Errorf("watermark operations failed: %w", err)
	}

	// 4. 查询统计信息
	if err := demonstrateQueryOperations(ctx, queryService); err != nil {
		return fmt.Errorf("query operations failed: %w", err)
	}

	return nil
}

// demonstrateObjectOperations 演示对象操作
func demonstrateObjectOperations(ctx context.Context, store MetadataStore) error {
	fmt.Println("=== Object Operations Demo ===")

	// 创建示例对象
	now := types.BuildTS(time.Now().UnixNano(), 0)
	objects := []ObjectInfo{
		{
			ObjectName:  "example_object_1",
			ObjectStats: []byte("mock_stats_1"), // 实际应该是objectio.ObjectStats
			CreateTS:    now,
			DeleteTS:    types.TS{}, // 空表示未删除
			DatabaseID:  1,
			TableID:     100,
			AccountID:   1,
			ObjectType:  ObjectTypeData,
			GCStatus:    GCStatusPending,
			TaskName:    "demo_task",
		},
		{
			ObjectName:  "example_object_2",
			ObjectStats: []byte("mock_stats_2"),
			CreateTS:    now,
			DeleteTS:    types.BuildTS(time.Now().Add(time.Hour).UnixNano(), 0),
			DatabaseID:  1,
			TableID:     101,
			AccountID:   1,
			ObjectType:  ObjectTypeTombstone,
			GCStatus:    GCStatusPending,
			TaskName:    "demo_task",
		},
	}

	// 保存对象
	if err := store.SaveObjects(ctx, objects); err != nil {
		return fmt.Errorf("failed to save objects: %w", err)
	}
	fmt.Printf("Saved %d objects\n", len(objects))

	// 查询对象
	filter := ObjectFilter{
		TableIDs:   []uint64{100, 101},
		GCStatuses: []GCStatus{GCStatusPending},
		Limit:      10,
	}

	queriedObjects, err := store.QueryObjects(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to query objects: %w", err)
	}
	fmt.Printf("Queried %d objects\n", len(queriedObjects))

	// 更新对象状态
	objectNames := []string{"example_object_1"}
	if err := store.UpdateObjectGCStatus(ctx, objectNames, GCStatusProcessed); err != nil {
		return fmt.Errorf("failed to update object status: %w", err)
	}
	fmt.Printf("Updated status for %d objects\n", len(objectNames))

	return nil
}

// demonstrateSnapshotOperations 演示快照操作
func demonstrateSnapshotOperations(ctx context.Context, store MetadataStore) error {
	fmt.Println("\n=== Snapshot Operations Demo ===")

	// 创建示例快照
	now := types.BuildTS(time.Now().UnixNano(), 0)
	accountID := uint32(1)
	tableID := uint64(100)

	snapshots := []SnapshotInfo{
		{
			SnapshotID:   "snap_001",
			SnapshotName: "daily_backup",
			SnapshotTS:   now,
			Level:        SnapshotLevelAccount,
			AccountID:    &accountID,
			AccountName:  "test_account",
			CreatedAt:    time.Now(),
		},
		{
			SnapshotID:   "snap_002",
			SnapshotName: "table_backup",
			SnapshotTS:   now,
			Level:        SnapshotLevelTable,
			AccountID:    &accountID,
			TableID:      &tableID,
			AccountName:  "test_account",
			TableName:    "test_table",
			CreatedAt:    time.Now(),
		},
	}

	// 保存快照
	if err := store.SaveSnapshots(ctx, snapshots); err != nil {
		return fmt.Errorf("failed to save snapshots: %w", err)
	}
	fmt.Printf("Saved %d snapshots\n", len(snapshots))

	// 查询快照
	filter := SnapshotFilter{
		Levels:     []SnapshotLevel{SnapshotLevelAccount, SnapshotLevelTable},
		AccountIDs: []uint32{1},
		Limit:      10,
	}

	queriedSnapshots, err := store.QuerySnapshots(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to query snapshots: %w", err)
	}
	fmt.Printf("Queried %d snapshots\n", len(queriedSnapshots))

	return nil
}

// demonstrateWatermarkOperations 演示水位线操作
func demonstrateWatermarkOperations(ctx context.Context, store MetadataStore) error {
	fmt.Println("\n=== Watermark Operations Demo ===")

	// 创建水位线
	now := types.BuildTS(time.Now().UnixNano(), 0)
	watermark := WatermarkInfo{
		WatermarkType:      WatermarkTypeScan,
		WatermarkTS:        now,
		CheckpointLocation: "ckp/checkpoint_001.ckp",
		CheckpointVersion:  1,
		TaskName:           "demo_scan_task",
	}

	// 保存水位线
	if err := store.SaveWatermark(ctx, watermark); err != nil {
		return fmt.Errorf("failed to save watermark: %w", err)
	}
	fmt.Printf("Saved watermark: %s\n", watermark.WatermarkType)

	// 加载水位线
	loadedWatermark, err := store.LoadWatermark(ctx, WatermarkTypeScan)
	if err != nil {
		return fmt.Errorf("failed to load watermark: %w", err)
	}

	if loadedWatermark != nil {
		fmt.Printf("Loaded watermark: %s at %s\n",
			loadedWatermark.WatermarkType,
			loadedWatermark.WatermarkTS.ToString())
	}

	// 更新水位线
	newTS := types.BuildTS(time.Now().Add(time.Hour).UnixNano(), 0)
	if err := store.UpdateWatermark(ctx, WatermarkTypeScan, newTS); err != nil {
		return fmt.Errorf("failed to update watermark: %w", err)
	}
	fmt.Printf("Updated watermark to: %s\n", newTS.ToString())

	return nil
}

// demonstrateQueryOperations 演示查询操作
func demonstrateQueryOperations(ctx context.Context, queryService QueryService) error {
	fmt.Println("\n=== Query Operations Demo ===")

	// 获取统计信息
	timeRange := TimeRange{
		Start: types.BuildTS(time.Now().Add(-24*time.Hour).UnixNano(), 0),
		End:   types.BuildTS(time.Now().UnixNano(), 0),
	}

	stats, err := queryService.GetGCStatistics(ctx, timeRange)
	if err != nil {
		return fmt.Errorf("failed to get statistics: %w", err)
	}

	fmt.Printf("GC Statistics:\n")
	fmt.Printf("  Total Objects: %d\n", stats.ObjectCount)
	fmt.Printf("  Pending GC: %d\n", stats.PendingGCCount)
	fmt.Printf("  Processed GC: %d\n", stats.ProcessedGCCount)
	fmt.Printf("  Total Snapshots: %d\n", stats.SnapshotCount)

	// 按状态获取对象计数
	objectCounts, err := queryService.GetObjectCountByStatus(ctx)
	if err != nil {
		return fmt.Errorf("failed to get object counts by status: %w", err)
	}

	fmt.Printf("Object Counts by Status:\n")
	for status, count := range objectCounts {
		fmt.Printf("  %s: %d\n", status, count)
	}

	// 获取水位线状态
	watermarkStatus, err := queryService.GetWatermarkStatus(ctx)
	if err != nil {
		return fmt.Errorf("failed to get watermark status: %w", err)
	}

	fmt.Printf("Watermark Status:\n")
	for wType, ts := range watermarkStatus {
		fmt.Printf("  %s: %s\n", wType, ts.ToString())
	}

	// 验证数据完整性
	report, err := queryService.ValidateDataIntegrity(ctx)
	if err != nil {
		return fmt.Errorf("failed to validate data integrity: %w", err)
	}

	fmt.Printf("Data Integrity: %t\n", report.IsValid)
	if len(report.Errors) > 0 {
		fmt.Printf("Errors: %v\n", report.Errors)
	}

	return nil
}

// ExampleMigration 演示v3到v4的迁移
func ExampleMigration() error {
	ctx := context.Background()

	// 创建迁移配置
	migrationConfig := DefaultMigrationConfig()
	migrationConfig.DryRun = true // 设置为dry run模式进行测试
	migrationConfig.ValidateData = true

	// 这里需要实际的文件服务和v4配置
	// migrationConfig.FileService = yourFileService
	// migrationConfig.V4Config.Engine = yourEngine

	// 创建迁移工具
	migrationTool, err := NewV3ToV4MigrationTool(migrationConfig)
	if err != nil {
		return fmt.Errorf("failed to create migration tool: %w", err)
	}

	// 执行迁移
	if err := migrationTool.Run(ctx); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}

	fmt.Println("Migration completed successfully!")
	return nil
}

// ExampleAdvancedUsage 演示高级用法
func ExampleAdvancedUsage() error {
	ctx := context.Background()

	// 1. 自定义配置
	config := &Config{
		DatabaseName:        "mo_catalog",
		BatchSize:           2000,
		MaxConcurrency:      8,
		QueryTimeout:        60 * time.Second,
		TransactionTimeout:  120 * time.Second,
		EnableCache:         true,
		CacheSize:           50000,
		CacheTTL:            10 * time.Minute,
		RetentionPeriod:     30 * 24 * time.Hour, // 30天
		CleanupInterval:     2 * time.Hour,
		MaxRetryAttempts:    5,
		EnableMonitoring:    true,
		MetricsInterval:     30 * time.Second,
		HealthCheckInterval: 2 * time.Minute,
	}

	// 2. 创建系统表存储
	store, err := NewSystemTableMetadataStore(config)
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	// 3. 事务操作示例
	if err := demonstrateTransactionOperations(ctx, store); err != nil {
		return fmt.Errorf("transaction operations failed: %w", err)
	}

	// 4. 批量操作示例
	if err := demonstrateBatchOperations(ctx, store); err != nil {
		return fmt.Errorf("batch operations failed: %w", err)
	}

	return nil
}

// demonstrateTransactionOperations 演示事务操作
func demonstrateTransactionOperations(ctx context.Context, store MetadataStore) error {
	fmt.Println("\n=== Transaction Operations Demo ===")

	// 开始事务
	tx, err := store.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	fmt.Printf("Started transaction: %s\n", tx.GetTxID())

	// 在事务中执行操作
	objects := []ObjectInfo{
		{
			ObjectName:  "tx_object_1",
			ObjectStats: []byte("tx_stats"),
			CreateTS:    types.BuildTS(time.Now().UnixNano(), 0),
			DatabaseID:  1,
			TableID:     200,
			AccountID:   1,
			ObjectType:  ObjectTypeData,
			GCStatus:    GCStatusPending,
			TaskName:    "tx_demo",
		},
	}

	if err := store.SaveObjects(ctx, objects); err != nil {
		// 出错时回滚
		store.RollbackTx(ctx, tx)
		return fmt.Errorf("failed to save objects in transaction: %w", err)
	}

	// 提交事务
	if err := store.CommitTx(ctx, tx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	fmt.Printf("Transaction committed successfully\n")
	return nil
}

// demonstrateBatchOperations 演示批量操作
func demonstrateBatchOperations(ctx context.Context, store MetadataStore) error {
	fmt.Println("\n=== Batch Operations Demo ===")

	// 创建大量对象
	batchSize := 1000
	objects := make([]ObjectInfo, batchSize)
	now := types.BuildTS(time.Now().UnixNano(), 0)

	for i := 0; i < batchSize; i++ {
		objects[i] = ObjectInfo{
			ObjectName:  fmt.Sprintf("batch_object_%d", i),
			ObjectStats: []byte(fmt.Sprintf("batch_stats_%d", i)),
			CreateTS:    now,
			DatabaseID:  1,
			TableID:     uint64(300 + i%10), // 分布到10个表
			AccountID:   1,
			ObjectType:  ObjectTypeData,
			GCStatus:    GCStatusPending,
			TaskName:    "batch_demo",
		}
	}

	// 批量保存
	start := time.Now()
	if err := store.SaveObjects(ctx, objects); err != nil {
		return fmt.Errorf("failed to save batch objects: %w", err)
	}
	duration := time.Since(start)

	fmt.Printf("Saved %d objects in %v (%.2f ops/sec)\n",
		batchSize, duration, float64(batchSize)/duration.Seconds())

	// 批量查询
	filter := ObjectFilter{
		TaskNames: []string{"batch_demo"},
		Limit:     batchSize,
	}

	start = time.Now()
	queriedObjects, err := store.QueryObjects(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to query batch objects: %w", err)
	}
	duration = time.Since(start)

	fmt.Printf("Queried %d objects in %v\n", len(queriedObjects), duration)

	return nil
}

// ExampleSystemIntegration 演示与系统的集成
func ExampleSystemIntegration(
	engine engine.Engine,
	checkpointCli checkpoint.Runner,
	fs fileservice.FileService,
) error {
	ctx := context.Background()

	// 1. 创建完整的v4配置
	config := DefaultConfig()
	config.Engine = engine
	config.DatabaseName = "mo_catalog"

	// 2. 创建GC清理器
	cleaner, err := NewSystemTableGCCleaner(ctx, config, checkpointCli)
	if err != nil {
		return fmt.Errorf("failed to create GC cleaner: %w", err)
	}
	defer cleaner.Stop()

	// 3. 重放现有元数据
	if err := cleaner.Replay(ctx); err != nil {
		return fmt.Errorf("failed to replay metadata: %w", err)
	}

	// 4. 添加自定义检查器
	cleaner.AddChecker(func(item any) bool {
		// 自定义GC检查逻辑
		if obj, ok := item.(ObjectInfo); ok {
			// 示例：不GC最近1小时创建的对象
			oneHourAgo := types.BuildTS(time.Now().Add(-time.Hour).UnixNano(), 0)
			return obj.CreateTS.LT(&oneHourAgo)
		}
		return true
	}, "recent_objects_checker")

	// 5. 处理检查点
	err = cleaner.Process(ctx, func(ckp *checkpoint.CheckpointEntry) bool {
		// 过滤检查点的逻辑
		oneWeekAgo := types.BuildTS(time.Now().Add(-7*24*time.Hour).UnixNano(), 0)
		return ckp.GetEnd().GT(&oneWeekAgo)
	})
	if err != nil {
		return fmt.Errorf("failed to process checkpoints: %w", err)
	}

	// 6. 执行GC检查
	if err := cleaner.DoCheck(ctx); err != nil {
		return fmt.Errorf("failed to do GC check: %w", err)
	}

	// 7. 获取统计信息
	stats := cleaner.GetStatistics()
	fmt.Printf("GC Statistics: %+v\n", stats)

	return nil
}
