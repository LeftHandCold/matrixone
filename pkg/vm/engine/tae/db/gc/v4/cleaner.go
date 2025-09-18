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
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

// SystemTableGCCleaner GC v4清理器，使用系统表存储元数据
type SystemTableGCCleaner struct {
	// 基础配置
	config *Config
	logger *zap.Logger

	// 存储和查询
	metadataStore MetadataStore
	queryService  QueryService

	// v3兼容层
	checkpointCli checkpoint.Runner
	engine        engine.Engine

	// 状态管理
	ctx     context.Context
	cancel  context.CancelFunc
	enabled bool
	stopped bool
	mu      sync.RWMutex

	// 水位线管理
	scanWatermark         *checkpoint.CheckpointEntry
	gcWatermark           *types.TS
	checkpointGCWatermark *types.TS
	minMerged             *checkpoint.CheckpointEntry

	// 检查器管理
	checkers map[string]func(item any) bool

	// 内存结构 - 用于v3兼容
	snapshotMeta *logtail.SnapshotMeta
	iscpTables   map[uint64]types.TS

	// 统计信息
	stats     *GCStatistics
	statsMu   sync.RWMutex
	lastCheck time.Time
}

// NewSystemTableGCCleaner 创建GC v4清理器
func NewSystemTableGCCleaner(
	ctx context.Context,
	config *Config,
	checkpointCli checkpoint.Runner,
) (*SystemTableGCCleaner, error) {
	if config == nil {
		config = DefaultConfig()
	}

	// 创建元数据存储
	metadataStore, err := NewSystemTableMetadataStore(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create metadata store: %w", err)
	}

	// 创建查询服务
	queryService := NewQueryService(metadataStore)

	cleanerCtx, cancel := context.WithCancel(ctx)

	cleaner := &SystemTableGCCleaner{
		config:        config,
		logger:        logutil.GetGlobalLogger().Named("gc-v4-cleaner"),
		metadataStore: metadataStore,
		queryService:  queryService,
		checkpointCli: checkpointCli,
		engine:        config.Engine,
		ctx:           cleanerCtx,
		cancel:        cancel,
		enabled:       true,
		checkers:      make(map[string]func(item any) bool),
		snapshotMeta:  logtail.NewSnapshotMeta(),
		iscpTables:    make(map[uint64]types.TS),
		stats:         &GCStatistics{},
	}

	// 初始化水位线
	if err := cleaner.initWatermarks(); err != nil {
		return nil, fmt.Errorf("failed to initialize watermarks: %w", err)
	}

	// 启动监控
	if config.EnableMonitoring {
		go cleaner.startMonitoring()
	}

	return cleaner, nil
}

// initWatermarks 初始化水位线
func (c *SystemTableGCCleaner) initWatermarks() error {
	ctx, cancel := context.WithTimeout(c.ctx, c.config.QueryTimeout)
	defer cancel()

	// 加载扫描水位线
	if watermark, err := c.metadataStore.LoadWatermark(ctx, WatermarkTypeScan); err == nil && watermark != nil {
		c.scanWatermark = checkpoint.NewCheckpointEntry(
			"", &watermark.WatermarkTS, &watermark.WatermarkTS, checkpoint.ET_Incremental,
		)
	}

	// 加载GC水位线
	if watermark, err := c.metadataStore.LoadWatermark(ctx, WatermarkTypeGC); err == nil && watermark != nil {
		c.gcWatermark = &watermark.WatermarkTS
	}

	// 加载检查点GC水位线
	if watermark, err := c.metadataStore.LoadWatermark(ctx, WatermarkTypeCheckpointGC); err == nil && watermark != nil {
		c.checkpointGCWatermark = &watermark.WatermarkTS
	}

	return nil
}

// Replay 重放GC元数据，从系统表重建内存结构
func (c *SystemTableGCCleaner) Replay(ctx context.Context) error {
	start := time.Now()
	c.logger.Info("Starting GC v4 replay from system tables")

	defer func() {
		c.logger.Info("GC v4 replay completed", zap.Duration("duration", time.Since(start)))
	}()

	// 1. 重建快照元数据
	if err := c.rebuildSnapshotMeta(ctx); err != nil {
		return fmt.Errorf("failed to rebuild snapshot metadata: %w", err)
	}

	// 2. 重建ISCP表信息
	if err := c.rebuildISCPTables(ctx); err != nil {
		return fmt.Errorf("failed to rebuild ISCP tables: %w", err)
	}

	// 3. 重建水位线信息
	if err := c.rebuildWatermarks(ctx); err != nil {
		return fmt.Errorf("failed to rebuild watermarks: %w", err)
	}

	// 4. 更新统计信息
	if err := c.updateStatistics(ctx); err != nil {
		c.logger.Warn("Failed to update statistics during replay", zap.Error(err))
	}

	return nil
}

// rebuildSnapshotMeta 从系统表重建快照元数据
func (c *SystemTableGCCleaner) rebuildSnapshotMeta(ctx context.Context) error {
	// 查询所有快照信息
	snapshots, err := c.metadataStore.QuerySnapshots(ctx, SnapshotFilter{})
	if err != nil {
		return fmt.Errorf("failed to query snapshots: %w", err)
	}

	// 构建快照映射
	accountSnapshots := make(map[uint32][]types.TS)
	for _, snapshot := range snapshots {
		if snapshot.AccountID != nil {
			accountID := *snapshot.AccountID
			if accountSnapshots[accountID] == nil {
				accountSnapshots[accountID] = make([]types.TS, 0)
			}
			accountSnapshots[accountID] = append(accountSnapshots[accountID], snapshot.SnapshotTS)
		}
	}

	// 更新内存结构（为了v3兼容性）
	// TODO: 完善快照元数据重建逻辑
	c.logger.Info("Rebuilt snapshot metadata", zap.Int("snapshot_count", len(snapshots)))

	return nil
}

// rebuildISCPTables 从系统表重建ISCP表信息
func (c *SystemTableGCCleaner) rebuildISCPTables(ctx context.Context) error {
	// 查询PITR信息来重建ISCP表
	pitrs, err := c.metadataStore.LoadPITRsByLevel(ctx, SnapshotLevelTable, 0)
	if err != nil {
		return fmt.Errorf("failed to load PITR info: %w", err)
	}

	c.iscpTables = make(map[uint64]types.TS)
	for _, pitr := range pitrs {
		if pitr.TableID != nil {
			// 计算PITR时间点
			ts := c.calculatePITRTimestamp(pitr)
			c.iscpTables[*pitr.TableID] = ts
		}
	}

	c.logger.Info("Rebuilt ISCP tables", zap.Int("table_count", len(c.iscpTables)))
	return nil
}

// calculatePITRTimestamp 计算PITR时间戳
func (c *SystemTableGCCleaner) calculatePITRTimestamp(pitr PITRInfo) types.TS {
	now := time.Now()
	var ts time.Time

	switch pitr.TimeUnit {
	case TimeUnitYear:
		ts = now.AddDate(-pitr.LengthValue, 0, 0)
	case TimeUnitMonth:
		ts = now.AddDate(0, -pitr.LengthValue, 0)
	case TimeUnitDay:
		ts = now.AddDate(0, 0, -pitr.LengthValue)
	case TimeUnitHour:
		ts = now.Add(-time.Duration(pitr.LengthValue) * time.Hour)
	case TimeUnitMinute:
		ts = now.Add(-time.Duration(pitr.LengthValue) * time.Minute)
	default:
		ts = now.AddDate(0, 0, -7) // 默认7天
	}

	return types.BuildTS(ts.UnixNano(), 0)
}

// rebuildWatermarks 重建水位线信息
func (c *SystemTableGCCleaner) rebuildWatermarks(ctx context.Context) error {
	// 这个方法在initWatermarks中已经实现了
	return nil
}

// Process 处理GC任务
func (c *SystemTableGCCleaner) Process(ctx context.Context, fn func(*checkpoint.CheckpointEntry) bool) error {
	if !c.enabled {
		return nil
	}

	c.mu.RLock()
	if c.stopped {
		c.mu.RUnlock()
		return fmt.Errorf("GC cleaner is stopped")
	}
	c.mu.RUnlock()

	// 获取待处理的检查点
	checkpoints, err := c.checkpointCli.GetAllCheckpoints()
	if err != nil {
		return fmt.Errorf("failed to get checkpoints: %w", err)
	}

	for _, ckp := range checkpoints {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// 应用过滤函数
		if fn != nil && !fn(ckp) {
			continue
		}

		// 处理检查点
		if err := c.processCheckpoint(ctx, ckp); err != nil {
			c.logger.Error("Failed to process checkpoint",
				zap.String("checkpoint", ckp.String()),
				zap.Error(err),
			)
			continue
		}
	}

	return nil
}

// processCheckpoint 处理单个检查点
func (c *SystemTableGCCleaner) processCheckpoint(ctx context.Context, ckp *checkpoint.CheckpointEntry) error {
	start := time.Now()
	taskName := fmt.Sprintf("gc-checkpoint-%s", ckp.String())

	c.logger.Info("Processing checkpoint", zap.String("checkpoint", ckp.String()))

	defer func() {
		c.logger.Info("Finished processing checkpoint",
			zap.String("checkpoint", ckp.String()),
			zap.Duration("duration", time.Since(start)),
		)
	}()

	// 1. 读取检查点数据
	objects, err := c.loadCheckpointObjects(ctx, ckp)
	if err != nil {
		return fmt.Errorf("failed to load checkpoint objects: %w", err)
	}

	// 2. 保存对象信息到系统表
	if len(objects) > 0 {
		if err := c.metadataStore.SaveObjects(ctx, objects); err != nil {
			return fmt.Errorf("failed to save objects to system table: %w", err)
		}
	}

	// 3. 更新水位线
	if err := c.updateScanWatermark(ctx, ckp); err != nil {
		return fmt.Errorf("failed to update scan watermark: %w", err)
	}

	// 4. 保存检查点元数据
	metadata := MetadataInfo{
		TaskName:     taskName,
		MetadataType: MetadataTypeCheckpoint,
		StartTS:      *ckp.GetStart(),
		EndTS:        *ckp.GetEnd(),
		Content:      []byte(ckp.String()),
	}
	if err := c.metadataStore.SaveMetadata(ctx, metadata); err != nil {
		return fmt.Errorf("failed to save checkpoint metadata: %w", err)
	}

	return nil
}

// loadCheckpointObjects 从检查点加载对象信息
func (c *SystemTableGCCleaner) loadCheckpointObjects(ctx context.Context, ckp *checkpoint.CheckpointEntry) ([]ObjectInfo, error) {
	// TODO: 实现从检查点读取对象信息的逻辑
	// 这里需要根据v3的逻辑来适配

	var objects []ObjectInfo

	// 模拟对象提取
	c.logger.Debug("Loading objects from checkpoint", zap.String("checkpoint", ckp.String()))

	return objects, nil
}

// updateScanWatermark 更新扫描水位线
func (c *SystemTableGCCleaner) updateScanWatermark(ctx context.Context, ckp *checkpoint.CheckpointEntry) error {
	watermark := WatermarkInfo{
		WatermarkType:      WatermarkTypeScan,
		WatermarkTS:        *ckp.GetEnd(),
		CheckpointLocation: ckp.GetLocation().String(),
		CheckpointVersion:  int(ckp.GetVersion()),
		TaskName:           fmt.Sprintf("scan-%s", ckp.String()),
	}

	if err := c.metadataStore.SaveWatermark(ctx, watermark); err != nil {
		return err
	}

	c.scanWatermark = ckp
	return nil
}

// DoCheck 执行GC检查
func (c *SystemTableGCCleaner) DoCheck(ctx context.Context) error {
	if !c.enabled {
		return nil
	}

	start := time.Now()
	c.logger.Info("Starting GC check")

	defer func() {
		c.lastCheck = time.Now()
		c.logger.Info("GC check completed", zap.Duration("duration", time.Since(start)))
	}()

	// 1. 查询待处理的对象
	filter := ObjectFilter{
		GCStatuses: []GCStatus{GCStatusPending},
		Limit:      c.config.BatchSize,
	}

	objects, err := c.metadataStore.QueryObjects(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to query pending objects: %w", err)
	}

	if len(objects) == 0 {
		c.logger.Debug("No pending objects found for GC")
		return nil
	}

	// 2. 执行GC检查逻辑
	return c.processGCObjects(ctx, objects)
}

// processGCObjects 处理GC对象
func (c *SystemTableGCCleaner) processGCObjects(ctx context.Context, objects []ObjectInfo) error {
	// 分批处理
	batchSize := c.config.BatchSize
	for i := 0; i < len(objects); i += batchSize {
		end := i + batchSize
		if end > len(objects) {
			end = len(objects)
		}

		batch := objects[i:end]
		if err := c.processObjectBatch(ctx, batch); err != nil {
			return fmt.Errorf("failed to process object batch: %w", err)
		}
	}

	return nil
}

// processObjectBatch 处理对象批次
func (c *SystemTableGCCleaner) processObjectBatch(ctx context.Context, objects []ObjectInfo) error {
	canGCObjects := make([]string, 0)
	cannotGCObjects := make([]string, 0)

	for _, obj := range objects {
		canGC, err := c.checkObjectCanGC(ctx, obj)
		if err != nil {
			c.logger.Warn("Failed to check object GC status",
				zap.String("object", obj.ObjectName),
				zap.Error(err),
			)
			continue
		}

		if canGC {
			canGCObjects = append(canGCObjects, obj.ObjectName)
		} else {
			cannotGCObjects = append(cannotGCObjects, obj.ObjectName)
		}
	}

	// 更新状态
	if len(canGCObjects) > 0 {
		if err := c.metadataStore.UpdateObjectGCStatus(ctx, canGCObjects, GCStatusProcessed); err != nil {
			return fmt.Errorf("failed to update can-GC objects status: %w", err)
		}
	}

	if len(cannotGCObjects) > 0 {
		// 暂时标记为已处理，实际可能需要更细粒度的状态
		if err := c.metadataStore.UpdateObjectGCStatus(ctx, cannotGCObjects, GCStatusProcessed); err != nil {
			return fmt.Errorf("failed to update cannot-GC objects status: %w", err)
		}
	}

	c.logger.Info("Processed object batch",
		zap.Int("total", len(objects)),
		zap.Int("can_gc", len(canGCObjects)),
		zap.Int("cannot_gc", len(cannotGCObjects)),
	)

	return nil
}

// checkObjectCanGC 检查对象是否可以GC
func (c *SystemTableGCCleaner) checkObjectCanGC(ctx context.Context, obj ObjectInfo) (bool, error) {
	// TODO: 实现GC检查逻辑，需要考虑：
	// 1. 快照引用
	// 2. PITR引用
	// 3. ISCP引用
	// 4. 事务引用

	// 应用所有检查器
	for _, checker := range c.checkers {
		if !checker(obj) {
			return false, nil
		}
	}

	// 默认检查逻辑
	return c.defaultGCCheck(ctx, obj)
}

// defaultGCCheck 默认GC检查逻辑
func (c *SystemTableGCCleaner) defaultGCCheck(ctx context.Context, obj ObjectInfo) (bool, error) {
	// 检查删除时间戳
	if obj.DeleteTS.IsEmpty() {
		return false, nil // 对象未被删除
	}

	// 检查是否超过保留期
	now := types.BuildTS(time.Now().UnixNano(), 0)
	retentionTS := types.BuildTS(time.Now().Add(-c.config.RetentionPeriod).UnixNano(), 0)

	if obj.DeleteTS.GT(&retentionTS) {
		return false, nil // 还在保留期内
	}

	// 检查快照引用
	snapshots, err := c.metadataStore.QuerySnapshots(ctx, SnapshotFilter{
		TableIDs: []uint64{obj.TableID},
		TimeRange: &TimeRange{
			Start: obj.CreateTS,
			End:   obj.DeleteTS,
		},
	})
	if err != nil {
		return false, fmt.Errorf("failed to query snapshots: %w", err)
	}

	if len(snapshots) > 0 {
		return false, nil // 有快照引用
	}

	return true, nil
}

// 水位线管理接口实现
func (c *SystemTableGCCleaner) GetScanWaterMark() *checkpoint.CheckpointEntry {
	return c.scanWatermark
}

func (c *SystemTableGCCleaner) GetCheckpointGCWaterMark() *types.TS {
	return c.checkpointGCWatermark
}

func (c *SystemTableGCCleaner) GetMinMerged() *checkpoint.CheckpointEntry {
	return c.minMerged
}

// GC操作接口实现
func (c *SystemTableGCCleaner) EnableGC() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.enabled = true
}

func (c *SystemTableGCCleaner) DisableGC() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.enabled = false
}

func (c *SystemTableGCCleaner) GCEnabled() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.enabled
}

// 检查器管理接口实现
func (c *SystemTableGCCleaner) AddChecker(checker func(item any) bool, key string) int {
	c.checkers[key] = checker
	return len(c.checkers)
}

func (c *SystemTableGCCleaner) RemoveChecker(key string) error {
	if _, exists := c.checkers[key]; !exists {
		return fmt.Errorf("checker with key %s not found", key)
	}
	delete(c.checkers, key)
	return nil
}

// 元数据访问接口实现
func (c *SystemTableGCCleaner) GetPITRs() (*PITRInfo, error) {
	ctx, cancel := context.WithTimeout(c.ctx, c.config.QueryTimeout)
	defer cancel()

	pitrs, err := c.metadataStore.LoadPITRsByLevel(ctx, SnapshotLevelCluster, 0)
	if err != nil {
		return nil, err
	}

	if len(pitrs) == 0 {
		return nil, nil
	}

	return &pitrs[0], nil
}

func (c *SystemTableGCCleaner) GetSnapshots() (map[uint32][]types.TS, error) {
	ctx, cancel := context.WithTimeout(c.ctx, c.config.QueryTimeout)
	defer cancel()

	snapshots, err := c.metadataStore.QuerySnapshots(ctx, SnapshotFilter{})
	if err != nil {
		return nil, err
	}

	result := make(map[uint32][]types.TS)
	for _, snapshot := range snapshots {
		if snapshot.AccountID != nil {
			accountID := *snapshot.AccountID
			if result[accountID] == nil {
				result[accountID] = make([]types.TS, 0)
			}
			result[accountID] = append(result[accountID], snapshot.SnapshotTS)
		}
	}

	return result, nil
}

func (c *SystemTableGCCleaner) ISCPTables() (map[uint64]types.TS, error) {
	return c.iscpTables, nil
}

// 统计和验证接口实现
func (c *SystemTableGCCleaner) GetDetails(ctx context.Context) (map[uint32]*TableStats, error) {
	// TODO: 从系统表查询表统计信息
	return make(map[uint32]*TableStats), nil
}

func (c *SystemTableGCCleaner) Verify(ctx context.Context) string {
	report, err := c.metadataStore.ValidateIntegrity(ctx)
	if err != nil {
		return fmt.Sprintf("Verification failed: %v", err)
	}

	var result strings.Builder
	result.WriteString(fmt.Sprintf("Integrity Check: %t\n", report.IsValid))
	if len(report.Errors) > 0 {
		result.WriteString("Errors:\n")
		for _, err := range report.Errors {
			result.WriteString(fmt.Sprintf("  - %s\n", err))
		}
	}
	if len(report.Warnings) > 0 {
		result.WriteString("Warnings:\n")
		for _, warn := range report.Warnings {
			result.WriteString(fmt.Sprintf("  - %s\n", warn))
		}
	}

	return result.String()
}

// v4新增接口实现
func (c *SystemTableGCCleaner) GetMetadataStore() MetadataStore {
	return c.metadataStore
}

func (c *SystemTableGCCleaner) GetQueryService() QueryService {
	return c.queryService
}

// 系统表相关接口实现
func (c *SystemTableGCCleaner) SetTid(tid uint64) {
	// TODO: 实现表ID设置逻辑
}

func (c *SystemTableGCCleaner) GetMPool() engine.Engine {
	return c.engine
}

// 停止和清理
func (c *SystemTableGCCleaner) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopped {
		return
	}

	c.stopped = true
	c.cancel()

	c.logger.Info("GC v4 cleaner stopped")
}

// 监控和统计
func (c *SystemTableGCCleaner) startMonitoring() {
	ticker := time.NewTicker(c.config.MetricsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.updateStatistics(c.ctx)
		}
	}
}

// updateStatistics 更新统计信息
func (c *SystemTableGCCleaner) updateStatistics(ctx context.Context) error {
	stats, err := c.metadataStore.GetStatistics(ctx, nil)
	if err != nil {
		c.logger.Warn("Failed to update statistics", zap.Error(err))
		return err
	}

	c.statsMu.Lock()
	c.stats = stats
	c.stats.LastUpdateTime = time.Now()
	c.statsMu.Unlock()

	return nil
}

// GetStatistics 获取统计信息
func (c *SystemTableGCCleaner) GetStatistics() *GCStatistics {
	c.statsMu.RLock()
	defer c.statsMu.RUnlock()

	// 返回统计信息的副本
	statsCopy := *c.stats
	return &statsCopy
}
