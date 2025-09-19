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
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// SystemTableMetadataStore 基于系统表的元数据存储实现
type SystemTableMetadataStore struct {
	config      *Config
	engine      engine.Engine
	database    engine.Database
	sqlExecutor executor.SQLExecutor // 新增 SQL executor
	cache       map[string]interface{}
	cacheMu     sync.RWMutex
	logger      *zap.Logger
}

// NewSystemTableMetadataStore 创建系统表元数据存储
func NewSystemTableMetadataStore(config *Config) (*SystemTableMetadataStore, error) {
	if config.Engine == nil {
		return nil, fmt.Errorf("engine is required")
	}

	// 从全局变量获取 SQLExecutor
	v, ok := runtime.ServiceRuntime(config.UUID).GetGlobalVariables(runtime.InternalSQLExecutor)
	if !ok {
		return nil, fmt.Errorf("internal SQL executor not found, please ensure TN service is properly initialized")
	}
	sqlExecutor := v.(executor.SQLExecutor)

	store := &SystemTableMetadataStore{
		config:      config,
		engine:      config.Engine,
		sqlExecutor: sqlExecutor,
		cache:       make(map[string]interface{}),
		logger:      logutil.GetGlobalLogger().Named("gc-v4-store"),
	}

	// 初始化数据库连接
	if err := store.initDatabase(); err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	// 创建系统表
	if err := store.ensureTables(); err != nil {
		return nil, fmt.Errorf("failed to ensure tables: %w", err)
	}

	return store, nil
}

// initDatabase 初始化数据库连接
func (s *SystemTableMetadataStore) initDatabase() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.QueryTimeout)
	defer cancel()

	db, err := s.engine.Database(ctx, s.config.DatabaseName, nil)
	if err != nil {
		return fmt.Errorf("failed to get database %s: %w", s.config.DatabaseName, err)
	}

	s.database = db
	return nil
}

// ensureTables 确保系统表存在
func (s *SystemTableMetadataStore) ensureTables() error {
	tables := []string{
		SystemTableGCMetadata,
		SystemTableGCObjects,
		SystemTableGCSnapshots,
		SystemTableGCPITR,
		SystemTableGCWatermarks,
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.config.QueryTimeout)
	defer cancel()

	for _, tableName := range tables {
		if exists, err := s.tableExists(ctx, tableName); err != nil {
			return fmt.Errorf("failed to check table %s: %w", tableName, err)
		} else if !exists {
			if err := s.createTable(ctx, tableName); err != nil {
				return fmt.Errorf("failed to create table %s: %w", tableName, err)
			}
		}
	}

	return nil
}

// tableExists 检查表是否存在
func (s *SystemTableMetadataStore) tableExists(ctx context.Context, tableName string) (bool, error) {
	rel, err := s.database.Relation(ctx, tableName, nil)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return false, nil
		}
		return false, err
	}
	return rel != nil, nil
}

// createTable 创建表
func (s *SystemTableMetadataStore) createTable(ctx context.Context, tableName string) error {
	var createSQL string

	switch tableName {
	case SystemTableGCMetadata:
		createSQL = `
		CREATE TABLE mo_gc_metadata (
			id BIGINT PRIMARY KEY AUTO_INCREMENT,
			task_name VARCHAR(255) NOT NULL,
			metadata_type VARCHAR(50) NOT NULL,
			start_ts TIMESTAMP NOT NULL,
			end_ts TIMESTAMP NOT NULL,
			account_id INT,
			database_id BIGINT,
			table_id BIGINT,
			content LONGBLOB,
			watermark_type VARCHAR(50),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			INDEX idx_task_type (task_name, metadata_type),
			INDEX idx_time_range (start_ts, end_ts),
			INDEX idx_watermark (watermark_type, end_ts)
		)`
	case SystemTableGCObjects:
		createSQL = `
		CREATE TABLE mo_gc_objects (
			id BIGINT PRIMARY KEY AUTO_INCREMENT,
			object_name VARCHAR(512) NOT NULL,
			object_stats LONGBLOB NOT NULL,
			create_ts TIMESTAMP NOT NULL,
			delete_ts TIMESTAMP,
			database_id BIGINT NOT NULL,
			table_id BIGINT NOT NULL,
			account_id INT NOT NULL,
			object_type VARCHAR(50) NOT NULL,
			gc_status VARCHAR(50) DEFAULT 'pending',
			task_name VARCHAR(255),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			UNIQUE KEY uk_object_name (object_name),
			INDEX idx_table_time (table_id, create_ts, delete_ts),
			INDEX idx_gc_status (gc_status, created_at),
			INDEX idx_task_name (task_name)
		)`
	case SystemTableGCSnapshots:
		createSQL = `
		CREATE TABLE mo_gc_snapshots (
			id BIGINT PRIMARY KEY AUTO_INCREMENT,
			snapshot_id VARCHAR(255) NOT NULL,
			snapshot_name VARCHAR(255),
			snapshot_ts TIMESTAMP NOT NULL,
			level VARCHAR(50) NOT NULL,
			account_id INT,
			database_id BIGINT,
			table_id BIGINT,
			account_name VARCHAR(255),
			database_name VARCHAR(255),
			table_name VARCHAR(255),
			object_id BIGINT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			INDEX idx_snapshot_ts (snapshot_ts),
			INDEX idx_level_account (level, account_id),
			INDEX idx_table_snapshot (table_id, snapshot_ts)
		)`
	case SystemTableGCPITR:
		createSQL = `
		CREATE TABLE mo_gc_pitr (
			id BIGINT PRIMARY KEY AUTO_INCREMENT,
			pitr_id VARCHAR(255) NOT NULL,
			pitr_name VARCHAR(255),
			create_account VARCHAR(255),
			create_time TIMESTAMP NOT NULL,
			modified_time TIMESTAMP NOT NULL,
			level VARCHAR(50) NOT NULL,
			account_id INT,
			database_id BIGINT,
			table_id BIGINT,
			account_name VARCHAR(255),
			database_name VARCHAR(255),
			table_name VARCHAR(255),
			object_id BIGINT,
			length_value INT NOT NULL,
			time_unit VARCHAR(10) NOT NULL,
			drop_at TIMESTAMP,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			INDEX idx_pitr_level (level, account_id),
			INDEX idx_pitr_time (create_time, modified_time)
		)`
	case SystemTableGCWatermarks:
		createSQL = `
		CREATE TABLE mo_gc_watermarks (
			id BIGINT PRIMARY KEY AUTO_INCREMENT,
			watermark_type VARCHAR(50) NOT NULL,
			watermark_ts TIMESTAMP NOT NULL,
			checkpoint_location VARCHAR(1024),
			checkpoint_version INT,
			task_name VARCHAR(255),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			UNIQUE KEY uk_watermark_type (watermark_type),
			INDEX idx_watermark_ts (watermark_ts)
		)`
	default:
		return fmt.Errorf("unknown table name: %s", tableName)
	}

	// 执行创建表SQL
	s.logger.Info("Creating table", zap.String("table", tableName))

	// 使用 SQLExecutor 执行 SQL
	opts := executor.Options{}.WithAccountID(defines.GetAccountId(ctx))
	_, err := s.sqlExecutor.Exec(ctx, createSQL, opts)
	if err != nil {
		return fmt.Errorf("failed to execute create table SQL: %w", err)
	}

	s.logger.Info("Successfully created table", zap.String("table", tableName))
	return nil
}

// SaveObjects 保存对象信息
func (s *SystemTableMetadataStore) SaveObjects(ctx context.Context, objects []ObjectInfo) error {
	if len(objects) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, s.config.TransactionTimeout)
	defer cancel()

	// 开始事务
	tx, err := s.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if err != nil {
			s.RollbackTx(ctx, tx)
		}
	}()

	// 批量插入
	for i := 0; i < len(objects); i += s.config.BatchSize {
		end := i + s.config.BatchSize
		if end > len(objects) {
			end = len(objects)
		}

		batch := objects[i:end]
		if err = s.saveObjectsBatch(ctx, tx, batch); err != nil {
			return fmt.Errorf("failed to save objects batch: %w", err)
		}
	}

	// 提交事务
	if err = s.CommitTx(ctx, tx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// 清除相关缓存
	s.clearObjectCache()

	return nil
}

// saveObjectsBatch 保存对象批次
func (s *SystemTableMetadataStore) saveObjectsBatch(ctx context.Context, tx TxContext, objects []ObjectInfo) error {
	// 构建批量插入SQL
	values := make([]string, 0, len(objects))

	for _, obj := range objects {
		// 序列化对象统计信息
		statsBytes, err := json.Marshal(obj.ObjectStats)
		if err != nil {
			return fmt.Errorf("failed to marshal object stats: %w", err)
		}

		deleteTS := "NULL"
		if !obj.DeleteTS.IsEmpty() {
			deleteTS = fmt.Sprintf("'%s'", obj.DeleteTS.ToString())
		}

		value := fmt.Sprintf(
			"('%s', '%s', '%s', %s, %d, %d, %d, '%s', '%s', '%s')",
			obj.ObjectName,
			string(statsBytes),
			obj.CreateTS.ToString(),
			deleteTS,
			obj.DatabaseID,
			obj.TableID,
			obj.AccountID,
			obj.ObjectType,
			obj.GCStatus,
			obj.TaskName,
		)
		values = append(values, value)
	}

	insertSQL := fmt.Sprintf(`
		INSERT INTO %s (object_name, object_stats, create_ts, delete_ts, 
		               database_id, table_id, account_id, object_type, 
		               gc_status, task_name) 
		VALUES %s
		ON DUPLICATE KEY UPDATE
		object_stats = VALUES(object_stats),
		delete_ts = VALUES(delete_ts),
		gc_status = VALUES(gc_status),
		task_name = VALUES(task_name)`,
		SystemTableGCObjects,
		strings.Join(values, ","),
	)

	// 使用 SQLExecutor 执行 SQL（在事务中）
	opts := executor.Options{}.WithAccountID(defines.GetAccountId(ctx))
	if tx != nil {
		// 如果有事务上下文，需要使用事务执行
		// 注意：这里需要根据实际的事务实现来调整
		// 暂时使用普通执行，后续可以优化
	}

	_, err := s.sqlExecutor.Exec(ctx, insertSQL, opts)
	if err != nil {
		return fmt.Errorf("failed to execute insert SQL: %w", err)
	}

	s.logger.Debug("Saved objects batch",
		zap.Int("count", len(objects)),
		zap.String("tx_id", tx.GetTxID()),
	)

	return nil
}

// LoadObjectsByTimeRange 按时间范围加载对象
func (s *SystemTableMetadataStore) LoadObjectsByTimeRange(ctx context.Context, start, end types.TS) ([]ObjectInfo, error) {
	// 检查缓存
	cacheKey := fmt.Sprintf("objects_%s_%s", start.ToString(), end.ToString())
	if s.config.EnableCache {
		if cached := s.getFromCache(cacheKey); cached != nil {
			if objects, ok := cached.([]ObjectInfo); ok {
				return objects, nil
			}
		}
	}

	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()

	querySQL := fmt.Sprintf(`
		SELECT id, object_name, object_stats, create_ts, delete_ts,
		       database_id, table_id, account_id, object_type,
		       gc_status, task_name, created_at
		FROM %s
		WHERE create_ts >= '%s' AND create_ts <= '%s'
		ORDER BY create_ts ASC`,
		SystemTableGCObjects,
		start.ToString(),
		end.ToString(),
	)

	// 使用 SQLExecutor 执行查询
	opts := executor.Options{}.WithAccountID(defines.GetAccountId(ctx))
	result, err := s.sqlExecutor.Exec(ctx, querySQL, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	var objects []ObjectInfo
	// 解析查询结果
	if result.Batches != nil && len(result.Batches) > 0 {
		// TODO: 实现批次数据解析逻辑
		// 这需要根据 MatrixOne 的实际数据格式来实现
		s.logger.Debug("Query returned batches", zap.Int("count", len(result.Batches)))

		// 暂时返回空结果，实际使用时需要实现完整的解析逻辑
		// 解析逻辑应该包括：
		// 1. 遍历 result.Batches
		// 2. 提取每行数据
		// 3. 构造 ObjectInfo 对象
		// 4. 处理时间戳和JSON反序列化
	}

	// 缓存结果
	if s.config.EnableCache {
		s.putToCache(cacheKey, objects)
	}

	return objects, nil
}

// UpdateObjectGCStatus 更新对象GC状态
func (s *SystemTableMetadataStore) UpdateObjectGCStatus(ctx context.Context, objectNames []string, status GCStatus) error {
	if len(objectNames) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, s.config.TransactionTimeout)
	defer cancel()

	// 构建IN子句
	namesList := make([]string, 0, len(objectNames))
	for _, name := range objectNames {
		namesList = append(namesList, fmt.Sprintf("'%s'", name))
	}

	updateSQL := fmt.Sprintf(`
		UPDATE %s 
		SET gc_status = '%s', updated_at = CURRENT_TIMESTAMP
		WHERE object_name IN (%s)`,
		SystemTableGCObjects,
		status,
		strings.Join(namesList, ","),
	)

	// 使用 SQLExecutor 执行更新
	opts := executor.Options{}.WithAccountID(defines.GetAccountId(ctx))
	_, err := s.sqlExecutor.Exec(ctx, updateSQL, opts)
	if err != nil {
		return fmt.Errorf("failed to execute update SQL: %w", err)
	}

	// 清除相关缓存
	s.clearObjectCache()

	s.logger.Info("Updated object GC status",
		zap.Int("count", len(objectNames)),
		zap.String("status", string(status)),
	)

	return nil
}

// QueryObjects 查询对象
func (s *SystemTableMetadataStore) QueryObjects(ctx context.Context, filter ObjectFilter) ([]ObjectInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()

	// 构建查询条件
	where := s.buildObjectWhereClause(filter)

	querySQL := fmt.Sprintf(`
		SELECT id, object_name, object_stats, create_ts, delete_ts,
		       database_id, table_id, account_id, object_type,
		       gc_status, task_name, created_at
		FROM %s
		%s
		ORDER BY create_ts DESC
		%s`,
		SystemTableGCObjects,
		where,
		s.buildLimitClause(filter.Limit, filter.Offset),
	)

	// 使用 SQLExecutor 执行查询
	opts := executor.Options{}.WithAccountID(defines.GetAccountId(ctx))
	result, err := s.sqlExecutor.Exec(ctx, querySQL, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	var objects []ObjectInfo
	// 解析查询结果
	if result.Batches != nil && len(result.Batches) > 0 {
		// TODO: 实现批次数据解析逻辑
		s.logger.Debug("Query returned batches", zap.Int("count", len(result.Batches)))
		// 暂时返回空结果，实际使用时需要实现完整的解析逻辑
	}

	return objects, nil
}

// buildObjectWhereClause 构建对象查询的WHERE子句
func (s *SystemTableMetadataStore) buildObjectWhereClause(filter ObjectFilter) string {
	conditions := make([]string, 0)

	if len(filter.ObjectNames) > 0 {
		names := make([]string, 0, len(filter.ObjectNames))
		for _, name := range filter.ObjectNames {
			names = append(names, fmt.Sprintf("'%s'", name))
		}
		conditions = append(conditions, fmt.Sprintf("object_name IN (%s)", strings.Join(names, ",")))
	}

	if len(filter.DatabaseIDs) > 0 {
		ids := make([]string, 0, len(filter.DatabaseIDs))
		for _, id := range filter.DatabaseIDs {
			ids = append(ids, fmt.Sprintf("%d", id))
		}
		conditions = append(conditions, fmt.Sprintf("database_id IN (%s)", strings.Join(ids, ",")))
	}

	if len(filter.TableIDs) > 0 {
		ids := make([]string, 0, len(filter.TableIDs))
		for _, id := range filter.TableIDs {
			ids = append(ids, fmt.Sprintf("%d", id))
		}
		conditions = append(conditions, fmt.Sprintf("table_id IN (%s)", strings.Join(ids, ",")))
	}

	if len(filter.AccountIDs) > 0 {
		ids := make([]string, 0, len(filter.AccountIDs))
		for _, id := range filter.AccountIDs {
			ids = append(ids, fmt.Sprintf("%d", id))
		}
		conditions = append(conditions, fmt.Sprintf("account_id IN (%s)", strings.Join(ids, ",")))
	}

	if len(filter.ObjectTypes) > 0 {
		types := make([]string, 0, len(filter.ObjectTypes))
		for _, t := range filter.ObjectTypes {
			types = append(types, fmt.Sprintf("'%s'", t))
		}
		conditions = append(conditions, fmt.Sprintf("object_type IN (%s)", strings.Join(types, ",")))
	}

	if len(filter.GCStatuses) > 0 {
		statuses := make([]string, 0, len(filter.GCStatuses))
		for _, status := range filter.GCStatuses {
			statuses = append(statuses, fmt.Sprintf("'%s'", status))
		}
		conditions = append(conditions, fmt.Sprintf("gc_status IN (%s)", strings.Join(statuses, ",")))
	}

	if filter.TimeRange != nil {
		conditions = append(conditions,
			fmt.Sprintf("create_ts >= '%s' AND create_ts <= '%s'",
				filter.TimeRange.Start.ToString(),
				filter.TimeRange.End.ToString()))
	}

	if len(filter.TaskNames) > 0 {
		names := make([]string, 0, len(filter.TaskNames))
		for _, name := range filter.TaskNames {
			names = append(names, fmt.Sprintf("'%s'", name))
		}
		conditions = append(conditions, fmt.Sprintf("task_name IN (%s)", strings.Join(names, ",")))
	}

	if len(conditions) == 0 {
		return ""
	}

	return "WHERE " + strings.Join(conditions, " AND ")
}

// buildLimitClause 构建LIMIT子句
func (s *SystemTableMetadataStore) buildLimitClause(limit, offset int) string {
	if limit <= 0 {
		return ""
	}

	if offset > 0 {
		return fmt.Sprintf("LIMIT %d OFFSET %d", limit, offset)
	}

	return fmt.Sprintf("LIMIT %d", limit)
}

// 缓存管理
func (s *SystemTableMetadataStore) getFromCache(key string) interface{} {
	s.cacheMu.RLock()
	defer s.cacheMu.RUnlock()

	return s.cache[key]
}

func (s *SystemTableMetadataStore) putToCache(key string, value interface{}) {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()

	// 简单的LRU缓存实现
	if len(s.cache) >= s.config.CacheSize {
		// 删除一个旧条目
		for k := range s.cache {
			delete(s.cache, k)
			break
		}
	}

	s.cache[key] = value
}

func (s *SystemTableMetadataStore) clearObjectCache() {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()

	// 清除所有以"objects_"开头的缓存
	for key := range s.cache {
		if strings.HasPrefix(key, "objects_") {
			delete(s.cache, key)
		}
	}
}

// 事务管理
type simpleTxContext struct {
	txID      string
	startTime time.Time
	valid     bool
}

func (tx *simpleTxContext) GetTxID() string {
	return tx.txID
}

func (tx *simpleTxContext) IsValid() bool {
	return tx.valid
}

func (s *SystemTableMetadataStore) BeginTx(ctx context.Context) (TxContext, error) {
	// TODO: 实现真实的事务开始逻辑
	txID := fmt.Sprintf("tx_%d", time.Now().UnixNano())

	return &simpleTxContext{
		txID:      txID,
		startTime: time.Now(),
		valid:     true,
	}, nil
}

func (s *SystemTableMetadataStore) CommitTx(ctx context.Context, tx TxContext) error {
	// TODO: 实现真实的事务提交逻辑
	if stx, ok := tx.(*simpleTxContext); ok {
		stx.valid = false
	}

	s.logger.Debug("Transaction committed", zap.String("tx_id", tx.GetTxID()))
	return nil
}

func (s *SystemTableMetadataStore) RollbackTx(ctx context.Context, tx TxContext) error {
	// TODO: 实现真实的事务回滚逻辑
	if stx, ok := tx.(*simpleTxContext); ok {
		stx.valid = false
	}

	s.logger.Debug("Transaction rolled back", zap.String("tx_id", tx.GetTxID()))
	return nil
}

// 其他接口的存根实现 - 需要根据实际需求完善

func (s *SystemTableMetadataStore) DeleteObjects(ctx context.Context, objectNames []string) error {
	// TODO: 实现删除对象
	return nil
}

func (s *SystemTableMetadataStore) SaveSnapshots(ctx context.Context, snapshots []SnapshotInfo) error {
	// TODO: 实现保存快照
	return nil
}

func (s *SystemTableMetadataStore) LoadSnapshotsByLevel(ctx context.Context, level SnapshotLevel, entityID uint64) ([]SnapshotInfo, error) {
	// TODO: 实现按级别加载快照
	return nil, nil
}

func (s *SystemTableMetadataStore) QuerySnapshots(ctx context.Context, filter SnapshotFilter) ([]SnapshotInfo, error) {
	// TODO: 实现查询快照
	return nil, nil
}

func (s *SystemTableMetadataStore) DeleteSnapshots(ctx context.Context, snapshotIDs []string) error {
	// TODO: 实现删除快照
	return nil
}

func (s *SystemTableMetadataStore) SavePITRs(ctx context.Context, pitrs []PITRInfo) error {
	// TODO: 实现保存PITR
	return nil
}

func (s *SystemTableMetadataStore) LoadPITRsByLevel(ctx context.Context, level SnapshotLevel, entityID uint64) ([]PITRInfo, error) {
	// TODO: 实现按级别加载PITR
	return nil, nil
}

func (s *SystemTableMetadataStore) DeletePITRs(ctx context.Context, pitrIDs []string) error {
	// TODO: 实现删除PITR
	return nil
}

func (s *SystemTableMetadataStore) SaveWatermark(ctx context.Context, watermark WatermarkInfo) error {
	// TODO: 实现保存水位线
	return nil
}

func (s *SystemTableMetadataStore) LoadWatermark(ctx context.Context, wType WatermarkType) (*WatermarkInfo, error) {
	// TODO: 实现加载水位线
	return nil, nil
}

func (s *SystemTableMetadataStore) UpdateWatermark(ctx context.Context, wType WatermarkType, ts types.TS) error {
	// TODO: 实现更新水位线
	return nil
}

func (s *SystemTableMetadataStore) SaveMetadata(ctx context.Context, metadata MetadataInfo) error {
	// TODO: 实现保存元数据
	return nil
}

func (s *SystemTableMetadataStore) LoadMetadata(ctx context.Context, taskName string, mType MetadataType) ([]MetadataInfo, error) {
	// TODO: 实现加载元数据
	return nil, nil
}

func (s *SystemTableMetadataStore) DeleteMetadata(ctx context.Context, taskName string, mType MetadataType) error {
	// TODO: 实现删除元数据
	return nil
}

func (s *SystemTableMetadataStore) GetStatistics(ctx context.Context, timeRange *TimeRange) (*GCStatistics, error) {
	// TODO: 实现统计查询
	return &GCStatistics{}, nil
}

func (s *SystemTableMetadataStore) ValidateIntegrity(ctx context.Context) (*IntegrityReport, error) {
	// TODO: 实现完整性验证
	return &IntegrityReport{}, nil
}
