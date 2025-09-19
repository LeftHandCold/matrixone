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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
)

const CurrentVersion = uint16(4)

// 元数据类型
type MetadataType string

const (
	MetadataTypeCheckpoint MetadataType = "checkpoint"
	MetadataTypeSnapshot   MetadataType = "snapshot"
	MetadataTypeAccount    MetadataType = "account"
	MetadataTypeObjectList MetadataType = "object_list"
)

// 水位线类型
type WatermarkType string

const (
	WatermarkTypeScan         WatermarkType = "scan"
	WatermarkTypeGC           WatermarkType = "gc"
	WatermarkTypeCheckpointGC WatermarkType = "checkpoint_gc"
)

// 快照级别
type SnapshotLevel string

const (
	SnapshotLevelCluster  SnapshotLevel = "cluster"
	SnapshotLevelAccount  SnapshotLevel = "account"
	SnapshotLevelDatabase SnapshotLevel = "database"
	SnapshotLevelTable    SnapshotLevel = "table"
)

// 对象类型
type ObjectType string

const (
	ObjectTypeData      ObjectType = "data"
	ObjectTypeTombstone ObjectType = "tombstone"
)

// GC状态
type GCStatus string

const (
	GCStatusPending   GCStatus = "pending"
	GCStatusProcessed GCStatus = "processed"
	GCStatusDeleted   GCStatus = "deleted"
)

// PITR时间单位
type TimeUnit string

const (
	TimeUnitYear   TimeUnit = "y"
	TimeUnitMonth  TimeUnit = "mo"
	TimeUnitDay    TimeUnit = "d"
	TimeUnitHour   TimeUnit = "h"
	TimeUnitMinute TimeUnit = "m"
)

// 系统表名称
const (
	SystemTableGCMetadata   = "mo_gc_metadata"
	SystemTableGCObjects    = "mo_gc_objects"
	SystemTableGCSnapshots  = "mo_gc_snapshots"
	SystemTableGCPITR       = "mo_gc_pitr"
	SystemTableGCWatermarks = "mo_gc_watermarks"
)

// ObjectInfo 对象信息
type ObjectInfo struct {
	ID          int64                `json:"id,omitempty"`
	ObjectName  string               `json:"object_name"`
	ObjectStats objectio.ObjectStats `json:"object_stats"`
	CreateTS    types.TS             `json:"create_ts"`
	DeleteTS    types.TS             `json:"delete_ts"`
	DatabaseID  uint64               `json:"database_id"`
	TableID     uint64               `json:"table_id"`
	AccountID   uint32               `json:"account_id"`
	ObjectType  ObjectType           `json:"object_type"`
	GCStatus    GCStatus             `json:"gc_status"`
	TaskName    string               `json:"task_name"`
	CreatedAt   time.Time            `json:"created_at"`
}

// SnapshotInfo 快照信息
type SnapshotInfo struct {
	ID           int64         `json:"id,omitempty"`
	SnapshotID   string        `json:"snapshot_id"`
	SnapshotName string        `json:"snapshot_name"`
	SnapshotTS   types.TS      `json:"snapshot_ts"`
	Level        SnapshotLevel `json:"level"`
	AccountID    *uint32       `json:"account_id,omitempty"`
	DatabaseID   *uint64       `json:"database_id,omitempty"`
	TableID      *uint64       `json:"table_id,omitempty"`
	AccountName  string        `json:"account_name"`
	DatabaseName string        `json:"database_name"`
	TableName    string        `json:"table_name"`
	ObjectID     *uint64       `json:"object_id,omitempty"`
	CreatedAt    time.Time     `json:"created_at"`
}

// PITRInfo PITR信息
type PITRInfo struct {
	ID            int64         `json:"id,omitempty"`
	PITRID        string        `json:"pitr_id"`
	PITRName      string        `json:"pitr_name"`
	CreateAccount string        `json:"create_account"`
	CreateTime    types.TS      `json:"create_time"`
	ModifiedTime  types.TS      `json:"modified_time"`
	Level         SnapshotLevel `json:"level"`
	AccountID     *uint32       `json:"account_id,omitempty"`
	DatabaseID    *uint64       `json:"database_id,omitempty"`
	TableID       *uint64       `json:"table_id,omitempty"`
	AccountName   string        `json:"account_name"`
	DatabaseName  string        `json:"database_name"`
	TableName     string        `json:"table_name"`
	ObjectID      *uint64       `json:"object_id,omitempty"`
	LengthValue   int           `json:"length_value"`
	TimeUnit      TimeUnit      `json:"time_unit"`
	DropAt        *types.TS     `json:"drop_at,omitempty"`
	CreatedAt     time.Time     `json:"created_at"`
}

// WatermarkInfo 水位线信息
type WatermarkInfo struct {
	ID                 int64         `json:"id,omitempty"`
	WatermarkType      WatermarkType `json:"watermark_type"`
	WatermarkTS        types.TS      `json:"watermark_ts"`
	CheckpointLocation string        `json:"checkpoint_location"`
	CheckpointVersion  int           `json:"checkpoint_version"`
	TaskName           string        `json:"task_name"`
	CreatedAt          time.Time     `json:"created_at"`
	UpdatedAt          time.Time     `json:"updated_at"`
}

// MetadataInfo 元数据信息
type MetadataInfo struct {
	ID            int64          `json:"id,omitempty"`
	TaskName      string         `json:"task_name"`
	MetadataType  MetadataType   `json:"metadata_type"`
	StartTS       types.TS       `json:"start_ts"`
	EndTS         types.TS       `json:"end_ts"`
	AccountID     *uint32        `json:"account_id,omitempty"`
	DatabaseID    *uint64        `json:"database_id,omitempty"`
	TableID       *uint64        `json:"table_id,omitempty"`
	Content       []byte         `json:"content"`
	WatermarkType *WatermarkType `json:"watermark_type,omitempty"`
	CreatedAt     time.Time      `json:"created_at"`
	UpdatedAt     time.Time      `json:"updated_at"`
}

// TimeRange 时间范围
type TimeRange struct {
	Start types.TS `json:"start"`
	End   types.TS `json:"end"`
}

// ObjectFilter 对象过滤器
type ObjectFilter struct {
	ObjectNames []string     `json:"object_names,omitempty"`
	DatabaseIDs []uint64     `json:"database_ids,omitempty"`
	TableIDs    []uint64     `json:"table_ids,omitempty"`
	AccountIDs  []uint32     `json:"account_ids,omitempty"`
	ObjectTypes []ObjectType `json:"object_types,omitempty"`
	GCStatuses  []GCStatus   `json:"gc_statuses,omitempty"`
	TimeRange   *TimeRange   `json:"time_range,omitempty"`
	TaskNames   []string     `json:"task_names,omitempty"`
	Limit       int          `json:"limit,omitempty"`
	Offset      int          `json:"offset,omitempty"`
}

// SnapshotFilter 快照过滤器
type SnapshotFilter struct {
	SnapshotIDs []string        `json:"snapshot_ids,omitempty"`
	Levels      []SnapshotLevel `json:"levels,omitempty"`
	AccountIDs  []uint32        `json:"account_ids,omitempty"`
	DatabaseIDs []uint64        `json:"database_ids,omitempty"`
	TableIDs    []uint64        `json:"table_ids,omitempty"`
	TimeRange   *TimeRange      `json:"time_range,omitempty"`
	Limit       int             `json:"limit,omitempty"`
	Offset      int             `json:"offset,omitempty"`
}

// GCStatistics GC统计信息
type GCStatistics struct {
	ObjectCount          int64                      `json:"object_count"`
	ObjectCountByType    map[ObjectType]int64       `json:"object_count_by_type"`
	ObjectCountByStatus  map[GCStatus]int64         `json:"object_count_by_status"`
	TotalObjectSize      int64                      `json:"total_object_size"`
	PendingGCCount       int64                      `json:"pending_gc_count"`
	ProcessedGCCount     int64                      `json:"processed_gc_count"`
	DeletedGCCount       int64                      `json:"deleted_gc_count"`
	SnapshotCount        int64                      `json:"snapshot_count"`
	SnapshotCountByLevel map[SnapshotLevel]int64    `json:"snapshot_count_by_level"`
	PITRCount            int64                      `json:"pitr_count"`
	PITRCountByLevel     map[SnapshotLevel]int64    `json:"pitr_count_by_level"`
	WatermarkInfo        map[WatermarkType]types.TS `json:"watermark_info"`
	LastUpdateTime       time.Time                  `json:"last_update_time"`
}

// IntegrityReport 完整性报告
type IntegrityReport struct {
	IsValid            bool            `json:"is_valid"`
	OrphanedObjects    []string        `json:"orphaned_objects"`
	MissingWatermarks  []WatermarkType `json:"missing_watermarks"`
	InconsistentRanges []TimeRange     `json:"inconsistent_ranges"`
	Errors             []string        `json:"errors"`
	Warnings           []string        `json:"warnings"`
	CheckTime          time.Time       `json:"check_time"`
}

// MetadataStore 元数据存储接口
type MetadataStore interface {
	// 对象管理
	SaveObjects(ctx context.Context, objects []ObjectInfo) error
	LoadObjectsByTimeRange(ctx context.Context, start, end types.TS) ([]ObjectInfo, error)
	UpdateObjectGCStatus(ctx context.Context, objectNames []string, status GCStatus) error
	QueryObjects(ctx context.Context, filter ObjectFilter) ([]ObjectInfo, error)
	DeleteObjects(ctx context.Context, objectNames []string) error

	// 快照管理
	SaveSnapshots(ctx context.Context, snapshots []SnapshotInfo) error
	LoadSnapshotsByLevel(ctx context.Context, level SnapshotLevel, entityID uint64) ([]SnapshotInfo, error)
	QuerySnapshots(ctx context.Context, filter SnapshotFilter) ([]SnapshotInfo, error)
	DeleteSnapshots(ctx context.Context, snapshotIDs []string) error

	// PITR管理
	SavePITRs(ctx context.Context, pitrs []PITRInfo) error
	LoadPITRsByLevel(ctx context.Context, level SnapshotLevel, entityID uint64) ([]PITRInfo, error)
	DeletePITRs(ctx context.Context, pitrIDs []string) error

	// 水位线管理
	SaveWatermark(ctx context.Context, watermark WatermarkInfo) error
	LoadWatermark(ctx context.Context, wType WatermarkType) (*WatermarkInfo, error)
	UpdateWatermark(ctx context.Context, wType WatermarkType, ts types.TS) error

	// 元数据管理
	SaveMetadata(ctx context.Context, metadata MetadataInfo) error
	LoadMetadata(ctx context.Context, taskName string, mType MetadataType) ([]MetadataInfo, error)
	DeleteMetadata(ctx context.Context, taskName string, mType MetadataType) error

	// 统计和查询
	GetStatistics(ctx context.Context, timeRange *TimeRange) (*GCStatistics, error)
	ValidateIntegrity(ctx context.Context) (*IntegrityReport, error)

	// 事务支持
	BeginTx(ctx context.Context) (TxContext, error)
	CommitTx(ctx context.Context, tx TxContext) error
	RollbackTx(ctx context.Context, tx TxContext) error
}

// TxContext 事务上下文
type TxContext interface {
	GetTxID() string
	IsValid() bool
}

// QueryService 查询服务接口
type QueryService interface {
	// 统计查询
	GetGCStatistics(ctx context.Context, timeRange TimeRange) (*GCStatistics, error)

	// 对象查询
	QueryObjects(ctx context.Context, filter ObjectFilter) ([]ObjectInfo, error)

	// 快照查询
	QuerySnapshots(ctx context.Context, filter SnapshotFilter) ([]SnapshotInfo, error)

	// 健康检查
	ValidateDataIntegrity(ctx context.Context) (*IntegrityReport, error)

	// 监控查询
	GetObjectCountByStatus(ctx context.Context) (map[GCStatus]int64, error)
	GetSnapshotCountByLevel(ctx context.Context) (map[SnapshotLevel]int64, error)
	GetWatermarkStatus(ctx context.Context) (map[WatermarkType]types.TS, error)
}

// GCCleaner GC清理器接口，继承自v3但使用系统表存储
type GCCleaner interface {
	// 基础操作
	Replay(ctx context.Context) error
	Process(ctx context.Context, fn func(*checkpoint.CheckpointEntry) bool) error
	Stop()

	// 检查器管理
	AddChecker(checker func(item any) bool, key string) int
	RemoveChecker(key string) error

	// 水位线管理
	GetScanWaterMark() *checkpoint.CheckpointEntry
	GetCheckpointGCWaterMark() *types.TS
	GetMinMerged() *checkpoint.CheckpointEntry

	// GC操作
	DoCheck(ctx context.Context) error
	EnableGC()
	DisableGC()
	GCEnabled() bool

	// 元数据访问
	GetPITRs() (*PITRInfo, error)
	GetSnapshots() (map[uint32][]types.TS, error)
	ISCPTables() (map[uint64]types.TS, error)

	// 统计和验证
	GetDetails(ctx context.Context) (map[uint32]*TableStats, error)
	Verify(ctx context.Context) string

	// 系统表相关
	SetTid(tid uint64)
	GetMPool() engine.Engine

	// v4新增：元数据存储访问
	GetMetadataStore() MetadataStore
	GetQueryService() QueryService
}

// TableStats 表统计信息
type TableStats struct {
	TableID     uint64    `json:"table_id"`
	ObjectCount int64     `json:"object_count"`
	TotalSize   int64     `json:"total_size"`
	LastGCTime  time.Time `json:"last_gc_time"`
	PendingGC   int64     `json:"pending_gc"`
	ProcessedGC int64     `json:"processed_gc"`
}

// Config GC v4配置
type Config struct {
	// 服务配置
	UUID string `json:"uuid"` // 服务 UUID，用于获取 SQLExecutor

	// 数据库连接配置
	Engine       engine.Engine `json:"-"`
	DatabaseName string        `json:"database_name"`

	// 性能配置
	BatchSize          int           `json:"batch_size"`
	MaxConcurrency     int           `json:"max_concurrency"`
	QueryTimeout       time.Duration `json:"query_timeout"`
	TransactionTimeout time.Duration `json:"transaction_timeout"`

	// 缓存配置
	EnableCache bool          `json:"enable_cache"`
	CacheSize   int           `json:"cache_size"`
	CacheTTL    time.Duration `json:"cache_ttl"`

	// 清理配置
	RetentionPeriod  time.Duration `json:"retention_period"`
	CleanupInterval  time.Duration `json:"cleanup_interval"`
	MaxRetryAttempts int           `json:"max_retry_attempts"`

	// 监控配置
	EnableMonitoring    bool          `json:"enable_monitoring"`
	MetricsInterval     time.Duration `json:"metrics_interval"`
	HealthCheckInterval time.Duration `json:"health_check_interval"`
}

// DefaultConfig 返回默认配置
func DefaultConfig() *Config {
	return &Config{
		DatabaseName:        "mo_catalog",
		BatchSize:           1000,
		MaxConcurrency:      4,
		QueryTimeout:        30 * time.Second,
		TransactionTimeout:  60 * time.Second,
		EnableCache:         true,
		CacheSize:           10000,
		CacheTTL:            5 * time.Minute,
		RetentionPeriod:     7 * 24 * time.Hour, // 7天
		CleanupInterval:     1 * time.Hour,
		MaxRetryAttempts:    3,
		EnableMonitoring:    true,
		MetricsInterval:     1 * time.Minute,
		HealthCheckInterval: 5 * time.Minute,
	}
}
