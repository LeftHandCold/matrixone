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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

// queryService 查询服务实现
type queryService struct {
	store  MetadataStore
	logger *zap.Logger
}

// NewQueryService 创建查询服务
func NewQueryService(store MetadataStore) QueryService {
	return &queryService{
		store:  store,
		logger: logutil.GetGlobalLogger().Named("gc-v4-query"),
	}
}

// GetGCStatistics 获取GC统计信息
func (q *queryService) GetGCStatistics(ctx context.Context, timeRange TimeRange) (*GCStatistics, error) {
	stats, err := q.store.GetStatistics(ctx, &timeRange)
	if err != nil {
		return nil, fmt.Errorf("failed to get statistics: %w", err)
	}

	return stats, nil
}

// QueryObjects 查询对象
func (q *queryService) QueryObjects(ctx context.Context, filter ObjectFilter) ([]ObjectInfo, error) {
	objects, err := q.store.QueryObjects(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to query objects: %w", err)
	}

	q.logger.Debug("Queried objects",
		zap.Int("count", len(objects)),
		zap.Any("filter", filter),
	)

	return objects, nil
}

// QuerySnapshots 查询快照
func (q *queryService) QuerySnapshots(ctx context.Context, filter SnapshotFilter) ([]SnapshotInfo, error) {
	snapshots, err := q.store.QuerySnapshots(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to query snapshots: %w", err)
	}

	q.logger.Debug("Queried snapshots",
		zap.Int("count", len(snapshots)),
		zap.Any("filter", filter),
	)

	return snapshots, nil
}

// ValidateDataIntegrity 验证数据完整性
func (q *queryService) ValidateDataIntegrity(ctx context.Context) (*IntegrityReport, error) {
	report, err := q.store.ValidateIntegrity(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to validate integrity: %w", err)
	}

	q.logger.Info("Data integrity validation completed",
		zap.Bool("is_valid", report.IsValid),
		zap.Int("error_count", len(report.Errors)),
		zap.Int("warning_count", len(report.Warnings)),
	)

	return report, nil
}

// GetObjectCountByStatus 按状态获取对象数量
func (q *queryService) GetObjectCountByStatus(ctx context.Context) (map[GCStatus]int64, error) {
	result := make(map[GCStatus]int64)

	// 查询各种状态的对象数量
	for _, status := range []GCStatus{GCStatusPending, GCStatusProcessed, GCStatusDeleted} {
		filter := ObjectFilter{
			GCStatuses: []GCStatus{status},
		}

		objects, err := q.store.QueryObjects(ctx, filter)
		if err != nil {
			return nil, fmt.Errorf("failed to query objects with status %s: %w", status, err)
		}

		result[status] = int64(len(objects))
	}

	return result, nil
}

// GetSnapshotCountByLevel 按级别获取快照数量
func (q *queryService) GetSnapshotCountByLevel(ctx context.Context) (map[SnapshotLevel]int64, error) {
	result := make(map[SnapshotLevel]int64)

	// 查询各种级别的快照数量
	for _, level := range []SnapshotLevel{SnapshotLevelCluster, SnapshotLevelAccount, SnapshotLevelDatabase, SnapshotLevelTable} {
		filter := SnapshotFilter{
			Levels: []SnapshotLevel{level},
		}

		snapshots, err := q.store.QuerySnapshots(ctx, filter)
		if err != nil {
			return nil, fmt.Errorf("failed to query snapshots with level %s: %w", level, err)
		}

		result[level] = int64(len(snapshots))
	}

	return result, nil
}

// GetWatermarkStatus 获取水位线状态
func (q *queryService) GetWatermarkStatus(ctx context.Context) (map[WatermarkType]types.TS, error) {
	result := make(map[WatermarkType]types.TS)

	// 查询各种类型的水位线
	for _, wType := range []WatermarkType{WatermarkTypeScan, WatermarkTypeGC, WatermarkTypeCheckpointGC} {
		watermark, err := q.store.LoadWatermark(ctx, wType)
		if err != nil {
			q.logger.Warn("Failed to load watermark",
				zap.String("type", string(wType)),
				zap.Error(err),
			)
			continue
		}

		if watermark != nil {
			result[wType] = watermark.WatermarkTS
		}
	}

	return result, nil
}
