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
	"sort"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"go.uber.org/zap"
)

// BatchProcessor defines a common interface for processing batches
type BatchProcessor interface {
	Process(ctx context.Context, bat *batch.Batch) error
	Finalize(ctx context.Context) error
}

// ObjectStatsExtractor provides utilities for extracting object stats from batches
type ObjectStatsExtractor struct {
	statsColumnIdx int
}

func NewObjectStatsExtractor() *ObjectStatsExtractor {
	return &ObjectStatsExtractor{
		statsColumnIdx: ObjectStatsColumnIdx,
	}
}

func (ose *ObjectStatsExtractor) ExtractStats(bat *batch.Batch, rowIdx int) objectio.ObjectStats {
	buf := bat.Vecs[ose.statsColumnIdx].GetRawBytesAt(rowIdx)
	return (objectio.ObjectStats)(buf)
}

func (ose *ObjectStatsExtractor) ExtractAllStats(bat *batch.Batch) []objectio.ObjectStats {
	stats := make([]objectio.ObjectStats, bat.RowCount())
	for i := 0; i < bat.RowCount(); i++ {
		stats[i] = ose.ExtractStats(bat, i)
	}
	return stats
}

// TimestampExtractor provides utilities for extracting timestamps from batches
type TimestampExtractor struct {
	createTSColumnIdx int
	deleteTSColumnIdx int
}

func NewTimestampExtractor() *TimestampExtractor {
	return &TimestampExtractor{
		createTSColumnIdx: CreateTSColumnIdx,
		deleteTSColumnIdx: DeleteTSColumnIdx,
	}
}

func (te *TimestampExtractor) ExtractCreateTS(bat *batch.Batch) []types.TS {
	return vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[te.createTSColumnIdx])
}

func (te *TimestampExtractor) ExtractDeleteTS(bat *batch.Batch) []types.TS {
	return vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[te.deleteTSColumnIdx])
}

func (te *TimestampExtractor) ExtractTimestamps(bat *batch.Batch, rowIdx int) (createTS, deleteTS types.TS) {
	createTSs := te.ExtractCreateTS(bat)
	deleteTSs := te.ExtractDeleteTS(bat)
	return createTSs[rowIdx], deleteTSs[rowIdx]
}

// TableIDExtractor provides utilities for extracting table and database IDs
type TableIDExtractor struct {
	dbIDColumnIdx    int
	tableIDColumnIdx int
}

func NewTableIDExtractor() *TableIDExtractor {
	return &TableIDExtractor{
		dbIDColumnIdx:    DBIDColumnIdx,
		tableIDColumnIdx: TableIDColumnIdx,
	}
}

func (tie *TableIDExtractor) ExtractDBIDs(bat *batch.Batch) []uint64 {
	return vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[tie.dbIDColumnIdx])
}

func (tie *TableIDExtractor) ExtractTableIDs(bat *batch.Batch) []uint64 {
	return vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[tie.tableIDColumnIdx])
}

func (tie *TableIDExtractor) ExtractIDs(bat *batch.Batch, rowIdx int) (dbID, tableID uint64) {
	dbIDs := tie.ExtractDBIDs(bat)
	tableIDs := tie.ExtractTableIDs(bat)
	return dbIDs[rowIdx], tableIDs[rowIdx]
}

// BatchDataExtractor combines all extractors for convenience
type BatchDataExtractor struct {
	*ObjectStatsExtractor
	*TimestampExtractor
	*TableIDExtractor
}

func NewBatchDataExtractor() *BatchDataExtractor {
	return &BatchDataExtractor{
		ObjectStatsExtractor: NewObjectStatsExtractor(),
		TimestampExtractor:   NewTimestampExtractor(),
		TableIDExtractor:     NewTableIDExtractor(),
	}
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

func (bde *BatchDataExtractor) ExtractAllObjectReferences(bat *batch.Batch) []ObjectReference {
	refs := make([]ObjectReference, bat.RowCount())
	for i := 0; i < bat.RowCount(); i++ {
		refs[i] = bde.ExtractObjectReference(bat, i)
	}
	return refs
}

// ResourcePool manages reusable resources
type ResourcePool struct {
	batchPool     *sync.Pool
	bitmapPool    *sync.Pool
	extractorPool *sync.Pool
}

func NewResourcePool() *ResourcePool {
	return &ResourcePool{
		batchPool: &sync.Pool{
			New: func() interface{} {
				return &batch.Batch{}
			},
		},
		bitmapPool: &sync.Pool{
			New: func() interface{} {
				return &bitmap.Bitmap{}
			},
		},
		extractorPool: &sync.Pool{
			New: func() interface{} {
				return NewBatchDataExtractor()
			},
		},
	}
}

func (rp *ResourcePool) GetBatch() *batch.Batch {
	return rp.batchPool.Get().(*batch.Batch)
}

func (rp *ResourcePool) PutBatch(bat *batch.Batch) {
	bat.CleanOnlyData()
	rp.batchPool.Put(bat)
}

func (rp *ResourcePool) GetBitmap() *bitmap.Bitmap {
	bm := rp.bitmapPool.Get().(*bitmap.Bitmap)
	bm.Clear()
	return bm
}

func (rp *ResourcePool) PutBitmap(bm *bitmap.Bitmap) {
	bm.Clear()
	rp.bitmapPool.Put(bm)
}

func (rp *ResourcePool) GetExtractor() *BatchDataExtractor {
	return rp.extractorPool.Get().(*BatchDataExtractor)
}

func (rp *ResourcePool) PutExtractor(extractor *BatchDataExtractor) {
	rp.extractorPool.Put(extractor)
}

// OperationTimer helps measure operation durations
type OperationTimer struct {
	startTime time.Time
	operation string
	logger    *zap.Logger
}

func NewOperationTimer(operation string) *OperationTimer {
	return &OperationTimer{
		startTime: time.Now(),
		operation: operation,
		logger:    logutil.GetGlobalLogger(),
	}
}

func (ot *OperationTimer) LogDuration(extraFields ...zap.Field) {
	duration := time.Since(ot.startTime)
	fields := []zap.Field{
		zap.String("operation", ot.operation),
		zap.Duration("duration", duration),
	}
	fields = append(fields, extraFields...)
	ot.logger.Info("Operation completed", fields...)
}

func (ot *OperationTimer) LogError(err error, extraFields ...zap.Field) {
	duration := time.Since(ot.startTime)
	fields := []zap.Field{
		zap.String("operation", ot.operation),
		zap.Duration("duration", duration),
		zap.Error(err),
	}
	fields = append(fields, extraFields...)
	ot.logger.Error("Operation failed", fields...)
}

// BatchValidator provides validation utilities for batches
type BatchValidator struct{}

func NewBatchValidator() *BatchValidator {
	return &BatchValidator{}
}

func (bv *BatchValidator) ValidateBatch(bat *batch.Batch) error {
	if bat == nil {
		return fmt.Errorf("batch is nil")
	}

	if bat.RowCount() == 0 {
		return fmt.Errorf("batch is empty")
	}

	if len(bat.Vecs) < len(ObjectTableAttrs) {
		return fmt.Errorf("batch has insufficient columns: expected %d, got %d",
			len(ObjectTableAttrs), len(bat.Vecs))
	}

	return nil
}

func (bv *BatchValidator) ValidateObjectReference(ref ObjectReference) error {
	if ref.Stats == nil {
		return fmt.Errorf("object stats is nil")
	}

	if ref.CreateTS.IsEmpty() {
		return fmt.Errorf("create timestamp is empty")
	}

	if ref.DB == 0 {
		return fmt.Errorf("database ID is zero")
	}

	if ref.Table == 0 {
		return fmt.Errorf("table ID is zero")
	}

	return nil
}

// SnapshotComparer provides utilities for comparing snapshots and timestamps
type SnapshotComparer struct{}

func NewSnapshotComparer() *SnapshotComparer {
	return &SnapshotComparer{}
}

func (sc *SnapshotComparer) IsSnapshotReferenced(
	objStats *objectio.ObjectStats,
	createTS, dropTS *types.TS,
	snapshots []types.TS,
	pitr *types.TS,
) bool {
	return logtail.ObjectIsSnapshotRefers(objStats, pitr, createTS, dropTS, snapshots)
}

func (sc *SnapshotComparer) SortSnapshots(snapshots []types.TS) {
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].LT(&snapshots[j])
	})
}

func (sc *SnapshotComparer) FindSnapshotInRange(
	snapshots []types.TS,
	startTS, endTS types.TS,
) []types.TS {
	result := make([]types.TS, 0)
	for _, snapshot := range snapshots {
		if snapshot.GE(&startTS) && snapshot.LE(&endTS) {
			result = append(result, snapshot)
		}
	}
	return result
}

// CheckpointHelper provides utilities for checkpoint operations
type CheckpointHelper struct{}

func NewCheckpointHelper() *CheckpointHelper {
	return &CheckpointHelper{}
}

func (ch *CheckpointHelper) SortCheckpoints(checkpoints []*checkpoint.CheckpointEntry) {
	sort.Slice(checkpoints, func(i, j int) bool {
		return checkpoints[i].GetEnd().LT(&checkpoints[j].GetEnd())
	})
}

func (ch *CheckpointHelper) FindLatestCheckpoint(checkpoints []*checkpoint.CheckpointEntry) *checkpoint.CheckpointEntry {
	if len(checkpoints) == 0 {
		return nil
	}

	ch.SortCheckpoints(checkpoints)
	return checkpoints[len(checkpoints)-1]
}

func (ch *CheckpointHelper) FilterCheckpointsByTimeRange(
	checkpoints []*checkpoint.CheckpointEntry,
	startTS, endTS types.TS,
) []*checkpoint.CheckpointEntry {
	result := make([]*checkpoint.CheckpointEntry, 0)
	for _, ckp := range checkpoints {
		ckpEnd := ckp.GetEnd()
		if ckpEnd.GE(&startTS) && ckpEnd.LE(&endTS) {
			result = append(result, ckp)
		}
	}
	return result
}

// FileOperationHelper provides utilities for file operations
type FileOperationHelper struct {
	fs fileservice.FileService
}

func NewFileOperationHelper(fs fileservice.FileService) *FileOperationHelper {
	return &FileOperationHelper{fs: fs}
}

func (foh *FileOperationHelper) DeleteFiles(ctx context.Context, files []string) error {
	for _, file := range files {
		if err := foh.fs.Delete(ctx, file); err != nil {
			return fmt.Errorf("failed to delete file %s: %w", file, err)
		}
	}
	return nil
}

func (foh *FileOperationHelper) DeleteFilesInBatches(
	ctx context.Context,
	files []string,
	batchSize int,
) error {
	for i := 0; i < len(files); i += batchSize {
		end := i + batchSize
		if end > len(files) {
			end = len(files)
		}

		if err := foh.DeleteFiles(ctx, files[i:end]); err != nil {
			return err
		}

		// Check for context cancellation between batches
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return nil
}

func (foh *FileOperationHelper) GetFileSize(ctx context.Context, fileName string) (int64, error) {
	stat, err := foh.fs.StatFile(ctx, fileName)
	if err != nil {
		return 0, err
	}
	return stat.Size, nil
}

// ContainerHelper provides utilities for working with containers
type ContainerHelper struct {
	mp *mpool.MPool
}

func NewContainerHelper(mp *mpool.MPool) *ContainerHelper {
	return &ContainerHelper{mp: mp}
}

func (ch *ContainerHelper) TransformSnapshotList(snapshots map[uint32]containers.Vector) map[uint32][]types.TS {
	result := make(map[uint32][]types.TS)
	for accountID, vec := range snapshots {
		if vec.GetDownstreamVector() == nil {
			continue
		}

		length := vec.GetDownstreamVector().Length()
		tsList := make([]types.TS, length)
		for i := 0; i < length; i++ {
			ts := vector.GetFixedAt[types.TS](vec.GetDownstreamVector(), i)
			tsList[i] = ts
		}
		result[accountID] = tsList
	}
	return result
}

func (ch *ContainerHelper) CreateBatchFromSchema(attrs []string, types []types.Type) *batch.Batch {
	bat := batch.NewWithSize(len(attrs))
	for i, typ := range types {
		bat.Vecs[i] = vector.NewVec(typ)
	}
	return bat
}

// StatisticsCollector collects various statistics during GC operations
type StatisticsCollector struct {
	objectsProcessed int64
	objectsGCed      int64
	objectsSkipped   int64
	totalSizeGCed    int64
	totalSizeSkipped int64
	operationCount   map[string]int64
	errorCount       map[string]int64
}

func NewStatisticsCollector() *StatisticsCollector {
	return &StatisticsCollector{
		operationCount: make(map[string]int64),
		errorCount:     make(map[string]int64),
	}
}

func (sc *StatisticsCollector) RecordObjectProcessed() {
	sc.objectsProcessed++
}

func (sc *StatisticsCollector) RecordObjectGCed(size int64) {
	sc.objectsGCed++
	sc.totalSizeGCed += size
}

func (sc *StatisticsCollector) RecordObjectSkipped(size int64) {
	sc.objectsSkipped++
	sc.totalSizeSkipped += size
}

func (sc *StatisticsCollector) RecordOperation(operation string) {
	sc.operationCount[operation]++
}

func (sc *StatisticsCollector) RecordError(operation string) {
	sc.errorCount[operation]++
}

func (sc *StatisticsCollector) GetSummary() map[string]interface{} {
	return map[string]interface{}{
		"objects_processed":  sc.objectsProcessed,
		"objects_gced":       sc.objectsGCed,
		"objects_skipped":    sc.objectsSkipped,
		"total_size_gced":    sc.totalSizeGCed,
		"total_size_skipped": sc.totalSizeSkipped,
		"operation_count":    sc.operationCount,
		"error_count":        sc.errorCount,
		"gc_ratio":           float64(sc.objectsGCed) / float64(sc.objectsProcessed),
	}
}

func (sc *StatisticsCollector) Reset() {
	sc.objectsProcessed = 0
	sc.objectsGCed = 0
	sc.objectsSkipped = 0
	sc.totalSizeGCed = 0
	sc.totalSizeSkipped = 0
	sc.operationCount = make(map[string]int64)
	sc.errorCount = make(map[string]int64)
}

// ContextHelper provides utilities for context management
type ContextHelper struct{}

func NewContextHelper() *ContextHelper {
	return &ContextHelper{}
}

func (ch *ContextHelper) WithTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, timeout)
}

func (ch *ContextHelper) WithCancel(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithCancel(ctx)
}

func (ch *ContextHelper) IsContextDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func (ch *ContextHelper) WaitWithTimeout(ctx context.Context, timeout time.Duration, fn func() error) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- fn()
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Common utility functions

// TransformToTSList converts a map of containers.Vector to a map of timestamp slices
func TransformToTSList(snapshots map[uint32]containers.Vector) map[uint32][]types.TS {
	helper := NewContainerHelper(nil)
	return helper.TransformSnapshotList(snapshots)
}

// SortObjectsByCreateTime sorts object references by creation time
func SortObjectsByCreateTime(objects []ObjectReference) {
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].CreateTS.LT(&objects[j].CreateTS)
	})
}

// SortObjectsByDropTime sorts object references by drop time
func SortObjectsByDropTime(objects []ObjectReference) {
	sort.Slice(objects, func(i, j int) bool {
		if objects[i].DropTS.IsEmpty() && !objects[j].DropTS.IsEmpty() {
			return false
		}
		if !objects[i].DropTS.IsEmpty() && objects[j].DropTS.IsEmpty() {
			return true
		}
		return objects[i].DropTS.LT(&objects[j].DropTS)
	})
}

// FilterObjectsByTimeRange filters objects within a specific time range
func FilterObjectsByTimeRange(objects []ObjectReference, startTS, endTS types.TS) []ObjectReference {
	result := make([]ObjectReference, 0)
	for _, obj := range objects {
		if obj.CreateTS.GE(&startTS) && obj.CreateTS.LE(&endTS) {
			result = append(result, obj)
		}
	}
	return result
}

// CalculateTotalSize calculates the total size of objects
func CalculateTotalSize(objects []ObjectReference) int64 {
	var totalSize int64
	for _, obj := range objects {
		totalSize += int64(obj.Stats.OriginSize())
	}
	return totalSize
}

// GroupObjectsByTable groups objects by table ID
func GroupObjectsByTable(objects []ObjectReference) map[uint64][]ObjectReference {
	result := make(map[uint64][]ObjectReference)
	for _, obj := range objects {
		result[obj.Table] = append(result[obj.Table], obj)
	}
	return result
}
