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

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"go.uber.org/zap"
)

// FilterContext contains all context needed for filtering operations
type FilterContext struct {
	Timestamp        *types.TS
	AccountSnapshots map[uint32][]types.TS
	TableSnapshots   map[uint64][]types.TS
	TablePITRs       map[uint64]*types.TS
	SnapshotMeta     *logtail.SnapshotMeta
	ISCPTables       map[uint64]types.TS
	TransObjects     map[string]map[uint64]*ObjectEntry
	ErrorHandler     *ErrorHandler
}

// BaseFilter provides common functionality for all filters
type BaseFilter struct {
	name     string
	priority int
	enabled  bool
}

func (bf *BaseFilter) Name() string {
	return bf.name
}

func (bf *BaseFilter) Priority() int {
	return bf.priority
}

func (bf *BaseFilter) IsEnabled() bool {
	return bf.enabled
}

func (bf *BaseFilter) SetEnabled(enabled bool) {
	bf.enabled = enabled
}

// CheckpointBloomFilter filters objects based on checkpoint bloom filter
type CheckpointBloomFilter struct {
	BaseFilter
	bloomFilter *bloomfilter.BloomFilter
}

func NewCheckpointBloomFilter(bf *bloomfilter.BloomFilter) *CheckpointBloomFilter {
	return &CheckpointBloomFilter{
		BaseFilter: BaseFilter{
			name:     "CheckpointBloomFilter",
			priority: 1,
			enabled:  true,
		},
		bloomFilter: bf,
	}
}

func (cbf *CheckpointBloomFilter) ShouldGC(ctx context.Context, obj ObjectReference) (bool, error) {
	if !cbf.enabled || cbf.bloomFilter == nil {
		return true, nil
	}

	objName := obj.Stats.ObjectName().UnsafeString()
	return !cbf.bloomFilter.TestString(objName), nil
}

// SnapshotFilter filters objects based on snapshot references
type SnapshotFilter struct {
	BaseFilter
	filterCtx *FilterContext
}

func NewSnapshotFilter(filterCtx *FilterContext) *SnapshotFilter {
	return &SnapshotFilter{
		BaseFilter: BaseFilter{
			name:     "SnapshotFilter",
			priority: 2,
			enabled:  true,
		},
		filterCtx: filterCtx,
	}
}

func (sf *SnapshotFilter) ShouldGC(ctx context.Context, obj ObjectReference) (bool, error) {
	if !sf.enabled {
		return true, nil
	}

	snapshots := sf.filterCtx.TableSnapshots[obj.Table]
	pitr := sf.filterCtx.TablePITRs[obj.Table]

	// Check if object is referenced by snapshots or PITR
	isReferenced := logtail.ObjectIsSnapshotRefers(
		obj.Stats,
		pitr,
		&obj.CreateTS,
		&obj.DropTS,
		snapshots,
	)

	return !isReferenced, nil
}

// ISCPFilter filters objects based on ISCP table information
type ISCPFilter struct {
	BaseFilter
	iscpTables map[uint64]types.TS
}

func NewISCPFilter(iscpTables map[uint64]types.TS) *ISCPFilter {
	return &ISCPFilter{
		BaseFilter: BaseFilter{
			name:     "ISCPFilter",
			priority: 3,
			enabled:  true,
		},
		iscpTables: iscpTables,
	}
}

func (isf *ISCPFilter) ShouldGC(ctx context.Context, obj ObjectReference) (bool, error) {
	if !isf.enabled || isf.iscpTables == nil {
		return true, nil
	}

	iscpTS, exists := isf.iscpTables[obj.Table]
	if !exists {
		return true, nil
	}

	// For CN created or appendable objects, apply ISCP logic
	if obj.Stats.GetCNCreated() || obj.Stats.GetAppendable() {
		if (!obj.DropTS.IsEmpty() && obj.DropTS.LT(&iscpTS)) ||
			obj.CreateTS.GT(&iscpTS) {
			return false, nil
		}
	}

	return true, nil
}

// TransObjectFilter filters objects based on transition object information
type TransObjectFilter struct {
	BaseFilter
	transObjects map[string]map[uint64]*ObjectEntry
}

func NewTransObjectFilter(transObjects map[string]map[uint64]*ObjectEntry) *TransObjectFilter {
	return &TransObjectFilter{
		BaseFilter: BaseFilter{
			name:     "TransObjectFilter",
			priority: 4,
			enabled:  true,
		},
		transObjects: transObjects,
	}
}

func (tof *TransObjectFilter) ShouldGC(ctx context.Context, obj ObjectReference) (bool, error) {
	if !tof.enabled || tof.transObjects == nil {
		return true, nil
	}

	objName := obj.Stats.ObjectName().UnsafeString()
	tables, exists := tof.transObjects[objName]
	if !exists {
		return true, nil
	}

	entry, exists := tables[obj.Table]
	if !exists {
		return true, nil
	}

	// If table hasn't been dropped (empty dropTS), cannot GC
	if entry.dropTS.IsEmpty() {
		return false, nil
	}

	return true, nil
}

// TimeBasedFilter filters objects based on time constraints
type TimeBasedFilter struct {
	BaseFilter
	cutoffTime *types.TS
}

func NewTimeBasedFilter(cutoffTime *types.TS) *TimeBasedFilter {
	return &TimeBasedFilter{
		BaseFilter: BaseFilter{
			name:     "TimeBasedFilter",
			priority: 5,
			enabled:  true,
		},
		cutoffTime: cutoffTime,
	}
}

func (tbf *TimeBasedFilter) ShouldGC(ctx context.Context, obj ObjectReference) (bool, error) {
	if !tbf.enabled || tbf.cutoffTime == nil {
		return true, nil
	}

	// Object must be created before cutoff time and deleted before cutoff time
	if !obj.CreateTS.LT(tbf.cutoffTime) || !obj.DropTS.LT(tbf.cutoffTime) {
		return false, nil
	}

	// If object hasn't been deleted yet, cannot GC
	if obj.DropTS.IsEmpty() {
		tbf.BaseFilter.name = "TimeBasedFilter"
		logutil.Warn("TimeBasedFilter: Object not deleted yet",
			zap.String("object", obj.Stats.ObjectName().String()),
			zap.String("createTS", obj.CreateTS.ToString()),
		)
		return false, nil
	}

	return true, nil
}

// CompositeFilter combines multiple filters with configurable logic
type CompositeFilter struct {
	BaseFilter
	filters      []ObjectFilter
	logic        FilterLogic
	errorHandler *ErrorHandler
}

type FilterLogic int

const (
	FilterLogicAND FilterLogic = iota // All filters must agree to GC
	FilterLogicOR                     // Any filter can decide to GC
)

func NewCompositeFilter(logic FilterLogic, errorHandler *ErrorHandler, filters ...ObjectFilter) *CompositeFilter {
	return &CompositeFilter{
		BaseFilter: BaseFilter{
			name:     "CompositeFilter",
			priority: 0,
			enabled:  true,
		},
		filters:      filters,
		logic:        logic,
		errorHandler: errorHandler,
	}
}

func (cf *CompositeFilter) ShouldGC(ctx context.Context, obj ObjectReference) (bool, error) {
	if !cf.enabled {
		return true, nil
	}

	switch cf.logic {
	case FilterLogicAND:
		return cf.shouldGCAND(ctx, obj)
	case FilterLogicOR:
		return cf.shouldGCOR(ctx, obj)
	default:
		return false, fmt.Errorf("unknown filter logic: %d", cf.logic)
	}
}

func (cf *CompositeFilter) shouldGCAND(ctx context.Context, obj ObjectReference) (bool, error) {
	for _, filter := range cf.filters {
		shouldGC, err := filter.ShouldGC(ctx, obj)
		if err != nil {
			if cf.errorHandler != nil {
				cf.errorHandler.HandleError(
					NewFilterExecutionError(err, filter.Name()),
					"filter_execution",
					zap.String("filter", filter.Name()),
					zap.String("object", obj.Stats.ObjectName().String()),
				)
			}
			return false, err
		}
		if !shouldGC {
			return false, nil
		}
	}
	return true, nil
}

func (cf *CompositeFilter) shouldGCOR(ctx context.Context, obj ObjectReference) (bool, error) {
	allErrors := make([]error, 0)

	for _, filter := range cf.filters {
		shouldGC, err := filter.ShouldGC(ctx, obj)
		if err != nil {
			allErrors = append(allErrors, err)
			continue
		}
		if shouldGC {
			return true, nil
		}
	}

	if len(allErrors) > 0 && cf.errorHandler != nil {
		for _, err := range allErrors {
			cf.errorHandler.HandleError(
				NewFilterExecutionError(err, "CompositeFilterOR"),
				"filter_execution",
				zap.String("object", obj.Stats.ObjectName().String()),
			)
		}
	}

	return len(allErrors) == 0, nil
}

// BatchFilterProcessor processes batches of objects through the filter chain
type BatchFilterProcessor struct {
	filter       ObjectFilter
	errorHandler *ErrorHandler
}

func NewBatchFilterProcessor(filter ObjectFilter, errorHandler *ErrorHandler) *BatchFilterProcessor {
	return &BatchFilterProcessor{
		filter:       filter,
		errorHandler: errorHandler,
	}
}

func (bfp *BatchFilterProcessor) ProcessBatch(
	ctx context.Context,
	bm *bitmap.Bitmap,
	bat *batch.Batch,
	mp *mpool.MPool,
) error {
	if bat.RowCount() == 0 {
		return nil
	}

	// Extract column data
	createTSs := vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[CreateTSColumnIdx])
	deleteTSs := vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[DeleteTSColumnIdx])
	dbIDs := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[DBIDColumnIdx])
	tableIDs := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[TableIDColumnIdx])

	errorCollector := NewErrorCollector("BatchFilterProcessor")

	for i := 0; i < bat.Vecs[0].Length(); i++ {
		select {
		case <-ctx.Done():
			return ContextualError(ctx, "batch_filter_processing")
		default:
		}

		// Parse object stats
		buf := bat.Vecs[ObjectStatsColumnIdx].GetRawBytesAt(i)
		stats := (objectio.ObjectStats)(buf)

		// Create object reference
		objRef := ObjectReference{
			Stats:    &stats,
			CreateTS: createTSs[i],
			DropTS:   deleteTSs[i],
			DB:       dbIDs[i],
			Table:    tableIDs[i],
		}

		// Apply filter
		shouldGC, err := bfp.filter.ShouldGC(ctx, objRef)
		if err != nil {
			errorCollector.Add(err)
			continue
		}

		if shouldGC {
			bm.Add(uint64(i))
		}
	}

	return errorCollector.ToAggregatedError()
}

// FilterChainBuilder helps build complex filter chains
type FilterChainBuilder struct {
	filters      []ObjectFilter
	logic        FilterLogic
	errorHandler *ErrorHandler
}

func NewFilterChainBuilder(errorHandler *ErrorHandler) *FilterChainBuilder {
	return &FilterChainBuilder{
		filters:      make([]ObjectFilter, 0),
		logic:        FilterLogicAND,
		errorHandler: errorHandler,
	}
}

func (fcb *FilterChainBuilder) WithLogic(logic FilterLogic) *FilterChainBuilder {
	fcb.logic = logic
	return fcb
}

func (fcb *FilterChainBuilder) AddCheckpointFilter(bf *bloomfilter.BloomFilter) *FilterChainBuilder {
	if bf != nil {
		fcb.filters = append(fcb.filters, NewCheckpointBloomFilter(bf))
	}
	return fcb
}

func (fcb *FilterChainBuilder) AddSnapshotFilter(filterCtx *FilterContext) *FilterChainBuilder {
	if filterCtx != nil {
		fcb.filters = append(fcb.filters, NewSnapshotFilter(filterCtx))
	}
	return fcb
}

func (fcb *FilterChainBuilder) AddISCPFilter(iscpTables map[uint64]types.TS) *FilterChainBuilder {
	if iscpTables != nil {
		fcb.filters = append(fcb.filters, NewISCPFilter(iscpTables))
	}
	return fcb
}

func (fcb *FilterChainBuilder) AddTransObjectFilter(transObjects map[string]map[uint64]*ObjectEntry) *FilterChainBuilder {
	if transObjects != nil {
		fcb.filters = append(fcb.filters, NewTransObjectFilter(transObjects))
	}
	return fcb
}

func (fcb *FilterChainBuilder) AddTimeBasedFilter(cutoffTime *types.TS) *FilterChainBuilder {
	if cutoffTime != nil {
		fcb.filters = append(fcb.filters, NewTimeBasedFilter(cutoffTime))
	}
	return fcb
}

func (fcb *FilterChainBuilder) AddCustomFilter(filter ObjectFilter) *FilterChainBuilder {
	if filter != nil {
		fcb.filters = append(fcb.filters, filter)
	}
	return fcb
}

func (fcb *FilterChainBuilder) Build() ObjectFilter {
	if len(fcb.filters) == 0 {
		// Return a no-op filter that allows everything to be GC'd
		return &BaseFilter{name: "NoOpFilter", priority: 0, enabled: true}
	}

	if len(fcb.filters) == 1 {
		return fcb.filters[0]
	}

	return NewCompositeFilter(fcb.logic, fcb.errorHandler, fcb.filters...)
}

// Utility function to create filter context from existing parameters
func NewFilterContext(
	timestamp *types.TS,
	accountSnapshots map[uint32][]types.TS,
	pitr *logtail.PitrInfo,
	snapshotMeta *logtail.SnapshotMeta,
	iscpTables map[uint64]types.TS,
	transObjects map[string]map[uint64]*ObjectEntry,
	errorHandler *ErrorHandler,
) *FilterContext {
	var tableSnapshots map[uint64][]types.TS
	var tablePITRs map[uint64]*types.TS

	if snapshotMeta != nil && pitr != nil {
		tableSnapshots, tablePITRs = snapshotMeta.AccountToTableSnapshots(accountSnapshots, pitr)
	}

	return &FilterContext{
		Timestamp:        timestamp,
		AccountSnapshots: accountSnapshots,
		TableSnapshots:   tableSnapshots,
		TablePITRs:       tablePITRs,
		SnapshotMeta:     snapshotMeta,
		ISCPTables:       iscpTables,
		TransObjects:     transObjects,
		ErrorHandler:     errorHandler,
	}
}
