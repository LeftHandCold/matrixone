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

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

// ObjectReference represents a reference to an object with its metadata
type ObjectReference struct {
	Stats    *objectio.ObjectStats
	CreateTS types.TS
	DropTS   types.TS
	DB       uint64
	Table    uint64
}

// GCContext encapsulates the context information needed for GC operations
type GCContext struct {
	TaskName         string
	AccountSnapshots map[uint32][]types.TS
	PITR             *logtail.PitrInfo
	SnapshotMeta     *logtail.SnapshotMeta
	ISCPTables       map[uint64]types.TS
	Timestamp        *types.TS
}

// FilterResult represents the result of a filter operation
type FilterResult struct {
	CanGC    []ObjectReference
	CannotGC []ObjectReference
	Errors   []error
}

// MetricsCollector provides metrics collection capabilities
type MetricsCollector interface {
	RecordFilteredObjects(canGC, cannotGC int)
	RecordDeletedObjects(count int, totalSize uint64)
	RecordExecutionTime(operation string, duration int64)
	RecordError(operation string, err error)
}

// ConfigManager manages GC configuration
type ConfigManager interface {
	GetCoarseEstimateRows() int
	GetCoarseProbility() float64
	GetCanGCCacheSize() int
	GetMaxMergeCheckpointCount() int
	Validate() error
	SetDefaults()
}

// CheckpointManager manages checkpoint operations
type CheckpointManager interface {
	GetMaxGlobalCheckpoint() *checkpoint.CheckpointEntry
	GetIncrementalCheckpoints(start, end types.TS) ([]*checkpoint.CheckpointEntry, error)
	ICKPRange(start, end types.TS) ([]*checkpoint.CheckpointEntry, error)
}

// SnapshotManager manages snapshot and PITR operations
type SnapshotManager interface {
	GetSnapshots(ctx context.Context, sid string) (map[uint32]containers.Vector, error)
	GetPITR(ctx context.Context, sid string, gcTime types.TS) (*logtail.PitrInfo, error)
	GetISCPTables() (map[uint64]types.TS, error)
	UpdateSnapshotMeta(ctx context.Context, data *logtail.CKPReader, start, end types.TS) error
}

// WindowManager manages GC windows
type WindowManager interface {
	CreateWindow(start, end types.TS) *GCWindow
	MergeWindows(windows []*GCWindow) *GCWindow
	FilterWindow(window *GCWindow, filter FilterFn) (*FilterResult, error)
	SaveWindow(window *GCWindow) error
	LoadWindow(name string) (*GCWindow, error)
}

// FileDeleter handles file deletion operations
type FileDeleter interface {
	DeleteFiles(ctx context.Context, files []string) error
	DeleteObjects(ctx context.Context, objects []objectio.ObjectStats) error
	ScheduleDelete(files []string) error
	GetPendingDeletes() []string
}

// ObjectFilter defines the interface for object filtering
type ObjectFilter interface {
	ShouldGC(ctx context.Context, obj ObjectReference) (bool, error)
	Name() string
	Priority() int
}

// ChainedFilter combines multiple filters
type ChainedFilter struct {
	filters []ObjectFilter
}

func NewChainedFilter(filters ...ObjectFilter) *ChainedFilter {
	return &ChainedFilter{filters: filters}
}

func (cf *ChainedFilter) ShouldGC(ctx context.Context, obj ObjectReference) (bool, error) {
	for _, filter := range cf.filters {
		shouldGC, err := filter.ShouldGC(ctx, obj)
		if err != nil {
			return false, err
		}
		if !shouldGC {
			return false, nil
		}
	}
	return true, nil
}

// GCExecutionStrategy defines different GC execution strategies
type GCExecutionStrategy interface {
	Execute(ctx context.Context, gcCtx *GCContext, window *GCWindow) (*FilterResult, error)
	Name() string
}

// TwoPhaseGCStrategy implements a two-phase GC strategy (coarse filter + fine filter)
type TwoPhaseGCStrategy struct {
	coarseFilter ObjectFilter
	fineFilter   ObjectFilter
}

func NewTwoPhaseGCStrategy(coarse, fine ObjectFilter) *TwoPhaseGCStrategy {
	return &TwoPhaseGCStrategy{
		coarseFilter: coarse,
		fineFilter:   fine,
	}
}

func (s *TwoPhaseGCStrategy) Name() string {
	return "TwoPhaseGC"
}

func (s *TwoPhaseGCStrategy) Execute(ctx context.Context, gcCtx *GCContext, window *GCWindow) (*FilterResult, error) {
	// Implementation would go here
	return &FilterResult{}, nil
}

// ResourceManager manages GC resources like memory pools and buffers
type ResourceManager interface {
	GetMemoryPool() *mpool.MPool
	GetBuffer() *batch.Batch
	ReturnBuffer(bat *batch.Batch)
	GetBitmap() *bitmap.Bitmap
	ReturnBitmap(bm *bitmap.Bitmap)
	Cleanup()
}

// GCTask represents a single GC task
type GCTask struct {
	ID          string
	Type        string
	Context     *GCContext
	Window      *GCWindow
	Strategy    GCExecutionStrategy
	CreatedAt   types.TS
	CompletedAt types.TS
	Status      string
	Error       error
}

// TaskScheduler schedules and manages GC tasks
type TaskScheduler interface {
	ScheduleTask(task *GCTask) error
	GetPendingTasks() []*GCTask
	GetRunningTasks() []*GCTask
	CancelTask(taskID string) error
	GetTaskStatus(taskID string) (string, error)
}

// Validator validates GC operations
type Validator interface {
	ValidateBeforeGC(ctx context.Context, window *GCWindow) error
	ValidateAfterGC(ctx context.Context, result *FilterResult) error
	ValidateConfiguration(config ConfigManager) error
}
