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
	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

const (
	// Version constants
	CurrentVersion           = uint16(3)
	ObjectTableVersion       = 0
	ObjectTablePrimaryKeyIdx = 0
)

const (
	// Memory size constants
	DefaultInMemoryStagedSize = mpool.MB * 32
	DefaultBufferSize         = mpool.MB * 16
	DefaultMemoryBufferSize   = mpool.MB * 16
)

const (
	// GC execution constants
	DefaultCoarseEstimateRows = 10000000
	DefaultCoarseProbility    = 0.00001
	DefaultCanGCTailSize      = 64 * malloc.MB
)

const (
	// Column indices
	ObjectStatsColumnIdx = 0
	CreateTSColumnIdx    = 1
	DeleteTSColumnIdx    = 2
	DBIDColumnIdx        = 3
	TableIDColumnIdx     = 4
)

const (
	// Batch types
	ObjectList BatchType = iota
	TombstoneList
)

const (
	// GC operation types
	CreateBlock BatchType = iota
	DeleteBlock
	DropTable
	DropDB
	DeleteFile
	Tombstone
)

// Attribute names
const (
	GCAttrObjectName = "name"
	GCAttrBlockId    = "block_id"
	GCAttrTableId    = "table_id"
	GCAttrDBId       = "db_id"
	GCAttrCommitTS   = "commit_ts"
	GCCreateTS       = "create_time"
	GCDeleteTS       = "delete_time"
	GCAttrTombstone  = "tombstone"
	GCAttrVersion    = "version"
)

// Error messages
const (
	ErrMsgGetPITRsFailed    = "GetPITRs failed"
	ErrMsgGetSnapshotFailed = "GetSnapshot failed"
	ErrMsgGCExecutionFailed = "doGCAgainstGlobalCheckpointLocked failed"
	ErrMsgDeleteFilesFailed = "ExecDelete failed"
)

// Log task names
const (
	LogTaskGCTrace       = "GC-TRACE-TRY-GC-AGAINST-GCKP"
	LogTaskGCError       = "GC-TRY-GC-AGAINST-GCKP-ERROR"
	LogTaskMergeTrace    = "GC-TRACE-MERGE-WINDOW"
	LogTaskWindowCompare = "GC-WINDOW-COMPARE"
)
