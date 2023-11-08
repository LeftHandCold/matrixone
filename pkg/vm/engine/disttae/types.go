// Copyright 2022 Matrix Origin
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

package disttae

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logservice"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	PREFETCH_THRESHOLD = 512
	PREFETCH_ROUNDS    = 32
)

const (
	INSERT = iota
	DELETE
	COMPACTION_CN
	UPDATE
	ALTER
)

const (
	MO_DATABASE_ID_NAME_IDX       = 1
	MO_DATABASE_ID_ACCOUNT_IDX    = 2
	MO_DATABASE_LIST_ACCOUNT_IDX  = 1
	MO_TABLE_ID_NAME_IDX          = 1
	MO_TABLE_ID_DATABASE_ID_IDX   = 2
	MO_TABLE_ID_ACCOUNT_IDX       = 3
	MO_TABLE_LIST_DATABASE_ID_IDX = 1
	MO_TABLE_LIST_ACCOUNT_IDX     = 2
	MO_PRIMARY_OFF                = 2
	INIT_ROWID_OFFSET             = math.MaxUint32
)

const (
	WorkspaceThreshold uint64 = 1 * mpool.MB
)

var (
	_ client.Workspace = (*Transaction)(nil)
)

var GcCycle = 10 * time.Second

type DNStore = metadata.TNService

type IDGenerator interface {
	AllocateID(ctx context.Context) (uint64, error)
	// AllocateIDByKey allocate a globally unique ID by key.
	AllocateIDByKey(ctx context.Context, key string) (uint64, error)
}

type Engine struct {
	sync.RWMutex
	mp         *mpool.MPool
	fs         fileservice.FileService
	ls         lockservice.LockService
	qs         queryservice.QueryService
	hakeeper   logservice.CNHAKeeperClient
	us         udf.Service
	cli        client.TxnClient
	idGen      IDGenerator
	catalog    *cache.CatalogCache
	tnID       string
	partitions map[[2]uint64]*logtailreplay.Partition
	packerPool *fileservice.Pool[*types.Packer]

	// XXX related to cn push model
	pClient pushClient
}

// Transaction represents a transaction
type Transaction struct {
	sync.Mutex
	engine *Engine
	// readOnly default value is true, once a write happen, then set to false
	readOnly atomic.Bool
	// db       *DB
	// blockId starts at 0 and keeps incrementing,
	// this is used to name the file on s3 and then give it to tae to use
	// not-used now
	// blockId uint64

	// local timestamp for workspace operations
	//meta     *txn.TxnMeta
	op       client.TxnOperator
	sqlCount atomic.Uint64

	// writes cache stores any writes done by txn
	writes []Entry
	// txn workspace size
	workspaceSize uint64

	tnStores []DNStore
	proc     *process.Process

	idGen IDGenerator

	// interim incremental rowid
	rowId [6]uint32
	segId types.Uuid
	// use to cache table
	tableCache struct {
		cachedIndex int
		tableMap    *sync.Map
	}
	// use to cache database
	databaseMap *sync.Map
	// use to cache created table
	createMap *sync.Map
	/*
		for deleted table
		CORNER CASE
		create table t1(a int);
		begin;
		drop table t1; // t1 does not exist
		select * from t1; // can not access t1
		create table t2(a int); // new table
		drop table t2;
		create table t2(a int,b int); //new table
		commit;
		show tables; //no table t1; has table t2(a,b)
	*/
	deletedTableMap *sync.Map

	//deletes for uncommitted blocks generated by CN writing S3.
	deletedBlocks *deletedBlocks

	// uncommitted block generated by CN writing S3 -> block's meta location.
	// notice that it's guarded by txn.Lock()
	cnBlkId_Pos map[types.Blockid]Pos
	// uncommitted block/raw batch generated by CN writing memory -> batch in txn.writes.
	// it is used to shrinking the batch of txn.writes.
	//TODO::remove it
	blockId_raw_batch map[types.Blockid]*batch.Batch
	// committed block belongs to txn's snapshot data -> delta locations for committed block's deletes.
	blockId_tn_delete_metaLoc_batch map[types.Blockid][]*batch.Batch
	//select list for raw batch comes from txn.writes.batch.
	batchSelectList map[*batch.Batch][]int64
	toFreeBatches   map[[2]string][]*batch.Batch

	rollbackCount int
	statementID   int
	statements    []int

	hasS3Op              atomic.Bool
	removed              bool
	startStatementCalled bool
	incrStatementCalled  bool
	syncCommittedTSCount uint64
}

type Pos struct {
	bat     *batch.Batch
	tbName  string
	dbName  string
	offset  int64
	blkInfo catalog.BlockInfo
}

// FIXME: The map inside this one will be accessed concurrently, using
// a mutex, not sure if there will be performance issues
type deletedBlocks struct {
	sync.RWMutex

	// used to store cn block's deleted rows
	// blockId => deletedOffsets
	offsets map[types.Blockid][]int64
}

func (b *deletedBlocks) addDeletedBlocks(blockID *types.Blockid, offsets []int64) {
	b.Lock()
	defer b.Unlock()
	b.offsets[*blockID] = append(b.offsets[*blockID], offsets...)
}

func (b *deletedBlocks) isDeleted(blockID *types.Blockid) bool {
	b.RLock()
	defer b.RUnlock()
	_, ok := b.offsets[*blockID]
	return ok
}

func (b *deletedBlocks) getDeletedOffsetsByBlock(blockID *types.Blockid, offsets *[]int64) {
	b.RLock()
	defer b.RUnlock()
	res := b.offsets[*blockID]
	*offsets = append(*offsets, res...)
	logutil.Infof("getDeletedOffsetsByBlock%v : %v, is %v", blockID.String(), res, *offsets)
}

func (b *deletedBlocks) iter(fn func(*types.Blockid, []int64) bool) {
	b.RLock()
	defer b.RUnlock()
	for id, offsets := range b.offsets {
		if !fn(&id, offsets) {
			return
		}
	}
}

func (txn *Transaction) PutCnBlockDeletes(blockId *types.Blockid, offsets []int64) {
	txn.deletedBlocks.addDeletedBlocks(blockId, offsets)
}

func (txn *Transaction) StartStatement() {
	if txn.startStatementCalled {
		logutil.Fatal("BUG: StartStatement called twice")
	}
	txn.startStatementCalled = true
	txn.incrStatementCalled = false
}

func (txn *Transaction) EndStatement() {
	if !txn.startStatementCalled {
		logutil.Fatal("BUG: StartStatement not called")
	}

	txn.startStatementCalled = false
	txn.incrStatementCalled = false
}

func (txn *Transaction) IncrStatementID(ctx context.Context, commit bool) error {
	if !commit {
		if !txn.startStatementCalled {
			logutil.Fatal("BUG: StartStatement not called")
		}
		if txn.incrStatementCalled {
			logutil.Fatal("BUG: IncrStatementID called twice")
		}
		txn.incrStatementCalled = true
	}

	txn.Lock()
	defer txn.Unlock()
	//free batches
	for key := range txn.toFreeBatches {
		for _, bat := range txn.toFreeBatches[key] {
			txn.proc.PutBatch(bat)
		}
		delete(txn.toFreeBatches, key)
	}
	if err := txn.mergeTxnWorkspaceLocked(); err != nil {
		return err
	}
	if err := txn.dumpBatchLocked(0); err != nil {
		return err
	}
	txn.statements = append(txn.statements, len(txn.writes))
	txn.statementID++

	return txn.handleRCSnapshot(ctx, commit)
}

// Adjust adjust writes order
func (txn *Transaction) Adjust() error {
	txn.Lock()
	defer txn.Unlock()
	if err := txn.adjustUpdateOrderLocked(); err != nil {
		return err
	}
	if err := txn.mergeTxnWorkspaceLocked(); err != nil {
		return err
	}
	return nil
}

// The current implementation, update's delete and insert are executed concurrently, inside workspace it
// may be the order of insert+delete that needs to be adjusted.
func (txn *Transaction) adjustUpdateOrderLocked() error {
	if txn.statementID > 0 {
		start := txn.statements[txn.statementID-1]
		writes := make([]Entry, 0, len(txn.writes[start:]))
		for i := start; i < len(txn.writes); i++ {
			if txn.writes[i].typ == DELETE {
				writes = append(writes, txn.writes[i])
			}
		}
		for i := start; i < len(txn.writes); i++ {
			if txn.writes[i].typ != DELETE {
				writes = append(writes, txn.writes[i])
			}
		}
		txn.writes = append(txn.writes[:start], writes...)
		// restore the scope of the statement
		txn.statements[txn.statementID-1] = len(txn.writes)
	}
	return nil
}

func (txn *Transaction) RollbackLastStatement(ctx context.Context) error {
	// If has s3 operation, can not rollback.
	if txn.hasS3Op.Load() {
		return moerr.NewTxnCannotRetry(ctx)
	}

	txn.Lock()
	defer txn.Unlock()

	txn.rollbackCount++
	if txn.statementID > 0 {
		txn.statementID--
		end := txn.statements[txn.statementID]
		for i := end; i < len(txn.writes); i++ {
			if txn.writes[i].bat == nil {
				continue
			}
			txn.writes[i].bat.Clean(txn.engine.mp)
		}
		txn.writes = txn.writes[:end]
		txn.statements = txn.statements[:txn.statementID]
	}
	// rollback current statement's writes info
	for b := range txn.batchSelectList {
		delete(txn.batchSelectList, b)
	}
	// current statement has been rolled back, make can call IncrStatementID again.
	txn.incrStatementCalled = false
	return nil
}
func (txn *Transaction) resetSnapshot() error {
	txn.tableCache.tableMap.Range(func(key, value interface{}) bool {
		value.(*txnTable).resetSnapshot()
		return true
	})
	return nil
}

func (txn *Transaction) IncrSQLCount() {
	txn.sqlCount.Add(1)
}

func (txn *Transaction) GetSQLCount() uint64 {
	return txn.sqlCount.Load()
}

// For RC isolation, update the snapshot TS of transaction for each statement.
// only 2 cases need to reset snapshot
// 1. cn sync latest commit ts from mo_ctl
// 2. not first sql
func (txn *Transaction) handleRCSnapshot(ctx context.Context, commit bool) error {
	needResetSnapshot := false
	newTimes := txn.proc.TxnClient.GetSyncLatestCommitTSTimes()
	if newTimes > txn.syncCommittedTSCount {
		txn.syncCommittedTSCount = newTimes
		needResetSnapshot = true
	}
	if !commit && txn.op.Txn().IsRCIsolation() &&
		(txn.GetSQLCount() > 1 || needResetSnapshot) {
		if err := txn.op.UpdateSnapshot(
			ctx,
			timestamp.Timestamp{}); err != nil {
			return err
		}
		txn.resetSnapshot()
	}
	return nil
}

// Entry represents a delete/insert
type Entry struct {
	typ          int
	tableId      uint64
	databaseId   uint64
	tableName    string
	databaseName string
	// blockName for s3 file
	fileName string
	// update or delete tuples
	bat       *batch.Batch
	tnStore   DNStore
	pkChkByTN int8
	/*
		if truncate is true,it denotes the Entry with typ DELETE
		on mo_tables is generated by the Truncate operation.
	*/
	truncate bool
}

// isGeneratedByTruncate denotes the entry is yielded by the truncate operation.
func (e *Entry) isGeneratedByTruncate() bool {
	return e.typ == DELETE &&
		e.databaseId == catalog.MO_CATALOG_ID &&
		e.tableId == catalog.MO_TABLES_ID &&
		e.truncate
}

// txnDatabase represents an opened database in a transaction
type txnDatabase struct {
	databaseId        uint64
	databaseName      string
	databaseType      string
	databaseCreateSql string
	txn               *Transaction
}

type tableKey struct {
	accountId  uint32
	databaseId uint64
	tableId    uint64
	name       string
}

type databaseKey struct {
	accountId uint32
	id        uint64
	name      string
}

// txnTable represents an opened table in a transaction
type txnTable struct {
	sync.Mutex

	tableId   uint64
	version   uint32
	tableName string
	tnList    []int
	db        *txnDatabase
	//	insertExpr *plan.Expr
	defs       []engine.TableDef
	tableDef   *plan.TableDef
	seqnums    []uint16
	typs       []types.Type
	_partState *logtailreplay.PartitionState

	// blockInfos stores all the block infos for this table of this transaction
	// it is only generated when the table is not created by this transaction
	// it is initialized by updateBlockInfos and once it is initialized, it will not be updated
	blockInfos []catalog.BlockInfo

	// specify whether the blockInfos is updated. once it is updated, it will not be updated again
	blockInfosUpdated bool
	// specify whether the logtail is updated. once it is updated, it will not be updated again
	logtailUpdated bool

	primaryIdx    int // -1 means no primary key
	primarySeqnum int // -1 means no primary key
	clusterByIdx  int // -1 means no clusterBy key
	viewdef       string
	comment       string
	partitioned   int8   //1 : the table has partitions ; 0 : no partition
	partition     string // the info about partitions when the table has partitions
	relKind       string
	createSql     string
	constraint    []byte

	// timestamp of the last operation on this table
	lastTS timestamp.Timestamp
	//entries belong to this table,and come from txn.writes.
	writes []Entry
	// offset of the writes in workspace
	writesOffset int

	// this should be the statement id
	// but seems that we're not maintaining it at the moment
	// localTS timestamp.Timestamp
	//rowid in mo_tables
	rowid types.Rowid
	//rowids in mo_columns
	rowids []types.Rowid
	//the old table id before truncate
	oldTableId uint64

	// process for statement
	//proc *process.Process
	proc atomic.Pointer[process.Process]
}

type column struct {
	accountId  uint32
	tableId    uint64
	databaseId uint64
	// column name
	name            string
	tableName       string
	databaseName    string
	typ             []byte
	typLen          int32
	num             int32
	comment         string
	notNull         int8
	hasDef          int8
	defaultExpr     []byte
	constraintType  string
	isClusterBy     int8
	isHidden        int8
	isAutoIncrement int8
	hasUpdate       int8
	updateExpr      []byte
	seqnum          uint16
	enumValues      string
}

type withFilterMixin struct {
	ctx      context.Context
	fs       fileservice.FileService
	ts       timestamp.Timestamp
	proc     *process.Process
	tableDef *plan.TableDef

	// columns used for reading
	columns struct {
		seqnums  []uint16
		colTypes []types.Type
		// colNulls []bool

		compPKPositions []uint16 // composite primary key pos in the columns

		pkPos    int // -1 means no primary key in columns
		rowidPos int // -1 means no rowid in columns

		indexOfFirstSortedColumn int
	}

	filterState struct {
		evaluated bool
		//point select for primary key
		expr     *plan.Expr
		filter   blockio.ReadFilter
		seqnums  []uint16 // seqnums of the columns in the filter
		colTypes []types.Type
		hasNull  bool
	}

	sels []int32
}

type blockReader struct {
	withFilterMixin

	// used for prefetch
	infos       [][]*catalog.BlockInfo
	steps       []int
	currentStep int

	// block list to scan
	blks []*catalog.BlockInfo
	//buffer for block's deletes
	buffer []int64
}

type blockMergeReader struct {
	*blockReader
	table *txnTable

	//for perfetch deletes
	loaded     bool
	pkidx      int
	deletaLocs map[string][]objectio.Location
}

type mergeReader struct {
	rds []engine.Reader
}

type emptyReader struct {
}

type pkRange struct {
	isRange bool
	items   []int64
	ranges  []int64
}
