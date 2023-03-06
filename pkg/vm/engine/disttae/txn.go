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
	"database/sql"
	"errors"
	"math"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage/memorytable"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage/memtable"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

/*
func (txn *Transaction) getTableList(ctx context.Context, databaseId uint64) ([]string, error) {
	rows, err := txn.getRows(ctx, "", catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID, txn.dnStores[:1],
		catalog.MoTablesTableDefs, []string{
			catalog.MoTablesSchema[catalog.MO_TABLES_REL_NAME_IDX],
			catalog.MoTablesSchema[catalog.MO_TABLES_RELDATABASE_ID_IDX],
			catalog.MoTablesSchema[catalog.MO_TABLES_ACCOUNT_ID_IDX],
		},
		genTableListExpr(ctx, getAccountId(ctx), databaseId))
	if err != nil {
		return nil, err
	}
	tableList := make([]string, len(rows))
	for i := range rows {
		tableList[i] = string(rows[i][0].([]byte))
	}
	return tableList, nil
}

func (txn *Transaction) getTableId(ctx context.Context, databaseId uint64,
	name string) (uint64, error) {
	accountId := getAccountId(ctx)
	row, err := txn.getRow(ctx, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID,
		txn.dnStores[:1],
		catalog.MoTablesTableDefs, []string{
			catalog.MoTablesSchema[catalog.MO_TABLES_REL_ID_IDX],
			catalog.MoTablesSchema[catalog.MO_TABLES_REL_NAME_IDX],
			catalog.MoTablesSchema[catalog.MO_TABLES_RELDATABASE_ID_IDX],
			catalog.MoTablesSchema[catalog.MO_TABLES_ACCOUNT_ID_IDX],
		},
		genTableIdExpr(ctx, accountId, databaseId, name))
	if err != nil {
		return 0, err
	}
	return row[0].(uint64), nil
}

func (txn *Transaction) getDatabaseList(ctx context.Context) ([]string, error) {
	rows, err := txn.getRows(ctx, "", catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID,
		txn.dnStores[:1],
		catalog.MoDatabaseTableDefs, []string{
			catalog.MoDatabaseSchema[catalog.MO_DATABASE_DAT_NAME_IDX],
			catalog.MoDatabaseSchema[catalog.MO_DATABASE_ACCOUNT_ID_IDX],
		},
		genDatabaseListExpr(ctx, getAccountId(ctx)))
	if err != nil {
		return nil, err
	}
	databaseList := make([]string, len(rows))
	for i := range rows {
		databaseList[i] = string(rows[i][0].([]byte))
	}
	return databaseList, nil
}

func (txn *Transaction) getDatabaseId(ctx context.Context, name string) (uint64, error) {
	accountId := getAccountId(ctx)
	key := genDatabaseIndexKey(name, accountId)
	rows, err := txn.getRowsByIndex(catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID, "",
		txn.dnStores[:1], []string{
			catalog.MoDatabaseSchema[catalog.MO_DATABASE_DAT_ID_IDX],
			catalog.MoDatabaseSchema[catalog.MO_DATABASE_DAT_NAME_IDX],
			catalog.MoDatabaseSchema[catalog.MO_DATABASE_ACCOUNT_ID_IDX],
		}, key, genDatabaseIdExpr(ctx, accountId, name))
	if err != nil {
		return 0, err
	}
	if len(rows) != 1 {
		return 0, moerr.NewDuplicate(ctx)
	}
	//	row, err := txn.getRow(ctx, catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID, txn.dnStores[:1],
	//		catalog.MoDatabaseTableDefs, []string{
	//			catalog.MoDatabaseSchema[catalog.MO_DATABASE_DAT_ID_IDX],
	//			catalog.MoDatabaseSchema[catalog.MO_DATABASE_DAT_NAME_IDX],
	//			catalog.MoDatabaseSchema[catalog.MO_DATABASE_ACCOUNT_ID_IDX],
	//		},
	//		genDatabaseIdExpr(accountId, name))
	//	if err != nil {
	//		return 0, err
	//	}
	return rows[0][0].(uint64), nil
}
*/

func (txn *Transaction) getTableMeta(ctx context.Context, databaseId uint64,
	name string, needUpdated bool, columnLength int, prefetch bool) (*tableMeta, error) {
	blocks := make([][]BlockMeta, len(txn.dnStores))
	if needUpdated {
		for i, dnStore := range txn.dnStores {
			rows, err := txn.getRows(ctx, name, databaseId, 0,
				[]DNStore{dnStore}, catalog.MoTableMetaDefs, catalog.MoTableMetaSchema, nil)
			if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
				continue
			}
			if err != nil {
				return nil, err
			}
			blocks[i], err = genBlockMetas(ctx, rows, columnLength, txn.proc.FileService,
				txn.proc.GetMPool(), prefetch)
			if err != nil {
				return nil, moerr.NewInternalError(ctx, "disttae: getTableMeta err: %v, table: %v", err.Error(), name)
			}
		}
	}
	return &tableMeta{
		tableName: name,
		blocks:    blocks,
		defs:      catalog.MoTableMetaDefs,
	}, nil
}

// detecting whether a transaction is a read-only transaction
func (txn *Transaction) ReadOnly() bool {
	return txn.readOnly
}

// use for solving halloween problem
func (txn *Transaction) IncStatementId() {
	txn.statementId++
	txn.writes = append(txn.writes, make([]Entry, 0, 1))
}

// Write used to write data to the transaction buffer
// insert/delete/update all use this api
func (txn *Transaction) WriteBatch(
	typ int,
	databaseId uint64,
	tableId uint64,
	databaseName string,
	tableName string,
	bat *batch.Batch,
	dnStore DNStore,
	primaryIdx int, // pass -1 to indicate no primary key or disable primary key checking
) error {
	txn.readOnly = false
	bat.Cnt = 1
	if typ == INSERT {
		len := bat.Length()
		vec := vector.New(types.New(types.T_Rowid, 0, 0, 0))
		for i := 0; i < len; i++ {
			if err := vec.Append(txn.genRowId(), false,
				txn.proc.Mp()); err != nil {
				return err
			}
		}
		bat.Vecs = append([]*vector.Vector{vec}, bat.Vecs...)
		bat.Attrs = append([]string{catalog.Row_ID}, bat.Attrs...)
	}
	txn.Lock()
	txn.writes[txn.statementId] = append(txn.writes[txn.statementId], Entry{
		typ:          typ,
		bat:          bat,
		tableId:      tableId,
		databaseId:   databaseId,
		tableName:    tableName,
		databaseName: databaseName,
		dnStore:      dnStore,
	})
	txn.Unlock()

	if err := txn.checkPrimaryKey(typ, primaryIdx, bat, tableName, tableId); err != nil {
		return err
	}

	return nil
}

func (txn *Transaction) checkPrimaryKey(
	typ int,
	primaryIdx int,
	bat *batch.Batch,
	tableName string,
	tableId uint64,
) error {

	// no primary key
	if primaryIdx < 0 {
		return nil
	}

	//TODO ignore these buggy auto incr tables for now
	if strings.Contains(tableName, "%!%mo_increment") {
		return nil
	}

	t := txn.nextLocalTS()
	tx := memorytable.NewTransaction(t)
	iter := memorytable.NewBatchIter(bat)
	for {
		tuple := iter()
		if len(tuple) == 0 {
			break
		}

		rowID := RowID(tuple[0].Value.(types.Rowid))

		switch typ {

		case INSERT:
			var indexes []memtable.Tuple

			idx := primaryIdx + 1 // skip the first row id column
			primaryKey := memtable.ToOrdered(tuple[idx].Value)
			index := memtable.Tuple{
				index_TableID_PrimaryKey,
				memtable.ToOrdered(tableId),
				primaryKey,
			}

			// check primary key
			entries, err := txn.workspace.Index(tx, index)
			if err != nil {
				return err
			}
			if len(entries) > 0 {
				return moerr.NewDuplicateEntry(
					txn.proc.Ctx,
					common.TypeStringValue(bat.Vecs[idx].Typ, tuple[idx].Value),
					bat.Attrs[idx],
				)
			}

			// add primary key
			indexes = append(indexes, index)

			row := &workspaceRow{
				rowID:   rowID,
				tableID: tableId,
				indexes: indexes,
			}
			err = txn.workspace.Insert(tx, row)
			if err != nil {
				return err
			}

		case DELETE:
			err := txn.workspace.Delete(tx, rowID)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}

		}
	}
	if err := tx.Commit(t); err != nil {
		return err
	}

	return nil
}

func (txn *Transaction) nextLocalTS() timestamp.Timestamp {
	txn.localTS = txn.localTS.Next()
	return txn.localTS
}

// WriteFile used to add a s3 file information to the transaction buffer
// insert/delete/update all use this api
func (txn *Transaction) WriteFile(typ int, databaseId, tableId uint64,
	databaseName, tableName string, fileName string, bat *batch.Batch, dnStore DNStore) error {
	txn.readOnly = false
	txn.writes[txn.statementId] = append(txn.writes[txn.statementId], Entry{
		typ:          typ,
		tableId:      tableId,
		databaseId:   databaseId,
		tableName:    tableName,
		databaseName: databaseName,
		fileName:     fileName,
		bat:          bat,
		dnStore:      dnStore,
	})
	return nil
}

// getRow used to get a row of table based on a condition
/*
func (txn *Transaction) getRow(ctx context.Context, databaseId uint64, tableId uint64,
	dnList []DNStore, defs []engine.TableDef, columns []string, expr *plan.Expr) ([]any, error) {
	bats, err := txn.readTable(ctx, "", databaseId, tableId, defs, dnList, columns, expr)
	if err != nil {
		return nil, err
	}
	if len(bats) == 0 {
		return nil, moerr.GetOkExpectedEOB()
	}
	rows := make([][]any, 0, len(bats))
	for _, bat := range bats {
		if bat.Length() > 0 {
			rows = append(rows, catalog.GenRows(bat)...)
		}
		bat.Clean(txn.proc.Mp())
	}
	if len(rows) == 0 {
		return nil, moerr.GetOkExpectedEOB()
	}
	if len(rows) != 1 {
		return nil, moerr.NewInvalidInput(ctx, "table is not unique")
	}
	return rows[0], nil
}
*/

// getRows used to get rows of table
func (txn *Transaction) getRows(ctx context.Context, name string, databaseId uint64, tableId uint64,
	dnList []DNStore, defs []engine.TableDef, columns []string, expr *plan.Expr) ([][]any, error) {
	bats, err := txn.readTable(ctx, name, databaseId, tableId, defs, dnList, columns, expr)
	if err != nil {
		return nil, err
	}
	if len(bats) == 0 {
		return nil, moerr.GetOkExpectedEOB()
	}
	rows := make([][]any, 0, len(bats))
	for _, bat := range bats {
		if bat.Length() > 0 {
			rows = append(rows, catalog.GenRows(bat)...)
		}
		bat.Clean(txn.proc.Mp())
	}
	return rows, nil
}

// readTable used to get tuples of table based on a condition
// only used to read data from catalog, for which the execution is currently single-core
func (txn *Transaction) readTable(ctx context.Context, name string, databaseId uint64, tableId uint64,
	defs []engine.TableDef, dnList []DNStore, columns []string, expr *plan.Expr) ([]*batch.Batch, error) {
	var parts Partitions
	/*
		var writes [][]Entry
		// consider halloween problem
		if int64(txn.statementId)-1 > 0 {
			writes = txn.writes[:txn.statementId-1]
		}
	*/
	writes := make([]Entry, 0, len(txn.writes))
	if len(name) == 0 { // meta table not need this
		for i := range txn.writes {
			for _, entry := range txn.writes[i] {
				if entry.databaseId == databaseId &&
					entry.tableId == tableId {
					writes = append(writes, entry)
				}
			}
		}
	}
	bats := make([]*batch.Batch, 0, 1)
	accessed := make(map[string]uint8)
	for _, dn := range dnList {
		accessed[dn.ServiceID] = 0
	}
	if len(name) == 0 {
		parts = txn.engine.getPartitions(databaseId, tableId)
	} else {
		parts = txn.engine.getMetaPartitions(name)
	}
	for i, dn := range txn.dnStores {
		if _, ok := accessed[dn.ServiceID]; !ok {
			continue
		}
		rds, err := parts[i].NewReader(ctx, 1, nil, defs, nil, nil, nil,
			txn.meta.SnapshotTS, nil, writes)
		if err != nil {
			return nil, err
		}
		for _, rd := range rds {
			for {
				bat, err := rd.Read(ctx, columns, expr, txn.proc.Mp())
				if err != nil {
					return nil, err
				}
				if bat != nil {
					bats = append(bats, bat)
				} else {
					break
				}
			}
		}
	}
	if expr == nil {
		return bats, nil
	}
	for i, bat := range bats {
		vec, err := colexec.EvalExpr(bat, txn.proc, expr)
		if err != nil {
			return nil, err
		}
		bs := vector.GetColumn[bool](vec)
		if vec.IsScalar() {
			if !bs[0] {
				bat.Shrink(nil)
			}
		} else {
			sels := txn.proc.Mp().GetSels()
			for i, b := range bs {
				if b {
					sels = append(sels, int64(i))
				}
			}
			bat.Shrink(sels)
			txn.proc.Mp().PutSels(sels)
		}
		vec.Free(txn.proc.Mp())
		bats[i] = bat
	}
	return bats, nil
}

func (txn *Transaction) deleteBatch(bat *batch.Batch,
	databaseId, tableId uint64) *batch.Batch {

	// tx for workspace operations
	t := txn.nextLocalTS()
	tx := memorytable.NewTransaction(t)
	defer func() {
		if err := tx.Commit(t); err != nil {
			panic(err)
		}
	}()

	mp := make(map[types.Rowid]uint8)
	rowids := vector.MustTCols[types.Rowid](bat.GetVector(0))
	for _, rowid := range rowids {
		mp[rowid] = 0
		// update workspace
		err := txn.workspace.Delete(tx, RowID(rowid))
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			panic(err)
		}
	}

	sels := txn.proc.Mp().GetSels()
	for i := range txn.writes {
		for j, e := range txn.writes[i] {
			sels = sels[:0]
			if e.tableId == tableId && e.databaseId == databaseId {
				vs := vector.MustTCols[types.Rowid](e.bat.GetVector(0))
				for k, v := range vs {
					if _, ok := mp[v]; !ok {
						sels = append(sels, int64(k))
					} else {
						mp[v]++
					}
				}
				if len(sels) != len(vs) {
					txn.writes[i][j].bat.Shrink(sels)
				}
			}
		}
	}
	sels = sels[:0]
	for k, rowid := range rowids {
		if mp[rowid] == 0 {
			sels = append(sels, int64(k))
		}
	}
	bat.Shrink(sels)
	txn.proc.Mp().PutSels(sels)
	return bat
}

func (txn *Transaction) allocateID(ctx context.Context) (uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	return txn.idGen.AllocateID(ctx)
}

func (txn *Transaction) genRowId() types.Rowid {
	txn.rowId[1]++
	return types.DecodeFixed[types.Rowid](types.EncodeSlice(txn.rowId[:]))
}

func (h transactionHeap) Len() int {
	return len(h)
}

func (h transactionHeap) Less(i, j int) bool {
	return h[i].meta.SnapshotTS.Less(h[j].meta.SnapshotTS)
}

func (h transactionHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *transactionHeap) Push(x any) {
	*h = append(*h, x.(*Transaction))
}

func (h *transactionHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// needRead determine if a block needs to be read
func needRead(ctx context.Context, expr *plan.Expr, blkInfo BlockMeta, tableDef *plan.TableDef, columnMap map[int]int, columns []int, maxCol int, proc *process.Process) bool {
	var err error
	if expr == nil {
		return true
	}
	notReportErrCtx := errutil.ContextWithNoReport(ctx, true)

	// if expr match no columns, just eval expr
	if len(columns) == 0 {
		bat := batch.NewWithSize(0)
		defer bat.Clean(proc.Mp())
		ifNeed, err := plan2.EvalFilterExpr(notReportErrCtx, expr, bat, proc)
		if err != nil {
			return true
		}
		return ifNeed
	}

	// get min max data from Meta
	datas, dataTypes, err := getZonemapDataFromMeta(ctx, columns, blkInfo, tableDef)
	if err != nil || datas == nil {
		return true
	}

	// use all min/max data to build []vectors.
	buildVectors := plan2.BuildVectorsByData(datas, dataTypes, proc.Mp())
	bat := batch.NewWithSize(maxCol + 1)
	defer bat.Clean(proc.Mp())
	for k, v := range columnMap {
		for i, realIdx := range columns {
			if realIdx == v {
				bat.SetVector(int32(k), buildVectors[i])
				break
			}
		}
	}
	bat.SetZs(buildVectors[0].Length(), proc.Mp())

	ifNeed, err := plan2.EvalFilterExpr(notReportErrCtx, expr, bat, proc)
	if err != nil {
		return true
	}
	return ifNeed

}

// get row count of block
func blockRows(meta BlockMeta) int64 {
	return meta.Rows
}

func blockMarshal(meta BlockMeta) []byte {
	data, _ := types.Encode(meta)
	return data
}

func blockUnmarshal(data []byte) BlockMeta {
	var meta BlockMeta

	types.Decode(data, &meta)
	return meta
}

func needSyncDnStores(ctx context.Context, expr *plan.Expr, tableDef *plan.TableDef,
	priKeys []*engine.Attribute, dnStores []DNStore, proc *process.Process) []int {
	var pk *engine.Attribute

	fullList := func() []int {
		dnList := make([]int, len(dnStores))
		for i := range dnStores {
			dnList[i] = i
		}
		return dnList
	}
	if len(dnStores) == 1 {
		return []int{0}
	}
	for _, key := range priKeys {
		// If it is a composite primary key, skip
		if key.Name == catalog.CPrimaryKeyColName {
			continue
		}
		pk = key
		break
	}
	// have no PrimaryKey, return all the list
	if expr == nil || pk == nil || tableDef == nil {
		return fullList()
	}
	if pk.Type.IsIntOrUint() {
		canComputeRange, intPkRange := computeRangeByIntPk(expr, pk.Name, "")
		if !canComputeRange {
			return fullList()
		}
		if intPkRange.isRange {
			r := intPkRange.ranges
			if r[0] == math.MinInt64 || r[1] == math.MaxInt64 || r[1]-r[0] > MAX_RANGE_SIZE {
				return fullList()
			}
			intPkRange.isRange = false
			for i := intPkRange.ranges[0]; i <= intPkRange.ranges[1]; i++ {
				intPkRange.items = append(intPkRange.items, i)
			}
		}
		return getListByItems(dnStores, intPkRange.items)
	}
	canComputeRange, hashVal := computeRangeByNonIntPk(ctx, expr, pk.Name, proc)
	if !canComputeRange {
		return fullList()
	}
	listLen := uint64(len(dnStores))
	idx := hashVal % listLen
	return []int{int(idx)}
}
