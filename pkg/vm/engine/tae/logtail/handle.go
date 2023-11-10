// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logtail

/*

an application on logtail mgr: build reponse to SyncLogTailRequest

More docs:
https://github.com/matrixorigin/docs/blob/main/tech-notes/dnservice/ref_logtail_impl.md


Main workflow:

          +------------------+
          | CheckpointRunner |
          +------------------+
            ^         |
            | range   | ckp & newRange
            |         v
          +------------------+  newRange  +----------------+  snapshot   +--------------+
 user ->  | HandleGetLogTail | ---------> | LogtailManager | ----------> | LogtailTable |
   ^      +------------------+            +----------------+             +--------------+
   |                                                                        |
   |           +------------------+                                         |
   +---------- |   RespBuilder    |  ------------------>+-------------------+
      return   +------------------+                     |
      entries                                           |  visit
                                                        |
                                                        v
                                  +-----------------------------------+
                                  |     txnblock2                     |
                     ...          +-----------------------------------+   ...
                                  | bornTs  | ... txn100 | txn101 |.. |
                                  +-----------------+---------+-------+
                                                    |         |
                                                    |         |
                                                    |         |
                                  +-----------------+    +----+-------+     dirty blocks
                                  |                 |    |            |
                                  v                 v    v            v
                              +-------+           +-------+       +-------+
                              | BLK-1 |           | BLK-2 |       | BLK-3 |
                              +---+---+           +---+---+       +---+---+
                                  |                   |               |
                                  v                   v               v
                            [V1@t25,disk]       [V1@t17,mem]     [V1@t17,disk]
                                  |                   |               |
                                  v                   v               v
                            [V0@t12,mem]        [V0@t10,mem]     [V0@t10,disk]
                                  |                                   |
                                  v                                   v
                            [V0@t7,mem]                           [V0@t7,mem]


*/

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"

	"github.com/matrixorigin/matrixone/pkg/perfcounter"

	"github.com/matrixorigin/matrixone/pkg/objectio"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
	"go.uber.org/zap"
)

const Size90M = 90 * 1024 * 1024

type CheckpointClient interface {
	CollectCheckpointsInRange(ctx context.Context, start, end types.TS) (ckpLoc string, lastEnd types.TS, err error)
	FlushTable(ctx context.Context, dbID, tableID uint64, ts types.TS) error
}

func DecideTableScope(tableID uint64) Scope {
	var scope Scope
	switch tableID {
	case pkgcatalog.MO_DATABASE_ID:
		scope = ScopeDatabases
	case pkgcatalog.MO_TABLES_ID:
		scope = ScopeTables
	case pkgcatalog.MO_COLUMNS_ID:
		scope = ScopeColumns
	default:
		scope = ScopeUserTables
	}
	return scope
}

func HandleSyncLogTailReq(
	ctx context.Context,
	ckpClient CheckpointClient,
	mgr *Manager,
	c *catalog.Catalog,
	req api.SyncLogTailReq,
	canRetry bool) (resp api.SyncLogTailResp, closeCB func(), err error) {
	now := time.Now()
	logutil.Debugf("[Logtail] begin handle %+v", req)
	defer func() {
		if elapsed := time.Since(now); elapsed > 5*time.Second {
			logutil.Infof("[Logtail] long pull cost %v, %v: %+v, %v ", elapsed, canRetry, req, err)
		}
		logutil.Debugf("[Logtail] end handle %d entries[%q], err %v", len(resp.Commands), resp.CkpLocation, err)
	}()
	start := types.BuildTS(req.CnHave.PhysicalTime, req.CnHave.LogicalTime)
	end := types.BuildTS(req.CnWant.PhysicalTime, req.CnWant.LogicalTime)
	did, tid := req.Table.DbId, req.Table.TbId
	dbEntry, err := c.GetDatabaseByID(did)
	if err != nil {
		return
	}
	tableEntry, err := dbEntry.GetTableEntryByID(tid)
	if err != nil {
		return
	}
	tableEntry.RLock()
	createTS := tableEntry.GetCreatedAtLocked()
	tableEntry.RUnlock()
	if start.Less(createTS) {
		start = createTS
	}

	ckpLoc, checkpointed, err := ckpClient.CollectCheckpointsInRange(ctx, start, end)
	if err != nil {
		return
	}

	if checkpointed.GreaterEq(end) {
		return api.SyncLogTailResp{
			CkpLocation: ckpLoc,
		}, nil, err
	} else if ckpLoc != "" {
		start = checkpointed.Next()
	}

	scope := DecideTableScope(tid)

	var visitor RespBuilder

	if scope == ScopeUserTables {
		visitor = NewTableLogtailRespBuilder(ctx, ckpLoc, start, end, tableEntry)
	} else {
		visitor = NewCatalogLogtailRespBuilder(ctx, scope, ckpLoc, start, end)
	}
	closeCB = visitor.Close

	operator := mgr.GetTableOperator(start, end, c, did, tid, scope, visitor)
	if err := operator.Run(); err != nil {
		return api.SyncLogTailResp{}, visitor.Close, err
	}
	resp, err = visitor.BuildResp()

	if canRetry && scope == ScopeUserTables { // check simple conditions first
		_, name, forceFlush := fault.TriggerFault("logtail_max_size")
		if (forceFlush && name == tableEntry.GetLastestSchema().Name) || resp.ProtoSize() > Size90M {
			_ = ckpClient.FlushTable(ctx, did, tid, end)
			// try again after flushing
			newResp, closeCB, err := HandleSyncLogTailReq(ctx, ckpClient, mgr, c, req, false)
			logutil.Infof("[logtail] flush result: %d -> %d err: %v", resp.ProtoSize(), newResp.ProtoSize(), err)
			return newResp, closeCB, err
		}
	}
	return
}

type RespBuilder interface {
	catalog.Processor
	BuildResp() (api.SyncLogTailResp, error)
	Close()
}

// CatalogLogtailRespBuilder knows how to make api-entry from db and table entry.
// impl catalog.Processor interface, driven by BoundTableOperator
type CatalogLogtailRespBuilder struct {
	ctx context.Context
	*catalog.LoopProcessor
	scope              Scope
	start, end         types.TS
	checkpoint         string
	insBatch           *containers.Batch
	delBatch           *containers.Batch
	specailDeleteBatch *containers.Batch
}

func NewCatalogLogtailRespBuilder(ctx context.Context, scope Scope, ckp string, start, end types.TS) *CatalogLogtailRespBuilder {
	b := &CatalogLogtailRespBuilder{
		ctx:           ctx,
		LoopProcessor: new(catalog.LoopProcessor),
		scope:         scope,
		start:         start,
		end:           end,
		checkpoint:    ckp,
	}
	switch scope {
	case ScopeDatabases:
		b.insBatch = makeRespBatchFromSchema(catalog.SystemDBSchema)
		b.delBatch = makeRespBatchFromSchema(DBDelSchema)
		b.specailDeleteBatch = makeRespBatchFromSchema(DBSpecialDeleteSchema)
	case ScopeTables:
		b.insBatch = makeRespBatchFromSchema(catalog.SystemTableSchema)
		b.delBatch = makeRespBatchFromSchema(TblDelSchema)
		b.specailDeleteBatch = makeRespBatchFromSchema(TBLSpecialDeleteSchema)
	case ScopeColumns:
		b.insBatch = makeRespBatchFromSchema(catalog.SystemColumnSchema)
		b.delBatch = makeRespBatchFromSchema(ColumnDelSchema)
	}
	b.DatabaseFn = b.VisitDB
	b.TableFn = b.VisitTbl

	return b
}

func (b *CatalogLogtailRespBuilder) Close() {
	if b.insBatch != nil {
		b.insBatch.Close()
		b.insBatch = nil
	}
	if b.delBatch != nil {
		b.delBatch.Close()
		b.delBatch = nil
	}
}

// VisitDB = catalog.Processor.OnDatabase
func (b *CatalogLogtailRespBuilder) VisitDB(entry *catalog.DBEntry) error {
	entry.RLock()
	if shouldIgnoreDBInLogtail(entry.ID) {
		entry.RUnlock()
		return nil
	}
	mvccNodes := entry.ClonePreparedInRange(b.start, b.end)
	entry.RUnlock()
	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		dbNode := node
		if dbNode.HasDropCommitted() {
			// delScehma is empty, it will just fill rowid / commit ts
			catalogEntry2Batch(b.delBatch, entry, dbNode, DBDelSchema, txnimpl.FillDBRow, objectio.HackU64ToRowid(entry.GetID()), dbNode.GetEnd())
			catalogEntry2Batch(b.specailDeleteBatch, entry, node, DBSpecialDeleteSchema, txnimpl.FillDBRow, objectio.HackU64ToRowid(entry.GetID()), node.GetEnd())
		} else {
			catalogEntry2Batch(b.insBatch, entry, dbNode, catalog.SystemDBSchema, txnimpl.FillDBRow, objectio.HackU64ToRowid(entry.GetID()), dbNode.GetEnd())
		}
	}
	return nil
}

// VisitTbl = catalog.Processor.OnTable
func (b *CatalogLogtailRespBuilder) VisitTbl(entry *catalog.TableEntry) error {
	entry.RLock()
	if shouldIgnoreTblInLogtail(entry.ID) {
		entry.RUnlock()
		return nil
	}
	mvccNodes := entry.ClonePreparedInRange(b.start, b.end)
	entry.RUnlock()
	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		if b.scope == ScopeColumns {
			var dstBatch *containers.Batch
			if !node.HasDropCommitted() {
				dstBatch = b.insBatch
				// fill unique syscol fields if inserting
				for _, syscol := range catalog.SystemColumnSchema.ColDefs {
					txnimpl.FillColumnRow(entry, node, syscol.Name, b.insBatch.GetVectorByName(syscol.Name))
				}
				// send dropped column del
				for _, name := range node.BaseNode.Schema.Extra.DroppedAttrs {
					b.delBatch.GetVectorByName(catalog.AttrRowID).Append(objectio.HackBytes2Rowid([]byte(fmt.Sprintf("%d-%s", entry.GetID(), name))), false)
					b.delBatch.GetVectorByName(catalog.AttrCommitTs).Append(node.GetEnd(), false)
					b.delBatch.GetVectorByName(pkgcatalog.SystemColAttr_UniqName).Append([]byte(fmt.Sprintf("%d-%s", entry.GetID(), name)), false)
				}
			} else {
				dstBatch = b.delBatch
			}

			// fill common syscol fields for every user column
			rowidVec := dstBatch.GetVectorByName(catalog.AttrRowID)
			commitVec := dstBatch.GetVectorByName(catalog.AttrCommitTs)
			tableID := entry.GetID()
			commitTs := node.GetEnd()
			for _, usercol := range node.BaseNode.Schema.ColDefs {
				rowidVec.Append(objectio.HackBytes2Rowid([]byte(fmt.Sprintf("%d-%s", tableID, usercol.Name))), false)
				commitVec.Append(commitTs, false)
			}
		} else {
			if node.HasDropCommitted() {
				catalogEntry2Batch(b.delBatch, entry, node, TblDelSchema, txnimpl.FillTableRow, objectio.HackU64ToRowid(entry.GetID()), node.GetEnd())
				catalogEntry2Batch(b.specailDeleteBatch, entry, node, TBLSpecialDeleteSchema, txnimpl.FillTableRow, objectio.HackU64ToRowid(entry.GetID()), node.GetEnd())
			} else {
				catalogEntry2Batch(b.insBatch, entry, node, catalog.SystemTableSchema, txnimpl.FillTableRow, objectio.HackU64ToRowid(entry.GetID()), node.GetEnd())
			}
		}
	}
	return nil
}

func (b *CatalogLogtailRespBuilder) BuildResp() (api.SyncLogTailResp, error) {
	entries := make([]*api.Entry, 0)
	var tblID uint64
	var tableName string
	switch b.scope {
	case ScopeDatabases:
		tblID = pkgcatalog.MO_DATABASE_ID
		tableName = pkgcatalog.MO_DATABASE
	case ScopeTables:
		tblID = pkgcatalog.MO_TABLES_ID
		tableName = pkgcatalog.MO_TABLES
	case ScopeColumns:
		tblID = pkgcatalog.MO_COLUMNS_ID
		tableName = pkgcatalog.MO_COLUMNS
	}

	if b.insBatch.Length() > 0 {
		bat, err := containersBatchToProtoBatch(b.insBatch)
		logutil.Debugf("[logtail] catalog insert to %d-%s, %s", tblID, tableName,
			DebugBatchToString("catalog", b.insBatch, true, zap.DebugLevel))
		if err != nil {
			return api.SyncLogTailResp{}, err
		}
		insEntry := &api.Entry{
			EntryType:    api.Entry_Insert,
			TableId:      tblID,
			TableName:    tableName,
			DatabaseId:   pkgcatalog.MO_CATALOG_ID,
			DatabaseName: pkgcatalog.MO_CATALOG,
			Bat:          bat,
		}
		entries = append(entries, insEntry)
		perfcounter.Update(b.ctx, func(counter *perfcounter.CounterSet) {
			counter.TAE.LogTail.Entries.Add(int64(b.insBatch.Length()))
			counter.TAE.LogTail.InsertEntries.Add(int64(b.insBatch.Length()))
		})
	}
	if b.delBatch.Length() > 0 {
		bat, err := containersBatchToProtoBatch(b.delBatch)
		logutil.Debugf("[logtail] catalog delete from %d-%s, %s", tblID, tableName,
			DebugBatchToString("catalog", b.delBatch, false, zap.DebugLevel))
		if err != nil {
			return api.SyncLogTailResp{}, err
		}
		delEntry := &api.Entry{
			EntryType:    api.Entry_Delete,
			TableId:      tblID,
			TableName:    tableName,
			DatabaseId:   pkgcatalog.MO_CATALOG_ID,
			DatabaseName: pkgcatalog.MO_CATALOG,
			Bat:          bat,
		}
		entries = append(entries, delEntry)
		perfcounter.Update(b.ctx, func(counter *perfcounter.CounterSet) {
			counter.TAE.LogTail.Entries.Add(int64(b.delBatch.Length()))
			counter.TAE.LogTail.DeleteEntries.Add(int64(b.delBatch.Length()))
		})
	}
	if b.specailDeleteBatch != nil && b.specailDeleteBatch.Length() > 0 {
		bat, err := containersBatchToProtoBatch(b.specailDeleteBatch)
		if err != nil {
			return api.SyncLogTailResp{}, err
		}
		delEntry := &api.Entry{
			EntryType:    api.Entry_SpecialDelete,
			TableId:      tblID,
			TableName:    tableName,
			DatabaseId:   pkgcatalog.MO_CATALOG_ID,
			DatabaseName: pkgcatalog.MO_CATALOG,
			Bat:          bat,
		}
		entries = append(entries, delEntry)
		perfcounter.Update(b.ctx, func(counter *perfcounter.CounterSet) {
			counter.TAE.LogTail.Entries.Add(int64(b.delBatch.Length()))
			counter.TAE.LogTail.DeleteEntries.Add(int64(b.delBatch.Length()))
		})
	}
	return api.SyncLogTailResp{
		CkpLocation: b.checkpoint,
		Commands:    entries,
	}, nil
}

// this is used to collect ONE ROW of db or table change
func catalogEntry2Batch[
	T *catalog.DBEntry | *catalog.TableEntry,
	N *catalog.MVCCNode[*catalog.EmptyMVCCNode] | *catalog.MVCCNode[*catalog.TableMVCCNode]](
	dstBatch *containers.Batch,
	e T,
	node N,
	schema *catalog.Schema,
	fillDataRow func(e T, node N, attr string, col containers.Vector),
	rowid types.Rowid,
	commitTs types.TS,
) {
	for _, col := range schema.ColDefs {
		fillDataRow(e, node, col.Name, dstBatch.GetVectorByName(col.Name))
	}
	dstBatch.GetVectorByName(catalog.AttrRowID).Append(rowid, false)
	dstBatch.GetVectorByName(catalog.AttrCommitTs).Append(commitTs, false)
}

// CatalogLogtailRespBuilder knows how to make api-entry from block entry.
// impl catalog.Processor interface, driven by BoundTableOperator
type TableLogtailRespBuilder struct {
	ctx context.Context
	*catalog.LoopProcessor
	start, end      types.TS
	did, tid        uint64
	dname, tname    string
	checkpoint      string
	blkMetaInsBatch *containers.Batch
	blkMetaDelBatch *containers.Batch
	segMetaDelBatch *containers.Batch
	dataInsBatches  map[uint32]*containers.Batch // schema version -> data batch
	dataDelBatch    *containers.Batch
}

func NewTableLogtailRespBuilder(ctx context.Context, ckp string, start, end types.TS, tbl *catalog.TableEntry) *TableLogtailRespBuilder {
	b := &TableLogtailRespBuilder{
		ctx:           ctx,
		LoopProcessor: new(catalog.LoopProcessor),
		start:         start,
		end:           end,
		checkpoint:    ckp,
	}
	b.BlockFn = b.VisitBlk
	b.SegmentFn = b.VisitSeg

	b.did = tbl.GetDB().GetID()
	b.tid = tbl.ID
	b.dname = tbl.GetDB().GetName()
	b.tname = tbl.GetLastestSchema().Name

	b.dataInsBatches = make(map[uint32]*containers.Batch)
	b.dataDelBatch = makeRespBatchFromSchema(DelSchema)
	b.blkMetaInsBatch = makeRespBatchFromSchema(BlkMetaSchema)
	b.blkMetaDelBatch = makeRespBatchFromSchema(DelSchema)
	b.segMetaDelBatch = makeRespBatchFromSchema(DelSchema)
	return b
}

func (b *TableLogtailRespBuilder) Close() {
	for _, vec := range b.dataInsBatches {
		if vec != nil {
			vec.Close()
		}
	}
	b.dataInsBatches = nil
	if b.dataDelBatch != nil {
		b.dataDelBatch.Close()
		b.dataDelBatch = nil
	}
	if b.blkMetaInsBatch != nil {
		b.blkMetaInsBatch.Close()
		b.blkMetaInsBatch = nil
	}
	if b.blkMetaDelBatch != nil {
		b.blkMetaDelBatch.Close()
		b.blkMetaDelBatch = nil
	}
}

func (b *TableLogtailRespBuilder) VisitSeg(e *catalog.SegmentEntry) error {
	e.RLock()
	mvccNodes := e.ClonePreparedInRange(b.start, b.end)
	e.RUnlock()

	for _, node := range mvccNodes {
		if node.HasDropCommitted() {
			// send segment deletation event
			b.segMetaDelBatch.GetVectorByName(catalog.AttrCommitTs).Append(node.DeletedAt, false)
			b.segMetaDelBatch.GetVectorByName(catalog.AttrRowID).Append(objectio.HackSegid2Rowid(&e.ID), false)
		}
	}
	return nil
}

// visitBlkMeta try to collect block metadata. It might prefetch and generate duplicated entry.
// see also https://github.com/matrixorigin/docs/blob/main/tech-notes/dnservice/ref_logtail_protocol.md#table-metadata-prefetch
func (b *TableLogtailRespBuilder) visitBlkMeta(e *catalog.BlockEntry) (skipData bool) {
	newEnd := b.end
	e.RLock()
	// try to find new end
	if newest := e.GetLatestCommittedNode(); newest != nil {
		latestPrepareTs := newest.GetPrepare()
		if latestPrepareTs.Greater(b.end) {
			newEnd = latestPrepareTs
		}
	}
	mvccNodes := e.ClonePreparedInRange(b.start, newEnd)
	e.RUnlock()

	for _, node := range mvccNodes {
		metaNode := node
		if !metaNode.BaseNode.MetaLoc.IsEmpty() && !metaNode.IsAborted() {
			b.appendBlkMeta(e, metaNode)
		}
	}

	if n := len(mvccNodes); n > 0 {
		newest := mvccNodes[n-1]
		if e.IsAppendable() {
			if !newest.BaseNode.MetaLoc.IsEmpty() {
				// appendable block has been flushed, no need to collect data
				return true
			}
		} else {
			if !newest.BaseNode.DeltaLoc.IsEmpty() && newest.GetEnd().GreaterEq(b.end) {
				// non-appendable block has newer delta data on s3, no need to collect data
				return true
			}
		}
	}
	return false
}

// appendBlkMeta add block metadata into api entry according to logtail protocol
// see also https://github.com/matrixorigin/docs/blob/main/tech-notes/dnservice/ref_logtail_protocol.md#table-metadata
func (b *TableLogtailRespBuilder) appendBlkMeta(e *catalog.BlockEntry, metaNode *catalog.MVCCNode[*catalog.MetadataMVCCNode]) {
	visitBlkMeta(e, metaNode, b.blkMetaInsBatch, b.blkMetaDelBatch, metaNode.HasDropCommitted(), metaNode.End, metaNode.CreatedAt, metaNode.DeletedAt)
}

func visitBlkMeta(e *catalog.BlockEntry, node *catalog.MVCCNode[*catalog.MetadataMVCCNode], insBatch, delBatch *containers.Batch, delete bool, committs, createts, deletets types.TS) {
	common.DoIfDebugEnabled(func() {
		logutil.Debugf("[Logtail] record block meta row %s, %v, %s, %s, %s, %s",
			e.AsCommonID().String(), e.IsAppendable(),
			createts.ToString(), node.DeletedAt.ToString(), node.BaseNode.MetaLoc, node.BaseNode.DeltaLoc)
	})
	is_sorted := false
	if !e.IsAppendable() && e.GetSchema().HasSortKey() {
		is_sorted = true
	}
	insBatch.GetVectorByName(pkgcatalog.BlockMeta_ID).Append(e.ID, false)
	insBatch.GetVectorByName(pkgcatalog.BlockMeta_EntryState).Append(e.IsAppendable(), false)
	insBatch.GetVectorByName(pkgcatalog.BlockMeta_Sorted).Append(is_sorted, false)
	insBatch.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(node.BaseNode.MetaLoc), false)
	insBatch.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(node.BaseNode.DeltaLoc), false)
	insBatch.GetVectorByName(pkgcatalog.BlockMeta_CommitTs).Append(committs, false)
	insBatch.GetVectorByName(pkgcatalog.BlockMeta_SegmentID).Append(e.GetSegment().ID, false)
	// for appendable block(deleted, because we skip empty metaloc), non-dropped non-appendabled blocks, those new nodes are
	// produced by flush table tail, it's safe to truncate mem data in CN
	memTruncTs := node.Start
	if !e.IsAppendable() && committs.Equal(deletets) {
		// for deleted non-appendable block, it must be produced by merging blocks. In this case,
		// do not truncate any data in CN, because merging blocks didn't flush deletes to disk.
		memTruncTs = types.TS{}
	}

	insBatch.GetVectorByName(pkgcatalog.BlockMeta_MemTruncPoint).Append(memTruncTs, false)
	insBatch.GetVectorByName(catalog.AttrCommitTs).Append(createts, false)
	insBatch.GetVectorByName(catalog.AttrRowID).Append(objectio.HackBlockid2Rowid(&e.ID), false)

	// if block is deleted, send both Insert and Delete api entry
	// see also https://github.com/matrixorigin/docs/blob/main/tech-notes/dnservice/ref_logtail_protocol.md#table-metadata-deletion-invalidate-table-data
	if delete {
		if node.DeletedAt.IsEmpty() {
			panic(moerr.NewInternalErrorNoCtx("no delete at time in a dropped entry"))
		}
		delBatch.GetVectorByName(catalog.AttrCommitTs).Append(deletets, false)
		delBatch.GetVectorByName(catalog.AttrRowID).Append(objectio.HackBlockid2Rowid(&e.ID), false)
	}

}

// visitBlkData collects logtail in memory
func (b *TableLogtailRespBuilder) visitBlkData(ctx context.Context, e *catalog.BlockEntry) (err error) {
	block := e.GetBlockData()
	insBatch, err := block.CollectAppendInRange(b.start, b.end, false)
	if err != nil {
		return
	}
	if insBatch != nil && insBatch.Length() > 0 {
		dest, ok := b.dataInsBatches[insBatch.Version]
		if !ok {
			// create new dest batch
			dest = DataChangeToLogtailBatch(insBatch)
			b.dataInsBatches[insBatch.Version] = dest
		} else {
			dest.Extend(insBatch.Batch)
			// insBatch is freed, don't use anymore
		}
	}
	delBatch, err := block.CollectDeleteInRange(ctx, b.start, b.end, false)
	if err != nil {
		return
	}
	if delBatch != nil && delBatch.Length() > 0 {
		if len(b.dataDelBatch.Vecs) == 2 {
			b.dataDelBatch.AddVector(delBatch.Attrs[2], containers.MakeVector(*delBatch.Vecs[2].GetType()))
		}
		b.dataDelBatch.Extend(delBatch)
		// delBatch is freed, don't use anymore
	}
	return nil
}

// VisitBlk = catalog.Processor.OnBlock
func (b *TableLogtailRespBuilder) VisitBlk(entry *catalog.BlockEntry) error {
	if b.visitBlkMeta(entry) {
		// data has been flushed, no need to collect data
		return nil
	}
	return b.visitBlkData(b.ctx, entry)
}

type TableRespKind int

const (
	TableRespKind_Data TableRespKind = iota
	TableRespKind_Blk
	TableRespKind_Seg
)

func (b *TableLogtailRespBuilder) BuildResp() (api.SyncLogTailResp, error) {
	entries := make([]*api.Entry, 0)
	tryAppendEntry := func(typ api.Entry_EntryType, kind TableRespKind, batch *containers.Batch, version uint32) error {
		if batch.Length() == 0 {
			return nil
		}
		bat, err := containersBatchToProtoBatch(batch)
		if err != nil {
			return err
		}

		tableName := ""
		switch kind {
		case TableRespKind_Data:
			tableName = b.tname
			logutil.Debugf("[logtail] table data [%v] %d-%s-%d: %s", typ, b.tid, b.tname, version,
				DebugBatchToString("data", batch, false, zap.InfoLevel))
		case TableRespKind_Blk:
			tableName = fmt.Sprintf("_%d_meta", b.tid)
			logutil.Debugf("[logtail] table meta [%v] %d-%s: %s", typ, b.tid, b.tname,
				DebugBatchToString("blkmeta", batch, false, zap.InfoLevel))
		case TableRespKind_Seg:
			tableName = fmt.Sprintf("_%d_seg", b.tid)
			logutil.Debugf("[logtail] table meta [%v] %d-%s: %s", typ, b.tid, b.tname,
				DebugBatchToString("segmeta", batch, false, zap.InfoLevel))
		}

		entry := &api.Entry{
			EntryType:    typ,
			TableId:      b.tid,
			TableName:    tableName,
			DatabaseId:   b.did,
			DatabaseName: b.dname,
			Bat:          bat,
		}
		entries = append(entries, entry)
		return nil
	}

	empty := api.SyncLogTailResp{}
	if err := tryAppendEntry(api.Entry_Insert, TableRespKind_Blk, b.blkMetaInsBatch, 0); err != nil {
		return empty, err
	}
	if err := tryAppendEntry(api.Entry_Delete, TableRespKind_Blk, b.blkMetaDelBatch, 0); err != nil {
		return empty, err
	}
	if err := tryAppendEntry(api.Entry_Delete, TableRespKind_Seg, b.segMetaDelBatch, 0); err != nil {
		return empty, err
	}
	keys := make([]uint32, 0, len(b.dataInsBatches))
	for k := range b.dataInsBatches {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for _, k := range keys {
		if err := tryAppendEntry(api.Entry_Insert, TableRespKind_Data, b.dataInsBatches[k], k); err != nil {
			return empty, err
		}
	}
	if err := tryAppendEntry(api.Entry_Delete, TableRespKind_Data, b.dataDelBatch, 0); err != nil {
		return empty, err
	}

	return api.SyncLogTailResp{
		CkpLocation: b.checkpoint,
		Commands:    entries,
	}, nil
}
func GetMetaIdxesByVersion(ver uint32) []uint16 {
	meteIdxSchema := checkpointDataReferVersions[ver][MetaIDX]
	idxes := make([]uint16, len(meteIdxSchema.attrs))
	for attr := range meteIdxSchema.attrs {
		idxes[attr] = uint16(attr)
	}
	return idxes
}
func LoadCheckpointEntries(
	ctx context.Context,
	metLoc string,
	tableID uint64,
	tableName string,
	dbID uint64,
	dbName string,
	mp *mpool.MPool,
	fs fileservice.FileService) ([]*api.Entry, []func(), error) {
	if metLoc == "" {
		return nil, nil, nil
	}
	v2.LogtailLoadCheckpointCounter.Inc()
	now := time.Now()
	defer func() {
		v2.LogTailLoadCheckpointDurationHistogram.Observe(time.Since(now).Seconds())
	}()
	locationsAndVersions := strings.Split(metLoc, ";")
	datas := make([]*CNCheckpointData, len(locationsAndVersions)/2)

	readers := make([]*blockio.BlockReader, len(locationsAndVersions)/2)
	objectLocations := make([]objectio.Location, len(locationsAndVersions)/2)
	versions := make([]uint32, len(locationsAndVersions)/2)
	locations := make([]objectio.Location, len(locationsAndVersions)/2)
	for i := 0; i < len(locationsAndVersions); i += 2 {
		key := locationsAndVersions[i]
		version, err := strconv.ParseUint(locationsAndVersions[i+1], 10, 32)
		if err != nil {
			return nil, nil, err
		}
		location, err := blockio.EncodeLocationFromString(key)
		if err != nil {
			return nil, nil, err
		}
		locations[i/2] = location
		reader, err := blockio.NewObjectReader(fs, location)
		if err != nil {
			return nil, nil, err
		}
		readers[i/2] = reader
		err = blockio.PrefetchMeta(fs, location)
		if err != nil {
			return nil, nil, err
		}
		objectLocations[i/2] = location
		versions[i/2] = uint32(version)
	}

	for i := range objectLocations {
		data := NewCNCheckpointData()
		meteIdxSchema := checkpointDataReferVersions[versions[i]][MetaIDX]
		idxes := make([]uint16, len(meteIdxSchema.attrs))
		for attr := range meteIdxSchema.attrs {
			idxes[attr] = uint16(attr)
		}
		err := data.PrefetchMetaIdx(ctx, versions[i], idxes, objectLocations[i], fs)
		if err != nil {
			return nil, nil, err
		}
		datas[i] = data
	}

	for i := range datas {
		err := datas[i].InitMetaIdx(ctx, versions[i], readers[i], locations[i], mp)
		if err != nil {
			return nil, nil, err
		}
	}

	for i := range datas {
		err := datas[i].PrefetchMetaFrom(ctx, versions[i], locations[i], fs, tableID)
		if err != nil {
			return nil, nil, err
		}
	}

	for i := range datas {
		err := datas[i].PrefetchFrom(ctx, versions[i], fs, locations[i], tableID)
		if err != nil {
			return nil, nil, err
		}
	}

	closeCBs := make([]func(), 0)
	bats := make([][]*batch.Batch, len(locationsAndVersions)/2)
	var err error
	for i, data := range datas {
		var bat []*batch.Batch
		bat, err = data.ReadFromData(ctx, tableID, locations[i], readers[i], versions[i], mp, i>0)
		closeCBs = append(closeCBs, data.GetCloseCB(versions[i], mp))
		if err != nil {
			for j := range closeCBs {
				if closeCBs[j] != nil {
					closeCBs[j]()
				}
			}
			return nil, nil, err
		}
		bats[i] = bat
	}
	entries := make([]*api.Entry, 0)
	for i := range objectLocations {
		data := datas[i]
		ins, del, cnIns, segDel, err := data.GetTableDataFromBats(tableID, bats[i])
		if err != nil {
			for j := range closeCBs {
				if closeCBs[j] != nil {
					closeCBs[j]()
				}
			}
			return nil, nil, err
		}
		if tableName != pkgcatalog.MO_DATABASE &&
			tableName != pkgcatalog.MO_COLUMNS &&
			tableName != pkgcatalog.MO_TABLES {
			tableName = fmt.Sprintf("_%d_meta", tableID)
		}
		if ins != nil {
			entry := &api.Entry{
				EntryType:    api.Entry_Insert,
				TableId:      tableID,
				TableName:    tableName,
				DatabaseId:   dbID,
				DatabaseName: dbName,
				Bat:          ins,
			}
			entries = append(entries, entry)
		}
		if cnIns != nil {
			entry := &api.Entry{
				EntryType:    api.Entry_Insert,
				TableId:      tableID,
				TableName:    tableName,
				DatabaseId:   dbID,
				DatabaseName: dbName,
				Bat:          cnIns,
			}
			entries = append(entries, entry)
		}
		if del != nil {
			entry := &api.Entry{
				EntryType:    api.Entry_Delete,
				TableId:      tableID,
				TableName:    tableName,
				DatabaseId:   dbID,
				DatabaseName: dbName,
				Bat:          del,
			}
			entries = append(entries, entry)
		}
		if segDel != nil {
			entry := &api.Entry{
				EntryType:    api.Entry_Delete,
				TableId:      tableID,
				TableName:    fmt.Sprintf("_%d_seg", tableID),
				DatabaseId:   dbID,
				DatabaseName: dbName,
				Bat:          segDel,
			}
			entries = append(entries, entry)
		}
	}
	return entries, closeCBs, nil
}

func LoadCheckpointEntriesFromKey(
	ctx context.Context,
	fs fileservice.FileService,
	location objectio.Location,
	version uint32,
) ([]objectio.Location, *CheckpointData, error) {
	locations := make([]objectio.Location, 0)
	locations = append(locations, location)
	data := NewCheckpointData()
	reader, err := blockio.NewObjectReader(fs, location)
	if err != nil {
		return nil, nil, err
	}
	err = data.readMetaBatch(ctx, version, reader, nil)
	if err != nil {
		return nil, nil, err
	}
	err = data.readAll(ctx, version, fs)
	if err != nil {
		return nil, nil, err
	}

	for _, location = range data.locations {
		locations = append(locations, location)
	}
	for i := 0; i < data.bats[BLKMetaInsertIDX].Length(); i++ {
		deltaLoc := objectio.Location(data.bats[BLKMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Get(i).([]byte))
		metaLoc := objectio.Location(data.bats[BLKMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Get(i).([]byte))
		if !metaLoc.IsEmpty() {
			locations = append(locations, metaLoc)
		}
		if !deltaLoc.IsEmpty() {
			locations = append(locations, deltaLoc)
		}
	}
	for i := 0; i < data.bats[BLKCNMetaInsertIDX].Length(); i++ {
		deltaLoc := objectio.Location(data.bats[BLKCNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Get(i).([]byte))
		metaLoc := objectio.Location(data.bats[BLKCNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Get(i).([]byte))
		if !metaLoc.IsEmpty() {
			locations = append(locations, metaLoc)
		}
		if !deltaLoc.IsEmpty() {
			locations = append(locations, deltaLoc)
		}

	}
	return locations, data, nil
}

type fileData struct {
	data      map[uint16]*blockData
	name      objectio.ObjectName
	isCnBatch bool
	isDnBatch bool
	isChange  bool
	isAblk    bool
}

type blockData struct {
	num       uint16
	cnRow     []int
	dnRow     []int
	blockType objectio.DataMetaType
	location  objectio.Location
	data      *batch.Batch
	pk        int32
	isAblk    bool
	commitTs  types.TS
	blockId   types.Blockid
	tid       uint64
}

type dataObject struct {
	isChange bool
	name     objectio.ObjectName
	data     blockData
}

type newRow struct {
	insertRow    int
	insertTxnRow int
}

type iBlocks struct {
	filenum int
	insertBlocks []*insertBlock
}

type insertBlock struct {
	blockId  objectio.Blockid
	location objectio.Location
	commitTs types.TS
	cnRow    int
	apply    bool
	data   *blockData
}

func applyDelete(dataBatch *batch.Batch, deleteBatch *batch.Batch) error {
	if deleteBatch == nil {
		return nil
	}
	deleteRow := make([]int64, 0)
	rowss := make(map[int64]bool)
	for i := 0; i < deleteBatch.Vecs[0].Length(); i++ {
		row := deleteBatch.Vecs[0].GetRawBytesAt(i)
		rowId := objectio.HackBytes2Rowid(row)
		_, ro := rowId.Decode()
		rowss[int64(ro)] = true
	}
	for i:=0; i<dataBatch.Vecs[0].Length(); i++ {
		if rowss[int64(i)] {
			deleteRow = append(deleteRow, int64(i))
		}
	}
	dataBatch.AntiShrink(deleteRow)
	return nil
}

func ReWriteCheckpointAndBlockFromKey(
	ctx context.Context,
	fs, dstFs fileservice.FileService,
	loc, tnLocation objectio.Location,
	version uint32, ts types.TS,
) (objectio.Location, objectio.Location, *CheckpointData, []string, error) {
	objectsData := make(map[string]*fileData, 0)
	data := NewCheckpointData()
	reader, err := blockio.NewObjectReader(fs, loc)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	err = data.readMetaBatch(ctx, version, reader, nil)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	err = data.readAll(ctx, version, fs)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	data.FormatData(common.DefaultAllocator)
	var files []string
	isCkpChange := false
	blkCNMetaInsert := data.bats[BLKCNMetaInsertIDX]
	blkCNMetaInsertMetaLoc := data.bats[BLKCNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_MetaLoc)
	blkCNMetaInsertDeltaLoc := data.bats[BLKCNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc)
	blkCNMetaInsertEntryState := data.bats[BLKCNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_EntryState)

	blkMetaDelTxnBat := data.bats[BLKMetaDeleteTxnIDX]
	blkMetaDelTxnTid := blkMetaDelTxnBat.GetVectorByName(SnapshotAttr_TID)

	blkMetaInsTxnBat := data.bats[BLKMetaInsertTxnIDX]
	blkMetaInsTxnBatTid := blkMetaInsTxnBat.GetVectorByName(SnapshotAttr_TID)

	blkMetaInsert := data.bats[BLKMetaInsertIDX]
	blkMetaInsertMetaLoc := data.bats[BLKMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_MetaLoc)
	blkMetaInsertDeltaLoc := data.bats[BLKMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc)
	blkMetaInsertEntryState := data.bats[BLKMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_EntryState)
	blkCNMetaInsertCommitTs := data.bats[BLKCNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_CommitTs)
	for i := 0; i < blkCNMetaInsert.Length(); i++ {
		metaLoc := objectio.Location(blkCNMetaInsertMetaLoc.Get(i).([]byte))
		deltaLoc := objectio.Location(blkCNMetaInsertDeltaLoc.Get(i).([]byte))
		isAblk := blkCNMetaInsertEntryState.Get(i).(bool)
		commits := blkCNMetaInsertCommitTs.Get(i).(types.TS)
		if commits.Less(ts) {
			panic("commitTs less than ts")
		}
		if !isAblk {
			logutil.Infof("cn metaLoc1 %v, row is %d", metaLoc.String(), i)
		}
		if !metaLoc.IsEmpty() {
			name := metaLoc.Name().String()
			if objectsData[name] == nil {
				object := &fileData{
					name:      metaLoc.Name(),
					data:      make(map[uint16]*blockData),
					isChange:  false,
					isCnBatch: true,
					isAblk: isAblk,
				}
				objectsData[name] = object
				isCkpChange = true
			}
			if objectsData[name].data[metaLoc.ID()] != nil {
				logutil.Infof("cn metaLoc2 %v, row is %d", metaLoc.String(), i)
				if len(objectsData[name].data[metaLoc.ID()].cnRow) > 0 {
					logutil.Infof("len(objectsData[name].data[metaLoc.ID()].cnRow) > 0 %v, row is %d", objectsData[name].data[metaLoc.ID()].cnRow, i)
				}
				objectsData[name].data[metaLoc.ID()].cnRow = append(objectsData[name].data[metaLoc.ID()].cnRow, i)
			} else {
				objectsData[name].data[metaLoc.ID()] = &blockData{
					num:       metaLoc.ID(),
					location:  metaLoc,
					blockType: objectio.SchemaData,
					cnRow:     []int{i},
					isAblk:    isAblk,
					tid: blkMetaDelTxnTid.Get(i).(uint64),
				}
				logutil.Infof("cn metaLoc %v, row is %d, tid is %d", metaLoc.String(), i, blkMetaDelTxnTid.Get(i).(uint64))

			}
		}
		if !deltaLoc.IsEmpty() {
			name := deltaLoc.Name().String()
			if objectsData[name] == nil {
				object := &fileData{
					name:      deltaLoc.Name(),
					data:      make(map[uint16]*blockData),
					isChange:  false,
					isCnBatch: true,
					isAblk: isAblk,
				}
				objectsData[name] = object
			}
			if objectsData[name].data[deltaLoc.ID()] == nil {
				objectsData[name].data[deltaLoc.ID()] = &blockData{
					num:       deltaLoc.ID(),
					location:  deltaLoc,
					blockType: objectio.SchemaTombstone,
					cnRow:     []int{i},
					isAblk:    isAblk,
					tid: blkMetaDelTxnTid.Get(i).(uint64),
				}
			} else {
				objectsData[name].data[deltaLoc.ID()].cnRow = append(objectsData[name].data[deltaLoc.ID()].cnRow, i)
			}
		}
	}

	for i := 0; i < blkMetaInsert.Length(); i++ {
		metaLoc := objectio.Location(blkMetaInsertMetaLoc.Get(i).([]byte))
		deltaLoc := objectio.Location(blkMetaInsertDeltaLoc.Get(i).([]byte))
		isAblk := blkMetaInsertEntryState.Get(i).(bool)
		if isAblk {
			logutil.Infof("dn metaLoc1 %v, row is %d", metaLoc.String(), i)
			panic("blkMetaInsertEntryState.Get(i).(bool) is false")
		}
		if !metaLoc.IsEmpty() {
			name := metaLoc.Name().String()
			if objectsData[name] == nil {
				object := &fileData{
					name:      metaLoc.Name(),
					data:      make(map[uint16]*blockData),
					isChange:  false,
					isDnBatch: true,
					isAblk: isAblk,
				}
				objectsData[name] = object
			}
			if objectsData[name].data[metaLoc.ID()] != nil {
				logutil.Infof("dn metaLoc2 %v, row is %d", metaLoc.String(), i)
				if len(objectsData[name].data[metaLoc.ID()].dnRow) > 0 {
					logutil.Infof("len(objectsData[name].data[metaLoc.ID()].dnRow) > 0 %v, row is %d", objectsData[name].data[metaLoc.ID()].dnRow, i)
				}
				objectsData[name].data[metaLoc.ID()].dnRow = append(objectsData[name].data[metaLoc.ID()].dnRow, i)
			} else {
				objectsData[name].data[metaLoc.ID()] = &blockData{
					num:       metaLoc.ID(),
					location:  metaLoc,
					blockType: objectio.SchemaData,
					dnRow:     []int{i},
					isAblk:    isAblk,
					tid: blkMetaInsTxnBatTid.Get(i).(uint64),
				}
				logutil.Infof("dn metaLoc %v, row is %d, tid is %d", metaLoc.String(), i, blkMetaInsTxnBatTid.Get(i).(uint64))
			}
		}

		if !deltaLoc.IsEmpty() {
			name := deltaLoc.Name().String()
			if objectsData[name] == nil {
				object := &fileData{
					name:     deltaLoc.Name(),
					data:     make(map[uint16]*blockData),
					isChange: false,
					isAblk: isAblk,
				}
				objectsData[name] = object
			}
			if objectsData[name].data[deltaLoc.ID()] == nil {
				objectsData[name].data[deltaLoc.ID()] = &blockData{
					num:       deltaLoc.ID(),
					location:  deltaLoc,
					blockType: objectio.SchemaTombstone,
					dnRow:     []int{i},
					isAblk:    isAblk,
					tid: blkMetaInsTxnBatTid.Get(i).(uint64),
				}
			} else {
				objectsData[name].data[deltaLoc.ID()].dnRow = append(objectsData[name].data[deltaLoc.ID()].dnRow, i)
			}
		}
	}

	for name, objectData := range objectsData {
		isChange := false
		for id, block := range objectData.data {
			if !block.isAblk && block.blockType == objectio.SchemaData {
				logutil.Infof("ec block is %v", block.location.String())
				continue
			}
			var bat *batch.Batch
			if !block.isAblk && block.blockType == objectio.SchemaTombstone {
				bat, err = blockio.LoadOneBlock(ctx, fs, block.location, objectio.SchemaTombstone)
				if err != nil {
					return nil, nil, nil, nil, err
				}
				commitTs := types.TS{}
				deleteRow := make([]int64, 0)
				logutil.Infof("block length is %d", bat.Vecs[0].Length())
				for v := 0; v < bat.Vecs[0].Length(); v++ {
					err = commitTs.Unmarshal(bat.Vecs[len(bat.Vecs)-3].GetRawBytesAt(v))
					if err != nil {
						return nil, nil, nil, nil, err
					}
					if commitTs.Greater(ts) {
						/*windowCNBatch(bat, 0, uint64(v))
						c := types.TS{}
						err = c.Unmarshal(bat.Vecs[len(bat.Vecs)-3].GetRawBytesAt(bat.Vecs[0].Length() -1))
						logutil.Infof("deltaCommitTs %v ts %v, c %v , block is %v", commitTs.ToString(), ts.ToString(), c.ToString(), block.location.String())*/
						isChange = true
						isCkpChange = true
						//break
					} else {
						deleteRow = append(deleteRow, int64(v))
					}
				}
				if len(deleteRow) != bat.Vecs[0].Length() {
					bat.Shrink(deleteRow)
					logutil.Infof("deleteRow is %d, bat length %d", len(deleteRow), bat.Vecs[0].Length())
					commitTs1 := types.TS{}
					for v := 0; v < bat.Vecs[0].Length(); v++ {
						err = commitTs1.Unmarshal(bat.Vecs[len(bat.Vecs)-3].GetRawBytesAt(v))
						if err != nil {
							return nil, nil, nil, nil, err
						}
						if commitTs1.Greater(ts) {
							logutil.Infof("commitTs1 %v ts %v, block is %v", commitTs.ToString(), ts.ToString(), block.location.String())
							panic("commitTs.Greater(ts)")
						}
					}
				}
				objectData.data[id].data = bat
			}

			if block.isAblk {
				commitTs := types.TS{}
				deleteRow := make([]int64, 0)
				pk := int32(-1)
				if block.blockType == objectio.SchemaTombstone {
					bat, err = blockio.LoadOneBlock(ctx, fs, block.location, objectio.SchemaTombstone)
					logutil.Infof("sdfsdfsdfdsfsdfsss")
					if err != nil {
						return nil, nil, nil, nil, err
					}
					var rowid string
					for i := 0; i < bat.Vecs[0].Length(); i++ {
						rowid += "1:"
						rid := objectio.HackBytes2Rowid(bat.Vecs[0].GetRawBytesAt(i))
						rowid += rid.String()
					}
					//logutil.Infof("blockread11 %s read delete %v \n", block.location.String(), rowid)
					for v := 0; v < bat.Vecs[0].Length(); v++ {
						err = commitTs.Unmarshal(bat.Vecs[len(bat.Vecs)-3].GetRawBytesAt(v))
						if err != nil {
							return nil, nil, nil, nil, err
						}
						if commitTs.Greater(ts) {
							logutil.Infof("SchemaTombstone commitTs %v ts %v, block is %v", commitTs.ToString(), ts.ToString(), block.location.String())
							for y := v; y < bat.Vecs[0].Length(); y++ {
								debugcommitTs := types.TS{}
								err = debugcommitTs.Unmarshal(bat.Vecs[len(bat.Vecs)-3].GetRawBytesAt(y))
								if err != nil {
									return nil, nil, nil, nil, err
								}
								if debugcommitTs.LessEq(ts) {
									//logutil.Infof("debugcommitTss1 is not sorted %v ts %v, block is %v", debugcommitTs.ToString(), ts.ToString(), block.location.String())
									//panic("debugcommitTs is not sorted")
								}
							}
							/*windowCNBatch(bat, 0, uint64(v))
							c := types.TS{}
							err = c.Unmarshal(bat.Vecs[len(bat.Vecs)-2].GetRawBytesAt(bat.Vecs[0].Length() -1))*/
							//logutil.Infof("blkCommitTs %v ts %v, c %v , block is %v", commitTs.ToString(), ts.ToString(), c.ToString(), block.location.String())
							isChange = true
							isCkpChange = true
						} else {
							deleteRow = append(deleteRow, int64(v))
						}
					}
					if len(deleteRow) != bat.Vecs[0].Length() {
						bat.Shrink(deleteRow)
						logutil.Infof("deleteRow1 is %d, bat length %d", len(deleteRow), bat.Vecs[0].Length())
						commitTs1 := types.TS{}
						for v := 0; v < bat.Vecs[0].Length(); v++ {
							if block.blockType == objectio.SchemaTombstone {
								err = commitTs1.Unmarshal(bat.Vecs[len(bat.Vecs)-3].GetRawBytesAt(v))
							} else {
								err = commitTs1.Unmarshal(bat.Vecs[len(bat.Vecs)-2].GetRawBytesAt(v))
							}
							if err != nil {
								return nil, nil, nil, nil, err
							}
							if commitTs1.Greater(ts) {
								logutil.Infof("commitTs111 %v ts %v, block is %v", commitTs.ToString(), ts.ToString(), block.location.String())
								panic("commitTs.Greater(ts2)")
							}
						}
						var rowid1 string
						for i := 0; i < bat.Vecs[0].Length(); i++ {
							rowid1 += "1:"
							rid := objectio.HackBytes2Rowid(bat.Vecs[0].GetRawBytesAt(i))
							rowid1 += rid.String()
						}
						logutil.Infof("blockread %s read delete %v \n", block.location.String(), rowid1)
					}
				} else {
					meta, err := objectio.FastLoadObjectMeta(ctx, &block.location, false, fs)
					if err != nil {
						return nil, nil, nil, nil, err
					}
					pk = meta.MustDataMeta().BlockHeader().PkIdxID()
					bat, err = blockio.LoadOneBlock(ctx, fs, block.location, objectio.SchemaData)
					if err != nil {
						return nil, nil, nil, nil, err
					}
					isWindow := false
					for v := 0; v < bat.Vecs[0].Length(); v++ {
						err = commitTs.Unmarshal(bat.Vecs[len(bat.Vecs)-2].GetRawBytesAt(v))
						if err != nil {
							return nil, nil, nil, nil, err
						}
						if commitTs.Greater(ts) {
							windowCNBatch(bat, 0, uint64(v))
							c := types.TS{}
							err = c.Unmarshal(bat.Vecs[len(bat.Vecs)-2].GetRawBytesAt(bat.Vecs[0].Length() - 1))
							logutil.Infof("blkCommitTs %v ts %v, c %v , block is %v", commitTs.ToString(), ts.ToString(), c.ToString(), block.location.String())
							isChange = true
							isCkpChange = true
							isWindow = true
							break
						}
					}
					if !isWindow {
						logutil.Infof("blockread %s read delet \n", block.location.String())
						windowCNBatch(bat, 0, uint64(bat.Vecs[0].Length()))
					}
				}

				objectData.data[id].data = bat
				objectData.data[id].pk = pk
			}
		}
		objectsData[name].isChange = isChange
	}
	tpool := dbutils.MakeDefaultSmallPool("smal-vector-pool1")
	if isCkpChange {
		insertBatch := make(map[uint64]*iBlocks)
		for fileName, objectData := range objectsData {
			if objectData.isChange || objectData.isCnBatch {
				datas := make([]*blockData, 0)
				var blocks []objectio.BlockObject
				var extent objectio.Extent
				for _, block := range objectData.data {
					logutil.Infof("object %v, id is %d", fileName, block.num)
					datas = append(datas, block)
				}
				sort.Slice(datas, func(i, j int) bool {
					return datas[i].num < datas[j].num
				})
				if objectData.isChange && !objectData.isCnBatch {
					writer, err := blockio.NewBlockWriter(dstFs, fileName)
					if err != nil {
						return nil, nil, nil, nil, err
					}
					for _, block := range datas {
						logutil.Infof("write object %v, id is %d", fileName, block.num)
						if block.pk > -1 {
							writer.SetPrimaryKey(uint16(block.pk))
						}
						if block.blockType == objectio.SchemaData {
							_, err = writer.WriteBatch(block.data)
							if err != nil {
								return nil, nil, nil, nil, err
							}
						} else if block.blockType == objectio.SchemaTombstone {
							_, err = writer.WriteTombstoneBatch(block.data)
							if err != nil {
								return nil, nil, nil, nil, err
							}
						}
					}

					blocks, extent, err = writer.Sync(ctx)
					if err != nil {
						if moerr.IsMoErrCode(err, moerr.ErrFileAlreadyExists) {
							err = fs.Delete(ctx, fileName)
							if err != nil {
								return nil, nil, nil, nil, err
							}
							blocks, extent, err = writer.Sync(ctx)
							if err != nil {
								return nil, nil, nil, nil, err
							}
						} else {
							return nil, nil, nil, nil, err
						}
					}
				}

				if objectData.isCnBatch {
					if objectData.isAblk {
						if len(datas) > 2 {
							logutil.Infof("datas len > 2 %v", datas[0].location.String())
							panic("datas len > 2")
						}
						if len(datas) > 1 {
							logutil.Infof("datas len is %d", datas[0].data.Vecs[0].Length())
							applyDelete(datas[0].data, datas[1].data)
							datas[0].data.Attrs = make([]string,0)
							for i := range datas[0].data.Vecs {
								att := fmt.Sprintf("col_%d", i)
								datas[0].data.Attrs = append(datas[0].data.Attrs, att)
							}
							sortData := containers.ToTNBatch(datas[0].data)
							if datas[0].pk > -1 {
								_, err = mergesort.SortBlockColumns(sortData.Vecs, int(datas[0].pk), tpool)
								if err != nil {
									return nil, nil, nil, nil, err
								}
							}
							datas[0].data = containers.ToCNBatch(sortData)
							result := batch.NewWithSize(len(datas[0].data.Vecs) - 3)
							for i := range result.Vecs {
								result.Vecs[i] = datas[0].data.Vecs[i]
							}
							datas[0].data = result
							//logutil.Infof("sortdata is %v", sortData.String())
							//task.transMappings.AddSortPhaseMapping(blkidx, rowCntBeforeApplyDelete, deletes, sortMapping)
						}
						logutil.Infof("datas2 len is %d, locatio %s", datas[0].data.Vecs[0].Length(), datas[0].location.String())
						fileNum := uint16(1000) + datas[0].location.Name().Num()
						segment := datas[0].location.Name().SegmentId()
						name := objectio.BuildObjectName(&segment, fileNum)

						writer, err := blockio.NewBlockWriter(dstFs, name.String())
						if err != nil {
							return nil, nil, nil, nil, err
						}
						if datas[0].pk > -1 {
							writer.SetPrimaryKey(uint16(datas[0].pk))
						}
						_, err = writer.WriteBatch(datas[0].data)
						if err != nil {
							return nil, nil, nil, nil, err
						}
						blocks, extent, err = writer.Sync(ctx)
						if err != nil {
							panic("sync error")
						}
						files = append(files, name.String())
						blockLocation := objectio.BuildLocation(name, extent, blocks[0].GetRows(), blocks[0].GetID())
						if insertBatch[datas[0].tid] == nil {
							logutil.Infof("tid is %d, file is %v", datas[0].tid, blockLocation.String())
							insertBatch[datas[0].tid] = &iBlocks{
								insertBlocks: make([]*insertBlock, 0),
							}
						}
						ib := &insertBlock{
							location: blockLocation,
							blockId:  *objectio.BuildObjectBlockid(name, blocks[0].GetID()),
							apply:    false,
							cnRow: datas[0].cnRow[0],
						}
						insertBatch[datas[0].tid].insertBlocks = append(insertBatch[datas[0].tid].insertBlocks, ib)
					} else {
						for _, dt := range datas {
							if insertBatch[datas[0].tid] == nil {
								logutil.Infof("tid is %d, file is %v", datas[0].location.String())
								insertBatch[datas[0].tid] = &iBlocks{
									insertBlocks: make([]*insertBlock, 0),
								}
							}
							ib := &insertBlock{
								blockId:  dt.blockId,
								apply:    false,
								cnRow: dt.cnRow[len(dt.cnRow) - 1],
								data: dt,
							}
							insertBatch[datas[0].tid].insertBlocks = append(insertBatch[datas[0].tid].insertBlocks, ib)
						}
					}

				} else {
					for i := range datas {
						logutil.Infof("write11 object %v, id is %d", fileName, datas[i].num)
						row := uint16(i)
						blockLocation := datas[row].location
						if objectData.isChange {
							blockLocation = objectio.BuildLocation(objectData.name, extent, blocks[row].GetRows(), datas[i].num)
						}
						for _, dnRow := range datas[i].dnRow {
							if datas[row].blockType == objectio.SchemaData {
								logutil.Infof("rewrite BlockMeta_DataLocdn %s, row is %d", blockLocation.String(), dnRow)
								data.bats[BLKMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Update(
									dnRow,
									[]byte(blockLocation),
									false)
								data.bats[BLKMetaInsertTxnIDX].GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Update(
									dnRow,
									[]byte(blockLocation),
									false)
							}
							if datas[row].blockType == objectio.SchemaTombstone {
								logutil.Infof("rewrite BlockMeta_DeltaLocdn %s, row is %d", blockLocation.String(), dnRow)
								data.bats[BLKMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Update(
									dnRow,
									[]byte(blockLocation),
									false)
								data.bats[BLKMetaInsertTxnIDX].GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Update(
									dnRow,
									[]byte(blockLocation),
									false)
							}
						}
						for _, cnRow := range datas[row].cnRow {
							if datas[row].blockType == objectio.SchemaData {
								logutil.Infof("rewrite BlockMeta_DataLoc %s, row is %d", blockLocation.String(), cnRow)
								if datas[row].isAblk {
									data.bats[BLKCNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Update(
										cnRow,
										[]byte(blockLocation),
										false)
									data.bats[BLKMetaDeleteTxnIDX].GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Update(
										cnRow,
										[]byte(blockLocation),
										false)
								}
							}
							if datas[row].blockType == objectio.SchemaTombstone {
								logutil.Infof("rewrite BlockMeta_DeltaLoc %s, row is %d", blockLocation.String(), cnRow)
								data.bats[BLKCNMetaInsertIDX].GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Update(
									cnRow,
									[]byte(blockLocation),
									false)
								data.bats[BLKMetaDeleteTxnIDX].GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Update(
									cnRow,
									[]byte(blockLocation),
									false)
							}
						}
					}
				}

			}
		}
		if len(insertBatch) > 0 {
			blkMeta := makeRespBatchFromSchema(checkpointDataSchemas_Curr[BLKMetaInsertIDX])
			blkMetaTxn := makeRespBatchFromSchema(checkpointDataSchemas_Curr[BLKMetaInsertTxnIDX])
			for i := 0; i < blkMetaInsert.Length(); i++ {
				tid := data.bats[BLKMetaInsertTxnIDX].GetVectorByName(SnapshotAttr_TID).Get(i).(uint64)
				for v, vec := range data.bats[BLKMetaInsertIDX].Vecs {
					val := vec.Get(i)
					if val == nil {
						blkMeta.Vecs[v].Append(val, true)
					} else {
						blkMeta.Vecs[v].Append(val, false)
					}
				}
				for v, vec := range data.bats[BLKMetaInsertTxnIDX].Vecs {
					val := vec.Get(i)
					if val == nil {
						blkMetaTxn.Vecs[v].Append(val, true)
					} else {
						blkMetaTxn.Vecs[v].Append(val, false)
					}
				}
				if insertBatch[tid] != nil {
					for b, blk := range insertBatch[tid].insertBlocks {
						if blk.apply {
							continue
						}
						cnRow := insertBatch[tid].insertBlocks[b].cnRow
						insertBatch[tid].insertBlocks[b].apply = true
						logutil.Infof("rewrite BLKCNMetaInsertIDX %s, row is %d", insertBatch[tid].insertBlocks[b].location.String(), cnRow)
						for v, vec := range data.bats[BLKCNMetaInsertIDX].Vecs {
							if !blk.location.IsEmpty() && data.bats[BLKCNMetaInsertIDX].Attrs[v] == pkgcatalog.BlockMeta_DeltaLoc{
								blkMeta.Vecs[v].Append(nil, true)
								continue
							}
							val := vec.Get(cnRow)
							if val == nil {
								blkMeta.Vecs[v].Append(val, true)
							} else {
								blkMeta.Vecs[v].Append(val, false)
							}
						}
						logutil.Infof("rewrite BLKMetaDeleteTxnIDX %s, row is %d", insertBatch[tid].insertBlocks[b].location.String(), cnRow)
						for v, vec := range data.bats[BLKMetaDeleteTxnIDX].Vecs {
							if !blk.location.IsEmpty() && data.bats[BLKMetaDeleteTxnIDX].Attrs[v] == pkgcatalog.BlockMeta_DeltaLoc{
								blkMetaTxn.Vecs[v].Append(nil, true)
								continue
							}
							val := vec.Get(cnRow)
							if val == nil {
								blkMetaTxn.Vecs[v].Append(val, true)
							} else {
								blkMetaTxn.Vecs[v].Append(val, false)
							}
						}

						leng := blkMeta.Vecs[0].Length() - 1
						ti := blkMetaTxn.GetVectorByName(SnapshotAttr_TID).Get(leng)
						t2 := data.bats[BLKMetaDeleteTxnIDX].GetVectorByName(SnapshotAttr_TID).Get(cnRow)
						logutil.Infof("rewrite update  BLKMetaInsertIDX %s, row is %d, t2 is %d, leng %d", insertBatch[tid].insertBlocks[b].location.String(), ti, t2, leng)
						blkMeta.GetVectorByName(catalog.AttrRowID).Update(
							leng,
							objectio.HackBlockid2Rowid(&insertBatch[tid].insertBlocks[b].blockId),
							false)
						blkMeta.GetVectorByName(pkgcatalog.BlockMeta_ID).Update(
							leng,
							insertBatch[tid].insertBlocks[b].blockId,
							false)
						blkMeta.GetVectorByName(pkgcatalog.BlockMeta_EntryState).Update(
							leng,
							false,
							false)
						blkMeta.GetVectorByName(pkgcatalog.BlockMeta_Sorted).Update(
							leng,
							true,
							false)
						if !blk.location.IsEmpty() {
							blkMeta.GetVectorByName(pkgcatalog.BlockMeta_SegmentID).Update(
								leng,
								insertBatch[tid].insertBlocks[b].location.Name().SegmentId(),
								false)
							blkMeta.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Update(
								leng,
								[]byte(insertBatch[tid].insertBlocks[b].location),
								false)
							blkMeta.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Update(
								leng,
								nil,
								true)
							test := blkMeta.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Get(leng)
							logutil.Infof("testfdsfs is %v", test)
							blkMetaTxn.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Update(
								leng,
								[]byte(insertBatch[tid].insertBlocks[b].location),
								false)
							blkMetaTxn.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Update(
								leng,
								nil,
								true)
						}
					}
				}
			}

			for tid := range insertBatch {
				for b:= range insertBatch[tid].insertBlocks {
					logutil.Infof("insertBatch is %d, apply is %v,file is %v", tid, insertBatch[tid].insertBlocks[b].apply, insertBatch[tid].insertBlocks[b].location.String())
					if insertBatch[tid].insertBlocks[b].apply {
						continue
					}
					if insertBatch[tid] != nil && !insertBatch[tid].insertBlocks[b].apply {
						cnRow := insertBatch[tid].insertBlocks[b].cnRow
						insertBatch[tid].insertBlocks[b].apply = true
						logutil.Infof("rewrite1 BLKCNMetaInsertIDX %s, row is %d", insertBatch[tid].insertBlocks[b].location.String(), cnRow)
						for v, vec := range data.bats[BLKCNMetaInsertIDX].Vecs {
							if !insertBatch[tid].insertBlocks[b].location.IsEmpty() && data.bats[BLKCNMetaInsertIDX].Attrs[v] == pkgcatalog.BlockMeta_DeltaLoc{
								blkMeta.Vecs[v].Append(nil, true)
								continue
							}
							val := vec.Get(cnRow)
							if val == nil {
								blkMeta.Vecs[v].Append(val, true)
							} else {
								blkMeta.Vecs[v].Append(val, false)
							}
						}
						logutil.Infof("rewrite1 BLKMetaDeleteTxnIDX %s, row is %d", insertBatch[tid].insertBlocks[b].location.String(), cnRow)
						for v, vec := range data.bats[BLKMetaDeleteTxnIDX].Vecs {
							if !insertBatch[tid].insertBlocks[b].location.IsEmpty() && data.bats[BLKMetaDeleteTxnIDX].Attrs[v] == pkgcatalog.BlockMeta_DeltaLoc{
								blkMetaTxn.Vecs[v].Append(nil, true)
								continue
							}
							val := vec.Get(cnRow)
							if val == nil {
								blkMetaTxn.Vecs[v].Append(val, true)
							} else {
								blkMetaTxn.Vecs[v].Append(val, false)
							}
						}
						i := blkMeta.Vecs[0].Length() - 1

						ti := blkMetaTxn.GetVectorByName(SnapshotAttr_TID).Get(i)
						t2 := data.bats[BLKMetaDeleteTxnIDX].GetVectorByName(SnapshotAttr_TID).Get(cnRow)
						logutil.Infof("rewrite111 update  BLKMetaInsertIDX %s, row is %d, t2 is %d", insertBatch[tid].insertBlocks[b].location.String(), ti, t2)

						blkMeta.GetVectorByName(catalog.AttrRowID).Update(
							i,
							objectio.HackBlockid2Rowid(&insertBatch[tid].insertBlocks[b].blockId),
							false)
						blkMeta.GetVectorByName(pkgcatalog.BlockMeta_ID).Update(
							i,
							insertBatch[tid].insertBlocks[b].blockId,
							false)
						blkMeta.GetVectorByName(pkgcatalog.BlockMeta_EntryState).Update(
							i,
							false,
							false)
						blkMeta.GetVectorByName(pkgcatalog.BlockMeta_Sorted).Update(
							i,
							true,
							false)
						if !insertBatch[tid].insertBlocks[b].location.IsEmpty() {
							blkMeta.GetVectorByName(pkgcatalog.BlockMeta_SegmentID).Update(
								i,
								insertBatch[tid].insertBlocks[b].location.Name().SegmentId(),
								false)
							blkMeta.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Update(
								i,
								[]byte(insertBatch[tid].insertBlocks[b].location),
								false)
							blkMeta.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Update(
								i,
								nil,
								true)
							blkMetaTxn.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Update(
								i,
								[]byte(insertBatch[tid].insertBlocks[b].location),
								false)
							blkMetaTxn.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Update(
								i,
								nil,
								true)
						}
					}
				}
			}

			for i := range insertBatch {
				for _, block := range insertBatch[i].insertBlocks {
					if block.data != nil {
						for _, cnRow := range block.data.cnRow{
							data.bats[BLKCNMetaInsertIDX].Delete(cnRow)
							data.bats[BLKMetaDeleteTxnIDX].Delete(cnRow)
						}
					}
				}
			}

			data.bats[BLKCNMetaInsertIDX].Compact()
			data.bats[BLKMetaDeleteTxnIDX].Compact()
			tableOff := make(map[uint64]*tableoffset)
			for i := 0 ; i < blkMetaTxn.Vecs[0].Length(); i++ {
				tid := blkMetaTxn.GetVectorByName(SnapshotAttr_TID).Get(i).(uint64)
				loca := blkMeta.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Get(i).([]byte)
				del := blkMeta.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Get(i).([]byte)
				if tableOff[tid] == nil {
					tableOff[tid] = &tableoffset{
						offset: i,
						end:i,
					}
				}
				tableOff[tid].end += 1
				logutil.Infof("tableOff tid  %d, loc %v, del %v, start %d, end %d, row is %d", tid, objectio.Location(loca).String(), objectio.Location(del).String(), tableOff[tid].offset, tableOff[tid].end, i)
			}


			for tid, tabl := range tableOff {
				data.UpdateBlockInsertBlkMeta(tid, int32(tabl.offset), int32(tabl.end))
			}
			data.bats[BLKMetaInsertIDX].Close()
			data.bats[BLKMetaInsertTxnIDX].Close()
			data.bats[BLKMetaInsertIDX] = blkMeta
			data.bats[BLKMetaInsertTxnIDX] = blkMetaTxn
		}
		cnLocation, dnLocation, checkpointFiles, err := data.WriteTo(dstFs, DefaultCheckpointBlockRows, DefaultCheckpointSize)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		logutil.Infof("checkpoint cnLocation %s, dnLocation %s, checkpointFiles %s", cnLocation.String(), dnLocation.String(), checkpointFiles)
		loc = cnLocation
		tnLocation = dnLocation
		files = append(files, checkpointFiles...)
		files = append(files, cnLocation.Name().String())
	}
	return loc, tnLocation, data, files, nil
}

type tableoffset struct {
	offset int
	end int
}