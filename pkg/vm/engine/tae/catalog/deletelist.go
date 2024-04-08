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

package catalog

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

// for each tombstone in range [start,end]
func (entry *TableEntry) foreachTombstoneInRange(
	ctx context.Context,
	start, end types.TS,
	mp *mpool.MPool,
	op func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error)) error {
	it := entry.MakeTombstoneIt(false)
	for node := it.Get(); it.Valid(); it.Next() {
		tombstone := node.GetPayload()
		err := tombstone.foreachTombstoneInRange(ctx, start, end, mp, op)
		if err != nil {
			return err
		}
	}
	return nil
}

// for each tombstone in range [start,end]
func (entry *TableEntry) foreachTombstoneInRangeWithObjectID(
	ctx context.Context,
	objID types.Objectid,
	start, end types.TS,
	mp *mpool.MPool,
	op func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error)) error {
	it := entry.MakeTombstoneIt(false)
	for node := it.Get(); it.Valid(); it.Next() {
		tombstone := node.GetPayload()
		err := tombstone.foreachTombstoneInRangeWithObjectID(ctx, objID, start, end, mp, op)
		if err != nil {
			return err
		}
	}
	return nil
}

// for each tombstone in range [start,end]
func (entry *TableEntry) foreachTombstoneInRangeWithBlockID(
	ctx context.Context,
	blkID types.Blockid,
	start, end types.TS,
	mp *mpool.MPool,
	op func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error)) error {
	it := entry.MakeTombstoneIt(false)
	for node := it.Get(); it.Valid(); it.Next() {
		tombstone := node.GetPayload()
		err := tombstone.foreachTombstoneInRangeWithBlockID(ctx, blkID, start, end, mp, op)
		if err != nil {
			return err
		}
	}
	return nil
}
func (entry *TableEntry) tryGetTombstone(
	ctx context.Context,
	rowID types.Rowid,
	mp *mpool.MPool) (ok bool, commitTS types.TS, aborted bool, pk any, err error) {
	it := entry.MakeTombstoneIt(false)
	for node := it.Get(); it.Valid(); it.Next() {
		tombstone := node.GetPayload()
		ok, commitTS, aborted, pk, err = tombstone.tryGetTombstone(ctx, rowID, mp)
		if err != nil {
			return
		}
		if ok {
			return
		}
	}
	return
}
func (entry *TableEntry) CollectDeleteInRange(
	ctx context.Context,
	start, end types.TS,
	blockID objectio.Blockid,
	mp *mpool.MPool,
) (bat *containers.Batch, err error) {
	entry.foreachTombstoneInRangeWithBlockID(
		ctx, blockID, start, end, mp,
		func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error) {
			if bat == nil {
				pkType := entry.GetLastestSchema().GetPrimaryKey().Type
				bat = NewTombstoneBatch(pkType, mp)
			}
			bat.GetVectorByName(PhyAddrColumnName).Append(rowID, false)
			bat.GetVectorByName(AttrCommitTs).Append(commitTS, false)
			bat.GetVectorByName(AttrPKVal).Append(pk, false)
			bat.GetVectorByName(AttrAborted).Append(aborted, false)
			return true, nil
		})
	return
}
func (entry *TableEntry) IsDeleted(
	ctx context.Context,
	txn txnif.TxnReader,
	rowID types.Rowid,
	mp *mpool.MPool) (deleted bool, err error) {
	it := entry.MakeTombstoneIt(false)
	for node := it.Get(); it.Valid(); it.Next() {
		tombstone := node.GetPayload()
		ok, _, _, _, err := tombstone.tryGetTombstoneVisible(ctx, txn, rowID, mp)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return
}

func (entry *TableEntry) FillDeletes(
	ctx context.Context,
	blkID types.Blockid,
	txn txnif.TxnReader,
	view *containers.BaseView,
	mp *mpool.MPool) (err error) {

	it := entry.MakeTombstoneIt(false)
	for node := it.Get(); it.Valid(); it.Next() {
		tombstone := node.GetPayload()
		entry.RLock()
		visible, err := tombstone.IsVisible(txn, entry.RWMutex)
		if err != nil {
			return err
		}
		entry.RUnlock()
		if !visible {
			return nil
		}
		blkCount := 1
		if !tombstone.IsAppendable() {
			stats, err := tombstone.MustGetObjectStats()
			if err != nil {
				return err
			}
			blkCount = int(stats.BlkCnt())
		}
		for i := 0; i < blkCount; i++ {
			err = tombstone.foreachTombstoneVisible(
				ctx,
				txn,
				uint16(i),
				mp,
				func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error) {
					if *rowID.BorrowBlockID() == blkID {
						if view.DeleteMask == nil {
							view.DeleteMask = &nulls.Nulls{}
						}
						_, rowOffset := rowID.Decode()
						view.DeleteMask.Add(uint64(rowOffset))
					}
					return true, nil
				})
			if err != nil {
				return err
			}
		}
	}
	return
}
func (entry *TableEntry) OnApplyDelete(
	deleted uint64,
	ts types.TS) (err error) {
	entry.RemoveRows(deleted)
	return
}

func (entry *TableEntry) IsDeletedLocked(
	row types.Rowid, txn txnif.TxnReader,
) (bool, error) {
	ok, _, _, _, err := entry.tryGetTombstone(txn.GetContext(), row, common.WorkspaceAllocator)
	return ok, err
}

func (entry *TableEntry) EstimateMemSize(objID types.Objectid) int {
	it := entry.MakeTombstoneIt(false)
	size := 0
	for node := it.Get(); it.Valid(); it.Next() {
		tombstone := node.GetPayload()
		if tombstone.HasPersistedData() {
			continue
		}
		err := tombstone.foreachTombstoneInRangeWithObjectID(
			context.Background(),
			objID, types.TS{},
			types.MaxTs(),
			common.MergeAllocator,
			func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error) {
				size += AppendNodeApproxSize
				return true, nil
			})
		if err != nil {
			logutil.Errorf("estimate mem size failed, tombstone %v, objectID %v, err %v", tombstone.ID.String(), objID.String(), err)
		}
	}
	return size
}

func (entry *TableEntry) ReplayDeltaLoc(vMVCCNode any, blkID uint16) {
	mvccNode := vMVCCNode.(*catalog.MVCCNode[*catalog.MetadataMVCCNode])
	mvcc := n.GetOrCreateDeleteChain(blkID)
	mvcc.ReplayDeltaLoc(mvccNode)
}

func (n *ObjectMVCCHandle) VisitDeletes(
	ctx context.Context,
	start, end types.TS,
	deltalocBat *containers.Batch,
	tnInsertBat *containers.Batch,
	skipInMemory bool) (delBatch *containers.Batch, deltalocStart, deltalocEnd int, err error) {
	deltalocStart = deltalocBat.Length()
	for blkOffset, mvcc := range n.deletes {
		n.RLock()
		nodes := mvcc.deltaloc.ClonePreparedInRange(start, end)
		n.RUnlock()
		var skipData bool
		if len(nodes) != 0 {
			blkID := objectio.NewBlockidWithObjectID(&n.meta.ID, blkOffset)
			for _, node := range nodes {
				VisitDeltaloc(deltalocBat, tnInsertBat, n.meta, blkID, node, node.End, node.CreatedAt)
			}
			newest := nodes[len(nodes)-1]
			// block has newer delta data on s3, no need to collect data
			startTS := newest.GetStart()
			skipData = startTS.GreaterEq(&end)
			start = newest.GetStart()
		}
		if !skipData && !skipInMemory {
			deletes := n.deletes[blkOffset]
			delBat, err := deletes.CollectDeleteInRangeAfterDeltalocation(ctx, start, end, false, common.LogtailAllocator)
			if err != nil {
				if delBatch != nil {
					delBatch.Close()
				}
				delBat.Close()
				return nil, 0, 0, err
			}
			if delBat != nil && delBat.Length() > 0 {
				if delBatch == nil {
					delBatch = containers.NewBatch()
					delBatch.AddVector(
						catalog.AttrRowID,
						containers.MakeVector(types.T_Rowid.ToType(), common.LogtailAllocator),
					)
					delBatch.AddVector(
						catalog.AttrCommitTs,
						containers.MakeVector(types.T_TS.ToType(), common.LogtailAllocator),
					)
					delBatch.AddVector(
						catalog.AttrPKVal,
						containers.MakeVector(*delBat.GetVectorByName(catalog.AttrPKVal).GetType(), common.LogtailAllocator),
					)
				}
				delBatch.Extend(delBat)
				// delBatch is freed, don't use anymore
				delBat.Close()
			}
		}
	}
	deltalocEnd = deltalocBat.Length()
	return
}

func VisitDeltaloc(bat, tnBatch *containers.Batch, object *catalog.ObjectEntry, blkID *objectio.Blockid, node *catalog.MVCCNode[*catalog.MetadataMVCCNode], commitTS, createTS types.TS) {
	is_sorted := false
	if !object.IsAppendable() && object.GetSchema().HasSortKey() {
		is_sorted = true
	}
	bat.GetVectorByName(pkgcatalog.BlockMeta_ID).Append(*blkID, false)
	bat.GetVectorByName(pkgcatalog.BlockMeta_EntryState).Append(object.IsAppendable(), false)
	bat.GetVectorByName(pkgcatalog.BlockMeta_Sorted).Append(is_sorted, false)
	bat.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(node.BaseNode.MetaLoc), false)
	bat.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(node.BaseNode.DeltaLoc), false)
	bat.GetVectorByName(pkgcatalog.BlockMeta_CommitTs).Append(commitTS, false)
	bat.GetVectorByName(pkgcatalog.BlockMeta_SegmentID).Append(*object.ID.Segment(), false)
	bat.GetVectorByName(pkgcatalog.BlockMeta_MemTruncPoint).Append(node.Start, false)
	bat.GetVectorByName(catalog.AttrCommitTs).Append(createTS, false)
	bat.GetVectorByName(catalog.AttrRowID).Append(objectio.HackBlockid2Rowid(blkID), false)

	// When pull and push, it doesn't collect tn batch
	if tnBatch != nil {
		tnBatch.GetVectorByName(catalog.SnapshotAttr_DBID).Append(object.GetTable().GetDB().ID, false)
		tnBatch.GetVectorByName(catalog.SnapshotAttr_TID).Append(object.GetTable().ID, false)
		node.TxnMVCCNode.AppendTuple(tnBatch)
	}
}
