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

package tables

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

var _ NodeT = (*persistedNode)(nil)

type persistedNode struct {
	common.RefHelper
	object *baseObject
}

func newPersistedNode(object *baseObject) *persistedNode {
	node := &persistedNode{
		object: object,
	}
	node.OnZeroCB = node.close
	return node
}

func (node *persistedNode) close() {}

func (node *persistedNode) Rows() (uint32, error) {
	stats, err := node.object.meta.MustGetObjectStats()
	if err != nil {
		return 0, err
	}
	return stats.Rows(), nil
}

func (node *persistedNode) Contains(
	ctx context.Context,
	keys containers.Vector,
	keysZM index.ZM,
	bf objectio.BloomFilter,
	txn txnif.TxnReader,
	isCommitting bool,
	mp *mpool.MPool,
) (err error) {
	panic("should not be called")
}
func (node *persistedNode) BatchDedup(
	ctx context.Context,
	txn txnif.TxnReader,
	precommit bool,
	keys containers.Vector,
	keysZM index.ZM,
	rowmask *roaring.Bitmap,
	bf objectio.BloomFilter,
) (err error) {
	panic("should not be called")
}

func (node *persistedNode) GetDuplicatedRows(
	ctx context.Context,
	txn txnif.TxnReader,
	maxVisibleRow uint32,
	keys containers.Vector,
	keysZM index.ZM,
	rowIDs containers.Vector,
	bf objectio.BloomFilter,
	isCommitting bool,
	_ bool,
	mp *mpool.MPool,
) (err error) {
	panic("should not be balled")
}

func (node *persistedNode) ContainsKey(ctx context.Context, key any, blkID uint32) (ok bool, err error) {
	pkIndex, err := MakeImmuIndex(ctx, node.object.meta, nil, node.object.rt)
	if err != nil {
		return
	}
	if err = pkIndex.Dedup(ctx, key, node.object.rt, blkID); err == nil {
		return
	}
	if !moerr.IsMoErrCode(err, moerr.OkExpectedPossibleDup) {
		return
	}
	ok = true
	err = nil
	return
}

func (node *persistedNode) GetDataWindow(
	readSchema *catalog.Schema, colIdxes []int, from, to uint32, mp *mpool.MPool,
) (bat *containers.Batch, err error) {
	panic("to be implemented")
}

func (node *persistedNode) IsPersisted() bool { return true }

func (node *persistedNode) PrepareAppend(rows uint32) (n uint32, err error) {
	panic(moerr.NewInternalErrorNoCtx("not supported"))
}

func (node *persistedNode) ApplyAppend(
	_ *containers.Batch,
	_ txnif.AsyncTxn,
) (from int, err error) {
	panic(moerr.NewInternalErrorNoCtx("not supported"))
}

func (node *persistedNode) GetValueByRow(_ *catalog.Schema, _, _ int) (v any, isNull bool) {
	panic(moerr.NewInternalErrorNoCtx("todo"))
}

func (node *persistedNode) GetRowsByKey(key any) ([]uint32, error) {
	panic(moerr.NewInternalErrorNoCtx("todo"))
}

// only used by tae only
// not to optimize it
func (node *persistedNode) GetRowByFilter(
	ctx context.Context,
	txn txnif.TxnReader,
	filter *handle.Filter,
	mp *mpool.MPool,
	vpool *containers.VectorPool,
) (blkID uint16, row uint32, err error) {
	for blkID = uint16(0); blkID < uint16(node.object.meta.BlockCnt()); blkID++ {
		var ok bool
		ok, err = node.ContainsKey(ctx, filter.Val, uint32(blkID))
		if err != nil {
			return
		}
		if !ok {
			continue
		}
		// Note: sort key do not change
		schema := node.object.meta.GetSchema()
		var sortKey containers.Vector
		sortKey, err = node.object.LoadPersistedColumnData(ctx, schema, schema.GetSingleSortKeyIdx(), mp, blkID)
		if err != nil {
			return
		}
		defer sortKey.Close()
		rows := make([]uint32, 0)
		err = sortKey.Foreach(func(v any, _ bool, offset int) error {
			if compute.CompareGeneric(v, filter.Val, sortKey.GetType().Oid) == 0 {
				row := uint32(offset)
				rows = append(rows, row)
				return nil
			}
			return nil
		}, nil)
		if err != nil && !moerr.IsMoErrCode(err, moerr.OkExpectedDup) {
			return
		}
		if len(rows) == 0 {
			continue
		}

		// Load persisted commit ts
		var commitTSVec containers.Vector
		commitTSVec, err = node.object.LoadPersistedCommitTS(blkID)
		if err != nil {
			return
		}
		defer commitTSVec.Close()

		// Load persisted deletes
		fullBlockID := objectio.NewBlockidWithObjectID(&node.object.meta.ID, blkID)
		view := containers.NewColumnView(0)
		if err = node.object.meta.GetTable().FillDeletes(ctx, *fullBlockID, txn, view.BaseView, mp); err != nil {
			return
		}
		id := node.object.meta.AsCommonID()
		id.SetBlockOffset(blkID)
		err = txn.GetStore().FillInWorkspaceDeletes(id, &view.BaseView.DeleteMask)
		if err != nil {
			return
		}

		exist := false
		var deleted bool
		for _, offset := range rows {
			commitTS := commitTSVec.Get(int(offset)).(types.TS)
			startTS := txn.GetStartTS()
			if commitTS.Greater(&startTS) {
				break
			}
			deleted = view.IsDeleted(int(offset))
			if !deleted {
				exist = true
				row = offset
				break
			}
		}
		if exist {
			return
		}
	}
	err = moerr.NewNotFoundNoCtx()
	return
}

func (node *persistedNode) CollectAppendInRange(
	start, end types.TS, withAborted bool, mp *mpool.MPool,
) (bat *containers.BatchWithVersion, err error) {
	// logtail should have sent metaloc
	return nil, nil
}

func (node *persistedNode) Scan(
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	blkID uint16,
	colIdxes []int,
	mp *mpool.MPool,
) (bat *containers.Batch, err error) {
	id := node.object.meta.AsCommonID()
	id.SetBlockOffset(uint16(blkID))
	location, err := node.object.buildMetalocation(uint16(blkID))
	if err != nil {
		return nil, err
	}
	vecs, err := LoadPersistedColumnDatas(
		txn.GetContext(), readSchema, node.object.rt, id, colIdxes, location, mp,
	)
	if err != nil {
		return nil, err
	}
	// TODO: check visibility
	if bat == nil {
		bat = containers.NewBatch()
		for i, idx := range colIdxes {
			attr := readSchema.ColDefs[idx].Name
			bat.AddVector(attr, vecs[i])
		}
	} else {
		for i, idx := range colIdxes {
			attr := readSchema.ColDefs[idx].Name
			bat.GetVectorByName(attr).Extend(vecs[i])
		}
	}
	return
}

func (node *persistedNode) FillBlockTombstones(
	txn txnif.TxnReader,
	blkID *objectio.Blockid,
	deletes **nulls.Nulls,
	mp *mpool.MPool) error {
	startTS := txn.GetStartTS()
	if !node.object.meta.IsAppendable() {
		node.object.RLock()
		createAt := node.object.meta.GetCreatedAtLocked()
		node.object.RUnlock()
		if createAt.Greater(&startTS) {
			return nil
		}
	}
	id := node.object.meta.AsCommonID()
	readSchema := node.object.meta.GetTable().GetLastestSchema(true)
	for tombstoneBlkID := 0; tombstoneBlkID < node.object.meta.BlockCnt(); tombstoneBlkID++ {
		id.SetBlockOffset(uint16(tombstoneBlkID))
		location, err := node.object.buildMetalocation(uint16(tombstoneBlkID))
		if err != nil {
			return err
		}
		vecs, err := LoadPersistedColumnDatas(
			txn.GetContext(), readSchema, node.object.rt, id, []int{0}, location, mp,
		)
		if err != nil {
			return err
		}
		var commitTSs []types.TS
		if node.object.meta.IsAppendable() {
			commitTSVec, err := node.object.LoadPersistedCommitTS(uint16(tombstoneBlkID))
			if err != nil {
				return err
			}
			commitTSs = vector.MustFixedCol[types.TS](commitTSVec.GetDownstreamVector())
		}
		rowIDs := vector.MustFixedCol[types.Rowid](vecs[0].GetDownstreamVector())
		// TODO: biselect, check visibility
		for i := 0; i < len(rowIDs); i++ {
			if node.object.meta.IsAppendable() {
				if commitTSs[i].Greater(&startTS) {
					continue
				}
			}
			rowID := rowIDs[i]
			if types.PrefixCompare(rowID[:], blkID[:]) == 0 {
				if *deletes == nil {
					*deletes = &nulls.Nulls{}
				}
				offset := rowID.GetRowOffset()
				(*deletes).Add(uint64(offset))
			}
		}
	}
	return nil
}
