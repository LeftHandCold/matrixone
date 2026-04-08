// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txnimpl

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

type compositePKLocalDeleteMasker struct {
	rowIDs    containers.Vector
	mp        *mpool.MPool
	slots     map[string][]int
	remaining int
}

func newCompositePKLocalDeleteMasker(
	pks containers.Vector,
	rowIDs containers.Vector,
	mp *mpool.MPool,
) (*compositePKLocalDeleteMasker, error) {
	masker := &compositePKLocalDeleteMasker{
		rowIDs: rowIDs,
		mp:     mp,
		slots:  make(map[string][]int),
	}
	if pks == nil || rowIDs == nil {
		return masker, nil
	}
	limit := pks.Length()
	if rowIDs.Length() < limit {
		limit = rowIDs.Length()
	}
	if limit == 0 {
		return masker, nil
	}
	err := containers.ForeachWindowBytes(
		pks.GetDownstreamVector(),
		0,
		limit,
		func(v []byte, isNull bool, row int) error {
			if isNull || rowIDs.IsNull(row) {
				return nil
			}
			key := string(v)
			masker.slots[key] = append(masker.slots[key], row)
			masker.remaining++
			return nil
		},
		nil,
	)
	if err != nil {
		return nil, err
	}
	return masker, nil
}

func (masker *compositePKLocalDeleteMasker) hasRemaining() bool {
	return masker != nil && masker.remaining > 0
}

func (masker *compositePKLocalDeleteMasker) apply(deletePKs containers.Vector) (bool, error) {
	if !masker.hasRemaining() || deletePKs == nil {
		return false, nil
	}
	changed := false
	err := containers.ForeachWindowBytes(
		deletePKs.GetDownstreamVector(),
		0,
		deletePKs.Length(),
		func(v []byte, isNull bool, _ int) error {
			if isNull || masker.remaining == 0 {
				return nil
			}
			key := string(v)
			idxs, ok := masker.slots[key]
			if !ok || len(idxs) == 0 {
				return nil
			}
			idx := idxs[0]
			if len(idxs) == 1 {
				delete(masker.slots, key)
			} else {
				masker.slots[key] = idxs[1:]
			}
			containers.UpdateValue(masker.rowIDs.GetDownstreamVector(), uint32(idx), nil, true, masker.mp)
			masker.remaining--
			changed = true
			return nil
		},
		nil,
	)
	return changed, err
}

func (tbl *txnTable) maskLocalCompositePKDeleteCandidates(
	ctx context.Context,
	pks containers.Vector,
	rowIDs containers.Vector,
	mp *mpool.MPool,
) (bool, error) {
	if tbl.tombstoneTable == nil || tbl.tombstoneTable.tableSpace == nil {
		return false, nil
	}
	masker, err := newCompositePKLocalDeleteMasker(pks, rowIDs, mp)
	if err != nil || !masker.hasRemaining() {
		return false, err
	}
	changed := false
	if node := tbl.tombstoneTable.tableSpace.node; node != nil {
		batchChanged, err := masker.apply(node.data.GetVectorByName(objectio.TombstoneAttr_PK_Attr))
		if err != nil {
			return changed, err
		}
		changed = changed || batchChanged
	}
	if !masker.hasRemaining() || len(tbl.tombstoneTable.tableSpace.stats) == 0 {
		return changed, nil
	}
	pkColIdx := tbl.tombstoneTable.schema.GetColIdx(objectio.TombstoneAttr_PK_Attr)
	maxRows := tbl.tombstoneTable.schema.Extra.BlockMaxRows
	for _, stats := range tbl.tombstoneTable.tableSpace.stats {
		for blk := uint16(0); blk < uint16(stats.BlkCnt()); blk++ {
			if !masker.hasRemaining() {
				return changed, nil
			}
			loc := stats.BlockLocation(blk, maxRows)
			vectors, closeFunc, err := ioutil.LoadColumns2(
				ctx,
				[]uint16{uint16(pkColIdx)},
				nil,
				tbl.store.rt.Fs,
				loc,
				fileservice.Policy(0),
				false,
				nil,
			)
			if err != nil {
				return changed, err
			}
			batchChanged, applyErr := masker.apply(vectors[0])
			closeFunc()
			if applyErr != nil {
				return changed, applyErr
			}
			changed = changed || batchChanged
		}
	}
	return changed, nil
}
