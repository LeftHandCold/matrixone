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

package logtailreplay

import (
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/assert"
)

func BenchmarkPartitionStateConcurrentWriteAndIter(b *testing.B) {
	partition := NewPartition()
	end := make(chan struct{})
	defer func() {
		close(end)
	}()

	// concurrent writer
	go func() {
		for {
			select {
			case <-end:
				return
			default:
			}
			state, end := partition.MutateState()
			_ = state
			end()
		}
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			state := partition.state.Load()
			iter := state.NewRowsIter(types.BuildTS(0, 0), nil, false)
			iter.Close()
		}
	})

}

func TestTruncate(t *testing.T) {
	partition := NewPartitionState(true)
	addObject(partition, types.BuildTS(1, 0), types.BuildTS(2, 0))
	addObject(partition, types.BuildTS(1, 0), types.BuildTS(3, 0))
	addObject(partition, types.BuildTS(1, 0), types.TS{})

	partition.truncate([2]uint64{0, 0}, types.BuildTS(1, 0))
	assert.Equal(t, 5, partition.objectIndexByTS.Len())

	partition.truncate([2]uint64{0, 0}, types.BuildTS(2, 0))
	assert.Equal(t, 3, partition.objectIndexByTS.Len())

	partition.truncate([2]uint64{0, 0}, types.BuildTS(3, 0))
	assert.Equal(t, 1, partition.objectIndexByTS.Len())

	partition.truncate([2]uint64{0, 0}, types.BuildTS(4, 0))
	assert.Equal(t, 1, partition.objectIndexByTS.Len())
}

func addObject(p *PartitionState, create, delete types.TS) {
	blkID := objectio.NewBlockid(objectio.NewSegmentid(), 0, 0)
	objShortName := objectio.ShortName(blkID)
	objIndex1 := ObjectIndexByTSEntry{
		Time:         create,
		ShortObjName: *objShortName,
		IsDelete:     false,
	}
	p.objectIndexByTS.Set(objIndex1)
	if !delete.IsEmpty() {
		objIndex2 := ObjectIndexByTSEntry{
			Time:         delete,
			ShortObjName: *objShortName,
			IsDelete:     true,
		}
		p.objectIndexByTS.Set(objIndex2)
	}
}

func TestBTree(t *testing.T) {
	p := NewPartitionState(true)
	for i := 0; i < 100; i++ {
		row := RowEntry{
			BlockID: *objectio.NewBlockid(objectio.NewSegmentid(), 0, 0),
			ID:      int64(i), // a unique version id, for primary index building and validating
			Time:    types.BuildTS(1, 0),
		}
		row.RowID = *objectio.NewRowid(&row.BlockID, 0)
		p.rows.Set(row)

	}
	row1 := RowEntry{
		BlockID: *objectio.NewBlockid(objectio.NewSegmentid(), 0, 0),
		ID:      int64(100), // a unique version id, for primary index building and validating
		Time:    types.BuildTS(1, 0),
	}
	row1.RowID = *objectio.NewRowid(&row1.BlockID, 0)
	p.rows.Set(row1)
	logutil.Infof("row1 is %v", row1.BlockID.String())
	row2 := row1
	row2.RowID = *objectio.NewRowid(&row1.BlockID, 1)
	p.rows.Set(row2)
	for i := 102; i < 200; i++ {
		row := RowEntry{
			BlockID: *objectio.NewBlockid(objectio.NewSegmentid(), 0, 0),
			ID:      int64(i), // a unique version id, for primary index building and validating
			Time:    types.BuildTS(1, 0),
		}
		row.RowID = *objectio.NewRowid(&row.BlockID, 0)
		p.rows.Set(row)

	}
	iter := p.rows.Copy().Iter()
	pivot := RowEntry{
		BlockID: row1.BlockID,
	}
	i := 0
	for ok := iter.Seek(pivot); ok; ok = iter.Next() {
		entry := iter.Item()
		if i == 1 {
			p.rows.Delete(entry)
		}
		i++
		logutil.Infof("delete row %v %v", entry.Time.ToString(), entry.RowID.String())
	}
	iter.Release()
	iter2 := p.rows.Copy().Iter()
	for ok := iter2.Seek(pivot); ok; ok = iter2.Next() {
		entry := iter2.Item()
		logutil.Infof("delete1 row %v %v", entry.Time.ToString(), entry.RowID.String())
	}
}
