// Copyright 2025 Matrix Origin
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

package table_function

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	ftnative "github.com/matrixorigin/matrixone/pkg/fulltext/native"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestAppendNativeTailBatchUsesRealRowIDs(t *testing.T) {
	mp := mpool.MustNewZero()

	rowIDVec := vector.NewVec(types.T_Rowid.ToType())
	pkVec := vector.NewVec(types.T_int64.ToType())
	contentVec := vector.NewVec(types.T_varchar.ToType())
	defer rowIDVec.Free(mp)
	defer pkVec.Free(mp)
	defer contentVec.Free(mp)

	obj1 := objectio.NewObjectid()
	obj2 := objectio.NewObjectid()
	rows := []types.Rowid{
		objectio.NewRowIDWithObjectIDBlkNumAndRowID(obj1, 0, 10),
		objectio.NewRowIDWithObjectIDBlkNumAndRowID(obj1, 0, 11),
		objectio.NewRowIDWithObjectIDBlkNumAndRowID(obj2, 1, 5),
	}
	pks := []int64{1, 1, 2}
	values := [][]byte{
		[]byte("mmmnnnppp"),
		[]byte("cccxxxzzz"),
		[]byte("cccxxxzzz"),
	}
	for i := range rows {
		require.NoError(t, vector.AppendFixed(rowIDVec, rows[i], false, mp))
		require.NoError(t, vector.AppendFixed(pkVec, pks[i], false, mp))
		require.NoError(t, vector.AppendBytes(contentVec, values[i], false, mp))
	}

	bat := batch.NewWithSize(3)
	bat.Attrs = []string{catalog.Row_ID, "id", "content"}
	bat.Vecs[0] = rowIDVec
	bat.Vecs[1] = pkVec
	bat.Vecs[2] = contentVec
	bat.SetRowCount(len(rows))

	resolved, err := resolveNativeTailBatchAttrs(bat, "id", []string{"content"})
	require.NoError(t, err)

	builders := make(map[string]*nativeTailSegmentBuilder)
	err = appendNativeTailBatch(
		builders,
		bat,
		resolved,
		types.T_int64,
		fulltext.FullTextParserParam{Parser: "ngram"},
	)
	require.NoError(t, err)

	objects, totalDocs, totalTokens := buildNativeTailSegmentsFromBuilders(builders)
	require.Len(t, objects, 2)
	require.Equal(t, int64(3), totalDocs)
	require.Greater(t, totalTokens, int64(0))

	obj1Name := objectio.BuildObjectNameWithObjectID(&obj1).String()
	obj2Name := objectio.BuildObjectNameWithObjectID(&obj2).String()
	var seg1, seg2 *nativeObjectSegment
	for i := range objects {
		require.True(t, objects[i].applyTombstones)
		switch objects[i].key {
		case obj1Name:
			seg1 = &objects[i]
		case obj2Name:
			seg2 = &objects[i]
		}
	}
	require.NotNil(t, seg1)
	require.NotNil(t, seg2)

	oldPostings, err := seg1.segment.Lookup("mmmnnnppp")
	require.NoError(t, err)
	require.Len(t, oldPostings, 1)
	require.Equal(t, uint16(0), oldPostings[0].Ref.Block)
	require.Equal(t, uint32(10), oldPostings[0].Ref.Row)

	newPostings1, err := seg1.segment.Lookup("cccxxxzzz")
	require.NoError(t, err)
	require.Len(t, newPostings1, 1)
	require.Equal(t, uint16(0), newPostings1[0].Ref.Block)
	require.Equal(t, uint32(11), newPostings1[0].Ref.Row)

	newPostings2, err := seg2.segment.Lookup("cccxxxzzz")
	require.NoError(t, err)
	require.Len(t, newPostings2, 1)
	require.Equal(t, uint16(1), newPostings2[0].Ref.Block)
	require.Equal(t, uint32(5), newPostings2[0].Ref.Row)
}

func TestFilterLiveNativeDocStatesBatchesByBlock(t *testing.T) {
	objID := objectio.NewObjectid()
	objName := objectio.BuildObjectNameWithObjectID(&objID)
	bid := objectio.NewBlockidWithObjectID(objName.ObjectId(), 0)

	tombstones := &mockNativeTombstoner{
		hasAnyTombstoneFile: true,
		persistedDeleted: map[string]map[int64]struct{}{
			bid.String(): {
				11: {},
			},
		},
	}
	scan := &nativePreparedScan{
		tombstones: tombstones,
	}
	states := []*nativeDocState{
		{
			pk:              int64(1),
			docLen:          3,
			ref:             ftnative.RowRef{Block: 0, Row: 10},
			obj:             objName,
			segmentKey:      objName.String(),
			applyTombstones: true,
		},
		{
			pk:              int64(2),
			docLen:          3,
			ref:             ftnative.RowRef{Block: 0, Row: 11},
			obj:             objName,
			segmentKey:      objName.String(),
			applyTombstones: true,
		},
		{
			pk:              int64(3),
			docLen:          3,
			ref:             ftnative.RowRef{Block: 0, Row: 12},
			obj:             objName,
			segmentKey:      objName.String(),
			applyTombstones: true,
		},
	}
	cache := newNativeDeleteCache()

	live, err := filterLiveNativeDocStates(context.Background(), scan, cache, states)
	require.NoError(t, err)
	require.Len(t, live, 2)
	require.Equal(t, int64(1), live[0].pk)
	require.Equal(t, int64(3), live[1].pk)
	require.Equal(t, 1, tombstones.hasBlockCalls)
	require.Equal(t, 1, tombstones.applyPersistedCalls)

	live, err = filterLiveNativeDocStates(context.Background(), scan, cache, states)
	require.NoError(t, err)
	require.Len(t, live, 2)
	require.Equal(t, 1, tombstones.hasBlockCalls)
	require.Equal(t, 1, tombstones.applyPersistedCalls)
}

type mockNativeTombstoner struct {
	hasAnyInMemoryTombstone bool
	hasAnyTombstoneFile     bool
	inMemDeleted            map[string]map[int64]struct{}
	persistedDeleted        map[string]map[int64]struct{}
	hasBlockCalls           int
	applyPersistedCalls     int
}

func (m *mockNativeTombstoner) Type() engine.TombstoneType { return 0 }
func (m *mockNativeTombstoner) HasAnyInMemoryTombstone() bool {
	return m.hasAnyInMemoryTombstone
}
func (m *mockNativeTombstoner) HasAnyTombstoneFile() bool { return m.hasAnyTombstoneFile }
func (m *mockNativeTombstoner) String() string            { return "" }
func (m *mockNativeTombstoner) StringWithPrefix(string) string {
	return ""
}
func (m *mockNativeTombstoner) HasBlockTombstone(
	ctx context.Context,
	id *objectio.Blockid,
	fs fileservice.FileService,
) (bool, error) {
	m.hasBlockCalls++
	if rows := m.inMemDeleted[id.String()]; len(rows) > 0 {
		return true, nil
	}
	if rows := m.persistedDeleted[id.String()]; len(rows) > 0 {
		return true, nil
	}
	return false, nil
}
func (m *mockNativeTombstoner) MarshalBinaryWithBuffer(w *bytes.Buffer) error { return nil }
func (m *mockNativeTombstoner) UnmarshalBinary(buf []byte) error              { return nil }
func (m *mockNativeTombstoner) PrefetchTombstones(string, fileservice.FileService, []objectio.Blockid) {
}
func (m *mockNativeTombstoner) ApplyInMemTombstones(
	bid *types.Blockid,
	rowsOffset []int64,
	deleted *objectio.Bitmap,
) (left []int64) {
	if len(m.inMemDeleted) == 0 {
		return rowsOffset
	}
	return filterMockDeletedRows(rowsOffset, m.inMemDeleted[bid.String()])
}
func (m *mockNativeTombstoner) ApplyPersistedTombstones(
	ctx context.Context,
	fs fileservice.FileService,
	snapshot *types.TS,
	bid *types.Blockid,
	rowsOffset []int64,
	deletedMask *objectio.Bitmap,
) (left []int64, err error) {
	m.applyPersistedCalls++
	return filterMockDeletedRows(rowsOffset, m.persistedDeleted[bid.String()]), nil
}
func (m *mockNativeTombstoner) Merge(other engine.Tombstoner) error { return nil }
func (m *mockNativeTombstoner) SortInMemory()                       {}

func filterMockDeletedRows(rows []int64, deleted map[int64]struct{}) []int64 {
	if len(rows) == 0 || len(deleted) == 0 {
		return rows
	}
	out := make([]int64, 0, len(rows))
	for _, row := range rows {
		if _, ok := deleted[row]; ok {
			continue
		}
		out = append(out, row)
	}
	return out
}
