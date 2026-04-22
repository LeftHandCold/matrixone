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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
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

	live, err := filterLiveNativeDocStates(context.Background(), "svc1", scan, cache, states)
	require.NoError(t, err)
	require.Len(t, live, 2)
	require.Equal(t, int64(1), live[0].pk)
	require.Equal(t, int64(3), live[1].pk)
	require.Equal(t, 1, tombstones.prefetchCalls)
	require.Equal(t, 1, tombstones.lastPrefetchBidCount)
	require.Equal(t, 1, tombstones.hasBlockCalls)
	require.Equal(t, 1, tombstones.applyPersistedCalls)

	live, err = filterLiveNativeDocStates(context.Background(), "svc1", scan, cache, states)
	require.NoError(t, err)
	require.Len(t, live, 2)
	require.Equal(t, 2, tombstones.prefetchCalls)
	require.Equal(t, 1, tombstones.hasBlockCalls)
	require.Equal(t, 1, tombstones.applyPersistedCalls)
}

type mockNativeTombstoner struct {
	hasAnyInMemoryTombstone bool
	hasAnyTombstoneFile     bool
	inMemDeleted            map[string]map[int64]struct{}
	persistedDeleted        map[string]map[int64]struct{}
	prefetchCalls           int
	lastPrefetchBidCount    int
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
func (m *mockNativeTombstoner) PrefetchTombstones(_ string, _ fileservice.FileService, bid []objectio.Blockid) {
	m.prefetchCalls++
	m.lastPrefetchBidCount = len(bid)
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

func TestNativeLookupLeafJoinMatchesAllPlusTerms(t *testing.T) {
	builder := ftnative.NewBuilder(fulltext.FullTextParserParam{}, nil)
	require.NoError(t, builder.Add(ftnative.Document{
		Block: 1,
		Row:   1,
		PK:    []byte("pk-1"),
		Values: []fulltext.IndexValue{
			{Text: "alpha stablegamma nativeprobe orange", Type: types.T_text},
		},
	}))
	require.NoError(t, builder.Add(ftnative.Document{
		Block: 1,
		Row:   2,
		PK:    []byte("pk-2"),
		Values: []fulltext.IndexValue{
			{Text: "alpha nativeprobe orange", Type: types.T_text},
		},
	}))
	require.NoError(t, builder.Add(ftnative.Document{
		Block: 1,
		Row:   3,
		PK:    []byte("pk-3"),
		Values: []fulltext.IndexValue{
			{Text: "stablegamma reference", Type: types.T_text},
		},
	}))
	require.NoError(t, builder.Add(ftnative.Document{
		Block: 1,
		Row:   4,
		PK:    []byte("pk-4"),
		Values: []fulltext.IndexValue{
			{Text: "alpha stablegamma", Type: types.T_text},
		},
	}))
	seg := builder.Build()

	patterns, err := fulltext.ParsePattern("+alpha +stablegamma", int64(tree.FULLTEXT_BOOLEAN))
	require.NoError(t, err)
	leafs := make(map[int32]*fulltext.Pattern)
	var phrases []*fulltext.Pattern
	collectNativePatterns(patterns, leafs, &phrases)
	require.Len(t, leafs, 1)

	var join *fulltext.Pattern
	for _, leaf := range leafs {
		join = leaf
	}
	require.NotNil(t, join)
	require.Equal(t, fulltext.JOIN, join.Operator)

	require.NoError(t, prefetchBooleanNativeTerms(seg, leafs, phrases))
	postings, err := nativeLookupLeaf(seg, join)
	require.NoError(t, err)
	require.Len(t, postings, 2)
	require.Equal(t, uint32(1), postings[0].Ref.Row)
	require.Equal(t, uint32(4), postings[1].Ref.Row)

	patterns, err = fulltext.ParsePattern("+alpha +nativeprobe +orange", int64(tree.FULLTEXT_BOOLEAN))
	require.NoError(t, err)
	leafs = make(map[int32]*fulltext.Pattern)
	phrases = nil
	collectNativePatterns(patterns, leafs, &phrases)
	require.Len(t, leafs, 1)
	for _, leaf := range leafs {
		join = leaf
	}
	postings, err = nativeLookupLeaf(seg, join)
	require.NoError(t, err)
	require.Len(t, postings, 2)
	require.Equal(t, uint32(1), postings[0].Ref.Row)
	require.Equal(t, uint32(2), postings[1].Ref.Row)
}

func TestCollectNativeNegativeLeafIndexes(t *testing.T) {
	patterns, err := fulltext.ParsePattern("+alpha -reference", int64(tree.FULLTEXT_BOOLEAN))
	require.NoError(t, err)
	negative := collectNativeNegativeLeafIndexes(patterns)
	require.Len(t, negative, 1)
	require.Contains(t, negative, int32(1))

	patterns, err = fulltext.ParsePattern("+alpha +stablegamma", int64(tree.FULLTEXT_BOOLEAN))
	require.NoError(t, err)
	negative = collectNativeNegativeLeafIndexes(patterns)
	require.Empty(t, negative)

	patterns, err = fulltext.ParsePattern("+(alpha beta) -gamma", int64(tree.FULLTEXT_BOOLEAN))
	require.NoError(t, err)
	negative = collectNativeNegativeLeafIndexes(patterns)
	require.Len(t, negative, 1)
	require.Contains(t, negative, int32(2))
}

func TestCollectNativeAnchorLeafIndexes(t *testing.T) {
	patterns, err := fulltext.ParsePattern("+alpha +stablegamma", int64(tree.FULLTEXT_BOOLEAN))
	require.NoError(t, err)
	anchor := collectNativeAnchorLeafIndexes(patterns, true)
	require.Len(t, anchor, 1)
	require.Contains(t, anchor, int32(0))

	patterns, err = fulltext.ParsePattern("+(alpha beta) +gamma", int64(tree.FULLTEXT_BOOLEAN))
	require.NoError(t, err)
	anchor = collectNativeAnchorLeafIndexes(patterns, true)
	require.Len(t, anchor, 2)
	require.Contains(t, anchor, int32(0))
	require.Contains(t, anchor, int32(1))

	patterns, err = fulltext.ParsePattern("alpha beta", int64(tree.FULLTEXT_BOOLEAN))
	require.NoError(t, err)
	anchor = collectNativeAnchorLeafIndexes(patterns, false)
	require.Empty(t, anchor)
}

func TestValidateNativeScanUsage(t *testing.T) {
	nativeOnly := fulltext.FullTextParserParam{
		Implementation: fulltext.FullTextImplementationNative,
		NativeOnlyMode: true,
	}
	nativeFallback := fulltext.FullTextParserParam{
		Implementation: fulltext.FullTextImplementationNative,
	}

	t.Run("unsupported pattern falls back when fallback allowed", func(t *testing.T) {
		used, err := validateNativeScanUsage(context.Background(), nativeFallback, false, nil)
		require.NoError(t, err)
		require.False(t, used)
	})

	t.Run("unsupported pattern errors in native only mode", func(t *testing.T) {
		used, err := validateNativeScanUsage(context.Background(), nativeOnly, false, nil)
		require.ErrorContains(t, err, "native-only fulltext query pattern is not supported")
		require.False(t, used)
	})

	t.Run("missing scan falls back when fallback allowed", func(t *testing.T) {
		used, err := validateNativeScanUsage(context.Background(), nativeFallback, true, nil)
		require.NoError(t, err)
		require.False(t, used)
	})

	t.Run("missing scan errors in native only mode", func(t *testing.T) {
		used, err := validateNativeScanUsage(context.Background(), nativeOnly, true, nil)
		require.ErrorContains(t, err, "native-only fulltext query is unavailable")
		require.False(t, used)
	})

	t.Run("incomplete scan falls back when fallback allowed", func(t *testing.T) {
		used, err := validateNativeScanUsage(context.Background(), nativeFallback, true, &nativePreparedScan{complete: false})
		require.NoError(t, err)
		require.False(t, used)
	})

	t.Run("incomplete scan errors in native only mode", func(t *testing.T) {
		used, err := validateNativeScanUsage(context.Background(), nativeOnly, true, &nativePreparedScan{complete: false})
		require.ErrorContains(t, err, "native sidecars are incomplete")
		require.False(t, used)
	})

	t.Run("complete scan uses native path", func(t *testing.T) {
		used, err := validateNativeScanUsage(context.Background(), nativeOnly, true, &nativePreparedScan{complete: true})
		require.NoError(t, err)
		require.True(t, used)
	})
}
