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

package native

import (
	"context"
	"sync"
	"testing"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/mocks"
	"github.com/stretchr/testify/require"
)

type nativeMergeSortPool struct {
	pool *containers.VectorPool
}

type recordingFS struct {
	fileservice.FileService
	mu        sync.Mutex
	readCalls int
	readSizes []int64
	readPaths []string
}

func (r *recordingFS) Read(ctx context.Context, vector *fileservice.IOVector) error {
	r.mu.Lock()
	r.readCalls++
	r.readPaths = append(r.readPaths, vector.FilePath)
	for _, entry := range vector.Entries {
		r.readSizes = append(r.readSizes, entry.Size)
	}
	r.mu.Unlock()
	return r.FileService.Read(ctx, vector)
}

func (r *recordingFS) snapshotReadSizes() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int64(nil), r.readSizes...)
}

func (r *recordingFS) snapshotReadCalls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.readCalls
}

func (r *recordingFS) snapshotReadPaths() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.readPaths...)
}

func (r *recordingFS) resetReadSizes() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.readCalls = 0
	r.readSizes = nil
	r.readPaths = nil
}

func (p *nativeMergeSortPool) GetVector(typ *types.Type) (*vector.Vector, func()) {
	v := p.pool.GetVector(typ)
	return v.GetDownstreamVector(), v.Close
}

func (p *nativeMergeSortPool) GetMPool() *mpool.MPool {
	return p.pool.GetMPool()
}

type mockPersistedReader struct {
	rows    []persistedRow
	emitted bool
}

type persistedRow struct {
	rowID types.Rowid
	pk    int64
	body  []byte
}

func (r *mockPersistedReader) Close() error { return nil }

func (r *mockPersistedReader) Read(
	_ context.Context,
	_ []string,
	_ *pbplan.Expr,
	mp *mpool.MPool,
	bat *batch.Batch,
) (bool, error) {
	if r.emitted {
		return true, nil
	}
	for _, row := range r.rows {
		if err := vector.AppendFixed(bat.Vecs[0], row.rowID, false, mp); err != nil {
			return false, err
		}
		if err := vector.AppendFixed(bat.Vecs[1], row.pk, false, mp); err != nil {
			return false, err
		}
		if err := vector.AppendBytes(bat.Vecs[2], row.body, false, mp); err != nil {
			return false, err
		}
	}
	bat.SetRowCount(len(r.rows))
	r.emitted = true
	return false, nil
}

func (r *mockPersistedReader) SetOrderBy([]*pbplan.OrderBySpec)          {}
func (r *mockPersistedReader) GetOrderBy() []*pbplan.OrderBySpec         { return nil }
func (r *mockPersistedReader) SetBlockTop([]*pbplan.OrderBySpec, uint64) {}
func (r *mockPersistedReader) SetFilterZM(objectio.ZoneMap)              {}

type mockPersistedRelation struct {
	readers []engine.Reader
}

func (r *mockPersistedRelation) Ranges(context.Context, engine.RangesParam) (engine.RelData, error) {
	return nil, nil
}

func (r *mockPersistedRelation) BuildReaders(
	context.Context,
	any,
	*pbplan.Expr,
	engine.RelData,
	int,
	int,
	bool,
	engine.TombstoneApplyPolicy,
	engine.FilterHint,
) ([]engine.Reader, error) {
	return r.readers, nil
}

func TestObjectIndexerBuildAndReadSidecar(t *testing.T) {
	schema := catalog.NewEmptySchema("fts_native_test")
	require.NoError(t, schema.AppendPKCol("id", types.T_int64.ToType(), 0))
	require.NoError(t, schema.AppendCol("body", types.T_varchar.ToType()))

	cstrDef := &engine.ConstraintDef{
		Cts: []engine.Constraint{
			&engine.PrimaryKeyDef{
				Pkey: &pbplan.PrimaryKeyDef{
					PkeyColName: "id",
					Names:       []string{"id"},
				},
			},
			&engine.IndexDef{
				Indexes: []*pbplan.IndexDef{{
					IndexName:       "idx_body",
					IndexTableName:  "__idx_body",
					IndexAlgo:       pkgcatalog.MOIndexFullTextAlgo.ToString(),
					Parts:           []string{"body"},
					IndexAlgoParams: `{"parser":"default"}`,
				}},
			},
		},
	}
	var err error
	schema.Constraint, err = cstrDef.MarshalBinary()
	require.NoError(t, err)
	require.NoError(t, schema.Finalize(false))

	mp := mpool.MustNewZero()

	idVec := vector.NewVec(types.T_int64.ToType())
	bodyVec := vector.NewVec(types.T_varchar.ToType())
	defer idVec.Free(mp)
	defer bodyVec.Free(mp)
	require.NoError(t, vector.AppendFixed[int64](idVec, 1, false, mp))
	require.NoError(t, vector.AppendFixed[int64](idVec, 2, false, mp))
	require.NoError(t, vector.AppendBytes(bodyVec, []byte("Matrix Origin native fulltext"), false, mp))
	require.NoError(t, vector.AppendBytes(bodyVec, []byte("native search sidecar"), false, mp))

	bat := batch.NewWithSize(2)
	bat.Attrs = []string{"id", "body"}
	bat.Vecs[0] = idVec
	bat.Vecs[1] = bodyVec
	bat.SetRowCount(2)

	indexer, err := NewObjectIndexer(schema)
	require.NoError(t, err)
	require.False(t, indexer.Empty())
	require.NoError(t, indexer.AddBatch(bat, []uint32{2}))

	fs, err := fileservice.NewMemoryFS("memory", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	objID := objectio.NewObjectid()
	objName := objectio.BuildObjectNameWithObjectID(&objID)
	ResetRuntimeSidecarRegistry()
	published, err := indexer.Write(context.Background(), fs, objName)
	require.NoError(t, err)
	require.Len(t, published, 1)
	require.Equal(t, "__idx_body", published[0].IndexTable)
	require.Equal(t, SidecarPath(objName.String(), "__idx_body"), published[0].SidecarPath)
	require.Equal(t, SidecarLocatorPath(objName.String()), published[0].LocatorPath)
	require.Equal(t, CurrentSegmentVersion, published[0].SegmentVersion)
	require.Equal(t, int64(2), published[0].DocCount)
	require.NotZero(t, published[0].Flags&SidecarFlagLocatorWritten)
	PublishRuntimeSidecars(42, objName.String(), published)
	registrySet, ok := LookupRuntimeSidecars(objName.String())
	require.True(t, ok)
	require.Equal(t, uint64(42), registrySet.TableID)
	require.Len(t, registrySet.Entries, 1)
	require.Equal(t, published[0], registrySet.Entries["__idx_body"])

	seg, ok, err := ReadSidecar(context.Background(), fs, objName, "__idx_body")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(2), seg.DocCount)
	require.Equal(t, int64(7), seg.TokenSum)
	nativePostings, err := seg.Lookup("native")
	require.NoError(t, err)
	require.Len(t, nativePostings, 2)
	matrixPostings, err := seg.Lookup("matrix")
	require.NoError(t, err)
	require.Len(t, matrixPostings, 1)

	locator, ok, err := ReadSidecarLocator(context.Background(), fs, objName.String())
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, locator.Entries, 1)
	require.Equal(t, "__idx_body", locator.Entries[0].IndexTable)
	require.Equal(t, SidecarPath(objName.String(), "__idx_body"), locator.Entries[0].FilePath)
}

func TestBackfillCommittedPersistedSidecars(t *testing.T) {
	ResetRuntimeSidecarRegistry()
	defer ResetRuntimeSidecarRegistry()

	fs, err := fileservice.NewMemoryFS("memory", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	mp := mpool.MustNewZero()
	obj1 := objectio.NewObjectid()
	obj2 := objectio.NewObjectid()
	tableDef := &pbplan.TableDef{
		Name: "docs",
		Cols: []*pbplan.ColDef{
			{Name: "id", Typ: planType(types.T_int64)},
			{Name: "body", Typ: planType(types.T_varchar)},
		},
		Name2ColIndex: map[string]int32{
			"id":   0,
			"body": 1,
		},
		Pkey: &pbplan.PrimaryKeyDef{
			PkeyColName: "id",
			Names:       []string{"id"},
		},
	}
	indexDef := &pbplan.IndexDef{
		IndexName:       "idx_body",
		IndexTableName:  "__idx_body",
		IndexAlgo:       pkgcatalog.MOIndexFullTextAlgo.ToString(),
		Parts:           []string{"body"},
		IndexAlgoParams: `{"parser":"default"}`,
	}
	rel := &mockPersistedRelation{
		readers: []engine.Reader{
			&mockPersistedReader{
				rows: []persistedRow{
					{
						rowID: objectio.NewRowIDWithObjectIDBlkNumAndRowID(obj1, 0, 1),
						pk:    1,
						body:  []byte("matrix origin native"),
					},
					{
						rowID: objectio.NewRowIDWithObjectIDBlkNumAndRowID(obj1, 0, 2),
						pk:    2,
						body:  []byte("native sidecar"),
					},
					{
						rowID: objectio.NewRowIDWithObjectIDBlkNumAndRowID(obj2, 1, 3),
						pk:    3,
						body:  []byte("matrix fulltext"),
					},
				},
			},
		},
	}

	err = BackfillCommittedPersistedSidecars(
		context.Background(),
		nil,
		mp,
		rel,
		fs,
		42,
		tableDef,
		indexDef,
	)
	require.NoError(t, err)

	obj1Name := objectio.BuildObjectNameWithObjectID(&obj1)
	obj2Name := objectio.BuildObjectNameWithObjectID(&obj2)

	registry1, ok := LookupRuntimeSidecars(obj1Name.String())
	require.True(t, ok)
	require.Equal(t, uint64(42), registry1.TableID)
	require.Contains(t, registry1.Entries, "__idx_body")

	registry2, ok := LookupRuntimeSidecars(obj2Name.String())
	require.True(t, ok)
	require.Equal(t, uint64(42), registry2.TableID)
	require.Contains(t, registry2.Entries, "__idx_body")

	locator1, ok, err := ReadSidecarLocator(context.Background(), fs, obj1Name.String())
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, locator1.Entries, 1)
	require.Equal(t, SidecarPath(obj1Name.String(), "__idx_body"), locator1.Entries[0].FilePath)

	locator2, ok, err := ReadSidecarLocator(context.Background(), fs, obj2Name.String())
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, locator2.Entries, 1)
	require.Equal(t, SidecarPath(obj2Name.String(), "__idx_body"), locator2.Entries[0].FilePath)

	seg1, ok, err := ReadPublishedSidecar(context.Background(), fs, obj1Name, "__idx_body")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(2), seg1.DocCount)

	seg2, ok, err := ReadPublishedSidecar(context.Background(), fs, obj2Name, "__idx_body")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(1), seg2.DocCount)
}

func TestReadPublishedSidecarSkipsDeterministicMissWhenLocatorMissing(t *testing.T) {
	baseFS, err := fileservice.NewMemoryFS("memory", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	fs := &recordingFS{FileService: baseFS}
	objID := objectio.NewObjectid()
	objName := objectio.BuildObjectNameWithObjectID(&objID)

	seg, ok, err := ReadPublishedSidecar(context.Background(), fs, objName, "__idx_body")
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, seg)

	paths := fs.snapshotReadPaths()
	require.Equal(t, []string{SidecarLocatorPath(objName.String())}, paths)
	require.NotContains(t, paths, SidecarPath(objName.String(), "__idx_body"))
}

func planType(oid types.T) pbplan.Type {
	return pbplan.Type{Id: int32(oid)}
}

func TestReadSidecarV4UsesRangeReads(t *testing.T) {
	schema := catalog.NewEmptySchema("fts_native_range_read")
	require.NoError(t, schema.AppendPKCol("id", types.T_int64.ToType(), 0))
	require.NoError(t, schema.AppendCol("body", types.T_varchar.ToType()))

	cstrDef := &engine.ConstraintDef{
		Cts: []engine.Constraint{
			&engine.PrimaryKeyDef{
				Pkey: &pbplan.PrimaryKeyDef{
					PkeyColName: "id",
					Names:       []string{"id"},
				},
			},
			&engine.IndexDef{
				Indexes: []*pbplan.IndexDef{{
					IndexName:       "idx_body",
					IndexTableName:  "__idx_body",
					IndexAlgo:       pkgcatalog.MOIndexFullTextAlgo.ToString(),
					Parts:           []string{"body"},
					IndexAlgoParams: `{"parser":"default"}`,
				}},
			},
		},
	}
	var err error
	schema.Constraint, err = cstrDef.MarshalBinary()
	require.NoError(t, err)
	require.NoError(t, schema.Finalize(false))

	mp := mpool.MustNewZero()
	idVec := vector.NewVec(types.T_int64.ToType())
	bodyVec := vector.NewVec(types.T_varchar.ToType())
	defer idVec.Free(mp)
	defer bodyVec.Free(mp)
	for i := 1; i <= 8; i++ {
		require.NoError(t, vector.AppendFixed[int64](idVec, int64(i), false, mp))
		require.NoError(t, vector.AppendBytes(bodyVec, []byte("native stablegamma marker payload"), false, mp))
	}

	bat := batch.NewWithSize(2)
	bat.Attrs = []string{"id", "body"}
	bat.Vecs[0] = idVec
	bat.Vecs[1] = bodyVec
	bat.SetRowCount(8)

	indexer, err := NewObjectIndexer(schema)
	require.NoError(t, err)
	require.NoError(t, indexer.AddBatch(bat, []uint32{8}))

	baseFS, err := fileservice.NewMemoryFS("memory", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	fs := &recordingFS{FileService: baseFS}

	objID := objectio.NewObjectid()
	objName := objectio.BuildObjectNameWithObjectID(&objID)
	_, err = indexer.Write(context.Background(), fs, objName)
	require.NoError(t, err)

	filePath := SidecarPath(objName.String(), "__idx_body")
	entry, err := fs.StatFile(context.Background(), filePath)
	require.NoError(t, err)

	fs.resetReadSizes()
	seg, ok, err := ReadSidecar(context.Background(), fs, objName, "__idx_body")
	require.NoError(t, err)
	require.True(t, ok)
	require.Empty(t, seg.Terms)

	initialReads := fs.snapshotReadSizes()
	require.GreaterOrEqual(t, len(initialReads), 3)
	var initialBytes int64
	for _, size := range initialReads {
		require.NotEqual(t, int64(-1), size)
		initialBytes += size
	}
	require.Less(t, initialBytes, entry.Size)

	postings, err := seg.Lookup("native")
	require.NoError(t, err)
	require.Len(t, postings, 8)
	afterFirstLookup := fs.snapshotReadSizes()
	require.Greater(t, len(afterFirstLookup), len(initialReads))

	postings, err = seg.Lookup("native")
	require.NoError(t, err)
	require.Len(t, postings, 8)
	require.Equal(t, afterFirstLookup, fs.snapshotReadSizes())
}

func TestReadSidecarV4EmptySegmentSkipsZeroLengthDirectoryRead(t *testing.T) {
	baseFS, err := fileservice.NewMemoryFS("memory", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	fs := &recordingFS{FileService: baseFS}

	objID := objectio.NewObjectid()
	objName := objectio.BuildObjectNameWithObjectID(&objID)
	buf, err := NewBuilder(fulltext.FullTextParserParam{}, nil).Build().MarshalBinary()
	require.NoError(t, err)
	require.NoError(t, fs.Write(context.Background(), fileservice.IOVector{
		FilePath: SidecarPath(objName.String(), "__idx_body"),
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(buf)),
			Data:   buf,
		}},
	}))

	fs.resetReadSizes()
	seg, ok, err := ReadSidecar(context.Background(), fs, objName, "__idx_body")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(0), seg.DocCount)
	require.Equal(t, int64(0), seg.TokenSum)
	require.Empty(t, seg.Terms)
	require.Equal(t, []int64{int64(segmentPrefixLen), int64(segmentHeaderLenV4)}, fs.snapshotReadSizes())
}

func TestReadSidecarV4BatchesExactTermReads(t *testing.T) {
	builder := NewBuilder(fulltext.FullTextParserParam{}, nil)
	require.NoError(t, builder.Add(Document{
		Block: 1,
		Row:   1,
		PK:    []byte("pk-1"),
		Values: []fulltext.IndexValue{
			{Text: "native stablegamma", Type: types.T_text},
		},
	}))
	require.NoError(t, builder.Add(Document{
		Block: 1,
		Row:   2,
		PK:    []byte("pk-2"),
		Values: []fulltext.IndexValue{
			{Text: "native stablegamma", Type: types.T_text},
		},
	}))

	baseFS, err := fileservice.NewMemoryFS("memory", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	fs := &recordingFS{FileService: baseFS}

	objID := objectio.NewObjectid()
	objName := objectio.BuildObjectNameWithObjectID(&objID)
	buf, err := builder.Build().MarshalBinary()
	require.NoError(t, err)
	require.NoError(t, fs.Write(context.Background(), fileservice.IOVector{
		FilePath: SidecarPath(objName.String(), "__idx_body"),
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(buf)),
			Data:   buf,
		}},
	}))

	fs.resetReadSizes()
	seg, ok, err := ReadSidecar(context.Background(), fs, objName, "__idx_body")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 3, fs.snapshotReadCalls())

	matches, err := seg.SearchAll([]string{"native", "stablegamma"})
	require.NoError(t, err)
	require.Len(t, matches, 2)
	require.Equal(t, 4, fs.snapshotReadCalls())

	matches, err = seg.SearchAll([]string{"native", "stablegamma"})
	require.NoError(t, err)
	require.Len(t, matches, 2)
	require.Equal(t, 4, fs.snapshotReadCalls())
}

func TestObjectIndexerBuildAndReadSidecarWithNullMultiColumn(t *testing.T) {
	schema := catalog.NewEmptySchema("fts_native_test_null_multi")
	require.NoError(t, schema.AppendPKCol("id", types.T_int64.ToType(), 0))
	require.NoError(t, schema.AppendCol("a", types.T_varchar.ToType()))
	require.NoError(t, schema.AppendCol("b", types.T_varchar.ToType()))

	cstrDef := &engine.ConstraintDef{
		Cts: []engine.Constraint{
			&engine.PrimaryKeyDef{
				Pkey: &pbplan.PrimaryKeyDef{
					PkeyColName: "id",
					Names:       []string{"id"},
				},
			},
			&engine.IndexDef{
				Indexes: []*pbplan.IndexDef{{
					IndexName:      "fi",
					IndexTableName: "__idx_ab",
					IndexAlgo:      pkgcatalog.MOIndexFullTextAlgo.ToString(),
					Parts:          []string{"a", "b"},
				}},
			},
		},
	}
	var err error
	schema.Constraint, err = cstrDef.MarshalBinary()
	require.NoError(t, err)
	require.NoError(t, schema.Finalize(false))

	mp := mpool.MustNewZero()

	idVec := vector.NewVec(types.T_int64.ToType())
	aVec := vector.NewVec(types.T_varchar.ToType())
	bVec := vector.NewVec(types.T_varchar.ToType())
	defer idVec.Free(mp)
	defer aVec.Free(mp)
	defer bVec.Free(mp)

	require.NoError(t, vector.AppendFixed[int64](idVec, 1, false, mp))
	require.NoError(t, vector.AppendFixed[int64](idVec, 2, false, mp))
	require.NoError(t, vector.AppendBytes(aVec, []byte("apple"), false, mp))
	require.NoError(t, vector.AppendBytes(aVec, nil, true, mp))
	require.NoError(t, vector.AppendBytes(bVec, []byte("banana"), false, mp))
	require.NoError(t, vector.AppendBytes(bVec, []byte("cherry"), false, mp))

	bat := batch.NewWithSize(3)
	bat.Attrs = []string{"id", "a", "b"}
	bat.Vecs[0] = idVec
	bat.Vecs[1] = aVec
	bat.Vecs[2] = bVec
	bat.SetRowCount(2)

	indexer, err := NewObjectIndexer(schema)
	require.NoError(t, err)
	require.False(t, indexer.Empty())
	require.NoError(t, indexer.AddBatch(bat, []uint32{2}))

	fs, err := fileservice.NewMemoryFS("memory", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	objID := objectio.NewObjectid()
	objName := objectio.BuildObjectNameWithObjectID(&objID)
	_, err = indexer.Write(context.Background(), fs, objName)
	require.NoError(t, err)

	seg, ok, err := ReadSidecar(context.Background(), fs, objName, "__idx_ab")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(2), seg.DocCount)
	applePostings, err := seg.Lookup("apple")
	require.NoError(t, err)
	require.Len(t, applePostings, 1)
	bananaPostings, err := seg.Lookup("banana")
	require.NoError(t, err)
	require.Len(t, bananaPostings, 1)
	cherryPostings, err := seg.Lookup("cherry")
	require.NoError(t, err)
	require.Len(t, cherryPostings, 1)
}

func TestObjectIndexerBuildAndReadSidecarWithNullMultiColumnAfterMergeAObj(t *testing.T) {
	schema := catalog.NewEmptySchema("fts_native_test_null_multi_merge")
	require.NoError(t, schema.AppendPKCol("id", types.T_int64.ToType(), 0))
	require.NoError(t, schema.AppendCol("a", types.T_varchar.ToType()))
	require.NoError(t, schema.AppendCol("b", types.T_varchar.ToType()))

	cstrDef := &engine.ConstraintDef{
		Cts: []engine.Constraint{
			&engine.PrimaryKeyDef{
				Pkey: &pbplan.PrimaryKeyDef{
					PkeyColName: "id",
					Names:       []string{"id"},
				},
			},
			&engine.IndexDef{
				Indexes: []*pbplan.IndexDef{{
					IndexName:      "fi",
					IndexTableName: "__idx_ab",
					IndexAlgo:      pkgcatalog.MOIndexFullTextAlgo.ToString(),
					Parts:          []string{"a", "b"},
				}},
			},
		},
	}
	var err error
	schema.Constraint, err = cstrDef.MarshalBinary()
	require.NoError(t, err)
	require.NoError(t, schema.Finalize(false))

	mp := mpool.MustNewZero()
	buildBatch := func(id int64, a []byte, aNull bool, b []byte) *containers.Batch {
		idVec := containers.MakeVector(types.T_int64.ToType(), mp)
		aVec := containers.MakeVector(types.T_varchar.ToType(), mp)
		bVec := containers.MakeVector(types.T_varchar.ToType(), mp)
		idVec.Append(id, false)
		aVec.Append(a, aNull)
		bVec.Append(b, false)

		bat := containers.NewBatch()
		bat.AddVector("id", idVec)
		bat.AddVector("a", aVec)
		bat.AddVector("b", bVec)
		return bat
	}

	batches := []*containers.Batch{
		buildBatch(1, []byte("apple"), false, []byte("banana")),
		buildBatch(2, nil, true, []byte("cherry")),
	}
	defer func() {
		for _, bat := range batches {
			bat.Close()
		}
	}()

	pool := &nativeMergeSortPool{pool: mocks.GetTestVectorPool()}
	merged, releaseF, _, err := mergesort.MergeAObj(context.Background(), pool, batches, 0, []uint32{2})
	require.NoError(t, err)
	defer releaseF()
	require.Len(t, merged, 1)
	require.Equal(t, 2, merged[0].RowCount())

	indexer, err := NewObjectIndexer(schema)
	require.NoError(t, err)
	require.NoError(t, indexer.AddBatch(merged[0], []uint32{2}))

	fs, err := fileservice.NewMemoryFS("memory", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	objID := objectio.NewObjectid()
	objName := objectio.BuildObjectNameWithObjectID(&objID)
	_, err = indexer.Write(context.Background(), fs, objName)
	require.NoError(t, err)

	seg, ok, err := ReadSidecar(context.Background(), fs, objName, "__idx_ab")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(2), seg.DocCount)
	cherryPostings, err := seg.Lookup("cherry")
	require.NoError(t, err)
	require.Len(t, cherryPostings, 1)
}

func TestObjectIndexerWriteSkipsEmptySidecarWithRowHint(t *testing.T) {
	schema := catalog.NewEmptySchema("fts_native_test_empty_segment_skip")
	require.NoError(t, schema.AppendPKCol("id", types.T_int64.ToType(), 0))
	require.NoError(t, schema.AppendCol("body", types.T_varchar.ToType()))

	cstrDef := &engine.ConstraintDef{
		Cts: []engine.Constraint{
			&engine.PrimaryKeyDef{
				Pkey: &pbplan.PrimaryKeyDef{
					PkeyColName: "id",
					Names:       []string{"id"},
				},
			},
			&engine.IndexDef{
				Indexes: []*pbplan.IndexDef{{
					IndexName:       "idx_body",
					IndexTableName:  "__idx_body",
					IndexAlgo:       pkgcatalog.MOIndexFullTextAlgo.ToString(),
					Parts:           []string{"body"},
					IndexAlgoParams: `{"parser":"default"}`,
				}},
			},
		},
	}
	var err error
	schema.Constraint, err = cstrDef.MarshalBinary()
	require.NoError(t, err)
	require.NoError(t, schema.Finalize(false))

	mp := mpool.MustNewZero()
	idVec := vector.NewVec(types.T_int64.ToType())
	bodyVec := vector.NewVec(types.T_varchar.ToType())
	defer idVec.Free(mp)
	defer bodyVec.Free(mp)
	require.NoError(t, vector.AppendFixed[int64](idVec, 1, false, mp))
	require.NoError(t, vector.AppendBytes(bodyVec, []byte(""), false, mp))

	bat := batch.NewWithSize(2)
	bat.Attrs = []string{"id", "body"}
	bat.Vecs[0] = idVec
	bat.Vecs[1] = bodyVec
	bat.SetRowCount(1)

	indexer, err := NewObjectIndexer(schema)
	require.NoError(t, err)
	require.NoError(t, indexer.AddBatch(bat, []uint32{1}))

	fs, err := fileservice.NewMemoryFS("memory", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	objID := objectio.NewObjectid()
	objName := objectio.BuildObjectNameWithObjectID(&objID)
	published, err := indexer.Write(context.Background(), fs, objName, 1)
	require.NoError(t, err)
	require.Nil(t, published)

	_, ok, err := ReadSidecar(context.Background(), fs, objName, "__idx_body")
	require.NoError(t, err)
	require.False(t, ok)

	_, ok, err = ReadSidecarLocator(context.Background(), fs, objName.String())
	require.NoError(t, err)
	require.False(t, ok)
}

func TestAppendQueryBatchBuildsSyntheticSegment(t *testing.T) {
	mp := mpool.MustNewZero()

	idVec := vector.NewVec(types.T_int64.ToType())
	bodyVec := vector.NewVec(types.T_varchar.ToType())
	defer idVec.Free(mp)
	defer bodyVec.Free(mp)
	require.NoError(t, vector.AppendFixed[int64](idVec, 10, false, mp))
	require.NoError(t, vector.AppendFixed[int64](idVec, 11, false, mp))
	require.NoError(t, vector.AppendBytes(bodyVec, []byte("appendable native"), false, mp))
	require.NoError(t, vector.AppendBytes(bodyVec, []byte("tail builder"), false, mp))

	bat := batch.NewWithSize(2)
	bat.Attrs = []string{"id", "body"}
	bat.Vecs[0] = idVec
	bat.Vecs[1] = bodyVec
	bat.SetRowCount(2)

	builder := NewBuilder(fulltext.FullTextParserParam{}, nil)
	nextDoc, err := AppendQueryBatch(builder, bat, "id", types.T_int64, []string{"body"}, 0)
	require.NoError(t, err)
	require.Equal(t, uint64(2), nextDoc)

	seg := builder.Build()
	require.Equal(t, int64(2), seg.DocCount)
	require.Equal(t, int64(4), seg.TokenSum)
	appendablePostings, err := seg.Lookup("appendable")
	require.NoError(t, err)
	require.Len(t, appendablePostings, 1)
	tailPostings, err := seg.Lookup("tail")
	require.NoError(t, err)
	require.Len(t, tailPostings, 1)
}

func TestAppendQueryBatchKeepsSyntheticRefsUniqueAcrossCalls(t *testing.T) {
	mp := mpool.MustNewZero()

	buildBatch := func(id int64, body string) *batch.Batch {
		idVec := vector.NewVec(types.T_int64.ToType())
		bodyVec := vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendFixed[int64](idVec, id, false, mp))
		require.NoError(t, vector.AppendBytes(bodyVec, []byte(body), false, mp))

		bat := batch.NewWithSize(2)
		bat.Attrs = []string{"id", "body"}
		bat.Vecs[0] = idVec
		bat.Vecs[1] = bodyVec
		bat.SetRowCount(1)
		return bat
	}

	bat1 := buildBatch(1, "shared token")
	bat2 := buildBatch(2, "shared token")
	defer bat1.Clean(mp)
	defer bat2.Clean(mp)

	builder := NewBuilder(fulltext.FullTextParserParam{}, nil)
	nextDoc, err := AppendQueryBatch(builder, bat1, "id", types.T_int64, []string{"body"}, 0)
	require.NoError(t, err)
	nextDoc, err = AppendQueryBatch(builder, bat2, "id", types.T_int64, []string{"body"}, nextDoc)
	require.NoError(t, err)
	require.Equal(t, uint64(2), nextDoc)

	postings, err := builder.Build().Lookup("shared")
	require.NoError(t, err)
	require.Len(t, postings, 2)
	require.NotEqual(t, postings[0].Ref.Row, postings[1].Ref.Row)
}
