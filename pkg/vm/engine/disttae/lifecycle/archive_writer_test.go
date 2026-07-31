// Copyright 2026 Matrix Origin
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

package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

const archiveTestMaxPhysicalBytes = uint64(1 << 30)

func TestArchiveWriterRoundTripAndStableChunks(t *testing.T) {
	ctx := context.Background()
	store := newMemoryArchiveStore()
	guard := &testArchiveSideEffectGuard{durable: true}
	schema := archiveTestSchema()
	schemaDigest, err := schema.Digest()
	require.NoError(t, err)

	writer, err := NewArchiveWriter(ctx, ArchiveWriterConfig{
		RootID:               "root-1",
		AttemptID:            "attempt-1",
		Prefix:               "archive/root-1/attempt-1",
		WriteID:              "write-1",
		Schema:               schema,
		SchemaDigest:         schemaDigest,
		MaxRestoreChunkRows:  2,
		MaxChunkLogicalBytes: 1 << 20,
		MaxPhysicalBytes:     archiveTestMaxPhysicalBytes,
	}, store, guard)
	require.NoError(t, err)

	mp := mpool.MustNewZero()
	value := archiveTestBatch(t, mp,
		archiveTestRow{1, "one"},
		archiveTestRow{2, "two"},
		archiveTestRow{3, "three"},
	)
	defer value.Clean(mp)

	require.NoError(t, writer.WriteBatch(ctx, value, nil))
	manifest, manifestKey, err := writer.Close(ctx)
	require.NoError(t, err)
	require.True(t, guard.checkedBeforeFirstPut)
	require.Equal(t, uint64(3), manifest.RowCount)
	require.Equal(t, uint64(2), manifest.TotalChunkCount)
	require.Len(t, manifest.Files, 2)
	require.Equal(t, uint64(0), manifest.Files[0].Chunks[0].ChunkOrdinal)
	require.Equal(t, uint64(1), manifest.Files[1].Chunks[0].ChunkOrdinal)
	require.Equal(t, uint32(0), manifest.Files[0].Chunks[0].RowGroupOrdinal)
	require.NotEmpty(t, manifest.ContentHash)
	require.Contains(t, manifestKey, "manifest-")
	require.Contains(t, manifest.Files[0].Key, "payload-000000-write-1.parquet")
	for _, file := range manifest.Files {
		require.Equal(t, uint64(1), store.getCount(file.Key),
			"Close must full-read each payload exactly once")
	}

	decoded, err := ReadAndVerifyArchive(ctx, store, manifestKey)
	require.NoError(t, err)
	require.Equal(t, manifest.ContentHash, decoded.ContentHash)
	require.Equal(t, manifest.SchemaDigest, decoded.SchemaDigest)
	require.Equal(t, uint64(3), decoded.RowCount)
	persisted, err := ReadArchiveManifest(ctx, store, manifestKey)
	require.NoError(t, err)
	require.Equal(t, "FULL_READBACK_VERIFIED", persisted.VerificationStatus)
}

func TestArchiveWriterFlushesBeforeLogicalByteLimitAndRejectsOversizeRow(t *testing.T) {
	ctx := context.Background()
	schema := archiveTestSchema()
	schemaDigest, err := schema.Digest()
	require.NoError(t, err)
	rowBytes, err := canonicalArchiveRowLogicalBytes(
		ctx,
		schemaDigest,
		[]CanonicalCell{
			{Type: types.T_int64.ToType(), Value: int64(1)},
			{Type: types.T_varchar.ToType(), Value: []byte("small")},
		},
	)
	require.NoError(t, err)

	store := newMemoryArchiveStore()
	writer, err := NewArchiveWriter(ctx, ArchiveWriterConfig{
		RootID:               "root-2",
		AttemptID:            "attempt-2",
		Prefix:               "archive/root-2/attempt-2",
		WriteID:              "write-2",
		Schema:               schema,
		SchemaDigest:         schemaDigest,
		MaxRestoreChunkRows:  100,
		MaxChunkLogicalBytes: rowBytes + canonicalArchiveChunkOverhead(schemaDigest),
		MaxPhysicalBytes:     archiveTestMaxPhysicalBytes,
	}, store, &testArchiveSideEffectGuard{durable: true})
	require.NoError(t, err)
	mp := mpool.MustNewZero()
	value := archiveTestBatch(t, mp,
		archiveTestRow{1, "small"},
		archiveTestRow{2, "small"},
	)
	defer value.Clean(mp)
	require.NoError(t, writer.WriteBatch(ctx, value, nil))
	manifest, _, err := writer.Close(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), manifest.TotalChunkCount)

	oversizeStore := newMemoryArchiveStore()
	oversizeWriter, err := NewArchiveWriter(ctx, ArchiveWriterConfig{
		RootID:               "root-3",
		AttemptID:            "attempt-3",
		Prefix:               "archive/root-3/attempt-3",
		WriteID:              "write-3",
		Schema:               schema,
		SchemaDigest:         schemaDigest,
		MaxRestoreChunkRows:  100,
		MaxChunkLogicalBytes: rowBytes + canonicalArchiveChunkOverhead(schemaDigest),
		MaxPhysicalBytes:     archiveTestMaxPhysicalBytes,
	}, oversizeStore, &testArchiveSideEffectGuard{durable: true})
	require.NoError(t, err)
	oversize := archiveTestBatch(t, mp, archiveTestRow{1, string(make([]byte, 4096))})
	defer oversize.Clean(mp)
	err = oversizeWriter.WriteBatch(ctx, oversize, nil)
	require.ErrorIs(t, err, ErrArchiveRowTooLarge)
	require.Empty(t, oversizeStore.keys())
}

func TestArchiveWriterRejectsChunkBeforeExceedingDatasetLimit(t *testing.T) {
	ctx := context.Background()
	schema := archiveTestSchema()
	schemaDigest, err := schema.Digest()
	require.NoError(t, err)
	store := newMemoryArchiveStore()
	writer, err := NewArchiveWriter(ctx, ArchiveWriterConfig{
		RootID:               "root-chunk-cap",
		AttemptID:            "attempt-chunk-cap",
		Prefix:               "archive/root-chunk-cap/attempt-chunk-cap",
		WriteID:              "write-chunk-cap",
		Schema:               schema,
		SchemaDigest:         schemaDigest,
		MaxRestoreChunkRows:  1,
		MaxChunkLogicalBytes: 1 << 20,
		MaxPhysicalBytes:     archiveTestMaxPhysicalBytes,
	}, store, &testArchiveSideEffectGuard{durable: true})
	require.NoError(t, err)
	writer.files = make([]ArchiveFile, maxArchiveChunksPerDataset)

	mp := mpool.MustNewZero()
	value := archiveTestBatch(t, mp, archiveTestRow{1, "one"})
	defer value.Clean(mp)
	require.NoError(t, writer.WriteBatch(ctx, value, nil))
	_, _, err = writer.Close(ctx)
	require.ErrorContains(t, err, "certified chunk limit")
	require.Empty(t, store.keys())
}

func TestArchiveWriterRequiresCleanupRootBeforeFirstSideEffect(t *testing.T) {
	ctx := context.Background()
	schema := archiveTestSchema()
	schemaDigest, err := schema.Digest()
	require.NoError(t, err)
	store := newMemoryArchiveStore()
	writer, err := NewArchiveWriter(ctx, ArchiveWriterConfig{
		RootID:               "root-4",
		AttemptID:            "attempt-4",
		Prefix:               "archive/root-4/attempt-4",
		WriteID:              "write-4",
		Schema:               schema,
		SchemaDigest:         schemaDigest,
		MaxRestoreChunkRows:  1,
		MaxChunkLogicalBytes: 1 << 20,
		MaxPhysicalBytes:     archiveTestMaxPhysicalBytes,
	}, store, &testArchiveSideEffectGuard{durable: false})
	require.NoError(t, err)
	mp := mpool.MustNewZero()
	value := archiveTestBatch(t, mp, archiveTestRow{1, "one"})
	defer value.Clean(mp)

	require.NoError(t, writer.WriteBatch(ctx, value, nil))
	_, _, err = writer.Close(ctx)
	require.ErrorIs(t, err, ErrCleanupRootNotDurable)
	require.Empty(t, store.keys())
}

func TestArchiveWriterRejectsPhysicalBytesBeforePut(t *testing.T) {
	ctx := context.Background()
	schema := archiveTestSchema()
	schemaDigest, err := schema.Digest()
	require.NoError(t, err)
	store := newMemoryArchiveStore()
	writer, err := NewArchiveWriter(ctx, ArchiveWriterConfig{
		RootID:               "root-physical-cap",
		AttemptID:            "attempt-physical-cap",
		Prefix:               "archive/root-physical-cap/attempt-physical-cap",
		WriteID:              "write-physical-cap",
		Schema:               schema,
		SchemaDigest:         schemaDigest,
		MaxRestoreChunkRows:  1,
		MaxChunkLogicalBytes: 1 << 20,
		MaxPhysicalBytes:     1,
	}, store, &testArchiveSideEffectGuard{durable: true})
	require.NoError(t, err)
	mp := mpool.MustNewZero()
	value := archiveTestBatch(t, mp, archiveTestRow{1, "one"})
	defer value.Clean(mp)
	require.NoError(t, writer.WriteBatch(ctx, value, nil))

	_, _, err = writer.Close(ctx)
	require.ErrorContains(t, err, "reserved physical bytes")
	require.Empty(t, store.keys())
}

func TestArchiveWriterFaultAfterPayloadPutLeavesOnlyRootOwnedImmutableFile(
	t *testing.T,
) {
	ctx := context.Background()
	schema := archiveTestSchema()
	schemaDigest, err := schema.Digest()
	require.NoError(t, err)
	store := newMemoryArchiveStore()
	faults := NewProgrammableFaultInjector(map[FaultPoint]FaultAction{
		FaultAfterPayloadPut: FailOnHit(1, "payload-put-response-lost"),
	})
	writer, err := NewArchiveWriter(ctx, ArchiveWriterConfig{
		RootID:               "root-payload-fault",
		AttemptID:            "attempt-payload-fault",
		Prefix:               "archive/root-payload-fault/attempt-payload-fault",
		WriteID:              "write-payload-fault",
		Schema:               schema,
		SchemaDigest:         schemaDigest,
		MaxRestoreChunkRows:  1,
		MaxChunkLogicalBytes: 1 << 20,
		MaxPhysicalBytes:     archiveTestMaxPhysicalBytes,
		Faults:               faults,
	}, store, &testArchiveSideEffectGuard{durable: true})
	require.NoError(t, err)
	mp := mpool.MustNewZero()
	value := archiveTestBatch(t, mp, archiveTestRow{1, "one"})
	defer value.Clean(mp)
	require.NoError(t, writer.WriteBatch(ctx, value, nil))

	_, _, err = writer.Close(ctx)
	require.EqualError(t, err, "payload-put-response-lost")
	require.Equal(t, []string{
		"archive/root-payload-fault/attempt-payload-fault/" +
			"payload-000000-write-payload-fault.parquet",
	}, store.keys())
	require.Equal(t, uint64(1), faults.Hits(FaultAfterPayloadPut))
	require.Zero(t, faults.Hits(FaultBeforeManifestPut))
}

func TestArchiveWriterPersistsAndReadbackVerifiesAutoIncrementMaximum(t *testing.T) {
	ctx := context.Background()
	store := newMemoryArchiveStore()
	schema := archiveTestSchema()
	schema.Columns[0].AutoIncrement = true
	schemaDigest, err := schema.Digest()
	require.NoError(t, err)
	writer, err := NewArchiveWriter(ctx, ArchiveWriterConfig{
		RootID:               "root-auto",
		AttemptID:            "attempt-auto",
		Prefix:               "archive/root-auto/attempt-auto",
		WriteID:              "write-auto",
		Schema:               schema,
		SchemaDigest:         schemaDigest,
		MaxRestoreChunkRows:  2,
		MaxChunkLogicalBytes: 1 << 20,
		MaxPhysicalBytes:     archiveTestMaxPhysicalBytes,
	}, store, &testArchiveSideEffectGuard{durable: true})
	require.NoError(t, err)
	mp := mpool.MustNewZero()
	value := archiveTestBatch(t, mp,
		archiveTestRow{7, "seven"},
		archiveTestRow{3, "three"},
		archiveTestRow{11, "eleven"},
	)
	defer value.Clean(mp)
	require.NoError(t, writer.WriteBatch(ctx, value, nil))
	manifest, key, err := writer.Close(ctx)
	require.NoError(t, err)
	require.Equal(t, []AutoIncrementMax{{
		ColumnOrdinal: 0,
		Value:         "11",
	}}, manifest.AutoIncrementMaxima)
	persisted, err := ReadArchiveManifest(ctx, store, key)
	require.NoError(t, err)
	require.Equal(t, manifest.AutoIncrementMaxima, persisted.AutoIncrementMaxima)
}

func TestArchiveWriterIgnoresNonPositiveSignedAutoIncrementValues(t *testing.T) {
	ctx := context.Background()
	store := newMemoryArchiveStore()
	schema := archiveTestSchema()
	schema.Columns[0].AutoIncrement = true
	schemaDigest, err := schema.Digest()
	require.NoError(t, err)
	writer, err := NewArchiveWriter(ctx, ArchiveWriterConfig{
		RootID:               "root-auto-negative",
		AttemptID:            "attempt-auto-negative",
		Prefix:               "archive/root-auto-negative/attempt-auto-negative",
		WriteID:              "write-auto-negative",
		Schema:               schema,
		SchemaDigest:         schemaDigest,
		MaxRestoreChunkRows:  8,
		MaxChunkLogicalBytes: 1 << 20,
		MaxPhysicalBytes:     archiveTestMaxPhysicalBytes,
	}, store, &testArchiveSideEffectGuard{durable: true})
	require.NoError(t, err)
	mp := mpool.MustNewZero()
	value := archiveTestBatch(t, mp,
		archiveTestRow{-7, "negative"},
		archiveTestRow{0, "zero"},
	)
	defer value.Clean(mp)
	require.NoError(t, writer.WriteBatch(ctx, value, nil))
	manifest, _, err := writer.Close(ctx)
	require.NoError(t, err)
	require.Empty(t, manifest.AutoIncrementMaxima)
}

func TestArchiveReadbackRejectsCorruptionAndReorderedManifest(t *testing.T) {
	ctx := context.Background()
	store := newMemoryArchiveStore()
	manifestKey := writeArchiveTestDataset(t, store)

	manifestBytes, err := store.Get(ctx, manifestKey)
	require.NoError(t, err)
	manifest, err := ParseArchiveManifest(manifestBytes)
	require.NoError(t, err)

	payload, err := store.Get(ctx, manifest.Files[0].Key)
	require.NoError(t, err)
	payload[len(payload)/2] ^= 0x55
	require.NoError(t, store.Put(ctx, manifest.Files[0].Key, payload))
	_, err = ReadAndVerifyArchive(ctx, store, manifestKey)
	require.Error(t, err)

	store = newMemoryArchiveStore()
	manifestKey = writeArchiveTestDataset(t, store)
	manifestBytes, err = store.Get(ctx, manifestKey)
	require.NoError(t, err)
	manifest, err = ParseArchiveManifest(manifestBytes)
	require.NoError(t, err)
	slices.Reverse(manifest.Files)
	reordered, err := json.Marshal(manifest)
	require.NoError(t, err)
	require.NoError(t, store.Put(ctx, manifestKey, reordered))
	_, err = ReadAndVerifyArchive(ctx, store, manifestKey)
	require.Error(t, err)
}

func TestArchiveReadbackRejectsPayloadSizeChangeBeforeReadingBody(t *testing.T) {
	ctx := context.Background()
	store := newMemoryArchiveStore()
	manifestKey := writeArchiveTestDataset(t, store)
	manifest, err := ReadArchiveManifest(ctx, store, manifestKey)
	require.NoError(t, err)
	file := manifest.Files[0]
	beforeReads := store.getCount(file.Key)

	store.mu.Lock()
	store.objects[file.Key] = append(store.objects[file.Key], 0x01)
	store.mu.Unlock()

	_, err = ReadAndVerifyArchive(ctx, store, manifestKey)
	require.ErrorContains(t, err, "size changed")
	require.Equal(t, beforeReads, store.getCount(file.Key))
}

func writeArchiveTestDataset(t *testing.T, store *memoryArchiveStore) string {
	t.Helper()
	ctx := context.Background()
	schema := archiveTestSchema()
	schemaDigest, err := schema.Digest()
	require.NoError(t, err)
	writer, err := NewArchiveWriter(ctx, ArchiveWriterConfig{
		RootID:               "root-corrupt",
		AttemptID:            "attempt-corrupt",
		Prefix:               "archive/root-corrupt/attempt-corrupt",
		WriteID:              "write-corrupt",
		Schema:               schema,
		SchemaDigest:         schemaDigest,
		MaxRestoreChunkRows:  1,
		MaxChunkLogicalBytes: 1 << 20,
		MaxPhysicalBytes:     archiveTestMaxPhysicalBytes,
	}, store, &testArchiveSideEffectGuard{durable: true})
	require.NoError(t, err)
	mp := mpool.MustNewZero()
	value := archiveTestBatch(t, mp,
		archiveTestRow{1, "one"},
		archiveTestRow{2, "two"},
	)
	defer value.Clean(mp)
	require.NoError(t, writer.WriteBatch(ctx, value, nil))
	_, key, err := writer.Close(ctx)
	require.NoError(t, err)
	return key
}

type archiveTestRow struct {
	id   int64
	name string
}

func archiveTestSchema() SchemaDescriptor {
	return SchemaDescriptor{
		FormatVersion:      schemaDescriptorFormatVersion,
		SourceTableID:      42,
		SourceTableVersion: 7,
		SourceDatabaseName: "db",
		SourceTableName:    "events",
		Columns: []SchemaColumn{
			{Ordinal: 0, SourceColumnID: 1, Name: "id", TypeID: int32(types.T_int64), NotNull: true},
			{Ordinal: 1, SourceColumnID: 2, Name: "name", TypeID: int32(types.T_varchar), Width: 1024},
		},
	}
}

func archiveTestBatch(t *testing.T, mp *mpool.MPool, rows ...archiveTestRow) *batch.Batch {
	t.Helper()
	value := batch.New([]string{"id", "name"})
	value.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	nameType := types.T_varchar.ToType()
	nameType.Width = 1024
	value.Vecs[1] = vector.NewVec(nameType)
	for _, row := range rows {
		require.NoError(t, vector.AppendFixed(value.Vecs[0], row.id, false, mp))
		require.NoError(t, vector.AppendBytes(value.Vecs[1], []byte(row.name), false, mp))
	}
	value.SetRowCount(len(rows))
	return value
}

type testArchiveSideEffectGuard struct {
	durable               bool
	checkedBeforeFirstPut bool
}

func (guard *testArchiveSideEffectGuard) EnsureDurable(
	_ context.Context,
	_, _ string,
) error {
	guard.checkedBeforeFirstPut = true
	if !guard.durable {
		return ErrCleanupRootNotDurable
	}
	return nil
}

type memoryArchiveStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	gets    map[string]uint64
}

func newMemoryArchiveStore() *memoryArchiveStore {
	return &memoryArchiveStore{
		objects: make(map[string][]byte),
		gets:    make(map[string]uint64),
	}
}

func (store *memoryArchiveStore) Put(_ context.Context, key string, value []byte) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.objects[key] = append([]byte(nil), value...)
	return nil
}

func (store *memoryArchiveStore) Get(_ context.Context, key string) ([]byte, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.gets[key]++
	value, ok := store.objects[key]
	if !ok {
		return nil, errors.New("object not found")
	}
	return append([]byte(nil), value...), nil
}

func (store *memoryArchiveStore) Stat(_ context.Context, key string) (int64, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	value, ok := store.objects[key]
	if !ok {
		return 0, errors.New("object not found")
	}
	return int64(len(value)), nil
}

func (store *memoryArchiveStore) GetExact(
	_ context.Context,
	key string,
	size int64,
) ([]byte, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.gets[key]++
	value, ok := store.objects[key]
	if !ok {
		return nil, errors.New("object not found")
	}
	if int64(len(value)) != size {
		return nil, errors.New("object size changed")
	}
	return append([]byte(nil), value...), nil
}

func (store *memoryArchiveStore) getCount(key string) uint64 {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.gets[key]
}

func (store *memoryArchiveStore) List(_ context.Context, prefix string) ([]string, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	keys := make([]string, 0)
	for key := range store.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	return keys, nil
}

func (store *memoryArchiveStore) Delete(_ context.Context, key string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	delete(store.objects, key)
	return nil
}

func (store *memoryArchiveStore) keys() []string {
	store.mu.Lock()
	defer store.mu.Unlock()
	keys := make([]string, 0, len(store.objects))
	for key := range store.objects {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
