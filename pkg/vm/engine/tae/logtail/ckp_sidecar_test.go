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

package logtail

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	ftnative "github.com/matrixorigin/matrixone/pkg/fulltext/native"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/stretchr/testify/require"
)

func TestCheckpointCarriesFTSSidecarRowsSeparately(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("memory", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	ftnative.ResetRuntimeSidecarRegistry()

	cata := catalog.MockCatalog(nil)
	defer cata.Close()

	db, err := cata.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)
	schema := catalog.MockSchema(2, 0)
	table, err := db.CreateTableEntry(schema, nil, nil)
	require.NoError(t, err)

	createTS := types.BuildTS(100, 0)
	obj := catalog.MockCreatedObjectEntry2List(table, cata, false, createTS)
	objectName := obj.ObjectStats.ObjectName().String()
	ftnative.PublishRuntimeSidecars(table.ID, objectName, []ftnative.PublishedSidecar{{
		IndexTable:     "__idx_body",
		SidecarPath:    ftnative.SidecarPath(objectName, "__idx_body"),
		LocatorPath:    ftnative.SidecarLocatorPath(objectName),
		SegmentVersion: ftnative.CurrentSegmentVersion,
		DocCount:       3,
		Flags:          ftnative.SidecarFlagLocatorWritten,
	}})

	collector := NewBaseCollector_V2(types.TS{}, createTS, 0, fs)
	require.NoError(t, collector.Collect(cata))
	data := collector.OrphanData()
	defer data.Close()
	collector.Close()

	location, _, err := data.Sync(ctx, fs)
	require.NoError(t, err)

	reader, err := GetCheckpointReader(ctx, "test", fs, location, CheckpointCurrentVersion)
	require.NoError(t, err)

	regularRows := 0
	err = reader.ForEachRow(
		ctx,
		func(
			_ uint32,
			_ uint64,
			tid uint64,
			objectType int8,
			objectStats objectio.ObjectStats,
			create, delete types.TS,
			_ types.Rowid,
		) error {
			regularRows++
			require.Equal(t, table.ID, tid)
			require.Equal(t, ckputil.ObjectType_Data, objectType)
			require.Equal(t, objectName, objectStats.ObjectName().String())
			require.Equal(t, createTS, create)
			require.True(t, delete.IsEmpty())
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, 1, regularRows)

	sidecarRows := 0
	err = reader.ForEachFTSSidecarRow(
		ctx,
		func(
			_ uint32,
			_ uint64,
			tid uint64,
			objectStats objectio.ObjectStats,
			create, delete types.TS,
			indexTable, sidecarPath, locatorPath string,
			segmentVersion uint32,
			docCount int64,
			flags uint16,
			_ types.Rowid,
		) error {
			sidecarRows++
			require.Equal(t, table.ID, tid)
			require.Equal(t, objectName, objectStats.ObjectName().String())
			require.Equal(t, createTS, create)
			require.True(t, delete.IsEmpty())
			require.Equal(t, "__idx_body", indexTable)
			require.Equal(t, ftnative.SidecarPath(objectName, "__idx_body"), sidecarPath)
			require.Equal(t, ftnative.SidecarLocatorPath(objectName), locatorPath)
			require.Equal(t, ftnative.CurrentSegmentVersion, segmentVersion)
			require.Equal(t, int64(3), docCount)
			require.NotZero(t, flags&ftnative.SidecarFlagLocatorWritten)
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, 1, sidecarRows)

	ckpData, err := reader.GetCheckpointData(ctx)
	require.NoError(t, err)
	defer ckpData.Clean(common.CheckpointAllocator)
	require.Equal(t, 1, ckpData.RowCount())

	rawData, err := reader.GetRawCheckpointData(ctx)
	require.NoError(t, err)
	defer rawData.Clean(common.CheckpointAllocator)
	require.Equal(t, 2, rawData.RowCount())
	objectTypes := vector.MustFixedColNoTypeCheck[int8](rawData.Vecs[ckputil.TableObjectsAttr_ObjectType_Idx])
	require.Equal(t, []int8{ckputil.ObjectType_Data, ckputil.ObjectType_FTSSidecar}, objectTypes)

	flatFiles, err := LoadCheckpointFTSFiles(ctx, reader, nil)
	require.NoError(t, err)
	require.Len(t, flatFiles, 2)
	flatFileNeedCopy := make(map[string]bool, len(flatFiles))
	for _, file := range flatFiles {
		flatFileNeedCopy[file.Path] = file.NeedCopy
	}
	require.Equal(t, map[string]bool{
		ftnative.SidecarLocatorPath(objectName):        true,
		ftnative.SidecarPath(objectName, "__idx_body"): true,
	}, flatFileNeedCopy)

	baseTS := types.BuildTS(101, 0)
	flatFiles, err = LoadCheckpointFTSFiles(ctx, reader, &baseTS)
	require.NoError(t, err)
	require.Len(t, flatFiles, 2)
	flatFileNeedCopy = make(map[string]bool, len(flatFiles))
	for _, file := range flatFiles {
		flatFileNeedCopy[file.Path] = file.NeedCopy
	}
	require.Equal(t, map[string]bool{
		ftnative.SidecarLocatorPath(objectName):        false,
		ftnative.SidecarPath(objectName, "__idx_body"): false,
	}, flatFileNeedCopy)

	tableReader := NewCKPReaderWithTableID_V2(
		CheckpointCurrentVersion,
		location,
		table.ID,
		common.CheckpointAllocator,
		fs,
	)
	require.NoError(t, tableReader.ReadMeta(ctx))
	ranges, err := tableReader.GetTableRanges(ctx)
	require.NoError(t, err)
	require.Len(t, ranges, 1)
	require.Equal(t, ckputil.ObjectType_Data, ranges[0].ObjectType)
	require.Equal(t, 1, ranges[0].Rows())

	dstFS, err := fileservice.NewMemoryFS("dst", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	lastReader, err := GetCheckpointReader(ctx, "test", fs, location, CheckpointCurrentVersion)
	require.NoError(t, err)
	rewrittenLocation, _, _, err := ReWriteCheckpointAndBlockFromKey(
		ctx,
		"test",
		fs,
		dstFS,
		location,
		lastReader,
		CheckpointCurrentVersion,
		createTS,
	)
	require.NoError(t, err)

	rewrittenReader, err := GetCheckpointReader(ctx, "test", dstFS, rewrittenLocation, CheckpointCurrentVersion)
	require.NoError(t, err)
	rewrittenRaw, err := rewrittenReader.GetRawCheckpointData(ctx)
	require.NoError(t, err)
	defer rewrittenRaw.Clean(common.CheckpointAllocator)
	require.Equal(t, 2, rewrittenRaw.RowCount())
	rewrittenSidecarRows := 0
	err = rewrittenReader.ForEachFTSSidecarRow(
		ctx,
		func(
			_ uint32,
			_ uint64,
			tid uint64,
			objectStats objectio.ObjectStats,
			create, delete types.TS,
			indexTable, sidecarPath, locatorPath string,
			segmentVersion uint32,
			docCount int64,
			flags uint16,
			_ types.Rowid,
		) error {
			rewrittenSidecarRows++
			require.Equal(t, table.ID, tid)
			require.Equal(t, objectName, objectStats.ObjectName().String())
			require.Equal(t, createTS, create)
			require.True(t, delete.IsEmpty())
			require.Equal(t, "__idx_body", indexTable)
			require.Equal(t, ftnative.SidecarPath(objectName, "__idx_body"), sidecarPath)
			require.Equal(t, ftnative.SidecarLocatorPath(objectName), locatorPath)
			require.Equal(t, ftnative.CurrentSegmentVersion, segmentVersion)
			require.Equal(t, int64(3), docCount)
			require.NotZero(t, flags&ftnative.SidecarFlagLocatorWritten)
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, 1, rewrittenSidecarRows)
}
