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
}
