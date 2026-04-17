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

package checkpoint

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	ftnative "github.com/matrixorigin/matrixone/pkg/fulltext/native"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/require"
)

func TestReplayFTSSidecarRegistry(t *testing.T) {
	defer testutils.AfterTest(t)()

	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("memory", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	ftnative.ResetRuntimeSidecarRegistry()

	cata := catalog.MockCatalog(nil)
	defer cata.Close()

	db, err := cata.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)
	table, err := db.CreateTableEntry(catalog.MockSchema(2, 0), nil, nil)
	require.NoError(t, err)

	createTS := types.BuildTS(200, 0)
	obj := catalog.MockCreatedObjectEntry2List(table, cata, false, createTS)
	objectName := obj.ObjectStats.ObjectName().String()
	expected := ftnative.PublishedSidecar{
		IndexTable:     "__idx_body",
		SidecarPath:    ftnative.SidecarPath(objectName, "__idx_body"),
		LocatorPath:    ftnative.SidecarLocatorPath(objectName),
		SegmentVersion: ftnative.CurrentSegmentVersion,
		DocCount:       7,
		Flags:          ftnative.SidecarFlagLocatorWritten,
	}
	ftnative.PublishRuntimeSidecars(table.ID, objectName, []ftnative.PublishedSidecar{expected})

	collector := logtail.NewBaseCollector_V2(types.TS{}, createTS, 0, fs)
	require.NoError(t, collector.Collect(cata))
	data := collector.OrphanData()
	defer data.Close()
	collector.Close()

	location, _, err := data.Sync(ctx, fs)
	require.NoError(t, err)

	reader, err := logtail.GetCheckpointReader(ctx, "test", fs, location, logtail.CheckpointCurrentVersion)
	require.NoError(t, err)

	ftnative.ResetRuntimeSidecarRegistry()
	replayer := &CkpReplayer{
		ckpEntries: []*CheckpointEntry{{
			end: createTS,
		}},
		ckpReader: &CheckpointReader{
			readers:      []*logtail.CKPReader{reader},
			maxGlobalEnd: createTS,
		},
	}
	require.NoError(t, replayer.replayFTSSidecarRegistry(ctx))

	set, ok := ftnative.LookupRuntimeSidecars(objectName)
	require.True(t, ok)
	require.Equal(t, table.ID, set.TableID)
	require.Len(t, set.Entries, 1)

	entry, ok := set.Entries[expected.IndexTable]
	require.True(t, ok)
	require.Equal(t, expected.SidecarPath, entry.SidecarPath)
	require.Equal(t, expected.LocatorPath, entry.LocatorPath)
	require.Equal(t, expected.SegmentVersion, entry.SegmentVersion)
	require.Equal(t, expected.DocCount, entry.DocCount)
	require.NotZero(t, entry.Flags&ftnative.SidecarFlagLocatorWritten)
	require.NotZero(t, entry.Flags&ftnative.SidecarFlagReplayed)
}
