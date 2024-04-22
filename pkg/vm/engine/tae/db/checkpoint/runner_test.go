// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package checkpoint

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"sort"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/objectio"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
)

func TestCkpCheck(t *testing.T) {
	defer testutils.AfterTest(t)()
	r := NewRunner(context.Background(), nil, nil, nil, nil)

	for i := 0; i < 100; i += 10 {
		r.storage.entries.Set(&CheckpointEntry{
			start:      types.BuildTS(int64(i), 0),
			end:        types.BuildTS(int64(i+9), 0),
			state:      ST_Finished,
			cnLocation: objectio.Location(fmt.Sprintf("loc-%d", i)),
			version:    1,
		})
	}

	r.storage.entries.Set(&CheckpointEntry{
		start:      types.BuildTS(int64(100), 0),
		end:        types.BuildTS(int64(109), 0),
		state:      ST_Running,
		cnLocation: objectio.Location("loc-100"),
		version:    1,
	})

	ctx := context.Background()

	loc, e, err := r.CollectCheckpointsInRange(ctx, types.BuildTS(4, 0), types.BuildTS(5, 0))
	assert.NoError(t, err)
	assert.True(t, e.Equal(types.BuildTSForTest(9, 0)))
	assert.Equal(t, "loc-0;1;[0-0_9-0]", loc)

	loc, e, err = r.CollectCheckpointsInRange(ctx, types.BuildTS(12, 0), types.BuildTS(25, 0))
	assert.NoError(t, err)
	assert.True(t, e.Equal(types.BuildTSForTest(29, 0)))
	assert.Equal(t, "loc-10;1;loc-20;1;[10-0_29-0]", loc)
}

func TestGetCheckpoints1(t *testing.T) {
	defer testutils.AfterTest(t)()
	r := NewRunner(context.Background(), nil, nil, nil, nil)

	// ckp0[0,10]
	// ckp1[10,20]
	// ckp2[20,30]
	// ckp3[30,40]
	// ckp4[40,50(unfinished)]
	timestamps := make([]types.TS, 0)
	for i := 0; i < 6; i++ {
		ts := types.BuildTS(int64(i*10), 0)
		timestamps = append(timestamps, ts)
	}
	for i := 0; i < 5; i++ {
		entry := &CheckpointEntry{
			start:      timestamps[i].Next(),
			end:        timestamps[i+1],
			state:      ST_Finished,
			cnLocation: objectio.Location(fmt.Sprintf("ckp%d", i)),
			version:    1,
		}
		if i == 4 {
			entry.state = ST_Pending
		}
		r.storage.entries.Set(entry)
	}

	ctx := context.Background()
	// [0,10]
	location, checkpointed, err := r.CollectCheckpointsInRange(ctx, types.BuildTS(0, 1), types.BuildTS(10, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "ckp0;1;[0-1_10-0]", location)
	assert.True(t, checkpointed.Equal(types.BuildTSForTest(10, 0)))

	// [45,50]
	location, checkpointed, err = r.CollectCheckpointsInRange(ctx, types.BuildTS(45, 0), types.BuildTS(50, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "", location)
	assert.True(t, checkpointed.IsEmpty())

	// [30,45]
	location, checkpointed, err = r.CollectCheckpointsInRange(ctx, types.BuildTS(30, 1), types.BuildTS(45, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "ckp3;1;[30-1_40-0]", location)
	assert.True(t, checkpointed.Equal(types.BuildTSForTest(40, 0)))

	// [25,45]
	location, checkpointed, err = r.CollectCheckpointsInRange(ctx, types.BuildTS(25, 1), types.BuildTS(45, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "ckp2;1;ckp3;1;[20-1_40-0]", location)
	assert.True(t, checkpointed.Equal(types.BuildTSForTest(40, 0)))

	// [22,25]
	location, checkpointed, err = r.CollectCheckpointsInRange(ctx, types.BuildTS(22, 1), types.BuildTS(25, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "ckp2;1;[20-1_30-0]", location)
	assert.True(t, checkpointed.Equal(types.BuildTSForTest(30, 0)))

	// [22,35]
	location, checkpointed, err = r.CollectCheckpointsInRange(ctx, types.BuildTS(22, 1), types.BuildTS(35, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "ckp2;1;ckp3;1;[20-1_40-0]", location)
	assert.True(t, checkpointed.Equal(types.BuildTSForTest(40, 0)))
}
func TestGetCheckpoints2(t *testing.T) {
	defer testutils.AfterTest(t)()
	r := NewRunner(context.Background(), nil, nil, nil, nil)

	// ckp0[0,10]
	// ckp1[10,20]
	// ckp2[20,30]
	// global3[0,30]
	// ckp3[30,40]
	// ckp4[40,50(unfinished)]
	timestamps := make([]types.TS, 0)
	for i := 0; i < 6; i++ {
		ts := types.BuildTS(int64(i*10), 0)
		timestamps = append(timestamps, ts)
	}
	for i := 0; i < 5; i++ {
		addGlobal := false
		if i == 3 {
			addGlobal = true
		}
		if addGlobal {
			entry := &CheckpointEntry{
				start:      types.TS{},
				end:        timestamps[i].Next(),
				state:      ST_Finished,
				cnLocation: objectio.Location(fmt.Sprintf("global%d", i)),
				version:    100,
			}
			r.storage.globals.Set(entry)
		}
		start := timestamps[i].Next()
		if addGlobal {
			start = start.Next()
		}
		entry := &CheckpointEntry{
			start:      start,
			end:        timestamps[i+1],
			state:      ST_Finished,
			cnLocation: objectio.Location(fmt.Sprintf("ckp%d", i)),
			version:    uint32(i),
		}
		if i == 4 {
			entry.state = ST_Pending
		}
		r.storage.entries.Set(entry)
	}

	ctx := context.Background()
	// [0,10]
	location, checkpointed, err := r.CollectCheckpointsInRange(ctx, types.BuildTS(0, 1), types.BuildTS(10, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "global3;100;[30-1_30-1]", location)
	assert.True(t, checkpointed.Equal(types.BuildTSForTest(30, 1)))

	// [45,50]
	location, checkpointed, err = r.CollectCheckpointsInRange(ctx, types.BuildTS(45, 0), types.BuildTS(50, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "", location)
	assert.True(t, checkpointed.IsEmpty())

	// [30,45]
	location, checkpointed, err = r.CollectCheckpointsInRange(ctx, types.BuildTS(30, 2), types.BuildTS(45, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "ckp3;3;[30-2_40-0]", location)
	assert.True(t, checkpointed.Equal(types.BuildTSForTest(40, 0)))

	// [25,45]
	location, checkpointed, err = r.CollectCheckpointsInRange(ctx, types.BuildTS(25, 1), types.BuildTS(45, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "global3;100;ckp3;3;[30-1_40-0]", location)
	assert.True(t, checkpointed.Equal(types.BuildTSForTest(40, 0)))

	// [22,25]
	location, checkpointed, err = r.CollectCheckpointsInRange(ctx, types.BuildTS(22, 1), types.BuildTS(25, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "global3;100;[30-1_30-1]", location)
	assert.True(t, checkpointed.Equal(types.BuildTSForTest(30, 1)))

	// [22,35]
	location, checkpointed, err = r.CollectCheckpointsInRange(ctx, types.BuildTS(22, 1), types.BuildTS(35, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "global3;100;ckp3;3;[30-1_40-0]", location)
	assert.True(t, checkpointed.Equal(types.BuildTSForTest(40, 0)))

	// [22,29]
	location, checkpointed, err = r.CollectCheckpointsInRange(ctx, types.BuildTS(22, 1), types.BuildTS(29, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "global3;100;[30-1_30-1]", location)
	assert.True(t, checkpointed.Equal(types.BuildTSForTest(30, 1)))
}
func TestICKPSeekLT(t *testing.T) {
	defer testutils.AfterTest(t)()
	r := NewRunner(context.Background(), nil, nil, nil, nil)

	// ckp0[0,10]
	// ckp1[10,20]
	// ckp2[20,30]
	// ckp3[30,40]
	// ckp4[40,50(unfinished)]
	timestamps := make([]types.TS, 0)
	for i := 0; i < 6; i++ {
		ts := types.BuildTS(int64(i*10), 0)
		timestamps = append(timestamps, ts)
	}
	for i := 0; i < 5; i++ {
		entry := &CheckpointEntry{
			start:      timestamps[i].Next(),
			end:        timestamps[i+1],
			state:      ST_Finished,
			cnLocation: objectio.Location(fmt.Sprintf("ckp%d", i)),
			version:    uint32(i),
		}
		if i == 4 {
			entry.state = ST_Pending
		}
		r.storage.entries.Set(entry)
	}

	// 0, 1
	ckps := r.ICKPSeekLT(types.BuildTS(0, 0), 1)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 1, len(ckps))
	assert.Equal(t, "ckp0", ckps[0].cnLocation.String())

	// 0, 0
	ckps = r.ICKPSeekLT(types.BuildTS(0, 0), 0)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 0, len(ckps))

	// 0, 2
	ckps = r.ICKPSeekLT(types.BuildTS(0, 0), 4)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 4, len(ckps))
	assert.Equal(t, "ckp0", ckps[0].cnLocation.String())
	assert.Equal(t, "ckp1", ckps[1].cnLocation.String())

	// 0, 4
	ckps = r.ICKPSeekLT(types.BuildTS(0, 0), 4)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 4, len(ckps))
	assert.Equal(t, "ckp0", ckps[0].cnLocation.String())
	assert.Equal(t, "ckp1", ckps[1].cnLocation.String())
	assert.Equal(t, "ckp2", ckps[2].cnLocation.String())
	assert.Equal(t, "ckp3", ckps[3].cnLocation.String())

	// 0,10
	ckps = r.ICKPSeekLT(types.BuildTS(0, 0), 10)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 4, len(ckps))
	assert.Equal(t, "ckp0", ckps[0].cnLocation.String())
	assert.Equal(t, "ckp1", ckps[1].cnLocation.String())
	assert.Equal(t, "ckp2", ckps[2].cnLocation.String())
	assert.Equal(t, "ckp3", ckps[3].cnLocation.String())

	// 5,1
	ckps = r.ICKPSeekLT(types.BuildTS(5, 0), 1)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 1, len(ckps))
	assert.Equal(t, "ckp1", ckps[0].cnLocation.String())

	// 50,1
	ckps = r.ICKPSeekLT(types.BuildTS(50, 0), 1)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 0, len(ckps))

	// 55,3
	ckps = r.ICKPSeekLT(types.BuildTS(55, 0), 3)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 0, len(ckps))

	// 40,3
	ckps = r.ICKPSeekLT(types.BuildTS(40, 0), 3)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 0, len(ckps))

	// 35,3
	ckps = r.ICKPSeekLT(types.BuildTS(35, 0), 3)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 0, len(ckps))

	// 30,3
	ckps = r.ICKPSeekLT(types.BuildTS(30, 0), 3)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 1, len(ckps))
	assert.Equal(t, "ckp3", ckps[0].cnLocation.String())

	// 30-2,3
	ckps = r.ICKPSeekLT(types.BuildTS(30, 2), 3)
	for _, e := range ckps {
		t.Log(e.String())
	}
	assert.Equal(t, 0, len(ckps))
}

func TestBlockWriter_GetName(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	fsDir := "/Users/shenjiangwei/Work/local/tae/matrixone/mo-data/shared"
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: fsDir,
	}
	service, err := fileservice.NewFileService(ctx, c, nil)
	assert.Nil(t, err)
	dirs, err := service.List(ctx, CheckpointDir)
	if err != nil {
		return
	}
	if len(dirs) == 0 {
		return
	}
	logutil.Infof("dirs: %v", dirs)
	metaFiles := make([]*metaFile, 0)
	var readDuration time.Duration
	for i, dir := range dirs {
		start, end := blockio.DecodeCheckpointMetadataFileName(dir.Name)
		metaFiles = append(metaFiles, &metaFile{
			start: start,
			end:   end,
			index: i,
		})
	}
	sort.Slice(metaFiles, func(i, j int) bool {
		return metaFiles[i].end.Less(metaFiles[j].end)
	})
	targetIdx := metaFiles[len(metaFiles)-1].index
	dir := dirs[targetIdx]
	reader, err := blockio.NewFileReader(service, CheckpointDir+dir.Name)
	if err != nil {
		logutil.Infof("failed to create reader: %v", err)
		return
	}
	bats, err := reader.LoadAllColumns(ctx, nil, common.DefaultAllocator)
	if err != nil {
		logutil.Infof("failed to load all columns: %v", err)
		return
	}
	bat := containers.NewBatch()
	defer bat.Close()
	t0 := time.Now()
	colNames := CheckpointSchema.Attrs()
	colTypes := CheckpointSchema.Types()
	var isCheckpointVersion1 bool
	// in version 1, checkpoint metadata doesn't contain 'version'.
	if len(bats[0].Vecs) < CheckpointSchemaColumnCountV1 {
		isCheckpointVersion1 = true
	}
	for i := range bats[0].Vecs {
		if len(bats) == 0 {
			continue
		}
		var vec containers.Vector
		if bats[0].Vecs[i].Length() == 0 {
			vec = containers.MakeVector(colTypes[i], common.DebugAllocator)
		} else {
			vec = containers.ToTNVector(bats[0].Vecs[i], common.DebugAllocator)
		}
		bat.AddVector(colNames[i], vec)
	}
	logutil.Infof("bat len %d", bat.Length())
	readDuration += time.Since(t0)
	datas := make([]*logtail.CheckpointData, bat.Length())

	entries := make([]*CheckpointEntry, bat.Length())
	emptyFile := make([]*CheckpointEntry, 0)
	readfn := func(i int, readType uint16) {
		start := bat.GetVectorByName(CheckpointAttr_StartTS).Get(i).(types.TS)
		end := bat.GetVectorByName(CheckpointAttr_EndTS).Get(i).(types.TS)
		cnLoc := objectio.Location(bat.GetVectorByName(CheckpointAttr_MetaLocation).Get(i).([]byte))
		isIncremental := bat.GetVectorByName(CheckpointAttr_EntryType).Get(i).(bool)
		typ := ET_Global
		if isIncremental {
			typ = ET_Incremental
		}
		var version uint32
		if isCheckpointVersion1 {
			version = logtail.CheckpointVersion1
		} else {
			version = bat.GetVectorByName(CheckpointAttr_Version).Get(i).(uint32)
		}
		var tnLoc objectio.Location
		if version <= logtail.CheckpointVersion4 {
			tnLoc = cnLoc
		} else {
			tnLoc = objectio.Location(bat.GetVectorByName(CheckpointAttr_AllLocations).Get(i).([]byte))
		}
		var ckpLSN, truncateLSN uint64
		if version >= logtail.CheckpointVersion7 {
			ckpLSN = bat.GetVectorByName(CheckpointAttr_CheckpointLSN).Get(i).(uint64)
			truncateLSN = bat.GetVectorByName(CheckpointAttr_TruncateLSN).Get(i).(uint64)
		}
		logutil.Infof("cnLoc: %v, tnLoc: %v", cnLoc.String(), tnLoc.String())
		checkpointEntry := &CheckpointEntry{
			start:       start,
			end:         end,
			cnLocation:  cnLoc,
			tnLocation:  tnLoc,
			state:       ST_Finished,
			entryType:   typ,
			version:     version,
			ckpLSN:      ckpLSN,
			truncateLSN: truncateLSN,
		}
		var err2 error
		if readType == PrefetchData {
			logutil.Warnf("read %v failed: %v", checkpointEntry.String(), err2)
			if err2 = checkpointEntry.PrefetchWithFs(ctx, service, datas[i]); err2 != nil {
				logutil.Warnf("read %v failed: %v", checkpointEntry.String(), err2)
			}
		} else if readType == PrefetchMetaIdx {
			datas[i], err = checkpointEntry.PrefetchMetaIdxWithFs(ctx, service)
			if err != nil {
				return
			}
		} else if readType == ReadMetaIdx {
			err = checkpointEntry.ReadMetaIdxWithFs(ctx, service, datas[i])
			if err != nil {
				return
			}
		} else {
			if err2 = checkpointEntry.ReadWithFs(ctx, service, datas[i]); err2 != nil {
				logutil.Warnf("read %v failed: %v", checkpointEntry.String(), err2)
				emptyFile = append(emptyFile, checkpointEntry)
			} else {
				entries[i] = checkpointEntry
			}
			datas[i].PrintMetaBatch()
		}
	}
	t0 = time.Now()
	for i := 0; i < bat.Length(); i++ {
		metaLoc := objectio.Location(bat.GetVectorByName(CheckpointAttr_MetaLocation).Get(i).([]byte))

		err = blockio.PrefetchMeta(service, metaLoc)
		if err != nil {
			return
		}
	}
	for i := 0; i < bat.Length(); i++ {
		readfn(i, PrefetchMetaIdx)
	}
	for i := 0; i < bat.Length(); i++ {
		readfn(i, ReadMetaIdx)
	}
	for i := 0; i < bat.Length(); i++ {
		readfn(i, PrefetchData)
	}
	for i := 0; i < bat.Length(); i++ {
		readfn(i, ReadData)
	}
	readDuration += time.Since(t0)
	if err != nil {
		return
	}
	t0 = time.Now()
	//globalIdx := 0
	for i := 0; i < bat.Length(); i++ {
		checkpointEntry := entries[i]
		if checkpointEntry == nil {
			continue
		}
		logutil.Infof("read %v", checkpointEntry.String())
	}
	/*meta, extent, err := blockio.GetLocationWithFilename(ctx, service, "7e625e16-6740-11ee-83ce-16a5e7607fd0_00000")
	assert.Nil(t, err)
	logutil.Infof("meta: %d, extent: %v", meta.SubMetaCount(), extent.String())*/
}
