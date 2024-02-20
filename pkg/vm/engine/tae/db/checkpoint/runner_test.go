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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"strconv"
	"strings"
	"testing"

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
	assert.True(t, e.Equal(types.BuildTS(9, 0)))
	assert.Equal(t, "loc-0;1", loc)

	loc, e, err = r.CollectCheckpointsInRange(ctx, types.BuildTS(12, 0), types.BuildTS(25, 0))
	assert.NoError(t, err)
	assert.True(t, e.Equal(types.BuildTS(29, 0)))
	assert.Equal(t, "loc-10;1;loc-20;1", loc)
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
	assert.Equal(t, "ckp0;1", location)
	assert.True(t, checkpointed.Equal(types.BuildTS(10, 0)))

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
	assert.Equal(t, "ckp3;1", location)
	assert.True(t, checkpointed.Equal(types.BuildTS(40, 0)))

	// [25,45]
	location, checkpointed, err = r.CollectCheckpointsInRange(ctx, types.BuildTS(25, 1), types.BuildTS(45, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "ckp2;1;ckp3;1", location)
	assert.True(t, checkpointed.Equal(types.BuildTS(40, 0)))

	// [22,25]
	location, checkpointed, err = r.CollectCheckpointsInRange(ctx, types.BuildTS(22, 1), types.BuildTS(25, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "ckp2;1", location)
	assert.True(t, checkpointed.Equal(types.BuildTS(30, 0)))

	// [22,35]
	location, checkpointed, err = r.CollectCheckpointsInRange(ctx, types.BuildTS(22, 1), types.BuildTS(35, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "ckp2;1;ckp3;1", location)
	assert.True(t, checkpointed.Equal(types.BuildTS(40, 0)))
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
	assert.Equal(t, "global3;100", location)
	assert.True(t, checkpointed.Equal(types.BuildTS(30, 1)))

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
	assert.Equal(t, "ckp3;3", location)
	assert.True(t, checkpointed.Equal(types.BuildTS(40, 0)))

	// [25,45]
	location, checkpointed, err = r.CollectCheckpointsInRange(ctx, types.BuildTS(25, 1), types.BuildTS(45, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "global3;100;ckp3;3", location)
	assert.True(t, checkpointed.Equal(types.BuildTS(40, 0)))

	// [22,25]
	location, checkpointed, err = r.CollectCheckpointsInRange(ctx, types.BuildTS(22, 1), types.BuildTS(25, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "global3;100", location)
	assert.True(t, checkpointed.Equal(types.BuildTS(30, 1)))

	// [22,35]
	location, checkpointed, err = r.CollectCheckpointsInRange(ctx, types.BuildTS(22, 1), types.BuildTS(35, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "global3;100;ckp3;3", location)
	assert.True(t, checkpointed.Equal(types.BuildTS(40, 0)))

	// [22,29]
	location, checkpointed, err = r.CollectCheckpointsInRange(ctx, types.BuildTS(22, 1), types.BuildTS(29, 0))
	assert.NoError(t, err)
	t.Log(location)
	t.Log(checkpointed.ToString())
	assert.Equal(t, "global3;100", location)
	assert.True(t, checkpointed.Equal(types.BuildTS(30, 1)))
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

func TestNewObjectReade1r(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	name := "c539476e-cf00-11ee-a7ee-fefcfef4c117_00000"

	fsDir := "/Users/shenjiangwei/Work/code/matrixone/mo-data/shared"
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: fsDir,
	}
	service, err := fileservice.NewFileService(ctx, c, nil)
	assert.Nil(t, err)
	reader, err := blockio.NewFileReader(service, name)
	if err != nil {
		return
	}
	//bats, err := reader.LoadAllColumns(ctx, []uint16{0, 1}, common.DefaultAllocator)
	bats, err := reader.LoadAllDeleteColumns(ctx, []uint16{0, 1}, common.DefaultAllocator)
	if err != nil {
		logutil.Infof("load all columns failed: %v", err)
		return
	}
	/*name1, err := EncodeNameFromString(reader.GetName())
	assert.Nil(t, err)
	location := objectio.BuildLocation(name1, *reader.GetObjectReader().GetMetaExtent(), 51, 1)
	_, err = blockio.LoadTombstoneColumns(context.Background(), []uint16{0}, nil, service, location, nil)*/
	//applyDelete(bats[0], bb)
	ts := types.TS{}
	for i := 0; i < bats[0].Vecs[0].Length(); i++ {
		num := objectio.HackBytes2Rowid(bats[0].Vecs[0].GetRawBytesAt(i))
		ts.Unmarshal(bats[0].Vecs[1].GetRawBytesAt(i))
		//_, ro := num.Decode()
		//logutil.Infof("num is %d, cmmit is %v,i is %d", ro, ts.ToString(), i)
		//ts.Unmarshal(bats[0].Vecs[1].GetRawBytesAt(i))
		//num := types.DecodeInt32(bats[0].Vecs[0].GetRawBytesAt(i))
		if strings.Contains(num.String(), "baade41d-8110-11ee-94f8-5254000adb85") {
			logutil.Infof("num is %v, cmmit is %v,i is %d", num.String(), ts.ToString(), i)
		}
		logutil.Infof("num is %v, cmmit is %v,i is %d", num.String(), ts.ToString(), i)
	}
	//logutil.Infof("bats[0].Vecs[1].String() is %v", bats[0].Vecs[0].String())
}

func TestNewObjectReader1(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	name := "cd78288f-cecc-11ee-a7ee-fefcfef4c117_00000"

	fsDir := "/Users/shenjiangwei/Work/code/matrixone/mo-data/shared"
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: fsDir,
	}
	service, err := fileservice.NewFileService(ctx, c, nil)
	assert.Nil(t, err)
	reader, err := blockio.NewFileReader(service, name)
	if err != nil {
		return
	}
	//bats, err := reader.LoadAllColumns(ctx, []uint16{0, 1}, common.DefaultAllocator)
	bats, err := reader.LoadAllColumns(ctx, []uint16{0}, common.DefaultAllocator)
	if err != nil {
		logutil.Infof("load all columns failed: %v", err)
		return
	}
	/*name1, err := EncodeNameFromString(reader.GetName())
	assert.Nil(t, err)
	location := objectio.BuildLocation(name1, *reader.GetObjectReader().GetMetaExtent(), 51, 1)
	_, err = blockio.LoadTombstoneColumns(context.Background(), []uint16{0}, nil, service, location, nil)*/
	//applyDelete(bats[0], bb)
	//ts := types.TS{}
	for y, bat := range bats {
		for i := 0; i < bat.Vecs[0].Length(); i++ {
			id := vector.GetFixedAt[int64](bat.Vecs[0], i)
			if id == 1756143097500782593 || i == 587 || i == 588 {
				logutil.Infof("id is %d, row is %d, blockiddd is %v, y is %v", id, i, name, y)
			}

			//ts.Unmarshal(bats[0].Vecs[1].GetRawBytesAt(i))
			/*num := types.DecodeInt32(bat.Vecs[0].GetRawBytesAt(i))
			num1 := types.DecodeInt32(bat.Vecs[1].GetRawBytesAt(i))
			num2 := types.DecodeInt32(bat.Vecs[2].GetRawBytesAt(i))
			//ts.Unmarshal(bat.Vecs[4].GetRawBytesAt(i))
			//rid := objectio.HackBytes2Rowid(bat.Vecs[3].GetRawBytesAt(i))
			//ab := types.DecodeBool(bat.Vecs[5].GetRawBytesAt(i))

			if num == 1 && num1 == 1 && num2 == 3591 {
				logutil.Infof("num111 is %d-%d-%d, cmmit is %v, i is %d, y is %dv", num, num1, num2, ts.ToString(), i, y)
			} else if num == 1 && num1 == 3 && num2 == 260 {
				logutil.Infof("num11122 is %d-%d-%d, cmmit is %v,i is %d", num, num1, num2, ts.ToString(), i)
			}
			//logutil.Infof("num is %d-%d-%d, cmmit is %v,i is %d", num, num1, num2, ts.ToString(), i)
			if i == bat.Vecs[0].Length()-1 {
				logutil.Infof("num11122 is %d-%d-%d, cmmit is %v,i is %d", num, num1, num2, ts.ToString(), i)
			}*/
		}
		//logutil.Infof("bats[0].Vecs[1].String() is %v", bat.Vecs[2].String())
	}
}

func TestNewObjectReader2(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	name := "55d07f0f-822b-11ee-ae27-5254000adb85_00002"

	fsDir := "/Users/shenjiangwei/Work/code/matrixone/mo-data/shared"
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: fsDir,
	}
	service, err := fileservice.NewFileService(ctx, c, nil)
	assert.Nil(t, err)
	reader, err := blockio.NewFileReader(service, name)
	if err != nil {
		return
	}
	//bats, err := reader.LoadAllColumns(ctx, []uint16{0, 1}, common.DefaultAllocator)
	bats, err := reader.LoadAllColumns(ctx, []uint16{0, 1, 2, 3, 5}, common.DefaultAllocator)
	if err != nil {
		logutil.Infof("load all columns failed: %v", err)
		return
	}
	/*name1, err := EncodeNameFromString(reader.GetName())
	assert.Nil(t, err)
	location := objectio.BuildLocation(name1, *reader.GetObjectReader().GetMetaExtent(), 51, 1)
	_, err = blockio.LoadTombstoneColumns(context.Background(), []uint16{0}, nil, service, location, nil)*/
	//applyDelete(bats[0], bb)
	bf, w, err := reader.LoadOneBF(ctx, 0)
	zm, err := reader.LoadZoneMaps(ctx, []uint16{0, 1, 2, 3, 5}, 0, nil)
	logutil.Infof("zm is %v-%v", zm[0].GetMax(), zm[0].GetMin())
	logutil.Infof("bf is %v, w is %v, err is %v", bf.String(), w, err)
	ts := types.TS{}
	for y, bat := range bats {
		for i := 0; i < bat.Vecs[0].Length(); i++ {
			//ts.Unmarshal(bats[0].Vecs[1].GetRawBytesAt(i))
			num := types.DecodeInt32(bat.Vecs[0].GetRawBytesAt(i))
			num1 := types.DecodeInt32(bat.Vecs[1].GetRawBytesAt(i))
			num3 := types.DecodeInt32(bat.Vecs[2].GetRawBytesAt(i))
			entry := common.TypeStringValue(*bat.Vecs[3].GetType(), any(bat.Vecs[3].GetRawBytesAt(i)), false)
			num2 := entry
			ts.Unmarshal(bat.Vecs[4].GetRawBytesAt(i))

			if num == 9 && num1 == 4 {
				logutil.Infof("num111 is %d-%d-%v, cmmit is %v,i is %d", num, num1, num2, ts.ToString(), i)
			} else if num == 1 && num1 == 3 {
				//logutil.Infof("num11122 is %d-%d-%v, cmmit is %v,i is %d", num, num1, num2, ts.ToString(), i)
			}
			if num2 == "3a15013a15013a160e07" {
				t1, _, _ := types.DecodeTuple(bat.Vecs[3].GetRawBytesAt(i))
				//t1.String()
				logutil.Infof("bats[0].Vecs[1].String() is %v, %d, n %d-%d-%d, y %d, t1.String() is %v, ts is %v", num2, i, num, num1, num3, y, t1.String(), ts.ToString())
			}
			//logutil.Infof("num is %d-%d-%d, cmmit is %v,i is %d", num, num1, num2, ts.ToString(), i)
		}
		//logutil.Infof("bats[0].Vecs[1].String() is %v", bat.Vecs[2].String())
	}
}

// EncodeLocationFromString Generate a metaloc from an info string
func EncodeNameFromString(info string) (objectio.ObjectName, error) {
	location := strings.Split(info, "_")
	num, err := strconv.ParseUint(location[1], 10, 32)
	if err != nil {
		return nil, err
	}
	uid, err := types.ParseUuid(location[0])
	if err != nil {
		return nil, err
	}
	name := objectio.BuildObjectName(&uid, uint16(num))
	return name, nil
}

func applyDelete(dataBatch *batch.Batch, deleteBatch *batch.Batch) error {
	if deleteBatch == nil {
		return nil
	}
	deleteRow := make([]int64, 0)
	rowss := make(map[int64]bool)
	for i := 0; i < deleteBatch.Vecs[0].Length(); i++ {
		row := deleteBatch.Vecs[0].GetRawBytesAt(i)
		rowId := objectio.HackBytes2Rowid(row)
		_, ro := rowId.Decode()
		rowss[int64(ro)] = true
	}
	for i := 0; i < dataBatch.Vecs[0].Length(); i++ {
		if rowss[int64(i)] {
			deleteRow = append(deleteRow, int64(i))
		}
	}
	dataBatch.AntiShrink(deleteRow)
	return nil
}
