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

package gc

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"sort"
)

func MergeCheckpoint(
	ctx context.Context,
	fs fileservice.FileService,
	ckpEntries []*checkpoint.CheckpointEntry,
	snapshotList map[uint32][]types.TS,
	objects map[string]*ObjectEntry,
	tables map[uint64]*logtail.TableInfo,
	pool *mpool.MPool,
) error {
	ckpData := logtail.NewCheckpointData(pool)
	ckpSnapList := make([]types.TS, 0)
	datas := make([]*logtail.CheckpointData, 0)
	for _, ts := range snapshotList {
		ckpSnapList = append(ckpSnapList, ts...)
	}
	sort.Slice(ckpSnapList, func(i, j int) bool {
		return ckpSnapList[i].Less(&ckpSnapList[j])
	})
	for _, ckpEntry := range ckpEntries {
		logutil.Infof("merge checkpoint %v", ckpEntry.String())
		if !isSnapshotCKPRefers(ckpEntry, ckpSnapList) {
			continue
		}
		_, data, err := logtail.LoadCheckpointEntriesFromKey(context.Background(), fs,
			ckpEntry.GetLocation(), ckpEntry.GetVersion(), nil)
		if err != nil {
			return err
		}
		datas = append(datas, data)
	}
	if len(datas) == 0 {
		return nil
	}

	tablesInfo := make([]*logtail.TableInfo, 0)
	for _, table := range tables {
		tablesInfo = append(tablesInfo, table)
	}
	sort.Slice(tablesInfo, func(i, j int) bool {
		return tablesInfo[i].Tid < tablesInfo[j].Tid
	})

	for _, table := range tablesInfo {
		for _, data := range datas {
			ins := data.GetObjectBatchs()
			tid := vector.MustFixedCol[uint64](ins.GetVectorByName(catalog.SnapshotAttr_TID).GetDownstreamVector())

			for i := 0; i < ins.Length(); i++ {
				var objectStats objectio.ObjectStats
				buf := ins.GetVectorByName(catalog.ObjectAttr_ObjectStats).Get(i).([]byte)
				objectStats.UnMarshal(buf)
				if objects[objectStats.ObjectName().String()] == nil || tid[i] != table.Tid {
					continue
				}
				appendValToBatch(ins, ckpData.GetObjectBatchs(), i)
			}

			blockIns := data.GetOneBatch(logtail.BLKMetaInsertIDX)
			blockTxnIns := data.GetOneBatch(logtail.BLKMetaInsertTxnIDX)
			tid = vector.MustFixedCol[uint64](blockTxnIns.GetVectorByName(catalog.SnapshotAttr_TID).GetDownstreamVector())
			for i := 0; i < blockIns.Length(); i++ {
				if tid[i] != table.Tid {
					continue
				}
				appendValToBatch(blockIns, ckpData.GetOneBatch(logtail.BLKMetaInsertIDX), i)
				appendValToBatch(blockTxnIns, ckpData.GetOneBatch(logtail.BLKMetaInsertTxnIDX), i)
			}
		}
	}
	for _, data := range datas {

		dbIns := data.GetOneBatch(logtail.DBInsertIDX)
		for i := 0; i < dbIns.Length(); i++ {
			appendValToBatch(dbIns, ckpData.GetOneBatch(logtail.DBInsertIDX), i)
		}
		dbDels := data.GetOneBatch(logtail.DBDeleteIDX)
		for i := 0; i < dbDels.Length(); i++ {
			appendValToBatch(dbDels, ckpData.GetOneBatch(logtail.DBDeleteIDX), i)
		}
		tableIns := data.GetOneBatch(logtail.TBLInsertIDX)
		for i := 0; i < tableIns.Length(); i++ {
			appendValToBatch(tableIns, ckpData.GetOneBatch(logtail.TBLInsertIDX), i)
		}
		tableDel := data.GetOneBatch(logtail.TBLDeleteIDX)
		for i := 0; i < tableDel.Length(); i++ {
			appendValToBatch(tableDel, ckpData.GetOneBatch(logtail.TBLDeleteIDX), i)
		}
		colIns := data.GetOneBatch(logtail.TBLColInsertIDX)
		for i := 0; i < colIns.Length(); i++ {
			appendValToBatch(colIns, ckpData.GetOneBatch(logtail.TBLColInsertIDX), i)
		}
		colDel := data.GetOneBatch(logtail.TBLColDeleteIDX)
		for i := 0; i < colDel.Length(); i++ {
			appendValToBatch(colDel, ckpData.GetOneBatch(logtail.TBLColDeleteIDX), i)
		}
	}
	cnLocation, tnLocation, _, err := ckpData.WriteTo(
		fs, logtail.DefaultCheckpointBlockRows, logtail.DefaultCheckpointSize,
	)
	bat := makeRespBatchFromSchema(checkpoint.CheckpointSchema)
	bat.GetVectorByName(checkpoint.CheckpointAttr_StartTS).Append(ckpEntries[0].GetStart(), false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_EndTS).Append(ckpEntries[len(ckpEntries)-1].GetEnd(), false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_MetaLocation).Append([]byte(cnLocation), false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_EntryType).Append(false, false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_Version).Append(ckpEntries[len(ckpEntries)-1].GetVersion(), false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_AllLocations).Append([]byte(tnLocation), false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_CheckpointLSN).Append(uint64(0), false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_TruncateLSN).Append(uint64(0), false)
	bat.GetVectorByName(checkpoint.CheckpointAttr_Type).Append(int8(checkpoint.ET_Global), false)
	defer bat.Close()
	name := blockio.EncodeSnapshotMetadataFileName(checkpoint.CheckpointDir, checkpoint.PrefixMetadata, ckpEntries[0].GetStart(), ckpEntries[len(ckpEntries)-1].GetEnd())
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterCheckpoint, name, fs)
	if err != nil {
		return err
	}
	if _, err = writer.Write(containers.ToCNBatch(bat)); err != nil {
		return err
	}

	// TODO: checkpoint entry should maintain the location
	_, err = writer.WriteEnd(ctx)
	logutil.Infof("write checkpoint %s", name)
	return err
}

func makeRespBatchFromSchema(schema *catalog.Schema) *containers.Batch {
	bat := containers.NewBatch()
	// Types() is not used, then empty schema can also be handled here
	typs := schema.AllTypes()
	attrs := schema.AllNames()
	for i, attr := range attrs {
		if attr == catalog.PhyAddrColumnName {
			continue
		}
		bat.AddVector(attr, containers.MakeVector(typs[i], common.CheckpointAllocator))
	}
	return bat
}

func appendValToBatch(src, dst *containers.Batch, row int) {
	for v, vec := range src.Vecs {
		val := vec.Get(row)
		if val == nil {
			dst.Vecs[v].Append(val, true)
		} else {
			dst.Vecs[v].Append(val, false)
		}
	}
}

func isSnapshotCKPRefers(ckp *checkpoint.CheckpointEntry, snapVec []types.TS) bool {
	if len(snapVec) == 0 {
		return false
	}
	left, right := 0, len(snapVec)-1
	for left <= right {
		mid := left + (right-left)/2
		snapTS := snapVec[mid]
		start := ckp.GetStart()
		end := ckp.GetEnd()
		if snapTS.GreaterEq(&start) && snapTS.Less(&end) {
			logutil.Infof("isSnapshotRefers: %s, create %v, drop %v",
				snapTS.ToString(), start.ToString(), end.ToString())
			return true
		} else if snapTS.Less(&start) {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return false
}
