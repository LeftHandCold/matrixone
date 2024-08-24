// Copyright 2021 Matrix Origin
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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type objData struct {
	stats      *objectio.ObjectStats
	data       []*batch.Batch
	sortKey    uint16
	ckpRow     int
	tid        uint64
	appendable bool
	dataType   objectio.DataMetaType
	isChange   bool
}

type tableOffset struct {
	offset int
	end    int
}

type BackupDeltaLocDataSource struct {
	ctx context.Context
	fs  fileservice.FileService
	ts  types.TS
	ds  map[string]*objData
}

func NewBackupDeltaLocDataSource(
	ctx context.Context,
	fs fileservice.FileService,
	ts types.TS,
	ds map[string]*objData,
) *BackupDeltaLocDataSource {
	return &BackupDeltaLocDataSource{
		ctx: ctx,
		fs:  fs,
		ts:  ts,
		ds:  ds,
	}
}

func (d *BackupDeltaLocDataSource) Next(
	_ context.Context,
	_ []string,
	_ []types.Type,
	_ []uint16,
	_ any,
	_ *mpool.MPool,
	_ engine.VectorPool,
) (*batch.Batch, *objectio.BlockInfo, engine.DataState, error) {
	return nil, nil, engine.Persisted, nil
}

func (d *BackupDeltaLocDataSource) Close() {

}

func (d *BackupDeltaLocDataSource) ApplyTombstones(
	_ context.Context,
	_ objectio.Blockid,
	_ []int64,
) ([]int64, error) {
	panic("Not Support ApplyTombstones")
}

func (d *BackupDeltaLocDataSource) SetOrderBy(orderby []*plan.OrderBySpec) {
	panic("Not Support order by")
}

func (d *BackupDeltaLocDataSource) GetOrderBy() []*plan.OrderBySpec {
	panic("Not Support order by")
}

func (d *BackupDeltaLocDataSource) SetFilterZM(zm objectio.ZoneMap) {
	panic("Not Support order by")
}

func ForeachTombstoneObject(
	onTombstone func(tombstone *objData) (next bool, err error),
	ds map[string]*objData,
) error {
	for _, d := range ds {
		if d.appendable && d.data[0].Vecs[0].Length() > 0 {
			if next, err := onTombstone(d); !next || err != nil {
				return err
			}
		}
	}

	return nil
}

func GetTombstonesByBlockId(
	bid objectio.Blockid,
	deleteMask *nulls.Nulls,
	scanOp func(func(tombstone *objData) (bool, error)) error,
) (err error) {

	onTombstone := func(oData *objData) (bool, error) {
		obj := oData.stats
		if !oData.appendable {
			return true, nil
		}
		if !obj.ZMIsEmpty() {
			objZM := obj.SortKeyZoneMap()
			if skip := !objZM.PrefixEq(bid[:]); skip {
				return true, nil
			}
		}

		for idx := 0; idx < int(obj.BlkCnt()); idx++ {
			rowids := vector.MustFixedCol[types.Rowid](oData.data[idx].Vecs[0])
			start, end := blockio.FindIntervalForBlock(rowids, &bid)
			if start == end {
				continue
			}
			deleteRows := make([]int64, 0, end-start)
			for i := start; i < end; i++ {
				row := rowids[i].GetRowOffset()
				deleteMask.Add(uint64(row))
				deleteRows = append(deleteRows, int64(i))
			}

			// Shrink the tombstone batch, Because the rowid in the tombstone is no longer needed after apply,
			// it cannot be written to disk
			oData.data[idx].Shrink(deleteRows, true)
		}
		return true, nil
	}

	err = scanOp(onTombstone)
	return err
}

func (d *BackupDeltaLocDataSource) GetTombstones(
	_ context.Context, bid objectio.Blockid,
) (deletedRows *nulls.Nulls, err error) {
	deletedRows = &nulls.Nulls{}
	deletedRows.InitWithSize(8192)

	scanOp := func(onTombstone func(tombstone *objData) (bool, error)) (err error) {
		return ForeachTombstoneObject(onTombstone, d.ds)
	}

	if err := GetTombstonesByBlockId(
		bid,
		deletedRows,
		scanOp); err != nil {
		return nil, err
	}
	return
}

func getCheckpointData(
	ctx context.Context,
	sid string,
	fs fileservice.FileService,
	location objectio.Location,
	version uint32,
) (*CheckpointData, error) {
	data := NewCheckpointData(sid, common.CheckpointAllocator)
	reader, err := blockio.NewObjectReader(sid, fs, location)
	if err != nil {
		return nil, err
	}
	err = data.readMetaBatch(ctx, version, reader, nil)
	if err != nil {
		return nil, err
	}
	err = data.readAll(ctx, version, fs)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func addObjectToObjectData(
	stats *objectio.ObjectStats,
	isABlk bool,
	row int, tid uint64,
	blockType objectio.DataMetaType,
	objectsData *map[string]*objData,
) {
	name := stats.ObjectName().String()
	if (*objectsData)[name] != nil {
		panic("object already exists")
	}
	object := &objData{
		stats:      stats,
		isChange:   false,
		appendable: isABlk,
		tid:        tid,
		dataType:   blockType,
		sortKey:    uint16(math.MaxUint16),
	}
	(*objectsData)[name] = object
	(*objectsData)[name].ckpRow = row
	return

}

func trimTombstoneData(
	ctx context.Context,
	fs fileservice.FileService,
	ts types.TS,
	objectsData *map[string]*objData,
) error {
	for name := range *objectsData {
		isChange := false
		if !(*objectsData)[name].appendable {
			continue
		}
		if (*objectsData)[name].dataType != objectio.SchemaTombstone {
			panic("Invalid data type")
		}

		location := (*objectsData)[name].stats.ObjectLocation()
		meta, err := objectio.FastLoadObjectMeta(ctx, &location, false, fs)
		if err != nil {
			return err
		}
		dataMeta := meta.MustDataMeta()
		if meta.MustDataMeta().BlockHeader().SortKey() != 0 {
			panic("sort key should be 0")
		}
		//if dataMeta.BlockHeader().ColumnCount() != 3 {
		//	panic("tombstone column count should be 3")
		//}
		for id := uint32(0); id < dataMeta.BlockCount(); id++ {
			var bat *batch.Batch
			var err error
			// As long as there is an aBlk to be deleted, isCkpChange must be set to true.
			commitTs := types.TS{}
			location.SetID(uint16(id))
			bat, _, err = blockio.LoadOneBlock(ctx, fs, location, objectio.SchemaData)
			if err != nil {
				return err
			}
			deleteRow := make([]int64, 0)
			for v := 0; v < bat.Vecs[0].Length(); v++ {
				err = commitTs.Unmarshal(bat.Vecs[len(bat.Vecs)-1].GetRawBytesAt(v))
				if err != nil {
					return err
				}
				if commitTs.Greater(&ts) {
					logutil.Debugf("delete row %v, commitTs %v, location %v",
						v, commitTs.ToString(), (*objectsData)[name].stats.ObjectLocation().String())
					isChange = true
				} else {
					deleteRow = append(deleteRow, int64(v))
				}
			}
			if len(deleteRow) != bat.Vecs[0].Length() {
				bat.Shrink(deleteRow, false)
			}
			bat = formatData(bat)
			(*objectsData)[name].data = make([]*batch.Batch, 0)
			(*objectsData)[name].data = append((*objectsData)[name].data, bat)
		}
		(*objectsData)[name].isChange = isChange
	}
	return nil
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

// Need to format the loaded batch, otherwise panic may occur when WriteBatch.
func formatData(data *batch.Batch) *batch.Batch {
	data.Attrs = make([]string, 0)
	for i := range data.Vecs {
		att := fmt.Sprintf("col_%d", i)
		data.Attrs = append(data.Attrs, att)
	}
	if data.Vecs[0].Length() > 0 {
		tmp := containers.ToTNBatch(data, common.CheckpointAllocator)
		data = containers.ToCNBatch(tmp)
	}
	return data
}

func LoadCheckpointEntriesFromKey(
	ctx context.Context,
	sid string,
	fs fileservice.FileService,
	location objectio.Location,
	version uint32,
	softDeletes *map[string]bool,
	baseTS *types.TS,
) ([]*objectio.BackupObject, *CheckpointData, error) {
	locations := make([]*objectio.BackupObject, 0)
	data, err := getCheckpointData(ctx, sid, fs, location, version)
	if err != nil {
		return nil, nil, err
	}

	locations = append(locations, &objectio.BackupObject{
		Location: location,
		NeedCopy: true,
	})

	for _, location = range data.locations {
		locations = append(locations, &objectio.BackupObject{
			Location: location,
			NeedCopy: true,
		})
	}
	for i := 0; i < data.bats[ObjectInfoIDX].Length(); i++ {
		var objectStats objectio.ObjectStats
		buf := data.bats[ObjectInfoIDX].GetVectorByName(ObjectAttr_ObjectStats).Get(i).([]byte)
		objectStats.UnMarshal(buf)
		deletedAt := data.bats[ObjectInfoIDX].GetVectorByName(EntryNode_DeleteAt).Get(i).(types.TS)
		createAt := data.bats[ObjectInfoIDX].GetVectorByName(EntryNode_CreateAt).Get(i).(types.TS)
		commitAt := data.bats[ObjectInfoIDX].GetVectorByName(txnbase.SnapshotAttr_CommitTS).Get(i).(types.TS)
		isAblk := data.bats[ObjectInfoIDX].GetVectorByName(ObjectAttr_State).Get(i).(bool)
		if objectStats.Extent().End() == 0 {
			// tn obj is in the batch too
			continue
		}

		if deletedAt.IsEmpty() && isAblk {
			// no flush, no need to copy
			continue
		}

		bo := &objectio.BackupObject{
			Location: objectStats.ObjectLocation(),
			CrateTS:  createAt,
			DropTS:   deletedAt,
		}
		if baseTS.IsEmpty() || (!baseTS.IsEmpty() &&
			(createAt.GreaterEq(baseTS) || commitAt.GreaterEq(baseTS))) {
			bo.NeedCopy = true
		}
		locations = append(locations, bo)
		if !deletedAt.IsEmpty() {
			if softDeletes != nil {
				if !(*softDeletes)[objectStats.ObjectName().String()] {
					(*softDeletes)[objectStats.ObjectName().String()] = true
				}
			}
		}
	}

	for i := 0; i < data.bats[TombstoneObjectInfoIDX].Length(); i++ {
		var objectStats objectio.ObjectStats
		buf := data.bats[TombstoneObjectInfoIDX].GetVectorByName(ObjectAttr_ObjectStats).Get(i).([]byte)
		objectStats.UnMarshal(buf)
		commitTS := data.bats[TombstoneObjectInfoIDX].GetVectorByName(txnbase.SnapshotAttr_CommitTS).Get(i).(types.TS)
		if objectStats.ObjectLocation().IsEmpty() {
			logutil.Infof("block %v deltaLoc is empty", objectStats.ObjectName().String())
			continue
		}
		bo := &objectio.BackupObject{
			Location: objectStats.ObjectLocation(),
			CrateTS:  commitTS,
		}
		if baseTS.IsEmpty() ||
			(!baseTS.IsEmpty() && commitTS.GreaterEq(baseTS)) {
			bo.NeedCopy = true
		}
		locations = append(locations, bo)
	}
	return locations, data, nil
}

func ReWriteCheckpointAndBlockFromKey(
	ctx context.Context,
	sid string,
	fs, dstFs fileservice.FileService,
	loc, tnLocation objectio.Location,
	version uint32, ts types.TS,
	softDeletes map[string]bool,
) (objectio.Location, objectio.Location, []string, error) {
	logutil.Info("[Start]", common.OperationField("ReWrite Checkpoint"),
		common.OperandField(loc.String()),
		common.OperandField(ts.ToString()))
	phaseNumber := 0
	var err error
	defer func() {
		if err != nil {
			logutil.Error("[DoneWithErr]", common.OperationField("ReWrite Checkpoint"),
				common.AnyField("error", err),
				common.AnyField("phase", phaseNumber),
			)
		}
	}()
	objectsData := make(map[string]*objData, 0)
	tombstonesData := make(map[string]*objData, 0)

	defer func() {
		for i := range objectsData {
			if objectsData[i] != nil && objectsData[i].data != nil {
				for z := range objectsData[i].data {
					for y := range objectsData[i].data[z].Vecs {
						objectsData[i].data[z].Vecs[y].Free(common.DebugAllocator)
					}
				}
			}
		}
	}()
	phaseNumber = 1
	// Load checkpoint
	data, err := getCheckpointData(ctx, sid, fs, loc, version)
	if err != nil {
		return nil, nil, nil, err
	}
	data.FormatData(common.CheckpointAllocator)
	defer data.Close()

	phaseNumber = 2
	// Analyze checkpoint to get the object file
	var files []string

	initData := func(
		od *map[string]*objData,
		idx uint16,
		dataType objectio.DataMetaType,
	) *containers.Batch {
		objInfoData := data.bats[idx]
		objInfoStats := objInfoData.GetVectorByName(ObjectAttr_ObjectStats)
		objInfoState := objInfoData.GetVectorByName(ObjectAttr_State)
		objInfoTid := objInfoData.GetVectorByName(SnapshotAttr_TID)
		objInfoDelete := objInfoData.GetVectorByName(EntryNode_DeleteAt)
		objInfoCreate := objInfoData.GetVectorByName(EntryNode_CreateAt)
		objInfoCommit := objInfoData.GetVectorByName(txnbase.SnapshotAttr_CommitTS)

		for i := 0; i < objInfoData.Length(); i++ {
			stats := objectio.NewObjectStats()
			stats.UnMarshal(objInfoStats.Get(i).([]byte))
			appendable := objInfoState.Get(i).(bool)
			deleteAt := objInfoDelete.Get(i).(types.TS)
			commitTS := objInfoCommit.Get(i).(types.TS)
			createAt := objInfoCreate.Get(i).(types.TS)
			tid := objInfoTid.Get(i).(uint64)
			if commitTS.Less(&ts) {
				panic(any(fmt.Sprintf("commitTs less than ts: %v-%v", commitTS.ToString(), ts.ToString())))
			}
			if deleteAt.IsEmpty() {
				continue
			}
			if createAt.GreaterEq(&ts) {
				panic(any(fmt.Sprintf("createAt equal to ts: %v-%v", createAt.ToString(), ts.ToString())))
			}
			addObjectToObjectData(stats, appendable, i, tid, dataType, od)
		}
		return objInfoData
	}

	objInfoData := initData(&objectsData, ObjectInfoIDX, objectio.SchemaData)
	tombstoneInfoData := initData(&tombstonesData, TombstoneObjectInfoIDX, objectio.SchemaTombstone)

	phaseNumber = 3

	// Trim tombstone files based on timestamp
	err = trimTombstoneData(ctx, fs, ts, &tombstonesData)
	if err != nil {
		return nil, nil, nil, err
	}

	backupPool := dbutils.MakeDefaultSmallPool("backup-vector-pool")
	defer backupPool.Destory()

	insertObjBatch := make(map[uint64][]*objData)

	phaseNumber = 4

	insertBatchFun := func(
		objsData map[string]*objData,
		initData func(*objData, *blockio.BlockWriter) error,
	) error {
		for _, objectData := range objsData {
			if insertObjBatch[objectData.tid] == nil {
				insertObjBatch[objectData.tid] = make([]*objData, 0)
			}
			if !objectData.appendable {
				insertObjBatch[objectData.tid] = append(insertObjBatch[objectData.tid], objectData)
			} else {
				objectName := objectData.stats.ObjectName()
				fileNum := uint16(1000) + objectName.Num()
				segment := objectName.SegmentId()
				name := objectio.BuildObjectName(&segment, fileNum)

				writer, err := blockio.NewBlockWriter(dstFs, name.String())
				if err != nil {
					return err
				}
				err = initData(objectData, writer)
				if err != nil {
					return err
				}
				var blockLocation objectio.Location
				// For the aBlock that needs to be retained,
				// the corresponding NBlock is generated and inserted into the corresponding batch.
				if len(objectData.data) > 2 {
					panic(any(fmt.Sprintf("dataBlocks len > 2: %v - %d",
						objectData.stats.ObjectLocation().String(), len(objectData.data))))
				}

				if objectData.data[0].Vecs[0].Length() == 0 {
					panic(any(fmt.Sprintf("dataBlocks[0] len == 0: %v", objectData.stats.ObjectLocation().String())))
				}

				sortData := containers.ToTNBatch(objectData.data[0], common.CheckpointAllocator)

				if objectData.sortKey != math.MaxUint16 {
					_, err = mergesort.SortBlockColumns(sortData.Vecs, int(objectData.sortKey), backupPool)
					if err != nil {
						return err
					}
				}
				objectData.data[0] = containers.ToCNBatch(sortData)
				_, err = writer.WriteBatch(objectData.data[0])
				if err != nil {
					return err
				}
				blocks, extent, err := writer.Sync(ctx)
				if err != nil {
					panic("sync error")
				}
				files = append(files, name.String())
				blockLocation = objectio.BuildLocation(name, extent, blocks[0].GetRows(), blocks[0].GetID())
				objectData.stats = &writer.GetObjectStats()[objectio.SchemaData]
				objectio.SetObjectStatsLocation(objectData.stats, blockLocation)
				logutil.Infof("delete object %v len blk %v is not  0, row is %v ,extent is %v, dataBlocks is %d", objectData.stats.String(), blockLocation.String(), blocks[0].GetRows(), extent.String(), len(objectData.data))
				insertObjBatch[objectData.tid] = append(insertObjBatch[objectData.tid], objectData)
			}
		}
		return nil
	}

	err = insertBatchFun(
		objectsData,
		func(oData *objData, writer *blockio.BlockWriter) error {
			ds := NewBackupDeltaLocDataSource(ctx, fs, ts, tombstonesData)
			location := oData.stats.ObjectLocation()
			name := oData.stats.ObjectName()
			l := objectio.BuildLocation(name, location.Extent(), 0, uint16(0))
			blk := objectio.BlockInfo{
				BlockID: *objectio.BuildObjectBlockid(name, uint16(0)),
				MetaLoc: objectio.ObjectLocation(l),
			}
			bat, sortKey, err := blockio.BlockDataReadBackup(ctx, sid, &blk, ds, ts, fs)
			if err != nil {
				return err
			}
			if bat.Vecs[0].Length() == 0 {
				logutil.Infof("%v block is empty , stat is %v,  ts %v, tid %d, block is empty",
					oData.stats.ObjectName().String(), oData.appendable, ts.ToString(), oData.tid)
				return nil
			}
			oData.sortKey = sortKey
			bat = formatData(bat)
			oData.data = make([]*batch.Batch, 0, 1)
			oData.data = append(oData.data, bat)
			oData.isChange = true
			if oData.sortKey != math.MaxUint16 {
				writer.SetPrimaryKey(oData.sortKey)
				logutil.Infof("SetPrimaryKeyWithType SchemaData %v", name)
			}
			result := batch.NewWithSize(len(oData.data[0].Vecs) - 2)
			for i := range result.Vecs {
				result.Vecs[i] = oData.data[0].Vecs[i]
			}
			oData.data[0] = result
			return nil
		})

	if err != nil {
		return nil, nil, nil, err
	}

	err = insertBatchFun(
		tombstonesData,
		func(oData *objData, writer *blockio.BlockWriter) error {
			writer.SetDataType(objectio.SchemaTombstone)
			writer.SetPrimaryKeyWithType(
				uint16(catalog.TombstonePrimaryKeyIdx),
				index.HBF,
				index.ObjectPrefixFn,
				index.BlockPrefixFn,
			)
			return nil
		})

	if err != nil {
		return nil, nil, nil, err
	}

	phaseNumber = 5

	if len(insertObjBatch) > 0 {
		objectInfoMeta := makeRespBatchFromSchema(checkpointDataSchemas_Curr[ObjectInfoIDX], common.CheckpointAllocator)
		tombstoneInfoMeta := makeRespBatchFromSchema(checkpointDataSchemas_Curr[TombstoneObjectInfoIDX], common.CheckpointAllocator)
		infoInsert := make(map[int]*objData, 0)
		infoInsertTombstone := make(map[int]*objData, 0)
		for tid := range insertObjBatch {
			for i := range insertObjBatch[tid] {
				obj := insertObjBatch[tid][i]
				if obj.dataType == objectio.SchemaData {
					if infoInsert[obj.ckpRow] != nil {
						panic("should not have info insert")
					}
					infoInsert[obj.ckpRow] = obj
				} else {
					if infoInsertTombstone[obj.ckpRow] != nil {
						panic("should not have info insert")
					}
					infoInsertTombstone[obj.ckpRow] = obj
				}
			}

		}
		for i := 0; i < objInfoData.Length(); i++ {
			appendValToBatch(objInfoData, objectInfoMeta, i)
			if infoInsert[i] != nil {
				if !infoInsert[i].appendable {
					row := objectInfoMeta.Length() - 1
					objectInfoMeta.GetVectorByName(EntryNode_DeleteAt).Update(row, types.TS{}, false)
				} else {
					appendValToBatch(objInfoData, objectInfoMeta, i)
					row := objectInfoMeta.Length() - 1
					objectInfoMeta.GetVectorByName(ObjectAttr_ObjectStats).Update(row, infoInsert[i].stats[:], false)
					objectInfoMeta.GetVectorByName(ObjectAttr_State).Update(row, false, false)
					objectInfoMeta.GetVectorByName(EntryNode_DeleteAt).Update(row, types.TS{}, false)
				}
			}
		}

		for i := 0; i < tombstoneInfoData.Length(); i++ {
			appendValToBatch(tombstoneInfoData, tombstoneInfoMeta, i)
			if infoInsertTombstone[i] != nil {
				if !infoInsertTombstone[i].appendable {
					row := tombstoneInfoMeta.Length() - 1
					tombstoneInfoMeta.GetVectorByName(EntryNode_DeleteAt).Update(row, types.TS{}, false)
				} else {
					appendValToBatch(tombstoneInfoData, tombstoneInfoMeta, i)
					row := tombstoneInfoMeta.Length() - 1
					tombstoneInfoMeta.GetVectorByName(ObjectAttr_ObjectStats).Update(row, infoInsertTombstone[i].stats[:], false)
					tombstoneInfoMeta.GetVectorByName(ObjectAttr_State).Update(row, false, false)
					tombstoneInfoMeta.GetVectorByName(EntryNode_DeleteAt).Update(row, types.TS{}, false)
				}
			}
		}
		data.bats[ObjectInfoIDX].Close()
		data.bats[ObjectInfoIDX] = objectInfoMeta
		data.bats[TombstoneObjectInfoIDX].Close()
		data.bats[TombstoneObjectInfoIDX] = tombstoneInfoMeta
		tableInsertOff := make(map[uint64]*tableOffset)
		tableTombstoneOff := make(map[uint64]*tableOffset)
		for i := 0; i < objectInfoMeta.Vecs[0].Length(); i++ {
			tid := objectInfoMeta.GetVectorByName(SnapshotAttr_TID).Get(i).(uint64)
			stats := objectio.NewObjectStats()
			stats.UnMarshal(objectInfoMeta.GetVectorByName(ObjectAttr_ObjectStats).Get(i).([]byte))
			logutil.Infof("object tid %d stats %v, row %d", tid, stats.ObjectName().String(), i)
			if tableInsertOff[tid] == nil {
				tableInsertOff[tid] = &tableOffset{
					offset: i,
					end:    i,
				}
			}
			tableInsertOff[tid].end += 1
		}

		for i := 0; i < tombstoneInfoMeta.Vecs[0].Length(); i++ {
			tid := tombstoneInfoMeta.GetVectorByName(SnapshotAttr_TID).Get(i).(uint64)
			stats := objectio.NewObjectStats()
			stats.UnMarshal(tombstoneInfoMeta.GetVectorByName(ObjectAttr_ObjectStats).Get(i).([]byte))
			logutil.Infof("tombstone tid %d stats %v, row %d", tid, stats.ObjectName().String(), i)
			if tableTombstoneOff[tid] == nil {
				tableTombstoneOff[tid] = &tableOffset{
					offset: i,
					end:    i,
				}
			}
			tableTombstoneOff[tid].end += 1
		}

		for tid, table := range tableInsertOff {
			logutil.Infof("object tid %d offset %d end %d", tid, table.offset, table.end)
			data.UpdateObjectInsertMeta(tid, int32(table.offset), int32(table.end))
		}
		for tid, table := range tableTombstoneOff {
			logutil.Infof("tombstone tid %d offset %d end %d", tid, table.offset, table.end)
			data.UpdateTombstoneInsertMeta(tid, int32(table.offset), int32(table.end))
		}
	}
	cnLocation, dnLocation, checkpointFiles, err := data.WriteTo(dstFs, DefaultCheckpointBlockRows, DefaultCheckpointSize)
	if err != nil {
		return nil, nil, nil, err
	}
	logutil.Info("[Done]",
		common.AnyField("checkpoint", cnLocation.String()),
		common.OperationField("ReWrite Checkpoint"),
		common.AnyField("new object", checkpointFiles))
	loc = cnLocation
	tnLocation = dnLocation
	files = append(files, checkpointFiles...)
	files = append(files, cnLocation.Name().String())
	return loc, tnLocation, files, nil
}
