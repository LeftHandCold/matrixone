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

package v2

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"math"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type objData struct {
	stats      *objectio.ObjectStats
	blocks     map[uint16]*blockData
	data       []*batch.Batch
	sortKey    uint16
	infoRow    []int
	infoDel    []int
	infoTNRow  []int
	tid        uint64
	delete     bool
	appendable bool
	dataType   objectio.DataMetaType
	isChange   bool
}

type blockData struct {
	num  uint16
	data *batch.Batch
}

type iBlocks struct {
	insertBlocks []*insertBlock
}

type iObjects struct {
	rowObjects []*insertObjects
}

type insertBlock struct {
	blockId   objectio.Blockid
	location  objectio.Location
	deleteRow int
	apply     bool
	data      *blockData
}

type insertObjects struct {
	location objectio.Location
	apply    bool
	obj      *objData
}

type tableOffset struct {
	offset int
	end    int
}

func getCheckpointData(
	ctx context.Context,
	sid string,
	fs fileservice.FileService,
	location objectio.Location,
	version uint32,
) (*logtail.CheckpointData, error) {
	data := logtail.NewCheckpointData(sid, common.CheckpointAllocator)
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
	isABlk, isDelete bool,
	row int, tid uint64, blockType objectio.DataMetaType,
	objectsData *map[string]*objData,
) {
	name := stats.ObjectName().String()
	if (*objectsData)[name] == nil {
		object := &objData{
			stats:      stats,
			delete:     isDelete,
			isChange:   false,
			appendable: isABlk,
			tid:        tid,
			dataType:   blockType,
			sortKey:    uint16(math.MaxUint16),
		}
		if isABlk {
			object.blocks = make(map[uint16]*blockData)
			object.blocks[0] = &blockData{
				num: 0,
			}
		}
		(*objectsData)[name] = object
		if isDelete {
			(*objectsData)[name].infoDel = []int{row}
		} else {
			(*objectsData)[name].infoRow = []int{row}
		}
		return
	}

	if isDelete {
		(*objectsData)[name].delete = true
		(*objectsData)[name].infoDel = append((*objectsData)[name].infoDel, row)
	} else {
		(*objectsData)[name].infoRow = append((*objectsData)[name].infoRow, row)
	}

}

func trimObjectsData(
	ctx context.Context,
	fs fileservice.FileService,
	ts types.TS,
	objectsData *map[string]*objData,
) (bool, error) {
	isCkpChange := false
	for name := range *objectsData {
		isChange := false
		if !(*objectsData)[name].appendable {
			continue
		}
		if (*objectsData)[name] != nil {
			if !(*objectsData)[name].delete {
				logutil.Infof(fmt.Sprintf("object %s is not a delete batch", name))
				continue
			}
			if len((*objectsData)[name].blocks) == 0 {
				// data23 is nil, is no tombstone
				panic(fmt.Sprintf("object %s has no blocks", name))
			}
		}

		for id, block := range (*objectsData)[name].blocks {
			if !block.isABlock {
				panic(fmt.Sprintf("object %s block %d is not a block", name, id))
			}
			var bat *batch.Batch
			var err error
			commitTs := types.TS{}
			if block.blockType == objectio.SchemaTombstone {
				bat, err = blockio.LoadOneBlock(ctx, fs, block.location, objectio.SchemaTombstone)
				if err != nil {
					return isCkpChange, err
				}
				deleteRow := make([]int64, 0)
				for v := 0; v < bat.Vecs[0].Length(); v++ {
					err = commitTs.Unmarshal(bat.Vecs[len(bat.Vecs)-3].GetRawBytesAt(v))
					if err != nil {
						return isCkpChange, err
					}
					if commitTs.Greater(&ts) {
						logutil.Debugf("delete row %v, commitTs %v, location %v",
							v, commitTs.ToString(), block.location.String())
						isChange = true
						isCkpChange = true
					} else {
						deleteRow = append(deleteRow, int64(v))
					}
				}
				if len(deleteRow) != bat.Vecs[0].Length() {
					bat.Shrink(deleteRow, false)
				}
			} else {
				// As long as there is an aBlk to be deleted, isCkpChange must be set to true.
				isCkpChange = true
				meta, err := objectio.FastLoadObjectMeta(ctx, &block.location, false, fs)
				if err != nil {
					return isCkpChange, err
				}
				sortKey := uint16(math.MaxUint16)
				if meta.MustDataMeta().BlockHeader().Appendable() {
					sortKey = meta.MustDataMeta().BlockHeader().SortKey()
				}
				bat, err = blockio.LoadOneBlock(ctx, fs, block.location, objectio.SchemaData)
				if err != nil {
					return isCkpChange, err
				}
				for v := 0; v < bat.Vecs[0].Length(); v++ {
					err = commitTs.Unmarshal(bat.Vecs[len(bat.Vecs)-2].GetRawBytesAt(v))
					if err != nil {
						return isCkpChange, err
					}
					if commitTs.Greater(&ts) {
						logtail.windowCNBatch(bat, 0, uint64(v))
						logutil.Debugf("blkCommitTs %v ts %v , block is %v",
							commitTs.ToString(), ts.ToString(), block.location.String())
						isChange = true
						break
					}
				}
				(*objectsData)[name].blocks[id].sortKey = sortKey
			}
			bat = formatData(bat)
			(*objectsData)[name].blocks[id].data = bat
		}
		(*objectsData)[name].isChange = isChange
	}
	return isCkpChange, nil
}

func updateBlockMeta(blkMeta, blkMetaTxn *containers.Batch, row int, blockID types.Blockid, location objectio.Location, sort bool) {
	blkMeta.GetVectorByName(catalog2.PhyAddrColumnName).Update(
		row,
		objectio.HackBlockid2Rowid(&blockID),
		false)
	blkMeta.GetVectorByName(catalog.BlockMeta_ID).Update(
		row,
		blockID,
		false)
	blkMeta.GetVectorByName(catalog.BlockMeta_EntryState).Update(
		row,
		false,
		false)
	blkMeta.GetVectorByName(catalog.BlockMeta_Sorted).Update(
		row,
		sort,
		false)
	blkMeta.GetVectorByName(catalog.BlockMeta_SegmentID).Update(
		row,
		*blockID.Segment(),
		false)
	blkMeta.GetVectorByName(catalog.BlockMeta_MetaLoc).Update(
		row,
		[]byte(location),
		false)
	blkMeta.GetVectorByName(catalog.BlockMeta_DeltaLoc).Update(
		row,
		nil,
		true)
	blkMetaTxn.GetVectorByName(catalog.BlockMeta_MetaLoc).Update(
		row,
		[]byte(location),
		false)
	blkMetaTxn.GetVectorByName(catalog.BlockMeta_DeltaLoc).Update(
		row,
		nil,
		true)

	if !sort {
		logutil.Infof("block %v is not sorted", blockID.String())
	}
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
	if data.Vecs[0].Length() > 0 {
		data.Attrs = make([]string, 0)
		for i := range data.Vecs {
			att := fmt.Sprintf("col_%d", i)
			data.Attrs = append(data.Attrs, att)
		}
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
) ([]*objectio.BackupObject, *logtail.CheckpointData, error) {
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
	for i := 0; i < data.bats[logtail.ObjectInfoIDX].Length(); i++ {
		var objectStats objectio.ObjectStats
		buf := data.bats[logtail.ObjectInfoIDX].GetVectorByName(logtail.ObjectAttr_ObjectStats).Get(i).([]byte)
		objectStats.UnMarshal(buf)
		deletedAt := data.bats[logtail.ObjectInfoIDX].GetVectorByName(logtail.EntryNode_DeleteAt).Get(i).(types.TS)
		createAt := data.bats[logtail.ObjectInfoIDX].GetVectorByName(logtail.EntryNode_CreateAt).Get(i).(types.TS)
		commitAt := data.bats[logtail.ObjectInfoIDX].GetVectorByName(txnbase.SnapshotAttr_CommitTS).Get(i).(types.TS)
		isAblk := data.bats[logtail.ObjectInfoIDX].GetVectorByName(logtail.ObjectAttr_State).Get(i).(bool)
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

	for i := 0; i < data.bats[logtail.TombstoneObjectInfoIDX].Length(); i++ {
		var objectStats objectio.ObjectStats
		buf := data.bats[logtail.ObjectInfoIDX].GetVectorByName(logtail.ObjectAttr_ObjectStats).Get(i).([]byte)
		objectStats.UnMarshal(buf)
		commitTS := data.bats[logtail.TombstoneObjectInfoIDX].GetVectorByName(txnbase.SnapshotAttr_CommitTS).Get(i).(types.TS)
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
	isCkpChange := false

	objInfoData := data.bats[logtail.ObjectInfoIDX]
	objInfoStats := objInfoData.GetVectorByName(logtail.ObjectAttr_ObjectStats)
	objInfoState := objInfoData.GetVectorByName(logtail.ObjectAttr_State)
	objInfoTid := objInfoData.GetVectorByName(logtail.SnapshotAttr_TID)
	objInfoDelete := objInfoData.GetVectorByName(logtail.EntryNode_DeleteAt)
	objInfoCommit := objInfoData.GetVectorByName(txnbase.SnapshotAttr_CommitTS)

	for i := 0; i < objInfoData.Length(); i++ {
		stats := objectio.NewObjectStats()
		stats.UnMarshal(objInfoStats.Get(i).([]byte))
		appendable := objInfoState.Get(i).(bool)
		deleteAt := objInfoDelete.Get(i).(types.TS)
		commitTS := objInfoCommit.Get(i).(types.TS)
		tid := objInfoTid.Get(i).(uint64)
		if commitTS.Less(&ts) {
			panic(any(fmt.Sprintf("commitTs less than ts: %v-%v", commitTS.ToString(), ts.ToString())))
		}

		if appendable && deleteAt.IsEmpty() {
			logutil.Infof("block %v deleteAt is empty", stats.ObjectName().String())
			continue
		}
		addObjectToObjectData(stats, appendable, !deleteAt.IsEmpty(), i, tid, objectio.SchemaData, &objectsData)
	}

	blkMetaInsert := data.bats[logtail.TombstoneObjectInfoIDX]
	blkMetaInsertStats := blkMetaInsert.GetVectorByName(logtail.ObjectAttr_ObjectStats)
	blkMetaInsertEntryState := blkMetaInsert.GetVectorByName(catalog.BlockMeta_EntryState)
	blkMetaInsertDelete := blkMetaInsert.GetVectorByName(logtail.EntryNode_DeleteAt)
	blkMetaInsertCommit := blkMetaInsert.GetVectorByName(txnbase.SnapshotAttr_CommitTS)
	blkMetaInsertTid := blkMetaInsert.GetVectorByName(logtail.SnapshotAttr_TID)

	for i := 0; i < blkMetaInsert.Length(); i++ {
		stats := objectio.NewObjectStats()
		stats.UnMarshal(blkMetaInsertStats.Get(i).([]byte))
		deleteAt := blkMetaInsertDelete.Get(i).(types.TS)
		commitTS := blkMetaInsertCommit.Get(i).(types.TS)
		appendable := blkMetaInsertEntryState.Get(i).(bool)
		tid := blkMetaInsertTid.Get(i).(uint64)
		if commitTS.Less(&ts) {
			panic(any(fmt.Sprintf("commitTs less than ts: %v-%v", commitTS.ToString(), ts.ToString())))
		}
		if appendable && deleteAt.IsEmpty() {
			logutil.Infof("block %v deleteAt is empty", stats.ObjectName().String())
			continue
		}
		addObjectToObjectData(stats, appendable, !deleteAt.IsEmpty(), i, tid, objectio.SchemaTombstone, &objectsData)
	}

	phaseNumber = 3
	// Trim object files based on timestamp
	isCkpChange, err = trimObjectsData(ctx, fs, ts, &objectsData)
	if err != nil {
		return nil, nil, nil, err
	}
	if !isCkpChange {
		return loc, tnLocation, files, nil
	}

	backupPool := dbutils.MakeDefaultSmallPool("backup-vector-pool")
	defer backupPool.Destory()

	insertBatch := make(map[uint64]*iBlocks)
	insertObjBatch := make(map[uint64]*iObjects)

	phaseNumber = 4
	// Rewrite object file
	for fileName, objectData := range objectsData {
		if !objectData.isChange && !objectData.delete {
			continue
		}
		dataBlocks := make([]*blockData, 0)
		var blocks []objectio.BlockObject
		var extent objectio.Extent
		for _, block := range objectData.blocks {
			dataBlocks = append(dataBlocks, block)
		}
		sort.Slice(dataBlocks, func(i, j int) bool {
			return dataBlocks[i].num < dataBlocks[j].num
		})

		if objectData.isChange &&
			(!objectData.delete || (objectData.blocks[0] != nil &&
				objectData.blocks[0].blockType == objectio.SchemaTombstone)) {
			// Rewrite the insert block/delete block file.
			objectData.delete = false
			writer, err := blockio.NewBlockWriter(dstFs, fileName)
			if err != nil {
				return nil, nil, nil, err
			}
			for _, block := range dataBlocks {
				if block.sortKey != math.MaxUint16 {
					writer.SetPrimaryKey(block.sortKey)
				}
				if block.blockType == objectio.SchemaData {
					panic(any("block type is data"))
				} else if block.blockType == objectio.SchemaTombstone {
					_, err = writer.WriteBatch(block.data)
					if err != nil {
						return nil, nil, nil, err
					}
				}
			}

			blocks, extent, err = writer.Sync(ctx)
			if err != nil {
				if !moerr.IsMoErrCode(err, moerr.ErrFileAlreadyExists) {
					return nil, nil, nil, err
				}
				err = fs.Delete(ctx, fileName)
				if err != nil {
					return nil, nil, nil, err
				}
				blocks, extent, err = writer.Sync(ctx)
				if err != nil {
					return nil, nil, nil, err
				}
			}
		}

		if objectData.delete &&
			objectData.blocks[0] != nil &&
			objectData.blocks[0].blockType != objectio.SchemaTombstone {
			var blockLocation objectio.Location
			if !objectData.appendable {
				// Case of merge nBlock
				for _, dt := range dataBlocks {
					if insertBatch[dataBlocks[0].tid] == nil {
						insertBatch[dataBlocks[0].tid] = &iBlocks{
							insertBlocks: make([]*insertBlock, 0),
						}
					}
					ib := &insertBlock{
						apply:     false,
						deleteRow: dt.deleteRow[len(dt.deleteRow)-1],
						data:      dt,
					}
					insertBatch[dataBlocks[0].tid].insertBlocks = append(insertBatch[dataBlocks[0].tid].insertBlocks, ib)
				}
			} else {
				// For the aBlock that needs to be retained,
				// the corresponding NBlock is generated and inserted into the corresponding batch.
				if len(dataBlocks) > 2 {
					panic(any(fmt.Sprintf("dataBlocks len > 2: %v - %d", dataBlocks[0].location.String(), len(dataBlocks))))
				}
				//if objectData.blocks[0].tombstone != nil {
				//	applyDelete(dataBlocks[0].data, objectData.data23[0].tombstone.data, dataBlocks[0].blockId.String())
				//}
				sortData := containers.ToTNBatch(dataBlocks[0].data, common.CheckpointAllocator)
				if dataBlocks[0].sortKey != math.MaxUint16 {
					_, err = mergesort.SortBlockColumns(sortData.Vecs, int(dataBlocks[0].sortKey), backupPool)
					if err != nil {
						return nil, nil, nil, err
					}
				}
				dataBlocks[0].data = containers.ToCNBatch(sortData)
				result := batch.NewWithSize(len(dataBlocks[0].data.Vecs) - 3)
				for i := range result.Vecs {
					result.Vecs[i] = dataBlocks[0].data.Vecs[i]
				}
				dataBlocks[0].data = result
				fileNum := uint16(1000) + dataBlocks[0].location.Name().Num()
				segment := dataBlocks[0].location.Name().SegmentId()
				name := objectio.BuildObjectName(&segment, fileNum)

				writer, err := blockio.NewBlockWriter(dstFs, name.String())
				if err != nil {
					return nil, nil, nil, err
				}
				if dataBlocks[0].sortKey != math.MaxUint16 {
					writer.SetPrimaryKey(dataBlocks[0].sortKey)
				}
				_, err = writer.WriteBatch(dataBlocks[0].data)
				if err != nil {
					return nil, nil, nil, err
				}
				blocks, extent, err = writer.Sync(ctx)
				if err != nil {
					panic("sync error")
				}
				files = append(files, name.String())
				blockLocation = objectio.BuildLocation(name, extent, blocks[0].GetRows(), blocks[0].GetID())
				if insertBatch[dataBlocks[0].tid] == nil {
					insertBatch[dataBlocks[0].tid] = &iBlocks{
						insertBlocks: make([]*insertBlock, 0),
					}
				}
				ib := &insertBlock{
					location: blockLocation,
					blockId:  *objectio.BuildObjectBlockid(name, blocks[0].GetID()),
					apply:    false,
				}
				if len(dataBlocks[0].deleteRow) > 0 {
					ib.deleteRow = dataBlocks[0].deleteRow[0]
				}
				insertBatch[dataBlocks[0].tid].insertBlocks = append(insertBatch[dataBlocks[0].tid].insertBlocks, ib)
				objectData.stats = &writer.GetObjectStats()[objectio.SchemaData]
				//if objectData.obj != nil {
				//	objectData.obj.stats = &writer.GetObjectStats()[objectio.SchemaData]
				//}
			}
			if insertObjBatch[objectData.tid] == nil {
				insertObjBatch[objectData.tid] = &iObjects{
					rowObjects: make([]*insertObjects, 0),
				}
			}
			io := &insertObjects{
				location: blockLocation,
				apply:    false,
				obj:      objectData,
			}
			insertObjBatch[objectData.tid].rowObjects = append(insertObjBatch[objectData.tid].rowObjects, io)
			//if objectData.obj != nil {
			//	obj := objectData.obj
			//	if insertObjBatch[obj.tid] == nil {
			//		insertObjBatch[obj.tid] = &iObjects{
			//			rowObjects: make([]*insertObjects, 0),
			//		}
			//	}
			//	io := &insertObjects{
			//		location: blockLocation,
			//		apply:    false,
			//		obj:      obj,
			//	}
			//	insertObjBatch[obj.tid].rowObjects = append(insertObjBatch[obj.tid].rowObjects, io)
			//}
		} else {
			if objectData.delete && objectData.blocks == nil {
				if !objectData.appendable {
					// Case of merge nBlock
					if insertObjBatch[objectData.tid] == nil {
						insertObjBatch[objectData.tid] = &iObjects{
							rowObjects: make([]*insertObjects, 0),
						}
					}
					io := &insertObjects{
						apply: false,
						obj:   objectData,
					}
					insertObjBatch[objectData.tid].rowObjects = append(insertObjBatch[objectData.tid].rowObjects, io)
				} else {
					sortData := containers.ToTNBatch(objectData.data[0], common.CheckpointAllocator)
					if objectData.sortKey != math.MaxUint16 {
						_, err = mergesort.SortBlockColumns(sortData.Vecs, int(objectData.sortKey), backupPool)
						if err != nil {
							return nil, nil, nil, err
						}
					}
					objectData.data[0] = containers.ToCNBatch(sortData)
					result := batch.NewWithSize(len(objectData.data[0].Vecs) - 3)
					for i := range result.Vecs {
						result.Vecs[i] = objectData.data[0].Vecs[i]
					}
					objectData.data[0] = result
					fileNum := uint16(1000) + objectData.stats.ObjectName().Num()
					segment := objectData.stats.ObjectName().SegmentId()
					name := objectio.BuildObjectName(&segment, fileNum)

					writer, err := blockio.NewBlockWriter(dstFs, name.String())
					if err != nil {
						return nil, nil, nil, err
					}
					if objectData.sortKey != math.MaxUint16 {
						writer.SetPrimaryKey(objectData.sortKey)
					}
					_, err = writer.WriteBatch(objectData.data[0])
					if err != nil {
						return nil, nil, nil, err
					}
					blocks, extent, err = writer.Sync(ctx)
					if err != nil {
						panic("sync error")
					}
					files = append(files, name.String())
					blockLocation := objectio.BuildLocation(name, extent, blocks[0].GetRows(), blocks[0].GetID())
					obj := objectData
					if insertObjBatch[obj.tid] == nil {
						insertObjBatch[obj.tid] = &iObjects{
							rowObjects: make([]*insertObjects, 0),
						}
					}
					obj.stats = &writer.GetObjectStats()[objectio.SchemaData]
					objectio.SetObjectStatsObjectName(obj.stats, blockLocation.Name())
					io := &insertObjects{
						location: blockLocation,
						apply:    false,
						obj:      obj,
					}
					insertObjBatch[obj.tid].rowObjects = append(insertObjBatch[obj.tid].rowObjects, io)
				}
			}

			for i := range dataBlocks {
				blockLocation := dataBlocks[i].location
				if objectData.isChange {
					blockLocation = objectio.BuildLocation(objectData.stats.ObjectName(), extent, blocks[uint16(i)].GetRows(), dataBlocks[i].num)
				}
				for _, insertRow := range dataBlocks[i].insertRow {
					if dataBlocks[uint16(i)].blockType == objectio.SchemaTombstone {
						data.bats[logtail.TombstoneObjectInfoIDX].GetVectorByName(catalog.ObjectMeta_ObjectStats).Update(
							insertRow,
							[]byte(blockLocation),
							false)
					}
				}
				for _, deleteRow := range dataBlocks[uint16(i)].deleteRow {
					if dataBlocks[uint16(i)].blockType == objectio.SchemaTombstone {
						data23.bats[BLKMetaInsertIDX].GetVectorByName(catalog.BlockMeta_DeltaLoc).Update(
							deleteRow,
							[]byte(blockLocation),
							false)
						data23.bats[BLKMetaInsertTxnIDX].GetVectorByName(catalog.BlockMeta_DeltaLoc).Update(
							deleteRow,
							[]byte(blockLocation),
							false)
					}
				}
			}
		}
	}

	phaseNumber = 5
	// Transfer the object file that needs to be deleted to insert
	// if len(insertBatch) > 0 {
	// 	blkMeta := makeRespBatchFromSchema(checkpointDataSchemas_Curr[BLKMetaInsertIDX], common.CheckpointAllocator)
	// 	blkMetaTxn := makeRespBatchFromSchema(checkpointDataSchemas_Curr[BLKMetaInsertTxnIDX], common.CheckpointAllocator)
	// 	for i := 0; i < blkMetaInsert.Length(); i++ {
	// 		tid := data23.bats[BLKMetaInsertTxnIDX].GetVectorByName(SnapshotAttr_TID).Get(i).(uint64)
	// 		appendValToBatch(data23.bats[BLKMetaInsertIDX], blkMeta, i)
	// 		appendValToBatch(data23.bats[BLKMetaInsertTxnIDX], blkMetaTxn, i)
	// 		if insertBatch[tid] != nil {
	// 			for b, blk := range insertBatch[tid].insertBlocks {
	// 				if blk.apply {
	// 					continue
	// 				}
	// 				if insertBatch[tid].insertBlocks[b].data23 == nil {

	// 				} else {
	// 					insertBatch[tid].insertBlocks[b].apply = true

	// 					row := blkMeta.Vecs[0].Length() - 1
	// 					if !blk.location.IsEmpty() {
	// 						sort := true
	// 						if insertBatch[tid].insertBlocks[b].data23 != nil &&
	// 							insertBatch[tid].insertBlocks[b].data23.appendable &&
	// 							insertBatch[tid].insertBlocks[b].data23.sortKey == math.MaxUint16 {
	// 							sort = false
	// 						}
	// 						updateBlockMeta(blkMeta, blkMetaTxn, row,
	// 							insertBatch[tid].insertBlocks[b].blockId,
	// 							insertBatch[tid].insertBlocks[b].location,
	// 							sort)
	// 					}
	// 				}
	// 			}
	// 		}
	// 	}

	// 	for tid := range insertBatch {
	// 		for b := range insertBatch[tid].insertBlocks {
	// 			if insertBatch[tid].insertBlocks[b].apply {
	// 				continue
	// 			}
	// 			if insertBatch[tid] != nil && !insertBatch[tid].insertBlocks[b].apply {
	// 				insertBatch[tid].insertBlocks[b].apply = true
	// 				if insertBatch[tid].insertBlocks[b].data23 == nil {

	// 				} else {
	// 					i := blkMeta.Vecs[0].Length() - 1
	// 					if !insertBatch[tid].insertBlocks[b].location.IsEmpty() {
	// 						sort := true
	// 						if insertBatch[tid].insertBlocks[b].data23 != nil &&
	// 							insertBatch[tid].insertBlocks[b].data23.appendable &&
	// 							insertBatch[tid].insertBlocks[b].data23.sortKey == math.MaxUint16 {
	// 							sort = false
	// 						}
	// 						updateBlockMeta(blkMeta, blkMetaTxn, i,
	// 							insertBatch[tid].insertBlocks[b].blockId,
	// 							insertBatch[tid].insertBlocks[b].location,
	// 							sort)
	// 					}
	// 				}
	// 			}
	// 		}
	// 	}

	// 	for i := range insertBatch {
	// 		for _, block := range insertBatch[i].insertBlocks {
	// 			if block.data23 != nil {
	// 				for _, cnRow := range block.data23.deleteRow {
	// 					if block.data23.appendable {
	// 						data23.bats[BLKMetaInsertIDX].Delete(cnRow)
	// 						data23.bats[BLKMetaInsertTxnIDX].Delete(cnRow)
	// 					}
	// 				}
	// 			}
	// 		}
	// 	}

	// 	data23.bats[BLKMetaInsertIDX].Compact()
	// 	data23.bats[BLKMetaInsertTxnIDX].Compact()
	// 	tableInsertOff := make(map[uint64]*tableOffset)
	// 	for i := 0; i < blkMetaTxn.Vecs[0].Length(); i++ {
	// 		tid := blkMetaTxn.GetVectorByName(SnapshotAttr_TID).Get(i).(uint64)
	// 		if tableInsertOff[tid] == nil {
	// 			tableInsertOff[tid] = &tableOffset{
	// 				offset: i,
	// 				end:    i,
	// 			}
	// 		}
	// 		tableInsertOff[tid].end += 1
	// 	}

	// 	for tid, table := range tableInsertOff {
	// 		data23.UpdateBlockInsertBlkMeta(tid, int32(table.offset), int32(table.end))
	// 	}
	// 	data23.bats[BLKMetaInsertIDX].Close()
	// 	data23.bats[BLKMetaInsertTxnIDX].Close()
	// 	data23.bats[BLKMetaInsertIDX] = blkMeta
	// 	data23.bats[BLKMetaInsertTxnIDX] = blkMetaTxn
	// }

	phaseNumber = 6
	if len(insertObjBatch) > 0 {
		deleteRow := make([]int, 0)
		objectInfoMeta := logtail.makeRespBatchFromSchema(logtail.checkpointDataSchemas_Curr[logtail.ObjectInfoIDX], common.CheckpointAllocator)
		infoInsert := make(map[int]*objData, 0)
		infoDelete := make(map[int]bool, 0)
		for tid := range insertObjBatch {
			for i := range insertObjBatch[tid].rowObjects {
				if insertObjBatch[tid].rowObjects[i].apply {
					continue
				}
				if !insertObjBatch[tid].rowObjects[i].location.IsEmpty() {
					obj := insertObjBatch[tid].rowObjects[i].obj
					if infoInsert[obj.infoDel[0]] != nil {
						panic("should not have info insert")
					}
					objectio.SetObjectStatsExtent(insertObjBatch[tid].rowObjects[i].obj.stats, insertObjBatch[tid].rowObjects[i].location.Extent())
					objectio.SetObjectStatsObjectName(insertObjBatch[tid].rowObjects[i].obj.stats, insertObjBatch[tid].rowObjects[i].location.Name())
					infoInsert[obj.infoDel[0]] = insertObjBatch[tid].rowObjects[i].obj
					// if len(obj.infoTNRow) > 0 {
					// 	data23.bats[TNObjectInfoIDX].Delete(obj.infoTNRow[0])
					// }
				} else {
					if infoDelete[insertObjBatch[tid].rowObjects[i].obj.infoDel[0]] {
						panic("should not have info delete")
					}
					infoDelete[insertObjBatch[tid].rowObjects[i].obj.infoDel[0]] = true
				}
			}

		}
		for i := 0; i < objInfoData.Length(); i++ {
			appendValToBatch(objInfoData, objectInfoMeta, i)
			if infoInsert[i] != nil && infoDelete[i] {
				panic("info should not have info delete")
			}
			if infoInsert[i] != nil {
				appendValToBatch(objInfoData, objectInfoMeta, i)
				row := objectInfoMeta.Length() - 1
				objectInfoMeta.GetVectorByName(logtail.ObjectAttr_ObjectStats).Update(row, infoInsert[i].stats[:], false)
				objectInfoMeta.GetVectorByName(logtail.ObjectAttr_State).Update(row, false, false)
				objectInfoMeta.GetVectorByName(logtail.EntryNode_DeleteAt).Update(row, types.TS{}, false)
			}

			if infoDelete[i] {
				deleteRow = append(deleteRow, objectInfoMeta.Length()-1)
			}
		}
		for i := range deleteRow {
			objectInfoMeta.Delete(deleteRow[i])
		}
		// data23.bats[TNObjectInfoIDX].Compact()
		objectInfoMeta.Compact()
		data.bats[logtail.ObjectInfoIDX].Close()
		data.bats[logtail.ObjectInfoIDX] = objectInfoMeta
		tableInsertOff := make(map[uint64]*tableOffset)
		for i := 0; i < objectInfoMeta.Vecs[0].Length(); i++ {
			tid := objectInfoMeta.GetVectorByName(logtail.SnapshotAttr_TID).Get(i).(uint64)
			if tableInsertOff[tid] == nil {
				tableInsertOff[tid] = &tableOffset{
					offset: i,
					end:    i,
				}
			}
			tableInsertOff[tid].end += 1
		}

		for tid, table := range tableInsertOff {
			data.UpdateDataObjectMeta(tid, int32(table.offset), int32(table.end))
		}
	}
	cnLocation, dnLocation, checkpointFiles, err := data.WriteTo(dstFs, logtail.DefaultCheckpointBlockRows, logtail.DefaultCheckpointSize)
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
