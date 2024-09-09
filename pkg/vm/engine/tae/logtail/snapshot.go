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
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"

	"go.uber.org/zap"

	catalog2 "github.com/matrixorigin/matrixone/pkg/catalog"
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
)

const (
	SnapshotTypeIdx types.Enum = iota
	SnapshotTypeCluster
	SnapshotTypeAccount
)

// mo_snapshot's schema
const (
	ColSnapshotId uint16 = iota
	ColSName
	ColTS
	ColLevel
	ColAccountName
	ColDatabaseName
	ColTableName
	ColObjId
)

const (
	PitrUnitYear   = "y"
	PitrUnitMonth  = "mo"
	PitrUnitDay    = "d"
	PitrUnitHour   = "h"
	PitrUnitMinute = "m"
)

const (
	PitrLevelCluster  = "cluster"
	PitrLevelAccount  = "account"
	PitrLevelDatabase = "database"
	PitrLevelTable    = "table"
)

// pitr's schema
const (
	ColPitrId uint16 = iota
	ColPitrName
	ColPitrCreateAccount
	ColPitrCreateTime
	ColPitrModifiedTime
	ColPitrLevel
	ColPitrAccountId
	ColPitrAccountName
	ColPitrDatabaseName
	ColPitrTableName
	ColPitrObjId
	ColPitrLength
	ColPitrUnit
)

var (
	objectInfoSchemaAttr = []string{
		catalog.ObjectAttr_ObjectStats,
		catalog.EntryNode_CreateAt,
		catalog.EntryNode_DeleteAt,
		catalog2.BlockMeta_DeltaLoc,
		SnapshotAttr_TID,
	}
	objectInfoSchemaTypes = []types.Type{
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_TS, types.MaxVarcharLen, 0),
		types.New(types.T_TS, types.MaxVarcharLen, 0),
		types.New(types.T_varchar, 5000, 0),
		types.New(types.T_uint64, 0, 0),
	}

	tableInfoSchemaAttr = []string{
		catalog2.SystemColAttr_AccID,
		catalog2.SystemRelAttr_DBID,
		SnapshotAttr_TID,
		catalog2.SystemRelAttr_CreateAt,
		catalog.EntryNode_DeleteAt,
	}

	tableInfoSchemaTypes = []types.Type{
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_TS, types.MaxVarcharLen, 0),
		types.New(types.T_TS, types.MaxVarcharLen, 0),
	}

	snapshotSchemaTypes = []types.Type{
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_int64, 0, 0),
		types.New(types.T_enum, 0, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_uint64, 0, 0),
	}
)

type objectInfo struct {
	stats    objectio.ObjectStats
	createAt types.TS
	deleteAt types.TS
}

type tableInfo struct {
	accountID uint32
	dbID      uint64
	tid       uint64
	createAt  types.TS
	deleteAt  types.TS
}

type PitrInfo struct {
	account  map[uint32]types.TS
	database map[uint64]types.TS
	tables   map[uint64]types.TS
}

func (p *PitrInfo) GetAccount() map[uint32]types.TS {
	return p.account
}

func (p *PitrInfo) GetDatabase() map[uint64]types.TS {
	return p.database
}

func (p *PitrInfo) GetTables() map[uint64]types.TS {
	return p.tables
}

type SnapshotMeta struct {
	sync.RWMutex

	// all objects&tombstones in the mo_snapshots table, because there
	// will be multiple mo_snapshots, so here is a map, the key is tid.
	objects    map[uint64]map[objectio.Segmentid]*objectInfo
	tombstones map[uint64]map[objectio.Segmentid]*objectInfo

	pitr struct {
		tid        uint64
		objects    map[objectio.Segmentid]*objectInfo
		tombstones map[objectio.Segmentid]*objectInfo
	}

	// tables records all the table information of mo, the key is account id,
	// and the map is the mapping of table id and table information.
	//
	// tables is used to facilitate the use of an account id to obtain
	// all table information under the account.
	tables map[uint32]map[uint64]*tableInfo

	// acctIndexes records all the index information of mo, the key is
	// account id, and the value is the index information.
	acctIndexes map[uint64]*tableInfo

	// pkIndexes records all the index information of mo, the key is
	// the mo_table pk, and the value is the index information.
	pkIndexes map[string][]*tableInfo

	// tides is used to consume the object and tombstone of the checkpoint.
	tides map[uint64]struct{}
}

func NewSnapshotMeta() *SnapshotMeta {
	meta := &SnapshotMeta{
		objects:     make(map[uint64]map[objectio.Segmentid]*objectInfo),
		tombstones:  make(map[uint64]map[objectio.Segmentid]*objectInfo),
		tables:      make(map[uint32]map[uint64]*tableInfo),
		acctIndexes: make(map[uint64]*tableInfo),
		tides:       make(map[uint64]struct{}),
		pkIndexes:   make(map[string][]*tableInfo),
	}
	meta.pitr.objects = make(map[objectio.Segmentid]*objectInfo)
	meta.pitr.tombstones = make(map[objectio.Segmentid]*objectInfo)
	return meta
}

func copyObjectsLocked(
	objects map[uint64]map[objectio.Segmentid]*objectInfo,
) map[uint64]map[objectio.Segmentid]*objectInfo {
	newMap := make(map[uint64]map[objectio.Segmentid]*objectInfo)
	for k, v := range objects {
		newMap[k] = make(map[objectio.Segmentid]*objectInfo)
		for kk, vv := range v {
			newMap[k][kk] = vv
		}
	}
	return newMap
}

func (sm *SnapshotMeta) copyTablesLocked() map[uint32]map[uint64]*tableInfo {
	tables := make(map[uint32]map[uint64]*tableInfo)
	for k, v := range sm.tables {
		tables[k] = make(map[uint64]*tableInfo)
		for kk, vv := range v {
			tables[k][kk] = vv
		}
	}
	return tables
}

func isMoTable(tid uint64) bool {
	return tid == catalog2.MO_TABLES_ID
}

func isMoDB(tid uint64) bool {
	return tid == catalog2.MO_DATABASE_ID
}

func isMoCol(tid uint64) bool {
	return tid == catalog2.MO_COLUMNS_ID
}

type tombstone struct {
	pk types.Tuple
	ts types.TS
}

func (sm *SnapshotMeta) updateTableInfo(
	ctx context.Context,
	fs fileservice.FileService,
	data *CheckpointData,
) error {
	var objects map[uint64]map[objectio.Segmentid]*objectInfo
	var tombstones map[uint64]map[objectio.Segmentid]*objectInfo
	objects = make(map[uint64]map[objectio.Segmentid]*objectInfo, 1)
	tombstones = make(map[uint64]map[objectio.Segmentid]*objectInfo, 1)
	objects[catalog2.MO_TABLES_ID] = make(map[objectio.Segmentid]*objectInfo)
	tombstones[catalog2.MO_TABLES_ID] = make(map[objectio.Segmentid]*objectInfo)
	collector := func(
		objects *map[uint64]map[objectio.Segmentid]*objectInfo,
		_ *map[objectio.Segmentid]*objectInfo,
		tid uint64,
		stats objectio.ObjectStats,
		createTS types.TS, deleteTS types.TS,
	) {
		if !isMoTable(tid) {
			return
		}
		if !stats.GetAppendable() {
			// mo_table only consumes appendable object
			return
		}
		id := stats.ObjectName().SegmentId()
		if deleteTS.IsEmpty() {
			return
		}
		moTable := (*objects)[tid]
		if moTable[id] != nil {
			// mo_table only consumes  appendable object that is
			// soft deleted once, otherwise panic
			panic(fmt.Sprintf("duplicate object %v", id.String()))
		}
		moTable[id] = &objectInfo{
			stats:    stats,
			createAt: createTS,
			deleteAt: deleteTS,
		}
	}
	collectObjects(&objects, nil, data.GetObjectBatchs(), collector)
	collectObjects(&tombstones, nil, data.GetTombstoneObjectBatchs(), collector)
	tObjects := objects[catalog2.MO_TABLES_ID]
	tTombstones := tombstones[catalog2.MO_TABLES_ID]
	for _, info := range tObjects {
		if info.stats.BlkCnt() != 1 {
			panic(fmt.Sprintf("mo_table object %v blk cnt %v",
				info.stats.ObjectName(), info.stats.BlkCnt()))
		}
		objectBat, _, err := blockio.LoadOneBlock(
			ctx, fs, info.stats.ObjectLocation(), objectio.SchemaData)
		if err != nil {
			return err
		}
		// 0 is table id
		// 1 is table name
		// 11 is account id
		// len(objectBat.Vecs)-1 is commit ts
		ids := vector.MustFixedColWithTypeCheck[uint64](objectBat.Vecs[0])
		nameVarlena := vector.MustFixedColWithTypeCheck[types.Varlena](objectBat.Vecs[1])
		nameArea := objectBat.Vecs[1].GetArea()
		dbs := vector.MustFixedColWithTypeCheck[uint64](objectBat.Vecs[3])
		accounts := vector.MustFixedColWithTypeCheck[uint32](objectBat.Vecs[11])
		creates := vector.MustFixedColWithTypeCheck[types.TS](objectBat.Vecs[len(objectBat.Vecs)-1])
		for i := 0; i < len(ids); i++ {
			name := string(nameVarlena[i].GetByteSlice(nameArea))
			tid := ids[i]
			account := accounts[i]
			db := dbs[i]
			createAt := creates[i]
			tuple, _, _, err := types.DecodeTuple(
				objectBat.Vecs[len(objectBat.Vecs)-3].GetRawBytesAt(i))
			if err != nil {
				return err
			}
			pk := tuple.String()
			if name == catalog2.MO_SNAPSHOTS {
				sm.tides[tid] = struct{}{}
				logutil.Info("[UpdateSnapTable]",
					zap.Uint64("tid", tid),
					zap.Uint32("account id", account),
					zap.String("create at", createAt.ToString()))
			}
			if name == catalog2.MO_PITR {
				if sm.pitr.tid > 0 && sm.pitr.tid != tid {
					panic(fmt.Sprintf("pitr table %v is not unique", tid))
				}
				sm.pitr.tid = tid
			}
			if sm.tables[account] == nil {
				sm.tables[account] = make(map[uint64]*tableInfo)
			}
			table := sm.tables[account][tid]
			if table != nil {
				if table.createAt.Greater(&createAt) {
					panic(fmt.Sprintf("table %v create at %v is greater than %v",
						tid, table.createAt.ToString(), createAt.ToString()))
				}
				sm.pkIndexes[pk] = append(sm.pkIndexes[pk], table)
				continue
			}
			table = &tableInfo{
				accountID: account,
				dbID:      db,
				tid:       tid,
				createAt:  createAt,
			}
			sm.tables[account][tid] = table
			sm.acctIndexes[tid] = table
			if sm.pkIndexes[pk] == nil {
				sm.pkIndexes[pk] = make([]*tableInfo, 0)
			}
			sm.pkIndexes[pk] = append(sm.pkIndexes[pk], table)
		}
	}

	deletes := make([]tombstone, 0)
	for _, info := range tTombstones {
		if info.stats.BlkCnt() != 1 {
			panic(fmt.Sprintf("mo_table tombstone %v blk cnt %v",
				info.stats.ObjectName(), info.stats.BlkCnt()))
		}
		objectBat, _, err := blockio.LoadOneBlock(
			ctx, fs, info.stats.ObjectLocation(), objectio.SchemaData)
		if err != nil {
			return err
		}

		commitTsVec := vector.MustFixedColWithTypeCheck[types.TS](objectBat.Vecs[len(objectBat.Vecs)-1])
		for i := 0; i < len(commitTsVec); i++ {
			pk, _, _, _ := types.DecodeTuple(objectBat.Vecs[1].GetRawBytesAt(i))
			commitTs := commitTsVec[i]
			deletes = append(deletes, tombstone{
				pk: pk,
				ts: commitTs,
			})
		}
	}
	sort.Slice(deletes, func(i, j int) bool {
		ts2 := deletes[j].ts
		return deletes[i].ts.Less(&ts2)
	})

	for _, del := range deletes {
		pk := del.pk.String()
		if sm.pkIndexes[pk] == nil {
			continue
		}
		if len(sm.pkIndexes[pk]) == 0 {
			panic(fmt.Sprintf("delete table %v not found", del.pk.SQLStrings(nil)))
		}
		table := sm.pkIndexes[pk][0]
		if !table.deleteAt.IsEmpty() && table.deleteAt.Greater(&del.ts) {
			panic(fmt.Sprintf("table %v delete at %v is greater than %v", table.tid, table.deleteAt, del.ts))
		}
		table.deleteAt = del.ts
		sm.pkIndexes[pk] = sm.pkIndexes[pk][1:]
		if sm.acctIndexes[table.tid] == nil {
			//In the upgraded cluster, because the inc checkpoint is consumed halfway,
			// there may be no record of the create table entry, only the delete entry
			continue
		}
		sm.acctIndexes[table.tid] = table
		sm.tables[table.accountID][table.tid] = table
	}
	return nil
}

func collectObjects(
	objects *map[uint64]map[objectio.Segmentid]*objectInfo,
	objects2 *map[objectio.Segmentid]*objectInfo,
	ins *containers.Batch,
	collector func(
		*map[uint64]map[objectio.Segmentid]*objectInfo,
		*map[objectio.Segmentid]*objectInfo,
		uint64,
		objectio.ObjectStats,
		types.TS, types.TS,
	),
) {
	insDeleteTSs := vector.MustFixedColWithTypeCheck[types.TS](
		ins.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector())
	insCreateTSs := vector.MustFixedColWithTypeCheck[types.TS](
		ins.GetVectorByName(catalog.EntryNode_CreateAt).GetDownstreamVector())
	insTableIDs := vector.MustFixedColWithTypeCheck[uint64](
		ins.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector())
	insStats := ins.GetVectorByName(catalog.ObjectAttr_ObjectStats).GetDownstreamVector()

	for i := 0; i < ins.Length(); i++ {
		table := insTableIDs[i]
		deleteTS := insDeleteTSs[i]
		createTS := insCreateTSs[i]
		objectStats := (objectio.ObjectStats)(insStats.GetBytesAt(i))
		collector(objects, objects2, table, objectStats, createTS, deleteTS)
	}
}

func (sm *SnapshotMeta) Update(
	ctx context.Context,
	fs fileservice.FileService,
	data *CheckpointData,
) (*SnapshotMeta, error) {
	sm.Lock()
	defer sm.Unlock()
	now := time.Now()
	defer func() {
		logutil.Infof("[UpdateSnapshot] cost %v", time.Since(now))
	}()
	err := sm.updateTableInfo(ctx, fs, data)
	if err != nil {
		logutil.Errorf("[UpdateSnapshot] updateTableInfo failed %v", err)
		return sm, err
	}
	if len(sm.tides) == 0 {
		return sm, nil
	}

	collector := func(
		objects *map[uint64]map[objectio.Segmentid]*objectInfo,
		objects2 *map[objectio.Segmentid]*objectInfo,
		tid uint64,
		stats objectio.ObjectStats,
		createTS types.TS, deleteTS types.TS,
	) {
		mapFun := func(
			objects map[objectio.Segmentid]*objectInfo,
		) {
			if objects == nil {
				objects = make(map[objectio.Segmentid]*objectInfo)
			}
			id := stats.ObjectName().SegmentId()
			if objects[id] == nil {
				if !deleteTS.IsEmpty() {
					return
				}
				objects[id] = &objectInfo{
					stats:    stats,
					createAt: createTS,
				}
				logutil.Info("[UpdateSnapshot] Add object",
					zap.Uint64("table id", tid),
					zap.String("object name", id.String()),
					zap.String("create at", createTS.ToString()))

				return
			}
			if deleteTS.IsEmpty() {
				panic(any("deleteTS is empty"))
			}
			logutil.Info("[UpdateSnapshot] Delete object",
				zap.Uint64("table id", tid),
				zap.String("object name", id.String()),
				zap.String("delete at", deleteTS.ToString()))

			delete(objects, id)
		}

		if tid == sm.pitr.tid {
			mapFun(*objects2)
			logutil.Infof("[UpdateSnapshot] sm.pitr.objects %d", len(*objects2))
		}
		if _, ok := sm.tides[tid]; !ok {
			return
		}
		if (*objects)[tid] == nil {
			(*objects)[tid] = make(map[objectio.Segmentid]*objectInfo)
		}
		mapFun((*objects)[tid])
	}
	collectObjects(&sm.objects, &sm.pitr.objects, data.GetObjectBatchs(), collector)
	logutil.Infof("[UpdateSnapshot] sm.pitr.objects %d", len(sm.pitr.objects))
	collectObjects(&sm.tombstones, &sm.pitr.tombstones, data.GetTombstoneObjectBatchs(), collector)
	return nil, nil
}

func NewSnapshotDataSource(
	ctx context.Context,
	fs fileservice.FileService,
	ts types.TS,
	stats []objectio.ObjectStats,
) *BackupDeltaLocDataSource {
	ds := make(map[string]*objData)
	return &BackupDeltaLocDataSource{
		ctx:        ctx,
		fs:         fs,
		ts:         ts,
		ds:         ds,
		tombstones: stats,
		needShrink: false,
	}
}

func (sm *SnapshotMeta) GetSnapshot(
	ctx context.Context,
	sid string,
	fs fileservice.FileService,
	mp *mpool.MPool,
) (map[uint32]containers.Vector, error) {
	now := time.Now()
	defer func() {
		logutil.Infof("[GetSnapshot] cost %v", time.Since(now))
	}()
	sm.RLock()
	objects := copyObjectsLocked(sm.objects)
	tombstones := copyObjectsLocked(sm.tombstones)
	tables := sm.copyTablesLocked()
	sm.RUnlock()
	snapshotList := make(map[uint32]containers.Vector)
	idxes := []uint16{ColTS, ColLevel, ColObjId}
	colTypes := []types.Type{
		snapshotSchemaTypes[ColTS],
		snapshotSchemaTypes[ColLevel],
		snapshotSchemaTypes[ColObjId],
	}
	for tid, objectMap := range objects {
		tombstonesStats := make([]objectio.ObjectStats, 0)
		for ttid, tombstoneMap := range tombstones {
			if ttid != tid {
				continue
			}
			for _, object := range tombstoneMap {
				tombstonesStats = append(tombstonesStats, object.stats)
			}
			break
		}
		checkpointTS := types.BuildTS(time.Now().UTC().UnixNano(), 0)
		ds := NewSnapshotDataSource(ctx, fs, checkpointTS, tombstonesStats)
		for _, object := range objectMap {
			location := object.stats.ObjectLocation()
			name := object.stats.ObjectName()
			for i := uint32(0); i < object.stats.BlkCnt(); i++ {
				loc := objectio.BuildLocation(name, location.Extent(), 0, uint16(i))
				blk := objectio.BlockInfo{
					BlockID: *objectio.BuildObjectBlockid(name, uint16(i)),
					MetaLoc: objectio.ObjectLocation(loc),
				}

				var vp engine.VectorPool
				buildBatch := func() *batch.Batch {
					result := batch.NewWithSize(len(colTypes))
					for i, typ := range colTypes {
						if vp == nil {
							result.Vecs[i] = vector.NewVec(typ)
						} else {
							result.Vecs[i] = vp.GetVector(typ)
						}
					}
					return result
				}

				bat := buildBatch()
				defer bat.Clean(mp)
				bat, _, err := blockio.BlockDataReadBackup(ctx, sid, &blk, ds, idxes, types.TS{}, fs)
				if err != nil {
					return nil, err
				}
				tsList := vector.MustFixedColWithTypeCheck[int64](bat.Vecs[0])
				typeList := vector.MustFixedColWithTypeCheck[types.Enum](bat.Vecs[1])
				acctList := vector.MustFixedColWithTypeCheck[uint64](bat.Vecs[2])
				for r := 0; r < bat.Vecs[0].Length(); r++ {
					ts := tsList[r]
					snapTs := types.BuildTS(ts, 0)
					acct := acctList[r]
					snapshotType := typeList[r]
					if snapshotType == SnapshotTypeCluster {
						for account := range tables {
							if snapshotList[account] == nil {
								snapshotList[account] = containers.MakeVector(types.T_TS.ToType(), mp)
							}
							err = vector.AppendFixed[types.TS](snapshotList[account].GetDownstreamVector(), snapTs, false, mp)
							if err != nil {
								return nil, err
							}
							// TODO: info to debug
							logutil.Info("[GetSnapshot] cluster snapshot",
								common.OperationField(snapTs.ToString()))
						}
						continue
					}
					id := uint32(acct)
					if snapshotList[id] == nil {
						snapshotList[id] = containers.MakeVector(types.T_TS.ToType(), mp)
					}
					// TODO: info to debug
					logutil.Info("[GetSnapshot] snapshot",
						zap.Uint32("account", id),
						zap.String("snap ts", snapTs.ToString()))
					err = vector.AppendFixed[types.TS](snapshotList[id].GetDownstreamVector(), snapTs, false, mp)
					if err != nil {
						return nil, err
					}
				}
			}
		}
	}
	for i := range snapshotList {
		snapshotList[i].GetDownstreamVector().InplaceSort()
	}
	return snapshotList, nil
}

func AddDate(t time.Time, year, month, day int) time.Time {
	targetDate := t.AddDate(year, month, -t.Day()+1)
	targetDay := targetDate.AddDate(0, 1, -1).Day()
	if targetDay > t.Day() {
		targetDay = t.Day()
	}
	targetDate = targetDate.AddDate(0, 0, targetDay-1+day)
	return targetDate
}

func (sm *SnapshotMeta) GetPITR(
	ctx context.Context,
	sid string,
	gcTime time.Time,
	fs fileservice.FileService,
	mp *mpool.MPool,
) (*PitrInfo, error) {
	idxes := []uint16{ColPitrLevel, ColPitrObjId, ColPitrLength, ColPitrUnit}
	colTypes := []types.Type{
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_int64, 0, 0),
		types.New(types.T_int64, 0, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
	}
	tombstonesStats := make([]objectio.ObjectStats, 0)
	for _, tombstone := range sm.pitr.tombstones {
		tombstonesStats = append(tombstonesStats, tombstone.stats)
	}
	checkpointTS := types.BuildTS(time.Now().UTC().UnixNano(), 0)
	ds := NewSnapshotDataSource(ctx, fs, checkpointTS, tombstonesStats)
	pitr := &PitrInfo{
		account:  make(map[uint32]types.TS),
		database: make(map[uint64]types.TS),
		tables:   make(map[uint64]types.TS),
	}
	for _, object := range sm.pitr.objects {
		location := object.stats.ObjectLocation()
		name := object.stats.ObjectName()
		for i := uint32(0); i < object.stats.BlkCnt(); i++ {
			loc := objectio.BuildLocation(name, location.Extent(), 0, uint16(i))
			blk := objectio.BlockInfo{
				BlockID: *objectio.BuildObjectBlockid(name, uint16(i)),
				MetaLoc: objectio.ObjectLocation(loc),
			}

			var vp engine.VectorPool
			buildBatch := func() *batch.Batch {
				result := batch.NewWithSize(len(colTypes))
				for i, typ := range colTypes {
					if vp == nil {
						result.Vecs[i] = vector.NewVec(typ)
					} else {
						result.Vecs[i] = vp.GetVector(typ)
					}
				}
				return result
			}

			bat := buildBatch()
			defer bat.Clean(mp)
			bat, _, err := blockio.BlockDataReadBackup(ctx, sid, &blk, ds, idxes, types.TS{}, fs)
			if err != nil {
				return nil, err
			}
			objIDList := vector.MustFixedColWithTypeCheck[uint64](bat.Vecs[1])
			lengList := vector.MustFixedColWithTypeCheck[uint8](bat.Vecs[2])
			for r := 0; r < bat.Vecs[0].Length(); r++ {
				length := lengList[r]
				leng := int(length)
				unit := bat.Vecs[3].GetStringAt(r)
				var ts time.Time
				if unit == PitrUnitYear {
					ts = AddDate(gcTime, 1-leng, 0, 0)
				} else if unit == PitrUnitMonth {
					ts = AddDate(gcTime, 0, 1-leng, 0)
				} else if unit == PitrUnitDay {
					ts = gcTime.AddDate(0, 0, 1-leng)
				} else if unit == PitrUnitHour {
					ts = gcTime.Add(-time.Duration(leng) * time.Hour)
				} else if unit == PitrUnitMinute {
					ts = gcTime.Add(-time.Duration(leng) * time.Minute)
				}
				pitrTs := types.BuildTS(ts.UnixNano(), 0)
				account := objIDList[i]
				level := bat.Vecs[0].GetStringAt(r)
				if level == PitrLevelAccount {
					id := uint32(account)
					p := pitr.account[id]
					if !p.IsEmpty() {
						panic("account duplicate pitr ")
					}
					pitr.account[id] = pitrTs
				} else if level == PitrLevelDatabase {
					id := uint64(account)
					p := pitr.database[id]
					if !p.IsEmpty() {
						panic("db duplicate pitr ")
					}
					pitr.database[id] = pitrTs
				} else if level == PitrLevelTable {
					id := uint64(account)
					p := pitr.tables[id]
					if !p.IsEmpty() {
						panic("table duplicate pitr ")
					}
					pitr.tables[id] = pitrTs
				}
				// TODO: info to debug
				logutil.Info("[GetPITR] pitr",
					zap.String("level", level),
					zap.Uint64("id", account),
					zap.String("snap ts", pitrTs.ToString()))
			}
		}
	}
	return pitr, nil
}

func (sm *SnapshotMeta) SetTid(tid uint64) {
	sm.tides[tid] = struct{}{}
}

func (sm *SnapshotMeta) SaveMeta(name string, fs fileservice.FileService) (uint32, error) {
	if len(sm.objects) == 0 {
		return 0, nil
	}
	bat := containers.NewBatch()
	for i, attr := range objectInfoSchemaAttr {
		bat.AddVector(attr, containers.MakeVector(objectInfoSchemaTypes[i], common.DebugAllocator))
	}
	deltaBat := containers.NewBatch()
	for i, attr := range objectInfoSchemaAttr {
		deltaBat.AddVector(attr, containers.MakeVector(objectInfoSchemaTypes[i], common.DebugAllocator))
	}
	appendBatForMap := func(
		bat *containers.Batch,
		tid uint64,
		objectMap map[objectio.Segmentid]*objectInfo) {
		for _, entry := range objectMap {
			vector.AppendBytes(
				bat.GetVectorByName(catalog.ObjectAttr_ObjectStats).GetDownstreamVector(),
				entry.stats[:], false, common.DebugAllocator)
			vector.AppendFixed[types.TS](
				bat.GetVectorByName(catalog.EntryNode_CreateAt).GetDownstreamVector(),
				entry.createAt, false, common.DebugAllocator)
			vector.AppendFixed[types.TS](
				bat.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector(),
				entry.deleteAt, false, common.DebugAllocator)
			vector.AppendFixed[uint64](
				bat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector(),
				tid, false, common.DebugAllocator)
		}
	}
	appendBat := func(
		bat *containers.Batch,
		objects map[uint64]map[objectio.Segmentid]*objectInfo) {
		for tid, objectMap := range objects {
			appendBatForMap(bat, tid, objectMap)
		}
	}
	appendBat(bat, sm.objects)
	appendBatForMap(bat, sm.pitr.tid, sm.pitr.objects)
	appendBat(deltaBat, sm.tombstones)
	appendBatForMap(bat, sm.pitr.tid, sm.pitr.tombstones)
	defer bat.Close()
	defer deltaBat.Close()
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterGC, name, fs)
	if err != nil {
		return 0, err
	}
	if _, err = writer.WriteWithoutSeqnum(containers.ToCNBatch(bat)); err != nil {
		return 0, err
	}
	if deltaBat.Length() > 0 {
		if _, err = writer.WriteWithoutSeqnum(containers.ToCNBatch(deltaBat)); err != nil {
			return 0, err
		}
	}

	_, err = writer.WriteEnd(context.Background())
	if err != nil {
		return 0, err
	}
	size := writer.GetObjectStats()[0].OriginSize()
	return size, err
}

func (sm *SnapshotMeta) SaveTableInfo(name string, fs fileservice.FileService) (uint32, error) {
	if len(sm.tables) == 0 {
		return 0, nil
	}
	bat := containers.NewBatch()
	snapTableBat := containers.NewBatch()
	pitrTableBat := containers.NewBatch()
	for i, attr := range tableInfoSchemaAttr {
		bat.AddVector(attr, containers.MakeVector(tableInfoSchemaTypes[i], common.DebugAllocator))
		snapTableBat.AddVector(attr, containers.MakeVector(tableInfoSchemaTypes[i], common.DebugAllocator))
		pitrTableBat.AddVector(attr, containers.MakeVector(tableInfoSchemaTypes[i], common.DebugAllocator))
	}
	for _, entry := range sm.tables {
		for _, table := range entry {
			vector.AppendFixed[uint32](
				bat.GetVectorByName(catalog2.SystemColAttr_AccID).GetDownstreamVector(),
				table.accountID, false, common.DebugAllocator)
			vector.AppendFixed[uint64](
				bat.GetVectorByName(catalog2.SystemRelAttr_DBID).GetDownstreamVector(),
				table.dbID, false, common.DebugAllocator)
			vector.AppendFixed[uint64](
				bat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector(),
				table.tid, false, common.DebugAllocator)
			vector.AppendFixed[types.TS](
				bat.GetVectorByName(catalog2.SystemRelAttr_CreateAt).GetDownstreamVector(),
				table.createAt, false, common.DebugAllocator)
			vector.AppendFixed[types.TS](
				bat.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector(),
				table.deleteAt, false, common.DebugAllocator)

			if _, ok := sm.tides[table.tid]; ok {
				vector.AppendFixed[uint32](
					snapTableBat.GetVectorByName(catalog2.SystemColAttr_AccID).GetDownstreamVector(),
					table.accountID, false, common.DebugAllocator)
				vector.AppendFixed[uint64](
					snapTableBat.GetVectorByName(catalog2.SystemRelAttr_DBID).GetDownstreamVector(),
					table.dbID, false, common.DebugAllocator)
				vector.AppendFixed[uint64](
					snapTableBat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector(),
					table.tid, false, common.DebugAllocator)
				vector.AppendFixed[types.TS](
					snapTableBat.GetVectorByName(catalog2.SystemRelAttr_CreateAt).GetDownstreamVector(),
					table.createAt, false, common.DebugAllocator)
				vector.AppendFixed[types.TS](
					snapTableBat.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector(),
					table.deleteAt, false, common.DebugAllocator)
			}

			if table.tid == sm.pitr.tid {
				vector.AppendFixed[uint32](
					pitrTableBat.GetVectorByName(catalog2.SystemColAttr_AccID).GetDownstreamVector(),
					table.accountID, false, common.DebugAllocator)
				vector.AppendFixed[uint64](
					pitrTableBat.GetVectorByName(catalog2.SystemRelAttr_DBID).GetDownstreamVector(),
					table.dbID, false, common.DebugAllocator)
				vector.AppendFixed[uint64](
					pitrTableBat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector(),
					table.tid, false, common.DebugAllocator)
				vector.AppendFixed[types.TS](
					pitrTableBat.GetVectorByName(catalog2.SystemRelAttr_CreateAt).GetDownstreamVector(),
					table.createAt, false, common.DebugAllocator)
				vector.AppendFixed[types.TS](
					pitrTableBat.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector(),
					table.deleteAt, false, common.DebugAllocator)
			}
		}
	}
	defer bat.Close()
	defer snapTableBat.Close()
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterGC, name, fs)
	if err != nil {
		return 0, err
	}
	if _, err = writer.WriteWithoutSeqnum(containers.ToCNBatch(bat)); err != nil {
		return 0, err
	}
	if _, err = writer.WriteWithoutSeqnum(containers.ToCNBatch(snapTableBat)); err != nil {
		return 0, err
	}

	_, err = writer.WriteEnd(context.Background())
	if err != nil {
		return 0, err
	}
	size := writer.GetObjectStats()[0].OriginSize()
	return size, err
}

func (sm *SnapshotMeta) RebuildTableInfo(ins *containers.Batch) {
	sm.Lock()
	defer sm.Unlock()
	insTIDs := vector.MustFixedColWithTypeCheck[uint64](ins.GetVectorByName(catalog.SnapshotAttr_TID).GetDownstreamVector())
	insAccIDs := vector.MustFixedColWithTypeCheck[uint32](ins.GetVectorByName(catalog2.SystemColAttr_AccID).GetDownstreamVector())
	insDBIDs := vector.MustFixedColWithTypeCheck[uint64](ins.GetVectorByName(catalog2.SystemRelAttr_DBID).GetDownstreamVector())
	insCreateTSs := vector.MustFixedColWithTypeCheck[types.TS](ins.GetVectorByName(catalog2.SystemRelAttr_CreateAt).GetDownstreamVector())
	insDeleteTSs := vector.MustFixedColWithTypeCheck[types.TS](ins.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector())
	for i := 0; i < ins.Length(); i++ {
		tid := insTIDs[i]
		dbid := insDBIDs[i]
		accid := insAccIDs[i]
		createTS := insCreateTSs[i]
		deleteTS := insDeleteTSs[i]
		if sm.tables[accid] == nil {
			sm.tables[accid] = make(map[uint64]*tableInfo)
		}
		table := &tableInfo{
			tid:       tid,
			dbID:      dbid,
			accountID: accid,
			createAt:  createTS,
			deleteAt:  deleteTS,
		}
		sm.tables[accid][tid] = table
		sm.acctIndexes[tid] = table
	}
}

func (sm *SnapshotMeta) RebuildTid(ins *containers.Batch) {
	sm.Lock()
	defer sm.Unlock()
	insTIDs := vector.MustFixedColWithTypeCheck[uint64](ins.GetVectorByName(catalog.SnapshotAttr_TID).GetDownstreamVector())
	accIDs := vector.MustFixedColWithTypeCheck[uint32](ins.GetVectorByName(catalog2.SystemColAttr_AccID).GetDownstreamVector())
	if ins.Length() < 1 {
		logutil.Warnf("RebuildTid unexpected length %d", ins.Length())
		return
	}
	logutil.Infof("RebuildTid tid %d", insTIDs[0])
	for i := 0; i < ins.Length(); i++ {
		tid := insTIDs[i]
		accid := accIDs[i]
		if _, ok := sm.tides[tid]; !ok {
			sm.tides[tid] = struct{}{}
			logutil.Info("[RebuildSnapshotTid]", zap.Uint64("tid", tid), zap.Uint32("account id", accid))
		}
	}
}

func (sm *SnapshotMeta) RebuildPitr(ins *containers.Batch) {
	sm.Lock()
	defer sm.Unlock()
	insTIDs := vector.MustFixedColWithTypeCheck[uint64](ins.GetVectorByName(catalog.SnapshotAttr_TID).GetDownstreamVector())
	if ins.Length() < 1 {
		logutil.Warnf("RebuildPitr unexpected length %d", ins.Length())
		return
	}
	logutil.Infof("RebuildPitr tid %d", insTIDs[0])
	for i := 0; i < ins.Length(); i++ {
		tid := insTIDs[i]
		sm.pitr.tid = tid
	}
}

func (sm *SnapshotMeta) Rebuild(ins *containers.Batch, objects *map[uint64]map[objectio.Segmentid]*objectInfo) {
	sm.Lock()
	defer sm.Unlock()
	insCreateTSs := vector.MustFixedColWithTypeCheck[types.TS](ins.GetVectorByName(catalog.EntryNode_CreateAt).GetDownstreamVector())
	insTides := vector.MustFixedColWithTypeCheck[uint64](ins.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector())
	for i := 0; i < ins.Length(); i++ {
		var objectStats objectio.ObjectStats
		buf := ins.GetVectorByName(catalog.ObjectAttr_ObjectStats).Get(i).([]byte)
		objectStats.UnMarshal(buf)
		createTS := insCreateTSs[i]
		tid := insTides[i]
		if _, ok := sm.tides[tid]; !ok {
			sm.tides[tid] = struct{}{}
			logutil.Info("[RebuildSnapTable]", zap.Uint64("tid", tid))
		}
		if (*objects)[tid] == nil {
			(*objects)[tid] = make(map[objectio.Segmentid]*objectInfo)
		}
		if (*objects)[tid][objectStats.ObjectName().SegmentId()] == nil {
			(*objects)[tid][objectStats.ObjectName().SegmentId()] = &objectInfo{
				stats:    objectStats,
				createAt: createTS,
			}
			logutil.Info("[RebuildSnapshot] Add object",
				zap.Uint64("table id", tid),
				zap.String("object name", objectStats.ObjectName().String()),
				zap.String("create at", createTS.ToString()))
			continue
		}
	}
}

func (sm *SnapshotMeta) ReadMeta(ctx context.Context, name string, fs fileservice.FileService) error {
	reader, err := blockio.NewFileReaderNoCache(fs, name)
	if err != nil {
		return err
	}
	bs, err := reader.LoadAllBlocks(ctx, common.DebugAllocator)
	if err != nil {
		return err
	}
	idxes := make([]uint16, len(objectInfoSchemaAttr))
	for i := range objectInfoSchemaAttr {
		idxes[i] = uint16(i)
	}
	mobat, release, err := reader.LoadColumns(ctx, idxes, nil, bs[0].GetID(), common.DebugAllocator)
	if err != nil {
		return err
	}
	defer release()
	bat := containers.NewBatch()
	defer bat.Close()
	for i := range objectInfoSchemaAttr {
		pkgVec := mobat.Vecs[i]
		var vec containers.Vector
		if pkgVec.Length() == 0 {
			vec = containers.MakeVector(objectInfoSchemaTypes[i], common.DebugAllocator)
		} else {
			vec = containers.ToTNVector(pkgVec, common.DebugAllocator)
		}
		bat.AddVector(objectInfoSchemaAttr[i], vec)
	}
	sm.Rebuild(bat, &sm.objects)

	if len(bs) == 1 {
		return nil
	}

	idxes = make([]uint16, len(objectInfoSchemaAttr))
	for i := range objectInfoSchemaAttr {
		idxes[i] = uint16(i)
	}
	moDeltaBat, releaseDelta, err := reader.LoadColumns(ctx, idxes, nil, bs[1].GetID(), common.DebugAllocator)
	if err != nil {
		return err
	}
	defer releaseDelta()
	deltaBat := containers.NewBatch()
	defer deltaBat.Close()
	for i := range objectInfoSchemaAttr {
		pkgVec := moDeltaBat.Vecs[i]
		var vec containers.Vector
		if pkgVec.Length() == 0 {
			vec = containers.MakeVector(objectInfoSchemaTypes[i], common.DebugAllocator)
		} else {
			vec = containers.ToTNVector(pkgVec, common.DebugAllocator)
		}
		deltaBat.AddVector(objectInfoSchemaAttr[i], vec)
	}
	sm.Rebuild(deltaBat, &sm.tombstones)
	return nil
}

func (sm *SnapshotMeta) ReadTableInfo(ctx context.Context, name string, fs fileservice.FileService) error {
	reader, err := blockio.NewFileReaderNoCache(fs, name)
	if err != nil {
		return err
	}
	bs, err := reader.LoadAllBlocks(ctx, common.DebugAllocator)
	if err != nil {
		return err
	}
	idxes := make([]uint16, len(tableInfoSchemaAttr))
	for i := range tableInfoSchemaAttr {
		idxes[i] = uint16(i)
	}
	for id, block := range bs {
		mobat, release, err := reader.LoadColumns(ctx, idxes, nil, block.GetID(), common.DebugAllocator)
		if err != nil {
			return err
		}
		defer release()
		bat := containers.NewBatch()
		defer bat.Close()
		for i := range tableInfoSchemaAttr {
			pkgVec := mobat.Vecs[i]
			var vec containers.Vector
			if pkgVec.Length() == 0 {
				vec = containers.MakeVector(objectInfoSchemaTypes[i], common.DebugAllocator)
			} else {
				vec = containers.ToTNVector(pkgVec, common.DebugAllocator)
			}
			bat.AddVector(tableInfoSchemaAttr[i], vec)
		}
		if id == 0 {
			sm.RebuildTableInfo(bat)
		} else if id == 1 {
			sm.RebuildTid(bat)
		} else if id == 2 {
			sm.RebuildTableInfo(bat)
		}
	}
	return nil
}

func (sm *SnapshotMeta) InitTableInfo(ctx context.Context, fs fileservice.FileService, data *CheckpointData) {
	sm.Lock()
	defer sm.Unlock()
	sm.updateTableInfo(ctx, fs, data)
}

func (sm *SnapshotMeta) TableInfoString() string {
	sm.RLock()
	defer sm.RUnlock()
	var buf bytes.Buffer
	for accID, tables := range sm.tables {
		buf.WriteString(fmt.Sprintf("accountID: %d\n", accID))
		for tid, table := range tables {
			buf.WriteString(fmt.Sprintf("tableID: %d, create: %s, deleteAt: %s\n",
				tid, table.createAt.ToString(), table.deleteAt.ToString()))
		}
	}
	return buf.String()
}

func (sm *SnapshotMeta) GetSnapshotList(SnapshotList map[uint32][]types.TS, tid uint64) []types.TS {
	sm.RLock()
	defer sm.RUnlock()
	if sm.acctIndexes[tid] == nil {
		return nil
	}
	accID := sm.acctIndexes[tid].accountID
	return SnapshotList[accID]
}

func (sm *SnapshotMeta) GetSnapshotListLocked(SnapshotList map[uint32][]types.TS, tid uint64) []types.TS {
	if isMoTable(tid) || isMoDB(tid) || isMoCol(tid) {
		allSnapshot := make(map[types.TS]struct{}, 0)
		snapshotList := make([]types.TS, 0)
		for _, snapshots := range SnapshotList {
			for _, snapshot := range snapshots {
				allSnapshot[snapshot] = struct{}{}
			}
		}

		for snapshot := range allSnapshot {
			snapshotList = append(snapshotList, snapshot)
		}
		sort.Slice(snapshotList, func(i, j int) bool {
			return snapshotList[i].Less(&snapshotList[j])
		})
		return snapshotList
	}
	if sm.acctIndexes[tid] == nil {
		return nil
	}
	accID := sm.acctIndexes[tid].accountID
	return SnapshotList[accID]
}

func (sm *SnapshotMeta) GetPitrLocked(pitr *PitrInfo, db, tid uint64) types.TS {
	var ts types.TS
	if isMoTable(tid) || isMoDB(tid) || isMoCol(tid) {
		for _, p := range pitr.account {
			if ts.IsEmpty() || p.Less(&ts) {
				ts = p
			}
		}
		for _, p := range pitr.database {
			if ts.IsEmpty() || p.Less(&ts) {
				ts = p
			}
		}

		for _, p := range pitr.tables {
			if ts.IsEmpty() || p.Less(&ts) {
				ts = p
			}
		}
		return ts
	}
	if sm.acctIndexes[tid] != nil {
		account := sm.acctIndexes[tid].accountID
		p := pitr.account[account]
		if !p.IsEmpty() {
			ts = p
		}
	}
	p := pitr.database[tid]
	if !p.IsEmpty() && (ts.IsEmpty() || p.Less(&ts)) {
		ts = p
	}
	p = pitr.tables[tid]
	if !p.IsEmpty() && (ts.IsEmpty() || p.Less(&ts)) {
		ts = p
	}
	return ts

}

func (sm *SnapshotMeta) MergeTableInfo(SnapshotList map[uint32][]types.TS) error {
	sm.Lock()
	defer sm.Unlock()
	if len(sm.tables) == 0 {
		return nil
	}
	for accID, tables := range sm.tables {
		if SnapshotList[accID] == nil {
			for _, table := range tables {
				if !table.deleteAt.IsEmpty() {
					logutil.Infof("MergeTableInfo delete table %d", table.tid)
					delete(sm.tables[accID], table.tid)
					delete(sm.acctIndexes, table.tid)
					if sm.objects[table.tid] != nil {
						delete(sm.objects, table.tid)
					}
				}
			}
			continue
		}
		for _, table := range tables {
			if !table.deleteAt.IsEmpty() && !isSnapshotRefers(table, SnapshotList[accID]) {
				logutil.Infof("MergeTableInfo delete table %d", table.tid)
				delete(sm.tables[accID], table.tid)
				delete(sm.acctIndexes, table.tid)
				if sm.objects[table.tid] != nil {
					delete(sm.objects, table.tid)
				}
			}
		}
	}
	return nil
}

func (sm *SnapshotMeta) String() string {
	sm.RLock()
	defer sm.RUnlock()
	return fmt.Sprintf("account count: %d, table count: %d, object count: %d",
		len(sm.tables), len(sm.acctIndexes), len(sm.objects))
}

func isSnapshotRefers(table *tableInfo, snapVec []types.TS) bool {
	if len(snapVec) == 0 {
		return false
	}
	left, right := 0, len(snapVec)-1
	for left <= right {
		mid := left + (right-left)/2
		snapTS := snapVec[mid]
		if snapTS.GreaterEq(&table.createAt) && snapTS.Less(&table.deleteAt) {
			logutil.Infof("isSnapshotRefers: %s, create %v, drop %v, tid %d",
				snapTS.ToString(), table.createAt.ToString(), table.deleteAt.ToString(), table.tid)
			return true
		} else if snapTS.Less(&table.createAt) {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return false
}

func CloseSnapshotList(snapshots map[uint32]containers.Vector) {
	for _, snapshot := range snapshots {
		snapshot.Close()
	}
}
