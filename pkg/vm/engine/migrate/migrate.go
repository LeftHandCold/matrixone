package migrate

import (
	"context"
	"fmt"
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/engine_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const (
	ckpDir    = "ckp"
	ckpBakDir = "ckp-bak"
	gcDir     = "gc"

	oldObjDir   = "rewritten/old"
	newObjDir   = "rewritten/new"
	rollbackDir = "rewritten/rollback"
)

func NewFileFs(ctx context.Context, path string) fileservice.FileService {
	fs := objectio.TmpNewFileservice(ctx, path)
	return fs
}

func ListCkpFiles(ctx context.Context, fs fileservice.FileService) (res []string) {
	entires, err := fileservice.SortedList(fs.List(ctx, ckpBakDir))
	if err != nil {
		panic(err)
	}
	for _, entry := range entires {
		res = append(res, filepath.Join(ckpBakDir, entry.Name))
	}
	return
}

var locs map[string]objectio.Location

func GetCkpFiles(ctx context.Context, dataFs, objectFs fileservice.FileService) {
	locs = make(map[string]objectio.Location)
	entries := ListCkpFiles(ctx, dataFs)
	sinker := NewSinker(ObjectListSchema, objectFs)
	defer sinker.Close()
	now := time.Now()
	duration := time.Now()
	for i, entry := range entries {
		DumpCkpFiles(ctx, dataFs, entry, sinker)
		if i%5 == 0 {
			logutil.Infof("[dumpCkp] dump old ckp files %v/%v, cost %v, total %v", i, len(entries), time.Since(duration), time.Since(now))
			duration = time.Now()
		}
	}
	err := sinker.Sync(ctx)
	if err != nil {
		panic(err)
	}
}

func DumpCkpFiles(ctx context.Context, fs fileservice.FileService, filepath string, sinker *engine_util.Sinker) {
	reader, err := blockio.NewFileReader("", fs, filepath)
	if err != nil {
		panic(err)
	}
	mp := common.CheckpointAllocator
	bats, closeCB, err := reader.LoadAllColumns(ctx, nil, mp)
	if err != nil {
		panic(err)
	}
	defer func() {
		if closeCB != nil {
			closeCB()
		}
	}()
	if len(bats) != 1 {
		panic("invalid checkpoint file")
	}
	var checkpointVersion int = 3
	bat := containers.NewBatch()
	defer bat.Close()
	{
		// convert to TN Batch
		colNames := checkpoint.CheckpointSchema.Attrs()
		colTypes := checkpoint.CheckpointSchema.Types()
		for i := range bats[0].Vecs {
			var vec containers.Vector
			if bats[0].Vecs[i].Length() == 0 {
				vec = containers.MakeVector(colTypes[i], mp)
			} else {
				vec = containers.ToTNVector(bats[0].Vecs[i], mp)
			}
			bat.AddVector(colNames[i], vec)
		}
	}
	entries, _ := checkpoint.ReplayCheckpointEntries(bat, checkpointVersion)
	batch := MakeBasicRespBatchFromSchema(ObjectListSchema, common.CheckpointAllocator, nil)

	collectObjects := func(bats []*containers.Batch) {
		collectStats := func(bat *containers.Batch) {
			objectStats := bat.GetVectorByName(ObjectAttr_ObjectStats)
			for i := 0; i < objectStats.Length(); i++ {
				obj := objectStats.Get(i).([]byte)
				obj = append(obj, byte(0))
				ss := objectio.ObjectStats(obj)
				name := ss.ObjectLocation().Name().String()
				batch.Vecs[0].Append([]byte(name), false)
			}
		}

		collectDeltaLoc := func(bat *containers.Batch) {
			deltaLoc := bat.GetVectorByName(BlockMeta_DeltaLoc)
			for i := 0; i < deltaLoc.Length(); i++ {
				loc := objectio.Location(deltaLoc.Get(i).([]byte))
				if loc.IsEmpty() {
					continue
				}
				name := loc.Name().String()
				batch.Vecs[0].Append([]byte(name), false)
			}
		}

		collectStats(bats[ObjectInfoIDX])
		collectStats(bats[TNObjectInfoIDX])
		collectDeltaLoc(bats[BLKMetaInsertIDX])
		collectDeltaLoc(bats[BLKMetaInsertTxnIDX])
	}
	for _, entry := range entries {
		if locs[entry.GetTNLocation().Name().String()] != nil {
			continue
		}
		locs[entry.GetTNLocation().Name().String()] = entry.GetTNLocation()
		data := GetCkpData(ctx, entry, fs)
		if data == nil {
			continue
		}
		collectObjects(data)
	}
	if err = sinker.Write(ctx, containers.ToCNBatch(batch)); err != nil {
		panic(err)
	}
}

func GetCkpData(ctx context.Context, baseEntry *checkpoint.CheckpointEntry, fs fileservice.FileService) (ckpData []*containers.Batch) {
	mp := common.CheckpointAllocator
	ckpData = make([]*containers.Batch, MaxIDX)
	for idx, schema := range checkpointDataSchemas_V11 {
		ckpData[idx] = makeRespBatchFromSchema(schema, mp)
	}
	reader1, err := blockio.NewObjectReader("", fs, baseEntry.GetTNLocation())
	if err != nil {
		panic(err)
	}
	// read meta
	typs := append([]types.Type{types.T_Rowid.ToType(), types.T_TS.ToType()}, MetaSchema.Types()...)
	attrs := append([]string{pkgcatalog.Row_ID, pkgcatalog.TableTailAttrCommitTs}, MetaSchema.Attrs()...)
	metaBats, err := logtail.LoadBlkColumnsByMeta(11, ctx, typs, attrs, uint16(MetaIDX), reader1, mp)
	if err != nil {
		return nil
	}
	metaBat := metaBats[0]
	ckpData[MetaIDX] = metaBat
	locations := make(map[string]objectio.Location)
	{ // read data locations
		tidVec := vector.MustFixedColNoTypeCheck[uint64](metaBat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector())
		insVec := metaBat.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchLocation).GetDownstreamVector()
		delVec := metaBat.GetVectorByName(SnapshotMetaAttr_BlockCNInsertBatchLocation).GetDownstreamVector()
		delCNVec := metaBat.GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchLocation).GetDownstreamVector()
		segVec := metaBat.GetVectorByName(SnapshotMetaAttr_SegDeleteBatchLocation).GetDownstreamVector()
		usageInsVec := metaBat.GetVectorByName(CheckpointMetaAttr_StorageUsageInsLocation).GetDownstreamVector()
		usageDelVec := metaBat.GetVectorByName(CheckpointMetaAttr_StorageUsageDelLocation).GetDownstreamVector()
		insertLoc := func(loc []byte) {
			bl := logtail.BlockLocations(loc)
			it := bl.MakeIterator()
			for it.HasNext() {
				block := it.Next()
				if !block.GetLocation().IsEmpty() {
					locations[block.GetLocation().Name().String()] = block.GetLocation()
				}
			}
		}
		for i := 0; i < len(tidVec); i++ {
			tid := tidVec[i]
			if tid == 0 {
				insertLoc(insVec.GetBytesAt(i))
				continue
			}
			insLocation := insVec.GetBytesAt(i)
			delLocation := delVec.GetBytesAt(i)
			delCNLocation := delCNVec.GetBytesAt(i)
			segLocation := segVec.GetBytesAt(i)
			tmp := [][]byte{insLocation, delLocation, delCNLocation, segLocation}
			tmp = append(tmp, usageInsVec.GetBytesAt(i))
			tmp = append(tmp, usageDelVec.GetBytesAt(i))
			for _, loc := range tmp {
				insertLoc(loc)
			}
		}
	}
	// read data
	for _, val := range locations {
		reader, err := blockio.NewObjectReader("", fs, val)
		if err != nil {
			panic(err)
		}
		for idx := 1; idx < MaxIDX; idx++ {
			typs := append([]types.Type{types.T_Rowid.ToType(), types.T_TS.ToType()}, checkpointDataSchemas_V11[idx].Types()...)
			attrs := append([]string{pkgcatalog.Row_ID, pkgcatalog.TableTailAttrCommitTs}, checkpointDataSchemas_V11[idx].Attrs()...)
			bats, err := logtail.LoadBlkColumnsByMeta(11, ctx, typs, attrs, uint16(idx), reader, mp)
			if err != nil {
				panic(err)
			}
			for i := range bats {
				ckpData[idx].Append(bats[i])
			}
		}
	}
	return ckpData
}

func NewSinker(schema *catalog.Schema, fs fileservice.FileService) *engine_util.Sinker {
	seqnums := make([]uint16, len(schema.Attrs()))
	for i := range schema.Attrs() {
		seqnums[i] = schema.GetSeqnum(schema.Attrs()[i])
	}
	factory := engine_util.NewFSinkerImplFactory(
		seqnums,
		schema.GetPrimaryKey().Idx,
		true,
		false,
		schema.Version,
	)
	sinker := engine_util.NewSinker(
		schema.GetPrimaryKey().Idx,
		schema.Attrs(),
		schema.Types(),
		factory,
		common.CheckpointAllocator,
		fs,
		engine_util.WithDedupAll(),
		engine_util.WithTailSizeCap(0),
		engine_util.WithBufferSizeCap(500*mpool.MB),
	)
	return sinker
}

func ReadCkp11File(ctx context.Context, fs fileservice.FileService, filepath string) (*checkpoint.CheckpointEntry, []*containers.Batch) {
	reader, err := blockio.NewFileReader("", fs, filepath)
	if err != nil {
		panic(err)
	}
	mp := common.CheckpointAllocator
	bats, closeCB, err := reader.LoadAllColumns(ctx, nil, mp)
	if err != nil {
		panic(err)
	}
	defer func() {
		if closeCB != nil {
			closeCB()
		}
	}()

	if len(bats) != 1 {
		panic("invalid checkpoint file")
	}

	var checkpointVersion int = 3
	bat := containers.NewBatch()
	defer bat.Close()
	{
		// convert to TN Batch
		colNames := checkpoint.CheckpointSchema.Attrs()
		colTypes := checkpoint.CheckpointSchema.Types()
		for i := range bats[0].Vecs {
			var vec containers.Vector
			if bats[0].Vecs[i].Length() == 0 {
				vec = containers.MakeVector(colTypes[i], mp)
			} else {
				vec = containers.ToTNVector(bats[0].Vecs[i], mp)
			}
			bat.AddVector(colNames[i], vec)
		}
	}

	entries, maxEnd := checkpoint.ReplayCheckpointEntries(bat, checkpointVersion)
	var baseEntry *checkpoint.CheckpointEntry
	for _, entry := range entries {
		end := entry.GetEnd()
		if end.LT(&maxEnd) {
			continue
		}
		if baseEntry == nil {
			baseEntry = entry
		} else {
			panic("not global checkpoint?")
		}
	}
	var ckpData = make([]*containers.Batch, MaxIDX)
	for idx, schema := range checkpointDataSchemas_V11 {
		ckpData[idx] = makeRespBatchFromSchema(schema, mp)
	}

	reader1, err := blockio.NewObjectReader("", fs, baseEntry.GetTNLocation())
	if err != nil {
		panic(err)
	}
	// read meta
	typs := append([]types.Type{types.T_Rowid.ToType(), types.T_TS.ToType()}, MetaSchema.Types()...)
	attrs := append([]string{pkgcatalog.Row_ID, pkgcatalog.TableTailAttrCommitTs}, MetaSchema.Attrs()...)
	metaBats, err := logtail.LoadBlkColumnsByMeta(11, ctx, typs, attrs, uint16(MetaIDX), reader1, mp)
	if err != nil {
		panic(err)
	}
	metaBat := metaBats[0]
	//println(baseEntry.GetTNLocation().Name().String(), len(metaBats), metaBat.Length())
	ckpData[MetaIDX] = metaBat

	locations := make(map[string]objectio.Location)
	{ // read data locations
		tidVec := vector.MustFixedColNoTypeCheck[uint64](metaBat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector())
		insVec := metaBat.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchLocation).GetDownstreamVector()
		delVec := metaBat.GetVectorByName(SnapshotMetaAttr_BlockCNInsertBatchLocation).GetDownstreamVector()
		delCNVec := metaBat.GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchLocation).GetDownstreamVector()
		segVec := metaBat.GetVectorByName(SnapshotMetaAttr_SegDeleteBatchLocation).GetDownstreamVector()
		usageInsVec := metaBat.GetVectorByName(CheckpointMetaAttr_StorageUsageInsLocation).GetDownstreamVector()
		usageDelVec := metaBat.GetVectorByName(CheckpointMetaAttr_StorageUsageDelLocation).GetDownstreamVector()

		insertLoc := func(loc []byte) {
			bl := logtail.BlockLocations(loc)
			it := bl.MakeIterator()
			for it.HasNext() {
				block := it.Next()
				if !block.GetLocation().IsEmpty() {
					locations[block.GetLocation().Name().String()] = block.GetLocation()
				}
			}
		}

		for i := 0; i < len(tidVec); i++ {
			tid := tidVec[i]
			if tid == 0 {
				insertLoc(insVec.GetBytesAt(i))
				continue
			}
			insLocation := insVec.GetBytesAt(i)
			delLocation := delVec.GetBytesAt(i)
			delCNLocation := delCNVec.GetBytesAt(i)
			segLocation := segVec.GetBytesAt(i)
			tmp := [][]byte{insLocation, delLocation, delCNLocation, segLocation}
			tmp = append(tmp, usageInsVec.GetBytesAt(i))
			tmp = append(tmp, usageDelVec.GetBytesAt(i))
			for _, loc := range tmp {
				insertLoc(loc)
			}
		}
	}

	// read data
	for _, val := range locations {
		reader, err := blockio.NewObjectReader("", fs, val)
		if err != nil {
			panic(err)
		}

		for idx := 1; idx < MaxIDX; idx++ {
			typs := append([]types.Type{types.T_Rowid.ToType(), types.T_TS.ToType()}, checkpointDataSchemas_V11[idx].Types()...)
			attrs := append([]string{pkgcatalog.Row_ID, pkgcatalog.TableTailAttrCommitTs}, checkpointDataSchemas_V11[idx].Attrs()...)
			bats, err := logtail.LoadBlkColumnsByMeta(11, ctx, typs, attrs, uint16(idx), reader, mp)
			if err != nil {
				panic(err)
			}
			for i := range bats {
				ckpData[idx].Append(bats[i])
			}
		}
	}
	return baseEntry, ckpData
}

func ReplayCatalogFromCkpData11(bats []*containers.Batch) *catalog.Catalog {
	cata, _ := catalog.OpenCatalog(nil)
	ReplayDB(cata, bats[DBInsertIDX], bats[DBInsertTxnIDX], bats[DBDeleteIDX], bats[DBDeleteTxnIDX])
	ReplayTable(cata, bats[TBLInsertIDX], bats[TBLInsertTxnIDX], bats[TBLColInsertIDX], bats[TBLDeleteIDX], bats[TBLDeleteTxnIDX])
	return cata
}

func ReplayDB(cata *catalog.Catalog, ins, insTxn, del, delTxn *containers.Batch) {
	for i := 0; i < ins.Length(); i++ {
		dbid := ins.GetVectorByName(pkgcatalog.SystemDBAttr_ID).Get(i).(uint64)
		name := string(ins.GetVectorByName(pkgcatalog.SystemDBAttr_Name).Get(i).([]byte))
		txnNode := txnbase.ReadTuple(insTxn, i)
		tenantID := ins.GetVectorByName(pkgcatalog.SystemDBAttr_AccID).Get(i).(uint32)
		userID := ins.GetVectorByName(pkgcatalog.SystemDBAttr_Creator).Get(i).(uint32)
		roleID := ins.GetVectorByName(pkgcatalog.SystemDBAttr_Owner).Get(i).(uint32)
		createAt := ins.GetVectorByName(pkgcatalog.SystemDBAttr_CreateAt).Get(i).(types.Timestamp)
		createSql := string(ins.GetVectorByName(pkgcatalog.SystemDBAttr_CreateSQL).Get(i).([]byte))
		datType := string(ins.GetVectorByName(pkgcatalog.SystemDBAttr_Type).Get(i).([]byte))
		cata.OnReplayCreateDB(dbid, name, txnNode, tenantID, userID, roleID, createAt, createSql, datType)
	}
	for i := 0; i < del.Length(); i++ {
		dbid := delTxn.GetVectorByName(SnapshotAttr_DBID).Get(i).(uint64)
		txnNode := txnbase.ReadTuple(delTxn, i)
		cata.OnReplayDeleteDB(dbid, txnNode)
	}
}

func ReplayTable(cata *catalog.Catalog, ins, insTxn, insCol, del, delTxn *containers.Batch) {
	schemaOffset := 0
	for i := 0; i < ins.Length(); i++ {
		tid := ins.GetVectorByName(pkgcatalog.SystemRelAttr_ID).Get(i).(uint64)
		dbid := ins.GetVectorByName(pkgcatalog.SystemRelAttr_DBID).Get(i).(uint64)
		name := string(ins.GetVectorByName(pkgcatalog.SystemRelAttr_Name).Get(i).([]byte))
		schema := catalog.NewEmptySchema(name)
		schemaOffset = schema.ReadFromBatch(insCol, schemaOffset, tid)
		schema.Comment = string(ins.GetVectorByName(pkgcatalog.SystemRelAttr_Comment).Get(i).([]byte))
		schema.Version = ins.GetVectorByName(pkgcatalog.SystemRelAttr_Version).Get(i).(uint32)
		schema.CatalogVersion = ins.GetVectorByName(pkgcatalog.SystemRelAttr_CatalogVersion).Get(i).(uint32)
		schema.Partitioned = ins.GetVectorByName(pkgcatalog.SystemRelAttr_Partitioned).Get(i).(int8)
		schema.Partition = string(ins.GetVectorByName(pkgcatalog.SystemRelAttr_Partition).Get(i).([]byte))
		schema.Relkind = string(ins.GetVectorByName(pkgcatalog.SystemRelAttr_Kind).Get(i).([]byte))
		schema.Createsql = string(ins.GetVectorByName(pkgcatalog.SystemRelAttr_CreateSQL).Get(i).([]byte))
		schema.View = string(ins.GetVectorByName(pkgcatalog.SystemRelAttr_ViewDef).Get(i).([]byte))
		schema.Constraint = ins.GetVectorByName(pkgcatalog.SystemRelAttr_Constraint).Get(i).([]byte)

		schema.AcInfo.RoleID = ins.GetVectorByName(pkgcatalog.SystemRelAttr_Owner).Get(i).(uint32)
		schema.AcInfo.UserID = ins.GetVectorByName(pkgcatalog.SystemRelAttr_Creator).Get(i).(uint32)
		schema.AcInfo.CreateAt = ins.GetVectorByName(pkgcatalog.SystemRelAttr_CreateAt).Get(i).(types.Timestamp)
		schema.AcInfo.TenantID = ins.GetVectorByName(pkgcatalog.SystemRelAttr_AccID).Get(i).(uint32)
		extra := insTxn.GetVectorByName(SnapshotAttr_SchemaExtra).Get(i).([]byte)
		schema.MustRestoreExtra(extra)
		schema.Extra.ObjectMaxBlocks = uint32(insTxn.GetVectorByName(SnapshotAttr_ObjectMaxBlock).Get(i).(uint16))
		schema.Extra.BlockMaxRows = insTxn.GetVectorByName(SnapshotAttr_BlockMaxRow).Get(i).(uint32)
		if err := schema.Finalize(true); err != nil {
			panic(err)
		}
		txnNode := txnbase.ReadTuple(insTxn, i)
		cata.OnReplayCreateTable(dbid, tid, schema, txnNode, &dummyDataFactory{})
	}
	for i := 0; i < del.Length(); i++ {
		dbid := delTxn.GetVectorByName(SnapshotAttr_DBID).Get(i).(uint64)
		tid := delTxn.GetVectorByName(SnapshotAttr_TID).Get(i).(uint64)
		txnNode := txnbase.ReadTuple(delTxn, i)
		cata.OnReplayDeleteTable(dbid, tid, txnNode)
	}
}

func DumpCatalogToBatches(cata *catalog.Catalog) (bDbs, bTables, bCols *containers.Batch, snapshotMeta *logtail.SnapshotMeta) {
	bDbs = MakeBasicRespBatchFromSchema(catalog.SystemDBSchema, common.CheckpointAllocator, nil)
	bTables = MakeBasicRespBatchFromSchema(catalog.SystemTableSchema, common.CheckpointAllocator, nil)
	bCols = MakeBasicRespBatchFromSchema(catalog.SystemColumnSchema, common.CheckpointAllocator, nil)
	visitor := &catalog.LoopProcessor{}
	visitor.DatabaseFn = func(db *catalog.DBEntry) error {
		if db.IsSystemDB() {
			return nil
		}
		node := db.GetLatestCommittedNodeLocked()
		if node.HasDropCommitted() {
			return nil
		}
		for _, def := range catalog.SystemDBSchema.ColDefs {
			if def.IsPhyAddr() {
				continue
			}
			txnimpl.FillDBRow(db, def.Name, bDbs.Vecs[def.Idx])
		}
		return nil
	}
	snapshotMeta = logtail.NewSnapshotMeta()
	visitor.TableFn = func(table *catalog.TableEntry) error {
		if pkgcatalog.IsSystemTable(table.GetID()) {
			return nil
		}
		node := table.GetLatestCommittedNodeLocked()
		if node.HasDropCommitted() {
			return nil
		}
		for _, def := range catalog.SystemTableSchema.ColDefs {
			if def.IsPhyAddr() {
				continue
			}
			txnimpl.FillTableRow(table, node.BaseNode.Schema, def.Name, bTables.Vecs[def.Idx])
		}
		createAt := node.GetCreatedAt()
		err := snapshotMeta.InsertTableInfo(node.BaseNode.Schema.AcInfo.TenantID,
			table.GetDB().GetID(), table.GetID(),
			table.GetDB().GetName(), node.BaseNode.Schema.Name,
			&createAt)
		if err != nil {
			panic(err)
		}

		for _, def := range catalog.SystemColumnSchema.ColDefs {
			if def.IsPhyAddr() {
				continue
			}
			txnimpl.FillColumnRow(table, node.BaseNode.Schema, def.Name, bCols.Vecs[def.Idx])
		}
		return nil
	}

	if err := cata.RecurLoop(visitor); err != nil {
		panic(err)
	}

	return
}

func SinkBatch(ctx context.Context, schema *catalog.Schema, bat *containers.Batch, fs fileservice.FileService) []objectio.ObjectStats {
	seqnums := make([]uint16, len(schema.Attrs()))
	for i := range schema.Attrs() {
		seqnums[i] = schema.GetSeqnum(schema.Attrs()[i])
	}

	factory := engine_util.NewFSinkerImplFactory(
		seqnums,
		schema.GetPrimaryKey().Idx,
		true,
		false,
		schema.Version,
	)

	sinker := engine_util.NewSinker(
		schema.GetPrimaryKey().Idx,
		schema.Attrs(),
		schema.Types(),
		factory,
		common.CheckpointAllocator,
		fs,
		engine_util.WithAllMergeSorted(),
		engine_util.WithDedupAll(),
		engine_util.WithTailSizeCap(0),
	)
	if err := sinker.Write(ctx, containers.ToCNBatch(bat)); err != nil {
		panic(err)
	}
	if err := sinker.Sync(ctx); err != nil {
		panic(err)
	}
	objStats, mem := sinker.GetResult()
	if len(mem) > 0 {
		panic("memory left")
	}
	return objStats
}

func RewriteCkp(
	ctx context.Context,
	cc *catalog.Catalog,
	dataFS, objFS, rollbackFS fileservice.FileService,
	oldCkpEntry *checkpoint.CheckpointEntry,
	oldCkpBats []*containers.Batch,
	txnMVCCNode *txnbase.TxnMVCCNode,
	entryMVCCNode *catalog.EntryMVCCNode,
	dbs, tbls, cols []objectio.ObjectStats,
	tid uint64,
) {
	now := time.Now()

	ckpData := logtail.NewCheckpointData("", common.CheckpointAllocator)
	dataObjectBatch := ckpData.GetObjectBatchs()
	tombstoneObjectBatch := ckpData.GetTombstoneObjectBatchs()

	sinker := NewSinker(ObjectListSchema, objFS)
	defer sinker.Close()
	rollbackSinker := NewSinker(ObjectListSchema, rollbackFS)
	defer rollbackSinker.Close()

	metaOffset := int32(0)
	fillObjStats := func(objs []objectio.ObjectStats, tid uint64) {
		bat := MakeBasicRespBatchFromSchema(ObjectListSchema, common.CheckpointAllocator, nil)
		for _, obj := range objs {
			// padding rowid + committs
			dataObjectBatch.GetVectorByName(catalog.PhyAddrColumnName).Append(objectio.HackObjid2Rowid(objectio.NewObjectid()), false)
			dataObjectBatch.GetVectorByName(objectio.DefaultCommitTS_Attr).Append(txnMVCCNode.End, false)
			dataObjectBatch.GetVectorByName(ObjectAttr_ObjectStats).Append(obj[:], false)
			txnMVCCNode.AppendTuple(dataObjectBatch)
			entryMVCCNode.AppendObjectTuple(dataObjectBatch, true)
			dataObjectBatch.GetVectorByName(SnapshotAttr_DBID).Append(uint64(pkgcatalog.MO_CATALOG_ID), false)
			dataObjectBatch.GetVectorByName(SnapshotAttr_TID).Append(tid, false)

			objid := obj.ObjectLocation().Name().String()
			bat.Vecs[0].Append([]byte(objid), false)

			//fmt.Println("A", tid, obj.String())
		}

		if err := sinker.Write(ctx, containers.ToCNBatch(bat)); err != nil {
			panic(err)
		}

		ckpData.UpdateDataObjectMeta(tid, metaOffset, metaOffset+int32(len(objs)))
		metaOffset += int32(len(objs))
	}

	start := time.Now()
	// write three table
	fillObjStats(dbs, pkgcatalog.MO_DATABASE_ID)
	fillObjStats(tbls, pkgcatalog.MO_TABLES_ID)
	fillObjStats(cols, pkgcatalog.MO_COLUMNS_ID)

	// write object stats
	metaOffset = ReplayObjectBatch(
		ctx,
		oldCkpEntry.GetEnd(),
		metaOffset, ckpData,
		oldCkpBats[ObjectInfoIDX], oldCkpBats[TNObjectInfoIDX],
		dataObjectBatch, tid, sinker)

	if err := sinker.Sync(ctx); err != nil {
		panic(err)
	}

	logutil.Infof("[rewrite] replay object stats done, cost %v, total %v", time.Since(start), time.Since(now))

	start = time.Now()
	// write delta location
	ReplayDeletes(
		ctx,
		ckpData,
		cc,
		oldCkpEntry.GetEnd(),
		dataFS,
		oldCkpBats[BLKMetaInsertIDX],
		oldCkpBats[BLKMetaInsertTxnIDX],
		tombstoneObjectBatch,
		rollbackSinker)

	logutil.Infof("[rewrite] replay delta location done, cost %v, total %v", time.Since(start), time.Since(now))

	start = time.Now()
	cnLocation, tnLocation, files, err := ckpData.WriteTo(dataFS, logtail.DefaultCheckpointBlockRows, logtail.DefaultCheckpointSize)
	if err != nil {
		panic(err)
	}

	bat := MakeBasicRespBatchFromSchema(ObjectListSchema, common.CheckpointAllocator, nil)
	bat.Vecs[0].Append([]byte(cnLocation.Name().String()), false)
	for _, file := range files {
		bat.Vecs[0].Append([]byte(file), false)
	}

	if err = rollbackSinker.Write(ctx, containers.ToCNBatch(bat)); err != nil {
		panic(err)
	}
	if err = rollbackSinker.Sync(ctx); err != nil {
		panic(err)
	}

	oldCkpEntry.SetLocation(cnLocation, tnLocation) // update location
	oldCkpEntry.SetVersion(logtail.CheckpointCurrentVersion)

	newCkpMetaBat := MakeBasicRespBatchFromSchema(checkpoint.CheckpointSchema, common.CheckpointAllocator, nil)
	newCkpMetaBat.GetVectorByName(checkpoint.CheckpointAttr_StartTS).Append(oldCkpEntry.GetStart(), false)
	newCkpMetaBat.GetVectorByName(checkpoint.CheckpointAttr_EndTS).Append(oldCkpEntry.GetEnd(), false)
	newCkpMetaBat.GetVectorByName(checkpoint.CheckpointAttr_MetaLocation).Append([]byte(oldCkpEntry.GetLocation()), false)
	newCkpMetaBat.GetVectorByName(checkpoint.CheckpointAttr_EntryType).Append(false, false)
	newCkpMetaBat.GetVectorByName(checkpoint.CheckpointAttr_Version).Append(oldCkpEntry.GetVersion(), false)
	newCkpMetaBat.GetVectorByName(checkpoint.CheckpointAttr_AllLocations).Append([]byte(oldCkpEntry.GetTNLocation()), false)
	newCkpMetaBat.GetVectorByName(checkpoint.CheckpointAttr_CheckpointLSN).Append(oldCkpEntry.LSN(), false)
	newCkpMetaBat.GetVectorByName(checkpoint.CheckpointAttr_TruncateLSN).Append(oldCkpEntry.TrunateLSN(), false)
	newCkpMetaBat.GetVectorByName(checkpoint.CheckpointAttr_Type).Append(int8(checkpoint.ET_Incremental), false)

	name := blockio.EncodeCheckpointMetadataFileName(checkpoint.CheckpointDir, checkpoint.PrefixMetadata, oldCkpEntry.GetStart(), oldCkpEntry.GetEnd())
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterCheckpoint, name, dataFS)
	if err != nil {
		panic(err)
	}
	if _, err = writer.Write(containers.ToCNBatch(newCkpMetaBat)); err != nil {
		panic(err)
	}

	_, err = writer.WriteEnd(ctx)
	if err != nil {
		panic(err)
	}
	logutil.Infof("[rewrite] rewrite ckp done, cost %v, total %v", time.Since(start), time.Since(now))

	logutil.Info("rewrite ckp",
		zap.Int("data object cnt", dataObjectBatch.Length()),
		zap.Int("tombstone object cnt", tombstoneObjectBatch.Length()),
		zap.String("cn location", cnLocation.Name().String()),
		zap.String("tn location", tnLocation.Name().String()),
		zap.String("files", strings.Join(files, ",")),
		zap.Duration("total took", time.Since(now)))

}

const (
	ObjectFlag_Appendable = 1 << iota
	ObjectFlag_Sorted
	ObjectFlag_CNCreated
)

func getTableSchemaFromCatalog(dbId, tblId uint64, cc *catalog.Catalog) *catalog.Schema {
	var (
		err      error
		dbEntry  *catalog.DBEntry
		tblEntry *catalog.TableEntry
	)

	dbEntry, err = cc.GetDatabaseByID(dbId)
	if err != nil {
		panic(err)
	}

	tblEntry, err = dbEntry.GetTableEntryByID(tblId)
	if err != nil {
		panic(err)
	}

	schema := tblEntry.GetLastestSchema(false)
	return schema
}

func IsObjectSoftDeleted(
	cc *catalog.Catalog,
	dbId, tblId uint64,
	obj *types.Objectid,
) (bool, error) {
	dbEntry, err := cc.GetDatabaseByID(dbId)
	if err != nil {
		return false, err
	}

	tblEntry, err := dbEntry.GetTableEntryByID(tblId)
	if err != nil {
		return false, err
	}

	objEntry, err := tblEntry.GetObjectByID(obj, false)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.GetOkExpectedEOB().ErrorCode()) {
			return true, nil
		}
		return false, err
	}

	return !objEntry.DeletedAt.IsEmpty(), nil
}

func replayObjectBatchHelper(
	ctx context.Context,
	ts types.TS,
	src, dest *containers.Batch,
	indexes []int,
	sinker *engine_util.Sinker,
) {

	objectStats := src.GetVectorByName(ObjectAttr_ObjectStats)
	sortedVec := src.GetVectorByName(ObjectAttr_Sorted)
	appendableVec := src.GetVectorByName(ObjectAttr_State)
	dbidVec := src.GetVectorByName(SnapshotAttr_DBID)
	tidVec := src.GetVectorByName(SnapshotAttr_TID)
	createAtVec := src.GetVectorByName(EntryNode_CreateAt)
	deleteAtVec := src.GetVectorByName(EntryNode_DeleteAt)
	startTSVec := src.GetVectorByName(txnbase.SnapshotAttr_StartTS)
	prepareTSVec := src.GetVectorByName(txnbase.SnapshotAttr_PrepareTS)
	commitTSVec := src.GetVectorByName(txnbase.SnapshotAttr_CommitTS)

	bat := MakeBasicRespBatchFromSchema(ObjectListSchema, common.CheckpointAllocator, nil)

	for _, idx := range indexes {
		oldStatsBytes := objectStats.Get(idx).([]byte)
		oldStatsBytes = append(oldStatsBytes, byte(0))
		obj := objectio.ObjectStats(oldStatsBytes)

		sorted := sortedVec.Get(idx).(bool)
		appendable := appendableVec.Get(idx).(bool)
		if appendable {
			objectio.WithAppendable()(&obj)
		} else {
			objectio.WithCNCreated()(&obj)
		}
		if sorted {
			objectio.WithSorted()(&obj)
		}

		dest.GetVectorByName(catalog.PhyAddrColumnName).Append(objectio.HackObjid2Rowid(obj.ObjectName().ObjectId()), false)
		dest.GetVectorByName(objectio.DefaultCommitTS_Attr).Append(ts, false)

		dest.GetVectorByName(ObjectAttr_ObjectStats).Append(obj[:], false)
		dest.GetVectorByName(SnapshotAttr_DBID).Append(dbidVec.Get(idx), false)
		dest.GetVectorByName(SnapshotAttr_TID).Append(tidVec.Get(idx), false)
		dest.GetVectorByName(EntryNode_CreateAt).Append(createAtVec.Get(idx), false)
		dest.GetVectorByName(EntryNode_DeleteAt).Append(deleteAtVec.Get(idx), false)
		dest.GetVectorByName(txnbase.SnapshotAttr_StartTS).Append(startTSVec.Get(idx), false)
		dest.GetVectorByName(txnbase.SnapshotAttr_PrepareTS).Append(prepareTSVec.Get(idx), false)
		dest.GetVectorByName(txnbase.SnapshotAttr_CommitTS).Append(commitTSVec.Get(idx), false)

		name := obj.ObjectLocation().Name().String()
		bat.Vecs[0].Append([]byte(name), false)

		//fmt.Println("B", tidVec.Get(idx), obj.String())
	}

	if err := sinker.Write(ctx, containers.ToCNBatch(bat)); err != nil {
		panic(err)
	}
}

func ReplayObjectBatch(
	ctx context.Context,
	ts types.TS,
	metaOffset int32,
	ckpData *logtail.CheckpointData,
	srcObjInfoBat, srcTNObjInfoBat *containers.Batch,
	dest *containers.Batch,
	tid uint64,
	sinker *engine_util.Sinker,
) int32 {
	now := time.Now()
	gatherTablId := func(mm *map[[2]uint64][]int, bat *containers.Batch) {
		dbidVec := bat.GetVectorByName(SnapshotAttr_DBID)
		tidVec := bat.GetVectorByName(SnapshotAttr_TID)

		for i := 0; i < bat.Length(); i++ {
			did := dbidVec.Get(i).(uint64)
			tid := tidVec.Get(i).(uint64)

			if pkgcatalog.IsSystemTable(tid) {
				continue
			}

			id := [2]uint64{did, tid}
			(*mm)[id] = append((*mm)[id], i)
		}
	}

	tblIdx1 := make(map[[2]uint64][]int)
	tblIdx2 := make(map[[2]uint64][]int)

	// gather all index that belongs to the same table
	gatherTablId(&tblIdx1, srcObjInfoBat)
	gatherTablId(&tblIdx2, srcTNObjInfoBat)

	i := 0
	duration := time.Now()

	for id, idxes2 := range tblIdx2 {
		if id[1] == tid {
			continue
		}
		replayObjectBatchHelper(ctx, ts, srcTNObjInfoBat, dest, idxes2, sinker)
		ckpData.UpdateDataObjectMeta(id[1], metaOffset, metaOffset+int32(len(idxes2)))
		metaOffset += int32(len(idxes2))

		if idxes1 := tblIdx1[id]; len(idxes1) != 0 {
			replayObjectBatchHelper(ctx, ts, srcObjInfoBat, dest, idxes1, sinker)
			ckpData.UpdateDataObjectMeta(id[1], metaOffset, metaOffset+int32(len(idxes1)))
			metaOffset += int32(len(idxes1))

			delete(tblIdx1, id)
		}
		if i%5 == 0 {
			logutil.Infof("[objectStats] replay object batch %v/%v, cost %v, total %v", i, len(tblIdx2), time.Since(duration), time.Since(now))
			duration = time.Now()
		}
		i++
	}

	for id, idxes := range tblIdx1 {
		if id[1] == tid {
			continue
		}
		replayObjectBatchHelper(ctx, ts, srcObjInfoBat, dest, idxes, sinker)
		ckpData.UpdateDataObjectMeta(id[1], metaOffset, metaOffset+int32(len(idxes)))
		metaOffset += int32(len(idxes))
	}

	return metaOffset
}

type tableDeletes struct {
	dbId, tblId uint64
	statsList   []objectio.ObjectStats
}

func replayDeletesHelper(
	ctx context.Context,
	cc *catalog.Catalog,
	fs fileservice.FileService,
	result chan tableDeletes,
	dbId, tblId uint64,
	blkIds []types.Blockid,
	blkDeltaLocs map[types.Blockid]objectio.Location) {

	start := time.Now()

	schema := getTableSchemaFromCatalog(dbId, tblId, cc)
	pkType := schema.GetPrimaryKey().Type

	var sinker *engine_util.Sinker

	locMap := make(map[[79]byte]struct{})

	for _, blk := range blkIds {
		loc := blkDeltaLocs[blk]

		if _, ok := locMap[[79]byte(loc)]; ok {
			continue
		}

		locMap[[79]byte(loc)] = struct{}{}

		bat, release, err := blockio.LoadTombstoneColumnsOldVersion(
			ctx, nil, fs, loc, common.CheckpointAllocator, 0)
		if err != nil {
			deleted, err2 := IsObjectSoftDeleted(cc, dbId, tblId, blk.Object())
			if err2 != nil {
				panic(err2)
			}
			if deleted {
				// soft deleted obj, skip
				logutil.Infof("replay deletes helper, skip soft deleted object: %s, loc: %s",
					blk.Object().String(), loc)
				continue
			} else {
				panic(err)
			}
		}

		// dedup bat
		if err = containers.DedupSortedBatches(
			objectio.TombstonePrimaryKeyIdx,
			[]*batch.Batch{bat},
		); err != nil {
			panic(err)
		}

		if sinker == nil {
			sinker = engine_util.NewTombstoneSinker(
				objectio.HiddenColumnSelection_None,
				pkType,
				common.CheckpointAllocator, fs,
				engine_util.WithTailSizeCap(0),
				engine_util.WithMemorySizeThreshold(mpool.MB*128),
				engine_util.WithDedupAll())
		}

		if err = sinker.Write(ctx, bat); err != nil {
			panic(err)
		}

		release()
	}

	if sinker == nil {
		return
	}

	if err := sinker.Sync(ctx); err != nil {
		panic(err)
	}

	ss, _ := sinker.GetResult()
	sinker.Close()

	objNames := make([]string, 0)
	avgSizeMB := float64(0)
	totalBlkCnt := 0
	totalRowCnt := 0

	for _, s := range ss {
		avgSizeMB += float64(s.Size()) / 1024.0 / 1024.0
		totalRowCnt += int(s.Rows())
		totalBlkCnt += int(s.BlkCnt())
		objNames = append(objNames, s.ObjectName().ObjectId().ShortStringEx())
	}

	result <- tableDeletes{
		dbId:      dbId,
		tblId:     tblId,
		statsList: ss,
	}

	logutil.Info("replay deletes",
		zap.String("tbl", fmt.Sprintf("%d-%d-%s", dbId, tblId, schema.Name)),
		zap.String("deltaLoc info", fmt.Sprintf("%d-%d", len(blkIds), len(locMap))),
		zap.String("obj summary", fmt.Sprintf("cnt(%d)-row(%d)-blk(%d)-size(%.6f)", len(ss), totalRowCnt, totalBlkCnt, avgSizeMB)),
		zap.Duration("total took", time.Since(start)),
		zap.String("obj names", strings.Join(objNames, ",")))
}

func ReplayDeletes(
	ctx context.Context,
	ckpData *logtail.CheckpointData,
	cc *catalog.Catalog,
	ts types.TS,
	fs fileservice.FileService,
	srcBat, srcTxnBat *containers.Batch,
	destBat *containers.Batch,
	sinker *engine_util.Sinker,
) {
	now := time.Now()
	var (
		err         error
		locColIdx   = 6
		blkIdColIdx = 2
	)

	srcCNBat := containers.ToCNBatch(srcBat)
	blkIdCol := vector.MustFixedColWithTypeCheck[types.Blockid](srcCNBat.Vecs[blkIdColIdx])

	//  BLKMetaInsertIDX
	//  BLKMetaInsertTxnIDX

	tblBlks := make(map[[2]uint64][]types.Blockid)
	blkDeltaLocs := make(map[types.Blockid]objectio.Location)
	blkDeltaCts := make(map[types.Blockid]types.TS)

	for i := range srcCNBat.RowCount() {
		dbid := srcTxnBat.GetVectorByName(SnapshotAttr_DBID).Get(i).(uint64)
		tid := srcTxnBat.GetVectorByName(SnapshotAttr_TID).Get(i).(uint64)

		loc := objectio.Location(srcCNBat.Vecs[locColIdx].GetBytesAt(i))
		cts := srcTxnBat.GetVectorByName(txnbase.SnapshotAttr_CommitTS).Get(i).(types.TS)

		if cur, ok := blkDeltaCts[blkIdCol[i]]; !ok {
			blkDeltaLocs[blkIdCol[i]] = loc
			blkDeltaCts[blkIdCol[i]] = cts
		} else if cur.LT(&cts) {
			blkDeltaLocs[blkIdCol[i]] = loc
			blkDeltaCts[blkIdCol[i]] = cts
		}

		tblBlks[[2]uint64{dbid, tid}] = append(tblBlks[[2]uint64{dbid, tid}], blkIdCol[i])
	}

	pool, err := ants.NewPool(runtime.NumCPU())
	if err != nil {
		panic(err)
	}

	result := make(chan tableDeletes, 1000)

	metaOffset := int32(0)
	for tblId, blks := range tblBlks {

		pool.Submit(func() {
			replayDeletesHelper(ctx, cc, fs, result, tblId[0], tblId[1], blks, blkDeltaLocs)
		})

	}
	duration := time.Now()
	for i := 0; i < len(tblBlks); i++ {
		dd := <-result

		for _, s := range dd.statsList {
			destBat.GetVectorByName(catalog.PhyAddrColumnName).Append(objectio.HackObjid2Rowid(s.ObjectName().ObjectId()), false)
			destBat.GetVectorByName(objectio.DefaultCommitTS_Attr).Append(ts, false)

			destBat.GetVectorByName(ObjectAttr_ObjectStats).Append(s[:], false)
			destBat.GetVectorByName(SnapshotAttr_DBID).Append(dd.dbId, false)
			destBat.GetVectorByName(SnapshotAttr_TID).Append(dd.tblId, false)
			destBat.GetVectorByName(EntryNode_CreateAt).Append(ts, false)
			destBat.GetVectorByName(EntryNode_DeleteAt).Append(types.TS{}, false)
			destBat.GetVectorByName(txnbase.SnapshotAttr_CommitTS).Append(ts, false)
			destBat.GetVectorByName(txnbase.SnapshotAttr_StartTS).Append(ts, false)
			destBat.GetVectorByName(txnbase.SnapshotAttr_PrepareTS).Append(ts, false)
		}

		SinkObjectBatch(ctx, sinker, dd.statsList)

		ckpData.UpdateTombstoneObjectMeta(dd.tblId, metaOffset, metaOffset+int32(len(dd.statsList)))
		metaOffset += int32(len(dd.statsList))
		if i%5 == 0 {
			logutil.Infof("[deltaLoc] replay tombstone batch %v/%v, cost %v, total %v", i, len(tblBlks), time.Since(duration), time.Since(now))
			duration = time.Now()
		}
	}

	close(result)
	pool.Free()

	return
}

func GcCheckpointFiles(ctx context.Context, fs fileservice.FileService) {
	mp := common.CheckpointAllocator
	bf := bloomfilter.New(1000000, 0.01)
	newObjs, err := fileservice.SortedList(fs.List(ctx, newObjDir))
	if err != nil {
		panic(err)
	}
	for _, obj := range newObjs {
		reader, err := blockio.NewFileReader("", fs, filepath.Join(newObjDir, obj.Name))
		if err != nil {
			panic(err)
		}
		bats, closeCB, err := reader.LoadAllColumns(ctx, nil, mp)
		if err != nil {
			panic(err)
		}
		defer func() {
			if closeCB != nil {
				closeCB()
			}
		}()

		for _, bat := range bats {
			for _, vec := range bat.Vecs {
				bf.Add(vec)
			}
		}
	}

	oldObjs, err := fileservice.SortedList(fs.List(ctx, oldObjDir))
	if err != nil {
		panic(err)
	}
	for _, obj := range oldObjs {
		reader, err := blockio.NewFileReader("", fs, filepath.Join(oldObjDir, obj.Name))
		if err != nil {
			panic(err)
		}
		bats, closeCB, err := reader.LoadAllColumns(ctx, nil, mp)
		if err != nil {
			panic(err)
		}
		defer func() {
			if closeCB != nil {
				closeCB()
			}
		}()

		for _, bat := range bats {
			for _, vec := range bat.Vecs {
				bf.TestAndAdd(vec, func(exist bool, i int) {
					if !exist {
						data := vec.GetBytesAt(i)
						objid := string(data)
						if err = fs.Delete(ctx, objid); err != nil {
							logutil.Infof("asdf delete %s failed, err: %v", objid, err)
						} else {
							logutil.Infof("asdf delete %s success", objid)
						}
					}
				})
			}
		}
	}

	cleanDir(fs, rollbackDir)
	cleanDir(fs, oldObjDir)
	cleanDir(fs, newObjDir)
}

func Rollback(ctx context.Context, fs fileservice.FileService) {
	mp := common.CheckpointAllocator
	objs, err := fileservice.SortedList(fs.List(ctx, rollbackDir))
	if err != nil {
		panic(err)
	}
	for _, obj := range objs {
		reader, err := blockio.NewFileReader("", fs, filepath.Join(rollbackDir, obj.Name))
		if err != nil {
			panic(err)
		}
		bats, closeCB, err := reader.LoadAllColumns(ctx, nil, mp)
		if err != nil {
			panic(err)
		}
		defer func() {
			if closeCB != nil {
				closeCB()
			}
		}()

		for _, bat := range bats {
			for i := range bat.Vecs[0].Length() {
				name := bat.Vecs[0].GetBytesAt(i)
				if err = fs.Delete(ctx, string(name)); err != nil {
					logutil.Infof("asdf delete %s failed, err: %v", string(name), err)
				} else {
					logutil.Infof("asdf delete %s success", string(name))
				}
			}
		}
	}

	RollbackDir(ctx, fs, ckpDir)
	RollbackDir(ctx, fs, gcDir)
	cleanDir(fs, rollbackDir)
	cleanDir(fs, oldObjDir)
	cleanDir(fs, newObjDir)
}
