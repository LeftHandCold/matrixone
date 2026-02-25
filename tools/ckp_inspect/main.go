package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

var (
	targetDBID    uint64
	targetTID     uint64
	keyword       string
	showAll       bool
	showObjects   bool
	scanTombstone bool
)

type objInfo struct {
	Stats    objectio.ObjectStats
	CreateTS types.TS
	DeleteTS types.TS
	ObjType  int8
	TID      uint64
	DBID     uint64
}

// rowLocation records where a matching row lives in a data object
type rowLocation struct {
	ObjName  string
	BlockIdx uint32
	RowIdx   uint32
	Rowid    types.Rowid
	TID      uint64
	DBID     uint64
	RelName  string
	ObjCreateTS types.TS
	ObjDeleteTS types.TS
}

func main() {
	dir := flag.String("dir", "", "shared dir path")
	flag.Uint64Var(&targetDBID, "dbid", 0, "database ID")
	flag.Uint64Var(&targetTID, "tid", 0, "table ID")
	flag.StringVar(&keyword, "keyword", "", "keyword in name/SQL")
	flag.BoolVar(&showAll, "all", false, "show all records")
	flag.BoolVar(&showObjects, "objects", false, "show object list")
	flag.BoolVar(&scanTombstone, "scan-tombstone", false, "scan tombstones for target tid")
	metaName := flag.String("meta", "", "checkpoint meta file name")
	flag.Parse()

	if *dir == "" {
		fmt.Println("please specify -dir")
		flag.Usage()
		os.Exit(1)
	}
	if targetDBID == 0 && targetTID == 0 && keyword == "" && !showAll && !showObjects && !scanTombstone {
		fmt.Println("please specify -dbid, -tid, -keyword, -all, -objects or -scan-tombstone")
		flag.Usage()
		os.Exit(1)
	}

	ctx := context.Background()
	fs, err := objectio.NewOfflineFS(ctx, *dir, false)
	if err != nil {
		fmt.Printf("init fs failed: %v\n", err)
		os.Exit(1)
	}

	meta := *metaName
	if meta == "" {
		meta, err = findLatestMeta(ctx, fs)
		if err != nil {
			fmt.Printf("find meta failed: %v\n", err)
			os.Exit(1)
		}
	}
	fmt.Printf("checkpoint meta: %s\n", meta)

	mp := common.CheckpointAllocator
	ckpDir := ioutil.GetCheckpointDir()
	entries, err := checkpoint.ReadEntriesFromMeta(ctx, "", ckpDir, meta, 0, nil, mp, fs)
	if err != nil {
		fmt.Printf("read entries failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("total %d checkpoint entries\n\n", len(entries))

	var dbObjects, tblObjects []objInfo
	collectObjects(ctx, entries, fs, mp, &dbObjects, &tblObjects)
	fmt.Printf("mo_database objects: %d, mo_tables objects: %d\n\n", len(dbObjects), len(tblObjects))

	if showObjects {
		printObjects("mo_database", dbObjects)
		printObjects("mo_tables", tblObjects)
	}

	if scanTombstone && targetTID != 0 {
		scanTombstoneForTID(ctx, fs, tblObjects)
		return
	}

	fmt.Println("========== mo_database rows ==========")
	readMODatabaseRows(ctx, fs, dbObjects)
	fmt.Println("\n========== mo_tables rows ==========")
	readMOTablesRows(ctx, fs, tblObjects)
}

func scanTombstoneForTID(ctx context.Context, fs fileservice.FileService, tblObjects []objInfo) {
	fmt.Printf("========== Scanning mo_tables for tid=%d ==========\n\n", targetTID)

	// Step 1: deduplicate objects - same objectName, keep the one with latest state
	// If an object appears with and without deleteTS, the one with deleteTS is the final state
	type objState struct {
		info    objInfo
		deleted bool
	}
	objMap := make(map[string]*objState)
	for _, obj := range tblObjects {
		key := obj.Stats.ObjectName().String()
		typeName := "data"
		if obj.ObjType == ioutil.ObjectType_Tombstone {
			typeName = "tombstone"
		}
		mapKey := fmt.Sprintf("%s_%s", typeName, key)
		existing, ok := objMap[mapKey]
		if !ok {
			objMap[mapKey] = &objState{info: obj, deleted: !obj.DeleteTS.IsEmpty()}
		} else {
			// if this version has deleteTS, mark as deleted
			if !obj.DeleteTS.IsEmpty() {
				existing.deleted = true
				existing.info = obj // keep the version with deleteTS for info
			}
		}
	}

	// Step 2: scan ALL data objects (including deleted ones) for tid=targetTID
	// mo_tables columns: rel_id(0), relname(1), reldatabase(2), reldatabase_id(3)
	cols := []uint16{0, 1, 2, 3}
	var matchedRows []rowLocation

	fmt.Println("--- Scanning ALL data objects (including deleted) for target tid ---")
	for mapKey, state := range objMap {
		if !strings.HasPrefix(mapKey, "data_") {
			continue
		}
		obj := state.info
		objName := obj.Stats.ObjectName()
		batches, err := readObjectBlocks(ctx, fs, obj.Stats, cols)
		if err != nil {
			fmt.Printf("  [ERROR] read obj %s failed: %v\n", objName.String(), err)
			continue
		}
		for blkIdx, bat := range batches {
			if bat == nil {
				continue
			}
			tids := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[0])
			for rowIdx := 0; rowIdx < bat.RowCount(); rowIdx++ {
				if tids[rowIdx] == targetTID {
					// construct rowid: objectID + blockOffset + rowOffset
					rowid := types.NewRowIDWithObjectIDBlkNumAndRowID(
						*objName.ObjectId(), uint16(blkIdx), uint32(rowIdx))

					relname := bat.Vecs[1].GetStringAt(rowIdx)
					dbname := bat.Vecs[2].GetStringAt(rowIdx)
					dbids := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[3])

					loc := rowLocation{
						ObjName:     objName.String(),
						BlockIdx:    uint32(blkIdx),
						RowIdx:      uint32(rowIdx),
						Rowid:       rowid,
						TID:         targetTID,
						DBID:        dbids[rowIdx],
						RelName:     relname,
						ObjCreateTS: obj.CreateTS,
						ObjDeleteTS: obj.DeleteTS,
					}
					matchedRows = append(matchedRows, loc)

					deletedStr := ""
					if state.deleted {
						deletedStr = fmt.Sprintf(" [OBJ-DELETED deleteTS=%s]", obj.DeleteTS.ToString())
					}
					fmt.Printf("  FOUND tid=%d name=%q db=%q dbid=%d obj=%s blk=%d row=%d rowid=%s%s\n",
						targetTID, relname, dbname, dbids[rowIdx],
						objName.String(), blkIdx, rowIdx,
						rowid.String(), deletedStr)
				}
			}
			bat.Clean(nil)
		}
	}

	if len(matchedRows) == 0 {
		fmt.Println("  No rows found for target tid")
		return
	}
	fmt.Printf("\n  Total %d row(s) found for tid=%d\n\n", len(matchedRows), targetTID)

	// Step 3: scan ALL tombstone objects, check if any rowid matches
	fmt.Println("--- Scanning ALL tombstone objects for matching rowids ---")
	tombstoneCols := []uint16{0} // column 0 is rowid

	// build a set of target rowids for fast lookup
	targetRowids := make(map[types.Rowid]rowLocation)
	for _, loc := range matchedRows {
		targetRowids[loc.Rowid] = loc
	}
	// also build a set of target objectIDs (from data objects containing the target tid)
	targetObjIDs := make(map[objectio.ObjectId]bool)
	for _, loc := range matchedRows {
		objID := loc.Rowid.BorrowObjectID()
		targetObjIDs[*objID] = true
	}

	foundTombstones := 0
	for mapKey, state := range objMap {
		if !strings.HasPrefix(mapKey, "tombstone_") {
			continue
		}
		obj := state.info
		objName := obj.Stats.ObjectName()

		// check zonemap prefix to skip irrelevant tombstones
		batches, err := readObjectBlocks(ctx, fs, obj.Stats, tombstoneCols)
		if err != nil {
			fmt.Printf("  [ERROR] read tombstone obj %s failed: %v\n", objName.String(), err)
			continue
		}
		for blkIdx, bat := range batches {
			if bat == nil {
				continue
			}
			rowids := vector.MustFixedColNoTypeCheck[types.Rowid](bat.Vecs[0])
			for rowIdx := 0; rowIdx < bat.RowCount(); rowIdx++ {
				rid := rowids[rowIdx]
				if loc, ok := targetRowids[rid]; ok {
					deletedStr := ""
					if state.deleted {
						deletedStr = fmt.Sprintf(" [TOMBSTONE-OBJ-DELETED deleteTS=%s]", obj.DeleteTS.ToString())
					}
					fmt.Printf("  MATCH! tombstone obj=%s blk=%d row=%d -> deletes rowid=%s (obj=%s blk=%d row=%d tid=%d) createTS=%s%s\n",
						objName.String(), blkIdx, rowIdx,
						rid.String(), loc.ObjName, loc.BlockIdx, loc.RowIdx, loc.TID,
						obj.CreateTS.ToString(), deletedStr)
					foundTombstones++
				}
			}
			bat.Clean(nil)
		}
	}

	fmt.Printf("\n========== Summary ==========\n")
	fmt.Printf("  Target tid=%d found in %d data object row(s)\n", targetTID, len(matchedRows))
	fmt.Printf("  Matching tombstone entries: %d\n", foundTombstones)
	if foundTombstones < len(matchedRows) {
		fmt.Printf("  WARNING: %d row(s) have NO tombstone coverage!\n", len(matchedRows)-foundTombstones)
		for _, loc := range matchedRows {
			if _, ok := targetRowids[loc.Rowid]; ok {
				fmt.Printf("    UNCOVERED: obj=%s blk=%d row=%d rowid=%s objCreateTS=%s objDeleteTS=%s\n",
					loc.ObjName, loc.BlockIdx, loc.RowIdx, loc.Rowid.String(),
					loc.ObjCreateTS.ToString(), loc.ObjDeleteTS.ToString())
			}
		}
	}
}

func findLatestMeta(ctx context.Context, fs fileservice.FileService) (string, error) {
	ckpDir := ioutil.GetCheckpointDir()
	files, err := ioutil.ListTSRangeFiles(ctx, ckpDir, fs)
	if err != nil {
		return "", err
	}
	if len(files) == 0 {
		return "", fmt.Errorf("no checkpoint files found")
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].GetEnd().LT(files[j].GetEnd())
	})
	for i := len(files) - 1; i >= 0; i-- {
		if files[i].IsMetadataFile() {
			return files[i].GetName(), nil
		}
	}
	for i := len(files) - 1; i >= 0; i-- {
		if files[i].IsCompactExt() {
			return files[i].GetName(), nil
		}
	}
	return "", fmt.Errorf("no meta file found")
}

func collectObjects(
	ctx context.Context,
	entries []*checkpoint.CheckpointEntry,
	fs fileservice.FileService,
	mp *mpool.MPool,
	dbObjects, tblObjects *[]objInfo,
) {
	for _, entry := range entries {
		reader := logtail.NewCKPReader(entry.GetVersion(), entry.GetLocation(), mp, fs)
		if err := reader.ReadMeta(ctx); err != nil {
			fmt.Printf("ReadMeta failed: %v\n", err)
			continue
		}
		_ = reader.ForEachRow(ctx, func(
			account uint32, dbid, tid uint64, objectType int8,
			objectStats objectio.ObjectStats, createTS, deleteTS types.TS,
			rowID types.Rowid,
		) error {
			info := objInfo{Stats: objectStats, CreateTS: createTS, DeleteTS: deleteTS,
				ObjType: objectType, TID: tid, DBID: dbid}
			if tid == catalog.MO_DATABASE_ID {
				*dbObjects = append(*dbObjects, info)
			} else if tid == catalog.MO_TABLES_ID {
				*tblObjects = append(*tblObjects, info)
			}
			return nil
		})
	}
}

func printObjects(name string, objects []objInfo) {
	fmt.Printf("--- %s objects ---\n", name)
	for i, obj := range objects {
		typeName := "Data"
		if obj.ObjType == ioutil.ObjectType_Tombstone {
			typeName = "Tombstone"
		}
		deleted := ""
		if !obj.DeleteTS.IsEmpty() {
			deleted = fmt.Sprintf(" deleteTS=%s", obj.DeleteTS.ToString())
		}
		fmt.Printf("  [%d] %s obj=%s rows=%d createTS=%s%s\n",
			i, typeName, obj.Stats.ObjectName().String(),
			obj.Stats.Rows(), obj.CreateTS.ToString(), deleted)
	}
	fmt.Println()
}

func readObjectBlocks(ctx context.Context, fs fileservice.FileService,
	stats objectio.ObjectStats, cols []uint16) ([]*batch.Batch, error) {
	name := stats.ObjectName().String()
	ext := stats.Extent()
	meta, err := objectio.ReadObjectMeta(ctx, name, &ext,
		fileservice.SkipMemoryCache|fileservice.SkipFullFilePreloads, fs)
	if err != nil {
		return nil, fmt.Errorf("ReadObjectMeta(%s): %w", name, err)
	}
	dataMeta := meta.MustGetMeta(objectio.SchemaData)
	blkCnt := dataMeta.BlockCount()
	result := make([]*batch.Batch, 0, blkCnt)
	for blk := uint32(0); blk < blkCnt; blk++ {
		bat, err := objectio.ReadOneBlockAllColumns(ctx, &dataMeta, name, blk, cols,
			fileservice.SkipMemoryCache, fs)
		if err != nil {
			return nil, fmt.Errorf("ReadBlock(%s, %d): %w", name, blk, err)
		}
		result = append(result, bat)
	}
	return result, nil
}

// mo_database: dat_id(0), datname(1), dat_catalog_name(2), dat_createsql(3),
// owner(4), creator(5), created_time(6), account_id(7), dat_type(8), cpkey(9)
func readMODatabaseRows(ctx context.Context, fs fileservice.FileService, objects []objInfo) {
	cols := []uint16{0, 1, 3, 7}
	found := false
	for _, obj := range objects {
		if obj.ObjType != ckputil.ObjectType_Data || !obj.DeleteTS.IsEmpty() {
			continue
		}
		batches, err := readObjectBlocks(ctx, fs, obj.Stats, cols)
		if err != nil {
			fmt.Printf("  read obj %s failed: %v\n", obj.Stats.ObjectName().String(), err)
			continue
		}
		for _, bat := range batches {
			if bat == nil {
				continue
			}
			dbids := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[0])
			accids := vector.MustFixedColNoTypeCheck[uint32](bat.Vecs[3])
			for i := 0; i < bat.RowCount(); i++ {
				dbid := dbids[i]
				name := bat.Vecs[1].GetStringAt(i)
				sql := bat.Vecs[2].GetStringAt(i)
				accid := accids[i]
				if matchDB(dbid, name, sql) {
					found = true
					fmt.Printf("  dbid=%d name=%q account=%d sql=%q obj=%s\n",
						dbid, name, accid, trunc(sql, 120), obj.Stats.ObjectName().String())
				}
			}
			bat.Clean(nil)
		}
	}
	for _, obj := range objects {
		if obj.ObjType == ioutil.ObjectType_Tombstone && obj.DeleteTS.IsEmpty() {
			fmt.Printf("  [Tombstone] obj=%s rows=%d createTS=%s\n",
				obj.Stats.ObjectName().String(), obj.Stats.Rows(), obj.CreateTS.ToString())
		}
	}
	if !found && !showAll {
		fmt.Println("  no matching mo_database records found")
	}
}

func matchDB(dbid uint64, name, sql string) bool {
	if showAll {
		return true
	}
	if targetDBID != 0 && dbid == targetDBID {
		return true
	}
	if keyword != "" && (strings.Contains(name, keyword) || strings.Contains(sql, keyword)) {
		return true
	}
	return false
}

// mo_tables: rel_id(0), relname(1), reldatabase(2), reldatabase_id(3),
// relpersistence(4), relkind(5), rel_comment(6), rel_createsql(7),
// created_time(8), creator(9), owner(10), account_id(11), ...
func readMOTablesRows(ctx context.Context, fs fileservice.FileService, objects []objInfo) {
	cols := []uint16{0, 1, 2, 3, 7, 11}
	found := false
	for _, obj := range objects {
		if obj.ObjType != ckputil.ObjectType_Data || !obj.DeleteTS.IsEmpty() {
			continue
		}
		batches, err := readObjectBlocks(ctx, fs, obj.Stats, cols)
		if err != nil {
			fmt.Printf("  read obj %s failed: %v\n", obj.Stats.ObjectName().String(), err)
			continue
		}
		for _, bat := range batches {
			if bat == nil {
				continue
			}
			tids := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[0])
			dbids := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[3])
			accids := vector.MustFixedColNoTypeCheck[uint32](bat.Vecs[5])
			for i := 0; i < bat.RowCount(); i++ {
				tid := tids[i]
				relname := bat.Vecs[1].GetStringAt(i)
				dbname := bat.Vecs[2].GetStringAt(i)
				dbid := dbids[i]
				sql := bat.Vecs[4].GetStringAt(i)
				accid := accids[i]
				if matchTbl(tid, dbid, relname, dbname, sql) {
					found = true
					fmt.Printf("  tid=%d name=%q db=%q dbid=%d account=%d sql=%q obj=%s\n",
						tid, relname, dbname, dbid, accid, trunc(sql, 120),
						obj.Stats.ObjectName().String())
				}
			}
			bat.Clean(nil)
		}
	}
	for _, obj := range objects {
		if obj.ObjType == ioutil.ObjectType_Tombstone && obj.DeleteTS.IsEmpty() {
			fmt.Printf("  [Tombstone] obj=%s rows=%d createTS=%s\n",
				obj.Stats.ObjectName().String(), obj.Stats.Rows(), obj.CreateTS.ToString())
		}
	}
	if !found && !showAll {
		fmt.Println("  no matching mo_tables records found")
	}
}

func matchTbl(tid, dbid uint64, relname, dbname, sql string) bool {
	if showAll {
		return true
	}
	if targetDBID != 0 && dbid == targetDBID {
		return true
	}
	if targetTID != 0 && tid == targetTID {
		return true
	}
	if keyword != "" && (strings.Contains(relname, keyword) ||
		strings.Contains(dbname, keyword) || strings.Contains(sql, keyword)) {
		return true
	}
	return false
}

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
