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
	traceMode     bool
)

type objInfo struct {
	Stats      objectio.ObjectStats
	CreateTS   types.TS
	DeleteTS   types.TS
	ObjType    int8
	TID        uint64
	DBID       uint64
	CkpIdx     int    // which checkpoint entry this came from
	CkpEntry   string // checkpoint entry description
	Appendable bool
	Sorted     bool
	CNCreated  bool
}

func (o *objInfo) ObjKind() string {
	if o.Appendable {
		return "aobj"
	}
	return "nobj"
}

func (o *objInfo) TypeName() string {
	if o.ObjType == ioutil.ObjectType_Tombstone {
		return "Tombstone"
	}
	return "Data"
}

type rowLocation struct {
	ObjName     string
	BlockIdx    uint32
	RowIdx      uint32
	Rowid       types.Rowid
	TID         uint64
	DBID        uint64
	RelName     string
	ObjCreateTS types.TS
	ObjDeleteTS types.TS
	ObjKind     string
	CkpEntry    string
}

func main() {
	dir := flag.String("dir", "", "shared dir path")
	flag.Uint64Var(&targetDBID, "dbid", 0, "database ID")
	flag.Uint64Var(&targetTID, "tid", 0, "table ID")
	flag.StringVar(&keyword, "keyword", "", "keyword in name/SQL")
	flag.BoolVar(&showAll, "all", false, "show all records")
	flag.BoolVar(&showObjects, "objects", false, "show object list")
	flag.BoolVar(&scanTombstone, "scan-tombstone", false, "scan tombstones for target tid")
	flag.BoolVar(&traceMode, "trace", false, "full trace: scan all objects+tombstones for dbid/tid")
	metaName := flag.String("meta", "", "checkpoint meta file name")
	flag.Parse()

	if *dir == "" {
		fmt.Println("please specify -dir")
		flag.Usage()
		os.Exit(1)
	}
	if targetDBID == 0 && targetTID == 0 && keyword == "" && !showAll && !showObjects && !scanTombstone && !traceMode {
		fmt.Println("please specify -dbid, -tid, -keyword, -all, -objects, -scan-tombstone or -trace")
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
	fmt.Printf("total %d checkpoint entries:\n", len(entries))
	for i, e := range entries {
		typ := "ICKP"
		if e.IsGlobal() {
			typ = "GCKP"
		}
		fmt.Printf("  [%d] %s %s\n", i, typ, e.String())
	}
	fmt.Println()

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

	if traceMode && (targetDBID != 0 || targetTID != 0) {
		traceDBAndTID(ctx, fs, dbObjects, tblObjects)
		return
	}

	fmt.Println("========== mo_database rows ==========")
	readMODatabaseRows(ctx, fs, dbObjects)
	fmt.Println("\n========== mo_tables rows ==========")
	readMOTablesRows(ctx, fs, tblObjects)
}

// dedup objects by objectName+type, merge ckp entries, keep final state (with deleteTS if any)
type objState struct {
	info    objInfo
	deleted bool
	allCkps []string
}

func dedupObjects(objects []objInfo) map[string]*objState {
	objMap := make(map[string]*objState)
	for _, obj := range objects {
		key := obj.Stats.ObjectName().String()
		mapKey := fmt.Sprintf("%s_%s", obj.TypeName(), key)
		existing, ok := objMap[mapKey]
		if !ok {
			objMap[mapKey] = &objState{
				info:    obj,
				deleted: !obj.DeleteTS.IsEmpty(),
				allCkps: []string{fmt.Sprintf("ckp[%d](%s)", obj.CkpIdx, obj.CkpEntry)},
			}
		} else {
			existing.allCkps = append(existing.allCkps,
				fmt.Sprintf("ckp[%d](%s)", obj.CkpIdx, obj.CkpEntry))
			if !obj.DeleteTS.IsEmpty() {
				existing.deleted = true
				existing.info = obj
			}
		}
	}
	return objMap
}

func traceDBAndTID(ctx context.Context, fs fileservice.FileService, dbObjects, tblObjects []objInfo) {
	fmt.Println("################################################################")
	fmt.Printf("# TRACE MODE: dbid=%d tid=%d\n", targetDBID, targetTID)
	fmt.Println("################################################################")

	dbObjMap := dedupObjects(dbObjects)
	tblObjMap := dedupObjects(tblObjects)

	var dbRowLocs []rowLocation
	dbCoveredRowids := make(map[types.Rowid]bool)

	// ============================================================
	// Part 1: mo_database - scan ALL data objects for dbid
	// ============================================================
	if targetDBID != 0 {
		fmt.Printf("\n========== [1/4] mo_database DATA objects: scan for dbid=%d ==========\n", targetDBID)
		dbDataCols := []uint16{0, 1, 3, 7} // dat_id, datname, dat_createsql, dat_account_id
		for mapKey, state := range dbObjMap {
			if !strings.HasPrefix(mapKey, "Data_") {
				continue
			}
			obj := state.info
			objName := obj.Stats.ObjectName()
			batches, err := readObjectBlocks(ctx, fs, obj.Stats, dbDataCols)
			if err != nil {
				fmt.Printf("  [ERROR] read obj %s: %v\n", objName.String(), err)
				continue
			}
			for blkIdx, bat := range batches {
				if bat == nil {
					continue
				}
				dbids := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[0])
				accids := vector.MustFixedColNoTypeCheck[uint32](bat.Vecs[3])
				for rowIdx := 0; rowIdx < bat.RowCount(); rowIdx++ {
					if dbids[rowIdx] == targetDBID {
						rowid := types.NewRowIDWithObjectIDBlkNumAndRowID(
							*objName.ObjectId(), uint16(blkIdx), uint32(rowIdx))
						name := bat.Vecs[1].GetStringAt(rowIdx)
						sql := bat.Vecs[2].GetStringAt(rowIdx)
						deletedStr := ""
						if state.deleted {
							deletedStr = fmt.Sprintf(" [OBJ-DELETED deleteTS=%s]", obj.DeleteTS.ToString())
						}
						fmt.Printf("  FOUND dbid=%d name=%q account=%d sql=%q\n"+
							"    obj=%s kind=%s appendable=%v sorted=%v cnCreated=%v\n"+
							"    blk=%d row=%d rowid=%s createTS=%s%s\n"+
							"    from: %s\n",
							targetDBID, name, accids[rowIdx], trunc(sql, 200),
							objName.String(), obj.ObjKind(), obj.Appendable, obj.Sorted, obj.CNCreated,
							blkIdx, rowIdx, rowid.String(), obj.CreateTS.ToString(), deletedStr,
							strings.Join(state.allCkps, ", "))
						dbRowLocs = append(dbRowLocs, rowLocation{
							ObjName:     objName.String(),
							BlockIdx:    uint32(blkIdx),
							RowIdx:      uint32(rowIdx),
							Rowid:       rowid,
							DBID:        targetDBID,
							RelName:     name,
							ObjCreateTS: obj.CreateTS,
							ObjDeleteTS: obj.DeleteTS,
							ObjKind:     obj.ObjKind(),
							CkpEntry:    strings.Join(state.allCkps, ", "),
						})
					}
				}
				bat.Clean(nil)
			}
		}
		if len(dbRowLocs) == 0 {
			fmt.Println("  No rows found for target dbid in any data object")
		} else {
			fmt.Printf("  Total %d row(s) found for dbid=%d\n", len(dbRowLocs), targetDBID)
		}

		// ============================================================
		// Part 2: mo_database TOMBSTONE objects - scan for dbid rowids
		// ============================================================
		fmt.Printf("\n========== [2/4] mo_database TOMBSTONE objects: scan for dbid=%d ==========\n", targetDBID)
		dbTargetRowids := make(map[types.Rowid]*rowLocation)
		for i := range dbRowLocs {
			dbTargetRowids[dbRowLocs[i].Rowid] = &dbRowLocs[i]
		}
		dbCoveredRowids = make(map[types.Rowid]bool)
		scannedDB := 0
		for mapKey, state := range dbObjMap {
			if !strings.HasPrefix(mapKey, "Tombstone_") {
				continue
			}
			obj := state.info
			objName := obj.Stats.ObjectName()
			scannedDB++
			tombCols := tombstoneColsForObj(obj)
			batches, err := readObjectBlocks(ctx, fs, obj.Stats, tombCols)
			if err != nil {
				fmt.Printf("  [ERROR] read tombstone obj %s: %v\n", objName.String(), err)
				continue
			}
			commitTSIdx := tombstoneCommitTSIdx(obj, tombCols)
			for blkIdx, bat := range batches {
				if bat == nil {
					continue
				}
				rowids := vector.MustFixedColNoTypeCheck[types.Rowid](bat.Vecs[0])
				for rowIdx := 0; rowIdx < bat.RowCount(); rowIdx++ {
					rid := rowids[rowIdx]
					if loc, ok := dbTargetRowids[rid]; ok {
						deleteTime := readTombstoneCommitTS(bat, commitTSIdx, rowIdx)
						deletedStr := ""
						if state.deleted {
							deletedStr = fmt.Sprintf(" [TOMBSTONE-OBJ-DELETED deleteTS=%s]", obj.DeleteTS.ToString())
						}
						fmt.Printf("  MATCH! tombstone deletes dbid=%d rowid=%s deleteTime=%s\n"+
							"    tombstone obj=%s kind=%s appendable=%v sorted=%v cnCreated=%v\n"+
							"    tombstone blk=%d row=%d objCreateTS=%s%s\n"+
							"    target data obj=%s blk=%d row=%d\n"+
							"    from: %s\n",
							targetDBID, rid.String(), deleteTime,
							objName.String(), obj.ObjKind(), obj.Appendable, obj.Sorted, obj.CNCreated,
							blkIdx, rowIdx, obj.CreateTS.ToString(), deletedStr,
							loc.ObjName, loc.BlockIdx, loc.RowIdx,
							strings.Join(state.allCkps, ", "))
						dbCoveredRowids[rid] = true
					}
				}
				bat.Clean(nil)
			}
		}
		fmt.Printf("  Scanned %d unique mo_database tombstone objects\n", scannedDB)
		fmt.Printf("  Matching tombstone entries for dbid=%d: %d\n", targetDBID, len(dbCoveredRowids))
		for _, loc := range dbRowLocs {
			if !dbCoveredRowids[loc.Rowid] {
				fmt.Printf("  WARNING UNCOVERED: obj=%s kind=%s blk=%d row=%d rowid=%s createTS=%s deleteTS=%s\n",
					loc.ObjName, loc.ObjKind, loc.BlockIdx, loc.RowIdx, loc.Rowid.String(),
					loc.ObjCreateTS.ToString(), loc.ObjDeleteTS.ToString())
			}
		}
	}

	// ============================================================
	// Part 3: mo_tables - scan ALL data objects for tid (and dbid)
	// ============================================================
	fmt.Printf("\n========== [3/4] mo_tables DATA objects: scan for tid=%d dbid=%d ==========\n", targetTID, targetDBID)
	tblDataCols := []uint16{0, 1, 2, 3, 7, 11} // rel_id, relname, reldatabase, reldatabase_id, rel_createsql, account_id
	var tblRowLocs []rowLocation
	for mapKey, state := range tblObjMap {
		if !strings.HasPrefix(mapKey, "Data_") {
			continue
		}
		obj := state.info
		objName := obj.Stats.ObjectName()
		batches, err := readObjectBlocks(ctx, fs, obj.Stats, tblDataCols)
		if err != nil {
			fmt.Printf("  [ERROR] read obj %s: %v\n", objName.String(), err)
			continue
		}
		for blkIdx, bat := range batches {
			if bat == nil {
				continue
			}
			tids := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[0])
			dbids := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[3])
			accids := vector.MustFixedColNoTypeCheck[uint32](bat.Vecs[5])
			for rowIdx := 0; rowIdx < bat.RowCount(); rowIdx++ {
				tid := tids[rowIdx]
				dbid := dbids[rowIdx]
				match := false
				if targetTID != 0 && tid == targetTID {
					match = true
				}
				if targetDBID != 0 && dbid == targetDBID {
					match = true
				}
				if !match {
					continue
				}
				rowid := types.NewRowIDWithObjectIDBlkNumAndRowID(
					*objName.ObjectId(), uint16(blkIdx), uint32(rowIdx))
				relname := bat.Vecs[1].GetStringAt(rowIdx)
				dbname := bat.Vecs[2].GetStringAt(rowIdx)
				sql := bat.Vecs[4].GetStringAt(rowIdx)
				deletedStr := ""
				if state.deleted {
					deletedStr = fmt.Sprintf(" [OBJ-DELETED deleteTS=%s]", obj.DeleteTS.ToString())
				}
				fmt.Printf("  FOUND tid=%d dbid=%d name=%q db=%q account=%d sql=%q\n"+
					"    obj=%s kind=%s appendable=%v sorted=%v cnCreated=%v\n"+
					"    blk=%d row=%d rowid=%s createTS=%s%s\n"+
					"    from: %s\n",
					tid, dbid, relname, dbname, accids[rowIdx], trunc(sql, 200),
					objName.String(), obj.ObjKind(), obj.Appendable, obj.Sorted, obj.CNCreated,
					blkIdx, rowIdx, rowid.String(), obj.CreateTS.ToString(), deletedStr,
					strings.Join(state.allCkps, ", "))
				tblRowLocs = append(tblRowLocs, rowLocation{
					ObjName:     objName.String(),
					BlockIdx:    uint32(blkIdx),
					RowIdx:      uint32(rowIdx),
					Rowid:       rowid,
					TID:         tid,
					DBID:        dbid,
					RelName:     relname,
					ObjCreateTS: obj.CreateTS,
					ObjDeleteTS: obj.DeleteTS,
					ObjKind:     obj.ObjKind(),
					CkpEntry:    strings.Join(state.allCkps, ", "),
				})
			}
			bat.Clean(nil)
		}
	}
	if len(tblRowLocs) == 0 {
		fmt.Println("  No rows found for target tid/dbid in any data object")
	} else {
		fmt.Printf("  Total %d row(s) found\n", len(tblRowLocs))
	}

	// ============================================================
	// Part 4: mo_tables TOMBSTONE objects - scan for tid rowids
	// ============================================================
	fmt.Printf("\n========== [4/4] mo_tables TOMBSTONE objects: scan for tid=%d dbid=%d ==========\n", targetTID, targetDBID)
	tblTargetRowids := make(map[types.Rowid]*rowLocation)
	for i := range tblRowLocs {
		tblTargetRowids[tblRowLocs[i].Rowid] = &tblRowLocs[i]
	}
	tblCoveredRowids := make(map[types.Rowid]bool)
	scannedTbl := 0
	for mapKey, state := range tblObjMap {
		if !strings.HasPrefix(mapKey, "Tombstone_") {
			continue
		}
		obj := state.info
		objName := obj.Stats.ObjectName()
		scannedTbl++
		tombCols := tombstoneColsForObj(obj)
		batches, err := readObjectBlocks(ctx, fs, obj.Stats, tombCols)
		if err != nil {
			fmt.Printf("  [ERROR] read tombstone obj %s: %v\n", objName.String(), err)
			continue
		}
		commitTSIdx := tombstoneCommitTSIdx(obj, tombCols)
		for blkIdx, bat := range batches {
			if bat == nil {
				continue
			}
			rowids := vector.MustFixedColNoTypeCheck[types.Rowid](bat.Vecs[0])
			for rowIdx := 0; rowIdx < bat.RowCount(); rowIdx++ {
				rid := rowids[rowIdx]
				if loc, ok := tblTargetRowids[rid]; ok {
					deleteTime := readTombstoneCommitTS(bat, commitTSIdx, rowIdx)
					deletedStr := ""
					if state.deleted {
						deletedStr = fmt.Sprintf(" [TOMBSTONE-OBJ-DELETED deleteTS=%s]", obj.DeleteTS.ToString())
					}
					fmt.Printf("  MATCH! tombstone deletes tid=%d rowid=%s deleteTime=%s\n"+
						"    tombstone obj=%s kind=%s appendable=%v sorted=%v cnCreated=%v\n"+
						"    tombstone blk=%d row=%d objCreateTS=%s%s\n"+
						"    target data obj=%s blk=%d row=%d\n"+
						"    from: %s\n",
						loc.TID, rid.String(), deleteTime,
						objName.String(), obj.ObjKind(), obj.Appendable, obj.Sorted, obj.CNCreated,
						blkIdx, rowIdx, obj.CreateTS.ToString(), deletedStr,
						loc.ObjName, loc.BlockIdx, loc.RowIdx,
						strings.Join(state.allCkps, ", "))
					tblCoveredRowids[rid] = true
				}
			}
			bat.Clean(nil)
		}
	}
	fmt.Printf("  Scanned %d unique mo_tables tombstone objects\n", scannedTbl)
	fmt.Printf("  Matching tombstone entries: %d\n", len(tblCoveredRowids))

	// ============================================================
	// Summary
	// ============================================================
	fmt.Println("\n################################################################")
	fmt.Println("# TRACE SUMMARY")
	fmt.Println("################################################################")
	if targetDBID != 0 {
		fmt.Printf("  mo_database: dbid=%d found in %d data row(s)\n", targetDBID, len(dbRowLocs))
		dbUncovered := 0
		for _, loc := range dbRowLocs {
			if !dbCoveredRowids[loc.Rowid] {
				dbUncovered++
			}
		}
		if dbUncovered > 0 {
			fmt.Printf("  WARNING: %d mo_database row(s) have NO tombstone!\n", dbUncovered)
			for _, loc := range dbRowLocs {
				if !dbCoveredRowids[loc.Rowid] {
					fmt.Printf("    UNCOVERED DB: obj=%s kind=%s blk=%d row=%d rowid=%s createTS=%s deleteTS=%s\n"+
						"      from: %s\n",
						loc.ObjName, loc.ObjKind, loc.BlockIdx, loc.RowIdx, loc.Rowid.String(),
						loc.ObjCreateTS.ToString(), loc.ObjDeleteTS.ToString(), loc.CkpEntry)
				}
			}
		} else if len(dbRowLocs) > 0 {
			fmt.Printf("  mo_database: all %d row(s) covered by tombstones (db was properly dropped)\n", len(dbRowLocs))
		}
	}
	fmt.Printf("  mo_tables: found %d data row(s) matching tid=%d/dbid=%d\n", len(tblRowLocs), targetTID, targetDBID)
	tblUncovered := 0
	for _, loc := range tblRowLocs {
		if !tblCoveredRowids[loc.Rowid] {
			tblUncovered++
		}
	}
	if tblUncovered > 0 {
		fmt.Printf("  WARNING: %d mo_tables row(s) have NO tombstone!\n", tblUncovered)
		for _, loc := range tblRowLocs {
			if !tblCoveredRowids[loc.Rowid] {
				fmt.Printf("    UNCOVERED TBL: tid=%d dbid=%d name=%q obj=%s kind=%s blk=%d row=%d rowid=%s\n"+
					"      createTS=%s deleteTS=%s\n"+
					"      from: %s\n",
					loc.TID, loc.DBID, loc.RelName,
					loc.ObjName, loc.ObjKind, loc.BlockIdx, loc.RowIdx, loc.Rowid.String(),
					loc.ObjCreateTS.ToString(), loc.ObjDeleteTS.ToString(), loc.CkpEntry)
			}
		}
	} else if len(tblRowLocs) > 0 {
		fmt.Printf("  mo_tables: all %d row(s) covered by tombstones\n", len(tblRowLocs))
	}
}

func scanTombstoneForTID(ctx context.Context, fs fileservice.FileService, tblObjects []objInfo) {
	fmt.Printf("========== Scanning mo_tables for tid=%d ==========\n\n", targetTID)

	// Step 1: deduplicate objects
	objMap := dedupObjects(tblObjects)

	// count deduped
	dataCount, tombCount := 0, 0
	for mapKey := range objMap {
		if strings.HasPrefix(mapKey, "Data_") {
			dataCount++
		} else {
			tombCount++
		}
	}
	fmt.Printf("After dedup: %d unique data objects, %d unique tombstone objects\n\n", dataCount, tombCount)

	// Step 2: scan ALL data objects for tid=targetTID
	cols := []uint16{0, 1, 2, 3} // rel_id, relname, reldatabase, reldatabase_id
	var matchedRows []rowLocation

	fmt.Println("--- Scanning ALL data objects (including deleted) for target tid ---")
	for mapKey, state := range objMap {
		if !strings.HasPrefix(mapKey, "Data_") {
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
						ObjKind:     obj.ObjKind(),
						CkpEntry:    obj.CkpEntry,
					}
					matchedRows = append(matchedRows, loc)

					deletedStr := ""
					if state.deleted {
						deletedStr = fmt.Sprintf(" [OBJ-DELETED deleteTS=%s]", obj.DeleteTS.ToString())
					}
					fmt.Printf("  FOUND tid=%d name=%q db=%q dbid=%d\n"+
						"    obj=%s kind=%s appendable=%v sorted=%v cnCreated=%v\n"+
						"    blk=%d row=%d rowid=%s createTS=%s%s\n"+
						"    from: %s\n",
						targetTID, relname, dbname, dbids[rowIdx],
						objName.String(), obj.ObjKind(), obj.Appendable, obj.Sorted, obj.CNCreated,
						blkIdx, rowIdx, rowid.String(), obj.CreateTS.ToString(), deletedStr,
						strings.Join(state.allCkps, ", "))
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

	// Step 3: scan ALL tombstone objects
	fmt.Println("--- Scanning ALL tombstone objects for matching rowids ---")

	targetRowids := make(map[types.Rowid]*rowLocation)
	for i := range matchedRows {
		targetRowids[matchedRows[i].Rowid] = &matchedRows[i]
	}
	targetObjIDs := make(map[objectio.ObjectId]bool)
	for _, loc := range matchedRows {
		objID := loc.Rowid.BorrowObjectID()
		targetObjIDs[*objID] = true
	}

	coveredRowids := make(map[types.Rowid]bool)
	scannedTombstones := 0
	for mapKey, state := range objMap {
		if !strings.HasPrefix(mapKey, "Tombstone_") {
			continue
		}
		obj := state.info
		objName := obj.Stats.ObjectName()
		scannedTombstones++

		tombCols := tombstoneColsForObj(obj)
		batches, err := readObjectBlocks(ctx, fs, obj.Stats, tombCols)
		if err != nil {
			fmt.Printf("  [ERROR] read tombstone obj %s failed: %v\n", objName.String(), err)
			continue
		}
		commitTSIdx := tombstoneCommitTSIdx(obj, tombCols)
		for blkIdx, bat := range batches {
			if bat == nil {
				continue
			}
			rowids := vector.MustFixedColNoTypeCheck[types.Rowid](bat.Vecs[0])
			for rowIdx := 0; rowIdx < bat.RowCount(); rowIdx++ {
				rid := rowids[rowIdx]
				if loc, ok := targetRowids[rid]; ok {
					deleteTime := readTombstoneCommitTS(bat, commitTSIdx, rowIdx)
					deletedStr := ""
					if state.deleted {
						deletedStr = fmt.Sprintf(" [TOMBSTONE-OBJ-DELETED deleteTS=%s]", obj.DeleteTS.ToString())
					}
					fmt.Printf("  MATCH! tombstone -> deletes rowid=%s deleteTime=%s\n"+
						"    tombstone obj=%s kind=%s appendable=%v sorted=%v cnCreated=%v\n"+
						"    tombstone blk=%d row=%d objCreateTS=%s%s\n"+
						"    target data obj=%s blk=%d row=%d tid=%d\n"+
						"    from: %s\n",
						rid.String(), deleteTime,
						objName.String(), obj.ObjKind(), obj.Appendable, obj.Sorted, obj.CNCreated,
						blkIdx, rowIdx, obj.CreateTS.ToString(), deletedStr,
						loc.ObjName, loc.BlockIdx, loc.RowIdx, loc.TID,
						strings.Join(state.allCkps, ", "))
					coveredRowids[rid] = true
				}
			}
			bat.Clean(nil)
		}
	}

	fmt.Printf("\n========== Summary ==========\n")
	fmt.Printf("  Target tid=%d found in %d data object row(s)\n", targetTID, len(matchedRows))
	fmt.Printf("  Scanned %d unique tombstone objects\n", scannedTombstones)
	fmt.Printf("  Matching tombstone entries: %d\n", len(coveredRowids))
	uncovered := 0
	for _, loc := range matchedRows {
		if !coveredRowids[loc.Rowid] {
			uncovered++
		}
	}
	if uncovered > 0 {
		fmt.Printf("  WARNING: %d row(s) have NO tombstone coverage!\n", uncovered)
		for _, loc := range matchedRows {
			if !coveredRowids[loc.Rowid] {
				fmt.Printf("    UNCOVERED: obj=%s kind=%s blk=%d row=%d rowid=%s\n"+
					"      createTS=%s deleteTS=%s from=%s\n",
					loc.ObjName, loc.ObjKind, loc.BlockIdx, loc.RowIdx, loc.Rowid.String(),
					loc.ObjCreateTS.ToString(), loc.ObjDeleteTS.ToString(), loc.CkpEntry)
			}
		}
	} else {
		fmt.Println("  All rows are covered by tombstones")
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
	for idx, entry := range entries {
		typ := "ICKP"
		if entry.IsGlobal() {
			typ = "GCKP"
		}
		ckpDesc := fmt.Sprintf("%s:%s", typ, entry.String())

		reader := logtail.NewCKPReader(entry.GetVersion(), entry.GetLocation(), mp, fs)
		if err := reader.ReadMeta(ctx); err != nil {
			fmt.Printf("ReadMeta failed for ckp[%d]: %v\n", idx, err)
			continue
		}
		_ = reader.ForEachRow(ctx, func(
			account uint32, dbid, tid uint64, objectType int8,
			objectStats objectio.ObjectStats, createTS, deleteTS types.TS,
			rowID types.Rowid,
		) error {
			info := objInfo{
				Stats:      objectStats,
				CreateTS:   createTS,
				DeleteTS:   deleteTS,
				ObjType:    objectType,
				TID:        tid,
				DBID:       dbid,
				CkpIdx:     idx,
				CkpEntry:   ckpDesc,
				Appendable: objectStats.GetAppendable(),
				Sorted:     objectStats.GetSorted(),
				CNCreated:  objectStats.GetCNCreated(),
			}
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
		deleted := ""
		if !obj.DeleteTS.IsEmpty() {
			deleted = fmt.Sprintf(" deleteTS=%s", obj.DeleteTS.ToString())
		}
		fmt.Printf("  [%d] %s %s obj=%s rows=%d kind=%s appendable=%v sorted=%v cnCreated=%v createTS=%s%s ckp[%d]\n",
			i, obj.TypeName(), obj.ObjKind(), obj.Stats.ObjectName().String(),
			obj.Stats.Rows(), obj.ObjKind(), obj.Appendable, obj.Sorted, obj.CNCreated,
			obj.CreateTS.ToString(), deleted, obj.CkpIdx)
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

// tombstoneColsForObj returns columns to read from a tombstone object.
// col 0 = rowid, col 1 = PK, col 2 = commitTS (nobj) or col 3 = commitTS (aobj)
func tombstoneColsForObj(obj objInfo) []uint16 {
	if obj.CNCreated {
		// CN-created tombstone: only rowid + PK, no commitTS column
		return []uint16{0, 1}
	}
	if obj.Appendable {
		// aobj: rowid(0), PK(1), phyAddr(2), commitTS(3)
		return []uint16{0, 1, 2, 3}
	}
	// nobj TN-created: rowid(0), PK(1), commitTS(2)
	return []uint16{0, 1, 2}
}

// tombstoneCommitTSIdx returns the index in the read batch that holds commitTS.
// Returns -1 if no commitTS available (CN-created).
func tombstoneCommitTSIdx(obj objInfo, cols []uint16) int {
	if obj.CNCreated {
		return -1
	}
	if obj.Appendable {
		return 3 // batch index 3
	}
	return 2 // batch index 2
}

// readTombstoneCommitTS reads the commitTS from a tombstone batch at the given row.
func readTombstoneCommitTS(bat *batch.Batch, colIdx int, rowIdx int) string {
	if colIdx < 0 || colIdx >= len(bat.Vecs) {
		return "N/A(cn-created)"
	}
	tss := vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[colIdx])
	return tss[rowIdx].ToString()
}
