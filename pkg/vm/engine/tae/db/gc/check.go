package gc

import (
	catalog2 "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"time"
)

type checker struct {
	cleaner *checkpointCleaner
}

func (c *checker) getObjects() (map[string]struct{}, error) {
	dirs, err := c.cleaner.fs.ListDir("")
	if err != nil {
		return nil, err
	}
	objects := make(map[string]struct{})
	for _, entry := range dirs {
		if entry.IsDir {
			continue
		}
		objects[entry.Name] = struct{}{}
	}
	return objects, nil
}

func (c *checker) Check() error {
	c.cleaner.inputs.RLock()
	defer c.cleaner.inputs.RUnlock()
	gcTables := c.cleaner.GetGCTables()
	gcTable := gcTables[0]
	for i, table := range gcTables {
		if i > 0 {
			gcTable.Merge(table)
		}
	}
	gcTable.Lock()
	objects := gcTable.objects
	tombstones := gcTable.tombstones
	gcTable.Unlock()
	entry := c.cleaner.GetMaxConsumed()
	maxTs := entry.GetEnd()
	checkpoints := c.cleaner.ckpClient.ICKPSeekLT(entry.GetEnd(), 40)
	unconsumedTable := NewGCTable()
	for _, ckp := range checkpoints {
		logutil.Infof("load checkpoint: %v, entry is %v", ckp.String(), entry.String())
		_, data, err := logtail.LoadCheckpointEntriesFromKey(c.cleaner.ctx, c.cleaner.fs.Service,
			ckp.GetLocation(), ckp.GetVersion(), nil, &types.TS{})
		if err != nil {
			logutil.Errorf("load checkpoint failed: %v", err)
			continue
		}
		unconsumedTable.UpdateTable(data)
		end := ckp.GetEnd()
		if end.Greater(&maxTs) {
			maxTs = ckp.GetEnd()
		}
	}
	unconsumedObjects := unconsumedTable.objects
	unconsumedTombstones := unconsumedTable.tombstones
	allObjects, err := c.getObjects()
	if err != nil {
		return err
	}
	ckpfiles, _, err := checkpoint.ListSnapshotMeta(c.cleaner.ctx, c.cleaner.fs.Service, entry.GetStart(), nil)
	if err != nil {
		return err
	}
	allCount := len(allObjects)
	for name := range allObjects {
		isfound := false
		if _, ok := objects[name]; ok {
			isfound = true
			delete(objects, name)
		}
		if _, ok := tombstones[name]; ok {
			logutil.Infof("tombstone: %v", name)
			isfound = true
			delete(tombstones, name)
		}
		if _, ok := unconsumedObjects[name]; ok {
			isfound = true
			delete(unconsumedObjects, name)
		}
		if _, ok := unconsumedTombstones[name]; ok {
			isfound = true
			delete(unconsumedTombstones, name)
		}
		if isfound {
			delete(allObjects, name)
		}
	}
	catalog := c.cleaner.ckpClient.GetCatalog()
	it := catalog.MakeDBIt(true)
	bat := makeRespBatchFromSchema(logtail.BlkMetaSchema, common.DebugAllocator)
	defer bat.Close()
	end := types.BuildTS(time.Now().UnixNano(), 0)
	for ; it.Valid(); it.Next() {
		db := it.Get().GetPayload()
		itTable := db.MakeTableIt(true)
		for itTable.Valid() {
			table := itTable.Get().GetPayload()
			itObject := table.MakeObjectIt(true)
			for itObject.Valid() {
				objectEntry := itObject.Get().GetPayload()
				stats := objectEntry.GetObjectStats()
				if _, ok := allObjects[stats.ObjectName().String()]; ok {
					delete(allObjects, stats.ObjectName().String())
				}
				itObject.Next()
			}
			it2 := table.GetDeleteList().Items()
			for _, itt := range it2 {
				_, _, _, err = itt.VisitDeletes(c.cleaner.ctx, maxTs, end, bat, nil, true)
				if err != nil {
					logutil.Errorf("visit deletes failed: %v", err)
					continue
				}
			}
			itTable.Next()
		}
	}
	for i := 0; i < bat.Length(); i++ {
		deltaLoc := objectio.Location(bat.GetVectorByName(catalog2.BlockMeta_DeltaLoc).Get(i).([]byte))
		if _, ok := allObjects[deltaLoc.Name().String()]; ok {
			delete(allObjects, deltaLoc.Name().String())
		}
	}

	logutil.Infof("batch length: %d, start: %v, end: %v", bat.Length(), maxTs.ToString(), end.ToString())
	if len(allObjects) > len(ckpfiles)*2 {
		for name := range allObjects {
			logutil.Infof("not found object %s,", name)
		}
		logutil.Warnf("GC abnormal!!! all objects: %d, objects: %d, tombstones: %d, unconsumed objects: %d, unconsumed tombstones: %d, allObjects: %d",
			allCount, len(objects), len(tombstones), len(unconsumedObjects), len(unconsumedTombstones), len(allObjects))
	} else {
		logutil.Infof("all objects: %d, objects: %d, tombstones: %d, unconsumed objects: %d, unconsumed tombstones: %d, allObjects: %d",
			allCount, len(objects), len(tombstones), len(unconsumedObjects), len(unconsumedTombstones), len(allObjects))
	}
	logutil.Infof("GC finished %v", catalog.SimplePPString(common.PPL3))
	return nil
}

func makeRespBatchFromSchema(schema *catalog.Schema, mp *mpool.MPool) *containers.Batch {
	bat := containers.NewBatch()

	bat.AddVector(
		catalog.AttrRowID,
		containers.MakeVector(types.T_Rowid.ToType(), mp),
	)
	bat.AddVector(
		catalog.AttrCommitTs,
		containers.MakeVector(types.T_TS.ToType(), mp),
	)
	// Types() is not used, then empty schema can also be handled here
	typs := schema.AllTypes()
	attrs := schema.AllNames()
	for i, attr := range attrs {
		if attr == catalog.PhyAddrColumnName {
			continue
		}
		bat.AddVector(
			attr,
			containers.MakeVector(typs[i], mp),
		)
	}
	return bat
}