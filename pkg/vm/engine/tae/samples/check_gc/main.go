package main

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/gc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"time"
)

const Dir = "/Users/shenjiangwei/Work/code/matrixone/mo-data/shared"

func initFS(ctx context.Context, dir string) fileservice.FileService {
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: dir,
	}
	service, err := fileservice.NewFileService(ctx, c, nil)
	if err != nil {
		panic(err)
	}
	return service
}

func main() {
	ctx := context.Background()

	opts := new(options.Options)
	opts = config.WithLongScanAndCKPOpts(opts)
	opts.Fs = initFS(ctx, Dir)
	opts.GCCfg = &options.GCCfg{
		GCTTL:          time.Hour,
		ScanGCInterval: 30 * time.Minute,
		DisableGC:      true,
	}
	db, _ := db.Open(ctx, Dir, opts)
	cleaner := db.DiskCleaner.GetCleaner()
	testutils.WaitExpect(10000, func() bool {
		return cleaner.GetMinMerged() != nil
	})
	cleaner.TryGC()
	fs := objectio.NewObjectFS(db.Opts.Fs, "serviceDir")
	db.DiskCleaner.GetCleaner().DisableGCForTest()
	inputs := cleaner.GetInputs()
	objects, tombstone := inputs.GetAllObject()
	checkpoints := db.BGCheckpointRunner.ICKPSeekLT(cleaner.GetMaxConsumed().GetEnd(), 40)
	input := gc.NewGCTable()
	for _, ckp := range checkpoints {
		_, data, err := logtail.LoadCheckpointEntriesFromKey(ctx, fs.Service,
			ckp.GetLocation(), ckp.GetVersion(), nil, &types.TS{})
		if err != nil {
			logutil.Errorf("load checkpoint failed: %v", err)
			continue
		}
		input.UpdateTable(data)
	}

	eObjects, eTombstone := input.GetAllObject()
	db.Close()
	dirs, err := fs.ListDir("")
	if err != nil {
		return
	}
	count := len(objects) + len(tombstone) + len(eObjects) + len(eTombstone)
	files := make(map[string]fileservice.DirEntry)
	for _, entry := range dirs {
		if entry.Name == "wal-1.rot" {
			logutil.Infof("skip wal-1.rot")
			continue
		}
		if entry.IsDir {
			continue
		}
		files[entry.Name] = entry
	}
	fileNum := len(files)
	for name, entry := range files {
		isfound := false
		if _, ok := objects[name]; ok {
			isfound = true
			delete(objects, name)
		}

		if _, ok := tombstone[name]; ok {
			isfound = true
			delete(tombstone, name)
		}

		if _, ok := eObjects[name]; ok {
			isfound = true
			delete(eObjects, name)
		}
		if _, ok := eTombstone[name]; ok {
			isfound = true
			delete(eTombstone, name)
		}

		if !isfound {
			// file is not in objects and tombstone, check if it is in files
			logutil.Infof("file %s is not in objects and tombstone", entry.Name)
		} else {
			delete(files, name)
		}
	}
	ckpfiles, _, err := checkpoint.ListSnapshotMeta(ctx, fs.Service, cleaner.GetMaxConsumed().GetStart(), nil)
	if err != nil {
		return
	}
	logutil.Infof("-(len(ckpfiles)*2) is %d, file %d", (len(ckpfiles) * 2), len(files))
	logutil.Infof("%d - %d - %d - %d - %d - %d", len(objects), len(tombstone), len(eObjects), len(eTombstone), len(files)-(len(ckpfiles)*2), fileNum)
	logutil.Infof("object count: %d, consumed count: %d, not consumed count: %d, tail count: %d", fileNum, count, len(objects), len(files)-(len(ckpfiles)*2))
	return
}
