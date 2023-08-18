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

package backup

import (
	"context"
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"path"
	"time"
)

func BackupData(ctx context.Context, fs fileservice.FileService, db *db.DB, catalog *catalog.Catalog) error {
	currTs := types.BuildTS(time.Now().UTC().UnixNano(), 0)

	err := db.ForceCheckpoint(ctx, currTs, 10*time.Second)
	if err != nil {
		return err
	}
	checkpoints := db.BGCheckpointRunner.GetAllCheckpoints()
	var data *logtail.CheckpointData
	files := make(map[string]*fileservice.DirEntry, 0)
	for _, candidate := range checkpoints {
		data, err = collectCkpData(candidate, catalog)
		if err != nil {
			logutil.Errorf("processing clean %s: %v", candidate.String(), err)
			// TODO
			return err
		}
		defer data.Close()
		ins, _, _, _ := data.GetBlkBatchs()
		for i := 0; i < ins.Length(); i++ {
			metaLoc := objectio.Location(ins.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Get(i).([]byte))
			if metaLoc == nil {
				continue
			}
			if files[metaLoc.Name().String()] == nil {
				dentry, err := db.Opts.Fs.StatFile(ctx, metaLoc.Name().String())
				if err != nil {
					return err
				}
				files[metaLoc.Name().String()] = dentry
			}
		}
	}

	for _, dentry := range files {
		if dentry.IsDir {
			panic("not support dir")
		}
		err = CopyFile(ctx, db.Opts.Fs, fs, dentry, "")
		if err != nil {
			return err
		}
	}
	err = CopyDir(ctx, db.Opts.Fs, fs, "ckp")
	if err != nil {
		return err
	}
	err = CopyDir(ctx, db.Opts.Fs, fs, "gc")
	if err != nil {
		return err
	}
	return nil
}

func collectCkpData(
	ckp *checkpoint.CheckpointEntry,
	catalog *catalog.Catalog,
) (data *logtail.CheckpointData, err error) {
	factory := logtail.IncrementalCheckpointDataFactory(
		ckp.GetStart(),
		ckp.GetEnd(),
	)
	data, err = factory(catalog)
	return
}

func CopyDir(ctx context.Context, srcFs, dstFs fileservice.FileService, dir string) error {
	files, err := srcFs.List(ctx, dir)
	if err != nil {
		return err
	}
	for _, file := range files {
		if file.IsDir {
			panic("not support dir")
		}
		err = CopyFile(ctx, srcFs, dstFs, &file, dir)
		if err != nil {
			return err
		}
	}
	return nil
}

func CopyFile(ctx context.Context, srcFs, dstFs fileservice.FileService, dentry *fileservice.DirEntry, dstDir string) error {
	name := dentry.Name
	if dstDir != "" {
		name = path.Join(dstDir, name)
	}
	ioVec := &fileservice.IOVector{
		FilePath:    name,
		Entries:     make([]fileservice.IOEntry, 1),
		CachePolicy: fileservice.SkipAll,
	}
	logutil.Infof("copy file %v", dentry)
	ioVec.Entries[0] = fileservice.IOEntry{
		Offset: 0,
		Size:   dentry.Size,
	}
	err := srcFs.Read(ctx, ioVec)
	if err != nil {
		return err
	}
	dstIoVec := fileservice.IOVector{
		FilePath:    name,
		Entries:     make([]fileservice.IOEntry, 1),
		CachePolicy: fileservice.SkipAll,
	}
	dstIoVec.Entries[0] = fileservice.IOEntry{
		Offset: 0,
		Data:   ioVec.Entries[0].Data,
		Size:   dentry.Size,
	}
	err = dstFs.Write(ctx, dstIoVec)
	return err
}
