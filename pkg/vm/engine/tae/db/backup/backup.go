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
	"fmt"
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"io"
	"os"
	"time"
)

func BackupData(ctx context.Context, fs fileservice.FileService, dst string, db *db.DB, catalog *catalog.Catalog) (err error) {
	currTs := types.BuildTS(time.Now().UTC().UnixNano(), 0)

	err = db.ForceCheckpoint(ctx, currTs, 10*time.Second)
	checkpoints := db.BGCheckpointRunner.GetAllIncrementalCheckpoints()
	var data *logtail.CheckpointData
	files := make(map[string]interface{}, 0)
	for _, candidate := range checkpoints {
		data, err = collectCkpData(candidate, catalog)
		if err != nil {
			logutil.Errorf("processing clean %s: %v", candidate.String(), err)
			// TODO
			return
		}
		defer data.Close()
		ins, _, _, _ := data.GetBlkBatchs()
		for i := 0; i < ins.Length(); i++ {
			metaLoc := objectio.Location(ins.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Get(i).([]byte))
			if metaLoc == nil {
				continue
			}
			if files[metaLoc.Name().String()] == nil {
				files[metaLoc.Name().String()] = struct{}{}
			}
		}
	}

	for file := range files {
		copy(file, dst)
	}

	return
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

func copy(src, dst string) (int64, error) {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return 0, err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return 0, fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer source.Close

	destination, err := os.Create(dst)
	if err != nil {
		return 0, err
	}

	defer destination.Close()
	nBytes, err := io.Copy(destination, source)
	return nBytes, err
}
