// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package checkpoint

import (
	"context"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

// `files` should be sorted by the end-ts in the asc order
// Ex.1
//
//	    files  :  [0,100],[100,200],[200,300],[0,300],[300,400],[400,500]
//		ts     :  250
//		return :  [0,100],[100,200],[200,300]
//
// Ex.2
//
//	    files  :  [0,100],[100,200],[200,300],[0,300],[300,400],[400,500]
//		ts     :  300
//		return :  [0,100],[100,200],[200,300],[0,300]
//
// Ex.3
//
//	    files  :  [0,100],[100,200],[200,300],[0,300],[300,400],[400,500],[500,600]
//		ts     :  450
//      return :  [0,100],[100,200],[200,300],[0,300],[300,400],[400,500],[500,600]

func FilterSortedMetaFiles(
	ts *types.TS,
	files []*MetaFile,
) []*MetaFile {
	if len(files) == 0 {
		return nil
	}

	prev := files[0]

	// start.IsEmpty() means the file is a global checkpoint
	// ts.LE(&prev.end) means the ts is in the range of the checkpoint
	// it means the ts is in the range of the global checkpoint
	// ts is within GCKP[0, end]
	if prev.start.IsEmpty() && ts.LE(&prev.end) {
		return files[:1]
	}

	for i := 1; i < len(files); i++ {
		curr := files[i]
		// curr.start.IsEmpty() means the file is a global checkpoint
		// ts.LE(&curr.end) means the ts is in the range of the checkpoint
		// ts.LT(&prev.end) means the ts is not in the range of the previous checkpoint
		if curr.start.IsEmpty() && ts.LE(&curr.end) {
			return files[:i]
		}
		prev = curr
	}

	return files
}

func AllAfterAndGCheckpoint(snapshot types.TS, files []*MetaFile) ([]*MetaFile, int, error) {
	prev := &MetaFile{}
	for i, file := range files {
		if snapshot.LE(&file.end) &&
			(prev.end.IsEmpty() || snapshot.LT(&prev.end)) &&
			file.start.IsEmpty() {
			if prev.end.IsEmpty() {
				return files, i, nil
			}
			return files, i - 1, nil
		}
		prev = file
	}
	return files, len(files) - 1, nil
}

func ListSnapshotCheckpoint(
	ctx context.Context,
	sid string,
	fs fileservice.FileService,
	snapshot types.TS,
	tid uint64,
) ([]*CheckpointEntry, error) {
	files, idx, err := ListSnapshotMeta(ctx, snapshot, fs)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, nil
	}
	return ListSnapshotCheckpointWithMeta(ctx, sid, fs, files, idx, types.TS{}, false)
}

func ListSnapshotMeta(
	ctx context.Context,
	snapshot types.TS,
	fs fileservice.FileService,
) ([]*MetaFile, int, error) {
	dirs, err := fs.List(ctx, CheckpointDir)
	if err != nil {
		return nil, 0, err
	}
	if len(dirs) == 0 {
		return nil, 0, nil
	}
	metaFiles := make([]*MetaFile, 0)
	for i, dir := range dirs {
		start, end := blockio.DecodeCheckpointMetadataFileName(dir.Name)
		metaFiles = append(metaFiles, &MetaFile{
			start: start,
			end:   end,
			index: i,
			name:  dir.Name,
		})
	}
	sort.Slice(metaFiles, func(i, j int) bool {
		return metaFiles[i].end.LT(&metaFiles[j].end)
	})

	for i, file := range metaFiles {
		// TODO: remove log
		logutil.Infof("metaFiles[%d]: %v", i, file.String())
	}

	return AllAfterAndGCheckpoint(snapshot, metaFiles)
}

func ListSnapshotMetaWithDiskCleaner(
	snapshot types.TS,
	checkpointMetaFiles map[string]struct{},
) ([]*MetaFile, []*MetaFile, int, error) {
	if len(checkpointMetaFiles) == 0 {
		return nil, nil, 0, nil
	}

	// parse meta file names to MetaFiles
	metaFiles := make([]*MetaFile, 0, len(checkpointMetaFiles))
	idx := 0
	for metaFile := range checkpointMetaFiles {
		start, end := blockio.DecodeCheckpointMetadataFileName(metaFile)
		metaFiles = append(metaFiles, &MetaFile{
			start: start,
			end:   end,
			index: idx,
			name:  metaFile,
		})
		idx++
	}

	// sort meta files by the end ts in the asc order
	sort.Slice(metaFiles, func(i, j int) bool {
		return metaFiles[i].end.LT(&metaFiles[j].end)
	})

	// find the first global checkpoint
	// JW TODO: refactor the following code
	pos := 0
	for i, file := range metaFiles {
		// TODO: remove log
		logutil.Infof("metaFiles[%d]: %v", i, file.String())
		if file.start.IsEmpty() && i < len(metaFiles)-1 && !metaFiles[i+1].start.IsEmpty() {
			pos = i
			break
		}
	}

	var mergeMetaFiles []*MetaFile
	// JW TODO: pos > 0 ?
	if pos > 0 {
		mergeMetaFiles = make([]*MetaFile, 0, pos)
		mergeMetaFiles = append(mergeMetaFiles, metaFiles[:pos]...)
		metaFiles = metaFiles[pos:]
	}

	files, num, err := AllAfterAndGCheckpoint(snapshot, metaFiles)
	return files, mergeMetaFiles, num, err
}

func ListSnapshotCheckpointWithMeta(
	ctx context.Context,
	sid string,
	fs fileservice.FileService,
	files []*MetaFile,
	idx int,
	gcStage types.TS,
	isAll bool,
) ([]*CheckpointEntry, error) {
	reader, err := blockio.NewFileReader(sid, fs, CheckpointDir+files[idx].name)
	if err != nil {
		return nil, nil
	}
	bats, closeCB, err := reader.LoadAllColumns(ctx, nil, common.DebugAllocator)
	if err != nil {
		return nil, nil
	}
	defer func() {
		if closeCB != nil {
			closeCB()
		}
	}()
	bat := containers.NewBatch()
	defer bat.Close()
	colNames := CheckpointSchema.Attrs()
	colTypes := CheckpointSchema.Types()
	for i := range bats[0].Vecs {
		if len(bats) == 0 {
			continue
		}
		var vec containers.Vector
		if bats[0].Vecs[i].Length() == 0 {
			vec = containers.MakeVector(colTypes[i], common.DebugAllocator)
		} else {
			vec = containers.ToTNVector(bats[0].Vecs[i], common.DebugAllocator)
		}
		bat.AddVector(colNames[i], vec)
	}

	var checkpointVersion int
	// in version 1, checkpoint metadata doesn't contain 'version'.
	vecLen := len(bats[0].Vecs)
	if vecLen < CheckpointSchemaColumnCountV1 {
		checkpointVersion = 1
	} else if vecLen < CheckpointSchemaColumnCountV2 {
		checkpointVersion = 2
	} else {
		checkpointVersion = 3
	}

	entries, maxGlobalEnd := ReplayCheckpointEntries(bat, checkpointVersion)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].end.LT(&entries[j].end)
	})
	if isAll && gcStage.IsEmpty() {
		return entries, nil
	}
	for i := range entries {
		if !gcStage.IsEmpty() {
			if entries[i].end.LT(&gcStage) {
				continue
			}
			return entries[i:], nil
		}
		p := maxGlobalEnd.Prev()
		if entries[i].end.Equal(&p) || (entries[i].end.Equal(&maxGlobalEnd) &&
			entries[i].entryType == ET_Global) {
			return entries[i:], nil
		}

	}
	return entries, nil
}
