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

package gc2

import (
	"bytes"
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

type ObjectEntry struct {
	commitTS types.TS
}

// GCTable is a data structure in memory after consuming checkpoint
type checkpointGCTable struct {
	sync.Mutex
	objects map[string]*ObjectEntry
}

func (t *checkpointGCTable) addObject(name string, commitTS types.TS) {
	t.Lock()
	defer t.Unlock()
	object := t.objects[name]
	if object == nil {
		object = &ObjectEntry{
			commitTS: commitTS,
		}
		t.objects[name] = object
		return
	}
	if object.commitTS.Less(commitTS) {
		t.objects[name].commitTS = commitTS
	}
}

func (t *checkpointGCTable) deleteObject(name string) {
	t.Lock()
	defer t.Unlock()
	delete(t.objects, name)
}

// Merge can merge two GCTables
func (t *checkpointGCTable) Merge(GCTable GCTable) {
	for name, entry := range GCTable.(*checkpointGCTable).objects {
		t.addObject(name, entry.commitTS)
	}
}

func (t *checkpointGCTable) getObjects() map[string]*ObjectEntry {
	t.Lock()
	defer t.Unlock()
	return t.objects
}

// SoftGC is to remove objectentry that can be deleted from GCTable
func (t *checkpointGCTable) SoftGC(table GCTable, entry GCEntry) []string {
	gc := make([]string, 0)
	objects := t.getObjects()
	for name, object := range objects {
		objectEntry := table.(*checkpointGCTable).objects[name]
		if objectEntry == nil && object.commitTS.Less(entry.(*checkpointGCEntry).checkpoint.GetEnd()) {
			gc = append(gc, name)
			t.deleteObject(name)
		}
	}
	return gc
}

func (t *checkpointGCTable) UpdateTable(bats []*batch.Batch) {
	insMetaObjectVec := bats[0].Vecs[3]
	insDeltaObjectVec := bats[0].Vecs[4]
	insCommitTSVec := bats[0].Vecs[5]
	for i := 0; i < insMetaObjectVec.Length(); i++ {
		metaLoc := objectio.Location(insMetaObjectVec.GetBytesAt(i))
		deltaLoc := objectio.Location(insDeltaObjectVec.GetBytesAt(i))
		commitTS := vector.GetFixedAt[types.TS](insCommitTSVec, i)
		if !metaLoc.IsEmpty() {
			t.addObject(metaLoc.Name().String(), commitTS)
		}
		if !deltaLoc.IsEmpty() {
			t.addObject(deltaLoc.Name().String(), commitTS)
		}
	}

	if bats[1].Vecs[0].Length() == 0 {
		return
	}
	delMetaObjectVec := bats[2].Vecs[8]
	delDeltaObjectVec := bats[2].Vecs[9]
	delCommitTSVec := bats[1].Vecs[1]
	for i := 0; i < bats[1].Vecs[0].Length(); i++ {
		metaLoc := objectio.Location(delMetaObjectVec.GetBytesAt(i))
		deltaLoc := objectio.Location(delDeltaObjectVec.GetBytesAt(i))
		commitTS := vector.GetFixedAt[types.TS](delCommitTSVec, i)
		if !metaLoc.IsEmpty() {
			t.addObject(metaLoc.Name().String(), commitTS)
		}
		if !deltaLoc.IsEmpty() {
			t.addObject(deltaLoc.Name().String(), commitTS)
		}
	}
}

func (t *checkpointGCTable) makeBatchWithGCTable() []*containers.Batch {
	bats := make([]*containers.Batch, 1)
	bats[CreateBlock] = containers.NewBatch()
	return bats
}

func (t *checkpointGCTable) closeBatch(bs []*containers.Batch) {
	for i := range bs {
		bs[i].Close()
	}
}

// collectData collects data from memory that can be written to s3
func (t *checkpointGCTable) collectData(files []string) []*containers.Batch {
	bats := t.makeBatchWithGCTable()
	for i, attr := range BlockSchemaAttr {
		bats[CreateBlock].AddVector(attr, containers.MakeVector(BlockSchemaTypes[i], common.CheckpointAllocator))
	}
	for name, entry := range t.objects {
		bats[CreateBlock].GetVectorByName(GCAttrObjectName).Append([]byte(name), false)
		bats[CreateBlock].GetVectorByName(GCAttrCommitTS).Append(entry.commitTS, false)
	}
	return bats
}

// SaveTable is to write data to s3
func (t *checkpointGCTable) SaveTable(name string, fs *objectio.ObjectFS, files []string) ([]objectio.BlockObject, error) {
	bats := t.collectData(files)
	defer t.closeBatch(bats)
	//name := blockio.EncodeCheckpointMetadataFileName(GCMetaDir, PrefixGCMeta, start, end)
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterGC, name, fs.Service)
	if err != nil {
		return nil, err
	}
	for i := range bats {
		if _, err := writer.WriteWithoutSeqnum(containers.ToCNBatch(bats[i])); err != nil {
			return nil, err
		}
	}

	blocks, err := writer.WriteEnd(context.Background())
	return blocks, err
}

// SaveFullTable is to write data to s3
func (t *checkpointGCTable) SaveFullTable(name string, fs *objectio.ObjectFS, files []string) ([]objectio.BlockObject, error) {
	bats := t.collectData(files)
	defer t.closeBatch(bats)
	//name := blockio.EncodeGCMetadataFileName(GCMetaDir, PrefixGCMeta, start, end)
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterGC, name, fs.Service)
	if err != nil {
		return nil, err
	}
	for i := range bats {
		if _, err := writer.WriteWithoutSeqnum(containers.ToCNBatch(bats[i])); err != nil {
			return nil, err
		}
	}

	blocks, err := writer.WriteEnd(context.Background())
	return blocks, err
}

func (t *checkpointGCTable) rebuildTable(bats []*containers.Batch) {
	for i := 0; i < bats[CreateBlock].Length(); i++ {
		name := string(bats[CreateBlock].GetVectorByName(GCAttrObjectName).Get(i).([]byte))
		commitTS := bats[CreateBlock].GetVectorByName(GCAttrCommitTS).Get(i).(types.TS)
		if t.objects[name] != nil {
			continue
		}
		t.addObject(name, commitTS)
	}
}

func (t *checkpointGCTable) replayData(ctx context.Context,
	typ BatchType,
	attrs []string,
	types []types.Type,
	bats []*containers.Batch,
	bs []objectio.BlockObject,
	reader *blockio.BlockReader) error {
	idxes := make([]uint16, len(attrs))
	for i := range attrs {
		idxes[i] = uint16(i)
	}
	mobat, err := reader.LoadColumns(ctx, idxes, nil, bs[typ].GetID(), common.DefaultAllocator)
	if err != nil {
		return err
	}
	for i := range attrs {
		pkgVec := mobat.Vecs[i]
		var vec containers.Vector
		if pkgVec.Length() == 0 {
			vec = containers.MakeVector(types[i], common.CheckpointAllocator)
		} else {
			vec = containers.ToTNVector(pkgVec, common.CheckpointAllocator)
		}
		bats[typ].AddVector(attrs[i], vec)
	}
	return nil
}

// ReadTable reads an s3 file and replays a GCTable in memory
func (t *checkpointGCTable) ReadTable(ctx context.Context, name string, size int64, fs *objectio.ObjectFS) error {
	reader, err := blockio.NewFileReaderNoCache(fs.Service, name)
	if err != nil {
		return err
	}
	bs, err := reader.LoadAllBlocks(ctx, common.DefaultAllocator)
	if err != nil {
		return err
	}
	bats := t.makeBatchWithGCTable()
	defer t.closeBatch(bats)
	err = t.replayData(ctx, CreateBlock, BlockSchemaAttr, BlockSchemaTypes, bats, bs, reader)
	if err != nil {
		return err
	}

	t.rebuildTable(bats)
	return nil
}

// For test
func (t *checkpointGCTable) Compare(table GCTable) bool {
	if len(t.objects) != len(table.(*checkpointGCTable).objects) {
		return false
	}
	for name, entry := range t.objects {
		object := table.(*checkpointGCTable).objects[name]
		if object == nil {
			return false
		}
		if !entry.commitTS.Equal(object.commitTS) {
			return false
		}
	}
	return true
}

func (t *checkpointGCTable) String() string {
	if len(t.objects) == 0 {
		return ""
	}
	var w bytes.Buffer
	_, _ = w.WriteString("objects:[\n")
	for name, entry := range t.objects {
		_, _ = w.WriteString(fmt.Sprintf("name: %s, commitTS: %v ", name, entry.commitTS.ToString()))
	}
	_, _ = w.WriteString("]\n")
	return w.String()
}
