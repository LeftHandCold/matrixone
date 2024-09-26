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

package gc

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/engine_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

type ObjectEntry struct {
	createTS types.TS
	dropTS   types.TS
	db       uint64
	table    uint64
}

type TableOption func(*GCTable)

func WithBufferSize(size int) TableOption {
	return func(table *GCTable) {
		table.buffer.impl = containers.NewOneSchemaBatchBuffer(
			size,
			ObjectTableAttrs,
			ObjectTableTypes,
		)
	}
}

func NewGCTable(
	fs fileservice.FileService,
	mp *mpool.MPool,
	opts ...TableOption,
) *GCTable {
	table := GCTable{
		objects: make(map[string]*ObjectEntry),
		fs:      fs,
		mp:      mp,
	}
	for _, opt := range opts {
		opt(&table)
	}
	return &table
}

type GCTable struct {
	sync.Mutex
	objects map[string]*ObjectEntry
	mp      *mpool.MPool
	fs      fileservice.FileService

	buffer struct {
		sync.Mutex
		impl *containers.OneSchemaBatchBuffer
	}

	files struct {
		sync.Mutex
		stats []objectio.ObjectStats
	}

	tsRange struct {
		start types.TS
		end   types.TS
	}
}

func addObjectLocked(
	name string,
	objEntry *ObjectEntry,
	objects map[string]*ObjectEntry,
) {
	object := objects[name]
	if object == nil {
		objects[name] = objEntry
		return
	}
	objects[name] = objEntry
}

func (t *GCTable) fillDefaults() {
	if t.buffer.impl == nil {
		t.buffer.impl = containers.NewOneSchemaBatchBuffer(
			mpool.MB*32,
			ObjectTableAttrs,
			ObjectTableTypes,
		)
	}
}

func (t *GCTable) fetchBuffer() *batch.Batch {
	t.buffer.Lock()
	defer t.buffer.Unlock()
	return t.buffer.impl.Fetch()
}

func (t *GCTable) putBuffer(bat *batch.Batch) {
	t.buffer.Lock()
	defer t.buffer.Unlock()
	t.buffer.impl.Putback(bat, t.mp)
}

// SoftGC is to remove objectentry that can be deleted from GCTable
func (t *GCTable) SoftGC(
	ctx context.Context,
	bf *bloomfilter.BloomFilter,
	ts types.TS,
	snapShotList map[uint32]containers.Vector,
	meta *logtail.SnapshotMeta,
) ([]string, map[uint32][]types.TS) {
	var (
		bm   bitmap.Bitmap
		sels []int64
	)

	sinker := t.getSinker()
	defer sinker.Close()

	processSoftGCBatch := func(
		ctx context.Context,
		data *batch.Batch,
	) error {
		// reset bitmap for each batch
		bm.Clear()
		bm.TryExpandWithSize(data.RowCount())

		bf.Test(data.Vecs[0], func(exits bool, i int) {
			if !exits {
				bm.Add(uint64(i))
			}
		})

		// convert bitmap to slice
		sels = sels[:0]
		bitmap.ToArray(&bm, &sels)

		tmpBat := t.fetchBuffer()
		defer t.putBuffer(tmpBat)
		if err := tmpBat.Union(data, sels, t.mp); err != nil {
			return err
		}

		// shrink data
		data.Shrink(sels, true)
		return sinker.Write(ctx, tmpBat)
	}
	err := t.Process(ctx, t.tsRange.start, t.tsRange.end, t.LoadBatchData, processSoftGCBatch)
	if err != nil {
		logutil.Error("GCTable SoftGC Process failed", zap.Error(err))
		return nil, nil
	}
	err = sinker.Sync(ctx)

	if err != nil {
		logutil.Error("GCTable SoftGC Sync failed", zap.Error(err))
		return nil, nil
	}

	softGCObjects, processBats := sinker.GetResult()

	snapList := make(map[uint32][]types.TS)
	for acct, snap := range snapShotList {
		snapList[acct] = vector.MustFixedColWithTypeCheck[types.TS](snap.GetDownstreamVector())
	}
	t.Lock()
	meta.Lock()
	defer func() {
		meta.Unlock()
		t.Unlock()
	}()
	gc := make([]string, 0)
	objects := make(map[string]*ObjectEntry)
	objectsComparedAndDeleteLocked := func(
		bat *batch.Batch,
		meta *logtail.SnapshotMeta,
		snapList map[uint32][]types.TS,
		ts types.TS,
	) {
		creates := vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[1])
		deletes := vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[2])
		dbs := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[3])
		tids := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[4])
		for i := 0; i < bat.Vecs[0].Length(); i++ {
			name := bat.Vecs[0].GetStringAt(i)
			tid := tids[i]
			createTs := creates[i]
			dropTs := deletes[i]

			if dropTs.IsEmpty() && objects[name] == nil {
				object := &ObjectEntry{
					createTS: createTs,
					dropTS:   dropTs,
					db:       dbs[i],
					table:    tid,
				}
				addObjectLocked(name, object, objects)
				continue
			}
			if objects[name] != nil && objects[name].dropTS.IsEmpty() {
				objects[name].dropTS = dropTs
			}
			tsList := meta.GetSnapshotListLocked(snapList, tid)
			if tsList == nil {
				if createTs.Less(&ts) && dropTs.Less(&ts) {
					gc = append(gc, name)
					delete(objects, name)
				}
				continue
			}
			if createTs.Less(&ts) &&
				dropTs.Less(&ts) &&
				!isSnapshotRefers(&createTs, &dropTs, tsList, name) {
				gc = append(gc, name)
				delete(objects, name)
			}
		}
	}
	buffer := t.fetchBuffer()
	defer t.putBuffer(buffer)
	for _, stats := range softGCObjects {
		err = loader(ctx, t.fs, &stats, buffer, t.mp)
		if err != nil {
			logutil.Error("GCTable SoftGC loader failed", zap.Error(err))
			return nil, nil
		}
		objectsComparedAndDeleteLocked(buffer, meta, snapList, ts)
		buffer.CleanOnlyData()
	}

	for _, bat := range processBats {
		objectsComparedAndDeleteLocked(bat, meta, snapList, ts)
	}
	return gc, snapList
}

func isSnapshotRefers(createTS, dropTS *types.TS, snapVec []types.TS, name string) bool {
	if len(snapVec) == 0 {
		return false
	}
	if dropTS.IsEmpty() {
		logutil.Debug("[soft GC]Snapshot Refers",
			zap.String("name", name),
			zap.String("createTS", createTS.ToString()),
			zap.String("dropTS", createTS.ToString()))
		return true
	}
	left, right := 0, len(snapVec)-1
	for left <= right {
		mid := left + (right-left)/2
		snapTS := snapVec[mid]
		if snapTS.GreaterEq(createTS) && snapTS.Less(dropTS) {
			logutil.Debug("[soft GC]Snapshot Refers",
				zap.String("name", name),
				zap.String("snapTS", snapTS.ToString()),
				zap.String("createTS", createTS.ToString()),
				zap.String("dropTS", dropTS.ToString()))
			return true
		} else if snapTS.Less(createTS) {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return false
}

func (t *GCTable) getSinker() *engine_util.Sinker {
	return engine_util.NewSinker(
		ObjectTablePrimaryKeyIdx,
		ObjectTableAttrs,
		ObjectTableTypes,
		FSinkerFactory,
		t.mp,
		t.fs,
	)
}

func (t *GCTable) Process(
	ctx context.Context,
	start, end types.TS,
	loadNextBatch func(context.Context, *batch.Batch, *mpool.MPool) (bool, error),
	processOneBatch func(context.Context, *batch.Batch) error,
) error {
	sinker := t.getSinker()
	defer sinker.Close()

	for {
		bat := t.fetchBuffer()
		done, err := loadNextBatch(ctx, bat, t.mp)
		if err != nil || done {
			t.putBuffer(bat)
			return err
		}
		if err = processOneBatch(ctx, bat); err != nil {
			t.putBuffer(bat)
			return err
		}

		if err = sinker.Write(ctx, bat); err != nil {
			t.putBuffer(bat)
			return err
		}
	}

	stats, _ := sinker.GetResult()
	return t.doneAllBatches(ctx, start, end, stats)
}

func (t *GCTable) doneAllBatches(ctx context.Context, start, end types.TS, stats []objectio.ObjectStats) error {
	name := blockio.EncodeCheckpointMetadataFileName(GCMetaDir, PrefixGCMeta, start, end)
	ret := batch.New(false, ObjectTableMetaAttrs)
	ret.SetVector(0, vector.NewVec(ObjectTableMetaTypes[0]))
	t.files.Lock()
	for _, s := range stats {
		vector.AppendBytes(ret.GetVector(0), s[:], false, t.mp)
		t.files.stats = append(t.files.stats, s)
	}
	t.files.Unlock()
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterGC, name, t.fs)
	if err != nil {
		return err
	}
	if _, err := writer.WriteWithoutSeqnum(ret); err != nil {
		return err
	}

	_, err = writer.WriteEnd(ctx)
	return err
}

func (t *GCTable) Merge(table *GCTable) {
	t.Lock()
	defer t.Unlock()
	for _, stats := range table.files.stats {
		t.files.stats = append(t.files.stats, stats)
	}

	if t.tsRange.start.Greater(&table.tsRange.start) {
		t.tsRange.start = table.tsRange.start
	}
	if t.tsRange.end.Less(&table.tsRange.end) {
		t.tsRange.end = table.tsRange.end
	}
}

func (t *GCTable) UpdateTable(data *logtail.CheckpointData) {
	ins := data.GetObjectBatchs()
	t.Lock()
	defer t.Unlock()
	t.updateObjectListLocked(ins, t.objects)
	bat := t.fetchBuffer()
	for name, object := range t.objects {
		addObjectToBatch(bat, name, object, t.mp)
	}
}

func (t *GCTable) updateObjectListLocked(ins *containers.Batch, objects map[string]*ObjectEntry) {
	insDeleteTSVec := ins.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector()
	insCreateTSVec := ins.GetVectorByName(catalog.EntryNode_CreateAt).GetDownstreamVector()
	dbid := ins.GetVectorByName(catalog.SnapshotAttr_DBID).GetDownstreamVector()
	tid := ins.GetVectorByName(catalog.SnapshotAttr_TID).GetDownstreamVector()

	for i := 0; i < ins.Length(); i++ {
		var objectStats objectio.ObjectStats
		buf := ins.GetVectorByName(catalog.ObjectAttr_ObjectStats).Get(i).([]byte)
		objectStats.UnMarshal(buf)
		deleteTS := vector.GetFixedAtNoTypeCheck[types.TS](insDeleteTSVec, i)
		createTS := vector.GetFixedAtNoTypeCheck[types.TS](insCreateTSVec, i)
		object := &ObjectEntry{
			createTS: createTS,
			dropTS:   deleteTS,
			db:       vector.GetFixedAtNoTypeCheck[uint64](dbid, i),
			table:    vector.GetFixedAtNoTypeCheck[uint64](tid, i),
		}
		addObjectLocked(objectStats.ObjectName().String(), object, objects)
	}
}

func (t *GCTable) Close() {
	if t.buffer.impl != nil {
		t.buffer.impl.Close(t.mp)
		t.buffer.impl = nil
	}
}

func (t *GCTable) closeBatch(bs []*containers.Batch) {
	for i := range bs {
		bs[i].Close()
	}
}

// collectData collects data from memory that can be written to s3
func (t *GCTable) CollectMapData(cxt context.Context, bat *batch.Batch, mp *mpool.MPool) (bool, error) {
	if len(t.objects) == 0 {
		return true, nil
	}
	for name, entry := range t.objects {
		addObjectToBatch(bat, name, entry, mp)
	}
	t.objects = nil
	return false, nil
}
func (t *GCTable) ProcessMapBatch(
	ctx context.Context,
	sinker *engine_util.Sinker,
	data *batch.Batch,
) error {
	if err := mergesort.SortColumnsByIndex(
		data.Vecs,
		ObjectTablePrimaryKeyIdx,
		t.mp,
	); err != nil {
		return err
	}
	return nil
}

// collectData collects data from memory that can be written to s3
func (t *GCTable) LoadBatchData(cxt context.Context, bat *batch.Batch, mp *mpool.MPool) (bool, error) {
	if len(t.files.stats) == 0 {
		return true, nil
	}
	err := loader(cxt, t.fs, &t.files.stats[0], bat, mp)
	if err != nil {
		return false, err
	}
	t.files.stats = t.files.stats[:1]
	return false, nil
}

func loader(
	cxt context.Context,
	fs fileservice.FileService,
	stats *objectio.ObjectStats,
	bat *batch.Batch,
	mp *mpool.MPool,
) error {
	for id := uint32(0); id < stats.BlkCnt(); id++ {
		stats.ObjectLocation().SetID(uint16(id))
		data, _, err := blockio.LoadOneBlock(cxt, fs, stats.ObjectLocation(), objectio.SchemaData)
		if err != nil {
			return err
		}
		bat.Append(cxt, mp, data)
	}
	return nil

}

func (t *GCTable) rebuildTable(bats []*containers.Batch, idx BatchType, objects map[string]*ObjectEntry) {
	for i := 0; i < bats[idx].Length(); i++ {
		name := string(bats[idx].GetVectorByName(GCAttrObjectName).Get(i).([]byte))
		creatTS := bats[idx].GetVectorByName(GCCreateTS).Get(i).(types.TS)
		deleteTS := bats[idx].GetVectorByName(GCDeleteTS).Get(i).(types.TS)
		tid := bats[idx].GetVectorByName(GCAttrTableId).Get(i).(uint64)
		if t.objects[name] != nil {
			continue
		}
		object := &ObjectEntry{
			createTS: creatTS,
			dropTS:   deleteTS,
			table:    tid,
		}
		addObjectLocked(name, object, objects)
	}
}

func (t *GCTable) replayData(
	ctx context.Context,
	typ BatchType,
	attrs []string,
	types []types.Type,
	bats []*containers.Batch,
	bs []objectio.BlockObject,
	reader *blockio.BlockReader) (func(), error) {
	idxes := make([]uint16, len(attrs))
	for i := range attrs {
		idxes[i] = uint16(i)
	}
	mobat, release, err := reader.LoadColumns(ctx, idxes, nil, bs[typ].GetID(), common.DefaultAllocator)
	if err != nil {
		return nil, err
	}
	for i := range attrs {
		pkgVec := mobat.Vecs[i]
		var vec containers.Vector
		if pkgVec.Length() == 0 {
			vec = containers.MakeVector(types[i], common.DefaultAllocator)
		} else {
			vec = containers.ToTNVector(pkgVec, common.DefaultAllocator)
		}
		bats[typ].AddVector(attrs[i], vec)
	}
	return release, nil
}

// ReadTable reads an s3 file and replays a GCTable in memory
func (t *GCTable) ReadTable(ctx context.Context, name string, size int64, fs *objectio.ObjectFS, ts types.TS) error {
	var release1, release2 func()
	defer func() {
		if release1 != nil {
			release1()
		}
		if release2 != nil {
			release2()
		}
	}()
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
	release1, err = t.replayData(ctx, ObjectList, BlockSchemaAttr, BlockSchemaTypes, bats, bs, reader)
	if err != nil {
		return err
	}
	release2, err = t.replayData(ctx, TombstoneList, BlockSchemaAttr, BlockSchemaTypes, bats, bs, reader)
	if err != nil {
		return err
	}
	t.Lock()
	t.rebuildTable(bats, ObjectList, t.objects)
	t.Unlock()
	return nil
}

// For test

func (t *GCTable) Compare(table *GCTable) bool {
	if !t.compareObjects(t.objects, table.objects) {
		logutil.Infof("objects are not equal")
		return false
	}
	return true
}

func (t *GCTable) compareObjects(objects, compareObjects map[string]*ObjectEntry) bool {
	for name, entry := range compareObjects {
		object := objects[name]
		if object == nil {
			logutil.Infof("object %s is nil, create %v, drop %v",
				name, entry.createTS.ToString(), entry.dropTS.ToString())
			return false
		}
		if !entry.createTS.Equal(&object.createTS) {
			logutil.Infof("object %s createTS is not equal", name)
			return false
		}
	}

	return len(compareObjects) == len(objects)
}

func (t *GCTable) String() string {
	if len(t.objects) == 0 {
		return ""
	}
	var w bytes.Buffer
	_, _ = w.WriteString("objects:[\n")
	for name, entry := range t.objects {
		_, _ = w.WriteString(fmt.Sprintf("name: %s, createTS: %v ", name, entry.createTS.ToString()))
	}
	_, _ = w.WriteString("]\n")
	return w.String()
}
