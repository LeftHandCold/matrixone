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

	"github.com/matrixorigin/matrixone/pkg/common/malloc"

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
		table.buffer = containers.NewOneSchemaBatchBuffer(
			size,
			ObjectTableAttrs,
			ObjectTableTypes,
		)
	}
}

func WithMetaPrefix(prefix string) TableOption {
	return func(table *GCTable) {
		table.metaDir = prefix
	}
}

func NewGCTable(
	fs fileservice.FileService,
	mp *mpool.MPool,
	opts ...TableOption,
) *GCTable {
	table := GCTable{
		fs: fs,
		mp: mp,
	}
	for _, opt := range opts {
		opt(&table)
	}
	WithBufferSize(64 * mpool.MB)(&table)
	if table.metaDir == "" {
		table.metaDir = GCMetaDir
	}
	return &table
}

type GCTable struct {
	sync.Mutex
	mp *mpool.MPool
	fs fileservice.FileService

	buffer *containers.OneSchemaBatchBuffer

	files struct {
		sync.Mutex
		stats []objectio.ObjectStats
	}

	tsRange struct {
		start types.TS
		end   types.TS
	}

	metaDir string
}

func (t *GCTable) fillDefaults() {
	if t.buffer == nil {
		t.buffer = containers.NewOneSchemaBatchBuffer(
			mpool.MB*32,
			ObjectTableAttrs,
			ObjectTableTypes,
		)
	}
}

func (t *GCTable) fetchBuffer() *batch.Batch {
	return t.buffer.Fetch()
}

func (t *GCTable) putBuffer(bat *batch.Batch) {
	t.buffer.Putback(bat, t.mp)
}

func (t *GCTable) filterGCProcessor(
	bm *bitmap.Bitmap,
	sels *[]int64,
	bf *bloomfilter.BloomFilter,
	canGCSinker *engine_util.Sinker,
) func(context.Context, *batch.Batch, *mpool.MPool) error {
	return func(
		ctx context.Context,
		data *batch.Batch,
		mp *mpool.MPool,
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
		*sels = (*sels)[:0]
		bitmap.ToArray(bm, sels)

		tmpBat := t.fetchBuffer()
		defer t.putBuffer(tmpBat)
		if err := tmpBat.Union(data, *sels, mp); err != nil {
			return err
		}

		// shrink data
		data.Shrink(*sels, true)
		return canGCSinker.Write(ctx, tmpBat)
	}
}

// SoftGC is to remove objectentry that can be deleted from GCTable
func (t *GCTable) SoftGC(
	ctx context.Context,
	bf *bloomfilter.BloomFilter,
	ts types.TS,
	snapShotList map[uint32]containers.Vector,
	pitrs *logtail.PitrInfo,
	meta *logtail.SnapshotMeta,
) ([]string, map[uint32][]types.TS, error) {
	var (
		bm   bitmap.Bitmap
		sels []int64
	)

	// canGCSinker is used to store the data that can be gc'ed
	canGCSinker := t.getSinker(64 * malloc.MB)
	defer canGCSinker.Close()

	// cannotGCSinker is used to store the data that can not be deleted
	cannotGCSinker := t.getSinker(0)
	defer cannotGCSinker.Close()

	gcFilter := t.filterGCProcessor(
		&bm,
		&sels,
		bf,
		canGCSinker,
	)

	// this process is to filter out the data that can be gc'ed into
	// two sinkers, one is to store the data that can be gc'ed, the other
	// is to store the data that can not be gc'ed
	if err := engine_util.StreamBatchProcess(
		ctx,
		t.LoadBatchData,
		gcFilter,
		cannotGCSinker.Write,
		t.buffer,
		t.mp,
	); err != nil {
		logutil.Error(
			"GCTable-SoftGC-SINK-ERROR",
			zap.Error(err),
		)
		return nil, nil, err
	}

	// sync the data that can be gc'ed to get persisted
	// files and tail batch
	if err := canGCSinker.Sync(ctx); err != nil {
		logutil.Error(
			"GCTable-SoftGC-Sync-ERROR",
			zap.Error(err),
		)
		return nil, nil, err
	}
	// canGCObjects is a list of files of object entries that can be gc'ed
	// canGCMemTable is batches of object entries that can be gc'ed in memory
	canGCObjects, canGCMemTable := canGCSinker.GetResult()
	snapList := make(map[uint32][]types.TS)
	for accountId, snapshot := range snapShotList {
		snapList[accountId] = vector.MustFixedColWithTypeCheck[types.TS](snapshot.GetDownstreamVector())
	}

	t.Lock()
	meta.Lock()
	defer func() {
		meta.Unlock()
		t.Unlock()
	}()
	objects := make(map[string]*ObjectEntry)
	var gcFiles []string
	objectsComparedAndDeleteLocked := func(
		bat *batch.Batch,
		meta *logtail.SnapshotMeta,
		snapList map[uint32][]types.TS,
		pitrList *logtail.PitrInfo,
		ts types.TS,
	) error {
		bm.Clear()
		bm.TryExpandWithSize(bat.RowCount())

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
				objects[name] = object
				continue
			}
			if objects[name] != nil && objects[name].dropTS.IsEmpty() {
				objects[name].dropTS = dropTs
			}
			tsList := meta.GetSnapshotListLocked(snapList, tid)
			pitr := meta.GetPitrLocked(pitrList, dbs[i], tid)
			if tsList == nil && pitr.IsEmpty() {
				if createTs.LT(&ts) && dropTs.LT(&ts) {
					gcFiles = append(gcFiles, name)
					delete(objects, name)
					bm.Add(uint64(i))
				}
				continue
			}
			if createTs.LT(&ts) &&
				dropTs.LT(&ts) &&
				!isSnapshotRefers(&createTs, &dropTs, tsList, &pitr, name) {
				gcFiles = append(gcFiles, name)
				delete(objects, name)
				bm.Add(uint64(i))
			}
		}
		// convert bitmap to slice
		sels = sels[:0]
		bitmap.ToArray(&bm, &sels)
		bat.Shrink(sels, true)

		return cannotGCSinker.Write(ctx, bat)
	}

	buffer := t.fetchBuffer()
	defer t.putBuffer(buffer)
	for _, stats := range canGCObjects {
		if err := loader(
			ctx, t.fs, &stats, buffer, t.mp,
		); err != nil {
			logutil.Error(
				"GCTable-SoftGC-LOAD-ERROR",
				zap.Error(err),
			)
			return nil, nil, err
		}
		if err := objectsComparedAndDeleteLocked(
			buffer, meta, snapList, pitrs, ts,
		); err != nil {
			logutil.Error(
				"GCTable-SoftGC-CMP-DEL-ERROR",
				zap.Error(err),
			)
			return nil, nil, err
		}
		buffer.CleanOnlyData()
	}

	for _, bat := range canGCMemTable {
		if err := objectsComparedAndDeleteLocked(
			bat, meta, snapList, pitrs, ts,
		); err != nil {
			logutil.Error(
				"GCTable-SoftGC-CMP-DEL-2-ERROR",
				zap.Error(err),
			)
			return nil, nil, err
		}
	}
	if _, err := collectMapData(objects, buffer, t.mp); err != nil {
		logutil.Error(
			"GCTable-SoftGC-COLLECT-ERROR",
			zap.Error(err),
		)
		return nil, nil, err
	}
	if err := cannotGCSinker.Write(ctx, buffer); err != nil {
		logutil.Error(
			"GCTable-SoftGC-Write-ERROR",
			zap.Error(err),
		)
		return nil, nil, err
	}
	if err := cannotGCSinker.Sync(ctx); err != nil {
		logutil.Error(
			"GCTable-SoftGC-Sync-ERROR",
			zap.Error(err),
		)
		return nil, nil, err
	}

	gcStats, _ := cannotGCSinker.GetResult()
	err := t.doneAllBatches(ctx, &t.tsRange.start, &t.tsRange.end, gcStats)
	t.files.stats = t.files.stats[0:]
	t.files.stats = append(t.files.stats, gcStats...)
	return gcFiles, snapList, err
}

func isSnapshotRefers(createTS, dropTS *types.TS, snapVec []types.TS, pitr *types.TS, name string) bool {
	if len(snapVec) == 0 &&
		pitr.IsEmpty() {
		return false
	}
	if dropTS.IsEmpty() {
		logutil.Debug("[soft GC]Snapshot Refers",
			zap.String("name", name),
			zap.String("createTS", createTS.ToString()),
			zap.String("dropTS", createTS.ToString()))
		return true
	}
	if !pitr.IsEmpty() {
		if dropTS.GT(pitr) {
			logutil.Info("[soft GC]Pitr Refers",
				zap.String("name", name),
				zap.String("snapTS", pitr.ToString()),
				zap.String("createTS", createTS.ToString()),
				zap.String("dropTS", dropTS.ToString()))
			return true
		}
	}

	left, right := 0, len(snapVec)-1
	for left <= right {
		mid := left + (right-left)/2
		snapTS := snapVec[mid]
		if snapTS.GE(createTS) && snapTS.LT(dropTS) {
			logutil.Debug("[soft GC]Snapshot Refers",
				zap.String("name", name),
				zap.String("snapTS", snapTS.ToString()),
				zap.String("createTS", createTS.ToString()),
				zap.String("dropTS", dropTS.ToString()))
			return true
		} else if snapTS.LT(createTS) {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return false
}

func (t *GCTable) getSinker(tailSize int) *engine_util.Sinker {
	return engine_util.NewSinker(
		ObjectTablePrimaryKeyIdx,
		ObjectTableAttrs,
		ObjectTableTypes,
		FSinkerFactory,
		t.mp,
		t.fs,
		engine_util.WithTailSizeCap(tailSize),
		engine_util.WithBuffer(t.buffer, false),
	)
}

func (t *GCTable) Process(
	ctx context.Context,
	start, end *types.TS,
	loadNextBatch func(context.Context, *batch.Batch, *mpool.MPool) (bool, error),
	processOneBatch func(context.Context, *batch.Batch, *mpool.MPool) error,
) error {

	t.tsRange.start = *start
	t.tsRange.end = *end

	sinker := t.getSinker(0)
	defer sinker.Close()
	if err := engine_util.StreamBatchProcess(
		ctx,
		loadNextBatch,
		processOneBatch,
		sinker.Write,
		t.buffer,
		t.mp,
	); err != nil {
		return err
	}

	if err := sinker.Sync(ctx); err != nil {
		return err
	}
	stats, _ := sinker.GetResult()
	return t.doneAllBatches(ctx, start, end, stats)
}

func (t *GCTable) doneAllBatches(ctx context.Context, start, end *types.TS, stats []objectio.ObjectStats) error {
	name := blockio.EncodeCheckpointMetadataFileName(t.metaDir, PrefixGCMeta, *start, *end)
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

	if t.tsRange.start.GT(&table.tsRange.start) {
		t.tsRange.start = table.tsRange.start
	}
	if t.tsRange.end.LT(&table.tsRange.end) {
		t.tsRange.end = table.tsRange.end
	}
}

func (t *GCTable) UpdateTable(data *logtail.CheckpointData) {
	ins := data.GetObjectBatchs()
	t.Lock()
	defer t.Unlock()
	t.updateObjectListLocked(ins, t.objects)
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
		objects[objectStats.ObjectName().String()] = object
	}
}

func (t *GCTable) Close() {
	if t.buffer != nil {
		t.buffer.Close(t.mp)
		t.buffer = nil
	}
}

// collectData collects data from memory that can be written to s3
func collectMapData(
	objects map[string]*ObjectEntry,
	bat *batch.Batch,
	mp *mpool.MPool,
) (bool, error) {
	if len(objects) == 0 {
		return true, nil
	}
	for name, entry := range objects {
		addObjectToBatch(bat, name, entry, mp)
	}
	batch.SetLength(bat, len(objects))
	return false, nil
}

func (t *GCTable) ProcessMapBatch(
	ctx context.Context,
	data *batch.Batch,
	mp *mpool.MPool,
) error {
	if err := mergesort.SortColumnsByIndex(
		data.Vecs,
		ObjectTablePrimaryKeyIdx,
		mp,
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
	t.files.stats = t.files.stats[1:]
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

func (t *GCTable) rebuildTable(bat *batch.Batch) {
	t.files.Lock()
	defer t.files.Unlock()
	for i := 0; i < bat.Vecs[0].Length(); i++ {
		stats := objectio.NewObjectStats()
		stats.UnMarshal(bat.Vecs[0].GetRawBytesAt(i))
		t.files.stats = append(t.files.stats, *stats)
	}
}

func (t *GCTable) replayData(
	ctx context.Context,
	bat *batch.Batch,
	bs []objectio.BlockObject,
	reader *blockio.BlockReader) (func(), error) {
	idxes := []uint16{0}
	var release func()
	var err error
	bat, release, err = reader.LoadColumns(ctx, idxes, nil, bs[0].GetID(), common.DefaultAllocator)
	if err != nil {
		return nil, err
	}
	return release, nil
}

// ReadTable reads an s3 file and replays a GCTable in memory
func (t *GCTable) ReadTable(ctx context.Context, name string, size int64, fs *objectio.ObjectFS, ts types.TS) error {
	var release1 func()
	defer func() {
		if release1 != nil {
			release1()
		}
	}()
	start, end, _ := blockio.DecodeGCMetadataFileName(name)
	t.tsRange.start = start
	t.tsRange.end = end
	reader, err := blockio.NewFileReaderNoCache(fs.Service, name)
	if err != nil {
		return err
	}
	bs, err := reader.LoadAllBlocks(ctx, common.DefaultAllocator)
	if err != nil {
		return err
	}
	buffer := t.fetchBuffer()
	defer t.putBuffer(buffer)
	release1, err = t.replayData(ctx, buffer, bs, reader)
	if err != nil {
		return err
	}
	t.Lock()
	t.rebuildTable(buffer)
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