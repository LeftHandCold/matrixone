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

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"

	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"

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
	stats    *objectio.ObjectStats
	createTS types.TS
	dropTS   types.TS
	db       uint64
	table    uint64
}

type WindowOption func(*GCWindow)

func WithMetaPrefix(prefix string) WindowOption {
	return func(table *GCWindow) {
		table.metaDir = prefix
	}
}

func NewGCWindow(
	mp *mpool.MPool,
	fs fileservice.FileService,
	opts ...WindowOption,
) *GCWindow {
	window := GCWindow{
		mp: mp,
		fs: fs,
	}
	for _, opt := range opts {
		opt(&window)
	}
	if window.metaDir == "" {
		window.metaDir = GCMetaDir
	}
	return &window
}

type GCWindow struct {
	metaDir string
	mp      *mpool.MPool
	fs      fileservice.FileService

	files []objectio.ObjectStats

	tsRange struct {
		start types.TS
		end   types.TS
	}
}

func (t *GCWindow) MakeFilesReader(
	ctx context.Context,
	fs fileservice.FileService,
) engine.Reader {
	return engine_util.SimpleMultiObjectsReader(
		ctx,
		fs,
		t.files,
		timestamp.Timestamp{},
		engine_util.WithColumns(
			ObjectTableSeqnums,
			ObjectTableTypes,
		),
	)
}

// ExecuteGlobalCheckpointBasedGC is to remove objectentry that can be deleted from GCWindow
// it will refresh the files in GCWindow with the files that can not be GC'ed
// it will return the files that could be GC'ed
func (t *GCWindow) ExecuteGlobalCheckpointBasedGC(
	ctx context.Context,
	gCkp *checkpoint.CheckpointEntry,
	accountSnapshots map[uint32][]types.TS,
	pitrs *logtail.PitrInfo,
	snapshotMeta *logtail.SnapshotMeta,
	buffer *containers.OneSchemaBatchBuffer,
	cacheSize int,
	mp *mpool.MPool,
	fs fileservice.FileService,
) ([]string, error) {

	sourcer := t.MakeFilesReader(ctx, fs)

	gcTS := gCkp.GetEnd()
	job := NewCheckpointBasedGCJob(
		&gcTS,
		gCkp.GetLocation(),
		sourcer,
		pitrs,
		accountSnapshots,
		snapshotMeta,
		buffer,
		false,
		mp,
		fs,
		WithGCJobCoarseConfig(0, 0, cacheSize),
	)
	defer job.Close()

	if err := job.Execute(ctx); err != nil {
		return nil, err
	}

	filesToGC, filesNotGC := job.Result()
	if err := t.writeMetaForRemainings(
		ctx, filesNotGC,
	); err != nil {
		return nil, err
	}

	t.files = filesNotGC
	return filesToGC, nil
}

func (t *GCWindow) ScanCheckpoints(
	ctx context.Context,
	ckps []*checkpoint.CheckpointEntry,
	collectCkpData func(*checkpoint.CheckpointEntry) (*logtail.CheckpointData, error),
	processCkpData func(*checkpoint.CheckpointEntry, *logtail.CheckpointData) error,
	fineProcess func() error,
	buffer *containers.OneSchemaBatchBuffer,
) error {
	if len(ckps) == 0 {
		return nil
	}
	start := ckps[0].GetStart()
	end := ckps[len(ckps)-1].GetEnd()
	getOneBatch := func(cxt context.Context, bat *batch.Batch, mp *mpool.MPool) (bool, error) {
		if len(ckps) == 0 {
			return true, nil
		}
		data, err := collectCkpData(ckps[0])
		if err != nil {
			return false, err
		}
		if processCkpData != nil {
			err = processCkpData(ckps[0], data)
			if err != nil {
				return false, err
			}
		}
		objects := make(map[string]*ObjectEntry)
		collectObjectsWithCkp(data, objects)
		err = collectMapData(objects, bat, mp)
		if err != nil {
			return false, err
		}
		ckps = ckps[1:]
		return false, nil
	}
	sinker := t.getSinker(0, buffer)
	defer sinker.Close()
	if err := engine_util.StreamBatchProcess(
		ctx,
		getOneBatch,
		t.sortOneBatch,
		sinker.Write,
		buffer,
		t.mp,
	); err != nil {
		logutil.Error(
			"GCWindow-createInput-SINK-ERROR",
			zap.Error(err),
		)
		return err
	}
	t.tsRange.start = start
	t.tsRange.end = end

	if fineProcess != nil {
		if err := fineProcess(); err != nil {
			return err
		}
	}

	if err := sinker.Sync(ctx); err != nil {
		return err
	}
	newFiles, _ := sinker.GetResult()
	if err := t.writeMetaForRemainings(
		ctx, newFiles,
	); err != nil {
		return err
	}
	t.files = append(t.files, newFiles...)
	return nil
}

func isSnapshotRefers(
	obj *objectio.ObjectStats,
	pitr, createTS, dropTS *types.TS,
	snapshots []types.TS,
) bool {
	// no snapshot and no pitr
	if len(snapshots) == 0 && (pitr == nil || pitr.IsEmpty()) {
		return false
	}

	// if dropTS is empty, it means the object is not dropped
	if dropTS.IsEmpty() {
		common.DoIfDebugEnabled(func() {
			logutil.Debug(
				"GCJOB-DEBUG-1",
				zap.String("obj", obj.ObjectName().String()),
				zap.String("create-ts", createTS.ToString()),
				zap.String("drop-ts", createTS.ToString()),
			)
		})
		return true
	}

	// if pitr is not empty, and pitr is greater than dropTS, it means the object is not dropped
	if pitr != nil && !pitr.IsEmpty() {
		if dropTS.GT(pitr) {
			common.DoIfDebugEnabled(func() {
				logutil.Debug(
					"GCJOB-PITR-PIN",
					zap.String("name", obj.ObjectName().String()),
					zap.String("pitr", pitr.ToString()),
					zap.String("create-ts", createTS.ToString()),
					zap.String("drop-ts", dropTS.ToString()),
				)
			})
			return true
		}
	}

	left, right := 0, len(snapshots)-1
	for left <= right {
		mid := left + (right-left)/2
		snapTS := snapshots[mid]
		if snapTS.GE(createTS) && snapTS.LT(dropTS) {
			common.DoIfDebugEnabled(func() {
				logutil.Debug(
					"GCJOB-DEBUG-2",
					zap.String("name", obj.ObjectName().String()),
					zap.String("pitr", snapTS.ToString()),
					zap.String("create-ts", createTS.ToString()),
					zap.String("drop-ts", dropTS.ToString()),
				)
			})
			return true
		} else if snapTS.LT(createTS) {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return false
}

func (t *GCWindow) getSinker(
	tailSize int,
	buffer *containers.OneSchemaBatchBuffer,
) *engine_util.Sinker {
	return engine_util.NewSinker(
		ObjectTablePrimaryKeyIdx,
		ObjectTableAttrs,
		ObjectTableTypes,
		FSinkerFactory,
		t.mp,
		t.fs,
		engine_util.WithTailSizeCap(tailSize),
		engine_util.WithBuffer(buffer, false),
	)
}

func (t *GCWindow) writeMetaForRemainings(
	ctx context.Context,
	stats []objectio.ObjectStats,
) error {
	name := blockio.EncodeCheckpointMetadataFileName(
		t.metaDir, PrefixGCMeta, t.tsRange.start, t.tsRange.end,
	)
	ret := batch.NewWithSchema(
		false, false, ObjectTableMetaAttrs, ObjectTableMetaTypes,
	)
	defer ret.Clean(t.mp)
	for _, s := range stats {
		if err := vector.AppendBytes(
			ret.GetVector(0), s[:], false, t.mp,
		); err != nil {
			return err
		}
	}
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

func (t *GCWindow) Merge(o *GCWindow) {
	if o == nil || (o.tsRange.start.IsEmpty() && o.tsRange.end.IsEmpty()) {
		return
	}
	if t.tsRange.start.IsEmpty() && t.tsRange.end.IsEmpty() {
		t.tsRange.start = o.tsRange.start
		t.tsRange.end = o.tsRange.end
		t.files = append(t.files, o.files...)
		return
	}

	for _, file := range o.files {
		t.files = append(t.files, file)
	}

	if t.tsRange.start.GT(&o.tsRange.start) {
		t.tsRange.start = o.tsRange.start
	}
	if t.tsRange.end.LT(&o.tsRange.end) {
		t.tsRange.end = o.tsRange.end
	}
}

func collectObjectsWithCkp(data *logtail.CheckpointData, objects map[string]*ObjectEntry) {
	ins := data.GetObjectBatchs()
	insDeleteTSVec := ins.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector()
	insCreateTSVec := ins.GetVectorByName(catalog.EntryNode_CreateAt).GetDownstreamVector()
	dbid := ins.GetVectorByName(catalog.SnapshotAttr_DBID).GetDownstreamVector()
	tableID := ins.GetVectorByName(catalog.SnapshotAttr_TID).GetDownstreamVector()

	for i := 0; i < ins.Length(); i++ {
		buf := ins.GetVectorByName(catalog.ObjectAttr_ObjectStats).Get(i).([]byte)
		stats := (objectio.ObjectStats)(buf)
		name := stats.ObjectName().String()
		deleteTS := vector.GetFixedAtNoTypeCheck[types.TS](insDeleteTSVec, i)
		createTS := vector.GetFixedAtNoTypeCheck[types.TS](insCreateTSVec, i)
		object := &ObjectEntry{
			stats:    &stats,
			createTS: createTS,
			dropTS:   deleteTS,
			db:       vector.GetFixedAtNoTypeCheck[uint64](dbid, i),
			table:    vector.GetFixedAtNoTypeCheck[uint64](tableID, i),
		}
		objects[name] = object
	}
}

func (t *GCWindow) Close() {
	t.files = nil
}

// collectData collects data from memory that can be written to s3
func collectMapData(
	objects map[string]*ObjectEntry,
	bat *batch.Batch,
	mp *mpool.MPool,
) error {
	if len(objects) == 0 {
		return nil
	}
	for _, entry := range objects {
		err := addObjectToBatch(bat, entry.stats, entry, mp)
		if err != nil {
			return err
		}
	}
	batch.SetLength(bat, len(objects))
	return nil
}

func (t *GCWindow) sortOneBatch(
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
func (t *GCWindow) LoadBatchData(
	ctx context.Context,
	_ []string,
	_ *plan.Expr,
	mp *mpool.MPool,
	bat *batch.Batch,
) (bool, error) {
	if len(t.files) == 0 {
		return true, nil
	}
	bat.CleanOnlyData()
	pint := "LoadBatchData is "
	for _, s := range t.files {
		pint += s.ObjectName().String() + ";"
	}
	logutil.Infof(pint)
	err := loader(ctx, t.fs, &t.files[0], bat, mp)
	if err != nil {
		return false, err
	}
	t.files = t.files[1:]
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

func (t *GCWindow) rebuildTable(bat *batch.Batch) {
	for i := 0; i < bat.Vecs[0].Length(); i++ {
		var stats objectio.ObjectStats
		stats.UnMarshal(bat.Vecs[0].GetRawBytesAt(i))
		t.files = append(t.files, stats)
	}
}

func (t *GCWindow) replayData(
	ctx context.Context,
	bs []objectio.BlockObject,
	reader *blockio.BlockReader) (*batch.Batch, func(), error) {
	idxes := []uint16{0}
	bat, release, err := reader.LoadColumns(ctx, idxes, nil, bs[0].GetID(), common.DefaultAllocator)
	if err != nil {
		return nil, nil, err
	}
	return bat, release, nil
}

// ReadTable reads an s3 file and replays a GCWindow in memory
func (t *GCWindow) ReadTable(ctx context.Context, name string, size int64, fs *objectio.ObjectFS, ts types.TS) error {
	var release1 func()
	var buffer *batch.Batch
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
	buffer, release1, err = t.replayData(ctx, bs, reader)
	if err != nil {
		return err
	}
	t.rebuildTable(buffer)
	return nil
}

// For test

func (t *GCWindow) Compare(
	table *GCWindow,
	buffer *containers.OneSchemaBatchBuffer,
) (
	map[string]*ObjectEntry,
	map[string]*ObjectEntry,
	bool,
) {
	if buffer == nil {
		buffer = MakeGCWindowBuffer(mpool.MB)
		defer buffer.Close(t.mp)
	}
	bat := buffer.Fetch()
	defer buffer.Putback(bat, t.mp)

	objects := make(map[string]*ObjectEntry)
	objects2 := make(map[string]*ObjectEntry)

	buildObjects := func(table *GCWindow,
		objects map[string]*ObjectEntry,
		loadfn func(context.Context, []string, *plan.Expr, *mpool.MPool, *batch.Batch) (bool, error),
	) error {
		for {
			bat.CleanOnlyData()
			done, err := loadfn(context.Background(), nil, nil, t.mp, bat)
			if err != nil {
				logutil.Infof("load data error")
				return err
			}

			if done {
				break
			}

			createTSs := vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[1])
			deleteTSs := vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[2])
			dbs := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[3])
			tableIDs := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[4])
			for i := 0; i < bat.Vecs[0].Length(); i++ {
				buf := bat.Vecs[0].GetRawBytesAt(i)
				stats := (objectio.ObjectStats)(buf)
				name := stats.ObjectName().String()
				tableID := tableIDs[i]
				createTS := createTSs[i]
				dropTS := deleteTSs[i]
				object := &ObjectEntry{
					createTS: createTS,
					dropTS:   dropTS,
					db:       dbs[i],
					table:    tableID,
				}
				objects[name] = object
			}
		}
		return nil
	}
	buildObjects(t, objects, t.LoadBatchData)
	buildObjects(table, objects2, table.LoadBatchData)
	if !t.compareObjects(objects, objects2) {
		logutil.Infof("objects are not equal")
		return objects, objects2, false
	}
	logutil.Infof("objects len %d", len(objects))
	return objects, objects2, true
}

func (t *GCWindow) compareObjects(objects, compareObjects map[string]*ObjectEntry) bool {
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

func (t *GCWindow) String(objects map[string]*ObjectEntry) string {
	if len(objects) == 0 {
		return ""
	}
	var w bytes.Buffer
	_, _ = w.WriteString("objects:[\n")
	for name, entry := range objects {
		_, _ = w.WriteString(fmt.Sprintf("name: %s, createTS: %v ", name, entry.createTS.ToString()))
	}
	_, _ = w.WriteString("]\n")
	return w.String()
	return ""
}
