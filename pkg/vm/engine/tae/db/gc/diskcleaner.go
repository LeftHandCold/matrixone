package gc

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

// TODO
// TO BE REMOVED
func tempSeekLT(
	client checkpoint.Client,
	startTs types.TS,
	_ int,
) (entries []*checkpoint.CheckpointEntry) {
	all := client.GetAllCheckpoints()
	if len(all) == 0 {
		return
	}
	if startTs.IsEmpty() {
		entries = append(entries, all...)
		return
	}
	for _, ckp := range all {
		if ckp.GetStart().Greater(startTs) {
			entries = append(entries, ckp)
			return
		}
	}
	return
}

const (
	MessgeReplay = iota
	MessgeNormal
)

type diskCleaner struct {
	fs        *objectio.ObjectFS
	ckpClient checkpoint.Client
	catalog   *catalog.Catalog

	maxConsumed atomic.Pointer[checkpoint.CheckpointEntry]
	inputs      struct {
		sync.RWMutex
		tables []GCTable
	}

	processQueue sm.Queue

	onceStart sync.Once
	onceStop  sync.Once
}

func NewDiskCleaner(
	fs *objectio.ObjectFS,
	ckpClient checkpoint.Client,
	catalog *catalog.Catalog,
) *diskCleaner {
	cleaner := &diskCleaner{
		fs:        fs,
		ckpClient: ckpClient,
		catalog:   catalog,
	}
	cleaner.processQueue = sm.NewSafeQueue(10000, 1000, cleaner.process)
	return cleaner
}

func (cleaner *diskCleaner) JobFactory(ctx context.Context) (err error) {
	return cleaner.tryClean(ctx)
}

func (cleaner *diskCleaner) Replay() (err error) {
	cleaner.tryReplay()
	return nil
}

func (cleaner *diskCleaner) tryReplay() {
	if _, err := cleaner.processQueue.Enqueue(MessgeReplay); err != nil {
		panic(err)
	}
}

func (cleaner *diskCleaner) tryClean(ctx context.Context) (err error) {
	_, err = cleaner.processQueue.Enqueue(MessgeNormal)
	return
}

func (cleaner *diskCleaner) replay() error {
	dirs, err := cleaner.fs.ListDir(GCMetaDir)
	if err != nil {
		return err
	}
	if len(dirs) == 0 {
		return nil
	}
	jobs := make([]*tasks.Job, len(dirs))
	jobScheduler := tasks.NewParallelJobScheduler(100)
	defer jobScheduler.Stop()
	makeJob := func(i int) (job *tasks.Job) {
		dir := dirs[i]
		exec := func(ctx context.Context) (result *tasks.JobResult) {
			result = &tasks.JobResult{}
			table := NewGCTable()
			err := table.ReadTable(ctx, GCMetaDir+dir.Name, dir.Size, cleaner.fs)
			if err != nil {
				result.Err = err
				return
			}
			cleaner.updateInputs(table)
			return
		}
		job = tasks.NewJob(
			fmt.Sprintf("load-%s", dir.Name),
			context.Background(),
			exec)
		return
	}

	for i := range dirs {
		jobs[i] = makeJob(i)
		if err = jobScheduler.Schedule(jobs[i]); err != nil {
			return err
		}
	}

	for _, job := range jobs {
		result := job.WaitDone()
		if err = result.Err; err != nil {
			return err
		}
	}
	return nil
}

func (cleaner *diskCleaner) process(items ...any) {
	if items[0].(int) == MessgeReplay {
		cleaner.replay()
		if len(items) == 1 {
			return
		}
	}

	var ts types.TS
	maxConsumed := cleaner.maxConsumed.Load()
	if maxConsumed != nil {
		ts = maxConsumed.GetStart()
	}

	// TODO: implemnet ICKPSeekLT
	// candidates := cleaner.ckpClient.ICKPSeekLT(ts, 1)
	candidates := tempSeekLT(cleaner.ckpClient, ts, 10)

	if len(candidates) == 0 {
		return
	}

	var input GCTable
	var err error
	if input, err = cleaner.createNewInput(candidates); err != nil {
		logutil.Errorf("processing clean %s: %v", candidates[0].String(), err)
		// TODO
		return
	}
	cleaner.updateInputs(input)
	cleaner.updateMaxConsumed(candidates[len(candidates)-1])

	// TODO:
	task := NewGCTask(cleaner.fs)
	gc := cleaner.softGC()
	err = task.ExecDelete(gc)
	if err != nil {
		logutil.Errorf("processing delete: %v", err)
		return
	}

}

func (cleaner *diskCleaner) collectCkpData(
	ckp *checkpoint.CheckpointEntry,
) (data *logtail.CheckpointData, err error) {
	factory := logtail.IncrementalCheckpointDataFactory(
		ckp.GetStart(),
		ckp.GetEnd(),
	)
	data, err = factory(cleaner.catalog)
	return
}

func (cleaner *diskCleaner) createNewInput(
	ckps []*checkpoint.CheckpointEntry) (input GCTable, err error) {
	input = NewGCTable()
	var data *logtail.CheckpointData
	for _, candidate := range ckps {
		data, err = cleaner.collectCkpData(candidate)
		if err != nil {
			logutil.Errorf("processing clean %s: %v", candidate.String(), err)
			// TODO
			return
		}
		defer data.Close()
		input.UpdateTable(data)
	}
	_, err = input.SaveTable(
		ckps[0].GetStart(),
		ckps[len(ckps)-1].GetEnd(),
		cleaner.fs,
	)
	return
}

func (cleaner *diskCleaner) softGC() []string {
	cleaner.inputs.Lock()
	defer cleaner.inputs.Unlock()
	mergeTable := NewGCTable()
	for _, table := range cleaner.inputs.tables {
		mergeTable.Merge(table)
	}
	gc := mergeTable.SoftGC()
	cleaner.inputs.tables = make([]GCTable, 0)
	cleaner.inputs.tables = append(cleaner.inputs.tables, mergeTable)
	return gc
}

func (cleaner *diskCleaner) updateMaxConsumed(e *checkpoint.CheckpointEntry) {
	cleaner.maxConsumed.Store(e)
}

func (cleaner *diskCleaner) updateInputs(input GCTable) {
	cleaner.inputs.Lock()
	defer cleaner.inputs.Unlock()
	cleaner.inputs.tables = append(cleaner.inputs.tables, input)
}

func (cleaner *diskCleaner) Start() {
	cleaner.onceStart.Do(func() {
		cleaner.processQueue.Start()
		cleaner.tryReplay()
	})
}

func (cleaner *diskCleaner) Stop() {
	cleaner.onceStop.Do(func() {
		cleaner.processQueue.Stop()
	})
}
