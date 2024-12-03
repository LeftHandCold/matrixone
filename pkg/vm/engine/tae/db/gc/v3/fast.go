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
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/store"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"go.uber.org/zap"
)

type fastCleaner struct {
	ctx context.Context

	sid string

	mp *mpool.MPool
	fs *objectio.ObjectFS

	checkpointCli checkpoint.RunnerReader
	deleter       *Deleter

	watermarks struct {
		// scanWaterMark is the watermark of the incremental checkpoint which has been
		// scanned by the cleaner. After the cleaner scans the checkpoint, it
		// records all the object-list found in the checkpoint into some GC-specific
		// files. The scanWaterMark is used to record the end of the checkpoint.
		// For example:
		// Incremental checkpoint: [t100, t200), [t200, t300), [t300, t400)
		// scanWaterMark: [t100, t200)
		// remainingObjects: windows: [t100, t200), [f1, f2, f3]
		// The cleaner will scan the checkpoint [t200, t300) next time. Then:
		// scanWaterMark: [t100, t200), [t200, t300)
		// remainingObjects: windows:
		// {[t100, t200), [f1, f2, f3]}, {[t200, t300), [f4, f5, f6]}
		scanWaterMark atomic.Pointer[checkpoint.CheckpointEntry]
	}

	options struct {
		gcEnabled           atomic.Bool
		checkEnabled        atomic.Bool
		gcCheckpointEnabled atomic.Bool
	}

	config struct {
		canGCCacheSize          int
		maxMergeCheckpointCount int
		estimateRows            int
		probility               float64
	}

	// checker is to check whether the checkpoint can be consumed
	checker struct {
		sync.RWMutex
		extras map[string]func(item any) bool
	}

	mutation struct {
		sync.Mutex
		taskState struct {
			id        uint64
			name      string
			startTime time.Time
		}
		scanned      *GCWindow
		metaFiles    map[string]GCMetaFile
		snapshotMeta *logtail.SnapshotMeta
		replayDone   bool
	}
}

type FastCleanerOption func(cleaner *fastCleaner)

func (c *fastCleaner) GetCheckpointGCWaterMark() *types.TS {
	return &types.TS{}
}

func NewFastCleaner(
	ctx context.Context,
	sid string,
	fs *objectio.ObjectFS,
	checkpointCli checkpoint.RunnerReader,
	opts ...FastCleanerOption,
) Cleaner {
	cleaner := &fastCleaner{
		ctx:           ctx,
		fs:            fs,
		checkpointCli: checkpointCli,
	}
	for _, opt := range opts {
		opt(cleaner)
	}
	cleaner.deleter = NewDeleter(fs)
	cleaner.options.gcEnabled.Store(true)
	cleaner.mp = common.CheckpointAllocator
	cleaner.checker.extras = make(map[string]func(item any) bool)
	cleaner.mutation.metaFiles = make(map[string]GCMetaFile)
	cleaner.mutation.snapshotMeta = logtail.NewSnapshotMeta()
	return cleaner
}

func (c *fastCleaner) Stop() {
	c.mutation.Lock()
	defer c.mutation.Unlock()
	c.mutation.scanned = nil
	c.mutation.metaFiles = nil
	c.mutation.snapshotMeta = nil
}

func (c *fastCleaner) GetMPool() *mpool.MPool {
	return c.mp
}

func (c *fastCleaner) SetTid(tid uint64) {
	c.mutation.Lock()
	defer c.mutation.Unlock()
	c.mutation.snapshotMeta.SetTid(tid)
}

func (c *fastCleaner) EnableGC() {
	c.options.gcEnabled.Store(true)
}

func (c *fastCleaner) DisableGC() {
	c.options.gcEnabled.Store(false)
}

func (c *fastCleaner) GCEnabled() bool {
	return c.options.gcEnabled.Load()
}

func (c *fastCleaner) GCCheckpointEnabled() bool {
	return c.options.gcCheckpointEnabled.Load()
}

func (c *fastCleaner) EnableCheck() {
	c.options.checkEnabled.Store(true)
}
func (c *fastCleaner) DisableCheck() {
	c.options.checkEnabled.Store(false)
}

func (c *fastCleaner) CheckEnabled() bool {
	return c.options.checkEnabled.Load()
}

func (c *fastCleaner) StartMutationTask(name string) {
	c.mutation.Lock()
	c.mutation.taskState.id++
	c.mutation.taskState.name = fmt.Sprintf("%s-%d", name, c.mutation.taskState.id)
	c.mutation.taskState.startTime = time.Now()
	logutil.Info(
		"GC-Task-Started",
		zap.String("task", c.TaskNameLocked()),
	)
}

func (c *fastCleaner) StopMutationTask() {
	logutil.Info(
		"GC-Task-Done",
		zap.String("task", c.TaskNameLocked()),
		zap.Duration("duration", time.Since(c.mutation.taskState.startTime)),
	)
	c.mutation.taskState.name = ""
	c.mutation.Unlock()
}

func (c *fastCleaner) TaskNameLocked() string {
	return c.mutation.taskState.name
}

func (c *fastCleaner) Replay() (err error) {
	return
}

func (c *fastCleaner) GetCheckpointMetaFiles() map[string]struct{} {
	return c.checkpointCli.GetCheckpointMetaFiles()
}

func (c *fastCleaner) updateScanWaterMark(e *checkpoint.CheckpointEntry) {
	c.watermarks.scanWaterMark.Store(e)
}

func (c *fastCleaner) mutAddScannedLocked(window *GCWindow) {
	if c.mutation.scanned == nil {
		c.mutation.scanned = window
	} else {
		c.mutation.scanned.Merge(window)
		window.Close()
	}
}

func (c *fastCleaner) GetScanWaterMark() *checkpoint.CheckpointEntry {
	return c.watermarks.scanWaterMark.Load()
}

func (c *fastCleaner) GetMinMerged() *checkpoint.CheckpointEntry {
	return c.GetScanWaterMark()
}

func (c *fastCleaner) GetScannedWindow() *GCWindow {
	c.mutation.Lock()
	defer c.mutation.Unlock()
	return c.mutation.scanned
}

func (c *fastCleaner) GetScannedWindowLocked() *GCWindow {
	return c.mutation.scanned
}

func (c *fastCleaner) CloneMetaFilesLocked() map[string]GCMetaFile {
	metaFiles := make(map[string]GCMetaFile, len(c.mutation.metaFiles))
	for k, v := range c.mutation.metaFiles {
		metaFiles[k] = v
	}
	return metaFiles
}

func (c *fastCleaner) deleteStaleSnapshotFilesLocked() error {
	var (
		maxSnapEnd  types.TS
		maxSnapFile string

		maxAcctEnd  types.TS
		maxAcctFile string

		err error
	)

	metaFiles := c.CloneMetaFilesLocked()

	prevNum := len(metaFiles)

	doDeleteFileFn := func(
		thisFile string, thisTS *types.TS,
		maxFile string, maxTS *types.TS,
	) (
		newMaxFile string,
		newMaxTS types.TS,
		err error,
	) {
		if maxFile == "" {
			newMaxFile = thisFile
			newMaxTS = *thisTS
			logutil.Info(
				"GC-TRACE-DELETE-SNAPSHOT-FILE",
				zap.String("task", c.TaskNameLocked()),
				zap.String("max-file", newMaxFile),
				zap.String("max-ts", newMaxTS.ToString()),
			)
			return
		}
		if maxTS.LT(thisTS) {
			newMaxFile = thisFile
			newMaxTS = *thisTS
			if err = c.fs.Delete(GCMetaDir + maxFile); err != nil {
				logutil.Error(
					"GC-DELETE-SNAPSHOT-FILE-ERROR",
					zap.String("task", c.TaskNameLocked()),
					zap.String("file", maxFile),
					zap.Error(err),
					zap.String("new-max-file", newMaxFile),
					zap.String("new-max-ts", newMaxTS.ToString()),
				)
				return
			}
			logutil.Info(
				"GC-TRACE-DELETE-SNAPSHOT-FILE",
				zap.String("task", c.TaskNameLocked()),
				zap.String("max-file", newMaxFile),
				zap.String("max-ts", newMaxTS.ToString()),
			)
			// TODO: seem to be a bug
			delete(metaFiles, maxFile)
			return
		}

		// thisTS <= maxTS: this file is expired and should be deleted
		if err = c.fs.Delete(GCMetaDir + thisFile); err != nil {
			logutil.Error(
				"GC-DELETE-SNAPSHOT-FILE-ERROR",
				zap.String("task", c.TaskNameLocked()),
				zap.String("file", GCMetaDir+thisFile),
				zap.Error(err),
				zap.String("max-file", maxFile),
				zap.String("max-ts", maxTS.ToString()),
			)
		}
		logutil.Info(
			"GC-TRACE-DELETE-SNAPSHOT-FILE",
			zap.String("task", c.TaskNameLocked()),
			zap.String("max-file", thisFile),
			zap.String("max-ts", thisTS.ToString()),
		)
		delete(metaFiles, thisFile)

		return
	}

	for _, metaFile := range metaFiles {
		switch metaFile.Ext() {
		case blockio.SnapshotExt:
			if maxSnapFile, maxSnapEnd, err = doDeleteFileFn(
				metaFile.Name(), metaFile.End(), maxSnapFile, &maxSnapEnd,
			); err != nil {
				return err
			}
		case blockio.AcctExt:
			if maxAcctFile, maxAcctEnd, err = doDeleteFileFn(
				metaFile.Name(), metaFile.End(), maxAcctFile, &maxAcctEnd,
			); err != nil {
				return err
			}
		}
	}
	if len(metaFiles) != prevNum {
		var w bytes.Buffer
		for _, v := range metaFiles {
			w.WriteString(fmt.Sprintf("%s,", v.String()))
		}
		logutil.Info(
			"GC-TRACE-DELETE-SNAPSHOT-FILES",
			zap.String("task", c.TaskNameLocked()),
			zap.Int("left-len", len(metaFiles)),
			zap.String("left-files", w.String()),
		)
	}

	return c.mutSetNewMetaFilesLocked(metaFiles)
}

// filterCheckpoints filters the checkpoints with the endTS less than the highWater
func (c *fastCleaner) filterCheckpoints(
	highWater *types.TS,
	checkpoints []*checkpoint.CheckpointEntry,
) ([]*checkpoint.CheckpointEntry, error) {
	if len(checkpoints) == 0 {
		return nil, nil
	}
	var i int
	for i = len(checkpoints) - 1; i >= 0; i-- {
		endTS := checkpoints[i].GetEnd()
		if endTS.LE(highWater) {
			logutil.Infof("filterCheckpoints: endTS: %v, highWater: %v", endTS.ToString(), highWater.ToString())
			break
		}
	}
	return checkpoints[:i+1], nil
}

func (c *fastCleaner) collectCkpData(
	ckp *checkpoint.CheckpointEntry,
) (data *logtail.CheckpointData, err error) {
	return logtail.GetCheckpointData(
		c.ctx, c.sid, c.fs.Service, ckp.GetLocation(), ckp.GetVersion())
}

func (c *fastCleaner) GetPITRs() (*logtail.PitrInfo, error) {
	c.mutation.Lock()
	defer c.mutation.Unlock()
	ts := time.Now()
	return c.mutation.snapshotMeta.GetPITR(c.ctx, c.sid, ts, c.fs.Service, c.mp)
}

func (c *fastCleaner) GetPITRsLocked() (*logtail.PitrInfo, error) {
	ts := time.Now()
	return c.mutation.snapshotMeta.GetPITR(c.ctx, c.sid, ts, c.fs.Service, c.mp)
}

func (c *fastCleaner) TryGC() (err error) {
	now := time.Now()
	c.StartMutationTask("gc-try-gc")
	defer c.StopMutationTask()
	defer func() {
		logutil.Info(
			"GC-TRACE-TRY-GC",
			zap.String("task", c.TaskNameLocked()),
			zap.Duration("duration", time.Since(now)),
			zap.Error(err),
		)
	}()
	memoryBuffer := MakeGCWindowBuffer(16 * mpool.MB)
	defer memoryBuffer.Close(c.mp)
	err = c.tryGCLocked(memoryBuffer)
	return
}

// (no incremental checkpoint scan)
// `tryGCLocked` will update
// `mutation.scanned` and `mutation.metaFiles` and `mutation.snapshotMeta`
// it will update the GC watermark and the checkpoint GC watermark
// `mutation.scanned`: it will be GC'ed against the max global checkpoint.
func (c *fastCleaner) tryGCLocked(
	memoryBuffer *containers.OneSchemaBatchBuffer,
) (err error) {
	// 1.2. If there is no incremental checkpoint scanned, no need to do GC.
	//      because GC is based on the scanned result.
	var scannedWindow *GCWindow
	if scannedWindow = c.GetScannedWindowLocked(); scannedWindow == nil {
		return
	}

	if err = c.tryGCAgainstGCKPLocked(memoryBuffer); err != nil {
		logutil.Error(
			"GC-TRY-GC-AGAINST-GCKP-ERROR",
			zap.Error(err),
			zap.String("task", c.TaskNameLocked()),
		)
		return
	}

	return
}

// when calling this function:
// at least one incremental checkpoint has been scanned
// the GC'ed water mark less than the global checkpoint
// `gckp` is the global checkpoint that needs to be GC'ed against
// `memoryBuffer` is the buffer used to read the data of the GC window
func (c *fastCleaner) tryGCAgainstGCKPLocked(
	memoryBuffer *containers.OneSchemaBatchBuffer,
) (err error) {
	now := time.Now()
	var snapshots map[uint32]containers.Vector
	var extraErrMsg string
	defer func() {
		logtail.CloseSnapshotList(snapshots)
		logutil.Info(
			"GC-TRACE-TRY-GC-AGAINST-GCKP",
			zap.String("task", c.TaskNameLocked()),
			zap.Duration("duration", time.Since(now)),
			zap.Error(err),
			zap.String("extra-err-msg", extraErrMsg),
		)
	}()
	pitrs, err := c.GetPITRsLocked()
	if err != nil {
		extraErrMsg = "GetPITRs failed"
		return
	}
	snapshots, err = c.mutation.snapshotMeta.GetSnapshot(c.ctx, c.sid, c.fs.Service, c.mp)
	if err != nil {
		extraErrMsg = "GetSnapshot failed"
		return
	}
	accountSnapshots := TransformToTSList(snapshots)
	filesToGC, err := c.doGCAgainstGlobalCheckpointLocked(
		accountSnapshots, pitrs, memoryBuffer,
	)
	if err != nil {
		extraErrMsg = "doGCAgainstGlobalCheckpointLocked failed"
		return
	}
	// Delete files after doGCAgainstGlobalCheckpointLocked
	// TODO:Requires Physical Removal Policy
	if err = c.deleter.DeleteMany(
		c.ctx,
		c.TaskNameLocked(),
		filesToGC,
	); err != nil {
		extraErrMsg = fmt.Sprintf("ExecDelete %v failed", filesToGC)
		return
	}
	return
}

// at least one incremental checkpoint has been scanned
// and the GC'ed water mark less than the global checkpoint
func (c *fastCleaner) doGCAgainstGlobalCheckpointLocked(
	accountSnapshots map[uint32][]types.TS,
	pitrs *logtail.PitrInfo,
	memoryBuffer *containers.OneSchemaBatchBuffer,
) ([]string, error) {
	now := time.Now()

	var (
		filesToGC           []string
		metafile            string
		err                 error
		softCost, mergeCost time.Duration
		extraErrMsg         string
	)

	defer func() {
		logutil.Info(
			"GC-TRACE-DO-GC-AGAINST-GCKP",
			zap.String("task", c.TaskNameLocked()),
			zap.Duration("duration", time.Since(now)),
			zap.Duration("soft-gc", softCost),
			zap.Duration("merge-table", mergeCost),
			zap.Error(err),
			zap.String("metafile", metafile),
			zap.String("extra-err-msg", extraErrMsg),
		)
	}()

	// do GC against the global checkpoint
	// the result is the files that need to be deleted
	// it will update the file list in the oneWindow
	// Before:
	// [t100, t400] [f1, f2, f3, f4, f5, f6, f7, f8, f9]
	// After:
	// [t100, t400] [f10, f11]
	// Also, it will update the GC metadata
	scannedWindow := c.GetScannedWindowLocked()
	if filesToGC, metafile, err = scannedWindow.ExecuteFastBasedGC(
		c.ctx,
		accountSnapshots,
		pitrs,
		c.mutation.snapshotMeta,
		memoryBuffer,
		c.config.canGCCacheSize,
		c.config.estimateRows,
		c.config.probility,
		c.mp,
		c.fs.Service,
	); err != nil {
		extraErrMsg = fmt.Sprintf("ExecuteGlobalCheckpointBasedGC %v failed", scannedWindow)
		return nil, err
	}

	if err = c.appendFilesToWAL(scannedWindow.metaDir + metafile); err != nil {
		logutil.Error(
			"GC-TRACE-APPEND-FILES-TO-WAL-FAILED",
			zap.String("task", c.TaskNameLocked()),
			zap.String("metafile", metafile),
			zap.Error(err))
		return nil, err
	}
	c.mutAddMetaFileLocked(metafile, GCMetaFile{
		name:  metafile,
		start: scannedWindow.tsRange.start,
		end:   scannedWindow.tsRange.end,
		ext:   blockio.CheckpointExt,
	})
	softCost = time.Since(now)

	// update gc watermark and refresh snapshot meta with the latest gc result
	// gcWaterMark will be updated to the end of the global checkpoint after each GC
	// Before:
	// gcWaterMark: GCKP[t100, t200)
	// After:
	// gcWaterMark: GCKP[t200, t400)
	now = time.Now()
	c.mutation.snapshotMeta.MergeTableInfo(accountSnapshots, pitrs)
	mergeCost = time.Since(now)
	return filesToGC, nil
}

func (c *fastCleaner) scanCheckpointsAsDebugWindow(
	ckps []*checkpoint.CheckpointEntry,
	buffer *containers.OneSchemaBatchBuffer,
) (window *GCWindow, err error) {
	window = NewGCWindow(c.mp, c.fs.Service, WithMetaPrefix("debug/"))
	if _, err = window.ScanCheckpoints(
		c.ctx, ckps, c.collectCkpData, nil, nil, buffer,
	); err != nil {
		window.Close()
		window = nil
	}
	return
}

func (c *fastCleaner) DoCheck() error {
	return nil
}

func (c *fastCleaner) Process() {
	if !c.GCEnabled() {
		return
	}
	now := time.Now()

	c.StartMutationTask("gc-process")
	defer c.StopMutationTask()

	startScanWaterMark := c.GetScanWaterMark()

	var err error
	defer func() {
		endScanWaterMark := c.GetScanWaterMark()
		logutil.Info(
			"GC-TRACE-PROCESS",
			zap.String("task", c.TaskNameLocked()),
			zap.Duration("duration", time.Since(now)),
			zap.Error(err),
			zap.String("start-scan-watermark", startScanWaterMark.String()),
			zap.String("end-scan-watermark", endScanWaterMark.String()),
		)
	}()

	memoryBuffer := MakeGCWindowBuffer(16 * mpool.MB)
	defer memoryBuffer.Close(c.mp)

	if err = c.tryScanLocked(memoryBuffer); err != nil {
		return
	}
	if err := c.tryGCLocked(memoryBuffer); err != nil {
		return
	}
}

// tryScanLocked scans the incremental checkpoints and tries to create a new GC window
// it will update `mutation.scanned` and `mutation.metaFiles`
// it will update the scan watermark
// it will save the snapshot meta and table info to the disk
func (c *fastCleaner) tryScanLocked(
	memoryBuffer *containers.OneSchemaBatchBuffer,
) (err error) {
	// get the max scanned timestamp
	var maxScannedTS types.TS
	if scanWaterMark := c.GetScanWaterMark(); scanWaterMark != nil {
		maxScannedTS = scanWaterMark.GetEnd()
	}

	// get up to 10 incremental checkpoints starting from the max scanned timestamp
	checkpoints := c.checkpointCli.ICKPSeekLT(maxScannedTS, 10)

	// quick return if there is no incremental checkpoint
	if len(checkpoints) == 0 {
		return
	}

	candidates := make([]*checkpoint.CheckpointEntry, 0, len(checkpoints))
	// filter out the incremental checkpoints that do not meet the requirements
	for _, ckp := range checkpoints {
		if !c.checkExtras(ckp) {
			logutil.Infof("skip incremental checkpoint %s", ckp.String())
			continue
		}
		candidates = append(candidates, ckp)
	}

	if len(candidates) == 0 {
		logutil.Infof("no incremental checkpoint to scan")
		return
	}

	var newWindow *GCWindow
	var tmpNewFiles []string
	if newWindow, tmpNewFiles, err = c.scanCheckpointsLocked(
		candidates, memoryBuffer,
	); err != nil {
		logutil.Error(
			"GC-SCAN-WINDOW-ERROR",
			zap.Error(err),
			zap.String("checkpoint", candidates[0].String()),
		)
		return
	}
	c.mutAddScannedLocked(newWindow)
	c.updateScanWaterMark(candidates[len(candidates)-1])
	files := tmpNewFiles
	for _, stats := range c.GetScannedWindowLocked().files {
		files = append(files, stats.ObjectName().String())
	}
	if err = c.appendFilesToWAL(files...); err != nil {
		logutil.Error(
			"GC-APPEND-SNAPSHOT-TO-WAL-ERROR",
			zap.String("task", c.TaskNameLocked()),
			zap.Error(err),
		)
		return
	}
	return
}

func (c *fastCleaner) mutSetNewMetaFilesLocked(
	metaFiles map[string]GCMetaFile,
) error {
	c.mutation.metaFiles = metaFiles
	return nil
}

func (c *fastCleaner) mutAddMetaFileLocked(
	key string,
	metaFile GCMetaFile,
) error {
	c.mutation.metaFiles[key] = metaFile
	return nil
}

func (c *fastCleaner) checkExtras(item any) bool {
	c.checker.RLock()
	defer c.checker.RUnlock()
	for _, checker := range c.checker.extras {
		if !checker(item) {
			return false
		}
	}
	return true
}

// AddChecker add&update a checker to the cleaner，return the number of checkers
// key is the unique identifier of the checker
func (c *fastCleaner) AddChecker(checker func(item any) bool, key string) int {
	c.checker.Lock()
	defer c.checker.Unlock()
	c.checker.extras[key] = checker
	return len(c.checker.extras)
}

// RemoveChecker remove a checker from the cleaner，return true if the checker is removed successfully
func (c *fastCleaner) RemoveChecker(key string) error {
	c.checker.Lock()
	defer c.checker.Unlock()
	if len(c.checker.extras) == 1 {
		return moerr.NewCantDelGCCheckerNoCtx()
	}
	delete(c.checker.extras, key)
	return nil
}

// appendFilesToWAL append the GC meta files to WAL.
func (c *fastCleaner) appendFilesToWAL(files ...string) error {
	driver := c.checkpointCli.GetDriver()
	if driver == nil {
		return nil
	}
	entry, err := store.BuildFilesEntry(files)
	if err != nil {
		return err
	}
	_, err = driver.AppendEntry(store.GroupFiles, entry)
	if err != nil {
		return err
	}
	return nil
}

// this function will update:
// `c.mutation.metaFiles`
// `c.mutation.snapshotMeta`
// this function will save the snapshot meta and table info to the disk
func (c *fastCleaner) scanCheckpointsLocked(
	ckps []*checkpoint.CheckpointEntry,
	memoryBuffer *containers.OneSchemaBatchBuffer,
) (gcWindow *GCWindow, newFiles []string, err error) {
	now := time.Now()

	var (
		snapSize, tableSize uint32
	)
	defer func() {
		logutil.Info(
			"GC-TRACE-SCAN",
			zap.String("task", c.TaskNameLocked()),
			zap.Int("checkpoint-count", len(ckps)),
			zap.Duration("duration", time.Since(now)),
			zap.Uint32("snap-meta-size :", snapSize),
			zap.Uint32("table-meta-size :", tableSize),
			zap.String("snapshot-detail", c.mutation.snapshotMeta.String()))
	}()

	var snapshotFile, accountFile GCMetaFile
	newFiles = make([]string, 0, 3)
	saveSnapshot := func() (err2 error) {
		name := blockio.EncodeSnapshotMetadataFileName(
			PrefixSnapMeta,
			ckps[0].GetStart(),
			ckps[len(ckps)-1].GetEnd(),
		)
		if snapSize, err2 = c.mutation.snapshotMeta.SaveMeta(
			GCMetaDir+name, c.fs.Service,
		); err2 != nil {
			logutil.Error(
				"GC-SAVE-SNAPSHOT-META-ERROR",
				zap.String("task", c.TaskNameLocked()),
				zap.Error(err2),
			)
			return
		}
		newFiles = append(newFiles, GCMetaDir+name)
		snapshotFile = GCMetaFile{
			name:  name,
			start: ckps[0].GetStart(),
			end:   ckps[len(ckps)-1].GetEnd(),
			ext:   blockio.SnapshotExt,
		}
		name = blockio.EncodeTableMetadataFileName(
			PrefixAcctMeta,
			ckps[0].GetStart(),
			ckps[len(ckps)-1].GetEnd(),
		)
		if tableSize, err2 = c.mutation.snapshotMeta.SaveTableInfo(
			GCMetaDir+name, c.fs.Service,
		); err2 != nil {
			logutil.Error(
				"GC-SAVE-TABLE-META-ERROR",
				zap.String("task", c.TaskNameLocked()),
				zap.Error(err2),
			)
		}
		newFiles = append(newFiles, GCMetaDir+name)
		accountFile = GCMetaFile{
			name:  name,
			start: ckps[0].GetStart(),
			end:   ckps[len(ckps)-1].GetEnd(),
			ext:   blockio.AcctExt,
		}
		return
	}

	gcWindow = NewGCWindow(c.mp, c.fs.Service)
	var gcMetaFile string
	if gcMetaFile, err = gcWindow.ScanCheckpoints(
		c.ctx,
		ckps,
		c.collectCkpData,
		c.mutUpdateSnapshotMetaLocked,
		saveSnapshot,
		memoryBuffer,
	); err != nil {
		gcWindow.Close()
		gcWindow = nil
		return
	}
	newFiles = append(newFiles, gcWindow.metaDir+gcMetaFile)
	c.mutAddMetaFileLocked(snapshotFile.name, snapshotFile)
	c.mutAddMetaFileLocked(accountFile.name, accountFile)
	c.mutAddMetaFileLocked(gcMetaFile, GCMetaFile{
		name:  gcMetaFile,
		start: gcWindow.tsRange.start,
		end:   gcWindow.tsRange.end,
		ext:   blockio.CheckpointExt,
	})
	return
}

func (c *fastCleaner) mutUpdateSnapshotMetaLocked(
	ckp *checkpoint.CheckpointEntry,
	data *logtail.CheckpointData,
) error {
	return c.mutation.snapshotMeta.Update(
		c.ctx,
		c.fs.Service,
		data,
		ckp.GetStart(),
		ckp.GetEnd(),
		c.TaskNameLocked(),
	)
}

func (c *fastCleaner) GetSnapshots() (map[uint32]containers.Vector, error) {
	c.mutation.Lock()
	defer c.mutation.Unlock()
	return c.mutation.snapshotMeta.GetSnapshot(c.ctx, c.sid, c.fs.Service, c.mp)
}
func (c *fastCleaner) GetSnapshotsLocked() (map[uint32]containers.Vector, error) {
	return c.mutation.snapshotMeta.GetSnapshot(c.ctx, c.sid, c.fs.Service, c.mp)
}
func (c *fastCleaner) GetTablePK(tid uint64) string {
	c.mutation.Lock()
	defer c.mutation.Unlock()
	return c.mutation.snapshotMeta.GetTablePK(tid)
}
