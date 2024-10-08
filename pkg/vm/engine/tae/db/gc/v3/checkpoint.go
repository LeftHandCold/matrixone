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
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"go.uber.org/zap"
)

type checkpointCleaner struct {
	ctx context.Context

	// TODO: remove `sid`
	sid string

	mp *mpool.MPool
	fs *objectio.ObjectFS

	checkpointCli checkpoint.RunnerReader
	delWorker     *GCWorker

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

		// gcWaterMark is the watermark of the global checkpoint which has been GC'ed.
		// when the cleaner GCs the global checkpoint, it scans all the object-list
		// of the global checkpoint and build a bloom filter for the object-list.
		// Then it scans all the remainingObjects generated by `incremental-checkpoint-scan`
		// and filters out the objects that are not in the bloom filter: canGC and cannotGC.
		// it records the cannotGC objects into some GC-specific files and do fine filter on
		// the canGC objects. The gcWaterMark is used to record the end of the global checkpoint
		// when the cleaner finishes this GC operation.
		gcWaterMark atomic.Pointer[checkpoint.CheckpointEntry]

		// checkpointGCWaterMark is the watermark after being merged. The checkpoint runner will
		// GC the entry in the checkpoint tree according to this watermark And the diskcleaner
		// will use this watermark as the start, and use `gcWaterMark` as the end to call `ICKPRange`,
		// to get the ickp to perform a merge operation
		checkpointGCWaterMark atomic.Pointer[types.TS]
	}

	options struct {
		gcEnabled           atomic.Bool
		checkEnabled        atomic.Bool
		gcCheckpointEnabled atomic.Bool
	}

	config struct {
		canGCCacheSize int
	}

	// checker is to check whether the checkpoint can be consumed
	checker struct {
		sync.RWMutex
		extras map[string]func(item any) bool
	}

	mutation struct {
		sync.Mutex
		scanned      *GCWindow
		metaFiles    map[string]GCMetaFile
		snapshotMeta *logtail.SnapshotMeta
	}
}

func WithCanGCCacheSize(
	size int,
) CheckpointCleanerOption {
	return func(e *checkpointCleaner) {
		e.config.canGCCacheSize = size
	}
}

// for ut
func WithGCCheckpointOption(enable bool) CheckpointCleanerOption {
	return func(e *checkpointCleaner) {
		e.options.gcCheckpointEnabled.Store(enable)
	}
}

func WithCheckOption(enable bool) CheckpointCleanerOption {
	return func(e *checkpointCleaner) {
		e.options.checkEnabled.Store(enable)
	}
}

type CheckpointCleanerOption func(*checkpointCleaner)

func NewCheckpointCleaner(
	ctx context.Context,
	sid string,
	fs *objectio.ObjectFS,
	checkpointCli checkpoint.RunnerReader,
	opts ...CheckpointCleanerOption,
) Cleaner {
	cleaner := &checkpointCleaner{
		ctx:           ctx,
		sid:           sid,
		fs:            fs,
		checkpointCli: checkpointCli,
	}
	for _, opt := range opts {
		opt(cleaner)
	}
	cleaner.delWorker = NewGCWorker(fs, cleaner)
	cleaner.options.gcEnabled.Store(true)
	cleaner.mp = common.CheckpointAllocator
	cleaner.checker.extras = make(map[string]func(item any) bool)
	cleaner.mutation.metaFiles = make(map[string]GCMetaFile)
	cleaner.mutation.snapshotMeta = logtail.NewSnapshotMeta()
	return cleaner
}

func (c *checkpointCleaner) Stop() {
}

func (c *checkpointCleaner) GetMPool() *mpool.MPool {
	return c.mp
}

func (c *checkpointCleaner) SetTid(tid uint64) {
	c.mutation.Lock()
	defer c.mutation.Unlock()
	c.mutation.snapshotMeta.SetTid(tid)
}

func (c *checkpointCleaner) EnableGC() {
	c.options.gcEnabled.Store(true)
}

func (c *checkpointCleaner) DisableGC() {
	c.options.gcEnabled.Store(false)
}

func (c *checkpointCleaner) GCEnabled() bool {
	return c.options.gcEnabled.Load()
}

func (c *checkpointCleaner) GCCheckpointEnabled() bool {
	return c.options.gcCheckpointEnabled.Load()
}

func (c *checkpointCleaner) EnableCheck() {
	c.options.checkEnabled.Store(true)
}
func (c *checkpointCleaner) DisableCheck() {
	c.options.checkEnabled.Store(false)
}

func (c *checkpointCleaner) CheckEnabled() bool {
	return c.options.checkEnabled.Load()
}

func (c *checkpointCleaner) Replay() error {
	dirs, err := c.fs.ListDir(GCMetaDir)
	if err != nil {
		return err
	}
	if len(dirs) == 0 {
		return nil
	}

	maxConsumedStart := types.TS{}
	maxConsumedEnd := types.TS{}
	maxSnapEnd := types.TS{}
	maxAcctEnd := types.TS{}
	// Get effective minMerged
	var snapFile, acctFile string
	for _, dir := range dirs {
		start, end, ext := blockio.DecodeGCMetadataFileName(dir.Name)
		if ext == blockio.SnapshotExt && maxSnapEnd.LT(&end) {
			maxSnapEnd = end
			snapFile = dir.Name
		}
		if ext == blockio.AcctExt && maxAcctEnd.LT(&end) {
			maxAcctEnd = end
			acctFile = dir.Name
		}
		c.mutation.metaFiles[dir.Name] = GCMetaFile{
			name:  dir.Name,
			start: start,
			end:   end,
			ext:   ext,
		}
	}
	readDirs := make([]fileservice.DirEntry, 0)
	for _, dir := range dirs {
		start, end, ext := blockio.DecodeGCMetadataFileName(dir.Name)
		if ext == blockio.SnapshotExt || ext == blockio.AcctExt {
			continue
		}
		if maxConsumedStart.IsEmpty() || maxConsumedStart.LT(&end) {
			maxConsumedStart = start
			maxConsumedEnd = end
			readDirs = append(readDirs, dir)
		}
	}
	if len(readDirs) == 0 {
		return nil
	}
	logger := logutil.Info
	for _, dir := range readDirs {
		start := time.Now()
		window := NewGCWindow(c.mp, c.fs.Service)
		_, end, _ := blockio.DecodeGCMetadataFileName(dir.Name)
		err = window.ReadTable(c.ctx, GCMetaDir+dir.Name, dir.Size, c.fs, end)
		if err != nil {
			logger = logutil.Error
		}
		logger(
			"Replay-GC-Metadata-File",
			zap.String("name", dir.Name),
			zap.Duration("cost", time.Since(start)),
			zap.Error(err),
		)
		if err != nil {
			return err
		}
		c.mutAddScannedLocked(window)
	}
	if acctFile != "" {
		err = c.mutation.snapshotMeta.ReadTableInfo(c.ctx, GCMetaDir+acctFile, c.fs.Service)
		if err != nil {
			return err
		}
	}
	if snapFile != "" {
		err = c.mutation.snapshotMeta.ReadMeta(c.ctx, GCMetaDir+snapFile, c.fs.Service)
		if err != nil {
			return err
		}
	}
	ckp := checkpoint.NewCheckpointEntry(c.sid, maxConsumedStart, maxConsumedEnd, checkpoint.ET_Incremental)
	c.updateScanWaterMark(ckp)
	compacted := c.checkpointCli.GetAllCompactedCheckpoints()
	if len(compacted) > 0 {
		sort.Slice(compacted, func(i, j int) bool {
			end1 := compacted[i].GetEnd()
			end2 := compacted[j].GetEnd()
			return end1.LT(&end2)
		})
		end := compacted[len(compacted)-1].GetEnd()
		c.updateCheckpointGCWaterMark(&end)
	}
	if acctFile == "" {
		//No account table information, it may be a new cluster or an upgraded cluster,
		//and the table information needs to be initialized from the checkpoint
		scanWaterMark := c.GetScanWaterMark()
		isConsumedGCkp := false
		checkpointEntries, err := checkpoint.ListSnapshotCheckpoint(c.ctx, c.sid, c.fs.Service, scanWaterMark.GetEnd(), 0)
		if err != nil {
			// TODO: why only warn???
			logutil.Warn(
				"Replay-GC-List-Error",
				zap.Error(err),
			)
		}
		if len(checkpointEntries) == 0 {
			return nil
		}
		for _, entry := range checkpointEntries {
			logutil.Infof("load checkpoint: %s, consumedEnd: %s", entry.String(), scanWaterMark.String())
			ckpData, err := c.collectCkpData(entry)
			if err != nil {
				// TODO: why only warn???
				logutil.Warn(
					"Replay-GC-Collect-Error",
					zap.Error(err),
				)
				continue
			}
			if entry.GetType() == checkpoint.ET_Global {
				isConsumedGCkp = true
			}
			c.mutation.snapshotMeta.InitTableInfo(c.ctx, c.fs.Service, ckpData, entry.GetStart(), entry.GetEnd())
		}
		if !isConsumedGCkp {
			// The global checkpoint that Specified checkpoint depends on may have been GC,
			// so we need to load a latest global checkpoint
			entry := c.checkpointCli.MaxGlobalCheckpoint()
			if entry == nil {
				logutil.Warn("not found max global checkpoint!")
				return nil
			}
			logutil.Info(
				"Replay-GC-Load-Global-Checkpoint",
				zap.String("max-gloabl", entry.String()),
				zap.String("max-consumed", scanWaterMark.String()),
			)
			ckpData, err := c.collectCkpData(entry)
			if err != nil {
				// TODO: why only warn???
				logutil.Warn(
					"Replay-GC-Collect-Global-Error",
					zap.Error(err),
				)
				return nil
			}
			c.mutation.snapshotMeta.InitTableInfo(c.ctx, c.fs.Service, ckpData, entry.GetStart(), entry.GetEnd())
		}
		logutil.Info(
			"Replay-GC-Init-Table-Info",
			zap.String("info", c.mutation.snapshotMeta.TableInfoString()),
		)
	}
	return nil

}

func (c *checkpointCleaner) GetCheckpointMetaFiles() map[string]struct{} {
	return c.checkpointCli.GetCheckpointMetaFiles()
}

func (c *checkpointCleaner) updateScanWaterMark(e *checkpoint.CheckpointEntry) {
	c.watermarks.scanWaterMark.Store(e)
}

func (c *checkpointCleaner) updateGCWaterMark(e *checkpoint.CheckpointEntry) {
	c.watermarks.gcWaterMark.Store(e)
}

func (c *checkpointCleaner) updateCheckpointGCWaterMark(ts *types.TS) {
	c.watermarks.checkpointGCWaterMark.Store(ts)
}

func (c *checkpointCleaner) mutAddScannedLocked(window *GCWindow) {
	if c.mutation.scanned == nil {
		c.mutation.scanned = window
	} else {
		c.mutation.scanned.Merge(window)
		window.Close()
	}
}

func (c *checkpointCleaner) GetScanWaterMark() *checkpoint.CheckpointEntry {
	return c.watermarks.scanWaterMark.Load()
}

func (c *checkpointCleaner) GetMinMerged() *checkpoint.CheckpointEntry {
	return c.GetScanWaterMark()
}

func (c *checkpointCleaner) GetGCWaterMark() *checkpoint.CheckpointEntry {
	return c.watermarks.gcWaterMark.Load()
}

func (c *checkpointCleaner) GetCheckpointGCWaterMark() *types.TS {
	return c.watermarks.checkpointGCWaterMark.Load()
}

func (c *checkpointCleaner) GetScannedWindow() *GCWindow {
	c.mutation.Lock()
	defer c.mutation.Unlock()
	return c.mutation.scanned
}

func (c *checkpointCleaner) GetScannedWindowLocked() *GCWindow {
	return c.mutation.scanned
}

func (c *checkpointCleaner) CloneMetaFilesLocked() map[string]GCMetaFile {
	metaFiles := make(map[string]GCMetaFile, len(c.mutation.metaFiles))
	for k, v := range c.mutation.metaFiles {
		metaFiles[k] = v
	}
	return metaFiles
}

func (c *checkpointCleaner) deleteStaleSnapshotFilesLocked() error {
	var (
		maxSnapEnd  types.TS
		maxSnapFile string

		maxAcctEnd  types.TS
		maxAcctFile string

		err error
	)

	metaFiles := c.CloneMetaFilesLocked()

	doDeleteFileFn := func(
		thisFile string, thisTS *types.TS,
		maxFile string, maxTS *types.TS,
	) (
		newMaxFile string,
		newMaxTS types.TS,
		err error,
	) {
		if maxFile == "" {
			newMaxFile = GCMetaDir + thisFile
			newMaxTS = *thisTS
			logutil.Info(
				"Merging-GC-File-SnapAcct-File",
				zap.String("max-file", newMaxFile),
				zap.String("max-ts", newMaxTS.ToString()),
			)
			delete(metaFiles, thisFile)
			return
		}
		if maxTS.LT(thisTS) {
			newMaxFile = GCMetaDir + thisFile
			newMaxTS = *thisTS
			if err = c.fs.Delete(maxFile); err != nil {
				logutil.Errorf("DelFiles failed: %v, max: %v", err.Error(), newMaxTS.ToString())
				return
			}
			logutil.Info(
				"Merging-GC-File-SnapAcct-File",
				zap.String("max-file", newMaxFile),
				zap.String("max-ts", newMaxTS.ToString()),
			)
			// TODO: seem to be a bug
			delete(metaFiles, thisFile)
			return
		}

		// thisTS <= maxTS: this file is expired and should be deleted
		if err = c.fs.Delete(GCMetaDir + thisFile); err != nil {
			logutil.Errorf("DelFiles failed: %v, file: %s, ts: %s", err.Error(), thisFile, thisTS.ToString())
		}
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

	return c.mutSetNewMetaFilesLocked(metaFiles)
}

// when call this function: at least one incremental checkpoint has been scanned
// it uses `c.mutation.metaFiles` and `c.mutation.scanned` as the input
// it updates `c.mutation.metaFiles` after the deletion
// it deletes the stale checkpoint meta files
// Example:
// Before:
// `c.mutation.metaFiles`: [t100_t200_xxx.ckp, t200_t300_xxx.ckp, t300_t400_xxx.ckp]
// `c.mutation.scanned`: [t300, t400]
// After:
// `c.mutation.metaFiles`: [t300_t400_xxx.ckp]
// `c.mutation.scanned`: [t300, t400]
// `t100_t200_xxx.ckp`, `t200_t300_xxx.ckp` are hard deleted
func (c *checkpointCleaner) deleteStaleCKPMetaFileLocked() (err error) {
	// TODO: add log
	window := c.GetScannedWindowLocked()
	metaFiles := c.CloneMetaFilesLocked()
	filesToDelete := make([]string, 0)
	for _, metaFile := range metaFiles {
		if (metaFile.Ext() == blockio.CheckpointExt) &&
			!(metaFile.EqualRange(&window.tsRange.start, &window.tsRange.end)) {
			filesToDelete = append(filesToDelete, GCMetaDir+metaFile.Name())
			delete(metaFiles, metaFile.Name())
		}
	}

	// TODO: if file is not found, it should be ignored
	if err = c.fs.DelFiles(c.ctx, filesToDelete); err != nil {
		logutil.Error(
			"Merging-GC-File-Error",
			zap.Error(err),
		)
		return
	}
	return c.mutSetNewMetaFilesLocked(metaFiles)
}

// getMetaFilesToMerge returns the files that can be merged.
// metaFiles: all checkpoint meta files should be merged.
func (c *checkpointCleaner) getMetaFilesToMerge(ts *types.TS) (
	checkpoints []*checkpoint.CheckpointEntry,
) {
	gcWaterMark := c.GetCheckpointGCWaterMark()
	start := types.TS{}
	if gcWaterMark != nil {
		start = *gcWaterMark
	}
	if !ts.GE(&start) {
		panic(fmt.Sprintf("getMetaFilesToMerge end < start. "+
			"end: %v, start: %v", ts.ToString(), start.ToString()))
	}
	return c.checkpointCli.ICKPRange(&start, ts, 20)
}

// filterCheckpoints filters the checkpoints with the endTS less than the highWater
func (c *checkpointCleaner) filterCheckpoints(
	highWater *types.TS,
	checkpoints []*checkpoint.CheckpointEntry,
) ([]*checkpoint.CheckpointEntry, error) {
	if len(checkpoints) == 0 {
		return nil, nil
	}
	var i int
	for i = len(checkpoints) - 1; i >= 0; i-- {
		endTS := checkpoints[i].GetEnd()
		if endTS.LT(highWater) {
			break
		}
	}
	return checkpoints[:i+1], nil
}

func (c *checkpointCleaner) mergeCheckpointFilesLocked(
	checkpointLowWaterMark *types.TS,
	memoryBuffer *containers.OneSchemaBatchBuffer,
) (err error) {
	// checkpointLowWaterMark is empty only in the following cases:
	// 1. no incremental and no gloabl checkpoint
	// 2. one incremental checkpoint with empty start
	// 3. no incremental checkpoint and one or more global checkpoints
	// if there are global checkpoints and incremental checkpoints, the low water mark is:
	// min(min(startTS of all incremental checkpoints),min(endTS of global checkpoints))
	if checkpointLowWaterMark.IsEmpty() {
		return
	}
	checkpoints := c.getMetaFilesToMerge(checkpointLowWaterMark)
	if len(checkpoints) == 0 {
		return
	}

	gcWaterMark := checkpointLowWaterMark
	rangeEnd := checkpoints[len(checkpoints)-1].GetEnd()
	if rangeEnd.GT(gcWaterMark) {
		panic(fmt.Sprintf("rangeEnd %s < gcWaterMark %s", rangeEnd.ToString(), gcWaterMark.ToString()))
	}

	logutil.Info("[MergeCheckpoint]",
		common.OperationField("MergeCheckpointFiles"),
		common.OperandField(checkpointLowWaterMark.ToString()))

	var checkpointsToMerge []*checkpoint.CheckpointEntry

	if checkpointsToMerge, err = c.filterCheckpoints(
		checkpointLowWaterMark,
		checkpoints,
	); err != nil {
		return
	}
	if len(checkpointsToMerge) == 0 {
		logutil.Warnf("[MergeCheckpoint] checkpoints len %d", len(checkpoints))
		return
	}

	// get the scanned window, it should not be nil
	window := c.GetScannedWindowLocked()
	if rangeEnd.GT(&window.tsRange.end) {
		panic(fmt.Sprintf("rangeEnd %s < window end %s", rangeEnd.ToString(), window.tsRange.end.ToString()))
	}

	sourcer := window.MakeFilesReader(c.ctx, c.fs.Service)
	bf, err := BuildBloomfilter(
		c.ctx,
		Default_Coarse_EstimateRows,
		Default_Coarse_Probility,
		0,
		sourcer.Read,
		memoryBuffer,
		c.mp,
	)

	var (
		dFiles        []string
		newCheckpoint *checkpoint.CheckpointEntry
	)
	dFiles, newCheckpoint, err = MergeCheckpoint(
		c.ctx,
		c.sid,
		c.fs.Service,
		checkpointsToMerge,
		bf,
		&rangeEnd,
		blockio.EncodeCheckpointMetadataFileName,
		c.mp)
	if err != nil {
		return err
	}
	if newCheckpoint == nil {
		panic("MergeCheckpoint new checkpoint is nil")
	}

	c.checkpointCli.AddCompacted(newCheckpoint)

	// update checkpoint gc water mark
	newWaterMark := newCheckpoint.GetEnd()
	c.updateCheckpointGCWaterMark(&newWaterMark)

	deleteFiles := dFiles

	// When the compacted tree exceeds 10 entries, you need to merge once
	compacted, ok := c.checkpointCli.CompactedRange(10)
	if ok {
		end := compacted[len(compacted)-1].GetEnd()
		if dFiles, newCheckpoint, err = MergeCheckpoint(c.ctx,
			c.sid,
			c.fs.Service,
			compacted,
			bf,
			&end,
			blockio.EncodeCompactedMetadataFileName,
			c.mp,
		); err != nil {
			return
		}
		if newCheckpoint == nil {
			panic("MergeCheckpoint new compacted checkpoint is nil")
		}

		c.checkpointCli.AddCompacted(newCheckpoint)
		deleteFiles = append(deleteFiles, dFiles...)
		for _, ckp := range compacted {
			c.checkpointCli.DeleteCompactedEntry(ckp)
		}
	}

	logutil.Info("[MergeCheckpoint] CKP GC",
		zap.Bool("gc-checkpoint", c.GCCheckpointEnabled()),
		zap.Strings("files", deleteFiles))
	if c.GCCheckpointEnabled() {
		err = c.fs.DelFiles(c.ctx, deleteFiles)
		if err != nil {
			logutil.Errorf("DelFiles failed: %v", err.Error())
			return err
		}
	}
	for _, file := range deleteFiles {
		if strings.Contains(file, checkpoint.PrefixMetadata) {
			info := strings.Split(file, checkpoint.CheckpointDir+"/")
			name := info[1]
			c.checkpointCli.RemoveCheckpointMetaFile(name)
		}
	}
	return nil
}

func (c *checkpointCleaner) collectCkpData(
	ckp *checkpoint.CheckpointEntry,
) (data *logtail.CheckpointData, err error) {
	return logtail.GetCheckpointData(
		c.ctx, c.sid, c.fs.Service, ckp.GetLocation(), ckp.GetVersion())
}

func (c *checkpointCleaner) GetPITRs() (*logtail.PitrInfo, error) {
	c.mutation.Lock()
	defer c.mutation.Unlock()
	ts := time.Now()
	return c.mutation.snapshotMeta.GetPITR(c.ctx, c.sid, ts, c.fs.Service, c.mp)
}

func (c *checkpointCleaner) GetPITRsLocked() (*logtail.PitrInfo, error) {
	ts := time.Now()
	return c.mutation.snapshotMeta.GetPITR(c.ctx, c.sid, ts, c.fs.Service, c.mp)
}

func (c *checkpointCleaner) TryGC() error {
	c.mutation.Lock()
	defer c.mutation.Unlock()
	memoryBuffer := MakeGCWindowBuffer(16 * mpool.MB)
	defer memoryBuffer.Close(c.mp)
	if err := c.tryGCLocked(memoryBuffer); err != nil {
		logutil.Error(
			"DiskCleaner-TryGC-Error",
			zap.Error(err),
		)
		return err
	}
	return nil
}

// (no incremental checkpoint scan)
// `tryGCLocked` will update
// `mutation.scanned` and `mutation.metaFiles` and `mutation.snapshotMeta`
// it will update the GC watermark and the checkpoint GC watermark
// `mutation.scanned`: it will be GC'ed against the max global checkpoint.
func (c *checkpointCleaner) tryGCLocked(
	memoryBuffer *containers.OneSchemaBatchBuffer,
) (err error) {
	// 1. Quick check if GC is needed
	// 1.1. If there is no global checkpoint, no need to do GC
	var maxGlobalCKP *checkpoint.CheckpointEntry
	if maxGlobalCKP = c.checkpointCli.MaxGlobalCheckpoint(); maxGlobalCKP == nil {
		return
	}
	// 1.2. If there is no incremental checkpoint scanned, no need to do GC.
	//      because GC is based on the scanned result.
	var scannedWindow *GCWindow
	if scannedWindow = c.GetScannedWindowLocked(); scannedWindow == nil {
		return
	}

	// gcWaterMark is not nil, which means some global checkpoint has been GC'ed
	// if the GC'ed global checkpoint is greater than or equal to the max global checkpoint,
	// it means no need to do GC again
	gcWaterMark := c.GetGCWaterMark()
	if gcWaterMark != nil {
		gcWaterMarkTS := gcWaterMark.GetEnd()
		maxGlobalCKPTS := maxGlobalCKP.GetEnd()
		if gcWaterMarkTS.GE(&maxGlobalCKPTS) {
			return
		}
	}

	if err = c.tryGCAgainstGCKPLocked(
		maxGlobalCKP, memoryBuffer,
	); err != nil {
		logutil.Error(
			"DiskCleaner-Replay-TryGC-Error",
			zap.Error(err),
			zap.String("checkpoint", maxGlobalCKP.String()),
		)
		return
	}

	if err = c.deleteStaleCKPMetaFileLocked(); err != nil {
		logutil.Error(
			"DiskCleaner-Replay-UpgradeGC-Error",
			zap.Error(err),
		)
	}

	if err = c.deleteStaleSnapshotFilesLocked(); err != nil {
		logutil.Error(
			"DiskCleaner-Replay-UpgradeGC-Error",
			zap.Error(err),
		)
	}

	return
}

// when calling this function:
// at least one incremental checkpoint has been scanned
// the GC'ed water mark less than the global checkpoint
// `gckp` is the global checkpoint that needs to be GC'ed against
// `memoryBuffer` is the buffer used to read the data of the GC window
func (c *checkpointCleaner) tryGCAgainstGCKPLocked(
	gckp *checkpoint.CheckpointEntry,
	memoryBuffer *containers.OneSchemaBatchBuffer,
) (err error) {
	// TODO: no error here???
	if !c.delWorker.Start() {
		return
	}
	var snapshots map[uint32]containers.Vector
	defer func() {
		if err != nil {
			logutil.Errorf("[DiskCleaner] tryGCAgainstGCKPLocked failed: %v", err.Error())
			c.delWorker.Idle()
		}
		logtail.CloseSnapshotList(snapshots)
	}()
	pitrs, err := c.GetPITRsLocked()
	if err != nil {
		logutil.Errorf("[DiskCleaner] GetPitrs failed: %v", err.Error())
		return
	}
	snapshots, err = c.mutation.snapshotMeta.GetSnapshot(c.ctx, c.sid, c.fs.Service, c.mp)
	if err != nil {
		logutil.Errorf("[DiskCleaner] GetSnapshot failed: %v", err.Error())
		return
	}
	accountSnapshots := TransformToTSList(snapshots)
	filesToGC, err := c.doGCAgainstGlobalCheckpointLocked(
		gckp, accountSnapshots, pitrs, memoryBuffer,
	)
	if err != nil {
		logutil.Errorf("[DiskCleaner] doGCAgainstGlobalCheckpointLocked failed: %v", err.Error())
		return
	}
	// Delete files after doGCAgainstGlobalCheckpointLocked
	// TODO:Requires Physical Removal Policy
	if err = c.delWorker.ExecDelete(c.ctx, filesToGC); err != nil {
		logutil.Infof("[DiskCleaner] ExecDelete failed: %v", err)
		return
	}
	if c.GetGCWaterMark() == nil {
		return nil
	}
	waterMark := c.GetGCWaterMark().GetEnd()
	mergeMark := types.TS{}
	if c.GetCheckpointGCWaterMark() != nil {
		mergeMark = *c.GetCheckpointGCWaterMark()
	}
	if !waterMark.GT(&mergeMark) {
		return nil
	}
	scanMark := c.GetScanWaterMark().GetEnd()
	if scanMark.IsEmpty() {
		panic("scanMark is empty")
	}
	if waterMark.GT(&scanMark) {
		waterMark = scanMark
	}
	err = c.mergeCheckpointFilesLocked(&waterMark, memoryBuffer)
	if err != nil {
		// TODO: Error handle
		logutil.Errorf("[DiskCleaner] mergeCheckpointFilesLocked failed: %v", err.Error())
	}
	return
}

// at least one incremental checkpoint has been scanned
// and the GC'ed water mark less than the global checkpoint
func (c *checkpointCleaner) doGCAgainstGlobalCheckpointLocked(
	gckp *checkpoint.CheckpointEntry,
	accountSnapshots map[uint32][]types.TS,
	pitrs *logtail.PitrInfo,
	memoryBuffer *containers.OneSchemaBatchBuffer,
) ([]string, error) {
	now := time.Now()

	var softCost, mergeCost time.Duration
	defer func() {
		logutil.Info("[DiskCleaner] doGCAgainstGlobalCheckpointLocked cost",
			zap.String("soft-gc cost", softCost.String()),
			zap.String("merge-table cost", mergeCost.String()))
	}()

	var (
		filesToGC []string
		metafile  string
		err       error
	)
	// do GC against the global checkpoint
	// the result is the files that need to be deleted
	// it will update the file list in the oneWindow
	// Before:
	// [t100, t400] [f1, f2, f3, f4, f5, f6, f7, f8, f9]
	// After:
	// [t100, t400] [f10, f11]
	// Also, it will update the GC metadata
	scannedWindow := c.GetScannedWindowLocked()
	if filesToGC, metafile, err = scannedWindow.ExecuteGlobalCheckpointBasedGC(
		c.ctx,
		gckp,
		accountSnapshots,
		pitrs,
		c.mutation.snapshotMeta,
		memoryBuffer,
		c.config.canGCCacheSize,
		c.mp,
		c.fs.Service,
	); err != nil {
		logutil.Errorf("doGCAgainstGlobalCheckpointLocked failed: %v", err.Error())
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
	// TODO:
	c.updateGCWaterMark(gckp)
	c.mutation.snapshotMeta.MergeTableInfo(accountSnapshots, pitrs)
	mergeCost = time.Since(now)
	return filesToGC, nil
}

func (c *checkpointCleaner) scanCheckpointsAsDebugWindow(
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

func (c *checkpointCleaner) DoCheck() error {
	debugCandidates := c.checkpointCli.GetAllIncrementalCheckpoints()
	compactedCandidates := c.checkpointCli.GetAllCompactedCheckpoints()
	gckps := c.checkpointCli.GetAllGlobalCheckpoints()

	c.mutation.Lock()
	defer c.mutation.Unlock()

	// no scan watermark, GC has not yet run
	var scanWaterMark *checkpoint.CheckpointEntry
	if scanWaterMark = c.GetScanWaterMark(); scanWaterMark == nil {
		return moerr.NewInternalErrorNoCtx("GC has not yet run")
	}

	gCkp := c.GetGCWaterMark()
	testutils.WaitExpect(10000, func() bool {
		gCkp = c.GetGCWaterMark()
		return gCkp != nil
	})
	if gCkp == nil {
		gCkp = c.checkpointCli.MaxGlobalCheckpoint()
		if gCkp == nil {
			return nil
		}
		logutil.Warnf("MaxCompared is nil, use maxGlobalCkp %v", gCkp.String())
	}

	for i, ckp := range debugCandidates {
		maxEnd := scanWaterMark.GetEnd()
		ckpEnd := ckp.GetEnd()
		if ckpEnd.Equal(&maxEnd) {
			debugCandidates = debugCandidates[:i+1]
			break
		}
	}

	start1 := debugCandidates[len(debugCandidates)-1].GetEnd()
	start2 := scanWaterMark.GetEnd()
	if !start1.Equal(&start2) {
		logutil.Info("[DiskCleaner]", common.OperationField("Compare not equal"),
			common.OperandField(start1.ToString()), common.OperandField(start2.ToString()))
		return moerr.NewInternalErrorNoCtx("TS Compare not equal")
	}
	buffer := MakeGCWindowBuffer(16 * mpool.MB)
	defer buffer.Close(c.mp)

	debugWindow, err := c.scanCheckpointsAsDebugWindow(
		debugCandidates, buffer,
	)
	if err != nil {
		logutil.Errorf("processing clean %s: %v", debugCandidates[0].String(), err)
		// TODO
		return moerr.NewInternalErrorNoCtxf("processing clean %s: %v", debugCandidates[0].String(), err)
	}

	snapshots, err := c.GetSnapshotsLocked()
	if err != nil {
		logutil.Errorf("processing clean %s: %v", debugCandidates[0].String(), err)
		return moerr.NewInternalErrorNoCtxf("processing clean GetSnapshotsLocked %s: %v", debugCandidates[0].String(), err)
	}
	defer logtail.CloseSnapshotList(snapshots)

	pitr, err := c.GetPITRsLocked()
	if err != nil {
		logutil.Errorf("processing clean %s: %v", debugCandidates[0].String(), err)
		return moerr.NewInternalErrorNoCtxf("processing clean GetPITRsLocked %s: %v", debugCandidates[0].String(), err)
	}

	mergeWindow := c.GetScannedWindowLocked().Clone()
	defer mergeWindow.Close()

	accoutSnapshots := TransformToTSList(snapshots)
	logutil.Infof(
		"merge table is %d, stats is %v",
		len(mergeWindow.files),
		mergeWindow.files[0].ObjectName().String(),
	)
	if _, _, err = mergeWindow.ExecuteGlobalCheckpointBasedGC(
		c.ctx,
		gCkp,
		accoutSnapshots,
		pitr,
		c.mutation.snapshotMeta,
		buffer,
		c.config.canGCCacheSize,
		c.mp,
		c.fs.Service,
	); err != nil {
		logutil.Infof("err is %v", err)
		return err
	}

	//logutil.Infof("debug table is %d, stats is %v", len(debugWindow.files.stats), debugWindow.files.stats[0].ObjectName().String())
	if _, _, err = debugWindow.ExecuteGlobalCheckpointBasedGC(
		c.ctx,
		gCkp,
		accoutSnapshots,
		pitr,
		c.mutation.snapshotMeta,
		buffer,
		c.config.canGCCacheSize,
		c.mp,
		c.fs.Service,
	); err != nil {
		logutil.Infof("err is %v", err)
		return err
	}

	//logutil.Infof("debug table2 is %d, stats is %v", len(debugWindow.files.stats), debugWindow.files.stats[0].ObjectName().String())
	objects1, objects2, equal := mergeWindow.Compare(debugWindow, buffer)
	if !equal {
		logutil.Errorf("remainingObjects :%v", mergeWindow.String(objects1))
		logutil.Errorf("debugWindow :%v", debugWindow.String(objects2))
		return moerr.NewInternalErrorNoCtx("Compare is failed")
	} else {
		logutil.Info("[DiskCleaner]", common.OperationField("Compare is End"),
			common.AnyField("table :", debugWindow.String(objects2)),
			common.OperandField(start1.ToString()))
	}

	if len(compactedCandidates) == 0 {
		return nil
	}
	cend := compactedCandidates[len(compactedCandidates)-1].GetEnd()
	dend := debugCandidates[0].GetEnd()
	if dend.GT(&cend) {
		return nil
	}
	ickpObjects := make(map[string]*ObjectEntry, 0)
	ok := false
	gcWaterMark := cend
	for _, ckp := range gckps {
		end := ckp.GetEnd()
		if end.GE(&gcWaterMark) {
			gcWaterMark = ckp.GetEnd()
		}
	}
	for i, ckp := range debugCandidates {
		end := ckp.GetEnd()
		if end.Equal(&gcWaterMark) {
			debugCandidates = debugCandidates[:i+1]
			ok = true
			break
		}
	}
	if !ok {
		return nil
	}

	for _, ckp := range debugCandidates {
		data, err := c.collectCkpData(ckp)
		if err != nil {
			return err
		}
		collectObjectsFromCheckpointData(data, ickpObjects)
	}
	cptCkpObjects := make(map[string]*ObjectEntry, 0)
	for _, ckp := range compactedCandidates {
		data, err := c.collectCkpData(ckp)
		if err != nil {
			return err
		}
		collectObjectsFromCheckpointData(data, cptCkpObjects)
	}

	tList, pList := c.mutation.snapshotMeta.AccountToTableSnapshots(accoutSnapshots, pitr)
	for name, entry := range ickpObjects {
		if cptCkpObjects[name] != nil {
			continue
		}
		logutil.Infof("Check name %s", name)
		if isSnapshotRefers(entry.stats, pList[entry.table], &entry.createTS, &entry.dropTS, tList[entry.table]) {
			logutil.Error(
				"Check-Error",
				zap.String("name", entry.stats.ObjectName().String()),
				zap.String("pitr", pList[entry.table].ToString()),
				zap.String("create-ts", entry.createTS.ToString()),
				zap.String("drop-ts", entry.dropTS.ToString()),
			)
			return moerr.NewInternalError(c.ctx, "snapshot refers")
		}
	}
	return nil
}

func (c *checkpointCleaner) Process() {
	if !c.GCEnabled() {
		return
	}
	now := time.Now()

	c.mutation.Lock()
	defer c.mutation.Unlock()

	startScanWaterMark := c.GetScanWaterMark()
	startGCWaterMark := c.GetGCWaterMark()

	var err error
	defer func() {
		endScanWaterMark := c.GetScanWaterMark()
		endGCWaterMark := c.GetGCWaterMark()
		logutil.Info(
			"DiskCleaner-Process-End",
			zap.Duration("duration", time.Since(now)),
			zap.Error(err),
			zap.String("start-scan-watermark", startScanWaterMark.String()),
			zap.String("end-scan-watermark", endScanWaterMark.String()),
			zap.String("start-gc-watermark", startGCWaterMark.String()),
			zap.String("end-gc-watermark", endGCWaterMark.String()),
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
func (c *checkpointCleaner) tryScanLocked(
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
			continue
		}
		candidates = append(candidates, ckp)
	}

	if len(candidates) == 0 {
		return
	}

	var newWindow *GCWindow
	if newWindow, err = c.scanCheckpointsLocked(
		candidates, memoryBuffer,
	); err != nil {
		logutil.Error(
			"DiskCleaner-TryScan-Error",
			zap.Error(err),
			zap.String("checkpoint", candidates[0].String()),
		)
		return
	}
	c.mutAddScannedLocked(newWindow)
	c.updateScanWaterMark(candidates[len(candidates)-1])
	return
}

func (c *checkpointCleaner) mutSetNewMetaFilesLocked(
	metaFiles map[string]GCMetaFile,
) error {
	c.mutation.metaFiles = metaFiles
	return nil
}

func (c *checkpointCleaner) mutAddMetaFileLocked(
	key string,
	metaFile GCMetaFile,
) error {
	c.mutation.metaFiles[key] = metaFile
	return nil
}

func (c *checkpointCleaner) checkExtras(item any) bool {
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
func (c *checkpointCleaner) AddChecker(checker func(item any) bool, key string) int {
	c.checker.Lock()
	defer c.checker.Unlock()
	c.checker.extras[key] = checker
	return len(c.checker.extras)
}

// RemoveChecker remove a checker from the cleaner，return true if the checker is removed successfully
func (c *checkpointCleaner) RemoveChecker(key string) error {
	c.checker.Lock()
	defer c.checker.Unlock()
	if len(c.checker.extras) == 1 {
		return moerr.NewCantDelGCCheckerNoCtx()
	}
	delete(c.checker.extras, key)
	return nil
}

// this function will update:
// `c.mutation.metaFiles`
// `c.mutation.snapshotMeta`
// this function will save the snapshot meta and table info to the disk
func (c *checkpointCleaner) scanCheckpointsLocked(
	ckps []*checkpoint.CheckpointEntry,
	memoryBuffer *containers.OneSchemaBatchBuffer,
) (gcWindow *GCWindow, err error) {
	now := time.Now()
	logutil.Info(
		"DiskCleaner-Consume-Start",
		zap.Int("entry-count :", len(ckps)),
	)

	var (
		snapSize, tableSize uint32
	)
	defer func() {
		logutil.Info("DiskCleaner-Consume-End",
			zap.Duration("duration", time.Since(now)),
			zap.Uint32("snap-meta-size :", snapSize),
			zap.Uint32("table-meta-size :", tableSize),
			zap.String("snapshot-detail", c.mutation.snapshotMeta.String()))
	}()

	var snapshotFile, accountFile GCMetaFile
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
				"DiskCleaner-Error-SaveMeta",
				zap.Error(err2),
			)
			return
		}
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
				"DiskCleaner-Error-SaveTableInfo",
				zap.Error(err2),
			)
		}
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

func (c *checkpointCleaner) mutUpdateSnapshotMetaLocked(
	ckp *checkpoint.CheckpointEntry,
	data *logtail.CheckpointData,
) error {
	_, err := c.mutation.snapshotMeta.Update(
		c.ctx, c.fs.Service, data, ckp.GetStart(), ckp.GetEnd(),
	)
	return err
}

func (c *checkpointCleaner) GetSnapshots() (map[uint32]containers.Vector, error) {
	c.mutation.Lock()
	defer c.mutation.Unlock()
	return c.mutation.snapshotMeta.GetSnapshot(c.ctx, c.sid, c.fs.Service, c.mp)
}
func (c *checkpointCleaner) GetSnapshotsLocked() (map[uint32]containers.Vector, error) {
	return c.mutation.snapshotMeta.GetSnapshot(c.ctx, c.sid, c.fs.Service, c.mp)
}
