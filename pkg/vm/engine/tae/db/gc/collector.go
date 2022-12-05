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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

const (
	GCState_Active int32 = iota
	GCState_Noop
	GCState_Scheduled
	GCState_ScheduledDone
)

type gcCandidates struct {
	blocks   *common.Tree
	segments *common.Tree
	tables   *common.Tree
	dbs      map[uint64]bool
}

func newGCCandidates() *gcCandidates {
	return &gcCandidates{
		blocks:   common.NewTree(),
		segments: common.NewTree(),
		tables:   common.NewTree(),
		dbs:      make(map[uint64]bool),
	}
}

func (candidates *gcCandidates) Reset() {
	candidates.blocks.Reset()
	candidates.segments.Reset()
	candidates.tables.Reset()
	candidates.dbs = make(map[uint64]bool)
}

func (candidates *gcCandidates) IsEmpty() bool {
	if !candidates.blocks.IsEmpty() {
		return false
	}
	if !candidates.segments.IsEmpty() {
		return false
	}
	if !candidates.tables.IsEmpty() {
		return false
	}
	return len(candidates.dbs) == 0
}

func (candidates *gcCandidates) AddBlock(dbId, tableId, segmentId, blockId uint64) {
	candidates.blocks.AddBlock(dbId, tableId, segmentId, blockId)
}

func (candidates *gcCandidates) AddSegment(dbId, tableId, segmentId uint64) {
	candidates.segments.AddSegment(dbId, tableId, segmentId)
}

func (candidates *gcCandidates) AddTable(dbId, tableId uint64) {
	candidates.tables.AddTable(dbId, tableId)
}

func (candidates *gcCandidates) AddDB(dbId uint64) {
	candidates.dbs[dbId] = true
}

func (candidates *gcCandidates) String() string {
	if candidates.IsEmpty() {
		return ""
	}
	var w bytes.Buffer
	if len(candidates.dbs) != 0 {
		_, _ = w.WriteString("DB TO GC:[")
		for id := range candidates.dbs {
			_, _ = w.WriteString(fmt.Sprintf(" %d", id))
		}
		_, _ = w.WriteString("]\n")
	}
	if !candidates.tables.IsEmpty() {
		_, _ = w.WriteString("TABLE TO GC:[")
		_, _ = w.WriteString(candidates.tables.String())
		_, _ = w.WriteString("]\n")
	}
	if !candidates.segments.IsEmpty() {
		_, _ = w.WriteString("SEMENT TO GC:[")
		_, _ = w.WriteString(candidates.segments.String())
		_, _ = w.WriteString("]\n")
	}
	if !candidates.blocks.IsEmpty() {
		_, _ = w.WriteString("BLOCK TO GC:[")
		_, _ = w.WriteString(candidates.blocks.String())
		_, _ = w.WriteString("]\n")
	}
	return w.String()
}

type CollectorOption func(*collector)

func WithCollectorTTLFunc(fn TTLFunc) CollectorOption {
	return func(c *collector) {
		c.ttls = append(c.ttls, fn)
	}
}

func WithCollectorInterval(interval time.Duration) CollectorOption {
	return func(c *collector) {
		c.minInterval = interval
	}
}

type collector struct {
	*catalog.LoopProcessor
	scheduler       tasks.TaskScheduler
	state           atomic.Int32
	loopState       int32
	epoch           types.TS
	checkpointedLsn uint64
	clock           *types.TsAlloctor
	candidates      *gcCandidates
	minInterval     time.Duration
	lastRunTime     time.Time

	ttls []TTLFunc
}

func NewCollector(
	scheduler tasks.TaskScheduler,
	clock clock.Clock,
	opts ...CollectorOption) *collector {
	c := &collector{
		LoopProcessor: new(catalog.LoopProcessor),
		scheduler:     scheduler,
		clock:         types.NewTsAlloctor(clock),
		candidates:    newGCCandidates(),
		ttls:          make([]TTLFunc, 0),
	}
	for _, opt := range opts {
		opt(c)
	}
	c.BlockFn = c.onBlock
	c.SegmentFn = c.onSegment
	c.TableFn = c.onTable
	c.DatabaseFn = c.onDatabase
	c.refreshEpoch()
	c.ResetState()
	return c
}

func (c *collector) fillDefaults() {
	if c.minInterval <= 0 {
		c.minInterval = time.Second * 10
	}
}

func (c *collector) ResetState() {
	c.state.Store(GCState_Active)
}

func (c *collector) StopSchedule() {
	c.state.Store(GCState_ScheduledDone)
	// logutil.Infof("Stop Schedule GCJOB")
}

func (c *collector) StartSchedule() {
	c.state.Store(GCState_Scheduled)
	// logutil.Infof("Start Schedule GCJOB")
}

func (c *collector) refreshEpoch() {
	c.epoch = types.BuildTS(time.Now().UTC().UnixNano()-c.minInterval.Nanoseconds(), 0)
}

func (c *collector) PreExecute() (err error) {
	// logutil.Infof("GCJOB INV=%d, ACT INV=%s", c.minInterval.Milliseconds()/2, time.Since(c.lastRunTime))
	c.checkpointedLsn = c.scheduler.GetCheckpointedLSN()
	c.loopState = c.state.Load()
	// If scheduled done, we need to refresh a new gc epoch
	if c.loopState == GCState_ScheduledDone {
		c.refreshRunTime()
		c.refreshEpoch()
		c.ResetState()
		c.loopState = GCState_Active
	}
	// If state is active and the interval since last run time is below a limit. Skip this loop
	if c.canRun() && time.Since(c.lastRunTime) < c.minInterval/2 {
		c.loopState = GCState_Noop
		// logutil.Infof("Start Noop GCJOB")
	}
	if c.canRun() {
		c.refreshRunTime()
		// logutil.Infof("Start Run GCJOB")
	}
	return
}

func (c *collector) refreshRunTime() {
	c.lastRunTime = time.Now()
}

func (c *collector) runTTLs() {
	for _, fn := range c.ttls {
		fn(time.Now())
	}
}

func (c *collector) PostExecute() (err error) {
	c.runTTLs()

	if !c.canRun() {
		return
	}
	if c.candidates.IsEmpty() {
		c.refreshEpoch()
	} else {
		// logutil.Infof("Epoch: %s", c.epoch.ToString())
		c.StartSchedule()
		_, err = c.scheduler.ScheduleFn(
			nil,
			tasks.GCTask,
			func() error {
				defer c.StopSchedule()
				// logutil.Info(c.candidates.String())
				// TODO: GC all candidates
				c.candidates.Reset()
				return nil
			})
	}
	return
}

func (c *collector) canRun() bool {
	return c.loopState == GCState_Active
}

func (c *collector) isEntryCheckpointed(entry catalog.BaseEntry) bool {
	node := entry.GetLatestNodeLocked()
	index := node.GetLogIndex()
	if index == nil {
		return false
	}
	if index.LSN <= c.checkpointedLsn {
		return true
	}
	return false
}

func (c *collector) isCandidate(
	entry catalog.BaseEntry,
	terminated bool,
	rwlocker *sync.RWMutex) (ok bool) {
	entry.RLock()
	defer entry.RUnlock()
	ok = false
	if terminated {
		ok = c.isEntryCheckpointed(entry)
		return
	}
	if !entry.HasDropCommittedLocked() {
		return
	}
	if visible, _ := entry.IsVisible(c.epoch, rwlocker); visible {
		return
	}
	ok = c.isEntryCheckpointed(entry)
	return
}

func (c *collector) onBlock(entry *catalog.BlockEntry) (err error) {
	if !c.canRun() {
		return
	}
	var ts types.TS
	var terminated bool
	if ts, terminated = entry.GetTerminationTS(); terminated {
		if ts.Greater(c.epoch) {
			terminated = false
		}
	}
	if c.isCandidate(entry.MetaBaseEntry, terminated, entry.RWMutex) {
		id := entry.AsCommonID()
		c.candidates.AddBlock(entry.GetSegment().GetTable().GetDB().ID,
			id.TableID,
			id.SegmentID,
			id.BlockID)
	}
	return
}

func (c *collector) onSegment(entry *catalog.SegmentEntry) (err error) {
	if !c.canRun() {
		return moerr.GetOkStopCurrRecur()
	}
	var ts types.TS
	var terminated bool
	if ts, terminated = entry.GetTerminationTS(); terminated {
		if ts.Greater(c.epoch) {
			terminated = false
		}
	}
	if c.isCandidate(entry.MetaBaseEntry, terminated, entry.RWMutex) {
		id := entry.AsCommonID()
		c.candidates.AddSegment(entry.GetTable().GetDB().ID,
			id.TableID,
			id.SegmentID)
	}
	return
}

func (c *collector) onTable(entry *catalog.TableEntry) (err error) {
	if !c.canRun() {
		return moerr.GetOkStopCurrRecur()
	}
	var ts types.TS
	var terminated bool
	if ts, terminated = entry.GetTerminationTS(); terminated {
		if ts.Greater(c.epoch) {
			terminated = false
		}
	}
	if c.isCandidate(entry.TableBaseEntry, terminated, entry.RWMutex) {
		c.candidates.AddTable(entry.GetDB().ID, entry.ID)
	}
	return
}

func (c *collector) onDatabase(entry *catalog.DBEntry) (err error) {
	if !c.canRun() {
		return moerr.GetOkStopCurrRecur()
	}
	if c.isCandidate(entry.DBBaseEntry, false, entry.RWMutex) {
		c.candidates.AddDB(entry.ID)
	}
	return
}
