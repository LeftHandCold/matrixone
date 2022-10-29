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
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/tidwall/btree"
)

// Q: What is `maxDuration`?
// A: `maxDuration` is the safe time interval at which soft-deleted data can be
//    physically wiped.
//																		Time
//    ----------------+-----------------------------------------+------------->
//                    |<------------ maxDuration -------------->|
//                    |                                         |
//                safe-time                                    now

// Q: What is `checkpoint` and what is the use in GC?
// A: `checkpoint` is timestamp that all data created no later than this timestamp
//    has been persisted. This is also the only judgment basis used to clean up WAL.
//    GC runner has been listening for checkpoint events and maintaining a memory
//    storage, and then consumes one by one from small to large.
//    At startup, there's no checkpoint in the memory storage. During startup, the
//    replayer will scan all visible checkpoints (max global and related incremental).
//    GC runner, as the observer of the checkpoint event in the replayer, will receive
//    all the related checkpoint events.

// Q: What is `epoch` and how to determine a `epoch`?
// A: GC always works on a specified `epoch`, which is a timestamp. Resources
//    deleted before this timestamp are candidates to be cleaned up.
//    `epoch` is determined by two factors. One is `maxDuration` and the other
//    is `minCheckpoint`. If the timestamp of `minCheckpoint` is older than the
//    current interval by more than `maxDuration`, we can refresh the `epoch` with
//    `minCheckpoint`. Otherwise, GC runner is working on an empty epoch.
//    At startup, the `epoch` is empty. And the GC runner does nothing on an
//    empty `epoch`. If w/o any checkpoint, GC runner will always work on an empty
//    `epoch`. Then checkpoint events will come one after another and the `epoch`
//    will be refreshed
//
//             ckp1        ckp2          ckp3           ckp4
//              |           |             |              |
//    ----------+-----+-----+-------------+--------------+------+------------->
//                    |<------------ maxDuration -------------->|
//                    |                                         |
//                safe-time                                    now
//    ckp1  :  can be a `epoch` candidate
//    ckp2-4:  cannot be a `epoch` candidate

// Q: How GC runner work on a `epoch`?
//
//          OnCheckpointEvent
//                  |
//                 \|/
//    +-------+  Refresh   +--------+  OnEvent   +------------+ Collect
//    | Empty | ---------> | Active | ---------> | Collecting | -------+
//    +-------+   Epoch    +--------+  Resource  +------------+        | Stale Locations
//       /|\                                                           |
//        |                                          Persist      +----------+
//        + ----------------------------------------------------- | Updating |
//        | Enqueue                                 ObjectTbl     +----------+
//        | PruneEvent
//       \|/                    (scan object table and hard delete resources)
//     +--------+   OnEvent     +--------+
//    (| Queue (|) -----------> | Prunng |
//     +--------+  PruneEvent   +--------+
//       /|\
//        |
//        | Enqueue
//        | PruneEvent
//        |
//      CronJob

// Q: How to optimize IO?
//    A cron job periodically scan the object table and find all objects that are shared by
//    limited blocks. Schedule a merge block or merge metadata task to optimize the IO

type gcRunner struct {
	mu struct {
		sync.RWMutex
		epoch      types.TS
		staleEpoch types.TS
		state      GCStateT
	}

	checkpoints struct {
		mu      sync.RWMutex
		storage []types.TS
	}

	resources struct {
		mu      sync.RWMutex
		storage *btree.BTreeG[*gcResource]
	}

	options struct {
		eventQueueSize int
		maxDuration    time.Duration
	}

	eventQueue sm.Queue
	stopper    *stopper.Stopper

	onceStart sync.Once
	onceStop  sync.Once
}

func NewGCRunner(opts ...GCOption) *gcRunner {
	runner := &gcRunner{}
	runner.resources.storage = btree.NewBTreeG[*gcResource](func(a, b *gcResource) bool {
		return a.epoch.Less(b.epoch)
	})
	for _, opt := range opts {
		opt(runner)
	}
	runner.fillDefaults()
	runner.mu.state = GCState_NonEpoch
	runner.eventQueue = sm.NewSafeQueue(
		runner.options.eventQueueSize,
		100,
		runner.onEvents)
	runner.stopper = stopper.NewStopper("GCRunner")
	return runner
}

func (runner *gcRunner) Start() {
	runner.onceStart.Do(func() {
		runner.eventQueue.Start()
	})
}

func (runner *gcRunner) Stop() {
	runner.onceStop.Do(func() {
		runner.eventQueue.Stop()
		runner.stopper.Stop()
	})
}

func (runner *gcRunner) fillDefaults() {
	if runner.options.eventQueueSize <= 100 || runner.options.eventQueueSize >= 100000 {
		// TODO: default
		runner.options.eventQueueSize = 5000
	}
}

func (runner *gcRunner) Stats() *Stats {
	stats := new(Stats)
	stats.Epoch = runner.getEpoch()
	stats.MinCheckpoint = runner.minCheckpoint()
	stats.MaxCheckpoint = runner.maxCheckpoint()
	stats.State = runner.State()
	return stats
}

func (runner *gcRunner) State() GCStateT {
	runner.mu.RLock()
	defer runner.mu.RUnlock()
	return runner.mu.state
}

func (runner *gcRunner) SendCheckpoint(
	_ context.Context, ts types.TS) (err error) {
	event := new(GCEvent)
	event.Type = GCEvent_Checkpoint
	event.Payload = ts
	_, err = runner.eventQueue.Enqueue(event)
	return
}

// it resets the state to GCState_NonEpoch
// it update the maxStaleEpoch with the current epoch
func (runner *gcRunner) getReadyForNextEpoch() {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	runner.mu.state = GCState_NonEpoch
	runner.mu.staleEpoch = runner.mu.epoch
}

func (runner *gcRunner) onEvents(events ...any) {
	for _, event := range events {
		runner.onEvent(event.(*GCEvent))
	}
}

func (runner *gcRunner) onEvent(event *GCEvent) {
	switch event.Type {
	case GCEvent_Noop:
		return
	case GCEvent_Checkpoint:
		ts := event.Payload.(types.TS)
		runner.onReceiveCheckpoint(ts)
		return
	case GCEvent_Resource:
		resource := event.Payload.(*gcResource)
		runner.onReceiveResource(resource)
		return
	default:
		panic(moerr.NewInternalError("unexpect gc event type: %d", event.Type))
	}
}

func (runner *gcRunner) onReceiveCheckpoint(ts types.TS) {
	runner.checkpoints.mu.Lock()
	defer runner.checkpoints.mu.Unlock()
	if len(runner.checkpoints.storage) == 0 {
		runner.checkpoints.storage = append(runner.checkpoints.storage, ts)
	} else {
		storage := runner.checkpoints.storage
		if storage[len(storage)-1].Less(ts) {
			storage = append(storage, ts)
		}
		runner.checkpoints.storage = storage
	}
}

func (runner *gcRunner) onReceiveResource(resource *gcResource) {
	if _, replaced := runner.resources.storage.Set(resource); replaced {
		panic(moerr.NewInternalError("duplicate resource epoch found: %s", resource.epoch.ToString()))
	}
}

func (runner *gcRunner) minCheckpoint() types.TS {
	runner.checkpoints.mu.RLock()
	defer runner.checkpoints.mu.RUnlock()
	if len(runner.checkpoints.storage) == 0 {
		return types.TS{}
	}
	return runner.checkpoints.storage[0]
}

func (runner *gcRunner) maxCheckpoint() types.TS {
	runner.checkpoints.mu.RLock()
	defer runner.checkpoints.mu.RUnlock()
	if len(runner.checkpoints.storage) == 0 {
		return types.TS{}
	}
	return runner.checkpoints.storage[len(runner.checkpoints.storage)-1]
}

func (runner *gcRunner) popMinCheckpoint() types.TS {
	runner.checkpoints.mu.RLock()
	defer runner.checkpoints.mu.RUnlock()
	if len(runner.checkpoints.storage) == 0 {
		return types.TS{}
	}
	ts := runner.checkpoints.storage[0]
	runner.checkpoints.storage = append(runner.checkpoints.storage[:0],
		runner.checkpoints.storage[1:]...)
	return ts
}

func (runner *gcRunner) getEpoch() types.TS {
	runner.mu.RLock()
	defer runner.mu.RUnlock()
	return runner.mu.epoch
}

func (runner *gcRunner) updateEpoch(nts types.TS) {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	if runner.mu.epoch.GreaterEq(nts) {
		panic(moerr.NewInternalError("update epoch %s with stale epoch %s",
			runner.mu.epoch.ToString(), nts.ToString()))
	}
	runner.mu.epoch = nts
	runner.mu.state = GCState_InEpoch
}

func (runner *gcRunner) tryRefreshEpoch() (candidate types.TS, updated bool) {
	// 1. If a epoch is running, skip this refresh
	state := runner.State()
	if state == GCState_InEpoch {
		return
	}

	// 2. If no checkpoint found, skip this refresh
	checkpointTs := runner.minCheckpoint()
	if checkpointTs.IsEmpty() {
		return
	}

	// 3. Get current epoch
	//    Maybe it's 0 at the beginning.
	//    The actual epoch used
	curr := runner.getEpoch()

	// 4. Get a candidate epoch by rule
	candidate = types.BuildTS(time.Now().UTC().UnixNano()-
		runner.options.maxDuration.Nanoseconds(), 0)

	// 5. If the epoch candidate is less than current epoch, do nothing
	if candidate.LessEq(curr) {
		updated = false
		return
	}

	// 6. If the candidate is greater equal than the min checkpoint, using the
	//    min checkpoint as the epoch candidate
	if candidate.Greater(checkpointTs) {
		candidate = runner.popMinCheckpoint()
		runner.updateEpoch(candidate)
		updated = true
		return
	}

	return
}
