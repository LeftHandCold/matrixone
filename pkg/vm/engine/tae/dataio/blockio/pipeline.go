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

package blockio

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

var (
	errCancelJobOnStop = moerr.NewInternalErrorNoCtx("cancel job on stop")
)

// type Pipeline interface {
// 	Start()
// 	Stop()
// 	Prefetch(location string) error
// 	Fetch(ctx context.Context, location string) (any, error)
// 	AsyncFetch(ctx context.Context, location string) (tasks.Job, error)
// }

func makeName(location string) string {
	return fmt.Sprintf("%s-%d", time.Now().UTC().Nanosecond())
}

func makeJob(
	ctx context.Context,
	fs *objectio.ObjectFS,
	location string,
) *tasks.Job {
	return tasks.NewJob(
		makeName(location),
		ctx,
		func(_ context.Context) (res *tasks.JobResult) {
			// TODO
			return
		},
	)
}

type ioPipeline struct {
	options struct {
		fetchParallism    int
		prefetchParallism int
	}
	fetch struct {
		queue     sm.Queue
		scheduler tasks.JobScheduler
	}

	prefetch struct {
		queue     sm.Queue
		scheduler tasks.JobScheduler
	}

	fs     *objectio.ObjectFS
	waitQ  sm.Queue
	active atomic.Bool

	onceStart sync.Once
	onceStop  sync.Once
}

func NewIOPipeline(
	fs *objectio.ObjectFS,
	opts ...Option,
) *ioPipeline {
	p := new(ioPipeline)
	for _, opt := range opts {
		opt(p)
	}
	p.fillDefaults()
	p.fs = fs

	p.waitQ = sm.NewSafeQueue(
		100000,
		p.options.prefetchParallism*10,
		p.onWait)

	p.prefetch.queue = sm.NewSafeQueue(
		100000,
		p.options.prefetchParallism*2,
		p.onPrefetch)
	p.prefetch.scheduler = tasks.NewParallelJobScheduler(p.options.prefetchParallism)

	p.fetch.queue = sm.NewSafeQueue(
		100000,
		p.options.fetchParallism*2,
		p.onFetch)
	p.fetch.scheduler = tasks.NewParallelJobScheduler(p.options.fetchParallism)
	return p
}

func (p *ioPipeline) fillDefaults() {
	if p.options.fetchParallism <= 0 {
		p.options.fetchParallism = runtime.NumCPU() * 4
	}
	if p.options.prefetchParallism <= 0 {
		p.options.prefetchParallism = runtime.NumCPU() * 4
	}
}

func (p *ioPipeline) Start() {
	p.onceStart.Do(func() {
		p.active.Store(true)
		p.waitQ.Start()
		p.fetch.queue.Start()
		p.prefetch.queue.Start()
	})
}

func (p *ioPipeline) Stop() {
	p.onceStop.Do(func() {
		p.active.Store(false)

		p.prefetch.queue.Stop()
		p.fetch.queue.Stop()

		p.prefetch.scheduler.Stop()
		p.fetch.scheduler.Stop()

		p.waitQ.Stop()
	})
}

func (p *ioPipeline) Fetch(
	ctx context.Context,
	location string,
) (res any, err error) {
	job, err := p.AsyncFetch(ctx, location)
	if err != nil {
		return
	}
	result := job.WaitDone()
	res, err = result.Res, result.Err
	return
}

func (p *ioPipeline) AsyncFetch(
	ctx context.Context,
	location string,
) (job *tasks.Job, err error) {
	job = makeJob(
		ctx,
		p.fs,
		location,
	)
	if _, err = p.fetch.queue.Enqueue(job); err != nil {
		job.DoneWithErr(err)
	}
	return
}

func (p *ioPipeline) Prefetch(location string) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	job := makeJob(
		ctx,
		p.fs,
		location,
	)
	if _, err = p.prefetch.queue.Enqueue(job); err != nil {
		job.DoneWithErr(err)
	}
	return
}

func (p *ioPipeline) onFetch(jobs ...any) {
	for _, j := range jobs {
		job := j.(*tasks.Job)
		logutil.Infof("11111 %s", job.ID())
		if err := p.fetch.scheduler.Schedule(job); err != nil {
			job.DoneWithErr(err)
		}
	}
}

func (p *ioPipeline) onPrefetch(jobs ...any) {
	for _, j := range jobs {
		job := j.(*tasks.Job)
		if !p.active.Load() {
			job.DoneWithErr(errCancelJobOnStop)
			continue
		}
		if err := p.prefetch.scheduler.Schedule(job); err != nil {
			job.DoneWithErr(err)
		} else {
			if _, err := p.waitQ.Enqueue(job); err != nil {
				job.DoneWithErr(err)
			}
		}
	}
}

func (p *ioPipeline) onWait(jobs ...any) {
	for _, j := range jobs {
		job := j.(*tasks.Job)
		res := job.WaitDone()
		if res.Err != nil {
			logutil.Warnf("Prefetch %s err: %s", job.ID(), res.Err)
		}
		bat := res.Res.(*containers.Batch)
		bat.Close()
	}
}
