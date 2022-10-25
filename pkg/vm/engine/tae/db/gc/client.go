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

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type gcClient struct {
	runner *gcRunner
}

func newGCClient(runner *gcRunner) *gcClient {
	return &gcClient{
		runner: runner,
	}
}

func (client *gcClient) withContextDo(ctx context.Context, fn func() error) (err error) {
	done := make(chan struct{}, 1)
	done <- struct{}{}
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case <-done:
		err = fn()
	}
	return err
}

func (client *gcClient) RegisterCheckpoint(ctx context.Context, ts types.TS) (err error) {
	err = client.withContextDo(ctx, func() error {
		_, err := client.runner.eventQueue.Enqueue(&GCEvent{Type: GCEvent_Checkpoint, Payload: ts})
		return err
	})
	return
}

func (client *gcClient) AddResource(ctx context.Context, resource string) (err error) {
	err = client.withContextDo(ctx, func() error {
		_, err := client.runner.eventQueue.Enqueue(&GCEvent{Type: GCEvent_Checkpoint, Payload: resource})
		return err
	})
	return
}

func (client *gcClient) GetEpoch(ctx context.Context) (ts types.TS, err error) {
	// TODO
	return
}

func (client *gcClient) Status(ctx context.Context) (status string, err error) {
	// TODO
	return
}
