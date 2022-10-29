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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/assert"
)

func TestRefreshEpoch(t *testing.T) {
	maxDuration := time.Second * 10
	runner := NewGCRunner(WithMaxDuration(maxDuration))
	runner.Start()
	defer runner.Stop()

	assert.True(t, runner.getEpoch().IsEmpty())
	assert.True(t, runner.minCheckpoint().IsEmpty())

	_, updated := runner.tryRefreshEpoch()
	assert.False(t, updated)
	assert.True(t, runner.getEpoch().IsEmpty())

	ckp1 := types.BuildTS(
		time.Now().UTC().UnixNano()-10*maxDuration.Nanoseconds(), 0)
	ckp2 := types.BuildTS(ckp1.Physical()+2*maxDuration.Nanoseconds(), 0)
	ckp3 := types.BuildTS(ckp1.Physical()+4*maxDuration.Nanoseconds(), 0)
	ckp4 := types.BuildTS(ckp1.Physical()+20*maxDuration.Nanoseconds(), 0)

	assert.NoError(t, runner.SendCheckpoint(context.Background(), ckp1))
	time.Sleep(time.Millisecond)

	assert.True(t, runner.minCheckpoint().Equal(ckp1))
	assert.True(t, runner.maxCheckpoint().Equal(ckp1))

	stats := runner.Stats()
	assert.Equal(t, GCState_NonEpoch, stats.State)
	t.Log(stats.String())
	_, updated = runner.tryRefreshEpoch()
	assert.True(t, updated)

	stats = runner.Stats()
	assert.Equal(t, GCState_InEpoch, stats.State)
	t.Log(stats.String())

	_, updated = runner.tryRefreshEpoch()
	assert.False(t, updated)

	assert.NoError(t, runner.SendCheckpoint(context.Background(), ckp2))
	assert.NoError(t, runner.SendCheckpoint(context.Background(), ckp3))
	time.Sleep(time.Millisecond)

	assert.True(t, runner.minCheckpoint().Equal(ckp2))
	assert.True(t, runner.maxCheckpoint().Equal(ckp3))

	for i := 0; i < 10; i++ {
		_, updated = runner.tryRefreshEpoch()
		assert.False(t, updated)
	}

	runner.getReadyForNextEpoch()

	_, updated = runner.tryRefreshEpoch()
	assert.True(t, updated)

	for i := 0; i < 10; i++ {
		_, updated = runner.tryRefreshEpoch()
		assert.False(t, updated)
	}

	assert.True(t, runner.minCheckpoint().Equal(ckp3))
	assert.True(t, runner.maxCheckpoint().Equal(ckp3))
	assert.True(t, runner.getEpoch().Equal(ckp2))
	assert.True(t, runner.SafeTimestamp().Equal(ckp1))

	// set ckp2 stale
	runner.getReadyForNextEpoch()
	// set epoch to ckp3
	_, updated = runner.tryRefreshEpoch()
	assert.True(t, updated)

	for i := 0; i < 10; i++ {
		_, updated = runner.tryRefreshEpoch()
		assert.False(t, updated)
	}
	assert.True(t, runner.SafeTimestamp().Equal(ckp2))

	assert.NoError(t, runner.SendCheckpoint(context.Background(), ckp4))
	time.Sleep(time.Millisecond)

	// set ckp3 stale
	runner.getReadyForNextEpoch()

	// try to refresh epoch. the ckp4 is not a candidate.
	_, updated = runner.tryRefreshEpoch()
	assert.False(t, updated)

	stats = runner.Stats()
	t.Log(stats.String())
}
