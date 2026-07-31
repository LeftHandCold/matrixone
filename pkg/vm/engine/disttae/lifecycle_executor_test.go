// Copyright 2026 Matrix Origin
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

package disttae

import (
	"context"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	lifecyclepkg "github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/lifecycle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestLifecycleCutoffUsesEvaluationTimezoneAndGrace(t *testing.T) {
	evaluation := time.Date(2026, 7, 31, 22, 0, 0, 0, time.UTC)
	cutoff, encoded, err := lifecycleCutoff(
		evaluation,
		90,
		2,
		"Asia/Shanghai",
		types.T_date,
	)
	require.NoError(t, err)
	require.Equal(t, "2026-05-01T06:00:00+08:00", cutoff.Format(time.RFC3339))
	require.Equal(t, int64(types.DateFromCalendar(2026, 5, 1)), encoded)
}

func TestLifecycleObjectExpirationUsesOnlyLifecycleSortKeyProof(t *testing.T) {
	stats := objectio.NewObjectStats()
	zoneMap := index.NewZM(types.T_timestamp, 0)
	zoneMap.Update(types.Timestamp(100))
	zoneMap.Update(types.Timestamp(200))
	require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(stats, zoneMap))

	whole, skip := lifecycleObjectExpirationByZoneMap(
		*stats,
		0,
		0,
		types.T_timestamp,
		201,
	)
	require.True(t, whole)
	require.False(t, skip)

	whole, skip = lifecycleObjectExpirationByZoneMap(
		*stats,
		0,
		0,
		types.T_timestamp,
		100,
	)
	require.False(t, whole)
	require.True(t, skip)

	whole, skip = lifecycleObjectExpirationByZoneMap(
		*stats,
		1,
		0,
		types.T_timestamp,
		201,
	)
	require.False(t, whole)
	require.False(t, skip)
}

func TestLifecycleDiscoveryCursorTreatsCorruptionAsResettableHint(t *testing.T) {
	snapshot := types.BuildTS(123, 4)
	name := objectio.BuildObjectName(objectio.NewSegmentid(), 7).Short()
	cursor := lifecycleDiscoveryCursor(lifecyclepkg.Binding{
		ScanSnapshotHex:       hex.EncodeToString(snapshot[:]),
		ScanLastObjectNameHex: hex.EncodeToString(name[:]),
	})
	require.Equal(t, snapshot, cursor.Snapshot)
	require.True(t, cursor.HasLastObject)
	require.Equal(t, *name, cursor.LastObjectName)

	cursor = lifecycleDiscoveryCursor(lifecyclepkg.Binding{
		ScanSnapshotHex:       "bad",
		ScanLastObjectNameHex: "bad",
	})
	require.True(t, cursor.Snapshot.IsEmpty())
	require.False(t, cursor.HasLastObject)
}

func TestLifecycleRewriteSlotBoundsLocalRewriteConcurrency(t *testing.T) {
	slots := make(chan struct{}, 1)
	releaseFirst, err := tryAcquireLifecycleRewriteSlot(
		context.Background(),
		slots,
	)
	require.NoError(t, err)

	_, err = tryAcquireLifecycleRewriteSlot(context.Background(), slots)
	require.ErrorContains(t, err, "RESOURCE_BLOCKED")

	releaseFirst()
	releaseSecond, err := tryAcquireLifecycleRewriteSlot(
		context.Background(),
		slots,
	)
	require.NoError(t, err)
	releaseSecond()
}

func TestPlanLifecycleObjectTasksBatchesWholeAndKeepsMixedSingleton(t *testing.T) {
	wholeA := lifecyclePlanTestSource(t, 128<<20)
	wholeB := lifecyclePlanTestSource(t, 256<<20)
	mixed := lifecyclePlanTestSource(t, 512<<20)
	wholeC := lifecyclePlanTestSource(t, 64<<20)

	plans := planLifecycleObjectTasks([]lifecycleObjectPlanInput{
		{Source: wholeA, Whole: true},
		{Source: wholeB, Whole: true},
		{Source: mixed},
		{Source: wholeC, Whole: true},
	})

	require.Len(t, plans, 3)
	require.True(t, plans[0].Whole)
	require.Len(t, plans[0].Sources, 2)
	require.Equal(t, uint64(384<<20), plans[0].SourceBytes)
	require.False(t, plans[1].Whole)
	require.Len(t, plans[1].Sources, 1)
	require.Equal(t, uint64(512<<20), plans[1].SourceBytes)
	require.True(t, plans[2].Whole)
	require.Len(t, plans[2].Sources, 1)
}

func TestPlanLifecycleObjectTasksBoundsWholeSourceCountAndBytes(t *testing.T) {
	inputs := make([]lifecycleObjectPlanInput, 0, 65)
	for range 65 {
		inputs = append(inputs, lifecycleObjectPlanInput{
			Source: lifecyclePlanTestSource(t, 1),
			Whole:  true,
		})
	}
	plans := planLifecycleObjectTasks(inputs)
	require.Len(t, plans, 2)
	require.Len(t, plans[0].Sources, lifecycleWholeBatchMaxSources)
	require.Len(t, plans[1].Sources, 1)

	plans = planLifecycleObjectTasks([]lifecycleObjectPlanInput{
		{Source: lifecyclePlanTestSource(t, 3<<30), Whole: true},
		{Source: lifecyclePlanTestSource(t, 3<<30), Whole: true},
	})
	require.Len(t, plans, 2)
	require.LessOrEqual(
		t,
		plans[0].SourceBytes,
		uint64(lifecycleWholeBatchMaxSourceBytes),
	)
	require.LessOrEqual(
		t,
		plans[1].SourceBytes,
		uint64(lifecycleWholeBatchMaxSourceBytes),
	)
}

func lifecyclePlanTestSource(t *testing.T, sourceBytes uint32) objectio.ObjectEntry {
	t.Helper()
	objectID := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(
		&objectID,
		false,
		true,
		false,
	)
	require.NoError(t, objectio.SetObjectStatsOriginSize(stats, sourceBytes))
	return objectio.ObjectEntry{ObjectStats: *stats}
}

func TestLifecycleCoordinatorRunSlotDoesNotQueueDuplicateRun(t *testing.T) {
	slots := make(chan struct{}, 1)
	releaseFirst, acquired := tryAcquireLifecycleCoordinatorRunSlot(slots)
	require.True(t, acquired)

	_, acquired = tryAcquireLifecycleCoordinatorRunSlot(slots)
	require.False(t, acquired)

	releaseFirst()
	releaseSecond, acquired := tryAcquireLifecycleCoordinatorRunSlot(slots)
	require.True(t, acquired)
	releaseSecond()
}

func TestLifecycleDisabledContinuesMaintenanceAndSkipsBindingScan(t *testing.T) {
	fake := &disabledLifecycleSQLExecutor{
		t:  t,
		mp: mpool.MustNewZero(),
	}
	run := LifecycleTaskExecutorFactory(nil, nil, fake, nil, nil)
	require.NoError(t, run(context.Background(), &task.AsyncTask{}))
	require.NotEmpty(t, fake.queries)
	sawAccountPage := false
	sawMetadataCompaction := false
	for _, query := range fake.queries {
		if strings.Contains(query, "mo_account") {
			sawAccountPage = true
		}
		if strings.Contains(query, "delete from mo_catalog.mo_lifecycle_cleanup_roots") {
			sawMetadataCompaction = true
		}
		require.NotContains(t, query, "mo_lifecycle_bindings")
		require.NotContains(t, query, "mo_lifecycle_restore_attempts")
	}
	require.True(t, sawAccountPage)
	require.True(t, sawMetadataCompaction)
}

type disabledLifecycleSQLExecutor struct {
	t       *testing.T
	mp      *mpool.MPool
	queries []string
}

func (fake *disabledLifecycleSQLExecutor) Exec(
	_ context.Context,
	sql string,
	_ executor.Options,
) (executor.Result, error) {
	fake.queries = append(fake.queries, strings.ToLower(sql))
	if strings.Contains(strings.ToLower(sql), "mo_feature_registry") {
		value := batch.NewWithSize(2)
		value.Vecs[0] = vector.NewVec(types.T_bool.ToType())
		value.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
		require.NoError(fake.t, vector.AppendFixed(
			value.Vecs[0], false, false, fake.mp,
		))
		require.NoError(fake.t, vector.AppendBytes(
			value.Vecs[1], []byte(`{"archive_stages":[]}`), false, fake.mp,
		))
		value.SetRowCount(1)
		return executor.Result{
			Batches: []*batch.Batch{value},
			Mp:      fake.mp,
		}, nil
	}
	return executor.Result{Mp: fake.mp}, nil
}

func (*disabledLifecycleSQLExecutor) ExecTxn(
	context.Context,
	func(executor.TxnExecutor) error,
	executor.Options,
) error {
	panic("unexpected Lifecycle disabled transaction")
}
