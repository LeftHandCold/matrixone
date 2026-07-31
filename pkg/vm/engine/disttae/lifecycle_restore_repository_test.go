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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	lifecyclepkg "github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/lifecycle"
	"github.com/stretchr/testify/require"
)

func TestSQLRestoreInitializeOwnsLeaseTableAndAttemptInOneTransaction(t *testing.T) {
	mp := mpool.MustNewZero()
	var statements []string
	sqlExecutor := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		statements = append(statements, strings.ToLower(sql))
		switch len(statements) {
		case 1:
			return executor.Result{Mp: mp}, nil
		case 2, 3, 5:
			return executor.Result{AffectedRows: 1, Mp: mp}, nil
		case 4:
			value := executor.NewMemResult([]types.Type{types.T_uint64.ToType()}, mp)
			value.NewBatch()
			require.NoError(t, executor.AppendFixedRows(
				value,
				0,
				[]uint64{88},
			))
			return value.GetResult(), nil
		default:
			t.Fatalf("unexpected SQL %s", sql)
			return executor.Result{}, nil
		}
	})
	repository := SQLRestoreRepository{
		AccountID:          17,
		TargetDatabaseName: "history",
		Executor:           sqlExecutor,
		Engine:             lifecycleRestoreEngineStub{},
		MPool:              mp,
	}
	attempt, err := repository.Initialize(
		context.Background(),
		lifecyclepkg.RestoreInitializeRequest{
			Dataset: lifecyclepkg.RestoreDataset{
				DatasetID: "22222222-2222-2222-2222-222222222222",
				Version:   3,
			},
			Attempt: lifecyclepkg.RestoreAttempt{
				RestoreID:         "11111111-1111-1111-1111-111111111111",
				LeaseID:           "33333333-3333-3333-3333-333333333333",
				Deadline:          time.Now().Add(time.Minute),
				StagingDatabaseID: 7,
				HiddenName:        "__mo_lifecycle_restore_1",
				TargetDatabaseID:  7,
				TargetName:        "events_history",
			},
			HiddenCreateSQL: "create table history.__mo_lifecycle_restore_1(id bigint)",
		},
	)
	require.NoError(t, err)
	require.Equal(t, uint64(88), attempt.StagingTableID)
	require.Equal(t, "IMPORTING", attempt.State)
	require.Contains(t, statements[1], "restore_lease_id")
	require.Contains(t, statements[1], "and restore_lease_id is null")
	require.NotContains(t, statements[1], "or restore_deadline")
	require.Contains(t, statements[2], "create table")
	require.Contains(t, statements[4], "insert into mo_catalog.mo_lifecycle_restore_attempts")
}

func TestSQLRestorePurgeRequiresLeaseFullyReleased(t *testing.T) {
	mp := mpool.MustNewZero()
	calls := 0
	sqlExecutor := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		calls++
		lower := strings.ToLower(sql)
		require.Contains(t, lower, "restore_lease_id is null")
		require.NotContains(t, lower, "restore_deadline")
		return executor.Result{AffectedRows: 0, Mp: mp}, nil
	})
	repository := SQLRestoreRepository{
		AccountID: 17,
		Executor:  sqlExecutor,
	}
	err := repository.RequestPurge(
		context.Background(),
		lifecyclepkg.RestoreDataset{
			DatasetID: "22222222-2222-2222-2222-222222222222",
			State:     "PUBLISHED",
			Version:   3,
		},
		time.Now(),
	)
	require.ErrorIs(t, err, lifecyclepkg.ErrRestoreInProgress)
	require.Equal(t, 1, calls)
}

func TestSQLRestoreFindsResumableAttemptByDatasetAndTarget(t *testing.T) {
	mp := mpool.MustNewZero()
	sqlExecutor := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		lower := strings.ToLower(sql)
		require.Contains(t, lower, "a.state='importing'")
		require.Contains(t, lower, "a.deadline>utc_timestamp()")
		require.Contains(t, lower, "a.state='done'")
		require.Contains(t, lower, "a.verified_content_hash is not null")
		require.Contains(t, lower, "t.rel_id=a.staging_table_id")
		require.Contains(t, lower, "t.relname=a.target_name")
		require.Contains(t, lower, "h.relname=a.hidden_name")
		require.Contains(t, strings.ToLower(sql), "target_database_id=7")
		require.Contains(t, strings.ToLower(sql), "target_name='events_history'")
		value := executor.NewMemResult([]types.Type{
			types.T_varchar.ToType(),
			types.T_varchar.ToType(),
			types.T_varchar.ToType(),
			types.T_varchar.ToType(),
			types.T_uint64.ToType(),
			types.T_uint64.ToType(),
			types.T_varchar.ToType(),
			types.T_uint64.ToType(),
			types.T_varchar.ToType(),
			types.T_varchar.ToType(),
			types.T_uint64.ToType(),
			types.T_uint64.ToType(),
			types.T_varchar.ToType(),
		}, mp)
		value.NewBatchWithRowCount(1)
		require.NoError(t, executor.AppendStringRows(value, 0, []string{
			"11111111111111111111111111111111",
		}))
		require.NoError(t, executor.AppendStringRows(value, 1, []string{
			"22222222222222222222222222222222",
		}))
		require.NoError(t, executor.AppendStringRows(value, 2, []string{
			"33333333333333333333333333333333",
		}))
		require.NoError(t, executor.AppendStringRows(value, 3, []string{
			"2026-08-01 09:00:00.000000",
		}))
		require.NoError(t, executor.AppendFixedRows(value, 4, []uint64{7}))
		require.NoError(t, executor.AppendFixedRows(value, 5, []uint64{88}))
		require.NoError(t, executor.AppendStringRows(value, 6, []string{
			"__mo_lifecycle_restore_1",
		}))
		require.NoError(t, executor.AppendFixedRows(value, 7, []uint64{7}))
		require.NoError(t, executor.AppendStringRows(value, 8, []string{
			"events_history",
		}))
		require.NoError(t, executor.AppendStringRows(value, 9, []string{
			"DONE",
		}))
		require.NoError(t, executor.AppendFixedRows(value, 10, []uint64{4}))
		require.NoError(t, executor.AppendFixedRows(value, 11, []uint64{100}))
		require.NoError(t, executor.AppendStringRows(value, 12, []string{""}))
		return value.GetResult(), nil
	})
	repository := SQLRestoreRepository{
		AccountID:          17,
		TargetDatabaseName: "history",
		Executor:           sqlExecutor,
	}
	attempt, found, err := repository.FindResumable(
		context.Background(),
		"22222222-2222-2222-2222-222222222222",
		7,
		"events_history",
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(
		t,
		"11111111-1111-1111-1111-111111111111",
		attempt.RestoreID,
	)
	require.Equal(t, uint64(4), attempt.NextChunkOrdinal)
	require.Equal(t, uint64(100), attempt.RestoredRows)
	require.Equal(t, "DONE", attempt.State)
	require.Equal(t, "history", attempt.TargetDatabaseName)
}

func TestValidateLifecycleRestoreHiddenIdentityIncludesDatabase(t *testing.T) {
	attempt := lifecyclepkg.RestoreAttempt{
		StagingDatabaseID: 7,
		StagingTableID:    88,
		HiddenName:        "__mo_lifecycle_restore_1",
	}
	require.NoError(t, validateLifecycleRestoreHiddenIdentity(
		attempt,
		7,
		"__mo_lifecycle_restore_1",
		88,
	))
	require.Error(t, validateLifecycleRestoreHiddenIdentity(
		attempt,
		8,
		"__mo_lifecycle_restore_1",
		88,
	))
	require.Error(t, validateLifecycleRestoreHiddenIdentity(
		attempt,
		7,
		"events_history",
		88,
	))
}

func TestSQLRestorePublishRetryStopsAtDoneBeforeHiddenIdentityLookup(t *testing.T) {
	mp := mpool.MustNewZero()
	verified := [32]byte{1, 2, 3}
	calls := 0
	sqlExecutor := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		calls++
		require.Contains(
			t,
			strings.ToLower(sql),
			"from mo_catalog.mo_lifecycle_restore_attempts",
		)
		value := executor.NewMemResult([]types.Type{
			types.T_varchar.ToType(),
			types.T_varchar.ToType(),
			types.T_varchar.ToType(),
			types.T_varchar.ToType(),
			types.T_uint64.ToType(),
			types.T_uint64.ToType(),
			types.T_varchar.ToType(),
			types.T_uint64.ToType(),
			types.T_varchar.ToType(),
			types.T_varchar.ToType(),
			types.T_uint64.ToType(),
			types.T_uint64.ToType(),
			types.T_varchar.ToType(),
		}, mp)
		value.NewBatchWithRowCount(1)
		require.NoError(t, executor.AppendStringRows(
			value, 0, []string{"11111111111111111111111111111111"},
		))
		require.NoError(t, executor.AppendStringRows(
			value, 1, []string{"22222222222222222222222222222222"},
		))
		require.NoError(t, executor.AppendStringRows(
			value, 2, []string{"33333333333333333333333333333333"},
		))
		require.NoError(t, executor.AppendStringRows(
			value, 3, []string{"2026-08-01 09:00:00.000000"},
		))
		require.NoError(t, executor.AppendFixedRows(value, 4, []uint64{7}))
		require.NoError(t, executor.AppendFixedRows(value, 5, []uint64{88}))
		require.NoError(t, executor.AppendStringRows(
			value, 6, []string{"events_history"},
		))
		require.NoError(t, executor.AppendFixedRows(value, 7, []uint64{7}))
		require.NoError(t, executor.AppendStringRows(
			value, 8, []string{"events_history"},
		))
		require.NoError(t, executor.AppendStringRows(
			value, 9, []string{"DONE"},
		))
		require.NoError(t, executor.AppendFixedRows(value, 10, []uint64{4}))
		require.NoError(t, executor.AppendFixedRows(value, 11, []uint64{100}))
		require.NoError(t, executor.AppendStringRows(
			value, 12, []string{hex.EncodeToString(verified[:])},
		))
		return value.GetResult(), nil
	})
	repository := SQLRestoreRepository{
		AccountID: 17,
		Executor:  sqlExecutor,
	}
	require.NoError(t, repository.Publish(
		context.Background(),
		lifecyclepkg.RestoreAttempt{
			RestoreID:    "11111111-1111-1111-1111-111111111111",
			DatasetID:    "22222222-2222-2222-2222-222222222222",
			LeaseID:      "33333333-3333-3333-3333-333333333333",
			VerifiedHash: verified,
		},
		verified,
		lifecyclepkg.SchemaDescriptor{},
		nil,
	))
	require.Equal(t, 1, calls)
}

type lifecycleRestoreEngineStub struct{}

func (lifecycleRestoreEngineStub) GetRelationById(
	context.Context,
	client.TxnOperator,
	uint64,
) (string, string, engine.Relation, error) {
	panic("not used by initialization")
}
