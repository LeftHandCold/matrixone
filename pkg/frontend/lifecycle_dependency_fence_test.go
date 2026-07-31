// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
)

func TestLifecycleDependencyPublicationLockUsesSystemFeatureRow(t *testing.T) {
	bh := &lineagePublicationLockExec{}
	bh.init()
	ctx := defines.AttachAccountId(context.Background(), 42)

	require.NoError(t, lockLifecycleDependencyPublication(ctx, bh))
	require.Equal(t, uint32(catalog.System_Account), bh.accountID)
	require.Equal(t, []string{
		"update mo_catalog.mo_feature_registry set scope_spec = scope_spec, updated_at = updated_at where feature_code = 'LIFECYCLE'",
	}, bh.executedSQLs)
}

func TestLifecycleCloneDependencyFenceLocksSourceBeforePublication(t *testing.T) {
	steps := make([]string, 0, 3)
	record := func(step string) func() error {
		return func() error {
			steps = append(steps, step)
			return nil
		}
	}

	require.NoError(t, runLifecycleCloneDependencyFence(
		record("source"),
		record("publication"),
		record("binding"),
	))
	require.Equal(t, []string{"source", "publication", "binding"}, steps)
}

func TestLifecycleBindingScopeProbeSQL(t *testing.T) {
	require.Equal(t,
		"select binding_id from mo_catalog.mo_lifecycle_bindings where state in ('ACTIVE','PAUSED','BLOCKED') limit 1",
		lifecycleBindingScopeProbeSQL(0, 0),
	)
	require.Equal(t,
		"select binding_id from mo_catalog.mo_lifecycle_bindings where state in ('ACTIVE','PAUSED','BLOCKED') and database_id=17 limit 1",
		lifecycleBindingScopeProbeSQL(17, 0),
	)
	require.Equal(t,
		"select binding_id from mo_catalog.mo_lifecycle_bindings where state in ('ACTIVE','PAUSED','BLOCKED') and physical_table_id=23 limit 1",
		lifecycleBindingScopeProbeSQL(17, 23),
	)
}

func TestRejectLifecycleBindingInScopeFailsClosed(t *testing.T) {
	bh := &backgroundExecTest{}
	bh.init()
	sql := lifecycleBindingScopeProbeSQL(17, 23)
	bh.sql2result[sql] = newMrsForPasswordOfUser([][]interface{}{{"binding"}})

	err := rejectLifecycleBindingInScope(
		context.Background(),
		bh,
		9,
		17,
		23,
		"CREATE SNAPSHOT",
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Lifecycle-bound")
}

func TestRejectLifecycleBindingInScopeAllowsTenantBeforeCatalogUpgrade(t *testing.T) {
	bh := &backgroundExecTest{}
	bh.init()
	sql := lifecycleBindingScopeProbeSQL(17, 23)
	bh.sql2err[sql] = moerr.NewNoSuchTableNoCtx(
		"mo_catalog",
		"mo_lifecycle_bindings",
	)

	require.NoError(t, rejectLifecycleBindingInScope(
		context.Background(),
		bh,
		9,
		17,
		23,
		"CREATE SNAPSHOT",
	))

	wantErr := moerr.NewInternalErrorNoCtx("catalog read failed")
	bh.sql2err[sql] = wantErr
	err := rejectLifecycleBindingInScope(
		context.Background(),
		bh,
		9,
		17,
		23,
		"CREATE SNAPSHOT",
	)
	require.ErrorIs(t, err, wantErr)
}

func TestLifecycleBackupStateProbesAreScopedAndTerminalAware(t *testing.T) {
	bindingSQL := lifecycleBackupBindingProbeSQL(17)
	require.Contains(t, bindingSQL, "account_id=17")
	require.NotContains(t, bindingSQL, "state")
	require.Contains(t, bindingSQL, "limit 1")

	datasetSQL := lifecycleBackupDatasetProbeSQL(17)
	require.Contains(t, datasetSQL, "account_id=17")
	require.Contains(t, datasetSQL, "state <> 'PURGED'")
	require.Contains(t, datasetSQL, "limit 1")

	require.Contains(t, lifecycleBackupRootProbeSQL, "state <> 'CLEANED'")
	require.Contains(t, lifecycleBackupRootProbeSQL, "limit 1")
}

func TestRejectBackupWithLifecycleStateWhenRetirementEnabled(t *testing.T) {
	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[lifecycleBackupGateProbeSQL] =
		newMrsForPasswordOfUser([][]interface{}{{"LIFECYCLE"}})

	err := rejectBackupWithLifecycleState(
		context.Background(),
		bh,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "enabled")
}

func TestRejectBackupWithLifecycleBindingAfterGateDisabled(t *testing.T) {
	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[lifecycleBackupGateProbeSQL] = newMrsForPasswordOfUser(nil)
	bh.sql2result[getAccountIdNamesSql] = newMrsForGetAllAccounts([][]interface{}{
		{uint64(17), "account-17", "open", uint64(1), nil},
	})
	bh.sql2result[lifecycleBackupBindingProbeSQL(17)] =
		newMrsForPasswordOfUser([][]interface{}{{"binding"}})

	err := rejectBackupWithLifecycleState(context.Background(), bh)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Binding")
}

func TestRejectBackupWithLifecycleDatasetAfterGateDisabled(t *testing.T) {
	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[lifecycleBackupGateProbeSQL] = newMrsForPasswordOfUser(nil)
	bh.sql2result[getAccountIdNamesSql] = newMrsForGetAllAccounts([][]interface{}{
		{uint64(17), "account-17", "open", uint64(1), nil},
	})
	bh.sql2result[lifecycleBackupBindingProbeSQL(17)] = newMrsForPasswordOfUser(nil)
	bh.sql2result[lifecycleBackupDatasetProbeSQL(17)] =
		newMrsForPasswordOfUser([][]interface{}{{"dataset"}})

	err := rejectBackupWithLifecycleState(context.Background(), bh)
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-PURGED")
}

func TestRejectBackupWithUnconvergedLifecycleRootAfterGateDisabled(t *testing.T) {
	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[lifecycleBackupGateProbeSQL] = newMrsForPasswordOfUser(nil)
	bh.sql2result[getAccountIdNamesSql] = newMrsForGetAllAccounts(nil)
	bh.sql2result[lifecycleBackupRootProbeSQL] =
		newMrsForPasswordOfUser([][]interface{}{{"root"}})

	err := rejectBackupWithLifecycleState(context.Background(), bh)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Cleanup Root")
}

func TestAllowBackupWithoutLifecycleStateAfterGateDisabled(t *testing.T) {
	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[lifecycleBackupGateProbeSQL] = newMrsForPasswordOfUser(nil)
	bh.sql2result[getAccountIdNamesSql] = newMrsForGetAllAccounts([][]interface{}{
		{uint64(17), "account-17", "open", uint64(1), nil},
	})
	bh.sql2result[lifecycleBackupBindingProbeSQL(17)] = newMrsForPasswordOfUser(nil)
	bh.sql2result[lifecycleBackupDatasetProbeSQL(17)] = newMrsForPasswordOfUser(nil)
	bh.sql2result[lifecycleBackupRootProbeSQL] = newMrsForPasswordOfUser(nil)

	require.NoError(t, rejectBackupWithLifecycleState(
		context.Background(),
		bh,
	))
	require.Equal(t, []string{
		lifecycleBackupGateProbeSQL,
		getAccountIdNamesSql,
		lifecycleBackupBindingProbeSQL(17),
		lifecycleBackupDatasetProbeSQL(17),
		lifecycleBackupRootProbeSQL,
	}, bh.executedSQLs)
}

func TestAllowBackupWhileTenantLifecycleCatalogUpgradeIsPending(t *testing.T) {
	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[lifecycleBackupGateProbeSQL] = newMrsForPasswordOfUser(nil)
	bh.sql2result[getAccountIdNamesSql] = newMrsForGetAllAccounts([][]interface{}{
		{uint64(17), "account-17", "open", uint64(1), nil},
	})
	bh.sql2err[lifecycleBackupBindingProbeSQL(17)] = moerr.NewNoSuchTableNoCtx(
		"mo_catalog",
		"mo_lifecycle_bindings",
	)
	bh.sql2err[lifecycleBackupDatasetProbeSQL(17)] = moerr.NewNoSuchTableNoCtx(
		"mo_catalog",
		"mo_lifecycle_datasets",
	)
	bh.sql2result[lifecycleBackupRootProbeSQL] = newMrsForPasswordOfUser(nil)

	require.NoError(t, rejectBackupWithLifecycleState(context.Background(), bh))
}
