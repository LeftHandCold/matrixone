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

func TestLifecycleAlterPublicationScopeFenceIsNarrowAndOrdered(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), 17)
	lockSQL := "update mo_catalog.mo_feature_registry set scope_spec = scope_spec, updated_at = updated_at where feature_code = 'LIFECYCLE'"
	probeSQL := `select b.binding_id from mo_catalog.mo_lifecycle_bindings b
join mo_catalog.mo_tables t on t.rel_id=b.logical_table_id
where b.state in ('ACTIVE','PAUSED','BLOCKED') and b.database_id=7
and t.relname in ('t1','t2') limit 1`

	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[probeSQL] = newMrsForPasswordOfUser(nil)
	require.NoError(t, fenceLifecycleAlterPublicationScope(
		ctx,
		bh,
		true,
		17,
		7,
		"t1,t2",
	))
	require.Equal(t, []string{lockSQL, probeSQL}, bh.executedSQLs)

	bh = &backgroundExecTest{}
	bh.init()
	require.NoError(t, fenceLifecycleAlterPublicationScope(
		ctx,
		bh,
		false,
		17,
		7,
		"t1,t2",
	))
	require.Empty(t, bh.executedSQLs)
}

func TestLifecycleAlterPublicationScopeFenceRejectsFinalBoundScope(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), 17)
	probeSQL := `select b.binding_id from mo_catalog.mo_lifecycle_bindings b
join mo_catalog.mo_tables t on t.rel_id=b.logical_table_id
where b.state in ('ACTIVE','PAUSED','BLOCKED') and b.database_id=7
and t.relname in ('t2') limit 1`
	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[probeSQL] = newMrsForPasswordOfUser(
		[][]interface{}{{"binding"}},
	)

	err := fenceLifecycleAlterPublicationScope(
		ctx,
		bh,
		true,
		17,
		7,
		"t2",
	)
	require.ErrorContains(t, err, "ALTER PUBLICATION")
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
