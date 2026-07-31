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

func TestRejectBackupWhenLifecycleRetirementEnabled(t *testing.T) {
	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[lifecycleBackupGateProbeSQL] =
		newMrsForPasswordOfUser([][]interface{}{{"LIFECYCLE"}})

	err := rejectBackupWhenLifecycleRetirementEnabled(
		context.Background(),
		bh,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "BACKUP")
}

func TestAllowBackupWhenLifecycleRetirementDisabled(t *testing.T) {
	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[lifecycleBackupGateProbeSQL] =
		newMrsForPasswordOfUser(nil)
	require.NoError(t, rejectBackupWhenLifecycleRetirementEnabled(
		context.Background(),
		bh,
	))
}
