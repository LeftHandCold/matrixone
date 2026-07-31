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

package lifecycle

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestSQLMetadataCompactorUsesBoundedTerminalDeletes(t *testing.T) {
	mp := mpool.MustNewZero()
	fake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{
			{
				contains:  "from mo_catalog.mo_account where account_id > 0",
				accountID: 0,
				result:    lifecycleAccountResult(t, mp, 17),
			},
			{
				contains: `select root_id from mo_catalog.mo_lifecycle_cleanup_roots
where owner_account_id=17 and mode='TTL_REWRITE'
and state='COMMIT_UNKNOWN' limit 1`,
				accountID: 0,
				result:    executor.Result{},
			},
			{
				contains:  "delete from mo_catalog.mo_lifecycle_restore_chunks",
				accountID: 17,
				result:    executor.Result{AffectedRows: 8},
			},
			{
				contains:  "delete from mo_catalog.mo_lifecycle_restore_attempts",
				accountID: 17,
				result:    executor.Result{AffectedRows: 1},
			},
			{
				contains:  "delete from mo_catalog.mo_lifecycle_ttl_receipts",
				accountID: 17,
				result:    executor.Result{AffectedRows: 8},
			},
			{
				contains:  "delete from mo_catalog.mo_lifecycle_datasets",
				accountID: 17,
				result:    executor.Result{AffectedRows: 1},
			},
			{
				contains:  "delete from mo_catalog.mo_lifecycle_cleanup_roots",
				accountID: 0,
				result:    executor.Result{AffectedRows: 1},
			},
		},
	}
	next, _, err := (SQLMetadataCompactor{Executor: fake}).CompactPage(
		context.Background(),
		0,
		time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		30*24*time.Hour,
		8,
		64,
	)
	require.NoError(t, err)
	require.Equal(t, uint32(17), next)
	require.Equal(t, len(fake.steps), fake.offset)
}

func TestSQLMetadataCompactorRejectsUnboundedConfiguration(t *testing.T) {
	_, _, err := (SQLMetadataCompactor{}).CompactPage(
		context.Background(),
		0,
		time.Now(),
		0,
		0,
		0,
	)
	require.Error(t, err)
}

func TestSQLMetadataCompactorKeepsTTLReceiptsForUnknownRewrite(t *testing.T) {
	mp := mpool.MustNewZero()
	fake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{
			{
				contains:  "from mo_catalog.mo_account where account_id > 0",
				accountID: 0,
				result:    lifecycleAccountResult(t, mp, 17),
			},
			{
				contains: `select root_id from mo_catalog.mo_lifecycle_cleanup_roots
where owner_account_id=17 and mode='TTL_REWRITE'
and state='COMMIT_UNKNOWN' limit 1`,
				accountID: 0,
				result:    lifecycleAccountResult(t, mp, 1),
			},
			{
				contains:    "delete from mo_catalog.mo_lifecycle_restore_chunks",
				notContains: "mo_lifecycle_ttl_receipts",
				accountID:   17,
				result:      executor.Result{AffectedRows: 8},
			},
			{
				contains:    "delete from mo_catalog.mo_lifecycle_restore_attempts",
				notContains: "mo_lifecycle_ttl_receipts",
				accountID:   17,
				result:      executor.Result{AffectedRows: 1},
			},
			{
				contains:    "delete from mo_catalog.mo_lifecycle_datasets",
				notContains: "mo_lifecycle_ttl_receipts",
				accountID:   17,
				result:      executor.Result{AffectedRows: 1},
			},
			{
				contains:  "delete from mo_catalog.mo_lifecycle_cleanup_roots",
				accountID: 0,
				result:    executor.Result{AffectedRows: 1},
			},
		},
	}
	next, _, err := (SQLMetadataCompactor{Executor: fake}).CompactPage(
		context.Background(),
		0,
		time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		30*24*time.Hour,
		8,
		64,
	)
	require.NoError(t, err)
	require.Equal(t, uint32(17), next)
	require.Equal(t, len(fake.steps), fake.offset)
}

func TestTerminalLifecycleMetadataDeletesKeepPurgedDatasetsLonger(t *testing.T) {
	terminalCutoff := "'2026-07-02 00:00:00.000000'"
	datasetCutoff := "'2026-05-03 00:00:00.000000'"
	deletes := terminalLifecycleMetadataDeletes(
		terminalCutoff,
		datasetCutoff,
		64,
		true,
	)

	require.Len(t, deletes, 4)
	require.Contains(t, deletes[0], terminalCutoff)
	require.Contains(t, deletes[1], terminalCutoff)
	require.Contains(t, deletes[2], terminalCutoff)
	require.Contains(t, deletes[3], datasetCutoff)
	require.NotContains(t, deletes[3], terminalCutoff)
}
