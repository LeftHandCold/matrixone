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
	"fmt"
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const lifecycleBackupGateProbeSQL = `select feature_code
from mo_catalog.mo_feature_registry
where feature_code = 'LIFECYCLE' and enabled = true
limit 1`

const lifecycleBackupRootProbeSQL = `select root_id
from mo_catalog.mo_lifecycle_cleanup_roots
where state <> 'CLEANED'
limit 1`

func lifecycleBackupBindingProbeSQL(accountID uint32) string {
	return fmt.Sprintf(
		`select binding_id from mo_catalog.mo_lifecycle_bindings
where account_id=%d limit 1`,
		accountID,
	)
}

func lifecycleBackupDatasetProbeSQL(accountID uint32) string {
	return fmt.Sprintf(
		`select dataset_id from mo_catalog.mo_lifecycle_datasets
where account_id=%d and state <> 'PURGED' limit 1`,
		accountID,
	)
}

// lockLifecycleDependencyPublication serializes the rare Lifecycle Binding
// control operation with Snapshot/PITR/Publication/Clone publication. It uses
// the bootstrap-created feature row as an empty-set write barrier and never
// enters ordinary query, DML, or Merge paths.
func lockLifecycleDependencyPublication(
	ctx context.Context,
	background BackgroundExec,
) error {
	systemCtx := defines.AttachAccountId(ctx, catalog.System_Account)
	background.ClearExecResultSet()
	return background.Exec(
		systemCtx,
		"update mo_catalog.mo_feature_registry set scope_spec = scope_spec, updated_at = updated_at where feature_code = 'LIFECYCLE'",
	)
}

func rejectLifecyclePublicationScope(
	ctx context.Context,
	background BackgroundExec,
	accountID uint32,
	databaseID uint64,
	tableNames tree.TableNames,
) error {
	if len(tableNames) == 0 {
		return rejectLifecycleBindingInScope(
			ctx,
			background,
			accountID,
			databaseID,
			0,
			"CREATE PUBLICATION",
		)
	}
	quoted := make([]string, 0, len(tableNames))
	for _, tableName := range tableNames {
		if tableName == nil {
			return moerr.NewInvalidInput(
				ctx,
				"CREATE PUBLICATION contains an empty table identity",
			)
		}
		quoted = append(
			quoted,
			quoteSQLStringLiteral(tableName.ObjectName.String()),
		)
	}
	accountCtx := defines.AttachAccountId(ctx, accountID)
	background.ClearExecResultSet()
	sql := fmt.Sprintf(
		`select b.binding_id from mo_catalog.mo_lifecycle_bindings b
join mo_catalog.mo_tables t on t.rel_id=b.logical_table_id
where b.state in ('ACTIVE','PAUSED','BLOCKED') and b.database_id=%d
and t.relname in (%s) limit 1`,
		databaseID,
		strings.Join(quoted, ","),
	)
	if err := background.Exec(accountCtx, sql); err != nil {
		return err
	}
	results, err := getResultSet(accountCtx, background)
	if err != nil {
		return err
	}
	if execResultArrayHasData(results) {
		return moerr.NewNotSupported(
			ctx,
			"CREATE PUBLICATION while a selected table has a Lifecycle binding",
		)
	}
	return nil
}

func rejectLifecycleBindingByName(
	ctx context.Context,
	background BackgroundExec,
	accountID uint32,
	databaseName string,
	tableName string,
	operation string,
) error {
	accountCtx := defines.AttachAccountId(ctx, accountID)
	background.ClearExecResultSet()
	predicate := fmt.Sprintf(
		"t.reldatabase=%s",
		quoteSQLStringLiteral(databaseName),
	)
	if tableName != "" {
		predicate += fmt.Sprintf(
			" and t.relname=%s",
			quoteSQLStringLiteral(tableName),
		)
	}
	sql := fmt.Sprintf(
		`select b.binding_id from mo_catalog.mo_lifecycle_bindings b
join mo_catalog.mo_tables t on t.rel_id=b.logical_table_id
where b.state in ('ACTIVE','PAUSED','BLOCKED') and %s limit 1`,
		predicate,
	)
	if err := background.Exec(accountCtx, sql); err != nil {
		return err
	}
	results, err := getResultSet(accountCtx, background)
	if err != nil {
		return err
	}
	if execResultArrayHasData(results) {
		return moerr.NewNotSupportedf(
			ctx,
			"%s while the source contains a Lifecycle-bound table",
			operation,
		)
	}
	return nil
}

func lifecycleBindingScopeProbeSQL(
	databaseID uint64,
	physicalTableID uint64,
) string {
	sql := "select binding_id from mo_catalog.mo_lifecycle_bindings where state in ('ACTIVE','PAUSED','BLOCKED')"
	switch {
	case physicalTableID != 0:
		sql += fmt.Sprintf(" and physical_table_id=%d", physicalTableID)
	case databaseID != 0:
		sql += fmt.Sprintf(" and database_id=%d", databaseID)
	}
	return sql + " limit 1"
}

// rejectLifecycleBindingInScope is a management-path-only fail-closed check.
// Callers must hold lockLifecycleDependencyPublication in the same transaction
// so an empty probe cannot race the first Binding insert.
func rejectLifecycleBindingInScope(
	ctx context.Context,
	background BackgroundExec,
	accountID uint32,
	databaseID uint64,
	physicalTableID uint64,
	operation string,
) error {
	accountCtx := defines.AttachAccountId(ctx, accountID)
	background.ClearExecResultSet()
	if err := background.Exec(
		accountCtx,
		lifecycleBindingScopeProbeSQL(databaseID, physicalTableID),
	); err != nil {
		return err
	}
	results, err := getResultSet(accountCtx, background)
	if err != nil {
		return err
	}
	if execResultArrayHasData(results) {
		return moerr.NewNotSupportedf(
			ctx,
			"%s while the target scope contains a Lifecycle-bound table",
			operation,
		)
	}
	return nil
}

func rejectLifecycleBindingsInAllAccounts(
	ctx context.Context,
	background BackgroundExec,
	operation string,
) error {
	accounts, _, err := getAccounts(ctx, background, false)
	if err != nil {
		return err
	}
	for accountID := range accounts {
		if accountID < 0 {
			continue
		}
		if err := rejectLifecycleBindingInScope(
			ctx,
			background,
			uint32(accountID),
			0,
			0,
			operation,
		); err != nil {
			return err
		}
	}
	return nil
}

func rejectLifecycleHistoricalOwnerScope(
	ctx context.Context,
	background BackgroundExec,
	level string,
	accountID uint32,
	objectID uint64,
	operation string,
) error {
	switch level {
	case "cluster":
		return rejectLifecycleBindingsInAllAccounts(ctx, background, operation)
	case "account":
		return rejectLifecycleBindingInScope(
			ctx, background, accountID, 0, 0, operation,
		)
	case "database":
		return rejectLifecycleBindingInScope(
			ctx, background, accountID, objectID, 0, operation,
		)
	case "table":
		return rejectLifecycleBindingInScope(
			ctx, background, accountID, 0, objectID, operation,
		)
	default:
		return moerr.NewInvalidInputf(
			ctx,
			"unknown Lifecycle dependency scope %q",
			level,
		)
	}
}

func lifecycleBackupProbeHasRows(
	ctx context.Context,
	background BackgroundExec,
	sql string,
) (bool, error) {
	background.ClearExecResultSet()
	if err := background.Exec(ctx, sql); err != nil {
		return false, err
	}
	results, err := getResultSet(ctx, background)
	if err != nil {
		return false, err
	}
	return execResultArrayHasData(results), nil
}

// rejectBackupWithLifecycleState makes the Phase 1 support boundary explicit:
// physical BACKUP is not archive-aware. Turning off the release gate stops new
// retirement, but it does not remove Bindings, Datasets, Cleanup Roots, or
// external payloads. BACKUP is therefore allowed only after the gate is off
// and all Lifecycle-owned state in the cluster has been explicitly removed or
// converged. These checks run only in the rare BACKUP control path.
func rejectBackupWithLifecycleState(
	ctx context.Context,
	background BackgroundExec,
) error {
	systemCtx := defines.AttachAccountId(ctx, catalog.System_Account)
	background.ClearExecResultSet()
	if err := background.Exec(systemCtx, lifecycleBackupGateProbeSQL); err != nil {
		return err
	}
	results, err := getResultSet(systemCtx, background)
	if err != nil {
		return err
	}
	if execResultArrayHasData(results) {
		return moerr.NewNotSupported(
			ctx,
			"BACKUP while TAE object Lifecycle retirement is enabled; disable Lifecycle first",
		)
	}

	accounts, _, err := getAccounts(ctx, background, false)
	if err != nil {
		return err
	}
	accountIDs := make([]int, 0, len(accounts))
	for accountID := range accounts {
		// System-account Lifecycle bindings are forbidden by the product
		// contract. Cluster-owned Cleanup Roots are checked separately below.
		if accountID > 0 {
			accountIDs = append(accountIDs, int(accountID))
		}
	}
	sort.Ints(accountIDs)
	for _, rawAccountID := range accountIDs {
		accountID := uint32(rawAccountID)
		accountCtx := defines.AttachAccountId(ctx, accountID)
		hasBinding, err := lifecycleBackupProbeHasRows(
			accountCtx,
			background,
			lifecycleBackupBindingProbeSQL(accountID),
		)
		if err != nil {
			return err
		}
		if hasBinding {
			return moerr.NewNotSupportedf(
				ctx,
				"BACKUP while account %d contains a Lifecycle Binding; UNSET LIFECYCLE first",
				accountID,
			)
		}

		hasDataset, err := lifecycleBackupProbeHasRows(
			accountCtx,
			background,
			lifecycleBackupDatasetProbeSQL(accountID),
		)
		if err != nil {
			return err
		}
		if hasDataset {
			return moerr.NewNotSupportedf(
				ctx,
				"BACKUP while account %d contains a non-PURGED Lifecycle Dataset; PURGE it first",
				accountID,
			)
		}
	}

	hasRoot, err := lifecycleBackupProbeHasRows(
		systemCtx,
		background,
		lifecycleBackupRootProbeSQL,
	)
	if err != nil {
		return err
	}
	if hasRoot {
		return moerr.NewNotSupported(
			ctx,
			"BACKUP while a Lifecycle Cleanup Root has not converged to CLEANED",
		)
	}
	return nil
}
