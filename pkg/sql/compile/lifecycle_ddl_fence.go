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

package compile

import (
	"context"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type lifecycleDDLQuery func(string, int32) (executor.Result, error)

type lifecycleForeignKeyParent struct {
	databaseName string
	tableName    string
}

// ignoreMissingLifecycleCatalog keeps ordinary management DDL usable while a
// tenant's asynchronous version upgrade has not created the Lifecycle tables
// yet. Table absence proves that no Lifecycle Binding can exist; every other
// Catalog error remains fail-closed.
func ignoreMissingLifecycleCatalog(err error) error {
	if err == nil || moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
		return nil
	}
	return err
}

func lifecycleForeignKeyIsSelfReference(
	childDatabaseName string,
	childTableName string,
	parentDatabaseName string,
	parentTableName string,
) bool {
	return strings.EqualFold(parentDatabaseName, childDatabaseName) &&
		strings.EqualFold(parentTableName, childTableName)
}

func lifecycleForeignKeyParents(
	ctx context.Context,
	childDatabaseName string,
	childTableName string,
	parentDatabaseNames []string,
	parentTableNames []string,
) ([]lifecycleForeignKeyParent, error) {
	if len(parentDatabaseNames) != len(parentTableNames) {
		return nil, moerr.NewInternalError(
			ctx,
			"foreign key parent metadata is incomplete",
		)
	}
	parents := make([]lifecycleForeignKeyParent, 0, len(parentTableNames))
	seen := make(map[string]struct{}, len(parentTableNames))
	for index, tableName := range parentTableNames {
		databaseName := parentDatabaseNames[index]
		if lifecycleForeignKeyIsSelfReference(
			childDatabaseName,
			childTableName,
			databaseName,
			tableName,
		) {
			continue
		}
		key := strings.ToLower(databaseName) + "\x00" + strings.ToLower(tableName)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		parents = append(parents, lifecycleForeignKeyParent{
			databaseName: databaseName,
			tableName:    tableName,
		})
	}
	return parents, nil
}

func rejectBoundLifecycleDDL(
	ctx context.Context,
	accountID uint32,
	physicalTableID uint64,
	operation string,
	query lifecycleDDLQuery,
) error {
	// Lifecycle bindings are forbidden in the system account. Keep ordinary
	// system-account DDL completely outside the Lifecycle Catalog path.
	if accountID == 0 {
		return nil
	}
	if query == nil || physicalTableID == 0 {
		return moerr.NewInternalError(ctx, "Lifecycle DDL fence input is incomplete")
	}
	result, err := query(
		fmt.Sprintf(
			`select binding_id from mo_catalog.mo_lifecycle_bindings
where account_id=%d and physical_table_id=%d
and state in ('ACTIVE','PAUSED','BLOCKED') limit 1`,
			accountID,
			physicalTableID,
		),
		int32(accountID),
	)
	if err != nil {
		return ignoreMissingLifecycleCatalog(err)
	}
	defer result.Close()
	bound := false
	result.ReadRows(func(rows int, _ []*vector.Vector) bool {
		bound = rows > 0
		return false
	})
	if bound {
		return moerr.NewNotSupportedf(
			ctx,
			"%s on a Lifecycle-bound table; UNSET LIFECYCLE first",
			operation,
		)
	}
	return nil
}

func (c *Compile) rejectBoundLifecycleDDL(
	physicalTableID uint64,
	operation string,
) error {
	// Lifecycle bindings are a user-facing control-plane contract. Background
	// and internal DDL re-entry must keep using the ordinary MO path and must
	// not acquire a new Lifecycle Catalog dependency.
	if !c.proc.Base.IsFrontend {
		return nil
	}
	// The caller and SET LIFECYCLE already hold the same target mo_tables row
	// lock. That existing lock closes their first-Binding race, so table DDL
	// needs only one indexed Binding lookup and must not take the cluster-wide
	// feature-row barrier used by Snapshot/PITR/Publication scope publishing.
	accountID, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}
	return rejectBoundLifecycleDDL(
		c.proc.Ctx,
		accountID,
		physicalTableID,
		operation,
		c.runSqlWithResult,
	)
}

// lockAndRejectLifecycleForeignKeyParents closes the reverse dependency race
// between SET LIFECYCLE on a parent and CREATE/ALTER FOREIGN KEY on a child.
// It runs only for user-facing DDL, after using the same mo_tables row lock as
// SET LIFECYCLE, and reuses the indexed Binding lookup above.
func (c *Compile) lockAndRejectLifecycleForeignKeyParents(
	childDatabaseName string,
	childTableName string,
	parentDatabaseNames []string,
	parentTableNames []string,
) error {
	if !c.proc.Base.IsFrontend {
		return nil
	}
	parents, err := lifecycleForeignKeyParents(
		c.proc.Ctx,
		childDatabaseName,
		childTableName,
		parentDatabaseNames,
		parentTableNames,
	)
	if err != nil {
		return err
	}
	for _, parent := range parents {
		if err = lockMoTable(
			c,
			parent.databaseName,
			parent.tableName,
			lock.LockMode_Exclusive,
		); err != nil {
			return err
		}
		if err = c.rejectLifecycleForeignKeyParentAfterLock(
			parent.databaseName,
			parent.tableName,
		); err != nil {
			return err
		}
	}
	return nil
}

func (c *Compile) rejectLifecycleForeignKeyParentAfterLock(
	databaseName string,
	tableName string,
) error {
	if !c.proc.Base.IsFrontend {
		return nil
	}
	database, err := c.e.Database(
		c.proc.Ctx,
		databaseName,
		c.proc.GetTxnOperator(),
	)
	if err != nil {
		return err
	}
	relation, err := database.Relation(c.proc.Ctx, tableName, nil)
	if err != nil {
		return err
	}
	return c.rejectBoundLifecycleDDL(
		relation.GetTableID(c.proc.Ctx),
		"ADD FOREIGN KEY referencing",
	)
}

func (c *Compile) detachLifecycleBindingForDrop(
	physicalTableID uint64,
) error {
	if !c.proc.Base.IsFrontend {
		return nil
	}
	accountID, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}
	return ignoreMissingLifecycleCatalog(c.runSqlWithAccountId(
		fmt.Sprintf(
			`delete from mo_catalog.mo_lifecycle_bindings
where account_id=%d and physical_table_id=%d`,
			accountID,
			physicalTableID,
		),
		int32(accountID),
	))
}

func lifecycleDatabaseDropBindingDeleteSQL(
	accountID uint32,
	databaseID uint64,
) string {
	return fmt.Sprintf(
		`delete from mo_catalog.mo_lifecycle_bindings
where account_id=%d and database_id=%d`,
		accountID,
		databaseID,
	)
}

func (c *Compile) detachLifecycleBindingsForDatabaseDrop(
	accountID uint32,
	databaseID uint64,
) error {
	if !c.proc.Base.IsFrontend {
		return nil
	}
	return ignoreMissingLifecycleCatalog(c.runSqlWithAccountId(
		lifecycleDatabaseDropBindingDeleteSQL(accountID, databaseID),
		int32(accountID),
	))
}

func (c *Compile) lockLifecycleDependencyPublication() error {
	return c.runSqlWithAccountId(
		"update mo_catalog.mo_feature_registry set scope_spec = scope_spec, updated_at = updated_at where feature_code = 'LIFECYCLE'",
		int32(0),
	)
}

func lifecyclePitrBindingProbeSQL(
	level tree.PitrLevel,
	objectID uint64,
) string {
	sql := `select binding_id from mo_catalog.mo_lifecycle_bindings
where state in ('ACTIVE','PAUSED','BLOCKED')`
	switch level {
	case tree.PITRLEVELDATABASE:
		sql += fmt.Sprintf(" and database_id=%d", objectID)
	case tree.PITRLEVELTABLE:
		sql += fmt.Sprintf(" and physical_table_id=%d", objectID)
	}
	return sql + " limit 1"
}

func (c *Compile) rejectLifecyclePitrBindings(
	createPitr *plan2.CreatePitr,
	objectID uint64,
) error {
	if createPitr == nil {
		return moerr.NewInternalError(
			c.proc.Ctx,
			"Lifecycle PITR fence input is incomplete",
		)
	}
	level := tree.PitrLevel(createPitr.GetLevel())
	accountIDs := []uint32{createPitr.GetAccountId()}
	if level == tree.PITRLEVELCLUSTER {
		result, err := c.runSqlWithResultAndOptions(
			"select account_id from mo_catalog.mo_account",
			0,
			executor.StatementOption{}.WithDisableLog(),
		)
		if err != nil {
			return err
		}
		defer result.Close()
		accountIDs = accountIDs[:0]
		result.ReadRows(func(rows int, vectors []*vector.Vector) bool {
			if rows == 0 || len(vectors) == 0 {
				return false
			}
			values := vector.MustFixedColWithTypeCheck[uint32](vectors[0])
			accountIDs = append(accountIDs, values[:rows]...)
			return true
		})
	}
	for _, accountID := range accountIDs {
		result, err := c.runSqlWithResultAndOptions(
			lifecyclePitrBindingProbeSQL(level, objectID),
			int32(accountID),
			executor.StatementOption{}.WithDisableLog(),
		)
		if err != nil {
			if ignoreErr := ignoreMissingLifecycleCatalog(err); ignoreErr != nil {
				return ignoreErr
			}
			continue
		}
		bound := false
		result.ReadRows(func(rows int, _ []*vector.Vector) bool {
			bound = rows > 0
			return false
		})
		result.Close()
		if bound {
			return moerr.NewNotSupported(
				c.proc.Ctx,
				"CREATE PITR while the target scope contains a Lifecycle-bound table",
			)
		}
	}
	return nil
}
