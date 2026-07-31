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

package frontend

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

var lifecycleBindingShowColumns = []Column{
	lifecycleShowColumn("action", defines.MYSQL_TYPE_VARCHAR),
	lifecycleShowColumn("state", defines.MYSQL_TYPE_VARCHAR),
	lifecycleShowColumn("expire_after_days", defines.MYSQL_TYPE_LONGLONG),
	lifecycleShowColumn("stage_id", defines.MYSQL_TYPE_LONGLONG),
	lifecycleShowColumn("purge_after_days", defines.MYSQL_TYPE_LONGLONG),
	lifecycleShowColumn("binding_generation", defines.MYSQL_TYPE_LONGLONG),
	lifecycleShowColumn("updated_at", defines.MYSQL_TYPE_VARCHAR),
}

var lifecycleDatasetShowColumns = []Column{
	lifecycleShowColumn("dataset_id", defines.MYSQL_TYPE_VARCHAR),
	lifecycleShowColumn("state", defines.MYSQL_TYPE_VARCHAR),
	lifecycleShowColumn("row_count", defines.MYSQL_TYPE_LONGLONG),
	lifecycleShowColumn("logical_bytes", defines.MYSQL_TYPE_LONGLONG),
	lifecycleShowColumn("purge_eligible_at", defines.MYSQL_TYPE_VARCHAR),
	lifecycleShowColumn("manifest_key", defines.MYSQL_TYPE_VARCHAR),
}

var lifecycleJobShowColumns = []Column{
	lifecycleShowColumn("root_id", defines.MYSQL_TYPE_VARCHAR),
	lifecycleShowColumn("mode", defines.MYSQL_TYPE_VARCHAR),
	lifecycleShowColumn("state", defines.MYSQL_TYPE_VARCHAR),
	lifecycleShowColumn("cleanup_after", defines.MYSQL_TYPE_VARCHAR),
	lifecycleShowColumn("last_error", defines.MYSQL_TYPE_VARCHAR),
}

func lifecycleShowColumn(name string, columnType defines.MysqlType) Column {
	return &MysqlColumn{ColumnImpl: ColumnImpl{name: name, columnType: columnType}}
}

func handleShowLifecycle(ctx context.Context, ses *Session, statement *tree.ShowLifecycle) error {
	accountID := ses.GetTenantInfo().GetTenantID()
	background := ses.GetBackgroundExec(ctx)
	defer background.Close()

	var (
		query   string
		columns []Column
		system  bool
	)
	switch statement.Kind {
	case tree.ShowLifecycleBinding:
		tableDef, err := resolveLifecycleShowTable(ctx, ses, statement.Table)
		if err != nil {
			return err
		}
		query = fmt.Sprintf(
			`select action,state,expire_after_days,stage_id,purge_after_days,binding_generation,cast(updated_at as varchar)
from mo_catalog.mo_lifecycle_bindings where account_id = %d and physical_table_id = %d`,
			accountID,
			tableDef.TblId,
		)
		columns = lifecycleBindingShowColumns

	case tree.ShowLifecycleDatasets:
		tableDef, err := resolveLifecycleShowTable(ctx, ses, statement.Table)
		if err != nil {
			return err
		}
		logicalTableID := tableDef.LogicalId
		if logicalTableID == 0 {
			logicalTableID = tableDef.TblId
		}
		query = fmt.Sprintf(
			`select hex(dataset_id),state,row_count,logical_bytes,cast(purge_eligible_at as varchar),manifest_key
from mo_catalog.mo_lifecycle_datasets where account_id = %d and logical_table_id = %d order by created_at desc limit 1000`,
			accountID,
			logicalTableID,
		)
		columns = lifecycleDatasetShowColumns

	case tree.ShowLifecycleJobs:
		query = fmt.Sprintf(
			`select hex(root_id),mode,state,cast(cleanup_after as varchar),last_error
from mo_catalog.mo_lifecycle_cleanup_roots where owner_account_id = %d order by updated_at desc limit 1000`,
			accountID,
		)
		columns = lifecycleJobShowColumns
		system = true

	default:
		return moerr.NewInvalidInput(ctx, "unknown SHOW LIFECYCLE kind")
	}

	queryCtx := ctx
	if system {
		queryCtx = defines.AttachAccountId(ctx, catalog.System_Account)
	}
	background.ClearExecResultSet()
	if err := background.Exec(queryCtx, query); err != nil {
		return err
	}
	results, err := getResultSet(queryCtx, background)
	if err != nil {
		return err
	}

	resultSet := ses.GetMysqlResultSet()
	for _, column := range columns {
		resultSet.AddColumn(column)
	}
	if len(results) == 0 {
		return trySaveQueryResult(ctx, ses, resultSet)
	}
	for row := uint64(0); row < results[0].GetRowCount(); row++ {
		values := make([]any, len(columns))
		for column := range columns {
			if isNull, nullErr := results[0].ColumnIsNull(queryCtx, row, uint64(column)); nullErr != nil {
				return nullErr
			} else if isNull {
				values[column] = nil
				continue
			}
			switch columns[column].ColumnType() {
			case defines.MYSQL_TYPE_LONGLONG:
				values[column], err = results[0].GetUint64(queryCtx, row, uint64(column))
			default:
				values[column], err = results[0].GetString(queryCtx, row, uint64(column))
			}
			if err != nil {
				return err
			}
		}
		resultSet.AddRow(values)
	}
	return trySaveQueryResult(ctx, ses, resultSet)
}

func resolveLifecycleShowTable(
	ctx context.Context,
	ses *Session,
	table *tree.TableName,
) (*plan.TableDef, error) {
	if table == nil {
		return nil, moerr.NewInvalidInput(ctx, "SHOW LIFECYCLE requires a table")
	}
	databaseName := string(table.Schema())
	if databaseName == "" {
		databaseName = ses.GetDatabaseName()
	}
	if databaseName == "" {
		return nil, moerr.NewNoDB(ctx)
	}
	tableName := string(table.Name())
	_, tableDef, err := ses.GetTxnCompileCtx().Resolve(databaseName, tableName, nil)
	if err != nil {
		return nil, err
	}
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx, databaseName, tableName)
	}
	return tableDef, nil
}
