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

package frontend

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const externalCatalogStatusActive = "active"

var (
	externalCatalogAllowedOptions = map[string]struct{}{
		"include_databases":        {},
		"exclude_databases":        {},
		"include_schemas":          {},
		"exclude_schemas":          {},
		"metadata_cache_ttl":       {},
		"lower_case_meta_names":    {},
		"quoted_identifier":        {},
		"connection_pool_min_size": {},
		"connection_pool_max_size": {},
		"test_connection":          {},
		"default_search_path":      {},
		"default_timezone":         {},
	}

	showCreateCatalogCols = []Column{
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "Catalog",
				columnType: defines.MYSQL_TYPE_VARCHAR,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "Create Catalog",
				columnType: defines.MYSQL_TYPE_VARCHAR,
			},
		},
	}
)

type externalCatalogMetadata struct {
	name           string
	typ            string
	connectionName string
	optionsJSON    string
	cacheTTL       string
}

func doCreateExternalCatalog(ctx context.Context, ses *Session, cs *tree.CreateExternalCatalog) (err error) {
	if err = doCheckRole(ctx, ses); err != nil {
		return err
	}
	if err = inputNameIsInvalid(ctx, string(cs.Name)); err != nil {
		return err
	}
	if err = inputNameIsInvalid(ctx, string(cs.ConnectionName)); err != nil {
		return err
	}

	catalogType, err := normalizeConnectionType(ctx, cs.Type)
	if err != nil {
		return err
	}
	options, err := normalizeExternalCatalogOptions(ctx, cs.Options)
	if err != nil {
		return err
	}
	cacheTTL, err := validateExternalCatalogOptions(ctx, catalogType, options)
	if err != nil {
		return err
	}

	tenantInfo := ses.GetTenantInfo()
	if tenantInfo == nil {
		return moerr.NewInternalError(ctx, "missing tenant info")
	}

	optionsJSON, err := json.Marshal(options)
	if err != nil {
		return err
	}

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	if err = bh.Exec(ctx, "begin;"); err != nil {
		return err
	}
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()

	exists, err := checkExternalCatalogExistOrNot(ctx, bh, string(cs.Name))
	if err != nil {
		return err
	}
	if exists {
		if cs.IfNotExists {
			return nil
		}
		return moerr.NewInternalErrorf(ctx, "the external catalog %s exists", cs.Name)
	}

	connectionMeta, err := loadConnectionMetadata(ctx, bh, string(cs.ConnectionName), true)
	if err != nil {
		return err
	}
	if connectionMeta == nil {
		return moerr.NewInternalErrorf(ctx, "the connection %s does not exist", cs.ConnectionName)
	}
	if connectionMeta.typ != catalogType {
		return moerr.NewBadConfig(ctx, fmt.Sprintf(
			"external catalog type %q does not match connection type %q",
			catalogType,
			connectionMeta.typ,
		))
	}

	sql, err := getSqlForInsertIntoMoExternalCatalogs(
		ctx,
		string(cs.Name),
		catalogType,
		connectionMeta.id,
		string(optionsJSON),
		cacheTTL,
		externalCatalogStatusActive,
		uint64(tenantInfo.GetDefaultRoleID()),
		uint64(tenantInfo.GetUserID()),
		uint64(tenantInfo.GetTenantID()),
		types.CurrentTimestamp().String2(time.UTC, 0),
		"",
	)
	if err != nil {
		return err
	}
	return bh.Exec(ctx, sql)
}

func doDropExternalCatalog(ctx context.Context, ses *Session, ds *tree.DropExternalCatalog) (err error) {
	if err = doCheckRole(ctx, ses); err != nil {
		return err
	}
	if err = inputNameIsInvalid(ctx, string(ds.Name)); err != nil {
		return err
	}

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	if err = bh.Exec(ctx, "begin;"); err != nil {
		return err
	}
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()

	exists, err := checkExternalCatalogExistOrNot(ctx, bh, string(ds.Name))
	if err != nil {
		return err
	}
	if !exists {
		if ds.IfExists {
			return nil
		}
		return moerr.NewInternalErrorf(ctx, "the external catalog %s does not exist", ds.Name)
	}

	return bh.Exec(ctx, getSqlForDropExternalCatalog(string(ds.Name)))
}

func doShowCreateCatalog(ctx context.Context, ses *Session, sc *tree.ShowCreateCatalog) (err error) {
	if err = doCheckRole(ctx, ses); err != nil {
		return err
	}
	if err = inputNameIsInvalid(ctx, sc.Name); err != nil {
		return err
	}

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	meta, err := loadExternalCatalogMetadata(ctx, bh, sc.Name)
	if err != nil {
		return err
	}
	if meta == nil {
		return moerr.NewInternalErrorf(ctx, "the external catalog %s does not exist", sc.Name)
	}

	options, err := decodeExternalCatalogOptions(meta.optionsJSON)
	if err != nil {
		return err
	}
	if _, ok := options["metadata_cache_ttl"]; !ok && strings.TrimSpace(meta.cacheTTL) != "" {
		options["metadata_cache_ttl"] = meta.cacheTTL
	}

	mrs := ses.GetMysqlResultSet()
	for _, col := range showCreateCatalogCols {
		mrs.AddColumn(col)
	}
	mrs.AddRow([]interface{}{
		meta.name,
		buildShowCreateCatalogSQL(meta.name, meta.connectionName, meta.typ, options),
	})
	return trySaveQueryResult(ctx, ses, mrs)
}

func normalizeExternalCatalogOptions(ctx context.Context, raw []*tree.ExternalCatalogOption) (map[string]string, error) {
	options := make(map[string]string, len(raw))
	for _, opt := range raw {
		if opt == nil {
			return nil, moerr.NewBadConfig(ctx, "external catalog option is nil")
		}
		key := strings.ToLower(strings.TrimSpace(string(opt.Key)))
		if key == "" {
			return nil, moerr.NewBadConfig(ctx, "external catalog option name is empty")
		}
		if _, ok := externalCatalogAllowedOptions[key]; !ok {
			return nil, moerr.NewBadConfig(ctx, fmt.Sprintf("unsupported external catalog option %q", key))
		}
		if _, exists := options[key]; exists {
			return nil, moerr.NewBadConfig(ctx, fmt.Sprintf("duplicate external catalog option %q", key))
		}
		options[key] = opt.Value
	}
	return options, nil
}

func validateExternalCatalogOptions(ctx context.Context, typ string, options map[string]string) (string, error) {
	cacheTTL := strings.TrimSpace(options["metadata_cache_ttl"])
	if cacheTTL == "" {
		return "", moerr.NewBadConfig(ctx, `external catalog option "metadata_cache_ttl" is required`)
	}

	switch typ {
	case "mysql":
		if !hasExternalCatalogOption(options, "include_databases", "exclude_databases") {
			return "", moerr.NewBadConfig(ctx, `mysql external catalog requires "include_databases" or "exclude_databases"`)
		}
		if hasExternalCatalogOption(options, "include_schemas", "exclude_schemas", "quoted_identifier", "default_search_path", "default_timezone") {
			return "", moerr.NewBadConfig(ctx, "mysql external catalog does not support oracle/postgresql-specific options")
		}
	case "oracle":
		if !hasExternalCatalogOption(options, "include_schemas", "exclude_schemas") {
			return "", moerr.NewBadConfig(ctx, `oracle external catalog requires "include_schemas" or "exclude_schemas"`)
		}
		if hasExternalCatalogOption(options, "include_databases", "exclude_databases", "lower_case_meta_names", "default_search_path", "default_timezone") {
			return "", moerr.NewBadConfig(ctx, "oracle external catalog does not support mysql/postgresql-specific options")
		}
	case "postgresql":
		if !hasExternalCatalogOption(options, "include_schemas", "exclude_schemas") {
			return "", moerr.NewBadConfig(ctx, `postgresql external catalog requires "include_schemas" or "exclude_schemas"`)
		}
		if hasExternalCatalogOption(options, "include_databases", "exclude_databases", "lower_case_meta_names", "quoted_identifier") {
			return "", moerr.NewBadConfig(ctx, "postgresql external catalog does not support mysql/oracle-specific options")
		}
	}

	if value := strings.TrimSpace(options["connection_pool_min_size"]); value != "" {
		minSize, err := strconv.Atoi(value)
		if err != nil || minSize < 0 {
			return "", moerr.NewBadConfig(ctx, `external catalog option "connection_pool_min_size" must be a non-negative integer`)
		}
		if maxValue := strings.TrimSpace(options["connection_pool_max_size"]); maxValue != "" {
			maxSize, err := strconv.Atoi(maxValue)
			if err != nil || maxSize <= 0 {
				return "", moerr.NewBadConfig(ctx, `external catalog option "connection_pool_max_size" must be a positive integer`)
			}
			if minSize > maxSize {
				return "", moerr.NewBadConfig(ctx, `external catalog option "connection_pool_min_size" cannot exceed "connection_pool_max_size"`)
			}
		}
	} else if value := strings.TrimSpace(options["connection_pool_max_size"]); value != "" {
		maxSize, err := strconv.Atoi(value)
		if err != nil || maxSize <= 0 {
			return "", moerr.NewBadConfig(ctx, `external catalog option "connection_pool_max_size" must be a positive integer`)
		}
	}

	if value := strings.TrimSpace(options["test_connection"]); value != "" &&
		!strings.EqualFold(value, "true") && !strings.EqualFold(value, "false") {
		return "", moerr.NewBadConfig(ctx, `external catalog option "test_connection" must be "true" or "false"`)
	}

	if value := strings.TrimSpace(options["lower_case_meta_names"]); value != "" &&
		!strings.EqualFold(value, "true") && !strings.EqualFold(value, "false") {
		return "", moerr.NewBadConfig(ctx, `external catalog option "lower_case_meta_names" must be "true" or "false"`)
	}

	if value := strings.TrimSpace(options["quoted_identifier"]); value != "" &&
		!strings.EqualFold(value, "auto") &&
		!strings.EqualFold(value, "always") &&
		!strings.EqualFold(value, "never") {
		return "", moerr.NewBadConfig(ctx, `external catalog option "quoted_identifier" must be one of "auto", "always", or "never"`)
	}

	return cacheTTL, nil
}

func hasExternalCatalogOption(options map[string]string, keys ...string) bool {
	for _, key := range keys {
		if strings.TrimSpace(options[key]) != "" {
			return true
		}
	}
	return false
}

func checkExternalCatalogExistOrNot(ctx context.Context, bh BackgroundExec, catalogName string) (bool, error) {
	bh.ClearExecResultSet()
	if err := bh.Exec(ctx, getSqlForCheckExternalCatalog(catalogName)); err != nil {
		return false, err
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return false, err
	}
	return execResultArrayHasData(erArray), nil
}

func loadExternalCatalogMetadata(ctx context.Context, bh BackgroundExec, catalogName string) (*externalCatalogMetadata, error) {
	bh.ClearExecResultSet()
	if err := bh.Exec(ctx, getSqlForGetExternalCatalog(catalogName)); err != nil {
		return nil, err
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}
	if !execResultArrayHasData(erArray) {
		return nil, nil
	}

	typ, err := erArray[0].GetString(ctx, 0, 0)
	if err != nil {
		return nil, err
	}
	connectionName, err := erArray[0].GetString(ctx, 0, 1)
	if err != nil {
		return nil, err
	}
	optionsJSON, err := erArray[0].GetString(ctx, 0, 2)
	if err != nil {
		return nil, err
	}
	cacheTTL, err := erArray[0].GetString(ctx, 0, 3)
	if err != nil {
		return nil, err
	}

	return &externalCatalogMetadata{
		name:           catalogName,
		typ:            typ,
		connectionName: connectionName,
		optionsJSON:    optionsJSON,
		cacheTTL:       cacheTTL,
	}, nil
}

func decodeExternalCatalogOptions(raw string) (map[string]string, error) {
	if raw == "" {
		return map[string]string{}, nil
	}

	options := make(map[string]string)
	if err := json.Unmarshal([]byte(raw), &options); err != nil {
		return nil, err
	}
	return options, nil
}

func buildShowCreateCatalogSQL(name, connectionName, typ string, options map[string]string) string {
	keys := make([]string, 0, len(options))
	for key := range options {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	formattedOptions := make([]*tree.ExternalCatalogOption, 0, len(keys))
	for _, key := range keys {
		formattedOptions = append(formattedOptions, &tree.ExternalCatalogOption{
			Key:   tree.Identifier(key),
			Value: options[key],
		})
	}

	stmt := &tree.CreateExternalCatalog{
		Name:           tree.Identifier(name),
		ConnectionName: tree.Identifier(connectionName),
		Type:           typ,
		Options:        formattedOptions,
	}
	return tree.String(stmt, dialect.MYSQL)
}

func getSqlForCheckExternalCatalog(catalogName string) string {
	return fmt.Sprintf(
		"select catalog_id from %s.%s where catalog_name = '%s' order by catalog_id;",
		catalog.MO_CATALOG,
		catalog.MO_EXTERNAL_CATALOGS,
		escapeConnectionSQLLiteral(catalogName),
	)
}

func getSqlForGetExternalCatalog(catalogName string) string {
	return fmt.Sprintf(
		"select ec.catalog_type, c.connection_name, ec.catalog_options, ec.metadata_cache_ttl from %s.%s ec join %s.%s c on ec.connection_id = c.connection_id where ec.catalog_name = '%s' order by ec.catalog_id;",
		catalog.MO_CATALOG,
		catalog.MO_EXTERNAL_CATALOGS,
		catalog.MO_CATALOG,
		catalog.MO_CONNECTIONS,
		escapeConnectionSQLLiteral(catalogName),
	)
}

func getSqlForInsertIntoMoExternalCatalogs(
	ctx context.Context,
	catalogName string,
	catalogType string,
	connectionID uint64,
	catalogOptions string,
	metadataCacheTTL string,
	catalogStatus string,
	owner uint64,
	creator uint64,
	accountID uint64,
	createdTime string,
	comment string,
) (string, error) {
	if err := inputNameIsInvalid(ctx, catalogName); err != nil {
		return "", err
	}

	return fmt.Sprintf(
		"insert into %s.%s(catalog_name, catalog_type, connection_id, catalog_options, metadata_cache_ttl, catalog_status, owner, creator, account_id, created_time, comment) values ('%s', '%s', %d, '%s', '%s', '%s', %d, %d, %d, '%s', '%s');",
		catalog.MO_CATALOG,
		catalog.MO_EXTERNAL_CATALOGS,
		escapeConnectionSQLLiteral(catalogName),
		escapeConnectionSQLLiteral(catalogType),
		connectionID,
		escapeConnectionSQLLiteral(catalogOptions),
		escapeConnectionSQLLiteral(metadataCacheTTL),
		escapeConnectionSQLLiteral(catalogStatus),
		owner,
		creator,
		accountID,
		escapeConnectionSQLLiteral(createdTime),
		escapeConnectionSQLLiteral(comment),
	), nil
}

func getSqlForDropExternalCatalog(catalogName string) string {
	return fmt.Sprintf(
		"delete from %s.%s where catalog_name = '%s' order by catalog_id;",
		catalog.MO_CATALOG,
		catalog.MO_EXTERNAL_CATALOGS,
		escapeConnectionSQLLiteral(catalogName),
	)
}
