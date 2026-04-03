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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	mysqlparser "github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestNormalizeExternalCatalogOptionsRejectsDuplicates(t *testing.T) {
	_, err := normalizeExternalCatalogOptions(context.Background(), []*tree.ExternalCatalogOption{
		{Key: tree.Identifier("include_schemas"), Value: "public"},
		{Key: tree.Identifier("INCLUDE_SCHEMAS"), Value: "sales"},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, `duplicate external catalog option "include_schemas"`)
}

func TestValidateExternalCatalogOptionsPostgreSQLNeedsSchemas(t *testing.T) {
	_, err := validateExternalCatalogOptions(context.Background(), "postgresql", map[string]string{
		"metadata_cache_ttl": "300s",
	})
	require.Error(t, err)
	require.ErrorContains(t, err, `postgresql external catalog requires "include_schemas" or "exclude_schemas"`)
}

func TestValidateExternalCatalogOptionsRejectsCrossTypeOptions(t *testing.T) {
	_, err := validateExternalCatalogOptions(context.Background(), "mysql", map[string]string{
		"include_databases":  "sales",
		"metadata_cache_ttl": "300s",
		"quoted_identifier":  "auto",
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "mysql external catalog does not support oracle/postgresql-specific options")
}

func TestValidateExternalCatalogOptionsRejectsWrongFilterDimension(t *testing.T) {
	_, err := validateExternalCatalogOptions(context.Background(), "postgresql", map[string]string{
		"include_schemas":     "public",
		"include_databases":   "sales",
		"metadata_cache_ttl":  "300s",
		"default_search_path": "public",
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "postgresql external catalog does not support mysql/oracle-specific options")
}

func TestBuildShowCreateCatalogSQLRoundTripsEscapedOptions(t *testing.T) {
	sql := buildShowCreateCatalogSQL("pg_sales", "conn_pg_sales", "postgresql", map[string]string{
		"default_search_path": "public,bi",
		"include_schemas":     "public,bi",
		"metadata_cache_ttl":  "300s",
		"default_timezone":    "UTC",
	})

	stmt, err := mysqlparser.ParseOne(context.TODO(), sql, 1)
	require.NoError(t, err)

	createStmt, ok := stmt.(*tree.CreateExternalCatalog)
	require.True(t, ok)

	got := make(map[string]string, len(createStmt.Options))
	for _, opt := range createStmt.Options {
		got[string(opt.Key)] = opt.Value
	}

	require.Equal(t, map[string]string{
		"default_search_path": "public,bi",
		"include_schemas":     "public,bi",
		"metadata_cache_ttl":  "300s",
		"default_timezone":    "UTC",
	}, got)
}

func TestGetSqlForInsertIntoMoExternalCatalogsEscapesSpecialCharacters(t *testing.T) {
	rawJSON := `{"path":"C:\\tmp\\fq","note":"a'b\n"}`
	sql, err := getSqlForInsertIntoMoExternalCatalogs(
		context.Background(),
		"pg_sales",
		"postgresql",
		7,
		rawJSON,
		"300s",
		externalCatalogStatusActive,
		1,
		2,
		3,
		"2026-04-01 00:00:00",
		"",
	)
	require.NoError(t, err)
	require.Contains(t, sql, `C:\\\\tmp\\\\fq`)
	require.Contains(t, sql, `a''b\\n`)
	require.False(t, strings.Contains(sql, `C:\tmp\fq`))
}

func TestLoadExternalCatalogMetadata(t *testing.T) {
	bh := &backgroundExecTest{}
	bh.init()

	result := &MysqlResultSet{}
	col1 := &MysqlColumn{}
	col1.SetName("catalog_type")
	col1.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col2 := &MysqlColumn{}
	col2.SetName("connection_name")
	col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col3 := &MysqlColumn{}
	col3.SetName("catalog_options")
	col3.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col4 := &MysqlColumn{}
	col4.SetName("metadata_cache_ttl")
	col4.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	result.AddColumn(col1)
	result.AddColumn(col2)
	result.AddColumn(col3)
	result.AddColumn(col4)
	result.AddRow([]interface{}{"postgresql", "conn_pg_sales", `{"include_schemas":"public"}`, "300s"})

	sql := getSqlForGetExternalCatalog("pg_sales")
	bh.sql2result[sql] = result

	meta, err := loadExternalCatalogMetadata(context.Background(), bh, "pg_sales")
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.Equal(t, "postgresql", meta.typ)
	require.Equal(t, "conn_pg_sales", meta.connectionName)
	require.Equal(t, `{"include_schemas":"public"}`, meta.optionsJSON)
	require.Equal(t, "300s", meta.cacheTTL)
}
