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

package test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	lifecyclepkg "github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/lifecycle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/stretchr/testify/require"
)

func TestLifecycleRestoreNoPKTableThroughCNToTN(t *testing.T) {
	const (
		accountID         = uint32(1)
		databaseName      = "lifecycle_restore_fakepk"
		hiddenName        = catalog.LifecycleRestoreTableNamePrefix + "11111111111111111111111111111111"
		targetName        = "restored_events"
		restoreID         = "11111111-1111-1111-1111-111111111111"
		datasetID         = "22222222-2222-2222-2222-222222222222"
		leaseID           = "33333333-3333-3333-3333-333333333333"
		rootID            = "44444444-4444-4444-4444-444444444444"
		archiveAttemptID  = "55555555-5555-5555-5555-555555555555"
		bindingID         = "66666666-6666-6666-6666-666666666666"
		datasetLogicalCap = uint64(1 << 20)
	)

	baseCtx := context.WithValue(context.Background(), defines.TenantIDKey{}, accountID)
	disttaeEngine, taeEngine, rpcAgent, mp := testutil.CreateEngines(
		baseCtx,
		testutil.TestOptions{},
		t,
	)
	defer func() {
		disttaeEngine.Close(baseCtx)
		require.NoError(t, taeEngine.Close(true))
		rpcAgent.Close()
	}()

	ctx, cancel := context.WithTimeout(baseCtx, 2*time.Minute)
	defer cancel()
	systemCtx := context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)

	autoIncrement := incrservice.NewIncrService(
		"",
		incrservice.NewMemStore(),
		incrservice.Config{CountPerAllocate: 32},
	)
	previousAutoIncrement := incrservice.GetAutoIncrementService("")
	if previousAutoIncrement == nil {
		previousAutoIncrement = NewMockAutoIncrementService("restore-test-fallback")
	}
	incrservice.SetAutoIncrementServiceByID("", autoIncrement)
	defer func() {
		incrservice.SetAutoIncrementServiceByID("", previousAutoIncrement)
		autoIncrement.Close()
	}()

	value, ok := runtime.ServiceRuntime("").GetGlobalVariables(runtime.InternalSQLExecutor)
	require.True(t, ok)
	sqlExecutor, ok := value.(executor.SQLExecutor)
	require.True(t, ok)

	// The test engine bootstraps only the core catalog. Keep this fixture local
	// and create exactly the system and tenant tables used by Restore.
	mustExecLifecycleRestoreSQL(
		t,
		ctx,
		sqlExecutor,
		accountID,
		frontend.MoCatalogMoIndexesDDL,
	)
	mustExecLifecycleRestoreSQL(
		t,
		ctx,
		sqlExecutor,
		accountID,
		frontend.MoCatalogMoForeignKeysDDL,
	)
	mustExecLifecycleRestoreSQL(
		t,
		systemCtx,
		sqlExecutor,
		catalog.System_Account,
		frontend.MoCatalogMoIndexesDDL,
	)
	mustExecLifecycleRestoreSQL(
		t,
		systemCtx,
		sqlExecutor,
		catalog.System_Account,
		frontend.MoCatalogMoAccountDDL,
	)
	mustExecLifecycleRestoreSQL(
		t,
		systemCtx,
		sqlExecutor,
		catalog.System_Account,
		frontend.MoCatalogMoISCPLogDDL,
	)
	mustExecLifecycleRestoreSQL(
		t,
		systemCtx,
		sqlExecutor,
		catalog.System_Account,
		frontend.MoCatalogMoIndexUpdateDDL,
	)
	mustExecLifecycleRestoreSQL(
		t,
		systemCtx,
		sqlExecutor,
		catalog.System_Account,
		frontend.MoCatalogFeatureRegistryDDL,
	)
	mustExecLifecycleRestoreSQL(
		t,
		systemCtx,
		sqlExecutor,
		catalog.System_Account,
		catalog.MoLifecycleCleanupRootsDDL,
	)
	mustExecLifecycleRestoreSQL(
		t,
		systemCtx,
		sqlExecutor,
		catalog.System_Account,
		`insert into mo_catalog.mo_feature_registry(
feature_code,description,scope_spec,enabled)
values('LIFECYCLE','test','{"allowed_scope":[]}',true)`,
	)

	for _, ddl := range []string{
		frontend.MoCatalogMoTablePartitionsDDL,
		frontend.MoCatalogMoAutoIncrTableDDL,
		catalog.MoLifecycleDatasetsDDL,
		catalog.MoLifecycleRestoreAttemptsDDL,
		catalog.MoLifecycleRestoreChunksDDL,
	} {
		mustExecLifecycleRestoreSQL(t, ctx, sqlExecutor, accountID, ddl)
	}
	mustExecLifecycleRestoreSQL(
		t,
		ctx,
		sqlExecutor,
		accountID,
		"create database "+databaseName,
	)
	databaseID := queryLifecycleRestoreUint64(
		t,
		ctx,
		sqlExecutor,
		accountID,
		fmt.Sprintf(
			"select dat_id from mo_catalog.mo_database where datname='%s'",
			databaseName,
		),
	)

	schema := lifecyclepkg.SchemaDescriptor{
		FormatVersion:      1,
		SourceTableID:      901,
		SourceTableVersion: 1,
		SourceDatabaseName: "source_db",
		SourceTableName:    "events",
		Columns: []lifecyclepkg.SchemaColumn{{
			Ordinal:        0,
			SourceColumnID: 1,
			Name:           "payload",
			TypeID:         int32(types.T_int64),
			NotNull:        true,
		}},
	}
	schemaDigest, err := schema.Digest()
	require.NoError(t, err)
	verifiedHash := sha256.Sum256([]byte("Lifecycle Restore fake-PK integration"))
	zeroDigest := [sha256.Size]byte{}
	mustExecLifecycleRestoreSQL(
		t,
		ctx,
		sqlExecutor,
		accountID,
		fmt.Sprintf(
			`insert into mo_catalog.mo_lifecycle_datasets(
dataset_id,account_id,binding_id,binding_generation,logical_table_id,
source_physical_table_id,source_snapshot_ts,evaluation_time,cutoff,
source_set_digest,schema_descriptor_digest,lifecycle_min,lifecycle_max,
root_id,attempt_id,manifest_key,manifest_sha256,content_hash,row_count,
logical_bytes,stage_id,stage_identity_blob,purge_eligible_at,state,version,
access_generation,restore_lease_id,restore_deadline,publish_txn_id,
created_at,updated_at)
values(unhex('%s'),%d,unhex('%s'),1,900,901,x'01',utc_timestamp(),
utc_timestamp(),unhex('%s'),unhex('%s'),null,null,unhex('%s'),unhex('%s'),
'manifest.json',unhex('%s'),unhex('%s'),4,%d,7,x'01',
date_add(utc_timestamp(),interval 1 day),'PUBLISHED',1,1,null,null,x'01',
utc_timestamp(),utc_timestamp())`,
			lifecycleRestoreTestUUIDHex(datasetID),
			accountID,
			lifecycleRestoreTestUUIDHex(bindingID),
			hex.EncodeToString(zeroDigest[:]),
			hex.EncodeToString(schemaDigest[:]),
			lifecycleRestoreTestUUIDHex(rootID),
			lifecycleRestoreTestUUIDHex(archiveAttemptID),
			hex.EncodeToString(zeroDigest[:]),
			hex.EncodeToString(verifiedHash[:]),
			datasetLogicalCap,
		),
	)

	hiddenCreateSQL, err := schema.BuildRestoreCreateTableSQL(
		ctx,
		databaseName,
		hiddenName,
	)
	require.NoError(t, err)
	repository := disttae.SQLRestoreRepository{
		AccountID:                        accountID,
		TargetDatabaseName:               databaseName,
		Executor:                         sqlExecutor,
		Engine:                           disttaeEngine.Engine,
		MPool:                            mp,
		AutoIncrement:                    autoIncrement,
		MaxRestoreStagingBytesPerAccount: 2 * datasetLogicalCap,
		MaxRestoreStagingBytesPerCluster: 4 * datasetLogicalCap,
	}
	attempt, err := repository.Initialize(
		ctx,
		lifecyclepkg.RestoreInitializeRequest{
			Dataset: lifecyclepkg.RestoreDataset{
				DatasetID:    datasetID,
				AccountID:    accountID,
				ContentHash:  verifiedHash,
				RowCount:     4,
				LogicalBytes: datasetLogicalCap,
				Version:      1,
				State:        "PUBLISHED",
			},
			Attempt: lifecyclepkg.RestoreAttempt{
				RestoreID:          restoreID,
				DatasetID:          datasetID,
				LeaseID:            leaseID,
				Deadline:           time.Now().UTC().Add(time.Minute).Truncate(time.Microsecond),
				StagingDatabaseID:  databaseID,
				HiddenName:         hiddenName,
				TargetDatabaseID:   databaseID,
				TargetDatabaseName: databaseName,
				TargetName:         targetName,
			},
			HiddenCreateSQL: hiddenCreateSQL,
		},
	)
	require.NoError(t, err)
	require.Equal(t, "IMPORTING", attempt.State)
	require.NotZero(t, attempt.StagingTableID)

	readTxn, err := disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Now())
	require.NoError(t, err)
	require.NoError(t, disttaeEngine.Engine.New(ctx, readTxn))
	_, _, stagingRelation, err := disttaeEngine.Engine.GetRelationById(
		ctx,
		readTxn,
		attempt.StagingTableID,
	)
	require.NoError(t, err)
	tableDef := stagingRelation.GetTableDef(ctx)
	require.NotNil(t, tableDef.Pkey)
	require.Equal(t, catalog.FakePrimaryKeyColName, tableDef.Pkey.PkeyColName)
	require.Len(t, tableDef.Cols, len(schema.Columns)+2)
	fakePKIndex, rowIDIndex := -1, -1
	for index, column := range tableDef.Cols {
		switch column.Name {
		case catalog.FakePrimaryKeyColName:
			fakePKIndex = index
		case catalog.Row_ID:
			rowIDIndex = index
		}
	}
	require.NotEqual(t, -1, fakePKIndex)
	require.NotEqual(t, -1, rowIDIndex)
	require.True(t, tableDef.Cols[fakePKIndex].Hidden)
	require.True(t, tableDef.Cols[fakePKIndex].Typ.AutoIncr)
	require.Equal(t, int32(types.T_uint64), tableDef.Cols[fakePKIndex].Typ.Id)
	require.True(t, tableDef.Cols[rowIDIndex].Hidden)
	require.Equal(t, int32(types.T_Rowid), tableDef.Cols[rowIDIndex].Typ.Id)
	require.NoError(t, readTxn.Commit(ctx))

	failedRows, failedReceipt := lifecycleRestoreTestChunk(
		t,
		ctx,
		schema,
		restoreID,
		0,
		[]int64{11, 22},
	)
	failedRepository := repository
	failedRepository.Executor = &failLifecycleRestoreChunkReceiptExecutor{
		delegate: sqlExecutor,
	}
	_, err = failedRepository.ImportChunk(
		ctx,
		attempt,
		failedReceipt,
		schema,
		failedRows,
	)
	require.ErrorContains(t, err, "injected Lifecycle Restore Chunk Receipt failure")
	require.Empty(t, queryLifecycleRestoreRows(
		t,
		ctx,
		sqlExecutor,
		accountID,
		databaseName,
		hiddenName,
	).payloads)
	require.Equal(t, uint64(0), queryLifecycleRestoreUint64(
		t,
		ctx,
		sqlExecutor,
		accountID,
		fmt.Sprintf(
			`select cast(count(*) as bigint unsigned) from mo_catalog.mo_lifecycle_restore_chunks
where restore_id=unhex('%s')`,
			lifecycleRestoreTestUUIDHex(restoreID),
		),
	))
	rolledBackAttempt, getErr := repository.GetAttempt(ctx, restoreID)
	require.NoError(t, getErr)
	require.Equal(t, uint64(0), rolledBackAttempt.NextChunkOrdinal)
	require.Equal(t, uint64(0), rolledBackAttempt.RestoredRows)

	for ordinal, values := range [][]int64{{11, 22}, {33, 44}} {
		rows, receipt := lifecycleRestoreTestChunk(
			t,
			ctx,
			schema,
			restoreID,
			uint64(ordinal),
			values,
		)
		attempt, err = repository.ImportChunk(ctx, attempt, receipt, schema, rows)
		require.NoError(t, err)
	}
	require.Equal(t, uint64(2), attempt.NextChunkOrdinal)
	require.Equal(t, uint64(4), attempt.RestoredRows)
	receipts, err := repository.ListChunkReceipts(ctx, restoreID)
	require.NoError(t, err)
	require.Len(t, receipts, 2)

	hiddenRows := queryLifecycleRestoreRows(
		t,
		ctx,
		sqlExecutor,
		accountID,
		databaseName,
		hiddenName,
	)
	require.Equal(t, []int64{11, 22, 33, 44}, hiddenRows.payloads)
	require.Len(t, hiddenRows.fakePKs, 4)
	assertLifecycleRestoreFakePKs(t, hiddenRows.fakePKs)

	require.NoError(t, repository.Publish(ctx, attempt, verifiedHash, schema, nil))
	publishedRows := queryLifecycleRestoreRows(
		t,
		ctx,
		sqlExecutor,
		accountID,
		databaseName,
		targetName,
	)
	require.Equal(t, hiddenRows, publishedRows)
	assertLifecycleRestoreFakePKs(t, publishedRows.fakePKs)

	current, err := repository.GetAttempt(ctx, restoreID)
	require.NoError(t, err)
	require.Equal(t, "DONE", current.State)
	require.Equal(t, verifiedHash, current.VerifiedHash)
	previousFakePKs := append([]uint64(nil), publishedRows.fakePKs...)
	mustExecLifecycleRestoreSQL(
		t,
		ctx,
		sqlExecutor,
		accountID,
		fmt.Sprintf(
			"insert into `%s`.`%s`(payload) values(55)",
			databaseName,
			targetName,
		),
	)
	afterInsert := queryLifecycleRestoreRows(
		t,
		ctx,
		sqlExecutor,
		accountID,
		databaseName,
		targetName,
	)
	require.Equal(t, []int64{11, 22, 33, 44, 55}, afterInsert.payloads)
	assertLifecycleRestoreFakePKs(t, afterInsert.fakePKs)
	require.NotContains(t, previousFakePKs, afterInsert.fakePKs[len(afterInsert.fakePKs)-1])
	leaseCount := queryLifecycleRestoreUint64(
		t,
		ctx,
		sqlExecutor,
		accountID,
		fmt.Sprintf(
			`select cast(count(*) as bigint unsigned) from mo_catalog.mo_lifecycle_datasets
where dataset_id=unhex('%s') and restore_lease_id is null`,
			lifecycleRestoreTestUUIDHex(datasetID),
		),
	)
	require.Equal(t, uint64(1), leaseCount)
}

func mustExecLifecycleRestoreSQL(
	t *testing.T,
	ctx context.Context,
	sqlExecutor executor.SQLExecutor,
	accountID uint32,
	sql string,
) {
	t.Helper()
	result, err := sqlExecutor.Exec(
		ctx,
		sql,
		executor.Options{}.
			WithAccountID(accountID).
			WithWaitCommittedLogApplied(),
	)
	require.NoError(t, err, sql)
	result.Close()
}

func queryLifecycleRestoreUint64(
	t *testing.T,
	ctx context.Context,
	sqlExecutor executor.SQLExecutor,
	accountID uint32,
	sql string,
) uint64 {
	t.Helper()
	result, err := sqlExecutor.Exec(
		ctx,
		sql,
		executor.Options{}.
			WithAccountID(accountID).
			WithWaitCommittedLogApplied(),
	)
	require.NoError(t, err, sql)
	defer result.Close()
	var value uint64
	rowsRead := 0
	result.ReadRows(func(rows int, columns []*vector.Vector) bool {
		require.Len(t, columns, 1)
		for row := 0; row < rows; row++ {
			require.False(t, columns[0].GetNulls().Contains(uint64(row)))
			value = vector.GetFixedAtNoTypeCheck[uint64](columns[0], row)
			rowsRead++
		}
		return true
	})
	require.Equal(t, 1, rowsRead)
	return value
}

type lifecycleRestoreTestRows struct {
	payloads []int64
	fakePKs  []uint64
}

func queryLifecycleRestoreRows(
	t *testing.T,
	ctx context.Context,
	sqlExecutor executor.SQLExecutor,
	accountID uint32,
	databaseName string,
	tableName string,
) lifecycleRestoreTestRows {
	t.Helper()
	result, err := sqlExecutor.Exec(
		ctx,
		fmt.Sprintf(
			"select payload,%s from `%s`.`%s` order by payload",
			catalog.FakePrimaryKeyColName,
			databaseName,
			tableName,
		),
		executor.Options{}.
			WithAccountID(accountID).
			WithWaitCommittedLogApplied(),
	)
	require.NoError(t, err)
	defer result.Close()
	var values lifecycleRestoreTestRows
	result.ReadRows(func(rows int, columns []*vector.Vector) bool {
		require.Len(t, columns, 2)
		for row := 0; row < rows; row++ {
			require.False(t, columns[0].GetNulls().Contains(uint64(row)))
			require.False(t, columns[1].GetNulls().Contains(uint64(row)))
			values.payloads = append(
				values.payloads,
				vector.GetFixedAtNoTypeCheck[int64](columns[0], row),
			)
			values.fakePKs = append(
				values.fakePKs,
				vector.GetFixedAtNoTypeCheck[uint64](columns[1], row),
			)
		}
		return true
	})
	return values
}

func assertLifecycleRestoreFakePKs(t *testing.T, values []uint64) {
	t.Helper()
	seen := make(map[uint64]struct{}, len(values))
	for _, value := range values {
		require.NotZero(t, value)
		_, duplicate := seen[value]
		require.False(t, duplicate, "duplicate fake primary key %d", value)
		seen[value] = struct{}{}
	}
}

func lifecycleRestoreTestChunk(
	t *testing.T,
	ctx context.Context,
	schema lifecyclepkg.SchemaDescriptor,
	restoreID string,
	ordinal uint64,
	values []int64,
) ([][]lifecyclepkg.CanonicalCell, lifecyclepkg.RestoreChunkReceipt) {
	t.Helper()
	schemaDigest, err := schema.Digest()
	require.NoError(t, err)
	encoder := lifecyclepkg.NewCanonicalValueEncoder(schemaDigest)
	rows := make([][]lifecyclepkg.CanonicalCell, len(values))
	for index, value := range values {
		rows[index] = []lifecyclepkg.CanonicalCell{{
			Type:  types.T_int64.ToType(),
			Value: value,
		}}
		require.NoError(t, encoder.WriteRow(ctx, rows[index]))
	}
	contentHash := encoder.Sum()
	digestInput := append([]byte(fmt.Sprintf("chunk-%d:", ordinal)), contentHash[:]...)
	return rows, lifecyclepkg.RestoreChunkReceipt{
		RestoreID:            restoreID,
		ChunkOrdinal:         ordinal,
		FileOrdinal:          uint32(ordinal),
		RowGroupOrdinal:      0,
		ChunkDigest:          sha256.Sum256(digestInput),
		RowCount:             encoder.RowCount(),
		LogicalBytes:         encoder.LogicalBytes(),
		CanonicalContentHash: contentHash,
	}
}

func lifecycleRestoreTestUUIDHex(value string) string {
	return strings.ReplaceAll(value, "-", "")
}

type failLifecycleRestoreChunkReceiptExecutor struct {
	delegate executor.SQLExecutor
	failed   bool
}

func (sqlExecutor *failLifecycleRestoreChunkReceiptExecutor) Exec(
	ctx context.Context,
	sql string,
	options executor.Options,
) (executor.Result, error) {
	return sqlExecutor.delegate.Exec(ctx, sql, options)
}

func (sqlExecutor *failLifecycleRestoreChunkReceiptExecutor) ExecTxn(
	ctx context.Context,
	execFunc func(executor.TxnExecutor) error,
	options executor.Options,
) error {
	return sqlExecutor.delegate.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			return execFunc(failLifecycleRestoreChunkReceiptTxn{
				delegate: txn,
				owner:    sqlExecutor,
			})
		},
		options,
	)
}

type failLifecycleRestoreChunkReceiptTxn struct {
	delegate executor.TxnExecutor
	owner    *failLifecycleRestoreChunkReceiptExecutor
}

func (txn failLifecycleRestoreChunkReceiptTxn) Use(database string) {
	txn.delegate.Use(database)
}

func (txn failLifecycleRestoreChunkReceiptTxn) LockTable(table string) error {
	return txn.delegate.LockTable(table)
}

func (txn failLifecycleRestoreChunkReceiptTxn) Exec(
	sql string,
	options executor.StatementOption,
) (executor.Result, error) {
	if !txn.owner.failed && strings.Contains(
		strings.ToLower(sql),
		"insert into mo_catalog.mo_lifecycle_restore_chunks",
	) {
		txn.owner.failed = true
		return executor.Result{}, fmt.Errorf(
			"injected Lifecycle Restore Chunk Receipt failure",
		)
	}
	return txn.delegate.Exec(sql, options)
}

func (txn failLifecycleRestoreChunkReceiptTxn) Txn() client.TxnOperator {
	return txn.delegate.Txn()
}
