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
	"encoding/hex"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func lifecycleTableDef(columnType types.T) *plan.TableDef {
	return &plan.TableDef{
		TblId:     42,
		LogicalId: 42,
		DbId:      7,
		Name:      "events",
		DbName:    "db",
		Version:   3,
		Cols: []*plan.ColDef{
			{
				ColId:   1,
				Name:    "id",
				NotNull: true,
				Typ:     plan.Type{Id: int32(types.T_int64), NotNullable: true},
			},
			{
				ColId:   2,
				Name:    "created_at",
				NotNull: true,
				Typ:     plan.Type{Id: int32(columnType), NotNullable: true},
			},
		},
	}
}

func TestValidateLifecyclePolicy(t *testing.T) {
	for _, typ := range []types.T{types.T_date, types.T_datetime, types.T_timestamp} {
		t.Run(typ.String(), func(t *testing.T) {
			def := lifecycleTableDef(typ)
			policy := tree.LifecyclePolicy{
				Column:          "created_at",
				ExpireAfterDays: 90,
				Action:          tree.LifecycleActionDelete,
			}
			column, digest, err := validateLifecyclePolicy(context.Background(), def, policy)
			require.NoError(t, err)
			require.Equal(t, uint64(2), column.ColId)
			require.NotEqual(t, [32]byte{}, digest)
		})
	}
}

func TestValidateLifecycleArchiveRequiresExplicitPurgeEligibility(t *testing.T) {
	def := lifecycleTableDef(types.T_timestamp)
	policy := tree.LifecyclePolicy{
		Column:          "created_at",
		ExpireAfterDays: 90,
		Action:          tree.LifecycleActionArchive,
		HasStage:        true,
		Stage:           "archive",
	}
	_, _, err := validateLifecyclePolicy(context.Background(), def, policy)
	require.ErrorContains(t, err, "requires PURGE ELIGIBLE AFTER")

	policy.HasPurgeAfter = true
	policy.PurgeAfterDays = 365
	_, _, err = validateLifecyclePolicy(context.Background(), def, policy)
	require.NoError(t, err)
}

func TestValidateLifecyclePolicyRejectsUnsupportedPayloadColumn(t *testing.T) {
	def := lifecycleTableDef(types.T_timestamp)
	def.Cols = append(def.Cols, &plan.ColDef{
		ColId: 3,
		Name:  "embedding",
		Typ:   plan.Type{Id: int32(types.T_array_float32)},
	})
	policy := tree.LifecyclePolicy{
		Column:          "created_at",
		ExpireAfterDays: 90,
		Action:          tree.LifecycleActionArchive,
		HasStage:        true,
		Stage:           "archive",
		HasPurgeAfter:   true,
		PurgeAfterDays:  365,
	}

	_, _, err := validateLifecyclePolicy(context.Background(), def, policy)
	require.ErrorContains(t, err, "Lifecycle archive column embedding")
}

func TestValidateLifecycleBindingAccountRejectsSystemAccount(t *testing.T) {
	require.ErrorContains(
		t,
		validateLifecycleBindingAccount(context.Background(), sysAccountID),
		"system account",
	)
	require.NoError(
		t,
		validateLifecycleBindingAccount(context.Background(), 17),
	)
}

func TestValidateLifecyclePolicyRejectsUnsupportedTables(t *testing.T) {
	validPolicy := tree.LifecyclePolicy{
		Column:          "created_at",
		ExpireAfterDays: 90,
		Action:          tree.LifecycleActionDelete,
	}

	tests := []struct {
		name   string
		mutate func(*plan.TableDef)
	}{
		{
			name: "nullable lifecycle column",
			mutate: func(def *plan.TableDef) {
				def.Cols[1].NotNull = false
				def.Cols[1].Typ.NotNullable = false
			},
		},
		{
			name: "unsupported lifecycle type",
			mutate: func(def *plan.TableDef) {
				def.Cols[1].Typ.Id = int32(types.T_varchar)
			},
		},
		{
			name: "partition table",
			mutate: func(def *plan.TableDef) {
				def.Partition = &plan.Partition{PartitionDefs: []*plan.PartitionDef{{}}}
			},
		},
		{
			name: "secondary index",
			mutate: func(def *plan.TableDef) {
				def.Indexes = []*plan.IndexDef{{IndexName: "idx_created_at"}}
			},
		},
		{
			name: "foreign key",
			mutate: func(def *plan.TableDef) {
				def.Fkeys = []*plan.ForeignKeyDef{{Name: "fk"}}
			},
		},
		{
			name: "hidden table",
			mutate: func(def *plan.TableDef) {
				def.Hidden = true
			},
		},
		{
			name: "external table",
			mutate: func(def *plan.TableDef) {
				def.TableType = catalog.SystemExternalRel
			},
		},
		{
			name: "publication subscription table",
			mutate: func(def *plan.TableDef) {
				def.Props = []*plan.PropertyDef{{
					Key:   catalog.PropFromPublication,
					Value: "true",
				}}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			def := lifecycleTableDef(types.T_timestamp)
			test.mutate(def)
			_, _, err := validateLifecyclePolicy(context.Background(), def, validPolicy)
			require.Error(t, err)
		})
	}
}

func TestLifecycleSchemaDigestChangesOnSemanticSchemaChange(t *testing.T) {
	base := lifecycleTableDef(types.T_timestamp)
	first := lifecycleSchemaDigest(base)
	second := lifecycleSchemaDigest(lifecycleTableDef(types.T_timestamp))
	require.Equal(t, first, second)

	changed := lifecycleTableDef(types.T_timestamp)
	changed.Cols[1].Typ.Scale = 6
	require.NotEqual(t, first, lifecycleSchemaDigest(changed))

	changed = lifecycleTableDef(types.T_timestamp)
	changed.Cols[1].Name = "event_time"
	require.NotEqual(t, first, lifecycleSchemaDigest(changed))
}

func TestBuildLifecycleBindingUpsertSQL(t *testing.T) {
	def := lifecycleTableDef(types.T_timestamp)
	column := def.Cols[1]
	schemaDigest := lifecycleSchemaDigest(def)
	stageIdentity := lifecycleStageIdentity{
		ID:               12,
		URL:              "s3://archive-bucket/mo",
		Provider:         "amazon",
		Endpoint:         "https://s3.example.com",
		Region:           "me-south-1",
		Bucket:           "archive-bucket",
		Prefix:           "mo",
		CredentialHandle: "deployment-role/archive",
	}
	stageDigest := lifecycleStageIdentityDigest(stageIdentity)
	sql := buildLifecycleBindingUpsertSQL(
		17,
		"00112233445566778899aabbccddeeff",
		def,
		column,
		schemaDigest,
		tree.LifecyclePolicy{
			Column:          "created_at",
			ExpireAfterDays: 90,
			Action:          tree.LifecycleActionArchive,
			Stage:           "archive_stage",
			HasStage:        true,
			PurgeAfterDays:  730,
			HasPurgeAfter:   true,
		},
		&lifecycleStageIdentity{ID: 12, Digest: stageDigest},
	)

	require.Contains(t, sql, "insert into mo_catalog.mo_lifecycle_bindings")
	require.Contains(t, sql, "unhex('00112233445566778899aabbccddeeff')")
	require.Contains(t, sql, "unhex('"+hex.EncodeToString(schemaDigest[:])+"')")
	require.Contains(t, sql, "unhex('"+hex.EncodeToString(stageDigest[:])+"')")
	require.Contains(t, sql, "'ARCHIVE'")
	require.Contains(t, sql, "90")
	require.Contains(t, sql, "730")
	require.Contains(t, sql, "binding_generation = binding_generation + 1")
	require.Contains(t, sql, "version = version + 1")
	require.NotContains(t, strings.ToLower(sql), "credential")
}

func TestBuildLifecycleBindingUpsertSQLDeleteHasNoStage(t *testing.T) {
	def := lifecycleTableDef(types.T_date)
	sql := buildLifecycleBindingUpsertSQL(
		17,
		"00112233445566778899aabbccddeeff",
		def,
		def.Cols[1],
		lifecycleSchemaDigest(def),
		tree.LifecyclePolicy{
			Column:          "created_at",
			ExpireAfterDays: 7,
			Action:          tree.LifecycleActionDelete,
		},
		nil,
	)
	require.Contains(t, sql, "'DELETE'")
	require.Contains(t, sql, "NULL,NULL,NULL")
}

func TestLifecycleStageIdentityDigestIsStableAndCredentialIndependent(t *testing.T) {
	first := lifecycleStageIdentity{
		ID:               7,
		URL:              "s3://bucket/prefix",
		Provider:         "amazon",
		Endpoint:         "https://s3.example.com",
		Region:           "region-1",
		Bucket:           "bucket",
		Prefix:           "prefix",
		CredentialHandle: "iam-role-1",
	}
	first.Digest = lifecycleStageIdentityDigest(first)
	require.Equal(t, first.Digest, lifecycleStageIdentityDigest(first))
	changed := first
	changed.Prefix = "other"
	require.NotEqual(t, first.Digest, lifecycleStageIdentityDigest(changed))
	changed = first
	changed.CredentialHandle = "iam-role-2"
	require.NotEqual(t, first.Digest, lifecycleStageIdentityDigest(changed))
}

func TestResolveLifecycleStageIdentityRequiresDeploymentCertification(t *testing.T) {
	stageURL, err := url.Parse("s3://archive-bucket/mo/history")
	require.NoError(t, err)
	credentials := "provider=amazon,endpoint=https://s3.example.com,aws_region=me-south-1,aws_key_id=inline,aws_secret_key=secret"
	certified := lifecycleArchiveStageCertification{
		AccountID:                17,
		StageID:                  12,
		CanonicalURL:             "s3://archive-bucket/mo/history",
		Provider:                 "amazon",
		Endpoint:                 "https://s3.example.com",
		Region:                   "me-south-1",
		CredentialHandle:         "role-arn:arn:aws:iam::17:role/mo-archive",
		VersioningDisabled:       true,
		AbortIncompleteMultipart: true,
	}
	identity, err := resolveLifecycleStageIdentity(
		context.Background(),
		17,
		12,
		stageURL,
		credentials,
		[]lifecycleArchiveStageCertification{certified},
	)
	require.NoError(t, err)
	require.Equal(t, "archive-bucket", identity.Bucket)
	require.Equal(t, "mo/history", identity.Prefix)
	require.Equal(
		t,
		"role-arn:arn:aws:iam::17:role/mo-archive",
		identity.CredentialHandle,
	)
	require.NotContains(t, string(identity.Frozen), "inline")
	require.NotContains(t, string(identity.Frozen), "secret")

	_, err = resolveLifecycleStageIdentity(
		context.Background(),
		17,
		12,
		stageURL,
		credentials,
		nil,
	)
	require.Error(t, err)

	certified.VersioningDisabled = false
	_, err = resolveLifecycleStageIdentity(
		context.Background(),
		17,
		12,
		stageURL,
		credentials,
		[]lifecycleArchiveStageCertification{certified},
	)
	require.Error(t, err)

	certified.VersioningDisabled = true
	certified.CredentialHandle = "deployment-role/archive"
	_, err = resolveLifecycleStageIdentity(
		context.Background(),
		17,
		12,
		stageURL,
		credentials,
		[]lifecycleArchiveStageCertification{certified},
	)
	require.ErrorContains(t, err, "credential handle")
}

func TestBuildLifecycleBindingStateSQL(t *testing.T) {
	require.Contains(t,
		buildLifecycleBindingStateSQL(3, 42, lifecycleBindingStatePaused),
		"state = 'PAUSED'")
	require.Contains(t,
		buildLifecycleBindingDeleteSQL(3, 42),
		"delete from mo_catalog.mo_lifecycle_bindings")
}

func TestRejectReferencedLifecycleStageMutation(t *testing.T) {
	ctx := context.Background()
	background := &backgroundExecTest{}
	background.init()
	lockSQL := lifecycleStageLockSQL("archive_stage")
	bindingSQL := lifecycleStageBindingReferenceSQL(12)
	datasetSQL := lifecycleStageDatasetReferenceSQL(12)
	background.sql2result[lockSQL] = newMrsForPasswordOfUser(
		[][]interface{}{{uint64(12)}},
	)
	background.sql2result[bindingSQL] = newMrsForPasswordOfUser(
		[][]interface{}{{"binding"}},
	)

	err := rejectReferencedLifecycleStageMutation(
		ctx,
		background,
		"archive_stage",
	)
	require.ErrorContains(t, err, "TAE object Lifecycle")
	require.NotContains(t, background.executedSQLs, datasetSQL)

	background = &backgroundExecTest{}
	background.init()
	background.sql2result[lockSQL] = newMrsForPasswordOfUser(
		[][]interface{}{{uint64(12)}},
	)
	background.sql2result[bindingSQL] = newMrsForPasswordOfUser(nil)
	background.sql2result[datasetSQL] = newMrsForPasswordOfUser(
		[][]interface{}{{"dataset"}},
	)
	err = rejectReferencedLifecycleStageMutation(
		ctx,
		background,
		"archive_stage",
	)
	require.ErrorContains(t, err, "TAE object Lifecycle")

	background = &backgroundExecTest{}
	background.init()
	background.sql2result[lockSQL] = newMrsForPasswordOfUser(
		[][]interface{}{{uint64(12)}},
	)
	background.sql2result[bindingSQL] = newMrsForPasswordOfUser(nil)
	background.sql2result[datasetSQL] = newMrsForPasswordOfUser(nil)
	require.NoError(t, rejectReferencedLifecycleStageMutation(
		ctx,
		background,
		"archive_stage",
	))
}

func TestLockLifecycleTableDDLRequiresTheExactCatalogRow(t *testing.T) {
	ctx := context.Background()
	sql := `select rel_id from mo_catalog.mo_tables
where rel_id=42 and reldatabase_id=7 for update`
	background := &backgroundExecTest{}
	background.init()
	background.sql2result[sql] = newMrsForPasswordOfUser(
		[][]interface{}{{uint64(42)}},
	)
	require.NoError(t, lockLifecycleTableDDL(ctx, background, 7, 42))

	background = &backgroundExecTest{}
	background.init()
	background.sql2result[sql] = newMrsForPasswordOfUser(nil)
	require.ErrorContains(t,
		lockLifecycleTableDDL(ctx, background, 7, 42),
		"disappeared",
	)
}

func TestValidateLifecycleExistingDependenciesFailsClosed(t *testing.T) {
	ctx := context.Background()
	definition := lifecycleTableDef(types.T_timestamp)
	queries := []string{
		`select snapshot_id from mo_catalog.mo_snapshots
where (level in ('cluster','account')
or (level='database' and obj_id=7)
or (level='table' and obj_id=42))
limit 1`,
		`select pitr_id from mo_catalog.mo_pitr
where pitr_status=1 and (level in ('cluster','account')
or (level='database' and obj_id=7)
or (level='table' and obj_id=42))
limit 1`,
		`select task_id from mo_catalog.mo_cdc_watermark
where account_id=17 and db_name='db' and table_name='events' limit 1`,
		`select pub_name from mo_catalog.mo_pubs
where account_id=17 and database_id=7
and (all_table=true or table_list='*' or find_in_set('events',table_list)>0)
limit 1`,
	}
	for index, dependency := range []string{
		"Snapshot", "PITR", "CDC", "Publication",
	} {
		background := &backgroundExecTest{}
		background.init()
		for queryIndex, query := range queries {
			rows := [][]interface{}(nil)
			if queryIndex == index {
				rows = [][]interface{}{{"dependency"}}
			}
			background.sql2result[query] = newMrsForPasswordOfUser(rows)
		}
		err := validateLifecycleExistingDependencies(
			ctx,
			background,
			17,
			definition,
		)
		require.ErrorContains(t, err, dependency)
		require.Len(t, background.executedSQLs, index+1)
	}

	background := &backgroundExecTest{}
	background.init()
	for _, query := range queries {
		background.sql2result[query] = newMrsForPasswordOfUser(nil)
	}
	require.NoError(t, validateLifecycleExistingDependencies(
		ctx,
		background,
		17,
		definition,
	))
}
