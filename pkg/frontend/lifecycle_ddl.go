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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/stage"
	lifecyclepkg "github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/lifecycle"
)

const lifecycleSchemaDigestVersion uint16 = 1
const featureCodeLifecycle = "LIFECYCLE"

const (
	lifecycleBindingStateActive = "ACTIVE"
	lifecycleBindingStatePaused = "PAUSED"
)

type lifecycleStageIdentity struct {
	ID               uint64   `json:"stage_id"`
	URL              string   `json:"canonical_url"`
	Provider         string   `json:"provider"`
	Endpoint         string   `json:"endpoint"`
	Region           string   `json:"region"`
	Bucket           string   `json:"bucket"`
	Prefix           string   `json:"prefix"`
	StorageClass     string   `json:"storage_class,omitempty"`
	Encryption       string   `json:"encryption_identity,omitempty"`
	CredentialHandle string   `json:"credential_handle"`
	Digest           [32]byte `json:"-"`
	Frozen           []byte   `json:"-"`
}

type lifecycleArchiveStageCertification struct {
	AccountID                uint32 `json:"account_id"`
	StageID                  uint64 `json:"stage_id"`
	CanonicalURL             string `json:"canonical_url"`
	Provider                 string `json:"provider"`
	Endpoint                 string `json:"endpoint"`
	Region                   string `json:"region"`
	CredentialHandle         string `json:"credential_handle"`
	StorageClass             string `json:"storage_class,omitempty"`
	EncryptionIdentity       string `json:"encryption_identity,omitempty"`
	VersioningDisabled       bool   `json:"versioning_disabled"`
	AbortIncompleteMultipart bool   `json:"abort_incomplete_multipart"`
}

type lifecycleReleaseSpec struct {
	ArchiveStages []lifecycleArchiveStageCertification `json:"archive_stages"`
}

func lifecycleOptionFromAlterTable(alter *tree.AlterTable) (*tree.AlterOptionLifecycle, bool) {
	if alter == nil || len(alter.Options) != 1 {
		return nil, false
	}
	option, ok := alter.Options[0].(*tree.AlterOptionLifecycle)
	return option, ok
}

func handleAlterTableLifecycle(ctx context.Context, ses *Session, alter *tree.AlterTable) error {
	option, ok := lifecycleOptionFromAlterTable(alter)
	if !ok {
		return moerr.NewInternalError(ctx, "invalid Lifecycle ALTER TABLE dispatch")
	}

	databaseName := string(alter.Table.Schema())
	if databaseName == "" {
		databaseName = ses.GetDatabaseName()
	}
	if databaseName == "" {
		return moerr.NewNoDB(ctx)
	}
	tableName := string(alter.Table.Name())
	_, tableDef, err := ses.GetTxnCompileCtx().Resolve(databaseName, tableName, nil)
	if err != nil {
		return err
	}
	if tableDef == nil {
		return moerr.NewNoSuchTable(ctx, databaseName, tableName)
	}

	accountID := ses.GetTenantInfo().GetTenantID()
	if err = validateLifecycleBindingAccount(ctx, accountID); err != nil {
		return err
	}
	background := ses.GetBackgroundExec(ctx)
	defer background.Close()
	if option.Operation == tree.LifecycleOperationSet ||
		option.Operation == tree.LifecycleOperationResume {
		if err = ensureLifecycleFeatureEnabled(ctx, ses, background); err != nil {
			return err
		}
	}
	var archiveStageCertifications []lifecycleArchiveStageCertification
	if option.Operation == tree.LifecycleOperationSet &&
		option.Policy.Action == tree.LifecycleActionArchive {
		archiveStageCertifications, err = loadLifecycleArchiveStageCertifications(
			ctx,
			background,
		)
		if err != nil {
			return err
		}
	}
	if err = background.Exec(ctx, "begin;"); err != nil {
		return err
	}

	rollback := func(cause error) error {
		if rollbackErr := background.Exec(ctx, "rollback;"); rollbackErr != nil && cause == nil {
			return rollbackErr
		}
		return cause
	}
	if err = lockLifecycleTableDDL(
		ctx,
		background,
		tableDef.DbId,
		tableDef.TblId,
	); err != nil {
		return rollback(err)
	}
	_, lockedTableDef, resolveErr := ses.GetTxnCompileCtx().Resolve(
		databaseName,
		tableName,
		nil,
	)
	if resolveErr != nil {
		return rollback(resolveErr)
	}
	if lockedTableDef == nil || lockedTableDef.TblId != tableDef.TblId {
		return rollback(moerr.NewInvalidInput(
			ctx,
			"Lifecycle table identity changed while acquiring the DDL fence",
		))
	}
	tableDef = lockedTableDef
	if option.Operation == tree.LifecycleOperationSet {
		// SET takes the existing mo_tables row lock before the scope-publication
		// feature-row barrier. Ordinary table DDL uses only the table lock and an
		// indexed Binding lookup; the feature row closes the first-Binding
		// empty-probe race with Snapshot/PITR/Publication/Clone creation.
		if err = lockLifecycleDependencyPublication(ctx, background); err != nil {
			return rollback(err)
		}
	}

	switch option.Operation {
	case tree.LifecycleOperationSet:
		column, schemaDigest, validateErr := validateLifecyclePolicy(ctx, tableDef, option.Policy)
		if validateErr != nil {
			return rollback(validateErr)
		}
		if validateErr = validateLifecycleExistingDependencies(
			ctx,
			background,
			accountID,
			tableDef,
		); validateErr != nil {
			return rollback(validateErr)
		}

		var stageIdentity *lifecycleStageIdentity
		if option.Policy.Action == tree.LifecycleActionArchive {
			stageIdentity, validateErr = loadLifecycleStageIdentity(
				ctx,
				background,
				accountID,
				string(option.Policy.Stage),
				archiveStageCertifications,
			)
			if validateErr != nil {
				return rollback(validateErr)
			}
		}

		bindingID, buildErr := types.BuildUuid()
		if buildErr != nil {
			return rollback(buildErr)
		}
		sql := buildLifecycleBindingUpsertSQL(
			accountID,
			hex.EncodeToString(bindingID[:]),
			tableDef,
			column,
			schemaDigest,
			option.Policy,
			stageIdentity,
		)
		if err = background.Exec(ctx, sql); err != nil {
			return rollback(err)
		}

	case tree.LifecycleOperationPause, tree.LifecycleOperationResume, tree.LifecycleOperationUnset:
		exists, existsErr := lifecycleBindingExists(ctx, background, accountID, tableDef.TblId)
		if existsErr != nil {
			return rollback(existsErr)
		}
		if !exists {
			return rollback(moerr.NewInvalidInputf(
				ctx,
				"table %s.%s has no Lifecycle binding",
				databaseName,
				tableName,
			))
		}

		switch option.Operation {
		case tree.LifecycleOperationPause:
			err = background.Exec(ctx, buildLifecycleBindingStateSQL(
				accountID, tableDef.TblId, lifecycleBindingStatePaused))
		case tree.LifecycleOperationResume:
			err = background.Exec(ctx, buildLifecycleBindingStateSQL(
				accountID, tableDef.TblId, lifecycleBindingStateActive))
		case tree.LifecycleOperationUnset:
			err = background.Exec(ctx, buildLifecycleBindingDeleteSQL(accountID, tableDef.TblId))
		}
		if err != nil {
			return rollback(err)
		}

	default:
		return rollback(moerr.NewInvalidInput(ctx, "unknown Lifecycle operation"))
	}

	if err = background.Exec(ctx, "commit;"); err != nil {
		return err
	}
	return nil
}

func validateLifecycleBindingAccount(ctx context.Context, accountID uint32) error {
	if accountID == sysAccountID {
		return moerr.NewNotSupported(
			ctx,
			"Lifecycle bindings in the system account",
		)
	}
	return nil
}

func validateLifecycleExistingDependencies(
	ctx context.Context,
	background BackgroundExec,
	accountID uint32,
	tableDef *plan.TableDef,
) error {
	if background == nil || tableDef == nil {
		return moerr.NewInvalidInput(
			ctx,
			"Lifecycle dependency validation is incomplete",
		)
	}
	queries := []struct {
		name string
		sql  string
	}{
		{
			name: "Snapshot/Clone/Branch",
			sql: fmt.Sprintf(
				`select snapshot_id from mo_catalog.mo_snapshots
where (level in ('cluster','account')
or (level='database' and obj_id=%d)
or (level='table' and obj_id=%d))
limit 1`,
				tableDef.DbId,
				tableDef.TblId,
			),
		},
		{
			name: "PITR",
			sql: fmt.Sprintf(
				`select pitr_id from mo_catalog.mo_pitr
where pitr_status=1 and (level in ('cluster','account')
or (level='database' and obj_id=%d)
or (level='table' and obj_id=%d))
limit 1`,
				tableDef.DbId,
				tableDef.TblId,
			),
		},
		{
			name: "CDC",
			sql: fmt.Sprintf(
				`select task_id from mo_catalog.mo_cdc_watermark
where account_id=%d and db_name=%s and table_name=%s limit 1`,
				accountID,
				quoteSQLStringLiteral(tableDef.DbName),
				quoteSQLStringLiteral(tableDef.Name),
			),
		},
		{
			name: "Publication",
			sql: fmt.Sprintf(
				`select pub_name from mo_catalog.mo_pubs
where account_id=%d and database_id=%d
and (all_table=true or table_list='*' or find_in_set(%s,table_list)>0)
limit 1`,
				accountID,
				tableDef.DbId,
				quoteSQLStringLiteral(tableDef.Name),
			),
		},
	}
	for _, query := range queries {
		background.ClearExecResultSet()
		if err := background.Exec(ctx, query.sql); err != nil {
			return err
		}
		results, err := getResultSet(ctx, background)
		if err != nil {
			return err
		}
		if execResultArrayHasData(results) {
			return moerr.NewNotSupportedf(
				ctx,
				"Lifecycle while %s references the table",
				query.name,
			)
		}
	}
	return nil
}

func ensureLifecycleFeatureEnabled(
	ctx context.Context,
	ses *Session,
	background BackgroundExec,
) error {
	systemCtx := defines.AttachAccountId(ctx, sysAccountID)
	enabled, _, exists, err := queryFeatureRegistry(
		systemCtx,
		ses,
		background,
		featureCodeLifecycle,
	)
	if err != nil {
		return err
	}
	if !exists || !enabled {
		return moerr.NewNotSupported(
			ctx,
			"TAE object Lifecycle retirement is disabled by the cluster release gate",
		)
	}
	return nil
}

func lockLifecycleTableDDL(
	ctx context.Context,
	background BackgroundExec,
	databaseID uint64,
	physicalTableID uint64,
) error {
	background.ClearExecResultSet()
	sql := fmt.Sprintf(
		`select rel_id from mo_catalog.mo_tables
where rel_id=%d and reldatabase_id=%d for update`,
		physicalTableID,
		databaseID,
	)
	if err := background.Exec(ctx, sql); err != nil {
		return err
	}
	results, err := getResultSet(ctx, background)
	if err != nil {
		return err
	}
	if !execResultArrayHasData(results) {
		return moerr.NewInvalidInput(
			ctx,
			"Lifecycle table disappeared while acquiring the DDL fence",
		)
	}
	return nil
}

func lifecycleBindingExists(
	ctx context.Context,
	background BackgroundExec,
	accountID uint32,
	physicalTableID uint64,
) (bool, error) {
	background.ClearExecResultSet()
	sql := fmt.Sprintf(
		`select binding_id from mo_catalog.mo_lifecycle_bindings where account_id = %d and physical_table_id = %d`,
		accountID,
		physicalTableID,
	)
	if err := background.Exec(ctx, sql); err != nil {
		return false, err
	}
	results, err := getResultSet(ctx, background)
	if err != nil {
		return false, err
	}
	return execResultArrayHasData(results), nil
}

func loadLifecycleStageIdentity(
	ctx context.Context,
	background BackgroundExec,
	accountID uint32,
	stageName string,
	certifications []lifecycleArchiveStageCertification,
) (*lifecycleStageIdentity, error) {
	background.ClearExecResultSet()
	sql := fmt.Sprintf(
		`select stage_id,url,stage_credentials,stage_status from mo_catalog.mo_stages where stage_name = %s for update`,
		quoteSQLStringLiteral(stageName),
	)
	if err := background.Exec(ctx, sql); err != nil {
		return nil, err
	}
	results, err := getResultSet(ctx, background)
	if err != nil {
		return nil, err
	}
	if !execResultArrayHasData(results) {
		return nil, moerr.NewInvalidInputf(ctx, "Lifecycle stage %q does not exist", stageName)
	}
	stageID, err := results[0].GetUint64(ctx, 0, 0)
	if err != nil {
		return nil, err
	}
	stageURL, err := results[0].GetString(ctx, 0, 1)
	if err != nil {
		return nil, err
	}
	stageCredentials, err := results[0].GetString(ctx, 0, 2)
	if err != nil {
		return nil, err
	}
	stageStatus, err := results[0].GetString(ctx, 0, 3)
	if err != nil {
		return nil, err
	}
	if !strings.EqualFold(stageStatus, "in_use") {
		return nil, moerr.NewInvalidInputf(ctx, "Lifecycle stage %q is not in use", stageName)
	}
	parsedURL, err := url.Parse(stageURL)
	if err != nil || parsedURL.Scheme == "" {
		return nil, moerr.NewInvalidInputf(ctx, "Lifecycle stage %q has an invalid URL", stageName)
	}
	if !strings.EqualFold(parsedURL.Scheme, "s3") {
		return nil, moerr.NewNotSupportedf(
			ctx,
			"Lifecycle archive stage scheme %q",
			parsedURL.Scheme,
		)
	}
	return resolveLifecycleStageIdentity(
		ctx,
		accountID,
		stageID,
		parsedURL,
		stageCredentials,
		certifications,
	)
}

func loadLifecycleArchiveStageCertifications(
	ctx context.Context,
	background BackgroundExec,
) ([]lifecycleArchiveStageCertification, error) {
	systemCtx := defines.AttachAccountId(ctx, sysAccountID)
	background.ClearExecResultSet()
	sql := fmt.Sprintf(
		`select scope_spec from mo_catalog.mo_feature_registry where feature_code = %s and enabled = true`,
		quoteSQLStringLiteral(featureCodeLifecycle),
	)
	if err := background.Exec(systemCtx, sql); err != nil {
		return nil, err
	}
	results, err := getResultSet(systemCtx, background)
	if err != nil {
		return nil, err
	}
	if !execResultArrayHasData(results) {
		return nil, moerr.NewNotSupported(
			ctx,
			"Lifecycle release has no certified Archive Stage configuration",
		)
	}
	encoded, err := results[0].GetString(systemCtx, 0, 0)
	if err != nil {
		return nil, err
	}
	var spec lifecycleReleaseSpec
	if err := json.Unmarshal([]byte(encoded), &spec); err != nil {
		return nil, moerr.NewInvalidInputf(
			ctx,
			"invalid Lifecycle release configuration: %v",
			err,
		)
	}
	return spec.ArchiveStages, nil
}

func resolveLifecycleStageIdentity(
	ctx context.Context,
	accountID uint32,
	stageID uint64,
	stageURL *url.URL,
	credentials string,
	certifications []lifecycleArchiveStageCertification,
) (*lifecycleStageIdentity, error) {
	if stageURL == nil || !strings.EqualFold(stageURL.Scheme, stage.S3_PROTOCOL) {
		return nil, moerr.NewNotSupported(ctx, "Lifecycle requires an S3-compatible Stage")
	}
	credentialValues, err := stage.CredentialsToMap(credentials)
	if err != nil {
		return nil, err
	}
	provider := strings.ToLower(strings.TrimSpace(credentialValues[stage.PARAMKEY_PROVIDER]))
	endpoint := strings.TrimRight(strings.TrimSpace(credentialValues[stage.PARAMKEY_ENDPOINT]), "/")
	region := strings.TrimSpace(credentialValues[stage.PARAMKEY_AWS_REGION])
	canonicalURL := stageURL.String()
	bucket, prefix, _, err := stage.ParseS3Url(stageURL)
	if err != nil {
		return nil, err
	}
	prefix = strings.Trim(prefix, "/")
	for _, certification := range certifications {
		if certification.AccountID != accountID ||
			certification.StageID != stageID {
			continue
		}
		if !certification.VersioningDisabled ||
			!certification.AbortIncompleteMultipart {
			return nil, moerr.NewNotSupported(
				ctx,
				"Lifecycle Archive Stage must certify disabled versioning and incomplete multipart cleanup",
			)
		}
		if certification.CredentialHandle == "" ||
			certification.CanonicalURL != canonicalURL ||
			!strings.EqualFold(certification.Provider, provider) ||
			strings.TrimRight(certification.Endpoint, "/") != endpoint ||
			certification.Region != region {
			return nil, moerr.NewInvalidInput(
				ctx,
				"Lifecycle Archive Stage does not match its deployment certification",
			)
		}
		if err := lifecyclepkg.ValidateArchiveCredentialHandle(
			certification.CredentialHandle,
		); err != nil {
			return nil, moerr.NewNotSupported(ctx, err.Error())
		}
		identity := &lifecycleStageIdentity{
			ID:               stageID,
			URL:              canonicalURL,
			Provider:         provider,
			Endpoint:         endpoint,
			Region:           region,
			Bucket:           bucket,
			Prefix:           prefix,
			StorageClass:     certification.StorageClass,
			Encryption:       certification.EncryptionIdentity,
			CredentialHandle: certification.CredentialHandle,
		}
		identity.Frozen, err = json.Marshal(identity)
		if err != nil {
			return nil, err
		}
		identity.Digest = lifecycleStageIdentityDigest(*identity)
		return identity, nil
	}
	return nil, moerr.NewNotSupported(
		ctx,
		"Lifecycle Archive Stage is not present in the deployment certification allowlist",
	)
}

func validateLifecyclePolicy(
	ctx context.Context,
	tableDef *plan.TableDef,
	policy tree.LifecyclePolicy,
) (*plan.ColDef, [32]byte, error) {
	if tableDef == nil {
		return nil, [32]byte{}, moerr.NewNoSuchTable(ctx, "", "")
	}
	if tableDef.Hidden {
		return nil, [32]byte{}, moerr.NewNotSupported(ctx, "Lifecycle on hidden tables")
	}
	if tableDef.TableType == catalog.SystemExternalRel ||
		tableDef.TableType == catalog.SystemSourceRel {
		return nil, [32]byte{}, moerr.NewNotSupported(
			ctx,
			"Lifecycle on external or source tables",
		)
	}
	if lifecycleTableFromPublication(tableDef) {
		return nil, [32]byte{}, moerr.NewNotSupported(
			ctx,
			"Lifecycle on publication subscription tables",
		)
	}
	if tableDef.Partition != nil && len(tableDef.Partition.PartitionDefs) > 0 {
		return nil, [32]byte{}, moerr.NewNotSupported(ctx, "Lifecycle on partition tables")
	}
	if len(tableDef.Indexes) != 0 {
		return nil, [32]byte{}, moerr.NewNotSupported(ctx, "Lifecycle on tables with secondary indexes")
	}
	if len(tableDef.Fkeys) != 0 || len(tableDef.RefChildTbls) != 0 {
		return nil, [32]byte{}, moerr.NewNotSupported(ctx, "Lifecycle on tables with foreign keys")
	}
	if policy.ExpireAfterDays == 0 {
		return nil, [32]byte{}, moerr.NewInvalidInput(ctx, "Lifecycle EXPIRE AFTER must be positive")
	}
	if policy.Action == tree.LifecycleActionArchive {
		if !policy.HasStage || policy.Stage == "" {
			return nil, [32]byte{}, moerr.NewInvalidInput(ctx, "Lifecycle ARCHIVE requires STAGE")
		}
		if !policy.HasPurgeAfter {
			return nil, [32]byte{}, moerr.NewInvalidInput(
				ctx,
				"Lifecycle ARCHIVE requires PURGE ELIGIBLE AFTER",
			)
		}
		if policy.PurgeAfterDays <= policy.ExpireAfterDays {
			return nil, [32]byte{}, moerr.NewInvalidInput(ctx, "Lifecycle PURGE ELIGIBLE AFTER must be greater than EXPIRE AFTER")
		}
	} else if policy.HasStage || policy.HasPurgeAfter {
		return nil, [32]byte{}, moerr.NewInvalidInput(ctx, "Lifecycle DELETE does not accept STAGE or PURGE ELIGIBLE AFTER")
	}

	var lifecycleColumn *plan.ColDef
	for _, column := range tableDef.Cols {
		if column != nil && strings.EqualFold(column.Name, string(policy.Column)) {
			lifecycleColumn = column
			break
		}
	}
	if lifecycleColumn == nil {
		return nil, [32]byte{}, moerr.NewInvalidInputf(ctx, "Lifecycle column %q does not exist", policy.Column)
	}
	if !lifecycleColumn.NotNull || !lifecycleColumn.Typ.NotNullable {
		return nil, [32]byte{}, moerr.NewInvalidInputf(ctx, "Lifecycle column %q must be NOT NULL", policy.Column)
	}
	switch types.T(lifecycleColumn.Typ.Id) {
	case types.T_date, types.T_datetime, types.T_timestamp:
	default:
		return nil, [32]byte{}, moerr.NewInvalidInputf(ctx, "Lifecycle column %q must be DATE, DATETIME, or TIMESTAMP", policy.Column)
	}
	if lifecycleColumn.GeneratedCol != nil {
		return nil, [32]byte{}, moerr.NewNotSupported(ctx, "Lifecycle on generated columns")
	}
	// The background classifier/rewrite and Archive/Restore paths share this
	// descriptor. Reject an unsupported user column while binding instead of
	// creating a policy whose jobs can never make progress.
	if _, _, err := lifecyclepkg.BuildSchemaDescriptor(ctx, tableDef); err != nil {
		return nil, [32]byte{}, err
	}
	return lifecycleColumn, lifecycleSchemaDigest(tableDef), nil
}

func lifecycleTableFromPublication(tableDef *plan.TableDef) bool {
	if tableDef == nil {
		return false
	}
	for _, property := range tableDef.Props {
		if property != nil &&
			property.Key == catalog.PropFromPublication &&
			strings.EqualFold(property.Value, "true") {
			return true
		}
	}
	for _, definition := range tableDef.Defs {
		properties, ok := definition.GetDef().(*plan.TableDef_DefType_Properties)
		if !ok || properties.Properties == nil {
			continue
		}
		for _, property := range properties.Properties.Properties {
			if property != nil &&
				property.Key == catalog.PropFromPublication &&
				strings.EqualFold(property.Value, "true") {
				return true
			}
		}
	}
	return false
}

func lifecycleSchemaDigest(tableDef *plan.TableDef) [32]byte {
	return lifecyclepkg.BindingSchemaDigest(tableDef)
}

func lifecycleStageIdentityDigest(identity lifecycleStageIdentity) [32]byte {
	var buf bytes.Buffer
	writeLifecycleString(&buf, "mo-lifecycle-stage-identity-v1")
	writeLifecycleUint64(&buf, identity.ID)
	writeLifecycleString(&buf, identity.URL)
	writeLifecycleString(&buf, identity.Provider)
	writeLifecycleString(&buf, identity.Endpoint)
	writeLifecycleString(&buf, identity.Region)
	writeLifecycleString(&buf, identity.Bucket)
	writeLifecycleString(&buf, identity.Prefix)
	writeLifecycleString(&buf, identity.StorageClass)
	writeLifecycleString(&buf, identity.Encryption)
	writeLifecycleString(&buf, identity.CredentialHandle)
	return sha256.Sum256(buf.Bytes())
}

func buildLifecycleBindingUpsertSQL(
	accountID uint32,
	bindingIDHex string,
	tableDef *plan.TableDef,
	lifecycleColumn *plan.ColDef,
	schemaDigest [32]byte,
	policy tree.LifecyclePolicy,
	stageIdentity *lifecycleStageIdentity,
) string {
	logicalTableID := tableDef.LogicalId
	if logicalTableID == 0 {
		logicalTableID = tableDef.TblId
	}
	action := "DELETE"
	if policy.Action == tree.LifecycleActionArchive {
		action = "ARCHIVE"
	}
	timezone := policy.EvaluationTimezone
	if timezone == "" {
		timezone = "UTC"
	}

	stageIDSQL, stageDigestSQL, purgeAfterSQL := "NULL", "NULL", "NULL"
	if stageIdentity != nil {
		stageIDSQL = fmt.Sprintf("%d", stageIdentity.ID)
		stageDigestSQL = fmt.Sprintf("unhex('%s')", hex.EncodeToString(stageIdentity.Digest[:]))
	}
	if policy.HasPurgeAfter {
		purgeAfterSQL = fmt.Sprintf("%d", policy.PurgeAfterDays)
	}

	return fmt.Sprintf(
		`insert into mo_catalog.mo_lifecycle_bindings (
binding_id,account_id,database_id,logical_table_id,physical_table_id,
binding_generation,schema_digest,lifecycle_column_id,action,expire_after_days,
late_arrival_grace_days,evaluation_timezone,stage_id,stage_identity_digest,
purge_after_days,state,version,created_at,updated_at)
values (unhex('%s'),%d,%d,%d,%d,1,unhex('%s'),%d,'%s',%d,%d,%s,%s,%s,%s,'%s',1,utc_timestamp,utc_timestamp)
on duplicate key update
database_id = values(database_id),
logical_table_id = values(logical_table_id),
schema_digest = values(schema_digest),
lifecycle_column_id = values(lifecycle_column_id),
action = values(action),
expire_after_days = values(expire_after_days),
late_arrival_grace_days = values(late_arrival_grace_days),
evaluation_timezone = values(evaluation_timezone),
stage_id = values(stage_id),
stage_identity_digest = values(stage_identity_digest),
purge_after_days = values(purge_after_days),
state = 'ACTIVE',
binding_generation = binding_generation + 1,
version = version + 1,
updated_at = utc_timestamp`,
		bindingIDHex,
		accountID,
		tableDef.DbId,
		logicalTableID,
		tableDef.TblId,
		hex.EncodeToString(schemaDigest[:]),
		lifecycleColumn.ColId,
		action,
		policy.ExpireAfterDays,
		policy.LateArrivalDays,
		quoteSQLStringLiteral(timezone),
		stageIDSQL,
		stageDigestSQL,
		purgeAfterSQL,
		lifecycleBindingStateActive,
	)
}

func buildLifecycleBindingStateSQL(accountID uint32, physicalTableID uint64, state string) string {
	return fmt.Sprintf(
		`update mo_catalog.mo_lifecycle_bindings
set state = %s,
binding_generation = binding_generation + 1,
version = version + 1,
updated_at = utc_timestamp
where account_id = %d and physical_table_id = %d`,
		quoteSQLStringLiteral(state), accountID, physicalTableID)
}

func buildLifecycleBindingDeleteSQL(accountID uint32, physicalTableID uint64) string {
	return fmt.Sprintf(
		`delete from mo_catalog.mo_lifecycle_bindings where account_id = %d and physical_table_id = %d`,
		accountID, physicalTableID)
}

func writeLifecycleString(buf *bytes.Buffer, value string) {
	writeLifecycleUint32(buf, uint32(len(value)))
	buf.WriteString(value)
}

func writeLifecycleBool(buf *bytes.Buffer, value bool) {
	if value {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}
}

func writeLifecycleUint16(buf *bytes.Buffer, value uint16) {
	var encoded [2]byte
	binary.BigEndian.PutUint16(encoded[:], value)
	buf.Write(encoded[:])
}

func writeLifecycleUint32(buf *bytes.Buffer, value uint32) {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	buf.Write(encoded[:])
}

func writeLifecycleUint64(buf *bytes.Buffer, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	buf.Write(encoded[:])
}
