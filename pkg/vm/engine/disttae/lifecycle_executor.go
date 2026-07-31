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

package disttae

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	taskpb "github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	lifecyclepkg "github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/lifecycle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

const (
	lifecycleDiscoveryPageObjects       = 64
	lifecycleDiscoveryMetaBytes         = 8 << 20
	lifecycleTargetObjectBytes          = 128 << 20
	lifecycleMaxCreatedObjects          = 32
	lifecycleMaxCertifiedBlockReadBytes = 256 << 20
	lifecycleWholeBatchMaxSources       = 64
	lifecycleWholeBatchMaxSourceBytes   = 4 << 30
)

type lifecycleObjectPlanInput struct {
	Source objectio.ObjectEntry
	Whole  bool
}

type lifecycleObjectPlan struct {
	Sources     []objectio.ObjectEntry
	Whole       bool
	SourceBytes uint64
}

// planLifecycleObjectTasks keeps Mixed Rewrite strictly single-source while
// coalescing adjacent Whole Objects into one bounded exact-retire transaction.
// This avoids one Dataset/Root per 128 MiB Object on TB-scale ordered tables
// without introducing persistent candidate state or an Object index.
func planLifecycleObjectTasks(
	inputs []lifecycleObjectPlanInput,
) []lifecycleObjectPlan {
	plans := make([]lifecycleObjectPlan, 0, len(inputs))
	wholeSources := make([]objectio.ObjectEntry, 0, lifecycleWholeBatchMaxSources)
	var wholeBytes uint64
	flushWhole := func() {
		if len(wholeSources) == 0 {
			return
		}
		plans = append(plans, lifecycleObjectPlan{
			Sources:     wholeSources,
			Whole:       true,
			SourceBytes: wholeBytes,
		})
		wholeSources = make(
			[]objectio.ObjectEntry,
			0,
			lifecycleWholeBatchMaxSources,
		)
		wholeBytes = 0
	}
	for _, input := range inputs {
		sourceBytes := lifecycleObjectPressureBytes(input.Source.ObjectStats)
		if !input.Whole {
			flushWhole()
			plans = append(plans, lifecycleObjectPlan{
				Sources:     []objectio.ObjectEntry{input.Source},
				SourceBytes: sourceBytes,
			})
			continue
		}
		if len(wholeSources) > 0 &&
			(len(wholeSources) == lifecycleWholeBatchMaxSources ||
				wholeBytes+sourceBytes > lifecycleWholeBatchMaxSourceBytes) {
			flushWhole()
		}
		wholeSources = append(wholeSources, input.Source)
		wholeBytes += sourceBytes
	}
	flushWhole()
	return plans
}

type lifecycleBindingExecutor struct {
	engine       engine.Engine
	txnClient    client.TxnClient
	sqlExecutor  executor.SQLExecutor
	taeFS        fileservice.FileService
	release      lifecyclepkg.SQLReleaseConfig
	pager        lifecyclepkg.SQLBindingPager
	admission    *lifecyclepkg.RewriteAdmission
	rewriteSlots chan struct{}
	faults       lifecyclepkg.FaultInjector
	now          func() time.Time
	epoch        uint64
}

// LifecycleTaskExecutorFactory wires the existing TaskService, transaction
// engine, FileService, Merge producer, and GC SyncProtection path. Ordinary
// tables are untouched because the coordinator pages only explicit Bindings.
func LifecycleTaskExecutorFactory(
	txnEngine engine.Engine,
	txnClient client.TxnClient,
	sqlExecutor executor.SQLExecutor,
	taeFS fileservice.FileService,
	faults lifecyclepkg.FaultInjector,
) func(context.Context, taskpb.Task) error {
	release := lifecyclepkg.SQLReleaseConfig{Executor: sqlExecutor}
	pager := lifecyclepkg.SQLBindingPager{Executor: sqlExecutor}
	admission, admissionErr := lifecyclepkg.NewRewriteAdmission(
		lifecyclepkg.RewriteReleaseProfile{
			Window:                   24 * time.Hour,
			MaxAmplification:         20,
			MaxSourceBytesPerAccount: 1 << 40,
			MaxSourceBytesPerCluster: 4 << 40,
		},
	)
	cleanupReconcileCursor := ""
	var metadataAccountCursor uint32
	var restoreCleanupAccountCursor uint32
	rewriteSlots := make(chan struct{}, 1)
	runSlots := make(chan struct{}, 1)
	var lastMetadataCompaction time.Time
	return func(ctx context.Context, scheduled taskpb.Task) error {
		// TaskService declares coordinator concurrency one, and this local guard
		// also protects cursors if a duplicate invocation is delivered during
		// runner handoff. A duplicate tick is skipped instead of queued behind a
		// potentially long Lifecycle run. Ordinary transaction and Merge paths
		// never access this slot.
		releaseRun, acquired := tryAcquireLifecycleCoordinatorRunSlot(runSlots)
		if !acquired {
			return nil
		}
		defer releaseRun()
		if admissionErr != nil {
			return admissionErr
		}
		var cleanupErr error
		cleanupReconcileCursor, cleanupErr = sweepLifecycleCleanupRoots(
			ctx,
			sqlExecutor,
			taeFS,
			faults,
			cleanupReconcileCursor,
		)
		var restoreCleanupErr error
		restoreCleanupAccountCursor, restoreCleanupErr =
			cleanupExpiredLifecycleRestores(
				ctx,
				sqlExecutor,
				restoreCleanupAccountCursor,
			)
		var metadataErr error
		now := time.Now()
		if lastMetadataCompaction.IsZero() ||
			now.Sub(lastMetadataCompaction) >= 24*time.Hour {
			metadataAccountCursor, _, metadataErr =
				(lifecyclepkg.SQLMetadataCompactor{Executor: sqlExecutor}).
					CompactPage(
						ctx,
						metadataAccountCursor,
						now,
						30*24*time.Hour,
						8,
						256,
					)
			if metadataErr == nil {
				lastMetadataCompaction = now
			}
		}
		enabled, err := release.Enabled(ctx)
		if err != nil || !enabled {
			return errors.Join(
				cleanupErr,
				restoreCleanupErr,
				metadataErr,
				err,
			)
		}
		epoch := lifecycleTaskEpoch(scheduled)
		runner := &lifecycleBindingExecutor{
			engine:       txnEngine,
			txnClient:    txnClient,
			sqlExecutor:  sqlExecutor,
			taeFS:        taeFS,
			release:      release,
			pager:        pager,
			admission:    admission,
			rewriteSlots: rewriteSlots,
			faults:       faults,
			now:          time.Now,
			epoch:        epoch,
		}
		coordinator := lifecyclepkg.NewCoordinator(
			lifecyclepkg.CoordinatorConfig{
				Enabled:             true,
				PageSize:            64,
				MaxBindingsPerRun:   1000,
				MaxClusterChildren:  8,
				MaxAccountChildren:  4,
				MaxDatabaseChildren: 2,
				MaxTableChildren:    1,
			},
			pager,
			runner.run,
		)
		return errors.Join(
			cleanupErr,
			restoreCleanupErr,
			metadataErr,
			coordinator.Run(ctx),
		)
	}
}

func tryAcquireLifecycleCoordinatorRunSlot(
	slots chan struct{},
) (func(), bool) {
	select {
	case slots <- struct{}{}:
		return func() { <-slots }, true
	default:
		return nil, false
	}
}

func cleanupExpiredLifecycleRestores(
	ctx context.Context,
	sqlExecutor executor.SQLExecutor,
	afterAccountID uint32,
) (uint32, error) {
	attempts, next, err := (lifecyclepkg.SQLExpiredRestorePager{
		Executor: sqlExecutor,
	}).Next(
		ctx,
		afterAccountID,
		time.Now(),
		8,
		64,
	)
	if err != nil {
		return afterAccountID, err
	}
	var cleanupErr error
	for _, attempt := range attempts {
		cleanupErr = errors.Join(
			cleanupErr,
			(SQLRestoreRepository{
				AccountID:          attempt.AccountID,
				TargetDatabaseName: attempt.TargetDatabaseName,
				Executor:           sqlExecutor,
			}).CleanupHidden(ctx, attempt.RestoreID),
		)
	}
	return next, cleanupErr
}

func sweepLifecycleCleanupRoots(
	ctx context.Context,
	sqlExecutor executor.SQLExecutor,
	taeFS fileservice.FileService,
	faults lifecyclepkg.FaultInjector,
	reconcileCursor string,
) (string, error) {
	roots := lifecyclepkg.SQLCleanupRootRepository{Executor: sqlExecutor}
	temporary, err := roots.ListPublishedTemporary(ctx, 64)
	if err != nil {
		return reconcileCursor, err
	}
	reconcileable, nextCursor, _, err := roots.ListReconcileable(
		ctx,
		reconcileCursor,
		64,
	)
	if err != nil {
		return reconcileCursor, err
	}
	due, err := roots.ListSweepable(ctx, time.Now(), 64)
	if err != nil {
		return reconcileCursor, err
	}
	if len(due) == 0 &&
		len(temporary) == 0 &&
		len(reconcileable) == 0 {
		return nextCursor, nil
	}
	taeStore := lifecyclepkg.FileServiceArchiveStore{
		FileService:    taeFS,
		MaxListEntries: 100_000,
	}
	var sweepErr error
	for _, root := range temporary {
		cleaned, cleanupErr := lifecyclepkg.CleanupPublishedTemporary(
			ctx,
			roots,
			taeStore,
			root,
		)
		if cleanupErr != nil {
			root.LastError = cleanupErr.Error()
			_, updateErr := roots.UpdateCleanup(ctx, root, root.StateVersion)
			sweepErr = errors.Join(sweepErr, cleanupErr, updateErr)
			continue
		}
		if cleaned.Mode == lifecyclepkg.CleanupModeTTLRewrite {
			_, transitionErr := roots.Transition(
				ctx,
				cleaned.RootID,
				cleaned.AttemptID,
				cleaned.ExecutorEpoch,
				lifecyclepkg.CleanupRootPublished,
				cleaned.StateVersion,
				lifecyclepkg.CleanupRootDeletePending,
			)
			sweepErr = errors.Join(sweepErr, transitionErr)
		}
	}
	reconcileCatalog := lifecyclepkg.SQLCleanupReconcileCatalog{
		Executor: sqlExecutor,
	}
	reconciler := lifecyclepkg.CleanupReconciler{
		Roots:   roots,
		Catalog: reconcileCatalog,
	}
	now := time.Now()
	for _, root := range reconcileable {
		_, reconcileErr := reconciler.ReconcileOne(ctx, root, now)
		sweepErr = errors.Join(sweepErr, reconcileErr)
	}
	archiveServices := make([]fileservice.FileService, 0, len(due))
	defer func() {
		for _, archiveFS := range archiveServices {
			closeCtx, cancelClose := context.WithTimeout(
				context.WithoutCancel(ctx),
				lifecycleProtectionReleaseTimeout,
			)
			archiveFS.Close(closeCtx)
			cancelClose()
		}
	}()
	sweeper := lifecyclepkg.CleanupSweeper{
		Roots: roots,
		ResolveArchive: func(
			resolveCtx context.Context,
			root lifecyclepkg.CleanupRoot,
		) (lifecyclepkg.CleanupObjectStore, error) {
			if root.ArchivePrefix == "" {
				return nil, nil
			}
			target, parseErr := lifecyclepkg.ParseFrozenArchiveTarget(
				[]byte(root.ArchiveNamespace),
			)
			if parseErr != nil {
				return nil, parseErr
			}
			archiveFS, createErr := lifecyclepkg.NewArchiveFileService(
				resolveCtx,
				target,
			)
			if createErr != nil {
				return nil, createErr
			}
			archiveServices = append(archiveServices, archiveFS)
			return lifecyclepkg.FileServiceArchiveStore{
				FileService:    archiveFS,
				MaxListEntries: 100_000,
			}, nil
		},
		ResolveTAE: func(
			context.Context,
			lifecyclepkg.CleanupRoot,
		) (lifecyclepkg.CleanupObjectStore, error) {
			return taeStore, nil
		},
		FinalizePublication: reconcileCatalog.FinalizeCleanup,
		QuiescenceWindow:    10 * time.Minute,
		Faults:              faults,
	}
	now = time.Now()
	for _, root := range due {
		rootErr := func() error {
			rootCtx, cancelRoot := context.WithTimeout(
				ctx,
				lifecycleTemporaryCleanupTimeout,
			)
			defer cancelRoot()
			return sweeper.SweepOne(rootCtx, root.RootID, now)
		}()
		sweepErr = errors.Join(sweepErr, rootErr)
	}
	return nextCursor, sweepErr
}

func lifecycleTaskEpoch(scheduled taskpb.Task) uint64 {
	switch value := scheduled.(type) {
	case *taskpb.AsyncTask:
		return max(uint64(value.GetEpoch()), 1)
	default:
		return 1
	}
}

func (runner *lifecycleBindingExecutor) run(
	ctx context.Context,
	binding lifecyclepkg.Binding,
) (err error) {
	if runner.engine == nil ||
		runner.txnClient == nil ||
		runner.sqlExecutor == nil ||
		runner.taeFS == nil ||
		runner.now == nil {
		return fmt.Errorf("Lifecycle binding executor dependencies are incomplete")
	}
	archiveAction := strings.EqualFold(binding.Action, "ARCHIVE")
	deleteAction := strings.EqualFold(binding.Action, "DELETE")
	if !archiveAction && !deleteAction {
		return fmt.Errorf(
			"Lifecycle action %q is not enabled",
			binding.Action,
		)
	}
	if archiveAction && binding.PurgeAfterDays <= binding.ExpireAfterDays {
		return fmt.Errorf("Lifecycle Archive retention window is invalid")
	}
	var target lifecyclepkg.FrozenArchiveTarget
	var archiveFS fileservice.FileService
	if archiveAction {
		target, err = runner.release.ResolveArchiveTarget(
			ctx,
			binding.AccountID,
			binding.StageID,
			binding.StageIdentityDigest,
		)
		if err != nil {
			return err
		}
		archiveFS, err = lifecyclepkg.NewArchiveFileService(ctx, target)
		if err != nil {
			return err
		}
		defer func() {
			closeCtx, cancelClose := context.WithTimeout(
				context.WithoutCancel(ctx),
				lifecycleProtectionReleaseTimeout,
			)
			defer cancelClose()
			archiveFS.Close(closeCtx)
		}()
	}

	accountCtx := defines.AttachAccount(
		ctx,
		binding.AccountID,
		catalog.System_User,
		catalog.System_Role,
	)
	operator, err := runner.txnClient.New(
		accountCtx,
		runner.engine.LatestLogtailAppliedTime(),
		client.WithTxnCreateBy(
			binding.AccountID,
			"",
			"tae object lifecycle reader",
			0,
		),
	)
	if err != nil {
		return err
	}
	defer func() {
		rollbackCtx, cancelRollback := lifecycleRollbackContext(accountCtx)
		defer cancelRollback()
		err = errors.Join(
			err,
			operator.Rollback(rollbackCtx),
		)
	}()
	if err = runner.engine.New(accountCtx, operator); err != nil {
		return err
	}
	_, _, relation, err := runner.engine.GetRelationById(
		accountCtx,
		operator,
		binding.PhysicalTableID,
	)
	if err != nil {
		return err
	}
	table, ok := relation.(LifecycleTable)
	if !ok {
		return fmt.Errorf(
			"table %d does not expose Lifecycle capabilities",
			binding.PhysicalTableID,
		)
	}
	tableDef := relation.GetTableDef(accountCtx)
	if tableDef == nil || tableDef.TblId != binding.PhysicalTableID {
		return fmt.Errorf("Lifecycle table definition identity changed")
	}
	bindingSchemaDigest := lifecyclepkg.BindingSchemaDigest(tableDef)
	if !strings.EqualFold(
		binding.SchemaDigest,
		hex.EncodeToString(bindingSchemaDigest[:]),
	) {
		return fmt.Errorf("Lifecycle Binding schema fence changed")
	}
	schema, schemaDigest, err := lifecyclepkg.BuildSchemaDescriptor(
		accountCtx,
		tableDef,
	)
	if err != nil {
		return err
	}
	columnOrdinal, columnType, err := lifecycleColumn(
		tableDef,
		binding.LifecycleColumnID,
	)
	if err != nil {
		return err
	}
	evaluation := runner.now()
	cutoff, encodedCutoff, err := lifecycleCutoff(
		evaluation,
		binding.ExpireAfterDays,
		binding.LateArrivalGraceDays,
		binding.EvaluationTimezone,
		columnType,
	)
	if err != nil {
		return err
	}
	snapshot := types.TimestampToTS(operator.SnapshotTS())
	cursor := lifecycleDiscoveryCursor(binding)
	page, err := table.LifecycleDiscoverObjectPage(
		accountCtx,
		lifecyclepkg.DiscoveryRequest{
			Snapshot: snapshot,
			Now:      evaluation,
			Cursor:   cursor,
			Limits: lifecyclepkg.DiscoveryLimits{
				MaxObjects:   lifecycleDiscoveryPageObjects,
				MaxMetaBytes: lifecycleDiscoveryMetaBytes,
				MaxDuration:  30 * time.Second,
			},
		},
	)
	if err != nil {
		return err
	}
	binding, err = runner.pager.SaveCursor(accountCtx, binding, page.Next)
	if err != nil {
		return err
	}
	if len(page.Candidates) == 0 {
		return nil
	}

	var archiveStore lifecyclepkg.ArchiveStore
	if archiveAction {
		archiveStore = lifecyclepkg.FileServiceArchiveStore{
			FileService:    archiveFS,
			MaxListEntries: 100_000,
		}
	}
	processor := &LifecycleProcessor{
		Config: LifecycleProcessorConfig{
			TAENamespace:          "shared",
			MaxRestoreChunkRows:   65_536,
			MaxChunkBytes:         64 << 20,
			MaxActiveCleanupRoots: 4096,
			MaxActiveCleanupBytes: 64 << 40,
			CleanupGrace:          10 * time.Minute,
		},
		Roots: lifecyclepkg.SQLCleanupRootRepository{
			Executor: runner.sqlExecutor,
		},
		CleanupCapacity: lifecyclepkg.SQLCleanupRootRepository{
			Executor: runner.sqlExecutor,
		},
		Store: archiveStore,
		TemporaryStore: lifecyclepkg.FileServiceArchiveStore{
			FileService:    runner.taeFS,
			MaxListEntries: 100_000,
		},
		Protection: lifecyclepkg.SQLSyncProtectionClient{
			Executor:    runner.sqlExecutor,
			FileService: runner.taeFS,
			TaskID:      lifecyclepkg.CoordinatorTaskID,
		},
		RewriteAdmission: runner.admission,
		Finalizer: TxnLifecycleFinalCommitter{
			Engine:      runner.engine,
			TxnClient:   runner.txnClient,
			SQLExecutor: runner.sqlExecutor,
		},
		Faults: runner.faults,
	}
	planInputs := make([]lifecycleObjectPlanInput, 0, len(page.Candidates))
	for _, candidate := range page.Candidates {
		source := candidate.Source
		whole, notYetExpired := lifecycleObjectExpirationByZoneMap(
			source.ObjectStats,
			table.LifecycleSortKeyOrdinal(),
			columnOrdinal,
			columnType,
			encodedCutoff,
		)
		if notYetExpired {
			continue
		}
		planInputs = append(planInputs, lifecycleObjectPlanInput{
			Source: source,
			Whole:  whole,
		})
	}
	for _, objectPlan := range planLifecycleObjectTasks(planInputs) {
		var maxCreated uint32
		var deltaRows uint64
		var deltaBytes uint64
		var deltaBlocks uint32
		if !objectPlan.Whole {
			source := objectPlan.Sources[0]
			maxCreated = uint32(math.Ceil(
				float64(lifecycleObjectPressureBytes(source.ObjectStats))/
					float64(lifecycleTargetObjectBytes),
			)) + 1
			maxCreated = min(maxCreated, lifecycleMaxCreatedObjects)
			deltaRows = 100_000
			deltaBytes = 32 << 20
			deltaBlocks = source.ObjectStats.BlkCnt()
		}
		objectTask := LifecycleObjectTask{
			Binding:             binding,
			Table:               table,
			Sources:             objectPlan.Sources,
			SourceSnapshot:      snapshot,
			Schema:              schema,
			SchemaDigest:        schemaDigest,
			BindingSchemaDigest: bindingSchemaDigest,
			Classifier: lifecyclepkg.ExpirationClassifier{
				ColumnOrdinal: columnOrdinal,
				ColumnType:    columnType,
				Cutoff:        encodedCutoff,
			}.Classify,
			Whole:                      objectPlan.Whole,
			Cutoff:                     cutoff,
			Now:                        evaluation,
			Deadline:                   evaluation.Add(30 * time.Minute),
			ExecutorEpoch:              runner.epoch,
			TargetObjectSize:           lifecycleTargetObjectBytes,
			MaxCreatedObjects:          maxCreated,
			MaxCertifiedBlockReadBytes: lifecycleMaxCertifiedBlockReadBytes,
			DeltaRows:                  deltaRows,
			DeltaBytes:                 deltaBytes,
			DeltaBlocks:                deltaBlocks,
			ProtectionLimits: logtailreplay.LifecycleTombstoneSelectionLimits{
				MaxScannedObjects:  10_000,
				MaxSelectedObjects: 1_024,
				MaxMetaBytes:       64 << 20,
			},
		}
		processErr := func() error {
			releaseRewrite := func() {}
			if !objectPlan.Whole {
				var acquireErr error
				releaseRewrite, acquireErr =
					tryAcquireLifecycleRewriteSlot(
						accountCtx,
						runner.rewriteSlots,
					)
				if acquireErr != nil {
					return acquireErr
				}
			}
			defer releaseRewrite()
			if archiveAction {
				objectTask.ArchiveTarget = target
				objectTask.DatasetID = uuid.NewString()
				objectTask.PurgeAfter =
					time.Duration(binding.PurgeAfterDays) * 24 * time.Hour
				_, processErr := processor.ProcessArchiveObject(
					accountCtx,
					objectTask,
				)
				return processErr
			}
			objectTask.ReceiptID = uuid.NewString()
			_, processErr := processor.ProcessTTLObject(
				accountCtx,
				objectTask,
			)
			return processErr
		}()
		if processErr != nil {
			return processErr
		}
		operation := "ttl_whole"
		if archiveAction {
			operation = "archive_whole"
		}
		if !objectPlan.Whole {
			if archiveAction {
				operation = "archive_rewrite"
			} else {
				operation = "ttl_rewrite"
			}
		}
		metricv2.LifecycleObjectCounter.WithLabelValues(operation).Add(
			float64(len(objectPlan.Sources)),
		)
		metricv2.LifecycleBytesCounter.WithLabelValues(
			"retired_source",
		).Add(float64(objectPlan.SourceBytes))
		// The final Binding fence increments the same row version.
		binding.Version++
	}
	return nil
}

// tryAcquireLifecycleRewriteSlot is a CN-local Scheduler guard. It applies
// only to Lifecycle Mixed Rewrite and never enters the TN, ordinary Merge, or
// transaction paths. Fail-fast admission avoids keeping a read transaction
// open while waiting; a later bounded metadata scan may retry the Object.
func tryAcquireLifecycleRewriteSlot(
	ctx context.Context,
	slots chan struct{},
) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if slots == nil || cap(slots) == 0 {
		return nil, fmt.Errorf(
			"Lifecycle Rewrite concurrency is not configured",
		)
	}
	select {
	case slots <- struct{}{}:
		var once sync.Once
		return func() {
			once.Do(func() {
				<-slots
			})
		}, nil
	default:
		metricv2.LifecycleResourceRejectionCounter.WithLabelValues(
			"rewrite_concurrency",
		).Inc()
		return nil, fmt.Errorf(
			"RESOURCE_BLOCKED: Lifecycle Rewrite concurrency is exhausted",
		)
	}
}

func lifecycleColumn(
	table *plan.TableDef,
	columnID uint64,
) (int, types.T, error) {
	ordinal := 0
	for _, column := range table.Cols {
		if column == nil || column.Hidden {
			continue
		}
		if column.ColId == columnID {
			oid := types.T(column.Typ.Id)
			switch oid {
			case types.T_date, types.T_datetime, types.T_timestamp:
				return ordinal, oid, nil
			default:
				return 0, 0, fmt.Errorf(
					"Lifecycle column type %s is no longer supported",
					oid,
				)
			}
		}
		ordinal++
	}
	return 0, 0, fmt.Errorf("Lifecycle column %d no longer exists", columnID)
}

func lifecycleCutoff(
	evaluation time.Time,
	expireDays uint32,
	graceDays uint32,
	timezone string,
	columnType types.T,
) (time.Time, int64, error) {
	if evaluation.IsZero() || expireDays == 0 {
		return time.Time{}, 0, fmt.Errorf("Lifecycle cutoff input is incomplete")
	}
	location, err := time.LoadLocation(timezone)
	if err != nil {
		return time.Time{}, 0, err
	}
	localCutoff := evaluation.In(location).AddDate(
		0,
		0,
		-int(expireDays)-int(graceDays),
	)
	switch columnType {
	case types.T_date:
		year, month, day := localCutoff.Date()
		return localCutoff, int64(types.DateFromCalendar(
			int32(year),
			uint8(month),
			uint8(day),
		)), nil
	case types.T_datetime:
		year, month, day := localCutoff.Date()
		hour, minute, second := localCutoff.Clock()
		return localCutoff, int64(types.DatetimeFromClock(
			int32(year),
			uint8(month),
			uint8(day),
			uint8(hour),
			uint8(minute),
			uint8(second),
			uint32(localCutoff.Nanosecond()/1_000),
		)), nil
	case types.T_timestamp:
		return localCutoff, int64(types.UnixNanoToTimestamp(
			localCutoff.UTC().UnixNano(),
		)), nil
	default:
		return time.Time{}, 0, fmt.Errorf(
			"unsupported Lifecycle column type %s",
			columnType,
		)
	}
}

// lifecycleObjectExpirationByZoneMap returns whole=true only when the
// lifecycle column is the physical sort key and max < cutoff. skip=true only
// when min >= cutoff proves there is no expired row.
func lifecycleObjectExpirationByZoneMap(
	stats objectio.ObjectStats,
	sortKeyOrdinal int,
	columnOrdinal int,
	columnType types.T,
	cutoff int64,
) (whole bool, skip bool) {
	if sortKeyOrdinal != columnOrdinal {
		return false, false
	}
	zoneMap := stats.SortKeyZoneMap()
	if !zoneMap.IsInited() ||
		zoneMap.GetType() != columnType ||
		zoneMap.MaxTruncated() {
		return false, false
	}
	minimum, minOK := lifecycleTemporalValue(zoneMap.GetMin())
	maximum, maxOK := lifecycleTemporalValue(zoneMap.GetMax())
	if !minOK || !maxOK {
		return false, false
	}
	return maximum < cutoff, minimum >= cutoff
}

func lifecycleTemporalValue(value any) (int64, bool) {
	switch typed := value.(type) {
	case types.Date:
		return int64(typed), true
	case types.Datetime:
		return int64(typed), true
	case types.Timestamp:
		return int64(typed), true
	default:
		return 0, false
	}
}

func lifecycleDiscoveryCursor(
	binding lifecyclepkg.Binding,
) lifecyclepkg.DiscoveryCursor {
	cursor := lifecyclepkg.DiscoveryCursor{Wrapped: binding.ScanWrapped}
	if encoded, err := hex.DecodeString(binding.ScanSnapshotHex); err == nil &&
		len(encoded) == len(cursor.Snapshot) {
		copy(cursor.Snapshot[:], encoded)
	}
	if encoded, err := hex.DecodeString(binding.ScanLastObjectNameHex); err == nil &&
		len(encoded) == objectio.ObjectNameShortLen {
		copy(cursor.LastObjectName[:], encoded)
		cursor.HasLastObject = true
	}
	return cursor
}

func lifecycleSchemaDigestString(value [sha256.Size]byte) string {
	return hex.EncodeToString(value[:])
}
