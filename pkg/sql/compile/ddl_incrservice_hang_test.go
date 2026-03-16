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

package compile

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// TestCreateTable_IncrserviceHangAfterRetry reproduces the production bug where:
//
// 1. CREATE TABLE acquires lock on mo_tables
// 2. lockMoTable returns ErrTxnNeedRetryWithDefChanged (concurrent DDL changed table def)
// 3. Compile.Run retries: rollback workspace, rebuild plan, re-execute CreateTable
// 4. On retry, CreateTable succeeds up to maybeCreateAutoIncrement
// 5. maybeCreateAutoIncrement -> incrservice.Create -> newColumnCache -> preAllocate -> asyncAllocate
// 6. asyncAllocate RPC to TN hangs forever (TN slow/stuck)
// 7. waitPrevAllocatingLocked blocks indefinitely
// 8. txn leaks, locks never released, cascade DDL failures
//
// The bug: asyncAllocate has no timeout protection. If TN doesn't respond,
// the entire CREATE TABLE hangs until client cancels the context.
//
// This test verifies that context cancellation properly unblocks the hang.
func TestCreateTable_IncrserviceHangAfterRetry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.Buf = buffer.New()

	// Use a cancellable context to simulate client disconnect (like DBeaver timeout)
	ctx, cancel := context.WithCancel(defines.AttachAccountId(context.Background(), sysAccountId))
	defer cancel()

	proc.Ctx = ctx
	txnCli, txnOp := newTestTxnClientAndOp(ctrl)
	proc.Base.TxnClient = txnCli
	proc.Base.TxnOperator = txnOp
	proc.ReplaceTopCtx(ctx)

	// --- Mock engine setup ---
	relation := mock_frontend.NewMockRelation(ctrl)
	relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(2777462)).AnyTimes()

	mockDb := mock_frontend.NewMockDatabase(ctrl)
	mockDb.EXPECT().Relation(gomock.Any(), catalog.MO_DATABASE, gomock.Any()).Return(relation, nil).AnyTimes()
	mockDb.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil).AnyTimes()
	mockDb.EXPECT().RelationExists(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()
	mockDb.EXPECT().Create(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().HasTempEngine().Return(false).AnyTimes()
	eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockDb, nil).AnyTimes()

	// --- Stub lockMoDatabase: always succeed ---
	lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
		return nil
	})
	defer lockMoDb.Reset()

	// --- Stub lockMoTable: succeed (the retry already happened at Compile.Run level) ---
	lockMoTbl := gostub.Stub(&lockMoTable, func(_ *Compile, _ string, _ string, _ lock.LockMode) error {
		return nil
	})
	defer lockMoTbl.Reset()

	// --- Stub checkIndexInitializable: skip index building ---
	checkIndexInit := gostub.Stub(&checkIndexInitializable, func(_ string, _ string) bool {
		return false
	})
	defer checkIndexInit.Reset()

	// --- Key: Stub maybeCreateAutoIncrement to simulate the hang ---
	// In production, this hangs inside incrservice.Create() -> newColumnCache ->
	// preAllocate -> asyncAllocate -> waitPrevAllocatingLocked
	// because the asyncAllocate RPC to TN never returns.
	var hangStarted atomic.Bool
	createAutoIncrement := gostub.Stub(&maybeCreateAutoIncrement,
		func(innerCtx context.Context, _ string, _ engine.Database, _ *plan.TableDef, _ client.TxnOperator, _ func() string) error {
			hangStarted.Store(true)
			t.Log("maybeCreateAutoIncrement: simulating asyncAllocate hang (waiting for context cancel)...")

			// Simulate the exact behavior of waitPrevAllocatingLocked:
			// it blocks on a channel, only unblocked by ctx.Done()
			<-innerCtx.Done()
			t.Log("maybeCreateAutoIncrement: context canceled, returning error")
			return innerCtx.Err()
		})
	defer createAutoIncrement.Reset()

	// --- Build the CREATE TABLE plan ---
	tableDef := &plan.TableDef{
		Name: "staff_info",
		Cols: []*plan2.ColDef{
			{
				Name: "id",
				Alg:  plan2.CompressType_Lz4,
				Typ: plan2.Type{
					Id:          22, // INT32
					NotNullable: true,
					AutoIncr:    true,
					Width:       32,
					Scale:       -1,
				},
			},
			{
				Name: "name",
				Alg:  plan2.CompressType_Lz4,
				Typ: plan2.Type{
					Id:    25, // VARCHAR
					Width: 100,
				},
			},
		},
	}

	s := &Scope{
		Plan: &plan.Plan{
			Plan: &plan2.Plan_Ddl{
				Ddl: &plan2.DataDefinition{
					Definition: &plan2.DataDefinition_CreateTable{
						CreateTable: &plan2.CreateTable{
							IfNotExists: false,
							Database:    "test_db",
							TableDef:    tableDef,
						},
					},
				},
			},
		},
		Proc: proc,
	}

	sql := "CREATE TABLE test_db.staff_info (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100))"
	c := NewCompile("test", "test_db", sql, "", "", eng, proc, nil, false, nil, time.Now())

	// --- Run CreateTable in a goroutine (it will hang) ---
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.CreateTable(c)
	}()

	// Wait for the hang to start
	deadline := time.After(5 * time.Second)
	for !hangStarted.Load() {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for maybeCreateAutoIncrement to be called")
		case err := <-errCh:
			// If it returned early, that's also fine — check the error
			t.Fatalf("CreateTable returned unexpectedly: %v", err)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	t.Log("Confirmed: CreateTable is hanging in maybeCreateAutoIncrement (simulating asyncAllocate RPC hang)")
	t.Log("In production, this txn would leak, holding locks indefinitely.")
	t.Log("The leak checker would log 'found leak txn' every 20s but cannot force-abort.")

	// Simulate client disconnect (DBeaver cancel / connection pool reclaim)
	t.Log("Simulating client context cancellation...")
	cancel()

	// CreateTable should now return with context.Canceled
	select {
	case err := <-errCh:
		assert.Error(t, err)
		t.Logf("CreateTable returned after cancel: %v", err)
		// The error should be context.Canceled (propagated from waitPrevAllocatingLocked)
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("BUG: CreateTable did not return after context cancellation — " +
			"this means the hang is not properly unblocked by context cancel")
	}
}
