//go:build manual

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

package test

import (
	"context"
	"testing"
	"time"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/stretchr/testify/require"
)

func TestManualCompositePKDuplicateAfterMerge(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)

	ctx := context.Background()
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	schema := newManualCompositePKSchema(t, "manual_cpkey_duplicate_after_merge")
	tae.BindSchema(schema)

	bat := buildManualCompositePKBatch(schema, 200)
	defer bat.Close()
	tae.CreateRelAndAppend(bat, true)

	targetWarehouse := int32(0)
	targetItem := int32(42)
	targetCPKey := mustPackCompositeInt32Key(targetWarehouse, targetItem)
	filter := handle.NewEQFilter(targetCPKey)

	fault.Enable()
	defer fault.Disable()

	waitKey := objectio.FJ_TxnFreezeBeforeDedup
	waitersKey := t.Name() + "/getwaiters"
	notifyKey := t.Name() + "/notify"
	require.NoError(t, fault.AddFaultPoint(ctx, waitKey, "1:1::", "wait", 0, "", false))
	defer func() {
		_, err := fault.RemoveFaultPoint(ctx, waitKey)
		require.NoError(t, err)
	}()
	require.NoError(t, fault.AddFaultPoint(ctx, waitersKey, ":::", "getwaiters", 0, waitKey, false))
	defer func() {
		_, err := fault.RemoveFaultPoint(ctx, waitersKey)
		require.NoError(t, err)
	}()
	rmNotify, err := objectio.InjectNotify(notifyKey, waitKey)
	require.NoError(t, err)
	defer rmNotify()

	txn := mustStartTxn(t, tae, 0)
	txn.SetDedupType(txnif.DedupPolicy_CheckIncremental)
	rel := tae.GetRelationWithTxn(txn)
	require.NoError(t, rel.UpdateByFilter(ctx, filter, uint16(schema.GetColIdx("quantity")), int32(9999), false))

	errCh := make(chan error, 1)
	go func() {
		errCh <- txn.Commit(ctx)
	}()

	waitForFaultWaiter(t, waitersKey)
	tae.CompactBlocks(false)
	tae.MergeBlocks(false)
	objectio.NotifyInjected(notifyKey)

	err = <-errCh
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry), "expected duplicate entry, got %v", err)
}

func newManualCompositePKSchema(t *testing.T, name string) *catalog.Schema {
	t.Helper()

	schema := catalog.NewEmptySchema(name)
	constraintDef := &engine.ConstraintDef{
		Cts: make([]engine.Constraint, 0, 1),
	}

	require.NoError(t, schema.AppendCol("warehouse", types.T_int32.ToType()))
	schema.ColDefs[len(schema.ColDefs)-1].NullAbility = false

	require.NoError(t, schema.AppendCol("item", types.T_int32.ToType()))
	schema.ColDefs[len(schema.ColDefs)-1].NullAbility = false

	require.NoError(t, schema.AppendCol("quantity", types.T_int32.ToType()))
	schema.ColDefs[len(schema.ColDefs)-1].NullAbility = false

	cpkeyType := types.T_varchar.ToType()
	cpkeyType.Width = types.MaxVarcharLen
	require.NoError(t, schema.AppendColDef(&catalog.ColDef{
		Name:        pkgcatalog.CPrimaryKeyColName,
		Type:        cpkeyType,
		Hidden:      true,
		Primary:     true,
		SortKey:     true,
		SortIdx:     0,
		NullAbility: false,
	}))

	constraintDef.Cts = append(constraintDef.Cts, &engine.PrimaryKeyDef{
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"warehouse", "item"},
			PkeyColName: pkgcatalog.CPrimaryKeyColName,
			CompPkeyCol: &plan.ColDef{
				Name:   pkgcatalog.CPrimaryKeyColName,
				Hidden: true,
				Typ: plan.Type{
					Id:    int32(types.T_varchar),
					Width: types.MaxVarcharLen,
				},
				Default: &plan.Default{},
			},
		},
	})
	schema.Constraint, _ = constraintDef.MarshalBinary()
	schema.Extra.BlockMaxRows = 10
	schema.Extra.ObjectMaxBlocks = 10
	require.NoError(t, schema.Finalize(false))
	return schema
}

func buildManualCompositePKBatch(schema *catalog.Schema, rows int) *containers.Batch {
	bat := containers.NewBatchWithCapacity(4)
	warehouseVec := containers.MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
	itemVec := containers.MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
	quantityVec := containers.MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
	cpkeyVec := containers.MakeVector(schema.GetPrimaryKey().Type, common.DefaultAllocator)
	for row := 0; row < rows; row++ {
		warehouse := int32(row / 100)
		item := int32(row)
		quantity := int32(row * 10)
		warehouseVec.Append(warehouse, false)
		itemVec.Append(item, false)
		quantityVec.Append(quantity, false)
		cpkeyVec.Append(mustPackCompositeInt32Key(warehouse, item), false)
	}
	bat.AddVector("warehouse", warehouseVec)
	bat.AddVector("item", itemVec)
	bat.AddVector("quantity", quantityVec)
	bat.AddVector(pkgcatalog.CPrimaryKeyColName, cpkeyVec)
	return bat
}

func mustPackCompositeInt32Key(v1, v2 int32) []byte {
	packer := types.NewPacker()
	defer packer.Close()
	packer.EncodeInt32(v1)
	packer.EncodeInt32(v2)
	return packer.Bytes()
}

func waitForFaultWaiter(t *testing.T, waitersKey string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		waiters, _, ok := fault.TriggerFault(waitersKey)
		if ok && waiters > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("fault waiter %q was not reached", waitersKey)
}
