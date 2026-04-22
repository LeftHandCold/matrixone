// Copyright 2025 Matrix Origin
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

package rpc

import (
	"context"
	"testing"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	ftnative "github.com/matrixorigin/matrixone/pkg/fulltext/native"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
)

func TestFTSInspectReconcile(t *testing.T) {
	ftnative.ResetRuntimeSidecarRegistry()
	defer ftnative.ResetRuntimeSidecarRegistry()

	handle := mockTAEHandle(context.Background(), t, &options.Options{})
	ctx := context.Background()
	asyncTxn, err := handle.db.StartTxn(nil)
	require.NoError(t, err)

	database, err := testutil.CreateDatabase2(ctx, asyncTxn, "db1")
	require.NoError(t, err)
	schema := newFTSInspectSchema(t, "docs")
	table, err := testutil.CreateRelation2(ctx, asyncTxn, database, schema)
	require.NoError(t, err)

	objectVec := containers.NewVector(types.T_varchar.ToType())
	obj1Stats := newInspectableObjectStats()
	obj2Stats := newInspectableObjectStats()
	objectVec.Append(obj1Stats.Marshal(), false)
	objectVec.Append(obj2Stats.Marshal(), false)
	require.NoError(t, table.AddDataFiles(ctx, objectVec))
	require.NoError(t, asyncTxn.Commit(ctx))

	fs := handle.db.Runtime.Fs
	published1 := writeInspectSidecar(t, ctx, fs, schema, obj1Stats.ObjectName())
	require.Len(t, published1, 1)
	published2 := writeInspectSidecar(t, ctx, fs, schema, obj2Stats.ObjectName())
	require.Len(t, published2, 1)
	ftnative.PublishRuntimeSidecars(table.GetMeta().(*catalog.TableEntry).ID, obj2Stats.ObjectName().String(), published2)
	require.NoError(t, fs.Delete(ctx, ftnative.SidecarLocatorPath(obj2Stats.ObjectName().String())))
	require.NoError(t, ftnative.WriteSidecarLocator(ctx, fs, obj2Stats.ObjectName().String(), []ftnative.SidecarLocatorEntry{{
		IndexTable: "__idx_body",
		FilePath:   published2[0].SidecarPath + ".stale",
	}}))

	showResp, err := handle.runInspectCmd("fts show -t db1.docs")
	require.NoError(t, err)
	require.Contains(t, string(showResp.Payload), "missing_registry=1")
	require.Contains(t, string(showResp.Payload), "locator_mismatch=1")
	require.Contains(t, string(showResp.Payload), "repairable_registry_backfill=1")
	require.Contains(t, string(showResp.Payload), "repairable_locator_rewrite=1")

	reconcileResp, err := handle.runInspectCmd("fts reconcile -t db1.docs")
	require.NoError(t, err)
	require.Contains(t, string(reconcileResp.Payload), "registry_backfilled_objects=1")
	require.Contains(t, string(reconcileResp.Payload), "locator_rewritten_objects=1")
	require.Contains(t, string(reconcileResp.Payload), "post_repair:")
	require.Contains(t, string(reconcileResp.Payload), "missing_registry=0")
	require.Contains(t, string(reconcileResp.Payload), "locator_mismatch=0")

	registry1, ok := ftnative.LookupRuntimeSidecars(obj1Stats.ObjectName().String())
	require.True(t, ok)
	require.Contains(t, registry1.Entries, "__idx_body")
	require.Equal(t, int64(2), registry1.Entries["__idx_body"].DocCount)
	require.NotZero(t, registry1.Entries["__idx_body"].Flags&ftnative.SidecarFlagLocatorWritten)

	locator2, ok, err := ftnative.ReadSidecarLocator(ctx, fs, obj2Stats.ObjectName().String())
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, locator2.Entries, 1)
	require.Equal(t, published2[0].SidecarPath, locator2.Entries[0].FilePath)
}

func TestFTSInspectRepairLocatorOnly(t *testing.T) {
	ftnative.ResetRuntimeSidecarRegistry()
	defer ftnative.ResetRuntimeSidecarRegistry()

	handle := mockTAEHandle(context.Background(), t, &options.Options{})
	ctx := context.Background()
	asyncTxn, err := handle.db.StartTxn(nil)
	require.NoError(t, err)

	database, err := testutil.CreateDatabase2(ctx, asyncTxn, "db1")
	require.NoError(t, err)
	schema := newFTSInspectSchema(t, "docs")
	table, err := testutil.CreateRelation2(ctx, asyncTxn, database, schema)
	require.NoError(t, err)

	objectVec := containers.NewVector(types.T_varchar.ToType())
	objStats := newInspectableObjectStats()
	objectVec.Append(objStats.Marshal(), false)
	require.NoError(t, table.AddDataFiles(ctx, objectVec))
	require.NoError(t, asyncTxn.Commit(ctx))

	fs := handle.db.Runtime.Fs
	published := writeInspectSidecar(t, ctx, fs, schema, objStats.ObjectName())
	ftnative.PublishRuntimeSidecars(table.GetMeta().(*catalog.TableEntry).ID, objStats.ObjectName().String(), published)
	require.NoError(t, fs.Delete(ctx, ftnative.SidecarLocatorPath(objStats.ObjectName().String())))
	require.NoError(t, ftnative.WriteSidecarLocator(ctx, fs, objStats.ObjectName().String(), []ftnative.SidecarLocatorEntry{{
		IndexTable: "__idx_body",
		FilePath:   published[0].SidecarPath + ".stale",
	}}))

	resp, err := handle.runInspectCmd("fts repair -t db1.docs --mode locator")
	require.NoError(t, err)
	require.Contains(t, string(resp.Payload), "mode=locator")
	require.Contains(t, string(resp.Payload), "registry_backfilled_objects=0")
	require.Contains(t, string(resp.Payload), "locator_rewritten_objects=1")

	locator, ok, err := ftnative.ReadSidecarLocator(ctx, fs, objStats.ObjectName().String())
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, published[0].SidecarPath, locator.Entries[0].FilePath)
}

func newFTSInspectSchema(t *testing.T, name string) *catalog.Schema {
	schema := catalog.NewEmptySchema(name)
	require.NoError(t, schema.AppendPKCol("id", types.T_int64.ToType(), 0))
	require.NoError(t, schema.AppendCol("body", types.T_varchar.ToType()))
	cstrDef := &engine.ConstraintDef{
		Cts: []engine.Constraint{
			&engine.PrimaryKeyDef{
				Pkey: &plan.PrimaryKeyDef{
					PkeyColName: "id",
					Names:       []string{"id"},
				},
			},
			&engine.IndexDef{
				Indexes: []*plan.IndexDef{{
					IndexName:       "idx_body",
					IndexTableName:  "__idx_body",
					IndexAlgo:       pkgcatalog.MOIndexFullTextAlgo.ToString(),
					Parts:           []string{"body"},
					IndexAlgoParams: `{"parser":"default"}`,
				}},
			},
		},
	}
	var err error
	schema.Constraint, err = cstrDef.MarshalBinary()
	require.NoError(t, err)
	require.NoError(t, schema.Finalize(false))
	return schema
}

func newInspectableObjectStats() objectio.ObjectStats {
	id := objectio.NewObjectid()
	return *objectio.NewObjectStatsWithObjectID(&id, false, true, false)
}

func writeInspectSidecar(
	t *testing.T,
	ctx context.Context,
	fs fileservice.FileService,
	schema *catalog.Schema,
	objName objectio.ObjectName,
) []ftnative.PublishedSidecar {
	indexer, err := ftnative.NewObjectIndexer(schema)
	require.NoError(t, err)

	mp := mpool.MustNewZero()
	idVec := vector.NewVec(types.T_int64.ToType())
	bodyVec := vector.NewVec(types.T_varchar.ToType())
	defer idVec.Free(mp)
	defer bodyVec.Free(mp)
	require.NoError(t, vector.AppendFixed[int64](idVec, 1, false, mp))
	require.NoError(t, vector.AppendFixed[int64](idVec, 2, false, mp))
	require.NoError(t, vector.AppendBytes(bodyVec, []byte("native inspect repair"), false, mp))
	require.NoError(t, vector.AppendBytes(bodyVec, []byte("repair locator registry"), false, mp))

	bat := batch.NewWithSize(2)
	bat.Attrs = []string{"id", "body"}
	bat.Vecs[0] = idVec
	bat.Vecs[1] = bodyVec
	bat.SetRowCount(2)

	require.NoError(t, indexer.AddBatch(bat, []uint32{2}))
	published, err := indexer.Write(ctx, fs, objName, 2)
	require.NoError(t, err)
	return published
}
