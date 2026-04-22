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

package gc

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/stretchr/testify/require"
)

func TestAppendValToBatchForObjectListBatchPreservesFTSSidecarColumns(t *testing.T) {
	src := ckputil.NewObjectListBatch()
	dst := ckputil.NewObjectListBatch()
	defer src.Clean(common.CheckpointAllocator)
	defer dst.Clean(common.CheckpointAllocator)

	var stats objectio.ObjectStats
	objectName := objectio.MockObjectName()
	objectio.SetObjectStatsObjectName(&stats, objectName)
	objectio.SetObjectStatsSize(&stats, 64)

	packer := types.NewPacker()
	defer packer.Close()
	ckputil.EncodeCluser(packer, 42, ckputil.ObjectType_FTSSidecar, objectName.ObjectId(), false)

	require.NoError(t, vector.AppendFixed(src.Vecs[ckputil.TableObjectsAttr_Accout_Idx], uint32(1), false, common.CheckpointAllocator))
	require.NoError(t, vector.AppendFixed(src.Vecs[ckputil.TableObjectsAttr_DB_Idx], uint64(7), false, common.CheckpointAllocator))
	require.NoError(t, vector.AppendFixed(src.Vecs[ckputil.TableObjectsAttr_Table_Idx], uint64(42), false, common.CheckpointAllocator))
	require.NoError(t, vector.AppendFixed(src.Vecs[ckputil.TableObjectsAttr_ObjectType_Idx], ckputil.ObjectType_FTSSidecar, false, common.CheckpointAllocator))
	require.NoError(t, vector.AppendBytes(src.Vecs[ckputil.TableObjectsAttr_ID_Idx], stats[:], false, common.CheckpointAllocator))
	require.NoError(t, vector.AppendFixed(src.Vecs[ckputil.TableObjectsAttr_CreateTS_Idx], types.BuildTS(10, 0), false, common.CheckpointAllocator))
	require.NoError(t, vector.AppendFixed(src.Vecs[ckputil.TableObjectsAttr_DeleteTS_Idx], types.TS{}, false, common.CheckpointAllocator))
	require.NoError(t, vector.AppendBytes(src.Vecs[ckputil.TableObjectsAttr_Cluster_Idx], packer.Bytes(), false, common.CheckpointAllocator))
	require.NoError(t, vector.AppendBytes(src.Vecs[ckputil.TableObjectsAttr_FTSIndexTable_Idx], []byte("__idx_body"), false, common.CheckpointAllocator))
	require.NoError(t, vector.AppendBytes(src.Vecs[ckputil.TableObjectsAttr_FTSSidecarPath_Idx], []byte("obj.fts.__idx_body"), false, common.CheckpointAllocator))
	require.NoError(t, vector.AppendBytes(src.Vecs[ckputil.TableObjectsAttr_FTSLocatorPath_Idx], []byte("obj.fts.locator"), false, common.CheckpointAllocator))
	require.NoError(t, vector.AppendFixed(src.Vecs[ckputil.TableObjectsAttr_FTSSegmentVersion_Idx], uint32(4), false, common.CheckpointAllocator))
	require.NoError(t, vector.AppendFixed(src.Vecs[ckputil.TableObjectsAttr_FTSDocCount_Idx], int64(9), false, common.CheckpointAllocator))
	require.NoError(t, vector.AppendFixed(src.Vecs[ckputil.TableObjectsAttr_FTSFlags_Idx], uint16(3), false, common.CheckpointAllocator))
	src.SetRowCount(1)

	appendValToBatchForObjectListBatch(src, dst, 0, common.CheckpointAllocator)

	require.Equal(t, 1, dst.RowCount())
	require.Equal(t, ckputil.ObjectType_FTSSidecar, vector.GetFixedAtNoTypeCheck[int8](dst.Vecs[ckputil.TableObjectsAttr_ObjectType_Idx], 0))
	require.Equal(t, "__idx_body", dst.Vecs[ckputil.TableObjectsAttr_FTSIndexTable_Idx].GetStringAt(0))
	require.Equal(t, "obj.fts.__idx_body", dst.Vecs[ckputil.TableObjectsAttr_FTSSidecarPath_Idx].GetStringAt(0))
	require.Equal(t, "obj.fts.locator", dst.Vecs[ckputil.TableObjectsAttr_FTSLocatorPath_Idx].GetStringAt(0))
	require.Equal(t, uint32(4), vector.GetFixedAtNoTypeCheck[uint32](dst.Vecs[ckputil.TableObjectsAttr_FTSSegmentVersion_Idx], 0))
	require.Equal(t, int64(9), vector.GetFixedAtNoTypeCheck[int64](dst.Vecs[ckputil.TableObjectsAttr_FTSDocCount_Idx], 0))
	require.Equal(t, uint16(3), vector.GetFixedAtNoTypeCheck[uint16](dst.Vecs[ckputil.TableObjectsAttr_FTSFlags_Idx], 0))
}
