package blockio

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path"
	"testing"
)

const (
	ModuleName = "BlockIO"
)

func newBatch() *batch.Batch {
	mp := mpool.MustNewZero()
	types := []types.Type{
		{Oid: types.T_int32},
		{Oid: types.T_int16},
		{Oid: types.T_int32},
		{Oid: types.T_int64},
		{Oid: types.T_uint16},
		{Oid: types.T_uint32},
		{Oid: types.T_uint8},
		{Oid: types.T_uint64},
	}
	return testutil.NewBatch(types, false, int(40000*2), mp)
}

func TestWriter_WriteBlockAndZoneMap(t *testing.T) {
	defer testutils.AfterTest(t)()
	dir := testutils.InitTestEnv(ModuleName, t)
	dir = path.Join(dir, "/local")
	id := 1
	name := fmt.Sprintf("%d.blk", id)
	bat := newBatch()
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: dir,
	}
	service, err := fileservice.NewFileService(c)
	assert.Nil(t, err)
	fs := objectio.NewObjectFS(service, "local")
	writer := NewWriter(context.Background(), fs, name)
	idxs := make([]uint16, 3)
	idxs[0] = 0
	idxs[1] = 2
	idxs[2] = 4
	_, err = writer.WriteBlockAndZoneMap(bat, idxs)
	assert.Nil(t, err)
	blocks, err := writer.Sync()
	assert.Equal(t, 1, len(blocks))
	fd := blocks[0]
	col, err := fd.GetColumn(0)
	assert.Nil(t, err)
	zm := index.NewZoneMap(bat.Vecs[0].Typ)
	colZoneMap := col.GetMeta().GetZoneMap()

	err = zm.Unmarshal(colZoneMap.GetData())
	require.NoError(t, err)
	res := zm.Contains(int32(500))
	require.True(t, res)
	res = zm.Contains(int32(79999))
	require.True(t, res)
	res = zm.Contains(int32(100000))
	require.False(t, res)
}
