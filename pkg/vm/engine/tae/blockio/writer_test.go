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

package blockio

import (
	"bufio"
	"context"
	"github.com/FastFilter/xorfilter"
	"github.com/cespare/xxhash/v2"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"io"
	"math/rand"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	ModuleName = "BlockIO"
)

func TestWriter_WriteBlockAndZoneMap(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	dir := testutils.InitTestEnv(ModuleName, t)
	dir = path.Join(dir, "/local")
	name := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: dir,
	}
	service, err := fileservice.NewFileService(ctx, c, nil)
	assert.Nil(t, err)
	writer, _ := NewBlockWriterNew(service, name, 0, nil)

	schema := catalog.MockSchemaAll(13, 2)
	bats := catalog.MockBatch(schema, 40000*2).Split(2)

	_, err = writer.WriteBatch(containers.ToCNBatch(bats[0]))
	assert.Nil(t, err)
	_, err = writer.WriteBatch(containers.ToCNBatch(bats[1]))
	assert.Nil(t, err)
	blocks, _, err := writer.Sync(context.Background())
	assert.Nil(t, err)
	assert.Equal(t, 2, len(blocks))
	fd := blocks[0]
	col := fd.MustGetColumn(2)
	colZoneMap := col.ZoneMap()
	zm := index.DecodeZM(colZoneMap)

	require.NoError(t, err)
	res := zm.Contains(int32(500))
	require.True(t, res)
	res = zm.Contains(int32(39999))
	require.True(t, res)
	res = zm.Contains(int32(40000))
	require.False(t, res)

	mp := mpool.MustNewZero()
	metaloc := EncodeLocation(writer.GetName(), blocks[0].GetExtent(), 40000, blocks[0].GetID())
	require.NoError(t, err)
	reader, err := NewObjectReader(service, metaloc)
	require.NoError(t, err)
	meta, err := reader.LoadObjectMeta(context.TODO(), mp)
	require.NoError(t, err)
	blkMeta1 := meta.GetBlockMeta(0)
	blkMeta2 := meta.GetBlockMeta(1)
	for i := uint16(0); i < meta.BlockHeader().ColumnCount(); i++ {
		offset := blkMeta1.ColumnMeta(i).Location().Offset()
		length := blkMeta1.ColumnMeta(i).Location().Length() + blkMeta2.ColumnMeta(i).Location().Length()
		oSize := blkMeta1.ColumnMeta(i).Location().OriginSize() + blkMeta2.ColumnMeta(i).Location().OriginSize()
		assert.Equal(t, offset, meta.MustGetColumn(i).Location().Offset())
		assert.Equal(t, length, meta.MustGetColumn(i).Location().Length())
		assert.Equal(t, oSize, meta.MustGetColumn(i).Location().OriginSize())
	}
	header := meta.BlockHeader()
	require.Equal(t, uint32(80000), header.Rows())
	t.Log(meta.MustGetColumn(0).Ndv(), meta.MustGetColumn(1).Ndv(), meta.MustGetColumn(2).Ndv())
	zm = meta.MustGetColumn(2).ZoneMap()
	require.True(t, zm.Contains(int32(40000)))
	require.False(t, zm.Contains(int32(100000)))
	zm = meta.GetColumnMeta(0, 2).ZoneMap()
	require.True(t, zm.Contains(int32(39999)))
	require.False(t, zm.Contains(int32(40000)))
	zm = meta.GetColumnMeta(1, 2).ZoneMap()
	require.True(t, zm.Contains(int32(40000)))
	require.True(t, zm.Contains(int32(79999)))
	require.False(t, zm.Contains(int32(80000)))
}

func TestWriter_WriteBlockAfterAlter(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	dir := testutils.InitTestEnv(ModuleName, t)
	dir = path.Join(dir, "/local")
	name := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: dir,
	}
	service, err := fileservice.NewFileService(ctx, c, nil)
	assert.Nil(t, err)

	schema := catalog.MockSchemaAll(13, 2)
	schema.ApplyAlterTable(api.NewAddColumnReq(0, 0, "xyz", types.NewProtoType(types.T_int32), 1))
	schema.ApplyAlterTable(api.NewRemoveColumnReq(0, 0, 6, 5))

	seqnums := make([]uint16, 0, len(schema.ColDefs))
	for _, col := range schema.ColDefs {
		seqnums = append(seqnums, col.SeqNum)
	}
	t.Log(seqnums)
	bats := containers.MockBatchWithAttrs(
		schema.AllTypes(),
		schema.AllNames(),
		40000*2,
		schema.GetSingleSortKey().Idx, nil).Split(2)

	writer, _ := NewBlockWriterNew(service, name, 1, seqnums)
	_, err = writer.WriteBatch(containers.ToCNBatch(bats[0]))
	assert.Nil(t, err)
	_, err = writer.WriteBatch(containers.ToCNBatch(bats[1]))
	assert.Nil(t, err)
	blocks, _, err := writer.Sync(context.Background())
	assert.Nil(t, err)
	assert.Equal(t, 2, len(blocks))
	fd := blocks[0]
	colx := fd.MustGetColumn(13 /* xyz seqnum*/)
	assert.Equal(t, uint8(types.T_int32), colx.DataType())
	assert.Equal(t, uint16(1), colx.Idx())
	colx = fd.MustGetColumn(14 /* rowid seqnum*/)
	assert.Equal(t, uint8(types.T_Rowid), colx.DataType())
	assert.Equal(t, uint16(13), colx.Idx())
	assert.NoError(t, err)

	col := fd.MustGetColumn(2 /*pk seqnum*/)
	assert.Nil(t, err)
	colZoneMap := col.ZoneMap()
	zm := index.DecodeZM(colZoneMap)

	require.NoError(t, err)
	res := zm.Contains(int32(500))
	require.True(t, res)
	res = zm.Contains(int32(39999))
	require.True(t, res)
	res = zm.Contains(int32(40000))
	require.False(t, res)

	mp := mpool.MustNewZero()
	metaloc := EncodeLocation(writer.GetName(), blocks[0].GetExtent(), 40000, blocks[0].GetID())
	require.NoError(t, err)
	reader, err := NewObjectReader(service, metaloc)
	require.NoError(t, err)
	meta, err := reader.LoadObjectMeta(context.TODO(), mp)
	require.Equal(t, uint16(15), meta.BlockHeader().MetaColumnCount())
	require.Equal(t, uint16(14), meta.BlockHeader().ColumnCount())
	require.NoError(t, err)
	header := meta.BlockHeader()
	require.Equal(t, uint32(80000), header.Rows())
	t.Log(meta.MustGetColumn(0).Ndv(), meta.MustGetColumn(1).Ndv(), meta.MustGetColumn(2).Ndv())
	zm = meta.MustGetColumn(2).ZoneMap()
	require.True(t, zm.Contains(int32(40000)))
	require.False(t, zm.Contains(int32(100000)))
	zm = meta.GetColumnMeta(0, 2).ZoneMap()
	require.True(t, zm.Contains(int32(39999)))
	require.False(t, zm.Contains(int32(40000)))
	zm = meta.GetColumnMeta(1, 2).ZoneMap()
	require.True(t, zm.Contains(int32(40000)))
	require.True(t, zm.Contains(int32(79999)))
	require.False(t, zm.Contains(int32(80000)))
}

func TestDebugData(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	dir := testutils.InitTestEnv(ModuleName, t)
	dir = path.Join(dir, "/local")
	sid := objectio.NewSegmentid()
	name := objectio.BuildObjectName(sid, 0)
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: dir,
	}
	service, err := fileservice.NewFileService(ctx, c, nil)
	assert.Nil(t, err)
	writer, _ := NewBlockWriterNew(service, name, 0, nil)

	schema := catalog.MockSchemaAll(13, 2)
	bats := catalog.MockBatch(schema, 40000*2).Split(2)

	_, err = writer.WriteBatch(containers.ToCNBatch(bats[0]))
	assert.Nil(t, err)
	_, err = writer.WriteBatch(containers.ToCNBatch(bats[1]))
	assert.Nil(t, err)
	blocks, _, err := writer.Sync(context.Background())
	assert.Nil(t, err)
	assert.Equal(t, 2, len(blocks))
	block1 := blocks[0]
	blockId := *objectio.NewBlockid(
		sid,
		0,
		block1.GetID())
	meta, location, err := GetLocationWithBlockID(ctx, service, blockId.String())
	assert.Nil(t, err)
	dataMeta := meta.MustDataMeta()
	block := dataMeta.GetBlockMeta(0)
	colx := block.MustGetColumn(12 /* xyz seqnum*/)
	colZoneMap := colx.ZoneMap()
	zm := index.DecodeZM(colZoneMap)
	logutil.Infof("zone map max: %v, min: %v", zm.GetMax(), zm.GetMin())

	reader, err := NewObjectReader(service, location)
	assert.Nil(t, err)
	dataMeta1, err := reader.LoadObjectMeta(ctx, nil)
	blocksss := dataMeta1.GetBlockMeta(0)
	colx1 := blocksss.MustGetColumn(12 /* xyz seqnum*/)
	colZoneMap1 := colx1.ZoneMap()
	zm1 := index.DecodeZM(colZoneMap1)
	logutil.Infof("zone map max: %v, min: %v", zm1.GetMax(), zm1.GetMin())
}

func TestBlockWriter_BF(t *testing.T) {
	schema := catalog.MockSchemaAll(13, -1)
	bats := catalog.MockBatch(schema, 40000).Split(1)
	cnbat := containers.ToCNBatch(bats[0])
	columnData := containers.ToTNVector(cnbat.Vecs[12])
	logutil.Infof("bats[0].Attrs[13] : %v, type: %v, key: %v", bats[0].Attrs[12], columnData.GetType(), columnData.Get(0))
	bf, err := index.NewBinaryFuseFilter(columnData)
	assert.Nil(t, err)
	buf, err := bf.Marshal()
	logutil.Infof("buf leng is %d", len(buf))
	assert.Nil(t, err)
	for j := 0; j < columnData.Length(); j++ {
		key := columnData.Get(j)
		tuples, _, err := types.DecodeTuple(key.([]byte))
		v := types.EncodeValue(key, columnData.GetType().Oid)
		var exist bool
		exist, err = bf.MayContainsKey(v)
		if err != nil {
			panic(err)
		}
		if j == 0 {
			logutil.Infof("pk not exist, key: %v, t: %d, al2: %d,bf : %v, bf : %v", key.([]byte), tuples[0], tuples[1], bf.String(), buf[:30])
		}
		if !exist {
			logutil.Infof("pk not exist, key: %v, val: %d, al2: %d v,bf : %v, bf : %v", key.([]byte), tuples[0], tuples[1], bf.String(), buf[:30])
		}
	}
}

func ReadLinesV2(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	reader := bufio.NewReader(file)
	for {
		// ReadString reads until the first occurrence of delim in the input,
		// returning a string containing the data up to and including the delimiter.
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			lines = append(lines, line)
			break
		}
		if err != nil {
			return lines, err
		}
		lines = append(lines, line[:len(line)-1])
	}
	return lines, nil
}

func Test_DebugBF2(t *testing.T) {
	strs, err := ReadLinesV2("/Users/shenjiangwei/Downloads/debugfile")
	assert.Nil(t, err)
	var data []uint64
	mp := mpool.MustNewZero()
	for j := 0; j < len(strs)-1; j++ {
		key := strs[j]
		location := strings.Split(key, "-")
		k := location[0]
		logutil.Infof(" k: %v, kk %v, d %v", key, k, j)
		val1, err := strconv.ParseUint(location[1], 10, 32)
		assert.Nil(t, err)
		val2, err := strconv.ParseUint(location[2], 10, 32)
		packer := types.NewPacker(mp)
		packer.EncodeInt32(int32(val1))
		packer.EncodeInt32(int32(val2))
		tuples, _, err := types.DecodeTuple(packer.Bytes())
		if err != nil {
			panic(err)
		}
		logutil.Infof("key: %v, t1 : %d, t2: %d", k, tuples[0], tuples[1])
		data = append(data, xxhash.Sum64(packer.Bytes()))
	}
	inner, err := xorfilter.PopulateBinaryFuse8(data)
	assert.Nil(t, err)
	for _, d := range data {
		e := inner.Contains(d)
		if !e {
			logutil.Infof("fsdfsdfdsfsdfsdf")
		}
	}

}

func Test_DebugBF(t *testing.T) {
	data := make([]uint64, 0)
	mp := mpool.MustNewZero()
	colbyte := make([][]byte, 0)
	for i := 0; i < 4000; i++ {
		packer := types.NewPacker(mp)
		packer.EncodeInt32(int32(rand.Intn(10)))
		packer.EncodeInt32(int32(rand.Intn(100000)))
		colbyte = append(colbyte, packer.Bytes())
	}
	for j := 0; j < len(colbyte); j++ {
		key := colbyte[j]
		tuples, _, err := types.DecodeTuple(key)
		if err != nil {
			panic(err)
		}
		logutil.Debugf("key: %v, t1 : %d, t2: %d", key, tuples[0], tuples[1])
		data = append(data, xxhash.Sum64(key))
	}
	inner, err := xorfilter.PopulateBinaryFuse8(data)
	assert.Nil(t, err)
	for _, d := range data {
		e := inner.Contains(d)
		if !e {
			logutil.Infof("fsdfsdfdsfsdfsdf")
		}
	}

}
