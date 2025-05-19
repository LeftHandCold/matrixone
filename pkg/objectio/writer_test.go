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

package objectio

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"math"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	ModuleName = "ObjectIo"
)

func GetDefaultTestPath(module string, name string) string {
	return filepath.Join("/tmp", module, name)
}

func MakeDefaultTestPath(module string, name string) string {
	path := GetDefaultTestPath(module, name)
	os.MkdirAll(path, os.FileMode(0755))
	return path
}

func RemoveDefaultTestPath(module string, name string) {
	path := GetDefaultTestPath(module, name)
	os.RemoveAll(path)
}

func InitTestEnv(module string, name string) string {
	RemoveDefaultTestPath(module, name)
	return MakeDefaultTestPath(module, name)
}

func TestNewObjectWriter(t *testing.T) {
	ctx := context.Background()

	dir := InitTestEnv(ModuleName, t.Name())
	dir = path.Join(dir, "/local")
	id := 1
	name := fmt.Sprintf("%d.blk", id)
	mp := mpool.MustNewZero()
	bat := newBatch(mp)
	defer bat.Clean(mp)
	bat2 := newBatch2(mp)
	defer bat.Clean(mp)
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: dir,
		Cache:   fileservice.DisabledCacheConfig,
	}
	service, err := fileservice.NewFileService(ctx, c, nil)
	assert.Nil(t, err)
	defer service.Close(ctx)

	objectWriter, err := NewObjectWriterSpecial(WriterNormal, name, service)
	assert.Nil(t, err)
	objectWriter.SetAppendable()
	//objectWriter.pkColIdx = 3
	fd, err := objectWriter.Write(bat)
	assert.Nil(t, err)
	for i := range bat.Vecs {
		zbuf := make([]byte, 64)
		zbuf[31] = 1
		zbuf[63] = 10
		fd.ColumnMeta(uint16(i)).SetZoneMap(zbuf)
	}
	_, err = objectWriter.Write(bat)
	assert.Nil(t, err)
	_, err = objectWriter.WriteWithoutSeqnum(bat2)
	assert.Nil(t, err)
	ts := time.Now()
	option := WriteOptions{
		Type: WriteTS,
		Val:  ts,
	}
	blocks, err := objectWriter.WriteEnd(context.Background(), option)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(blocks))
	assert.Nil(t, objectWriter.buffer)
	require.Equal(t, objectWriter.objStats.Size(), blocks[0].GetExtent().End()+FooterSize)

	objectReader, _ := NewObjectReaderWithStr(name, service)
	extents := make([]Extent, 3)
	for i, blk := range blocks {
		extents[i] = NewExtent(1, blk.GetExtent().Offset(), blk.GetExtent().Length(), blk.GetExtent().OriginSize())
	}
	pool, err := mpool.NewMPool("objectio_test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	nb0 := pool.CurrNB()
	objectReader.CacheMetaExtent(&extents[0])
	metaHeader, err := objectReader.ReadMeta(context.Background(), pool)
	assert.Nil(t, err)
	meta, _ := metaHeader.DataMeta()
	oSize := uint32(0)
	for i := uint32(0); i < 3; i++ {
		blockMeta := meta.GetBlockMeta(i)
		for y := uint16(0); y < blockMeta.GetColumnCount(); y++ {
			oSize += blockMeta.MustGetColumn(y).Location().OriginSize()
		}
	}
	oSize += meta.BlockHeader().BFExtent().OriginSize()
	oSize += meta.BlockHeader().ZoneMapArea().OriginSize()
	// 24 is the size of empty bf and zm
	oSize += HeaderSize + FooterSize + 24 + extents[0].OriginSize()
	require.Equal(t, objectWriter.objStats.OriginSize(), oSize)
	assert.Equal(t, uint32(3), meta.BlockCount())
	assert.True(t, meta.BlockHeader().Appendable())
	assert.Equal(t, uint16(math.MaxUint16), meta.BlockHeader().SortKey())
	idxs := make([]uint16, 3)
	idxs[0] = 0
	idxs[1] = 2
	idxs[2] = 3
	typs := []types.Type{types.T_int8.ToType(), types.T_int32.ToType(), types.T_int64.ToType()}
	vec1, err := objectReader.ReadOneBlock(context.Background(), idxs, typs, 0, pool)
	assert.Nil(t, err)
	defer vec1.Release()

	obj, err := Decode(vec1.Entries[0].CachedData.Bytes())
	assert.Nil(t, err)
	vector1 := obj.(*vector.Vector)
	assert.Equal(t, int8(3), vector.MustFixedColWithTypeCheck[int8](vector1)[3])

	obj, err = Decode(vec1.Entries[1].CachedData.Bytes())
	assert.Nil(t, err)
	vector2 := obj.(*vector.Vector)
	assert.Equal(t, int32(3), vector.MustFixedColWithTypeCheck[int32](vector2)[3])

	obj, err = Decode(vec1.Entries[2].CachedData.Bytes())
	assert.Nil(t, err)
	vector3 := obj.(*vector.Vector)
	assert.Equal(t, int64(3), vector.GetFixedAtWithTypeCheck[int64](vector3, 3))

	blk := blocks[0].MustGetColumn(idxs[0])
	buf := blk.ZoneMap()
	assert.Equal(t, uint8(0x1), buf[31])
	assert.Equal(t, uint8(0xa), buf[63])
	assert.True(t, nb0 == pool.CurrNB())

	dirs, err := fileservice.SortedList(service.List(ctx, ""))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(dirs))
	objectReader, err = NewObjectReaderWithStr(name, service)
	assert.Nil(t, err)
	metaHeader, err = objectReader.ReadAllMeta(context.Background(), pool)
	assert.Nil(t, err)
	meta, _ = metaHeader.DataMeta()
	assert.Equal(t, uint32(3), meta.BlockCount())
	assert.Nil(t, err)
	assert.Equal(t, uint32(3), meta.BlockCount())
	idxs = make([]uint16, 3)
	idxs[0] = 0
	idxs[1] = 2
	idxs[2] = 3
	vec2, err := objectReader.ReadOneBlock(context.Background(), idxs, typs, 0, pool)
	assert.Nil(t, err)
	defer vec2.Release()

	obj, err = Decode(vec2.Entries[0].CachedData.Bytes())
	assert.Nil(t, err)
	vector1 = obj.(*vector.Vector)
	assert.Equal(t, int8(3), vector.MustFixedColWithTypeCheck[int8](vector1)[3])

	obj, err = Decode(vec2.Entries[1].CachedData.Bytes())
	assert.Nil(t, err)
	vector2 = obj.(*vector.Vector)
	assert.Equal(t, int32(3), vector.MustFixedColWithTypeCheck[int32](vector2)[3])

	obj, err = Decode(vec2.Entries[2].CachedData.Bytes())
	assert.Nil(t, err)
	vector3 = obj.(*vector.Vector)
	assert.Equal(t, int64(3), vector.GetFixedAtWithTypeCheck[int64](vector3, 3))
	blk = blocks[0].MustGetColumn(idxs[0])
	buf = blk.ZoneMap()
	assert.Equal(t, uint8(0x1), buf[31])
	assert.Equal(t, uint8(0xa), buf[63])
	assert.True(t, nb0 == pool.CurrNB())
	buf1, err := objectReader.ReadExtent(context.Background(), meta.BlockHeader().ZoneMapArea())
	assert.Nil(t, err)
	zma := ZoneMapArea(buf1)
	buf = zma.GetZoneMap(0, 0)
	assert.Equal(t, uint8(0x1), buf[31])
	assert.Equal(t, uint8(0xa), buf[63])
}

func getObjectMeta(ctx context.Context, t *testing.B) ObjectDataMeta {
	dir := InitTestEnv(ModuleName, t.Name())
	dir = path.Join(dir, "/local")
	id := 1
	name := fmt.Sprintf("%d.blk", id)
	mp := mpool.MustNewZero()
	bat := newBatch(mp)
	defer bat.Clean(mp)
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: dir,
	}
	service, err := fileservice.NewFileService(ctx, c, nil)
	assert.Nil(t, err)

	objectWriter, err := NewObjectWriterSpecial(WriterNormal, name, service)
	assert.Nil(t, err)
	for y := 0; y < 1; y++ {
		fd, err := objectWriter.Write(bat)
		assert.Nil(t, err)
		for i := range bat.Vecs {
			zbuf := make([]byte, 64)
			zbuf[31] = 1
			zbuf[63] = 10
			fd.ColumnMeta(uint16(i)).SetZoneMap(zbuf)
		}
	}
	ts := time.Now()
	option := WriteOptions{
		Type: WriteTS,
		Val:  ts,
	}
	blocks, err := objectWriter.WriteEnd(context.Background(), option)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(blocks))
	assert.Nil(t, objectWriter.buffer)
	objectReader, _ := NewObjectReaderWithStr(name, service)
	ext := blocks[0].BlockHeader().MetaLocation()
	objectReader.CacheMetaExtent(&ext)
	metaHeader, err := objectReader.ReadMeta(context.Background(), nil)
	assert.Nil(t, err)
	meta, _ := metaHeader.DataMeta()
	return meta
}

func BenchmarkMetadata(b *testing.B) {
	ctx := context.Background()
	meta := getObjectMeta(ctx, b)
	b.Run("GetBlockMeta", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			meta.GetBlockMeta(0)
		}
	})
	b.Log(meta.GetBlockMeta(0).GetID())
	b.Run("GetColumnMeta", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			meta.Length()
		}
	})
	b.Run("BlockCount", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			meta.BlockCount()
		}
	})
	b.Log(meta.BlockCount())
}

func TestNewObjectReader(t *testing.T) {
	ctx := context.Background()

	dir := InitTestEnv(ModuleName, t.Name())
	dir = path.Join(dir, "/local")
	id := 1
	name := fmt.Sprintf("%d.blk", id)
	mp := mpool.MustNewZero()
	bat := newBatch(mp)
	defer bat.Clean(mp)
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: dir,
	}
	service, err := fileservice.NewFileService(ctx, c, nil)
	assert.Nil(t, err)
	defer service.Close(ctx)

	objectWriter, err := NewObjectWriterSpecial(WriterNormal, name, service)
	assert.Nil(t, err)
	fd, err := objectWriter.Write(bat)
	assert.Nil(t, err)
	for i := range bat.Vecs {
		zbuf := make([]byte, 64)
		zbuf[31] = 1
		zbuf[63] = 10
		fd.ColumnMeta(uint16(i)).SetZoneMap(zbuf)
	}
	_, err = objectWriter.Write(bat)
	assert.Nil(t, err)
	_, _, err = objectWriter.WriteSubBlock(bat, 2)
	assert.Nil(t, err)
	_, _, err = objectWriter.WriteSubBlock(bat, 26)
	assert.Nil(t, err)
	ts := time.Now()
	option := WriteOptions{
		Type: WriteTS,
		Val:  ts,
	}
	blocks, err := objectWriter.WriteEnd(context.Background(), option)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(blocks))
	assert.Nil(t, objectWriter.buffer)
	objectReader, _ := NewObjectReaderWithStr(name, service)
	ext := blocks[0].BlockHeader().MetaLocation()
	objectReader.CacheMetaExtent(&ext)
	metaHeader, err := objectReader.ReadMeta(context.Background(), nil)
	assert.Nil(t, err)
	meta, _ := metaHeader.DataMeta()
	assert.Equal(t, uint32(2), meta.BlockCount())
	meta, _ = metaHeader.SubMeta(0)
	assert.Equal(t, uint32(1), meta.BlockCount())
	meta, _ = metaHeader.SubMeta(24)
	assert.Equal(t, uint32(1), meta.BlockCount())
}

func TestNewObjectReader2(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	fsDir := "/Users/shenjiangwei/Work/local/tae/matrixone/mo-data"
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "S3",
		S3: fileservice.ObjectStorageArguments{
			Endpoint: "disk",
			Bucket:   fsDir,
		},
	}
	service, err := fileservice.NewFileService(ctx, c, nil)
	assert.Nil(t, err)
	objectReader, _ := NewObjectReaderWithStr("0196d32a-6db7-784d-be30-7674a0cc495a_00000", service)
	ext := NewExtent(1, 1922707, 2323, 9523)
	objectReader.CacheMetaExtent(&ext)
	metaHeader, err := objectReader.ReadMeta(context.Background(), nil)
	assert.Nil(t, err)
	meta := metaHeader.MustDataMeta()
	cols := meta.BlockHeader().ColumnCount()
	ioVec := fileservice.IOVector{
		FilePath: "0196d32a-6db7-784d-be30-7674a0cc495a_00000",
		Entries:  make([]fileservice.IOEntry, 0, int(cols)*int(meta.BlockCount())),
	}
	for i := uint32(0); i < meta.BlockCount(); i++ {
		for seqnum := uint16(0); seqnum < cols; seqnum++ {
			blkmeta := meta.GetBlockMeta(i)
			if seqnum > blkmeta.GetMaxSeqnum() || blkmeta.ColumnMeta(seqnum).DataType() == 0 {
				// prefetch, do not generate
				panic("ReadAllBlocksWithMeta expect no schema changes")
			}
			col := blkmeta.ColumnMeta(seqnum)
			ext := col.Location()
			ioVec.Entries = append(ioVec.Entries, fileservice.IOEntry{
				Offset: int64(ext.Offset()),
				Size:   int64(ext.Length()),

				ToCacheData: constructorFactory(int64(ext.OriginSize()), ext.Alg()),
			})

		}
	}

	err = service.Read(ctx, &ioVec)
	assert.Nil(t, err)
	//TODO when to call ioVec.Release?
	for i, vec := range ioVec.Entries {
		header := DecodeIOEntryHeader(vec.CachedData.Bytes())
		if header.Type != IOET_ColData {
			panic("sdfsdfs")
		}
		logutil.Infof("i is %v, vec is %v, cols %d, header is %v", i, len(vec.CachedData.Bytes()), cols, header.String())
	}
}

func newBatch(mp *mpool.MPool) *batch.Batch {
	types := []types.Type{
		types.T_int8.ToType(),
		types.T_int16.ToType(),
		types.T_int32.ToType(),
		types.T_int64.ToType(),
		types.T_uint16.ToType(),
		types.T_uint32.ToType(),
		types.T_uint8.ToType(),
		types.T_uint64.ToType(),
	}
	return NewBatch(types, false, int(40000*2), mp)
}

func newBatch2(mp *mpool.MPool) *batch.Batch {
	types := []types.Type{
		types.T_int8.ToType(),
		types.T_int16.ToType(),
		types.T_int32.ToType(),
		types.T_int64.ToType(),
		types.T_uint16.ToType(),
		types.T_uint32.ToType(),
	}
	return NewBatch(types, false, int(40000*2), mp)
}
