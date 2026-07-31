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

package lifecycle

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/stretchr/testify/require"
)

func TestRewriteHostPreservesPhysicalBatchAndUnionsDAndE(t *testing.T) {
	mp := mpool.MustNewZero()
	source := batch.NewWithSize(1)
	source.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	for value := int64(0); value < 5; value++ {
		require.NoError(t, vector.AppendFixed(source.Vecs[0], value, false, mp))
	}
	source.SetRowCount(5)
	base := &rewriteFakeMergeHost{
		bat:     source,
		deleted: nulls.Build(5, 1),
		mp:      mp,
	}
	var archived *batch.Batch
	host, err := NewRewriteHost(
		base,
		func(
			_ context.Context,
			got *batch.Batch,
			deleted *nulls.Nulls,
		) (*nulls.Nulls, error) {
			require.Same(t, source, got)
			require.True(t, deleted.Contains(1))
			return nulls.Build(5, 3), nil
		},
		func(_ context.Context, got *batch.Batch, expired *nulls.Nulls) error {
			archived = got
			require.True(t, expired.Contains(3))
			return nil
		},
	)
	require.NoError(t, err)
	require.True(t, host.DoTransfer())

	got, skipped, release, err := host.LoadNextBatch(context.Background(), 0, nil)
	require.NoError(t, err)
	require.Same(t, source, got)
	require.Same(t, source, archived)
	require.True(t, skipped.Contains(1))
	require.True(t, skipped.Contains(3))
	require.False(t, skipped.Contains(0))
	release()
	require.Equal(t, 1, base.releaseCount)
	source.Clean(mp)
}

func TestRewriteHostRejectsInvalidClassAndMultipleSources(t *testing.T) {
	base := &rewriteFakeMergeHost{objectCount: 2}
	_, err := NewRewriteHost(base, func(
		context.Context,
		*batch.Batch,
		*nulls.Nulls,
	) (*nulls.Nulls, error) {
		return nil, nil
	}, nil)
	require.Error(t, err)

	base.objectCount = 1
	base.bat = batch.NewWithSize(0)
	base.bat.SetRowCount(2)
	base.deleted = nulls.Build(2, 0)
	host, err := NewRewriteHost(base, func(
		context.Context,
		*batch.Batch,
		*nulls.Nulls,
	) (*nulls.Nulls, error) {
		return nulls.Build(2, 0), nil
	}, nil)
	require.NoError(t, err)
	_, _, _, err = host.LoadNextBatch(context.Background(), 0, nil)
	require.Error(t, err)
	require.Equal(t, 1, base.releaseCount)
}

type rewriteFakeMergeHost struct {
	bat          *batch.Batch
	deleted      *nulls.Nulls
	mp           *mpool.MPool
	releaseCount int
	objectCount  int
}

func (host *rewriteFakeMergeHost) GetVector(
	typ *types.Type,
) (*vector.Vector, func()) {
	value := vector.NewVec(*typ)
	return value, func() { value.Free(host.mp) }
}
func (host *rewriteFakeMergeHost) GetMPool() *mpool.MPool { return host.mp }
func (*rewriteFakeMergeHost) Name() string                { return "lifecycle-test" }
func (*rewriteFakeMergeHost) HostHintName() string        { return "CN" }
func (*rewriteFakeMergeHost) TaskSourceNote() string      { return "" }
func (*rewriteFakeMergeHost) GetCommitEntry() *api.MergeCommitEntry {
	return &api.MergeCommitEntry{}
}
func (*rewriteFakeMergeHost) HasBigDelEvent() bool { return false }
func (*rewriteFakeMergeHost) SetTransferTable(*mergesort.TransferTable) {
}
func (*rewriteFakeMergeHost) PrepareNewWriter() *ioutil.BlockWriter { return nil }
func (*rewriteFakeMergeHost) DoTransfer() bool                      { return false }
func (host *rewriteFakeMergeHost) GetObjectCnt() int {
	if host.objectCount == 0 {
		return 1
	}
	return host.objectCount
}
func (*rewriteFakeMergeHost) GetBlkCnts() []int    { return []int{1} }
func (*rewriteFakeMergeHost) GetAccBlkCnts() []int { return []int{0} }
func (*rewriteFakeMergeHost) GetSortKeyType() types.Type {
	return types.T_int64.ToType()
}
func (host *rewriteFakeMergeHost) LoadNextBatch(
	context.Context,
	uint32,
	*batch.Batch,
) (*batch.Batch, *nulls.Nulls, func(), error) {
	return host.bat, host.deleted, func() { host.releaseCount++ }, nil
}
func (*rewriteFakeMergeHost) GetTotalSize() uint64       { return 0 }
func (*rewriteFakeMergeHost) GetTotalRowCnt() uint32     { return 0 }
func (*rewriteFakeMergeHost) GetBlockMaxRows() uint32    { return 8192 }
func (*rewriteFakeMergeHost) GetObjectMaxBlocks() uint16 { return 256 }
func (*rewriteFakeMergeHost) GetTargetObjSize() uint32   { return 0 }
