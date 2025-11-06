package group

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestAttachAggregateColumnsDistributesRowsAcrossBatches(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)

	ctr := container{
		groupByBatches: []*batch.Batch{
			batch.NewWithSize(0),
			batch.NewWithSize(0),
		},
	}
	ctr.groupByBatches[0].SetRowCount(3)
	ctr.groupByBatches[1].SetRowCount(2)

	vec1 := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList[int64](vec1, []int64{1, 2, 3, 4, 5}, nil, mp))
	vec2 := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList[int64](vec2, []int64{10, 20, 30, 40, 50}, nil, mp))

	require.NoError(t, ctr.attachAggregateColumns(proc, vec1, vec2))

	require.Equal(t, 2, len(ctr.groupByBatches[0].Vecs))
	require.Equal(t, []int64{1, 2, 3}, vector.MustFixedColWithTypeCheck[int64](ctr.groupByBatches[0].Vecs[0]))
	require.Equal(t, []int64{10, 20, 30}, vector.MustFixedColWithTypeCheck[int64](ctr.groupByBatches[0].Vecs[1]))

	require.Equal(t, 2, len(ctr.groupByBatches[1].Vecs))
	require.Equal(t, []int64{4, 5}, vector.MustFixedColWithTypeCheck[int64](ctr.groupByBatches[1].Vecs[0]))
	require.Equal(t, []int64{40, 50}, vector.MustFixedColWithTypeCheck[int64](ctr.groupByBatches[1].Vecs[1]))
}
