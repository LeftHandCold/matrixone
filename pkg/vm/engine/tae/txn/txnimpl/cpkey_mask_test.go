// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txnimpl

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/objectio"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/require"
)

func TestCompositePKLocalDeleteMaskerConsumesMatchingDeletePKs(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)

	pks := containers.MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
	rowIDs := containers.MakeVector(objectio.RowidType, common.DefaultAllocator)
	deletePKs := containers.MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
	defer pks.Close()
	defer rowIDs.Close()
	defer deletePKs.Close()

	for _, key := range [][]byte{[]byte("a"), []byte("b"), []byte("c")} {
		pks.Append(key, false)
		rowIDs.Append(types.EmptyRowid, false)
	}
	for _, key := range [][]byte{[]byte("b"), []byte("x"), []byte("c")} {
		deletePKs.Append(key, false)
	}

	masker, err := newCompositePKLocalDeleteMasker(pks, rowIDs, common.DefaultAllocator)
	require.NoError(t, err)

	changed, err := masker.apply(deletePKs)
	require.NoError(t, err)
	require.True(t, changed)
	require.True(t, masker.hasRemaining())
	require.False(t, rowIDs.IsNull(0))
	require.True(t, rowIDs.IsNull(1))
	require.True(t, rowIDs.IsNull(2))
}

func TestCompositePKLocalDeleteMaskerUsesMultiplicity(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)

	pks := containers.MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
	rowIDs := containers.MakeVector(objectio.RowidType, common.DefaultAllocator)
	deletePKs := containers.MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
	defer pks.Close()
	defer rowIDs.Close()
	defer deletePKs.Close()

	for i := 0; i < 3; i++ {
		pks.Append([]byte("same"), false)
		rowIDs.Append(types.EmptyRowid, false)
	}
	for i := 0; i < 2; i++ {
		deletePKs.Append([]byte("same"), false)
	}

	masker, err := newCompositePKLocalDeleteMasker(pks, rowIDs, common.DefaultAllocator)
	require.NoError(t, err)

	changed, err := masker.apply(deletePKs)
	require.NoError(t, err)
	require.True(t, changed)

	nullCount := 0
	for i := 0; i < rowIDs.Length(); i++ {
		if rowIDs.IsNull(i) {
			nullCount++
		}
	}
	require.Equal(t, 2, nullCount)
	require.True(t, masker.hasRemaining())
}
