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
	"github.com/stretchr/testify/require"
)

func TestSQLDatasetReaderCountsActiveRestoreStagingBytes(t *testing.T) {
	mp := mpool.MustNewZero()
	fake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{{
			contains:  "sum(d.logical_bytes)",
			accountID: 17,
			result:    lifecycleAccountResult(t, mp, 4096),
		}},
	}
	bytes, err := (SQLDatasetReader{Executor: fake}).
		ActiveRestoreLogicalBytes(context.Background(), 17)
	require.NoError(t, err)
	require.Equal(t, uint64(4096), bytes)
}
