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

package catalog

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/stretchr/testify/require"
)

func TestLifecycleRestoreChunkCatalogHasNoRedundantAutoIncrementState(t *testing.T) {
	require.NotContains(
		t,
		strings.ToLower(MoLifecycleRestoreChunksDDL),
		"auto_increment_maxima_blob",
	)
}

func TestParseEntryListRejectsUnknownEntryBeforeBatch(t *testing.T) {
	require.NotPanics(t, func() {
		_, remaining, err := ParseEntryList([]*api.Entry{{
			EntryType: api.Entry_EntryType(99),
			Bat:       nil,
		}})
		require.Error(t, err)
		require.Empty(t, remaining)
	})
}

func TestParseEntryListReturnsLifecycleControlWithoutBatch(t *testing.T) {
	control := &api.LifecycleCommitEntry{
		ProtocolVersion: 1,
		RootId:          "root",
		AttemptId:       "attempt",
	}
	entry, remaining, err := ParseEntryList([]*api.Entry{{
		EntryType:       api.Entry_LifecycleCommit,
		LifecycleCommit: control,
	}})
	require.NoError(t, err)
	require.Empty(t, remaining)
	require.Same(t, control, entry)
}
