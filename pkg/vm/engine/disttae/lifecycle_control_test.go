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

package disttae

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/stretchr/testify/require"
)

func TestLifecycleCommitControlIsImmutableAndOutsideWrites(t *testing.T) {
	transaction := &Transaction{}
	control := &api.LifecycleCommitEntry{
		ProtocolVersion: 1,
		RootId:          "root-1",
		AttemptId:       "attempt-1",
		DatabaseId:      7,
		PhysicalTableId: 42,
	}
	require.NoError(t, transaction.SetLifecycleCommitControl(DNStore{}, control))
	control.RootId = "mutated"
	require.Empty(t, transaction.writes)
	require.Equal(t, "root-1", transaction.lifecycleCommitControl.Entry.RootId)
	require.False(t, transaction.readOnly.Load())
	require.Error(t, transaction.SetLifecycleCommitControl(DNStore{}, control))
}

func TestAppendLifecycleCommitControlAfterOrdinaryEntries(t *testing.T) {
	ordinary := &api.Entry{EntryType: api.Entry_Insert}
	control := &LifecycleCommitControl{Entry: &api.LifecycleCommitEntry{
		ProtocolVersion: 1,
		DatabaseId:      7,
		PhysicalTableId: 42,
	}}
	entries, err := appendLifecycleCommitControl([]*api.Entry{ordinary}, control)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Same(t, ordinary, entries[0])
	require.Equal(t, api.Entry_LifecycleCommit, entries[1].EntryType)
	require.Nil(t, entries[1].Bat)
	require.Same(t, control.Entry, entries[1].LifecycleCommit)
	require.Equal(t, uint64(7), entries[1].DatabaseId)
	require.Equal(t, uint64(42), entries[1].TableId)
}

func TestAppendLifecycleCommitControlRejectsUnknownVersion(t *testing.T) {
	_, err := appendLifecycleCommitControl(nil, &LifecycleCommitControl{
		Entry: &api.LifecycleCommitEntry{ProtocolVersion: 2},
	})
	require.Error(t, err)
}
