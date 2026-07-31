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

package frontend

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLifecycleRestoreAdmissionUsageDoesNotDoubleCountResumedAttempt(
	t *testing.T,
) {
	usage, err := lifecycleRestoreAdmissionUsage(30, 10, true)
	require.NoError(t, err)
	require.Equal(t, uint64(20), usage)

	usage, err = lifecycleRestoreAdmissionUsage(30, 10, false)
	require.NoError(t, err)
	require.Equal(t, uint64(30), usage)

	_, err = lifecycleRestoreAdmissionUsage(5, 10, true)
	require.ErrorContains(t, err, "usage is inconsistent")
}

func TestLifecycleRestorePublishedRetryNeedsNoStaging(t *testing.T) {
	require.True(t, lifecycleRestoreAlreadyPublished(true, "DONE"))
	require.False(t, lifecycleRestoreAlreadyPublished(true, "IMPORTING"))
	require.False(t, lifecycleRestoreAlreadyPublished(false, "DONE"))
}
