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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRewriteAdmissionAmplificationAndWindowBudgets(t *testing.T) {
	admission, err := NewRewriteAdmission(RewriteReleaseProfile{
		Window:                   time.Hour,
		MaxAmplification:         4,
		MaxSourceBytesPerAccount: 100,
		MaxSourceBytesPerCluster: 150,
	})
	require.NoError(t, err)
	now := time.Unix(3600, 0)
	require.Error(t, admission.Admit(RewriteAdmissionRequest{
		AccountID:           1,
		SourceBytes:         50,
		LiveLogicalBytes:    90,
		ExpiredLogicalBytes: 10,
		Now:                 now,
	}))
	require.NoError(t, admission.Admit(RewriteAdmissionRequest{
		AccountID:           1,
		SourceBytes:         60,
		LiveLogicalBytes:    30,
		ExpiredLogicalBytes: 20,
		Now:                 now,
	}))
	require.Error(t, admission.Admit(RewriteAdmissionRequest{
		AccountID:           1,
		SourceBytes:         60,
		LiveLogicalBytes:    30,
		ExpiredLogicalBytes: 20,
		Now:                 now,
	}))
	require.NoError(t, admission.Admit(RewriteAdmissionRequest{
		AccountID:           2,
		SourceBytes:         90,
		LiveLogicalBytes:    30,
		ExpiredLogicalBytes: 20,
		Now:                 now,
	}))
	require.Error(t, admission.Admit(RewriteAdmissionRequest{
		AccountID:           3,
		SourceBytes:         1,
		LiveLogicalBytes:    1,
		ExpiredLogicalBytes: 1,
		Now:                 now,
	}))
	require.NoError(t, admission.Admit(RewriteAdmissionRequest{
		AccountID:           1,
		SourceBytes:         100,
		LiveLogicalBytes:    1,
		ExpiredLogicalBytes: 1,
		Now:                 now.Add(time.Hour),
	}))
}

func TestRewriteAdmissionReservesSourceBeforeClassification(t *testing.T) {
	admission, err := NewRewriteAdmission(RewriteReleaseProfile{
		Window:                   time.Hour,
		MaxAmplification:         4,
		MaxSourceBytesPerAccount: 100,
		MaxSourceBytesPerCluster: 150,
	})
	require.NoError(t, err)
	now := time.Unix(3600, 0)
	require.NoError(t, admission.ReserveSource(1, 80, now))
	require.ErrorContains(t,
		admission.ReserveSource(1, 30, now),
		"account Rewrite byte window exhausted",
	)
	require.ErrorContains(t,
		admission.CheckAmplification(90, 10),
		"rewrite amplification",
	)
	require.NoError(t, admission.CheckAmplification(30, 20))
}
