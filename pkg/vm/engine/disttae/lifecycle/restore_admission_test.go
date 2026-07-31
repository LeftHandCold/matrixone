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

	"github.com/stretchr/testify/require"
)

func TestRestoreStagingAdmissionBoundsAccountAndCoordinatorBytes(t *testing.T) {
	admission, err := NewRestoreStagingAdmission(100, 150)
	require.NoError(t, err)

	release, err := admission.Reserve(1, 20, 70)
	require.NoError(t, err)
	require.ErrorContains(t,
		func() error {
			_, reserveErr := admission.Reserve(1, 40, 70)
			return reserveErr
		}(),
		"account Restore staging",
	)
	secondRelease, err := admission.Reserve(2, 0, 70)
	require.NoError(t, err)
	require.ErrorContains(t,
		func() error {
			_, reserveErr := admission.Reserve(3, 0, 20)
			return reserveErr
		}(),
		"coordinator Restore staging",
	)

	release()
	secondRelease()
	thirdRelease, err := admission.Reserve(3, 0, 100)
	require.NoError(t, err)
	thirdRelease()
}

func TestRestoreStagingAdmissionReleaseIsIdempotent(t *testing.T) {
	admission, err := NewRestoreStagingAdmission(100, 100)
	require.NoError(t, err)
	release, err := admission.Reserve(1, 0, 80)
	require.NoError(t, err)
	release()
	release()
	_, err = admission.Reserve(2, 0, 100)
	require.NoError(t, err)
}
