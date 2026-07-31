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
	"fmt"
	"sync"

	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

// RestoreStagingAdmission is a CN-local pressure bound, not a distributed
// correctness lock. The tenant Catalog supplies durable active usage; this
// object only closes concurrent reservations on the current coordinator.
type RestoreStagingAdmission struct {
	mu sync.Mutex

	maxAccountBytes     uint64
	maxCoordinatorBytes uint64
	coordinatorBytes    uint64
	accountBytes        map[uint32]uint64
}

func NewRestoreStagingAdmission(
	maxAccountBytes uint64,
	maxCoordinatorBytes uint64,
) (*RestoreStagingAdmission, error) {
	if maxAccountBytes == 0 ||
		maxCoordinatorBytes == 0 ||
		maxAccountBytes > maxCoordinatorBytes {
		return nil, fmt.Errorf("Lifecycle Restore staging limits are invalid")
	}
	return &RestoreStagingAdmission{
		maxAccountBytes:     maxAccountBytes,
		maxCoordinatorBytes: maxCoordinatorBytes,
		accountBytes:        make(map[uint32]uint64),
	}, nil
}

func (admission *RestoreStagingAdmission) Reserve(
	accountID uint32,
	activeAccountBytes uint64,
	requestedBytes uint64,
) (func(), error) {
	if admission == nil || accountID == 0 || requestedBytes == 0 {
		return nil, fmt.Errorf("Lifecycle Restore staging reservation is incomplete")
	}
	admission.mu.Lock()
	defer admission.mu.Unlock()
	localAccount, overflow := addUint64(
		admission.accountBytes[accountID],
		requestedBytes,
	)
	if overflow {
		metricv2.LifecycleResourceRejectionCounter.WithLabelValues(
			"restore_account_bytes",
		).Inc()
		return nil, fmt.Errorf("RESOURCE_BLOCKED: account Restore staging overflow")
	}
	accountTotal, overflow := addUint64(activeAccountBytes, localAccount)
	if overflow || accountTotal > admission.maxAccountBytes {
		metricv2.LifecycleResourceRejectionCounter.WithLabelValues(
			"restore_account_bytes",
		).Inc()
		return nil, fmt.Errorf(
			"RESOURCE_BLOCKED: account Restore staging bytes exhausted",
		)
	}
	coordinatorTotal, overflow := addUint64(
		admission.coordinatorBytes,
		requestedBytes,
	)
	if overflow || coordinatorTotal > admission.maxCoordinatorBytes {
		metricv2.LifecycleResourceRejectionCounter.WithLabelValues(
			"restore_coordinator_bytes",
		).Inc()
		return nil, fmt.Errorf(
			"RESOURCE_BLOCKED: coordinator Restore staging bytes exhausted",
		)
	}
	admission.accountBytes[accountID] = localAccount
	admission.coordinatorBytes = coordinatorTotal

	var once sync.Once
	return func() {
		once.Do(func() {
			admission.mu.Lock()
			defer admission.mu.Unlock()
			admission.accountBytes[accountID] -= requestedBytes
			if admission.accountBytes[accountID] == 0 {
				delete(admission.accountBytes, accountID)
			}
			admission.coordinatorBytes -= requestedBytes
		})
	}, nil
}
