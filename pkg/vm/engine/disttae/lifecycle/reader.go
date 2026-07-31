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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

type lifecycleTombstoneSelector interface {
	SelectLifecycleTombstoneObjects(
		context.Context,
		types.TS,
		[]objectio.ObjectId,
		logtailreplay.LifecycleTombstoneSelectionLimits,
	) ([]objectio.ObjectEntry, int, error)
}

type ProtectionSet struct {
	DataSources         []objectio.ObjectStats
	ProtectedTombstones []objectio.ObjectStats
	ProtectedObjects    []objectio.ObjectStats
	SourceSetDigest     [32]byte
	ProtectionSetDigest [32]byte
}

// SelectProtectionSet derives a conservative superset of physical Tombstone
// Objects that MO's existing snapshot reader may consume. The snapshot reader
// continues to use the same PartitionState and its ordinary visibility logic;
// Lifecycle uses the selected identities only for SyncProtection. Tombstones
// are never added to DataSources and therefore can never enter a retirement
// entry.
func SelectProtectionSet(
	ctx context.Context,
	selector lifecycleTombstoneSelector,
	snapshot types.TS,
	dataSources []objectio.ObjectEntry,
	limits logtailreplay.LifecycleTombstoneSelectionLimits,
) (ProtectionSet, error) {
	if selector == nil || len(dataSources) == 0 {
		return ProtectionSet{}, moerr.NewInvalidInput(
			ctx,
			"Lifecycle protection selection requires a selector and Data Objects",
		)
	}
	sourceIDs := make([]objectio.ObjectId, len(dataSources))
	set := ProtectionSet{
		DataSources: make([]objectio.ObjectStats, len(dataSources)),
	}
	for index, entry := range dataSources {
		if !entry.Visible(snapshot) || entry.GetAppendable() {
			return ProtectionSet{}, moerr.NewInvalidInput(
				ctx,
				"Lifecycle protection source is not an exact visible Data Object",
			)
		}
		set.DataSources[index] = entry.ObjectStats
		sourceIDs[index] = *entry.ObjectStats.ObjectName().ObjectId()
	}
	tombstones, _, err := selector.SelectLifecycleTombstoneObjects(
		ctx,
		snapshot,
		sourceIDs,
		limits,
	)
	if err != nil {
		return ProtectionSet{}, err
	}
	set.ProtectedTombstones = make([]objectio.ObjectStats, len(tombstones))
	for index := range tombstones {
		set.ProtectedTombstones[index] = tombstones[index].ObjectStats
	}
	set.ProtectedObjects = make(
		[]objectio.ObjectStats,
		0,
		len(set.DataSources)+len(set.ProtectedTombstones),
	)
	set.ProtectedObjects = append(set.ProtectedObjects, set.DataSources...)
	set.ProtectedObjects = append(set.ProtectedObjects, set.ProtectedTombstones...)
	set.SourceSetDigest = digestObjectStats(
		"matrixone/lifecycle/data-sources/v1",
		set.DataSources,
	)
	set.ProtectionSetDigest = digestObjectStats(
		"matrixone/lifecycle/protection-set/v1",
		set.ProtectedObjects,
	)
	return set, nil
}

func digestObjectStats(domain string, values []objectio.ObjectStats) [32]byte {
	sorted := append([]objectio.ObjectStats(nil), values...)
	slices.SortFunc(sorted, func(left, right objectio.ObjectStats) int {
		return bytes.Compare(left.ObjectName(), right.ObjectName())
	})
	sum := sha256.New()
	_, _ = sum.Write([]byte(domain))
	for index := range sorted {
		_, _ = sum.Write(sorted[index][:])
	}
	var digest [32]byte
	copy(digest[:], sum.Sum(nil))
	return digest
}

type SyncProtectionClient interface {
	Register(
		ctx context.Context,
		jobID string,
		objects []objectio.ObjectStats,
		validUntil time.Time,
	) error
	StatExact(ctx context.Context, objects []objectio.ObjectStats) error
	Renew(ctx context.Context, jobID string, validUntil time.Time) error
	Release(ctx context.Context, jobID string) error
}

type ProtectionLease struct {
	client   SyncProtectionClient
	jobID    string
	released bool
	mu       sync.Mutex
}

func AcquireProtection(
	ctx context.Context,
	client SyncProtectionClient,
	attemptID string,
	set ProtectionSet,
	validUntil time.Time,
) (*ProtectionLease, error) {
	if client == nil || attemptID == "" || len(set.ProtectedObjects) == 0 {
		return nil, moerr.NewInvalidInput(ctx, "Lifecycle SyncProtection identity is incomplete")
	}
	if !validUntil.After(time.Now()) {
		return nil, moerr.NewInvalidInput(ctx, "Lifecycle SyncProtection deadline has expired")
	}
	jobDigest := sha256.Sum256(append(
		append([]byte("matrixone/lifecycle/sync-protection/v1"), []byte(attemptID)...),
		set.ProtectionSetDigest[:]...,
	))
	jobID := attemptID + "-" + hex.EncodeToString(jobDigest[:8])
	if err := client.Register(ctx, jobID, set.ProtectedObjects, validUntil); err != nil {
		return nil, err
	}
	lease := &ProtectionLease{client: client, jobID: jobID}
	if err := client.StatExact(ctx, set.ProtectedObjects); err != nil {
		releaseErr := lease.Release(ctx)
		return nil, errors.Join(err, releaseErr)
	}
	return lease, nil
}

func (lease *ProtectionLease) JobID() string {
	return lease.jobID
}

func (lease *ProtectionLease) Renew(
	ctx context.Context,
	validUntil time.Time,
) error {
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.released {
		return fmt.Errorf("Lifecycle SyncProtection %s is already released", lease.jobID)
	}
	if !validUntil.After(time.Now()) {
		return fmt.Errorf("Lifecycle SyncProtection renewal deadline has expired")
	}
	return lease.client.Renew(ctx, lease.jobID, validUntil)
}

func (lease *ProtectionLease) Release(ctx context.Context) error {
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.released {
		return nil
	}
	if err := lease.client.Release(ctx, lease.jobID); err != nil {
		return err
	}
	lease.released = true
	return nil
}

type ExactBlockLoader func(
	ctx context.Context,
	block objectio.BlockInfo,
) (*batch.Batch, *nulls.Bitmap, func(), error)

type ExactBlockConsumer func(*batch.Batch, *nulls.Bitmap) error

// ReadExactBlocks serially consumes complete physical Blocks in caller-provided
// Object/block order. A borrowed Batch is released exactly once before the
// next Block is loaded, including callback and cancellation failures.
func ReadExactBlocks(
	ctx context.Context,
	blocks []objectio.BlockInfo,
	load ExactBlockLoader,
	consume ExactBlockConsumer,
) error {
	if load == nil || consume == nil {
		return moerr.NewInvalidInput(ctx, "Lifecycle exact reader callbacks are required")
	}
	for _, block := range blocks {
		if err := ctx.Err(); err != nil {
			return err
		}
		value, deletes, release, err := load(ctx, block)
		if err != nil {
			return err
		}
		if release == nil {
			return moerr.NewInternalError(ctx, "Lifecycle exact reader returned no release callback")
		}
		err = func() error {
			defer release()
			return consume(value, deletes)
		}()
		if err != nil {
			return err
		}
	}
	return nil
}
