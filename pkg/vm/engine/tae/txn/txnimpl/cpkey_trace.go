// Copyright 2022 Matrix Origin
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

package txnimpl

import (
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"go.uber.org/zap"
)

const dedupTraceSampleLimit = 8

type dedupTraceSnapshot struct {
	valid        bool
	totalCount   int
	nonNullCount int
	pairSample   string
}

func captureDedupTraceSnapshot(
	pks containers.Vector,
	rowIDs containers.Vector,
	compositePK bool,
) dedupTraceSnapshot {
	snapshot := dedupTraceSnapshot{
		valid:      true,
		pairSample: "[]",
	}
	if pks == nil || rowIDs == nil {
		return snapshot
	}
	limit := pks.Length()
	if rowIDs.Length() < limit {
		limit = rowIDs.Length()
	}
	snapshot.totalCount = limit
	if limit == 0 {
		return snapshot
	}

	sampleLimit := limit
	if sampleLimit > dedupTraceSampleLimit {
		sampleLimit = dedupTraceSampleLimit
	}
	pairs := make([]string, 0, sampleLimit)
	for i := 0; i < limit; i++ {
		if !rowIDs.IsNull(i) {
			snapshot.nonNullCount++
		}
		if i >= sampleLimit {
			continue
		}
		pairs = append(pairs, fmt.Sprintf(
			"%d:%s=>%s",
			i,
			formatDedupTraceValue(pks, i, compositePK),
			formatDedupTraceValue(rowIDs, i, false),
		))
	}
	snapshot.pairSample = "[" + strings.Join(pairs, ", ") + "]"
	if limit > sampleLimit {
		snapshot.pairSample += "..."
	}
	return snapshot
}

func formatDedupTraceValue(vec containers.Vector, idx int, compositePK bool) string {
	if vec == nil || idx < 0 || idx >= vec.Length() {
		return "<out-of-range>"
	}
	if vec.IsNull(idx) {
		return "NULL"
	}
	if compositePK {
		return common.TypeStringValue(*vec.GetType(), vec.Get(idx), false, common.WithIsComposite{})
	}
	return common.TypeStringValue(*vec.GetType(), vec.Get(idx), false)
}

func appendDedupSnapshotFields(
	fields []zap.Field,
	prefix string,
	snapshot dedupTraceSnapshot,
) []zap.Field {
	if !snapshot.valid {
		return fields
	}
	fields = append(fields,
		zap.Int(prefix+"-total", snapshot.totalCount),
		zap.Int(prefix+"-nonnull", snapshot.nonNullCount),
		zap.String(prefix+"-pair-sample", snapshot.pairSample),
	)
	return fields
}

func (tbl *txnTable) logCompositePKFindDeletesUnmasked(
	phase string,
	lookupFrom types.TS,
	lookupTo types.TS,
	maskTo types.TS,
	before dedupTraceSnapshot,
	after dedupTraceSnapshot,
) {
	fields := []zap.Field{
		zap.Uint64("table-id", tbl.GetID()),
		zap.String("table", tbl.dataTable.schema.Name),
		zap.String("txn", tbl.store.txn.String()),
		zap.String("phase", phase),
		zap.String("lookup-from", lookupFrom.ToString()),
		zap.String("lookup-to", lookupTo.ToString()),
		zap.String("mask-to", maskTo.ToString()),
	}
	fields = appendDedupSnapshotFields(fields, "before", before)
	fields = appendDedupSnapshotFields(fields, "after", after)
	logutil.Warn("TN-COMPOSITE-PK-FINDDELETES-UNMASKED", fields...)
}

func (tbl *txnTable) logCompositePKLocalTombstoneMask(
	phase string,
	lookupFrom types.TS,
	lookupTo types.TS,
	maskTo types.TS,
	before dedupTraceSnapshot,
	after dedupTraceSnapshot,
) {
	fields := []zap.Field{
		zap.Uint64("table-id", tbl.GetID()),
		zap.String("table", tbl.dataTable.schema.Name),
		zap.String("txn", tbl.store.txn.String()),
		zap.String("phase", phase),
		zap.String("lookup-from", lookupFrom.ToString()),
		zap.String("lookup-to", lookupTo.ToString()),
		zap.String("mask-to", maskTo.ToString()),
	}
	fields = appendDedupSnapshotFields(fields, "before", before)
	fields = appendDedupSnapshotFields(fields, "after", after)
	logutil.Info("TN-COMPOSITE-PK-LOCAL-TOMBSTONE-MASK", fields...)
}

func (tbl *txnTable) logDedupDuplicateDetails(
	pks containers.Vector,
	rowIDs containers.Vector,
	isTombstone bool,
	phase string,
	lookupFrom types.TS,
	lookupTo types.TS,
	maskTo types.TS,
	duplicateIdx int,
	before dedupTraceSnapshot,
	after dedupTraceSnapshot,
) {
	compositePK := !isTombstone && tbl.hasHiddenCompositePrimaryKey(false)
	current := captureDedupTraceSnapshot(pks, rowIDs, compositePK)
	fields := []zap.Field{
		zap.Uint64("table-id", tbl.GetID()),
		zap.String("table", tbl.dataTable.schema.Name),
		zap.Bool("is-tombstone", isTombstone),
		zap.String("txn", tbl.store.txn.String()),
		zap.String("phase", phase),
		zap.String("lookup-from", lookupFrom.ToString()),
		zap.String("lookup-to", lookupTo.ToString()),
		zap.Int("duplicate-idx", duplicateIdx),
		zap.String("duplicate-pk", formatDedupTraceValue(pks, duplicateIdx, compositePK)),
		zap.String("duplicate-rowid", formatDedupTraceValue(rowIDs, duplicateIdx, false)),
	}
	if !maskTo.IsEmpty() {
		fields = append(fields, zap.String("mask-to", maskTo.ToString()))
	}
	fields = appendDedupSnapshotFields(fields, "current", current)
	fields = appendDedupSnapshotFields(fields, "before", before)
	fields = appendDedupSnapshotFields(fields, "after", after)
	logutil.Error("TN-DEDUP-DUPLICATE-DETAIL", fields...)
}
