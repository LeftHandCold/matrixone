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
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"go.uber.org/zap"
)

type tombstoneDuplicateSourceStats struct {
	localNodeRows               int
	localNodeHits               int
	localStatsTotal             int
	localStatsHits              int
	localStatsMatchedObjects    int
	localStatsObjectSample      string
	visibleObjectTotal          int
	visibleObjectHits           int
	visibleObjectMatchedObjects int
	visibleObjectSample         string
	err                         string
}

func (tbl *txnTable) logTombstoneTransferBatchDuplicate(
	phase string,
	from types.TS,
	to types.TS,
	sourceObject *catalog.ObjectEntry,
	batch *containers.Batch,
) {
	if batch == nil {
		return
	}
	rowIDVec := batch.GetVectorByName(objectio.TombstoneAttr_Rowid_Attr)
	if rowIDVec == nil {
		return
	}
	rowIDs := vector.MustFixedColWithTypeCheck[types.Rowid](rowIDVec.GetDownstreamVector())
	if len(rowIDs) == 0 {
		return
	}
	seen := make(map[types.Rowid]int, len(rowIDs))
	sample := make([]string, 0, dedupTraceSampleLimit)
	duplicateRowIDs := 0
	for _, rowID := range rowIDs {
		seen[rowID]++
		if seen[rowID] != 2 {
			continue
		}
		duplicateRowIDs++
		if len(sample) < dedupTraceSampleLimit {
			sample = append(sample, (&rowID).ShortStringEx())
		}
	}
	if duplicateRowIDs == 0 {
		return
	}
	fields := []zap.Field{
		zap.Uint64("table-id", tbl.GetID()),
		zap.String("table", tbl.dataTable.schema.Name),
		zap.String("txn", tbl.store.txn.String()),
		zap.String("phase", phase),
		zap.String("from", from.ToString()),
		zap.String("to", to.ToString()),
		zap.Int("batch-rows", len(rowIDs)),
		zap.Int("duplicate-rowid-count", duplicateRowIDs),
		zap.String("duplicate-rowid-sample", "["+strings.Join(sample, ", ")+"]"),
	}
	if sourceObject != nil {
		fields = append(fields,
			zap.String("source-object-id", sourceObject.ID().ShortStringEx()),
			zap.String("source-object-name", sourceObject.ObjectName().String()),
		)
	}
	logutil.Warn("TN-TOMBSTONE-TRANSFER-BATCH-DUPLICATE", fields...)
}

func (tbl *txnTable) collectTombstoneDuplicateSourceStats(
	ctx context.Context,
	rowID types.Rowid,
	lookupTo types.TS,
) tombstoneDuplicateSourceStats {
	summary := tombstoneDuplicateSourceStats{
		localStatsObjectSample: "[]",
		visibleObjectSample:    "[]",
	}
	if tbl.tombstoneTable == nil || tbl.tombstoneTable.tableSpace == nil {
		return summary
	}
	space := tbl.tombstoneTable.tableSpace
	if space.node != nil {
		nodeRowIDs := vector.MustFixedColWithTypeCheck[types.Rowid](
			space.node.data.GetVectorByName(objectio.TombstoneAttr_Rowid_Attr).GetDownstreamVector(),
		)
		summary.localNodeRows = len(nodeRowIDs)
		for _, candidate := range nodeRowIDs {
			if candidate == rowID {
				summary.localNodeHits++
			}
		}
	}
	summary.localStatsTotal = len(space.stats)
	if len(space.stats) != 0 {
		localResult, err := tbl.countExactTombstoneRowIDInStatsList(ctx, rowID, space.stats)
		if err != nil {
			summary.err = err.Error()
			return summary
		}
		summary.localStatsHits = localResult.hits
		summary.localStatsMatchedObjects = localResult.matchedObjects
		summary.localStatsObjectSample = localResult.objectSample
	}

	tbl.entry.WaitTombstoneObjectCommitted(lookupTo)
	visibleStats := make([]objectio.ObjectStats, 0)
	it := tbl.entry.MakeTombstoneObjectIt()
	defer it.Release()
	for ok := it.Last(); ok; ok = it.Prev() {
		obj := it.Item()
		if obj.IsLocal || !obj.VisibleByTS(lookupTo) {
			continue
		}
		visibleStats = append(visibleStats, obj.ObjectStats)
	}
	summary.visibleObjectTotal = len(visibleStats)
	if len(visibleStats) == 0 {
		return summary
	}
	visibleResult, err := tbl.countExactTombstoneRowIDInStatsList(ctx, rowID, visibleStats)
	if err != nil {
		summary.err = err.Error()
		return summary
	}
	summary.visibleObjectHits = visibleResult.hits
	summary.visibleObjectMatchedObjects = visibleResult.matchedObjects
	summary.visibleObjectSample = visibleResult.objectSample
	return summary
}

type exactTombstoneRowIDSearchResult struct {
	hits           int
	matchedObjects int
	objectSample   string
}

func (tbl *txnTable) countExactTombstoneRowIDInStatsList(
	ctx context.Context,
	rowID types.Rowid,
	statsList []objectio.ObjectStats,
) (result exactTombstoneRowIDSearchResult, err error) {
	result.objectSample = "[]"
	if len(statsList) == 0 {
		return
	}
	sel, err := ioutil.FindTombstonesOfObject(ctx, rowID.BorrowObjectID(), statsList, tbl.store.rt.Fs)
	if err != nil {
		return
	}
	iter := sel.Iterator()
	sample := make([]string, 0, dedupTraceSampleLimit)
	seen := make(map[string]struct{})
	for iter.HasNext() {
		statsOffset := iter.Next()
		stats := statsList[statsOffset]
		hits, err := countExactTombstoneRowIDInStats(
			ctx,
			rowID,
			stats,
			tbl.tombstoneTable.schema.Extra.BlockMaxRows,
			tbl.store.rt.Fs,
		)
		if err != nil {
			return result, err
		}
		if hits == 0 {
			continue
		}
		result.hits += hits
		result.matchedObjects++
		name := stats.ObjectName().String()
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		if len(sample) < dedupTraceSampleLimit {
			sample = append(sample, name)
		}
	}
	result.objectSample = "[" + strings.Join(sample, ", ") + "]"
	return
}

func countExactTombstoneRowIDInStats(
	ctx context.Context,
	rowID types.Rowid,
	stats objectio.ObjectStats,
	blkMaxRows uint32,
	fs fileservice.FileService,
) (hits int, err error) {
	for i := uint16(0); i < uint16(stats.BlkCnt()); i++ {
		loc := stats.BlockLocation(i, blkMaxRows)
		vectors, closeFunc, err := ioutil.LoadColumns2(
			ctx,
			[]uint16{0},
			nil,
			fs,
			loc,
			fileservice.Policy(0),
			false,
			nil,
		)
		if err != nil {
			closeFunc()
			return 0, err
		}
		rowIDs := vector.MustFixedColWithTypeCheck[types.Rowid](vectors[0].GetDownstreamVector())
		for _, candidate := range rowIDs {
			if candidate == rowID {
				hits++
			}
		}
		closeFunc()
	}
	return
}
