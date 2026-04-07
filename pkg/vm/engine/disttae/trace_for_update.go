// Copyright 2023 Matrix Origin
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
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"go.uber.org/zap"
)

func traceForUpdatePKCheck(
	tbl *txnTable,
	level int,
	from, to types.TS,
	snap *logtailreplay.PartitionState,
	keys [][]byte,
	path string,
	inMemChanged bool,
	inMemFlushed bool,
	changed bool,
	err error,
) {
	fields := []zap.Field{
		zap.String("db", tbl.db.databaseName),
		zap.String("table", tbl.tableName),
		zap.Uint64("table-id", tbl.tableId),
		zap.String("txn", traceForUpdateTxnID(tbl)),
		zap.String("txn-snapshot-ts", tbl.db.op.Txn().SnapshotTS.DebugString()),
		zap.String("from", from.ToString()),
		zap.String("to", to.ToString()),
		zap.String("path", path),
		zap.Bool("in-mem-changed", inMemChanged),
		zap.Bool("in-mem-flushed", inMemFlushed),
		zap.Bool("changed", changed),
		zap.Error(err),
	}
	if snap != nil {
		fields = append(fields,
			zap.String("ps", fmt.Sprintf("%p", snap)),
			zap.String("ps-start", snap.GetStart().ToString()),
			zap.String("ps-end", snap.GetEnd().ToString()),
		)
	}
	if len(keys) > 0 {
		fields = append(fields, zap.Int("key-count", len(keys)))
		if level >= objectio.FJ_LogLevel1 {
			fields = append(fields, zap.String("key-sample", traceForUpdateKeySample(keys, 4)))
		}
	}
	logutil.Info("RC-FOR-UPDATE-PK-CHECK", fields...)
}

func traceForUpdateEnabled(tbl *txnTable) (bool, int) {
	return objectio.TraceForUpdateInjected(tbl.db.databaseName, tbl.tableName)
}

func traceForUpdateTxnID(tbl *txnTable) string {
	return hex.EncodeToString(tbl.db.op.Txn().ID)
}

func traceForUpdateKeySample(keys [][]byte, limit int) string {
	if len(keys) == 0 {
		return ""
	}
	if limit <= 0 || limit > len(keys) {
		limit = len(keys)
	}
	parts := make([]string, 0, limit+1)
	for i := 0; i < limit; i++ {
		parts = append(parts, hex.EncodeToString(keys[i]))
	}
	if len(keys) > limit {
		parts = append(parts, fmt.Sprintf("...(%d total)", len(keys)))
	}
	return strings.Join(parts, ",")
}
