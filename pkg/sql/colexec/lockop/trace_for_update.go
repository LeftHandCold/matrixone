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

package lockop

import (
	"encoding/hex"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

func traceForUpdateInput(proc *process.Process, target lockTarget, bat *batch.Batch) {
	dbName, tableName, ok, level := getTraceForUpdateTarget(target)
	if !ok {
		return
	}

	fields := []zap.Field{
		zap.String("db", dbName),
		zap.String("table", tableName),
		zap.Uint64("table-id", target.tableID),
		zap.String("txn", traceForUpdateTxnID(proc)),
		zap.String("snapshot-ts", proc.GetTxnOperator().Txn().SnapshotTS.DebugString()),
		zap.Int32("primary-index", target.primaryColumnIndexInBatch),
		zap.Int32("refresh-ts-index", target.refreshTimestampIndexInBatch),
	}
	if bat != nil {
		fields = append(fields, zap.Int("rows", bat.RowCount()))
		if level >= objectio.FJ_LogLevel1 {
			fields = append(fields, zap.String("batch", common.MoBatchToString(bat, 16)))
		}
	}
	logutil.Info("RC-FOR-UPDATE-INPUT", fields...)
}

func traceForUpdateResult(
	proc *process.Process,
	target lockTarget,
	locked bool,
	defChanged bool,
	refreshTS timestamp.Timestamp,
	err error,
) {
	dbName, tableName, ok, _ := getTraceForUpdateTarget(target)
	if !ok {
		return
	}

	logutil.Info("RC-FOR-UPDATE-LOCK-RESULT",
		zap.String("db", dbName),
		zap.String("table", tableName),
		zap.Uint64("table-id", target.tableID),
		zap.String("txn", traceForUpdateTxnID(proc)),
		zap.String("snapshot-ts", proc.GetTxnOperator().Txn().SnapshotTS.DebugString()),
		zap.Bool("locked", locked),
		zap.Bool("def-changed", defChanged),
		zap.String("refresh-ts", refreshTS.DebugString()),
		zap.Error(err),
	)
}

func getTraceForUpdateTarget(target lockTarget) (string, string, bool, int) {
	if target.objRef == nil {
		return "", "", false, 0
	}
	dbName := target.objRef.SchemaName
	if dbName == "" {
		dbName = target.objRef.DbName
	}
	tableName := target.objRef.ObjName
	ok, level := objectio.TraceForUpdateInjected(dbName, tableName)
	return dbName, tableName, ok, level
}

func traceForUpdateTxnID(proc *process.Process) string {
	return hex.EncodeToString(proc.GetTxnOperator().Txn().ID)
}
