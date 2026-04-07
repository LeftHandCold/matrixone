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

package logtailreplay

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"go.uber.org/zap"
)

func traceForUpdateApply(p *PartitionState, entry *api.Entry) {
	ok, level := objectio.TraceForUpdateInjected(entry.DatabaseName, entry.TableName)
	if !ok {
		return
	}

	fields := []zap.Field{
		zap.String("db", entry.DatabaseName),
		zap.String("table", entry.TableName),
		zap.Uint64("table-id", p.tid),
		zap.String("ps", fmt.Sprintf("%p", p)),
		zap.String("entry-type", entry.EntryType.String()),
	}
	if entry.Bat != nil {
		data, err := batch.ProtoBatchToBatch(entry.Bat)
		if err != nil {
			fields = append(fields, zap.Error(err))
		} else {
			fields = append(fields, zap.Int("rows", data.RowCount()))
			if level >= objectio.FJ_LogLevel1 {
				fields = append(fields, zap.String("batch", common.MoBatchToString(data, 16)))
			}
		}
	}
	logutil.Info("RC-FOR-UPDATE-PS-APPLY", fields...)
}
