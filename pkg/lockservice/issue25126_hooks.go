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

package lockservice

import (
	"math/rand"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"go.uber.org/zap"
)

const (
	issue25126HookValidTxnRandomSleep        = "issue25126.lockservice.valid_txn.random_sleep"
	issue25126HookValidTxnForceInvalid       = "issue25126.lockservice.valid_txn.force_invalid"
	issue25126HookCannotCommitRandomSleep    = "issue25126.lockservice.cannot_commit.random_sleep"
	issue25126HookCannotCommitForceUnlock    = "issue25126.lockservice.cannot_commit.force_unlock"
	issue25126HookOrphanUnlockRandomSleep    = "issue25126.lockservice.orphan_unlock.random_sleep"
	issue25126HookStaleBindUnlockRandomSleep = "issue25126.lockservice.stale_bind_unlock.random_sleep"
)

func triggerIssue25126Hook(
	logger *log.MOLogger,
	name string,
	fields ...zap.Field,
) (int64, string, bool) {
	iarg, sarg, ok := fault.TriggerFault(name)
	if ok && logger != nil {
		logger.Warn("issue25126 lockservice hook triggered",
			append([]zap.Field{
				zap.String("hook", name),
				zap.Int64("iarg", iarg),
				zap.String("sarg", sarg),
			}, fields...)...)
	}
	return iarg, sarg, ok
}

func randomSleepIssue25126Hook(
	logger *log.MOLogger,
	name string,
	fields ...zap.Field,
) bool {
	maxMillis, _, ok := triggerIssue25126Hook(logger, name, fields...)
	if !ok || maxMillis <= 0 {
		return ok
	}

	sleep := time.Duration(rand.Int63n(maxMillis+1)) * time.Millisecond
	if sleep == 0 {
		return true
	}
	if logger != nil {
		logger.Warn("issue25126 lockservice hook sleep",
			append([]zap.Field{
				zap.String("hook", name),
				zap.Duration("sleep", sleep),
			}, fields...)...)
	}
	time.Sleep(sleep)
	return true
}
