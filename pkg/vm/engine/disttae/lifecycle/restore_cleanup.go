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
	"context"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type ExpiredRestoreAttempt struct {
	AccountID          uint32
	RestoreID          string
	TargetDatabaseName string
}

// SQLExpiredRestorePager finds only deadline-expired hidden-table owners.
// Cleanup itself remains an ordinary tenant transaction in SQLRestoreRepository.
type SQLExpiredRestorePager struct {
	Executor executor.SQLExecutor
}

func (pager SQLExpiredRestorePager) Next(
	ctx context.Context,
	afterAccountID uint32,
	now time.Time,
	maxAccounts int,
	maxAttempts int,
) ([]ExpiredRestoreAttempt, uint32, error) {
	if pager.Executor == nil ||
		now.IsZero() ||
		maxAccounts <= 0 ||
		maxAttempts <= 0 {
		return nil, afterAccountID, fmt.Errorf(
			"Lifecycle expired Restore pager is incomplete",
		)
	}
	accounts, err := (SQLMetadataCompactor{Executor: pager.Executor}).
		listAccounts(ctx, afterAccountID, maxAccounts)
	if err != nil {
		return nil, afterAccountID, err
	}
	if len(accounts) == 0 && afterAccountID != 0 {
		accounts, err = (SQLMetadataCompactor{Executor: pager.Executor}).
			listAccounts(ctx, 0, maxAccounts)
		if err != nil {
			return nil, afterAccountID, err
		}
	}
	attempts := make([]ExpiredRestoreAttempt, 0, min(maxAttempts, 64))
	next := afterAccountID
	for _, accountID := range accounts {
		remaining := maxAttempts - len(attempts)
		if remaining == 0 {
			break
		}
		result, queryErr := pager.Executor.Exec(
			ctx,
			fmt.Sprintf(
				`select hex(a.restore_id),coalesce(d.datname,'')
from mo_catalog.mo_lifecycle_restore_attempts a
left join mo_catalog.mo_database d on d.dat_id=a.staging_database_id
where a.state in ('IMPORTING','PUBLISHING') and a.deadline<=%s
order by a.deadline,a.restore_id limit %d`,
				lifecycleSQLTime(now),
				remaining,
			),
			executor.Options{}.WithAccountID(accountID),
		)
		if queryErr != nil {
			return nil, afterAccountID, queryErr
		}
		var decodeErr error
		result.ReadRows(func(rows int, columns []*vector.Vector) bool {
			if len(columns) != 2 {
				decodeErr = fmt.Errorf(
					"Lifecycle expired Restore query is invalid",
				)
				return false
			}
			for row := 0; row < rows; row++ {
				restoreID, idErr := lifecycleUUIDFromHex(
					columns[0].GetStringAt(row),
				)
				if idErr != nil {
					decodeErr = idErr
					return false
				}
				attempts = append(attempts, ExpiredRestoreAttempt{
					AccountID:          accountID,
					RestoreID:          restoreID,
					TargetDatabaseName: columns[1].GetStringAt(row),
				})
			}
			return true
		})
		result.Close()
		if decodeErr != nil {
			return nil, afterAccountID, decodeErr
		}
		next = accountID
	}
	return attempts, next, nil
}
