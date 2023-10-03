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

package incrservice

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"go.uber.org/zap"
	"math"
)

var (
	database      = "mo_catalog"
	incrTableName = "mo_increment_columns"
)

func (c AutoColumn) getInsertSQL() string {
	return fmt.Sprintf(`insert into %s(table_id, col_name, col_index, offset, step) 
		values(%d, '%s', %d, %d, %d)`,
		incrTableName,
		c.TableID,
		c.ColName,
		c.ColIndex,
		c.Offset,
		c.Step)
}

type sqlStore struct {
	exec executor.SQLExecutor
}

func NewSQLStore(exec executor.SQLExecutor) (IncrValueStore, error) {
	return &sqlStore{exec: exec}, nil
}

func (s *sqlStore) Create(
	ctx context.Context,
	tableID uint64,
	cols []AutoColumn,
	txnOp client.TxnOperator) error {
	opts := executor.Options{}.WithDatabase(database).WithTxn(txnOp)
	if txnOp != nil {
		opts = opts.WithDisableIncrStatement()
	}

	return s.exec.ExecTxn(
		ctx,
		func(te executor.TxnExecutor) error {
			for _, col := range cols {
				res, err := te.Exec(col.getInsertSQL())
				if err != nil {
					return err
				}
				res.Close()
			}
			return nil
		},
		opts)
}

func (s *sqlStore) Allocate(
	ctx context.Context,
	tableID uint64,
	colName string,
	count int,
	txnOp client.TxnOperator) (uint64, uint64, error) {
	var current, next, step uint64
	ok := false

	fetchSQL := fmt.Sprintf(`select offset, step from %s where table_id = %d and col_name = '%s' for update`,
		incrTableName,
		tableID,
		colName)
	opts := executor.Options{}.
		WithDatabase(database).
		WithTxn(txnOp).
		WithWaitCommittedLogApplied() // make sure the update is visible to the subsequence txn, wait log tail applied
	if txnOp != nil {
		opts = opts.WithDisableIncrStatement()
	}
	for {
		err := s.exec.ExecTxn(
			ctx,
			func(te executor.TxnExecutor) error {
				/*_, sarg, _ := fault.TriggerFault("lock_deadline")
				if sarg != "" {
					logutil.Infof("lock_deadline fault: %s, tableid: %d", sarg, tableID)
					time.Sleep(10 * time.Second)
				}*/
				res, err := te.Exec(fetchSQL)
				if err != nil {
					logutil.Infof("fetchSQL: %s, err: %v", fetchSQL, err)
					return err
				}
				rows := 0
				res.ReadRows(func(cols []*vector.Vector) bool {
					current = executor.GetFixedRows[uint64](cols[0])[0]
					step = executor.GetFixedRows[uint64](cols[1])[0]
					rows += len(executor.GetFixedRows[uint64](cols[0]))
					return true
				})
				res.Close()

				if rows != 1 {
					getLogger().Info("BUG: read incr record invalid",
						zap.String("fetch-sql", fetchSQL),
						zap.Any("account", ctx.Value(defines.TenantIDKey{})),
						zap.Uint64("table", tableID),
						zap.String("col", colName),
						zap.Int("rows", rows))

					fetchNoUpdateSQL := fmt.Sprintf(`select offset, step from %s where table_id = %d and col_name = '%s'`,
						incrTableName,
						tableID,
						colName)
					res, err := te.Exec(fetchNoUpdateSQL)
					if err != nil {
						return err
					}
					rows = 0
					res.ReadRows(func(cols []*vector.Vector) bool {
						current = executor.GetFixedRows[uint64](cols[0])[0]
						step = executor.GetFixedRows[uint64](cols[1])[0]
						rows += len(executor.GetFixedRows[uint64](cols[0]))
						return true
					})
					res.Close()
					getLogger().Info("After BUG: read incr record invalid, fetch without for update",
						zap.String("fetch-sql", fetchNoUpdateSQL),
						zap.Any("account", ctx.Value(defines.TenantIDKey{})),
						zap.Uint64("table", tableID),
						zap.String("col", colName),
						zap.Int("rows", rows))

					fetchAllSQL := fmt.Sprintf(`select offset, step, table_id, col_name from %s`,
						incrTableName)
					getLogger().Info("After BUG: read incr record invalid, begin to fetch all data",
						zap.String("fetch-sql", fetchAllSQL))
					res, err = te.Exec(fetchAllSQL)
					if err != nil {
						return err
					}
					getLogger().Info("After BUG: read incr record invalid, finish fetch all data",
						zap.String("fetch-sql", fetchAllSQL))
					rows = 0
					res.ReadRows(func(cols []*vector.Vector) bool {
						getLogger().Info("After BUG: read incr record invalid, begin to read rows for fetching all data")
						for i := 0; i < len(executor.GetFixedRows[uint64](cols[0])); i++ {
							current = executor.GetFixedRows[uint64](cols[0])[i]
							step = executor.GetFixedRows[uint64](cols[1])[i]
							tid := executor.GetFixedRows[uint64](cols[2])[i]
							coln := executor.GetStringRows(cols[3])[i]
							getLogger().Info("After BUG: read incr record invalid, fetch all data",
								zap.Uint64("current", current),
								zap.Uint64("step", step),
								zap.Uint64("tid", tid),
								zap.String("colName", coln))
							rows++
						}
						return true
					})
					txnOp2 := res.Txn
					res.Close()

					// read all data from blocks
					fs := s.exec.GetFileService()
					blocks := txnOp2.(client.TxnOperatorWithBlocks).GetAllBlocks().([]catalog.BlockInfo)
					for _, b := range blocks {
						_, err := blockio.DebugDataWithBlockID(ctx, fs, b.BlockID.String())
						if err != nil {
							getLogger().Error("read block data failed",
								zap.String("block", b.String()),
								zap.Error(err))
						}
					}

					// read mem data
					state := txnOp2.(client.TxnOperatorWithBlocks).GetPartitionState().(*logtailreplay.PartitionState)
					iter := state.NewRowsIter2(types.TimestampToTS(timestamp.Timestamp{PhysicalTime: math.MaxInt64}), nil, false)
					for {
						if !iter.Next() {
							break
						}

						e := iter.Entry()
						v1ColIndex := 0
						v2ColIndex := 0
						for i, attr := range e.Batch.Attrs {
							if attr == "table_id" {
								v1ColIndex = i
							}
							if attr == "col_name" {
								v2ColIndex = i
							}
						}

						tid := executor.GetFixedRows[uint64](e.Batch.Vecs[v1ColIndex])[e.Offset]
						col := executor.GetStringRows(e.Batch.Vecs[v2ColIndex])[e.Offset]
						logutil.Infof("%x: read mem [%d, %s, %s]", txnOp2.Txn().ID, tid, col, e.Time.ToTimestamp().DebugString())
					}

					getLogger().Fatal("BUG: read incr record invalid",
						zap.String("fetch-sql", fetchSQL),
						zap.Any("account", ctx.Value(defines.TenantIDKey{})),
						zap.Uint64("table", tableID),
						zap.String("col", colName),
						zap.Int("rows", rows),
						zap.Int("blocks", len(blocks)),
						zap.String("txn", res.Txn.Txn().DebugString()))
				}

				next = getNext(current, count, int(step))
				sql := fmt.Sprintf(`update %s set offset = %d 
				where table_id = %d and col_name = '%s' and offset = %d`,
					incrTableName,
					next,
					tableID,
					colName,
					current)
				res, err = te.Exec(sql)
				if err != nil {
					return err
				}

				if res.AffectedRows == 1 {
					ok = true
				} else {
					getLogger().Fatal("BUG: update incr record returns invalid affected rows",
						zap.String("update-sql", sql),
						zap.Any("account", ctx.Value(defines.TenantIDKey{})),
						zap.Uint64("table", tableID),
						zap.String("col", colName),
						zap.Uint64("affected-rows", res.AffectedRows),
						zap.String("txn", res.Txn.Txn().DebugString()))
				}
				res.Close()
				return nil
			},
			opts)
		if err != nil {
			return 0, 0, err
		}
		if ok {
			break
		}
	}

	from, to := getNextRange(current, next, int(step))
	return from, to, nil
}

func (s *sqlStore) UpdateMinValue(
	ctx context.Context,
	tableID uint64,
	col string,
	minValue uint64,
	txnOp client.TxnOperator) error {
	opts := executor.Options{}.WithDatabase(database).WithTxn(txnOp)
	// txnOp is nil means the auto increment metadata is already insert into catalog.MOAutoIncrTable and committed.
	// So updateMinValue will use a new txn to update the min value. To avoid w-w conflict, we need to wait this
	// committed log tail applied to ensure subsequence txn must get a snapshot ts which is large than this commit.
	if txnOp == nil {
		opts = opts.WithWaitCommittedLogApplied()
	} else {
		opts = opts.WithDisableIncrStatement()
	}
	res, err := s.exec.Exec(
		ctx,
		fmt.Sprintf("update %s set offset = %d where table_id = %d and col_name = '%s' and offset < %d",
			incrTableName,
			minValue,
			tableID,
			col,
			minValue),
		opts)
	if err != nil {
		return err
	}
	defer res.Close()
	return nil
}

func (s *sqlStore) Delete(
	ctx context.Context,
	tableID uint64) error {
	opts := executor.Options{}.
		WithDatabase(database).
		WithWaitCommittedLogApplied()
	res, err := s.exec.Exec(
		ctx,
		fmt.Sprintf("delete from %s where table_id = %d",
			incrTableName, tableID),
		opts)
	if err != nil {
		return err
	}
	defer res.Close()
	return nil
}

func (s *sqlStore) GetColumns(
	ctx context.Context,
	tableID uint64,
	txnOp client.TxnOperator) ([]AutoColumn, error) {
	fetchSQL := fmt.Sprintf(`select col_name, col_index, offset, step from %s where table_id = %d order by col_index`,
		incrTableName,
		tableID)
	opts := executor.Options{}.WithDatabase(database).WithTxn(txnOp)
	if txnOp != nil {
		opts = opts.WithDisableIncrStatement()
	}

	res, err := s.exec.Exec(ctx, fetchSQL, opts)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var colNames []string
	var indexes []int32
	var offsets []uint64
	var steps []uint64
	res.ReadRows(func(cols []*vector.Vector) bool {
		colNames = append(colNames, executor.GetStringRows(cols[0])...)
		indexes = append(indexes, executor.GetFixedRows[int32](cols[1])...)
		offsets = append(offsets, executor.GetFixedRows[uint64](cols[2])...)
		steps = append(steps, executor.GetFixedRows[uint64](cols[3])...)
		return true
	})

	cols := make([]AutoColumn, len(colNames))
	for idx, colName := range colNames {
		cols[idx] = AutoColumn{
			TableID:  tableID,
			ColName:  colName,
			ColIndex: int(indexes[idx]),
			Offset:   offsets[idx],
			Step:     steps[idx],
		}
	}
	return cols, nil
}

func (s *sqlStore) Close() {

}
