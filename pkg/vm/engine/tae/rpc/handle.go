// Copyright 2021 - 2022 Matrix Origin
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

package rpc

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
	"os"
	"syscall"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/moengine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
)

type Handle struct {
	eng     moengine.TxnEngine
	rels    map[memoryengine.ID]moengine.Relation
	readers map[memoryengine.ID]engine.Reader
}

func (h *Handle) GetTxnEngine() moengine.TxnEngine {
	return h.eng
}

func NewTAEHandle(opt *options.Options) *Handle {
	//just for test
	path := "./store"
	tae, err := openTAE(path, opt)
	if err != nil {
		panic(err)
	}

	h := &Handle{
		eng:     moengine.NewEngine(tae),
		rels:    make(map[memoryengine.ID]moengine.Relation),
		readers: make(map[memoryengine.ID]engine.Reader),
	}
	return h
}
func (h *Handle) HandleCommit(meta txn.TxnMeta) (err error) {
	var txn moengine.Txn
	txn, err = h.eng.GetTxnByID(meta.GetID())
	if err != nil {
		return err
	}
	if txn.Is2PC() {
		txn.SetCommitTS(types.TimestampToTS(meta.GetCommitTS()))
	}
	err = txn.Commit()
	return
}

func (h *Handle) HandleRollback(meta txn.TxnMeta) (err error) {
	var txn moengine.Txn
	txn, err = h.eng.GetTxnByID(meta.GetID())
	if err != nil {
		return err
	}
	err = txn.Rollback()
	return
}

func (h *Handle) HandleCommitting(meta txn.TxnMeta) (err error) {
	var txn moengine.Txn
	txn, err = h.eng.GetTxnByID(meta.GetID())
	if err != nil {
		return err
	}
	txn.SetCommitTS(types.TimestampToTS(meta.GetCommitTS()))
	err = txn.Committing()
	return
}

func (h *Handle) HandlePrepare(meta txn.TxnMeta) (pts timestamp.Timestamp, err error) {
	var txn moengine.Txn
	txn, err = h.eng.GetTxnByID(meta.GetID())
	if err != nil {
		return timestamp.Timestamp{}, err
	}
	participants := make([]uint64, 0, len(meta.GetDNShards()))
	for _, shard := range meta.GetDNShards() {
		participants = append(participants, shard.GetShardID())
	}
	txn.SetParticipants(participants)
	var ts types.TS
	ts, err = txn.Prepare()
	pts = ts.ToTimestamp()
	return
}

func (h *Handle) HandleStartRecovery(ch chan txn.TxnMeta) {
	//panic(moerr.NewNYI("HandleStartRecovery is not implemented yet"))
	//TODO:: TAE replay
	close(ch)
}

func (h *Handle) HandleClose() (err error) {
	return h.eng.Close()
}

func (h *Handle) HandleDestroy() (err error) {
	return h.eng.Destroy()
}

func (h *Handle) HandleGetLogTail(
	meta txn.TxnMeta,
	req apipb.SyncLogTailReq,
	resp *apipb.SyncLogTailResp) (err error) {
	tae := h.eng.GetTAE(context.Background())
	res, err := logtail.HandleSyncLogTailReq(tae.LogtailMgr, tae.Catalog, req)
	if err != nil {
		return err
	}
	*resp = res
	return nil
}

// TODO:: need to handle resp.
func (h *Handle) HandlePreCommit(
	meta txn.TxnMeta,
	req apipb.PrecommitWriteCmd,
	resp *apipb.SyncLogTailResp) (err error) {
	var e any

	es := req.EntryList
	for len(es) > 0 {
		e, es, err = catalog.ParseEntryList(es)
		if err != nil {
			return err
		}
		switch cmds := e.(type) {
		case []catalog.CreateDatabase:
			for _, cmd := range cmds {
				req := db.CreateDatabaseReq{
					Name: cmd.Name,
					AccessInfo: db.AccessInfo{
						UserID:    cmd.Owner,
						RoleID:    cmd.Creator,
						AccountID: cmd.AccountId,
					},
				}
				if err = h.HandleCreateDatabase(meta, req,
					new(db.CreateDatabaseResp)); err != nil {
					return err
				}
			}
		case []catalog.CreateTable:
			for _, cmd := range cmds {
				req := db.CreateRelationReq{
					AccessInfo: db.AccessInfo{
						UserID:    req.UserId,
						RoleID:    req.RoleId,
						AccountID: req.AccountId,
					},
					Name:         cmd.Name,
					DatabaseName: cmd.DatabaseName,
					DatabaseID:   cmd.DatabaseId,
					Defs:         cmd.Defs,
				}
				if err = h.HandleCreateRelation(meta, req,
					new(db.CreateRelationResp)); err != nil {
					return err
				}
			}
		case []catalog.DropDatabase:
			for _, cmd := range cmds {
				req := db.DropDatabaseReq{
					Name: cmd.Name,
					ID:   cmd.Id,
					AccessInfo: db.AccessInfo{
						UserID:    req.UserId,
						RoleID:    req.RoleId,
						AccountID: req.AccountId,
					},
				}
				if err = h.HandleDropDatabase(meta, req,
					new(db.DropDatabaseResp)); err != nil {
					return err
				}
			}
		case []catalog.DropTable:
			for _, cmd := range cmds {
				req := db.DropRelationReq{
					AccessInfo: db.AccessInfo{
						UserID:    req.UserId,
						RoleID:    req.RoleId,
						AccountID: req.AccountId,
					},
					Name:         cmd.Name,
					ID:           cmd.Id,
					DatabaseName: cmd.DatabaseName,
					DatabaseID:   cmd.DatabaseId,
				}
				if err = h.HandleDropRelation(meta, req,
					new(db.DropRelationResp)); err != nil {
					return err
				}
			}
		case *apipb.Entry:
			//Handle DML
			pe := e.(*apipb.Entry)
			moBat, err := batch.ProtoBatchToBatch(pe.GetBat())
			if err != nil {
				panic(err)
			}
			req := db.WriteReq{
				AccessInfo: db.AccessInfo{
					UserID:    req.UserId,
					RoleID:    req.RoleId,
					AccountID: req.AccountId,
				},
				Type:         db.EntryType(pe.EntryType),
				TableID:      pe.GetTableId(),
				DatabaseName: pe.GetDatabaseName(),
				TableName:    pe.GetTableName(),
				Batch:        moBat,
			}
			if err = h.HandleWrite(meta, req,
				new(db.WriteResp)); err != nil {
				return err
			}
		default:
			panic(moerr.NewNYI(""))
		}
	}
	return nil

}

func (h *Handle) HandleOpenDatabase(
	ctx context.Context,
	meta txn.TxnMeta,
	req memoryengine.OpenDatabaseReq,
	resp *memoryengine.OpenDatabaseResp) (err error) {
	txn, err := h.eng.GetOrCreateTxnWithMeta(nil, meta.GetID(),
		types.TimestampToTS(meta.GetSnapshotTS()))
	if err != nil {
		return err
	}
	//	ctx := context.Background()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, req.AccessInfo.AccountID)
	ctx = context.WithValue(ctx, defines.UserIDKey{}, req.AccessInfo.UserID)
	ctx = context.WithValue(ctx, defines.RoleIDKey{}, req.AccessInfo.RoleID)
	db, err := h.eng.GetDatabase(ctx, req.Name, txn)
	if err != nil {
		return err
	}
	id := db.GetDatabaseID(ctx)
	resp.ID = memoryengine.ID(id)
	resp.Name = req.Name
	return nil
}

func (h *Handle) HandleOpenRelation(
	ctx context.Context,
	meta txn.TxnMeta,
	req memoryengine.OpenRelationReq,
	resp *memoryengine.OpenRelationResp,
) error {
	txn, err := h.eng.GetOrCreateTxnWithMeta(nil, meta.GetID(),
		types.TimestampToTS(meta.GetSnapshotTS()))
	db, err := h.eng.GetDatabase(ctx, req.DatabaseName, txn)
	if err != nil {
		return err
	}
	rel, err := db.GetRelation(ctx, req.Name)
	if err != nil {
		return err
	}

	resp.ID = memoryengine.ID(rel.GetRelationID(ctx))
	resp.RelationName = req.Name
	resp.DatabaseName = req.DatabaseName
	resp.Type = memoryengine.RelationTable
	h.rels[resp.ID] = rel
	return nil
}

func (h *Handle) HandleGetRelations(
	ctx context.Context,
	meta txn.TxnMeta,
	req memoryengine.GetRelationsReq,
	resp *memoryengine.GetRelationsResp,
) error {
	txn, err := h.eng.GetOrCreateTxnWithMeta(nil, meta.GetID(),
		types.TimestampToTS(meta.GetSnapshotTS()))
	db, err := h.eng.GetDatabase(ctx, req.DatabaseName, txn)
	if err != nil {
		return err
	}
	rels, err := db.RelationNames(ctx)
	if err != nil {
		return err
	}
	resp.Names = rels
	return nil
}

func (h *Handle) HandleGetTableDefs(
	ctx context.Context,
	meta txn.TxnMeta,
	req memoryengine.GetTableDefsReq,
	resp *memoryengine.GetTableDefsResp,
) error {
	_, err := h.eng.GetOrCreateTxnWithMeta(nil, meta.GetID(),
		types.TimestampToTS(meta.GetSnapshotTS()))
	if err != nil {
		return err
	}
	rel := h.rels[req.TableID]
	resp.Defs, err = rel.TableDefs(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (h *Handle) HandleTableStats(
	ctx context.Context,
	meta txn.TxnMeta,
	req memoryengine.TableStatsReq,
	resp *memoryengine.TableStatsResp,
) error {
	resp.Rows = 0
	return nil
}

func (h *Handle) HandleNewTableIter(
	ctx context.Context,
	meta txn.TxnMeta,
	req memoryengine.NewTableIterReq,
	resp *memoryengine.NewTableIterResp,
) error {
	return nil
}

//Handle DDL commands.

func (h *Handle) HandleCreateDatabase(
	meta txn.TxnMeta,
	req db.CreateDatabaseReq,
	resp *db.CreateDatabaseResp) (err error) {

	txn, err := h.eng.GetOrCreateTxnWithMeta(nil, meta.GetID(),
		types.TimestampToTS(meta.GetSnapshotTS()))
	if err != nil {
		return err
	}
	ctx := context.Background()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, req.AccessInfo.AccountID)
	ctx = context.WithValue(ctx, defines.UserIDKey{}, req.AccessInfo.UserID)
	ctx = context.WithValue(ctx, defines.RoleIDKey{}, req.AccessInfo.RoleID)
	err = h.eng.CreateDatabase(ctx, req.Name, txn)
	if err != nil {
		return
	}
	db, err := h.eng.GetDatabase(ctx, req.Name, txn)
	if err != nil {
		return
	}
	resp.ID = db.GetDatabaseID(ctx)
	return
}

func (h *Handle) HandleCreateDatabaseTmp(
	ctx context.Context,
	meta txn.TxnMeta,
	req memoryengine.OpenDatabaseReq,
	resp *memoryengine.OpenDatabaseResp) (err error) {

	txn, err := h.eng.GetOrCreateTxnWithMeta(nil, meta.GetID(),
		types.TimestampToTS(meta.GetSnapshotTS()))
	logutil.Infof("meta.GetSnapshotTS()111 is %v", types.TimestampToTS(meta.GetSnapshotTS()))
	if err != nil {
		return err
	}
	err = h.eng.CreateDatabase(ctx, req.Name, txn)
	if err != nil {
		return
	}
	db, err := h.eng.GetDatabase(ctx, req.Name, txn)
	if err != nil {
		return
	}
	resp.ID = memoryengine.ID(db.GetDatabaseID(ctx))
	resp.Name = req.Name
	return
}

func (h *Handle) HandleDropDatabase(
	meta txn.TxnMeta,
	req db.DropDatabaseReq,
	resp *db.DropDatabaseResp) (err error) {

	txn, err := h.eng.GetOrCreateTxnWithMeta(nil, meta.GetID(),
		types.TimestampToTS(meta.GetSnapshotTS()))
	if err != nil {
		return err
	}
	ctx := context.Background()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, req.AccessInfo.AccountID)
	ctx = context.WithValue(ctx, defines.UserIDKey{}, req.AccessInfo.UserID)
	ctx = context.WithValue(ctx, defines.RoleIDKey{}, req.AccessInfo.RoleID)
	err = h.eng.DropDatabase(ctx, req.Name, txn)
	if err != nil {
		return
	}

	db, err := h.eng.GetDatabase(ctx, req.Name, txn)
	if err != nil {
		return
	}
	resp.ID = db.GetDatabaseID(ctx)
	return
}

func (h *Handle) HandleCreateRelation(
	meta txn.TxnMeta,
	req db.CreateRelationReq,
	resp *db.CreateRelationResp) (err error) {

	txn, err := h.eng.GetOrCreateTxnWithMeta(nil, meta.GetID(),
		types.TimestampToTS(meta.GetSnapshotTS()))
	logutil.Infof("meta.GetSnapshotTS() is %v", types.TimestampToTS(meta.GetSnapshotTS()))
	if err != nil {
		return
	}
	ctx := context.Background()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, req.AccessInfo.AccountID)
	ctx = context.WithValue(ctx, defines.UserIDKey{}, req.AccessInfo.UserID)
	ctx = context.WithValue(ctx, defines.RoleIDKey{}, req.AccessInfo.RoleID)
	db, err := h.eng.GetDatabase(ctx, req.DatabaseName, txn)
	if err != nil {
		return
	}
	err = db.CreateRelation(context.TODO(), req.Name, req.Defs)
	if err != nil {
		return
	}

	tb, err := db.GetRelation(context.TODO(), req.Name)
	if err != nil {
		return
	}
	resp.ID = tb.GetRelationID(context.TODO())
	return
}

func (h *Handle) HandleCreateRelationTmp(
	ctx context.Context,
	meta txn.TxnMeta,
	req memoryengine.CreateRelationReq,
	resp *memoryengine.CreateRelationResp) (err error) {

	txn, err := h.eng.GetOrCreateTxnWithMeta(nil, meta.GetID(),
		types.TimestampToTS(meta.GetSnapshotTS()))
	logutil.Infof("meta.GetSnapshotTS() is %v", types.TimestampToTS(meta.GetSnapshotTS()))
	if err != nil {
		return
	}
	db, err := h.eng.GetDatabase(ctx, req.DatabaseName, txn)
	if err != nil {
		return
	}
	err = db.CreateRelation(context.TODO(), req.Name, req.Defs)
	if err != nil {
		return
	}

	tb, err := db.GetRelation(context.TODO(), req.Name)
	if err != nil {
		return
	}
	resp.ID = memoryengine.ID(tb.GetRelationID(context.TODO()))
	return
}

func (h *Handle) HandleDropRelation(
	meta txn.TxnMeta,
	req db.DropRelationReq,
	resp *db.DropRelationResp) (err error) {

	txn, err := h.eng.GetOrCreateTxnWithMeta(nil, meta.GetID(),
		types.TimestampToTS(meta.GetSnapshotTS()))
	if err != nil {
		return
	}
	ctx := context.Background()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, req.AccessInfo.AccountID)
	ctx = context.WithValue(ctx, defines.UserIDKey{}, req.AccessInfo.UserID)
	ctx = context.WithValue(ctx, defines.RoleIDKey{}, req.AccessInfo.RoleID)
	db, err := h.eng.GetDatabase(ctx, req.DatabaseName, txn)
	if err != nil {
		return
	}

	tb, err := db.GetRelation(context.TODO(), req.Name)
	if err != nil {
		return
	}
	resp.ID = tb.GetRelationID(context.TODO())

	err = db.DropRelation(context.TODO(), req.Name)
	if err != nil {
		return
	}
	return
}

// HandleWrite Handle DML commands
func (h *Handle) HandleWrite(
	meta txn.TxnMeta,
	req db.WriteReq,
	resp *db.WriteResp) (err error) {

	txn, err := h.eng.GetOrCreateTxnWithMeta(nil, meta.GetID(),
		types.TimestampToTS(meta.GetSnapshotTS()))
	if err != nil {
		return err
	}

	ctx := context.Background()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, req.AccessInfo.AccountID)
	ctx = context.WithValue(ctx, defines.UserIDKey{}, req.AccessInfo.UserID)
	ctx = context.WithValue(ctx, defines.RoleIDKey{}, req.AccessInfo.RoleID)
	dbase, err := h.eng.GetDatabase(ctx, req.DatabaseName, txn)
	if err != nil {
		return
	}

	tb, err := dbase.GetRelation(context.TODO(), req.TableName)
	if err != nil {
		return
	}

	if req.Type == db.EntryInsert {
		//Append a block had been bulk-loaded into S3
		if req.FileName != "" {
			//TODO::Precommit a block from S3
			//tb.AppendBlock()
			panic(moerr.NewNYI("Precommit a block is not implemented yet"))
		}
		//Add a batch into table
		//TODO::add a parameter to Append for PreCommit-Append?
		err = tb.Write(context.TODO(), req.Batch)
		return
	}

	//TODO:: handle delete rows of block had been bulk-loaded into S3.

	//Vecs[0]--> rowid
	//Vecs[1]--> PrimaryKey
	err = tb.DeleteByPhyAddrKeys(context.TODO(), req.Batch.GetVector(0))
	return

}

func openTAE(targetDir string, opt *options.Options) (tae *db.DB, err error) {

	if targetDir != "" {
		mask := syscall.Umask(0)
		if err := os.MkdirAll(targetDir, os.FileMode(0755)); err != nil {
			syscall.Umask(mask)
			logutil.Infof("Recreate dir error:%v\n", err)
			return nil, err
		}
		syscall.Umask(mask)
		tae, err = db.Open(targetDir+"/tae", opt)
		if err != nil {
			logutil.Infof("Open tae failed. error:%v", err)
			return nil, err
		}
		return tae, nil
	}

	tae, err = db.Open(targetDir, opt)
	if err != nil {
		logutil.Infof("Open tae failed. error:%v", err)
		return nil, err
	}
	return
}
