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

package taestorage

import (
	"bytes"
	"context"
	"encoding/gob"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
)

// Write implements storage.TxnTAEStorage

func (s *taeStorage) Write(
	ctx context.Context,
	txnMeta txn.TxnMeta,
	op uint32,
	payload []byte) (result []byte, err error) {
	logutil.Infof("op is %d", op)
	logutil.Infof("meta.GetSnapshotTS() Write is %v", types.TimestampToTS(txnMeta.GetSnapshotTS()).ToString())
	switch op {

	case uint32(apipb.OpCode_OpPreCommit):

		return handleWrite(
			s, txnMeta, payload,
			s.taeHandler.HandlePreCommit,
		)
	case uint32(memoryengine.OpCreateDatabase):
		return handleWriteTmp(
			ctx, txnMeta, payload,
			s.taeHandler.HandleCreateDatabaseTmp,
		)
	case uint32(memoryengine.OpCreateRelation):
		return handleWriteTmp(
			ctx, txnMeta, payload,
			s.taeHandler.HandleCreateRelationTmp,
		)
	default:
		panic(moerr.NewInfo("OpCode is not supported"))

	}

}

func handleWrite[
	Req any,
	Resp any,
](
	s *taeStorage,
	meta txn.TxnMeta,
	payload []byte,
	fn func(
		meta txn.TxnMeta,
		req Req,
		resp *Resp,
	) (
		err error,
	),
) (
	res []byte,
	err error,
) {

	var req Req
	if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&req); err != nil {
		return nil, err
	}

	var resp Resp
	defer logReq("write", req, meta, &resp, &err)()

	err = fn(meta, req, &resp)
	if err != nil {
		return nil, err
	}

	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(resp); err != nil {
		return nil, err
	}
	res = buf.Bytes()

	return
}

func handleWriteTmp[
	Req any,
	Resp any,
](
	ctx context.Context,
	meta txn.TxnMeta,
	payload []byte,
	fn func(
		ctx context.Context,
		meta txn.TxnMeta,
		req Req,
		resp *Resp,
	) (
		err error,
	),
) (
	res []byte,
	err error,
) {

	var req Req
	if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&req); err != nil {
		return nil, err
	}

	var resp Resp
	defer logReq("write", req, meta, &resp, &err)()

	err = fn(ctx, meta, req, &resp)
	if err != nil {
		return nil, err
	}

	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(resp); err != nil {
		return nil, err
	}
	res = buf.Bytes()

	return
}
