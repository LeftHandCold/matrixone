// Copyright 2021 Matrix Origin
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

package txnentries

import (
	"bytes"
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type mergeBlocksCmd struct {
	txnbase.BaseCmd
	tid         uint64
	droppedObjs []*common.ID
	createdObjs []*common.ID
	mapping     []uint32
	fromAddr    []uint32
	toAddr      []uint32
	txn         txnif.AsyncTxn
	id          uint32
}

func newMergeBlocksCmd(
	tid uint64,
	droppedObjs, createdObjs []*common.ID,
	mapping, fromAddr, toAddr []uint32,
	txn txnif.AsyncTxn,
	id uint32) *mergeBlocksCmd {
	return &mergeBlocksCmd{
		tid:         tid,
		droppedObjs: droppedObjs,
		createdObjs: createdObjs,
		mapping:     mapping,
		fromAddr:    fromAddr,
		toAddr:      toAddr,
		txn:         txn,
		id:          id,
	}
}

func (cmd *mergeBlocksCmd) GetType() uint16 { return IOET_WALTxnCommand_Merge }

func (cmd *mergeBlocksCmd) WriteTo(w io.Writer) (n int64, err error) {
	typ := IOET_WALTxnCommand_Merge
	if _, err = w.Write(types.EncodeUint16(&typ)); err != nil {
		return
	}
	n = 2
	ver := IOET_WALTxnCommand_Merge_CurrVer
	if _, err = w.Write(types.EncodeUint16(&ver)); err != nil {
		return
	}
	n = 2
	return
}
func (cmd *mergeBlocksCmd) ReadFrom(r io.Reader) (n int64, err error) {
	return
}
func (cmd *mergeBlocksCmd) MarshalBinary() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = cmd.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}
func (cmd *mergeBlocksCmd) UnmarshalBinary(buf []byte) (err error) {
	bbuf := bytes.NewBuffer(buf)
	_, err = cmd.ReadFrom(bbuf)
	return
}

func (cmd *mergeBlocksCmd) Desc() string {
	s := "CmdName=MERGE;From=["
	for _, blk := range cmd.droppedObjs {
		s = fmt.Sprintf("%s %d", s, blk.BlockID)
	}
	s = fmt.Sprintf("%s ];To=[", s)
	for _, blk := range cmd.createdObjs {
		s = fmt.Sprintf("%s %d", s, blk.BlockID)
	}
	s = fmt.Sprintf("%s ]", s)
	return s
}

func (cmd *mergeBlocksCmd) String() string {
	s := "CmdName=MERGE;From=["
	for _, blk := range cmd.droppedObjs {
		s = fmt.Sprintf("%s %d", s, blk.BlockID)
	}
	s = fmt.Sprintf("%s ];To=[", s)
	for _, blk := range cmd.createdObjs {
		s = fmt.Sprintf("%s %d", s, blk.BlockID)
	}
	s = fmt.Sprintf("%s ]", s)
	return s
}
func (cmd *mergeBlocksCmd) VerboseString() string {
	s := "CmdName=MERGE;From=["
	for _, blk := range cmd.droppedObjs {
		s = fmt.Sprintf("%s %s", s, blk.BlockString())
	}
	s = fmt.Sprintf("%s ];To=[", s)
	for _, blk := range cmd.createdObjs {
		s = fmt.Sprintf("%s %s", s, blk.BlockString())
	}
	s = fmt.Sprintf("%s ];FromFormat=%v;ToFormat=%v", s, cmd.fromAddr, cmd.toAddr)
	return s
}
func (cmd *mergeBlocksCmd) ApplyCommit()                  {}
func (cmd *mergeBlocksCmd) ApplyRollback()                {}
func (cmd *mergeBlocksCmd) SetReplayTxn(_ txnif.AsyncTxn) {}
