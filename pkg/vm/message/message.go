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

package message

import (
	"bytes"
	"context"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

const messageTimeout = 300 * time.Second
const ALLCN = "ALLCN"
const CURRENTCN = "CURRENTCN"

const messageBoardDiagnosticMarker = "issue25816-messageboard"

var nextMessageBoardDiagnosticID atomic.Uint64
var nextMessageBoardDiagnosticEventSequence atomic.Uint64

type MsgType int32

const (
	MsgTopValue      MsgType = 0
	MsgPipelineStart MsgType = 1
	MsgPipelineStop  MsgType = 2
	MsgRuntimeFilter MsgType = 3
	MsgJoinMap       MsgType = 4
	MaxMessage       MsgType = 1024
)

func (m MsgType) MessageName() string {
	switch m {
	case MsgTopValue:
		return "MsgTopValue"
	case MsgRuntimeFilter:
		return "MsgRuntimeFilter"
	}
	return "unknown message type"
}

type MessageAddress struct {
	CnAddr     string
	OperatorID int32
	ParallelID int32
}

type Message interface {
	Serialize() []byte
	Deserialize([]byte) Message
	NeedBlock() bool
	GetMsgTag() int32
	GetReceiverAddr() MessageAddress
	DebugString() string
	Destroy()
}

type MessageCenter struct {
	StmtIDToBoard map[uuid.UUID]*MessageBoard
	RwMutex       *sync.Mutex
}

type MessageBoard struct {
	diagnosticID  uint64
	reset         bool // for debug purpose
	multiCN       bool
	stmtId        uuid.UUID
	messageCenter *MessageCenter
	messages      []*Message
	waiters       []chan bool
	rwMutex       *sync.RWMutex
}

type messageBoardDiagnosticSnapshot struct {
	diagnosticID  uint64
	reset         bool
	multiCN       bool
	stmtID        uuid.UUID
	messageCenter *MessageCenter
}

func NewMessageBoard() *MessageBoard {
	m := &MessageBoard{
		diagnosticID: nextMessageBoardDiagnosticID.Add(1),
		messages:     make([]*Message, 0, 16),
		waiters:      make([]chan bool, 0, 16),
		rwMutex:      &sync.RWMutex{},
	}
	runtime.SetFinalizer(m, (*MessageBoard).finalize)
	logMessageBoardDiagnostic("board-new", m.diagnosticSnapshot(), "")
	return m
}

func (m *MessageBoard) diagnosticSnapshot() messageBoardDiagnosticSnapshot {
	m.rwMutex.RLock()
	snapshot := m.diagnosticSnapshotLocked()
	m.rwMutex.RUnlock()
	return snapshot
}

func (m *MessageBoard) diagnosticSnapshotLocked() messageBoardDiagnosticSnapshot {
	return messageBoardDiagnosticSnapshot{
		diagnosticID:  m.diagnosticID,
		reset:         m.reset,
		multiCN:       m.multiCN,
		stmtID:        m.stmtId,
		messageCenter: m.messageCenter,
	}
}

func logMessageBoardDiagnostic(
	event string,
	snapshot messageBoardDiagnosticSnapshot,
	detailFormat string,
	detailArgs ...any,
) {
	eventSequence := nextMessageBoardDiagnosticEventSequence.Add(1)
	format := messageBoardDiagnosticMarker +
		" event_seq=%d event=%s stmt_id=%s board_id=%d multi_cn=%t reset=%t center=%p"
	args := []any{
		eventSequence,
		event,
		snapshot.stmtID.String(),
		snapshot.diagnosticID,
		snapshot.multiCN,
		snapshot.reset,
		snapshot.messageCenter,
	}
	if detailFormat != "" {
		format += " " + detailFormat
		args = append(args, detailArgs...)
	}
	logutil.Infof(format, args...)
}

func (m *MessageBoard) finalize() {
	m.cleanupQueuedMessages()
}

func (m *MessageBoard) DebugString() string {
	buf := bytes.NewBuffer(make([]byte, 0, 400))
	if m.reset {
		buf.WriteString("messageBoard has been reseted!\n")
	}
	if m.multiCN {
		buf.WriteString("messageBoard on MultiCN\n")
	} else {
		buf.WriteString("messageBoard on single CN\n")
	}
	buf.WriteString("messageBoard length: " + strconv.Itoa(len(m.messages)) + "\n")
	for i := range m.messages {
		message := *m.messages[i]
		buf.WriteString("message " + strconv.Itoa(i) + ": " + message.DebugString() + "\n")
	}
	return buf.String()
}

func (m *MessageBoard) SetMultiCN(center *MessageCenter, stmtId uuid.UUID) *MessageBoard {
	var mb *MessageBoard
	var snapshot messageBoardDiagnosticSnapshot
	var found bool
	func() {
		center.RwMutex.Lock()
		defer center.RwMutex.Unlock()
		mb, found = center.StmtIDToBoard[stmtId]
		if found {
			return
		}
		m.rwMutex.Lock()
		m.multiCN = true
		m.stmtId = stmtId
		m.messageCenter = center
		snapshot = m.diagnosticSnapshotLocked()
		m.rwMutex.Unlock()
		center.StmtIDToBoard[stmtId] = m
		mb = m
	}()
	if found {
		logMessageBoardDiagnostic(
			"set-multicn-hit",
			mb.diagnosticSnapshot(),
			"candidate_board_id=%d",
			m.diagnosticID,
		)
		return mb
	}
	logMessageBoardDiagnostic("set-multicn-new", snapshot, "")
	return mb
}

func (m *MessageBoard) BeforeRunonce() {
	// call this before runonce
	m.rwMutex.Lock()
	previousReset := m.reset
	m.reset = false
	snapshot := m.diagnosticSnapshotLocked()
	m.rwMutex.Unlock()
	logMessageBoardDiagnostic(
		"before-run-once",
		snapshot,
		"previous_reset=%t",
		previousReset,
	)
}

func (m *MessageBoard) Reset() *MessageBoard {
	snapshot := m.diagnosticSnapshot()
	if snapshot.multiCN {
		center := snapshot.messageCenter
		center.RwMutex.Lock()
		mappedBoard, mapPresent := center.StmtIDToBoard[snapshot.stmtID]
		mappedBoardID := uint64(0)
		if mappedBoard != nil {
			mappedBoardID = mappedBoard.diagnosticID
		}
		mappedBoardMatches := mappedBoard == m
		delete(center.StmtIDToBoard, snapshot.stmtID)
		center.RwMutex.Unlock()
		// other pipeline could still access thie messageBoard
		// so reset current message board to a new one
		replacement := NewMessageBoard()
		logMessageBoardDiagnostic(
			"reset-multicn",
			snapshot,
			"mapped_board_id=%d map_present=%t mapped_board_matches=%t replacement_board_id=%d",
			mappedBoardID,
			mapPresent,
			mappedBoardMatches,
			replacement.diagnosticID,
		)
		return replacement
	}
	func() {
		m.rwMutex.Lock()
		defer m.rwMutex.Unlock()
		m.cleanupQueuedMessagesLocked()
		m.multiCN = false
		m.reset = true
		snapshot = m.diagnosticSnapshotLocked()
	}()
	logMessageBoardDiagnostic("reset-singlecn", snapshot, "")
	return m
}

func (m *MessageBoard) cleanupQueuedMessages() {
	if m == nil || m.rwMutex == nil {
		return
	}
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()
	m.cleanupQueuedMessagesLocked()
}

func (m *MessageBoard) cleanupQueuedMessagesLocked() {
	for i := range m.messages {
		if m.messages[i] == nil {
			continue
		}
		message := *m.messages[i]
		message.Destroy()
		m.messages[i] = nil
	}
	m.messages = m.messages[:0]
	m.waiters = m.waiters[:0]
}

type MessageReceiver struct {
	debug    bool // for unit test
	offset   int32
	tags     []int32
	received []int32
	addr     *MessageAddress
	mb       *MessageBoard
	waiter   chan bool
}

func NewMessageReceiver(tags []int32, addr MessageAddress, mb *MessageBoard) *MessageReceiver {
	return &MessageReceiver{
		tags: tags,
		addr: &addr,
		mb:   mb,
	}
}

func SendMessage(m Message, mb *MessageBoard) {
	if m.GetReceiverAddr().CnAddr == CURRENTCN { // message for current CN
		mb.rwMutex.Lock()
		mb.messages = append(mb.messages, &m)
		if m.NeedBlock() {
			// broadcast for block message
			for _, ch := range mb.waiters {
				if ch != nil && len(ch) == 0 {
					ch <- true
				}
			}
		}
		mb.rwMutex.Unlock()
		logJoinMapSendDiagnostic(m, mb)
	} else {
		//todo: send message to other CN, need to lookup cnlist
		panic("unsupported message yet!")
	}
}

func logJoinMapSendDiagnostic(m Message, mb *MessageBoard) {
	var msg JoinMapMsg
	switch typed := m.(type) {
	case JoinMapMsg:
		msg = typed
	case *JoinMapMsg:
		if typed == nil {
			return
		}
		msg = *typed
	default:
		return
	}
	logMessageBoardDiagnostic(
		"joinmap-send",
		mb.diagnosticSnapshot(),
		"tag=%d joinmap_nil=%t is_shuffle=%t shuffle_idx=%d spilled=%t",
		msg.Tag,
		msg.JoinMapPtr == nil,
		msg.IsShuffle,
		msg.ShuffleIdx,
		msg.Spilled,
	)
}

func (mr *MessageReceiver) receiveMessageNonBlock() []Message {
	mr.mb.rwMutex.RLock()
	defer mr.mb.rwMutex.RUnlock()
	var result []Message
	lenMessages := int32(len(mr.mb.messages))
	for ; mr.offset < lenMessages; mr.offset++ {
		if mr.mb.messages[mr.offset] == nil {
			continue
		}
		message := *mr.mb.messages[mr.offset]
		if !MatchAddress(message, mr.addr) {
			continue
		}
		for i := range mr.tags {
			if mr.tags[i] == message.GetMsgTag() {
				result = append(result, message)
				mr.received = append(mr.received, mr.offset)
				break
			}
		}
	}
	return result
}

func (mr *MessageReceiver) ReceiveMessage(needBlock bool, ctx context.Context) ([]Message, bool, error) {
	var result = mr.receiveMessageNonBlock()
	if !needBlock || len(result) > 0 {
		return result, false, nil
	}
	if mr.waiter == nil {
		mr.waiter = make(chan bool, 1)
		mr.mb.rwMutex.Lock()
		mr.mb.waiters = append(mr.mb.waiters, mr.waiter)
		mr.mb.rwMutex.Unlock()
	}
	for {
		result = mr.receiveMessageNonBlock()
		if len(result) > 0 {
			break
		}
		timeout := messageTimeout
		if mr.debug {
			timeout = 1 * time.Second
		}
		timeoutCtx, timeoutCancel := context.WithTimeoutCause(context.Background(), timeout, moerr.CauseReceiveMessage)
		select {
		case <-timeoutCtx.Done():
			timeoutCancel()
			logutil.Warnf("waiting messsage timeout, waiting for tag %v, messageBoard debug message %v", mr.tags, mr.mb.DebugString())
		case <-mr.waiter:
			timeoutCancel()
		case <-ctx.Done():
			timeoutCancel()
			return result, true, nil
		}
	}
	return result, false, nil
}

func MatchAddress(m Message, raddr *MessageAddress) bool {
	mAddr := m.GetReceiverAddr()
	if mAddr.OperatorID != raddr.OperatorID && mAddr.OperatorID != -1 {
		return false
	}
	if mAddr.ParallelID != raddr.ParallelID && mAddr.ParallelID != -1 {
		return false
	}
	return true
}

func AddrBroadCastOnCurrentCN() MessageAddress {
	return MessageAddress{
		CnAddr:     CURRENTCN,
		OperatorID: -1,
		ParallelID: -1,
	}
}

func AddrBroadCastOnALLCN() MessageAddress {
	return MessageAddress{
		CnAddr:     ALLCN,
		OperatorID: -1,
		ParallelID: -1,
	}
}
