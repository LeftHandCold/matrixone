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

package logservice

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/lni/dragonboat/v4"
	cli "github.com/lni/dragonboat/v4/client"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/plugin/tan"
	"github.com/lni/dragonboat/v4/plugin/tee"
	"github.com/lni/dragonboat/v4/raftpb"
	sm "github.com/lni/dragonboat/v4/statemachine"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/bootstrap"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/task"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
)

type storeMeta struct {
	serviceAddress string
}

func (l *storeMeta) marshal() []byte {
	return []byte(l.serviceAddress)
}

func (l *storeMeta) unmarshal(data []byte) {
	l.serviceAddress = string(data)
}

func isUserUpdate(cmd []byte) bool {
	return parseCmdTag(cmd) == pb.UserEntryUpdate
}

func isSetLeaseHolderUpdate(cmd []byte) bool {
	return parseCmdTag(cmd) == pb.LeaseHolderIDUpdate
}

func getNodeHostConfig(cfg Config) config.NodeHostConfig {
	meta := storeMeta{
		serviceAddress: cfg.ServiceAddress,
	}
	if cfg.GossipProbeInterval.Duration == 0 {
		panic("cfg.GossipProbeInterval.Duration is 0")
	}
	logdb := config.GetTinyMemLogDBConfig()
	logdb.KVWriteBufferSize = cfg.LogDBBufferSize
	logdbFactory := (config.LogDBFactory)(nil)
	logdbFactory = tan.Factory
	if cfg.UseTeeLogDB {
		logutil.Warn("using tee based logdb backed by pebble and tan, for testing purposes only")
		logdbFactory = tee.TanPebbleLogDBFactory
	}
	return config.NodeHostConfig{
		DeploymentID:        cfg.DeploymentID,
		NodeHostID:          cfg.UUID,
		NodeHostDir:         cfg.DataDir,
		RTTMillisecond:      cfg.RTTMillisecond,
		AddressByNodeHostID: true,
		RaftAddress:         cfg.RaftAddress,
		ListenAddress:       cfg.RaftListenAddress,
		Expert: config.ExpertConfig{
			FS:           cfg.FS,
			LogDBFactory: logdbFactory,
			// FIXME: dragonboat need to be updated to make this field a first class
			// citizen
			TestGossipProbeInterval: cfg.GossipProbeInterval.Duration,
			LogDB:                   logdb,
		},
		Gossip: config.GossipConfig{
			BindAddress:      cfg.GossipListenAddress,
			AdvertiseAddress: cfg.GossipAddress,
			Seed:             cfg.GossipSeedAddresses,
			Meta:             meta.marshal(),
		},
	}
}

func getRaftConfig(shardID uint64, replicaID uint64) config.Config {
	return config.Config{
		ShardID:             shardID,
		ReplicaID:           replicaID,
		CheckQuorum:         true,
		PreVote:             true,
		ElectionRTT:         10,
		HeartbeatRTT:        1,
		OrderedConfigChange: true,
	}
}

// store manages log shards including the HAKeeper shard on each node.
type store struct {
	cfg               Config
	nh                *dragonboat.NodeHost
	haKeeperReplicaID uint64
	checker           hakeeper.Checker
	alloc             hakeeper.IDAllocator
	stopper           *stopper.Stopper
	tickerStopper     *stopper.Stopper
	logger            *zap.Logger

	bootstrapCheckCycles uint64
	bootstrapMgr         *bootstrap.Manager

	taskScheduler hakeeper.TaskScheduler

	mu struct {
		sync.Mutex
		truncateCh      chan struct{}
		pendingTruncate map[uint64]struct{}
		metadata        metadata.LogStore
	}
}

func newLogStore(cfg Config, taskService taskservice.TaskService, logger *zap.Logger) (*store, error) {
	nh, err := dragonboat.NewNodeHost(getNodeHostConfig(cfg))
	if err != nil {
		return nil, err
	}
	hakeeperConfig := cfg.GetHAKeeperConfig()
	logger = logutil.Adjust(logger)
	logger.Info("HAKeeper Timeout Configs",
		zap.Int64("LogStoreTimeout", int64(hakeeperConfig.LogStoreTimeout)),
		zap.Int64("DNStoreTimeout", int64(hakeeperConfig.DNStoreTimeout)),
		zap.Int64("CNStoreTimeout", int64(hakeeperConfig.CNStoreTimeout)),
	)
	ls := &store{
		cfg:           cfg,
		nh:            nh,
		checker:       checkers.NewCoordinator(hakeeperConfig),
		taskScheduler: task.NewTaskScheduler(taskService, hakeeperConfig),
		alloc:         newIDAllocator(),
		stopper:       stopper.NewStopper("log-store"),
		tickerStopper: stopper.NewStopper("hakeeper-ticker"),
		logger:        logger,
	}
	ls.mu.truncateCh = make(chan struct{})
	ls.mu.pendingTruncate = make(map[uint64]struct{})
	ls.mu.metadata = metadata.LogStore{UUID: cfg.UUID}
	if err := ls.stopper.RunNamedTask("truncation-worker", func(ctx context.Context) {
		logger.Info("logservice truncation worker started")
		ls.truncationWorker(ctx)
	}); err != nil {
		return nil, err
	}
	return ls, nil
}

func (l *store) close() error {
	l.tickerStopper.Stop()
	l.stopper.Stop()
	if l.nh != nil {
		l.nh.Close()
	}
	return nil
}

func (l *store) id() string {
	return l.nh.ID()
}

func (l *store) startReplicas() error {
	l.mu.Lock()
	shards := make([]metadata.LogShard, 0)
	shards = append(shards, l.mu.metadata.Shards...)
	l.mu.Unlock()

	for _, rec := range shards {
		if rec.ShardID == hakeeper.DefaultHAKeeperShardID {
			if err := l.startHAKeeperReplica(rec.ReplicaID, nil, false); err != nil {
				return err
			}
		} else {
			if err := l.startReplica(rec.ShardID, rec.ReplicaID, nil, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func (l *store) startHAKeeperReplica(replicaID uint64,
	initialReplicas map[uint64]dragonboat.Target, join bool) error {
	raftConfig := getRaftConfig(hakeeper.DefaultHAKeeperShardID, replicaID)
	if err := l.nh.StartReplica(initialReplicas,
		join, hakeeper.NewStateMachine, raftConfig); err != nil {
		return err
	}
	l.addMetadata(hakeeper.DefaultHAKeeperShardID, replicaID)
	atomic.StoreUint64(&l.haKeeperReplicaID, replicaID)
	if !l.cfg.DisableWorkers {
		if err := l.tickerStopper.RunNamedTask("hakeeper-ticker", func(ctx context.Context) {
			l.logger.Info("HAKeeper ticker started")
			l.ticker(ctx)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (l *store) startReplica(shardID uint64, replicaID uint64,
	initialReplicas map[uint64]dragonboat.Target, join bool) error {
	if shardID == hakeeper.DefaultHAKeeperShardID {
		return moerr.NewInvalidInput("shardID %d does not match DefaultHAKeeperShardID %d", shardID, hakeeper.DefaultHAKeeperShardID)
	}
	cfg := getRaftConfig(shardID, replicaID)
	if err := l.nh.StartReplica(initialReplicas, join, newStateMachine, cfg); err != nil {
		return err
	}
	l.addMetadata(shardID, replicaID)
	return nil
}

func (l *store) stopReplica(shardID uint64, replicaID uint64) error {
	if shardID == hakeeper.DefaultHAKeeperShardID {
		defer func() {
			atomic.StoreUint64(&l.haKeeperReplicaID, 0)
		}()
	}
	return l.nh.StopReplica(shardID, replicaID)
}

func (l *store) requestLeaderTransfer(shardID uint64, targetReplicaID uint64) error {
	return l.nh.RequestLeaderTransfer(shardID, targetReplicaID)
}

func (l *store) addReplica(shardID uint64, replicaID uint64,
	target dragonboat.Target, cci uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	count := 0
	for {
		count++
		if err := l.nh.SyncRequestAddReplica(ctx, shardID, replicaID, target, cci); err != nil {
			if errors.Is(err, dragonboat.ErrShardNotReady) {
				l.retryWait()
				continue
			}
			if errors.Is(err, dragonboat.ErrTimeoutTooSmall) && count > 1 {
				return dragonboat.ErrTimeout
			}
			return err
		}
		return nil
	}
}

func (l *store) removeReplica(shardID uint64, replicaID uint64, cci uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	count := 0
	for {
		count++
		if err := l.nh.SyncRequestDeleteReplica(ctx, shardID, replicaID, cci); err != nil {
			if errors.Is(err, dragonboat.ErrShardNotReady) {
				l.retryWait()
				continue
			}
			// FIXME: internally handle dragonboat.ErrTimeoutTooSmall
			if errors.Is(err, dragonboat.ErrTimeoutTooSmall) && count > 1 {
				return dragonboat.ErrTimeout
			}
			return err
		}
		l.removeMetadata(shardID, replicaID)
		return nil
	}
}

func (l *store) retryWait() {
	if l.nh.NodeHostConfig().RTTMillisecond == 1 {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(time.Duration(l.nh.NodeHostConfig().RTTMillisecond/2) * time.Millisecond)
}

func (l *store) propose(ctx context.Context,
	session *cli.Session, cmd []byte) (sm.Result, error) {
	count := 0
	for {
		count++
		result, err := l.nh.SyncPropose(ctx, session, cmd)
		if err != nil {
			if errors.Is(err, dragonboat.ErrShardNotReady) {
				l.retryWait()
				continue
			}
			if errors.Is(err, dragonboat.ErrTimeoutTooSmall) && count > 1 {
				return sm.Result{}, dragonboat.ErrTimeout
			}
			return sm.Result{}, err
		}
		return result, nil
	}
}

func (l *store) read(ctx context.Context,
	shardID uint64, query interface{}) (interface{}, error) {
	count := 0
	for {
		count++
		result, err := l.nh.SyncRead(ctx, shardID, query)
		if err != nil {
			if errors.Is(err, dragonboat.ErrShardNotReady) {
				l.retryWait()
				continue
			}
			if errors.Is(err, dragonboat.ErrTimeoutTooSmall) && count > 1 {
				return nil, dragonboat.ErrTimeout
			}
			return nil, err
		}
		return result, nil
	}
}

func (l *store) getOrExtendDNLease(ctx context.Context,
	shardID uint64, dnID uint64) error {
	session := l.nh.GetNoOPSession(shardID)
	cmd := getSetLeaseHolderCmd(dnID)
	_, err := l.propose(ctx, session, cmd)
	return err
}

func (l *store) truncateLog(ctx context.Context,
	shardID uint64, index Lsn) error {
	session := l.nh.GetNoOPSession(shardID)
	cmd := getSetTruncatedLsnCmd(index)
	result, err := l.propose(ctx, session, cmd)
	if err != nil {
		l.logger.Error("propose truncate log cmd failed", zap.Error(err))
		return err
	}
	if result.Value > 0 {
		l.logger.Error(fmt.Sprintf("shardID %d already truncated to index %d", shardID, result.Value))
		return moerr.NewInvalidTruncateLsn(shardID, result.Value)
	}
	l.mu.Lock()
	l.mu.pendingTruncate[shardID] = struct{}{}
	l.mu.Unlock()
	l.mu.truncateCh <- struct{}{}
	return nil
}

func (l *store) append(ctx context.Context,
	shardID uint64, cmd []byte) (Lsn, error) {
	session := l.nh.GetNoOPSession(shardID)
	result, err := l.propose(ctx, session, cmd)
	if err != nil {
		l.logger.Error("propose failed", zap.Error(err))
		return 0, err
	}
	if len(result.Data) > 0 {
		l.logger.Error("not current lease holder", zap.Uint64("data", binaryEnc.Uint64(result.Data)))
		return 0, moerr.NewNotLeaseHolder(binaryEnc.Uint64(result.Data))
	}
	if result.Value == 0 {
		panic(moerr.NewInvalidState("unexpected Lsn value"))
	}
	return result.Value, nil
}

func (l *store) getTruncatedLsn(ctx context.Context,
	shardID uint64) (uint64, error) {
	v, err := l.read(ctx, shardID, truncatedLsnQuery{})
	if err != nil {
		return 0, err
	}
	return v.(uint64), nil
}

func (l *store) tsoUpdate(ctx context.Context, count uint64) (uint64, error) {
	cmd := getTsoUpdateCmd(count)
	session := l.nh.GetNoOPSession(firstLogShardID)
	result, err := l.propose(ctx, session, cmd)
	if err != nil {
		l.logger.Error("failed to propose tso updat", zap.Error(err))
		return 0, err
	}
	return result.Value, nil
}

func handleNotHAKeeperError(err error) error {
	if err == nil {
		return err
	}
	if errors.Is(err, dragonboat.ErrShardNotFound) {
		return moerr.NewNoHAKeeper()
	}
	return err
}

func (l *store) addLogStoreHeartbeat(ctx context.Context,
	hb pb.LogStoreHeartbeat) (pb.CommandBatch, error) {
	data := MustMarshal(&hb)
	cmd := hakeeper.GetLogStoreHeartbeatCmd(data)
	session := l.nh.GetNoOPSession(hakeeper.DefaultHAKeeperShardID)
	if result, err := l.propose(ctx, session, cmd); err != nil {
		l.logger.Error("propose failed", zap.Error(err))
		return pb.CommandBatch{}, handleNotHAKeeperError(err)
	} else {
		var cb pb.CommandBatch
		MustUnmarshal(&cb, result.Data)
		return cb, nil
	}
}

func (l *store) addCNStoreHeartbeat(ctx context.Context,
	hb pb.CNStoreHeartbeat) error {
	data := MustMarshal(&hb)
	cmd := hakeeper.GetCNStoreHeartbeatCmd(data)
	session := l.nh.GetNoOPSession(hakeeper.DefaultHAKeeperShardID)
	if _, err := l.propose(ctx, session, cmd); err != nil {
		l.logger.Error("propose failed", zap.Error(err))
		return handleNotHAKeeperError(err)
	}
	return nil
}

func (l *store) cnAllocateID(ctx context.Context,
	req pb.CNAllocateID) (uint64, error) {
	cmd := hakeeper.GetGetIDCmd(req.Batch)
	session := l.nh.GetNoOPSession(hakeeper.DefaultHAKeeperShardID)
	result, err := l.propose(ctx, session, cmd)
	if err != nil {
		l.logger.Error("propose get id failed", zap.Error(err))
		return 0, err
	}
	return result.Value, nil
}

func (l *store) addDNStoreHeartbeat(ctx context.Context,
	hb pb.DNStoreHeartbeat) (pb.CommandBatch, error) {
	data := MustMarshal(&hb)
	cmd := hakeeper.GetDNStoreHeartbeatCmd(data)
	session := l.nh.GetNoOPSession(hakeeper.DefaultHAKeeperShardID)
	if result, err := l.propose(ctx, session, cmd); err != nil {
		l.logger.Error("propose failed", zap.Error(err))
		return pb.CommandBatch{}, handleNotHAKeeperError(err)
	} else {
		var cb pb.CommandBatch
		MustUnmarshal(&cb, result.Data)
		return cb, nil
	}
}

func (l *store) getCommandBatch(ctx context.Context,
	uuid string) (pb.CommandBatch, error) {
	v, err := l.read(ctx,
		hakeeper.DefaultHAKeeperShardID, &hakeeper.ScheduleCommandQuery{UUID: uuid})
	if err != nil {
		return pb.CommandBatch{}, handleNotHAKeeperError(err)
	}
	return *(v.(*pb.CommandBatch)), nil
}

func (l *store) getClusterDetails(ctx context.Context) (pb.ClusterDetails, error) {
	v, err := l.read(ctx,
		hakeeper.DefaultHAKeeperShardID, &hakeeper.ClusterDetailsQuery{Cfg: l.cfg.GetHAKeeperConfig()})
	if err != nil {
		return pb.ClusterDetails{}, handleNotHAKeeperError(err)
	}
	return *(v.(*pb.ClusterDetails)), nil
}

func (l *store) addScheduleCommands(ctx context.Context,
	term uint64, cmds []pb.ScheduleCommand) error {
	cmd := hakeeper.GetUpdateCommandsCmd(term, cmds)
	session := l.nh.GetNoOPSession(hakeeper.DefaultHAKeeperShardID)
	if _, err := l.propose(ctx, session, cmd); err != nil {
		return handleNotHAKeeperError(err)
	}
	return nil
}

func (l *store) getLeaseHolderID(ctx context.Context,
	shardID uint64, entries []raftpb.Entry) (uint64, error) {
	if len(entries) == 0 {
		panic("empty entries")
	}
	// first entry is an update lease cmd
	e := entries[0]
	if !isRaftInternalEntry(e) && isSetLeaseHolderUpdate(l.decodeCmd(e)) {
		return parseLeaseHolderID(l.decodeCmd(e)), nil
	}
	v, err := l.read(ctx, shardID, leaseHistoryQuery{lsn: e.Index})
	if err != nil {
		l.logger.Error("failed to read", zap.Error(err))
		return 0, err
	}
	return v.(uint64), nil
}

func (l *store) decodeCmd(e raftpb.Entry) []byte {
	if e.Type == raftpb.ApplicationEntry {
		panic(moerr.NewInvalidState("unexpected entry type"))
	}
	if e.Type == raftpb.EncodedEntry {
		if e.Cmd[0] != 0 {
			panic(moerr.NewInvalidState("unexpected cmd header"))
		}
		return e.Cmd[1:]
	}
	panic(moerr.NewInvalidState("invalid cmd"))
}

func isRaftInternalEntry(e raftpb.Entry) bool {
	if len(e.Cmd) == 0 {
		return true
	}
	return e.Type == raftpb.ConfigChangeEntry || e.Type == raftpb.MetadataEntry
}

func (l *store) markEntries(ctx context.Context,
	shardID uint64, entries []raftpb.Entry) ([]pb.LogRecord, error) {
	if len(entries) == 0 {
		return []pb.LogRecord{}, nil
	}
	leaseHolderID, err := l.getLeaseHolderID(ctx, shardID, entries)
	if err != nil {
		return nil, err
	}
	result := make([]pb.LogRecord, 0)
	for _, e := range entries {
		if isRaftInternalEntry(e) {
			// raft internal stuff
			result = append(result, LogRecord{
				Type: pb.Internal,
				Lsn:  e.Index,
			})
			continue
		}
		cmd := l.decodeCmd(e)
		if isSetLeaseHolderUpdate(cmd) {
			leaseHolderID = parseLeaseHolderID(cmd)
			result = append(result, LogRecord{
				Type: pb.LeaseUpdate,
				Lsn:  e.Index,
			})
			continue
		}
		if isUserUpdate(cmd) {
			if parseLeaseHolderID(cmd) != leaseHolderID {
				// lease not match, skip
				result = append(result, LogRecord{
					Type: pb.LeaseRejected,
					Lsn:  e.Index,
				})
				continue
			}
			result = append(result, LogRecord{
				Data: cmd,
				Type: pb.UserRecord,
				Lsn:  e.Index,
			})
		}
	}
	return result, nil
}

func getNextIndex(entries []raftpb.Entry, firstIndex Lsn, lastIndex Lsn) Lsn {
	if len(entries) == 0 {
		return firstIndex
	}
	lastResultIndex := entries[len(entries)-1].Index
	if lastResultIndex+1 < lastIndex {
		return lastResultIndex + 1
	}
	return firstIndex
}

// high priority test
// FIXME: add a test that queries the log with LeaseUpdate, LeaseRejected
// entries, no matter what is the firstLsn specified in queryLog(), returned
// results should make sense
func (l *store) queryLog(ctx context.Context, shardID uint64,
	firstIndex Lsn, maxSize uint64) ([]LogRecord, Lsn, error) {
	v, err := l.read(ctx, shardID, indexQuery{})
	if err != nil {
		return nil, 0, err
	}
	lastIndex := v.(uint64)
	// FIXME: check whether lastIndex >= firstIndex
	rs, err := l.nh.QueryRaftLog(shardID, firstIndex, lastIndex+1, maxSize)
	if err != nil {
		l.logger.Error("QueryRaftLog failed", zap.Error(err))
		return nil, 0, err
	}
	select {
	case v := <-rs.ResultC():
		if v.Completed() {
			entries, logRange := v.RaftLogs()
			next := getNextIndex(entries, firstIndex, logRange.LastIndex)
			results, err := l.markEntries(ctx, shardID, entries)
			if err != nil {
				l.logger.Error("markEntries failed", zap.Error(err))
				return nil, 0, err
			}
			return results, next, nil
		} else if v.RequestOutOfRange() {
			// FIXME: add more details to the log, what is the available range
			l.logger.Error("OutOfRange query found")
			return nil, 0, dragonboat.ErrInvalidRange
		}
		panic(moerr.NewInvalidState("unexpected rs state"))
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	}
}

func (l *store) ticker(ctx context.Context) {
	if l.cfg.HAKeeperTickInterval.Duration == 0 {
		panic("invalid HAKeeperTickInterval")
	}
	l.logger.Info("Hakeeper interval configs",
		zap.Int64("HAKeeperTickInterval", int64(l.cfg.HAKeeperTickInterval.Duration)),
		zap.Int64("HAKeeperCheckInterval", int64(l.cfg.HAKeeperCheckInterval.Duration)))
	ticker := time.NewTicker(l.cfg.HAKeeperTickInterval.Duration)
	defer ticker.Stop()
	if l.cfg.HAKeeperCheckInterval.Duration == 0 {
		panic("invalid HAKeeperCheckInterval")
	}
	defer func() {
		l.logger.Info("HAKeeper ticker stopped")
	}()
	haTicker := time.NewTicker(l.cfg.HAKeeperCheckInterval.Duration)
	defer haTicker.Stop()

	for {
		select {
		case <-ticker.C:
			l.hakeeperTick()
		case <-haTicker.C:
			l.hakeeperCheck()
		case <-ctx.Done():
			return
		}

		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

func (l *store) truncationWorker(ctx context.Context) {
	defer func() {
		l.logger.Info("truncation worker stopped")
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-l.mu.truncateCh:
			if err := l.processTruncateLog(ctx); err != nil {
				l.logger.Error("truncate failed", zap.Error(err))
			}
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}
}

func (l *store) isLeaderHAKeeper() (bool, uint64, error) {
	leaderID, term, ok, err := l.nh.GetLeaderID(hakeeper.DefaultHAKeeperShardID)
	if err != nil {
		return false, 0, err
	}
	replicaID := atomic.LoadUint64(&l.haKeeperReplicaID)
	return ok && replicaID != 0 && leaderID == replicaID, term, nil
}

// TODO: add test for this
func (l *store) hakeeperTick() {
	isLeader, _, err := l.isLeaderHAKeeper()
	if err != nil {
		l.logger.Error("failed to get HAKeeper Leader ID", zap.Error(err))
		return
	}

	if isLeader {
		cmd := hakeeper.GetTickCmd()
		ctx, cancel := context.WithTimeout(context.Background(), hakeeperDefaultTimeout)
		defer cancel()
		session := l.nh.GetNoOPSession(hakeeper.DefaultHAKeeperShardID)
		if _, err := l.propose(ctx, session, cmd); err != nil {
			l.logger.Error("propose tick failed", zap.Error(err))
			return
		}
	}
}

// TODO: add tests for this
func (l *store) processTruncateLog(ctx context.Context) error {
	l.mu.Lock()
	pendings := l.mu.pendingTruncate
	l.mu.pendingTruncate = make(map[uint64]struct{})
	l.mu.Unlock()

	for shardID := range pendings {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if err := func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			lsn, err := l.getTruncatedLsn(ctx, shardID)
			if err != nil {
				l.logger.Error("GetTruncatedIndex failed", zap.Error(err))
				// FIXME: check error type, see whether it is a tmp one
				return err
			}
			// the first 4 entries for a 3-replica raft group are tiny anyway
			if lsn > 1 {
				opts := dragonboat.SnapshotOption{
					OverrideCompactionOverhead: true,
					CompactionIndex:            lsn - 1,
				}
				if _, err := l.nh.SyncRequestSnapshot(ctx, shardID, opts); err != nil {
					l.logger.Error("SyncRequestSnapshot failed", zap.Error(err))
					// FIXME: check error type, see whether it is a tmp one
					return err
				}
			}
			return nil
		}(); err != nil {
			return err
		}
	}
	return nil
}

func (l *store) getHeartbeatMessage() pb.LogStoreHeartbeat {
	m := pb.LogStoreHeartbeat{
		UUID:           l.id(),
		RaftAddress:    l.cfg.RaftAddress,
		ServiceAddress: l.cfg.ServiceAddress,
		GossipAddress:  l.cfg.GossipAddress,
		Replicas:       make([]pb.LogReplicaInfo, 0),
	}
	opts := dragonboat.NodeHostInfoOption{
		SkipLogInfo: true,
	}
	nhi := l.nh.GetNodeHostInfo(opts)
	for _, ci := range nhi.ShardInfoList {
		if ci.Pending {
			l.logger.Info(fmt.Sprintf("shard %d is pending, not included into the heartbeat",
				ci.ShardID))
			continue
		}
		if ci.ConfigChangeIndex == 0 {
			panic("ci.ConfigChangeIndex is 0")
		}
		replicaInfo := pb.LogReplicaInfo{
			LogShardInfo: pb.LogShardInfo{
				ShardID:  ci.ShardID,
				Replicas: ci.Nodes,
				Epoch:    ci.ConfigChangeIndex,
				LeaderID: ci.LeaderID,
				Term:     ci.Term,
			},
			ReplicaID: ci.ReplicaID,
		}
		// FIXME: why we need this?
		if replicaInfo.Replicas == nil {
			replicaInfo.Replicas = make(map[uint64]dragonboat.Target)
		}
		m.Replicas = append(m.Replicas, replicaInfo)
	}
	return m
}
