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
	"errors"
	"fmt"
	"github.com/lni/vfs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/lni/dragonboat/v4"
	cli "github.com/lni/dragonboat/v4/client"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/plugin/tan"
	"github.com/lni/dragonboat/v4/plugin/tee"
	"github.com/lni/dragonboat/v4/raftpb"
	sm "github.com/lni/dragonboat/v4/statemachine"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
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

// checkNFSFilesystem checks if the given directory is on an NFS filesystem.
// Returns (isNFS, error) where:
//   - isNFS: true if NFS is detected
//   - error: non-nil if NFS is detected and there's a problem (for backward compatibility)
//
// NFS filesystems have limitations that affect Dragonboat:
//   - Directory fsync() is not supported, causing "operation not supported" errors
//   - File locking may be unreliable across network partitions
//   - Atomic operations may not be guaranteed
//
// Dragonboat (the underlying Raft library) requires these operations in startShard,
// which will fail with "operation not supported" on NFS. Specifically:
//   - In startShard -> CreateSnapshotDir -> Mkdir -> SyncDir -> df.Sync()
//   - The SyncDir function calls fsync() on a directory file descriptor
//   - NFS does not support fsync() on directories, causing "operation not supported"
//
// See: dragonboat/internal/fileutil/utils.go:SyncDir() line 168
//
//	dragonboat/internal/server/environment.go:CreateSnapshotDir() line 184
//	dragonboat/nodehost.go:startShard() line 1657
func checkNFSFilesystem(dataDir string) (bool, error) {
	if dataDir == "" {
		return false, nil
	}

	// Get absolute path
	absPath, err := filepath.Abs(dataDir)
	if err != nil {
		return false, nil // If we can't get absolute path, skip the check
	}

	// Ensure the directory exists or can be created
	if err := os.MkdirAll(absPath, 0755); err != nil {
		return false, nil // If we can't create it, skip the check
	}

	var nfsDetected bool
	var nfsError error

	// Check if the path is on NFS by reading /proc/mounts (Linux)
	// On Linux, we can check /proc/mounts to detect NFS mounts
	if mountInfo, err := os.ReadFile("/proc/mounts"); err == nil {
		lines := strings.Split(string(mountInfo), "\n")
		for _, line := range lines {
			fields := strings.Fields(line)
			if len(fields) >= 3 {
				mountPoint := fields[1]
				fstype := fields[2]
				// Check if it's an NFS filesystem (nfs, nfs4, nfsd, etc.)
				if strings.HasPrefix(fstype, "nfs") {
					// Check if our dataDir is under this mount point
					// Use the longest matching mount point to handle nested mounts
					if strings.HasPrefix(absPath, mountPoint) {
						nfsDetected = true
						nfsError = moerr.NewInternalErrorNoCtx(
							fmt.Sprintf("logservice DataDir '%s' is on an NFS filesystem (type: %s, mount: %s). "+
								"NFS filesystems have limitations that affect Dragonboat (the Raft library):\n"+
								"  - Directory fsync() is not supported (will cause 'operation not supported')\n"+
								"  - File locking may be unreliable across network partitions\n"+
								"  - Atomic operations may not be guaranteed\n\n"+
								"An NFS compatibility layer will be used, but this reduces durability guarantees. "+
								"For production use, please use a local filesystem (ext4, xfs, etc.) or a properly "+
								"configured network block device (iSCSI, FC) instead.",
								dataDir, fstype, mountPoint))
					}
				}
			}
		}
	}

	// Additional check: try to detect unsupported operations by testing common
	// operations that Dragonboat uses in startShard. This is a best-effort check,
	// as some NFS configurations might pass this but still fail later.
	testFile := filepath.Join(absPath, ".nfs_check")
	if f, err := os.Create(testFile); err == nil {
		// Test 1: Try to sync the file - if this fails with "operation not supported",
		// it's likely an unsupported filesystem
		if err := f.Sync(); err != nil {
			if strings.Contains(err.Error(), "operation not supported") ||
				strings.Contains(err.Error(), "not supported") {
				os.Remove(testFile)
				nfsDetected = true
				nfsError = moerr.NewInternalErrorNoCtx(
					fmt.Sprintf("logservice DataDir '%s' appears to be on a filesystem that doesn't support "+
						"required operations (fsync failed: %v). This is the same error that will occur when "+
						"Dragonboat's startShard tries to initialize Raft shards. An NFS compatibility layer "+
						"will be used, but this reduces durability guarantees. For production use, please use "+
						"a local filesystem instead.",
						dataDir, err))
			}
		}
		f.Close()

		// Test 2: Try to open the directory and sync it (syncfs operation)
		// This is another operation that Dragonboat may use
		if dir, err := os.Open(absPath); err == nil {
			if err := dir.Sync(); err != nil {
				if strings.Contains(err.Error(), "operation not supported") ||
					strings.Contains(err.Error(), "not supported") {
					dir.Close()
					os.Remove(testFile)
					nfsDetected = true
					if nfsError == nil {
						nfsError = moerr.NewInternalErrorNoCtx(
							fmt.Sprintf("logservice DataDir '%s' appears to be on a filesystem that doesn't support "+
								"directory sync operations (syncfs failed: %v). An NFS compatibility layer will be used, "+
								"but this reduces durability guarantees. For production use, please use a local filesystem instead.",
								dataDir, err))
					}
				}
			}
			dir.Close()
		}

		os.Remove(testFile)
	}

	return nfsDetected, nfsError
}

func getNodeHostConfig(cfg Config, fs fileservice.FileService) config.NodeHostConfig {
	meta := storeMeta{
		serviceAddress: cfg.LogServiceServiceAddr(),
	}
	if cfg.GossipProbeInterval.Duration == 0 {
		panic("cfg.GossipProbeInterval.Duration is 0")
	}
	logdb := config.GetTinyMemLogDBConfig()
	logdb.KVWriteBufferSize = cfg.LogDBBufferSize
	logdbFactory := (config.LogDBFactory)(nil)
	logdbFactory = tan.Factory
	logdb.MaxLogFileSize = cfg.LogDBMaxLogFileSize
	logdb.DisablePrealloc = cfg.LogDBDisablePrealloc
	if cfg.UseTeeLogDB {
		logutil.Warn("using tee based logdb backed by pebble and tan, for testing purposes only")
		logdbFactory = tee.TanPebbleLogDBFactory
	}
	c := config.NodeHostConfig{
		DeploymentID:        cfg.DeploymentID,
		NodeHostID:          cfg.UUID,
		NodeHostDir:         cfg.DataDir,
		RTTMillisecond:      cfg.RTTMillisecond,
		AddressByNodeHostID: true,
		RaftAddress:         cfg.RaftServiceAddr(),
		ListenAddress:       cfg.RaftListenAddr(),
		Expert: config.ExpertConfig{
			FS:           cfg.FS,
			LogDBFactory: logdbFactory,
			// FIXME: dragonboat need to be updated to make this field a first class
			// citizen
			TestGossipProbeInterval: cfg.GossipProbeInterval.Duration,
			LogDB:                   logdb,
			ExplicitHostname:        cfg.ExplicitHostname,
			MembershipImmovable:     cfg.MembershipImmovable,
		},
		Gossip: config.GossipConfig{
			BindAddress:      cfg.GossipListenAddr(),
			AdvertiseAddress: cfg.GossipServiceAddr(),
			Seed:             cfg.GossipSeedAddresses,
			Meta:             meta.marshal(),
			CanUseSelfAsSeed: cfg.GossipAllowSelfAsSeed,
		},
	}
	if cfg.ArchiveLogEnabled {
		c.Expert.ArchiveIO = newArchiveIO(cfg.FS, fs)
	}
	return c
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
	runtime           runtime.Runtime

	bootstrapCheckCycles uint64
	bootstrapMgr         *bootstrap.Manager

	taskScheduler hakeeper.TaskScheduler

	mu struct {
		sync.Mutex
		metadata metadata.LogStore
	}
	shardSnapshotInfo shardSnapshotInfo
	snapshotMgr       *snapshotManager

	// onReplicaChanged is a called when the replicas on the store changes.
	onReplicaChanged func(shardID uint64, replicaID uint64, typ ChangeType)
}

func newLogStore(cfg Config,
	taskServiceGetter func() taskservice.TaskService,
	onReplicaChanged func(shardID uint64, replicaID uint64, typ ChangeType),
	rt runtime.Runtime,
	fs fileservice.FileService,
) (*store, error) {
	// Check if DataDir is on an NFS filesystem before initializing dragonboat
	nfsDetected, nfsErr := checkNFSFilesystem(cfg.DataDir)
	if nfsErr != nil {
		// If NFS is detected, wrap the vfs.FS with NFS compatibility layer
		// This allows Dragonboat to work on NFS by gracefully handling unsupported operations
		if cfg.FS == nil || cfg.FS == vfs.Default {
			// Use default FS wrapped with NFS compatibility
			cfg.FS = newNFSCompatibleFS(vfs.Default)
			rt.SubLogger(runtime.SystemInit).Warn("NFS filesystem detected for logservice DataDir. "+
				"Using NFS compatibility mode with reduced durability guarantees. "+
				"This is not recommended for production use.",
				zap.String("dataDir", cfg.DataDir),
				zap.Error(nfsErr))
		} else {
			// Wrap the existing FS with NFS compatibility
			cfg.FS = newNFSCompatibleFS(cfg.FS)
			rt.SubLogger(runtime.SystemInit).Warn("NFS filesystem detected for logservice DataDir. "+
				"Wrapping provided vfs.FS with NFS compatibility layer. "+
				"This may reduce durability guarantees.",
				zap.String("dataDir", cfg.DataDir),
				zap.Error(nfsErr))
		}
	} else if nfsDetected {
		// NFS detected but no error (e.g., during testing), still wrap for safety
		if cfg.FS == nil || cfg.FS == vfs.Default {
			cfg.FS = newNFSCompatibleFS(vfs.Default)
		} else {
			cfg.FS = newNFSCompatibleFS(cfg.FS)
		}
		rt.SubLogger(runtime.SystemInit).Info("NFS filesystem detected, using compatibility mode",
			zap.String("dataDir", cfg.DataDir))
	}

	nh, err := dragonboat.NewNodeHost(getNodeHostConfig(cfg, fs))
	if err != nil {
		return nil, err
	}
	hakeeperConfig := cfg.GetHAKeeperConfig()
	rt.SubLogger(runtime.SystemInit).Info("HAKeeper Timeout Configs",
		zap.Int64("LogStoreTimeout", int64(hakeeperConfig.LogStoreTimeout)),
		zap.Int64("DNStoreTimeout", int64(hakeeperConfig.TNStoreTimeout)),
		zap.Int64("CNStoreTimeout", int64(hakeeperConfig.CNStoreTimeout)),
	)
	ls := &store{
		cfg:           cfg,
		nh:            nh,
		checker:       checkers.NewCoordinator(cfg.UUID, hakeeperConfig),
		taskScheduler: task.NewScheduler(cfg.UUID, taskServiceGetter, hakeeperConfig),
		alloc:         newIDAllocator(),
		stopper:       stopper.NewStopper("log-store"),
		tickerStopper: stopper.NewStopper("hakeeper-ticker"),
		runtime:       rt,

		shardSnapshotInfo: newShardSnapshotInfo(),
		snapshotMgr:       newSnapshotManager(&cfg),
		onReplicaChanged:  onReplicaChanged,
	}
	ls.mu.metadata = metadata.LogStore{UUID: cfg.UUID}
	if err := ls.stopper.RunNamedTask("truncation-worker", func(ctx context.Context) {
		rt.SubLogger(runtime.SystemInit).Info("logservice truncation worker started")
		ls.truncationWorker(ctx)
	}); err != nil {
		return nil, err
	}
	return ls, nil
}

// newLogStoreWithRetry mainly used in tests which create new log store.
// If an error occurred and the error is syscall.EADDRINUSE, retry to
// create a new log store instance.
func newLogStoreWithRetry(
	genCfg func() Config,
	taskServiceGetter func() taskservice.TaskService,
	onReplicaChanged func(shardID uint64, replicaID uint64, typ ChangeType),
	rt runtime.Runtime,
	fs fileservice.FileService,
) (*store, error) {
	for {
		s, err := newLogStore(genCfg(), taskServiceGetter, onReplicaChanged, rt, fs)
		if err != nil {
			if strings.Contains(err.Error(), "address already in use") {
				continue
			}
			return nil, err
		}
		return s, nil
	}
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
			if !rec.NonVoting {
				if err := l.startHAKeeperReplica(rec.ReplicaID, nil, false); err != nil {
					return err
				}
			} else {
				if err := l.startHAKeeperNonVotingReplica(rec.ReplicaID, nil, false); err != nil {
					return err
				}
			}
		} else {
			if !rec.NonVoting {
				if err := l.startReplica(rec.ShardID, rec.ReplicaID, nil, false); err != nil {
					return err
				}
			} else {
				if err := l.startNonVotingReplica(rec.ShardID, rec.ReplicaID, nil, false); err != nil {
					return err
				}
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
	l.addMetadata(hakeeper.DefaultHAKeeperShardID, replicaID, false)
	atomic.StoreUint64(&l.haKeeperReplicaID, replicaID)
	if !l.cfg.DisableWorkers {
		if err := l.tickerStopper.RunNamedTask("hakeeper-ticker", func(ctx context.Context) {
			l.runtime.SubLogger(runtime.SystemInit).Info("HAKeeper ticker started")
			l.ticker(ctx)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (l *store) startHAKeeperNonVotingReplica(replicaID uint64,
	initialReplicas map[uint64]dragonboat.Target, join bool) error {
	raftConfig := getRaftConfig(hakeeper.DefaultHAKeeperShardID, replicaID)
	raftConfig.IsNonVoting = true
	if err := l.nh.StartReplica(initialReplicas,
		join, hakeeper.NewStateMachine, raftConfig); err != nil {
		return err
	}
	l.addMetadata(hakeeper.DefaultHAKeeperShardID, replicaID, true)
	atomic.StoreUint64(&l.haKeeperReplicaID, replicaID)
	if !l.cfg.DisableWorkers {
		if err := l.tickerStopper.RunNamedTask("hakeeper-ticker", func(ctx context.Context) {
			l.runtime.SubLogger(runtime.SystemInit).Info("HAKeeper ticker started")
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
		return moerr.NewInvalidInputNoCtxf("shardID %d does not match DefaultHAKeeperShardID %d", shardID, hakeeper.DefaultHAKeeperShardID)
	}
	cfg := getRaftConfig(shardID, replicaID)
	if err := l.snapshotMgr.Init(shardID, replicaID); err != nil {
		panic(err)
	}
	if err := l.nh.StartReplica(initialReplicas, join, newStateMachine, cfg); err != nil {
		return err
	}
	l.addMetadata(shardID, replicaID, false)
	if l.onReplicaChanged != nil {
		l.onReplicaChanged(shardID, replicaID, AddReplica)
	}
	return nil
}

func (l *store) startNonVotingReplica(shardID uint64, replicaID uint64,
	initialReplicas map[uint64]dragonboat.Target, join bool) error {
	if shardID == hakeeper.DefaultHAKeeperShardID {
		return moerr.NewInvalidInputNoCtx(fmt.Sprintf(
			"shardID %d does not match DefaultHAKeeperShardID %d",
			shardID, hakeeper.DefaultHAKeeperShardID),
		)
	}
	cfg := getRaftConfig(shardID, replicaID)
	cfg.IsNonVoting = true
	if err := l.snapshotMgr.Init(shardID, replicaID); err != nil {
		panic(err)
	}
	if err := l.nh.StartReplica(initialReplicas, join, newStateMachine, cfg); err != nil {
		return err
	}
	l.addMetadata(shardID, replicaID, true)
	if l.onReplicaChanged != nil {
		l.onReplicaChanged(shardID, replicaID, AddReplica)
	}
	return nil
}

func (l *store) stopReplica(shardID uint64, replicaID uint64) error {
	if shardID == hakeeper.DefaultHAKeeperShardID {
		defer func() {
			atomic.StoreUint64(&l.haKeeperReplicaID, 0)
		}()
	}
	if err := l.nh.StopReplica(shardID, replicaID); err != nil {
		return err
	}
	if l.onReplicaChanged != nil {
		l.onReplicaChanged(shardID, replicaID, DelReplica)
	}
	return nil
}

func (l *store) requestLeaderTransfer(shardID uint64, targetReplicaID uint64) error {
	return l.nh.RequestLeaderTransfer(shardID, targetReplicaID)
}

func (l *store) addReplica(shardID uint64, replicaID uint64,
	target dragonboat.Target, cci uint64) error {
	// Set timeout to a little bigger value to prevent Timeout Error and
	// returns a dragonboat.ErrRejected at last, in which case, it will take
	// longer time to finish this operation.
	ctx, cancel := context.WithTimeoutCause(context.Background(), time.Second*5, moerr.CauseAddReplica)
	defer cancel()
	count := 0
	for {
		count++
		if err := l.nh.SyncRequestAddReplica(ctx, shardID, replicaID, target, cci); err != nil {
			err = moerr.AttachCause(ctx, err)
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

func (l *store) addNonVotingReplica(
	shardID uint64,
	replicaID uint64,
	target dragonboat.Target,
	cci uint64,
) error {
	ctx, cancel := context.WithTimeoutCause(context.Background(), time.Second*5, moerr.CauseAddNonVotingReplica)
	defer cancel()
	count := 0
	for {
		count++
		if err := l.nh.SyncRequestAddNonVoting(ctx, shardID, replicaID, target, cci); err != nil {
			err = moerr.AttachCause(ctx, err)
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
	ctx, cancel := context.WithTimeoutCause(context.Background(), time.Second, moerr.CauseRemoveReplica)
	defer cancel()
	count := 0
	for {
		count++
		if err := l.nh.SyncRequestDeleteReplica(ctx, shardID, replicaID, cci); err != nil {
			err = moerr.AttachCause(ctx, err)
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
		if l.onReplicaChanged != nil {
			l.onReplicaChanged(shardID, replicaID, DelReplica)
		}
		return nil
	}
}

func (l *store) addLogShard(ctx context.Context, addLogShard pb.AddLogShard) error {
	cmd := hakeeper.GetAddLogShardCmd(addLogShard)
	session := l.nh.GetNoOPSession(hakeeper.DefaultHAKeeperShardID)
	if _, err := l.propose(ctx, session, cmd); err != nil {
		l.runtime.Logger().Error("failed to propose add log shard",
			zap.Uint64("shard ID", addLogShard.ShardID),
			zap.Error(err),
		)
		return handleNotHAKeeperError(ctx, err)
	}
	return nil
}

func (l *store) checkHealth(shardID uint64) error {
	opts := dragonboat.NodeHostInfoOption{
		SkipLogInfo: true,
	}
	nhi := l.nh.GetNodeHostInfo(opts)
	for _, ci := range nhi.ShardInfoList {
		if ci.ShardID == shardID {
			if ci.Pending {
				return moerr.NewInternalErrorNoCtxf("shard %d is pending on store %s",
					shardID, l.cfg.UUID)
			}
			break
		}
	}
	cs, err := l.getCheckerState()
	if err != nil {
		return err
	}
	storeInfo, ok := cs.LogState.Stores[l.cfg.UUID]
	if ok {
		if storeInfo.Tick >= cs.Tick {
			return nil
		}
		if (cs.Tick-storeInfo.Tick)*uint64(l.cfg.HAKeeperTickInterval.Duration) >
			uint64(l.cfg.HAKeeperConfig.LogStoreTimeout.Duration) {
			return moerr.NewInternalErrorNoCtxf("log store %s of shard %d expired",
				l.cfg.UUID, shardID)
		} else {
			return nil
		}
	} else {
		return moerr.NewInternalErrorNoCtxf("shard %d not started on store %s",
			shardID, l.cfg.UUID)
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
			if errors.Is(err, dragonboat.ErrShardNotReady) ||
				errors.Is(err, dragonboat.ErrSystemBusy) {
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

func (l *store) getOrExtendTNLease(ctx context.Context,
	shardID uint64, tnID uint64) error {
	session := l.nh.GetNoOPSession(shardID)
	cmd := getSetLeaseHolderCmd(tnID)
	_, err := l.propose(ctx, session, cmd)
	return err
}

func (l *store) truncateLog(ctx context.Context,
	shardID uint64, index Lsn) error {
	session := l.nh.GetNoOPSession(shardID)
	cmd := getSetTruncatedLsnCmd(index)
	result, err := l.propose(ctx, session, cmd)
	if err != nil {
		l.runtime.Logger().Error("propose truncate log cmd failed", zap.Error(err))
		return err
	}
	if result.Value > 0 {
		l.runtime.Logger().Error(fmt.Sprintf("shardID %d already truncated to index %d", shardID, result.Value))
		return moerr.NewInvalidTruncateLsn(ctx, shardID, result.Value)
	}
	return nil
}

func (l *store) append(ctx context.Context,
	shardID uint64, cmd []byte) (Lsn, error) {
	session := l.nh.GetNoOPSession(shardID)
	result, err := l.propose(ctx, session, cmd)
	if err != nil {
		l.runtime.Logger().Error("propose failed", zap.Error(err))
		return 0, err
	}
	if len(result.Data) > 0 {
		l.runtime.Logger().Error("not current lease holder", zap.Uint64("data", binaryEnc.Uint64(result.Data)))
		return 0, moerr.NewNotLeaseHolder(ctx, binaryEnc.Uint64(result.Data))
	}
	if result.Value == 0 {
		panic(moerr.NewInvalidState(ctx, "unexpected Lsn value"))
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
		l.runtime.Logger().Error("failed to propose tso updat", zap.Error(err))
		return 0, err
	}
	return result.Value, nil
}

func handleNotHAKeeperError(ctx context.Context, err error) error {
	if err == nil {
		return err
	}
	if errors.Is(err, dragonboat.ErrShardNotFound) {
		return moerr.NewNoHAKeeper(ctx)
	}
	return err
}

func (l *store) addLogStoreHeartbeat(ctx context.Context,
	hb pb.LogStoreHeartbeat) (pb.CommandBatch, error) {
	data := MustMarshal(&hb)
	cmd := hakeeper.GetLogStoreHeartbeatCmd(data)
	session := l.nh.GetNoOPSession(hakeeper.DefaultHAKeeperShardID)
	if result, err := l.propose(ctx, session, cmd); err != nil {
		l.runtime.Logger().Error("propose failed", zap.Error(err))
		return pb.CommandBatch{}, handleNotHAKeeperError(ctx, err)
	} else {
		var cb pb.CommandBatch
		MustUnmarshal(&cb, result.Data)
		return cb, nil
	}
}

func (l *store) addCNStoreHeartbeat(ctx context.Context,
	hb pb.CNStoreHeartbeat) (pb.CommandBatch, error) {
	data := MustMarshal(&hb)
	cmd := hakeeper.GetCNStoreHeartbeatCmd(data)
	session := l.nh.GetNoOPSession(hakeeper.DefaultHAKeeperShardID)
	if result, err := l.propose(ctx, session, cmd); err != nil {
		l.runtime.Logger().Error("propose failed", zap.Error(err))
		return pb.CommandBatch{}, handleNotHAKeeperError(ctx, err)
	} else {
		var cb pb.CommandBatch
		MustUnmarshal(&cb, result.Data)
		return cb, nil
	}
}

func (l *store) cnAllocateID(ctx context.Context,
	req pb.CNAllocateID) (uint64, error) {
	cmd := hakeeper.GetAllocateIDCmd(req)
	session := l.nh.GetNoOPSession(hakeeper.DefaultHAKeeperShardID)
	result, err := l.propose(ctx, session, cmd)
	if err != nil {
		l.runtime.Logger().Error("propose get id failed", zap.Error(err))
		return 0, err
	}
	return result.Value, nil
}

func (l *store) addTNStoreHeartbeat(ctx context.Context,
	hb pb.TNStoreHeartbeat) (pb.CommandBatch, error) {
	data := MustMarshal(&hb)
	cmd := hakeeper.GetTNStoreHeartbeatCmd(data)
	session := l.nh.GetNoOPSession(hakeeper.DefaultHAKeeperShardID)
	if result, err := l.propose(ctx, session, cmd); err != nil {
		l.runtime.Logger().Error("propose failed", zap.Error(err))
		return pb.CommandBatch{}, handleNotHAKeeperError(ctx, err)
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
		return pb.CommandBatch{}, handleNotHAKeeperError(ctx, err)
	}
	return *(v.(*pb.CommandBatch)), nil
}

func (l *store) getClusterDetails(ctx context.Context) (pb.ClusterDetails, error) {
	v, err := l.read(ctx,
		hakeeper.DefaultHAKeeperShardID, &hakeeper.ClusterDetailsQuery{Cfg: l.cfg.GetHAKeeperConfig()})
	if err != nil {
		return pb.ClusterDetails{}, handleNotHAKeeperError(ctx, err)
	}
	return *(v.(*pb.ClusterDetails)), nil
}

func (l *store) addScheduleCommands(ctx context.Context,
	term uint64, cmds []pb.ScheduleCommand) error {
	cmd := hakeeper.GetUpdateCommandsCmd(term, cmds)
	session := l.nh.GetNoOPSession(hakeeper.DefaultHAKeeperShardID)
	if _, err := l.propose(ctx, session, cmd); err != nil {
		return handleNotHAKeeperError(ctx, err)
	}
	return nil
}

func (l *store) updateCNLabel(ctx context.Context, label pb.CNStoreLabel) error {
	state, err := l.getCheckerState()
	if err != nil {
		return err
	}
	if _, ok := state.CNState.Stores[label.UUID]; !ok {
		return moerr.NewInternalErrorf(ctx, "CN [%s] does not exist", label.UUID)
	}
	cmd := hakeeper.GetUpdateCNLabelCmd(label)
	session := l.nh.GetNoOPSession(hakeeper.DefaultHAKeeperShardID)
	if result, err := l.propose(ctx, session, cmd); err != nil {
		l.runtime.Logger().Error("failed to propose CN label",
			zap.String("label", label.String()),
			zap.Error(err))
		return handleNotHAKeeperError(ctx, err)
	} else {
		var cb pb.CommandBatch
		MustUnmarshal(&cb, result.Data)
		return nil
	}
}

func (l *store) updateCNWorkState(ctx context.Context, workState pb.CNWorkState) error {
	state, err := l.getCheckerState()
	if err != nil {
		return err
	}
	if _, ok := state.CNState.Stores[workState.UUID]; !ok {
		return moerr.NewInternalErrorf(ctx, "CN [%s] does not exist", workState.UUID)
	}
	cmd := hakeeper.GetUpdateCNWorkStateCmd(workState)
	session := l.nh.GetNoOPSession(hakeeper.DefaultHAKeeperShardID)
	if result, err := l.propose(ctx, session, cmd); err != nil {
		l.runtime.Logger().Error("failed to propose CN work state",
			zap.String("state", state.String()),
			zap.Error(err))
		return handleNotHAKeeperError(ctx, err)
	} else {
		var cb pb.CommandBatch
		MustUnmarshal(&cb, result.Data)
		return nil
	}
}

func (l *store) patchCNStore(ctx context.Context, stateLabel pb.CNStateLabel) error {
	state, err := l.getCheckerState()
	if err != nil {
		return err
	}
	if _, ok := state.CNState.Stores[stateLabel.UUID]; !ok {
		return moerr.NewInternalErrorf(ctx, "CN [%s] does not exist", stateLabel.UUID)
	}
	cmd := hakeeper.GetPatchCNStoreCmd(stateLabel)
	session := l.nh.GetNoOPSession(hakeeper.DefaultHAKeeperShardID)
	if result, err := l.propose(ctx, session, cmd); err != nil {
		l.runtime.Logger().Error("failed to propose CN patch store",
			zap.String("state", state.String()),
			zap.Error(err))
		return handleNotHAKeeperError(ctx, err)
	} else {
		var cb pb.CommandBatch
		MustUnmarshal(&cb, result.Data)
		return nil
	}
}

func (l *store) deleteCNStore(ctx context.Context, cnStore pb.DeleteCNStore) error {
	state, err := l.getCheckerState()
	if err != nil {
		return err
	}
	if _, ok := state.CNState.Stores[cnStore.StoreID]; !ok {
		return nil
	}
	cmd := hakeeper.GetDeleteCNStoreCmd(cnStore)
	session := l.nh.GetNoOPSession(hakeeper.DefaultHAKeeperShardID)
	if result, err := l.propose(ctx, session, cmd); err != nil {
		l.runtime.Logger().Error("failed to propose delete CN store",
			zap.String("state", state.String()),
			zap.Error(err))
		return handleNotHAKeeperError(ctx, err)
	} else {
		var cb pb.CommandBatch
		MustUnmarshal(&cb, result.Data)
		return nil
	}
}

func (l *store) addProxyHeartbeat(ctx context.Context, hb pb.ProxyHeartbeat) (pb.CommandBatch, error) {
	data := MustMarshal(&hb)
	cmd := hakeeper.GetProxyHeartbeatCmd(data)
	session := l.nh.GetNoOPSession(hakeeper.DefaultHAKeeperShardID)
	if result, err := l.propose(ctx, session, cmd); err != nil {
		l.runtime.Logger().Error("propose failed", zap.Error(err))
		return pb.CommandBatch{}, handleNotHAKeeperError(ctx, err)
	} else {
		var cb pb.CommandBatch
		MustUnmarshal(&cb, result.Data)
		return cb, nil
	}
}

func (l *store) updateNonVotingReplicaNum(ctx context.Context, num uint64) error {
	cmd := hakeeper.GetUpdateNonVotingReplicaNumCmd(num)
	session := l.nh.GetNoOPSession(hakeeper.DefaultHAKeeperShardID)
	if _, err := l.propose(ctx, session, cmd); err != nil {
		l.runtime.Logger().Error("failed to propose update non-voting-replica-num",
			zap.Uint64("num", num),
			zap.Error(err))
		return handleNotHAKeeperError(ctx, err)
	}
	return nil
}

func (l *store) updateNonVotingLocality(ctx context.Context, locality pb.Locality) error {
	cmd := hakeeper.GetUpdateNonVotingLocality(locality)
	session := l.nh.GetNoOPSession(hakeeper.DefaultHAKeeperShardID)
	if _, err := l.propose(ctx, session, cmd); err != nil {
		l.runtime.Logger().Error("failed to propose update non-voting-locality",
			zap.Any("locality", locality),
			zap.Error(err))
		return handleNotHAKeeperError(ctx, err)
	}
	return nil
}

func (l *store) getLatestLsn(ctx context.Context, shardID uint64) (uint64, error) {
	v, err := l.read(ctx, shardID, indexQuery{})
	if err != nil {
		return 0, err
	}
	return v.(uint64), nil
}

func (l *store) setRequiredLsn(ctx context.Context, shardID uint64, lsn Lsn) error {
	session := l.nh.GetNoOPSession(shardID)
	cmd := getSetRequiredLsnCmd(lsn)
	result, err := l.propose(ctx, session, cmd)
	if err != nil {
		l.runtime.Logger().Error("propose set required lsn cmd failed", zap.Error(err))
		return err
	}
	if result.Value > 0 {
		l.runtime.Logger().Warn(fmt.Sprintf("shardID %d already set required to lsn %d", shardID, result.Value))
	}
	return nil
}

func (l *store) getRequiredLsn(ctx context.Context, shardID uint64) (uint64, error) {
	v, err := l.read(ctx, shardID, requiredLsnQuery{})
	if err != nil {
		return 0, err
	}
	return v.(uint64), nil
}

func (l *store) decodeCmd(ctx context.Context, e raftpb.Entry) []byte {
	if e.Type == raftpb.ApplicationEntry {
		panic(moerr.NewInvalidState(ctx, "unexpected entry type"))
	}
	if e.Type == raftpb.EncodedEntry {
		if e.Cmd[0] != 0 {
			panic(moerr.NewInvalidState(ctx, "unexpected cmd header"))
		}
		return e.Cmd[1:]
	}
	panic(moerr.NewInvalidState(ctx, "invalid cmd"))
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
		cmd := l.decodeCmd(ctx, e)
		if isSetLeaseHolderUpdate(cmd) {
			result = append(result, LogRecord{
				Type: pb.LeaseUpdate,
				Lsn:  e.Index,
			})
			continue
		}
		if isUserUpdate(cmd) {
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
		l.runtime.Logger().Error("QueryRaftLog failed", zap.Error(err))
		return nil, 0, err
	}
	select {
	case v := <-rs.ResultC():
		if v.Completed() {
			entries, logRange := v.RaftLogs()
			next := getNextIndex(entries, firstIndex, logRange.LastIndex)
			results, err := l.markEntries(ctx, shardID, entries)
			if err != nil {
				l.runtime.Logger().Error("markEntries failed", zap.Error(err))
				return nil, 0, err
			}
			return results, next, nil
		} else if v.RequestOutOfRange() {
			// FIXME: add more details to the log, what is the available range
			l.runtime.Logger().Error("OutOfRange query found")
			return nil, 0, dragonboat.ErrInvalidRange
		}
		panic(moerr.NewInvalidState(ctx, "unexpected rs state"))
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	}
}

func (l *store) queryLogLsn(ctx context.Context, shardID uint64, ts time.Time) (Lsn, error) {
	rs, err := l.nh.QueryLogLsn(shardID, ts)
	if err != nil {
		l.runtime.Logger().Error("QueryLogLsn failed", zap.Error(err))
		return 0, err
	}
	select {
	case v := <-rs.ResultC():
		if v.Completed() {
			_, logRange := v.RaftLogs()
			return logRange.FirstIndex, nil
		} else if v.RequestOutOfRange() {
			l.runtime.Logger().Error("OutOfRange query found, could not find the Lsn")
			return 0, dragonboat.ErrInvalidRange
		}
		panic(moerr.NewInvalidState(ctx, "unexpected rs state"))
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (l *store) tickerForTaskSchedule(ctx context.Context, duration time.Duration) {
	ticker := time.NewTicker(duration)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			state, _ := l.getCheckerStateFromLeader()
			if state != nil && state.State == pb.HAKeeperRunning {
				l.taskSchedule(state)
			}

		case <-ctx.Done():
			return
		}

		// l.taskSchedule could be blocking a long time, this extra select
		// can give a chance immediately to check the ctx status when it resumes.
		select {
		case <-ctx.Done():
			return
		default:
			// nothing to do
		}
	}

}

func (l *store) ticker(ctx context.Context) {
	if l.cfg.HAKeeperTickInterval.Duration == 0 {
		panic("invalid HAKeeperTickInterval")
	}
	l.runtime.Logger().Info("Hakeeper interval configs",
		zap.Int64("HAKeeperTickInterval", int64(l.cfg.HAKeeperTickInterval.Duration)),
		zap.Int64("HAKeeperCheckInterval", int64(l.cfg.HAKeeperCheckInterval.Duration)))
	ticker := time.NewTicker(l.cfg.HAKeeperTickInterval.Duration)
	defer ticker.Stop()
	if l.cfg.HAKeeperCheckInterval.Duration == 0 {
		panic("invalid HAKeeperCheckInterval")
	}
	defer func() {
		l.runtime.Logger().Info("HAKeeper ticker stopped")
	}()
	haTicker := time.NewTicker(l.cfg.HAKeeperCheckInterval.Duration)
	defer haTicker.Stop()

	// moving task schedule from the ticker normal routine to a
	// separate goroutine can avoid the hakeeper's health check and tick update
	// operations being blocked by task schedule, or the tick will be skipped and
	// can not correctly estimate the time passing.
	go l.tickerForTaskSchedule(ctx, l.cfg.HAKeeperCheckInterval.Duration)

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
		l.runtime.Logger().Error("failed to get HAKeeper Leader ID", zap.Error(err))
		return
	}

	if isLeader {
		cmd := hakeeper.GetTickCmd()
		ctx, cancel := context.WithTimeoutCause(context.Background(), hakeeperDefaultTimeout, moerr.CauseHakeeperTick)
		defer cancel()
		session := l.nh.GetNoOPSession(hakeeper.DefaultHAKeeperShardID)
		if _, err := l.propose(ctx, session, cmd); err != nil {
			err = moerr.AttachCause(ctx, err)
			l.runtime.Logger().Error("propose tick failed", zap.Error(err))
			return
		}
	}
}

func (l *store) getHeartbeatMessage() pb.LogStoreHeartbeat {
	m := pb.LogStoreHeartbeat{
		UUID:           l.id(),
		RaftAddress:    l.cfg.RaftServiceAddr(),
		ServiceAddress: l.cfg.LogServiceServiceAddr(),
		GossipAddress:  l.cfg.GossipServiceAddr(),
		Replicas:       make([]pb.LogReplicaInfo, 0),
		Locality:       l.cfg.getLocality(),
	}
	opts := dragonboat.NodeHostInfoOption{
		SkipLogInfo: true,
	}
	nhi := l.nh.GetNodeHostInfo(opts)
	for _, ci := range nhi.ShardInfoList {
		if ci.Pending {
			l.runtime.Logger().Info(fmt.Sprintf("shard %d is pending, not included into the heartbeat",
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
				// NonVotingReplicas are the non-voting replicas.
				NonVotingReplicas: ci.NonVotingNodes,
				Epoch:             ci.ConfigChangeIndex,
				LeaderID:          ci.LeaderID,
				Term:              ci.Term,
			},
			ReplicaID: ci.ReplicaID,
			// the non-voting role is only known to replica itself.
			IsNonVoting: ci.IsNonVoting,
		}
		// FIXME: why we need this?
		if replicaInfo.Replicas == nil {
			replicaInfo.Replicas = make(map[uint64]dragonboat.Target)
		}
		m.Replicas = append(m.Replicas, replicaInfo)
	}
	return m
}

// leaderID returns the leader ID of the specified shard.
func (l *store) leaderID(shardID uint64) (uint64, error) {
	leaderID, _, ok, err := l.nh.GetLeaderID(shardID)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	return leaderID, nil
}
