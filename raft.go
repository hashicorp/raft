package raft

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/armon/go-metrics"
)

var (
	// ErrLeader is returned when an operation can't be completed on a
	// leader node.
	ErrLeader = errors.New("node is the leader")

	// ErrNotLeader is returned when an operation can't be completed on a
	// follower or candidate node.
	ErrNotLeader = errors.New("node is not the leader")

	// ErrLeadershipLost is returned when a leader fails to commit a log entry
	// because it's been deposed in the process.
	ErrLeadershipLost = errors.New("leadership lost while committing log")

	// ErrRaftShutdown is returned when operations are requested against an
	// inactive Raft.
	ErrRaftShutdown = errors.New("raft is already shutdown")

	// ErrEnqueueTimeout is returned when a command fails due to a timeout.
	ErrEnqueueTimeout = errors.New("timed out enqueuing operation")

	// ErrKnownPeer is returned when trying to add a peer to the configuration
	// that already exists.
	ErrKnownPeer = errors.New("peer already known")

	// ErrUnknownPeer is returned when trying to remove a peer from the
	// configuration that doesn't exist.
	ErrUnknownPeer = errors.New("peer is unknown")

	// ErrNothingNewToSnapshot is returned when trying to create a snapshot
	// but there's nothing new commited to the FSM since we started.
	ErrNothingNewToSnapshot = errors.New("Nothing new to snapshot")
)

// Raft implements a Raft node.
type Raft struct {
	raftState

	// applyCh is used to async send logs to the main thread to
	// be committed and applied to the FSM.
	applyCh chan *logFuture

	// Configuration provided at Raft initialization
	conf *Config

	// FSM is the client state machine to apply commands to
	fsm FSM

	// fsmCommitCh is used to trigger async application of logs to the fsm
	fsmCommitCh chan commitTuple

	// fsmRestoreCh is used to trigger a restore from snapshot
	fsmRestoreCh chan *restoreFuture

	// fsmSnapshotCh is used to trigger a new snapshot being taken
	fsmSnapshotCh chan *reqSnapshotFuture

	// lastContact is the last time we had contact from the
	// leader node. This can be used to gauge staleness.
	lastContact     time.Time
	lastContactLock sync.RWMutex

	// Leader is the current cluster leader
	leader     ServerAddress
	leaderLock sync.RWMutex

	// leaderCh is used to notify of leadership changes
	leaderCh chan bool

	// leaderState used only while state is leader
	leaderState leaderState

	// Stores our local server ID, used to avoid sending RPCs to ourself
	localID ServerID

	// Stores our local addr
	localAddr ServerAddress

	// Used for our logging
	logger *log.Logger

	// LogStore provides durable storage for logs
	logs LogStore

	// Used to request the leader to make configuration changes.
	configurationChangeCh chan *configurationChangeFuture

	// Tracks the latest configuration and latest committed configuration from
	// the log/snapshot.
	configurations configurations

	// RPC chan comes from the transport layer
	rpcCh <-chan RPC

	// Shutdown channel to exit, protected to prevent concurrent exits
	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex

	// snapshots is used to store and retrieve snapshots
	snapshots SnapshotStore

	// snapshotCh is used for user triggered snapshots
	snapshotCh chan *snapshotFuture

	// stable is a StableStore implementation for durable state
	// It provides stable storage for many fields in raftState
	stable StableStore

	// The transport layer we use
	trans Transport

	// verifyCh is used to async send verify futures to the main thread
	// to verify we are still the leader
	verifyCh chan *verifyFuture

	// configurationsCh is used to get the configuration data safely from
	// outside of the main thread.
	configurationsCh chan *configurationsFuture

	// List of observers and the mutex that protects them. The observers list
	// is indexed by an artificial ID which is used for deregistration.
	observersLock sync.RWMutex
	observers     map[uint64]*Observer
}

// BootstrapCluster initializes a server's storage with the given cluster
// configuration. This should only be called at the beginning of time for the
// cluster, and you absolutely must make sure that you call it with the same
// configuration on all the Voter servers. There is no need to bootstrap
// Nonvoter and Staging servers.
//
// One sane approach is to boostrap a single server with a configuration
// listing just itself as a Voter, then invoke AddVoter() on it to add other
// servers to the cluster.
func BootstrapCluster(config *Config, logs LogStore, stable StableStore, snaps SnapshotStore, configuration Configuration) error {
	// Sanity check the configuration
	if err := checkConfiguration(configuration); err != nil {
		return err
	}

	// Make sure we don't have a current term
	currentTerm, err := stable.GetUint64(keyCurrentTerm)
	if err == nil {
		if currentTerm > 0 {
			return fmt.Errorf("Bootstrap only works on new clusters, found current term: %v", currentTerm)
		}
	} else {
		if err.Error() != "not found" {
			return fmt.Errorf("failed to read current term: %v", err)
		}
	}

	// Make sure we have an empty log
	lastIdx, err := logs.LastIndex()
	if err != nil {
		return fmt.Errorf("failed to get last log index: %v", err)
	}
	if lastIdx > 0 {
		return fmt.Errorf("Bootstrap only works on new clusters, found non-empty log, last index: %v", lastIdx)
	}

	// Make sure we have no snapshots
	snapshots, err := snaps.List()
	if err != nil {
		return fmt.Errorf("failed to list snapshots: %v", err)
	}
	if len(snapshots) > 0 {
		return fmt.Errorf("Bootstrap only works on new clusters, found snapshots")
	}

	// Set current term to 1
	if err := stable.SetUint64(keyCurrentTerm, 1); err != nil {
		return fmt.Errorf("failed to save current term: %v", err)
	}

	// Append configuration entry to log
	entry := &Log{
		Index: 1,
		Term:  1,
		Type:  LogConfiguration,
		Data:  encodeConfiguration(configuration),
	}
	if err := logs.StoreLog(entry); err != nil {
		return fmt.Errorf("failed to append configuration entry to log: %v", err)
	}

	return nil
}

// NewRaft is used to construct a new Raft node. It takes a configuration, as well
// as implementations of various interfaces that are required. If we have any old state,
// such as snapshots, logs, peers, etc, all those will be restored when creating the
// Raft node.
func NewRaft(conf *Config, fsm FSM, logs LogStore, stable StableStore, snaps SnapshotStore,
	trans Transport) (*Raft, error) {
	// Validate the configuration
	if err := ValidateConfig(conf); err != nil {
		return nil, err
	}

	// Ensure we have a LogOutput
	var logger *log.Logger
	if conf.Logger != nil {
		logger = conf.Logger
	} else {
		if conf.LogOutput == nil {
			conf.LogOutput = os.Stderr
		}
		logger = log.New(conf.LogOutput, "", log.LstdFlags)
	}

	// Try to restore the current term
	currentTerm, err := stable.GetUint64(keyCurrentTerm)
	if err != nil && err.Error() != "not found" {
		return nil, fmt.Errorf("failed to load current term: %v", err)
	}

	// Read the last log value
	lastIdx, err := logs.LastIndex()
	if err != nil {
		return nil, fmt.Errorf("failed to find last log: %v", err)
	}

	// Get the log
	var lastLog Log
	if lastIdx > 0 {
		if err = logs.GetLog(lastIdx, &lastLog); err != nil {
			return nil, fmt.Errorf("failed to get last log: %v", err)
		}
	}

	localAddr := ServerAddress(trans.LocalAddr())
	localID := conf.LocalID
	if localID == "" {
		logger.Printf("[WARN] raft: No server ID given, using network address: %v. This default will be removed in the future. Set server ID explicitly in config.",
			localAddr)
		localID = ServerID(localAddr)
	}

	// Create Raft struct
	r := &Raft{
		applyCh:       make(chan *logFuture),
		conf:          conf,
		fsm:           fsm,
		fsmCommitCh:   make(chan commitTuple, 128),
		fsmRestoreCh:  make(chan *restoreFuture),
		fsmSnapshotCh: make(chan *reqSnapshotFuture),
		leaderCh:      make(chan bool),
		localID:       localID,
		localAddr:     localAddr,
		logger:        logger,
		logs:          logs,
		configurationChangeCh: make(chan *configurationChangeFuture),
		configurations:        configurations{},
		rpcCh:                 trans.Consumer(),
		snapshots:             snaps,
		snapshotCh:            make(chan *snapshotFuture),
		shutdownCh:            make(chan struct{}),
		stable:                stable,
		trans:                 trans,
		verifyCh:              make(chan *verifyFuture, 64),
		configurationsCh:      make(chan *configurationsFuture, 8),
		observers:             make(map[uint64]*Observer),
	}

	// Initialize as a follower
	r.setState(Follower)

	// Start as leader if specified. This should only be used
	// for testing purposes.
	if conf.StartAsLeader {
		r.setState(Leader)
		r.setLeader(r.localAddr)
	}

	// Restore the current term and the last log
	r.setCurrentTerm(currentTerm)
	r.setLastLog(lastLog.Index, lastLog.Term)

	// Attempt to restore a snapshot if there are any
	if err := r.restoreSnapshot(); err != nil {
		return nil, err
	}

	// Scan through the log for any configuration change entries
	snapshotIndex, _ := r.getLastSnapshot()
	for index := snapshotIndex + 1; index <= lastLog.Index; index++ {
		var entry Log
		if err := r.logs.GetLog(index, &entry); err != nil {
			r.logger.Printf("[ERR] raft: Failed to get log at %d: %v", index, err)
			panic(err)
		}
		// TODO: support LogAddPeer, LogRemovePeer too
		if entry.Type == LogConfiguration {
			r.configurations.committed = r.configurations.latest
			r.configurations.committedIndex = r.configurations.latestIndex
			r.configurations.latest = decodeConfiguration(entry.Data)
			r.configurations.latestIndex = entry.Index
		}
	}
	r.logger.Printf("[INFO] NewRaft configurations: %+v", r.configurations)

	// Setup a heartbeat fast-path to avoid head-of-line
	// blocking where possible. It MUST be safe for this
	// to be called concurrently with a blocking RPC.
	trans.SetHeartbeatHandler(r.processHeartbeat)

	// Start the background work
	r.goFunc(r.run)
	r.goFunc(r.runFSM)
	r.goFunc(r.runSnapshots)
	return r, nil
}

// Leader is used to return the current leader of the cluster.
// It may return empty string if there is no current leader
// or the leader is unknown.
func (r *Raft) Leader() ServerAddress {
	r.leaderLock.RLock()
	leader := r.leader
	r.leaderLock.RUnlock()
	return leader
}

// Apply is used to apply a command to the FSM in a highly consistent
// manner. This returns a future that can be used to wait on the application.
// An optional timeout can be provided to limit the amount of time we wait
// for the command to be started. This must be run on the leader or it
// will fail.
func (r *Raft) Apply(cmd []byte, timeout time.Duration) ApplyFuture {
	metrics.IncrCounter([]string{"raft", "apply"}, 1)
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}

	// Create a log future, no index or term yet
	logFuture := &logFuture{
		log: Log{
			Type: LogCommand,
			Data: cmd,
		},
	}
	logFuture.init()

	select {
	case <-timer:
		return errorFuture{ErrEnqueueTimeout}
	case <-r.shutdownCh:
		return errorFuture{ErrRaftShutdown}
	case r.applyCh <- logFuture:
		return logFuture
	}
}

// Barrier is used to issue a command that blocks until all preceeding
// operations have been applied to the FSM. It can be used to ensure the
// FSM reflects all queued writes. An optional timeout can be provided to
// limit the amount of time we wait for the command to be started. This
// must be run on the leader or it will fail.
func (r *Raft) Barrier(timeout time.Duration) Future {
	metrics.IncrCounter([]string{"raft", "barrier"}, 1)
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}

	// Create a log future, no index or term yet
	logFuture := &logFuture{
		log: Log{
			Type: LogBarrier,
		},
	}
	logFuture.init()

	select {
	case <-timer:
		return errorFuture{ErrEnqueueTimeout}
	case <-r.shutdownCh:
		return errorFuture{ErrRaftShutdown}
	case r.applyCh <- logFuture:
		return logFuture
	}
}

// VerifyLeader is used to ensure the current node is still
// the leader. This can be done to prevent stale reads when a
// new leader has potentially been elected.
func (r *Raft) VerifyLeader() Future {
	metrics.IncrCounter([]string{"raft", "verify_leader"}, 1)
	verifyFuture := &verifyFuture{}
	verifyFuture.init()
	select {
	case <-r.shutdownCh:
		return errorFuture{ErrRaftShutdown}
	case r.verifyCh <- verifyFuture:
		return verifyFuture
	}
}

// GetConfiguration returns the latest configuration and its associated index
// currently in use. This may not yet be committed. This must not be called on
// the main thread (which can access the information directly).
func (r *Raft) GetConfiguration() (Configuration, uint64, error) {
	configurationsFuture := r.getConfigurations()
	if err := configurationsFuture.Error(); err != nil {
		return Configuration{}, 0, err
	}

	configurations := &configurationsFuture.configurations
	return configurations.latest, configurations.latestIndex, nil
}

// AddPeer (deprecated) is used to add a new peer into the cluster. This must be
// run on the leader or it will fail. Use AddVoter/AddNonvoter instead.
func (r *Raft) AddPeer(peer ServerAddress) Future {
	return r.AddVoter(ServerID(peer), peer, 0, 0)
}

// RemovePeer (deprecated) is used to remove a peer from the cluster. If the
// current leader is being removed, it will cause a new election
// to occur. This must be run on the leader or it will fail.
// Use RemoveServer instead.
func (r *Raft) RemovePeer(peer ServerAddress) Future {
	return r.RemoveServer(ServerID(peer), 0, 0)
}

// AddVoter will add the given server to the cluster as a staging server. If the
// server is already in the cluster as a voter, this does nothing. This must be
// run on the leader or it will fail. The leader will promote the staging server
// to a voter once that server is ready. If nonzero, prevIndex is the index of
// the only configuration upon which this change may be applied; if another
// configuration entry has been added in the meantime, this request will fail.
// If nonzero, timeout is how long this server should wait before the
// configuration change log entry is appended.
func (r *Raft) AddVoter(id ServerID, address ServerAddress, prevIndex uint64, timeout time.Duration) IndexFuture {
	return r.requestConfigChange(configurationChangeRequest{
		command:       AddStaging,
		serverID:      id,
		serverAddress: address,
		prevIndex:     prevIndex,
	}, timeout)
}

// AddNonvoter will add the given server to the cluster but won't assign it a
// vote. The server will receive log entries, but it won't participate in
// elections or log entry commitment. If the server is already in the cluster as
// a staging server or voter, this does nothing. This must be run on the leader
// or it will fail. For prevIndex and timeout, see AddVoter.
func (r *Raft) AddNonvoter(id ServerID, address ServerAddress, prevIndex uint64, timeout time.Duration) IndexFuture {
	return r.requestConfigChange(configurationChangeRequest{
		command:       AddNonvoter,
		serverID:      id,
		serverAddress: address,
		prevIndex:     prevIndex,
	}, timeout)
}

// RemoveServer will remove the given server from the cluster. If the current
// leader is being removed, it will cause a new election to occur. This must be
// run on the leader or it will fail. For prevIndex and timeout, see AddVoter.
func (r *Raft) RemoveServer(id ServerID, prevIndex uint64, timeout time.Duration) IndexFuture {
	return r.requestConfigChange(configurationChangeRequest{
		command:   RemoveServer,
		serverID:  id,
		prevIndex: prevIndex,
	}, timeout)
}

// DemoteVoter will take away a server's vote, if it has one. If present, the
// server will continue to receive log entries, but it won't participate in
// elections or log entry commitment. If the server is not in the cluster, this
// does nothing. This must be run on the leader or it will fail. For prevIndex
// and timeout, see AddVoter.
func (r *Raft) DemoteVoter(id ServerID, prevIndex uint64, timeout time.Duration) IndexFuture {
	return r.requestConfigChange(configurationChangeRequest{
		command:   DemoteVoter,
		serverID:  id,
		prevIndex: prevIndex,
	}, timeout)
}

// Shutdown is used to stop the Raft background routines.
// This is not a graceful operation. Provides a future that
// can be used to block until all background routines have exited.
func (r *Raft) Shutdown() Future {
	r.shutdownLock.Lock()
	defer r.shutdownLock.Unlock()

	if !r.shutdown {
		close(r.shutdownCh)
		r.shutdown = true
		r.setState(Shutdown)
		return &shutdownFuture{r}
	}

	// avoid closing transport twice
	return &shutdownFuture{nil}
}

// Snapshot is used to manually force Raft to take a snapshot.
// Returns a future that can be used to block until complete.
func (r *Raft) Snapshot() Future {
	snapFuture := &snapshotFuture{}
	snapFuture.init()
	select {
	case r.snapshotCh <- snapFuture:
		return snapFuture
	case <-r.shutdownCh:
		return errorFuture{ErrRaftShutdown}
	}

}

// State is used to return the current raft state.
func (r *Raft) State() RaftState {
	return r.getState()
}

// LeaderCh is used to get a channel which delivers signals on
// acquiring or losing leadership. It sends true if we become
// the leader, and false if we lose it. The channel is not buffered,
// and does not block on writes.
func (r *Raft) LeaderCh() <-chan bool {
	return r.leaderCh
}

// String returns a string representation of this Raft node.
func (r *Raft) String() string {
	return fmt.Sprintf("Node at %s [%v]", r.localAddr, r.getState())
}

// LastContact returns the time of last contact by a leader.
// This only makes sense if we are currently a follower.
func (r *Raft) LastContact() time.Time {
	r.lastContactLock.RLock()
	last := r.lastContact
	r.lastContactLock.RUnlock()
	return last
}

// Stats is used to return a map of various internal stats. This should only
// be used for informative purposes or debugging.
func (r *Raft) Stats() map[string]string {
	toString := func(v uint64) string {
		return strconv.FormatUint(v, 10)
	}
	lastLogIndex, lastLogTerm := r.getLastLog()
	lastSnapIndex, lastSnapTerm := r.getLastSnapshot()
	s := map[string]string{
		"state":               r.getState().String(),
		"term":                toString(r.getCurrentTerm()),
		"last_log_index":      toString(lastLogIndex),
		"last_log_term":       toString(lastLogTerm),
		"commit_index":        toString(r.getCommitIndex()),
		"applied_index":       toString(r.getLastApplied()),
		"fsm_pending":         toString(uint64(len(r.fsmCommitCh))),
		"last_snapshot_index": toString(lastSnapIndex),
		"last_snapshot_term":  toString(lastSnapTerm),
	}

	configuration, _, err := r.GetConfiguration()
	if err != nil {
		s["latest_configuration"] = fmt.Sprintf("%+v", configuration)
	} else {
		r.logger.Printf("[WARN] raft: could not get configuration for Stats: %v", err)
	}

	last := r.LastContact()
	if last.IsZero() {
		s["last_contact"] = "never"
	} else if r.getState() == Leader {
		s["last_contact"] = "0"
	} else {
		s["last_contact"] = fmt.Sprintf("%v", time.Now().Sub(last))
	}
	return s
}

// LastIndex returns the last index in stable storage,
// either from the last log or from the last snapshot.
func (r *Raft) LastIndex() uint64 {
	return r.getLastIndex()
}

// AppliedIndex returns the last index applied to the FSM. This is generally
// lagging behind the last index, especially for indexes that are persisted but
// have not yet been considered committed by the leader. NOTE - this reflects
// the last index that was sent to the application's FSM over the apply channel
// but DOES NOT mean that the application's FSM has yet consumed it and applied
// it to its internal state. Thus, the application's state may lag behind this
// index.
func (r *Raft) AppliedIndex() uint64 {
	return r.getLastApplied()
}
