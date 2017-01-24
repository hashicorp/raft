package raft

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
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

	// ErrNothingNewToSnapshot is returned when trying to create a snapshot
	// but there's nothing new commited to the FSM since we started.
	ErrNothingNewToSnapshot = errors.New("nothing new to snapshot")

	// ErrUnsupportedProtocol is returned when an operation is attempted
	// that's not supported by the current protocol version.
	ErrUnsupportedProtocol = errors.New("operation not supported with current protocol version")

	// ErrCantBootstrap is returned when attempt is made to bootstrap a
	// cluster that already has state present.
	ErrCantBootstrap = errors.New("bootstrap only works on new clusters")
)

type Raft struct {
	// channels are used to plumb futures into the Raft server in order to
	// perform actions via the public interfaces.
	channels *apiChannels

	// goRoutines tracks running goroutines.
	goRoutines *waitGroup

	// protocolVersion is used to transition between different versions of the
	// library. See comments in config.go for more details.
	protocolVersion ProtocolVersion

	// serverInternals is used from unit tests to get at internals.
	serverInternals *raftServer
}

type apiChannels struct {
	// applyCh is used to async send logs to the main thread to
	// be committed and applied to the FSM.
	applyCh chan *logFuture

	// Used to request the leader to make membership configuration changes.
	membershipChangeCh chan *membershipChangeFuture

	// snapshotCh is used for user triggered snapshots
	snapshotCh chan *snapshotFuture

	// verifyCh is used to async send verify futures to the main thread
	// to verify we are still the leader
	verifyCh chan *verifyFuture

	// membershipsCh is used to get the membership configuration
	// data safely from outside of the main thread.
	membershipsCh chan *membershipsFuture

	// statsCh is used to get stats safely from outside of the main thread.
	statsCh chan *statsFuture

	// bootstrapCh is used to attempt an initial bootstrap from outside of
	// the main thread.
	bootstrapCh chan *bootstrapFuture

	// leaderCh is used to notify the application of leadership changes. See
	// Raft.LeaderCh().
	leaderCh chan bool

	// Shutdown channel to exit. Use ensureClosed() in case of repeated calls.
	shutdownCh chan struct{}
}

// NewRaft is used to construct a new Raft node. It takes a configuration, as well
// as implementations of various interfaces that are required. If we have any
// old state, such as snapshots, logs, peers, etc, all those will be restored
// when creating the Raft node.
func NewRaft(conf *Config, fsm FSM, logs LogStore, stable StableStore, snaps SnapshotStore, trans Transport) (*Raft, error) {
	api := &Raft{
		channels: &apiChannels{
			applyCh:            make(chan *logFuture),
			membershipChangeCh: make(chan *membershipChangeFuture),
			snapshotCh:         make(chan *snapshotFuture),
			verifyCh:           make(chan *verifyFuture, 64),
			membershipsCh:      make(chan *membershipsFuture, 8),
			statsCh:            make(chan *statsFuture, 8),
			bootstrapCh:        make(chan *bootstrapFuture),
			leaderCh:           make(chan bool),
			shutdownCh:         make(chan struct{}),
		},
		goRoutines:      &waitGroup{},
		protocolVersion: conf.ProtocolVersion,
	}
	server, err := newRaftServer(conf, fsm, logs, stable, snaps, trans, api.channels, api.goRoutines)
	if err != nil {
		return nil, err
	}
	api.serverInternals = server
	return api, nil
}

// BootstrapCluster is equivalent to non-member BootstrapCluster but can be
// called on an un-bootstrapped Raft instance after it has been created. This
// should only be called at the beginning of time for the cluster, and you
// absolutely must make sure that you call it with the same configuration on all
// the Voter servers. There is no need to bootstrap Nonvoter and Staging
// servers.
func (r *Raft) BootstrapCluster(membership Membership) Future {
	bootstrapReq := &bootstrapFuture{}
	bootstrapReq.init()
	bootstrapReq.membership = membership
	select {
	case <-r.channels.shutdownCh:
		return errorFuture{ErrRaftShutdown}
	case r.channels.bootstrapCh <- bootstrapReq:
		return bootstrapReq
	}
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
	case <-r.channels.shutdownCh:
		return errorFuture{ErrRaftShutdown}
	case r.channels.applyCh <- logFuture:
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
	case <-r.channels.shutdownCh:
		return errorFuture{ErrRaftShutdown}
	case r.channels.applyCh <- logFuture:
		return logFuture
	}
}

// VerifyLeader is used to ensure the current node is still
// the leader. This can be done to prevent stale reads when a
// new leader has potentially been elected.
func (r *Raft) VerifyLeader() Future {
	metrics.IncrCounter([]string{"raft", "verify_leader"}, 1)
	verifyFuture := &verifyFuture{}
	verifyFuture.shutdownCh = r.channels.shutdownCh
	verifyFuture.init()
	select {
	case <-r.channels.shutdownCh:
		return errorFuture{ErrRaftShutdown}
	case r.channels.verifyCh <- verifyFuture:
		return verifyFuture
	}
}

// GetMembership returns the latest membership configuration and its associated index
// currently in use. This may not yet be committed. This must not be called on
// the main thread (which can access the information directly).
func (r *Raft) GetMembership() MembershipFuture {
	configReq := &membershipsFuture{}
	configReq.shutdownCh = r.channels.shutdownCh
	configReq.init()
	select {
	case <-r.channels.shutdownCh:
		configReq.respond(ErrRaftShutdown)
		return configReq
	case r.channels.membershipsCh <- configReq:
		return configReq
	}
}

// requestMembershipChange is a helper for the functions that make
// membership change requests. 'req' describes the change. For timeout,
// see AddVoter.
func (r *Raft) requestMembershipChange(req membershipChangeRequest, timeout time.Duration) IndexFuture {
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}
	future := &membershipChangeFuture{
		req: req,
	}
	future.init()
	select {
	case <-timer:
		return errorFuture{ErrEnqueueTimeout}
	case r.channels.membershipChangeCh <- future:
		return future
	case <-r.channels.shutdownCh:
		return errorFuture{ErrRaftShutdown}
	}
}

// AddPeer (deprecated) is used to add a new peer into the cluster. This must be
// run on the leader or it will fail. Use AddVoter/AddNonvoter instead.
func (r *Raft) AddPeer(peer ServerAddress) Future {
	if r.protocolVersion > 2 {
		return errorFuture{ErrUnsupportedProtocol}
	}

	return r.requestMembershipChange(membershipChangeRequest{
		command:       AddStaging,
		serverID:      ServerID(peer),
		serverAddress: peer,
		prevIndex:     0,
	}, 0)
}

// RemovePeer (deprecated) is used to remove a peer from the cluster. If the
// current leader is being removed, it will cause a new election
// to occur. This must be run on the leader or it will fail.
// Use RemoveServer instead.
func (r *Raft) RemovePeer(peer ServerAddress) Future {
	if r.protocolVersion > 2 {
		return errorFuture{ErrUnsupportedProtocol}
	}

	return r.requestMembershipChange(membershipChangeRequest{
		command:   RemoveServer,
		serverID:  ServerID(peer),
		prevIndex: 0,
	}, 0)
}

// AddVoter will add the given server to the cluster as a staging server. If the
// server is already in the cluster as a voter, this does nothing. This must be
// run on the leader or it will fail. The leader will promote the staging server
// to a voter once that server is ready. If nonzero, prevIndex is the index of
// the only configuration upon which this change may be applied; if another
// configuration entry has been added in the meantime, this request will fail.
// If nonzero, timeout is how long this server should wait before the
// configuration change log entry is appended.
func (r *Raft) AddVoter(id ServerID, address ServerAddress, prevIndex Index, timeout time.Duration) IndexFuture {
	if r.protocolVersion < 2 {
		return errorFuture{ErrUnsupportedProtocol}
	}

	return r.requestMembershipChange(membershipChangeRequest{
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
func (r *Raft) AddNonvoter(id ServerID, address ServerAddress, prevIndex Index, timeout time.Duration) IndexFuture {
	if r.protocolVersion < 3 {
		return errorFuture{ErrUnsupportedProtocol}
	}

	return r.requestMembershipChange(membershipChangeRequest{
		command:       AddNonvoter,
		serverID:      id,
		serverAddress: address,
		prevIndex:     prevIndex,
	}, timeout)
}

// RemoveServer will remove the given server from the cluster. If the current
// leader is being removed, it will cause a new election to occur. This must be
// run on the leader or it will fail. For prevIndex and timeout, see AddVoter.
func (r *Raft) RemoveServer(id ServerID, prevIndex Index, timeout time.Duration) IndexFuture {
	if r.protocolVersion < 2 {
		return errorFuture{ErrUnsupportedProtocol}
	}

	return r.requestMembershipChange(membershipChangeRequest{
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
func (r *Raft) DemoteVoter(id ServerID, prevIndex Index, timeout time.Duration) IndexFuture {
	if r.protocolVersion < 3 {
		return errorFuture{ErrUnsupportedProtocol}
	}

	return r.requestMembershipChange(membershipChangeRequest{
		command:   DemoteVoter,
		serverID:  id,
		prevIndex: prevIndex,
	}, timeout)
}

// Shutdown is used to stop the Raft background routines.
// This is not a graceful operation. Provides a future that
// can be used to block until all background routines have exited.
func (r *Raft) Shutdown() Future {
	ensureClosed(r.channels.shutdownCh)
	return &shutdownFuture{r}
}

// Snapshot is used to manually force Raft to take a snapshot.
// Returns a future that can be used to block until complete.
func (r *Raft) Snapshot() Future {
	snapFuture := &snapshotFuture{}
	snapFuture.init()
	select {
	case r.channels.snapshotCh <- snapFuture:
		return snapFuture
	case <-r.channels.shutdownCh:
		return errorFuture{ErrRaftShutdown}
	}

}

// LeaderCh is used to get a channel which delivers signals on
// acquiring or losing leadership. It sends true if we become
// the leader, and false if we lose it. The channel is not buffered,
// and does not block on writes.
func (r *Raft) LeaderCh() <-chan bool {
	return r.channels.leaderCh
}

// String returns a string representation of this Raft node.
func (r *Raft) String() string {
	future := r.Stats()
	err := future.Error()
	switch err {
	case ErrRaftShutdown:
		return "Node has been shutdown"
	case nil:
		stats := future.Stats()
		return fmt.Sprintf("Node at %s [%v]",
			stats.ServerAddress,
			stats.State)
	default:
		return fmt.Sprintf("Node unknown [err: %v]", err)
	}
}

type Stats struct {
	ServerID      ServerID
	ServerAddress ServerAddress
	State         RaftState
	Term          Term
	LastLogIndex  Index
	LastLogTerm   Term
	CommitIndex   Index
	// AppliedIndex is the last index applied to the FSM. This is generally
	// lagging behind the last index, especially for indexes that are persisted but
	// have not yet been considered committed by the leader. NOTE - this reflects
	// the last index that was sent to the application's FSM over the apply channel
	// but DOES NOT mean that the application's FSM has yet consumed it and applied
	// it to its internal state. Thus, the application's state may lag behind this
	// index.
	AppliedIndex          Index
	FSMPending            int
	LastSnapshotIndex     Index
	LastSnapshotTerm      Term
	LatestMembership      Membership
	LatestMembershipIndex Index
	LastContact           time.Time
	// The current leader of the cluster. Empty if there is no current leader or
	// the leader is unknown.
	LastLeader ServerAddress
	// numPeers is the number of other voting servers in the cluster, not
	// including this node. If this node isn't part of the configuration then this
	// will be 0.
	NumPeers           int
	ProtocolVersion    ProtocolVersion
	ProtocolVersionMin ProtocolVersion
	ProtocolVersionMax ProtocolVersion
	SnapshotVersionMin SnapshotVersion
	SnapshotVersionMax SnapshotVersion
}

// Stringify a Stats struct into key-value strings.
//
// The value of "last_contact" is either "never" if there
// has been no contact with a leader, "0" if the node is in the
// leader state, or the time since last contact with a leader
// formatted as a string.
func (s *Stats) Strings() []struct{ K, V string } {
	toString := func(v uint64) string {
		return strconv.FormatUint(v, 10)
	}
	var lastContact string
	if s.LastContact.IsZero() {
		lastContact = "never"
	} else if s.State == Leader {
		lastContact = "0"
	} else {
		lastContact = fmt.Sprintf("%v", time.Now().Sub(s.LastContact))
	}
	return []struct{ K, V string }{
		{"server_id", string(s.ServerID)},
		{"server_address", string(s.ServerAddress)},
		{"state", s.State.String()},
		{"term", toString(uint64(s.Term))},
		{"last_log_index", toString(uint64(s.LastLogIndex))},
		{"last_log_term", toString(uint64(s.LastLogTerm))},
		{"commit_index", toString(uint64(s.CommitIndex))},
		{"applied_index", toString(uint64(s.AppliedIndex))},
		{"fsm_pending", toString(uint64(s.FSMPending))},
		{"last_snapshot_index", toString(uint64(s.LastSnapshotIndex))},
		{"last_snapshot_term", toString(uint64(s.LastSnapshotTerm))},
		{"latest_membership", fmt.Sprintf("%v", s.LatestMembership)},
		{"latest_membership_index", toString(uint64(s.LatestMembershipIndex))},
		{"last_contact", lastContact},
		{"last_leader", string(s.LastLeader)},
		{"num_peers", toString(uint64(s.NumPeers))},
		{"protocol_version", toString(uint64(s.ProtocolVersion))},
		{"protocol_version_min", toString(uint64(s.ProtocolVersionMin))},
		{"protocol_version_max", toString(uint64(s.ProtocolVersionMax))},
		{"snapshot_version_min", toString(uint64(s.SnapshotVersionMin))},
		{"snapshot_version_max", toString(uint64(s.SnapshotVersionMax))},
	}
}

func (s *Stats) String() string {
	kvs := s.Strings()
	lines := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		lines = append(lines, fmt.Sprintf("%v: %v", kv.K, kv.V))
	}
	return strings.Join(lines, "\n")
}

// Stats returns various internal stats.
func (r *Raft) Stats() StatsFuture {
	statsReq := &statsFuture{}
	statsReq.shutdownCh = r.channels.shutdownCh
	statsReq.init()
	select {
	case <-r.channels.shutdownCh:
		statsReq.respond(ErrRaftShutdown)
	case r.channels.statsCh <- statsReq:
	}
	return statsReq
}
