package raft

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

var (
	keyCurrentTerm  = []byte("CurrentTerm")
	keyLastVoteTerm = []byte("LastVoteTerm")
	keyLastVoteCand = []byte("LastVoteCand")
	NotLeader       = fmt.Errorf("node is not the leader")
	LeadershipLost  = fmt.Errorf("leadership lost while committing log")
	RaftShutdown    = fmt.Errorf("raft is already shutdown")
	EnqueueTimeout  = fmt.Errorf("timed out enqueuing operation")
)

// commitTupel is used to send an index that was committed,
// with an optional associated future that should be invoked
type commitTuple struct {
	index  uint64
	future *logFuture
}

type Raft struct {
	raftState

	// applyCh is used to async send logs to the main thread to
	// be committed and applied to the FSM.
	applyCh chan *logFuture

	// Commit chan is used to provide the newest commit index
	// and potentially a future to the FSM manager routine.
	// The FSM should apply up to the index and notify the future.
	commitCh chan commitTuple

	// Configuration provided at Raft initialization
	conf *Config

	// FSM is the client state machine to apply commands to
	fsm FSM

	// Stores our local addr
	localAddr net.Addr

	// LogStore provides durable storage for logs
	logs LogStore

	// Track our known peers
	peers     []net.Addr
	peerLock  sync.Mutex
	peerStore PeerStore

	// RPC chan comes from the transport layer
	rpcCh <-chan RPC

	// Shutdown channel to exit, protected to prevent concurrent exits
	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex

	// stable is a StableStore implementation for durable state
	// It provides stable storage for many fields in raftState
	stable StableStore

	// The transport layer we use
	trans Transport
}

// NewRaft is used to construct a new Raft node
func NewRaft(conf *Config, fsm FSM, logs LogStore, stable StableStore, peerStore PeerStore, trans Transport) (*Raft, error) {
	// Try to restore the current term
	currentTerm, err := stable.GetUint64(keyCurrentTerm)
	if err != nil && err.Error() != "not found" {
		return nil, fmt.Errorf("Failed to load current term: %v", err)
	}

	// Read the last log value
	lastLog, err := logs.LastIndex()
	if err != nil {
		return nil, fmt.Errorf("Failed to find last log: %v", err)
	}

	// Construct the list of peers that excludes us
	localAddr := trans.LocalAddr()
	peers, err := peerStore.Peers()
	if err != nil {
		return nil, fmt.Errorf("Failed to get list of peers: %v", err)
	}
	peers = excludePeer(peers, localAddr)

	// Create Raft struct
	r := &Raft{
		applyCh:    make(chan *logFuture),
		commitCh:   make(chan commitTuple, 128),
		conf:       conf,
		fsm:        fsm,
		localAddr:  localAddr,
		logs:       logs,
		peers:      peers,
		peerStore:  peerStore,
		rpcCh:      trans.Consumer(),
		shutdownCh: make(chan struct{}),
		stable:     stable,
		trans:      trans,
	}

	// Initialize as a follower
	r.setState(Follower)

	// Restore the current term and the last log
	r.setCurrentTerm(currentTerm)
	r.setLastLog(lastLog)

	// Start the background work
	go r.run()
	go r.runPostCommit()
	return r, nil
}

// Apply is used to apply a command to the FSM in a highly consistent
// manner. This returns a future that can be used to wait on the application.
// An optional timeout can be provided to limit the amount of time we wait
// for the command to be started. This must be run on the leader or it
// will fail.
func (r *Raft) Apply(cmd []byte, timeout time.Duration) ApplyFuture {
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
		errCh: make(chan error, 1),
	}

	select {
	case <-timer:
		return errorFuture{EnqueueTimeout}
	case <-r.shutdownCh:
		return errorFuture{RaftShutdown}
	case r.applyCh <- logFuture:
		return logFuture
	}
}

// AddPeer is used to add a new peer into the cluster. This must be
// run on the leader or it will fail.
func (r *Raft) AddPeer(peer net.Addr) ApplyFuture {
	logFuture := &logFuture{
		log: Log{
			Type: LogAddPeer,
			Data: r.trans.EncodePeer(peer),
		},
		errCh: make(chan error, 1),
	}
	select {
	case r.applyCh <- logFuture:
		return logFuture
	case <-r.shutdownCh:
		return errorFuture{RaftShutdown}
	}
}

// RemovePeer is used to remove a peer from the cluster. If the
// current leader is being removed, it will cause a new election
// to occur. This must be run on the leader or it will fail.
func (r *Raft) RemovePeer(peer net.Addr) ApplyFuture {
	logFuture := &logFuture{
		log: Log{
			Type: LogRemovePeer,
			Data: r.trans.EncodePeer(peer),
		},
		errCh: make(chan error, 1),
	}
	select {
	case r.applyCh <- logFuture:
		return logFuture
	case <-r.shutdownCh:
		return errorFuture{RaftShutdown}
	}
}

// Shutdown is used to stop the Raft background routines.
// This is not a graceful operation.
func (r *Raft) Shutdown() {
	r.shutdownLock.Lock()
	defer r.shutdownLock.Unlock()

	if !r.shutdown {
		close(r.shutdownCh)
		r.shutdown = true
		r.setState(Shutdown)
	}
}

// State is used to return the state raft is currently in
func (r *Raft) State() RaftState {
	return r.getState()
}

func (r *Raft) String() string {
	return fmt.Sprintf("Node at %s", r.localAddr.String())
}

// run is a long running goroutine that runs the Raft FSM
func (r *Raft) run() {
	for {
		// Check if we are doing a shutdown
		select {
		case <-r.shutdownCh:
			return
		default:
		}

		// Enter into a sub-FSM
		switch r.getState() {
		case Follower:
			r.runFollower()
		case Candidate:
			r.runCandidate()
		case Leader:
			r.runLeader()
		}
	}
}

// runFollower runs the FSM for a follower
func (r *Raft) runFollower() {
	log.Printf("[INFO] %v entering Follower state", r)
	for {
		select {
		case rpc := <-r.rpcCh:
			switch cmd := rpc.Command.(type) {
			case *AppendEntriesRequest:
				r.appendEntries(rpc, cmd)
			case *RequestVoteRequest:
				r.requestVote(rpc, cmd)
			default:
				log.Printf("[ERR] Follower state, got unexpected command: %#v",
					rpc.Command)
				rpc.Respond(nil, fmt.Errorf("Unexpected command"))
			}

		case a := <-r.applyCh:
			// Reject any operations since we are not the leader
			a.respond(NotLeader)

		case <-randomTimeout(r.conf.HeartbeatTimeout):
			// Heartbeat failed! Transition to the candidate state
			log.Printf("[WARN] Heartbeat timeout reached, starting election")
			r.setState(Candidate)
			return

		case <-r.shutdownCh:
			return
		}
	}
}

// runCandidate runs the FSM for a candidate
func (r *Raft) runCandidate() {
	log.Printf("[INFO] %v entering Candidate state", r)

	// Start vote for us, and set a timeout
	voteCh := r.electSelf()
	electionTimer := randomTimeout(r.conf.ElectionTimeout)

	// Tally the votes, need a simple majority
	grantedVotes := 0
	votesNeeded := r.quorumSize()
	log.Printf("[DEBUG] Votes needed: %d", votesNeeded)

	transition := false
	for !transition {
		select {
		case rpc := <-r.rpcCh:
			switch cmd := rpc.Command.(type) {
			case *AppendEntriesRequest:
				transition = r.appendEntries(rpc, cmd)
			case *RequestVoteRequest:
				transition = r.requestVote(rpc, cmd)
			default:
				log.Printf("[ERR] Candidate state, got unexpected command: %#v",
					rpc.Command)
				rpc.Respond(nil, fmt.Errorf("Unexpected command"))
			}

		case vote := <-voteCh:
			// Check if the term is greater than ours, bail
			if vote.Term > r.getCurrentTerm() {
				log.Printf("[DEBUG] Newer term discovered")
				r.setState(Follower)
				if err := r.setCurrentTerm(vote.Term); err != nil {
					log.Printf("[ERR] Failed to update current term: %v", err)
				}
				return
			}

			// If we are not a peer, we could have been removed but failed
			// to receive the log message. OR it could mean an improperly configured
			// cluster. Either way, bail now.
			if !peerContained(vote.Peers, r.localAddr) {
				log.Printf("[WARN] Remote peer does not have local node listed as a peer")
			}

			// Check if the vote is granted
			if vote.Granted {
				grantedVotes++
				log.Printf("[DEBUG] Vote granted. Tally: %d", grantedVotes)
			}

			// Check if we've become the leader
			if grantedVotes >= votesNeeded {
				log.Printf("[INFO] Election won. Tally: %d", grantedVotes)
				r.setState(Leader)
				return
			}

		case a := <-r.applyCh:
			// Reject any operations since we are not the leader
			a.respond(NotLeader)

		case <-electionTimer:
			// Election failed! Restart the elction. We simply return,
			// which will kick us back into runCandidate
			log.Printf("[WARN] Election timeout reached, restarting election")
			return

		case <-r.shutdownCh:
			return
		}
	}
}

type leaderState struct {
	commitCh  chan *logFuture
	inflight  *inflight
	replState map[string]*followerReplication
}

func (l *leaderState) Release() {
	// Stop replication
	for _, p := range l.replState {
		close(p.stopCh)
	}

	// Cancel inflight requests
	l.inflight.Cancel(LeadershipLost)
}

// runLeader runs the FSM for a leader. Do the setup here and drop into
// the leaderLoop for the hot loop
func (r *Raft) runLeader() {
	log.Printf("[INFO] %v entering Leader state", r)
	state := leaderState{
		commitCh:  make(chan *logFuture, 128),
		replState: make(map[string]*followerReplication),
	}
	defer state.Release()

	// Initialize inflight tracker
	state.inflight = NewInflight(state.commitCh)

	// Start a replication routine for each peer
	r.peerLock.Lock()
	for _, peer := range r.peers {
		r.startReplication(&state, peer)
	}
	r.peerLock.Unlock()

	// Append no-op command to seal leadership
	go r.applyNoop()

	// Sit in the leader loop until we step down
	r.leaderLoop(&state)
}

// startReplication is a helper to setup state and start async replication to a peer
func (r *Raft) startReplication(state *leaderState, peer net.Addr) {
	s := &followerReplication{
		peer:       peer,
		inflight:   state.inflight,
		stopCh:     make(chan struct{}),
		triggerCh:  make(chan struct{}, 1),
		matchIndex: r.getLastLog(),
		nextIndex:  r.getLastLog() + 1,
	}
	state.replState[peer.String()] = s
	go r.replicate(s)
}

// leaderLoop is the hot loop for a leader, it is invoked
// after all the various leader setup is done
func (r *Raft) leaderLoop(s *leaderState) {
	transition := false
	for !transition {
		select {
		case rpc := <-r.rpcCh:
			switch cmd := rpc.Command.(type) {
			case *AppendEntriesRequest:
				transition = r.appendEntries(rpc, cmd)
			case *RequestVoteRequest:
				transition = r.requestVote(rpc, cmd)
			default:
				log.Printf("[ERR] Leader state, got unexpected command: %#v",
					rpc.Command)
				rpc.Respond(nil, fmt.Errorf("Unexpected command"))
			}

		case commitLog := <-s.commitCh:
			// Increment the commit index
			idx := commitLog.log.Index
			r.setCommitIndex(idx)

			// Perform leader-specific processing
			transition = r.leaderProcessLog(s, &commitLog.log)

			// Trigger applying logs locally
			r.commitCh <- commitTuple{idx, commitLog}

		case applyLog := <-r.applyCh:
			// Prepare log
			applyLog.log.Index = r.getLastLog() + 1
			applyLog.log.Term = r.getCurrentTerm()

			// Write the log entry locally
			if err := r.logs.StoreLog(&applyLog.log); err != nil {
				log.Printf("[ERR] Failed to commit log: %v", err)
				applyLog.respond(err)
				r.setState(Follower)
				return
			}

			// Add this to the inflight logs, commit
			s.inflight.Start(applyLog, r.quorumSize())
			s.inflight.Commit(applyLog.log.Index)

			// Update the last log since it's on disk now
			r.setLastLog(applyLog.log.Index)

			// Notify the replicators of the new log
			for _, f := range s.replState {
				asyncNotifyCh(f.triggerCh)
			}

		case <-r.shutdownCh:
			return
		}
	}
}

// leaderProcessLog is used for leader-specific log handling before we
// hand off to the generic runPostCommit handler. Returns true if there
// should be a state transition.
func (r *Raft) leaderProcessLog(s *leaderState, l *Log) bool {
	// Only handle LogAddPeer and LogRemove Peer
	if l.Type != LogAddPeer && l.Type != LogRemovePeer {
		return false
	}

	// Process the log immediately to update the peer list
	r.processLog(l)

	// Decode the peer
	peer := r.trans.DecodePeer(l.Data)
	isSelf := peer.String() == r.localAddr.String()

	// Get the replication state
	repl, ok := s.replState[peer.String()]

	// Start replicattion for new nodes
	if l.Type == LogAddPeer && !ok && !isSelf {
		log.Printf("[INFO] Added peer %v, starting replication", peer)
		r.startReplication(s, peer)
	}

	// Stop replication for old nodes
	if l.Type == LogRemovePeer && ok {
		log.Printf("[INFO] Removed peer %v, stopping replication", peer)
		close(repl.stopCh)
		delete(s.replState, peer.String())
	}

	// Step down if we are being removed
	if l.Type == LogRemovePeer && isSelf {
		log.Printf("[INFO] Removed ourself, stepping down as leader")
		return true
	}

	return false
}

// runPostCommit is a long running goroutine responsible for the handling
// of a log entry after it has been committed. This involves managing the
// local FSM, configuration changes, etc.
func (r *Raft) runPostCommit() {
	for {
		select {
		case commitTuple := <-r.commitCh:
			// Reject logs we've applied already
			if commitTuple.index <= r.getLastApplied() {
				log.Printf("[WARN] Skipping application of old log: %d",
					commitTuple.index)
				continue
			}

			// Apply all the preceeding logs
			for idx := r.getLastApplied() + 1; idx <= commitTuple.index; idx++ {
				// Get the log, either from the future or from our log store
				var l *Log
				if commitTuple.future != nil && commitTuple.future.log.Index == idx {
					l = &commitTuple.future.log
				} else {
					l = new(Log)
					if err := r.logs.GetLog(idx, l); err != nil {
						log.Printf("[ERR] Failed to get log at %d: %v", idx, err)
						panic(err)
					}
				}
				r.processLog(l)
			}

			// Update the lastApplied
			r.setLastApplied(commitTuple.index)

			// Invoke the future if given
			if commitTuple.future != nil {
				commitTuple.future.respond(nil)
			}

		case <-r.shutdownCh:
			return
		}
	}
}

// processLog is invoked to process the application of a single committed log
func (r *Raft) processLog(l *Log) {
	switch l.Type {
	case LogCommand:
		r.fsm.Apply(l.Data)

	case LogAddPeer:
		peer := r.trans.DecodePeer(l.Data)

		// Avoid adding ourself as a peer
		r.peerLock.Lock()
		if peer.String() != r.localAddr.String() {
			r.peers = addUniquePeer(r.peers, peer)
		}

		// Update the PeerStore
		r.peerStore.SetPeers(append([]net.Addr{r.localAddr}, r.peers...))
		r.peerLock.Unlock()

	case LogRemovePeer:
		peer := r.trans.DecodePeer(l.Data)

		// Removing ourself acts like removing all other peers
		r.peerLock.Lock()
		if peer.String() == r.localAddr.String() {
			r.peers = nil
			if r.conf.ShutdownOnRemove {
				log.Printf("[INFO] Removed ourself, shutting down")
				r.Shutdown()
			} else {
				r.setState(Follower)
			}
		} else {
			r.peers = excludePeer(r.peers, peer)
		}

		// Update the PeerStore
		r.peerStore.SetPeers(append([]net.Addr{r.localAddr}, r.peers...))
		r.peerLock.Unlock()

	case LogNoop:
		// Ignore the no-op
	default:
		log.Printf("[ERR] Got unrecognized log type: %#v", l)
	}
}

// appendEntries is invoked when we get an append entries RPC call
// Returns true if we transition to a Follower
func (r *Raft) appendEntries(rpc RPC, a *AppendEntriesRequest) (transition bool) {
	// Setup a response
	resp := &AppendEntriesResponse{
		Term:    r.getCurrentTerm(),
		LastLog: r.getLastLog(),
		Success: false,
	}
	var rpcErr error
	defer rpc.Respond(resp, rpcErr)

	// Ignore an older term
	if a.Term < r.getCurrentTerm() {
		return
	}

	// Increase the term if we see a newer one, also transition to follower
	// if we ever get an appendEntries call
	if a.Term > r.getCurrentTerm() || r.getState() != Follower {
		if err := r.setCurrentTerm(a.Term); err != nil {
			log.Printf("[ERR] Failed to update current term: %v", err)
			return
		}
		resp.Term = a.Term

		// Ensure transition to follower
		transition = true
		r.setState(Follower)
	}

	// Verify the last log entry
	var prevLog Log
	if a.PrevLogEntry > 0 {
		if err := r.logs.GetLog(a.PrevLogEntry, &prevLog); err != nil {
			log.Printf("[WARN] Failed to get previous log: %d %v",
				a.PrevLogEntry, err)
			return
		}
		if a.PrevLogTerm != prevLog.Term {
			log.Printf("[WARN] Previous log term mis-match: ours: %d remote: %d",
				prevLog.Term, a.PrevLogTerm)
			return
		}
	}

	// Add all the entries
	for _, entry := range a.Entries {
		// Delete any conflicting entries
		if entry.Index <= r.getLastLog() {
			log.Printf("[WARN] Clearing log suffix from %d to %d", entry.Index, r.getLastLog())
			if err := r.logs.DeleteRange(entry.Index, r.getLastLog()); err != nil {
				log.Printf("[ERR] Failed to clear log suffix: %v", err)
				return
			}
		}

		// Append the entry
		if err := r.logs.StoreLog(entry); err != nil {
			log.Printf("[ERR] Failed to append to log: %v", err)
			return
		}

		// Update the lastLog
		r.setLastLog(entry.Index)
	}

	// Update the commit index
	if a.LeaderCommitIndex > 0 && a.LeaderCommitIndex > r.getCommitIndex() {
		idx := min(a.LeaderCommitIndex, r.getLastLog())
		r.setCommitIndex(idx)

		// Trigger applying logs locally
		r.commitCh <- commitTuple{idx, nil}
	}

	// Everything went well, set success
	resp.Success = true
	return
}

// requestVote is invoked when we get an request vote RPC call
// Returns true if we transition to a Follower
func (r *Raft) requestVote(rpc RPC, req *RequestVoteRequest) (transition bool) {
	// Ensure a protected view of the peers
	r.peerLock.Lock()
	defer r.peerLock.Unlock()

	// Setup a response
	resp := &RequestVoteResponse{
		Term:    r.getCurrentTerm(),
		Peers:   r.peers,
		Granted: false,
	}
	var rpcErr error
	defer rpc.Respond(resp, rpcErr)

	// Ignore an older term
	if req.Term < r.getCurrentTerm() {
		return
	}

	// Increase the term if we see a newer one
	if req.Term > r.getCurrentTerm() {
		if err := r.setCurrentTerm(req.Term); err != nil {
			log.Printf("[ERR] Failed to update current term: %v", err)
			return
		}
		resp.Term = req.Term

		// Ensure transition to follower
		transition = true
		r.setState(Follower)
	}

	// Check if we have voted yet
	lastVoteTerm, err := r.stable.GetUint64(keyLastVoteTerm)
	if err != nil && err.Error() != "not found" {
		log.Printf("[ERR] Failed to get last vote term: %v", err)
		return
	}
	lastVoteCandBytes, err := r.stable.Get(keyLastVoteCand)
	if err != nil && err.Error() != "not found" {
		log.Printf("[ERR] Failed to get last vote candidate: %v", err)
		return
	}

	// Check if we've voted in this election before
	if lastVoteTerm == req.Term && lastVoteCandBytes != nil {
		log.Printf("[INFO] Duplicate RequestVote for same term: %d", req.Term)
		if string(lastVoteCandBytes) == req.Candidate.String() {
			log.Printf("[WARN] Duplicate RequestVote from candidate: %s", req.Candidate)
			resp.Granted = true
		}
		return
	}

	// Reject if their term is older
	if r.getLastLog() > 0 {
		var lastLog Log
		if err := r.logs.GetLog(r.getLastLog(), &lastLog); err != nil {
			log.Printf("[ERR] Failed to get last log: %d %v",
				r.getLastLog(), err)
			return
		}
		if lastLog.Term > req.LastLogTerm {
			log.Printf("[WARN] Rejecting vote since our last term is greater")
			return
		}

		if lastLog.Index > req.LastLogIndex {
			log.Printf("[WARN] Rejecting vote since our last index is greater")
			return
		}
	}

	// Persist a vote for safety
	if err := r.persistVote(req.Term, req.Candidate.String()); err != nil {
		log.Printf("[ERR] Failed to persist vote: %v", err)
		return
	}

	resp.Granted = true
	return
}

// electSelf is used to send a RequestVote RPC to all peers,
// and vote for ourself. This has the side affecting of incrementing
// the current term. The response channel returned is used to wait
// for all the responses (including a vote for ourself).
func (r *Raft) electSelf() <-chan *RequestVoteResponse {
	// Ensure a protected view of the peers
	r.peerLock.Lock()
	defer r.peerLock.Unlock()

	// Create a response channel
	respCh := make(chan *RequestVoteResponse, len(r.peers)+1)

	// Get the last log
	var lastLog Log
	if r.getLastLog() > 0 {
		if err := r.logs.GetLog(r.getLastLog(), &lastLog); err != nil {
			log.Printf("[ERR] Failed to get last log: %d %v",
				r.getLastLog(), err)
			return nil
		}
	}

	// Increment the term
	if err := r.setCurrentTerm(r.getCurrentTerm() + 1); err != nil {
		log.Printf("[ERR] Failed to update current term: %v", err)
		return nil
	}

	// Construct the request
	req := &RequestVoteRequest{
		Term:         r.getCurrentTerm(),
		Candidate:    r.localAddr,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}

	// Construct a function to ask for a vote
	askPeer := func(peer net.Addr) {
		resp := new(RequestVoteResponse)
		err := r.trans.RequestVote(peer, req, resp)
		if err != nil {
			log.Printf("[ERR] Failed to make RequestVote RPC to %v: %v", peer, err)
			resp.Term = req.Term
			resp.Granted = false
		}
		respCh <- resp
	}

	// For each peer, request a vote
	for _, peer := range r.peers {
		go askPeer(peer)
	}

	// Persist a vote for ourselves
	if err := r.persistVote(req.Term, req.Candidate.String()); err != nil {
		log.Printf("[ERR] Failed to persist vote : %v", err)
		return nil
	}

	// Include our own vote
	respCh <- &RequestVoteResponse{
		Term:    req.Term,
		Peers:   []net.Addr{r.localAddr},
		Granted: true,
	}
	return respCh
}

// quorumSize returns the number of votes required to
// consider a log committed
func (r *Raft) quorumSize() int {
	clusterSize := len(r.peers) + 1
	votesNeeded := (clusterSize / 2) + 1
	return votesNeeded
}

// persistVote is used to persist our vote for safety
func (r *Raft) persistVote(term uint64, candidate string) error {
	if err := r.stable.SetUint64(keyLastVoteTerm, term); err != nil {
		return err
	}
	if err := r.stable.Set(keyLastVoteCand, []byte(candidate)); err != nil {
		return err
	}
	return nil
}

// setCurrentTerm is used to set the current term in a durable manner
func (r *Raft) setCurrentTerm(t uint64) error {
	// Persist to disk first
	if err := r.stable.SetUint64(keyCurrentTerm, t); err != nil {
		log.Printf("[ERR] Failed to save current term: %v", err)
		return err
	}
	r.raftState.setCurrentTerm(t)
	return nil
}

// applyNoop is a blocking command that appends a no-op log
// entry. It is used to seal leadership.
func (r *Raft) applyNoop() {
	logFuture := &logFuture{
		log: Log{
			Type: LogNoop,
		},
	}
	select {
	case r.applyCh <- logFuture:
	case <-r.shutdownCh:
	}
}
