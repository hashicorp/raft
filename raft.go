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
	keyCandidateId  = []byte("CandidateId")
	NotLeader       = fmt.Errorf("node is not the leader")
	LeadershipLost  = fmt.Errorf("leadership lost while committing log")
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
	peers []net.Addr

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
func NewRaft(conf *Config, fsm FSM, logs LogStore, stable StableStore, peers []net.Addr, trans Transport) (*Raft, error) {
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
	otherPeers := make([]net.Addr, 0, len(peers))
	for _, p := range peers {
		if p.String() != localAddr.String() {
			otherPeers = append(otherPeers, p)
		}
	}

	// Create Raft struct
	r := &Raft{
		applyCh:    make(chan *logFuture),
		commitCh:   make(chan commitTuple, 128),
		conf:       conf,
		fsm:        fsm,
		localAddr:  localAddr,
		logs:       logs,
		peers:      otherPeers,
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
	go r.runFSM()
	return r, nil
}

// Apply is used to apply a command to the FSM in a highly consistent
// manner. This returns a future that can be used to wait on the application.
// An optional timeout can be provided to limit the amount of time we wait
// for the command to be started.
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
		return errorFuture{fmt.Errorf("timed out enqueuing operation")}
	case r.applyCh <- logFuture:
		return logFuture
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
	}
}

// State is used to return the state raft is currently in
func (r *Raft) State() RaftState {
	return r.getState()
}

func (r *Raft) String() string {
	return fmt.Sprintf("Node %s at %s", r.candidateId(), r.localAddr.String())
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

// runLeader runs the FSM for a leader. Do the setup here and drop into
// the leaderLoop for the hot loop
func (r *Raft) runLeader() {
	log.Printf("[INFO] %v entering Leader state", r)

	// Make a channel to processes commits, defer cancelation
	// of all inflight processes when we step down
	commitCh := make(chan *logFuture, 128)
	inflight := NewInflight(commitCh)
	defer inflight.Cancel(LeadershipLost)

	// Make a channel that lasts while we are leader, and
	// is closed as soon as we step down. Used to stop extra
	// goroutines.
	stopCh := make(chan struct{})
	defer close(stopCh)

	// Create the trigger channels
	triggers := make([]chan struct{}, 0, len(r.peers))
	for i := 0; i < len(r.peers); i++ {
		triggers = append(triggers, make(chan struct{}, 1))
	}

	// Start a replication routine for each peer
	for i, peer := range r.peers {
		go r.replicate(inflight, triggers[i], stopCh, peer)
	}

	// Append no-op command to seal leadership
	go r.applyNoop()

	// Sit in the leader loop until we step down
	r.leaderLoop(inflight, commitCh, triggers)
}

// leaderLoop is the hot loop for a leader, it is invoked
// after all the various leader setup is done
func (r *Raft) leaderLoop(inflight *inflight, commitCh <-chan *logFuture,
	triggers []chan struct{}) {
	transition := false
	for !transition {
		select {
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
			inflight.Start(applyLog, r.quorumSize())
			inflight.Commit(applyLog.log.Index)

			// Update the last log since it's on disk now
			r.setLastLog(applyLog.log.Index)

			// Notify the replicators of the new log
			asyncNotify(triggers)

		case commitLog := <-commitCh:
			// Increment the commit index
			idx := commitLog.log.Index
			r.setCommitIndex(idx)

			// Trigger applying logs locally
			r.commitCh <- commitTuple{idx, commitLog}

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
		case <-r.shutdownCh:
			return
		}
	}
}

// runFSM is a long running goroutine responsible for the management
// of the local FSM.
func (r *Raft) runFSM() {
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

				// Only apply commands, ignore other logs
				if l.Type == LogCommand {
					r.fsm.Apply(l.Data)
				}

				// Update the lastApplied
				r.setLastApplied(l.Index)
			}

			// Invoke the future if given
			if commitTuple.future != nil {
				commitTuple.future.respond(nil)
			}

		case <-r.shutdownCh:
			return
		}
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
	if a.LeaderCommitIndex > r.getCommitIndex() {
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
	// Setup a response
	resp := &RequestVoteResponse{
		Term:    r.getCurrentTerm(),
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
		if string(lastVoteCandBytes) == req.CandidateId {
			log.Printf("[WARN] Duplicate RequestVote from candidate: %s", req.CandidateId)
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
	if err := r.persistVote(req.Term, req.CandidateId); err != nil {
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
		CandidateId:  r.candidateId(),
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
	if err := r.persistVote(req.Term, req.CandidateId); err != nil {
		log.Printf("[ERR] Failed to persist vote : %v", err)
		return nil
	}

	// Include our own vote
	respCh <- &RequestVoteResponse{Term: req.Term, Granted: true}
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

// CandidateId is used to return a stable and unique candidate ID
func (r *Raft) candidateId() string {
	// Get the persistent id
	raw, err := r.stable.Get(keyCandidateId)
	if err == nil {
		return string(raw)
	}

	// Generate a UUID on the first call
	if err != nil && err.Error() == "not found" {
		id := generateUUID()
		if err := r.stable.Set(keyCandidateId, []byte(id)); err != nil {
			panic(fmt.Errorf("Failed to write CandidateId: %v", err))
		}
		return id
	}
	panic(fmt.Errorf("Failed to read CandidateId: %v", err))
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
	r.applyCh <- logFuture
}

type followerReplication struct {
	matchIndex uint64
	nextIndex  uint64
}

// replicate is a long running routine that is used to manage
// the process of replicating logs to our followers
func (r *Raft) replicate(inflight *inflight, triggerCh, stopCh chan struct{}, peer net.Addr) {
	// Initialize the indexes
	last := r.getLastLog()
	indexes := followerReplication{
		matchIndex: last,
		nextIndex:  last + 1,
	}

	// Replicate when a new log arrives or if we timeout
	shouldStop := false
	for !shouldStop {
		select {
		case <-triggerCh:
			shouldStop = r.replicateTo(inflight, &indexes, r.getLastLog(), peer)
		case <-randomTimeout(r.conf.CommitTimeout):
			shouldStop = r.replicateTo(inflight, &indexes, r.getLastLog(), peer)
		case <-stopCh:
			return
		}
	}
}

// replicateTo is used to replicate the logs up to a given last index.
// If the follower log is behind, we take care to bring them up to date
func (r *Raft) replicateTo(inflight *inflight, indexes *followerReplication, lastIndex uint64, peer net.Addr) (shouldStop bool) {
	// Create the base request
	var l Log
	var req AppendEntriesRequest
	var resp AppendEntriesResponse
START:
	req = AppendEntriesRequest{
		Term:              r.getCurrentTerm(),
		LeaderId:          r.candidateId(),
		LeaderCommitIndex: r.getCommitIndex(),
	}

	// Get the previous log entry based on the nextIndex.
	// Guard for the first index, since there is no 0 log entry
	if indexes.nextIndex > 1 {
		if err := r.logs.GetLog(indexes.nextIndex-1, &l); err != nil {
			log.Printf("[ERR] Failed to get log at index %d: %v",
				indexes.nextIndex-1, err)
			return
		}

		// Set the previous index and term (0 if nextIndex is 1)
		req.PrevLogEntry = l.Index
		req.PrevLogTerm = l.Term
	} else {
		req.PrevLogEntry = 0
		req.PrevLogTerm = 0
	}

	// Append up to MaxAppendEntries or up to the lastIndex
	req.Entries = make([]*Log, 0, 16)
	maxIndex := min(indexes.nextIndex+uint64(r.conf.MaxAppendEntries)-1, lastIndex)
	for i := indexes.nextIndex; i <= maxIndex; i++ {
		oldLog := new(Log)
		if err := r.logs.GetLog(i, oldLog); err != nil {
			log.Printf("[ERR] Failed to get log at index %d: %v", i, err)
			return
		}
		req.Entries = append(req.Entries, oldLog)
	}

	// Make the RPC call
	if err := r.trans.AppendEntries(peer, &req, &resp); err != nil {
		log.Printf("[ERR] Failed to AppendEntries to %v: %v", peer, err)
		return
	}

	// Check for a newer term, stop running
	if resp.Term > req.Term {
		return true
	}

	// Update the indexes based on success
	if resp.Success {
		// Mark any inflight logs as committed
		for i := indexes.matchIndex; i <= maxIndex; i++ {
			inflight.Commit(i)
		}

		indexes.matchIndex = maxIndex
		indexes.nextIndex = maxIndex + 1
	} else {
		log.Printf("[WARN] AppendEntries to %v rejected, sending older logs", peer)
		indexes.nextIndex = max(min(indexes.nextIndex-1, resp.LastLog+1), 1)
		indexes.matchIndex = indexes.nextIndex - 1
	}

	// Check if there are more logs to replicate
	if indexes.nextIndex <= lastIndex {
		goto START
	}
	return
}
