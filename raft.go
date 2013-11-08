package raft

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

type RaftState uint8

const (
	Follower RaftState = iota
	Candidate
	Leader
)

var (
	keyCurrentTerm  = []byte("CurrentTerm")
	keyLastVoteTerm = []byte("LastVoteTerm")
	keyLastVoteCand = []byte("LastVoteCand")
	keyCandidateId  = []byte("CandidateId")
	NotLeader       = fmt.Errorf("node is not the leader")
)

// commitTupel is used to send an index that was committed,
// with an optional associated future that should be invoked
type commitTuple struct {
	index  uint64
	future *logFuture
}

type Raft struct {
	// applyCh is used to manage commands to be applyed
	applyCh chan *logFuture

	// Configuration
	conf *Config

	// Current state
	state RaftState

	// stable is a StableStore implementation for durable state
	stable StableStore

	// Cache the current term, write through to StableStore
	currentTerm uint64

	// logs is a LogStore implementation to keep our logs
	logs LogStore

	// Cache the latest log, though we can get from LogStore
	lastLog uint64

	// Commit chan is used to provide the newest commit index
	// so that changes can be applied to the FSM. This is used
	// so the main goroutine can use commitIndex without locking,
	// and the FSM manager goroutine can read from this and manipulate
	// lastApplied without a lock.
	commitCh chan commitTuple

	// Highest commited log entry
	commitIndex uint64

	// Last applied log to the FSM
	lastApplied uint64

	// FSM is the state machine that can handle the logs
	fsm FSM

	// The transport layer we use
	trans Transport

	// Track our known peers
	peers []net.Addr

	// Shutdown channel to exit
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

// NewRaft is used to construct a new Raft node
func NewRaft(conf *Config, stable StableStore, logs LogStore, fsm FSM, trans Transport) (*Raft, error) {
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

	// Create Raft struct
	r := &Raft{
		applyCh:     make(chan *logFuture),
		conf:        conf,
		state:       Follower,
		stable:      stable,
		currentTerm: currentTerm,
		logs:        logs,
		lastLog:     lastLog,
		commitCh:    make(chan commitTuple, 128),
		commitIndex: 0,
		lastApplied: 0,
		fsm:         fsm,
		trans:       trans,
		peers:       make([]net.Addr, 0, 5),
		shutdownCh:  make(chan struct{}),
	}

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
	defer r.shutdownLock.Lock()

	if r.shutdownCh != nil {
		close(r.shutdownCh)
		r.shutdownCh = nil
	}
}

// run is a long running goroutine that runs the Raft FSM
func (r *Raft) run() {
	ch := r.trans.Consumer()
	for {
		// Check if we are doing a shutdown
		select {
		case <-r.shutdownCh:
			return
		default:
		}

		// Enter into a sub-FSM
		switch r.state {
		case Follower:
			r.runFollower(ch)
		case Candidate:
			r.runCandidate(ch)
		case Leader:
			r.runLeader(ch)
		}
	}
}

// runFollower runs the FSM for a follower
func (r *Raft) runFollower(ch <-chan RPC) {
	log.Printf("[INFO] Entering Follower state")
	for {
		select {
		case rpc := <-ch:
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
			r.state = Candidate
			return

		case <-r.shutdownCh:
			return
		}
	}
}

// quorumSize returns the number of votes required to
// consider a log committed
func (r *Raft) quorumSize() int {
	clusterSize := len(r.peers) + 1
	votesNeeded := (clusterSize / 2) + 1
	return votesNeeded
}

// runCandidate runs the FSM for a candidate
func (r *Raft) runCandidate(ch <-chan RPC) {
	log.Printf("[INFO] Entering Candidate state")

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
		case rpc := <-ch:
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
			if vote.Term > r.currentTerm {
				log.Printf("[DEBUG] Newer term discovered")
				r.state = Follower
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
				r.state = Leader
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

// runLeader runs the FSM for a leader
func (r *Raft) runLeader(rpcCh <-chan RPC) {
	log.Printf("[INFO] Entering Leader state")

	// Make a channel to processes commits, defer cancelation
	// of all inflight processes when we step down
	commitCh := make(chan *logFuture)
	inflight := NewInflight(commitCh)
	defer inflight.Cancel(NotLeader)

	// Make a channel that lasts while we are leader, and
	// is closed as soon as we step down. Used to stop extra
	// goroutines.
	stopCh := make(chan struct{})
	defer close(stopCh)

	// Create the trigger channels
	triggers := make([]chan struct{}, 0, len(r.peers))
	for i := 0; i < len(r.peers); i++ {
		triggers = append(triggers, make(chan struct{}))
	}

	// Start a replication routine for each peer
	for i, peer := range r.peers {
		go r.replicate(triggers[i], stopCh, peer)
	}

	// Sit in the leader loop until we step down
	r.leaderLoop(inflight, commitCh, rpcCh, triggers)
}

// leaderLoop is the hot loop for a leader, it is invoked
// after all the various leader setup is done
func (r *Raft) leaderLoop(inflight *inflight, commitCh <-chan *logFuture,
	rpcCh <-chan RPC, triggers []chan struct{}) {
	transition := false
	for !transition {
		select {
		case applyLog := <-r.applyCh:
			// Prepare log
			applyLog.log.Index = r.lastLog + 1
			applyLog.log.Term = r.currentTerm

			// Write the log entry locally
			if err := r.logs.StoreLog(&applyLog.log); err != nil {
				log.Printf("[ERR] Failed to commit log: %v", err)
				applyLog.respond(err)
				r.state = Follower
				return
			}
			r.lastLog++

			// Add this to the inflight logs
			inflight.Start(applyLog, r.quorumSize())

			// Notify the replicators of the new log
			asyncNotify(triggers)

		case commitLog := <-commitCh:
			// Increment the commit index
			r.commitIndex = commitLog.log.Index

			// Trigger applying logs locally
			r.commitCh <- commitTuple{commitLog.log.Index, commitLog}

		case rpc := <-rpcCh:
			switch cmd := rpc.Command.(type) {
			case *AppendEntriesRequest:
				transition = r.appendEntries(rpc, cmd)
			case *RequestVoteRequest:
				transition = r.requestVote(rpc, cmd)
			default:
				log.Printf("[ERR] Leaderstate, got unexpected command: %#v",
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
			// Get the log, either from the future or from our log store
			var l *Log
			if commitTuple.future != nil {
				l = &commitTuple.future.log
			} else {
				l = new(Log)
				if err := r.logs.GetLog(commitTuple.index, l); err != nil {
					log.Printf("[ERR] Failed to get log: %v", err)
					panic(err)
				}
			}

			// Only apply commands, ignore other logs
			if l.Type == LogCommand {
				r.fsm.Apply(l.Data)
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
		Term:    r.currentTerm,
		Success: false,
	}
	var rpcErr error
	defer rpc.Respond(resp, rpcErr)

	// Ignore an older term
	if a.Term < r.currentTerm {
		return
	}

	// Increase the term if we see a newer one, also transition to follower
	// if we ever get an appendEntries call
	if a.Term > r.currentTerm || r.state != Follower {
		if err := r.setCurrentTerm(a.Term); err != nil {
			log.Printf("[ERR] Failed to update current term: %v", err)
			return
		}
		resp.Term = a.Term

		// Ensure transition to follower
		transition = true
		r.state = Follower
	}

	// Verify the last log entry
	var prevLog Log
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

	// Add all the entries
	for _, entry := range a.Entries {
		// Delete any conflicting entries
		if entry.Index <= r.lastLog {
			log.Printf("[WARN] Clearing log suffix from %d to %d", entry.Index, r.lastLog)
			if err := r.logs.DeleteRange(entry.Index, r.lastLog); err != nil {
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
		r.lastLog = entry.Index
	}

	// Update the commit index
	if a.LeaderCommitIndex > r.commitIndex {
		r.commitIndex = min(a.LeaderCommitIndex, r.lastLog)

		// Trigger applying logs locally
		r.commitCh <- commitTuple{r.commitIndex, nil}
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
		Term:    r.currentTerm,
		Granted: false,
	}
	var rpcErr error
	defer rpc.Respond(resp, rpcErr)

	// Ignore an older term
	if req.Term < r.currentTerm {
		return
	}

	// Increase the term if we see a newer one
	if req.Term > r.currentTerm {
		if err := r.setCurrentTerm(req.Term); err != nil {
			log.Printf("[ERR] Failed to update current term: %v", err)
			return
		}
		resp.Term = req.Term

		// Ensure transition to follower
		transition = true
		r.state = Follower
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
	if r.lastLog > 0 {
		var lastLog Log
		if err := r.logs.GetLog(r.lastLog, &lastLog); err != nil {
			log.Printf("[ERR] Failed to get last log: %d %v",
				r.lastLog, err)
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
	if r.lastLog > 0 {
		if err := r.logs.GetLog(r.lastLog, &lastLog); err != nil {
			log.Printf("[ERR] Failed to get last log: %d %v",
				r.lastLog, err)
			return nil
		}
	}

	// Increment the term
	if err := r.setCurrentTerm(r.currentTerm + 1); err != nil {
		log.Printf("[ERR] Failed to update current term: %v", err)
		return nil
	}

	// Construct the request
	req := &RequestVoteRequest{
		Term:         r.currentTerm,
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
	if err != nil && err.Error() != "not found" {
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
	r.currentTerm = t
	return nil
}

// replicate is a long running routine that is used to manage
// the process of replicating logs to our followers
func (r *Raft) replicate(triggerCh, stopCh chan struct{}, peer net.Addr) {
	// Initialize timer to fire immediately since
	// we just established leadership.
	timeout := time.After(time.Microsecond)
	for {
		select {

		case <-timeout:
			timeout = randomTimeout(r.conf.CommitTimeout)
			r.heartbeat(peer)

		case <-stopCh:
			return
		}
	}
}

func (r *Raft) heartbeat(peer net.Addr) {
	// TODO: Cache prevLogEntry, prevLogTerm!
	var prevLogEntry, prevLogTerm uint64
	prevLogEntry = 0
	prevLogTerm = 0
	req := AppendEntriesRequest{
		Term:              r.currentTerm,
		LeaderId:          r.candidateId(),
		PrevLogEntry:      prevLogEntry,
		PrevLogTerm:       prevLogTerm,
		LeaderCommitIndex: r.commitIndex,
	}
	var resp AppendEntriesResponse
	if err := r.trans.AppendEntries(peer, &req, &resp); err != nil {
		log.Printf("[ERR] Failed to heartbeat with %v: %v",
			peer, err)
	}
}
