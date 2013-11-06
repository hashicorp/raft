package raft

import (
	"fmt"
	"log"
	"sync"
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
)

type Raft struct {
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

	// Highest commited log entry
	commitIndex uint64

	// Last applied log to the FSM
	lastApplied uint64

	// FSM is the state machine that can handle the logs
	fsm FSM

	// The transport layer we use
	trans Transport

	// If we are the leader, we have extra state
	leader *LeaderState

	// Shutdown channel to exit
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

// NewRaft is used to construct a new Raft node
func NewRaft(conf *Config, stable StableStore, logs LogStore, fsm FSM, trans Transport) (*Raft, error) {
	// Read the last log value
	lastLog, err := logs.LastIndex()
	if err != nil {
		return nil, fmt.Errorf("Failed to find last log: %v", err)
	}

	// Create Raft struct
	r := &Raft{
		conf:        conf,
		state:       Follower,
		stable:      stable,
		logs:        logs,
		lastLog:     lastLog,
		commitIndex: 0,
		lastApplied: 0,
		fsm:         fsm,
		trans:       trans,
		shutdownCh:  make(chan struct{}),
	}

	// Start the background work
	go r.run()
	return r, nil
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
		switch r.state {
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
	ch := r.trans.Consumer()
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

		case <-randomTimeout(r.conf.HeartbeatTimeout):
			// Heartbeat failed! Go to the candidate state
			r.state = Candidate
			return

		case <-r.shutdownCh:
			return
		}
	}
}

// runCandidate runs the FSM for a candidate
func (r *Raft) runCandidate() {
	ch := r.trans.Consumer()
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

		case <-randomTimeout(r.conf.ElectionTimeout):
			// Election failed! Restart the elction. We simply return,
			// which will kick us back into runCandidate
			return

		case <-r.shutdownCh:
			return
		}
	}
}

// runLeader runs the FSM for a leader
func (r *Raft) runLeader() {
	for {
		select {
		case <-r.shutdownCh:
			return
		}
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

// appendEntries is invoked when we get an append entries RPC call
// Returns true if we transition to a Follower
func (r *Raft) appendEntries(rpc RPC, a *AppendEntriesRequest) (transition bool) {
	// Setup a response
	resp := &AppendEntriesResponse{
		Term:    r.currentTerm,
		Success: false,
	}
	var err error
	defer rpc.Respond(resp, err)

	// Ignore an older term
	if a.Term < r.currentTerm {
		return
	}

	// Increase the term if we see a newer one, also transition to follower
	// if we ever get an appendEntries call
	if a.Term > r.currentTerm || r.state != Follower {
		r.currentTerm = a.Term
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

		// TODO: Trigger applying logs locally!
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
	var err error
	defer rpc.Respond(resp, err)

	// Ignore an older term
	if req.Term < r.currentTerm {
		return
	}

	// Increase the term if we see a newer one
	if req.Term > r.currentTerm {
		r.currentTerm = req.Term
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

	// Seems we should grant a vote
	if err := r.stable.SetUint64(keyLastVoteTerm, req.Term); err != nil {
		log.Printf("[ERR] Failed to persist last vote term: %v", err)
		return
	}
	if err := r.stable.Set(keyLastVoteCand, []byte(req.CandidateId)); err != nil {
		log.Printf("[ERR] Failed to persist last vote candidate: %v", err)
		return
	}

	resp.Granted = true
	return
}
