package raft

import (
	"fmt"
	"log"
	"net"
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

	// Track our known peers
	peers []net.Addr

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
		peers:       make([]net.Addr, 0, 5),
		shutdownCh:  make(chan struct{}),
	}

	// Start the background work
	go r.run()
	return r, nil
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

		case <-randomTimeout(r.conf.HeartbeatTimeout):
			// Heartbeat failed! Go to the candidate state
			log.Printf("[WARN] Heartbeat timeout reached, starting election")
			r.state = Candidate
			return

		case <-r.shutdownCh:
			return
		}
	}
}

// runCandidate runs the FSM for a candidate
func (r *Raft) runCandidate(ch <-chan RPC) {
	log.Printf("[INFO] Entering Candidate state")

	// Start vote for us, and set a timeout
	voteCh := r.electSelf()
	electionTimer := randomTimeout(r.conf.ElectionTimeout)

	// Tally the votes, need a simple majority
	grantedVotes := 0
	clusterSize := len(r.peers) + 1
	votesNeeded := (clusterSize >> 1) + 1
	log.Printf("[DEBUG] Cluster size: %d, votes needed: %d", clusterSize, votesNeeded)

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
func (r *Raft) runLeader(ch <-chan RPC) {
	log.Printf("[INFO] Entering Leader state")
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
		CandidateId:  r.CandidateId(),
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
func (r *Raft) CandidateId() string {
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
	r.currentTerm = t
	// TODO stable store
	return nil
}
