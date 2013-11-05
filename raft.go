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

type Raft struct {
	// Configuration
	conf *Config

	// Current state
	state RaftState

	// stable is a StableStore implementation for durable state
	stable StableStore

	// logs is a LogStore implementation to keep our logs
	logs LogStore

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
	r := &Raft{
		conf:        conf,
		state:       Follower,
		stable:      stable,
		logs:        logs,
		commitIndex: 0,
		lastApplied: 0,
		fsm:         fsm,
		trans:       trans,
		shutdownCh:  make(chan struct{}),
	}

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
			// Handle the command
			switch cmd := rpc.Command.(type) {
			case *AppendEntriesRequest:
				r.followerAppendEntries(rpc, cmd)
			case *RequestVoteRequest:
				r.followerRequestVote(rpc, cmd)
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
	for {
		select {
		case rpc := <-ch:
			// Handle the command
			switch rpc.Command.(type) {
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
