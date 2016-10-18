package raft

import (
	"sync"
	"sync/atomic"
)

// RaftState captures the state of a Raft node: Follower, Candidate, Leader,
// or Shutdown.
type RaftState uint32

const (
	// Follower is the initial state of a Raft node.
	Follower RaftState = iota

	// Candidate is one of the valid states of a Raft node.
	Candidate

	// Leader is one of the valid states of a Raft node.
	Leader

	// Shutdown is the terminal state of a Raft node.
	Shutdown
)

func (s RaftState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Shutdown:
		return "Shutdown"
	default:
		return "Unknown"
	}
}

// raftShared is used to maintain various state variables
// and provides an interface to set/get the variables in a
// thread safe manner.
type raftShared struct {
	// protects 4 next fields
	lastLock sync.Mutex

	// Cache the latest snapshot index/term
	lastSnapshotIndex Index
	lastSnapshotTerm  Term

	// Cache the latest log from LogStore
	lastLogIndex Index
	lastLogTerm  Term

	// The current state
	state RaftState
}

func (r *raftShared) getState() RaftState {
	stateAddr := (*uint32)(&r.state)
	return RaftState(atomic.LoadUint32(stateAddr))
}

func (r *raftShared) setState(s RaftState) {
	stateAddr := (*uint32)(&r.state)
	atomic.StoreUint32(stateAddr, uint32(s))
}

func (r *raftShared) getLastLog() (index Index, term Term) {
	r.lastLock.Lock()
	index = r.lastLogIndex
	term = r.lastLogTerm
	r.lastLock.Unlock()
	return
}

func (r *raftShared) setLastLog(index Index, term Term) {
	r.lastLock.Lock()
	r.lastLogIndex = index
	r.lastLogTerm = term
	r.lastLock.Unlock()
}

func (r *raftShared) getLastSnapshot() (index Index, term Term) {
	r.lastLock.Lock()
	index = r.lastSnapshotIndex
	term = r.lastSnapshotTerm
	r.lastLock.Unlock()
	return
}

func (r *raftShared) setLastSnapshot(index Index, term Term) {
	r.lastLock.Lock()
	r.lastSnapshotIndex = index
	r.lastSnapshotTerm = term
	r.lastLock.Unlock()
}

// getLastIndex returns the last index in stable storage.
// Either from the last log or from the last snapshot.
func (r *raftShared) getLastIndex() Index {
	r.lastLock.Lock()
	defer r.lastLock.Unlock()
	if r.lastLogIndex > r.lastSnapshotIndex {
		return r.lastLogIndex
	}
	return r.lastSnapshotIndex
}

// getLastEntry returns the last index and term in stable storage.
// Either from the last log or from the last snapshot.
func (r *raftShared) getLastEntry() (Index, Term) {
	r.lastLock.Lock()
	defer r.lastLock.Unlock()
	if r.lastLogIndex >= r.lastSnapshotIndex {
		return r.lastLogIndex, r.lastLogTerm
	}
	return r.lastSnapshotIndex, r.lastSnapshotTerm
}
