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

// raftState is used to maintain various state variables
// and provides an interface to set/get the variables in a
// thread safe manner.
type raftState struct {
	// The current term, cache of StableStore
	currentTerm Term

	// Highest committed log entry
	commitIndex Index

	// Last applied log to the FSM
	lastApplied Index

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

	// Tracks running goroutines
	goRoutines *waitGroup
}

func (r *raftState) getState() RaftState {
	stateAddr := (*uint32)(&r.state)
	return RaftState(atomic.LoadUint32(stateAddr))
}

func (r *raftState) setState(s RaftState) {
	stateAddr := (*uint32)(&r.state)
	atomic.StoreUint32(stateAddr, uint32(s))
}

func (r *raftState) getCurrentTerm() Term {
	return Term(atomic.LoadUint64((*uint64)(&r.currentTerm)))
}

func (r *raftState) setCurrentTerm(term Term) {
	atomic.StoreUint64((*uint64)(&r.currentTerm), uint64(term))
}

func (r *raftState) getLastLog() (index Index, term Term) {
	r.lastLock.Lock()
	index = r.lastLogIndex
	term = r.lastLogTerm
	r.lastLock.Unlock()
	return
}

func (r *raftState) setLastLog(index Index, term Term) {
	r.lastLock.Lock()
	r.lastLogIndex = index
	r.lastLogTerm = term
	r.lastLock.Unlock()
}

func (r *raftState) getLastSnapshot() (index Index, term Term) {
	r.lastLock.Lock()
	index = r.lastSnapshotIndex
	term = r.lastSnapshotTerm
	r.lastLock.Unlock()
	return
}

func (r *raftState) setLastSnapshot(index Index, term Term) {
	r.lastLock.Lock()
	r.lastSnapshotIndex = index
	r.lastSnapshotTerm = term
	r.lastLock.Unlock()
}

func (r *raftState) getCommitIndex() Index {
	return Index(atomic.LoadUint64((*uint64)(&r.commitIndex)))
}

func (r *raftState) setCommitIndex(index Index) {
	atomic.StoreUint64((*uint64)(&r.commitIndex), uint64(index))
}

func (r *raftState) getLastApplied() Index {
	return Index(atomic.LoadUint64((*uint64)(&r.lastApplied)))
}

func (r *raftState) setLastApplied(index Index) {
	atomic.StoreUint64((*uint64)(&r.lastApplied), uint64(index))
}

// getLastIndex returns the last index in stable storage.
// Either from the last log or from the last snapshot.
func (r *raftState) getLastIndex() Index {
	r.lastLock.Lock()
	defer r.lastLock.Unlock()
	if r.lastLogIndex > r.lastSnapshotIndex {
		return r.lastLogIndex
	}
	return r.lastSnapshotIndex
}

// getLastEntry returns the last index and term in stable storage.
// Either from the last log or from the last snapshot.
func (r *raftState) getLastEntry() (Index, Term) {
	r.lastLock.Lock()
	defer r.lastLock.Unlock()
	if r.lastLogIndex >= r.lastSnapshotIndex {
		return r.lastLogIndex, r.lastLogTerm
	}
	return r.lastSnapshotIndex, r.lastSnapshotTerm
}
