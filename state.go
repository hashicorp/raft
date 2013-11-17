package raft

import (
	"sync/atomic"
)

type RaftState uint32

const (
	Follower RaftState = iota
	Candidate
	Leader
	Shutdown
)

// raftState is used to maintain various state variables
// and provides an interface to set/get the variables in a
// thread safe manner
type raftState struct {
	// The current state
	state RaftState

	// The current term, cache of StableStore
	currentTerm uint64

	// Cache the latest log from LogStore
	lastLog uint64

	// Highest commited log entry
	commitIndex uint64

	// Last applied log to the FSM
	lastApplied uint64

	// Term of the last applied log
	lastAppliedTerm uint64

	// Tracks the number of live routines
	runningRoutines int32
}

func (r *raftState) getState() RaftState {
	stateAddr := (*uint32)(&r.state)
	return RaftState(atomic.LoadUint32(stateAddr))
}

func (r *raftState) setState(s RaftState) {
	stateAddr := (*uint32)(&r.state)
	atomic.StoreUint32(stateAddr, uint32(s))
}

func (r *raftState) getCurrentTerm() uint64 {
	return atomic.LoadUint64(&r.currentTerm)
}

func (r *raftState) setCurrentTerm(term uint64) {
	atomic.StoreUint64(&r.currentTerm, term)
}

func (r *raftState) getLastLog() uint64 {
	return atomic.LoadUint64(&r.lastLog)
}

func (r *raftState) setLastLog(term uint64) {
	atomic.StoreUint64(&r.lastLog, term)
}

func (r *raftState) getCommitIndex() uint64 {
	return atomic.LoadUint64(&r.commitIndex)
}

func (r *raftState) setCommitIndex(term uint64) {
	atomic.StoreUint64(&r.commitIndex, term)
}

func (r *raftState) getLastApplied() uint64 {
	return atomic.LoadUint64(&r.lastApplied)
}

func (r *raftState) setLastApplied(term uint64) {
	atomic.StoreUint64(&r.lastApplied, term)
}

func (r *raftState) getLastAppliedTerm() uint64 {
	return atomic.LoadUint64(&r.lastAppliedTerm)
}

func (r *raftState) setLastAppliedTerm(term uint64) {
	atomic.StoreUint64(&r.lastAppliedTerm, term)
}

func (r *raftState) incrRoutines() {
	atomic.AddInt32(&r.runningRoutines, 1)
}

func (r *raftState) decrRoutines() {
	atomic.AddInt32(&r.runningRoutines, -1)
}

func (r *raftState) getRoutines() int32 {
	return atomic.LoadInt32(&r.runningRoutines)
}

// Start a goroutine and properly handle the race between a routine
// starting and incrementing, and exiting and decrementing.
func (r *raftState) goFunc(f func()) {
	r.incrRoutines()
	go func() {
		defer r.decrRoutines()
		f()
	}()
}
