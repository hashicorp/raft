package raft

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

	// If we are the leader, we have extra state
	leader *LeaderState
}
