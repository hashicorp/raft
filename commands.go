package raft

import (
	"net"
)

type AppendEntriesRequest struct {
	// Provide the current term and leader
	Term   uint64
	Leader net.Addr

	// Provide the previous entries for integrity checking
	PrevLogEntry uint64
	PrevLogTerm  uint64

	// New entries to commit
	Entries []*Log

	// Commit index on the leader
	LeaderCommitIndex uint64
}

type AppendEntriesResponse struct {
	// Newer term if leader is out of date
	Term uint64

	// Last Log is a hint to help accelerate rebuilding slow nodes
	LastLog uint64

	// We may not succeed if we have a conflicting entry
	Success bool
}

type RequestVoteRequest struct {
	// Provide the term and our id
	Term      uint64
	Candidate net.Addr

	// Used to ensure safety
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteResponse struct {
	// Newer term if leader is out of date
	Term uint64

	// Is the vote granted
	Granted bool
}
