package raft

type AppendEntriesRequest struct {
	// Provide the current term and leader
	Term   uint64
	Leader []byte

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
	Candidate []byte

	// Used to ensure safety
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteResponse struct {
	// Newer term if leader is out of date
	Term uint64

	// Return the peers, so that a node can shutdown on removal
	Peers []byte

	// Is the vote granted
	Granted bool
}

type InstallSnapshotRequest struct {
	Term   uint64
	Leader []byte

	// These are the last index/term included in the snapshot
	LastLogIndex uint64
	LastLogTerm  uint64

	// Peer Set in the snapshot
	Peers []byte

	// Size of the snapshot
	Size int64
}

type InstallSnapshotResponse struct {
	Term    uint64
	Success bool
}
