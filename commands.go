package raft

// VersionInfo is a common sub-structure used to pass along
// protocol version information. For older Raft implementations before
// versioning was added this will default to protocol version 0.
type VersionInfo struct {
	// ProtocolVersion is the version of the protocol the sender is
	// speaking.
	ProtocolVersion int
}

// WithVersionInfo is an interface that exposes version info.
type WithVersionInfo interface {
	GetVersionInfo() VersionInfo
}

// AppendEntriesRequest is the command used to append entries to the
// replicated log.
type AppendEntriesRequest struct {
	VersionInfo

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

// See WithVersionInfo.
func (r *AppendEntriesRequest) GetVersionInfo() VersionInfo {
	return r.VersionInfo
}

// AppendEntriesResponse is the response returned from an
// AppendEntriesRequest.
type AppendEntriesResponse struct {
	VersionInfo

	// Newer term if leader is out of date
	Term uint64

	// Last Log is a hint to help accelerate rebuilding slow nodes
	LastLog uint64

	// We may not succeed if we have a conflicting entry
	Success bool

	// There are scenarios where this request didn't succeed
	// but there's no need to wait/back-off the next attempt.
	NoRetryBackoff bool
}

// See WithVersionInfo.
func (r *AppendEntriesResponse) GetVersionInfo() VersionInfo {
	return r.VersionInfo
}

// RequestVoteRequest is the command used by a candidate to ask a Raft peer
// for a vote in an election.
type RequestVoteRequest struct {
	VersionInfo

	// Provide the term and our id
	Term      uint64
	Candidate []byte

	// Used to ensure safety
	LastLogIndex uint64
	LastLogTerm  uint64
}

// See WithVersionInfo.
func (r *RequestVoteRequest) GetVersionInfo() VersionInfo {
	return r.VersionInfo
}

// RequestVoteResponse is the response returned from a RequestVoteRequest.
type RequestVoteResponse struct {
	VersionInfo

	// Newer term if leader is out of date
	Term uint64

	// Is the vote granted
	Granted bool
}

// See WithVersionInfo.
func (r *RequestVoteResponse) GetVersionInfo() VersionInfo {
	return r.VersionInfo
}

// InstallSnapshotRequest is the command sent to a Raft peer to bootstrap its
// log (and state machine) from a snapshot on another peer.
type InstallSnapshotRequest struct {
	VersionInfo

	Term   uint64
	Leader []byte

	// These are the last index/term included in the snapshot
	LastLogIndex uint64
	LastLogTerm  uint64

	// Peer Set in the snapshot. This is deprecated in favor of Configuration
	// but remains here in case we receive an InstallSnapshot from a leader
	// that's running old code.
	Peers []byte

	// Cluster membership.
	Configuration []byte
	// Log index where 'Configuration' entry was originally written.
	ConfigurationIndex uint64

	// Size of the snapshot
	Size int64
}

// See WithVersionInfo.
func (r *InstallSnapshotRequest) GetVersionInfo() VersionInfo {
	return r.VersionInfo
}

// InstallSnapshotResponse is the response returned from an
// InstallSnapshotRequest.
type InstallSnapshotResponse struct {
	VersionInfo

	Term    uint64
	Success bool
}

// See WithVersionInfo.
func (r *InstallSnapshotResponse) GetVersionInfo() VersionInfo {
	return r.VersionInfo
}
