package raft

// RPCHeader is a common sub-structure used to pass along protocol version and
// other information about the cluster. For older Raft implementations before
// versioning was added this will default to a zero-valued structure when read
// by newer Raft versions.
type RPCHeader struct {
	// ProtocolVersion is the version of the protocol the sender is
	// speaking.
	ProtocolVersion ProtocolVersion
}

// WithRPCHeader is an interface that exposes the RPC header.
type WithRPCHeader interface {
	GetRPCHeader() RPCHeader
}

// AppendEntriesRequest is the command used to append entries to the
// replicated log.
//
// These are also used as heartbeats. Older versions of the code always sent
// heartbeats with PrevLogEntry, PrevLogTerm, and LeaderCommitIndex set to 0.
type AppendEntriesRequest struct {
	RPCHeader

	// The current term of the leader.
	Term uint64

	// The leader's network address.
	Leader []byte

	// The index and term of the log entry preceding 'Entries', which is used as a
	// consistency check to guarantee Raft's Log Matching Property. On older
	// versions of the code, these had to match on the follower for it to consider
	// the leader alive (setLastContact). This is no longer required.
	PrevLogEntry uint64
	PrevLogTerm  uint64

	// New entries to append to the log (empty for heartbeats).
	Entries []*Log

	// Leaders running the current code set LeaderCommitIndex to the smaller of
	// the last entry marked committed in the leader's log and PrevLogEntry +
	// len(Entries). This cap ties in the commit index with the consistency check,
	// so that a follower can only update its commit index when it and the leader
	// agree on the entries in question.
	//
	// Older leaders would set the LeaderCommitIndex to either 0 or the index of
	// the last log entry marked committed on the leader. It could have been past
	// the last entry sent in the request and/or past the follower's last log
	// index.
	//
	// Older versions of the follower code behaved like this:
	//     if (PrevLogEntry and PrevLogTerm match) {
	//        (truncate conflicting entries and append new entries, if any)
	//        if LeaderCommitIndex > f.commitIndex {
	//            f.commitIndex = min(LeaderCommitIndex, f.lastIndex)
	//        }
	//     }
	// This is dangerous. For example, the request
	//    {PrevLogEntry=0, PrevLogTerm=0, Entries=[], LeaderCommitIndex=100}
	// would mark the first 100 entries on that follower committed, even though
	// they may differ from the leader's first 100 entries.
	//
	// These conditions would be required to cause a diverging state machine:
	//  1. PrevLogEntry and PrevLogTerm would have to match the follower's.
	//  2. Until #113, entries would have to be empty. After #113, entries could
	//     also be nonempty but not conflict with the follower's log (causing no
	//     log suffix truncation).
	//  3. LeaderCommitIndex would have to be nonzero.
	//  4. The follower would need some entries at indexes <= LeaderCommitIndex
	//     that differ from the leader's (the truly committed entries).
	//
	// We don't think the old code would trigger this. Heartbeats always had
	// LeaderCommitIndex set to 0 (avoiding condition 3). Most other AppendEntries
	// in the old code contained new or conflicting entries (avoiding condition
	// 2). Others, like those triggered by the old timer called CommitTimeout,
	// sent no entries but had PrevLogEntry set to the end of the leader's log
	// (avoiding condition 4). Duplication of requests at the transport layer or a
	// nextIndex that was somehow set too low could cause trouble, but those are
	// not expected to occur.
	//
	// Newer versions of the leader code always cap LeaderCommitIndex to at most
	// PrevLogEntry + len(Entries). This avoids condition 4 above for followers
	// running older code.
	//
	// Newer versions of the follower code also apply this cap to provide
	// additional safety when interoperating with leaders running older code.
	// Thus, newer versions of the follower code behave like this:
	//     if LeaderCommitIndex > PrevLogEntry + len(Entries) {
	//         LeaderCommitIndex = PrevLogEntry + len(Entries)
	//     }
	//     if (PrevLogEntry and PrevLogTerm match) {
	//        (truncate conflicting entries and append new entries, if any)
	//        if LeaderCommitIndex > f.commitIndex {
	//            f.commitIndex = LeaderCommitIndex
	//        }
	//     }
	LeaderCommitIndex uint64
}

// See WithRPCHeader.
func (r *AppendEntriesRequest) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

// AppendEntriesResponse is the response returned from an
// AppendEntriesRequest.
type AppendEntriesResponse struct {
	RPCHeader

	// Newer term if leader is out of date
	Term uint64

	// Last Log is a hint to help accelerate rebuilding slow nodes
	LastLog uint64

	// We may not succeed if we have a conflicting entry
	Success bool
}

// See WithRPCHeader.
func (r *AppendEntriesResponse) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

// RequestVoteRequest is the command used by a candidate to ask a Raft peer
// for a vote in an election.
type RequestVoteRequest struct {
	RPCHeader

	// Provide the term and our id
	Term      uint64
	Candidate []byte

	// Used to ensure safety
	LastLogIndex uint64
	LastLogTerm  uint64
}

// See WithRPCHeader.
func (r *RequestVoteRequest) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

// RequestVoteResponse is the response returned from a RequestVoteRequest.
type RequestVoteResponse struct {
	RPCHeader

	// Newer term if leader is out of date.
	Term uint64

	// Peers is deprecated, but required by servers that only understand
	// protocol version 0. This is not populated in protocol version 2
	// and later.
	Peers []byte

	// Is the vote granted.
	Granted bool
}

// See WithRPCHeader.
func (r *RequestVoteResponse) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

// InstallSnapshotRequest is the command sent to a Raft peer to bootstrap its
// log (and state machine) from a snapshot on another peer.
type InstallSnapshotRequest struct {
	RPCHeader
	SnapshotVersion SnapshotVersion

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

// See WithRPCHeader.
func (r *InstallSnapshotRequest) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

// InstallSnapshotResponse is the response returned from an
// InstallSnapshotRequest.
type InstallSnapshotResponse struct {
	RPCHeader

	Term    uint64
	Success bool
}

// See WithRPCHeader.
func (r *InstallSnapshotResponse) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}
