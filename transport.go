package raft

import (
	"net"
)

type AppendEntriesRequest struct {
	// Provide the current term and leader
	Term     uint64
	LeaderId string

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

	// We may not succeed if we have a conflicting entry
	Success bool
}

type RequestVoteRequest struct {
	// Provide the term and our id
	Term        uint64
	CandidateId string

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

// RPCResponse captures both a response and a potential error
type RPCResponse struct {
	Response interface{}
	Error    error
}

// RPC has a command, and provides a Reponse mechanism
type RPC struct {
	// Type assert to determien the type
	Command  interface{}
	RespChan chan<- RPCResponse
}

// Respond is used to respond with a response, error or both
func (r *RPC) Respond(resp interface{}, err error) {
	r.RespChan <- RPCResponse{resp, err}
}

// Transport provides an interface for network transports
// to allow Raft to communicate with other nodes
type Transport interface {
	// Consumer returns a channel that can be used to
	// consume and respond to RPC requests.
	Consumer() <-chan RPC

	// AppendEntries sends the appropriate RPC to the target node
	AppendEntries(target net.Addr, args *AppendEntriesRequest, resp *AppendEntriesResponse) error

	// RequestVote sends the appropriate RPC to the target node
	RequestVote(target net.Addr, args *RequestVoteRequest, resp *RequestVoteResponse) error
}
