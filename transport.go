package raft

import (
	"net"
)

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

	// LocalAddr is used to return our local address to distinguish from our peers
	LocalAddr() net.Addr

	// AppendEntries sends the appropriate RPC to the target node
	AppendEntries(target net.Addr, args *AppendEntriesRequest, resp *AppendEntriesResponse) error

	// RequestVote sends the appropriate RPC to the target node
	RequestVote(target net.Addr, args *RequestVoteRequest, resp *RequestVoteResponse) error

	// EncodePeer is used to serialize a peer name
	EncodePeer(net.Addr) []byte

	// DecodePeer is used to deserialize a peer name
	DecodePeer([]byte) net.Addr
}
