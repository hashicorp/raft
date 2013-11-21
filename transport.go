package raft

import (
	"io"
	"net"
)

// RPCResponse captures both a response and a potential error
type RPCResponse struct {
	Response interface{}
	Error    error
}

// RPC has a command, and provides a Reponse mechanism
type RPC struct {
	Command  interface{}
	Reader   io.Reader // Set only for InstallSnapshot
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

	// InstallSnapshot is used to push a snapshot down to a follower. The data is read from
	// the ReadCloser and streamed to the client.
	InstallSnapshot(target net.Addr, args *InstallSnapshotRequest, resp *InstallSnapshotResponse, data io.Reader) error

	// EncodePeer is used to serialize a peer name
	EncodePeer(net.Addr) []byte

	// DecodePeer is used to deserialize a peer name
	DecodePeer([]byte) net.Addr
}
