package raft

import (
	"errors"
	"io"
	"sync"
	"time"
)

// RPCResponse captures both a response and a potential error.
type RPCResponse struct {
	Response interface{}
	Error    error
}

// RPC has a command, and provides a response mechanism.
type RPC struct {
	Command  interface{}
	Reader   io.Reader // Set only for InstallSnapshot
	RespChan chan<- RPCResponse
}

// Respond is used to respond with a response, error or both
func (r *RPC) Respond(resp interface{}, err error) {
	r.RespChan <- RPCResponse{resp, err}
}

var (
	// ErrPipelineReplicationNotSupported can be returned by the transport to
	// signal that pipeline replication is not supported in general, and that
	// no error message should be produced.
	ErrPipelineReplicationNotSupported = errors.New("pipeline replication not supported")
)

// Transport provides an interface for network transports
// to allow Raft to communicate with other nodes.
type Transport interface {
	// Consumer returns a channel that can be used to
	// consume and respond to RPC requests.
	Consumer() <-chan RPC

	// LocalAddr is used to return our local address to distinguish from our peers.
	LocalAddr() ServerAddress

	// AppendEntriesPipeline returns an interface that can be used to pipeline
	// AppendEntries requests. It may alternatively return
	// ErrPipelineReplicationNotSupported or other errors.
	AppendEntriesPipeline(target ServerAddress) (AppendPipeline2, error)

	// AppendEntries sends the appropriate RPC to the target node.
	AppendEntries(target ServerAddress, args *AppendEntriesRequest, resp *AppendEntriesResponse) error

	// RequestVote sends the appropriate RPC to the target node.
	RequestVote(target ServerAddress, args *RequestVoteRequest, resp *RequestVoteResponse) error

	// InstallSnapshot is used to push a snapshot down to a follower. The data is read from
	// the ReadCloser and streamed to the client.
	InstallSnapshot(target ServerAddress, args *InstallSnapshotRequest, resp *InstallSnapshotResponse, data io.Reader) error

	// EncodePeer is used to serialize a peer's address.
	EncodePeer(ServerAddress) []byte

	// DecodePeer is used to deserialize a peer's address.
	DecodePeer([]byte) ServerAddress

	// SetHeartbeatHandler is used to setup a heartbeat handler
	// as a fast-pass. This is to avoid head-of-line blocking from
	// disk IO. If a Transport does not support this, it can simply
	// ignore the call, and push the heartbeat onto the Consumer channel.
	SetHeartbeatHandler(cb func(rpc RPC))
}

// WithClose is an interface that a transport may provide which
// allows a transport to be shut down cleanly when a Raft instance
// shuts down.
//
// It is defined separately from Transport as unfortunately it wasn't in the
// original interface specification.
type WithClose interface {
	// Close permanently closes a transport, stopping
	// any associated goroutines and freeing other resources.
	Close() error
}

// LoopbackTransport is an interface that provides a loopback transport suitable for testing
// e.g. InmemTransport. It's there so we don't have to rewrite tests.
type LoopbackTransport interface {
	Transport // Embedded transport reference
	WithPeers // Embedded peer management
	WithClose // with a close routine
}

// WithPeers is an interface that a transport may provide which allows for connection and
// disconnection. Unless the transport is a loopback transport, the transport specified to
// "Connect" is likely to be nil.
type WithPeers interface {
	Connect(peer ServerAddress, t Transport) // Connect a peer
	Disconnect(peer ServerAddress)           // Disconnect a given peer
	DisconnectAll()                          // Disconnect all peers, possibly to reconnect them later
}

// AppendPipeline2 is used for pipelining AppendEntries requests. It is used
// to increase the replication throughput by masking latency and better
// utilizing bandwidth.
//
// AppendPipeline2 replaces AppendPipeline, which had the following method:
//     // Consumer returns a channel that can be used to consume
//     // response futures when they are ready.
//     Consumer() <-chan AppendFuture
// Consumer() is no longer utilized and should be removed from transports.
// Because some transports were written assuming that Consumer() would be called
// and the channel returned would be drained, we decided to make this a
// compile-breaking change. Some transports were also buggy in not erroring out
// all the existing futures upon Close(); doing so is now required.
type AppendPipeline2 interface {
	// AppendEntries is used to add another request to the pipeline.
	// The send may block which is an effective form of back-pressure.
	AppendEntries(args *AppendEntriesRequest, resp *AppendEntriesResponse) (AppendFuture, error)

	// Close closes the pipeline, cancels all inflight RPCs, and errors all their
	// futures.
	Close() error
}

// AppendFuture is used to return information about a pipelined AppendEntries request.
type AppendFuture interface {
	Future

	// Start returns the time that the append request was started.
	// It is always OK to call this method.
	Start() time.Time

	// Request holds the parameters of the AppendEntries call.
	// It is always OK to call this method.
	Request() *AppendEntriesRequest

	// Response holds the results of the AppendEntries call.
	// This method must only be called after the Error
	// method returns, and will only be valid on success.
	Response() *AppendEntriesResponse
}

// Adapter to reopen new pipelines upon failures.
type reliablePipeline struct {
	transport Transport
	target    ServerAddress
	// Protects 'current'.
	mutex sync.Mutex
	// An open pipeline without errors, or nil.
	current AppendPipeline2
}

// Future used in reliablePipeline to close old pipeline upon failures.
type reliableAppendFuture struct {
	base     AppendFuture
	adapter  *reliablePipeline
	pipeline AppendPipeline2
}

// Masks errors in using an AppendEntries pipeline by creating new pipelines
// internally.
func makeReliablePipeline(transport Transport, target ServerAddress) AppendPipeline2 {
	return &reliablePipeline{
		transport: transport,
		target:    target,
		current:   nil,
	}
}

func (rp *reliablePipeline) failed(pipeline AppendPipeline2) {
	rp.mutex.Lock()
	defer rp.mutex.Unlock()
	if rp.current == pipeline {
		_ = rp.current.Close()
		rp.current = nil
	}
}

func (rp *reliablePipeline) getPipeline() (AppendPipeline2, error) {
	rp.mutex.Lock()
	defer rp.mutex.Unlock()
	if rp.current == nil {
		pipeline, err := rp.transport.AppendEntriesPipeline(rp.target)
		if err != nil {
			return nil, err
		}
		rp.current = pipeline
	}
	return rp.current, nil
}

func (rp *reliablePipeline) AppendEntries(args *AppendEntriesRequest, resp *AppendEntriesResponse) (AppendFuture, error) {
	pipeline, err := rp.getPipeline()
	if err != nil {
		return nil, err
	}
	future, err := pipeline.AppendEntries(args, resp)
	if err != nil {
		rp.failed(pipeline)
		return nil, err
	}
	return &reliableAppendFuture{
		base:     future,
		adapter:  rp,
		pipeline: pipeline,
	}, nil
}

func (rp *reliablePipeline) Close() error {
	rp.mutex.Lock()
	defer rp.mutex.Unlock()
	if rp.current != nil {
		err := rp.current.Close()
		rp.current = nil
		return err
	}
	return nil
}

func (rf *reliableAppendFuture) Error() error {
	err := rf.base.Error()
	if err != nil {
		rf.adapter.failed(rf.pipeline)
	}
	return err
}

func (rf *reliableAppendFuture) Start() time.Time {
	return rf.base.Start()
}
func (rf *reliableAppendFuture) Request() *AppendEntriesRequest {
	return rf.base.Request()
}
func (rf *reliableAppendFuture) Response() *AppendEntriesResponse {
	return rf.base.Response()
}
