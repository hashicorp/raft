package raft

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/hashicorp/go-msgpack/codec"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

// DatagramTransport implements the Transport interface for a transport by datagrams
type DatagramTransport struct {
	consumerCh chan RPC      // The consumer channel read by raft
	Logger     *log.Logger   // A logger
	rpc        DatagramRPC   // the RPC layer
	timeout    time.Duration // the timeout
}

// datagramPipeline is used to pipeline requests for the datagram transport.
type datagramPipeline struct {
	trans    *DatagramTransport // The datagram transport layer
	peerAddr string             // the address of the peer to which we are speaking

	doneCh       chan AppendFuture              // A channel to which we send done items
	inprogressCh chan *datagramPipelineInflight // A channel of in-progress items

	shutdownLock sync.Mutex    // mutex protecting shutdown
	shutdown     bool          // true if shutdown
	shutdownCh   chan struct{} // close this channel to shutdown the pipeline
}

// An inflight item within a datagramPipeline
type datagramPipelineInflight struct {
	future *appendFuture
	respCh <-chan RPCResponse
}

// Datagram RPC is an interface the implementation of which provides a simple RPC call
type DatagramRPC interface {
	// Asynchronously call the transport
	CallAsync(transport *DatagramTransport, target string, args interface{}, data io.Reader, responseChan chan<- RPCResponse, timeout <-chan time.Time, shutdown <-chan struct{}) error
	// Return the local address
	LocalAddr() string
	// Close the channel
	Close() error
}

// Various errors
var (
	ErrUnknownRPCType  = errors.New("Unknown RPC type")
	ErrCommandTimedOut = errors.New("Command timed out")
)

// Consumer returns a channel that can be used to consume and respond to RPC requests.
func (d *DatagramTransport) Consumer() <-chan RPC {
	return d.consumerCh
}

// LocalAddr is used to return our local address to distinguish from our peers.
func (d *DatagramTransport) LocalAddr() string {
	return d.rpc.LocalAddr()
}

// AppendEntriesPipeline returns an interface that can be used to pipeline
// AppendEntries requests.
func (d *DatagramTransport) AppendEntriesPipeline(target string) (AppendPipeline, error) {
	pipeline := newDatagramPipeline(d, target)
	return pipeline, nil
}

// AppendEntries sends the appropriate RPC to the target node.
func (d *DatagramTransport) AppendEntries(target string, args *AppendEntriesRequest, resp *AppendEntriesResponse) error {
	rpcResp, err := d.makeRPC(target, args, nil, d.timeout)
	if err != nil {
		return err
	}

	// Copy the result back
	out := rpcResp.Response.(*AppendEntriesResponse)
	*resp = *out
	return nil
}

// RequestVote sends the appropriate RPC to the target node.
func (d *DatagramTransport) RequestVote(target string, args *RequestVoteRequest, resp *RequestVoteResponse) error {
	rpcResp, err := d.makeRPC(target, args, nil, d.timeout)
	if err != nil {
		return err
	}

	// Copy the result back
	out := rpcResp.Response.(*RequestVoteResponse)
	*resp = *out
	return nil
}

// InstallSnapshot is used to push a snapshot down to a follower. The data is read from the ReadCloser and streamed to the client.
func (d *DatagramTransport) InstallSnapshot(target string, args *InstallSnapshotRequest, resp *InstallSnapshotResponse, data io.Reader) error {
	rpcResp, err := d.makeRPC(target, args, data, 10*d.timeout)
	if err != nil {
		return err
	}

	// Copy the result back
	out := rpcResp.Response.(*InstallSnapshotResponse)
	*resp = *out
	return nil
}

// EncodePeer is used to serialize a peer name.
func (d *DatagramTransport) EncodePeer(p string) []byte {
	return []byte(p)
}

// DecodePeer is used to deserialize a peer name.
func (d *DatagramTransport) DecodePeer(buf []byte) string {
	return string(buf)
}

// SetHeartbeatHandler is used to setup a heartbeat handler as a fast-pass. This is to avoid head-of-line blocking from
// disk IO. If a Transport does not support this, it can simply ignore the call, and push the heartbeat onto the Consumer channel.
// Currently unsupported by this handler
func (d *DatagramTransport) SetHeartbeatHandler(cb func(rpc RPC)) {
	// TODO
}

// makeRPC sends an RPC call
func (d *DatagramTransport) makeRPC(target string, args interface{}, r io.Reader, timeout time.Duration) (rpcResp RPCResponse, err error) {
	// Send the RPC over
	respCh := make(chan RPCResponse)

	// Handle a timeout
	var timeoutCh <-chan time.Time
	if timeout > 0 {
		timeoutCh = time.After(timeout)
	}

	if err = d.rpc.CallAsync(d, target, args, r, respCh, timeoutCh, nil); err != nil {
		close(respCh)
		return
	}

	// Wait for a response
	select {
	case rpcResp = <-respCh:
		if rpcResp.Error != nil {
			err = rpcResp.Error
		}
	case <-time.After(timeout):
		err = ErrCommandTimedOut
	}
	return
}

// Convert an object to an RPC type suitable for encoding
func (d *DatagramTransport) toRpcType(args interface{}) uint8 {
	switch args.(type) {
	case *AppendEntriesRequest:
		return rpcAppendEntries
	case *RequestVoteRequest:
		return rpcRequestVote
	case *InstallSnapshotRequest:
		return rpcInstallSnapshot

	case *AppendEntriesResponse:
		return rpcAppendEntries
	case *RequestVoteResponse:
		return rpcRequestVote
	case *InstallSnapshotResponse:
		return rpcInstallSnapshot

	default:
		panic(fmt.Sprintf("Unknown RPC type passed [%T] %+v", args, args))
	}
}

// Encode a request
func (d *DatagramTransport) EncodeRequest(rpc *RPC) ([]byte, error) {
	var buf bytes.Buffer
	if err := buf.WriteByte(d.toRpcType(rpc.Command)); err != nil {
		return nil, err
	}
	enc := codec.NewEncoder(&buf, &codec.MsgpackHandle{})
	if err := enc.Encode(rpc.Command); err != nil {
		return nil, err
	}
	if rpc.Reader != nil {
		_, err := io.Copy(&buf, rpc.Reader)
		if err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

// Encode a response
func (d *DatagramTransport) EncodeResponse(response *RPCResponse) ([]byte, error) {
	var buf bytes.Buffer
	if err := buf.WriteByte(d.toRpcType(response.Response)); err != nil {
		return nil, err
	}
	enc := codec.NewEncoder(&buf, &codec.MsgpackHandle{})
	var rpcError string
	if response.Error != nil {
		rpcError = response.Error.Error()
	}
	if err := enc.Encode(rpcError); err != nil {
		return nil, err
	}
	if err := enc.Encode(response.Response); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode a request
func (d *DatagramTransport) DecodeRequest(buffer []byte) (*RPC, error) {
	rpcType := buffer[0]
	buf := bytes.NewBuffer(buffer[1:])
	dec := codec.NewDecoder(buf, &codec.MsgpackHandle{})
	var rpc RPC

	switch rpcType {
	case rpcAppendEntries:
		var req AppendEntriesRequest
		if err := dec.Decode(&req); err != nil {
			return nil, err
		}
		rpc.Command = &req

	case rpcRequestVote:
		var req RequestVoteRequest
		if err := dec.Decode(&req); err != nil {
			return nil, err
		}
		rpc.Command = &req

	case rpcInstallSnapshot:
		var req InstallSnapshotRequest
		if err := dec.Decode(&req); err != nil {
			return nil, err
		}
		rpc.Command = &req
		rpc.Reader = bytes.NewBuffer(buf.Bytes())

	default:
		return nil, ErrUnknownRPCType
	}

	return &rpc, nil
}

// Decode a response
func (d *DatagramTransport) DecodeResponse(buffer []byte) (*RPCResponse, error) {
	rpcType := buffer[0]
	buf := bytes.NewBuffer(buffer[1:])
	dec := codec.NewDecoder(buf, &codec.MsgpackHandle{})
	var response RPCResponse

	var rpcError string
	if err := dec.Decode(&rpcError); err != nil {
		return nil, err
	}

	if rpcError != "" {
		response.Error = fmt.Errorf("RPC Error: %s", rpcError)
	}

	switch rpcType {
	case rpcAppendEntries:
		var resp AppendEntriesResponse
		if err := dec.Decode(&resp); err != nil {
			return nil, err
		}
		response.Response = &resp

	case rpcRequestVote:
		var resp RequestVoteResponse
		if err := dec.Decode(&resp); err != nil {
			return nil, err
		}
		response.Response = &resp

	case rpcInstallSnapshot:
		var resp InstallSnapshotResponse
		if err := dec.Decode(&resp); err != nil {
			return nil, err
		}
		response.Response = &resp

	default:
		return nil, ErrUnknownRPCType
	}
	return &response, nil
}

// Close() is purely here so we implement Transport. In fact Close() logic is normally provided by the
// concrete struct into which this is embedded
func (d *DatagramTransport) Close() error {
	d.rpc.Close()
	return nil
}

// NewDatagramTransport creates a new DatagramTransport with the given DatagramRPC implementation
// The timeout is used to apply I/O deadlines. For InstallSnapshot, we multiply
// the timeout by (SnapshotSize / TimeoutScale).
func NewDatagramTransport(rpc DatagramRPC, timeout time.Duration, logOutput io.Writer) *DatagramTransport {
	if logOutput == nil {
		logOutput = os.Stderr
	}
	return NewDatagramTransportWithLogger(rpc, timeout, log.New(logOutput, "", log.LstdFlags))
}

// NewDatagramTransportWithLogger creates a new Datagram transport with the given DatagramRPC implementation
func NewDatagramTransportWithLogger(rpc DatagramRPC, timeout time.Duration, logger *log.Logger) *DatagramTransport {
	if logger == nil {
		logger = log.New(os.Stderr, "", log.LstdFlags)
	}
	if timeout == time.Duration(0) {
		timeout = 500 * time.Millisecond
	}
	trans := &DatagramTransport{
		consumerCh: make(chan RPC, 16),
		Logger:     logger,
		rpc:        rpc,
		timeout:    timeout,
	}
	return trans
}

// Create a new pipeline
func newDatagramPipeline(trans *DatagramTransport, addr string) *datagramPipeline {
	d := &datagramPipeline{
		trans:        trans,
		peerAddr:     addr,
		doneCh:       make(chan AppendFuture, 16),
		inprogressCh: make(chan *datagramPipelineInflight, 16),
		shutdownCh:   make(chan struct{}),
	}
	go d.decodeResponses()
	return d
}

// goroutine to decode pipeline responses
func (d *datagramPipeline) decodeResponses() {
	timeout := d.trans.timeout
	for {
		select {
		case inp := <-d.inprogressCh:
			var timeoutCh <-chan time.Time
			if timeout > 0 {
				timeoutCh = time.After(timeout)
			}

			select {
			case rpcResp := <-inp.respCh:
				// Copy the result back
				*inp.future.resp = *rpcResp.Response.(*AppendEntriesResponse)
				inp.future.respond(rpcResp.Error)

				select {
				case d.doneCh <- inp.future:
				case <-d.shutdownCh:
					return
				}

			case <-timeoutCh:
				inp.future.respond(ErrCommandTimedOut)
				select {
				case d.doneCh <- inp.future:
				case <-d.shutdownCh:
					return
				}

			case <-d.shutdownCh:
				return
			}
		case <-d.shutdownCh:
			return
		}
	}
}

// Append entries via a pipeline
func (dp *datagramPipeline) AppendEntries(args *AppendEntriesRequest, resp *AppendEntriesResponse) (AppendFuture, error) {
	// Create a new future
	future := &appendFuture{
		start: time.Now(),
		args:  args,
		resp:  resp,
	}
	future.init()

	// Handle a timeout
	var timeout <-chan time.Time
	if dp.trans.timeout > 0 {
		timeout = time.After(dp.trans.timeout)
	}

	// Send the RPC over
	respCh := make(chan RPCResponse, 1)

	if err := dp.trans.rpc.CallAsync(dp.trans, dp.peerAddr, args, nil, respCh, timeout, dp.shutdownCh); err != nil {
		close(respCh)
		return nil, err
	}

	// Send to be decoded
	select {
	case dp.inprogressCh <- &datagramPipelineInflight{future, respCh}:
		return future, nil
	case <-dp.shutdownCh:
		return nil, ErrPipelineShutdown
	}
}

// return the channel of completed operations
func (dp *datagramPipeline) Consumer() <-chan AppendFuture {
	return dp.doneCh
}

// close a pipeline
func (dp *datagramPipeline) Close() error {
	dp.shutdownLock.Lock()
	defer dp.shutdownLock.Unlock()
	if dp.shutdown {
		return nil
	}

	dp.shutdown = true
	close(dp.shutdownCh)
	return nil
}
