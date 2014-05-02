package raft

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// InmemAddr implements the net.Addr interface.
type InmemAddr struct {
	ID string
}

// NewInmemAddr returns a new in-memory addr with
// a randomly generate UUID as the ID
func NewInmemAddr() *InmemAddr {
	return &InmemAddr{generateUUID()}
}

// Network implements the net.Addr interface.
func (ia *InmemAddr) Network() string {
	return "inmem"
}

// String implements the net.Addr interface.
func (ia *InmemAddr) String() string {
	return ia.ID
}

// InmemTransport Implements the Transport interface, to allow Raft to be
// tested in-memory without going over a network.
type InmemTransport struct {
	sync.RWMutex
	consumerCh chan RPC
	localAddr  *InmemAddr
	peers      map[string]*InmemTransport
	timeout    time.Duration
}

// NewInmemTransport is used to initialize a new transport
// and generates a random local address.
func NewInmemTransport() (*InmemAddr, *InmemTransport) {
	addr := NewInmemAddr()
	trans := &InmemTransport{
		consumerCh: make(chan RPC, 16),
		localAddr:  addr,
		peers:      make(map[string]*InmemTransport),
		timeout:    50 * time.Millisecond,
	}
	return addr, trans
}

// Consumer implements the Transport interface.
func (i *InmemTransport) Consumer() <-chan RPC {
	return i.consumerCh
}

// LocalAddr implements the Transport interface.
func (i *InmemTransport) LocalAddr() net.Addr {
	return i.localAddr
}

// AppendEntries implements the Transport interface.
func (i *InmemTransport) AppendEntries(target net.Addr, args *AppendEntriesRequest, resp *AppendEntriesResponse) error {
	rpcResp, err := i.makeRPC(target, args, nil, i.timeout)
	if err != nil {
		return err
	}

	// Copy the result back
	out := rpcResp.Response.(*AppendEntriesResponse)
	*resp = *out
	return nil
}

// RequestVote implements the Transport interface.
func (i *InmemTransport) RequestVote(target net.Addr, args *RequestVoteRequest, resp *RequestVoteResponse) error {
	rpcResp, err := i.makeRPC(target, args, nil, i.timeout)
	if err != nil {
		return err
	}

	// Copy the result back
	out := rpcResp.Response.(*RequestVoteResponse)
	*resp = *out
	return nil
}

// InstallSnapshot implements the Transport interface.
func (i *InmemTransport) InstallSnapshot(target net.Addr, args *InstallSnapshotRequest, resp *InstallSnapshotResponse, data io.Reader) error {
	rpcResp, err := i.makeRPC(target, args, data, 10*i.timeout)
	if err != nil {
		return err
	}

	// Copy the result back
	out := rpcResp.Response.(*InstallSnapshotResponse)
	*resp = *out
	return nil
}

func (i *InmemTransport) makeRPC(target net.Addr, args interface{}, r io.Reader, timeout time.Duration) (rpcResp RPCResponse, err error) {
	i.RLock()
	peer, ok := i.peers[target.String()]
	i.RUnlock()

	if !ok {
		err = fmt.Errorf("failed to connect to peer: %v", target)
		return
	}

	// Send the RPC over
	respCh := make(chan RPCResponse)
	peer.consumerCh <- RPC{
		Command:  args,
		Reader:   r,
		RespChan: respCh,
	}

	// Wait for a response
	select {
	case rpcResp = <-respCh:
		if rpcResp.Error != nil {
			err = rpcResp.Error
		}
	case <-time.After(timeout):
		err = fmt.Errorf("command timed out")
	}
	return
}

// EncodePeer implements the Transport interface. It uses the UUID as the
// address directly.
func (i *InmemTransport) EncodePeer(p net.Addr) []byte {
	return []byte(p.String())
}

// DecodePeer implements the Transport interface. It wraps the UUID in an
// InmemAddr.
func (i *InmemTransport) DecodePeer(buf []byte) net.Addr {
	return &InmemAddr{string(buf)}
}

// Connect is used to connect this transport to another transport for
// a given peer name. This allows for local routing.
func (i *InmemTransport) Connect(peer net.Addr, trans *InmemTransport) {
	i.Lock()
	defer i.Unlock()
	i.peers[peer.String()] = trans
}

// Disconnect is used to remove the ability to route to a given peer.
func (i *InmemTransport) Disconnect(peer net.Addr) {
	i.Lock()
	defer i.Unlock()
	delete(i.peers, peer.String())
}

// DisconnectAll is used to remove all routes to peers.
func (i *InmemTransport) DisconnectAll() {
	i.Lock()
	defer i.Unlock()
	i.peers = make(map[string]*InmemTransport)
}
