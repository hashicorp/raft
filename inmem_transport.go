package raft

import (
	"fmt"
	"net"
	"sync"
)

// Implements the net.Addr interface
type InmemAddr struct {
	Id string
}

// NewInmemAddr returns a new in-memory addr with
// a randomly generate UUID as the ID
func NewInmemAddr() *InmemAddr {
	return &InmemAddr{generateUUID()}
}

func (ia *InmemAddr) Network() string {
	return "inmem"
}

func (ia *InmemAddr) String() string {
	return ia.Id
}

// Implements the Transport interface to allow Raft to be tested
// in-memory without going over a network
type InmemTransport struct {
	sync.RWMutex
	consumerCh chan RPC
	localAddr  *InmemAddr
	peers      map[string]*InmemTransport
}

// NewInmemTransport is used to initialize a new transport
// and generates a random local address.
func NewInmemTransport() (*InmemAddr, *InmemTransport) {
	addr := NewInmemAddr()
	trans := &InmemTransport{
		consumerCh: make(chan RPC, 16),
		localAddr:  addr,
		peers:      make(map[string]*InmemTransport),
	}
	return addr, trans
}

func (i *InmemTransport) Consumer() <-chan RPC {
	return i.consumerCh
}

func (i *InmemTransport) LocalAddr() net.Addr {
	return i.localAddr
}

func (i *InmemTransport) AppendEntries(target net.Addr, args *AppendEntriesRequest, resp *AppendEntriesResponse) error {
	rpcResp, err := i.makeRPC(target, args)
	if err != nil {
		return err
	}

	// Copy the result back
	out := rpcResp.Response.(*AppendEntriesResponse)
	*resp = *out
	return nil
}

func (i *InmemTransport) RequestVote(target net.Addr, args *RequestVoteRequest, resp *RequestVoteResponse) error {
	rpcResp, err := i.makeRPC(target, args)
	if err != nil {
		return err
	}

	// Copy the result back
	out := rpcResp.Response.(*RequestVoteResponse)
	*resp = *out
	return nil
}

func (i *InmemTransport) makeRPC(target net.Addr, args interface{}) (rpcResp RPCResponse, err error) {
	i.RLock()
	peer, ok := i.peers[target.String()]
	i.RUnlock()

	if !ok {
		err = fmt.Errorf("Failed to connect to peer: %v", target)
		return
	}

	// Send the RPC over
	respCh := make(chan RPCResponse)
	peer.consumerCh <- RPC{
		Command:  args,
		RespChan: respCh,
	}

	// Wait for a response
	rpcResp = <-respCh
	if rpcResp.Error != nil {
		err = rpcResp.Error
	}
	return
}

// Use the UUID as the address directly
func (i *InmemTransport) EncodePeer(p net.Addr) []byte {
	return []byte(p.String())
}

// Wrap the UUID in an InmemAddr
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

// Disconnect is used to remove the ability to route to a given peer
func (i *InmemTransport) Disconnect(peer net.Addr) {
	i.Lock()
	defer i.Unlock()
	delete(i.peers, peer.String())
}

// DisconnectAll is used to remove all routes to peers
func (i *InmemTransport) DisconnectAll() {
	i.Lock()
	defer i.Unlock()
	i.peers = make(map[string]*InmemTransport)
}
