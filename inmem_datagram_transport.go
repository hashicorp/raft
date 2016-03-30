package raft

import (
	"fmt"
	"io"
	"sync"
	"time"
)

// An implementation of DatagramTransport that uses channels to pipe datagrams between two in-memory instances
type InmemDatagramTransport struct {
	DatagramTransport
	sync.RWMutex
	peers     map[string]*InmemDatagramTransport
	pipelines []*inmemPipeline
}

// RPC driver for InmemDatagramTransport
type InmemDatagramRPC struct {
	localaddr string
	trans     *InmemDatagramTransport
}

// Connect is used to connect this transport to another transport for
// a given peer name. This allows for local routing.
func (i *InmemDatagramTransport) Connect(peer string, t Transport) {
	trans := t.(*InmemDatagramTransport)
	i.Lock()
	defer i.Unlock()
	i.peers[peer] = trans
}

// Disconnect is used to remove the ability to route to a given peer.
func (i *InmemDatagramTransport) Disconnect(peer string) {
	i.Lock()
	defer i.Unlock()
	delete(i.peers, peer)

	// Disconnect any pipelines
	n := len(i.pipelines)
	for idx := 0; idx < n; idx++ {
		if i.pipelines[idx].peerAddr == peer {
			i.pipelines[idx].Close()
			i.pipelines[idx], i.pipelines[n-1] = i.pipelines[n-1], nil
			idx--
			n--
		}
	}
	i.pipelines = i.pipelines[:n]
}

// DisconnectAll is used to remove all routes to peers.
func (i *InmemDatagramTransport) DisconnectAll() {
	i.Lock()
	defer i.Unlock()
	i.peers = make(map[string]*InmemDatagramTransport)

	// Handle pipelines
	for _, pipeline := range i.pipelines {
		pipeline.Close()
	}
	i.pipelines = nil
}

// Close the transport permanently
func (i *InmemDatagramTransport) Close() error {
	i.DisconnectAll()
	return nil
}

// Implementation of async call
func (idrpc *InmemDatagramRPC) CallAsync(transport *DatagramTransport, target string, args interface{}, data io.Reader, responseChan chan<- RPCResponse, timeout <-chan time.Time, shutdown <-chan struct{}) error {
	i := idrpc.trans
	i.RLock()
	peer, ok := i.peers[target]
	i.RUnlock()

	if !ok {
		return fmt.Errorf("failed to connect to peer: %v", target)
	}

	rpc := RPC{
		Command:  args,
		Reader:   data,
		RespChan: responseChan,
	}
	// Send the RPC over
	select {
	case peer.consumerCh <- rpc:
		break
	case <-timeout:
		return fmt.Errorf("command enqueue timeout")
	case <-shutdown:
		return ErrPipelineShutdown
	}
	return nil
}

// Trivial implementation of LocalAddr()
func (idprc *InmemDatagramRPC) LocalAddr() string {
	return idprc.localaddr
}

// RPC close routine - does nothing
func (idprc *InmemDatagramRPC) Close() error {
	return nil
}

// Create a new InmemDatagramRPC object
func NewInmemDatagramRPC(localaddr string) *InmemDatagramRPC {
	idrpc := &InmemDatagramRPC{
		localaddr: localaddr,
	}
	return idrpc
}

// NewInmemDatagramTransport is used to initialize a new transport
// and generates a random local address.
func NewInmemDatagramTransport(addr string) (string, *InmemDatagramTransport) {
	if addr == "" {
		addr = NewInmemAddr()
	}
	rpc := NewInmemDatagramRPC(addr)
	trans := &InmemDatagramTransport{
		peers:             make(map[string]*InmemDatagramTransport),
		DatagramTransport: *NewDatagramTransportWithLogger(rpc, 50*time.Millisecond, nil),
	}
	rpc.trans = trans
	return addr, trans
}
