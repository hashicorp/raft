package raft

import (
	"fmt"
	"io"
	"log"
	"net"
	"sync"
)

// NetworkTransport provides a network based transport that can be
// used to communicate with Raft on remote machines. It requires
// an underlying layer to provide a stream abstraction, which can
// be simple TCP, TLS, or something more advanced. Underlying addresses
// must be castable to TCPAddr
type NetworkTransport struct {
	connPool     map[string][]net.Conn
	connPoolLock sync.Mutex

	consumeCh chan RPC

	dialer   net.Dialer
	listener net.Listener

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

// Creates a new network transport with the given dailer and listener
func NewNetworkTransport(dialer net.Dialer, listener net.Listener) *NetworkTransport {
	trans := &NetworkTransport{
		connPool:   make(map[string][]net.Conn),
		consumeCh:  make(chan RPC),
		dialer:     dialer,
		listener:   listener,
		shutdownCh: make(chan struct{}),
	}
	go trans.listen()
	return trans
}

// Close is used to stop the network transport
func (n *NetworkTransport) Close() error {
	n.shutdownLock.Lock()
	defer n.shutdownLock.Lock()

	if !n.shutdown {
		close(n.shutdownCh)
		n.listener.Close()
		n.shutdown = true
	}
	return nil
}

func (n *NetworkTransport) Consumer() <-chan RPC {
	return n.consumeCh
}

func (n *NetworkTransport) LocalAddr() net.Addr {
	return n.listener.Addr()
}

func (n *NetworkTransport) AppendEntries(target net.Addr, args *AppendEntriesRequest, resp *AppendEntriesResponse) error {
	// TODO
	return nil
}

func (n *NetworkTransport) RequestVote(target net.Addr, args *RequestVoteRequest, resp *RequestVoteResponse) error {
	// TODO
	return nil
}

func (n *NetworkTransport) InstallSnapshot(target net.Addr, args *InstallSnapshotRequest, resp *InstallSnapshotResponse, data io.ReadCloser) error {
	// TODO
	return nil
}

func (n *NetworkTransport) EncodePeer(p net.Addr) []byte {
	return []byte(p.String())
}

func (n *NetworkTransport) DecodePeer(buf []byte) net.Addr {
	addr, err := net.ResolveTCPAddr("tcp", string(buf))
	if err != nil {
		panic(fmt.Errorf("Failed to parse network address: %s", buf))
	}
	return addr
}

// listen is used to handling incoming connections
func (n *NetworkTransport) listen() {
	for {
		// Accept incoming connections
		conn, err := n.listener.Accept()
		if err != nil {
			if n.shutdown {
				return
			}
			log.Printf("[ERR] Failed to accept connection: %v", err)
			continue
		}

		// Handle the connection in dedicated routine
		go n.handleConn(conn)
	}
}

// handleConn is used to handle an inbound connection for its lifespan
func (n *NetworkTransport) handleConn(conn net.Conn) {
	defer conn.Close()

	// TODO:
}
