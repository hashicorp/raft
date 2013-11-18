package raft

import (
	"bufio"
	"fmt"
	"github.com/ugorji/go/codec"
	"io"
	"log"
	"net"
	"sync"
)

const (
	rpcAppendEntries uint8 = iota
	rpcRequestVote
	rpcInstallSnapshot
)

var (
	TransportShutdown = fmt.Errorf("transport shutdown")
)

/*

NetworkTransport provides a network based transport that can be
used to communicate with Raft on remote machines. It requires
an underlying layer to provide a stream abstraction, which can
be simple TCP, TLS, etc. Underlying addresses must be castable to TCPAddr

This transport is very simple and lightweight. Each RPC request is
framed by sending a byte that indicates the message type, followed
by the MsgPack encoded request.

The response is an error string followed by the response object,
both are encoded using MsgPack.

InstallSnapshot is special, in that after the RPC request we stream
the entire state. That socket is not re-used as the connection state
is not known if there is an error.

*/
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
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	dec := codec.NewDecoder(r, &codec.MsgpackHandle{})
	enc := codec.NewEncoder(w, &codec.MsgpackHandle{})

	for {
		if err := n.handleCommand(r, dec, enc); err != nil {
			if err != io.EOF {
				log.Printf("[ERR] Failed to decode incoming command: %v", err)
			}
			return
		}
		if err := w.Flush(); err != nil {
			log.Printf("[ERR] Failed to flush response: %v", err)
			return
		}
	}
}

// handleCommand is used to decode and dispatch a single command
func (n *NetworkTransport) handleCommand(r *bufio.Reader, dec *codec.Decoder, enc *codec.Encoder) error {
	// Get the rpc type
	rpcType, err := r.ReadByte()
	if err != nil {
		return err
	}

	// Create the RPC object
	respCh := make(chan RPCResponse)
	rpc := RPC{
		RespChan: respCh,
	}

	// Decode the command
	switch rpcType {
	case rpcAppendEntries:
		var req AppendEntriesRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req

	case rpcRequestVote:
		var req RequestVoteRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req

	case rpcInstallSnapshot:
		var req InstallSnapshotRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req
		rpc.Reader = io.LimitReader(r, req.Size)

	default:
		return fmt.Errorf("unknown rpc type %d", rpcType)
	}

	// Dispatch the RPC
	select {
	case n.consumeCh <- rpc:
	case <-n.shutdownCh:
		return TransportShutdown
	}

	// Wait for response
	select {
	case resp := <-respCh:
		// Send the error first
		respErr := ""
		if resp.Error != nil {
			respErr = resp.Error.Error()
		}
		if err := enc.Encode(respErr); err != nil {
			return err
		}

		// Send the response
		if err := enc.Encode(resp.Response); err != nil {
			return err
		}
	case <-n.shutdownCh:
		return TransportShutdown
	}
	return nil
}
