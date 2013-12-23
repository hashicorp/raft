package raft

import (
	"io"
	"net"
	"time"
)

// TCPStreamLayer implements StreamLayer interface for plain TCP
type TCPStreamLayer struct {
	listener *net.TCPListener
}

// NewTCPTransport returns a NetworkTransport that is built on top of
// a TCP streaming transport layer
func NewTCPTransport(bindAddr string, maxPool int, timeout time.Duration, logOutput io.Writer) (*NetworkTransport, error) {
	// Try to bind
	list, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, err
	}

	// Create steram
	stream := &TCPStreamLayer{listener: list.(*net.TCPListener)}

	// Create the network transport
	trans := NewNetworkTransport(stream, maxPool, timeout, logOutput)
	return trans, nil
}

func (t *TCPStreamLayer) Dial(address string, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", address, timeout)
}

func (t *TCPStreamLayer) Accept() (c net.Conn, err error) {
	return t.listener.Accept()
}

func (t *TCPStreamLayer) Close() (err error) {
	return t.listener.Close()
}

func (t *TCPStreamLayer) Addr() net.Addr {
	return t.listener.Addr()
}
