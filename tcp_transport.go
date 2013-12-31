package raft

import (
	"fmt"
	"io"
	"net"
	"time"
)

// TCPStreamLayer implements StreamLayer interface for plain TCP
type TCPStreamLayer struct {
	advertise net.Addr
	listener  *net.TCPListener
}

// NewTCPTransport returns a NetworkTransport that is built on top of
// a TCP streaming transport layer
func NewTCPTransport(bindAddr string, advertise net.Addr, maxPool int,
	timeout time.Duration, logOutput io.Writer) (*NetworkTransport, error) {
	// Try to bind
	list, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, err
	}

	// Create steram
	stream := &TCPStreamLayer{
		advertise: advertise,
		listener:  list.(*net.TCPListener),
	}

	// Verify that we have a usable advertise address
	addr, ok := stream.Addr().(*net.TCPAddr)
	if !ok {
		list.Close()
		return nil, fmt.Errorf("Local address is not a TCP Address!")
	}
	if addr.IP.IsUnspecified() {
		list.Close()
		return nil, fmt.Errorf("Local bind address is not advertisable!")
	}

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
	// Use an advertise addr if provided
	if t.advertise != nil {
		return t.advertise
	}
	return t.listener.Addr()
}
