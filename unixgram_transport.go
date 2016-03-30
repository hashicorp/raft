package raft

import (
	"io/ioutil"
	"net"
	"os"
	"path"
	"sync"
	"time"
)

var unixgramOnce sync.Once
var UnixgramTransportTmpDir string

// Resolve an address to an a filesystem address of a unix socket
func unixgramTranslateAddress(addr string) (net.Addr, error) {
	return net.ResolveUnixAddr("unixgram", path.Join(UnixgramTransportTmpDir, addr))
}

// Create a new Unixgram RPC layer
func NewUnixgramRPC(localaddr string, tmpdir string) (*PacketConnRPC, error) {
	path := path.Join(tmpdir, localaddr)
	addr, err := net.ResolveUnixAddr("unixgram", path)
	if err != nil {
		return nil, err
	}
	pc, err := net.ListenUnixgram("unixgram", addr)
	if err != nil {
		return nil, err
	}
	pc.SetReadBuffer(65536)
	pc.SetWriteBuffer(65536)
	idrpc, _ := NewPacketConnRPC(pc, localaddr, func(p *PacketConnRPC) {
		p.pc.Close()
		os.Remove(path)
	})
	return idrpc, nil
}

// NewUnixgramTransport is used to initialize a new unixgram transport
// and generates a random local address if none is specified. Essentially
// this is a loopback transport that behaves like UDP, and is for testing only.
func NewUnixgramTransport(addr string) (string, *PacketConnTransport, error) {
	unixgramOnce.Do(func() {
		UnixgramTransportTmpDir, _ = ioutil.TempDir("", "raft")
	})
	if addr == "" {
		addr = NewInmemAddr()
	}
	rpc, err := NewUnixgramRPC(addr, UnixgramTransportTmpDir)
	if err != nil {
		return "", nil, err
	}

	tr := NewPacketConnTransport(unixgramTranslateAddress, func() *DatagramTransport {
		return NewDatagramTransport(rpc, 50*time.Millisecond, nil)
	})
	return addr, tr, nil
}
