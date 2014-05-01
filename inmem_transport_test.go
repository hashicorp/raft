package raft

import (
	"net"
	"testing"
)

func TestInmemAddrImpl(t *testing.T) {
	var inm interface{} = NewInmemAddr()
	if _, ok := inm.(net.Addr); !ok {
		t.Fatalf("InmemAddr is not a net.Addr")
	}
}

func TestInmemAddr(t *testing.T) {
	inm := NewInmemAddr()
	if inm.Network() != "inmem" {
		t.Fatalf("bad network")
	}
	if inm.String() != inm.ID {
		t.Fatalf("bad string")
	}
}

func TestInmemTransportImpl(t *testing.T) {
	var inm interface{} = &InmemTransport{}
	if _, ok := inm.(Transport); !ok {
		t.Fatalf("InmemTransport is not a Transport")
	}
}
