package raft

import (
	"testing"
)

func TestDatagramTransportImpl(t *testing.T) {
	var dat interface{} = &DatagramTransport{}
	if _, ok := dat.(Transport); !ok {
		t.Fatalf("DatagramTransport is not a Transport")
	}
}
