// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"net"
	"testing"
)

func TestTCPTransport_BadAddr(t *testing.T) {
	_, err := NewTCPTransportWithLogger("0.0.0.0:0", nil, 1, 0, newTestLogger(t))
	if err != errNotAdvertisable {
		t.Fatalf("err: %v", err)
	}
}

func TestTCPTransport_EmptyAddr(t *testing.T) {
	_, err := NewTCPTransportWithLogger(":0", nil, 1, 0, newTestLogger(t))
	if err != errNotAdvertisable {
		t.Fatalf("err: %v", err)
	}
}

func TestTCPTransport_WithAdvertise(t *testing.T) {
	ips, err := net.LookupIP("localhost")
	if err != nil {
		t.Fatal(err)
	}
	if len(ips) == 0 {
		t.Fatalf("localhost did not resolve to any IPs")
	}
	addr := &net.TCPAddr{IP: ips[0], Port: 12345}
	trans, err := NewTCPTransportWithLogger("0.0.0.0:0", addr, 1, 0, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if trans.LocalAddr() != ServerAddress(net.JoinHostPort(ips[0].String(), "12345")) {
		t.Fatalf("bad: %v", trans.LocalAddr())
	}
}
