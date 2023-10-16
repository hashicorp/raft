// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInmemTransportImpl(t *testing.T) {
	var inm interface{} = &InmemTransport{}
	if _, ok := inm.(Transport); !ok {
		t.Fatalf("InmemTransport is not a Transport")
	}
	if _, ok := inm.(LoopbackTransport); !ok {
		t.Fatalf("InmemTransport is not a Loopback Transport")
	}
	if _, ok := inm.(WithPeers); !ok {
		t.Fatalf("InmemTransport is not a WithPeers Transport")
	}
}

func TestInmemTransportWriteTimeout(t *testing.T) {
	// InmemTransport should timeout if the other end has gone away
	// when it tries to send a request.
	// Use unbuffered channels so that we can see the write failing
	// without having to contrive to fill up the buffer first.
	timeout := 10 * time.Millisecond
	t1 := &InmemTransport{
		consumerCh: make(chan RPC),
		localAddr:  NewInmemAddr(),
		peers:      make(map[ServerAddress]*InmemTransport),
		timeout:    timeout,
	}
	t2 := &InmemTransport{
		consumerCh: make(chan RPC),
		localAddr:  NewInmemAddr(),
		peers:      make(map[ServerAddress]*InmemTransport),
		timeout:    timeout,
	}
	a2 := t2.LocalAddr()
	t1.Connect(a2, t2)

	stop := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		var i uint64
		for {
			select {
			case <-stop:
				return
			case rpc := <-t2.Consumer():
				i++
				rpc.Respond(&AppendEntriesResponse{
					Success: true,
					LastLog: i,
				}, nil)
			}
		}
	}()

	var resp AppendEntriesResponse
	// Sanity check that sending is working before stopping the
	// responder.
	err := t1.AppendEntries("server1", a2, &AppendEntriesRequest{}, &resp)
	NoErr(err, t)
	require.True(t, resp.LastLog == 1)

	close(stop)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for responder to stop")
	}

	err = t1.AppendEntries("server1", a2, &AppendEntriesRequest{}, &resp)
	if err == nil {
		t.Fatalf("expected AppendEntries to time out")
	}
	if err.Error() != "send timed out" {
		t.Fatalf("unexpected error: %v", err)
	}
}
