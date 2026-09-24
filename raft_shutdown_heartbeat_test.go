// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"sync"
	"testing"
	"time"
)

// heartbeatFastPathTransport captures the heartbeat fast-path handler that Raft
// registers, so a test can invoke it from a separate goroutine the same way a
// real transport does from its connection handler.
type heartbeatFastPathTransport struct {
	*InmemTransport

	mu sync.Mutex
	fn func(RPC)
}

func (t *heartbeatFastPathTransport) SetHeartbeatHandler(cb func(RPC)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.fn = cb
}

func (t *heartbeatFastPathTransport) heartbeatHandler() func(RPC) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.fn
}

// blockingStableStore wraps a StableStore so a test can pause exactly one
// CurrentTerm write and observe whether any write arrives after the store has
// been "closed" by the owner.
type blockingStableStore struct {
	StableStore

	mu              sync.Mutex
	armed           bool
	closed          bool
	writeAfterClose bool

	entered chan struct{} // signalled once the armed write is inside the store
	release chan struct{} // closed by the test to let the armed write proceed
}

func newBlockingStableStore(inner StableStore) *blockingStableStore {
	return &blockingStableStore{
		StableStore: inner,
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
}

// arm makes the next CurrentTerm write block until release is closed.
func (s *blockingStableStore) arm() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.armed = true
}

// close marks the store closed, standing in for the owner closing its BoltDB.
func (s *blockingStableStore) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
}

func (s *blockingStableStore) sawWriteAfterClose() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeAfterClose
}

func (s *blockingStableStore) SetUint64(key []byte, val uint64) error {
	if string(key) == string(keyCurrentTerm) {
		s.mu.Lock()
		take := s.armed
		if take {
			s.armed = false
		}
		s.mu.Unlock()

		if take {
			// Tell the test we are inside the dangerous path, then wait.
			close(s.entered)
			<-s.release
		}
	}

	s.mu.Lock()
	if s.closed {
		// The owner already closed the store; a real BoltDB would return
		// "database not open" here and setCurrentTerm would panic.
		s.writeAfterClose = true
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	return s.StableStore.SetUint64(key, val)
}

// TestRaft_ShutdownWaitsForInFlightHeartbeat asserts the lifecycle invariant
// behind https://github.com/hashicorp/raft/issues/717: once
// Raft.Shutdown().Error() has returned, an already in-flight heartbeat handler
// must not still be able to write to durable Raft storage.
func TestRaft_ShutdownWaitsForInFlightHeartbeat(t *testing.T) {
	conf := inmemConfig(t)
	conf.LocalID = ServerID("node1")

	_, inmem := NewInmemTransport("")
	trans := &heartbeatFastPathTransport{InmemTransport: inmem}

	logs := NewInmemStore()
	stable := newBlockingStableStore(NewInmemStore())
	snap := NewInmemSnapshotStore()

	configuration := Configuration{Servers: []Server{{
		Suffrage: Voter,
		ID:       conf.LocalID,
		Address:  trans.LocalAddr(),
	}}}
	if err := BootstrapCluster(conf, logs, stable, snap, trans, configuration); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	r, err := NewRaft(conf, &MockFSM{}, logs, stable, snap, trans)
	if err != nil {
		t.Fatalf("NewRaft: %v", err)
	}

	// Let the node settle so later CurrentTerm writes are ours alone.
	select {
	case isLeader := <-r.LeaderCh():
		if !isLeader {
			t.Fatal("expected to gain leadership")
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("node never became leader, state=%v", r.State())
	}

	hb := trans.heartbeatHandler()
	if hb == nil {
		t.Fatal("raft did not register a heartbeat fast-path handler")
	}

	// Arm the store, then deliver a heartbeat carrying a newer term on a
	// separate goroutine, exactly as a transport connection handler would.
	stable.arm()
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		respCh := make(chan RPCResponse, 1)
		hb(RPC{
			Command: &AppendEntriesRequest{
				RPCHeader:    r.getRPCHeader(),
				Term:         r.getCurrentTerm() + 1,
				Leader:       trans.EncodePeer(conf.LocalID, trans.LocalAddr()),
				PrevLogEntry: 0,
				PrevLogTerm:  0,
			},
			RespChan: respCh,
		})
	}()

	// Deterministic barrier: the heartbeat is now inside the store write, past
	// processHeartbeat's early shutdown check.
	select {
	case <-stable.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("heartbeat never reached the CurrentTerm write")
	}

	// Shut down while that heartbeat is still in flight.
	shutdownReturned := make(chan struct{})
	go func() {
		defer close(shutdownReturned)
		if err := r.Shutdown().Error(); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
		// The owner of the store closes it as soon as Shutdown reports that
		// all background routines have exited.
		stable.close()
	}()

	// If Shutdown().Error() returns while the heartbeat is still blocked, the
	// store gets closed underneath it. Only release the heartbeat after we know
	// which happened, so the interleaving is decided by ordering, not timing.
	select {
	case <-shutdownReturned:
		// Shutdown claimed completion with a heartbeat still in flight.
	case <-time.After(2 * time.Second):
		// Shutdown is correctly waiting for the in-flight heartbeat.
	}
	close(stable.release)

	select {
	case <-hbDone:
	case <-time.After(10 * time.Second):
		t.Fatal("heartbeat handler never returned")
	}
	select {
	case <-shutdownReturned:
	case <-time.After(10 * time.Second):
		t.Fatal("Shutdown().Error() never returned")
	}

	if stable.sawWriteAfterClose() {
		t.Fatal("heartbeat wrote to durable storage after Shutdown().Error() returned " +
			"and the store was closed (issue #717)")
	}
}
