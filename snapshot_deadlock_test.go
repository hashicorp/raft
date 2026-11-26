// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"io"
	"sync"
	"testing"
	"time"
)

// lockingFSM simulates an FSM whose Snapshot takes a read lock released in
// FSMSnapshot.Release, while Apply takes a write lock.
type lockingFSM struct {
	mu   sync.RWMutex
	logs [][]byte
}

func (f *lockingFSM) Apply(l *Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logs = append(f.logs, l.Data)
	return nil
}

func (f *lockingFSM) Snapshot() (FSMSnapshot, error) {
	f.mu.RLock()
	time.Sleep(500 * time.Millisecond)
	return &lockedSnap{f: f}, nil
}

func (f *lockingFSM) Restore(rc io.ReadCloser) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return rc.Close()
}

type lockedSnap struct {
	f    *lockingFSM
	once sync.Once
}

func (s *lockedSnap) Persist(sink SnapshotSink) error { return sink.Close() }
func (s *lockedSnap) Release()                        { s.once.Do(s.f.mu.RUnlock) }

func TestRaft_SnapshotApplyDeadlockE2E(t *testing.T) {
	conf := inmemConfig(t)
	conf.SnapshotInterval = time.Hour
	conf.SnapshotThreshold = 1 << 62
	conf.MaxAppendEntries = 1
	c := MakeClusterCustom(t, &MakeClusterOpts{
		Peers:     1,
		Bootstrap: true,
		Conf:      conf,
		MakeFSMFunc: func() FSM {
			return &lockingFSM{}
		},
	})
	defer c.Close()

	leader := c.Leader()
	for i := 0; i < 5; i++ {
		if err := leader.Apply([]byte{byte(i)}, 0).Error(); err != nil {
			t.Fatal(err)
		}
	}

	snapDone := make(chan error, 1)
	go func() {
		snapDone <- leader.Snapshot().Error()
	}()

	time.Sleep(100 * time.Millisecond)

	applyCount := cap(leader.fsmMutateCh) + 64
	applyErrs := make(chan error, applyCount)
	for i := 0; i < applyCount; i++ {
		go func(v byte) {
			applyErrs <- leader.Apply([]byte{v}, 0).Error()
		}(byte(i))
	}

	select {
	case err := <-snapDone:
		if err != nil {
			t.Fatalf("Snapshot() failed: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("deadlock: Snapshot() did not return")
	}

	for i := 0; i < applyCount; i++ {
		select {
		case err := <-applyErrs:
			if err != nil {
				t.Fatalf("Apply() failed: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for Apply() futures after snapshot")
		}
	}
}

// blockingApplyFSM blocks in Apply once signaled until gate is closed.
type blockingApplyFSM struct {
	blockOnce sync.Once
	inApply   chan struct{}
	gate      chan struct{}
}

func (f *blockingApplyFSM) Apply(l *Log) interface{} {
	f.blockOnce.Do(func() {
		close(f.inApply)
		<-f.gate
	})
	return nil
}

func (f *blockingApplyFSM) Snapshot() (FSMSnapshot, error) { return &MockSnapshot{}, nil }
func (f *blockingApplyFSM) Restore(rc io.ReadCloser) error { return rc.Close() }

func TestRaft_SnapshotApplyDeadlockDeterministic(t *testing.T) {
	conf := inmemConfig(t)
	conf.SnapshotInterval = time.Hour
	conf.SnapshotThreshold = 1 << 62
	conf.MaxAppendEntries = 1

	fsm := &blockingApplyFSM{
		inApply: make(chan struct{}),
		gate:    make(chan struct{}),
	}
	c := MakeClusterCustom(t, &MakeClusterOpts{
		Peers:     1,
		Bootstrap: true,
		Conf:      conf,
		MakeFSMFunc: func() FSM {
			return fsm
		},
	})
	defer func() {
		select {
		case <-fsm.gate:
		default:
			close(fsm.gate)
		}
		c.Close()
	}()

	leader := c.Leader()

	// Trigger the first Apply so runFSM enters Apply() and blocks on fsm.gate.
	firstFuture := leader.Apply([]byte("first"), 0)
	select {
	case <-fsm.inApply:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for FSM Apply to start")
	}

	// Fill fsmMutateCh plus extra entries so the main loop blocks in applyBatch.
	// Send Apply calls concurrently because applyCh is unbuffered and stops
	// draining once leaderLoop blocks inside applyBatch.
	fillCount := cap(leader.fsmMutateCh) + 16
	applyFutures := make(chan ApplyFuture, fillCount)
	for i := 0; i < fillCount; i++ {
		go func(v byte) {
			applyFutures <- leader.Apply([]byte{v}, 0)
		}(byte(i))
	}

	// Wait until the main loop has saturated fsmMutateCh.
	deadline := time.Now().Add(5 * time.Second)
	for len(leader.fsmMutateCh) < cap(leader.fsmMutateCh) {
		if time.Now().After(deadline) {
			t.Fatalf("fsmMutateCh did not fill in time: len=%d cap=%d", len(leader.fsmMutateCh), cap(leader.fsmMutateCh))
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Give leaderLoop a moment to block inside applyBatch on the overflow send.
	time.Sleep(50 * time.Millisecond)

	// Simulate takeSnapshot querying configurationsCh while applyBatch is blocked.
	configReq := &configurationsFuture{}
	configReq.ShutdownCh = leader.shutdownCh
	configReq.init()
	leader.configurationsCh <- configReq

	errCh := make(chan error, 1)
	go func() {
		errCh <- configReq.Error()
	}()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("configurationsFuture returned error: %v", err)
		}
		if configReq.configurations.committedIndex == 0 {
			t.Fatalf("expected non-zero committed configuration index")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("deadlock: configuration request not served while applyBatch is blocked on fsmMutateCh")
	}

	// Unblock FSM and verify all pending applies complete cleanly.
	close(fsm.gate)
	if err := firstFuture.Error(); err != nil {
		t.Fatalf("first apply failed: %v", err)
	}
	for i := 0; i < fillCount; i++ {
		select {
		case f := <-applyFutures:
			if err := f.Error(); err != nil {
				t.Fatalf("apply %d failed: %v", i, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for apply %d", i)
		}
	}
}

func TestRaft_SnapshotRestoreDeadlockDeterministic(t *testing.T) {
	conf := inmemConfig(t)
	conf.SnapshotInterval = time.Hour
	conf.SnapshotThreshold = 1 << 62

	fsm := &lockingFSM{}
	c := MakeClusterCustom(t, &MakeClusterOpts{
		Peers:     1,
		Bootstrap: true,
		Conf:      conf,
		MakeFSMFunc: func() FSM {
			return fsm
		},
	})
	defer c.Close()

	leader := c.Leader()
	if err := leader.Apply([]byte("seed"), 0).Error(); err != nil {
		t.Fatal(err)
	}

	// Create a valid snapshot first so we can restore from it.
	snapFuture := leader.Snapshot()
	if err := snapFuture.Error(); err != nil {
		t.Fatalf("initial snapshot failed: %v", err)
	}
	meta, rc, err := snapFuture.Open()
	if err != nil {
		t.Fatalf("open snapshot failed: %v", err)
	}
	defer rc.Close()

	// Acquire the FSM read lock (simulating takeSnapshot between Snapshot() and
	// Release()) so Restore() inside runFSM blocks until we release it.
	fsm.mu.RLock()
	restoreDone := make(chan error, 1)
	go func() {
		restoreDone <- leader.Restore(meta, rc, 5*time.Second)
	}()

	// Wait briefly for leaderLoop to enter restoreUserSnapshot and block in awaitFSMFuture.
	time.Sleep(100 * time.Millisecond)

	configReq := &configurationsFuture{}
	configReq.ShutdownCh = leader.shutdownCh
	configReq.init()
	leader.configurationsCh <- configReq

	configErr := make(chan error, 1)
	go func() {
		configErr <- configReq.Error()
	}()

	select {
	case err := <-configErr:
		if err != nil {
			fsm.mu.RUnlock()
			t.Fatalf("configurationsFuture returned error during restore: %v", err)
		}
		if configReq.configurations.committedIndex == 0 {
			fsm.mu.RUnlock()
			t.Fatalf("expected non-zero committed configuration index")
		}
	case <-time.After(3 * time.Second):
		fsm.mu.RUnlock()
		t.Fatal("deadlock: configuration request not served while awaiting FSM restore")
	}

	fsm.mu.RUnlock()
	select {
	case err := <-restoreDone:
		if err != nil {
			t.Fatalf("Restore failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Restore did not complete after releasing snapshot lock")
	}
}
