// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// blockingLogStore wraps a LogStore and blocks GetLog calls on demand,
// simulating disk IO stalls.
type blockingLogStore struct {
	LogStore
	mu       sync.Mutex
	blocked  atomic.Bool
	unblockC chan struct{}
}

func newBlockingLogStore(inner LogStore) *blockingLogStore {
	return &blockingLogStore{
		LogStore: inner,
		unblockC: make(chan struct{}),
	}
}

func (b *blockingLogStore) block() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.blocked.Store(true)
	b.unblockC = make(chan struct{})
}

func (b *blockingLogStore) unblock() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.blocked.Load() {
		b.blocked.Store(false)
		close(b.unblockC)
	}
}

func (b *blockingLogStore) GetLog(index uint64, log *Log) error {
	if b.blocked.Load() {
		b.mu.Lock()
		ch := b.unblockC
		b.mu.Unlock()
		<-ch
	}
	return b.LogStore.GetLog(index, log)
}

// TestRaft_HeartbeatTermCheck verifies that heartbeat() checks resp.Term and
// steps down when a follower responds with a higher term.
//
// When disk IO blocks the replicate() goroutine, only heartbeat() continues
// sending RPCs. If heartbeat() does not check resp.Term, a stale leader can
// maintain its lease indefinitely via "phantom contacts" — setLastContact()
// called for followers that have rejected the heartbeat due to a higher term.
//
// Setup:
//  1. 3-node cluster with stable leader L at term T
//  2. Block L's disk IO so replicate() goroutines freeze on GetLog
//  3. Disconnect L from F2 so L can only reach F1
//  4. Restart F1 with a higher persisted term (T+5)
//  5. L's heartbeat sends term T to F1 at term T+5 — F1 rejects
//  6. Expect: L should step down (not maintain lease via phantom contact)
func TestRaft_HeartbeatTermCheck(t *testing.T) {
	conf := inmemConfig(t)

	stores := make([]*InmemStore, 3)
	blockStores := make([]*blockingLogStore, 3)
	fsms := make([]FSM, 3)
	snapStores := make([]*FileSnapshotStore, 3)
	snapDirs := make([]string, 3)
	transports := make([]*InmemTransport, 3)
	addrs := make([]ServerAddress, 3)

	var configuration Configuration
	for i := 0; i < 3; i++ {
		stores[i] = NewInmemStore()
		blockStores[i] = newBlockingLogStore(stores[i])
		fsms[i] = &MockFSM{}
		dir, snap := FileSnapTest(t)
		snapDirs[i] = dir
		snapStores[i] = snap
		addr, tr := NewInmemTransport("")
		addrs[i] = addr
		transports[i] = tr

		localID := ServerID(fmt.Sprintf("server-%s", addr))
		configuration.Servers = append(configuration.Servers, Server{
			Suffrage: Voter,
			ID:       localID,
			Address:  addr,
		})
	}

	for i := 0; i < 3; i++ {
		for j := 0; j < 3; j++ {
			if i != j {
				transports[i].Connect(addrs[j], transports[j])
			}
		}
	}

	rafts := make([]*Raft, 3)
	for i := 0; i < 3; i++ {
		peerConf := *conf
		peerConf.LocalID = configuration.Servers[i].ID
		peerConf.Logger = newTestLoggerWithPrefix(t, string(configuration.Servers[i].ID))

		err := BootstrapCluster(&peerConf, blockStores[i], stores[i], snapStores[i], transports[i], configuration)
		if err != nil {
			t.Fatalf("BootstrapCluster failed: %v", err)
		}

		raft, err := NewRaft(&peerConf, fsms[i], blockStores[i], stores[i], snapStores[i], transports[i])
		if err != nil {
			t.Fatalf("NewRaft failed: %v", err)
		}
		rafts[i] = raft
	}

	defer func() {
		for _, bs := range blockStores {
			bs.unblock()
		}
		for _, r := range rafts {
			if r != nil {
				f := r.Shutdown()
				f.Error()
			}
		}
		for _, d := range snapDirs {
			os.RemoveAll(d)
		}
	}()

	// Wait for leader
	var leaderRaft *Raft
	var leaderI int
	deadline := time.After(10 * time.Second)
	for leaderRaft == nil {
		select {
		case <-deadline:
			t.Fatalf("timeout waiting for leader")
		case <-time.After(10 * time.Millisecond):
		}
		for i, r := range rafts {
			if r.State() == Leader {
				leaderRaft = r
				leaderI = i
				break
			}
		}
	}

	time.Sleep(3 * conf.HeartbeatTimeout)
	if leaderRaft.State() != Leader {
		t.Fatalf("leader lost leadership during stabilization")
	}

	f1I := (leaderI + 1) % 3
	f2I := (leaderI + 2) % 3

	// Apply data to ensure logs are replicated
	applyFuture := leaderRaft.Apply([]byte("test"), time.Second)
	if err := applyFuture.Error(); err != nil {
		t.Fatalf("failed to apply: %v", err)
	}
	time.Sleep(3 * conf.HeartbeatTimeout)

	leaderTerm := leaderRaft.getCurrentTerm()

	// Block leader's disk IO — replicate() goroutines freeze on GetLog,
	// only heartbeat() continues.
	blockStores[leaderI].block()
	time.Sleep(3 * conf.CommitTimeout)

	// Disconnect L↔F2. L can only reach F1 via heartbeat now.
	transports[leaderI].Disconnect(addrs[f2I])
	transports[f2I].Disconnect(addrs[leaderI])

	time.Sleep(3 * conf.LeaderLeaseTimeout)
	if leaderRaft.State() != Leader {
		t.Fatalf("expected leader to remain leader with heartbeat to F1, got %v", leaderRaft.State())
	}

	// Shutdown F1, bump its persisted term to simulate F1 having participated
	// in a higher-term election, then restart it.
	blockStores[f1I].unblock()
	f1Shutdown := rafts[f1I].Shutdown()
	if err := f1Shutdown.Error(); err != nil {
		t.Fatalf("F1 shutdown failed: %v", err)
	}

	bumpedTerm := leaderTerm + 5
	if err := stores[f1I].SetUint64(keyCurrentTerm, bumpedTerm); err != nil {
		t.Fatalf("failed to set bumped term: %v", err)
	}

	// Restart F1 with new transport at the same address, connected to L only.
	// High election timeout prevents F1 from starting its own elections.
	_, newF1Trans := NewInmemTransport(addrs[f1I])
	newF1Trans.Connect(addrs[leaderI], transports[leaderI])
	transports[leaderI].Connect(addrs[f1I], newF1Trans)

	newF1Conf := *conf
	newF1Conf.LocalID = configuration.Servers[f1I].ID
	newF1Conf.Logger = newTestLoggerWithPrefix(t, string(configuration.Servers[f1I].ID))
	newF1Conf.HeartbeatTimeout = 10 * time.Minute
	newF1Conf.ElectionTimeout = 10 * time.Minute
	newF1Conf.LeaderLeaseTimeout = 10 * time.Minute

	newF1Snap, err := NewFileSnapshotStoreWithLogger(snapDirs[f1I], 3, newTestLogger(t))
	if err != nil {
		t.Fatalf("failed to create snapshot store: %v", err)
	}
	newF1Snap.noSync = true

	newF1Raft, err := NewRaft(&newF1Conf, &MockFSM{}, stores[f1I], stores[f1I], newF1Snap, newF1Trans)
	if err != nil {
		t.Fatalf("NewRaft for restarted F1 failed: %v", err)
	}
	rafts[f1I] = newF1Raft

	if f1Term := newF1Raft.getCurrentTerm(); f1Term != bumpedTerm {
		t.Fatalf("F1 should have term %d, got %d", bumpedTerm, f1Term)
	}

	// L's heartbeat now sends term T to F1 at term T+5. F1 rejects.
	// If heartbeat() checks resp.Term, L should step down within a few
	// heartbeat cycles. We give it 10x LeaderLeaseTimeout.
	steppedDown := false
	checkDeadline := time.After(10 * conf.LeaderLeaseTimeout)
	for !steppedDown {
		select {
		case <-checkDeadline:
			goto CHECK
		case <-time.After(10 * time.Millisecond):
		}
		if leaderRaft.State() != Leader {
			steppedDown = true
		}
	}
CHECK:
	if !steppedDown {
		t.Fatalf("leader at term %d should have stepped down after heartbeat to F1 at term %d, but remained Leader",
			leaderRaft.getCurrentTerm(), newF1Raft.getCurrentTerm())
	}
}
