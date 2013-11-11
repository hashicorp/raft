package raft

import (
	"net"
	"os"
	"testing"
	"time"
)

// MockFSM is an implementation of the FSM interface, and just stores
// the logs sequentially
type MockFSM struct {
	logs [][]byte
}

func (m *MockFSM) Apply(log []byte) {
	m.logs = append(m.logs, log)
}

// Return configurations optimized for in-memory
func inmemConfig() *Config {
	return &Config{
		HeartbeatTimeout: 5 * time.Millisecond,
		ElectionTimeout:  10 * time.Millisecond,
		CommitTimeout:    time.Millisecond,
		MaxAppendEntries: 16,
	}
}

func TestRaft_StartStop(t *testing.T) {
	dir, store := LevelDBTestStore(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	_, trans := NewInmemTransport()
	fsm := &MockFSM{}
	conf := DefaultConfig()

	raft, err := NewRaft(conf, fsm, store, store, nil, trans)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer raft.Shutdown()
}

func TestRaft_SingleNode(t *testing.T) {
	dir, store := LevelDBTestStore(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	_, trans := NewInmemTransport()
	fsm := &MockFSM{}
	conf := inmemConfig()

	raft, err := NewRaft(conf, fsm, store, store, nil, trans)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer raft.Shutdown()

	time.Sleep(15 * time.Millisecond)

	// Should be leader
	if s := raft.State(); s != Leader {
		t.Fatalf("expected leader: %v", s)
	}

	// Should be able to apply
	future := raft.Apply([]byte("test"), time.Millisecond)
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Check that it is applied to the FSM
	if len(fsm.logs) != 1 {
		t.Fatalf("did not apply to FSM!")
	}
}

type cluster struct {
	dirs   []string
	stores []*LevelDBStore
	fsms   []*MockFSM
	trans  []*InmemTransport
	rafts  []*Raft
}

func (c *cluster) Close() {
	for _, r := range c.rafts {
		r.Shutdown()
	}
	for _, s := range c.stores {
		s.Close()
	}
	for _, d := range c.dirs {
		os.RemoveAll(d)
	}
}

func (c *cluster) GetInState(s RaftState) []*Raft {
	in := make([]*Raft, 0, 1)
	for _, r := range c.rafts {
		if r.State() == s {
			in = append(in, r)
		}
	}
	return in
}

func MakeCluster(n int, t *testing.T) *cluster {
	c := &cluster{}
	peers := make([]net.Addr, 0, n)

	// Setup the stores and transports
	for i := 0; i < n; i++ {
		dir, store := LevelDBTestStore(t)
		c.dirs = append(c.dirs, dir)
		c.stores = append(c.stores, store)
		c.fsms = append(c.fsms, &MockFSM{})

		addr, trans := NewInmemTransport()
		c.trans = append(c.trans, trans)
		peers = append(peers, addr)
	}

	// Wire the transports together
	for i, t1 := range c.trans {
		for j, t2 := range c.trans {
			if i != j {
				t1.Connect(t2.LocalAddr(), t2)
				t2.Connect(t1.LocalAddr(), t1)
			}
		}
	}

	// Create all the rafts
	for i := 0; i < n; i++ {
		conf := inmemConfig()
		store := c.stores[i]
		trans := c.trans[i]
		raft, err := NewRaft(conf, c.fsms[i], store, store, peers, trans)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		c.rafts = append(c.rafts, raft)
	}

	return c
}

func TestRaft_TripleNode(t *testing.T) {
	// Make the cluster
	c := MakeCluster(3, t)
	defer c.Close()

	// Wait for elections
	time.Sleep(15 * time.Millisecond)

	// Should be one leader
	leaders := c.GetInState(Leader)
	if len(leaders) != 1 {
		t.Fatalf("expected one leader: %v", leaders)
	}
	leader := leaders[0]

	// Should be able to apply
	future := leader.Apply([]byte("test"), time.Millisecond)
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Wait for replication
	time.Sleep(3 * time.Millisecond)

	// Check that it is applied to the FSM
	for _, fsm := range c.fsms {
		if len(fsm.logs) != 1 {
			t.Fatalf("did not apply to FSM!")
		}
	}
}
