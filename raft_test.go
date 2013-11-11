package raft

import (
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
