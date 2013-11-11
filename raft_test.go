package raft

import (
	"os"
	"testing"
)

// MockFSM is an implementation of the FSM interface, and just stores
// the logs sequentially
type MockFSM struct {
	logs [][]byte
}

func (m *MockFSM) Apply(log []byte) {
	m.logs = append(m.logs, log)
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
