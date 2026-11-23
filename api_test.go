// Copyright (c) 2026 Manas Choksi
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"testing"
	"time"
)

// TestRaft_RestoreCommittedLogs_QueueCapacity makes sure NewRaft can start a
// node whose committed-log replay produces more FSM batches than the
// fsmMutateCh queue can hold. restoreFromCommittedLogs used to run before
// runFSM, the only consumer of that queue, was started, so a replay larger
// than the queue's capacity blocked the constructor forever.
//
// The node boots, commits numCommands entries one FSM batch each
// (MaxAppendEntries=1) and then restarts with RestoreCommittedLogs enabled,
// which has to push every persisted entry through the queue again.
// See hashicorp/raft#729.
func TestRaft_RestoreCommittedLogs_QueueCapacity(t *testing.T) {
	conf := inmemConfig(t)
	conf.LocalID = "node"
	conf.RestoreCommittedLogs = true
	conf.MaxAppendEntries = 1
	conf.SnapshotThreshold = 100000

	logs := NewInmemCommitTrackingStore()
	stable := NewInmemStore()
	snaps := NewInmemSnapshotStore()
	_, transport := NewInmemTransport("node")
	fsm := &MockFSM{}

	configuration := Configuration{Servers: []Server{
		{ID: "node", Address: "node", Suffrage: Voter},
	}}

	if err := BootstrapCluster(conf, logs, stable, snaps, transport, configuration); err != nil {
		t.Fatalf("BootstrapCluster: %v", err)
	}

	// With MaxAppendEntries=1 each committed entry becomes its own FSM
	// batch, so more than 128 committed entries are enough to overflow
	// the queue during the restore replay.
	const numCommands = 130

	node, err := NewRaft(conf, fsm, logs, stable, snaps, transport)
	if err != nil {
		t.Fatalf("NewRaft: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for node.State() != Leader && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if node.State() != Leader {
		node.Shutdown().Error()
		t.Fatal("node did not become leader")
	}

	for i := 0; i < numCommands; i++ {
		if applyErr := node.Apply([]byte("command"), time.Second).Error(); applyErr != nil {
			node.Shutdown().Error()
			t.Fatalf("Apply %d: %v", i, applyErr)
		}
	}

	if err = node.Shutdown().Error(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	commitIndex, err := logs.GetCommitIndex()
	if err != nil {
		t.Fatalf("GetCommitIndex: %v", err)
	}
	if commitIndex == 0 {
		t.Fatal("no commit index was persisted")
	}

	fsm.Lock()
	appliedBefore := len(fsm.logs)
	fsm.Unlock()

	// A shut down node cannot keep using its transport, so hand the
	// restarted node a fresh one.
	_, restartTransport := NewInmemTransport("node")
	restarted, err := NewRaft(conf, fsm, logs, stable, snaps, restartTransport)
	if err != nil {
		t.Fatalf("NewRaft on restart: %v", err)
	}
	defer func() { _ = restarted.Shutdown().Error() }()

	if lastApplied := restarted.getLastApplied(); lastApplied != commitIndex {
		t.Errorf("lastApplied after restart = %d, want %d", lastApplied, commitIndex)
	}

	// The restore replays every entry up to the commit index the store
	// persisted, on top of the state the live node already applied, so the
	// FSM has to see exactly those command entries drain through, not just
	// pile up in the queue. The bootstrap configuration entry and the
	// election no-op don't reach the FSM.
	wantApplied := appliedBefore
	for index := uint64(1); index <= commitIndex; index++ {
		var entry Log
		if err = logs.GetLog(index, &entry); err != nil {
			t.Fatalf("GetLog %d: %v", index, err)
		}
		if entry.Type == LogCommand {
			wantApplied++
		}
	}
	deadline = time.Now().Add(10 * time.Second)
	for {
		fsm.Lock()
		applied := len(fsm.logs)
		fsm.Unlock()
		if applied >= wantApplied {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("FSM applied %d logs after restart, want %d", applied, wantApplied)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
