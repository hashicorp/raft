// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import "testing"

func TestShouldSnapshot(t *testing.T) {
	newRaft := func(snapshotIndex uint64, logs ...*Log) *Raft {
		r := &Raft{logs: NewInmemStore()}
		r.conf.Store(Config{SnapshotThreshold: 8192})
		r.setLastSnapshot(snapshotIndex, 1)
		if len(logs) > 0 {
			if err := r.logs.StoreLogs(logs); err != nil {
				t.Fatalf("failed to store logs: %v", err)
			}
		}
		return r
	}

	t.Run("enough new logs past the snapshot", func(t *testing.T) {
		r := newRaft(10000, &Log{Index: 20000, Term: 1})
		if !r.shouldSnapshot() {
			t.Fatalf("expected a snapshot when the log is well ahead of the last snapshot")
		}
	})

	t.Run("too few new logs past the snapshot", func(t *testing.T) {
		r := newRaft(10000, &Log{Index: 12000, Term: 1})
		if r.shouldSnapshot() {
			t.Fatalf("did not expect a snapshot when fewer than SnapshotThreshold logs are new")
		}
	})

	// A follower that catches up via InstallSnapshot sets its last snapshot
	// index ahead of its (compacted or empty) log store. In that case the last
	// log index is behind the last snapshot index and there is nothing new to
	// snapshot. The delta must not underflow and force a needless snapshot.
	t.Run("last log behind the last snapshot", func(t *testing.T) {
		r := newRaft(10000)
		if r.shouldSnapshot() {
			t.Fatalf("did not expect a snapshot when the last log is behind the last snapshot")
		}
	})
}
