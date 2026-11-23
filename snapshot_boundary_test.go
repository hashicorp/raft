package raft

import "testing"

// Regression test for #741: AppendEntries rejects a predecessor that exactly
// matches the last snapshot's index and term when the log entry at that index
// has been compacted away and a later log entry exists.
func TestSnapshotBoundaryRepro(t *testing.T) {
	for _, keepBoundaryLog := range []bool{true, false} {
		name := "snapshot_boundary_log_missing"
		if keepBoundaryLog {
			name = "control_boundary_log_present"
		}

		t.Run(name, func(t *testing.T) {
			store := NewInmemStore()

			if keepBoundaryLog {
				if err := store.StoreLog(&Log{Index: 89, Term: 3, Type: LogNoop}); err != nil {
					t.Fatal(err)
				}
			}

			if err := store.StoreLog(&Log{Index: 90, Term: 3, Type: LogNoop}); err != nil {
				t.Fatal(err)
			}

			_, transport := NewInmemTransport("")
			r := &Raft{logs: store, trans: transport, logger: newTestLogger(t)}
			r.conf.Store(*DefaultConfig())
			r.raftState.setCurrentTerm(3)
			r.setLastLog(90, 3)
			r.setLastSnapshot(89, 3)

			ch := make(chan RPCResponse, 1)
			r.appendEntries(RPC{RespChan: ch}, &AppendEntriesRequest{
				Term: 3, PrevLogEntry: 89, PrevLogTerm: 3,
			})

			response := <-ch
			if response.Error != nil {
				t.Fatal(response.Error)
			}

			got := response.Response.(*AppendEntriesResponse)
			if !got.Success {
				t.Fatalf("valid snapshot-boundary predecessor rejected: "+
					"snapshot=89/3 lastLog=90/3 keepBoundaryLog=%v response=%+v",
					keepBoundaryLog, got)
			}
		})
	}
}

func TestCompactionRemovesSnapshotBoundaryEntry(t *testing.T) {
	store := NewInmemStore()
	for i := uint64(1); i <= 90; i++ {
		if err := store.StoreLog(&Log{Index: i, Term: 3, Type: LogNoop}); err != nil {
			t.Fatal(err)
		}
	}

	r := &Raft{logs: store, logger: newTestLogger(t)}
	r.conf.Store(*DefaultConfig())
	r.setLastLog(90, 3)

	if err := r.compactLogsWithTrailing(89, 90, 1); err != nil {
		t.Fatal(err)
	}

	var l Log
	if err := store.GetLog(89, &l); err == nil {
		t.Fatal("expected index 89 to be compacted away")
	}
	if err := store.GetLog(90, &l); err != nil {
		t.Fatalf("expected index 90 to remain: %v", err)
	}
}

// A predecessor at the snapshot boundary with a non-matching term must still
// be rejected: the snapshot term is authoritative for exactly that index.
func TestSnapshotBoundaryPredecessorTermMismatch(t *testing.T) {
	store := NewInmemStore()
	if err := store.StoreLog(&Log{Index: 90, Term: 3, Type: LogNoop}); err != nil {
		t.Fatal(err)
	}

	_, transport := NewInmemTransport("")
	r := &Raft{logs: store, trans: transport, logger: newTestLogger(t)}
	r.conf.Store(*DefaultConfig())
	r.raftState.setCurrentTerm(3)
	r.setLastLog(90, 3)
	r.setLastSnapshot(89, 3)

	ch := make(chan RPCResponse, 1)
	r.appendEntries(RPC{RespChan: ch}, &AppendEntriesRequest{
		Term: 3, PrevLogEntry: 89, PrevLogTerm: 4,
	})

	response := <-ch
	if response.Error != nil {
		t.Fatal(response.Error)
	}

	got := response.Response.(*AppendEntriesResponse)
	if got.Success {
		t.Fatalf("snapshot-boundary predecessor with mismatched term accepted: response=%+v", got)
	}
}
