// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// recordingFSM records the index, term and data of every log it applies.
type recordingFSM struct {
	mu      sync.Mutex
	applied []Log
}

func (f *recordingFSM) Apply(l *Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = append(f.applied, Log{Index: l.Index, Term: l.Term, Data: append([]byte(nil), l.Data...)})
	return nil
}

func (f *recordingFSM) Snapshot() (FSMSnapshot, error) { return &MockSnapshot{}, nil }

func (f *recordingFSM) Restore(rc io.ReadCloser) error {
	_, _ = io.Copy(io.Discard, rc)
	return rc.Close()
}

func (f *recordingFSM) logs() []Log {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Log(nil), f.applied...)
}

// waitApplied waits until the FSM has applied the command at index.
func (f *recordingFSM) waitApplied(t *testing.T, index uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, l := range f.logs() {
			if l.Index == index {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("FSM did not apply index %d; applied %v", index, f.logs())
}

// testFollower is a single Raft node "f" whose log is set up by the test,
// and a transport for a fake leader "l" that sends it hand-built RPCs.
type testFollower struct {
	r      *Raft
	store  LogStore
	fsm    *recordingFSM
	leader *InmemTransport
}

// newTestFollower starts node "f" with the given log and current term. The
// configuration has three voters (f, l, g); timeouts are long so that f stays
// a follower for the duration of the test.
func newTestFollower(t *testing.T, term uint64, entries ...*Log) *testFollower {
	t.Helper()
	logs := append([]*Log{{Index: 1, Term: 1, Type: LogConfiguration, Data: EncodeConfiguration(testFollowerConf)}}, entries...)
	return newTestFollowerOn(t, NewInmemStore(), term, logs...)
}

// testFollowerConf is the configuration newTestFollower stores at index 1.
var testFollowerConf = Configuration{Servers: []Server{
	{Suffrage: Voter, ID: "f", Address: "f"},
	{Suffrage: Voter, ID: "l", Address: "l"},
	{Suffrage: Voter, ID: "g", Address: "g"},
}}

// newTestFollowerOn starts node "f" on the given log store holding exactly
// logs, with the given current term.
func newTestFollowerOn(t *testing.T, store LogStore, term uint64, logs ...*Log) *testFollower {
	t.Helper()
	if len(logs) > 0 {
		require.NoError(t, store.StoreLogs(logs))
	}
	stable := NewInmemStore()
	require.NoError(t, stable.SetUint64(keyCurrentTerm, term))

	_, fTrans := NewInmemTransport("f")
	_, lTrans := NewInmemTransport("l")
	lTrans.Connect("f", fTrans)
	fTrans.Connect("l", lTrans)

	cfg := inmemConfig(t)
	cfg.LocalID = "f"
	cfg.HeartbeatTimeout = 10 * time.Second
	cfg.ElectionTimeout = 10 * time.Second
	cfg.LeaderLeaseTimeout = 10 * time.Second
	fsm := &recordingFSM{}
	r, err := NewRaft(cfg, fsm, store, stable, NewInmemSnapshotStore(), fTrans)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Shutdown().Error() })
	return &testFollower{r: r, store: store, fsm: fsm, leader: lTrans}
}

func (f *testFollower) header() RPCHeader {
	return RPCHeader{ProtocolVersion: ProtocolVersionMax, ID: []byte("l"), Addr: f.leader.EncodePeer("l", "l")}
}

// appendEntries sends req from the fake leader and requires it to succeed.
func (f *testFollower) appendEntries(t *testing.T, req AppendEntriesRequest) {
	t.Helper()
	req.RPCHeader = f.header()
	var resp AppendEntriesResponse
	require.NoError(t, f.leader.AppendEntries("f", "f", &req, &resp))
	require.True(t, resp.Success, "append rejected: %+v", resp)
}

// requireCommitted checks the follower's commit and applied indexes. Both are
// set on the main thread before the RPC response is sent, so no wait is needed.
func (f *testFollower) requireCommitted(t *testing.T, commit, applied uint64) {
	t.Helper()
	require.Equal(t, commit, f.r.getCommitIndex(), "commit index")
	require.Equal(t, applied, f.r.getLastApplied(), "applied index")
}

func cmd(index, term uint64, data string) *Log {
	return &Log{Index: index, Term: term, Type: LogCommand, Data: []byte(data)}
}
