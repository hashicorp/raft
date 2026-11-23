// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// installSnapshot sends an InstallSnapshot for index and term from the fake
// leader and requires it to succeed.
func (f *testFollower) installSnapshot(t *testing.T, term, index, snapTerm uint64) {
	t.Helper()
	resp, err := f.sendSnapshot(term, index, snapTerm)
	require.NoError(t, err)
	require.True(t, resp.Success, "install rejected: %+v", resp)
}

// sendSnapshot sends an InstallSnapshot for index and term from the fake
// leader, carrying the configuration newTestFollower starts with.
func (f *testFollower) sendSnapshot(term, index, snapTerm uint64) (InstallSnapshotResponse, error) {
	data := []byte("snapshot")
	req := InstallSnapshotRequest{
		RPCHeader:          f.header(),
		SnapshotVersion:    SnapshotVersionMax,
		Term:               term,
		LastLogIndex:       index,
		LastLogTerm:        snapTerm,
		Configuration:      EncodeConfiguration(testFollowerConf),
		ConfigurationIndex: 1,
		Size:               int64(len(data)),
	}
	var resp InstallSnapshotResponse
	err := f.leader.InstallSnapshot("f", "f", &req, &resp, bytes.NewReader(data))
	return resp, err
}

// storedTerms returns the term of every entry in the follower's log store.
func (f *testFollower) storedTerms(t *testing.T) map[uint64]uint64 {
	t.Helper()
	first, err := f.store.FirstIndex()
	require.NoError(t, err)
	last, err := f.store.LastIndex()
	require.NoError(t, err)
	terms := map[uint64]uint64{}
	for i := first; i <= last && last > 0; i++ {
		var l Log
		require.NoError(t, f.store.GetLog(i, &l))
		terms[i] = l.Term
	}
	return terms
}

// A follower holding divergent, never-committed entries must not keep them
// when it installs a snapshot: below the snapshot index they could later be
// replicated by this server as leader (hashicorp/raft#721), and at the
// snapshot index one makes every following AppendEntries fail its consistency
// check, so the leader sends the snapshot again and again (#697).
func TestRaft_InstallSnapshot_DiscardsDivergentLogs(t *testing.T) {
	cases := []struct {
		name      string
		lastStale uint64 // the follower's stale term-2 entries are 3..lastStale
		snapIndex uint64
	}{
		{"stale entries at and beyond the snapshot index", 5, 4},
		{"log ends below the snapshot index", 3, 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			entries := []*Log{cmd(2, 1, "committed")}
			for i := uint64(3); i <= tc.lastStale; i++ {
				entries = append(entries, cmd(i, 2, fmt.Sprintf("stale-%d", i)))
			}
			f := newTestFollower(t, 2, entries...)

			// Leader l (term 3) commits 1-2 on f, then sends a snapshot of
			// its own log, whose entries 3 onwards are from term 3.
			f.appendEntries(t, AppendEntriesRequest{Term: 3, PrevLogEntry: 2, PrevLogTerm: 1, LeaderCommitIndex: 2})
			f.requireCommitted(t, 2, 2)
			f.installSnapshot(t, 3, tc.snapIndex, 3)

			// Only the committed entries 1-2 remain.
			require.Equal(t, map[uint64]uint64{1: 1, 2: 1}, f.storedTerms(t), "uncommitted entries survived a conflicting snapshot")
			require.Equal(t, tc.snapIndex, f.r.LastIndex())

			// The leader's next append, right after the snapshot, succeeds.
			next := tc.snapIndex + 1
			f.appendEntries(t, AppendEntriesRequest{Term: 3, PrevLogEntry: tc.snapIndex, PrevLogTerm: 3,
				LeaderCommitIndex: next, Entries: []*Log{cmd(next, 3, "leader")}})
			f.requireCommitted(t, next, next)
			f.fsm.waitApplied(t, next)
			for _, l := range f.fsm.logs() {
				require.NotContains(t, string(l.Data), "stale", "applied a never-committed entry")
			}
		})
	}
}

// A log that agrees with the snapshot keeps its entries beyond it.
func TestRaft_InstallSnapshot_KeepsMatchingLogs(t *testing.T) {
	t.Run("entry at the snapshot index has its term", func(t *testing.T) {
		f := newTestFollower(t, 1, cmd(2, 1, "a"), cmd(3, 1, "b"), cmd(4, 1, "c"), cmd(5, 1, "d"), cmd(6, 1, "e"))
		f.installSnapshot(t, 1, 4, 1)

		terms := f.storedTerms(t)
		require.Equal(t, uint64(1), terms[5])
		require.Equal(t, uint64(1), terms[6])
		require.Equal(t, uint64(6), f.r.LastIndex())
		f.appendEntries(t, AppendEntriesRequest{Term: 1, PrevLogEntry: 6, PrevLogTerm: 1, LeaderCommitIndex: 6})
		f.requireCommitted(t, 6, 6)
	})

	// The snapshot index is committed here but its entry was compacted away.
	// The entries above it may be committed through this server, so they
	// must stay.
	t.Run("snapshot index already committed", func(t *testing.T) {
		f := newTestFollower(t, 1, cmd(2, 1, "a"), cmd(3, 1, "b"), cmd(4, 1, "c"), cmd(5, 1, "d"), cmd(6, 1, "e"))
		f.appendEntries(t, AppendEntriesRequest{Term: 1, PrevLogEntry: 6, PrevLogTerm: 1, LeaderCommitIndex: 6})
		f.requireCommitted(t, 6, 6)
		require.NoError(t, f.store.DeleteRange(1, 4))
		f.installSnapshot(t, 1, 4, 1)

		terms := f.storedTerms(t)
		require.Equal(t, uint64(1), terms[5])
		require.Equal(t, uint64(1), terms[6])
		require.Equal(t, uint64(6), f.r.LastIndex())
	})
}

// histFSM records every Apply, and is never reset by Restore.
type histFSM struct {
	*MockFSM
	mu   sync.Mutex
	hist []string
}

func (h *histFSM) Apply(l *Log) interface{} {
	h.mu.Lock()
	h.hist = append(h.hist, fmt.Sprintf("%d/t%d/%s", l.Index, l.Term, l.Data))
	h.mu.Unlock()
	return h.MockFSM.Apply(l)
}

func (h *histFSM) Underlying() FSM { return h.MockFSM }

func (h *histFSM) history() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string{}, h.hist...)
}

func waitForCondition(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

// End to end: a follower with divergent entries installs a snapshot, becomes
// leader, and a new server that joins must not apply the divergent entries.
// Adapted from the reproduction in hashicorp/raft#721 by Shashank Mohan Jain.
func TestRaft_InstallSnapshot_DivergentEntriesNotReplicated(t *testing.T) {
	conf := inmemConfig(t)
	conf.TrailingLogs = 500 // >= the divergent follower's last index
	conf.SnapshotThreshold = 1 << 40
	conf.SnapshotInterval = time.Hour
	mk := func() FSM { return &histFSM{MockFSM: &MockFSM{}} }
	c := MakeClusterCustom(t, &MakeClusterOpts{Peers: 5, Bootstrap: true, Conf: conf, MakeFSMFunc: mk})
	defer c.Close()

	// 1. A committed prefix.
	A := c.Leader()
	for i := 0; i < 10; i++ {
		require.NoError(t, A.Apply([]byte(fmt.Sprintf("good-%d", i)), time.Second).Error())
	}
	waitForCondition(t, "prefix replicated", 5*time.Second, func() bool {
		for _, r := range c.rafts {
			if r.AppliedIndex() < A.getLastIndex() {
				return false
			}
		}
		return true
	})
	F := c.Followers()[0]

	// 2. Partition {A, F} from the rest. A, still leader until its lease
	// runs out, appends entries that reach only F and never commit.
	c.Partition([]ServerAddress{A.localAddr, F.localAddr})
	for i := 0; i < 200; i++ {
		A.Apply([]byte(fmt.Sprintf("STALE-%d", i)), 50*time.Millisecond)
	}
	var fLast uint64
	waitForCondition(t, "F to receive stale entries", 3*time.Second, func() bool {
		l := F.getLastIndex()
		stable := l == fLast && l > 11
		fLast = l
		return stable && l >= 50
	})

	// 3. The majority elects a new leader L, which commits new entries and
	// takes a snapshot.
	var L *Raft
	waitForCondition(t, "new leader on majority side", 5*time.Second, func() bool {
		for _, r := range c.rafts {
			if r != A && r != F && r.State() == Leader {
				L = r
				return true
			}
		}
		return false
	})
	var fut Future
	for i := 0; i < 1300; i++ {
		fut = L.Apply([]byte(fmt.Sprintf("new-%d", i)), 5*time.Second)
	}
	require.NoError(t, fut.Error())
	require.NoError(t, L.Snapshot().Error())
	snapIdx, _ := L.getLastSnapshot()

	// 4. A goes away; F rejoins and catches up through the snapshot.
	require.NoError(t, A.Shutdown().Error())
	c.FullyConnect()
	c.Disconnect(A.localAddr)
	waitForCondition(t, "F to catch up via snapshot", 30*time.Second, func() bool {
		s, _ := F.getLastSnapshot()
		return s >= snapIdx && F.AppliedIndex() >= L.getLastIndex()
	})

	// 5. F becomes leader, then a new server joins and replicates from it.
	require.NoError(t, L.LeadershipTransferToServer(F.localID, F.localAddr).Error())
	waitForCondition(t, "F leader", 5*time.Second, func() bool { return F.State() == Leader })
	c1, err := makeCluster(t, &MakeClusterOpts{Peers: 1, Bootstrap: false, Conf: conf, MakeFSMFunc: mk})
	require.NoError(t, err)
	c.Merge(c1)
	c.FullyConnect()
	c.Disconnect(A.localAddr)
	N := c1.rafts[0]
	require.NoError(t, F.AddNonvoter(N.localID, N.localAddr, 0, 5*time.Second).Error())
	waitForCondition(t, "N to catch up", 20*time.Second, func() bool { return N.AppliedIndex() >= F.getCommitIndex()-1 })

	// 6. N must not have applied any entry that was never committed.
	stale, first := 0, ""
	for _, e := range c.fsms[len(c.fsms)-1].(*histFSM).history() {
		if strings.Contains(e, "STALE-") {
			if stale == 0 {
				first = e
			}
			stale++
		}
	}
	require.Zero(t, stale, "N applied never-committed entries; the first: %s", first)
}
