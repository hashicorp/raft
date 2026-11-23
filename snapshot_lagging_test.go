// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// boltLikeLogStore is a LogStore with raft-boltdb's semantics: FirstIndex and
// LastIndex come from the keys present (0 when empty), and DeleteRange only
// visits keys present in [min, max]. Unlike InmemStore, it keeps no low and
// high index that a DeleteRange beyond the stored range can corrupt.
type boltLikeLogStore struct {
	mu   sync.Mutex
	logs map[uint64]Log
}

func newBoltLikeLogStore() *boltLikeLogStore {
	return &boltLikeLogStore{logs: map[uint64]Log{}}
}

func (s *boltLikeLogStore) keys() []uint64 {
	keys := make([]uint64, 0, len(s.logs))
	for k := range s.logs {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

func (s *boltLikeLogStore) FirstIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if k := s.keys(); len(k) > 0 {
		return k[0], nil
	}
	return 0, nil
}

func (s *boltLikeLogStore) LastIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if k := s.keys(); len(k) > 0 {
		return k[len(k)-1], nil
	}
	return 0, nil
}

func (s *boltLikeLogStore) GetLog(index uint64, log *Log) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok := s.logs[index]
	if !ok {
		return ErrLogNotFound
	}
	*log = l
	return nil
}

func (s *boltLikeLogStore) StoreLog(log *Log) error { return s.StoreLogs([]*Log{log}) }

func (s *boltLikeLogStore) StoreLogs(logs []*Log) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, l := range logs {
		s.logs[l.Index] = *l
	}
	return nil
}

func (s *boltLikeLogStore) DeleteRange(min, max uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range s.keys() {
		if k >= min && k <= max {
			delete(s.logs, k)
		}
	}
	return nil
}

// flakyDeletes fails the next fails calls to DeleteRange.
type flakyDeletes struct {
	LogStore
	mu    sync.Mutex
	fails int
}

func (s *flakyDeletes) DeleteRange(min, max uint64) error {
	s.mu.Lock()
	if s.fails > 0 {
		s.fails--
		s.mu.Unlock()
		return errors.New("injected DeleteRange failure")
	}
	s.mu.Unlock()
	return s.LogStore.DeleteRange(min, max)
}

var testLogStores = []struct {
	name string
	new  func() LogStore
}{
	{"InmemStore", func() LogStore { return NewInmemStore() }},
	{"bolt-like", func() LogStore { return newBoltLikeLogStore() }},
}

func confLog() *Log {
	return &Log{Index: 1, Term: 1, Type: LogConfiguration, Data: EncodeConfiguration(testFollowerConf)}
}

func (f *testFollower) setTrailingLogs(t *testing.T, n uint64) {
	t.Helper()
	rc := f.r.ReloadableConfig()
	rc.TrailingLogs = n
	require.NoError(t, f.r.ReloadConfig(rc))
}

// stored returns the term of every entry in the follower's log store, which
// may have gaps.
func (f *testFollower) stored(t *testing.T) map[uint64]uint64 {
	t.Helper()
	first, err := f.store.FirstIndex()
	require.NoError(t, err)
	last, err := f.store.LastIndex()
	require.NoError(t, err)
	require.Less(t, last, uint64(1000), "the log store's last index is corrupt")
	terms := map[uint64]uint64{}
	for i := first; i <= last && last > 0; i++ {
		var l Log
		err := f.store.GetLog(i, &l)
		if errors.Is(err, ErrLogNotFound) {
			continue
		}
		require.NoError(t, err)
		terms[i] = l.Term
	}
	return terms
}

// After a snapshot install the follower's log store holds what it should, its
// last log never points past the store's end, and the leader's next append
// succeeds. On InmemStore the lagging, empty-log and divergent TrailingLogs 0
// cases hang on the parent commit: recording the snapshot index as the last log made compaction call
// DeleteRange(0, x) on an empty InmemStore, which wraps its last index to
// MaxUint64, and the next install's DeleteRange(commit+1, MaxUint64) never
// ends.
func TestRaft_InstallSnapshot_LogIndexes(t *testing.T) {
	type install struct{ index, term uint64 }
	stale := func(from, to, term uint64) []*Log {
		var l []*Log
		for i := from; i <= to; i++ {
			l = append(l, cmd(i, term, fmt.Sprintf("stale-%d", i)))
		}
		return l
	}
	committed := func(from, to, term uint64) []*Log {
		var l []*Log
		for i := from; i <= to; i++ {
			l = append(l, cmd(i, term, fmt.Sprintf("c-%d", i)))
		}
		return l
	}
	cases := []struct {
		name         string
		logs         []*Log
		followerTerm uint64
		leaderTerm   uint64
		commit       uint64 // the leader's first append commits up to here
		trailing     uint64
		installs     []install
		wantStored   map[uint64]uint64
		wantLastLog  uint64
		wantLast     uint64 // LastIndex(): max(last log, snapshot)
	}{
		{
			name: "lagging follower, repeated installs, TrailingLogs 0",
			logs: append([]*Log{confLog()}, committed(2, 3, 1)...), followerTerm: 1, leaderTerm: 2, commit: 3,
			trailing: 0, installs: []install{{10, 2}, {20, 2}, {30, 2}},
			wantStored: map[uint64]uint64{}, wantLastLog: 0, wantLast: 30,
		},
		{
			name: "lagging follower, repeated installs, TrailingLogs 2",
			logs: append([]*Log{confLog()}, committed(2, 3, 1)...), followerTerm: 1, leaderTerm: 2, commit: 3,
			trailing: 2, installs: []install{{10, 2}, {20, 2}, {30, 2}},
			wantStored: map[uint64]uint64{2: 1, 3: 1}, wantLastLog: 3, wantLast: 30,
		},
		{
			name: "empty log, repeated installs",
			logs: nil, followerTerm: 0, leaderTerm: 2, commit: 0,
			trailing: 0, installs: []install{{10, 2}, {20, 2}},
			wantStored: map[uint64]uint64{}, wantLastLog: 0, wantLast: 20,
		},
		{
			name: "matching log beyond the snapshot is kept",
			logs: append([]*Log{confLog()}, committed(2, 8, 1)...), followerTerm: 1, leaderTerm: 1, commit: 2,
			trailing: 2, installs: []install{{5, 1}},
			wantStored: map[uint64]uint64{6: 1, 7: 1, 8: 1}, wantLastLog: 8, wantLast: 8,
		},
		{
			name: "divergent tail above the commit index is discarded",
			logs: append([]*Log{confLog(), cmd(2, 1, "c-2")}, stale(3, 8, 2)...), followerTerm: 2, leaderTerm: 3, commit: 2,
			trailing: 2, installs: []install{{5, 3}},
			wantStored: map[uint64]uint64{1: 1, 2: 1}, wantLastLog: 2, wantLast: 5,
		},
		{
			name: "divergent tail, TrailingLogs 0, repeated installs",
			logs: append([]*Log{confLog(), cmd(2, 1, "c-2")}, stale(3, 8, 2)...), followerTerm: 2, leaderTerm: 3, commit: 2,
			trailing: 0, installs: []install{{5, 3}, {10, 3}, {15, 3}},
			wantStored: map[uint64]uint64{}, wantLastLog: 0, wantLast: 15,
		},
		{
			name: "snapshot index at the last entry, matching",
			logs: append([]*Log{confLog()}, committed(2, 5, 1)...), followerTerm: 1, leaderTerm: 1, commit: 2,
			trailing: 2, installs: []install{{5, 1}},
			wantStored: map[uint64]uint64{4: 1, 5: 1}, wantLastLog: 5, wantLast: 5,
		},
		{
			name: "snapshot index at the last entry, conflicting",
			logs: append([]*Log{confLog(), cmd(2, 1, "c-2")}, stale(3, 5, 2)...), followerTerm: 2, leaderTerm: 3, commit: 2,
			trailing: 2, installs: []install{{5, 3}},
			wantStored: map[uint64]uint64{1: 1, 2: 1}, wantLastLog: 2, wantLast: 5,
		},
	}
	for _, st := range testLogStores {
		for _, tc := range cases {
			t.Run(st.name+"/"+tc.name, func(t *testing.T) {
				f := newTestFollowerOn(t, st.new(), tc.followerTerm, tc.logs...)
				f.setTrailingLogs(t, tc.trailing)
				var prevTerm uint64
				for _, l := range tc.logs {
					if l.Index == tc.commit {
						prevTerm = l.Term
					}
				}
				f.appendEntries(t, AppendEntriesRequest{Term: tc.leaderTerm, PrevLogEntry: tc.commit, PrevLogTerm: prevTerm, LeaderCommitIndex: tc.commit})
				f.requireCommitted(t, tc.commit, tc.commit)
				for _, in := range tc.installs {
					f.installSnapshot(t, tc.leaderTerm, in.index, in.term)
				}

				require.Equal(t, tc.wantStored, f.stored(t), "log store")
				lastLog, _ := f.r.getLastLog()
				require.Equal(t, tc.wantLastLog, lastLog, "last log")
				require.Equal(t, tc.wantLast, f.r.LastIndex())

				// The leader's next append, right after our last index, succeeds.
				next := tc.wantLast + 1
				f.appendEntries(t, AppendEntriesRequest{Term: tc.leaderTerm, PrevLogEntry: tc.wantLast, PrevLogTerm: tc.leaderTerm,
					LeaderCommitIndex: next, Entries: []*Log{cmd(next, tc.leaderTerm, "leader")}})
				f.requireCommitted(t, next, next)
				lastLog, _ = f.r.getLastLog()
				require.Equal(t, next, lastLog, "last log after the append")
				require.Equal(t, tc.leaderTerm, f.stored(t)[next])
				f.fsm.waitApplied(t, next)
				for _, l := range f.fsm.logs() {
					require.NotContains(t, string(l.Data), "stale", "applied a never-committed entry")
				}
			})
		}
	}
}

// Compaction must leave an empty log store alone. Here the follower's own
// snapshot with TrailingLogs 0 empties the store while the last log stays at
// the compacted index 5; installing a snapshot that matches then compacts
// again. Without the empty-store check that called DeleteRange(0, 5), which
// on InmemStore wraps its last index to MaxUint64 (also on v1.8.0).
func TestRaft_InstallSnapshot_CompactsEmptyStore(t *testing.T) {
	for _, st := range testLogStores {
		t.Run(st.name, func(t *testing.T) {
			f := newTestFollowerOn(t, st.new(), 1, confLog(), cmd(2, 1, "a"), cmd(3, 1, "b"), cmd(4, 1, "c"), cmd(5, 1, "d"))
			f.setTrailingLogs(t, 0)
			f.appendEntries(t, AppendEntriesRequest{Term: 1, PrevLogEntry: 5, PrevLogTerm: 1, LeaderCommitIndex: 5})
			f.requireCommitted(t, 5, 5)
			// The FSM goroutine applies 1-5 asynchronously; until it has,
			// there is nothing new to snapshot.
			waitForCondition(t, "a snapshot of 1-5", 5*time.Second, func() bool {
				err := f.r.Snapshot().Error()
				require.True(t, err == nil || errors.Is(err, ErrNothingNewToSnapshot), "snapshot: %v", err)
				return err == nil
			})
			require.Equal(t, map[uint64]uint64{}, f.stored(t))

			f.installSnapshot(t, 1, 5, 1)
			require.Equal(t, map[uint64]uint64{}, f.stored(t), "log store")
			require.Equal(t, uint64(5), f.r.LastIndex())

			f.appendEntries(t, AppendEntriesRequest{Term: 1, PrevLogEntry: 5, PrevLogTerm: 1,
				LeaderCommitIndex: 6, Entries: []*Log{cmd(6, 1, "e")}})
			f.requireCommitted(t, 6, 6)
			require.Equal(t, map[uint64]uint64{6: 1}, f.stored(t))
		})
	}
}

// A log store error while discarding fails the install; the follower still
// rejects appends that need the snapshot, and the leader's retry, once the
// store works again, leaves the expected log.
func TestRaft_InstallSnapshot_RetriesAfterLogStoreError(t *testing.T) {
	for _, st := range testLogStores {
		t.Run(st.name, func(t *testing.T) {
			store := &flakyDeletes{LogStore: st.new()}
			logs := []*Log{confLog(), cmd(2, 1, "c-2")}
			for i := uint64(3); i <= 8; i++ {
				logs = append(logs, cmd(i, 2, fmt.Sprintf("stale-%d", i)))
			}
			f := newTestFollowerOn(t, store, 2, logs...)
			f.setTrailingLogs(t, 0)
			f.appendEntries(t, AppendEntriesRequest{Term: 3, PrevLogEntry: 2, PrevLogTerm: 1, LeaderCommitIndex: 2})

			store.mu.Lock()
			store.fails = 1
			store.mu.Unlock()
			resp, err := f.sendSnapshot(3, 5, 3)
			require.Error(t, err, "install succeeded: %+v", resp)

			// Our entry 5 still conflicts, so an append after it is rejected
			// and the leader falls back to the snapshot again.
			req := AppendEntriesRequest{RPCHeader: f.header(), Term: 3, PrevLogEntry: 5, PrevLogTerm: 3, LeaderCommitIndex: 5}
			var aresp AppendEntriesResponse
			require.NoError(t, f.leader.AppendEntries("f", "f", &req, &aresp))
			require.False(t, aresp.Success, "append after a failed install accepted")

			f.installSnapshot(t, 3, 5, 3)
			require.Equal(t, map[uint64]uint64{}, f.stored(t), "log store")
			lastLog, _ := f.r.getLastLog()
			require.Equal(t, uint64(2), lastLog, "last log")
			require.Equal(t, uint64(5), f.r.LastIndex())

			f.appendEntries(t, AppendEntriesRequest{Term: 3, PrevLogEntry: 5, PrevLogTerm: 3,
				LeaderCommitIndex: 6, Entries: []*Log{cmd(6, 3, "leader")}})
			f.requireCommitted(t, 6, 6)
			require.Equal(t, map[uint64]uint64{6: 3}, f.stored(t))
		})
	}
}
