// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A follower holding a stale, never-committed tail must not commit it when an
// append whose entries end before the tail carries a higher LeaderCommitIndex.
// This happens when a new leader builds its first append to a follower (prev =
// its election-time last index, no entries) but reads its commit index only
// after its no-op at the next index has committed through other servers.
func TestRaft_AppendEntries_FollowerDoesNotCommitStaleTail(t *testing.T) {
	// Index 1-2 are committed in term 1. Index 3 was written by a term-2
	// leader that was cut off; it never committed.
	f := newTestFollower(t, 2, cmd(2, 1, "committed"), cmd(3, 2, "stale"))

	// Leader l (term 3, elected with last index 2) committed its own entry 3
	// through g, then sends: prev 2, no entries, commit 3.
	f.appendEntries(t, AppendEntriesRequest{Term: 3, PrevLogEntry: 2, PrevLogTerm: 1, LeaderCommitIndex: 3})
	f.requireCommitted(t, 2, 2)
	f.fsm.waitApplied(t, 2)

	// The next append carries l's entry 3, which replaces the stale one.
	f.appendEntries(t, AppendEntriesRequest{Term: 3, PrevLogEntry: 2, PrevLogTerm: 1, LeaderCommitIndex: 3,
		Entries: []*Log{cmd(3, 3, "leader")}})
	f.requireCommitted(t, 3, 3)
	f.fsm.waitApplied(t, 3)

	for _, l := range f.fsm.logs() {
		require.NotEqual(t, "stale", string(l.Data), "applied a never-committed entry: %+v", l)
	}
	applied := f.fsm.logs()
	require.Len(t, applied, 2)
	require.Equal(t, uint64(3), applied[1].Term)
}

// An append whose last covered entry lies below the follower's commit index
// must not move the commit index, backwards or forwards, nor apply anything.
func TestRaft_AppendEntries_CoveredEndBelowCommitIndex(t *testing.T) {
	f := newTestFollower(t, 1, cmd(2, 1, "a"), cmd(3, 1, "b"), cmd(4, 1, "c"), cmd(5, 1, "d"))

	f.appendEntries(t, AppendEntriesRequest{Term: 1, PrevLogEntry: 5, PrevLogTerm: 1, LeaderCommitIndex: 4})
	f.requireCommitted(t, 4, 4)

	// No entries, matching prev 2, LeaderCommitIndex 5: covers up to 2 only.
	f.appendEntries(t, AppendEntriesRequest{Term: 1, PrevLogEntry: 2, PrevLogTerm: 1, LeaderCommitIndex: 5})
	f.requireCommitted(t, 4, 4)

	// Duplicate entries 2-3 after prev 1, LeaderCommitIndex 5: covers up to 3.
	f.appendEntries(t, AppendEntriesRequest{Term: 1, PrevLogEntry: 1, PrevLogTerm: 1, LeaderCommitIndex: 5,
		Entries: []*Log{cmd(2, 1, "a"), cmd(3, 1, "b")}})
	f.requireCommitted(t, 4, 4)

	// An append covering index 5 still advances the commit index.
	f.appendEntries(t, AppendEntriesRequest{Term: 1, PrevLogEntry: 5, PrevLogTerm: 1, LeaderCommitIndex: 5})
	f.requireCommitted(t, 5, 5)
	f.fsm.waitApplied(t, 5)
	require.Len(t, f.fsm.logs(), 4)
}

// A follower still catches up: entries arrive with a commit index below their
// end, and the leader's later commit-only append (prev = the follower's last
// index, no entries) commits the rest.
func TestRaft_AppendEntries_FollowerCatchesUpOnCommitOnlyAppend(t *testing.T) {
	f := newTestFollower(t, 1)

	f.appendEntries(t, AppendEntriesRequest{Term: 1, PrevLogEntry: 1, PrevLogTerm: 1, LeaderCommitIndex: 2,
		Entries: []*Log{cmd(2, 1, "a"), cmd(3, 1, "b"), cmd(4, 1, "c")}})
	f.requireCommitted(t, 2, 2)

	f.appendEntries(t, AppendEntriesRequest{Term: 1, PrevLogEntry: 4, PrevLogTerm: 1, LeaderCommitIndex: 4})
	f.requireCommitted(t, 4, 4)
	f.fsm.waitApplied(t, 4)
	require.Len(t, f.fsm.logs(), 3)

	// A commit index beyond the follower's log is capped at the covered end.
	f.appendEntries(t, AppendEntriesRequest{Term: 1, PrevLogEntry: 4, PrevLogTerm: 1, LeaderCommitIndex: 9,
		Entries: []*Log{cmd(5, 1, "d")}})
	f.requireCommitted(t, 5, 5)
}
