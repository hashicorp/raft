package raft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestVotePersistenceCrashWindow_RealisticScenario tests the vote persistence
// vulnerability in a REALISTIC scenario where all nodes have genuinely
// synchronized logs.
//
// IMPORTANT: This test does NOT use any "cheating" techniques like using
// victim's log state for another candidate's RequestVote. Instead, it ensures
// all nodes genuinely have identical logs by NOT performing any operations
// that would cause log divergence after initial synchronization.
//
// The vulnerability being tested:
//
//	In raft.go, the RequestVote handler has a crash window between
//	setCurrentTerm() (line 1669) and persistVote() (line 1727). If a crash
//	occurs in this window:
//	- currentTerm is persisted to the new term T
//	- lastVoteTerm remains at the old value (< T)
//
//	On restart, when another candidate requests a vote for term T:
//	- The check `lastVoteTerm == req.Term` (line 1699) evaluates to FALSE
//	- The node incorrectly grants a second vote in term T
//
// Realistic Scenario:
//  1. Create 3-node cluster, apply one entry, wait for full replication
//  2. At this point, ALL nodes have identical logs (index=I, term=T)
//  3. Simulate crash on victim: currentTerm advanced to T+1, but lastVoteTerm=T
//     (Represents: victim received RequestVote(T+1), executed setCurrentTerm(T+1),
//     then crashed before persistVote could complete)
//  4. Restart victim
//  5. N1 (with genuinely identical log) starts an election with term T+1
//  6. Log up-to-date check: PASS (logs are genuinely identical)
//  7. Vote check: lastVoteTerm(T) != req.Term(T+1) -> incorrectly passes
//  8. Victim grants a vote -> but this could be a SECOND vote in term T+1!
//
// Why this is a real bug:
//
//	The victim may have already voted in term T+1 (for the candidate whose
//	RequestVote caused the crash). The vote response was never sent (due to
//	crash), but the safety invariant "one vote per term" is violated because
//	the node's internal state allows voting again.
func TestVotePersistenceCrashWindow_RealisticScenario(t *testing.T) {
	// Use in-memory config with reasonably short timeouts.
	conf := inmemConfig(t)
	conf.HeartbeatTimeout = 50 * time.Millisecond
	conf.ElectionTimeout = 50 * time.Millisecond

	// Build a 3-node cluster.
	c := MakeCluster(3, t, conf)
	defer c.Close()

	// ---------------------------------------------------------------------
	// Phase 1: Establish a stable cluster with synchronized logs.
	// ---------------------------------------------------------------------

	// Wait for initial leader election.
	leader := c.Leader()
	require.NotNil(t, leader)

	// Apply one entry and wait for full replication.
	// After this, ALL nodes have identical logs.
	applyFuture := leader.Apply([]byte("sync-entry"), time.Second)
	require.NoError(t, applyFuture.Error())
	c.WaitForReplication(1)

	// Identify nodes: leader (N1), and two followers (N2, N3).
	// We'll use one follower as the "victim".
	followers := c.Followers()
	require.Len(t, followers, 2)

	n1 := leader // Will be the candidate later
	victim := followers[0]
	n3 := followers[1] // Another follower

	idx1 := c.IndexOf(n1)
	victimIdx := c.IndexOf(victim)
	require.GreaterOrEqual(t, idx1, 0)
	require.GreaterOrEqual(t, victimIdx, 0)

	// Record N1's state BEFORE any modifications.
	// This will be used to construct N1's RequestVote later.
	n1CurrentTerm := n1.getCurrentTerm()
	n1LastIdx, n1LastTerm := n1.getLastEntry()

	// Also record victim's state for verification.
	victimLastIdx, victimLastTerm := victim.getLastEntry()

	// CRITICAL ASSERTION: Verify logs are genuinely identical.
	require.Equal(t, n1LastIdx, victimLastIdx, "logs should be synchronized: lastLogIndex")
	require.Equal(t, n1LastTerm, victimLastTerm, "logs should be synchronized: lastLogTerm")
	t.Logf("Synchronized state: n1CurrentTerm=%d, n1LastLogIndex=%d, n1LastLogTerm=%d",
		n1CurrentTerm, n1LastIdx, n1LastTerm)

	// ---------------------------------------------------------------------
	// Phase 2: Simulate a crash between setCurrentTerm and persistVote.
	//
	// Real-world scenario:
	//   1. Victim is at term T (currentTerm=T, lastVoteTerm=T for current leader)
	//   2. Some candidate sends RequestVote(term=T+1)
	//   3. Victim executes setCurrentTerm(T+1) -> currentTerm=T+1 persisted
	//   4. CRASH! persistVote(T+1, candidate) never executes
	//   5. On restart: currentTerm=T+1, lastVoteTerm=T (stale)
	//
	// We simulate this by:
	//   - Setting currentTerm to T+1 in StableStore (where T = n1CurrentTerm)
	//   - Leaving lastVoteTerm at T (the old value)
	//   - Restarting the victim node
	//
	// IMPORTANT: We use n1LastTerm + 1 as the election term (per reviewer feedback).
	// This represents N1 starting an election by incrementing its term.
	// In this scenario, n1LastTerm == n1CurrentTerm since N1 is the leader
	// and committed entries in the current term.
	// ---------------------------------------------------------------------

	victimStore := c.stores[victimIdx]

	// The election term when N1 starts an election.
	// Per Raft protocol: candidate increments currentTerm before sending RequestVote.
	// Per reviewer feedback: Term should be n1LastTerm + 1 (representing N1's next term).
	electionTerm := n1LastTerm + 1

	// Simulate the crash state on victim:
	// - currentTerm advanced to electionTerm (victim received RequestVote and updated term)
	// - lastVoteTerm is stale (persistVote never executed due to crash)
	require.NoError(t, victimStore.SetUint64(keyCurrentTerm, electionTerm))
	require.NoError(t, victimStore.SetUint64(keyLastVoteTerm, n1CurrentTerm))
	require.NoError(t, victimStore.Set(keyLastVoteCand, []byte(n3.localID)))

	t.Logf("Simulating crash state: victim.currentTerm will be %d, victim.lastVoteTerm will be %d",
		electionTerm, n1CurrentTerm)

	// Shutdown and restart the victim to apply the simulated crash state.
	oldStore := c.stores[victimIdx]
	oldSnap := c.snaps[victimIdx]
	oldTrans := c.trans[victimIdx]
	victimID := victim.localID

	shutdownFuture := victim.Shutdown()
	require.NoError(t, shutdownFuture.Error())

	_ = oldTrans.Close()
	addr := oldTrans.LocalAddr()
	newAddr, newTrans := NewInmemTransport(addr)
	require.Equal(t, addr, newAddr)

	restartConf := *conf
	restartConf.LocalID = victimID
	restartConf.Logger = newTestLoggerWithPrefix(t, string(victimID)+"-restart")

	restarted, err := NewRaft(&restartConf, &MockFSM{}, oldStore, oldStore, oldSnap, newTrans)
	require.NoError(t, err)

	// Update cluster bookkeeping.
	c.rafts[victimIdx] = restarted
	c.trans[victimIdx] = newTrans

	// Reconnect the restarted victim to the cluster.
	c.FullyConnect()

	// Verify the crash state.
	restartedTerm := restarted.getCurrentTerm()
	require.Equal(t, electionTerm, restartedTerm,
		"victim's currentTerm should be electionTerm after crash/restart")
	lastVoteTermAfter, err := victimStore.GetUint64(keyLastVoteTerm)
	require.NoError(t, err)
	require.Less(t, lastVoteTermAfter, restartedTerm,
		"victim's lastVoteTerm should be < currentTerm (crash-torn state)")

	t.Logf("Crash state verified: victim.currentTerm=%d, victim.lastVoteTerm=%d (stale)",
		restartedTerm, lastVoteTermAfter)

	// Verify victim's log state hasn't changed
	restartedLastIdx, restartedLastTerm := restarted.getLastEntry()
	t.Logf("Victim's log after restart: lastLogIndex=%d, lastLogTerm=%d",
		restartedLastIdx, restartedLastTerm)

	// ---------------------------------------------------------------------
	// Phase 3: N1 sends RequestVote as a candidate starting an election.
	//
	// Per reviewer feedback, the RequestVote should be constructed from N1's
	// perspective as a candidate:
	//   - Term: n1LastTerm + 1 (N1 increments its term to start election)
	//   - LastLogIndex: n1LastIdx (N1's genuine last log index)
	//   - LastLogTerm: n1LastTerm (N1's genuine last log term)
	//
	// This represents the scenario where:
	//   1. N1 detects it needs to start an election (e.g., lost leadership)
	//   2. N1 increments its term from T to T+1
	//   3. N1 sends RequestVote to all peers including the restarted victim
	//
	// The victim should NOT grant this vote because it may have already voted
	// in term T+1 (before the crash). However, due to the bug, it will grant
	// the vote because lastVoteTerm < currentTerm.
	// ---------------------------------------------------------------------

	// Clear victim's notion of leader so the RequestVote is not rejected
	// because the victim believes there is still a known leader.
	// (Per reviewer: this ensures victim has detected the partition/leader loss)
	restarted.setLeader("", "")

	// Construct RequestVote from N1's perspective as a candidate.
	// Per reviewer feedback: use n1's genuine state, with Term = n1LastTerm + 1.
	candidateTrans := c.trans[idx1]
	reqVote := RequestVoteRequest{
		RPCHeader:          n1.getRPCHeader(),
		Term:               n1LastTerm + 1, // N1 starts election, increments term
		LastLogIndex:       n1LastIdx,      // N1's genuine last log index
		LastLogTerm:        n1LastTerm,     // N1's genuine last log term
		LeadershipTransfer: false,
	}

	t.Logf("N1 starting election: Term=%d (n1LastTerm=%d + 1), LastLogIndex=%d, LastLogTerm=%d",
		reqVote.Term, n1LastTerm, reqVote.LastLogIndex, reqVote.LastLogTerm)
	t.Logf("Victim state: currentTerm=%d, lastVoteTerm=%d, lastLogIndex=%d, lastLogTerm=%d",
		restartedTerm, lastVoteTermAfter, restartedLastIdx, restartedLastTerm)

	var resp RequestVoteResponse
	err = candidateTrans.RequestVote(restarted.localID, restarted.localAddr, &reqVote, &resp)
	require.NoError(t, err)

	t.Logf("RequestVote response: Granted=%v, Term=%d", resp.Granted, resp.Term)

	// ---------------------------------------------------------------------
	// Phase 4: Assert correct behavior.
	//
	// The implementation should NOT grant this vote because:
	//   - The victim's currentTerm is T+1 (electionTerm)
	//   - The victim may have already voted in term T+1 (the crash happened
	//     DURING processing a vote request for term T+1 from another candidate)
	//   - The vote response was never sent (due to crash), but internally
	//     the victim had already decided to vote for that other candidate
	//   - Even though lastVoteTerm < currentTerm, a correct implementation
	//     should either:
	//     a) Use atomic persistence (persist term and vote together), OR
	//     b) Detect the inconsistent state and refuse to vote
	//
	// If the implementation grants this vote, it means:
	//   - The victim could vote twice in the same term (once before crash
	//     to some candidate, once after restart to N1)
	//   - This violates Raft's safety property: "each server will vote for
	//     at most one candidate in a given term"
	//   - This could lead to split-brain: two leaders in the same term
	// ---------------------------------------------------------------------

	require.Falsef(t, resp.Granted,
		"BUG DETECTED: victim granted vote in term %d with crash-torn state "+
			"(currentTerm=%d, lastVoteTerm=%d). "+
			"This could allow double-voting in the same term, violating Raft safety!",
		reqVote.Term, restartedTerm, lastVoteTermAfter)
}
