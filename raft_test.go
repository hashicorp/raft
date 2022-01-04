package raft

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/require"
)

func TestRaft_StartStop(t *testing.T) {
	c := MakeCluster(1, t, nil)
	c.Close()
}

func TestRaft_AfterShutdown(t *testing.T) {
	c := MakeCluster(1, t, nil)
	c.Close()
	raft := c.rafts[0]

	// Everything should fail now
	if f := raft.Apply(nil, 0); f.Error() != ErrRaftShutdown {
		t.Fatalf("should be shutdown: %v", f.Error())
	}

	// TODO (slackpad) - Barrier, VerifyLeader, and GetConfiguration can get
	// stuck if the buffered channel consumes the future but things are shut
	// down so they never get processed.
	if f := raft.AddVoter(ServerID("id"), ServerAddress("addr"), 0, 0); f.Error() != ErrRaftShutdown {
		t.Fatalf("should be shutdown: %v", f.Error())
	}
	if f := raft.AddNonvoter(ServerID("id"), ServerAddress("addr"), 0, 0); f.Error() != ErrRaftShutdown {
		t.Fatalf("should be shutdown: %v", f.Error())
	}
	if f := raft.RemoveServer(ServerID("id"), 0, 0); f.Error() != ErrRaftShutdown {
		t.Fatalf("should be shutdown: %v", f.Error())
	}
	if f := raft.DemoteVoter(ServerID("id"), 0, 0); f.Error() != ErrRaftShutdown {
		t.Fatalf("should be shutdown: %v", f.Error())
	}
	if f := raft.Snapshot(); f.Error() != ErrRaftShutdown {
		t.Fatalf("should be shutdown: %v", f.Error())
	}

	// Should be idempotent
	if f := raft.Shutdown(); f.Error() != nil {
		t.Fatalf("shutdown should be idempotent")
	}

}

func TestRaft_LiveBootstrap(t *testing.T) {
	// Make the cluster.
	c := MakeClusterNoBootstrap(3, t, nil)
	defer c.Close()

	// Build the configuration.
	configuration := Configuration{}
	for _, r := range c.rafts {
		server := Server{
			ID:      r.localID,
			Address: r.localAddr,
		}
		configuration.Servers = append(configuration.Servers, server)
	}

	// Bootstrap one of the nodes live.
	boot := c.rafts[0].BootstrapCluster(configuration)
	if err := boot.Error(); err != nil {
		t.Fatalf("bootstrap err: %v", err)
	}

	// Should be one leader.
	c.Followers()
	leader := c.Leader()
	c.EnsureLeader(t, leader.localAddr)

	// Should be able to apply.
	future := leader.Apply([]byte("test"), c.conf.CommitTimeout)
	if err := future.Error(); err != nil {
		t.Fatalf("apply err: %v", err)
	}
	c.WaitForReplication(1)

	// Make sure the live bootstrap fails now that things are started up.
	boot = c.rafts[0].BootstrapCluster(configuration)
	if err := boot.Error(); err != ErrCantBootstrap {
		t.Fatalf("bootstrap should have failed: %v", err)
	}
}

func TestRaft_RecoverCluster_NoState(t *testing.T) {
	c := MakeClusterNoBootstrap(1, t, nil)
	defer c.Close()

	r := c.rafts[0]
	configuration := Configuration{
		Servers: []Server{
			{
				ID:      r.localID,
				Address: r.localAddr,
			},
		},
	}
	cfg := r.config()
	err := RecoverCluster(&cfg, &MockFSM{}, r.logs, r.stable,
		r.snapshots, r.trans, configuration)
	if err == nil || !strings.Contains(err.Error(), "no initial state") {
		t.Fatalf("should have failed for no initial state: %v", err)
	}
}

func TestRaft_RecoverCluster(t *testing.T) {
	snapshotThreshold := 5
	runRecover := func(t *testing.T, applies int) {
		var err error
		conf := inmemConfig(t)
		conf.TrailingLogs = 10
		conf.SnapshotThreshold = uint64(snapshotThreshold)
		c := MakeCluster(3, t, conf)
		defer c.Close()

		// Perform some commits.
		c.logger.Debug("running with", "applies", applies)
		leader := c.Leader()
		for i := 0; i < applies; i++ {
			future := leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
			if err = future.Error(); err != nil {
				t.Fatalf("[ERR] apply err: %v", err)
			}
		}

		// Snap the configuration.
		future := leader.GetConfiguration()
		if err = future.Error(); err != nil {
			t.Fatalf("[ERR] get configuration err: %v", err)
		}
		configuration := future.Configuration()

		// Shut down the cluster.
		for _, sec := range c.rafts {
			if err = sec.Shutdown().Error(); err != nil {
				t.Fatalf("[ERR] shutdown err: %v", err)
			}
		}

		// Recover the cluster. We need to replace the transport and we
		// replace the FSM so no state can carry over.
		for i, r := range c.rafts {
			var before []*SnapshotMeta
			before, err = r.snapshots.List()
			if err != nil {
				t.Fatalf("snapshot list err: %v", err)
			}
			cfg := r.config()
			if err = RecoverCluster(&cfg, &MockFSM{}, r.logs, r.stable,
				r.snapshots, r.trans, configuration); err != nil {
				t.Fatalf("recover err: %v", err)
			}

			// Make sure the recovery looks right.
			var after []*SnapshotMeta
			after, err = r.snapshots.List()
			if err != nil {
				t.Fatalf("snapshot list err: %v", err)
			}
			if len(after) != len(before)+1 {
				t.Fatalf("expected a new snapshot, %d vs. %d", len(before), len(after))
			}
			var first uint64
			first, err = r.logs.FirstIndex()
			if err != nil {
				t.Fatalf("first log index err: %v", err)
			}
			var last uint64
			last, err = r.logs.LastIndex()
			if err != nil {
				t.Fatalf("last log index err: %v", err)
			}
			if first != 0 || last != 0 {
				t.Fatalf("expected empty logs, got %d/%d", first, last)
			}

			// Fire up the recovered Raft instance. We have to patch
			// up the cluster state manually since this is an unusual
			// operation.
			_, trans := NewInmemTransport(r.localAddr)
			var r2 *Raft
			r2, err = NewRaft(&cfg, &MockFSM{}, r.logs, r.stable, r.snapshots, trans)
			if err != nil {
				t.Fatalf("new raft err: %v", err)
			}
			c.rafts[i] = r2
			c.trans[i] = r2.trans.(*InmemTransport)
			c.fsms[i] = r2.fsm.(*MockFSM)
		}
		c.FullyConnect()
		time.Sleep(c.propagateTimeout * 3)

		// Let things settle and make sure we recovered.
		c.EnsureLeader(t, c.Leader().localAddr)
		c.EnsureSame(t)
		c.EnsureSamePeers(t)
	}

	t.Run("no snapshot, no trailing logs", func(t *testing.T) {
		runRecover(t, 0)
	})
	t.Run("no snapshot, some trailing logs", func(t *testing.T) {
		runRecover(t, snapshotThreshold-1)
	})
	t.Run("snapshot, with trailing logs", func(t *testing.T) {
		runRecover(t, snapshotThreshold+20)
	})
}

func TestRaft_HasExistingState(t *testing.T) {
	var err error
	// Make a cluster.
	c := MakeCluster(2, t, nil)
	defer c.Close()

	// Make a new cluster of 1.
	c1 := MakeClusterNoBootstrap(1, t, nil)

	// Make sure the initial state is clean.
	var hasState bool
	hasState, err = HasExistingState(c1.rafts[0].logs, c1.rafts[0].stable, c1.rafts[0].snapshots)
	if err != nil || hasState {
		t.Fatalf("should not have any existing state, %v", err)
	}

	// Merge clusters.
	c.Merge(c1)
	c.FullyConnect()

	// Join the new node in.
	future := c.Leader().AddVoter(c1.rafts[0].localID, c1.rafts[0].localAddr, 0, 0)
	if err = future.Error(); err != nil {
		t.Fatalf("[ERR] err: %v", err)
	}

	// Check the FSMs.
	c.EnsureSame(t)

	// Check the peers.
	c.EnsureSamePeers(t)

	// Ensure one leader.
	c.EnsureLeader(t, c.Leader().localAddr)

	// Make sure it's not clean.
	hasState, err = HasExistingState(c1.rafts[0].logs, c1.rafts[0].stable, c1.rafts[0].snapshots)
	if err != nil || !hasState {
		t.Fatalf("should have some existing state, %v", err)
	}
}

func TestRaft_SingleNode(t *testing.T) {
	conf := inmemConfig(t)
	c := MakeCluster(1, t, conf)
	defer c.Close()
	raft := c.rafts[0]

	// Watch leaderCh for change
	select {
	case v := <-raft.LeaderCh():
		if !v {
			t.Fatalf("should become leader")
		}
	case <-time.After(conf.HeartbeatTimeout * 3):
		t.Fatalf("timeout becoming leader")
	}

	// Should be leader
	if s := raft.State(); s != Leader {
		t.Fatalf("expected leader: %v", s)
	}

	// Should be able to apply
	future := raft.Apply([]byte("test"), c.conf.HeartbeatTimeout)
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Check the response
	if future.Response().(int) != 1 {
		t.Fatalf("bad response: %v", future.Response())
	}

	// Check the index
	if idx := future.Index(); idx == 0 {
		t.Fatalf("bad index: %d", idx)
	}

	// Check that it is applied to the FSM
	if len(getMockFSM(c.fsms[0]).logs) != 1 {
		t.Fatalf("did not apply to FSM!")
	}
}

func TestRaft_TripleNode(t *testing.T) {
	// Make the cluster
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// Should be one leader
	c.Followers()
	leader := c.Leader()
	c.EnsureLeader(t, leader.localAddr)

	// Should be able to apply
	future := leader.Apply([]byte("test"), c.conf.CommitTimeout)
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}
	c.WaitForReplication(1)
}

func TestRaft_LeaderFail(t *testing.T) {
	// Make the cluster
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// Should be one leader
	c.Followers()
	leader := c.Leader()

	// Should be able to apply
	future := leader.Apply([]byte("test"), c.conf.CommitTimeout)
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}
	c.WaitForReplication(1)

	// Disconnect the leader now
	t.Logf("[INFO] Disconnecting %v", leader)
	leaderTerm := leader.getCurrentTerm()
	c.Disconnect(leader.localAddr)

	// Wait for new leader
	limit := time.Now().Add(c.longstopTimeout)
	var newLead *Raft
	for time.Now().Before(limit) && newLead == nil {
		c.WaitEvent(nil, c.conf.CommitTimeout)
		leaders := c.GetInState(Leader)
		if len(leaders) == 1 && leaders[0] != leader {
			newLead = leaders[0]
		}
	}
	if newLead == nil {
		t.Fatalf("expected new leader")
	}

	// Ensure the term is greater
	if newLead.getCurrentTerm() <= leaderTerm {
		t.Fatalf("expected newer term! %d %d (%v, %v)", newLead.getCurrentTerm(), leaderTerm, newLead, leader)
	}

	// Apply should work not work on old leader
	future1 := leader.Apply([]byte("fail"), c.conf.CommitTimeout)

	// Apply should work on newer leader
	future2 := newLead.Apply([]byte("apply"), c.conf.CommitTimeout)

	// Future2 should work
	if err := future2.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Reconnect the networks
	t.Logf("[INFO] Reconnecting %v", leader)
	c.FullyConnect()

	// Future1 should fail
	if err := future1.Error(); err != ErrLeadershipLost && err != ErrNotLeader {
		t.Fatalf("err: %v", err)
	}

	// Wait for log replication
	c.EnsureSame(t)

	// Check two entries are applied to the FSM
	for _, fsmRaw := range c.fsms {
		fsm := getMockFSM(fsmRaw)
		fsm.Lock()
		if len(fsm.logs) != 2 {
			t.Fatalf("did not apply both to FSM! %v", fsm.logs)
		}
		if bytes.Compare(fsm.logs[0], []byte("test")) != 0 {
			t.Fatalf("first entry should be 'test'")
		}
		if bytes.Compare(fsm.logs[1], []byte("apply")) != 0 {
			t.Fatalf("second entry should be 'apply'")
		}
		fsm.Unlock()
	}
}

func TestRaft_BehindFollower(t *testing.T) {
	// Make the cluster
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// Disconnect one follower
	leader := c.Leader()
	followers := c.Followers()
	behind := followers[0]
	c.Disconnect(behind.localAddr)

	// Commit a lot of things
	var future Future
	for i := 0; i < 100; i++ {
		future = leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
	}

	// Wait for the last future to apply
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	} else {
		t.Logf("[INFO] Finished apply without behind follower")
	}

	// Check that we have a non zero last contact
	if behind.LastContact().IsZero() {
		t.Fatalf("expected previous contact")
	}

	// Reconnect the behind node
	c.FullyConnect()

	// Ensure all the logs are the same
	c.EnsureSame(t)

	// Ensure one leader
	leader = c.Leader()
	c.EnsureLeader(t, leader.localAddr)
}

func TestRaft_ApplyNonLeader(t *testing.T) {
	// Make the cluster
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// Wait for a leader
	c.Leader()

	// Try to apply to them
	followers := c.GetInState(Follower)
	if len(followers) != 2 {
		t.Fatalf("Expected 2 followers")
	}
	follower := followers[0]

	// Try to apply
	future := follower.Apply([]byte("test"), c.conf.CommitTimeout)
	if future.Error() != ErrNotLeader {
		t.Fatalf("should not apply on follower")
	}

	// Should be cached
	if future.Error() != ErrNotLeader {
		t.Fatalf("should not apply on follower")
	}
}

func TestRaft_ApplyConcurrent(t *testing.T) {
	// Make the cluster
	conf := inmemConfig(t)
	conf.HeartbeatTimeout = 2 * conf.HeartbeatTimeout
	conf.ElectionTimeout = 2 * conf.ElectionTimeout
	c := MakeCluster(3, t, conf)
	defer c.Close()

	// Wait for a leader
	leader := c.Leader()

	// Create a wait group
	const sz = 100
	var group sync.WaitGroup
	group.Add(sz)

	applyF := func(i int) {
		defer group.Done()
		future := leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
		if err := future.Error(); err != nil {
			c.Failf("[ERR] err: %v", err)
		}
	}

	// Concurrently apply
	for i := 0; i < sz; i++ {
		go applyF(i)
	}

	// Wait to finish
	doneCh := make(chan struct{})
	go func() {
		group.Wait()
		close(doneCh)
	}()
	select {
	case <-doneCh:
	case <-time.After(c.longstopTimeout):
		t.Fatalf("timeout")
	}

	// If anything failed up to this point then bail now, rather than do a
	// confusing compare.
	if t.Failed() {
		t.Fatalf("One or more of the apply operations failed")
	}

	// Check the FSMs
	c.EnsureSame(t)
}

func TestRaft_ApplyConcurrent_Timeout(t *testing.T) {
	// Make the cluster
	conf := inmemConfig(t)
	conf.CommitTimeout = 1 * time.Millisecond
	conf.HeartbeatTimeout = 2 * conf.HeartbeatTimeout
	conf.ElectionTimeout = 2 * conf.ElectionTimeout
	c := MakeCluster(1, t, conf)
	defer c.Close()

	// Wait for a leader
	leader := c.Leader()

	// Enough enqueues should cause at least one timeout...
	var didTimeout int32
	for i := 0; (i < 5000) && (atomic.LoadInt32(&didTimeout) == 0); i++ {
		go func(i int) {
			future := leader.Apply([]byte(fmt.Sprintf("test%d", i)), time.Microsecond)
			if future.Error() == ErrEnqueueTimeout {
				atomic.StoreInt32(&didTimeout, 1)
			}
		}(i)

		// Give the leader loop some other things to do in order to
		// increase the odds of a timeout.
		if i%5 == 0 {
			leader.VerifyLeader()
		}
	}

	// Loop until we see a timeout, or give up.
	limit := time.Now().Add(c.longstopTimeout)
	for time.Now().Before(limit) {
		if atomic.LoadInt32(&didTimeout) != 0 {
			return
		}
		c.WaitEvent(nil, c.propagateTimeout)
	}
	t.Fatalf("Timeout waiting to detect apply timeouts")
}

func TestRaft_JoinNode(t *testing.T) {
	// Make a cluster
	c := MakeCluster(2, t, nil)
	defer c.Close()

	// Make a new cluster of 1
	c1 := MakeClusterNoBootstrap(1, t, nil)

	// Merge clusters
	c.Merge(c1)
	c.FullyConnect()

	// Join the new node in
	future := c.Leader().AddVoter(c1.rafts[0].localID, c1.rafts[0].localAddr, 0, 0)
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Ensure one leader
	c.EnsureLeader(t, c.Leader().localAddr)

	// Check the FSMs
	c.EnsureSame(t)

	// Check the peers
	c.EnsureSamePeers(t)
}

func TestRaft_JoinNode_ConfigStore(t *testing.T) {
	// Make a cluster
	conf := inmemConfig(t)
	c := makeCluster(t, &MakeClusterOpts{
		Peers:          1,
		Bootstrap:      true,
		Conf:           conf,
		ConfigStoreFSM: true,
	})
	defer c.Close()

	// Make a new nodes
	c1 := makeCluster(t, &MakeClusterOpts{
		Peers:          1,
		Bootstrap:      false,
		Conf:           conf,
		ConfigStoreFSM: true,
	})
	c2 := makeCluster(t, &MakeClusterOpts{
		Peers:          1,
		Bootstrap:      false,
		Conf:           conf,
		ConfigStoreFSM: true,
	})

	// Merge clusters
	c.Merge(c1)
	c.Merge(c2)
	c.FullyConnect()

	// Join the new node in
	future := c.Leader().AddVoter(c1.rafts[0].localID, c1.rafts[0].localAddr, 0, 0)
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}
	// Join the new node in
	future = c.Leader().AddVoter(c2.rafts[0].localID, c2.rafts[0].localAddr, 0, 0)
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Ensure one leader
	c.EnsureLeader(t, c.Leader().localAddr)

	// Check the FSMs
	c.EnsureSame(t)

	// Check the peers
	c.EnsureSamePeers(t)

	// Check the fsm holds the correct config logs
	for _, fsmRaw := range c.fsms {
		fsm := getMockFSM(fsmRaw)
		if len(fsm.configurations) != 3 {
			t.Fatalf("unexpected number of configuration changes: %d", len(fsm.configurations))
		}
		if len(fsm.configurations[0].Servers) != 1 {
			t.Fatalf("unexpected number of servers in config change: %v", fsm.configurations[0].Servers)
		}
		if len(fsm.configurations[1].Servers) != 2 {
			t.Fatalf("unexpected number of servers in config change: %v", fsm.configurations[1].Servers)
		}
		if len(fsm.configurations[2].Servers) != 3 {
			t.Fatalf("unexpected number of servers in config change: %v", fsm.configurations[2].Servers)
		}
	}

}

func TestRaft_RemoveFollower(t *testing.T) {
	// Make a cluster
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// Get the leader
	leader := c.Leader()

	// Wait until we have 2 followers
	limit := time.Now().Add(c.longstopTimeout)
	var followers []*Raft
	for time.Now().Before(limit) && len(followers) != 2 {
		c.WaitEvent(nil, c.conf.CommitTimeout)
		followers = c.GetInState(Follower)
	}
	if len(followers) != 2 {
		t.Fatalf("expected two followers: %v", followers)
	}

	// Remove a follower
	follower := followers[0]
	future := leader.RemoveServer(follower.localID, 0, 0)
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Wait a while
	time.Sleep(c.propagateTimeout)

	// Other nodes should have fewer peers
	if configuration := c.getConfiguration(leader); len(configuration.Servers) != 2 {
		t.Fatalf("too many peers")
	}
	if configuration := c.getConfiguration(followers[1]); len(configuration.Servers) != 2 {
		t.Fatalf("too many peers")
	}

	// The removed node should remain in a follower state
	require.Equal(t, Follower, follower.getState())
}

func TestRaft_RemoveLeader(t *testing.T) {
	// Make a cluster
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// Get the leader
	leader := c.Leader()

	// Wait until we have 2 followers
	limit := time.Now().Add(c.longstopTimeout)
	var followers []*Raft
	for time.Now().Before(limit) && len(followers) != 2 {
		c.WaitEvent(nil, c.conf.CommitTimeout)
		followers = c.GetInState(Follower)
	}
	if len(followers) != 2 {
		t.Fatalf("expected two followers: %v", followers)
	}

	// Remove the leader
	f := leader.RemoveServer(leader.localID, 0, 0)

	// Wait for the future to complete
	if f.Error() != nil {
		t.Fatalf("RemoveServer() returned error %v", f.Error())
	}

	// Wait a bit for log application
	time.Sleep(c.propagateTimeout)

	// Should have a new leader
	time.Sleep(c.propagateTimeout)
	newLeader := c.Leader()
	if newLeader == leader {
		t.Fatalf("removed leader is still leader")
	}

	// Other nodes should have fewer peers
	if configuration := c.getConfiguration(newLeader); len(configuration.Servers) != 2 {
		t.Fatalf("wrong number of peers %d", len(configuration.Servers))
	}

	// Old leader should be shutdown
	if leader.State() != Shutdown {
		t.Fatalf("old leader should be shutdown")
	}
}

func TestRaft_RemoveLeader_NoShutdown(t *testing.T) {
	// Make a cluster
	conf := inmemConfig(t)
	conf.ShutdownOnRemove = false
	c := MakeCluster(3, t, conf)
	defer c.Close()

	// Get the leader
	c.Followers()
	leader := c.Leader()

	// Remove the leader
	for i := byte(0); i < 100; i++ {
		if i == 80 {
			removeFuture := leader.RemoveServer(leader.localID, 0, 0)
			if err := removeFuture.Error(); err != nil {
				t.Fatalf("err: %v, remove leader failed", err)
			}
		}
		future := leader.Apply([]byte{i}, 0)
		if i > 80 {
			if err := future.Error(); err == nil || err != ErrNotLeader {
				t.Fatalf("err: %v, future entries should fail", err)
			}
		}
	}

	// Wait a while
	time.Sleep(c.propagateTimeout)

	// Should have a new leader
	newLeader := c.Leader()

	// Wait a bit for log application
	time.Sleep(c.propagateTimeout)

	// Other nodes should have pulled the leader.
	configuration := c.getConfiguration(newLeader)
	if len(configuration.Servers) != 2 {
		t.Fatalf("too many peers")
	}
	if hasVote(configuration, leader.localID) {
		t.Fatalf("old leader should no longer have a vote")
	}

	// Old leader should be a follower.
	if leader.State() != Follower {
		t.Fatalf("leader should be follower")
	}

	// Old leader should not include itself in its peers.
	configuration = c.getConfiguration(leader)
	if len(configuration.Servers) != 2 {
		t.Fatalf("too many peers")
	}
	if hasVote(configuration, leader.localID) {
		t.Fatalf("old leader should no longer have a vote")
	}

	// Other nodes should have the same state
	c.EnsureSame(t)
}

func TestRaft_RemoveFollower_SplitCluster(t *testing.T) {
	// Make a cluster.
	conf := inmemConfig(t)
	c := MakeCluster(4, t, conf)
	defer c.Close()

	// Wait for a leader to get elected.
	leader := c.Leader()

	// Wait to make sure knowledge of the 4th server is known to all the
	// peers.
	numServers := 0
	limit := time.Now().Add(c.longstopTimeout)
	for time.Now().Before(limit) && numServers != 4 {
		time.Sleep(c.propagateTimeout)
		configuration := c.getConfiguration(leader)
		numServers = len(configuration.Servers)
	}
	if numServers != 4 {
		t.Fatalf("Leader should have 4 servers, got %d", numServers)
	}
	c.EnsureSamePeers(t)

	// Isolate two of the followers.
	followers := c.Followers()
	if len(followers) != 3 {
		t.Fatalf("Expected 3 followers, got %d", len(followers))
	}
	c.Partition([]ServerAddress{followers[0].localAddr, followers[1].localAddr})

	// Try to remove the remaining follower that was left with the leader.
	future := leader.RemoveServer(followers[2].localID, 0, 0)
	if err := future.Error(); err == nil {
		t.Fatalf("Should not have been able to make peer change")
	}
}

func TestRaft_AddKnownPeer(t *testing.T) {
	// Make a cluster
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// Get the leader
	leader := c.Leader()
	followers := c.GetInState(Follower)

	configReq := &configurationsFuture{}
	configReq.init()
	leader.configurationsCh <- configReq
	if err := configReq.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}
	startingConfig := configReq.configurations.committed
	startingConfigIdx := configReq.configurations.committedIndex

	// Add a follower
	future := leader.AddVoter(followers[0].localID, followers[0].localAddr, 0, 0)
	if err := future.Error(); err != nil {
		t.Fatalf("AddVoter() err: %v", err)
	}
	configReq = &configurationsFuture{}
	configReq.init()
	leader.configurationsCh <- configReq
	if err := configReq.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}
	newConfig := configReq.configurations.committed
	newConfigIdx := configReq.configurations.committedIndex
	if newConfigIdx <= startingConfigIdx {
		t.Fatalf("AddVoter should have written a new config entry, but configurations.commitedIndex still %d", newConfigIdx)
	}
	if !reflect.DeepEqual(newConfig, startingConfig) {
		t.Fatalf("[ERR} AddVoter with existing peer shouldn't have changed config, was %#v, but now %#v", startingConfig, newConfig)
	}
}

func TestRaft_RemoveUnknownPeer(t *testing.T) {
	// Make a cluster
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// Get the leader
	leader := c.Leader()
	configReq := &configurationsFuture{}
	configReq.init()
	leader.configurationsCh <- configReq
	if err := configReq.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}
	startingConfig := configReq.configurations.committed
	startingConfigIdx := configReq.configurations.committedIndex

	// Remove unknown
	future := leader.RemoveServer(ServerID(NewInmemAddr()), 0, 0)

	// nothing to do, should be a new config entry that's the same as before
	if err := future.Error(); err != nil {
		t.Fatalf("RemoveServer() err: %v", err)
	}
	configReq = &configurationsFuture{}
	configReq.init()
	leader.configurationsCh <- configReq
	if err := configReq.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}
	newConfig := configReq.configurations.committed
	newConfigIdx := configReq.configurations.committedIndex
	if newConfigIdx <= startingConfigIdx {
		t.Fatalf("RemoveServer should have written a new config entry, but configurations.commitedIndex still %d", newConfigIdx)
	}
	if !reflect.DeepEqual(newConfig, startingConfig) {
		t.Fatalf("[ERR} RemoveServer with unknown peer shouldn't of changed config, was %#v, but now %#v", startingConfig, newConfig)
	}
}

func TestRaft_SnapshotRestore(t *testing.T) {
	// Make the cluster
	conf := inmemConfig(t)
	conf.TrailingLogs = 10
	c := MakeCluster(1, t, conf)
	defer c.Close()

	// Commit a lot of things
	leader := c.Leader()
	var future Future
	for i := 0; i < 100; i++ {
		future = leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
	}

	// Wait for the last future to apply
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Take a snapshot
	snapFuture := leader.Snapshot()
	if err := snapFuture.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Check for snapshot
	snaps, _ := leader.snapshots.List()
	if len(snaps) != 1 {
		t.Fatalf("should have a snapshot")
	}
	snap := snaps[0]

	// Logs should be trimmed
	if idx, _ := leader.logs.FirstIndex(); idx != snap.Index-conf.TrailingLogs+1 {
		t.Fatalf("should trim logs to %d: but is %d", snap.Index-conf.TrailingLogs+1, idx)
	}

	// Shutdown
	shutdown := leader.Shutdown()
	if err := shutdown.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Restart the Raft
	r := leader
	// Can't just reuse the old transport as it will be closed
	_, trans2 := NewInmemTransport(r.trans.LocalAddr())
	cfg := r.config()
	r, err := NewRaft(&cfg, r.fsm, r.logs, r.stable, r.snapshots, trans2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	c.rafts[0] = r

	// We should have restored from the snapshot!
	if last := r.getLastApplied(); last != snap.Index {
		t.Fatalf("bad last index: %d, expecting %d", last, snap.Index)
	}
}

// TODO: Need a test that has a previous format Snapshot and check that it can
// be read/installed on the new code.

// TODO: Need a test to process old-style entries in the Raft log when starting
// up.

func TestRaft_NoRestoreOnStart(t *testing.T) {
	conf := inmemConfig(t)
	conf.TrailingLogs = 10
	conf.NoSnapshotRestoreOnStart = true
	c := MakeCluster(1, t, conf)

	// Commit a lot of things.
	leader := c.Leader()
	var future Future
	for i := 0; i < 100; i++ {
		future = leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
	}

	// Wait for the last future to apply
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Take a snapshot.
	snapFuture := leader.Snapshot()
	if err := snapFuture.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Shutdown.
	shutdown := leader.Shutdown()
	if err := shutdown.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	_, trans := NewInmemTransport(leader.localAddr)
	newFSM := &MockFSM{}
	cfg := leader.config()
	_, err := NewRaft(&cfg, newFSM, leader.logs, leader.stable, leader.snapshots, trans)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if len(newFSM.logs) != 0 {
		t.Fatalf("expected empty FSM, got %v", newFSM)
	}
}

func TestRaft_SnapshotRestore_PeerChange(t *testing.T) {
	var err error
	// Make the cluster.
	conf := inmemConfig(t)
	conf.ProtocolVersion = 1
	conf.TrailingLogs = 10
	c := MakeCluster(3, t, conf)
	defer c.Close()

	// Commit a lot of things.
	leader := c.Leader()
	var future Future
	for i := 0; i < 100; i++ {
		future = leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
	}

	// Wait for the last future to apply
	if err = future.Error(); err != nil {
		t.Fatalf("[ERR] err: %v", err)
	}

	// Take a snapshot.
	snapFuture := leader.Snapshot()
	if err = snapFuture.Error(); err != nil {
		t.Fatalf("[ERR] err: %v", err)
	}

	// Shutdown.
	shutdown := leader.Shutdown()
	if err = shutdown.Error(); err != nil {
		t.Fatalf("[ERR] err: %v", err)
	}

	// Make a separate cluster.
	c2 := MakeClusterNoBootstrap(2, t, conf)
	defer c2.Close()

	// Kill the old cluster.
	for _, sec := range c.rafts {
		if sec != leader {
			if err = sec.Shutdown().Error(); err != nil {
				t.Fatalf("[ERR] shutdown err: %v", err)
			}
		}
	}

	// Restart the Raft with new peers.
	r := leader

	// Gather the new peer address list.
	var peers []string
	peers = append(peers, fmt.Sprintf("%q", leader.trans.LocalAddr()))
	for _, sec := range c2.rafts {
		peers = append(peers, fmt.Sprintf("%q", sec.trans.LocalAddr()))
	}
	content := []byte(fmt.Sprintf("[%s]", strings.Join(peers, ",")))

	// Perform a manual recovery on the cluster.
	base, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer os.RemoveAll(base)
	peersFile := filepath.Join(base, "peers.json")
	if err = ioutil.WriteFile(peersFile, content, 0666); err != nil {
		t.Fatalf("[ERR] err: %v", err)
	}
	configuration, err := ReadPeersJSON(peersFile)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	cfg := r.config()
	if err = RecoverCluster(&cfg, &MockFSM{}, r.logs, r.stable,
		r.snapshots, r.trans, configuration); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Can't just reuse the old transport as it will be closed. We also start
	// with a fresh FSM for good measure so no state can carry over.
	_, trans := NewInmemTransport(r.localAddr)
	r, err = NewRaft(&cfg, &MockFSM{}, r.logs, r.stable, r.snapshots, trans)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	c.rafts[0] = r
	c2.rafts = append(c2.rafts, r)
	c2.trans = append(c2.trans, r.trans.(*InmemTransport))
	c2.fsms = append(c2.fsms, r.fsm.(*MockFSM))
	c2.FullyConnect()

	// Wait a while.
	time.Sleep(c.propagateTimeout)

	// Ensure we elect a leader, and that we replicate to our new followers.
	c2.EnsureSame(t)

	// We should have restored from the snapshot! Note that there's one
	// index bump from the noop the leader tees up when it takes over.
	if last := r.getLastApplied(); last != 103 {
		t.Fatalf("bad last: %v", last)
	}

	// Check the peers.
	c2.EnsureSamePeers(t)
}

func TestRaft_AutoSnapshot(t *testing.T) {
	// Make the cluster
	conf := inmemConfig(t)
	conf.SnapshotInterval = conf.CommitTimeout * 2
	conf.SnapshotThreshold = 50
	conf.TrailingLogs = 10
	c := MakeCluster(1, t, conf)
	defer c.Close()

	// Commit a lot of things
	leader := c.Leader()
	var future Future
	for i := 0; i < 100; i++ {
		future = leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
	}

	// Wait for the last future to apply
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Wait for a snapshot to happen
	time.Sleep(c.propagateTimeout)

	// Check for snapshot
	if snaps, _ := leader.snapshots.List(); len(snaps) == 0 {
		t.Fatalf("should have a snapshot")
	}
}

func TestRaft_UserSnapshot(t *testing.T) {
	// Make the cluster.
	conf := inmemConfig(t)
	conf.SnapshotThreshold = 50
	conf.TrailingLogs = 10
	c := MakeCluster(1, t, conf)
	defer c.Close()

	// With nothing committed, asking for a snapshot should return an error.
	leader := c.Leader()
	if userSnapshotErrorsOnNoData {
		if err := leader.Snapshot().Error(); err != ErrNothingNewToSnapshot {
			t.Fatalf("Request for Snapshot failed: %v", err)
		}
	}

	// Commit some things.
	var future Future
	for i := 0; i < 10; i++ {
		future = leader.Apply([]byte(fmt.Sprintf("test %d", i)), 0)
	}
	if err := future.Error(); err != nil {
		t.Fatalf("Error Apply new log entries: %v", err)
	}

	// Now we should be able to ask for a snapshot without getting an error.
	if err := leader.Snapshot().Error(); err != nil {
		t.Fatalf("Request for Snapshot failed: %v", err)
	}

	// Check for snapshot
	if snaps, _ := leader.snapshots.List(); len(snaps) == 0 {
		t.Fatalf("should have a snapshot")
	}
}

// snapshotAndRestore does a snapshot and restore sequence and applies the given
// offset to the snapshot index, so we can try out different situations.
func snapshotAndRestore(t *testing.T, offset uint64) {
	// Make the cluster.
	conf := inmemConfig(t)

	// snapshot operations perform some file IO operations.
	// increase times out to account for that
	conf.HeartbeatTimeout = 500 * time.Millisecond
	conf.ElectionTimeout = 500 * time.Millisecond
	conf.LeaderLeaseTimeout = 500 * time.Millisecond

	c := MakeCluster(3, t, conf)
	defer c.Close()

	// Wait for things to get stable and commit some things.
	leader := c.Leader()
	var future Future
	for i := 0; i < 10; i++ {
		future = leader.Apply([]byte(fmt.Sprintf("test %d", i)), 0)
	}
	if err := future.Error(); err != nil {
		t.Fatalf("Error Apply new log entries: %v", err)
	}

	// Take a snapshot.
	snap := leader.Snapshot()
	if err := snap.Error(); err != nil {
		t.Fatalf("Request for Snapshot failed: %v", err)
	}

	// Commit some more things.
	for i := 10; i < 20; i++ {
		future = leader.Apply([]byte(fmt.Sprintf("test %d", i)), 0)
	}
	if err := future.Error(); err != nil {
		t.Fatalf("Error Apply new log entries: %v", err)
	}

	// Get the last index before the restore.
	preIndex := leader.getLastIndex()

	// Restore the snapshot, twiddling the index with the offset.
	meta, reader, err := snap.Open()
	meta.Index += offset
	if err != nil {
		t.Fatalf("Snapshot open failed: %v", err)
	}
	defer reader.Close()
	if err := leader.Restore(meta, reader, 5*time.Second); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// Make sure the index was updated correctly. We add 2 because we burn
	// an index to create a hole, and then we apply a no-op after the
	// restore.
	var expected uint64
	if meta.Index < preIndex {
		expected = preIndex + 2
	} else {
		expected = meta.Index + 2
	}
	lastIndex := leader.getLastIndex()
	if lastIndex != expected {
		t.Fatalf("Index was not updated correctly: %d vs. %d", lastIndex, expected)
	}

	// Ensure all the logs are the same and that we have everything that was
	// part of the original snapshot, and that the contents after were
	// reverted.
	c.EnsureSame(t)
	fsm := getMockFSM(c.fsms[0])
	fsm.Lock()
	if len(fsm.logs) != 10 {
		t.Fatalf("Log length bad: %d", len(fsm.logs))
	}
	for i, entry := range fsm.logs {
		expected := []byte(fmt.Sprintf("test %d", i))
		if bytes.Compare(entry, expected) != 0 {
			t.Fatalf("Log entry bad: %v", entry)
		}
	}
	fsm.Unlock()

	// Commit some more things.
	for i := 20; i < 30; i++ {
		future = leader.Apply([]byte(fmt.Sprintf("test %d", i)), 0)
	}
	if err := future.Error(); err != nil {
		t.Fatalf("Error Apply new log entries: %v", err)
	}
	c.EnsureSame(t)
}

func TestRaft_UserRestore(t *testing.T) {
	cases := []uint64{
		0,
		1,
		2,

		// Snapshots from the future
		100,
		1000,
		10000,
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("case %v", c), func(t *testing.T) {
			snapshotAndRestore(t, c)
		})
	}
}

func TestRaft_SendSnapshotFollower(t *testing.T) {
	// Make the cluster
	conf := inmemConfig(t)
	conf.TrailingLogs = 10
	c := MakeCluster(3, t, conf)
	defer c.Close()

	// Disconnect one follower
	followers := c.Followers()
	leader := c.Leader()
	behind := followers[0]
	c.Disconnect(behind.localAddr)

	// Commit a lot of things
	var future Future
	for i := 0; i < 100; i++ {
		future = leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
	}

	// Wait for the last future to apply
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	} else {
		t.Logf("[INFO] Finished apply without behind follower")
	}

	// Snapshot, this will truncate logs!
	for _, r := range c.rafts {
		future = r.Snapshot()
		// the disconnected node will have nothing to snapshot, so that's expected
		if err := future.Error(); err != nil && err != ErrNothingNewToSnapshot {
			t.Fatalf("err: %v", err)
		}
	}

	// Reconnect the behind node
	c.FullyConnect()

	// Ensure all the logs are the same
	c.EnsureSame(t)
}

func TestRaft_SendSnapshotAndLogsFollower(t *testing.T) {
	// Make the cluster
	conf := inmemConfig(t)
	conf.TrailingLogs = 10
	c := MakeCluster(3, t, conf)
	defer c.Close()

	// Disconnect one follower
	followers := c.Followers()
	leader := c.Leader()
	behind := followers[0]
	c.Disconnect(behind.localAddr)

	// Commit a lot of things
	var future Future
	for i := 0; i < 100; i++ {
		future = leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
	}

	// Wait for the last future to apply
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	} else {
		t.Logf("[INFO] Finished apply without behind follower")
	}

	// Snapshot, this will truncate logs!
	for _, r := range c.rafts {
		future = r.Snapshot()
		// the disconnected node will have nothing to snapshot, so that's expected
		if err := future.Error(); err != nil && err != ErrNothingNewToSnapshot {
			t.Fatalf("err: %v", err)
		}
	}

	// Commit more logs past the snapshot.
	for i := 100; i < 200; i++ {
		future = leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
	}

	// Wait for the last future to apply
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	} else {
		t.Logf("[INFO] Finished apply without behind follower")
	}

	// Reconnect the behind node
	c.FullyConnect()

	// Ensure all the logs are the same
	c.EnsureSame(t)
}

func TestRaft_ReJoinFollower(t *testing.T) {
	// Enable operation after a remove.
	conf := inmemConfig(t)
	conf.ShutdownOnRemove = false
	c := MakeCluster(3, t, conf)
	defer c.Close()

	// Get the leader.
	leader := c.Leader()

	// Wait until we have 2 followers.
	limit := time.Now().Add(c.longstopTimeout)
	var followers []*Raft
	for time.Now().Before(limit) && len(followers) != 2 {
		c.WaitEvent(nil, c.conf.CommitTimeout)
		followers = c.GetInState(Follower)
	}
	if len(followers) != 2 {
		t.Fatalf("expected two followers: %v", followers)
	}

	// Remove a follower.
	follower := followers[0]
	future := leader.RemoveServer(follower.localID, 0, 0)
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Other nodes should have fewer peers.
	time.Sleep(c.propagateTimeout)
	if configuration := c.getConfiguration(leader); len(configuration.Servers) != 2 {
		t.Fatalf("too many peers: %v", configuration)
	}
	if configuration := c.getConfiguration(followers[1]); len(configuration.Servers) != 2 {
		t.Fatalf("too many peers: %v", configuration)
	}

	// Get the leader. We can't use the normal stability checker here because
	// the removed server will be trying to run an election but will be
	// ignored. The stability check will think this is off nominal because
	// the RequestVote RPCs won't stop firing.
	limit = time.Now().Add(c.longstopTimeout)
	var leaders []*Raft
	for time.Now().Before(limit) && len(leaders) != 1 {
		c.WaitEvent(nil, c.conf.CommitTimeout)
		leaders, _ = c.pollState(Leader)
	}
	if len(leaders) != 1 {
		t.Fatalf("expected a leader")
	}
	leader = leaders[0]

	// Rejoin. The follower will have a higher term than the leader,
	// this will cause the leader to step down, and a new round of elections
	// to take place. We should eventually re-stabilize.
	future = leader.AddVoter(follower.localID, follower.localAddr, 0, 0)
	if err := future.Error(); err != nil && err != ErrLeadershipLost {
		t.Fatalf("err: %v", err)
	}

	// We should level back up to the proper number of peers. We add a
	// stability check here to make sure the cluster gets to a state where
	// there's a solid leader.
	leader = c.Leader()
	if configuration := c.getConfiguration(leader); len(configuration.Servers) != 3 {
		t.Fatalf("missing peers: %v", configuration)
	}
	if configuration := c.getConfiguration(followers[1]); len(configuration.Servers) != 3 {
		t.Fatalf("missing peers: %v", configuration)
	}

	// Should be a follower now.
	if follower.State() != Follower {
		t.Fatalf("bad state: %v", follower.State())
	}
}

func TestRaft_LeaderLeaseExpire(t *testing.T) {
	// Make a cluster
	conf := inmemConfig(t)
	c := MakeCluster(2, t, conf)
	defer c.Close()

	// Get the leader
	leader := c.Leader()

	// Wait until we have a followers
	limit := time.Now().Add(c.longstopTimeout)
	var followers []*Raft
	for time.Now().Before(limit) && len(followers) != 1 {
		c.WaitEvent(nil, c.conf.CommitTimeout)
		followers = c.GetInState(Follower)
	}
	if len(followers) != 1 {
		t.Fatalf("expected a followers: %v", followers)
	}

	// Disconnect the follower now
	follower := followers[0]
	t.Logf("[INFO] Disconnecting %v", follower)
	c.Disconnect(follower.localAddr)

	// Watch the leaderCh
	timeout := time.After(conf.LeaderLeaseTimeout * 2)
LOOP:
	for {
		select {
		case v := <-leader.LeaderCh():
			if !v {
				break LOOP
			}
		case <-timeout:
			t.Fatalf("timeout stepping down as leader")
		}
	}

	// Ensure the last contact of the leader is non-zero
	if leader.LastContact().IsZero() {
		t.Fatalf("expected non-zero contact time")
	}

	// Should be no leaders
	if len(c.GetInState(Leader)) != 0 {
		t.Fatalf("expected step down")
	}

	// Verify no further contact
	last := follower.LastContact()
	time.Sleep(c.propagateTimeout)

	// Check that last contact has not changed
	if last != follower.LastContact() {
		t.Fatalf("unexpected further contact")
	}

	// Ensure both have cleared their leader
	if l := leader.Leader(); l != "" {
		t.Fatalf("bad: %v", l)
	}
	if l := follower.Leader(); l != "" {
		t.Fatalf("bad: %v", l)
	}
}

func TestRaft_Barrier(t *testing.T) {
	// Make the cluster
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// Get the leader
	leader := c.Leader()

	// Commit a lot of things
	for i := 0; i < 100; i++ {
		leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
	}

	// Wait for a barrier complete
	barrier := leader.Barrier(0)

	// Wait for the barrier future to apply
	if err := barrier.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Ensure all the logs are the same
	c.EnsureSame(t)
	if len(getMockFSM(c.fsms[0]).logs) != 100 {
		t.Fatalf(fmt.Sprintf("Bad log length: %d", len(getMockFSM(c.fsms[0]).logs)))
	}
}

func TestRaft_VerifyLeader(t *testing.T) {
	// Make the cluster
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// Get the leader
	leader := c.Leader()

	// Verify we are leader
	verify := leader.VerifyLeader()

	// Wait for the verify to apply
	if err := verify.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}
}

func TestRaft_VerifyLeader_Single(t *testing.T) {
	// Make the cluster
	c := MakeCluster(1, t, nil)
	defer c.Close()

	// Get the leader
	leader := c.Leader()

	// Verify we are leader
	verify := leader.VerifyLeader()

	// Wait for the verify to apply
	if err := verify.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}
}

func TestRaft_VerifyLeader_Fail(t *testing.T) {
	// Make a cluster
	conf := inmemConfig(t)
	c := MakeCluster(2, t, conf)
	defer c.Close()

	leader := c.Leader()
	// Remove the leader election notification from the channel buffer
	<-leader.LeaderCh()

	// Wait until we have a followers
	followers := c.Followers()

	// Force follower to different term
	follower := followers[0]
	follower.setCurrentTerm(follower.getCurrentTerm() + 1)

	// Wait for the leader to step down
	select {
	case v := <-leader.LeaderCh():
		if v {
			t.Fatalf("expected the leader to step down")
		}
	case <-time.After(conf.HeartbeatTimeout * 3):
		c.FailNowf("timeout waiting for leader to step down")
	}

	// Verify we are leader
	verify := leader.VerifyLeader()

	if err := verify.Error(); err != ErrNotLeader && err != ErrLeadershipLost {
		t.Fatalf("err: %v", err)
	}

	// Ensure the known leader is cleared
	if l := leader.Leader(); l != "" {
		t.Fatalf("bad: %v", l)
	}
}

func TestRaft_VerifyLeader_PartialConnect(t *testing.T) {
	// Make a cluster
	conf := inmemConfig(t)
	c := MakeCluster(3, t, conf)
	defer c.Close()

	// Get the leader
	leader := c.Leader()

	// Wait until we have a followers
	limit := time.Now().Add(c.longstopTimeout)
	var followers []*Raft
	for time.Now().Before(limit) && len(followers) != 2 {
		c.WaitEvent(nil, c.conf.CommitTimeout)
		followers = c.GetInState(Follower)
	}
	if len(followers) != 2 {
		t.Fatalf("expected two followers but got: %v", followers)
	}

	// Force partial disconnect
	follower := followers[0]
	t.Logf("[INFO] Disconnecting %v", follower)
	c.Disconnect(follower.localAddr)

	// Verify we are leader
	verify := leader.VerifyLeader()

	// Wait for the leader to step down
	if err := verify.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}
}

func TestRaft_NotifyCh(t *testing.T) {
	ch := make(chan bool, 1)
	conf := inmemConfig(t)
	conf.NotifyCh = ch
	c := MakeCluster(1, t, conf)
	defer c.Close()

	// Watch leaderCh for change
	select {
	case v := <-ch:
		if !v {
			t.Fatalf("should become leader")
		}
	case <-time.After(conf.HeartbeatTimeout * 8):
		t.Fatalf("timeout becoming leader")
	}

	// Close the cluster
	c.Close()

	// Watch leaderCh for change
	select {
	case v := <-ch:
		if v {
			t.Fatalf("should step down as leader")
		}
	case <-time.After(conf.HeartbeatTimeout * 6):
		t.Fatalf("timeout on step down as leader")
	}
}

func TestRaft_Voting(t *testing.T) {
	c := MakeCluster(3, t, nil)
	defer c.Close()
	followers := c.Followers()
	ldr := c.Leader()
	ldrT := c.trans[c.IndexOf(ldr)]

	reqVote := RequestVoteRequest{
		RPCHeader:          ldr.getRPCHeader(),
		Term:               ldr.getCurrentTerm() + 10,
		Candidate:          ldrT.EncodePeer(ldr.localID, ldr.localAddr),
		LastLogIndex:       ldr.LastIndex(),
		LastLogTerm:        ldr.getCurrentTerm(),
		LeadershipTransfer: false,
	}
	// a follower that thinks there's a leader should vote for that leader.
	var resp RequestVoteResponse
	if err := ldrT.RequestVote(followers[0].localID, followers[0].localAddr, &reqVote, &resp); err != nil {
		t.Fatalf("RequestVote RPC failed %v", err)
	}
	if !resp.Granted {
		t.Fatalf("expected vote to be granted, but wasn't %+v", resp)
	}
	// a follower that thinks there's a leader shouldn't vote for a different candidate
	reqVote.Candidate = ldrT.EncodePeer(followers[0].localID, followers[0].localAddr)
	if err := ldrT.RequestVote(followers[1].localID, followers[1].localAddr, &reqVote, &resp); err != nil {
		t.Fatalf("RequestVote RPC failed %v", err)
	}
	if resp.Granted {
		t.Fatalf("expected vote not to be granted, but was %+v", resp)
	}
	// a follower that thinks there's a leader, but the request has the leadership transfer flag, should
	// vote for a different candidate
	reqVote.LeadershipTransfer = true
	reqVote.Candidate = ldrT.EncodePeer(followers[0].localID, followers[0].localAddr)
	if err := ldrT.RequestVote(followers[1].localID, followers[1].localAddr, &reqVote, &resp); err != nil {
		t.Fatalf("RequestVote RPC failed %v", err)
	}
	if !resp.Granted {
		t.Fatalf("expected vote to be granted, but wasn't %+v", resp)
	}
}

func TestRaft_ProtocolVersion_RejectRPC(t *testing.T) {
	c := MakeCluster(3, t, nil)
	defer c.Close()
	followers := c.Followers()
	ldr := c.Leader()
	ldrT := c.trans[c.IndexOf(ldr)]

	reqVote := RequestVoteRequest{
		RPCHeader: RPCHeader{
			ProtocolVersion: ProtocolVersionMax + 1,
		},
		Term:         ldr.getCurrentTerm() + 10,
		Candidate:    ldrT.EncodePeer(ldr.localID, ldr.localAddr),
		LastLogIndex: ldr.LastIndex(),
		LastLogTerm:  ldr.getCurrentTerm(),
	}

	// Reject a message from a future version we don't understand.
	var resp RequestVoteResponse
	err := ldrT.RequestVote(followers[0].localID, followers[0].localAddr, &reqVote, &resp)
	if err == nil || !strings.Contains(err.Error(), "protocol version") {
		t.Fatalf("expected RPC to get rejected: %v", err)
	}

	// Reject a message that's too old.
	reqVote.RPCHeader.ProtocolVersion = followers[0].protocolVersion - 2
	err = ldrT.RequestVote(followers[0].localID, followers[0].localAddr, &reqVote, &resp)
	if err == nil || !strings.Contains(err.Error(), "protocol version") {
		t.Fatalf("expected RPC to get rejected: %v", err)
	}
}

func TestRaft_ProtocolVersion_Upgrade_1_2(t *testing.T) {
	// Make a cluster back on protocol version 1.
	conf := inmemConfig(t)
	conf.ProtocolVersion = 1
	c := MakeCluster(2, t, conf)
	defer c.Close()

	// Set up another server speaking protocol version 2.
	conf = inmemConfig(t)
	conf.ProtocolVersion = 2
	c1 := MakeClusterNoBootstrap(1, t, conf)

	// Merge clusters.
	c.Merge(c1)
	c.FullyConnect()

	// Make sure the new ID-based operations aren't supported in the old
	// protocol.
	future := c.Leader().AddNonvoter(c1.rafts[0].localID, c1.rafts[0].localAddr, 0, 1*time.Second)
	if err := future.Error(); err != ErrUnsupportedProtocol {
		t.Fatalf("err: %v", err)
	}
	future = c.Leader().DemoteVoter(c1.rafts[0].localID, 0, 1*time.Second)
	if err := future.Error(); err != ErrUnsupportedProtocol {
		t.Fatalf("err: %v", err)
	}

	// Now do the join using the old address-based API.
	if future := c.Leader().AddPeer(c1.rafts[0].localAddr); future.Error() != nil {
		t.Fatalf("err: %v", future.Error())
	}

	// Sanity check the cluster.
	c.EnsureSame(t)
	c.EnsureSamePeers(t)
	c.EnsureLeader(t, c.Leader().localAddr)

	// Now do the remove using the old address-based API.
	if future := c.Leader().RemovePeer(c1.rafts[0].localAddr); future.Error() != nil {
		t.Fatalf("err: %v", future.Error())
	}
}

func TestRaft_ProtocolVersion_Upgrade_2_3(t *testing.T) {
	// Make a cluster back on protocol version 2.
	conf := inmemConfig(t)
	conf.ProtocolVersion = 2
	c := MakeCluster(2, t, conf)
	defer c.Close()
	oldAddr := c.Followers()[0].localAddr

	// Set up another server speaking protocol version 3.
	conf = inmemConfig(t)
	conf.ProtocolVersion = 3
	c1 := MakeClusterNoBootstrap(1, t, conf)

	// Merge clusters.
	c.Merge(c1)
	c.FullyConnect()

	// Use the new ID-based API to add the server with its ID.
	future := c.Leader().AddVoter(c1.rafts[0].localID, c1.rafts[0].localAddr, 0, 1*time.Second)
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Sanity check the cluster.
	c.EnsureSame(t)
	c.EnsureSamePeers(t)
	c.EnsureLeader(t, c.Leader().localAddr)

	// Remove an old server using the old address-based API.
	if future := c.Leader().RemovePeer(oldAddr); future.Error() != nil {
		t.Fatalf("err: %v", future.Error())
	}
}

func TestRaft_LeadershipTransferInProgress(t *testing.T) {
	r := &Raft{leaderState: leaderState{}}
	r.setupLeaderState()

	if r.getLeadershipTransferInProgress() != false {
		t.Errorf("should be true after setup")
	}

	r.setLeadershipTransferInProgress(true)
	if r.getLeadershipTransferInProgress() != true {
		t.Errorf("should be true because we set it before")
	}
	r.setLeadershipTransferInProgress(false)
	if r.getLeadershipTransferInProgress() != false {
		t.Errorf("should be false because we set it before")
	}
}

func pointerToString(s string) *string {
	return &s
}

func TestRaft_LeadershipTransferPickServer(t *testing.T) {
	type variant struct {
		lastLogIndex int
		servers      map[string]uint64
		expected     *string
	}
	leaderID := "z"
	variants := []variant{
		{lastLogIndex: 10, servers: map[string]uint64{}, expected: nil},
		{lastLogIndex: 10, servers: map[string]uint64{leaderID: 11, "a": 9}, expected: pointerToString("a")},
		{lastLogIndex: 10, servers: map[string]uint64{leaderID: 11, "a": 9, "b": 8}, expected: pointerToString("a")},
		{lastLogIndex: 10, servers: map[string]uint64{leaderID: 11, "c": 9, "b": 8, "a": 8}, expected: pointerToString("c")},
		{lastLogIndex: 10, servers: map[string]uint64{leaderID: 11, "a": 7, "b": 11, "c": 8}, expected: pointerToString("b")},
	}
	for i, v := range variants {
		servers := []Server{}
		replState := map[ServerID]*followerReplication{}
		for id, idx := range v.servers {
			servers = append(servers, Server{ID: ServerID(id)})
			replState[ServerID(id)] = &followerReplication{nextIndex: idx}
		}
		r := Raft{leaderState: leaderState{}, localID: ServerID(leaderID), configurations: configurations{latest: Configuration{Servers: servers}}}
		r.lastLogIndex = uint64(v.lastLogIndex)
		r.leaderState.replState = replState

		actual := r.pickServer()
		if v.expected == nil && actual == nil {
			continue
		} else if v.expected == nil && actual != nil {
			t.Errorf("case %d: actual: %v doesn't match expected: %v", i, actual, v.expected)
		} else if actual == nil && v.expected != nil {
			t.Errorf("case %d: actual: %v doesn't match expected: %v", i, actual, v.expected)
		} else if string(actual.ID) != *v.expected {
			t.Errorf("case %d: actual: %v doesn't match expected: %v", i, actual.ID, *v.expected)
		}
	}
}

func TestRaft_LeadershipTransfer(t *testing.T) {
	c := MakeCluster(3, t, nil)
	defer c.Close()

	oldLeader := string(c.Leader().localID)
	err := c.Leader().LeadershipTransfer()
	if err.Error() != nil {
		t.Fatalf("Didn't expect error: %v", err.Error())
	}
	newLeader := string(c.Leader().localID)
	if oldLeader == newLeader {
		t.Error("Leadership should have been transitioned to another peer.")
	}
}

func TestRaft_LeadershipTransferWithOneNode(t *testing.T) {
	c := MakeCluster(1, t, nil)
	defer c.Close()

	future := c.Leader().LeadershipTransfer()
	if future.Error() == nil {
		t.Fatal("leadership transfer should err")
	}

	expected := "cannot find peer"
	actual := future.Error().Error()
	if !strings.Contains(actual, expected) {
		t.Errorf("leadership transfer should err with: %s", expected)
	}
}

func TestRaft_LeadershipTransferWithSevenNodes(t *testing.T) {
	c := MakeCluster(7, t, nil)
	defer c.Close()

	oldLeader := c.Leader().localID
	follower := c.GetInState(Follower)[0]
	future := c.Leader().LeadershipTransferToServer(follower.localID, follower.localAddr)
	if future.Error() != nil {
		t.Fatalf("Didn't expect error: %v", future.Error())
	}
	if oldLeader == c.Leader().localID {
		t.Error("Leadership should have been transitioned to specified server.")
	}
}

func TestRaft_LeadershipTransferToInvalidID(t *testing.T) {
	c := MakeCluster(3, t, nil)
	defer c.Close()

	future := c.Leader().LeadershipTransferToServer(ServerID("abc"), ServerAddress("localhost"))
	if future.Error() == nil {
		t.Fatal("leadership transfer should err")
	}

	expected := "cannot find replication state"
	actual := future.Error().Error()
	if !strings.Contains(actual, expected) {
		t.Errorf("leadership transfer should err with: %s", expected)
	}
}

func TestRaft_LeadershipTransferToInvalidAddress(t *testing.T) {
	c := MakeCluster(3, t, nil)
	defer c.Close()

	follower := c.GetInState(Follower)[0]
	future := c.Leader().LeadershipTransferToServer(follower.localID, ServerAddress("localhost"))
	if future.Error() == nil {
		t.Fatal("leadership transfer should err")
	}
	expected := "failed to make TimeoutNow RPC"
	actual := future.Error().Error()
	if !strings.Contains(actual, expected) {
		t.Errorf("leadership transfer should err with: %s", expected)
	}
}

func TestRaft_LeadershipTransferToBehindServer(t *testing.T) {
	c := MakeCluster(3, t, nil)
	defer c.Close()

	l := c.Leader()
	behind := c.GetInState(Follower)[0]

	// Commit a lot of things
	for i := 0; i < 1000; i++ {
		l.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
	}

	future := l.LeadershipTransferToServer(behind.localID, behind.localAddr)
	if future.Error() != nil {
		t.Fatalf("This is not supposed to error: %v", future.Error())
	}
	if c.Leader().localID != behind.localID {
		t.Fatal("Behind server did not get leadership")
	}
}

func TestRaft_LeadershipTransferToItself(t *testing.T) {
	c := MakeCluster(3, t, nil)
	defer c.Close()

	l := c.Leader()

	future := l.LeadershipTransferToServer(l.localID, l.localAddr)
	if future.Error() == nil {
		t.Fatal("leadership transfer should err")
	}
	expected := "cannot transfer leadership to itself"
	actual := future.Error().Error()
	if !strings.Contains(actual, expected) {
		t.Errorf("leadership transfer should err with: %s", expected)
	}
}

func TestRaft_LeadershipTransferLeaderRejectsClientRequests(t *testing.T) {
	c := MakeCluster(3, t, nil)
	defer c.Close()
	l := c.Leader()
	l.setLeadershipTransferInProgress(true)

	// tests for API > protocol version 3 is missing here because leadership transfer
	// is only available for protocol version >= 3
	// TODO: is something missing here?
	futures := []Future{
		l.AddNonvoter(ServerID(""), ServerAddress(""), 0, 0),
		l.AddVoter(ServerID(""), ServerAddress(""), 0, 0),
		l.Apply([]byte("test"), 0),
		l.Barrier(0),
		l.DemoteVoter(ServerID(""), 0, 0),

		// the API is tested, but here we are making sure we reject any config change.
		l.requestConfigChange(configurationChangeRequest{}, 100*time.Millisecond),
	}
	futures = append(futures, l.LeadershipTransfer())

	for i, f := range futures {
		t.Logf("waiting on future %v", i)
		if f.Error() != ErrLeadershipTransferInProgress {
			t.Errorf("case %d: should have errored with: %s, instead of %s", i, ErrLeadershipTransferInProgress, f.Error())
		}
	}

	f := l.LeadershipTransferToServer(ServerID(""), ServerAddress(""))
	if f.Error() != ErrLeadershipTransferInProgress {
		t.Errorf("should have errored with: %s, instead of %s", ErrLeadershipTransferInProgress, f.Error())
	}
}

func TestRaft_LeadershipTransferLeaderReplicationTimeout(t *testing.T) {
	c := MakeCluster(3, t, nil)
	defer c.Close()

	l := c.Leader()
	behind := c.GetInState(Follower)[0]

	// Commit a lot of things, so that the timeout can kick in
	for i := 0; i < 10000; i++ {
		l.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
	}

	// set ElectionTimeout really short because this is used to determine
	// how long a transfer is allowed to take.
	cfg := l.config()
	cfg.ElectionTimeout = 1 * time.Nanosecond
	l.conf.Store(cfg)

	future := l.LeadershipTransferToServer(behind.localID, behind.localAddr)
	if future.Error() == nil {
		t.Log("This test is fishing for a replication timeout, but this is not guaranteed to happen.")
	} else {
		expected := "leadership transfer timeout"
		actual := future.Error().Error()
		if !strings.Contains(actual, expected) {
			t.Errorf("leadership transfer should err with: %s", expected)
		}
	}
}

func TestRaft_LeadershipTransferIgnoresNonvoters(t *testing.T) {
	c := MakeCluster(2, t, nil)
	defer c.Close()

	follower := c.Followers()[0]

	demoteFuture := c.Leader().DemoteVoter(follower.localID, 0, 0)
	if demoteFuture.Error() != nil {
		t.Fatalf("demote voter err'd: %v", demoteFuture.Error())
	}

	future := c.Leader().LeadershipTransfer()
	if future.Error() == nil {
		t.Fatal("leadership transfer should err")
	}

	expected := "cannot find peer"
	actual := future.Error().Error()
	if !strings.Contains(actual, expected) {
		t.Errorf("leadership transfer should err with: %s", expected)
	}
}

func TestRaft_LeadershipTransferStopRightAway(t *testing.T) {
	r := Raft{leaderState: leaderState{}}
	r.setupLeaderState()

	stopCh := make(chan struct{})
	doneCh := make(chan error, 1)
	close(stopCh)
	r.leadershipTransfer(ServerID("a"), ServerAddress(""), &followerReplication{}, stopCh, doneCh)
	err := <-doneCh
	if err != nil {
		t.Errorf("leadership shouldn't have started, but instead it error with: %v", err)
	}
}
func TestRaft_GetConfigurationNoBootstrap(t *testing.T) {
	c := MakeCluster(2, t, nil)
	defer c.Close()

	// Should be one leader
	c.Followers()
	leader := c.Leader()
	c.EnsureLeader(t, leader.localAddr)

	// Should be able to apply
	future := leader.Apply([]byte("test"), c.conf.CommitTimeout)
	if err := future.Error(); err != nil {
		t.Fatalf("[ERR] err: %v", err)
	}
	c.WaitForReplication(1)

	// Get configuration via GetConfiguration of a running node
	cfgf := c.rafts[0].GetConfiguration()
	if err := cfgf.Error(); err != nil {
		t.Fatal(err)
	}
	expected := cfgf.Configuration()

	// Obtain the same configuration via GetConfig
	logs := c.stores[0]
	store := c.stores[0]
	snap := c.snaps[0]
	trans := c.trans[0]
	observed, err := GetConfiguration(c.conf, c.fsms[0], logs, store, snap, trans)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(observed, expected) {
		t.Errorf("GetConfiguration result differ from Raft.GetConfiguration: observed %+v, expected %+v", observed, expected)
	}
}

func TestRaft_CacheLogWithStoreError(t *testing.T) {
	c := MakeCluster(2, t, nil)
	defer c.Close()

	// Should be one leader
	follower := c.Followers()[0]
	leader := c.Leader()
	c.EnsureLeader(t, leader.localAddr)

	// There is no lock to protect this assignment I am afraid.
	es := &errorStore{LogStore: follower.logs}
	cl, _ := NewLogCache(100, es)
	follower.logs = cl

	// Commit some logs
	for i := 0; i < 5; i++ {
		future := leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
		if err := future.Error(); err != nil {
			t.Fatalf("[ERR] err: %v", err)
		}
	}

	// Make the next fail
	es.failNext(1)
	leader.Apply([]byte("test6"), 0)

	leader.Apply([]byte("test7"), 0)
	future := leader.Apply([]byte("test8"), 0)

	// Wait for the last future to apply
	if err := future.Error(); err != nil {
		t.Fatalf("[ERR] err: %v", err)
	}

	// Shutdown follower
	if f := follower.Shutdown(); f.Error() != nil {
		t.Fatalf("error shuting down follower: %v", f.Error())
	}

	// Try to restart the follower and make sure it does not fail with a LogNotFound error
	_, trans := NewInmemTransport(follower.localAddr)
	follower.logs = es.LogStore
	conf := follower.config()
	n, err := NewRaft(&conf, &MockFSM{}, follower.logs, follower.stable, follower.snapshots, trans)
	if err != nil {
		t.Fatalf("error restarting follower: %v", err)
	}
	n.Shutdown()
}

func TestRaft_ReloadConfig(t *testing.T) {
	conf := inmemConfig(t)
	c := MakeCluster(1, t, conf)
	defer c.Close()
	raft := c.rafts[0]

	// Make sure the reloadable values are as expected before
	require.Equal(t, uint64(10240), raft.config().TrailingLogs)
	require.Equal(t, 120*time.Second, raft.config().SnapshotInterval)
	require.Equal(t, uint64(8192), raft.config().SnapshotThreshold)

	// Reload with different values
	newCfg := ReloadableConfig{
		TrailingLogs:      12345,
		SnapshotInterval:  234 * time.Second,
		SnapshotThreshold: 6789,
	}

	require.NoError(t, raft.ReloadConfig(newCfg))

	// Now we should have new values
	require.Equal(t, newCfg.TrailingLogs, raft.config().TrailingLogs)
	require.Equal(t, newCfg.SnapshotInterval, raft.config().SnapshotInterval)
	require.Equal(t, newCfg.SnapshotThreshold, raft.config().SnapshotThreshold)
}

func TestRaft_ReloadConfigValidates(t *testing.T) {
	conf := inmemConfig(t)
	c := MakeCluster(1, t, conf)
	defer c.Close()
	raft := c.rafts[0]

	// Make sure the reloadable values are as expected before
	require.Equal(t, uint64(10240), raft.config().TrailingLogs)
	require.Equal(t, 120*time.Second, raft.config().SnapshotInterval)
	require.Equal(t, uint64(8192), raft.config().SnapshotThreshold)

	// Reload with different values that are invalid per ValidateConfig
	newCfg := ReloadableConfig{
		TrailingLogs:      12345,
		SnapshotInterval:  1 * time.Millisecond, // must be >= 5 millisecond
		SnapshotThreshold: 6789,
	}

	require.Error(t, raft.ReloadConfig(newCfg))

	// Now we should have same values
	require.Equal(t, uint64(10240), raft.config().TrailingLogs)
	require.Equal(t, 120*time.Second, raft.config().SnapshotInterval)
	require.Equal(t, uint64(8192), raft.config().SnapshotThreshold)
}

// TODO: These are test cases we'd like to write for appendEntries().
// Unfortunately, it's difficult to do so with the current way this file is
// tested.
//
// Term check:
// - m.term is too small: no-op.
// - m.term is too large: update term, become follower, process request.
// - m.term is right but we're candidate: become follower, process request.
//
// Previous entry check:
// - prev is within the snapshot, before the snapshot's index: assume match.
// - prev is within the snapshot, exactly the snapshot's index: check
//   snapshot's term.
// - prev is a log entry: check entry's term.
// - prev is past the end of the log: return fail.
//
// New entries:
// - new entries are all new: add them all.
// - new entries are all duplicate: ignore them all without ever removing dups.
// - new entries some duplicate, some new: add the new ones without ever
//   removing dups.
// - new entries all conflict: remove the conflicting ones, add their
//   replacements.
// - new entries some duplicate, some conflict: remove the conflicting ones,
//   add their replacement, without ever removing dups.
//
// Storage errors handled properly.
// Commit index updated properly.

func TestRaft_InstallSnapshot_InvalidPeers(t *testing.T) {
	_, transport := NewInmemTransport("")
	r := &Raft{
		trans:  transport,
		logger: hclog.New(nil),
	}

	req := &InstallSnapshotRequest{
		Peers: []byte("invalid msgpack"),
	}
	chResp := make(chan RPCResponse, 1)
	rpc := RPC{
		Reader:   new(bytes.Buffer),
		RespChan: chResp,
	}
	r.installSnapshot(rpc, req)
	resp := <-chResp
	require.Error(t, resp.Error)
	require.Contains(t, resp.Error.Error(), "failed to decode peers")
}

func TestRaft_runFollower_State_Transition(t *testing.T) {
	type fields struct {
		conf     *Config
		servers  []Server
		serverID ServerID
	}
	tests := []struct {
		name          string
		fields        fields
		expectedState RaftState
	}{
		{"NonVoter", fields{conf: DefaultConfig(), servers: []Server{{Nonvoter, "first", ""}}, serverID: "first"}, Follower},
		{"Voter", fields{conf: DefaultConfig(), servers: []Server{{Voter, "first", ""}}, serverID: "first"}, Candidate},
		{"Not in Config", fields{conf: DefaultConfig(), servers: []Server{{Voter, "second", ""}}, serverID: "first"}, Follower},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// set timeout to tests specific
			tt.fields.conf.LocalID = tt.fields.serverID
			tt.fields.conf.HeartbeatTimeout = 50 * time.Millisecond
			tt.fields.conf.ElectionTimeout = 50 * time.Millisecond
			tt.fields.conf.LeaderLeaseTimeout = 50 * time.Millisecond
			tt.fields.conf.CommitTimeout = 5 * time.Millisecond
			tt.fields.conf.SnapshotThreshold = 100
			tt.fields.conf.TrailingLogs = 10
			tt.fields.conf.skipStartup = true

			// Create a raft instance and set the latest configuration
			env1 := MakeRaft(t, tt.fields.conf, false)
			env1.raft.setLatestConfiguration(Configuration{Servers: tt.fields.servers}, 1)
			env1.raft.setState(Follower)

			// run the follower loop exclusively
			go env1.raft.runFollower()

			// wait enough time to have HeartbeatTimeout
			time.Sleep(tt.fields.conf.HeartbeatTimeout * 3)

			// Check the follower loop set the right state
			require.Equal(t, tt.expectedState, env1.raft.getState())
		})
	}
}
