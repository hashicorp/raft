package raft_compat

import (
	"fmt"
	"github.com/hashicorp/raft"
	raftprevious "github.com/hashicorp/raft-previous-version"
	"github.com/hashicorp/raft/compat/testcluster"
	"github.com/hashicorp/raft/compat/utils"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// TestRaft_RollingUpgrade This test perform a rolling upgrade by adding a new node,
// wait for it to join the cluster and remove one of the old nodes, until all nodes
// are cycled
func TestRaft_RollingUpgrade(t *testing.T) {

	initCount := 3
	rLatest := testcluster.NewRaftCluster(t, testcluster.InitLatest, initCount, "raftOld")
	configuration := raftprevious.Configuration{}

	for i := 0; i < initCount; i++ {
		var err error
		require.NoError(t, err)
		configuration.Servers = append(configuration.Servers, raftprevious.Server{
			ID:      raftprevious.ServerID(rLatest.ID(i)),
			Address: raftprevious.ServerAddress(rLatest.Addr(i)),
		})
	}
	raft0 := rLatest.Raft(rLatest.ID(0)).(*raftprevious.Raft)
	boot := raft0.BootstrapCluster(configuration)
	if err := boot.Error(); err != nil {
		t.Fatalf("bootstrap err: %v", err)
	}
	utils.WaitForNewLeader(t, "", rLatest)
	getLeader := rLatest.GetLeader()
	require.NotEmpty(t, getLeader)
	a, _ := getLeader.GetRaft().(*raftprevious.Raft).LeaderWithID()
	require.NotEmpty(t, a)
	future := getLeader.GetRaft().(*raftprevious.Raft).Apply([]byte("test"), time.Second)
	utils.WaitFuture(t, future)

	rUIT := testcluster.NewRaftCluster(t, testcluster.InitUIT, initCount, "raftNew")
	leader, _ := getLeader.GetRaft().(*raftprevious.Raft).LeaderWithID()
	require.NotEmpty(t, leader)

	// Upgrade all the followers
	leaderIdx := 0
	for i := 0; i < initCount; i++ {
		if getLeader.GetLocalID() == rLatest.ID(i) {
			leaderIdx = i
			continue
		}

		future := getLeader.GetRaft().(*raftprevious.Raft).AddVoter(raftprevious.ServerID(rUIT.ID(i)), raftprevious.ServerAddress(rUIT.Addr(i)), 0, 0)

		utils.WaitFuture(t, future)
		// Check Leader haven't changed as we are not replacing the leader
		a, _ := getLeader.GetRaft().(*raftprevious.Raft).LeaderWithID()
		require.Equal(t, a, leader)
		getLeader.GetRaft().(*raftprevious.Raft).RemoveServer(raftprevious.ServerID(rLatest.ID(i)), 0, 0)
		rLatest.Raft(rLatest.ID(i)).(*raftprevious.Raft).Shutdown()
	}
	future = getLeader.GetRaft().(*raftprevious.Raft).Apply([]byte("test2"), time.Second)
	require.NoError(t, future.Error())

	fa := getLeader.GetRaft().(*raftprevious.Raft).AddVoter(raftprevious.ServerID(rUIT.ID(leaderIdx)), raftprevious.ServerAddress(rUIT.Addr(leaderIdx)), 0, 0)
	utils.WaitFuture(t, fa)

	// Check Leader haven't changed as we haven't replaced it yet
	a, _ = getLeader.GetRaft().(*raftprevious.Raft).LeaderWithID()
	require.Equal(t, a, leader)
	fr := getLeader.GetRaft().(*raftprevious.Raft).RemoveServer(raftprevious.ServerID(rLatest.ID(leaderIdx)), 0, 0)
	utils.WaitFuture(t, fr)
	rLatest.Raft(getLeader.GetLocalID()).(*raftprevious.Raft).Shutdown()
	utils.WaitForNewLeader(t, getLeader.GetLocalID(), rUIT)
	newLeader := rUIT.GetLeader()
	require.NotEmpty(t, newLeader)
	aNew, _ := newLeader.GetRaft().(*raft.Raft).LeaderWithID()
	require.NotEqual(t, aNew, leader)

	require.Equal(t, newLeader.NumLogs(), 2)

}

// TestRaft_ReplaceUpgrade This test perform a rolling upgrade by removing an old node,
// and create a new node with the same store until all old nodes are cycled to new nodes.
// This simulate the advised way of upgrading in Consul.
func TestRaft_ReplaceUpgrade(t *testing.T) {

	initCount := 3
	rLatest := testcluster.NewRaftCluster(t, testcluster.InitLatest, initCount, "raftOld")
	configuration := raftprevious.Configuration{}

	for i := 0; i < initCount; i++ {
		var err error
		require.NoError(t, err)
		configuration.Servers = append(configuration.Servers, raftprevious.Server{
			ID:      raftprevious.ServerID(rLatest.ID(i)),
			Address: raftprevious.ServerAddress(rLatest.Addr(i)),
		})
	}
	raft0 := rLatest.Raft(rLatest.ID(0)).(*raftprevious.Raft)
	boot := raft0.BootstrapCluster(configuration)
	if err := boot.Error(); err != nil {
		t.Fatalf("bootstrap err: %v", err)
	}
	utils.WaitForNewLeader(t, "", rLatest)
	getLeader := rLatest.GetLeader()
	require.NotEmpty(t, getLeader)
	a, _ := getLeader.GetRaft().(*raftprevious.Raft).LeaderWithID()
	require.NotEmpty(t, a)
	future := getLeader.GetRaft().(*raftprevious.Raft).Apply([]byte("test"), time.Second)
	utils.WaitFuture(t, future)

	leader, _ := getLeader.GetRaft().(*raftprevious.Raft).LeaderWithID()
	require.NotEmpty(t, leader)
	// Upgrade all the followers
	leaderIdx := 0
	for i := 0; i < initCount; i++ {
		if getLeader.GetLocalID() == rLatest.ID(i) {
			leaderIdx = i
			continue
		}

		// Check Leader haven't changed
		a, _ := getLeader.GetRaft().(*raftprevious.Raft).LeaderWithID()
		require.Equal(t, a, leader)

		//
		getLeader.GetRaft().(*raftprevious.Raft).RemoveServer(raftprevious.ServerID(rLatest.ID(i)), 0, 0)
		rLatest.Raft(rLatest.ID(i)).(*raftprevious.Raft).Shutdown()

		// Keep the store, to be passed to the upgraded node.
		store := rLatest.Store(rLatest.ID(i))

		//Delete the node from the cluster
		rLatest.DeleteNode(rLatest.ID(i))

		//Create an upgraded node with the store
		rUIT := testcluster.InitUITWithStore(t, fmt.Sprintf("New-Raft-%d", i), store.(*raftprevious.InmemStore))
		future := getLeader.GetRaft().(*raftprevious.Raft).AddVoter(raftprevious.ServerID(rUIT.GetLocalID()), raftprevious.ServerAddress(rUIT.GetLocalAddr()), 0, 0)
		utils.WaitFuture(t, future)
		//Add the new node to the cluster
		rLatest.AddNode(rUIT)
	}

	// Wait enough to have the configuration propagated.
	time.Sleep(time.Second)

	//Apply some logs
	future = getLeader.GetRaft().(*raftprevious.Raft).Apply([]byte("test2"), time.Second)
	require.NoError(t, future.Error())

	// Check Leader haven't changed as we haven't replaced the leader yet
	a, _ = getLeader.GetRaft().(*raftprevious.Raft).LeaderWithID()
	require.Equal(t, a, leader)

	// keep a reference to the store
	store := rLatest.Store(getLeader.GetLocalID())

	//Remove and shutdown the leader node
	fr := getLeader.GetRaft().(*raftprevious.Raft).RemoveServer(raftprevious.ServerID(getLeader.GetLocalID()), 0, 0)
	utils.WaitFuture(t, fr)
	rLatest.Raft(getLeader.GetLocalID()).(*raftprevious.Raft).Shutdown()

	// Delete the old leader node from the cluster
	rLatest.DeleteNode(getLeader.GetLocalID())
	oldLeaderID := getLeader.GetLocalID()

	// Wait for a new leader to be elected
	utils.WaitForNewLeader(t, oldLeaderID, rLatest)
	getLeader = rLatest.GetLeader()
	require.NotEmpty(t, getLeader)

	// Create a new node to replace the deleted one
	rUIT := testcluster.InitUITWithStore(t, fmt.Sprintf("New-Raft-%d", leaderIdx), store.(*raftprevious.InmemStore))
	fa := getLeader.GetRaft().(*raft.Raft).AddVoter(raft.ServerID(rUIT.GetLocalID()), raft.ServerAddress(rUIT.GetLocalAddr()), 0, 0)
	utils.WaitFuture(t, fa)

	// Wait for new leader, (this happens because of not having prevote)
	utils.WaitForNewLeader(t, "", rLatest)
	newLeader := rUIT.GetLeaderID()
	require.NotEmpty(t, newLeader)

	require.NotEqual(t, newLeader, leader)

	require.Equal(t, rUIT.NumLogs(), 2)
}
