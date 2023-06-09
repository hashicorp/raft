package raft_compat

import (
	"github.com/hashicorp/raft"
	raftlatest "github.com/hashicorp/raft-latest"
	"github.com/hashicorp/raft/compat/testcluster"
	"github.com/hashicorp/raft/compat/utils"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestRaft_RollingUpgrade(t *testing.T) {

	initCount := 3
	rLatest := testcluster.NewRaftCluster[testcluster.RaftLatest](t, initCount, "raftOld")
	configuration := raftlatest.Configuration{}

	for i := 0; i < initCount; i++ {
		var err error
		require.NoError(t, err)
		configuration.Servers = append(configuration.Servers, raftlatest.Server{
			ID:      raftlatest.ServerID(rLatest.ID(i)),
			Address: raftlatest.ServerAddress(rLatest.Addr(i)),
		})
	}
	raft0 := rLatest.Raft(0).(*raftlatest.Raft)
	boot := raft0.BootstrapCluster(configuration)
	if err := boot.Error(); err != nil {
		t.Fatalf("bootstrap err: %v", err)
	}
	utils.WaitForNewLeader[testcluster.RaftLatest](t, "", rLatest)
	getLeader := rLatest.GetLeader()
	require.NotEmpty(t, getLeader)
	a, _ := getLeader.GetRaft().(*raftlatest.Raft).LeaderWithID()
	require.NotEmpty(t, a)
	future := getLeader.GetRaft().(*raftlatest.Raft).Apply([]byte("test"), time.Second)
	utils.WaitFuture(t, future)

	rUIT := testcluster.NewRaftCluster[testcluster.RaftUIT](t, initCount, "raftNew")
	leader, _ := getLeader.GetRaft().(*raftlatest.Raft).LeaderWithID()
	require.NotEmpty(t, leader)

	// Upgrade all the followers
	leaderIdx := 0
	for i := 0; i < initCount; i++ {
		if getLeader.GetLocalID() == rLatest.ID(i) {
			leaderIdx = i
			continue
		}

		future := getLeader.GetRaft().(*raftlatest.Raft).AddVoter(raftlatest.ServerID(rUIT.ID(i)), raftlatest.ServerAddress(rUIT.Addr(i)), 0, 0)

		utils.WaitFuture(t, future)
		// Check Leader haven't changed as we are not replacing the leader
		a, _ := getLeader.GetRaft().(*raftlatest.Raft).LeaderWithID()
		require.Equal(t, a, leader)
		getLeader.GetRaft().(*raftlatest.Raft).RemoveServer(raftlatest.ServerID(rLatest.ID(i)), 0, 0)
		rLatest.Raft(i).(*raftlatest.Raft).Shutdown()
	}
	future = getLeader.GetRaft().(*raftlatest.Raft).Apply([]byte("test2"), time.Second)
	require.NoError(t, future.Error())

	fa := getLeader.GetRaft().(*raftlatest.Raft).AddVoter(raftlatest.ServerID(rUIT.ID(leaderIdx)), raftlatest.ServerAddress(rUIT.Addr(leaderIdx)), 0, 0)
	utils.WaitFuture(t, fa)

	// Check Leader haven't changed as we are not replacing the leader
	a, _ = getLeader.GetRaft().(*raftlatest.Raft).LeaderWithID()
	require.Equal(t, a, leader)
	fr := getLeader.GetRaft().(*raftlatest.Raft).RemoveServer(raftlatest.ServerID(rLatest.ID(leaderIdx)), 0, 0)
	utils.WaitFuture(t, fr)
	rLatest.Raft(leaderIdx).(*raftlatest.Raft).Shutdown()
	utils.WaitForNewLeader[testcluster.RaftUIT](t, getLeader.GetLocalID(), rUIT)
	newLeader := rUIT.GetLeader()
	require.NotEmpty(t, newLeader)
	aNew, _ := newLeader.GetRaft().(*raft.Raft).LeaderWithID()
	require.NotEqual(t, aNew, leader)

	require.Equal(t, newLeader.NumLogs(), 2)

}
