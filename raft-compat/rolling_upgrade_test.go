package raft_compat

import (
	raftrs "github.com/dhiayachi/raft"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft/compat/testcluster"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestRaft_RollingUpgrade(t *testing.T) {

	initCount := 3
	rLatest := testcluster.NewRaftCluster[testcluster.RaftLatest](t, initCount, "raftOld")
	configuration := raftrs.Configuration{}

	for i := 0; i < initCount; i++ {
		var err error
		require.NoError(t, err)
		configuration.Servers = append(configuration.Servers, raftrs.Server{
			ID:      raftrs.ServerID(rLatest.ID(i)),
			Address: raftrs.ServerAddress(rLatest.Addr(i)),
		})
	}
	raft0 := rLatest.Raft(0).(*raftrs.Raft)
	boot := raft0.BootstrapCluster(configuration)
	if err := boot.Error(); err != nil {
		t.Fatalf("bootstrap err: %v", err)
	}
	lCh := raft0.LeaderCh()

	after := time.After(5 * time.Second)

	select {
	case <-after:
		t.Fatalf("timedout")
	case <-lCh:
	}
	getLeader := rLatest.GetLeader()
	require.NotNil(t, getLeader)
	a, _ := getLeader.GetRaft().(*raftrs.Raft).LeaderWithID()
	require.NotEmpty(t, a)
	future := getLeader.GetRaft().(*raftrs.Raft).Apply([]byte("test"), time.Second)
	require.NoError(t, future.Error())

	rUIT := testcluster.NewRaftCluster[testcluster.RaftUIT](t, initCount, "raftNew")
	leader, _ := getLeader.GetRaft().(*raftrs.Raft).LeaderWithID()
	require.NotEmpty(t, leader)

	// Upgrade all the followers
	leaderIdx := 0
	for i := 0; i < initCount; i++ {
		if getLeader.GetLocalID() == rLatest.ID(i) {
			leaderIdx = i
			continue
		}

		future := getLeader.GetRaft().(*raftrs.Raft).AddVoter(raftrs.ServerID(rUIT.ID(i)), raftrs.ServerAddress(rUIT.Addr(i)), 0, 0)

		time.Sleep(1 * time.Second)
		require.NoError(t, future.Error())
		// Check Leader haven't changed as we are not replacing the leader
		a, _ := getLeader.GetRaft().(*raftrs.Raft).LeaderWithID()
		require.Equal(t, a, leader)
		getLeader.GetRaft().(*raftrs.Raft).RemoveServer(raftrs.ServerID(rLatest.ID(i)), 0, 0)
		rLatest.Raft(i).(*raftrs.Raft).Shutdown()
	}
	future = getLeader.GetRaft().(*raftrs.Raft).Apply([]byte("test2"), time.Second)
	require.NoError(t, future.Error())

	f := getLeader.GetRaft().(*raftrs.Raft).AddVoter(raftrs.ServerID(rUIT.ID(leaderIdx)), raftrs.ServerAddress(rUIT.Addr(leaderIdx)), 0, 0)
	time.Sleep(1 * time.Second)
	require.NoError(t, f.Error())
	// Check Leader haven't changed as we are not replacing the leader
	a, _ = getLeader.GetRaft().(*raftrs.Raft).LeaderWithID()
	require.Equal(t, a, leader)
	getLeader.GetRaft().(*raftrs.Raft).RemoveServer(raftrs.ServerID(rLatest.ID(leaderIdx)), 0, 0)
	time.Sleep(1 * time.Second)
	rLatest.Raft(leaderIdx).(*raftrs.Raft).Shutdown()
	time.Sleep(1 * time.Second)
	newLeader := rUIT.GetLeader()
	aNew, _ := newLeader.GetRaft().(*raft.Raft).LeaderWithID()
	require.NotEqual(t, aNew, leader)

	require.Equal(t, newLeader.NumLogs(), 2)

}
