package raft_compat

import (
	"github.com/hashicorp/raft"
	raftprevious "github.com/hashicorp/raft-previous-version"
	"github.com/hashicorp/raft/compat/testcluster"
	"github.com/hashicorp/raft/compat/utils"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestRaft_PreVote_BootStrap_PreVote(t *testing.T) {
	leave := func(t *testing.T, cluster testcluster.RaftCluster, id string) {
		if cluster.GetLeader().GetLocalID() == id {
			transfer := cluster.Raft(id).(*raftprevious.Raft).LeadershipTransfer()
			utils.WaitFuture(t, transfer)
		}
		f := cluster.Raft(id).(*raftprevious.Raft).Shutdown()
		utils.WaitFuture(t, f)
	}
	tcs := []struct {
		name           string
		prevoteNum     int
		noprevoteNum   int
		preVoteEnabled bool
		Leave          func(t *testing.T, cluster testcluster.RaftCluster, id string)
	}{
		{"majority latest, prevote off", 1, 2, false, leave},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			initCount := 3
			cluster := testcluster.NewPreviousRaftCluster(t, initCount, "raftNode")
			configuration := raftprevious.Configuration{}

			for i := 0; i < initCount; i++ {
				var err error
				require.NoError(t, err)
				configuration.Servers = append(configuration.Servers, raftprevious.Server{
					ID:      raftprevious.ServerID(cluster.ID(i)),
					Address: raftprevious.ServerAddress(cluster.Addr(i)),
				})
			}
			raft0 := cluster.Raft(cluster.ID(0)).(*raftprevious.Raft)
			boot := raft0.BootstrapCluster(configuration)
			if err := boot.Error(); err != nil {
				t.Fatalf("bootstrap err: %v", err)
			}
			utils.WaitForNewLeader(t, "", cluster)
			getLeader := cluster.GetLeader()
			require.NotEmpty(t, getLeader)
			a, _ := getLeader.GetRaft().(*raftprevious.Raft).LeaderWithID()
			require.NotEmpty(t, a)
			future := getLeader.GetRaft().(*raftprevious.Raft).Apply([]byte("test"), time.Second)
			utils.WaitFuture(t, future)

			leader, _ := getLeader.GetRaft().(*raftprevious.Raft).LeaderWithID()
			require.NotEmpty(t, leader)
			// Upgrade all the followers
			for i := 0; i < initCount; i++ {
				if getLeader.GetLocalID() == cluster.ID(i) {
					continue
				}

				// Check Leader haven't changed
				a, _ := getLeader.GetRaft().(*raftprevious.Raft).LeaderWithID()
				require.Equal(t, a, leader)

				//
				tc.Leave(t, cluster, cluster.ID(i))

				// Keep the store, to be passed to the upgraded node.
				store := cluster.Store(cluster.ID(i))
				id := cluster.ID(i)

				//Delete the node from the cluster
				cluster.DeleteNode(cluster.ID(i))

				//Create an upgraded node with the store
				rUIT := testcluster.InitUITWithStore(t, id, store.(*raftprevious.InmemStore), func(config *raft.Config) {
					config.PreVote = true
				})
				future := getLeader.GetRaft().(*raftprevious.Raft).AddVoter(raftprevious.ServerID(rUIT.GetLocalID()), raftprevious.ServerAddress(rUIT.GetLocalAddr()), 0, 0)
				utils.WaitFuture(t, future)
				//Add the new node to the cluster
				cluster.AddNode(rUIT)
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
			store := cluster.Store(getLeader.GetLocalID())
			id := getLeader.GetLocalID()

			//Remove and shutdown the leader node
			tc.Leave(t, cluster, getLeader.GetLocalID())

			// Delete the old leader node from the cluster
			cluster.DeleteNode(getLeader.GetLocalID())
			oldLeaderID := getLeader.GetLocalID()

			// Wait for a new leader to be elected
			utils.WaitForNewLeader(t, oldLeaderID, cluster)
			getLeader = cluster.GetLeader()
			require.NotEmpty(t, getLeader)

			// Create a new node to replace the deleted one
			rUIT := testcluster.InitUITWithStore(t, id, store.(*raftprevious.InmemStore), func(config *raft.Config) { config.PreVote = true })
			fa := getLeader.GetRaft().(*raft.Raft).AddVoter(raft.ServerID(rUIT.GetLocalID()), raft.ServerAddress(rUIT.GetLocalAddr()), 0, 0)
			utils.WaitFuture(t, fa)

			// Wait for new leader, (this happens because of not having prevote)
			utils.WaitForNewLeader(t, "", cluster)
			newLeader := rUIT.GetLeaderID()
			require.NotEmpty(t, newLeader)

			require.NotEqual(t, newLeader, leader)

			require.Equal(t, rUIT.NumLogs(), 2)
		})
	}

}
