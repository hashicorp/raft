// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

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
	tcs := []struct {
		Name  string
		Leave func(t *testing.T, cluster testcluster.RaftCluster, id string)
	}{
		{
			Name: "leave before shutdown",
			Leave: func(t *testing.T, cluster testcluster.RaftCluster, id string) {
				fr := cluster.GetLeader().GetRaft().(*raftprevious.Raft).RemoveServer(raftprevious.ServerID(id), 0, 0)
				utils.WaitFuture(t, fr)
				f := cluster.Raft(id).(*raftprevious.Raft).Shutdown()
				utils.WaitFuture(t, f)
			},
		},
		{
			Name: "leader transfer",
			Leave: func(t *testing.T, cluster testcluster.RaftCluster, id string) {
				if cluster.GetLeader().GetLocalID() == id {
					transfer := cluster.Raft(id).(*raftprevious.Raft).LeadershipTransfer()
					utils.WaitFuture(t, transfer)
					utils.WaitForNewLeader(t, id, cluster)
				}
				switch cluster.GetLeader().GetRaft().(type) {
				case *raftprevious.Raft:
					fr := cluster.GetLeader().GetRaft().(*raftprevious.Raft).RemoveServer(raftprevious.ServerID(id), 0, 0)
					utils.WaitFuture(t, fr)
					f := cluster.Raft(id).(*raftprevious.Raft).Shutdown()
					utils.WaitFuture(t, f)
				case *raft.Raft:
					fr := cluster.GetLeader().GetRaft().(*raft.Raft).RemoveServer(raft.ServerID(id), 0, 0)
					utils.WaitFuture(t, fr)
					f := cluster.Raft(id).(*raftprevious.Raft).Shutdown()
					utils.WaitFuture(t, f)
				}

			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.Name, func(t *testing.T) {
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
			leaderIdx := 0

			followers := make([]string, 0)
			for i := 0; i < initCount; i++ {
				if getLeader.GetLocalID() == cluster.ID(i) {
					leaderIdx = i
					continue
				}
				followers = append(followers, cluster.ID(i))
			}

			for _, f := range followers {
				require.NotEqual(t, f, getLeader.GetLocalID())
				// Check Leader haven't changed
				a, _ := getLeader.GetRaft().(*raftprevious.Raft).LeaderWithID()
				require.Equal(t, a, leader)

				//Create an upgraded node with the store
				rUIT := testcluster.InitUIT(t, fmt.Sprintf("%s-new", f))
				future := getLeader.GetRaft().(*raftprevious.Raft).AddVoter(raftprevious.ServerID(rUIT.GetLocalID()), raftprevious.ServerAddress(rUIT.GetLocalAddr()), 0, 0)
				utils.WaitFuture(t, future)

				//Add the new node to the cluster

				tc.Leave(t, cluster, f)

				//Delete the node from the cluster
				cluster.AddNode(rUIT)
				cluster.DeleteNode(f)
			}

			// Wait enough to have the configuration propagated.
			time.Sleep(time.Second)

			//Apply some logs
			future = getLeader.GetRaft().(*raftprevious.Raft).Apply([]byte("test2"), time.Second)
			require.NoError(t, future.Error())

			// Check Leader haven't changed as we haven't replaced the leader yet
			a, _ = getLeader.GetRaft().(*raftprevious.Raft).LeaderWithID()
			require.Equal(t, a, leader)

			//Remove and shutdown the leader node
			tc.Leave(t, cluster, getLeader.GetLocalID())

			// Delete the old leader node from the cluster

			oldLeaderID := getLeader.GetLocalID()

			// Wait for a new leader to be elected
			utils.WaitForNewLeader(t, oldLeaderID, cluster)
			getLeader = cluster.GetLeader()
			require.NotEmpty(t, getLeader)

			// Create a new node to replace the deleted one
			rUIT := testcluster.InitUIT(t, fmt.Sprintf("raftNew-%d", leaderIdx))
			fa := getLeader.GetRaft().(*raft.Raft).AddVoter(raft.ServerID(rUIT.GetLocalID()), raft.ServerAddress(rUIT.GetLocalAddr()), 0, 0)
			utils.WaitFuture(t, fa)

			// Wait for new leader, (this happens because of not having prevote)
			utils.WaitForNewLeader(t, "", cluster)
			newLeader := rUIT.GetLeaderID()
			require.NotEmpty(t, newLeader)
			require.NotEqual(t, newLeader, leader)

			cluster.DeleteNode(getLeader.GetLocalID())
			require.Equal(t, rUIT.NumLogs(), 2)
		})
	}
}

// TestRaft_ReplaceUpgrade This test perform a rolling upgrade by removing an old node,
// and create a new node with the same store until all old nodes are cycled to new nodes.
// This simulate the advised way of upgrading in Consul.
func TestRaft_ReplaceUpgrade(t *testing.T) {

	tcs := []struct {
		Name  string
		Leave func(t *testing.T, cluster testcluster.RaftCluster, id string)
	}{
		{
			Name: "leave before shutdown",
			Leave: func(t *testing.T, cluster testcluster.RaftCluster, id string) {
				fr := cluster.GetLeader().GetRaft().(*raftprevious.Raft).RemoveServer(raftprevious.ServerID(id), 0, 0)
				utils.WaitFuture(t, fr)
				f := cluster.Raft(id).(*raftprevious.Raft).Shutdown()
				utils.WaitFuture(t, f)
			},
		},
		{
			Name: "shutdown without leave",
			Leave: func(t *testing.T, cluster testcluster.RaftCluster, id string) {
				f := cluster.Raft(id).(*raftprevious.Raft).Shutdown()
				utils.WaitFuture(t, f)
			},
		},
		{
			Name: "leader transfer",
			Leave: func(t *testing.T, cluster testcluster.RaftCluster, id string) {
				if cluster.GetLeader().GetLocalID() == id {
					transfer := cluster.Raft(id).(*raftprevious.Raft).LeadershipTransfer()
					utils.WaitFuture(t, transfer)
				}
				f := cluster.Raft(id).(*raftprevious.Raft).Shutdown()
				utils.WaitFuture(t, f)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.Name, func(t *testing.T) {
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
				rUIT := testcluster.InitUITWithStore(t, id, store.(*raftprevious.InmemStore), func(config *raft.Config) {})
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
			rUIT := testcluster.InitUITWithStore(t, id, store.(*raftprevious.InmemStore), func(config *raft.Config) {})
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
