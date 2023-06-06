package raft_compat

import (
	"fmt"
	raftrs "github.com/dhiayachi/raft"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestRaft_RollingUpgrade_NoLeaderUpgrade(t *testing.T) {

	initCount := 3
	rafts := make([]*raft.Raft, initCount)
	trans := make([]*raft.NetworkTransport, initCount)
	confs := make([]*raft.Config, initCount)
	stores := make([]*raft.InmemStore, initCount)
	snaps := make([]*raft.InmemSnapshotStore, initCount)
	id := make([]raft.ServerID, initCount)
	configuration := raft.Configuration{}

	for i := 0; i < initCount; i++ {
		confs[i] = raft.InmemConfig(t)
		id[i] = raft.ServerID(fmt.Sprintf("grpc%d", i))
		confs[i].LocalID = id[i]
		stores[i] = raft.NewInmemStore()
		snaps[i] = raft.NewInmemSnapshotStore()
		var err error
		trans[i], err = raft.NewTCPTransport("localhost:0", nil, 2, time.Second, nil)
		require.NoError(t, err)
		rafts[i], err = raft.NewRaft(confs[i], &raft.MockFSM{}, stores[i], stores[i], snaps[i], trans[i])
		require.NoError(t, err)
		configuration.Servers = append(configuration.Servers, raft.Server{
			ID:      id[i],
			Address: trans[i].LocalAddr(),
		})
	}
	boot := rafts[0].BootstrapCluster(configuration)
	if err := boot.Error(); err != nil {
		t.Fatalf("bootstrap err: %v", err)
	}
	lCh := rafts[0].LeaderCh()

	after := time.After(time.Second)

	select {
	case <-after:
		t.Fatalf("timedout")
	case <-lCh:
	}
	a := rafts[0].Leader()
	require.NotEmpty(t, a)
	future := rafts[0].Apply([]byte("test"), time.Second)
	require.NoError(t, future.Error())

	newRafts := make([]*raftrs.Raft, initCount)
	newTrans := make([]*raftrs.NetworkTransport, initCount)
	newConfs := make([]*raftrs.Config, initCount)
	newStores := make([]*raftrs.InmemStore, initCount)
	newSnaps := make([]*raftrs.InmemSnapshotStore, initCount)
	leader, _ := rafts[0].LeaderWithID()
	require.NotEmpty(t, leader)
	for i := 1; i < len(rafts); i++ {
		newConfs[i] = raftrs.DefaultConfig()
		newConfs[i].LocalID = raftrs.ServerID(fmt.Sprintf("grpc%d", i))
		newStores[i] = raftrs.NewInmemStore()
		newSnaps[i] = raftrs.NewInmemSnapshotStore()
		var err error
		newTrans[i], err = raftrs.NewTCPTransport("localhost:0", nil, 2, time.Second, nil)
		require.NoError(t, err)
		newRafts[i], err = raftrs.NewRaft(newConfs[i], &raftrs.MockFSM{}, newStores[i], newStores[i], newSnaps[i], newTrans[i])
		rafts[0].AddVoter("grpc4", raft.ServerAddress(newTrans[i].LocalAddr()), 0, 0)

		time.Sleep(1 * time.Second)

		// Check Leader haven't changed as we are not replacing the leader
		a := rafts[0].Leader()
		require.Equal(t, a, leader)
		rafts[0].RemoveServer(id[i], 0, 0)
		rafts[i].Shutdown()

	}
	future = rafts[0].Apply([]byte("test2"), time.Second)
	require.NoError(t, future.Error())

}
