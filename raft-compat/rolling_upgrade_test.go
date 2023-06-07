package raft_compat

import (
	"fmt"
	raftrs "github.com/dhiayachi/raft"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type raftUIT struct {
	raft   *raft.Raft
	trans  *raft.NetworkTransport
	Config *raft.Config
	Store  *raft.InmemStore
	Snap   *raft.InmemSnapshotStore
	id     raft.ServerID
	fsm    *raft.MockFSM
}

func (r raftUIT) getLocalID() string {
	return string(r.id)
}

func (r raftUIT) getLeaderID() string {
	_, id := r.raft.LeaderWithID()
	return string(id)
}

type raftLatest struct {
	raft   *raftrs.Raft
	trans  *raftrs.NetworkTransport
	Config *raftrs.Config
	Store  *raftrs.InmemStore
	Snap   *raftrs.InmemSnapshotStore
	id     raftrs.ServerID
	fsm    *raftrs.MockFSM
}

func (r raftLatest) getLocalID() string {
	return string(r.id)
}

func (r raftLatest) getLeaderID() string {
	_, id := r.raft.LeaderWithID()
	return string(id)
}

type raftNode interface {
	getLocalID() string
	getLeaderID() string
}

type raftCluster[T raftNode] struct {
	rafts []T
}

func newRaftCluster[T raftNode](count int, name string) raftCluster[T] {
	rc := raftCluster[T]{}
	rc.rafts = make([]T, count)
	for i := 0; i < count; i++ {
		initNode(&rc.rafts[i], fmt.Sprintf("%s-%d", name, i))
	}
	return rc
}

func (r *raftCluster[T]) getLeader() T {
	var empty T
	for _, n := range r.rafts {
		if n.getLocalID() == n.getLeaderID() {
			return n
		}
	}
	return empty
}

func initNode(node interface{}, id string) {
	switch node.(type) {
	case *raftLatest:
		initLatest(node.(*raftLatest), id)
	case *raftUIT:
		initUIT(node.(*raftUIT), id)
	default:
		panic("invalid node type")
	}
}

func initUIT(node *raftUIT, id string) {
	node.Config = raft.DefaultConfig()
	node.Config.HeartbeatTimeout = 50 * time.Millisecond
	node.Config.ElectionTimeout = 50 * time.Millisecond
	node.Config.LeaderLeaseTimeout = 50 * time.Millisecond
	node.Config.CommitTimeout = 5 * time.Millisecond
	node.id = raft.ServerID(id)
	node.Config.LocalID = node.id
	node.Store = raft.NewInmemStore()
	node.Snap = raft.NewInmemSnapshotStore()
	node.fsm = &raft.MockFSM{}
}

func initLatest(node *raftLatest, id string) {
	node.Config = raftrs.DefaultConfig()
	node.Config.HeartbeatTimeout = 50 * time.Millisecond
	node.Config.ElectionTimeout = 50 * time.Millisecond
	node.Config.LeaderLeaseTimeout = 50 * time.Millisecond
	node.Config.CommitTimeout = 5 * time.Millisecond
	node.id = raftrs.ServerID(id)
	node.Config.LocalID = node.id
	node.Store = raftrs.NewInmemStore()
	node.Snap = raftrs.NewInmemSnapshotStore()
	node.fsm = &raftrs.MockFSM{}
}

func TestRaft_RollingUpgrade(t *testing.T) {

	initCount := 3
	rLatest := newRaftCluster[raftLatest](initCount, "raftOld")
	configuration := raftrs.Configuration{}

	for i := 0; i < initCount; i++ {
		var err error
		rLatest.rafts[i].trans, err = raftrs.NewTCPTransport("localhost:0", nil, 2, time.Second, nil)
		require.NoError(t, err)
		rLatest.rafts[i].raft, err = raftrs.NewRaft(rLatest.rafts[i].Config, rLatest.rafts[i].fsm, rLatest.rafts[i].Store,
			rLatest.rafts[i].Store, rLatest.rafts[i].Snap, rLatest.rafts[i].trans)
		require.NoError(t, err)
		configuration.Servers = append(configuration.Servers, raftrs.Server{
			ID:      rLatest.rafts[i].id,
			Address: rLatest.rafts[i].trans.LocalAddr(),
		})
	}
	boot := rLatest.rafts[0].raft.BootstrapCluster(configuration)
	if err := boot.Error(); err != nil {
		t.Fatalf("bootstrap err: %v", err)
	}
	lCh := rLatest.rafts[0].raft.LeaderCh()

	after := time.After(5 * time.Second)

	select {
	case <-after:
		t.Fatalf("timedout")
	case <-lCh:
	}
	getLeader := rLatest.getLeader()
	require.NotNil(t, getLeader)
	a := getLeader.raft.Leader()
	require.NotEmpty(t, a)
	future := getLeader.raft.Apply([]byte("test"), time.Second)
	require.NoError(t, future.Error())

	rUIT := newRaftCluster[raftUIT](initCount, "raftNew")
	leader, _ := getLeader.raft.LeaderWithID()
	require.NotEmpty(t, leader)

	// Upgrade all the followers
	leaderIdx := 0
	for i := 0; i < len(rLatest.rafts); i++ {
		if getLeader.getLocalID() == rLatest.rafts[i].getLocalID() {
			leaderIdx = i
			continue
		}
		var err error
		rUIT.rafts[i].trans, err = raft.NewTCPTransport("localhost:0", nil, 2, time.Second, nil)
		require.NoError(t, err)
		rUIT.rafts[i].raft, err = raft.NewRaft(rUIT.rafts[i].Config, rUIT.rafts[i].fsm, rUIT.rafts[i].Store,
			rUIT.rafts[i].Store, rUIT.rafts[i].Snap, rUIT.rafts[i].trans)
		getLeader.raft.AddVoter(raftrs.ServerID(rUIT.rafts[i].getLocalID()), raftrs.ServerAddress(rUIT.rafts[i].trans.LocalAddr()), 0, 0)

		time.Sleep(1 * time.Second)

		// Check Leader haven't changed as we are not replacing the leader
		a := getLeader.raft.Leader()
		require.Equal(t, a, leader)
		getLeader.raft.RemoveServer(rLatest.rafts[i].id, 0, 0)
		rLatest.rafts[i].raft.Shutdown()
	}
	future = getLeader.raft.Apply([]byte("test2"), time.Second)
	require.NoError(t, future.Error())

	rUIT.rafts[leaderIdx].Config = raft.InmemConfig(t)
	rUIT.rafts[leaderIdx].id = raft.ServerID(fmt.Sprintf("newGrpc%d", leaderIdx))
	rUIT.rafts[leaderIdx].Config.LocalID = rUIT.rafts[leaderIdx].id
	rUIT.rafts[leaderIdx].Store = raft.NewInmemStore()
	rUIT.rafts[leaderIdx].Snap = raft.NewInmemSnapshotStore()
	rUIT.rafts[leaderIdx].fsm = &raft.MockFSM{}
	var err error
	rUIT.rafts[leaderIdx].trans, err = raft.NewTCPTransport("localhost:0", nil, 2, time.Second, nil)
	require.NoError(t, err)
	rUIT.rafts[leaderIdx].raft, err = raft.NewRaft(rUIT.rafts[leaderIdx].Config, rUIT.rafts[leaderIdx].fsm, rUIT.rafts[leaderIdx].Store,
		rUIT.rafts[leaderIdx].Store, rUIT.rafts[leaderIdx].Snap, rUIT.rafts[leaderIdx].trans)
	getLeader.raft.AddVoter(raftrs.ServerID(rUIT.rafts[leaderIdx].getLocalID()), raftrs.ServerAddress(rUIT.rafts[leaderIdx].trans.LocalAddr()), 0, 0)
	// Check Leader haven't changed as we are not replacing the leader
	a = getLeader.raft.Leader()
	require.Equal(t, a, leader)
	getLeader.raft.RemoveServer(rLatest.rafts[leaderIdx].id, 0, 0)
	time.Sleep(1 * time.Second)
	rLatest.rafts[leaderIdx].raft.Shutdown()
	time.Sleep(1 * time.Second)
	aNew := rUIT.getLeader().raft.Leader()
	require.NotEqual(t, aNew, leader)

	require.Len(t, rUIT.getLeader().fsm.Logs(), 2)

}
