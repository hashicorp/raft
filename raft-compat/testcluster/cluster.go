package testcluster

import (
	"fmt"
	"github.com/hashicorp/raft"
	raftrs "github.com/hashicorp/raft-latest"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type RaftUIT struct {
	raft   *raft.Raft
	trans  *raft.NetworkTransport
	Config *raft.Config
	Store  *raft.InmemStore
	Snap   *raft.InmemSnapshotStore
	id     raft.ServerID
	fsm    *raft.MockFSM
}

func (r RaftUIT) NumLogs() int {
	return len(r.fsm.Logs())
}

func (r RaftUIT) GetLocalAddr() string {
	return string(r.trans.LocalAddr())
}

func (r RaftUIT) GetRaft() interface{} {
	return r.raft
}

func (r RaftUIT) GetLocalID() string {
	return string(r.id)
}

func (r RaftUIT) GetLeaderID() string {
	_, id := r.raft.LeaderWithID()
	return string(id)
}

func (r *RaftCluster[T]) ID(i int) string {
	return r.rafts[i].GetLocalID()
}
func (r *RaftCluster[T]) Addr(i int) string {
	return r.rafts[i].GetLocalAddr()
}

func (r *RaftCluster[T]) Raft(i int) interface{} {
	return r.rafts[i].GetRaft()
}

type RaftLatest struct {
	raft   *raftrs.Raft
	trans  *raftrs.NetworkTransport
	Config *raftrs.Config
	Store  *raftrs.InmemStore
	Snap   *raftrs.InmemSnapshotStore
	id     raftrs.ServerID
	fsm    *raftrs.MockFSM
}

func (r RaftLatest) NumLogs() int {
	return len(r.fsm.Logs())
}

func (r RaftLatest) GetLocalAddr() string {
	return string(r.trans.LocalAddr())
}

func (r RaftLatest) GetRaft() interface{} {
	return r.raft
}

func (r RaftLatest) GetLocalID() string {
	return string(r.id)
}

func (r RaftLatest) GetLeaderID() string {
	_, id := r.raft.LeaderWithID()
	return string(id)
}

type RaftNode interface {
	GetLocalID() string
	GetLocalAddr() string
	GetLeaderID() string
	GetRaft() interface{}
	NumLogs() int
}

type RaftCluster[T RaftNode] struct {
	rafts []T
}

func NewRaftCluster[T RaftNode](t *testing.T, count int, name string) RaftCluster[T] {
	rc := RaftCluster[T]{}
	rc.rafts = make([]T, count)
	for i := 0; i < count; i++ {
		initNode(t, &rc.rafts[i], fmt.Sprintf("%s-%d", name, i))
	}
	return rc
}

func (r *RaftCluster[T]) GetLeader() T {
	var empty T
	for _, n := range r.rafts {
		if n.GetLocalID() == n.GetLeaderID() {
			return n
		}
	}
	return empty
}

func (r *RaftCluster[T]) Len() int {
	return len(r.rafts)
}

func initNode(t *testing.T, node interface{}, id string) {
	switch node.(type) {
	case *RaftLatest:
		initLatest(t, node.(*RaftLatest), id)
	case *RaftUIT:
		initUIT(t, node.(*RaftUIT), id)
	default:
		panic("invalid node type")
	}
}

func initUIT(t *testing.T, node *RaftUIT, id string) {
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
	var err error
	node.trans, err = raft.NewTCPTransport("localhost:0", nil, 2, time.Second, nil)
	require.NoError(t, err)
	node.raft, err = raft.NewRaft(node.Config, node.fsm, node.Store,
		node.Store, node.Snap, node.trans)
	require.NoError(t, err)
}

func initLatest(t *testing.T, node *RaftLatest, id string) {
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
	var err error
	node.trans, err = raftrs.NewTCPTransport("localhost:0", nil, 2, time.Second, nil)
	require.NoError(t, err)
	node.raft, err = raftrs.NewRaft(node.Config, node.fsm, node.Store,
		node.Store, node.Snap, node.trans)
	require.NoError(t, err)
}
