package testcluster

import (
	"fmt"
	"github.com/hashicorp/raft"
	raftprevious "github.com/hashicorp/raft-previous-version"
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

func (r RaftUIT) GetStore() interface{} {
	return r.Store
}

func (r RaftUIT) GetLocalID() string {
	return string(r.id)
}

func (r RaftUIT) GetLeaderID() string {
	_, id := r.raft.LeaderWithID()
	return string(id)
}

func (r *RaftCluster) ID(i int) string {
	return r.rafts[i].GetLocalID()
}
func (r *RaftCluster) Addr(i int) string {
	return r.rafts[i].GetLocalAddr()
}

func (r *RaftCluster) Raft(id string) interface{} {
	i := r.GetIndex(id)
	return r.rafts[i].GetRaft()
}

func (r *RaftCluster) Store(id string) interface{} {
	i := r.GetIndex(id)
	return r.rafts[i].GetStore()
}

type RaftLatest struct {
	raft   *raftprevious.Raft
	trans  *raftprevious.NetworkTransport
	Config *raftprevious.Config
	Store  *raftprevious.InmemStore
	Snap   *raftprevious.InmemSnapshotStore
	id     raftprevious.ServerID
	fsm    *raftprevious.MockFSM
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
func (r RaftLatest) GetStore() interface{} {
	return r.Store
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
	GetStore() interface{}
	NumLogs() int
}

type RaftCluster struct {
	rafts []RaftNode
}

func NewRaftCluster(t *testing.T, f func(t *testing.T, id string) RaftNode, count int, name string) RaftCluster {
	rc := RaftCluster{}
	rc.rafts = make([]RaftNode, count)
	for i := 0; i < count; i++ {
		rc.rafts[i] = f(t, fmt.Sprintf("%s-%d", name, i))
	}
	return rc
}

func (r *RaftCluster) GetLeader() RaftNode {
	for _, n := range r.rafts {
		if n.GetLocalID() == n.GetLeaderID() {
			return n
		}
	}
	return nil
}

func (r *RaftCluster) Len() int {
	return len(r.rafts)
}

func (r *RaftCluster) AddNode(node RaftNode) {
	r.rafts = append([]RaftNode{node}, r.rafts...)
}

func (r *RaftCluster) DeleteNode(id string) {
	i := r.GetIndex(id)
	r.rafts = append(r.rafts[:i], r.rafts[i+1:]...)
}

func (r *RaftCluster) GetIndex(id string) int {
	i := 0
	for _, r := range r.rafts {
		if r.GetLocalID() == id {
			return i
		}
		i++
	}
	return -1
}

func InitUIT(t *testing.T, id string) RaftNode {
	return InitUITWithStore(t, id, nil)
}

func InitUITWithStore(t *testing.T, id string, store *raftprevious.InmemStore) RaftNode {
	node := RaftUIT{}
	node.Config = raft.DefaultConfig()
	node.Config.HeartbeatTimeout = 50 * time.Millisecond
	node.Config.ElectionTimeout = 50 * time.Millisecond
	node.Config.LeaderLeaseTimeout = 50 * time.Millisecond
	node.Config.CommitTimeout = 5 * time.Millisecond
	node.id = raft.ServerID(id)
	node.Config.LocalID = node.id
	if store != nil {
		node.Store = convertInMemStore(store)
	} else {
		node.Store = raft.NewInmemStore()
	}

	node.Snap = raft.NewInmemSnapshotStore()
	node.fsm = &raft.MockFSM{}
	var err error
	node.trans, err = raft.NewTCPTransport("localhost:0", nil, 2, time.Second, nil)
	require.NoError(t, err)
	node.raft, err = raft.NewRaft(node.Config, node.fsm, node.Store,
		node.Store, node.Snap, node.trans)
	require.NoError(t, err)
	return node
}

func InitLatest(t *testing.T, id string) RaftNode {
	node := RaftLatest{}
	node.Config = raftprevious.DefaultConfig()
	node.Config.HeartbeatTimeout = 50 * time.Millisecond
	node.Config.ElectionTimeout = 50 * time.Millisecond
	node.Config.LeaderLeaseTimeout = 50 * time.Millisecond
	node.Config.CommitTimeout = 5 * time.Millisecond
	node.id = raftprevious.ServerID(id)
	node.Config.LocalID = node.id

	node.Store = raftprevious.NewInmemStore()
	node.Snap = raftprevious.NewInmemSnapshotStore()
	node.fsm = &raftprevious.MockFSM{}
	var err error
	node.trans, err = raftprevious.NewTCPTransport("localhost:0", nil, 2, time.Second, nil)
	require.NoError(t, err)
	node.raft, err = raftprevious.NewRaft(node.Config, node.fsm, node.Store,
		node.Store, node.Snap, node.trans)
	require.NoError(t, err)
	return node
}

func convertLog(ll *raftprevious.Log) *raft.Log {
	l := new(raft.Log)
	l.Index = ll.Index
	l.AppendedAt = ll.AppendedAt
	l.Type = raft.LogType(ll.Type)
	l.Term = ll.Term
	l.Data = ll.Data
	l.Extensions = ll.Extensions
	return l
}

var (
	keyCurrentTerm  = []byte("CurrentTerm")
	keyLastVoteTerm = []byte("LastVoteTerm")
	keyLastVoteCand = []byte("LastVoteCand")
)

func convertInMemStore(s *raftprevious.InmemStore) *raft.InmemStore {
	ss := raft.NewInmemStore()
	fi, _ := s.FirstIndex()
	li, _ := s.LastIndex()
	for i := fi; i <= li; i++ {
		log := new(raftprevious.Log)
		s.GetLog(i, log)
		ss.StoreLog(convertLog(log))
	}

	get, _ := ss.Get(keyCurrentTerm)
	ss.Set(keyCurrentTerm, get)

	get, _ = ss.Get(keyLastVoteTerm)
	ss.Set(keyLastVoteTerm, get)

	get, _ = ss.Get(keyLastVoteCand)
	ss.Set(keyLastVoteCand, get)

	get64, _ := ss.GetUint64(keyCurrentTerm)
	ss.SetUint64(keyCurrentTerm, get64)

	get64, _ = ss.GetUint64(keyLastVoteTerm)
	ss.SetUint64(keyLastVoteTerm, get64)

	get64, _ = ss.GetUint64(keyLastVoteCand)
	ss.SetUint64(keyLastVoteCand, get64)

	return ss
}
