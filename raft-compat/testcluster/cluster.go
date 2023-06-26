package testcluster

import (
	"fmt"
	"github.com/hashicorp/raft"
	raftlatest "github.com/hashicorp/raft-latest"
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
	raft   *raftlatest.Raft
	trans  *raftlatest.NetworkTransport
	Config *raftlatest.Config
	Store  *raftlatest.InmemStore
	Snap   *raftlatest.InmemSnapshotStore
	id     raftlatest.ServerID
	fsm    *raftlatest.MockFSM
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

//func NewRaftNodeWitStore[T RaftNode](t *testing.T, name string, store *raftlatest.InmemStore) *T {
//	raft := new(T)
//	initNode(t, raft, name, store)
//	return raft
//}

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

//func initNode(t *testing.T, node interface{}, id string, store *raftlatest.InmemStore) {
//	switch node.(type) {
//	case *RaftLatest:
//		initLatest(t, node.(*RaftLatest), id)
//	case *RaftUIT:
//		initUIT(t, node.(*RaftUIT), id, convertInMemStore(store))
//	default:
//		panic("invalid node type")
//	}
//}

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

func InitUITWithStore(t *testing.T, id string, store *raftlatest.InmemStore) RaftNode {
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
	node.Config = raftlatest.DefaultConfig()
	node.Config.HeartbeatTimeout = 50 * time.Millisecond
	node.Config.ElectionTimeout = 50 * time.Millisecond
	node.Config.LeaderLeaseTimeout = 50 * time.Millisecond
	node.Config.CommitTimeout = 5 * time.Millisecond
	node.id = raftlatest.ServerID(id)
	node.Config.LocalID = node.id

	node.Store = raftlatest.NewInmemStore()
	node.Snap = raftlatest.NewInmemSnapshotStore()
	node.fsm = &raftlatest.MockFSM{}
	var err error
	node.trans, err = raftlatest.NewTCPTransport("localhost:0", nil, 2, time.Second, nil)
	require.NoError(t, err)
	node.raft, err = raftlatest.NewRaft(node.Config, node.fsm, node.Store,
		node.Store, node.Snap, node.trans)
	require.NoError(t, err)
	return node
}

func convertLog(ll *raftlatest.Log) *raft.Log {
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

func convertInMemStore(s *raftlatest.InmemStore) *raft.InmemStore {
	ss := raft.NewInmemStore()
	fi, _ := s.FirstIndex()
	li, _ := s.LastIndex()
	for i := fi; i <= li; i++ {
		log := new(raftlatest.Log)
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
