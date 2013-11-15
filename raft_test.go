package raft

import (
	"bytes"
	"fmt"
	"github.com/ugorji/go/codec"
	"io"
	"log"
	"net"
	"os"
	"reflect"
	"testing"
	"time"
)

// MockFSM is an implementation of the FSM interface, and just stores
// the logs sequentially
type MockFSM struct {
	logs [][]byte
}

type MockSnapshot struct {
	logs     [][]byte
	maxIndex int
}

func (m *MockFSM) Apply(log []byte) {
	m.logs = append(m.logs, log)
}

func (m *MockFSM) Snapshot() (FSMSnapshot, error) {
	return &MockSnapshot{m.logs, len(m.logs)}, nil
}

func (m *MockFSM) Restore(inp io.ReadCloser) error {
	defer inp.Close()
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(inp, &hd)

	m.logs = nil
	return dec.Decode(&m.logs)
}

func (m *MockSnapshot) Persist(sink SnapshotSink) error {
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(sink, &hd)
	return enc.Encode(m.logs[:m.maxIndex])
}

func (m *MockSnapshot) Release() {
}

// Return configurations optimized for in-memory
func inmemConfig() *Config {
	conf := DefaultConfig()
	conf.HeartbeatTimeout = 10 * time.Millisecond
	conf.ElectionTimeout = 10 * time.Millisecond
	conf.CommitTimeout = time.Millisecond
	return conf
}

func TestRaft_StartStop(t *testing.T) {
	dir, logs, store := LevelDBTestStore(t)
	defer os.RemoveAll(dir)
	defer logs.Close()
	defer store.Close()

	dir2, snap := FileSnapTest(t)
	defer os.RemoveAll(dir2)

	_, trans := NewInmemTransport()
	fsm := &MockFSM{}
	conf := DefaultConfig()
	peers := &StaticPeers{}

	raft, err := NewRaft(conf, fsm, logs, store, snap, peers, trans)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer raft.Shutdown()
}

func TestRaft_AfterShutdown(t *testing.T) {
	dir, logs, store := LevelDBTestStore(t)
	defer os.RemoveAll(dir)
	defer logs.Close()
	defer store.Close()

	dir2, snap := FileSnapTest(t)
	defer os.RemoveAll(dir2)

	_, trans := NewInmemTransport()
	fsm := &MockFSM{}
	conf := DefaultConfig()
	peers := &StaticPeers{}

	raft, err := NewRaft(conf, fsm, logs, store, snap, peers, trans)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	raft.Shutdown()

	// Everything should fail now
	if f := raft.Apply(nil, 0); f.Error() != RaftShutdown {
		t.Fatalf("should be shutdown: %v", f.Error())
	}
	if f := raft.AddPeer(NewInmemAddr()); f.Error() != RaftShutdown {
		t.Fatalf("should be shutdown: %v", f.Error())
	}
	if f := raft.RemovePeer(NewInmemAddr()); f.Error() != RaftShutdown {
		t.Fatalf("should be shutdown: %v", f.Error())
	}

	// Should be idempotent
	raft.Shutdown()
}

func TestRaft_SingleNode(t *testing.T) {
	dir, logs, store := LevelDBTestStore(t)
	defer os.RemoveAll(dir)
	defer logs.Close()
	defer store.Close()

	dir2, snap := FileSnapTest(t)
	defer os.RemoveAll(dir2)

	_, trans := NewInmemTransport()
	fsm := &MockFSM{}
	conf := inmemConfig()
	peers := &StaticPeers{}

	raft, err := NewRaft(conf, fsm, logs, store, snap, peers, trans)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer raft.Shutdown()

	time.Sleep(conf.HeartbeatTimeout * 3)

	// Should be leader
	if s := raft.State(); s != Leader {
		t.Fatalf("expected leader: %v", s)
	}

	// Should be able to apply
	future := raft.Apply([]byte("test"), time.Millisecond)
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Check that it is applied to the FSM
	if len(fsm.logs) != 1 {
		t.Fatalf("did not apply to FSM!")
	}
}

type cluster struct {
	dirs   []string
	logs   []*LevelDBLogStore
	stores []*LevelDBStableStore
	fsms   []*MockFSM
	snaps  []*FileSnapshotStore
	trans  []*InmemTransport
	rafts  []*Raft
}

func (c *cluster) Merge(other *cluster) {
	c.dirs = append(c.dirs, other.dirs...)
	c.logs = append(c.logs, other.logs...)
	c.stores = append(c.stores, other.stores...)
	c.fsms = append(c.fsms, other.fsms...)
	c.snaps = append(c.snaps, other.snaps...)
	c.trans = append(c.trans, other.trans...)
	c.rafts = append(c.rafts, other.rafts...)
}

func (c *cluster) Close() {
	var futures []ApplyFuture
	for _, r := range c.rafts {
		futures = append(futures, r.Shutdown())
	}

	// Wait for shutdown
	timer := time.AfterFunc(100*time.Millisecond, func() {
		panic("timed out waiting for shutdown")
	})

	for _, f := range futures {
		if err := f.Error(); err != nil {
			panic(fmt.Errorf("shutdown future err: %v", err))
		}
	}
	timer.Stop()

	for _, l := range c.logs {
		l.Close()
	}
	for _, s := range c.stores {
		s.Close()
	}
	for _, d := range c.dirs {
		os.RemoveAll(d)
	}
}

func (c *cluster) GetInState(s RaftState) []*Raft {
	in := make([]*Raft, 0, 1)
	for _, r := range c.rafts {
		if r.State() == s {
			in = append(in, r)
		}
	}
	return in
}

func (c *cluster) Leader() *Raft {
	timeout := time.AfterFunc(250*time.Millisecond, func() {
		panic("timeout waiting for leader")
	})
	defer timeout.Stop()

	for len(c.GetInState(Leader)) < 1 {
		time.Sleep(time.Millisecond)
	}
	leaders := c.GetInState(Leader)
	if len(leaders) != 1 {
		panic(fmt.Errorf("expected one leader: %v", leaders))
	}
	return leaders[0]
}

func (c *cluster) FullyConnect() {
	log.Printf("[WARN] Fully Connecting")
	for i, t1 := range c.trans {
		for j, t2 := range c.trans {
			if i != j {
				t1.Connect(t2.LocalAddr(), t2)
				t2.Connect(t1.LocalAddr(), t1)
			}
		}
	}
}

func (c *cluster) Disconnect(a net.Addr) {
	log.Printf("[WARN] Disconnecting %v", a)
	for _, t := range c.trans {
		if t.localAddr == a {
			t.DisconnectAll()
		} else {
			t.Disconnect(a)
		}
	}
}

func (c *cluster) EnsureSame(t *testing.T) {
	limit := time.Now().Add(200 * time.Millisecond)
	first := c.fsms[0]

CHECK:
	for i, fsm := range c.fsms {
		if i == 0 {
			continue
		}

		if len(first.logs) != len(fsm.logs) {
			if time.Now().After(limit) {
				t.Fatalf("length mismatch: %d %d",
					len(first.logs), len(fsm.logs))
			} else {
				goto WAIT
			}
		}

		for idx := 0; idx < len(first.logs); idx++ {
			if bytes.Compare(first.logs[idx], fsm.logs[idx]) != 0 {
				if time.Now().After(limit) {
					t.Fatalf("log mismatch at index %d", idx)
				} else {
					goto WAIT
				}
			}
		}
	}
	return

WAIT:
	time.Sleep(20 * time.Millisecond)
	goto CHECK
}

func raftToPeerSet(r *Raft) map[string]struct{} {
	peers := make(map[string]struct{})
	peers[r.localAddr.String()] = struct{}{}
	for _, p := range r.peers {
		peers[p.String()] = struct{}{}
	}
	return peers
}

func (c *cluster) EnsureSamePeers(t *testing.T) {
	limit := time.Now().Add(200 * time.Millisecond)
	peerSet := raftToPeerSet(c.rafts[0])

CHECK:
	for i, raft := range c.rafts {
		if i == 0 {
			continue
		}

		otherSet := raftToPeerSet(raft)
		if !reflect.DeepEqual(peerSet, otherSet) {
			if time.Now().After(limit) {
				t.Fatalf("peer mismatch: %v %v", peerSet, otherSet)
			} else {
				goto WAIT
			}
		}
	}
	return

WAIT:
	time.Sleep(20 * time.Millisecond)
	goto CHECK
}

func MakeCluster(n int, t *testing.T, conf *Config) *cluster {
	c := &cluster{}
	peers := make([]net.Addr, 0, n)

	// Setup the stores and transports
	for i := 0; i < n; i++ {
		dir, logs, store := LevelDBTestStore(t)
		c.dirs = append(c.dirs, dir)
		c.logs = append(c.logs, logs)
		c.stores = append(c.stores, store)
		c.fsms = append(c.fsms, &MockFSM{})

		dir2, snap := FileSnapTest(t)
		c.dirs = append(c.dirs, dir2)
		c.snaps = append(c.snaps, snap)

		addr, trans := NewInmemTransport()
		c.trans = append(c.trans, trans)
		peers = append(peers, addr)
	}

	// Wire the transports together
	c.FullyConnect()

	// Create all the rafts
	for i := 0; i < n; i++ {
		if conf == nil {
			conf = inmemConfig()
		}
		logs := c.logs[i]
		store := c.stores[i]
		snap := c.snaps[i]
		trans := c.trans[i]
		peerStore := &StaticPeers{peers}

		raft, err := NewRaft(conf, c.fsms[i], logs, store, snap, peerStore, trans)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		c.rafts = append(c.rafts, raft)
	}

	return c
}

func TestRaft_TripleNode(t *testing.T) {
	// Make the cluster
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// Should be one leader
	leader := c.Leader()

	// Should be able to apply
	future := leader.Apply([]byte("test"), time.Millisecond)
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Wait for replication
	time.Sleep(3 * time.Millisecond)

	// Check that it is applied to the FSM
	for _, fsm := range c.fsms {
		if len(fsm.logs) != 1 {
			t.Fatalf("did not apply to FSM!")
		}
	}
}

func TestRaft_LeaderFail(t *testing.T) {
	// Make the cluster
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// Should be one leader
	leader := c.Leader()

	// Should be able to apply
	future := leader.Apply([]byte("test"), time.Millisecond)
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Wait for replication
	time.Sleep(3 * time.Millisecond)

	// Disconnect the leader now
	log.Printf("[INFO] Disconnecting %v", leader)
	c.Disconnect(leader.localAddr)

	// Wait for two leaders
	limit := time.Now().Add(100 * time.Millisecond)
	var leaders []*Raft
	for time.Now().Before(limit) && len(leaders) != 2 {
		time.Sleep(10 * time.Millisecond)
		leaders = c.GetInState(Leader)
	}
	if len(leaders) != 2 {
		t.Fatalf("expected two leader: %v", leaders)
	}

	// Get the 'new' leader
	var newLead *Raft
	if leaders[0] == leader {
		newLead = leaders[1]
	} else {
		newLead = leaders[0]
	}

	// Ensure the term is greater
	if newLead.getCurrentTerm() <= leader.getCurrentTerm() {
		t.Fatalf("expected newer term!")
	}

	// Apply should work not work on old leader
	future1 := leader.Apply([]byte("fail"), time.Millisecond)

	// Apply should work on newer leader
	future2 := newLead.Apply([]byte("apply"), time.Millisecond)

	// Future2 should work
	if err := future2.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Reconnect the networks
	log.Printf("[INFO] Reconnecting %v", leader)
	c.FullyConnect()

	// Future1 should fail
	if err := future1.Error(); err != LeadershipLost {
		t.Fatalf("err: %v", err)
	}

	// Wait for log replication
	time.Sleep(10 * time.Millisecond)

	// Check two entries are applied to the FSM
	for _, fsm := range c.fsms {
		if len(fsm.logs) != 2 {
			t.Fatalf("did not apply both to FSM! %v", fsm.logs)
		}
		if bytes.Compare(fsm.logs[0], []byte("test")) != 0 {
			t.Fatalf("first entry should be 'test'")
		}
		if bytes.Compare(fsm.logs[1], []byte("apply")) != 0 {
			t.Fatalf("second entry should be 'apply'")
		}
	}
}

func TestRaft_BehindFollower(t *testing.T) {
	// Make the cluster
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// Disconnect one follower
	followers := c.GetInState(Follower)
	behind := followers[0]
	c.Disconnect(behind.localAddr)

	// Commit a lot of things
	leader := c.Leader()
	var future ApplyFuture
	for i := 0; i < 100; i++ {
		future = leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
	}

	// Wait for the last future to apply
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	} else {
		log.Printf("[INFO] Finished apply without behind follower")
	}

	// Reconnect the behind node
	c.FullyConnect()

	// Ensure all the logs are the same
	c.EnsureSame(t)
}

func TestRaft_ApplyNonLeader(t *testing.T) {
	// Make the cluster
	c := MakeCluster(5, t, nil)
	defer c.Close()

	// Wait for a leader
	c.Leader()
	time.Sleep(10 * time.Millisecond)

	// Try to apply to them
	followers := c.GetInState(Follower)
	if len(followers) != 4 {
		t.Fatalf("Expected 4 followers")
	}
	follower := followers[0]

	// Try to apply
	future := follower.Apply([]byte("test"), time.Millisecond)

	if future.Error() != NotLeader {
		t.Fatalf("should not apply on follower")
	}

	// Should be cached
	if future.Error() != NotLeader {
		t.Fatalf("should not apply on follower")
	}
}

func TestRaft_ApplyConcurrent(t *testing.T) {
	// Make the cluster
	conf := inmemConfig()
	conf.HeartbeatTimeout = 100 * time.Millisecond
	c := MakeCluster(3, t, conf)
	defer c.Close()

	// Wait for a leader
	leader := c.Leader()

	applyF := func(i int) {
		future := leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
		if err := future.Error(); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	// Concurrently apply
	for i := 0; i < 100; i++ {
		go applyF(i)
	}

	// Block on the last one
	applyF(100)

	// Check the FSMs
	c.EnsureSame(t)
}

func TestRaft_ApplyConcurrent_Timeout(t *testing.T) {
	// Make the cluster
	conf := inmemConfig()
	conf.HeartbeatTimeout = 100 * time.Millisecond
	c := MakeCluster(1, t, conf)
	defer c.Close()

	// Wait for a leader
	leader := c.Leader()

	// Enough enqueues should cause at least one timeout...
	didTimeout := false
	for i := 0; i < 200; i++ {
		go func(i int) {
			future := leader.Apply([]byte(fmt.Sprintf("test%d", i)), time.Microsecond)
			if future.Error() == EnqueueTimeout {
				didTimeout = true
			}
		}(i)
	}

	// Wait
	time.Sleep(20 * time.Millisecond)

	// Some should have failed
	if !didTimeout {
		t.Fatalf("expected a timeout")
	}
}

func TestRaft_JoinNode(t *testing.T) {
	// Make a cluster
	c := MakeCluster(2, t, nil)
	defer c.Close()

	// Apply a log to this cluster to ensure it is 'newer'
	leader := c.Leader()
	future := leader.Apply([]byte("first"), 0)
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	} else {
		log.Printf("[INFO] Applied log")
	}

	// Make a new cluster of 1
	c1 := MakeCluster(1, t, nil)

	// Merge clusters
	c.Merge(c1)
	c.FullyConnect()

	// Wait until we have 2 leaders
	limit := time.Now().Add(100 * time.Millisecond)
	var leaders []*Raft
	for time.Now().Before(limit) && len(leaders) != 2 {
		time.Sleep(10 * time.Millisecond)
		leaders = c.GetInState(Leader)
	}
	if len(leaders) != 2 {
		t.Fatalf("expected two leader: %v", leaders)
	}

	// Join the new node in
	future = leader.AddPeer(c1.rafts[0].localAddr)
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Wait until we have 2 followers
	limit = time.Now().Add(100 * time.Millisecond)
	var followers []*Raft
	for time.Now().Before(limit) && len(followers) != 2 {
		time.Sleep(10 * time.Millisecond)
		followers = c.GetInState(Follower)
	}
	if len(followers) != 2 {
		t.Fatalf("expected two followers: %v", followers)
	}

	// Check the FSMs
	c.EnsureSame(t)

	// Check the peers
	c.EnsureSamePeers(t)
}

func TestRaft_RemoveFollower(t *testing.T) {
	// Make a cluster
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// Get the leader
	leader := c.Leader()

	// Wait until we have 2 followers
	limit := time.Now().Add(100 * time.Millisecond)
	var followers []*Raft
	for time.Now().Before(limit) && len(followers) != 2 {
		time.Sleep(10 * time.Millisecond)
		followers = c.GetInState(Follower)
	}
	if len(followers) != 2 {
		t.Fatalf("expected two followers: %v", followers)
	}

	// Remove a follower
	follower := followers[0]
	future := leader.RemovePeer(follower.localAddr)
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Wait a while
	time.Sleep(20 * time.Millisecond)

	// Other nodes should have fewer peers
	if len(leader.peers) != 1 {
		t.Fatalf("too many peers")
	}
	if len(followers[1].peers) != 1 {
		t.Fatalf("too many peers")
	}
}

func TestRaft_RemoveLeader(t *testing.T) {
	// Make a cluster
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// Get the leader
	leader := c.Leader()

	// Wait until we have 2 followers
	limit := time.Now().Add(100 * time.Millisecond)
	var followers []*Raft
	for time.Now().Before(limit) && len(followers) != 2 {
		time.Sleep(10 * time.Millisecond)
		followers = c.GetInState(Follower)
	}
	if len(followers) != 2 {
		t.Fatalf("expected two followers: %v", followers)
	}

	// Remove the leader
	leader.RemovePeer(leader.localAddr)

	// Wait a while
	time.Sleep(20 * time.Millisecond)

	// Should have a new leader
	newLeader := c.Leader()

	// Wait a bit for log application
	time.Sleep(20 * time.Millisecond)

	// Other nodes should have fewer peers
	if len(newLeader.peers) != 1 {
		t.Fatalf("too many peers")
	}

	// Old leader should be shutdown
	if leader.State() != Shutdown {
		t.Fatalf("leader should be shutdown")
	}

	// Old leader should have no peers
	if len(leader.peers) != 0 {
		t.Fatalf("leader should have no peers")
	}
}

func TestRaft_RemoveLeader_SplitCluster(t *testing.T) {
	// Enable operation after a remove
	conf := inmemConfig()
	conf.ShutdownOnRemove = false

	// Make a cluster
	c := MakeCluster(3, t, conf)
	defer c.Close()

	// Get the leader
	leader := c.Leader()

	// Remove the leader
	leader.RemovePeer(leader.localAddr)

	// Wait until we have 2 leaders
	limit := time.Now().Add(100 * time.Millisecond)
	var leaders []*Raft
	for time.Now().Before(limit) && len(leaders) != 2 {
		time.Sleep(10 * time.Millisecond)
		leaders = c.GetInState(Leader)
	}
	if len(leaders) != 2 {
		t.Fatalf("expected two leader: %v", leaders)
	}

	// Old leader should have no peers
	if len(leader.peers) != 0 {
		t.Fatalf("leader should have no peers")
	}
}

func TestRaft_AddKnownPeer(t *testing.T) {
	// Make a cluster
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// Get the leader
	leader := c.Leader()
	followers := c.GetInState(Follower)

	// Add a follower
	future := leader.AddPeer(followers[0].localAddr)

	// Should be already added
	if err := future.Error(); err != KnownPeer {
		t.Fatalf("err: %v", err)
	}
}

func TestRaft_RemoveUnknownPeer(t *testing.T) {
	// Make a cluster
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// Get the leader
	leader := c.Leader()

	// Remove unknown
	future := leader.RemovePeer(NewInmemAddr())

	// Should be already added
	if err := future.Error(); err != UnknownPeer {
		t.Fatalf("err: %v", err)
	}
}
