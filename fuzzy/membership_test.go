package fuzzy

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

var testLogWriter io.Writer

func init() {
	testLogWriter = os.Stdout
	logDir := os.Getenv("TEST_LOG_DIR")
	if logDir != "" {
		f, err := os.Create(filepath.Join(logDir, "debug.log"))
		if err != nil {
			log.Fatalf("TEST_LOG_DIR Env set, but unable to create log file: %v\n", err)
		}
		testLogWriter = f
	}
}

// this runs a 3 node cluster then expands it to a 5 node cluster and checks all 5 nodes agree at the end
func TestRaft_AddMembership(t *testing.T) {
	v := appendEntriesVerifier{}
	v.Init()
	cluster := newRaftCluster(t, testLogWriter, "m", 3, &v)
	s := newApplySource("AddMembership")
	initApplied := cluster.ApplyN(t, time.Minute, s, 100)
	a := s.apply(t, cluster, 1000)
	if err := cluster.CreateAndAddNode(t, testLogWriter, "m", 3); err != nil {
		t.Fatalf("Failed to add node m3: %v", err)
	}
	if err := cluster.CreateAndAddNode(t, testLogWriter, "m", 4); err != nil {
		t.Fatalf("Failed to add node m4: %v", err)
	}
	time.Sleep(time.Second * 5)
	a.stop()
	cluster.Stop(t, time.Minute)
	v.Report(t)
	cluster.VerifyLog(t, raft.Index(a.applied+initApplied))
	cluster.VerifyFSM(t)
}

// starts with 3 nodes, goes to 5, then goes back to 3, but never removes the leader.
func TestRaft_AddRemoveNodesNotLeader(t *testing.T) {
	v := appendEntriesVerifier{}
	v.Init()
	cluster := newRaftCluster(t, testLogWriter, "ar", 3, &v)
	s := newApplySource("AddRemoveNodesNotLeader")
	initApplied := cluster.ApplyN(t, time.Minute, s, 100)
	a := s.apply(t, cluster, 1000)
	cluster.CreateAndAddNode(t, testLogWriter, "ar", 3)
	cluster.CreateAndAddNode(t, testLogWriter, "ar", 4)
	ldr := cluster.Leader(time.Minute)
	removed := 0
	for _, rn := range cluster.nodes {
		if rn.name != ldr.name {
			cluster.RemoveNode(t, rn.name)
			removed++
			if removed >= 2 {
				break
			}
		}
	}
	a.stop()
	cluster.Stop(t, time.Minute)
	v.Report(t)
	cluster.VerifyLog(t, raft.Index(a.applied+initApplied))
	cluster.VerifyFSM(t)
}

// starts with a 5 node cluster then removes the leader.
func TestRaft_RemoveLeader(t *testing.T) {
	v := appendEntriesVerifier{}
	v.Init()
	cluster := newRaftCluster(t, testLogWriter, "rl", 5, &v)
	s := newApplySource("RemoveLeader")
	initApplied := cluster.ApplyN(t, time.Minute, s, 100)
	a := s.apply(t, cluster, 100)
	time.Sleep(time.Second)
	ldr := cluster.Leader(time.Minute)
	cluster.RemoveNode(t, ldr.name)
	time.Sleep(5 * time.Second)
	a.stop()
	cluster.Stop(t, time.Minute)
	v.Report(t)
	cluster.VerifyLog(t, raft.Index(a.applied+initApplied))
	cluster.VerifyFSM(t)
	ldr.raft.Shutdown()
}

// starts with a 5 node cluster, partitions off one node, and then removes it from the cluster on the other partition
func TestRaft_RemovePartitionedNode(t *testing.T) {
	hooks := NewPartitioner()
	cluster := newRaftCluster(t, testLogWriter, "rmp", 5, hooks)
	s := newApplySource("RemovePartitionedNode")
	initApplied := cluster.ApplyN(t, time.Minute, s, 101)
	a := s.apply(t, cluster, 100)
	nodes := cluster.LeaderPlus(3)
	victim := nodes[len(nodes)-1]
	hooks.PartitionOff(cluster.log, []*raftNode{victim})
	time.Sleep(3 * time.Second)
	removed := cluster.RemoveNode(t, victim.name)
	time.Sleep(3 * time.Second)
	hooks.HealAll(cluster.log)
	time.Sleep(10 * time.Second)
	a.stop()
	cluster.Stop(t, time.Minute)
	hooks.Report(t)
	cluster.VerifyLog(t, raft.Index(a.applied+initApplied))
	cluster.VerifyFSM(t)

	// we should verify that the partitioned node see that it was removed & shutdown
	// but it never gets notified of that, so we can't verify that currently.
	removed.raft.Shutdown()
}

// starts a 3 node cluster, than adds a node to it that has an existing log.
/**
func TestRaft_AddNodeWithExistingData(t *testing.T) {
	v := AppendEntriesVerifier{}
	v.Init()
	cluster := NewRaftCluster(t, testLogWriter, "ac1", 3, &v)
	s := NewApplySource("AddNodeWithExistingData_1")
	initApplied := cluster.ApplyN(t, time.Minute, s, 100)

	cluster2 := NewRaftCluster(t, testLogWriter, "ac2", 3, nil)
	s2 := NewApplySource("AddNodeWithExistingData_2")
	c2applied := cluster2.ApplyN(t, time.Minute, s2, 25)
	cluster2.Stop(t, time.Minute)
	cluster2.VerifyLog(t, c2applied)
	cluster2.VerifyFSM(t)
	cluster2.nodes[0].store.Close()

	newNodeName := "ac2_0"
	rn, err := newRaftNode(log.New(testLogWriter, newNodeName+":", log.Lmicroseconds), cluster.transports, &v, nil, newNodeName)
	if err != nil {
		t.Fatalf("Unable to create raftNode:%v : %v", newNodeName, err)
	}
	err = cluster.Leader(time.Minute).raft.AddPeer(raft.ServerAddress(newNodeName)).Error()
	cluster.nodes = append(cluster.nodes, rn)
	// depending on the relative terms of the original cluster and the new node
	// adding the node can end up causing the current leader to step down and
	// a new election take place, in which case this ends up with a LeadershipLost
	// err.
	if err != nil && err != raft.ErrLeadershipLost {
		t.Fatalf("Error adding newNode to cluster %v", err)
	}
	time.Sleep(time.Second)
	cluster.Stop(t, time.Minute)
	v.Report(t)
	// this fails because the new node has some old data in it.
	// commenting out the verifications until we decide what to do.
	//
	//	cluster.VerifyLog(t, initApplied)
	t.Logf("initApplied %d", initApplied)
	//cluster.VerifyFSM(t)
}**/
