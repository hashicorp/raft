// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package fuzzy

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"

	"github.com/hashicorp/raft"
)

type appliedItem struct {
	index uint64
	data  []byte
}

type cluster struct {
	nodes            []*raftNode
	removedNodes     []*raftNode
	lastApplySuccess raft.ApplyFuture
	lastApplyFailure raft.ApplyFuture
	applied          []appliedItem
	log              Logger
	transports       *transports
	hooks            TransportHooks
}

// Logger is abstract type for debug log messages
type Logger interface {
	Log(v ...interface{})
	Logf(s string, v ...interface{})
}

// LoggerAdapter allows a log.Logger to be used with the local Logger interface
type LoggerAdapter struct {
	log hclog.Logger
}

// Log a message to the contained debug log
func (a *LoggerAdapter) Log(v ...interface{}) {
	a.log.Info(fmt.Sprint(v...))
}

// Logf will record a formatted message to the contained debug log
func (a *LoggerAdapter) Logf(s string, v ...interface{}) {
	a.log.Info(fmt.Sprintf(s, v...))
}

func newRaftCluster(t *testing.T, logWriter io.Writer, namePrefix string, n uint, transportHooks TransportHooks) *cluster {
	res := make([]*raftNode, 0, n)
	names := make([]string, 0, n)
	for i := uint(0); i < n; i++ {
		names = append(names, nodeName(namePrefix, i))
	}
	l := hclog.New(&hclog.LoggerOptions{
		Output: logWriter,
		Level:  hclog.DefaultLevel,
	})
	transports := newTransports(l)
	for _, i := range names {

		r, err := newRaftNode(hclog.New(&hclog.LoggerOptions{
			Name:   i + ":",
			Output: logWriter,
			Level:  hclog.DefaultLevel,
		}), transports, transportHooks, names, i)
		if err != nil {
			t.Fatalf("Unable to create raftNode:%v : %v", i, err)
		}
		res = append(res, r)
	}
	return &cluster{
		nodes:        res,
		removedNodes: make([]*raftNode, 0, n),
		applied:      make([]appliedItem, 0, 1024),
		log:          &LoggerAdapter{l},
		transports:   transports,
		hooks:        transportHooks,
	}
}

func (c *cluster) CreateAndAddNode(t *testing.T, logWriter io.Writer, namePrefix string, nodeNum uint) error {
	name := nodeName(namePrefix, nodeNum)
	rn, err := newRaftNode(hclog.New(&hclog.LoggerOptions{
		Name:   name + ":",
		Output: logWriter,
		Level:  hclog.DefaultLevel,
	}), c.transports, c.hooks, nil, name)
	if err != nil {
		t.Fatalf("Unable to create raftNode:%v : %v", name, err)
	}
	c.nodes = append(c.nodes, rn)
	f := c.Leader(time.Minute).raft.AddVoter(raft.ServerID(name), raft.ServerAddress(name), 0, 0)
	return f.Error()
}

func nodeName(prefix string, num uint) string {
	return fmt.Sprintf("%v_%d", prefix, num)
}

func (c *cluster) RemoveNode(t *testing.T, name string) *raftNode {
	nc := make([]*raftNode, 0, len(c.nodes))
	var nodeToRemove *raftNode
	for _, rn := range c.nodes {
		if rn.name == name {
			nodeToRemove = rn
		} else {
			nc = append(nc, rn)
		}
	}
	if nodeToRemove == nil {
		t.Fatalf("Unable to find node with name '%v' in cluster", name)
	}
	c.log.Logf("Removing node %v from cluster", name)
	c.Leader(time.Minute).raft.RemovePeer(raft.ServerAddress(name)).Error()
	c.nodes = nc
	c.removedNodes = append(c.removedNodes, nodeToRemove)
	return nodeToRemove
}

// Leader returns the node that is currently the Leader, if there is no
// leader this function blocks until a leader is elected (or a timeout occurs)
func (c *cluster) Leader(timeout time.Duration) *raftNode {
	start := time.Now()
	for true {
		for _, n := range c.nodes {
			if n.raft.State() == raft.Leader {
				return n
			}
		}
		if time.Now().Sub(start) > timeout {
			return nil
		}
		time.Sleep(time.Millisecond)
	}
	return nil
}

// containsNode returns true if the slice 'nodes' contains 'n'
func containsNode(nodes []*raftNode, n *raftNode) bool {
	for _, rn := range nodes {
		if rn == n {
			return true
		}
	}
	return false
}

// LeaderPlus returns the leader + n additional nodes from the cluster
// the leader is always the first node in the returned slice.
func (c *cluster) LeaderPlus(n int) []*raftNode {
	r := make([]*raftNode, 0, n+1)
	ldr := c.Leader(time.Second)
	if ldr != nil {
		r = append(r, ldr)
	}
	if len(r) >= n {
		return r
	}
	for _, node := range c.nodes {
		if !containsNode(r, node) {
			r = append(r, node)
			if len(r) >= n {
				return r
			}
		}
	}
	return r
}

func (c *cluster) Stop(t *testing.T, maxWait time.Duration) {
	c.WaitTilUptoDate(t, maxWait)
	for _, n := range c.nodes {
		n.raft.Shutdown()
	}
}

// WaitTilUptoDate blocks until all nodes in the cluster have gotten their
// committedIndex upto the Index from the last successful call to Apply
func (c *cluster) WaitTilUptoDate(t *testing.T, maxWait time.Duration) {
	idx := c.lastApplySuccess.Index()
	start := time.Now()
	for true {
		allAtIdx := true
		for i := 0; i < len(c.nodes); i++ {
			nodeAppliedIdx := c.nodes[i].raft.AppliedIndex()
			if nodeAppliedIdx < idx {
				allAtIdx = false
				break
			} else if nodeAppliedIdx > idx {
				allAtIdx = false
				idx = nodeAppliedIdx
				break
			}
		}
		if allAtIdx {
			t.Logf("All nodes have appliedIndex=%d", idx)
			return
		}
		if time.Now().Sub(start) > maxWait {
			t.Fatalf("Gave up waiting for all nodes to reach raft Index %d, [currently at %v]", idx, c.appliedIndexes())
		}
		time.Sleep(time.Millisecond * 10)
	}
}

func (c *cluster) appliedIndexes() map[string]uint64 {
	r := make(map[string]uint64, len(c.nodes))
	for _, n := range c.nodes {
		r[n.name] = n.raft.AppliedIndex()
	}
	return r
}

func (c *cluster) generateNApplies(s *applySource, n uint) [][]byte {
	data := make([][]byte, n)
	for i := uint(0); i < n; i++ {
		data[i] = s.nextEntry()
	}
	return data
}

func (c *cluster) leadershipTransfer(leaderTimeout time.Duration) raft.Future {
	ldr := c.Leader(leaderTimeout)
	return ldr.raft.LeadershipTransfer()
}

type applyFutureWithData struct {
	future raft.ApplyFuture
	data   []byte
}

func (c *cluster) sendNApplies(leaderTimeout time.Duration, data [][]byte) []applyFutureWithData {
	f := []applyFutureWithData{}

	ldr := c.Leader(leaderTimeout)
	if ldr != nil {
		for _, d := range data {
			f = append(f, applyFutureWithData{future: ldr.raft.Apply(d, time.Second), data: d})
		}
	}
	return f
}

func (c *cluster) checkApplyFutures(futures []applyFutureWithData) uint64 {
	success := uint64(0)
	for _, a := range futures {
		if err := a.future.Error(); err == nil {
			success++
			c.lastApplySuccess = a.future
			c.applied = append(c.applied, appliedItem{a.future.Index(), a.data})
		} else {
			c.lastApplyFailure = a.future
		}
	}
	return success
}

func (c *cluster) ApplyN(t *testing.T, leaderTimeout time.Duration, s *applySource, n uint) uint64 {
	data := c.generateNApplies(s, n)
	futures := c.sendNApplies(leaderTimeout, data)
	return c.checkApplyFutures(futures)
}

func (c *cluster) VerifyFSM(t *testing.T) {
	exp := c.nodes[0].fsm
	expName := c.nodes[0].name
	for i, n := range c.nodes {
		if i > 0 {
			if exp.lastIndex != n.fsm.lastIndex {
				t.Errorf("Node %v FSM lastIndex is %d, but Node %v FSM lastIndex is %d", n.name, n.fsm.lastIndex, expName, exp.lastIndex)
			}
			if exp.lastTerm != n.fsm.lastTerm {
				t.Errorf("Node %v FSM lastTerm is %d, but Node %v FSM lastTerm is %d", n.name, n.fsm.lastTerm, expName, exp.lastTerm)
			}
			if !bytes.Equal(exp.lastHash, n.fsm.lastHash) {
				t.Errorf("Node %v FSM lastHash is %v, but Node %v FSM lastHash is %v", n.name, n.fsm.lastHash, expName, exp.lastHash)
			}
		}
		t.Logf("node %v final FSM hash is %v", n.name, n.fsm.lastHash)
	}
	if t.Failed() {
		c.RecordState(t)
	}
}

func (c *cluster) RecordState(t *testing.T) {
	td, _ := os.MkdirTemp(os.Getenv("TEST_FAIL_DIR"), "failure")
	sd, _ := resolveDirectory("data", false)
	copyDir(td, sd)
	dump := func(n *raftNode) {
		nt := filepath.Join(td, n.name)
		os.Mkdir(nt, 0o777)
		n.fsm.WriteTo(filepath.Join(nt, "fsm.txt"))
		n.transport.DumpLog(nt)
	}
	for _, n := range c.nodes {
		dump(n)
	}
	for _, n := range c.removedNodes {
		dump(n)
	}
	fmt.Printf("State of failing cluster captured in %v", td)
}

func copyDir(target, src string) {
	filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		relPath := path[len(src):]
		if info.IsDir() {
			return os.MkdirAll(filepath.Join(target, relPath), 0o777)
		}
		return copyFile(filepath.Join(target, relPath), path)
	})
}

func copyFile(target, src string) error {
	r, err := os.Open(src)
	if err != nil {
		return err
	}
	defer r.Close()
	w, err := os.Create(target)
	if err != nil {
		return err
	}
	defer w.Close()
	_, err = io.Copy(w, r)
	return err
}

func (c *cluster) VerifyLog(t *testing.T, applyCount uint64) {
	fi, _ := c.nodes[0].store.FirstIndex()
	li, _ := c.nodes[0].store.LastIndex()
	name := c.nodes[0].name
	for _, n := range c.nodes {
		nfi, err := n.store.FirstIndex()
		if err != nil {
			t.Errorf("Failed to get FirstIndex of log for node %v: %v", n.name, err)
			continue
		}
		if nfi != fi {
			t.Errorf("Node %v has FirstIndex of %d but node %v has %d", n.name, nfi, name, fi)
		}
		nli, err := n.store.LastIndex()
		if err != nil {
			t.Errorf("Failed to get LastIndex of log for node %v: %v", n.name, err)
			continue
		}
		if nli != li {
			t.Errorf("Node %v has LastIndex of %d, but node %v has %d", n.name, nli, name, li)
		}
		if nli-nfi < applyCount {
			t.Errorf("Node %v Log contains %d entries, but should contain at least %d", n.name, nli-nfi, applyCount)
			continue
		}
		var term uint64
		for i := fi; i <= li; i++ {
			var nEntry raft.Log
			var n0Entry raft.Log
			if err := c.nodes[0].store.GetLog(i, &n0Entry); err != nil {
				t.Errorf("Failed to log entry %d on node %v: %v", i, name, err)
				continue
			}
			if err := n.store.GetLog(i, &nEntry); err != nil {
				t.Errorf("Failed to log entry at log Index %d on node %v: %v", i, n.name, err)
				continue
			}
			if i != nEntry.Index {
				t.Errorf("Asked for Log Index %d from Store on node %v, but got index %d instead", i, n.name, nEntry.Index)
			}
			if i == fi {
				term = nEntry.Term
			} else {
				if nEntry.Term < term {
					t.Errorf("Node %v, Prior Log Entry was for term %d, but this log entry is for term %d, terms shouldn't go backwards", n.name, term, nEntry.Term)
				}
			}
			term = nEntry.Term
			assertLogEntryEqual(t, n.name, &n0Entry, &nEntry)
		}
		// the above checks the logs between the nodes, also check that the log
		// contains the items that Apply returned success for.
		var entry raft.Log
		for _, ai := range c.applied {
			err := n.store.GetLog(ai.index, &entry)
			if err != nil {
				t.Errorf("Failed to fetch logIndex %d on node %v: %v", ai.index, n.name, err)
			}
			if !bytes.Equal(ai.data, entry.Data) {
				t.Errorf("Client applied %v at index %d, but log for node %v contains %d", ai.data, ai.index, n.name, entry.Data)
			}
		}
	}
}

// assertLogEntryEqual compares the 2 raft Log entries and reports any differences to the supplied testing.T instance
// it return true if the 2 entries are equal, false otherwise.
func assertLogEntryEqual(t *testing.T, node string, exp *raft.Log, act *raft.Log) bool {
	res := true
	if exp.Term != act.Term {
		t.Errorf("Log Entry at Index %d for node %v has mismatched terms %d/%d", exp.Index, node, exp.Term, act.Term)
		res = false
	}
	if exp.Index != act.Index {
		t.Errorf("Node %v, Log Entry should be Index %d,but is %d", node, exp.Index, act.Index)
		res = false
	}
	if exp.Type != act.Type {
		t.Errorf("Node %v, Log Entry at Index %d should have type %v but is %v", node, exp.Index, exp.Type, act.Type)
		res = false
	}
	if !bytes.Equal(exp.Data, act.Data) {
		t.Errorf("Node %v, Log Entry at Index %d should have data %v, but has %v", node, exp.Index, exp.Data, act.Data)
		res = false
	}
	return res
}
