// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package fuzzy

import (
	"math/rand"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// 5 node cluster
func TestRaft_FuzzyLeadershipTransfer(t *testing.T) {
	cluster := newRaftCluster(t, testLogWriter, "lt", 5, nil)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	s := newApplySource("LeadershipTransfer")
	data := cluster.generateNApplies(s, uint(r.Intn(10000)))
	futures := cluster.sendNApplies(time.Minute, data)
	if err := cluster.leadershipTransfer(time.Minute).Error(); err != nil {
		t.Fatalf("leadership transfer failed: %v", err)
	}

	data = cluster.generateNApplies(s, uint(r.Intn(10000)))
	futures = append(futures, cluster.sendNApplies(time.Minute, data)...)
	if err := cluster.leadershipTransfer(time.Minute).Error(); err != nil {
		t.Fatalf("leadership transfer failed: %v", err)
	}

	data = cluster.generateNApplies(s, uint(r.Intn(10000)))
	futures = append(futures, cluster.sendNApplies(time.Minute, data)...)
	if err := cluster.leadershipTransfer(time.Minute).Error(); err != nil {
		t.Fatalf("leadership transfer failed: %v", err)
	}

	data = cluster.generateNApplies(s, uint(r.Intn(10000)))
	futures = append(futures, cluster.sendNApplies(time.Minute, data)...)

	ac := cluster.checkApplyFutures(futures)

	cluster.Stop(t, time.Minute)
	cluster.VerifyLog(t, ac)
	cluster.VerifyFSM(t)
}

func TestRaft_FuzzyLeadershipTransferWithoutLeader(t *testing.T) {
	cluster := newRaftCluster(t, testLogWriter, "lt_no_leader", 5, nil)

	if err := cluster.leadershipTransfer(0).Error(); err == nil {
		t.Fatal("expected leadership transfer to fail when no leader can be elected")
	}

	for _, n := range cluster.nodes {
		if err := n.raft.Shutdown().Error(); err != nil {
			t.Fatalf("shutdown failed for %s: %v", n.name, err)
		}
	}
}

type LeadershipTransferMode int

type LeadershipTransfer struct {
	verifier  appendEntriesVerifier
	slowNodes map[string]bool
	delayMin  time.Duration
	delayMax  time.Duration
	mode      LeadershipTransferMode
}

func (lt *LeadershipTransfer) Report(t *testing.T) {
	lt.verifier.Report(t)
}

func (lt *LeadershipTransfer) PreRPC(s, t string, r *raft.RPC) error {
	return nil
}

func (lt *LeadershipTransfer) nap() {
	d := lt.delayMin + time.Duration(rand.Int63n((lt.delayMax - lt.delayMin).Nanoseconds()))
	time.Sleep(d)
}

func (lt *LeadershipTransfer) PostRPC(src, target string, r *raft.RPC, res *raft.RPCResponse) error {
	return nil
}

func (lt *LeadershipTransfer) PreRequestVote(src, target string, v *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	return nil, nil
}

func (lt *LeadershipTransfer) PreAppendEntries(src, target string, v *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	lt.verifier.PreAppendEntries(src, target, v)
	return nil, nil
}
