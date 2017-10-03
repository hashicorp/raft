package fuzzy

import (
	"math/rand"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// 5 node cluster where 2 nodes always see a delay in getting a request vote msg.
func TestRaft_SlowSendVote(t *testing.T) {
	hooks := NewSlowVoter("sv_0", "sv_1")
	cluster := newRaftCluster(t, testLogWriter, "sv", 5, hooks)
	s := newApplySource("SlowSendVote")
	ac := cluster.ApplyN(t, time.Minute, s, 10000)
	cluster.Stop(t, time.Minute)
	hooks.Report(t)
	cluster.VerifyLog(t, ac)
	cluster.VerifyFSM(t)
}

// 5 node cluster where vote results from 3 nodes are slow to turn up.
// [they see the vote request normally, but their response is slow]
func TestRaft_SlowRecvVote(t *testing.T) {
	hooks := NewSlowVoter("svr_1", "svr_4", "svr_3")
	hooks.mode = SlowRecv
	cluster := newRaftCluster(t, testLogWriter, "svr", 5, hooks)
	s := newApplySource("SlowRecvVote")
	ac := cluster.ApplyN(t, time.Minute, s, 10000)
	cluster.Stop(t, time.Minute)
	hooks.Report(t)
	cluster.VerifyLog(t, ac)
	cluster.VerifyFSM(t)
}

type SlowVoterMode int

const (
	SlowSend SlowVoterMode = iota
	SlowRecv
)

type SlowVoter struct {
	verifier  appendEntriesVerifier
	slowNodes map[string]bool
	delayMin  time.Duration
	delayMax  time.Duration
	mode      SlowVoterMode
}

func NewSlowVoter(slowNodes ...string) *SlowVoter {
	sv := SlowVoter{
		slowNodes: make(map[string]bool, len(slowNodes)),
		delayMin:  time.Second,
		delayMax:  time.Second * 2,
		mode:      SlowSend,
	}
	for _, n := range slowNodes {
		sv.slowNodes[n] = true
	}
	sv.verifier.Init()
	return &sv
}

func (sv *SlowVoter) Report(t *testing.T) {
	sv.verifier.Report(t)
}

func (sv *SlowVoter) PreRPC(s, t string, r *raft.RPC) error {
	return nil
}

func (sv *SlowVoter) nap() {
	d := sv.delayMin + time.Duration(rand.Int63n((sv.delayMax - sv.delayMin).Nanoseconds()))
	time.Sleep(d)
}

func (sv *SlowVoter) PostRPC(src, target string, r *raft.RPC, res *raft.RPCResponse) error {
	if sv.mode == SlowRecv && sv.slowNodes[target] {
		_, ok := r.Command.(*raft.RequestVoteRequest)
		if ok {
			sv.nap()
		}
	}
	return nil
}

func (sv *SlowVoter) PreRequestVote(src, target string, v *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	if sv.mode == SlowSend && sv.slowNodes[target] {
		sv.nap()
	}
	return nil, nil
}

func (sv *SlowVoter) PreAppendEntries(src, target string, v *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	sv.verifier.PreAppendEntries(src, target, v)
	return nil, nil
}
