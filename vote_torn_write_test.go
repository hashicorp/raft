// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

// crashMidVote simulates a crash in the middle of persistVote: once armed,
// it lets the first vote write through and fails every write after it,
// whichever order persistVote writes LastVoteTerm and LastVoteCand in.
type crashMidVote struct {
	StableStore

	armed, crashed bool
}

func (s *crashMidVote) write(k []byte, do func() error) error {
	if s.crashed {
		return errors.New("crashed")
	}
	if s.armed && (bytes.Equal(k, keyLastVoteTerm) || bytes.Equal(k, keyLastVoteCand)) {
		s.crashed = true
	}
	return do()
}

func (s *crashMidVote) Set(k, v []byte) error {
	return s.write(k, func() error { return s.StableStore.Set(k, v) })
}

func (s *crashMidVote) SetUint64(k []byte, v uint64) error {
	return s.write(k, func() error { return s.StableStore.SetUint64(k, v) })
}

// A crash inside persistVote can leave LastVoteTerm=T paired with the
// candidate from an older term. If that old candidate then asks for a vote in term T,
// requestVote takes the "duplicate vote" path and grants it without checking
// that the candidate's log is up to date (§5.4.1).
//
// Cluster is {N, X, Z}. N is a real node; X and Z are simulated by sending RPCs.
func TestRaft_TornVoteWrite_SkipsLogCheck(t *testing.T) {
	conf := inmemConfig(t)
	conf.LocalID = "N"
	conf.HeartbeatTimeout, conf.ElectionTimeout = time.Hour, time.Hour // N never campaigns

	nAddr, nTrans := NewInmemTransport("N")
	_, xTrans := NewInmemTransport("X")
	_, zTrans := NewInmemTransport("Z")
	xTrans.Connect("N", nTrans)
	zTrans.Connect("N", nTrans)

	logs, snaps := NewInmemStore(), NewInmemSnapshotStore()
	stable := &crashMidVote{StableStore: NewInmemStore()}

	// Bootstrap writes log idx 1 at term 1.
	BootstrapCluster(conf, logs, stable, snaps, nTrans, Configuration{Servers: []Server{
		{Voter, "N", "N"}, {Voter, "X", "X"}, {Voter, "Z", "Z"},
	}})

	n, _ := NewRaft(conf, &MockFSM{}, logs, stable, snaps, nTrans)

	x := RPCHeader{ProtocolVersion: ProtocolVersionMax, ID: []byte("X"), Addr: []byte("X")}
	z := RPCHeader{ProtocolVersion: ProtocolVersionMax, ID: []byte("Z"), Addr: []byte("Z")}

	var vote RequestVoteResponse
	var app AppendEntriesResponse

	// Term 2: N votes for X.  -> LastVoteTerm=2, LastVoteCand=X
	xTrans.RequestVote("N", "N", &RequestVoteRequest{RPCHeader: x, Term: 2, LastLogIndex: 1, LastLogTerm: 1}, &vote)

	// Term 3: Z is leader and commits idx 2 on {Z, N}. X never sees it.
	zTrans.AppendEntries("N", "N", &AppendEntriesRequest{RPCHeader: z, Term: 3, PrevLogEntry: 1, PrevLogTerm: 1,
		Entries: []*Log{{Index: 2, Term: 3, Type: LogCommand}}, LeaderCommitIndex: 2}, &app)

	// Term 4: Z asks N for a vote. N crashes after the first of its two
	// vote writes. With the current order (term, then candidate) that leaves
	// LastVoteTerm=4, LastVoteCand=X: a term-4 vote for X that never happened.
	stable.armed = true
	zTrans.RequestVote("N", "N", &RequestVoteRequest{RPCHeader: z, Term: 4, LastLogIndex: 2, LastLogTerm: 3}, &vote)
	n.Shutdown().Error()
	stable.armed, stable.crashed = false, false

	// Restart N from the same disk state.
	_, nTrans = NewInmemTransport(nAddr)
	xTrans.Connect("N", nTrans)
	n, _ = NewRaft(conf, &MockFSM{}, logs, stable, snaps, nTrans)
	defer n.Shutdown()

	// Term 4: X asks N for a vote. X's log (idx 1, term 1) is behind N's
	// (idx 2, term 3) and is missing committed idx 2, so N must refuse.
	vote = RequestVoteResponse{}
	xTrans.RequestVote("N", "N", &RequestVoteRequest{RPCHeader: x, Term: 4, LastLogIndex: 1, LastLogTerm: 1}, &vote)
	if vote.Granted {
		t.Fatal("N voted for X, whose log is missing a committed entry; X can now win term 4 with {X, N}")
	}
}
