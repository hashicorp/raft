// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"errors"
	"testing"
)

type failingPipeline struct{ AppendPipeline }

func (failingPipeline) AppendEntries(*AppendEntriesRequest, *AppendEntriesResponse) (AppendFuture, error) {
	return nil, errors.New("failed")
}

// Run with -race: each read races with a peer address change unless it holds peerLock.
func TestFollowerReplication_PeerReadsHoldLock(t *testing.T) {
	_, trans := NewInmemTransport("")
	r := &Raft{trans: trans, logger: newTestLogger(t)}
	r.conf.Store(*DefaultConfig())

	for _, read := range []func(*followerReplication){
		func(s *followerReplication) { next := uint64(1); r.pipelineSend(s, failingPipeline{}, &next, 0) },
		r.handleStaleTerm,
		func(s *followerReplication) { updateLastAppended(s, &AppendEntriesRequest{Entries: []*Log{{}}}) },
	} {
		s := &followerReplication{commitment: newCommitment(nil, Configuration{}, 0), stepDown: make(chan struct{}, 1)}
		done := make(chan struct{})
		go func() {
			defer close(done)
			s.peerLock.Lock()
			s.peer = Server{Address: "new"}
			s.peerLock.Unlock()
		}()
		read(s)
		<-done
	}
}
