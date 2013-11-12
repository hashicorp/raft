package raft

import (
	"log"
	"net"
)

type followerReplication struct {
	peer     net.Addr
	inflight *inflight

	stopCh    chan struct{}
	triggerCh chan struct{}

	matchIndex uint64
	nextIndex  uint64
}

// replicate is a long running routine that is used to manage
// the process of replicating logs to our followers
func (r *Raft) replicate(s *followerReplication) {
	shouldStop := false
	for !shouldStop {
		select {
		case <-s.triggerCh:
			shouldStop = r.replicateTo(s, r.getLastLog())
		case <-randomTimeout(r.conf.CommitTimeout):
			shouldStop = r.replicateTo(s, r.getLastLog())
		case <-s.stopCh:
			return
		}
	}
}

// replicateTo is used to replicate the logs up to a given last index.
// If the follower log is behind, we take care to bring them up to date
func (r *Raft) replicateTo(s *followerReplication, lastIndex uint64) (shouldStop bool) {
	// Create the base request
	var l Log
	var req AppendEntriesRequest
	var resp AppendEntriesResponse
START:
	req = AppendEntriesRequest{
		Term:              r.getCurrentTerm(),
		LeaderId:          r.candidateId(),
		LeaderCommitIndex: r.getCommitIndex(),
	}

	// Get the previous log entry based on the nextIndex.
	// Guard for the first index, since there is no 0 log entry
	if s.nextIndex > 1 {
		if err := r.logs.GetLog(s.nextIndex-1, &l); err != nil {
			log.Printf("[ERR] Failed to get log at index %d: %v",
				s.nextIndex-1, err)
			return
		}

		// Set the previous index and term (0 if nextIndex is 1)
		req.PrevLogEntry = l.Index
		req.PrevLogTerm = l.Term
	} else {
		req.PrevLogEntry = 0
		req.PrevLogTerm = 0
	}

	// Append up to MaxAppendEntries or up to the lastIndex
	req.Entries = make([]*Log, 0, 16)
	maxIndex := min(s.nextIndex+uint64(r.conf.MaxAppendEntries)-1, lastIndex)
	for i := s.nextIndex; i <= maxIndex; i++ {
		oldLog := new(Log)
		if err := r.logs.GetLog(i, oldLog); err != nil {
			log.Printf("[ERR] Failed to get log at index %d: %v", i, err)
			return
		}
		req.Entries = append(req.Entries, oldLog)
	}

	// Make the RPC call
	if err := r.trans.AppendEntries(s.peer, &req, &resp); err != nil {
		log.Printf("[ERR] Failed to AppendEntries to %v: %v", s.peer, err)
		return
	}

	// Check for a newer term, stop running
	if resp.Term > req.Term {
		return true
	}

	// Update the s based on success
	if resp.Success {
		// Mark any inflight logs as committed
		for i := s.matchIndex; i <= maxIndex; i++ {
			s.inflight.Commit(i)
		}

		s.matchIndex = maxIndex
		s.nextIndex = maxIndex + 1
	} else {
		log.Printf("[WARN] AppendEntries to %v rejected, sending older logs", s.peer)
		s.nextIndex = max(min(s.nextIndex-1, resp.LastLog+1), 1)
		s.matchIndex = s.nextIndex - 1
	}

	// Check if there are more logs to replicate
	if s.nextIndex <= lastIndex {
		goto START
	}
	return
}
