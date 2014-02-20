package raft

import (
	"fmt"
	"github.com/armon/go-metrics"
	"net"
	"time"
)

const (
	maxHeartbeatBackoff = 256
	maxFailureScale     = 8
	failureWait         = 10 * time.Millisecond
)

type followerReplication struct {
	peer     net.Addr
	inflight *inflight

	stopCh    chan uint64
	triggerCh chan struct{}

	currentTerm uint64
	matchIndex  uint64
	nextIndex   uint64

	lastContact time.Time
	failures    uint64
}

// replicate is a long running routine that is used to manage
// the process of replicating logs to our followers
func (r *Raft) replicate(s *followerReplication) {
	// Start an async heartbeating routing
	stopHeartbeat := make(chan struct{})
	defer close(stopHeartbeat)
	r.goFunc(func() { r.heartbeat(s, stopHeartbeat) })

	shouldStop := false
	for !shouldStop {
		select {
		case maxIndex := <-s.stopCh:
			// Make a best effort to replicate up to this index
			if maxIndex > 0 {
				r.replicateTo(s, maxIndex)
			}
			return
		case <-s.triggerCh:
			shouldStop = r.replicateTo(s, r.getLastLogIndex())
		case <-randomTimeout(r.conf.CommitTimeout):
			shouldStop = r.replicateTo(s, r.getLastLogIndex())
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
	var maxIndex uint64
	var start time.Time
START:
	req = AppendEntriesRequest{
		Term:              s.currentTerm,
		Leader:            r.trans.EncodePeer(r.localAddr),
		LeaderCommitIndex: r.getCommitIndex(),
	}

	// Get the previous log entry based on the nextIndex.
	// Guard for the first index, since there is no 0 log entry
	// Guard against the previous index being a snapshot as well
	if s.nextIndex == 1 {
		req.PrevLogEntry = 0
		req.PrevLogTerm = 0

	} else if (s.nextIndex - 1) == r.getLastSnapshotIndex() {
		req.PrevLogEntry = r.getLastSnapshotIndex()
		req.PrevLogTerm = r.getLastSnapshotTerm()

	} else {
		if err := r.logs.GetLog(s.nextIndex-1, &l); err != nil {
			if err == LogNotFound {
				goto SEND_SNAP
			}
			r.logger.Printf("[ERR] raft: Failed to get log at index %d: %v",
				s.nextIndex-1, err)
			return
		}

		// Set the previous index and term (0 if nextIndex is 1)
		req.PrevLogEntry = l.Index
		req.PrevLogTerm = l.Term
	}

	// Append up to MaxAppendEntries or up to the lastIndex
	req.Entries = make([]*Log, 0, r.conf.MaxAppendEntries)
	maxIndex = min(s.nextIndex+uint64(r.conf.MaxAppendEntries)-1, lastIndex)
	for i := s.nextIndex; i <= maxIndex; i++ {
		oldLog := new(Log)
		if err := r.logs.GetLog(i, oldLog); err != nil {
			if err == LogNotFound {
				goto SEND_SNAP
			}
			r.logger.Printf("[ERR] raft: Failed to get log at index %d: %v", i, err)
			return
		}
		req.Entries = append(req.Entries, oldLog)
	}

	// Make the RPC call
	start = time.Now()
	if err := r.trans.AppendEntries(s.peer, &req, &resp); err != nil {
		r.logger.Printf("[ERR] raft: Failed to AppendEntries to %v: %v", s.peer, err)
		s.failures++
		time.Sleep(backoff(failureWait, s.failures, maxFailureScale))
		return
	}
	s.failures = 0
	metrics.MeasureSince([]string{"raft", "replication", "appendEntries", s.peer.String()}, start)

	// Check for a newer term, stop running
	if resp.Term > req.Term {
		r.logger.Printf("[ERR] raft: peer %v has newer term, stopping replication", s.peer)
		return true
	}

	// Update the last contact
	s.lastContact = time.Now()

	// Update the s based on success
	if resp.Success {
		// Mark any inflight logs as committed
		s.inflight.CommitRange(s.nextIndex, maxIndex, s.peer)

		// Update the indexes
		s.matchIndex = maxIndex
		s.nextIndex = maxIndex + 1
	} else {
		r.logger.Printf("[WARN] raft: AppendEntries to %v rejected, sending older logs", s.peer)
		s.nextIndex = max(min(s.nextIndex-1, resp.LastLog+1), 1)
		s.matchIndex = s.nextIndex - 1
	}

CHECK_MORE:
	// Check if there are more logs to replicate
	if s.nextIndex <= lastIndex {
		goto START
	}
	return

	// SEND_SNAP is used when we fail to get a log, usually because the follower
	// is too far behind, and we must ship a snapshot down instead
SEND_SNAP:
	stop, err := r.sendLatestSnapshot(s)

	// Check if we should stop
	if stop {
		return true
	}

	// Check for an error
	if err != nil {
		r.logger.Printf("[ERR] raft: Failed to send snapshot to %v: %v", s.peer, err)
		return
	}

	// Check if there is more to replicate
	goto CHECK_MORE
}

// sendLatestSnapshot is used to send the latest snapshot we have
// down to our follower
func (r *Raft) sendLatestSnapshot(s *followerReplication) (bool, error) {
	// Get the snapshots
	snapshots, err := r.snapshots.List()
	if err != nil {
		r.logger.Printf("[ERR] raft: Failed to list snapshots: %v", err)
		return false, err
	}

	// Check we have at least a single snapshot
	if len(snapshots) == 0 {
		return false, fmt.Errorf("no snapshots found")
	}

	// Open the most recent snapshot
	snapId := snapshots[0].ID
	meta, snapshot, err := r.snapshots.Open(snapId)
	if err != nil {
		r.logger.Printf("[ERR] raft: Failed to open snapshot %v: %v", snapId, err)
		return false, err
	}
	defer snapshot.Close()

	// Setup the request
	req := InstallSnapshotRequest{
		Term:         s.currentTerm,
		Leader:       r.trans.EncodePeer(r.localAddr),
		LastLogIndex: meta.Index,
		LastLogTerm:  meta.Term,
		Peers:        meta.Peers,
		Size:         meta.Size,
	}

	// Make the call
	start := time.Now()
	var resp InstallSnapshotResponse
	if err := r.trans.InstallSnapshot(s.peer, &req, &resp, snapshot); err != nil {
		r.logger.Printf("[ERR] raft: Failed to install snapshot %v: %v", snapId, err)
		return false, err
	}
	metrics.MeasureSince([]string{"raft", "replication", "installSnapshot", s.peer.String()}, start)

	// Check for a newer term, stop running
	if resp.Term > req.Term {
		r.logger.Printf("[ERR] raft: peer %v has newer term, stopping replication", s.peer)
		return true, nil
	}

	// Update the last contact
	s.lastContact = time.Now()

	// Check for success
	if resp.Success {
		// Mark any inflight logs as committed
		s.inflight.CommitRange(s.matchIndex+1, meta.Index, s.peer)

		// Update the indexes
		s.matchIndex = meta.Index
		s.nextIndex = s.matchIndex + 1
	} else {
		r.logger.Printf("[WARN] raft: InstallSnapshot to %v rejected", s.peer)
	}
	return false, nil
}

// hearbeat is used to periodically invoke AppendEntries on a peer
// to ensure they don't time out. This is done async of replicate(),
// since that routine could potentially be blocked on disk IO
func (r *Raft) heartbeat(s *followerReplication, stopCh chan struct{}) {
	// Use an exponential backoff for retries
	backoff := 1

	req := AppendEntriesRequest{
		Term:   s.currentTerm,
		Leader: r.trans.EncodePeer(r.localAddr),
	}
	var resp AppendEntriesResponse
	for {
		select {
		case <-randomTimeout((r.conf.HeartbeatTimeout / 4) * time.Duration(backoff)):
			start := time.Now()
			if err := r.trans.AppendEntries(s.peer, &req, &resp); err != nil {
				r.logger.Printf("[ERR] raft: Failed to heartbeat to %v: %v", s.peer, err)
				backoff *= 2
				if backoff > maxHeartbeatBackoff {
					backoff = maxHeartbeatBackoff
				}
			} else {
				s.lastContact = time.Now()
				backoff = 1
				metrics.MeasureSince([]string{"raft", "replication", "heartbeat", s.peer.String()}, start)
			}

		case <-stopCh:
			return
		}
	}
}
