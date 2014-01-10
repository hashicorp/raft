package raft

import (
	"fmt"
	"net"
	"sync"
)

// QuorumPolicy allows individual logFutures to have different
// commitment rules while still using the inflight mechanism
type quorumPolicy interface {
	// Checks if a commit from a given peer is enough to
	// satisfy the commitment rules
	Commit(net.Addr) bool
}

// MajorityQuorum is used by Apply transactions and requires
// a simple majority of nodes
type majorityQuorum struct {
	count       int
	votesNeeded int
}

func newMajorityQuorum(clusterSize int) *majorityQuorum {
	votesNeeded := (clusterSize / 2) + 1
	return &majorityQuorum{count: 0, votesNeeded: votesNeeded}
}

func (m *majorityQuorum) Commit(p net.Addr) bool {
	m.count++
	return m.count >= m.votesNeeded
}

// Inflight is used to track operations that are still in-flight
type inflight struct {
	sync.Mutex
	commitCh   chan *logFuture
	minCommit  uint64
	maxCommit  uint64
	operations map[uint64]*logFuture
	stopCh     chan struct{}
}

// NewInflight returns an inflight struct that notifies
// the provided channel when logs are finished commiting.
func newInflight(commitCh chan *logFuture) *inflight {
	return &inflight{
		commitCh:   commitCh,
		minCommit:  0,
		maxCommit:  0,
		operations: make(map[uint64]*logFuture),
		stopCh:     make(chan struct{}),
	}
}

// Start is used to mark a logFuture as being inflight
func (i *inflight) Start(l *logFuture) {
	i.Lock()
	defer i.Unlock()

	idx := l.log.Index
	i.operations[idx] = l

	if idx > i.maxCommit {
		i.maxCommit = idx
	}
	if i.minCommit == 0 {
		i.minCommit = idx
	}
}

// Cancel is used to cancel all in-flight operations.
// This is done when the leader steps down, and all futures
// are sent the given error.
func (i *inflight) Cancel(err error) {
	// Close the channel first to unblock any pending commits
	close(i.stopCh)

	// Lock after close to avoid deadlock
	i.Lock()
	defer i.Unlock()

	// Respond to all inflight operations
	for _, op := range i.operations {
		op.respond(err)
	}

	// Clear the map
	i.operations = make(map[uint64]*logFuture)

	// Close the commmitCh
	close(i.commitCh)

	// Reset indexes
	i.minCommit = 0
	i.maxCommit = 0
}

// Commit is used by leader replication routines to indicate that
// a follower was finished commiting a log to disk.
func (i *inflight) Commit(index uint64, peer net.Addr) {
	i.Lock()
	defer i.Unlock()

	op, ok := i.operations[index]
	if !ok {
		// Ignore if not in the map, as it may be commited already
		return
	}

	// Check if we've satisfied the commit
	if op.policy.Commit(peer) {
		// Sanity check for sequential commit
		if index != i.minCommit {
			panic(fmt.Sprintf("Non-sequential commit of %d, min index %d, max index %d",
				index, i.minCommit, i.maxCommit))
		}

		// Notify of commit
		select {
		case i.commitCh <- op:
			// Stop tracking since it is committed
			delete(i.operations, index)

			// Update the indexes
			if index == i.maxCommit {
				i.minCommit = 0
				i.maxCommit = 0

			} else {
				i.minCommit++
			}

		case <-i.stopCh:
		}
	}
}

// CommitRange is used to commit a range of indexes inclusively
// It optimized to avoid commits for indexes that are not tracked
func (i *inflight) CommitRange(minIndex, maxIndex uint64, peer net.Addr) {
	i.Lock()
	minInflight := i.minCommit
	i.Unlock()

	// Update the minimum index
	minIndex = max(minInflight, minIndex)

	// Commit each index
	for idx := minIndex; idx <= maxIndex; idx++ {
		i.Commit(idx, peer)
	}
}
