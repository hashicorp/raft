package raft

import (
	"log"
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

// ExcludeNodeQuorum requires a majority of nodes excluding
// a particular node to agree
type excludeNodeQuorum struct {
	exclude     net.Addr
	count       int
	votesNeeded int
}

func newExcludeNodeQuorum(clusterSize int, exclude net.Addr) *excludeNodeQuorum {
	votesNeeded := ((clusterSize - 1) / 2) + 1
	return &excludeNodeQuorum{exclude: exclude, count: 0, votesNeeded: votesNeeded}
}

func (e *excludeNodeQuorum) Commit(p net.Addr) bool {
	if p.String() == e.exclude.String() {
		log.Printf("[WARN] Excluded vote from %v", p)
		return false
	}
	e.count++
	return e.count >= e.votesNeeded
}

// Inflight is used to track operations that are still in-flight
type inflight struct {
	sync.Mutex
	commitCh   chan *logFuture
	operations map[uint64]*logFuture
}

// NewInflight returns an inflight struct that notifies
// the provided channel when logs are finished commiting.
func NewInflight(commitCh chan *logFuture) *inflight {
	return &inflight{
		commitCh:   commitCh,
		operations: make(map[uint64]*logFuture),
	}
}

// Start is used to mark a logFuture as being inflight
func (i *inflight) Start(l *logFuture) {
	i.Lock()
	defer i.Unlock()
	i.operations[l.log.Index] = l
}

// Cancel is used to cancel all in-flight operations.
// This is done when the leader steps down, and all futures
// are sent the given error.
func (i *inflight) Cancel(err error) {
	i.Lock()
	defer i.Unlock()

	// Respond to all inflight operations
	for _, op := range i.operations {
		op.respond(err)
	}

	// Clear the map
	i.operations = make(map[uint64]*logFuture)
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
		// Stop tracking since it is committed
		delete(i.operations, index)

		// Notify of commit
		i.commitCh <- op
	}
}
