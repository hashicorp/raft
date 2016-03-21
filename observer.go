package raft

import (
	"sync/atomic"
)

type Observation struct {
	Raft *Raft
	Data interface{}
}

type LeaderObservation struct {
	leader string
}

var nextObserverId uint64

// Observer describes what to do with a given observation
type Observer struct {
	channel     chan Observation          // channel of observations
	blocking    bool                      // whether it should block in order to write an observation (generally no)
	numObserved uint64                    // number observed
	numDropped  uint64                    // number dropped
	id          uint64                    // ID of this observer in the raft map
	filter      func(o *Observation) bool // filter to apply to determine whether observation should be sent to channel
}

// Register a new observer
func (r *Raft) RegisterObserver(or *Observer) {
	r.observerLock.Lock()
	defer r.observerLock.Unlock()
	r.observers[or.id] = or
}

// Deregister an observer
func (r *Raft) DeregisterObserver(or *Observer) {
	r.observerLock.Lock()
	defer r.observerLock.Unlock()
	delete(r.observers, or.id)
}

// Send an observation to every observer
func (r *Raft) observe(o interface{}) {
	// we hold this mutex whilst observers (potentially) block.
	// In general observers should not block. But in any case this isn't
	// disastrous as we only hold a read lock, which merely prevents
	// registration / deregistration of observers
	ob := Observation{Raft: r, Data: o}
	r.observerLock.RLock()
	defer r.observerLock.RUnlock()
	for _, or := range r.observers {
		if or.filter != nil {
			if !or.filter(&ob) {
				continue
			}
		}
		if or.channel == nil {
			return
		}
		if or.blocking {
			or.channel <- ob
			atomic.AddUint64(&or.numObserved, 1)
		} else {
			select {
			case or.channel <- ob:
				atomic.AddUint64(&or.numObserved, 1)
			default:
				atomic.AddUint64(&or.numDropped, 1)
			}
		}
	}
}

// get performance counters for an observer
func (or *Observer) GetCounters() (uint64, uint64, error) {
	return atomic.LoadUint64(&or.numObserved), atomic.LoadUint64(&or.numDropped), nil
}

// Create a new observer with the specified channel, blocking status, and filter (filter can be nil)
func NewObserver(channel chan Observation, blocking bool, filter func(o *Observation) bool) *Observer {
	ob := &Observer{
		channel:  channel,
		blocking: blocking,
		filter:   filter,
		id:       atomic.AddUint64(&nextObserverId, 1),
	}
	return ob
}
