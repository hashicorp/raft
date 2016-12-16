package raft

// RegisterObserver registers a new observer.
// Possible types of data sent are *RequestVoteRequest and RaftState.
func (r *raftServer) registerObserver(or chan<- interface{}) {
	r.observerLock.Lock()
	defer r.observerLock.Unlock()
	if r.observer != nil {
		close(r.observer)
	}
	r.observer = or
}

// observe sends an observation to every observer.
func (r *raftServer) observe(o interface{}) {
	r.observerLock.RLock()
	defer r.observerLock.RUnlock()
	select {
	case r.observer <- o:
	default:
	}
}
