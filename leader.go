package raft

// LeaderState is used to track the additional state
// needed as a leader
type LeaderState struct {
	followers map[string]*FollowerState
}

type FollowerState struct {
	// This is the next index to send
	nextIndex uint64

	// This is the last known replicated index
	replicatedIndex uint64
}
