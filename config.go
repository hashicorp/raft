package raft

import (
	"time"
)

// Config provides any necessary configuraiton to
// the Raft server
type Config struct {
	// Time in follower state without a leader before we attempt an election
	HeartbeatTimeout time.Duration

	// Time in candidate state without a leader before we attempt an election
	ElectionTimeout time.Duration

	// Time without an Apply() operation before we heartbeat to ensure
	// a timely commit. Should be far less than HeartbeatTimeout to ensure
	// we don't lose leadership.
	CommitTimeout time.Duration

	// MaxAppendEntries controls the maximum number of append entries
	// to send at once. We want to strike a balance between efficiency
	// and avoiding waste if the follower is going to reject because of
	// an inconsistent log
	MaxAppendEntries int

	// If we are a member of a cluster, and RemovePeer is invoked for the
	// local node, then we forget all peers and transition into the follower state.
	// If ShutdownOnRemove is is set, we additional shutdown Raft. Otherwise,
	// we can become a leader of a cluster containing only this node.
	ShutdownOnRemove bool
}

func DefaultConfig() *Config {
	return &Config{
		HeartbeatTimeout: 200 * time.Millisecond,
		ElectionTimeout:  250 * time.Millisecond,
		CommitTimeout:    10 * time.Millisecond,
		MaxAppendEntries: 32,
		ShutdownOnRemove: true,
	}
}
