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
}

func DefaultConfig() *Config {
	return &Config{
		HeartbeatTimeout: 100 * time.Millisecond,
		ElectionTimeout:  150 * time.Millisecond,
	}
}
