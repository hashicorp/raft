package raft

import (
	"time"
)

// Config provides any necessary configuraiton to
// the Raft server
type Config struct {
	// Time without a leader before we attempt an election
	ElectionTimeout time.Duration
}

func DefaultConfig() *Config {
	return &Config{
		ElectionTimeout: 150 * time.Millisecond,
	}
}
