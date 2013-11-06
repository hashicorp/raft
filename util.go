package raft

import (
	"math/rand"
	"time"
)

// randomTimeout returns a value that is between the minVal and 2x minVal
func randomTimeout(minVal time.Duration) <-chan time.Time {
	extra := (time.Duration(rand.Int63()) % minVal)
	return time.After(minVal + extra)
}

// min returns the minimum.
func min(a, b uint64) uint64 {
	if a <= b {
		return a
	}
	return b
}
