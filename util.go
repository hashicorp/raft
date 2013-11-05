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
