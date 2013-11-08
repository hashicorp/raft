package raft

import (
	crand "crypto/rand"
	"fmt"
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

// generateUUID is used to generate a random UUID
func generateUUID() string {
	buf := make([]byte, 16)
	if _, err := crand.Read(buf); err != nil {
		panic(fmt.Errorf("Failed to read random bytes: %v", err))
	}

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%12x",
		buf[0:4],
		buf[4:6],
		buf[6:8],
		buf[8:10],
		buf[10:16])
}

// asyncNotify is used to do an async channel send to
// a list of channels. This will not block.
func asyncNotify(chans []chan struct{}) {
	for _, ch := range chans {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
