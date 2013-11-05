package raft

import (
	"testing"
	"time"
)

func TestRandomTimeout(t *testing.T) {
	start := time.Now()
	timeout := randomTimeout(time.Millisecond)

	select {
	case <-timeout:
		diff := time.Now().Sub(start)
		if diff < time.Millisecond {
			t.Fatalf("fired early")
		}
	case <-time.After(3 * time.Millisecond):
		t.Fatalf("timeout")
	}
}
