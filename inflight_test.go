package raft

import (
	"fmt"
	"testing"
)

func TestInflight_StartCommit(t *testing.T) {
	commitCh := make(chan *logFuture, 1)
	in := NewInflight(commitCh)

	// Commit a transaction as being in flight
	l := &logFuture{log: Log{Index: 1}}
	in.Start(l, 3)

	// Commit 3 times
	in.Commit(1)
	select {
	case <-commitCh:
		t.Fatalf("should not be commited")
	default:
	}

	in.Commit(1)
	select {
	case <-commitCh:
		t.Fatalf("should not be commited")
	default:
	}

	in.Commit(1)
	select {
	case <-commitCh:
	default:
		t.Fatalf("should be commited")
	}
}

func TestInflight_Cancel(t *testing.T) {
	commitCh := make(chan *logFuture, 1)
	in := NewInflight(commitCh)

	// Commit a transaction as being in flight
	l := &logFuture{
		log:   Log{Index: 1},
		errCh: make(chan error, 1),
	}
	in.Start(l, 3)

	// Cancel with an error
	err := fmt.Errorf("error 1")
	in.Cancel(err)

	// Should get an error return
	if l.Error() != err {
		t.Fatalf("expected error")
	}
}
