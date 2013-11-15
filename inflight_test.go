package raft

import (
	"fmt"
	"testing"
)

func TestInflight_StartCommit(t *testing.T) {
	commitCh := make(chan *logFuture, 1)
	in := newInflight(commitCh)

	// Commit a transaction as being in flight
	l := &logFuture{log: Log{Index: 1}}
	l.policy = newMajorityQuorum(5)
	in.Start(l)

	// Commit 3 times
	in.Commit(1, nil)
	select {
	case <-commitCh:
		t.Fatalf("should not be commited")
	default:
	}

	in.Commit(1, nil)
	select {
	case <-commitCh:
		t.Fatalf("should not be commited")
	default:
	}

	in.Commit(1, nil)
	select {
	case <-commitCh:
	default:
		t.Fatalf("should be commited")
	}

	// Already commited but should work anyways
	in.Commit(1, nil)
}

func TestInflight_Cancel(t *testing.T) {
	commitCh := make(chan *logFuture, 1)
	in := newInflight(commitCh)

	// Commit a transaction as being in flight
	l := &logFuture{
		log: Log{Index: 1},
	}
	l.init()
	l.policy = newMajorityQuorum(3)
	in.Start(l)

	// Cancel with an error
	err := fmt.Errorf("error 1")
	in.Cancel(err)

	// Should get an error return
	if l.Error() != err {
		t.Fatalf("expected error")
	}
}
