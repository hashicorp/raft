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

func TestInflight_CommitRange(t *testing.T) {
	commitCh := make(chan *logFuture, 3)
	in := newInflight(commitCh)

	// Commit a few transaction as being in flight
	l1 := &logFuture{log: Log{Index: 2}}
	l1.policy = newMajorityQuorum(5)
	in.Start(l1)

	l2 := &logFuture{log: Log{Index: 3}}
	l2.policy = newMajorityQuorum(5)
	in.Start(l2)

	l3 := &logFuture{log: Log{Index: 4}}
	l3.policy = newMajorityQuorum(5)
	in.Start(l3)

	// Commit ranges
	in.CommitRange(1, 5, nil)
	in.CommitRange(1, 4, nil)
	in.CommitRange(1, 10, nil)

	// Should get 3 back
	if len(commitCh) != 3 {
		t.Fatalf("expected all 3 to commit")
	}
}

// Should panic if we commit non contiguously!
func TestInflight_NonContiguous(t *testing.T) {
	commitCh := make(chan *logFuture, 3)
	in := newInflight(commitCh)

	// Commit a few transaction as being in flight
	l1 := &logFuture{log: Log{Index: 2}}
	l1.policy = newMajorityQuorum(5)
	in.Start(l1)

	l2 := &logFuture{log: Log{Index: 3}}
	l2.policy = newMajorityQuorum(5)
	in.Start(l2)

	in.Commit(3, nil)
	in.Commit(3, nil)
	in.Commit(3, nil) // panic!

	select {
	case <-commitCh:
		t.Fatalf("should not commit")
	default:
	}

	in.Commit(2, nil)
	in.Commit(2, nil)
	in.Commit(2, nil) // panic!

	select {
	case l := <-commitCh:
		if l.log.Index != 2 {
			t.Fatalf("bad: %v", *l)
		}
	default:
		t.Fatalf("should commit")
	}

	select {
	case l := <-commitCh:
		if l.log.Index != 3 {
			t.Fatalf("bad: %v", *l)
		}
	default:
		t.Fatalf("should commit")
	}
}
