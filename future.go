package raft

import (
	"time"
)

// ApplyFuture is used to represent an application that may occur in the future
type ApplyFuture interface {
	Error() error
}

// errorFuture is used to return a static error
type errorFuture struct {
	err error
}

func (e errorFuture) Error() error {
	return e.err
}

// logFuture is used to apply a log entry and waits until
// the log is considered committed
type logFuture struct {
	log    Log
	policy quorumPolicy
	err    error
	errCh  chan error
}

func (l *logFuture) Error() error {
	if l.err != nil {
		return l.err
	}
	l.err = <-l.errCh
	return l.err
}

func (l *logFuture) respond(err error) {
	if l.errCh == nil {
		return
	}
	l.errCh <- err
	close(l.errCh)
}

type shutdownFuture struct {
	raft *Raft
}

func (s *shutdownFuture) Error() error {
	for s.raft.getRoutines() > 0 {
		time.Sleep(5 * time.Millisecond)
	}
	return nil
}

// snapshotFuture is used for an in-progress snapshot
type snapshotFuture struct {
	err   error
	errCh chan error
}

func (s *snapshotFuture) Error() error {
	if s.err != nil {
		return s.err
	}
	s.err = <-s.errCh
	return s.err
}

func (s *snapshotFuture) respond(err error) {
	if s.errCh == nil {
		return
	}
	s.errCh <- err
	close(s.errCh)
}
