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

// deferError can be embedded to allow a future
// to provide an error in the future
type deferError struct {
	err   error
	errCh chan error
}

func (d *deferError) init() {
	d.errCh = make(chan error, 1)
}

func (d *deferError) Error() error {
	if d.err != nil {
		return d.err
	}
	d.err = <-d.errCh
	return d.err
}

func (d *deferError) respond(err error) {
	if d.errCh == nil {
		return
	}
	d.errCh <- err
	close(d.errCh)
}

// logFuture is used to apply a log entry and waits until
// the log is considered committed
type logFuture struct {
	deferError
	log    Log
	policy quorumPolicy
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
	deferError
}
