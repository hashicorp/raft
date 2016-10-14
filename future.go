package raft

import "time"

// Future is used to represent an action that may occur in the future.
type Future interface {
	// Error blocks until the future arrives and then
	// returns the error status of the future.
	// This may be called any number of times - all
	// calls will return the same value.
	// Note that it is not OK to call this method
	// twice concurrently on the same Future instance.
	Error() error
}

// IndexFuture is used for future actions that can result in a raft log entry
// being created.
type IndexFuture interface {
	Future

	// Index holds the index of the newly applied log entry.
	// This must not be called until after the Error method has returned.
	Index() Index
}

// ApplyFuture is used for Apply and can return the FSM response.
type ApplyFuture interface {
	IndexFuture

	// Response returns the FSM response as returned
	// by the FSM.Apply method. This must not be called
	// until after the Error method has returned.
	Response() interface{}
}

// MembershipFuture is used for GetMembership and can return the
// latest membership configuration in use by Raft.
type MembershipFuture interface {
	IndexFuture

	// Membership contains the latest membership. This must
	// not be called until after the Error method has returned.
	Membership() Membership
}

// errorFuture is used to return a static error.
type errorFuture struct {
	err error
}

func (e errorFuture) Error() error {
	return e.err
}

func (e errorFuture) Response() interface{} {
	return nil
}

func (e errorFuture) Index() Index {
	return 0
}

// deferError can be embedded to allow a future
// to provide an error in the future.
type deferError struct {
	err       error
	errCh     chan error
	responded bool
}

func (d *deferError) init() {
	d.errCh = make(chan error, 1)
}

func (d *deferError) Error() error {
	if d.err != nil {
		// Note that when we've received a nil error, this
		// won't trigger, but the channel is closed after
		// send so we'll still return nil below.
		return d.err
	}
	if d.errCh == nil {
		panic("waiting for response on nil channel")
	}
	d.err = <-d.errCh
	return d.err
}

func (d *deferError) respond(err error) {
	if d.errCh == nil {
		return
	}
	if d.responded {
		return
	}
	d.errCh <- err
	close(d.errCh)
	d.responded = true
}

// membershipChangeFuture is to track a membership configuration change that is
// being applied to the log. These are encoded here for leaderLoop() to process.
// This is internal to a single server.
type membershipChangeFuture struct {
	logFuture
	req membershipChangeRequest
}

// bootstrapFuture is used to attempt a live bootstrap of the cluster. See the
// Raft object's BootstrapCluster member function for more details.
type bootstrapFuture struct {
	deferError

	// membership is the proposed bootstrap membership configuration to apply.
	membership Membership
}

// logFuture is used to apply a log entry and waits until
// the log is considered committed.
type logFuture struct {
	deferError
	log      Log
	response interface{}
	dispatch time.Time
}

func (l *logFuture) Response() interface{} {
	return l.response
}

func (l *logFuture) Index() Index {
	return l.log.Index
}

type shutdownFuture struct {
	raft *Raft
}

func (s *shutdownFuture) Error() error {
	if s.raft == nil {
		return nil
	}
	s.raft.server.goRoutines.waitShutdown()
	if closeable, ok := s.raft.server.trans.(WithClose); ok {
		closeable.Close()
	}
	return nil
}

// snapshotFuture is used for waiting on a snapshot to complete.
type snapshotFuture struct {
	deferError
}

// reqSnapshotFuture is used for requesting a snapshot start.
// It is only used internally.
type reqSnapshotFuture struct {
	deferError

	// snapshot details provided by the FSM runner before responding
	index    Index
	term     Term
	snapshot FSMSnapshot
}

// restoreFuture is used for requesting an FSM to perform a
// snapshot restore. Used internally only.
type restoreFuture struct {
	deferError
	ID string
}

// verifyFuture is returned by VerifyLeader(), used to check that a majority of
// the cluster still believes the local server to be the current leader.
type verifyFuture struct {
	deferError
}

// membershipsFuture is used to retrieve the current memberships. This is
// used to allow safe access to this information outside of the main thread.
type membershipsFuture struct {
	deferError
	memberships memberships
}

// Membership returns the latest membership configuration in use by Raft.
func (f *membershipsFuture) Membership() Membership {
	return f.memberships.latest
}

// Index returns the index of the latest membership in use by Raft.
func (f *membershipsFuture) Index() Index {
	return f.memberships.latestIndex
}

// appendFuture is used for waiting on a pipelined append
// entries RPC.
type appendFuture struct {
	deferError
	start time.Time
	args  *AppendEntriesRequest
	resp  *AppendEntriesResponse
}

func (a *appendFuture) Start() time.Time {
	return a.start
}

func (a *appendFuture) Request() *AppendEntriesRequest {
	return a.args
}

func (a *appendFuture) Response() *AppendEntriesResponse {
	return a.resp
}

type StatsFuture interface {
	Future
	// Stats returns variuos bits of internal information. This must
	// not be called until after the Error method has returned.
	Stats() *Stats
}

type statsFuture struct {
	deferError
	stats *Stats
}

func (s *statsFuture) Stats() *Stats {
	return s.stats
}
