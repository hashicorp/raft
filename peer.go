package raft

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/armon/go-metrics"
	log "github.com/mgutz/logxi/v1"
)

// Settings controlling Peer behavior, as passed to startPeer().
type peerOptions struct {
	// No more than this many entries will be sent in one AppendEntries request.
	maxAppendEntries uint64

	// The number of pipelined AppendEntries RPCs that are permitted in flight at
	// any given time. This is used as a safety cap to avoid spamming the peer as
	// fast as possible.
	maxPipelineWindow uint64

	// As leader, heartbeats will be sent to the peer this often durng periods of
	// inactivity.
	heartbeatInterval time.Duration

	// Minimum time to wait after a transport error before sending bulk RPCs.
	// This is scaled exponentially up to maxFailureWait upon repeated errors.
	failureWait time.Duration

	// Maximum amount to wait after many transport errors before retrying.
	maxFailureWait time.Duration
}

// This struct is accessed concurrently by different Peer goroutines.
type peerShared struct {
	// Policy settings (constant).
	options peerOptions

	// Where to print debug messages.
	logger log.Logger

	// Used to spawn goroutines so others can wait on their exit.
	goRoutines *waitGroup

	// Used to read log entries to send in AppendEntries.
	logs LogStore

	// Used to read snapshots to send in InstallSnapshot.
	snapshots SnapshotStore

	// Used to communicate with the remote server.
	trans Transport

	// Used to send many AppendEntries to the peer in rapid succession
	// (without waiting for the prevous response).
	pipeline AppendPipeline2

	// The address of this local server, which is sent to the remote server in
	// most RPCs (constant).
	localAddr ServerAddress

	// Uniquely identifies the remote server (constant).
	peerID ServerID

	// Network address of the remote server (constant).
	peerAddr ServerAddress

	// Version number sent in request headers (constant).
	protocolVersion ProtocolVersion

	// Helper goroutines send fully prepared RPC requests onto requestCh.
	// The main Peer goroutine validates each request against the latest
	// control information, then spawns another goroutine to send it.
	requestCh chan peerRPC

	// Helper goroutines send RPC replies onto replyCh for the main Peer goroutine
	// to process.
	replyCh chan peerRPC

	// Notified when the AppendEntries pipeline is ready to send further requests.
	// The term of the AppendEntries RPC is sent over the channel, used to clear
	// peerState.leader.outstandingPipelineSend if the term is current.
	pipelineSendDoneCh chan Term

	// stopCh is closed whenever helper goroutines should exit. In particular,
	// they should never block on sending to requestCh/replyCh exclusively; they
	// should always block on receiving from stopCh along with it. Nothing is ever
	// sent on this channel (it is only ever closed).
	stopCh chan struct{}
}

// The raft.go module uses this struct to instruct this Peer on what to do.
type peerControl struct {
	// The local server's current term. Once this changes, Peer will send no new
	// RPCs from the previous term, though requests that have already been
	// prepared will still be sent (the Raft protocol allows this, as it's
	// equivalent to a long network delay).
	term Term

	// If role is Follower, do nothing. If role is Candidate, request votes. If
	// role is Leader, send log entries and snapshots. If role is Shutdown, exit
	// immediately. Note that raft.go may mask the server's actual role if this
	// peer does not have a vote. For example, it may claim to be a Follower when
	// it's actually a Candidate, if there's no reason to request a vote from this
	// peer.
	role RaftState

	// Incremented whenever this should send a ping to the peer immediately. The
	// response to that ping will set the same value in verifiedCounter.
	verifyCounter uint64

	// As leader, the last entry in the log which should be replicated to the
	// peer. As candidate, included in RequestVote entries for log comparisons.
	lastIndex Index

	// The term of the lastIndex entry.
	lastTerm Term

	// The last entry committed in the log (this may be past lastIndex).
	commitIndex Index
}

// This Peer sends this struct to the raft.go module to inform it of newly
// received information and acknowledgments.
type peerProgress struct {
	// The unique ID for this peer. The progress structs from all peers are sent
	// to the Raft module along a single channel, so it uses this field to
	// distinguish them (constant).
	peerID ServerID

	// The peer's current term. If this is larger than the local server's
	// term, the local server will update and become a follower.
	term Term

	// Set to true if the peer granted this server a vote in 'term'.
	voteGranted bool

	// The index in the log that the remote server has acknowledged as matching
	// this server's. Reset to 0 when the term changes.
	matchIndex Index

	// The term of the matchIndex entry. This is probably uninteresting to the
	// Raft module. The Peer uses it internally to avoid reading from the
	// store on the heartbeat path.
	matchTerm Term

	// A lower bound of when the peer last heard from us. Upon the receipt of any
	// reply from the peer (successful or not, term matches or not), this is
	// updated to the time the request was started.
	lastContact time.Time

	// An upper bound of when the peer last heard from us. Upon the receipt of any
	// reply from the peer (successful or not, term matches or not), this is
	// updated to the current time.
	lastReply time.Time

	// The value of control.verifyCounter when the last completed heartbeat to the
	// peer was sent.
	verifiedCounter uint64
}

// Private state to the peer that is used while this server is a Candidate.
type peerCandidateState struct {
	// If true, a RequestVote RPC is in progress to this Peer during control.term.
	outstandingRequestVote bool

	// If true, a RequestVote reply has already been received from this Peer
	// during control.term, and there's no reason to request a vote again.
	voteReplied bool
}

// Private state to the peer that is used while this server is a Leader.
type peerLeaderState struct {
	// The last time a heartbeat has been sent to this Peer during control.term.
	// This will send another heartbeat after
	// lastHeartbeatSent + heartbeatInterval.
	lastHeartbeatSent time.Time

	// The index of the next log entry to send to the peer, which may fall one
	// entry past the end of the log. To support pipelining, this is always
	// updated optimistically as soon as the AppendEntries request is confirmed,
	// then rolled back in case of any error or negative acknowledgment.
	nextIndex Index

	// The index of the next log entry whose commitment to notify the peer of,
	// which may fall one entry past the end of the log. To support pipelining,
	// this is always updated optimistically as soon as the AppendEntries request
	// is confirmed, then rolled back in case of any error or negative
	// acknowledgment. Used to send the peer new commitIndex values aggressively
	// so that its state machine can serve fresher potentially-stale reads.
	nextCommitIndex Index

	// If true, the peer needs a snapshot because this server has already
	// discarded the log entries it would otherwise send in AppendEntries.
	needsSnapshot bool

	// Set to true while an InstallSnapshot RPC is in progress during
	// control.term. This prevents further InstallSnapshot and bulk AppendEntries
	// RPCs from starting.
	outstandingInstallSnapshotRPC bool

	// Counts the number of non-heartbeat AppendEntries and InstallSnapshot RPCs
	// that have been sent but have not completed (in either error or response)
	// during control.term. When not pipelining, this is capped at 1.
	// When pipelining, this is capped at peerOptions.maxPipelineWindow.
	outstandingAppendEntriesRPCs uint64

	// allowPipeline is true when AppendEntries RPCs should be sent over a
	// pipeline (a separate path through the Transport), false otherwise.
	// If allowPipeline is true, pipelineUnsupported must be false.
	allowPipeline bool

	// Set to true when an AppendEntries request is being sent on the
	// AppendEntries pipeline during control.term, then cleared when the pipeline
	// is ready for more.
	outstandingPipelineSend bool
}

// The main type of a Peer (peer clashes too much), which is in charge of all
// communication with a single remote server. Peers can span many terms and are
// normally kept until the remote server is removed from the cluster or the
// local server is shutdown.
type peerState struct {
	// This field is shared with other Peer goroutines.
	shared *peerShared

	///// The rest of this stuff is private to the main Peer goroutine. /////

	// The Raft module sends on this channel whenever any of the 'control' fields
	// change (unbuffered).
	controlCh <-chan peerControl

	// The Peer module sends on this channel to the Raft module shortly after any
	// of the 'progress' fields change (unbuffered).
	progressCh chan<- peerProgress

	// The latest control information received from the Raft module.
	control peerControl

	// The latest progress information computed by the Peer.
	progress peerProgress

	// If true, the Raft module has not yet received the latest updates to
	// 'progress', and these will be sent at the first availble opportunity.
	sendProgress bool

	// State used when control.role is Candidate, nil otherwise.
	candidate *peerCandidateState

	// State used when control.role is Leader, nil otherwise.
	leader *peerLeaderState

	// Counts the total number of PeerRPC objects that exist, excluding errorRPC
	// objects. Used only in unit tests to tell when things have quiesced.
	activeRPCs uint64

	// If true, the peer should not try to pipeline AppendEntries requests to
	// the peer, as it will not work.
	pipelineUnsupported bool

	// Counts the number of consecutive RPCs that have failed with transport-level
	// errors since the last success. Used to apply increasing amounts of backoff.
	failures uint64

	// If failures is nonzero, RequestVote, bulk (non-heartbet) AppendEntries, and
	// InstallSnapshot are not sent until this timer elapses.
	backoffTimer *time.Timer
}

// startPeer is the normal way for peers to be created.
func startPeer(serverID ServerID,
	serverAddress ServerAddress,
	logger log.Logger,
	logs LogStore,
	snapshots SnapshotStore,
	goRoutines *waitGroup,
	trans Transport,
	localAddr ServerAddress,
	protocolVersion ProtocolVersion,
	progressCh chan<- peerProgress,
	options peerOptions) chan<- peerControl {
	controlCh := make(chan peerControl)
	p := makePeerInternal(serverID, serverAddress, logger, logs, snapshots,
		goRoutines, trans, localAddr, protocolVersion, controlCh, progressCh, options)
	p.shared.goRoutines.spawn(p.selectLoop)
	return controlCh
}

// makePeerInternal is used for unit tests. Everyone else should use startPeer.
func makePeerInternal(serverID ServerID,
	serverAddress ServerAddress,
	logger log.Logger,
	logs LogStore,
	snapshots SnapshotStore,
	goRoutines *waitGroup,
	trans Transport,
	localAddr ServerAddress,
	protocolVersion ProtocolVersion,
	controlCh <-chan peerControl,
	progressCh chan<- peerProgress,
	options peerOptions) *peerState {

	if logger == nil {
		logger = DefaultStdLogger(os.Stderr)
	}
	if options.maxAppendEntries == 0 {
		options.maxAppendEntries = 1000
	}
	if options.heartbeatInterval == 0 {
		options.heartbeatInterval = 100 * time.Millisecond
	}
	if options.failureWait == 0 {
		options.failureWait = 10 * time.Millisecond
	}
	if options.maxFailureWait == 0 {
		options.maxFailureWait = 100 * time.Millisecond
	}
	if options.maxPipelineWindow == 0 {
		options.maxPipelineWindow = 32
	}

	p := &peerState{
		shared: &peerShared{
			options:            options,
			peerID:             serverID,
			peerAddr:           serverAddress,
			trans:              trans,
			localAddr:          localAddr,
			protocolVersion:    protocolVersion,
			logger:             logger,
			logs:               logs,
			snapshots:          snapshots,
			goRoutines:         goRoutines,
			pipeline:           makeReliablePipeline(trans, serverAddress),
			requestCh:          make(chan peerRPC),
			replyCh:            make(chan peerRPC),
			pipelineSendDoneCh: make(chan Term),
			stopCh:             make(chan struct{}),
		},
		controlCh:    controlCh,
		progressCh:   progressCh,
		backoffTimer: time.NewTimer(time.Hour),
		progress: peerProgress{
			peerID: serverID,
		},
	}
	p.backoffTimer.Stop()
	return p
}

func (p *peerState) checkInvariants() error {
	if p.shared.peerID != p.progress.peerID {
		return fmt.Errorf("Progress must have peerID (shared has %v but progress has %v)",
			p.shared.peerID, p.progress.peerID)
	}
	if p.progress.lastContact.After(p.progress.lastReply) ||
		(p.progress.lastContact == p.progress.lastReply && !p.progress.lastContact.IsZero()) {
		return fmt.Errorf("lastContact (%v) must be before lastReply (%v) or both 0",
			p.progress.lastContact, p.progress.lastReply)
	}
	switch p.control.role {
	case Candidate:
		if p.candidate == nil || p.leader != nil {
			return errors.New("Should have candidate and not leader state while candidate")
		}
	case Leader:
		if p.candidate != nil || p.leader == nil {
			return errors.New("Should have leader and not candidate state while leader")
		}
		if p.leader.nextIndex < 1 || p.leader.nextIndex > p.control.lastIndex+1 {
			return fmt.Errorf("nextIndex (%v) must be between 1 and lastIndex + 1 (between 1 and %v)",
				p.leader.nextIndex, p.control.lastIndex+1)
		}
		if p.leader.nextCommitIndex < 1 || p.leader.nextCommitIndex > p.leader.nextIndex {
			return fmt.Errorf("nextCommitIndex (%v) must be between 1 and nextIndex (%v)",
				p.leader.nextCommitIndex, p.leader.nextIndex)
		}
		if p.leader.outstandingInstallSnapshotRPC && p.leader.outstandingAppendEntriesRPCs > 0 {
			return errors.New("Must not send a snapshot and entries simultaneously")
		}
		if p.leader.allowPipeline && p.pipelineUnsupported {
			return errors.New("allowPipeline may only be set if transport supports it")
		}
	default:
		if p.candidate != nil || p.leader != nil {
			return fmt.Errorf("Should have no leader or candidate state while %v",
				p.control.role)
		}
	}
	return nil
}

// selectLoop is a long-running routine that sends RPCs to a single remote
// server.
func (p *peerState) selectLoop() {
	for p.control.role != Shutdown {
		if err := p.checkInvariants(); err != nil {
			p.shared.logger.Error("Peer invariant violated",
				"id", p.shared.peerID,
				"address", p.shared.peerAddr,
				"error", err)
			panic(err)
		}

		didSomething := p.issueWork()
		if didSomething {
			continue
		}

		// If we've got no more work to do, block until something changes.
		p.blockingSelect()
	}

	p.shared.logger.Info("Peer routine exiting",
		"id", p.shared.peerID, "address", p.shared.peerAddr)
	close(p.shared.stopCh)
	p.shared.pipeline.Close()
}

// blockingSelect reads/writes the Peer channels just once, blocking if needed.
func (p *peerState) blockingSelect() {
	var heartbeatTimer <-chan time.Time
	if p.control.role == Leader {
		// We need to send a heartbeat at lastHeartbeatSent + heartbeatInterval.
		heartbeatTimer = time.After(p.shared.options.heartbeatInterval -
			time.Since(p.leader.lastHeartbeatSent))
	}

	var progressChIfDirty chan<- peerProgress
	if p.sendProgress {
		progressChIfDirty = p.progressCh
	}

	select {
	case control := <-p.controlCh:
		p.updateControl(control)
	case progressChIfDirty <- p.progress:
		p.sendProgress = false
	case request := <-p.shared.requestCh:
		p.send(request)
	case rpc := <-p.shared.replyCh:
		p.processReply(rpc)
	case term := <-p.shared.pipelineSendDoneCh:
		if p.control.term == term && p.leader != nil {
			p.leader.outstandingPipelineSend = false
		}
	case <-p.backoffTimer.C:
		p.shared.logger.Info("Peer backoff period ended",
			"id", p.shared.peerID, "address", p.shared.peerAddr)
		p.failures = 0
	case <-heartbeatTimer:
		// nothing more to do, just needed to be woken
	}
}

// updateControl sets p.control and resets some other affected state.
func (p *peerState) updateControl(latest peerControl) {
	// Handle term changes.
	if p.control.term < latest.term {
		p.candidate = nil
		p.leader = nil
	}

	// Update candidate fields on term/role changes.
	if latest.role == Candidate {
		if p.candidate == nil {
			p.candidate = &peerCandidateState{}
		}
	} else {
		p.candidate = nil
	}

	// Update leader fields on term/role changes.
	if latest.role == Leader {
		if p.leader == nil {
			p.leader = &peerLeaderState{
				lastHeartbeatSent: time.Time{},
				nextIndex:         latest.lastIndex + 1,
				nextCommitIndex:   1,
				allowPipeline:     false,
			}
		}
	} else {
		p.leader = nil
	}

	p.control = latest
}

// start creates a peerRPC object, then spawns a goroutine to prepare its
// request and notify the peer's requestCh. It returns right away.
func (p *peerState) start(makeRPC func(*peerState) peerRPC) {
	p.activeRPCs++
	rpc := makeRPC(p)
	p.shared.goRoutines.spawn(func() {
		startHelper(rpc, p.shared, p.control)
	})
}

// startHelper does most of the work of p.start() but runs in a separate
// goroutine with no access to peerState.
func startHelper(rpc peerRPC, shared *peerShared, control peerControl) {
	err := rpc.prepare(shared, control)
	if err != nil {
		rpc = &errorRPC{err: err, orig: rpc}
	}
	select {
	case shared.requestCh <- rpc:
	case <-shared.stopCh:
	}
}

// send validates an RPC request, then spawns a goroutine to send it over the
// network and notify the peer's replyCh. It returns right away.
func (p *peerState) send(rpc peerRPC) {
	err := rpc.confirm(p)
	if err != nil {
		rpc.process(p, err)
		p.activeRPCs--
		return
	}
	p.shared.goRoutines.spawn(func() {
		sendHelper(p.shared, rpc)
	})
}

// sendHelper does most of the work of p.send() but runs in a separate
// goroutine with no access to peerState.
func sendHelper(shared *peerShared, rpc peerRPC) {
	err := rpc.sendRecv(shared)
	if err != nil {
		rpc = &errorRPC{err: err, orig: rpc, backoff: true}
	}
	select {
	case shared.replyCh <- rpc:
	case <-shared.stopCh:
	}
}

// processReply is called on the main Peer goroutine upon receiving from
// replyCh. It does some bookkeeping and invokes the RPC's process() method.
func (p *peerState) processReply(rpc peerRPC) {
	switch rpc := rpc.(type) {
	case *errorRPC:
		if rpc.backoff {
			if p.failures == 0 {
				p.shared.logger.Warn("RPC transport error. Backing off new RPCs for peer (except heartbeats)",
					"id", p.shared.peerID,
					"address", p.shared.peerAddr)
			}
			p.failures++
			p.backoffTimer.Reset(backoff(p.failures,
				p.shared.options.failureWait,
				p.shared.options.maxFailureWait))
		}
		rpc.orig.process(p, rpc.err)
		p.activeRPCs--

	default:
		p.sendProgress = true
		if p.failures > 0 {
			p.shared.logger.Info("Received RPC reply from. Clearing backoff for peer",
				"id", p.shared.peerID,
				"address", p.shared.peerAddr)
			p.failures = 0
			p.backoffTimer.Stop()
		}
		rpc.process(p, nil)
		p.activeRPCs--
		p.progress.lastContact = rpc.started()
		p.progress.lastReply = time.Now()
	}
}

// issueWork does a single unit of non-blocking work.
func (p *peerState) issueWork() (didSomething bool) {
	if p.progress.term > p.control.term {
		return false
	}

	switch p.control.role {
	case Shutdown:
		// do nothing

	case Follower:
		// do nothing

	case Candidate:
		// Send a RequestVote RPC.
		if !p.candidate.outstandingRequestVote && !p.candidate.voteReplied && p.failures == 0 {
			p.shared.logger.Info("Starting RequestVote RPC for peer",
				"id", p.shared.peerID,
				"address", p.shared.peerAddr)
			p.start(makeRequestVoteRPC)
			return true
		}

	case Leader:
		// Send a heartbeat (empty AppendEntries) RPC. Used as a keep-alive when
		// other RPCs are not completing quickly or this server is idle. This will
		// not block on the store.
		if time.Now().After(p.leader.lastHeartbeatSent.Add(p.shared.options.heartbeatInterval)) {
			p.shared.logger.Info("Starting heartbeat RPC for peer",
				"id", p.shared.peerID,
				"address", p.shared.peerAddr)
			p.start(makeHeartbeatRPC)
			return true
		}

		// Send an AppendEntries/InstallSnapshot RPC. Used to send the peer
		// snapshots, entries, and inform it of new commit index values. This may
		// block on the store or take a long time to transmit, so we don't want to
		// rely on it for heartbeats.
		if (p.leader.nextIndex <= p.control.lastIndex ||
			p.leader.nextCommitIndex <= p.control.commitIndex) &&
			p.failures == 0 &&
			!p.leader.outstandingInstallSnapshotRPC {
			if p.leader.needsSnapshot {
				p.shared.logger.Info("Starting InstallSnapshot RPC for peer",
					"id", p.shared.peerID,
					"address", p.shared.peerAddr)
				p.start(makeInstallSnapshotRPC)
				return true
			}
			if p.leader.outstandingAppendEntriesRPCs == 0 ||
				(p.leader.allowPipeline && !p.leader.outstandingPipelineSend &&
					p.leader.outstandingAppendEntriesRPCs < p.shared.options.maxPipelineWindow) {
				p.shared.logger.Info("Starting AppendEntries RPC for peer",
					"id", p.shared.peerID,
					"address", p.shared.peerAddr)
				p.start(makeAppendEntriesRPC)
				return true
			}
		}
	}

	return false
}

// confirmedLeadership is called after an AppendEntries or InstallSnapshot RPC
// returns with this leader's same term.
func (p *peerState) confirmedLeadership(start time.Time, verifyCounter uint64) {
	if start.After(p.leader.lastHeartbeatSent) {
		p.leader.lastHeartbeatSent = start
	}
	if p.progress.verifiedCounter < verifyCounter {
		p.progress.verifiedCounter = verifyCounter
	}
}

// updateTerm clears out some progress fields when its term changes.
func updateTerm(progress *peerProgress, term Term) {
	if term > progress.term {
		progress.term = term
		progress.voteGranted = false
		progress.matchIndex = 0
		progress.matchTerm = 0
	}
}

// Every type of RPC sent to peers implements peerRPC. Some of its methods are
// invoked on the main Peer goroutine and may not block. Others are run on
// separate routines and may block. A typical RPC goes through the following
// lifecycle:
//
//       selectLoop            helper routine
//      (nonblocking)            (blocking)
//      =============          ==============
//
//        make
//          |
//          '-------spawn-----> prepare
//                                 |
//       confirm <---requestCh-----'
//          |
//         (ok)
//          |
//          '-------spawn-----> sendRecv
//                                 |
//                                 '------------> Transport
//                                                    |
//                                 .------------------'
//                                 |
//       process <--replyCh--------'
//
type peerRPC interface {
	// prepare is a blocking operation invoked on a separate goroutine that
	// builds up the RPC request. For example, it may block to access state from
	// disk.
	prepare(shared *peerShared, control peerControl) error

	// confirm is called after a successful prepare(). It returns nil if the RPC
	// should still be sent, given the current control information. In particular,
	// it should return an error if the server's term has changed. confirm runs on
	// the main Peer goroutine so that it can update state if needed; it must not
	// do any blocking operations.
	confirm(p *peerState) error

	// sendRecv is a blocking operation invoked on a separate goroutine that
	// transmits the RPC request over the network and waits for and saves the
	// response.
	sendRecv(shared *peerShared) error

	// process updates the Peer's state based on the RPC's response or an error
	// returned by an earlier confirm() or sendRecv(). It's called from the main
	// Peer goroutine, so it must not do any blocking operations. It is the
	// responsibility of this method to log 'err' appropriately.
	process(p *peerState, err error)

	// started returns the time the RPC request was initially created.
	started() time.Time
}

// errorRPC is an internal type that's used to send back an error from an RPC's
// prepare() or sendRecv().
type errorRPC struct {
	// err is the first error that 'orig' encountered.
	err error
	// orig is the RPC that encountered an error.
	orig peerRPC
	// backoff is true for transport errors to indicate that we should
	// probably wait a little while before trying to send another RPC.
	backoff bool
}

func (err *errorRPC) prepare(shared *peerShared, control peerControl) error {
	panic("Don't call prepare() on errorRPC")
}

func (err *errorRPC) confirm(p *peerState) error {
	return nil
}

func (err *errorRPC) sendRecv(shared *peerShared) error {
	return nil
}

func (err *errorRPC) process(p *peerState, err2 error) {
	panic("Don't call process() on errorRPC")
}

func (err *errorRPC) started() time.Time {
	return err.orig.started()
}

///////////////////////// RequestVote /////////////////////////

type requestVoteRPC struct {
	start time.Time
	req   RequestVoteRequest
	resp  RequestVoteResponse
}

func makeRequestVoteRPC(p *peerState) peerRPC {
	p.candidate.outstandingRequestVote = true
	return &requestVoteRPC{
		start: time.Now(),
		req: RequestVoteRequest{
			RPCHeader:    RPCHeader{p.shared.protocolVersion},
			Term:         p.control.term,
			Candidate:    p.shared.trans.EncodePeer(p.shared.localAddr),
			LastLogIndex: p.control.lastIndex,
			LastLogTerm:  p.control.lastTerm,
		},
	}
}

func (rpc *requestVoteRPC) started() time.Time {
	return rpc.start
}

func (rpc *requestVoteRPC) prepare(shared *peerShared, control peerControl) error {
	return nil
}

func (rpc *requestVoteRPC) confirm(p *peerState) error {
	if rpc.req.Term != p.control.term {
		return errors.New("term changed, discarding RequestVote request")
	}
	if p.control.role != Candidate {
		return errors.New("no longer candidate, discarding RequestVote request")
	}
	return nil
}

func (rpc *requestVoteRPC) sendRecv(shared *peerShared) error {
	shared.logger.Info("Sending RequestVote to peer",
		"term", rpc.req.Term,
		"id", shared.peerID,
		"address", shared.peerAddr)
	err := shared.trans.RequestVote(shared.peerAddr, &rpc.req, &rpc.resp)
	if err != nil {
		shared.logger.Error("Failed to make RequestVote RPC to peer",
			"id", shared.peerID,
			"address", shared.peerAddr,
			"error", err)
	}
	return err
}

func (rpc *requestVoteRPC) process(p *peerState, err error) {
	// Update bookkeeping on outstanding RPCs.
	if p.control.term == rpc.req.Term && p.control.role == Candidate {
		p.candidate.outstandingRequestVote = false
	}

	// Handle errors during confirm/sendRecv.
	if err != nil {
		p.shared.logger.Error("RequestVote error to peer",
			"id", p.shared.peerID,
			"address", p.shared.peerAddr,
			"error", err)
		return
	}

	// Update progress and candidate state based on response.
	updateTerm(&p.progress, rpc.resp.Term)
	if p.control.term != rpc.req.Term || p.control.role != Candidate {
		return // term or role changed locally
	}
	p.candidate.voteReplied = true
	if rpc.req.Term == rpc.resp.Term {
		if rpc.resp.Granted {
			p.progress.voteGranted = true
			p.shared.logger.Info("Received vote from peer",
				"id", p.shared.peerID,
				"address", p.shared.peerAddr,
				"term", rpc.req.Term)
		} else {
			p.progress.voteGranted = false
			p.shared.logger.Info("Denied vote from peer",
				"id", p.shared.peerID,
				"address", p.shared.peerAddr,
				"term", rpc.req.Term)
		}
	} else {
		p.shared.logger.Info("Peer has newer term",
			"id", p.shared.peerID,
			"address", p.shared.peerAddr,
			"term", rpc.resp.Term)
		if rpc.resp.Granted {
			p.shared.logger.Error("Peer shouldn't grant vote in different term. ignoring",
				"id", p.shared.peerID,
				"address", p.shared.peerAddr)
		}
	}
}

///////////////////////// AppendEntries /////////////////////////

type appendEntriesRPC struct {
	start         time.Time
	req           *AppendEntriesRequest
	resp          *AppendEntriesResponse
	heartbeat     bool
	pipeline      bool
	verifyCounter uint64
}

// Returned from prepare() and checked in process() to set
// peer.leader.needsSnapshot.
var errNeedsSnapshot = errors.New("Need to send snapshot")

func makeHeartbeatRPC(p *peerState) peerRPC {
	start := time.Now()
	p.leader.lastHeartbeatSent = start
	rpc := &appendEntriesRPC{
		start: start,
		req: &AppendEntriesRequest{
			RPCHeader: RPCHeader{p.shared.protocolVersion},
			Term:      p.control.term,
			Leader:    p.shared.trans.EncodePeer(p.shared.localAddr),

			// Heartbeats in the current Peer code serve two purposes:
			//  1. As a sign of life to the peer so that it does not start an
			//     election, and
			//  2. To remind the peer of the commit index, in case it's rebooted.
			//     Specfically, here's the scenario:
			//       1. Leader catches up follower of entire log and commit index.
			//       2. Follower reboots, setting its own commit index to 0.
			//       3. Leader does not generate any additional log entries. The next
			//          heartbeat should inform the follower of the new commit index,
			//          so that the follower can serve not-so-stale reads.
			//
			// Older leader code used to send heartbeats with PrevLogEntry,
			// PrevLogTerm, and LeaderCommitIndex set to 0, serving as a sign of life
			// only. It would also send non-heartbeat AppendEntries RPCs periodically
			// to update the peer's commit index. The new code does not send
			// non-heartbeat AppendEntries RPCs in the absence of activity.
			//
			// Since all zeros won't work in the new code, perhaps the next most
			// obvious thing would be to set PrevLogEntry as the leader's last log
			// index and LeaderCommitIndex as the leader's commit index. However, this
			// would be incompatible with old follower code. While newer followers
			// will consider the leader alive if Term is current, older follower code
			// used to only consider the leader alive if the PrevLogEntry matched the
			// follower's log. So until the leader has determined where the follower's
			// log diverges, it might start an election.
			//
			// So, to retain compatibility with older followers and replicate the
			// commit index, the new code uses the follower's matchIndex for
			// PrevLogEntry. Even after a reboot, the follower will still consider
			// agree on PrevLogEntry, so it'll act as a sign of life, even for older
			// followers. And useful commit index values can still be transferred.
			PrevLogEntry: p.progress.matchIndex,
			PrevLogTerm:  p.progress.matchTerm,
		},
		resp:          &AppendEntriesResponse{},
		heartbeat:     true,
		verifyCounter: p.control.verifyCounter,
	}
	if p.control.commitIndex < p.progress.matchIndex {
		rpc.req.LeaderCommitIndex = p.control.commitIndex
	} else {
		rpc.req.LeaderCommitIndex = p.progress.matchIndex
	}
	return rpc
}

func makeAppendEntriesRPC(p *peerState) peerRPC {
	p.leader.outstandingAppendEntriesRPCs++
	if p.leader.allowPipeline {
		p.leader.outstandingPipelineSend = true
	}
	return &appendEntriesRPC{
		start: time.Now(),
		req: &AppendEntriesRequest{
			RPCHeader:         RPCHeader{p.shared.protocolVersion},
			Term:              p.control.term,
			Leader:            p.shared.trans.EncodePeer(p.shared.localAddr),
			PrevLogEntry:      p.leader.nextIndex - 1,
			PrevLogTerm:       0, // to be filled in during Prepare
			LeaderCommitIndex: 0, // to be filled in during Prepare
		},
		resp:          &AppendEntriesResponse{},
		heartbeat:     false,
		pipeline:      p.leader.allowPipeline,
		verifyCounter: p.control.verifyCounter,
	}
}

func (rpc *appendEntriesRPC) started() time.Time {
	return rpc.start
}

func (rpc *appendEntriesRPC) prepare(shared *peerShared, control peerControl) error {
	if rpc.heartbeat {
		return nil
	}

	// Find prevLogTerm.
	getTerm := func(index Index) (Term, error) {
		if index == 0 {
			return 0, nil
		}
		if index == control.lastIndex {
			return control.lastTerm, nil
		}

		var entry Log
		err := shared.logs.GetLog(index, &entry)
		if err == nil {
			return entry.Term, nil
		}
		if err == ErrLogNotFound {
			meta, err2 := getLastSnapshot(shared.snapshots)
			if err2 != nil {
				return 0, err
			}
			switch {
			case meta.Index == index:
				return meta.Term, nil
			case meta.Index < index:
				return 0, err
			case meta.Index > index:
				return 0, errNeedsSnapshot
			}
		}
		return 0, err
	}
	term, err := getTerm(rpc.req.PrevLogEntry)
	if err != nil {
		if err != errNeedsSnapshot {
			shared.logger.Fatal("Failed to get log term",
				"index", rpc.req.PrevLogEntry,
				"error", err)
		}
		return err
	}
	rpc.req.PrevLogTerm = term

	// Add entries to request.
	lastIndex := rpc.req.PrevLogEntry + Index(shared.options.maxAppendEntries)
	if lastIndex > control.lastIndex {
		lastIndex = control.lastIndex
	}
	numEntries := uint64(0)
	if lastIndex > rpc.req.PrevLogEntry {
		numEntries = uint64(lastIndex - rpc.req.PrevLogEntry)
	}
	rpc.req.Entries = make([]*Log, 0, numEntries)
	for i := rpc.req.PrevLogEntry + 1; i <= lastIndex; i++ {
		var entry Log
		err := shared.logs.GetLog(i, &entry)
		if err == ErrLogNotFound {
			return errNeedsSnapshot
		}
		if err != nil {
			shared.logger.Fatal("Failed to get log entry",
				"index", i, "error", err)
		}
		rpc.req.Entries = append(rpc.req.Entries, &entry)
	}
	rpc.req.LeaderCommitIndex = control.commitIndex
	if rpc.req.LeaderCommitIndex > lastIndex {
		rpc.req.LeaderCommitIndex = lastIndex
	}
	return nil
}

func (rpc *appendEntriesRPC) confirm(p *peerState) error {
	if rpc.req.Term != p.control.term {
		return errors.New("term changed, discarding AppendEntries request")
	}
	if p.control.role != Leader {
		return errors.New("no longer leader, discarding AppendEntries request")
	}
	lastIndex := rpc.req.PrevLogEntry + Index(len(rpc.req.Entries))
	if lastIndex+1 > p.leader.nextIndex {
		p.leader.nextIndex = lastIndex + 1
		p.shared.logger.Info("Optimistically setting next index for peer",
			"id", p.shared.peerID,
			"address", p.shared.peerAddr,
			"next_index", p.leader.nextIndex)
	} else {
		p.shared.logger.Info("Not updating next index for peer, already set",
			"id", p.shared.peerID,
			"address", p.shared.peerAddr,
			"next_index", p.leader.nextIndex)
	}
	if rpc.req.LeaderCommitIndex+1 > p.leader.nextCommitIndex {
		p.leader.nextCommitIndex = rpc.req.LeaderCommitIndex + 1
	}
	return nil
}

func (rpc *appendEntriesRPC) sendRecv(shared *peerShared) error {
	numEntries := uint64(len(rpc.req.Entries))
	var desc string
	if rpc.heartbeat {
		desc = fmt.Sprintf("heartbeat (term %v)", rpc.req.Term)
	} else {
		var entriesDesc string
		switch {
		case numEntries == 0:
			entriesDesc = fmt.Sprintf("no entries")
		case numEntries == 1:
			entriesDesc = fmt.Sprintf("entry %v", rpc.req.PrevLogEntry+1)
		default:
			entriesDesc = fmt.Sprintf("entries %v through %v",
				rpc.req.PrevLogEntry+1, rpc.req.PrevLogEntry+Index(numEntries))
		}
		desc = fmt.Sprintf("AppendEntries (term %v, %s, prev %v term %v, commit %v)",
			rpc.req.Term, entriesDesc, rpc.req.PrevLogEntry, rpc.req.PrevLogTerm, rpc.req.LeaderCommitIndex)
	}
	if rpc.pipeline {
		shared.logger.Info("Sending pipelined to peer",
			"messsage", desc, "id", shared.peerID, "address", shared.peerAddr)
		future, err := shared.pipeline.AppendEntries(rpc.req, rpc.resp)
		select {
		case shared.pipelineSendDoneCh <- rpc.req.Term:
		case <-shared.stopCh:
		}
		if err == nil {
			err = future.Error()
		}
		if err != nil {
			shared.logger.Error("Failed to pipeline AppendEntries to peer",
				"id", shared.peerID, "address", shared.peerAddr,
				"error", err)
			return err
		}
	} else {
		shared.logger.Info("Sending to peer",
			"message", desc, "id", shared.peerID, "address", shared.peerAddr)
		err := shared.trans.AppendEntries(shared.peerAddr, rpc.req, rpc.resp)
		if err != nil {
			shared.logger.Error("Failed to send AppendEntries to peer",
				"id", shared.peerID, "address", shared.peerAddr,
				"error", err)
			return err
		}
	}
	metrics.MeasureSince([]string{"raft", "replication", "appendEntries", "rpc", string(shared.peerID)}, rpc.start)
	metrics.IncrCounter([]string{"raft", "replication", "appendEntries", "logs", string(shared.peerID)}, float32(numEntries))
	return nil
}

func (rpc *appendEntriesRPC) process(p *peerState, err error) {
	lastIndex := rpc.req.PrevLogEntry + Index(len(rpc.req.Entries))

	// Update bookkeeping on outstanding RPCs.
	if p.control.term == rpc.req.Term && p.control.role == Leader &&
		!rpc.heartbeat {
		p.leader.outstandingAppendEntriesRPCs--
	}

	// Handle errors during confirm and sendRecv.
	if err == errNeedsSnapshot {
		if p.control.term == rpc.req.Term && p.control.role == Leader {
			p.leader.needsSnapshot = true
			p.leader.nextIndex = 1
			p.leader.nextCommitIndex = 1
		}
		return
	}

	if err != nil {
		p.shared.logger.Error("AppendEntries failed to peer",
			"id", p.shared.peerID, "address", p.shared.peerAddr,
			"error", err)
		if err == ErrPipelineReplicationNotSupported {
			p.pipelineUnsupported = true
		}
		if p.control.term == rpc.req.Term && p.control.role == Leader {
			if p.leader.allowPipeline {
				p.leader.allowPipeline = false
				p.shared.logger.Info("Disabling pipeline replication to peer",
					"id", p.shared.peerID, "address", p.shared.peerAddr)
			}
			// Restore nextIndex, which may have been updated optimistically in
			// confirm().
			if p.leader.nextIndex == lastIndex+1 {
				p.leader.nextIndex = rpc.req.PrevLogEntry + 1
			}
			if p.leader.nextCommitIndex == rpc.req.LeaderCommitIndex+1 {
				// By this point, we've forgotten what to restore nextCommitIndex to.
				// Setting it back to 1 will schedule another AppendEntries request.
				p.leader.nextCommitIndex = 1
			}
		}
		return
	}

	// Update progress and leader state based on response.
	updateTerm(&p.progress, rpc.resp.Term)
	if p.control.term != rpc.req.Term || p.control.role != Leader {
		return // term or role changed locally
	}

	// We might have succeeded in sending a keep-alive even if the PrevLogEntry
	// didn't match.
	if rpc.resp.Term == rpc.req.Term {
		p.confirmedLeadership(rpc.start, rpc.verifyCounter)
	} else {
		if rpc.resp.Success {
			p.shared.logger.Error("AppendEntries successful but not current term (peer should reply with Success set to false)",
				"id", p.shared.peerID, "address", p.shared.peerAddr)
			rpc.resp.Success = false
		}
	}

	if rpc.resp.Success {
		if p.progress.matchIndex < lastIndex {
			lastTerm := rpc.req.PrevLogTerm
			if len(rpc.req.Entries) > 0 {
				lastTerm = rpc.req.Entries[len(rpc.req.Entries)-1].Term
			}
			p.progress.matchIndex = lastIndex
			p.progress.matchTerm = lastTerm
		}
		if p.leader.nextIndex < lastIndex+1 && lastIndex <= p.control.lastIndex {
			p.leader.nextIndex = lastIndex + 1
		}
		if p.leader.nextCommitIndex < rpc.req.LeaderCommitIndex+1 && rpc.req.LeaderCommitIndex <= p.control.lastIndex {
			p.leader.nextCommitIndex = rpc.req.LeaderCommitIndex + 1
		}
		p.shared.logger.Info("AppendEntries to peer succeeded",
			"id", p.shared.peerID, "address", p.shared.peerAddr,
			"next_index", p.leader.nextIndex)
		if !p.leader.allowPipeline && !p.pipelineUnsupported && !rpc.heartbeat {
			p.leader.allowPipeline = true
			p.shared.logger.Info("Enabling pipelined replication to peer",
				"id", p.shared.peerID, "address", p.shared.peerAddr)
		}
	} else {
		if p.leader.nextIndex > rpc.req.PrevLogEntry {
			if rpc.req.PrevLogEntry > 0 {
				p.leader.nextIndex = rpc.req.PrevLogEntry
			} else {
				p.leader.nextIndex = 1
			}
		}
		if p.leader.nextIndex > rpc.resp.LastLog+1 {
			p.leader.nextIndex = rpc.resp.LastLog + 1
		}
		if p.leader.nextIndex > p.control.lastIndex+1 {
			p.leader.nextIndex = p.control.lastIndex + 1
		}
		if p.leader.nextCommitIndex > p.leader.nextIndex {
			p.leader.nextCommitIndex = p.leader.nextIndex
		}
		if !rpc.heartbeat {
			p.shared.logger.Warn("AppendEntries to peer rejected, sending older log entries",
				"id", p.shared.peerID, "address", p.shared.peerAddr,
				"next_index", p.leader.nextIndex)
		}
		if !rpc.heartbeat && p.leader.allowPipeline {
			p.leader.allowPipeline = false
			p.shared.logger.Info("Disabling pipeline replication to peer",
				"id", p.shared.peerID, "address", p.shared.peerAddr)
		}
	}
}

///////////////////////// InstallSnapshot /////////////////////////

type installSnapshotRPC struct {
	start         time.Time
	req           InstallSnapshotRequest
	resp          InstallSnapshotResponse
	snapID        string
	snapshot      io.ReadCloser
	verifyCounter uint64
}

func makeInstallSnapshotRPC(p *peerState) peerRPC {
	p.leader.outstandingInstallSnapshotRPC = true
	p.leader.needsSnapshot = false
	rpc := &installSnapshotRPC{
		start:         time.Now(),
		verifyCounter: p.control.verifyCounter,
	}
	return rpc
}

func (rpc *installSnapshotRPC) started() time.Time {
	return rpc.start
}

func (rpc *installSnapshotRPC) prepare(shared *peerShared, control peerControl) error {
	meta, err := getLastSnapshot(shared.snapshots)
	if err != nil {
		shared.logger.Error("Sending snapshot but couldn't get latest snapshot", "error", err)
		return err
	}

	// Open the most recent snapshot
	snapID := meta.ID
	meta, snapshot, err := shared.snapshots.Open(snapID)
	if err != nil {
		shared.logger.Error("Failed to open snapshot", "id", snapID, "error", err)
		return err
	}

	// Fill in the request.
	rpc.req = InstallSnapshotRequest{
		RPCHeader:          RPCHeader{shared.protocolVersion},
		SnapshotVersion:    meta.Version,
		Term:               control.term,
		Leader:             shared.trans.EncodePeer(shared.localAddr),
		LastLogIndex:       meta.Index,
		LastLogTerm:        meta.Term,
		Peers:              meta.Peers,
		Size:               meta.Size,
		Configuration:      encodeMembership(meta.Membership),
		ConfigurationIndex: meta.MembershipIndex,
	}
	rpc.snapID = snapID
	rpc.snapshot = snapshot
	return nil
}

func (rpc *installSnapshotRPC) confirm(p *peerState) error {
	if rpc.req.Term != p.control.term {
		return errors.New("term changed, discarding InstallSnapshot request")
	}
	if p.control.role != Leader {
		return errors.New("no longer leader, discarding InstallSnapshot request")
	}
	// Update nextIndex optimistically. Even if this thing fails, we want to kick
	// back to trying AppendEntries requests.
	p.leader.nextIndex = rpc.req.LastLogIndex + 1
	return nil
}

func (rpc *installSnapshotRPC) sendRecv(shared *peerShared) error {
	desc := fmt.Sprintf("InstallSnapshot (term %v, last index %v)", rpc.req.Term, rpc.req.LastLogIndex)
	shared.logger.Info("Sending to peer",
		"message", desc, "id", shared.peerID, "address", shared.peerAddr)
	err := shared.trans.InstallSnapshot(shared.peerAddr, &rpc.req, &rpc.resp, rpc.snapshot)
	if err != nil {
		shared.logger.Error("Failed to install snapshot", "id", rpc.snapID, "error", err)
	}
	metrics.MeasureSince([]string{"raft", "replication", "installSnapshot", string(shared.peerID)}, rpc.start)
	return err
}

func (rpc *installSnapshotRPC) process(p *peerState, err error) {
	// Release rpc.snapshot resources.
	if rpc.snapshot != nil {
		rpc.snapshot.Close()
	}

	// Update bookkeeping on outstanding RPCs.
	if p.control.term == rpc.req.Term && p.control.role == Leader {
		p.leader.outstandingInstallSnapshotRPC = false
	}

	// Handle errors during confirm/sendRecv.
	if err != nil {
		p.shared.logger.Error("InstallSnapshot to failed to peer",
			"id", p.shared.peerID, "address", p.shared.peerAddr, "error", err)
		return
	}

	// Update progress and leader state based on response.
	updateTerm(&p.progress, rpc.resp.Term)
	if p.control.term != rpc.req.Term || p.control.role != Leader {
		return // term or role changed locally
	}

	// We might have succeeded in sending a keep-alive even if the snapshot
	// somehow didn't succeed.
	if rpc.resp.Term == rpc.req.Term {
		p.confirmedLeadership(rpc.start, rpc.verifyCounter)
	} else {
		if rpc.resp.Success {
			p.shared.logger.Error("InstallSnapshot successful but not current term (peer should reply with Success set to false)",
				"id", p.shared.peerID, "address", p.shared.peerAddr)
			rpc.resp.Success = false
		}
	}

	if rpc.resp.Success {
		p.progress.matchIndex = rpc.req.LastLogIndex
		p.progress.matchTerm = rpc.req.LastLogTerm
		p.shared.logger.Info("InstallSnapshot to peer succeeded",
			"id", p.shared.peerID, "address", p.shared.peerAddr,
			"next_index", p.leader.nextIndex)
	} else {
		p.shared.logger.Error("InstallSnapshot to peer rejected",
			"id", p.shared.peerID, "address", p.shared.peerAddr)
	}
}
