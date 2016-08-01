package raft

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/armon/go-metrics"
)

type peerOptions struct {
	// Where to print debug messages, or nil for stderr.
	logger *log.Logger
	// No more than this many entries will be sent in one AppendEntries request.
	maxAppendEntries int
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

	// Where to print debug messages, or nil for stderr.
	logger *log.Logger

	// Used to spawn goroutines so others can wait on their exit.
	goRoutines *waitGroup

	// Used to read log entries to send in AppendEntries.
	logs LogStore

	// Used to read snapshots to send in InstallSnapshot.
	snapshots SnapshotStore

	// Used to communicate with the remote server.
	trans Transport

	// Used to send many AppendEntries to the peer in rapid succession
	// (without waiting for the prevous response). Set to nil if the transport
	// does not support pipelining.
	pipeline AppendPipeline

	// The address of this local server, which is sent to the remote server in
	// most RPCs (constant).
	localAddr ServerAddress

	// Uniquely identifies the remote server (constant).
	peerID ServerID

	// Network address of the remote server (constant).
	peerAddr ServerAddress

	// Helper goroutines send fully prepared RPC requests onto requestCh.
	// The main Peer goroutine validates each request against the latest
	// control information, then spawns another goroutine to send it.
	requestCh chan peerRPC

	// Helper goroutines send RPC replies onto replyCh for the main Peer goroutine
	// to process.
	replyCh chan peerRPC

	pipelineSendDoneCh chan struct{}

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
	term uint64

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
	lastIndex uint64

	// The term of the lastIndex entry.
	lastTerm uint64

	// The last entry committed in the log (this may be past lastIndex).
	commitIndex uint64
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
	term uint64

	// Set to true if the peer granted this server a vote in 'term'.
	voteGranted bool

	// The index in the log that the remote server has acknowledged as matching
	// this server's. Reset to 0 when the term changes.
	matchIndex uint64

	// A lower bound of when the peer last heard from us. Upon the receipt of any
	// reply from the peer (successful or not), this is updated to the time the
	// request was started.
	lastContact time.Time

	// An upper bound of when the peer last heard from us. Upon the receipt of any
	// reply from the peer (successful or not), this is updated to the current
	// time.
	lastReply time.Time

	// The value of control.verifyCounter when the last completed heartbeat to the
	// peer was sent.
	verifiedCounter uint64
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

	// If true, a vote has already been requested from this Peer during
	// control.term.
	sentRequestVote bool

	// The last time a heartbeat has been sent to this Peer during control.term.
	// As leader, this will send another heartbeat after
	// lastHeartbeatSent + heartbeatInterval.
	lastHeartbeatSent time.Time

	// As leader, nextIndex is the index of the next log entry to send to the
	// peer, which may fall one entry past the end of the log.
	nextIndex uint64

	// allowPipeline is true when AppendEntries RPCs should be sent over a
	// pipeline (a separate path through the Transport), false otherwise.
	allowPipeline bool

	// If true, the peer needs a snapshot because this server has already
	// discarded the log entries it would otherwise send in AppendEntries.
	needsSnapshot bool

	// Set to true while an InstallSnapshot RPC is in progress. This prevents
	// further InstallSnapshot and bulk AppendEntries RPCs from starting.
	outstandingInstallSnapshotRPC bool

	// Counts the number of non-heartbeat AppendEntries and InstallSnapshot RPCs
	// that have been sent but have not completed (in either error or response).
	// When not pipelining, such RPCs are only sent when this 0.
	outstandingAppendEntriesRPCs uint64

	outstandingPipelineSend bool

	// Counts the number of consecutive RPCs that have failed with transport-level
	// errors since the last success. Used to apply increasing amounts of backoff.
	failures uint64

	// If failures is nonzero, bulk RPCs (AppendEntries/InstallSnapshot) are not
	// sent until this timer elapses.
	backoffTimer *time.Timer
}

// startPeer is the normal way for
func startPeer(serverID ServerID,
	serverAddress ServerAddress,
	logs LogStore,
	snapshots SnapshotStore,
	goRoutines *waitGroup,
	trans Transport,
	localAddr ServerAddress,
	progressCh chan<- peerProgress,
	options peerOptions) chan<- peerControl {
	controlCh := make(chan peerControl)
	p := makePeerInternal(serverID, serverAddress, logs, snapshots,
		goRoutines, trans, localAddr, controlCh, progressCh, options)
	p.shared.goRoutines.spawn(p.selectLoop)
	return controlCh
}

func makePeerInternal(serverID ServerID,
	serverAddress ServerAddress,
	logs LogStore,
	snapshots SnapshotStore,
	goRoutines *waitGroup,
	trans Transport,
	localAddr ServerAddress,
	controlCh <-chan peerControl,
	progressCh chan<- peerProgress,
	options peerOptions) *peerState {

	if options.logger == nil {
		options.logger = log.New(os.Stderr, "", log.LstdFlags)
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

	p := &peerState{
		shared: &peerShared{
			options:            options,
			peerID:             serverID,
			peerAddr:           serverAddress,
			trans:              trans,
			localAddr:          localAddr,
			logger:             options.logger,
			logs:               logs,
			snapshots:          snapshots,
			goRoutines:         goRoutines,
			requestCh:          make(chan peerRPC),
			replyCh:            make(chan peerRPC),
			pipelineSendDoneCh: make(chan struct{}),
			stopCh:             make(chan struct{}),
		},
		controlCh:  controlCh,
		progressCh: progressCh,

		backoffTimer: time.NewTimer(time.Hour),
		progress: peerProgress{
			peerID: serverID,
		},
	}
	p.backoffTimer.Stop()

	pipeline, err := p.shared.trans.AppendEntriesPipeline(p.shared.peerAddr)
	if err != nil {
		if err != ErrPipelineReplicationNotSupported {
			p.shared.logger.Printf("[ERR] raft: Failed to start pipeline replication to %s: %s",
				p.shared.peerAddr, err)
		}
	} else {
		p.shared.pipeline = pipeline
	}

	return p
}

func (p *peerState) checkInvariants() error {
	if p.shared.peerID != p.progress.peerID {
		return fmt.Errorf("Progress must have peerID")
	}
	if p.progress.lastContact.After(p.progress.lastReply) ||
		p.progress.lastContact == p.progress.lastReply && !p.progress.lastContact.IsZero() {
		return fmt.Errorf("lastContact must be after lastReply (or both 0)")
	}
	if p.nextIndex > p.control.lastIndex+1 {
		return fmt.Errorf("nextIndex must not be more than 1 entry past the lastIndex")
	}
	if p.outstandingInstallSnapshotRPC && p.outstandingAppendEntriesRPCs > 0 {
		return fmt.Errorf("must not send a snapshot and entries simultaneously")
	}
	return nil
}

// selectLoop is a long-running routine that sends RPCs to a single remote
// server.
func (p *peerState) selectLoop() {
	for {
		// At this point, we may or may not have more work that we could do right away.
		// Check for new information but don't block.
		p.drainNonblocking()
		if p.control.role == Shutdown {
			break
		}
		if err := p.checkInvariants(); err != nil {
			panic("peer invariant violated: " + err.Error())
		}
		didSomething := p.issueWork()
		// If we've got no more work to do, block until something changes.
		if !didSomething {
			p.blockingSelect()
		}
	}

	p.shared.logger.Printf("[INFO] raft: Peer routine for %v exiting", p.shared.peerID)
	close(p.shared.stopCh)
	p.shared.pipeline.Close()
}

// drainNonblocking reads/writes the Peer channels as much as possible without
// blocking.
func (p *peerState) drainNonblocking() {
	for {
		// These cases should be exactly the same as those in blockingSelect, except
		// with an additional default branch and without the heartbeat timer.
		select {
		case control := <-p.controlCh:
			p.updateControl(control)
		case p.progressChIfDirty() <- p.progress:
			p.sendProgress = false
		case request := <-p.shared.requestCh:
			p.send(request)
		case rpc := <-p.shared.replyCh:
			p.processReply(rpc)
		case <-p.shared.pipelineSendDoneCh:
			p.outstandingPipelineSend = false
		case <-p.backoffTimer.C:
			p.failures = 0
		default:
			return
		}
	}
}

// blockingSelect reads/writes the Peer channels just once, blocking if needed.
func (p *peerState) blockingSelect() {
	var heartbeatTimer <-chan time.Time
	if p.control.role == Leader {
		// We need to send a heartbeat at lastHeartbeatSent + heartbeatInterval.
		heartbeatTimer = time.After(p.shared.options.heartbeatInterval -
			time.Since(p.lastHeartbeatSent))
	}

	// These cases should be exactly the same as those in drainNonblocking,
	// except missing the default branch and with the additional heartbeat timer.
	select {
	case control := <-p.controlCh:
		p.updateControl(control)
	case p.progressChIfDirty() <- p.progress:
		p.sendProgress = false
	case request := <-p.shared.requestCh:
		p.send(request)
	case rpc := <-p.shared.replyCh:
		p.processReply(rpc)
	case <-p.shared.pipelineSendDoneCh:
		p.outstandingPipelineSend = false
	case <-p.backoffTimer.C:
		p.failures = 0
	case <-heartbeatTimer:
		// nothing more to do, just needed to be woken
	}
}

// updateControl sets p.control and resets some other affected state.
func (p *peerState) updateControl(latest peerControl) {
	if p.control.term < latest.term {
		p.sentRequestVote = false
		p.lastHeartbeatSent = time.Time{}
		p.disablePipeline()
	}
	if latest.role == Leader &&
		(p.control.role != Leader || p.control.term < latest.term) {
		p.nextIndex = latest.lastIndex + 1
	}
	p.control = latest
}

func (p *peerState) progressChIfDirty() chan<- peerProgress {
	if p.sendProgress {
		return p.progressCh
	}
	return nil
}

// start creates a peerRPC object, then spawns a goroutine to prepare its
// request and notify the peer's requestCh. It returns right away.
func (p *peerState) start(makeRPC func(*peerState) peerRPC) {
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
			p.failures++
			p.backoffTimer.Reset(backoff(p.failures,
				p.shared.options.failureWait,
				p.shared.options.maxFailureWait))
		}
		rpc.orig.process(p, rpc.err)

	default:
		p.sendProgress = true
		p.failures = 0
		p.backoffTimer.Stop()
		rpc.process(p, nil)
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
		if !p.sentRequestVote && p.failures == 0 {
			p.start(makeRequestVoteRPC)
			return true
		}

	case Leader:
		// Send a heartbeat (empty AppendEntries) RPC.
		if time.Now().After(p.lastHeartbeatSent.Add(p.shared.options.heartbeatInterval)) {
			p.start(makeHeartbeatRPC)
			return true
		}

		// Send an AppendEntries/InstallSnapshot RPC.
		if p.nextIndex < p.control.lastIndex+1 && p.failures == 0 {
			if p.needsSnapshot {
				if !p.outstandingInstallSnapshotRPC {
					p.start(makeInstallSnapshotRPC)
					return true
				}
			} else {
				if p.outstandingAppendEntriesRPCs == 0 ||
					(p.allowPipeline && !p.outstandingPipelineSend) {
					p.start(makeAppendEntriesRPC)
					return true
				}
			}
		}
	}

	return false
}

func (p *peerState) enablePipeline() {
	if !p.allowPipeline {
		p.allowPipeline = true
		p.shared.logger.Printf("[INFO] raft: Enabling pipelining replication to peer %v",
			p.shared.peerID)
	}
}

func (p *peerState) disablePipeline() {
	if p.allowPipeline {
		p.allowPipeline = false
		p.shared.logger.Printf("[INFO] raft: Disabling pipeline replication to peer %v",
			p.shared.peerID)
	}
}

// updateTerm clears out some progress fields when its term changes.
func updateTerm(progress *peerProgress, term uint64) {
	if term > progress.term {
		progress.term = term
		progress.voteGranted = false
		progress.matchIndex = 0
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
	p.sentRequestVote = true
	return &requestVoteRPC{
		start: time.Now(),
		req: RequestVoteRequest{
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
		return fmt.Errorf("term changed, discarding RequestVote request")
	}
	if p.control.role != Candidate {
		return fmt.Errorf("no longer candidate, discarding RequestVote request")
	}
	return nil
}

func (rpc *requestVoteRPC) sendRecv(shared *peerShared) error {
	shared.logger.Printf("[INFO] raft: Sending RequestVote (term %v) to %v",
		rpc.req.Term, shared.peerID)
	err := shared.trans.RequestVote(shared.peerAddr, &rpc.req, &rpc.resp)
	if err != nil {
		shared.logger.Printf("[ERR] raft: Failed to make RequestVote RPC to %v: %v",
			shared.peerID, err)
	}
	return err
}

func (rpc *requestVoteRPC) process(p *peerState, err error) {
	if err != nil {
		p.shared.logger.Printf("[ERR] raft: RequestVote to %v error: %v",
			p.shared.peerID, err)
		return
	}
	updateTerm(&p.progress, rpc.resp.Term)
	if rpc.req.Term == rpc.resp.Term {
		if rpc.resp.Granted {
			p.progress.voteGranted = true
			p.shared.logger.Printf("[INFO] raft: Received vote from %v in term %v",
				p.shared.peerID, rpc.req.Term)
		} else {
			p.progress.voteGranted = false
			p.shared.logger.Printf("[INFO] raft: Denied vote from %v in term %v",
				p.shared.peerID, rpc.req.Term)
		}
	} else {
		p.shared.logger.Printf("[INFO] raft: %v has newer term %v",
			p.shared.peerID, rpc.resp.Term)
		if rpc.resp.Granted {
			p.shared.logger.Printf("[ERR] raft: %v shouldn't grant vote in different term. ignoring",
				p.shared.peerID)
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
	needsSnapshot bool
	stopPipeline  bool
	verifyCounter uint64
}

func makeHeartbeatRPC(p *peerState) peerRPC {
	start := time.Now()
	p.lastHeartbeatSent = start
	return &appendEntriesRPC{
		start: start,
		req: &AppendEntriesRequest{
			Term:              p.control.term,
			Leader:            p.shared.trans.EncodePeer(p.shared.localAddr),
			PrevLogEntry:      p.control.lastIndex,
			PrevLogTerm:       p.control.lastTerm,
			LeaderCommitIndex: p.control.commitIndex,
		},
		resp:          &AppendEntriesResponse{},
		heartbeat:     true,
		verifyCounter: p.control.verifyCounter,
	}
}

func makeAppendEntriesRPC(p *peerState) peerRPC {
	p.outstandingAppendEntriesRPCs++
	if p.allowPipeline {
		p.outstandingPipelineSend = true
	}
	return &appendEntriesRPC{
		start: time.Now(),
		req: &AppendEntriesRequest{
			Term:              p.control.term,
			Leader:            p.shared.trans.EncodePeer(p.shared.localAddr),
			PrevLogEntry:      p.nextIndex - 1,
			PrevLogTerm:       0, // to be filled in during Prepare
			LeaderCommitIndex: p.control.commitIndex,
		},
		resp:          &AppendEntriesResponse{},
		heartbeat:     false,
		pipeline:      p.allowPipeline,
		verifyCounter: p.control.verifyCounter,
	}
}

func (rpc *appendEntriesRPC) started() time.Time {
	return rpc.start
}

func getLastSnapshot(snapshots SnapshotStore) (*SnapshotMeta, error) {
	meta, err := snapshots.List()
	if err != nil {
		return nil, err
	}
	if len(meta) == 0 {
		return nil, errors.New("No snapshots found")
	}
	return meta[0], nil
}

func (rpc *appendEntriesRPC) prepare(shared *peerShared, control peerControl) error {
	if rpc.heartbeat {
		return nil
	}

	// Find prevLogTerm.
	getTerm := func(index uint64) (uint64, error) {
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
				rpc.needsSnapshot = true
				return 0, nil
			}
		}
		return 0, err
	}
	term, err := getTerm(rpc.req.PrevLogEntry)
	if err != nil {
		shared.logger.Printf("[ERR] raft: Failed to get log term for %d: %v", rpc.req.PrevLogEntry, err)
		return err
	}
	if rpc.needsSnapshot {
		return nil
	}
	rpc.req.PrevLogTerm = term

	// Add entries to request.
	lastIndex := min(control.lastIndex,
		rpc.req.PrevLogEntry+uint64(shared.options.maxAppendEntries))
	rpc.req.Entries = make([]*Log, 0, lastIndex-rpc.req.PrevLogEntry)
	for i := rpc.req.PrevLogEntry + 1; i <= lastIndex; i++ {
		var entry Log
		err := shared.logs.GetLog(i, &entry)
		if err == ErrLogNotFound {
			rpc.needsSnapshot = true
			return nil
		}
		if err != nil {
			shared.logger.Printf("[ERR] raft: Failed to get log entry %d: %v", i, err)
			return err
		}
		rpc.req.Entries = append(rpc.req.Entries, &entry)
	}
	return nil
}

func (rpc *appendEntriesRPC) confirm(p *peerState) error {
	if rpc.needsSnapshot {
		p.needsSnapshot = true
		p.disablePipeline()
		return fmt.Errorf("need to send snapshot")
	}
	if rpc.req.Term != p.control.term {
		return fmt.Errorf("term changed, discarding AppendEntries request")
	}
	if p.control.role != Leader {
		return fmt.Errorf("no longer leader, discarding AppendEntries request")
	}
	if rpc.pipeline {
		p.nextIndex = rpc.req.PrevLogEntry + uint64(len(rpc.req.Entries))
	}
	return nil
}

func (rpc *appendEntriesRPC) sendRecv(shared *peerShared) error {
	numEntries := uint64(len(rpc.req.Entries))
	defer func() {
		metrics.MeasureSince([]string{"raft", "replication", "appendEntries", "rpc", string(shared.peerID)}, rpc.start)
		metrics.IncrCounter([]string{"raft", "replication", "appendEntries", "logs", string(shared.peerID)}, float32(numEntries))
	}()
	var desc string
	switch {
	case rpc.heartbeat:
		desc = fmt.Sprintf("heartbeat (term %v, prev %v term %v)",
			rpc.req.Term, rpc.req.PrevLogEntry, rpc.req.PrevLogTerm)
	case numEntries == 0:
		desc = fmt.Sprintf("AppendEntries (term %v, no entries, prev %v term %v)",
			rpc.req.Term, rpc.req.PrevLogEntry, rpc.req.PrevLogTerm)
	default:
		desc = fmt.Sprintf("AppendEntries (term %v, entries %v through %v, prev %v term %v)",
			rpc.req.Term, rpc.req.PrevLogEntry+1, rpc.req.PrevLogEntry+numEntries,
			rpc.req.PrevLogEntry, rpc.req.PrevLogTerm)
	}
	if rpc.pipeline {
		shared.logger.Printf("[INFO] raft: Sending pipelined %v to %v", desc, shared.peerID)
		future, err := shared.pipeline.AppendEntries(rpc.req, rpc.resp)
		select {
		case shared.pipelineSendDoneCh <- struct{}{}:
		case <-shared.stopCh:
		}
		if err == nil {
			err = future.Error()
		}
		if err != nil {
			shared.logger.Printf("[ERR] raft: Failed to pipeline AppendEntries to %v: %v",
				shared.peerID, err)
			rpc.stopPipeline = true
			return err
		}
	} else {
		shared.logger.Printf("[INFO] raft: Sending %v to %v", desc, shared.peerID)
		err := shared.trans.AppendEntries(shared.peerAddr, rpc.req, rpc.resp)
		if err != nil {
			shared.logger.Printf("[ERR] raft: Failed to send AppendEntries to %v: %v",
				shared.peerID, err)
			return err
		}
	}
	return nil
}

func (rpc *appendEntriesRPC) process(p *peerState, err error) {
	defer func() {
		if !rpc.heartbeat {
			p.outstandingAppendEntriesRPCs--
		}
		if rpc.stopPipeline {
			p.disablePipeline()
		}
	}()
	if err != nil {
		p.shared.logger.Printf("[ERR] raft: AppendEntries to %v error: %v",
			p.shared.peerID, err)
		p.disablePipeline()
		return
	}
	updateTerm(&p.progress, rpc.resp.Term)
	if rpc.resp.Term != rpc.req.Term {
		return
	}
	if rpc.resp.Success {
		lastIndex := rpc.req.PrevLogEntry + uint64(len(rpc.req.Entries))
		if p.progress.matchIndex < lastIndex {
			p.progress.matchIndex = lastIndex
		}
		if p.nextIndex < lastIndex+1 && lastIndex <= p.control.lastIndex {
			p.nextIndex = lastIndex + 1
		}
		p.enablePipeline()
		p.shared.logger.Printf("[INFO] raft: AppendEntries to %v succeeded. nextIndex is now %v",
			p.shared.peerID, p.nextIndex)
	} else {
		if rpc.req.PrevLogEntry > 0 {
			if p.nextIndex > rpc.req.PrevLogEntry {
				p.nextIndex = rpc.req.PrevLogEntry
			}
			if p.nextIndex > rpc.resp.LastLog+1 {
				p.nextIndex = rpc.resp.LastLog + 1
			}
			if p.nextIndex > p.control.lastIndex+1 {
				p.nextIndex = p.control.lastIndex + 1
			}
			if !rpc.heartbeat {
				p.shared.logger.Printf("[WARN] raft: AppendEntries to %v rejected, sending older log entries (next: %d)",
					p.shared.peerID, p.nextIndex)
			}
		} else {
			p.shared.logger.Printf("[ERR] raft: AppendEntries to %v rejected but had sent entry 1",
				p.shared.peerID)
			p.nextIndex = 1
		}
		if !rpc.heartbeat {
			p.disablePipeline()
		}
	}
	if rpc.start.After(p.lastHeartbeatSent) {
		p.lastHeartbeatSent = rpc.start
	}
	if p.progress.verifiedCounter < rpc.verifyCounter {
		p.progress.verifiedCounter = rpc.verifyCounter
	}
}

///////////////////////// InstallSnapshot /////////////////////////

type installSnapshotRPC struct {
	start    time.Time
	req      InstallSnapshotRequest
	resp     InstallSnapshotResponse
	snapID   string
	snapshot io.ReadCloser
}

func makeInstallSnapshotRPC(p *peerState) peerRPC {
	p.outstandingInstallSnapshotRPC = true
	rpc := &installSnapshotRPC{
		start: time.Now(),
	}
	return rpc
}

func (rpc *installSnapshotRPC) started() time.Time {
	return rpc.start
}

func (rpc *installSnapshotRPC) prepare(shared *peerShared, control peerControl) error {
	// Get the snapshots
	snapshots, err := shared.snapshots.List()
	if err != nil {
		shared.logger.Printf("[ERR] raft: Failed to list snapshots: %v", err)
		return err
	}

	// Check we have at least a single snapshot
	if len(snapshots) == 0 {
		err := errors.New("raft: Sending snapshot but no snapshots found")
		shared.logger.Print(err)
		return err
	}

	// Open the most recent snapshot
	snapID := snapshots[0].ID
	meta, snapshot, err := shared.snapshots.Open(snapID)
	if err != nil {
		shared.logger.Printf("[ERR] raft: Failed to open snapshot %v: %v", snapID, err)
		return err
	}

	// Fill in the request.
	rpc.req = InstallSnapshotRequest{
		Term:               control.term,
		Leader:             shared.trans.EncodePeer(shared.localAddr),
		LastLogIndex:       meta.Index,
		LastLogTerm:        meta.Term,
		Peers:              meta.Peers,
		Size:               meta.Size,
		Configuration:      encodeConfiguration(meta.Configuration),
		ConfigurationIndex: meta.ConfigurationIndex,
	}
	rpc.snapID = snapID
	rpc.snapshot = snapshot
	return nil
}

func (rpc *installSnapshotRPC) confirm(p *peerState) error {
	if rpc.req.Term != p.control.term {
		return fmt.Errorf("term changed, discarding InstallSnapshot request")
	}
	if p.control.role != Leader {
		return fmt.Errorf("no longer leader, discarding InstallSnapshot request")
	}
	return nil
}

func (rpc *installSnapshotRPC) sendRecv(shared *peerShared) error {
	defer rpc.snapshot.Close()
	shared.logger.Printf("[INFO] raft: Sending InstallSnapshot (term %v, last index %v) to %v",
		rpc.req.Term, rpc.req.LastLogIndex, shared.peerID)
	err := shared.trans.InstallSnapshot(shared.peerAddr, &rpc.req, &rpc.resp, rpc.snapshot)
	if err != nil {
		shared.logger.Printf("[ERR] raft: Failed to install snapshot %v: %v", rpc.snapID, err)
	}
	return err
}

func (rpc *installSnapshotRPC) process(p *peerState, err error) {
	if rpc.snapshot != nil {
		defer rpc.snapshot.Close()
	}
	p.outstandingInstallSnapshotRPC = false
	p.needsSnapshot = false
	if err != nil {
		p.shared.logger.Printf("[ERR] raft: InstallSnapshot to %v error: %v",
			p.shared.peerID, err)
		return
	}
	metrics.MeasureSince([]string{"raft", "replication", "installSnapshot", string(p.shared.peerID)}, rpc.start)
	updateTerm(&p.progress, rpc.resp.Term)
	if rpc.resp.Term != rpc.req.Term {
		return
	}
	if rpc.resp.Success {
		p.progress.matchIndex = rpc.req.LastLogIndex
		p.nextIndex = rpc.req.LastLogIndex + 1
		p.shared.logger.Printf("[INFO] raft: InstallSnapshot to %v succeeded. nextIndex is now %v",
			p.shared.peerID, p.nextIndex)
	} else {
		p.shared.logger.Printf("[WARN] raft: InstallSnapshot to %v rejected", p.shared.peerID)
	}
	if rpc.start.After(p.lastHeartbeatSent) {
		p.lastHeartbeatSent = rpc.start
	}
}
