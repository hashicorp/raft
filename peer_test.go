package raft

import (
	"fmt"
	"log"
	"strings"
	"testing"
	"time"
)

type TestingPeer struct {
	peerID       ServerID
	peerAddr     ServerAddress
	peerTrans    *InmemTransport
	localAddr    ServerAddress
	localTrans   *InmemTransport
	logger       *log.Logger
	logs         LogStore
	snapshots    SnapshotStore
	goRoutines   *waitGroup
	controlCh    chan peerControl
	progressCh   chan peerProgress
	options      peerOptions
	initControl  *peerControl
	initProgress *peerProgress
	peer         *peerState
}

func makePeerTesting(t *testing.T, tp *TestingPeer) *TestingPeer {
	if tp.logger == nil {
		tp.logger = newTestLogger(t)
	}

	if tp.peerID == "" {
		tp.peerID = ServerID("id2")
	}
	if tp.logs == nil {
		tp.logs = NewInmemStore()
		tp.logs.StoreLogs([]*Log{
			&Log{
				Index: 2,
				Term:  7,
				Type:  LogCommand,
				Data:  []byte("foo"),
			},
			&Log{
				Index: 3,
				Term:  9,
				Type:  LogCommand,
				Data:  []byte("bar"),
			},
		})
	}
	if tp.snapshots == nil {
		// TODO: create snapshot store
	}
	if tp.goRoutines == nil {
		tp.goRoutines = new(waitGroup)
	}
	if tp.controlCh == nil {
		tp.controlCh = make(chan peerControl, 1) // Buffered for testing!
	}
	if tp.progressCh == nil {
		tp.progressCh = make(chan peerProgress, 1) // Buffered for testing!
	}

	if tp.localAddr == "" {
		tp.localAddr = ServerAddress("addr1")
	}
	if tp.localTrans == nil {
		_, tp.localTrans = NewInmemTransport(tp.localAddr)
	}
	if tp.peerAddr == "" {
		tp.peerAddr = ServerAddress("addr2")
	}
	if tp.peerTrans == nil {
		_, tp.peerTrans = NewInmemTransport(tp.peerAddr)
	}
	tp.localTrans.Connect(tp.peerAddr, tp.peerTrans)
	tp.peerTrans.Connect(tp.localAddr, tp.localTrans)

	tp.peer = makePeerInternal(
		tp.peerID,
		tp.peerAddr,
		tp.logger,
		tp.logs,
		tp.snapshots,
		tp.goRoutines,
		tp.localTrans, // give it localTrans so that it can talk to peerTrans
		tp.localAddr,
		tp.controlCh,
		tp.progressCh,
		tp.options)

	if tp.initProgress != nil {
		tp.peer.progress = *tp.initProgress
		tp.peer.progress.peerID = tp.peerID
	}

	if tp.initControl != nil {
		tp.controlCh <- *tp.initControl
		tp.peer.drainNonblocking()
	}

	return tp
}

func maskProgress(progress peerProgress) peerProgress {
	progress.lastContact = time.Time{}
	progress.lastReply = time.Time{}
	return progress
}

func checkProgressTimes(progress peerProgress) error {
	if !progress.lastReply.After(progress.lastContact) &&
		!(progress.lastReply.IsZero() && progress.lastContact.IsZero()) {
		return fmt.Errorf("lastReply should be after lastContact, or both zero. got lastReply %v and lastContact %v",
			progress.lastReply, progress.lastContact)
	}
	if progress.lastReply.After(time.Now()) {
		return fmt.Errorf("lastReply should always be before now. got %v",
			progress.lastReply)
	}
	return nil
}

func waitForProgress(tp *TestingPeer) (peerProgress, error) {
	for i := 0; i < 100; i++ {
		tp.peer.drainNonblocking()
		select {
		case progress := <-tp.progressCh:
			return progress, nil
		default:
			time.Sleep(time.Millisecond)
			continue
		}
	}
	return peerProgress{}, fmt.Errorf("timeout waiting for progress")
}

func waitFor(tp *TestingPeer, cond func() bool) error {
	for i := 0; i < 100; i++ {
		tp.peer.drainNonblocking()
		if cond() {
			return nil
		}
		time.Sleep(time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for condition")
}

func waitForFailure(tp *TestingPeer) error {
	err := waitFor(tp, func() bool { return tp.peer.failures == 1 })
	if err != nil {
		return fmt.Errorf("Expected 1 failure, got %d: %v", tp.peer.failures, err)
	}
	return nil
}

type cannedReply struct {
	expReq interface{}
	reply  interface{}
}

func reqsEqual(a, b interface{}) bool {
	return fmt.Sprintf("%+v", a) == fmt.Sprintf("%+v", b)
}

func serveReplies(tp *TestingPeer, replies []cannedReply) Future {
	f := deferError{}
	f.init()
	go func() {
		for len(replies) > 0 {
			rpc := <-tp.peerTrans.Consumer()
			reply := replies[0]
			replies = replies[1:]
			if reply.expReq != nil && !reqsEqual(rpc.Command, reply.expReq) {
				err := fmt.Errorf("Unexpected message sent:\ngot      %+v,\nexpected %+v",
					rpc.Command, reply.expReq)
				rpc.Respond(nil, err)
				f.respond(err)
				return
			}
			errReply, ok := reply.reply.(error)
			if ok {
				tp.peer.shared.logger.Printf("Replying to %T with error", rpc.Command)
				rpc.Respond(nil, errReply)
			} else {
				tp.peer.shared.logger.Printf("Replying to %T", rpc.Command)
				rpc.Respond(reply.reply, nil)
			}
			f.respond(nil)
		}
	}()
	return &f
}

func oneRPC(tp *TestingPeer, expReq interface{}, reply interface{},
	expProgress peerProgress) (peerProgress, error) {
	serverFuture := serveReplies(tp, []cannedReply{{expReq, reply}})
	start := time.Now()
	if !tp.peer.issueWork() {
		return peerProgress{}, fmt.Errorf("expected issueWork to send an RPC")
	}
	progress, err := waitForProgress(tp)
	if err != nil {
		return peerProgress{}, err
	}
	err = serverFuture.Error()
	if err != nil {
		return peerProgress{}, err
	}
	if maskProgress(progress) != expProgress {
		return peerProgress{}, fmt.Errorf("Unexpected progress:\n"+
			"got      %+v\n"+
			"masked   %+v\n"+
			"expected %+v",
			progress, maskProgress(progress), expProgress)
	}
	if err := checkProgressTimes(progress); err != nil {
		return peerProgress{}, err
	}
	if !progress.lastContact.After(start) {
		return peerProgress{}, fmt.Errorf("lastContact should be after test start")
	}

	tp.peer.drainNonblocking()
	if tp.peer.issueWork() {
		return peerProgress{}, fmt.Errorf("Unexpected work was done")
	}

	if tp.peer.failures > 0 {
		return peerProgress{}, fmt.Errorf("Unexpected failures (%d)", tp.peer.failures)
	}
	if tp.peer.backoffTimer.Stop() {
		return peerProgress{}, fmt.Errorf("Backoff timer was set unexpectedly")
	}

	return progress, nil
}

func oneErrRPC(tp *TestingPeer, expReq interface{}, reply error) error {
	startProgress := tp.peer.progress
	tp.peer.sendProgress = false
	serverFuture := serveReplies(tp, []cannedReply{{expReq, reply}})
	if !tp.peer.issueWork() {
		return fmt.Errorf("expected issueWork to prepare an RPC")
	}
	tp.peer.blockingSelect() // recv on requestCh, calls p.send()
	err := serverFuture.Error()
	if err != nil {
		return err
	}
	if tp.peer.sendProgress || startProgress != tp.peer.progress {
		return fmt.Errorf("Peer should have made no progress, got %+v, expected %+v",
			tp.peer.progress, startProgress)
	}
	if !tp.peer.progress.lastContact.IsZero() {
		return fmt.Errorf("last contact should be zero")
	}

	err = waitForFailure(tp)
	if err != nil {
		return fmt.Errorf("Expected 1 failure, got %d: %v", tp.peer.failures, err)
	}
	running := tp.peer.backoffTimer.Stop()
	if !running {
		return fmt.Errorf("Backoff timer wasn't running")
	}
	if tp.peer.issueWork() {
		return fmt.Errorf("Unexpected work was done")
	}

	return nil
}

// TODO: add test for updateControl

// TODO: add test for updateTerm

///////////////////////// RequestVote /////////////////////////

var requestVoteControl = peerControl{
	term:      12,
	role:      Candidate,
	lastIndex: 3,
	lastTerm:  9,
}

var requestVoteProgress = peerProgress{
	term:            12,
	matchIndex:      10,
	verifiedCounter: 90,
}

func TestPeer_RequestVoteRPC_granted(t *testing.T) {
	tp := makePeerTesting(t, &TestingPeer{
		initControl:  &requestVoteControl,
		initProgress: &requestVoteProgress,
	})
	exp := RequestVoteRequest{
		Term:         12,
		Candidate:    tp.localTrans.EncodePeer(tp.localAddr),
		LastLogIndex: 3,
		LastLogTerm:  9,
	}
	reply := RequestVoteResponse{
		Term:    12,
		Granted: true,
	}
	expProgress := peerProgress{
		peerID:          tp.peerID,
		term:            12,
		voteGranted:     true,
		matchIndex:      10,
		verifiedCounter: 90,
	}
	_, err := oneRPC(tp, &exp, &reply, expProgress)
	if err != nil {
		t.Fatal(err)
	}
}

func TestPeer_RequestVoteRPC_denied(t *testing.T) {
	tp := makePeerTesting(t, &TestingPeer{
		initControl:  &requestVoteControl,
		initProgress: &requestVoteProgress,
	})
	reply := RequestVoteResponse{
		Term:    12,
		Granted: false,
	}
	expProgress := peerProgress{
		peerID:          tp.peerID,
		term:            12,
		voteGranted:     false,
		matchIndex:      10,
		verifiedCounter: 90,
	}
	_, err := oneRPC(tp, nil, &reply, expProgress)
	if err != nil {
		t.Fatal(err)
	}
}

func TestPeer_RequestVoteRPC_newTerm(t *testing.T) {
	tp := makePeerTesting(t, &TestingPeer{
		initControl:  &requestVoteControl,
		initProgress: &requestVoteProgress,
	})
	reply := RequestVoteResponse{
		Term:    13,
		Granted: true,
	}
	expProgress := peerProgress{
		peerID:          tp.peerID,
		term:            13,
		voteGranted:     false,
		matchIndex:      0,
		verifiedCounter: 90,
	}
	_, err := oneRPC(tp, nil, &reply, expProgress)
	if err != nil {
		t.Fatal(err)
	}
}

func TestPeer_RequestVoteRPC_confirmError(t *testing.T) {
	control := requestVoteControl
	tp := makePeerTesting(t, &TestingPeer{
		initControl:  &control,
		initProgress: &requestVoteProgress,
	})
	rpc := makeRequestVoteRPC(tp.peer)
	err := rpc.prepare(tp.peer.shared, tp.peer.control)
	if err != nil {
		t.Fatalf("Unexpected error in prepare: %v", err)
	}

	control.role = Follower
	tp.controlCh <- control
	tp.peer.drainNonblocking()
	err = rpc.confirm(tp.peer)
	if err == nil || !strings.Contains(err.Error(), "no longer candidate") {
		t.Fatalf("Expected cancel due to not candidate, got %v", err)
	}

	control.role = Candidate
	control.term++
	tp.controlCh <- control
	tp.peer.drainNonblocking()
	err = rpc.confirm(tp.peer)
	if err == nil || !strings.Contains(err.Error(), "term changed") {
		t.Fatalf("Expected cancel due to larger term, got %v", err)
	}
}

func TestPeer_RequestVoteRPC_sendRecvError(t *testing.T) {
	tp := makePeerTesting(t, &TestingPeer{
		initControl:  &requestVoteControl,
		initProgress: &requestVoteProgress,
	})
	err := oneErrRPC(tp, nil, fmt.Errorf("a transport error"))
	if err != nil {
		t.Fatal(err)
	}
}

///////////////////////// AppendEntries /////////////////////////

var appendEntriesControl = peerControl{
	term:          12,
	role:          Leader,
	lastIndex:     3,
	lastTerm:      9,
	verifyCounter: 120,
}

var appendEntriesProgress = peerProgress{
	term:            12,
	matchIndex:      1,
	verifiedCounter: 90,
}

func TestPeer_AppendEntries_heartbeat(t *testing.T) {
	tp := makePeerTesting(t, &TestingPeer{
		initControl:  &appendEntriesControl,
		initProgress: &appendEntriesProgress,
	})
	exp := AppendEntriesRequest{
		Term:         12,
		Leader:       tp.localTrans.EncodePeer(tp.localAddr),
		PrevLogEntry: 3,
		PrevLogTerm:  9,
	}
	reply := AppendEntriesResponse{
		Term:    12,
		LastLog: 0,
		Success: true,
	}
	expProgress := peerProgress{
		peerID:          tp.peerID,
		term:            12,
		matchIndex:      3,
		verifiedCounter: 120,
	}
	_, err := oneRPC(tp, &exp, &reply, expProgress)
	if err != nil {
		t.Fatal(err)
	}
}

// TODO: more tests for AppendEntries

///////////////////////// InstallSnapshot /////////////////////////

// TODO: more tests for InstallSnapshot
