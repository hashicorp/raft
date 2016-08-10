package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
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
	snapshotDir  string
	goRoutines   *waitGroup
	controlCh    chan peerControl
	progressCh   chan peerProgress
	options      peerOptions
	initControl  *peerControl
	initProgress *peerProgress
	peer         *peerState
}

var (
	configuration3 = Configuration{}
	entry14        = Log{
		Index: 14,
		Term:  70,
		Type:  LogCommand,
		Data:  []byte("foo"),
	}
	entry15 = Log{
		Index: 15,
		Term:  75,
		Type:  LogCommand,
		Data:  []byte("bar"),
	}
	entry16 = Log{
		Index: 16,
		Term:  77,
		Type:  LogCommand,
		Data:  []byte("baz"),
	}
	entry17 = Log{
		Index: 17,
		Term:  80,
		Type:  LogCommand,
		Data:  []byte("almost"),
	}
	entry18 = Log{
		Index: 18,
		Term:  83,
		Type:  LogCommand,
		Data:  []byte("there"),
	}
)

// This peer has:
// One snapshot with index 1-15, term 75 and a configuration at index 3, length 5 bytes.
// Log entries 14-18 as above.
func makePeerTesting(t *testing.T, tp *TestingPeer) *TestingPeer {
	if tp.logger == nil {
		tp.logger = newTestLogger(t)
	}

	if tp.peerID == "" {
		tp.peerID = ServerID("id2")
	}
	if tp.logs == nil {
		tp.logs = NewInmemStore()
		tp.logs.StoreLogs([]*Log{&entry14, &entry15, &entry16, &entry17, &entry18})
	}
	if tp.snapshots == nil {
		tp.snapshotDir, tp.snapshots = FileSnapTest(t)
		sink, err := tp.snapshots.Create(15, 75, configuration3, 3)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		_, err = sink.Write([]byte("hello"))
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		err = sink.Close()
		if err != nil {
			t.Fatalf("err: %v", err)
		}
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
		tp.peer.blockingSelect()
	}

	return tp
}

func (tp *TestingPeer) close() {
	if tp.snapshotDir != "" {
		os.RemoveAll(tp.snapshotDir)
		tp.snapshotDir = ""
	}
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
		// Abuse backoffTimer to break out of blockingSelect
		tp.peer.backoffTimer.Reset(time.Millisecond)
		tp.peer.blockingSelect()
		select {
		case progress := <-tp.progressCh:
			return progress, nil
		default:
			continue
		}
	}
	return peerProgress{}, errors.New("timeout waiting for progress")
}

func waitFor(tp *TestingPeer, cond func() bool) error {
	for i := 0; i < 100; i++ {
		// Abuse backoffTimer to break out of blockingSelect
		tp.peer.backoffTimer.Reset(time.Millisecond)
		tp.peer.blockingSelect()
		if cond() {
			return nil
		}
	}
	return errors.New("timeout waiting for condition")
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

// Returns a string representation of an RPC request or response. Currently uses
// JSON encoding, as that does better with AppendEntriesRequest.Entries than
// "%#v" and "%+v". In case of an encoding error, returns a unique error string
// (so that strings for two distinct erroneous messages won't be equal).
var msgToStringNonce = 1

func msgToString(msg interface{}) string {
	bytes, err := json.Marshal(msg)
	if err != nil {
		msgToStringNonce++
		return fmt.Sprintf("%s (%d)", err.Error(), msgToStringNonce)
	}
	return string(bytes)
}

func msgsEqual(a, b interface{}) bool {
	return msgToString(a) == msgToString(b)
}

func serveReplies(tp *TestingPeer, replies []cannedReply) Future {
	f := deferError{}
	f.init()
	go func() {
		for len(replies) > 0 {
			rpc := <-tp.peerTrans.Consumer()
			reply := replies[0]
			replies = replies[1:]
			if reply.expReq != nil && !msgsEqual(rpc.Command, reply.expReq) {
				err := fmt.Errorf("Unexpected message sent:\n"+
					"got      %+v\n"+
					"expected %+v",
					msgToString(rpc.Command), msgToString(reply.expReq))
				rpc.Respond(nil, err)
				f.respond(err)
				return
			}
			errReply, ok := reply.reply.(error)
			if ok {
				tp.peer.shared.logger.Printf("Peer Test: Replying to %T with error", rpc.Command)
				rpc.Respond(nil, errReply)
			} else {
				tp.peer.shared.logger.Printf("Peer Test: Replying to %T", rpc.Command)
				rpc.Respond(reply.reply, nil)
			}
			f.respond(nil)
		}
	}()
	return &f
}

func oneRPC(tp *TestingPeer, expReq interface{}, reply interface{},
	expProgress peerProgress, more bool) error {
	serverFuture := serveReplies(tp, []cannedReply{{expReq, reply}})
	start := time.Now()
	if !tp.peer.issueWork() {
		return errors.New("expected issueWork to send an RPC")
	}
	progress, err := waitForProgress(tp)
	if err != nil {
		return err
	}
	err = serverFuture.Error()
	if err != nil {
		return err
	}
	if maskProgress(progress) != expProgress {
		return fmt.Errorf("Unexpected progress:\n"+
			"got      %+v\n"+
			"masked   %+v\n"+
			"expected %+v",
			progress, maskProgress(progress), expProgress)
	}
	if err := checkProgressTimes(progress); err != nil {
		return err
	}
	if !progress.lastContact.After(start) {
		return errors.New("lastContact should be after test start")
	}

	if !more {
		if tp.peer.issueWork() {
			return errors.New("Unexpected work was done")
		}
	}

	if tp.peer.failures > 0 {
		return fmt.Errorf("Unexpected failures (%d)", tp.peer.failures)
	}

	return nil
}

func oneErrRPC(tp *TestingPeer, expReq interface{}, reply error) error {
	startProgress := tp.peer.progress
	tp.peer.sendProgress = false
	serverFuture := serveReplies(tp, []cannedReply{{expReq, reply}})
	if !tp.peer.issueWork() {
		return errors.New("expected issueWork to prepare an RPC")
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
		return errors.New("last contact should be zero")
	}

	err = waitForFailure(tp)
	if err != nil {
		return fmt.Errorf("Expected 1 failure, got %d: %v", tp.peer.failures, err)
	}
	if tp.peer.issueWork() {
		return errors.New("Unexpected work was done")
	}

	return nil
}

///////////////////////// RequestVote /////////////////////////

var requestVoteControl = peerControl{
	term:          84,
	role:          Candidate,
	lastIndex:     18,
	lastTerm:      83,
	verifyCounter: 120,
}

var requestVoteProgress = peerProgress{
	term:            84,
	matchIndex:      10,
	matchTerm:       50,
	verifiedCounter: 90,
}

func TestPeer_RequestVoteRPC_granted(t *testing.T) {
	tp := makePeerTesting(t, &TestingPeer{
		initControl:  &requestVoteControl,
		initProgress: &requestVoteProgress,
	})
	defer tp.close()
	exp := RequestVoteRequest{
		Term:         84,
		Candidate:    tp.localTrans.EncodePeer(tp.localAddr),
		LastLogIndex: 18,
		LastLogTerm:  83,
	}
	reply := RequestVoteResponse{
		Term:    84,
		Granted: true,
	}
	expProgress := peerProgress{
		peerID:          tp.peerID,
		term:            84,
		voteGranted:     true,
		matchIndex:      10,
		matchTerm:       50,
		verifiedCounter: 90,
	}
	err := oneRPC(tp, &exp, &reply, expProgress, false)
	if err != nil {
		t.Fatal(err)
	}
	if !tp.peer.candidate.voteReplied {
		t.Fatalf("Expected voteReplied to be set")
	}
}

func TestPeer_RequestVoteRPC_denied(t *testing.T) {
	tp := makePeerTesting(t, &TestingPeer{
		initControl:  &requestVoteControl,
		initProgress: &requestVoteProgress,
	})
	defer tp.close()
	reply := RequestVoteResponse{
		Term:    84,
		Granted: false,
	}
	expProgress := peerProgress{
		peerID:          tp.peerID,
		term:            84,
		voteGranted:     false,
		matchIndex:      10,
		matchTerm:       50,
		verifiedCounter: 90,
	}
	err := oneRPC(tp, nil, &reply, expProgress, false)
	if err != nil {
		t.Fatal(err)
	}
	if !tp.peer.candidate.voteReplied {
		t.Fatalf("Expected voteReplied to be set")
	}
}

func TestPeer_RequestVoteRPC_newTerm(t *testing.T) {
	tp := makePeerTesting(t, &TestingPeer{
		initControl:  &requestVoteControl,
		initProgress: &requestVoteProgress,
	})
	defer tp.close()
	reply := RequestVoteResponse{
		Term:    85,
		Granted: true,
	}
	expProgress := peerProgress{
		peerID:          tp.peerID,
		term:            85,
		voteGranted:     false,
		matchIndex:      0,
		matchTerm:       0,
		verifiedCounter: 90,
	}
	err := oneRPC(tp, nil, &reply, expProgress, false)
	if err != nil {
		t.Fatal(err)
	}
	if !tp.peer.candidate.voteReplied {
		t.Fatalf("Expected voteReplied to be set")
	}
}

func TestPeer_RequestVoteRPC_confirmError(t *testing.T) {
	control := requestVoteControl
	tp := makePeerTesting(t, &TestingPeer{
		initControl:  &control,
		initProgress: &requestVoteProgress,
	})
	defer tp.close()
	rpc := makeRequestVoteRPC(tp.peer)
	err := rpc.prepare(tp.peer.shared, tp.peer.control)
	if err != nil {
		t.Fatalf("Unexpected error in prepare: %v", err)
	}

	control.role = Follower
	tp.controlCh <- control
	tp.peer.blockingSelect()
	err = rpc.confirm(tp.peer)
	if err == nil || !strings.Contains(err.Error(), "no longer candidate") {
		t.Fatalf("Expected cancel due to not candidate, got %v", err)
	}

	control.role = Candidate
	control.term++
	tp.controlCh <- control
	tp.peer.blockingSelect()
	err = rpc.confirm(tp.peer)
	if err == nil || !strings.Contains(err.Error(), "term changed") {
		t.Fatalf("Expected cancel due to larger term, got %v", err)
	}

	if tp.peer.candidate.voteReplied {
		t.Fatalf("Expected voteReplied to be unset")
	}
}

func TestPeer_RequestVoteRPC_sendRecvError(t *testing.T) {
	tp := makePeerTesting(t, &TestingPeer{
		initControl:  &requestVoteControl,
		initProgress: &requestVoteProgress,
	})
	defer tp.close()
	err := oneErrRPC(tp, nil, errors.New("a transport error"))
	if err != nil {
		t.Fatal(err)
	}
	if tp.peer.candidate.voteReplied {
		t.Fatalf("Expected voteReplied to be unset")
	}
}

///////////////////////// AppendEntries /////////////////////////

var appendEntriesControl = peerControl{
	term:          83,
	role:          Leader,
	lastIndex:     18,
	lastTerm:      83,
	commitIndex:   16,
	verifyCounter: 120,
}

var appendEntriesProgress = peerProgress{
	term:            75,
	matchIndex:      1,
	matchTerm:       2,
	verifiedCounter: 90,
}

func TestPeer_AppendEntriesRPC_heartbeat(t *testing.T) {
	tp := makePeerTesting(t, &TestingPeer{
		initControl:  &appendEntriesControl,
		initProgress: &appendEntriesProgress,
	})
	defer tp.close()
	tp.peer.failures = 1
	tp.peer.leader.commitIndexAcked = 1
	exp := AppendEntriesRequest{
		Term:              83,
		Leader:            tp.localTrans.EncodePeer(tp.localAddr),
		PrevLogEntry:      1,
		PrevLogTerm:       2,
		LeaderCommitIndex: 1,
	}
	reply := AppendEntriesResponse{
		Term:    83,
		LastLog: 2,
		Success: true,
	}
	expProgress := peerProgress{
		peerID:          tp.peerID,
		term:            83,
		matchIndex:      1,
		matchTerm:       2,
		verifiedCounter: 120,
	}

	// Stop Peer from sending normal AppendEntries. Only send heartbeats.
	tp.peer.leader.outstandingInstallSnapshotRPC = true
	defer func() { tp.peer.leader.outstandingInstallSnapshotRPC = false }()

	err := oneRPC(tp, &exp, &reply, expProgress, false)
	if err != nil {
		t.Fatal(err)
	}
	if tp.peer.failures != 0 {
		t.Fatalf("failures should have been cleared, got %v",
			tp.peer.failures)
	}
	if tp.peer.leader.commitIndexAcked != 1 {
		t.Fatalf("commitIndexAcked should be 1, got %v",
			tp.peer.leader.commitIndexAcked)
	}
}

// This test aims to excercise the following scenario:
// 1. Leader catches up follower of entire log and commit index.
// 2. Follower reboots, setting its own commit index to 0.
// 3. Leader does not generate any additional log entries.
// We still want the follower to be informed of the latest commit index on the
// next heartbeat.
func TestPeer_AppendEntriesRPC_heartbeat_commitIndex(t *testing.T) {
	control := appendEntriesControl
	control.commitIndex = 18
	progress := peerProgress{
		term:            83,
		matchIndex:      18,
		matchTerm:       83,
		verifiedCounter: 90,
	}
	tp := makePeerTesting(t, &TestingPeer{
		initControl:  &control,
		initProgress: &progress,
	})
	defer tp.close()
	tp.peer.leader.commitIndexAcked = 18

	// Stop Peer from sending normal AppendEntries. Only send heartbeats.
	tp.peer.leader.outstandingInstallSnapshotRPC = true
	defer func() { tp.peer.leader.outstandingInstallSnapshotRPC = false }()

	exp := AppendEntriesRequest{
		Term:              83,
		Leader:            tp.localTrans.EncodePeer(tp.localAddr),
		PrevLogEntry:      18,
		PrevLogTerm:       83,
		LeaderCommitIndex: 18,
	}
	reply := AppendEntriesResponse{
		Term:    83,
		LastLog: 18,
		Success: true,
	}
	expProgress := peerProgress{
		peerID:          tp.peerID,
		term:            83,
		matchIndex:      18,
		matchTerm:       83,
		verifiedCounter: 120,
	}
	err := oneRPC(tp, &exp, &reply, expProgress, false)
	if err != nil {
		t.Fatal(err)
	}
	if tp.peer.leader.commitIndexAcked != 18 {
		t.Fatalf("commitIndexAcked should be 18, got %v",
			tp.peer.leader.commitIndexAcked)
	}
}

func TestPeer_AppendEntriesRPC_success_noEntries(t *testing.T) {
	tp := makePeerTesting(t, &TestingPeer{
		initControl:  &appendEntriesControl,
		initProgress: &appendEntriesProgress,
	})
	defer tp.close()
	tp.peer.leader.lastHeartbeatSent = time.Now().Add(time.Minute)
	tp.peer.leader.commitIndexAcked = 1
	exp := AppendEntriesRequest{
		Term:              83,
		Leader:            tp.localTrans.EncodePeer(tp.localAddr),
		PrevLogEntry:      18,
		PrevLogTerm:       83,
		Entries:           []*Log{},
		LeaderCommitIndex: 16,
	}
	reply := AppendEntriesResponse{
		Term:    83,
		LastLog: 18,
		Success: true,
	}
	expProgress := peerProgress{
		peerID:          tp.peerID,
		term:            83,
		matchIndex:      18,
		matchTerm:       83,
		verifiedCounter: 120,
	}

	err := oneRPC(tp, &exp, &reply, expProgress, false)
	if err != nil {
		t.Fatal(err)
	}
	if tp.peer.leader.commitIndexAcked != 16 {
		t.Fatalf("commitIndexAcked should be 16, got %v",
			tp.peer.leader.commitIndexAcked)
	}
}

func TestPeer_AppendEntriesRPC_success_someEntries(t *testing.T) {
	control := appendEntriesControl
	control.commitIndex = 18
	tp := makePeerTesting(t, &TestingPeer{
		initControl:  &control,
		initProgress: &appendEntriesProgress,
		options:      peerOptions{maxAppendEntries: 2},
	})
	defer tp.close()
	tp.peer.leader.lastHeartbeatSent = time.Now().Add(time.Minute)
	tp.peer.leader.commitIndexAcked = 1
	tp.peer.leader.nextIndex = 16
	exp := AppendEntriesRequest{
		Term:              83,
		Leader:            tp.localTrans.EncodePeer(tp.localAddr),
		PrevLogEntry:      15,
		PrevLogTerm:       75,
		Entries:           []*Log{&entry16, &entry17},
		LeaderCommitIndex: 17,
	}
	reply := AppendEntriesResponse{
		Term:    83,
		LastLog: 17,
		Success: true,
	}
	expProgress := peerProgress{
		peerID:          tp.peerID,
		term:            83,
		matchIndex:      17,
		matchTerm:       80,
		verifiedCounter: 120,
	}

	err := oneRPC(tp, &exp, &reply, expProgress, true)
	if err != nil {
		t.Fatal(err)
	}
	if tp.peer.leader.commitIndexAcked != 17 {
		t.Fatalf("commitIndexAcked should be 17, got %v",
			tp.peer.leader.commitIndexAcked)
	}

	// Immediately send remaining entries.
	exp = AppendEntriesRequest{
		Term:              83,
		Leader:            tp.localTrans.EncodePeer(tp.localAddr),
		PrevLogEntry:      17,
		PrevLogTerm:       80,
		Entries:           []*Log{&entry18},
		LeaderCommitIndex: 18,
	}
	reply = AppendEntriesResponse{
		Term:    83,
		LastLog: 18,
		Success: true,
	}
	expProgress = peerProgress{
		peerID:          tp.peerID,
		term:            83,
		matchIndex:      18,
		matchTerm:       83,
		verifiedCounter: 120,
	}
	err = oneRPC(tp, &exp, &reply, expProgress, true)
	if err != nil {
		t.Fatal(err)
	}
	if tp.peer.leader.commitIndexAcked != 18 {
		t.Fatalf("commitIndexAcked should be 18, got %v",
			tp.peer.leader.commitIndexAcked)
	}
}

func TestPeer_AppendEntriesRPC_denied(t *testing.T) {
	tp := makePeerTesting(t, &TestingPeer{
		initControl:  &appendEntriesControl,
		initProgress: &appendEntriesProgress,
	})
	defer tp.close()
	tp.peer.leader.nextIndex = 16
	tp.peer.leader.lastHeartbeatSent = time.Now().Add(time.Minute)
	reply := AppendEntriesResponse{
		Term:    83,
		LastLog: 180,
		Success: false,
	}
	expProgress := peerProgress{
		peerID:          tp.peerID,
		term:            83,
		voteGranted:     false,
		matchIndex:      0,
		matchTerm:       0,
		verifiedCounter: 120,
	}
	err := oneRPC(tp, nil, &reply, expProgress, true)
	if err != nil {
		t.Fatal(err)
	}
	if tp.peer.leader.nextIndex != 15 {
		t.Fatalf("nextIndex should be 15, got %v",
			tp.peer.leader.nextIndex)
	}
}

func TestPeer_AppendEntriesRPC_newTerm(t *testing.T) {
	tp := makePeerTesting(t, &TestingPeer{
		initControl:  &appendEntriesControl,
		initProgress: &appendEntriesProgress,
	})
	defer tp.close()
	tp.peer.leader.lastHeartbeatSent = time.Now().Add(time.Minute)
	reply := AppendEntriesResponse{
		Term:    85,
		LastLog: 19,
		Success: false,
	}
	expProgress := peerProgress{
		peerID:          tp.peerID,
		term:            85,
		voteGranted:     false,
		matchIndex:      0,
		matchTerm:       0,
		verifiedCounter: 90,
	}
	err := oneRPC(tp, nil, &reply, expProgress, false)
	if err != nil {
		t.Fatal(err)
	}
}

func TestPeer_AppendEntriesRPC_confirmError(t *testing.T) {
	control := appendEntriesControl
	tp := makePeerTesting(t, &TestingPeer{
		initControl:  &control,
		initProgress: &appendEntriesProgress,
	})
	defer tp.close()
	tp.peer.leader.lastHeartbeatSent = time.Now().Add(time.Minute)
	tp.peer.leader.nextIndex = 16
	rpc := makeAppendEntriesRPC(tp.peer)
	err := rpc.prepare(tp.peer.shared, tp.peer.control)
	if err != nil {
		t.Fatalf("Unexpected error in prepare: %v", err)
	}

	control.role = Follower
	tp.controlCh <- control
	tp.peer.blockingSelect()
	err = rpc.confirm(tp.peer)
	if err == nil || !strings.Contains(err.Error(), "no longer leader") {
		t.Fatalf("Expected cancel due to not leader, got %v", err)
	}

	control.role = Leader
	control.term++
	tp.controlCh <- control
	tp.peer.blockingSelect()
	tp.peer.leader.nextIndex = 14
	err = rpc.confirm(tp.peer)
	if err == nil || !strings.Contains(err.Error(), "term changed") {
		t.Fatalf("Expected cancel due to larger term, got %v", err)
	}
	if tp.peer.leader.nextIndex != 14 {
		t.Fatalf("nextIndex should be 14, got %v",
			tp.peer.leader.nextIndex)
	}
}

func TestPeer_AppendEntriesRPC_heartbeat_sendRecvError(t *testing.T) {
	tp := makePeerTesting(t, &TestingPeer{
		initControl:  &appendEntriesControl,
		initProgress: &appendEntriesProgress,
	})
	defer tp.close()
	tp.peer.leader.nextIndex = 16
	err := oneErrRPC(tp, nil, errors.New("a transport error"))
	if err != nil {
		t.Fatal(err)
	}
	if tp.peer.leader.nextIndex != 16 {
		t.Fatalf("nextIndex should be 16, got %v",
			tp.peer.leader.nextIndex)
	}
}

func TestPeer_AppendEntriesRPC_sendRecvError(t *testing.T) {
	tp := makePeerTesting(t, &TestingPeer{
		initControl:  &appendEntriesControl,
		initProgress: &appendEntriesProgress,
	})
	defer tp.close()
	tp.peer.leader.nextIndex = 16
	tp.peer.leader.lastHeartbeatSent = time.Now().Add(time.Minute)
	err := oneErrRPC(tp, nil, errors.New("a transport error"))
	if err != nil {
		t.Fatal(err)
	}
	if tp.peer.leader.nextIndex != 16 {
		t.Fatalf("nextIndex should be 16, got %v",
			tp.peer.leader.nextIndex)
	}
}

///////////////////////// InstallSnapshot /////////////////////////

var installSnapshotControl = appendEntriesControl

var installSnapshotProgress = appendEntriesProgress

func TestPeer_InstallSnapshotRPC_success(t *testing.T) {
	tp := makePeerTesting(t, &TestingPeer{
		initControl:  &installSnapshotControl,
		initProgress: &installSnapshotProgress,
	})
	defer tp.close()
	tp.peer.leader.lastHeartbeatSent = time.Now().Add(time.Minute)
	tp.peer.leader.nextIndex = 1
	if !tp.peer.issueWork() {
		t.Fatalf("expected issueWork to start an AppendEntries RPC")
	}
	err := waitFor(tp, func() bool { return tp.peer.leader.needsSnapshot })
	if err != nil {
		t.Fatal("expected AppendEntries RPC to set needsSnapshot: %v", err)
	}

	exp := InstallSnapshotRequest{
		Term:               83,
		Leader:             tp.localTrans.EncodePeer(tp.localAddr),
		LastLogIndex:       15,
		LastLogTerm:        75,
		Configuration:      encodeConfiguration(configuration3),
		ConfigurationIndex: 3,
		Size:               5,
	}
	reply := InstallSnapshotResponse{
		Term:    83,
		Success: true,
	}
	expProgress := peerProgress{
		peerID:          tp.peerID,
		term:            83,
		voteGranted:     false,
		matchIndex:      15,
		matchTerm:       75,
		verifiedCounter: 120,
	}
	err = oneRPC(tp, &exp, &reply, expProgress, true)
	if err != nil {
		t.Fatal(err)
	}
	if tp.peer.leader.nextIndex != 16 {
		t.Fatalf("nextIndex should be 16, got %v",
			tp.peer.leader.nextIndex)
	}
	if tp.peer.leader.needsSnapshot {
		t.Fatalf("needsSnapshot should be false")
	}
}

func TestPeer_InstallSnapshotRPC_denied(t *testing.T) {
	tp := makePeerTesting(t, &TestingPeer{
		initControl:  &installSnapshotControl,
		initProgress: &installSnapshotProgress,
	})
	defer tp.close()
	tp.peer.leader.lastHeartbeatSent = time.Now().Add(time.Minute)
	tp.peer.leader.nextIndex = 1
	tp.peer.leader.needsSnapshot = true
	reply := InstallSnapshotResponse{
		Term:    83,
		Success: false,
	}
	expProgress := peerProgress{
		peerID:          tp.peerID,
		term:            83,
		voteGranted:     false,
		matchIndex:      0,
		matchTerm:       0,
		verifiedCounter: 120,
	}
	err := oneRPC(tp, nil, &reply, expProgress, true)
	if err != nil {
		t.Fatal(err)
	}
	if tp.peer.leader.needsSnapshot {
		t.Fatalf("needsSnapshot should be false")
	}
	if tp.peer.leader.nextIndex != 16 {
		t.Fatalf("nextIndex should be 16, got %v",
			tp.peer.leader.nextIndex)
	}
}

func TestPeer_InstallSnapshotRPC_newTerm(t *testing.T) {
	tp := makePeerTesting(t, &TestingPeer{
		initControl:  &installSnapshotControl,
		initProgress: &installSnapshotProgress,
	})
	defer tp.close()
	tp.peer.leader.lastHeartbeatSent = time.Now().Add(time.Minute)
	tp.peer.leader.nextIndex = 1
	tp.peer.leader.needsSnapshot = true
	reply := InstallSnapshotResponse{
		Term:    84,
		Success: true,
	}
	expProgress := peerProgress{
		peerID:          tp.peerID,
		term:            84,
		voteGranted:     false,
		matchIndex:      0,
		matchTerm:       0,
		verifiedCounter: 90,
	}
	err := oneRPC(tp, nil, &reply, expProgress, false)
	if err != nil {
		t.Fatal(err)
	}
}

func TestPeer_InstallSnapshotRPC_confirmError(t *testing.T) {
	control := installSnapshotControl
	tp := makePeerTesting(t, &TestingPeer{
		initControl:  &control,
		initProgress: &installSnapshotProgress,
	})
	defer tp.close()
	tp.peer.leader.lastHeartbeatSent = time.Now().Add(time.Minute)
	tp.peer.leader.nextIndex = 1
	tp.peer.leader.needsSnapshot = true
	rpc := makeInstallSnapshotRPC(tp.peer)
	err := rpc.prepare(tp.peer.shared, tp.peer.control)
	if err != nil {
		t.Fatalf("Unexpected error in prepare: %v", err)
	}

	control.role = Follower
	tp.controlCh <- control
	tp.peer.blockingSelect()
	err = rpc.confirm(tp.peer)
	if err == nil || !strings.Contains(err.Error(), "no longer leader") {
		t.Fatalf("Expected cancel due to not leader, got %v", err)
	}

	control.role = Leader
	control.term++
	tp.controlCh <- control
	tp.peer.blockingSelect()
	tp.peer.leader.nextIndex = 1
	err = rpc.confirm(tp.peer)
	if err == nil || !strings.Contains(err.Error(), "term changed") {
		t.Fatalf("Expected cancel due to larger term, got %v", err)
	}
	if tp.peer.leader.nextIndex != 1 {
		t.Fatalf("nextIndex should be 1, got %v",
			tp.peer.leader.nextIndex)
	}
}

func TestPeer_InstallSnapshotRPC_sendRecvError(t *testing.T) {
	tp := makePeerTesting(t, &TestingPeer{
		initControl:  &installSnapshotControl,
		initProgress: &installSnapshotProgress,
	})
	defer tp.close()
	tp.peer.leader.lastHeartbeatSent = time.Now().Add(time.Minute)
	tp.peer.leader.nextIndex = 1
	tp.peer.leader.needsSnapshot = true
	err := oneErrRPC(tp, nil, errors.New("a transport error"))
	if err != nil {
		t.Fatal(err)
	}
	if tp.peer.leader.needsSnapshot {
		t.Fatalf("Expected needsSnapshot to be unset")
	}
}
