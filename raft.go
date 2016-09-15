package raft

import (
	"bytes"
	"container/list"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/armon/go-metrics"
)

const (
	minCheckInterval = 10 * time.Millisecond
)

var (
	keyCurrentTerm  = []byte("CurrentTerm")
	keyLastVoteTerm = []byte("LastVoteTerm")
	keyLastVoteCand = []byte("LastVoteCand")
)

type raftPeer struct {
	controlCh chan<- peerControl
	progress  peerProgress
}

// commitTuple is used to send an index that was committed,
// with an optional associated future that should be invoked.
type commitTuple struct {
	log    *Log
	future *logFuture
}

type verifyBatch struct {
	start   uint64
	futures []*verifyFuture
}

// leaderState is state that is used while we are a leader.
type leaderState struct {
	// the first index of this leader's term: this needs to be replicated to a
	// majority of the cluster before this leader may mark anything committed
	// (per Raft's commitment rule)
	startIndex uint64

	inflight      *list.List // list of logFuture in log index order
	verifyBatches []verifyBatch
}

// setLeader is used to modify the current leader of the cluster
func (r *Raft) setLeader(leader ServerAddress) {
	r.leaderLock.Lock()
	r.leader = leader
	r.leaderLock.Unlock()
}

// getConfigurations returns the full configuration information. This must not
// be called on the main thread (which can access the information directly).
func (r *Raft) getConfigurations() *configurationsFuture {
	configurationsFuture := &configurationsFuture{}
	configurationsFuture.init()
	select {
	case <-r.shutdownCh:
		configurationsFuture.respond(ErrRaftShutdown)
		return configurationsFuture
	case r.configurationsCh <- configurationsFuture:
		return configurationsFuture
	}
}

// requestConfigChange is a helper for the above functions that make
// configuration change requests. 'req' describes the change. For timeout,
// see AddVoter.
func (r *Raft) requestConfigChange(req configurationChangeRequest, timeout time.Duration) IndexFuture {
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}
	future := &configurationChangeFuture{
		req: req,
	}
	future.init()
	select {
	case <-timer:
		return errorFuture{ErrEnqueueTimeout}
	case r.configurationChangeCh <- future:
		return future
	case <-r.shutdownCh:
		return errorFuture{ErrRaftShutdown}
	}
}

// run is a long running goroutine that runs the Raft FSM.
func (r *Raft) run() {
	for {
		// Check if we are doing a shutdown
		select {
		case <-r.shutdownCh:
			// Clear the leader to prevent forwarding
			r.setLeader("")
			r.shutdownPeers()
			return
		default:
		}

		// Enter into a sub-FSM
		switch r.getState() {
		case Follower:
			r.runFollower()
		case Candidate:
			r.runCandidate()
		case Leader:
			r.runLeader()
		}
	}
}

// runFollower runs the FSM for a follower.
func (r *Raft) runFollower() {
	didWarn := false
	r.logger.Printf("[INFO] raft: %v entering Follower state (Leader: %q)", r, r.Leader())
	metrics.IncrCounter([]string{"raft", "state", "follower"}, 1)
	heartbeatTimer := randomTimeout(r.conf.HeartbeatTimeout)
	for {
		select {
		case rpc := <-r.rpcCh:
			r.processRPC(rpc)

		case c := <-r.configurationChangeCh:
			// Reject any operations since we are not the leader
			c.respond(ErrNotLeader)

		case a := <-r.applyCh:
			// Reject any operations since we are not the leader
			a.respond(ErrNotLeader)

		case v := <-r.verifyCh:
			// Reject any operations since we are not the leader
			v.respond(ErrNotLeader)

		case c := <-r.configurationsCh:
			c.configurations = r.configurations.Clone()
			c.respond(nil)

		case <-heartbeatTimer:
			// Restart the heartbeat timer
			heartbeatTimer = randomTimeout(r.conf.HeartbeatTimeout)

			// Check if we have had a successful contact
			lastContact := r.LastContact()
			if time.Now().Sub(lastContact) < r.conf.HeartbeatTimeout {
				continue
			}

			// Heartbeat failed! Transition to the candidate state
			lastLeader := r.Leader()
			r.setLeader("")

			if r.configurations.latestIndex == 0 {
				if !didWarn {
					r.logger.Printf("[WARN] raft: no known peers, aborting election")
					didWarn = true
				}
			} else if r.configurations.latestIndex == r.configurations.committedIndex &&
				!hasVote(r.configurations.latest, r.localID) {
				if !didWarn {
					r.logger.Printf("[WARN] raft: not part of stable configuration, aborting election")
					didWarn = true
				}
			} else {
				r.logger.Printf(`[WARN] raft: Heartbeat timeout from %q reached in term %d (last contact %v), starting election`,
					lastLeader, r.getCurrentTerm(), lastContact)
				metrics.IncrCounter([]string{"raft", "transition", "heartbeat_timeout"}, 1)
				r.setState(Candidate)
				r.updatePeers()
				return
			}

		case <-r.shutdownCh:
			return
		}
	}
}

// runCandidate runs the FSM for a candidate.
func (r *Raft) runCandidate() {
	r.logger.Printf("[INFO] raft: %v entering Candidate state in term %v",
		r, r.getCurrentTerm()+1)
	metrics.IncrCounter([]string{"raft", "state", "candidate"}, 1)
	defer metrics.MeasureSince([]string{"raft", "candidate", "electSelf"}, time.Now())

	// Increment the term
	r.setCurrentTerm(r.getCurrentTerm() + 1)

	// Set a timeout
	electionTimer := randomTimeout(r.conf.ElectionTimeout)

	if hasVote(r.configurations.latest, r.localID) {
		// Persist a vote for ourselves
		err := r.persistVote(r.getCurrentTerm(), r.trans.EncodePeer(r.localAddr))
		if err != nil {
			r.logger.Printf("[ERR] raft: Failed to persist vote : %v", err)
			return // TODO: panic?
		}
	}
	r.computeCandidateProgress()

	// Ask peers to vote
	r.updatePeers()

	for r.getState() == Candidate {
		select {
		case rpc := <-r.rpcCh:
			r.processRPC(rpc)

		case progress := <-r.peerProgressCh:
			peer, ok := r.peers[progress.peerID]
			if ok {
				peer.progress = progress
				r.computeCandidateProgress()
			}

		case c := <-r.configurationChangeCh:
			// Reject any operations since we are not the leader
			c.respond(ErrNotLeader)

		case a := <-r.applyCh:
			// Reject any operations since we are not the leader
			a.respond(ErrNotLeader)

		case v := <-r.verifyCh:
			// Reject any operations since we are not the leader
			v.respond(ErrNotLeader)

		case c := <-r.configurationsCh:
			c.configurations = r.configurations.Clone()
			c.respond(nil)

		case <-electionTimer:
			// Election failed! Restart the election. We simply return,
			// which will kick us back into runCandidate
			r.logger.Printf("[WARN] raft: Election timeout reached, restarting election")
			return

		case <-r.shutdownCh:
			return
		}
	}
}

// runLeader runs the FSM for a leader. Do the setup here and drop into
// the leaderLoop for the hot loop.
func (r *Raft) runLeader() {
	r.logger.Printf("[INFO] raft: %v entering Leader state", r)
	metrics.IncrCounter([]string{"raft", "state", "leader"}, 1)

	// Notify that we are the leader
	asyncNotifyBool(r.leaderCh, true)

	// Push to the notify channel if given
	if notify := r.conf.NotifyCh; notify != nil {
		select {
		case notify <- true:
		case <-r.shutdownCh:
		}
	}

	// Setup leader state
	// first index that may be committed in this term
	r.leaderState.startIndex = r.getLastIndex() + 1
	r.leaderState.inflight = list.New()
	r.leaderState.verifyBatches = nil

	// Notify peers of leadership.
	r.updatePeers()

	// Cleanup state on step down
	defer func() {
		// Since we were the leader previously, we update our
		// last contact time when we step down, so that we are not
		// reporting a last contact time from before we were the
		// leader. Otherwise, to a client it would seem our data
		// is extremely stale.
		r.setLastContact()

		// Stop replication
		r.updatePeers()

		// Respond to all inflight operations
		for e := r.leaderState.inflight.Front(); e != nil; e = e.Next() {
			e.Value.(*logFuture).respond(ErrLeadershipLost)
		}

		// Respond to any pending verify requests
		for _, batch := range r.leaderState.verifyBatches {
			for _, f := range batch.futures {
				f.respond(ErrLeadershipLost)
			}
		}

		// Clear all the state
		r.leaderState.startIndex = 0
		r.leaderState.inflight = nil
		r.leaderState.verifyBatches = nil

		// If we are stepping down for some reason, no known leader.
		// We may have stepped down due to an RPC call, which would
		// provide the leader, so we cannot always blank this out.
		r.leaderLock.Lock()
		if r.leader == r.localAddr {
			r.leader = ""
		}
		r.leaderLock.Unlock()

		// Notify that we are not the leader
		asyncNotifyBool(r.leaderCh, false)

		// Push to the notify channel if given
		if notify := r.conf.NotifyCh; notify != nil {
			select {
			case notify <- false:
			case <-r.shutdownCh:
				// On shutdown, make a best effort but do not block
				select {
				case notify <- false:
				default:
				}
			}
		}
	}()

	// Dispatch a no-op log entry first. This gets this leader up to the latest
	// possible commit index, even in the absence of client commands. This used
	// to append a configuration entry instead of a noop. However, that permits
	// an unbounded number of uncommitted configurations in the log. We now
	// maintain that there exists at most one uncommitted configuration entry in
	// any log, so we have to do proper no-ops here.
	noop := &logFuture{
		log: Log{
			Type: LogNoop,
		},
	}
	r.dispatchLogs([]*logFuture{noop})

	// Sit in the leader loop until we step down
	r.leaderLoop()
}

// updatePeeers will start communication with new peers, send new and
// existing peers updated control information, and stop communication with
// removed peers. This must only be called from the main thread.
func (r *Raft) updatePeers() {
	inConfig := make(map[ServerID]Server, len(r.configurations.latest.Servers))

	// Start replication goroutines that need starting
	for _, server := range r.configurations.latest.Servers {
		if server.ID == r.localID {
			continue
		}
		inConfig[server.ID] = server
		if _, ok := r.peers[server.ID]; !ok {
			r.logger.Printf("[INFO] raft: Added peer %v, starting replication", server.ID)
			controlCh := startPeer(server.ID, server.Address, r.logger, r.logs, r.snapshots,
				r.goRoutines, r.trans, r.localAddr, r.peerProgressCh, peerOptions{
					maxAppendEntries:  uint64(r.conf.MaxAppendEntries),
					heartbeatInterval: r.conf.HeartbeatTimeout / 5,
				})
			peer := &raftPeer{
				controlCh: controlCh,
			}
			r.peers[server.ID] = peer
		}
	}

	// Send new control information and stop Peer goroutines that need stopping
	lastIndex, lastTerm := r.getLastEntry()
	for serverID, peer := range r.peers {
		role := r.getState()
		if role == Shutdown {
			// Shutdown must be the last control signal sent to the Peer, so we mask
			// it here and send it later in shutdownPeers().
			role = Follower
		}
		verifyCounter := r.verifyCounter
		server, ok := inConfig[serverID]
		if !ok {
			r.logger.Printf("[INFO] raft: Removed peer %v, stopping communication", serverID)
			delete(r.peers, serverID)
			role = Shutdown
		} else {
			if server.Suffrage != Voter {
				if role == Candidate {
					role = Follower
				}
				verifyCounter = 0
			}
		}
		control := peerControl{
			term:          r.getCurrentTerm(),
			role:          role,
			verifyCounter: verifyCounter,
			lastIndex:     lastIndex,
			lastTerm:      lastTerm,
			commitIndex:   r.getCommitIndex(),
		}
		peer.controlCh <- control
	}
}

// shutdownPeers instructs all peers to exit immediately.
func (r *Raft) shutdownPeers() {
	control := peerControl{
		term: r.getCurrentTerm(),
		role: Shutdown,
	}
	for serverID, peer := range r.peers {
		r.logger.Printf("[INFO] raft: Shutting down communication with peer %v", serverID)
		peer.controlCh <- control
		delete(r.peers, serverID)
		r.logger.Printf("[INFO] raft: Shut down communication with peer %v", serverID)
	}
}

// configurationChangeChIfStable returns r.configurationChangeCh if it's safe
// to process requests from it, or nil otherwise. This must only be called
// from the main thread.
//
// Note that if the conditions here were to change outside of leaderLoop to take
// this from nil to non-nil, we would need leaderLoop to be kicked.
func (r *Raft) configurationChangeChIfStable() chan *configurationChangeFuture {
	// Have to wait until:
	// 1. The latest configuration is committed, and
	// 2. This leader has committed some entry (the noop) in this term
	//    https://groups.google.com/forum/#!msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J
	if r.configurations.latestIndex == r.configurations.committedIndex &&
		r.getCommitIndex() >= r.leaderState.startIndex {
		return r.configurationChangeCh
	}
	return nil
}

// leaderLoop is the hot loop for a leader. It is invoked
// after all the various leader setup is done.
func (r *Raft) leaderLoop() {
	// stepDown is used to track if there is an inflight log that
	// would cause us to lose leadership (specifically a RemovePeer of
	// ourselves). If this is the case, we must not allow any logs to
	// be processed in parallel, otherwise we are basing commit on
	// only a single peer (ourself) and replicating to an undefined set
	// of peers.
	stepDown := false

	lease := time.After(r.conf.LeaderLeaseTimeout)
	for r.getState() == Leader {
		select {
		case rpc := <-r.rpcCh:
			r.processRPC(rpc)

		case future := <-r.verifyCh:
			// Try to batch all queued verifies together.
			futures := []*verifyFuture{future}
			drained := false
			for !drained {
				select {
				case another := <-r.verifyCh:
					futures = append(futures, another)
				case <-r.shutdownCh:
					return
				default:
					drained = true
				}
			}
			r.verifyLeader(futures)

		case c := <-r.configurationsCh:
			c.configurations = r.configurations.Clone()
			c.respond(nil)

		case future := <-r.configurationChangeChIfStable():
			r.appendConfigurationEntry(future)

		case newLog := <-r.applyCh:
			// Group commit, gather all the ready commits
			ready := []*logFuture{newLog}
			for i := 0; i < r.conf.MaxAppendEntries; i++ {
				select {
				case newLog := <-r.applyCh:
					ready = append(ready, newLog)
				default:
					break
				}
			}

			// Dispatch the logs
			if stepDown {
				// we're in the process of stepping down as leader, don't process anything new
				for i := range ready {
					ready[i].respond(ErrNotLeader)
				}
			} else {
				r.dispatchLogs(ready)
			}

		case <-lease:
			// Check if we've exceeded the lease, potentially stepping down
			r.checkLeaderLease()
			// Renew the lease timer
			lease = time.After(r.conf.LeaderLeaseTimeout)

		case progress := <-r.peerProgressCh:
			peer, ok := r.peers[progress.peerID]
			if ok {
				peer.progress = progress
				r.computeLeaderProgress()
			}

		case <-r.shutdownCh:
			return
		}

	}
}

func (r *Raft) updateCommitIndex(oldCommitIndex, commitIndex uint64) {
	stepDown := false
	r.setCommitIndex(commitIndex)
	// Process the newly committed entries
	if r.configurations.latestIndex > oldCommitIndex &&
		r.configurations.latestIndex <= commitIndex {
		r.configurations.committed = r.configurations.latest
		r.configurations.committedIndex = r.configurations.latestIndex
		if !hasVote(r.configurations.committed, r.localID) {
			stepDown = true
		}
	}

	for {
		e := r.leaderState.inflight.Front()
		if e == nil {
			break
		}
		commitLog := e.Value.(*logFuture)
		idx := commitLog.log.Index
		if idx > commitIndex {
			break
		}
		// Measure the commit time
		metrics.MeasureSince([]string{"raft", "commitTime"}, commitLog.dispatch)
		r.processLogs(idx, commitLog)
		r.leaderState.inflight.Remove(e)
	}

	if stepDown {
		if r.conf.ShutdownOnRemove {
			r.logger.Printf("[INFO] raft: Removed ourself, shutting down")
			r.Shutdown()
		} else {
			r.logger.Printf("[INFO] raft: Removed ourself, transitioning to follower")
			r.stepDown()
		}
	}

	r.updatePeers()
}

// Responds to any verify futures that have been satisfied. A majority of the
// voting servers in the cluster have acknowledged this server's leadership
// since the given 'count'.
func (r *Raft) verified(count uint64) {
	i := 0
	var batch verifyBatch
	for _, batch = range r.leaderState.verifyBatches {
		if batch.start > count {
			break
		}
		for _, f := range batch.futures {
			f.respond(nil)
		}
		i++
	}
	if i > 0 {
		r.logger.Printf("[INFO] raft: Verified leadership through counter %v", count)
		r.leaderState.verifyBatches = r.leaderState.verifyBatches[i:]
	}
}

func (r *Raft) computeCandidateProgress() {
	servers := len(r.configurations.latest.Servers)
	votes := make([]uint64, 0, servers)
	if hasVote(r.configurations.latest, r.localID) {
		votes = append(votes, 1)
	}
	for peerID, peer := range r.peers {
		switch {
		case peer.progress.term > r.getCurrentTerm():
			r.logger.Printf("[DEBUG] raft: Newer term discovered, fallback to follower")
			r.updateTerm(peer.progress.term)
			return
		case peer.progress.term == r.getCurrentTerm():
			if hasVote(r.configurations.latest, peerID) {
				if peer.progress.voteGranted {
					votes = append(votes, 1)
				} else {
					votes = append(votes, 0)
				}
			}
		case peer.progress.term < r.getCurrentTerm():
			votes = append(votes, 0)
		}
	}
	if quorumGeq(votes) == 1 {
		r.logger.Printf("[INFO] raft: Election won. Tally: %d", sum(votes))
		r.setState(Leader)
		r.setLeader(r.localAddr)
		r.updatePeers()
	}
}

func (r *Raft) computeLeaderProgress() {
	servers := len(r.configurations.latest.Servers)
	verifiedCounters := make([]uint64, 0, servers)
	matchIndexes := make([]uint64, 0, servers)
	if hasVote(r.configurations.latest, r.localID) {
		verifiedCounters = append(verifiedCounters, r.verifyCounter)
		matchIndexes = append(matchIndexes, r.getLastIndex())
	}
	for peerID, peer := range r.peers {
		switch {
		case peer.progress.term > r.getCurrentTerm():
			r.logger.Printf("[DEBUG] raft: Newer term discovered, fallback to follower")
			r.updateTerm(peer.progress.term)
			return
		case peer.progress.term == r.getCurrentTerm():
			if hasVote(r.configurations.latest, peerID) {
				verifiedCounters = append(verifiedCounters, peer.progress.verifiedCounter)
				matchIndexes = append(matchIndexes, peer.progress.matchIndex)
			}
		case peer.progress.term < r.getCurrentTerm():
			verifiedCounters = append(verifiedCounters, 0)
			matchIndexes = append(matchIndexes, 0)
		}
	}
	verifiedCounter := quorumGeq(verifiedCounters)
	matchIndex := quorumGeq(matchIndexes)

	oldCommitIndex := r.getCommitIndex()
	if matchIndex > oldCommitIndex && matchIndex >= r.leaderState.startIndex {
		r.updateCommitIndex(oldCommitIndex, matchIndex)
	}
	r.verified(verifiedCounter)
}

// Internal helper to calculate new commitIndex from matchIndexes,
// whether votes form a quorum, etc.
func quorumGeq(values []uint64) uint64 {
	if len(values) == 0 {
		return 0
	}
	sort.Sort(uint64Slice(values))
	return values[(len(values)-1)/2]
}

func sum(values []uint64) uint64 {
	var total uint64
	for _, v := range values {
		total += v
	}
	return total
}

// verifyLeader must be called from the main thread for safety.
// Causes the followers to attempt an immediate heartbeat.
func (r *Raft) verifyLeader(futures []*verifyFuture) {
	r.verifyCounter++
	counter := r.verifyCounter
	r.leaderState.verifyBatches = append(r.leaderState.verifyBatches, verifyBatch{
		start:   counter,
		futures: futures,
	})

	r.logger.Printf("[INFO] raft: Verifying leadership for %v requests with counter %v",
		len(futures), counter)

	// Trigger immediate heartbeats
	r.updatePeers()

	// verifying a 1-server cluster should succeed right away
	r.computeLeaderProgress()
}

// checkLeaderLease is used to check if we can contact a quorum of nodes
// within the last leader lease interval. If not, we need to step down,
// as we may have lost connectivity. Returns the maximum duration without
// contact. This must only be called from the main thread.
func (r *Raft) checkLeaderLease() {
	servers := len(r.configurations.latest.Servers)
	lastContacts := make([]uint64, 0, servers)
	if hasVote(r.configurations.latest, r.localID) {
		lastContacts = append(lastContacts, uint64(time.Now().UnixNano()))
	}
	for peerID, peer := range r.peers {
		if hasVote(r.configurations.latest, peerID) {
			lastContacts = append(lastContacts, uint64(peer.progress.lastContact.UnixNano()))
		}
	}
	lastContactUnix := quorumGeq(lastContacts)
	lastContact := time.Unix(int64(lastContactUnix/1e9), int64(lastContactUnix%1e9))
	diff := time.Now().Sub(lastContact)
	metrics.AddSample([]string{"raft", "leader", "lastContact"}, float32(diff/time.Millisecond))
	if r.conf.LeaderLeaseTimeout < diff {
		r.logger.Printf("[WARN] raft: Failed to contact quorum of nodes, stepping down")
		r.stepDown()
		metrics.IncrCounter([]string{"raft", "transition", "leader_lease_timeout"}, 1)
	}
}

// appendConfigurationEntry changes the configuration and adds a new
// configuration entry to the log. This must only be called from the
// main thread.
func (r *Raft) appendConfigurationEntry(future *configurationChangeFuture) {
	configuration, err := nextConfiguration(r.configurations.latest, r.configurations.latestIndex, future.req)
	if err != nil {
		future.respond(err)
		return
	}

	r.logger.Printf("[INFO] raft: Updating configuration with %s (%v, %v) to %v",
		future.req.command, future.req.serverAddress, future.req.serverID, configuration)
	future.log = Log{
		Type: LogConfiguration,
		Data: encodeConfiguration(configuration),
	}
	r.dispatchLogs([]*logFuture{&future.logFuture})
}

// dispatchLog is called on the leader to push a log to disk, mark it
// as inflight and begin replication of it.
func (r *Raft) dispatchLogs(applyLogs []*logFuture) {
	now := time.Now()
	defer metrics.MeasureSince([]string{"raft", "leader", "dispatchLog"}, now)

	term := r.getCurrentTerm()
	lastIndex := r.getLastIndex()
	logs := make([]*Log, len(applyLogs))

	for idx, applyLog := range applyLogs {
		applyLog.dispatch = now
		lastIndex++
		applyLog.log.Index = lastIndex
		applyLog.log.Term = term
		logs[idx] = &applyLog.log
		r.leaderState.inflight.PushBack(applyLog)

		if applyLog.log.Type == LogConfiguration {
			r.configurations.committed = r.configurations.latest
			r.configurations.committedIndex = r.configurations.latestIndex
			r.configurations.latest = decodeConfiguration(applyLog.log.Data)
			r.configurations.latestIndex = applyLog.log.Index
			r.updatePeers()
		}
	}

	// Write the log entry locally
	if err := r.logs.StoreLogs(logs); err != nil {
		r.logger.Printf("[ERR] raft: Failed to commit logs: %v", err)
		for _, applyLog := range applyLogs {
			applyLog.respond(err)
		}
		r.stepDown()
		return
	}

	// Update the last log since it's on disk now
	r.setLastLog(lastIndex, term)

	// In case we're a 1-server cluster, this might be committed already.
	r.computeLeaderProgress()

	// Notify the replicators of the new log
	r.updatePeers()
}

// processLogs is used to apply all the committed entires that haven't been
// applied up to the given index limit.
// This can be called from both leaders and followers.
// Followers call this from AppendEntires, for n entires at a time, and always
// pass future=nil.
// Leaders call this once per inflight when entries are committed. They pass
// the future from inflights.
func (r *Raft) processLogs(index uint64, future *logFuture) {
	// Reject logs we've applied already
	lastApplied := r.getLastApplied()
	if index <= lastApplied {
		r.logger.Printf("[WARN] raft: Skipping application of old log: %d", index)
		return
	}

	// Apply all the preceding logs
	for idx := r.getLastApplied() + 1; idx <= index; idx++ {
		// Get the log, either from the future or from our log store
		if future != nil && future.log.Index == idx {
			r.processLog(&future.log, future)

		} else {
			l := new(Log)
			if err := r.logs.GetLog(idx, l); err != nil {
				r.logger.Printf("[ERR] raft: Failed to get log at %d: %v", idx, err)
				panic(err)
			}
			r.processLog(l, nil)
		}

		// Update the lastApplied index and term
		r.setLastApplied(idx)
	}
}

// processLog is invoked to process the application of a single committed log entry.
func (r *Raft) processLog(l *Log, future *logFuture) {
	switch l.Type {
	case LogBarrier:
		// Barrier is handled by the FSM
		fallthrough

	case LogCommand:
		// Forward to the fsm handler
		select {
		case r.fsmCommitCh <- commitTuple{l, future}:
		case <-r.shutdownCh:
			if future != nil {
				future.respond(ErrRaftShutdown)
			}
		}

		// Return so that the future is only responded to
		// by the FSM handler when the application is done
		return

	case LogConfiguration:
	case LogAddPeerDeprecated:
	case LogRemovePeerDeprecated:
	case LogNoop:
		// Ignore the no-op
	default:
		r.logger.Printf("[ERR] raft: Got unrecognized log type: %#v", l)
	}

	// Invoke the future if given
	if future != nil {
		future.respond(nil)
	}
}

// processRPC is called to handle an incoming RPC request. This must only be
// called from the main thread.
func (r *Raft) processRPC(rpc RPC) {
	switch cmd := rpc.Command.(type) {
	case *AppendEntriesRequest:
		r.appendEntries(rpc, cmd)
	case *RequestVoteRequest:
		r.requestVote(rpc, cmd)
	case *InstallSnapshotRequest:
		r.installSnapshot(rpc, cmd)
	default:
		r.logger.Printf("[ERR] raft: Got unexpected command: %#v", rpc.Command)
		rpc.Respond(nil, fmt.Errorf("unexpected command"))
	}
}

// processHeartbeat is a special handler used just for heartbeat requests
// so that they can be fast-pathed if a transport supports it. This must only
// be called from the main thread.
func (r *Raft) processHeartbeat(rpc RPC) {
	defer metrics.MeasureSince([]string{"raft", "rpc", "processHeartbeat"}, time.Now())

	// Check if we are shutdown, just ignore the RPC
	select {
	case <-r.shutdownCh:
		return
	default:
	}

	// Ensure we are only handling a heartbeat
	switch cmd := rpc.Command.(type) {
	case *AppendEntriesRequest:
		r.appendEntries(rpc, cmd)
	default:
		r.logger.Printf("[ERR] raft: Expected heartbeat, got command: %#v", rpc.Command)
		rpc.Respond(nil, fmt.Errorf("unexpected command"))
	}
}

// appendEntries is invoked when we get an append entries RPC call. This must
// only be called from the main thread.
func (r *Raft) appendEntries(rpc RPC, a *AppendEntriesRequest) {
	defer metrics.MeasureSince([]string{"raft", "rpc", "appendEntries"}, time.Now())
	// Setup a response
	resp := &AppendEntriesResponse{
		Term:    r.getCurrentTerm(),
		LastLog: r.getLastIndex(),
		Success: false,
	}
	var rpcErr error
	defer func() {
		rpc.Respond(resp, rpcErr)
	}()

	// Ignore an older term
	if a.Term < r.getCurrentTerm() {
		return
	}

	// Increase the term if we see a newer one, also transition to follower
	// if we ever get an appendEntries call
	if a.Term > r.getCurrentTerm() {
		r.updateTerm(a.Term)
		resp.Term = a.Term
	}
	// Save the current leader
	r.stepDown()
	r.setLeader(ServerAddress(r.trans.DecodePeer(a.Leader)))
	defer r.setLastContact()

	// Verify the last log entry
	if a.PrevLogEntry > 0 {
		lastIdx, lastTerm := r.getLastEntry()

		var prevLogTerm uint64
		if a.PrevLogEntry == lastIdx {
			prevLogTerm = lastTerm

		} else {
			var prevLog Log
			if err := r.logs.GetLog(a.PrevLogEntry, &prevLog); err != nil {
				r.logger.Printf("[WARN] raft: Failed to get previous log entry %d: %v (last: %d)",
					a.PrevLogEntry, err, lastIdx)
				return
			}
			prevLogTerm = prevLog.Term
		}

		if a.PrevLogTerm != prevLogTerm {
			r.logger.Printf("[WARN] raft: Previous log term mis-match: ours: %d remote: %d",
				prevLogTerm, a.PrevLogTerm)
			return
		}
	}

	// Process any new entries
	if len(a.Entries) > 0 {
		start := time.Now()

		// Delete any conflicting entries, skip any duplicates
		lastLogIdx, _ := r.getLastLog()
		var newEntries []*Log
		for i, entry := range a.Entries {
			if entry.Index > lastLogIdx {
				newEntries = a.Entries[i:]
				break
			}
			var storeEntry Log
			if err := r.logs.GetLog(entry.Index, &storeEntry); err != nil {
				r.logger.Printf("[WARN] raft: Failed to get log entry %d: %v",
					entry.Index, err)
				return
			}
			if entry.Term != storeEntry.Term {
				r.logger.Printf("[WARN] raft: Clearing log suffix from %d to %d", entry.Index, lastLogIdx)
				if err := r.logs.DeleteRange(entry.Index, lastLogIdx); err != nil {
					r.logger.Printf("[ERR] raft: Failed to clear log suffix: %v", err)
					return
				}
				if entry.Index <= r.configurations.latestIndex {
					r.configurations.latest = r.configurations.committed
					r.configurations.latestIndex = r.configurations.committedIndex
					r.updatePeers()
				}
				newEntries = a.Entries[i:]
				break
			}
		}

		if n := len(newEntries); n > 0 {
			// Append the new entries
			if err := r.logs.StoreLogs(newEntries); err != nil {
				r.logger.Printf("[ERR] raft: Failed to append to logs: %v", err)
				// TODO: leaving r.getLastLog() in the wrong
				// state if there was a truncation above
				return
			}

			for _, newEntry := range newEntries {
				if newEntry.Type == LogConfiguration {
					r.configurations.committed = r.configurations.latest
					r.configurations.committedIndex = r.configurations.latestIndex
					r.configurations.latest = decodeConfiguration(newEntry.Data)
					r.configurations.latestIndex = newEntry.Index
					r.updatePeers()
				}
			}

			// Update the lastLog
			last := newEntries[n-1]
			r.setLastLog(last.Index, last.Term)
		}

		metrics.MeasureSince([]string{"raft", "rpc", "appendEntries", "storeLogs"}, start)
	}

	// Update the commit index (see comment in AppendEntriesRequest).
	if cap := a.PrevLogEntry + uint64(len(a.Entries)); a.LeaderCommitIndex > cap {
		a.LeaderCommitIndex = cap
	}
	if a.LeaderCommitIndex > r.getCommitIndex() {
		start := time.Now()
		r.setCommitIndex(a.LeaderCommitIndex)
		if r.configurations.latestIndex <= a.LeaderCommitIndex {
			r.configurations.committed = r.configurations.latest
			r.configurations.committedIndex = r.configurations.latestIndex
		}
		r.processLogs(a.LeaderCommitIndex, nil)
		metrics.MeasureSince([]string{"raft", "rpc", "appendEntries", "processLogs"}, start)
	}

	// Everything went well, set success
	resp.Success = true
	return
}

// requestVote is invoked when we get an request vote RPC call.
func (r *Raft) requestVote(rpc RPC, req *RequestVoteRequest) {
	defer metrics.MeasureSince([]string{"raft", "rpc", "requestVote"}, time.Now())
	r.observe(*req)

	// Setup a response
	resp := &RequestVoteResponse{
		Term:    r.getCurrentTerm(),
		Granted: false,
	}
	var rpcErr error
	defer func() {
		rpc.Respond(resp, rpcErr)
	}()

	// Check if we have an existing leader [who's not the candidate]
	candidate := r.trans.DecodePeer(req.Candidate)
	if leader := r.Leader(); leader != "" && leader != candidate {
		r.logger.Printf("[WARN] raft: Rejecting vote request from %v since we have a leader: %v",
			candidate, leader)
		return
	}

	// Ignore an older term
	if req.Term < r.getCurrentTerm() {
		return
	}

	// Increase the term if we see a newer one
	if req.Term > r.getCurrentTerm() {
		r.updateTerm(req.Term)
		resp.Term = req.Term
	}

	// Check if we have voted yet
	lastVoteTerm, err := r.stable.GetUint64(keyLastVoteTerm)
	if err != nil && err.Error() != "not found" {
		r.logger.Printf("[ERR] raft: Failed to get last vote term: %v", err)
		return
	}
	lastVoteCandBytes, err := r.stable.Get(keyLastVoteCand)
	if err != nil && err.Error() != "not found" {
		r.logger.Printf("[ERR] raft: Failed to get last vote candidate: %v", err)
		return
	}

	// Check if we've voted in this election before
	if lastVoteTerm == req.Term {
		if lastVoteCandBytes != nil && bytes.Compare(lastVoteCandBytes, req.Candidate) == 0 {
			r.logger.Printf("[WARN] raft: Duplicate RequestVote in term %d from candidate %s", req.Term, req.Candidate)
			resp.Granted = true
		} else {
			r.logger.Printf("[INFO] raft: Already voted for %s, cannot grant vote in term %d to candidate %s",
				lastVoteCandBytes, req.Term, req.Candidate)
		}
		return
	}

	// Reject if their term is older
	lastIdx, lastTerm := r.getLastEntry()
	if lastTerm > req.LastLogTerm {
		r.logger.Printf("[WARN] raft: Rejecting vote request from %v since our last term is greater (%d, %d)",
			candidate, lastTerm, req.LastLogTerm)
		return
	}

	if lastTerm == req.LastLogTerm && lastIdx > req.LastLogIndex {
		r.logger.Printf("[WARN] raft: Rejecting vote request from %v since our last index is greater (%d, %d)",
			candidate, lastIdx, req.LastLogIndex)
		return
	}

	// Persist a vote for safety
	if err := r.persistVote(req.Term, req.Candidate); err != nil {
		r.logger.Printf("[ERR] raft: Failed to persist vote: %v", err)
		return
	}

	resp.Granted = true
	r.setLastContact()
	return
}

// installSnapshot is invoked when we get a InstallSnapshot RPC call.
// We must be in the follower state for this, since it means we are
// too far behind a leader for log replay. This must only be called
// from the main thread.
func (r *Raft) installSnapshot(rpc RPC, req *InstallSnapshotRequest) {
	defer metrics.MeasureSince([]string{"raft", "rpc", "installSnapshot"}, time.Now())
	// Setup a response
	resp := &InstallSnapshotResponse{
		Term:    r.getCurrentTerm(),
		Success: false,
	}
	var rpcErr error
	defer func() {
		rpc.Respond(resp, rpcErr)
	}()

	// Ignore an older term
	if req.Term < r.getCurrentTerm() {
		return
	}

	// Increase the term if we see a newer one
	if req.Term > r.getCurrentTerm() {
		r.updateTerm(req.Term)
		resp.Term = req.Term
	}
	// Save the current leader
	r.stepDown()
	r.setLeader(ServerAddress(r.trans.DecodePeer(req.Leader)))
	defer r.setLastContact()

	// Create a new snapshot
	var reqConfiguration Configuration
	var reqConfigurationIndex uint64
	if req.ConfigurationIndex > 0 {
		reqConfiguration = decodeConfiguration(req.Configuration)
		reqConfigurationIndex = req.ConfigurationIndex
	} else {
		reqConfiguration = decodePeers(req.Peers, r.trans)
		reqConfigurationIndex = req.LastLogIndex
	}
	sink, err := r.snapshots.Create(req.LastLogIndex, req.LastLogTerm,
		reqConfiguration, reqConfigurationIndex)
	if err != nil {
		r.logger.Printf("[ERR] raft: Failed to create snapshot to install: %v", err)
		rpcErr = fmt.Errorf("failed to create snapshot: %v", err)
		return
	}

	// Spill the remote snapshot to disk
	n, err := io.Copy(sink, rpc.Reader)
	if err != nil {
		sink.Cancel()
		r.logger.Printf("[ERR] raft: Failed to copy snapshot: %v", err)
		rpcErr = err
		return
	}

	// Check that we received it all
	if n != req.Size {
		sink.Cancel()
		r.logger.Printf("[ERR] raft: Failed to receive whole snapshot: %d / %d", n, req.Size)
		rpcErr = fmt.Errorf("short read")
		return
	}

	// Finalize the snapshot
	if err := sink.Close(); err != nil {
		r.logger.Printf("[ERR] raft: Failed to finalize snapshot: %v", err)
		rpcErr = err
		return
	}
	r.logger.Printf("[INFO] raft: Copied %d bytes to local snapshot", n)

	// Restore snapshot
	future := &restoreFuture{ID: sink.ID()}
	future.init()
	select {
	case r.fsmRestoreCh <- future:
	case <-r.shutdownCh:
		future.respond(ErrRaftShutdown)
		return
	}

	// Wait for the restore to happen
	if err := future.Error(); err != nil {
		r.logger.Printf("[ERR] raft: Failed to restore snapshot: %v", err)
		rpcErr = err
		return
	}

	// Update the lastApplied so we don't replay old logs
	r.setLastApplied(req.LastLogIndex)

	// Update the last stable snapshot info
	r.setLastSnapshot(req.LastLogIndex, req.LastLogTerm)

	// Restore the peer set
	r.configurations.latest = reqConfiguration
	r.configurations.latestIndex = reqConfigurationIndex
	r.configurations.committed = reqConfiguration
	r.configurations.committedIndex = reqConfigurationIndex
	r.updatePeers()

	// Compact logs, continue even if this fails
	if err := r.compactLogs(req.LastLogIndex); err != nil {
		r.logger.Printf("[ERR] raft: Failed to compact logs: %v", err)
	}

	r.logger.Printf("[INFO] raft: Installed remote snapshot")
	resp.Success = true
	return
}

// setLastContact is used to set the last contact time to now
func (r *Raft) setLastContact() {
	r.lastContactLock.Lock()
	r.lastContact = time.Now()
	r.lastContactLock.Unlock()
}

type voteResult struct {
	RequestVoteResponse
	voterID ServerID
}

// persistVote is used to persist our vote for safety.
func (r *Raft) persistVote(term uint64, candidate []byte) error {
	if err := r.stable.SetUint64(keyLastVoteTerm, term); err != nil {
		return err
	}
	if err := r.stable.Set(keyLastVoteCand, candidate); err != nil {
		return err
	}
	return nil
}

func (r *Raft) stepDown() {
	if r.getState() != Follower {
		r.setState(Follower)
		r.updatePeers()
	}
}

func (r *Raft) updateTerm(term uint64) {
	r.setState(Follower)
	oldTerm := r.getCurrentTerm()
	if term > oldTerm {
		r.setCurrentTerm(term)
	} else if term < oldTerm {
		panic("Can't step down to older term")
	}
	r.updatePeers()
}

// setCurrentTerm is used to set the current term in a durable manner.
// The caller must call updatePeers() after changing the term.
func (r *Raft) setCurrentTerm(t uint64) {
	// Persist to disk first
	if err := r.stable.SetUint64(keyCurrentTerm, t); err != nil {
		panic(fmt.Errorf("failed to save current term: %v", err))
	}
	r.raftState.setCurrentTerm(t)
}

// setState is used to update the current state. Any state
// transition causes the known leader to be cleared. This means
// that leader should be set only after updating the state.
// The caller must call updatePeers() after changing the term.
func (r *Raft) setState(state RaftState) {
	r.setLeader("")
	oldState := r.raftState.getState()
	r.raftState.setState(state)
	if oldState != state {
		r.observe(state)
	}
}
