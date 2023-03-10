package raft

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/require"
)

// CheckInteg will skip a test if integration testing is not enabled.
func CheckInteg(t *testing.T) {
	if !IsInteg() {
		t.SkipNow()
	}
}

// IsInteg returns a boolean telling you if we're in integ testing mode.
func IsInteg() bool {
	return os.Getenv("INTEG_TESTS") != ""
}

type RaftEnv struct {
	dir      string
	conf     *Config
	fsm      *MockFSM
	store    *InmemStore
	snapshot *FileSnapshotStore
	trans    *NetworkTransport
	raft     *Raft
	logger   hclog.Logger
}

// Release shuts down and cleans up any stored data, its not restartable after this
func (r *RaftEnv) Release() {
	r.Shutdown()
	os.RemoveAll(r.dir)
}

// Shutdown shuts down raft & transport, but keeps track of its data, its restartable
// after a Shutdown() by calling Start()
func (r *RaftEnv) Shutdown() {
	r.logger.Warn(fmt.Sprintf("Shutdown node at %v", r.raft.localAddr))
	f := r.raft.Shutdown()
	if err := f.Error(); err != nil {
		panic(err)
	}
	r.trans.Close()
}

// Restart will start a raft node that was previously Shutdown()
func (r *RaftEnv) Restart(t *testing.T) {
	trans, err := NewTCPTransport(string(r.raft.localAddr), nil, 2, time.Second, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	r.trans = trans
	r.logger.Info("starting node", "addr", trans.LocalAddr())
	raft, err := NewRaft(r.conf, r.fsm, r.store, r.store, r.snapshot, r.trans)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	r.raft = raft
}

func MakeRaft(t *testing.T, conf *Config, bootstrap bool) *RaftEnv {
	// Set the config
	if conf == nil {
		conf = inmemConfig(t)
	}

	dir, err := ioutil.TempDir("", "raft")
	if err != nil {
		t.Fatalf("err: %v ", err)
	}

	stable := NewInmemStore()

	snap, err := NewFileSnapshotStore(dir, 3, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	env := &RaftEnv{
		conf:     conf,
		dir:      dir,
		store:    stable,
		snapshot: snap,
		fsm:      &MockFSM{},
	}
	trans, err := NewTCPTransport("localhost:0", nil, 2, time.Second, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	env.logger = hclog.New(&hclog.LoggerOptions{
		Name: string(trans.LocalAddr()) + " :",
	})
	env.trans = trans

	if bootstrap {
		var configuration Configuration
		configuration.Servers = append(configuration.Servers, Server{
			Suffrage: Voter,
			ID:       conf.LocalID,
			Address:  trans.LocalAddr(),
		})
		err = BootstrapCluster(conf, stable, stable, snap, trans, configuration)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
	}
	env.logger.Info("starting node", "addr", trans.LocalAddr())
	conf.Logger = env.logger
	raft, err := NewRaft(conf, env.fsm, stable, stable, snap, trans)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	env.raft = raft
	return env
}

func WaitFor(env *RaftEnv, state RaftState) error {
	limit := time.Now().Add(200 * time.Millisecond)
	for env.raft.State() != state {
		if time.Now().Before(limit) {
			time.Sleep(10 * time.Millisecond)
		} else {
			return fmt.Errorf("failed to transition to state %v", state)
		}
	}
	return nil
}

func WaitForAny(state RaftState, envs []*RaftEnv) (*RaftEnv, error) {
	limit := time.Now().Add(200 * time.Millisecond)
CHECK:
	for _, env := range envs {
		if env.raft.State() == state {
			return env, nil
		}
	}
	if time.Now().Before(limit) {
		goto WAIT
	}
	return nil, fmt.Errorf("failed to find node in %v state", state)
WAIT:
	time.Sleep(10 * time.Millisecond)
	goto CHECK
}

func WaitFuture(f Future, t *testing.T) error {
	timer := time.AfterFunc(1000*time.Millisecond, func() {
		panic(fmt.Errorf("timeout waiting for future %v", f))
	})
	defer timer.Stop()
	return f.Error()
}

func NoErr(err error, t *testing.T) {
	t.Helper()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
}

func CheckConsistent(envs []*RaftEnv, t *testing.T) {
	limit := time.Now().Add(400 * time.Millisecond)
	first := envs[0]
	first.fsm.Lock()
	defer first.fsm.Unlock()
	var err error
CHECK:
	l1 := len(first.fsm.logs)
	for i := 1; i < len(envs); i++ {
		env := envs[i]
		env.fsm.Lock()
		l2 := len(env.fsm.logs)
		if l1 != l2 {
			err = fmt.Errorf("log length mismatch %d %d", l1, l2)
			env.fsm.Unlock()
			goto ERR
		}
		for idx, log := range first.fsm.logs {
			other := env.fsm.logs[idx]
			if bytes.Compare(log, other) != 0 {
				err = fmt.Errorf("log entry %d mismatch between %s/%s : '%s' / '%s'", idx, first.raft.localAddr, env.raft.localAddr, log, other)
				env.fsm.Unlock()
				goto ERR
			}
		}
		env.fsm.Unlock()
	}
	return
ERR:
	if time.Now().After(limit) {
		t.Fatalf("%v", err)
	}
	first.fsm.Unlock()
	time.Sleep(20 * time.Millisecond)
	first.fsm.Lock()
	goto CHECK
}

// return a log entry that's at least sz long that has the prefix 'test i '
func logBytes(i, sz int) []byte {
	var logBuffer bytes.Buffer
	fmt.Fprintf(&logBuffer, "test %d ", i)
	for logBuffer.Len() < sz {
		logBuffer.WriteByte('x')
	}
	return logBuffer.Bytes()
}

// Tests Raft by creating a cluster, growing it to 5 nodes while
// causing various stressful conditions
func TestRaft_Integ(t *testing.T) {
	CheckInteg(t)
	conf := DefaultConfig()
	conf.LocalID = ServerID("first")
	conf.HeartbeatTimeout = 50 * time.Millisecond
	conf.ElectionTimeout = 50 * time.Millisecond
	conf.LeaderLeaseTimeout = 50 * time.Millisecond
	conf.CommitTimeout = 5 * time.Millisecond
	conf.SnapshotThreshold = 100
	conf.TrailingLogs = 10

	// Create a single node
	env1 := MakeRaft(t, conf, true)
	NoErr(WaitFor(env1, Leader), t)

	totalApplied := 0
	applyAndWait := func(leader *RaftEnv, n, sz int) {
		// Do some commits
		var futures []ApplyFuture
		for i := 0; i < n; i++ {
			futures = append(futures, leader.raft.Apply(logBytes(i, sz), 0))
		}
		for _, f := range futures {
			NoErr(WaitFuture(f, t), t)
			leader.logger.Debug("applied", "index", f.Index(), "size", sz)
		}
		totalApplied += n
	}
	// Do some commits
	applyAndWait(env1, 100, 10)

	// Do a snapshot
	NoErr(WaitFuture(env1.raft.Snapshot(), t), t)

	// Join a few nodes!
	var envs []*RaftEnv
	for i := 0; i < 4; i++ {
		conf.LocalID = ServerID(fmt.Sprintf("next-batch-%d", i))
		env := MakeRaft(t, conf, false)
		addr := env.trans.LocalAddr()
		NoErr(WaitFuture(env1.raft.AddVoter(conf.LocalID, addr, 0, 0), t), t)
		envs = append(envs, env)
	}

	// Wait for a leader
	leader, err := WaitForAny(Leader, append([]*RaftEnv{env1}, envs...))
	NoErr(err, t)

	// Do some more commits
	applyAndWait(leader, 100, 10)

	// Snapshot the leader
	NoErr(WaitFuture(leader.raft.Snapshot(), t), t)

	CheckConsistent(append([]*RaftEnv{env1}, envs...), t)

	// shutdown a follower
	disconnected := envs[len(envs)-1]
	disconnected.Shutdown()

	// Do some more commits [make sure the resulting snapshot will be a reasonable size]
	applyAndWait(leader, 100, 10000)

	// snapshot the leader [leaders log should be compacted past the disconnected follower log now]
	NoErr(WaitFuture(leader.raft.Snapshot(), t), t)

	// Unfortunately we need to wait for the leader to start backing off RPCs to the down follower
	// such that when the follower comes back up it'll run an election before it gets an rpc from
	// the leader
	time.Sleep(time.Second * 5)

	// start the now out of date follower back up
	disconnected.Restart(t)

	// wait for it to get caught up
	timeout := time.Now().Add(time.Second * 10)
	for disconnected.raft.getLastApplied() < leader.raft.getLastApplied() {
		time.Sleep(time.Millisecond)
		if time.Now().After(timeout) {
			t.Fatalf("Gave up waiting for follower to get caught up to leader")
		}
	}

	CheckConsistent(append([]*RaftEnv{env1}, envs...), t)

	// Shoot two nodes in the head!
	rm1, rm2 := envs[0], envs[1]
	rm1.Release()
	rm2.Release()
	envs = envs[2:]
	time.Sleep(10 * time.Millisecond)

	// Wait for a leader
	leader, err = WaitForAny(Leader, append([]*RaftEnv{env1}, envs...))
	NoErr(err, t)

	// Do some more commits
	applyAndWait(leader, 100, 10)

	// Join a few new nodes!
	for i := 0; i < 2; i++ {
		conf.LocalID = ServerID(fmt.Sprintf("final-batch-%d", i))
		env := MakeRaft(t, conf, false)
		addr := env.trans.LocalAddr()
		NoErr(WaitFuture(leader.raft.AddVoter(conf.LocalID, addr, 0, 0), t), t)
		envs = append(envs, env)

		leader, err = WaitForAny(Leader, append([]*RaftEnv{env1}, envs...))
		NoErr(err, t)
	}

	// Wait for a leader
	leader, err = WaitForAny(Leader, append([]*RaftEnv{env1}, envs...))
	NoErr(err, t)

	// Remove the old nodes
	NoErr(WaitFuture(leader.raft.RemoveServer(rm1.raft.localID, 0, 0), t), t)
	NoErr(WaitFuture(leader.raft.RemoveServer(rm2.raft.localID, 0, 0), t), t)

	// Shoot the leader
	env1.Release()
	time.Sleep(3 * conf.HeartbeatTimeout)

	// Wait for a leader
	leader, err = WaitForAny(Leader, envs)
	NoErr(err, t)

	allEnvs := append([]*RaftEnv{env1}, envs...)
	CheckConsistent(allEnvs, t)

	if len(env1.fsm.logs) != totalApplied {
		t.Fatalf("should apply %d logs! %d", totalApplied, len(env1.fsm.logs))
	}

	for _, e := range envs {
		e.Release()
	}
}

func TestRaft_RestartFollower_LongInitialHeartbeat(t *testing.T) {
	CheckInteg(t)
	tests := []struct {
		name                   string
		restartInitialTimeouts time.Duration
		expectNewLeader        bool
	}{
		{"Default", 0, true},
		{"InitialHigher", time.Second, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := DefaultConfig()
			conf.LocalID = ServerID("first")
			conf.HeartbeatTimeout = 50 * time.Millisecond
			conf.ElectionTimeout = 50 * time.Millisecond
			conf.LeaderLeaseTimeout = 50 * time.Millisecond
			conf.CommitTimeout = 5 * time.Millisecond
			conf.SnapshotThreshold = 100
			conf.TrailingLogs = 10

			// Create a single node
			env1 := MakeRaft(t, conf, true)
			NoErr(WaitFor(env1, Leader), t)

			// Join a few nodes!
			var envs []*RaftEnv
			for i := 0; i < 2; i++ {
				conf.LocalID = ServerID(fmt.Sprintf("next-batch-%d", i))
				env := MakeRaft(t, conf, false)
				addr := env.trans.LocalAddr()
				NoErr(WaitFuture(env1.raft.AddVoter(conf.LocalID, addr, 0, 0), t), t)
				envs = append(envs, env)
			}
			allEnvs := append([]*RaftEnv{env1}, envs...)

			// Wait for a leader
			_, err := WaitForAny(Leader, append([]*RaftEnv{env1}, envs...))
			NoErr(err, t)

			CheckConsistent(append([]*RaftEnv{env1}, envs...), t)
			// TODO without this sleep, the restarted follower doesn't have any stored config
			// and aborts the election because it doesn't know of any peers.  Shouldn't
			// CheckConsistent prevent that?
			time.Sleep(time.Second)

			// shutdown a follower
			disconnected := envs[len(envs)-1]
			disconnected.logger.Info("stopping follower")
			disconnected.Shutdown()

			seeNewLeader := func(o *Observation) bool { _, ok := o.Data.(LeaderObservation); return ok }
			leaderCh := make(chan Observation)
			// TODO Closing this channel results in panics, even though we're calling Release.
			//defer close(leaderCh)
			leaderChanges := new(uint32)
			go func() {
				for range leaderCh {
					atomic.AddUint32(leaderChanges, 1)
				}
			}()

			requestVoteCh := make(chan Observation)
			seeRequestVote := func(o *Observation) bool { _, ok := o.Data.(RequestVoteRequest); return ok }
			requestVotes := new(uint32)
			go func() {
				for range requestVoteCh {
					atomic.AddUint32(requestVotes, 1)
				}
			}()

			for _, env := range allEnvs {
				env.raft.RegisterObserver(NewObserver(leaderCh, false, seeNewLeader))
			}

			// Unfortunately we need to wait for the leader to start backing off RPCs to the down follower
			// such that when the follower comes back up it'll run an election before it gets an rpc from
			// the leader
			time.Sleep(time.Second * 5)

			if tt.restartInitialTimeouts != 0 {
				disconnected.conf.HeartbeatTimeout = tt.restartInitialTimeouts
				disconnected.conf.ElectionTimeout = tt.restartInitialTimeouts
			}
			disconnected.logger.Info("restarting follower")
			disconnected.Restart(t)

			time.Sleep(time.Second * 2)

			if tt.expectNewLeader {
				require.NotEqual(t, 0, atomic.LoadUint32(leaderChanges))
			} else {
				require.Equal(t, uint32(0), atomic.LoadUint32(leaderChanges))
			}

			if tt.restartInitialTimeouts != 0 {
				for _, env := range envs {
					env.raft.RegisterObserver(NewObserver(requestVoteCh, false, seeRequestVote))
					NoErr(env.raft.ReloadConfig(ReloadableConfig{
						TrailingLogs:      conf.TrailingLogs,
						SnapshotInterval:  conf.SnapshotInterval,
						SnapshotThreshold: conf.SnapshotThreshold,
						HeartbeatTimeout:  250 * time.Millisecond,
						ElectionTimeout:   250 * time.Millisecond,
					}), t)
				}
				// Make sure that reload by itself doesn't trigger a vote
				time.Sleep(300 * time.Millisecond)
				require.Equal(t, uint32(0), atomic.LoadUint32(requestVotes))

				// Stop the leader, ensure that we don't see a request vote within the first 50ms
				// (original config of the non-restarted follower), but that we do see one within
				// the 250ms both followers should now be using for heartbeat timeout.  Well, not
				// quite: we wait for two heartbeat intervals (plus a fudge factor), because the
				// first time around, last contact will have been recent enough that no vote will
				// be triggered.
				env1.logger.Info("stopping leader")
				env1.Shutdown()
				time.Sleep(50 * time.Millisecond)
				require.Equal(t, uint32(0), atomic.LoadUint32(requestVotes))
				time.Sleep(600 * time.Millisecond)
				require.NotEqual(t, uint32(0), atomic.LoadUint32(requestVotes))
			}

			for _, e := range allEnvs {
				e.Release()
			}
		})
	}
}
