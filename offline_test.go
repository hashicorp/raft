package raft

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestRaft_RecoverCluster_NoState(t *testing.T) {
	c := MakeClusterNoBootstrap(1, t, nil)
	defer c.Close()

	r := c.rafts[0].serverInternals
	membership := Membership{
		Servers: []Server{{
			ID:      r.localID,
			Address: r.localAddr,
		}},
	}
	err := RecoverCluster(&r.conf, &MockFSM{}, r.logs, r.stable,
		r.snapshots, r.trans, membership)
	if err == nil || !strings.Contains(err.Error(), "no initial state") {
		c.FailNowf("should have failed for no initial state: %v", err)
	}
}

func TestRaft_RecoverCluster(t *testing.T) {
	// Run with different number of applies which will cover no snapshot and
	// snapshot + log scenarios. By sweeping through the trailing logs value
	// we will also hit the case where we have a snapshot only.
	runRecover := func(applies int) {
		conf := inmemConfig(t)
		conf.TrailingLogs = 10
		c := MakeCluster(3, t, conf)
		defer c.Close()

		// Perform some commits.
		c.logger.Debug("Running with applies=%d", applies)
		leader := c.Leader()
		for i := 0; i < applies; i++ {
			future := leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
			if err := future.Error(); err != nil {
				c.FailNowf("apply err: %v", err)
			}
		}

		// Snap the membership configuration.
		future := leader.GetMembership()
		if err := future.Error(); err != nil {
			c.FailNowf("get membership err: %v", err)
		}
		membership := future.Membership()

		// Shut down the cluster.
		for _, sec := range c.rafts {
			if err := sec.Shutdown().Error(); err != nil {
				c.FailNowf("shutdown err: %v", err)
			}
		}

		// Recover the cluster. We need to replace the transport and we
		// replace the FSM so no state can carry over.
		for i, r := range c.rafts {
			r := r.serverInternals
			before, err := r.snapshots.List()
			if err != nil {
				c.FailNowf("snapshot list err: %v", err)
			}
			if err := RecoverCluster(&r.conf, &MockFSM{}, r.logs, r.stable,
				r.snapshots, r.trans, membership); err != nil {
				c.FailNowf("recover err: %v", err)
			}

			// Make sure the recovery looks right.
			after, err := r.snapshots.List()
			if err != nil {
				c.FailNowf("snapshot list err: %v", err)
			}
			if len(after) != len(before)+1 {
				c.FailNowf("expected a new snapshot, %d vs. %d", len(before), len(after))
			}
			first, err := r.logs.FirstIndex()
			if err != nil {
				c.FailNowf("first log index err: %v", err)
			}
			last, err := r.logs.LastIndex()
			if err != nil {
				c.FailNowf("last log index err: %v", err)
			}
			if first != 0 || last != 0 {
				c.FailNowf("expected empty logs, got %d/%d", first, last)
			}

			// Fire up the recovered Raft instance. We have to patch
			// up the cluster state manually since this is an unusual
			// operation.
			_, trans := NewInmemTransport(r.localAddr)
			r2, err := NewRaft(&r.conf, &MockFSM{}, r.logs, r.stable, r.snapshots, trans)
			if err != nil {
				c.FailNowf("new raft err: %v", err)
			}
			c.rafts[i] = r2
			c.trans[i] = r2.serverInternals.trans.(*InmemTransport)
			c.fsms[i] = r2.serverInternals.fsm.(*MockFSM)
		}
		c.FullyConnect()
		time.Sleep(c.propagateTimeout)

		// Let things settle and make sure we recovered.
		c.EnsureLeader(t, c.Leader().serverInternals.localAddr)
		c.EnsureSame(t)
		c.EnsureSamePeers(t)
	}
	for applies := 0; applies < 20; applies++ {
		runRecover(applies)
	}
}

func TestRaft_HasExistingState(t *testing.T) {
	// Make a cluster.
	c := MakeCluster(2, t, nil)
	defer c.Close()

	// Make a new cluster of 1.
	c1 := MakeClusterNoBootstrap(1, t, nil)

	// Make sure the initial state is clean.
	r := c1.rafts[0].serverInternals
	hasState, err := HasExistingState(r.logs, r.stable, r.snapshots)
	if err != nil || hasState {
		c.FailNowf("should not have any existing state, %v", err)
	}

	// Merge clusters.
	c.Merge(c1)
	c.FullyConnect()

	// Join the new node in.
	future := c.Leader().AddVoter(r.localID, r.localAddr, 0, 0)
	if err := future.Error(); err != nil {
		c.FailNowf("err: %v", err)
	}

	// Check the FSMs.
	c.EnsureSame(t)

	// Check the peers.
	c.EnsureSamePeers(t)

	// Ensure one leader.
	c.EnsureLeader(t, c.Leader().serverInternals.localAddr)

	// Make sure it's not clean.
	hasState, err = HasExistingState(r.logs, r.stable, r.snapshots)
	if err != nil || !hasState {
		c.FailNowf("should have some existing state, %v", err)
	}
}
