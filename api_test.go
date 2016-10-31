package raft

import (
	"fmt"
	"testing"
)

func TestAPI_Stats(t *testing.T) {
	c := MakeCluster(1, t, nil)
	defer c.Close()
	future := c.rafts[0].Stats()
	if err := future.Error(); err != nil {
		c.FailNowf("Stats() returned err %v", err)
	}
	s := future.Stats()
	if s.LastLogTerm != 1 {
		c.FailNowf("stats.LastLogTerm expected 1, got %v", s.LastLogTerm)
	}
}

func TestAPI_Stats_membershipFormatting(t *testing.T) {
	c := MakeCluster(1, t, nil)
	defer c.Close()
	c.GetInState(Leader)
	addFuture := c.rafts[0].AddNonvoter("S2", "s2-addr", 0, 0)
	err := addFuture.Error()
	if err != nil {
		c.FailNowf("AddNonvoter() returned err %v", err)
	}

	statsFuture := c.rafts[0].Stats()
	err = statsFuture.Error()
	if err != nil {
		c.FailNowf("Stats() returned err %v", err)
	}
	s := statsFuture.Stats()
	membership := "not found"
	for _, kv := range s.Strings() {
		if kv.K == "latest_membership" {
			membership = kv.V
		}
	}
	exp := fmt.Sprintf("[%v at %v (Voter), S2 at s2-addr (Nonvoter)]",
		c.rafts[0].server.localID, c.rafts[0].server.localAddr)
	if membership != exp {
		c.Failf("membership not stringified correctly. Expected: '%s', got: '%s'",
			exp, membership)
	}
}
