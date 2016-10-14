package raft

import "testing"

func TestAPI_Stats(t *testing.T) {
	c := MakeCluster(1, t, nil)
	future := c.rafts[0].Stats()
	if err := future.Error(); err != nil {
		c.FailNowf("[ERR] Stats() returned err %v", err)
	}
	s := future.Stats()
	if s.LastLogTerm != 1 {
		c.FailNowf("[ERR] stats.LastLogTerm expected 1, got %v", s.LastLogTerm)
	}
	c.Close()
}
