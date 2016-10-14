package raft

import "testing"

func TestAPI_Stats(t *testing.T) {
	c := MakeCluster(1, t, nil)
	s := c.rafts[0].Stats()
	if s.LastLogTerm != 1 {
		c.FailNowf("[ERR] err: stats.LastLogTerm expected 1, got %v", s.LastLogTerm)
	}
	c.Close()
}
