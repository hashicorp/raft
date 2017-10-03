package fuzzy

import (
	"testing"
	"time"
)

// this runs a 5 node cluster with verifications turned on, but no failures or issues injected.
func TestRaft_NoIssueSanity(t *testing.T) {
	v := appendEntriesVerifier{}
	v.Init()
	cluster := newRaftCluster(t, testLogWriter, "node", 5, &v)
	s := newApplySource("NoIssueSanity")
	applyCount := cluster.ApplyN(t, time.Minute, s, 10000)
	cluster.Stop(t, time.Minute)
	v.Report(t)
	cluster.VerifyLog(t, applyCount)
	cluster.VerifyFSM(t)
}
