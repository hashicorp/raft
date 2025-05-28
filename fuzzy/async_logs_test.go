package fuzzy

import (
	"math/rand"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// 5 node cluster where the leader and another node get regularly partitioned off
// eventually all partitions heal, but using async log cache.
func TestRaft_AsyncLogWithPartitions(t *testing.T) {
	hooks := NewPartitioner()

	cluster := newRaftClusterWithFactory(t, testLogWriter, "lp", 5, hooks, newAsyncRaft)
	cluster.Leader(time.Second * 10)
	s := newApplySource("LeaderPartitions")
	applier := s.apply(t, cluster, 5)
	for i := 0; i < 10; i++ {
		pg := hooks.PartitionOff(cluster.log, cluster.LeaderPlus(rand.Intn(4)))
		time.Sleep(time.Second * 4)
		r := rand.Intn(10)
		if r < 1 {
			cluster.log.Logf("Healing no partitions!")
		} else if r < 4 {
			hooks.HealAll(cluster.log)
		} else {
			hooks.Heal(cluster.log, pg)
		}
		time.Sleep(time.Second * 5)
	}
	hooks.HealAll(cluster.log)
	cluster.Leader(time.Hour)
	applier.stop()
	cluster.Stop(t, time.Minute*10)
	hooks.Report(t)
	cluster.VerifyLog(t, applier.applied)
	cluster.VerifyFSM(t)
}

func newAsyncRaft(conf *raft.Config, fsm raft.FSM, logs raft.LogStore, stable raft.StableStore, snaps raft.SnapshotStore, trans raft.Transport) (*raft.Raft, error) {
	// Wrap the log store in an async cache
	asyncLogs, err := raft.NewLogCacheAsync(128, logs)
	if err != nil {
		return nil, err
	}

	return raft.NewRaft(conf, fsm, asyncLogs, stable, snaps, trans)
}
