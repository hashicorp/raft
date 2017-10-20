package fuzzy

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// 5 node cluster where the leader and another node get regularly partitioned off
// eventually all partitions heal.
func TestRaft_LeaderPartitions(t *testing.T) {
	hooks := NewPartitioner()
	cluster := newRaftCluster(t, testLogWriter, "lp", 5, hooks)
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

type Partitioner struct {
	verifier appendEntriesVerifier
	lock     sync.RWMutex // protects partitoned / nextGroup
	// this is a map of node -> partition group, only nodes in the same partition group can communicate with each other
	partitioned map[string]int
	nextGroup   int
}

func NewPartitioner() *Partitioner {
	p := &Partitioner{
		partitioned: make(map[string]int),
		nextGroup:   1,
	}
	p.verifier.Init()
	return p
}

// PartitionOff creates a partition where the supplied nodes can only communicate with each other
// returns the partition group, which can be used later with Heal to heal this specific partition
func (p *Partitioner) PartitionOff(l Logger, nodes []*raftNode) int {
	nn := make([]string, 0, len(nodes))
	p.lock.Lock()
	defer p.lock.Unlock()
	pGroup := p.nextGroup
	p.nextGroup++
	for _, n := range nodes {
		p.partitioned[n.name] = pGroup
		nn = append(nn, n.name)
	}
	l.Logf("Created partition %d with nodes %v, partitions now are %v", pGroup, nn, p)
	return pGroup
}

func (p *Partitioner) Heal(l Logger, pGroup int) {
	p.lock.Lock()
	defer p.lock.Unlock()
	for k, v := range p.partitioned {
		if v == pGroup {
			p.partitioned[k] = 0
		}
	}
	l.Logf("Healing partition group %d, now partitions are %v", pGroup, p)
}

func (p *Partitioner) String() string {
	pl := make([][]string, 0, 10)
	for n, pv := range p.partitioned {
		if pv > 0 {
			for pv >= len(pl) {
				pl = append(pl, nil)
			}
			pl[pv] = append(pl[pv], n)
		}
	}
	b := bytes.Buffer{}
	for i, n := range pl {
		if len(n) > 0 {
			if b.Len() > 0 {
				b.WriteString(", ")
			}
			fmt.Fprintf(&b, "%d = %v", i, n)
		}
	}
	if b.Len() == 0 {
		return "[None]"
	}
	return b.String()
}

func (p *Partitioner) HealAll(l Logger) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.partitioned = make(map[string]int)
	l.Logf("Healing all partitions, partitions now %v", p)
}

func (p *Partitioner) Report(t *testing.T) {
	p.verifier.Report(t)
}

func (p *Partitioner) PreRPC(s, t string, r *raft.RPC) error {
	p.lock.RLock()
	sp := p.partitioned[s]
	st := p.partitioned[t]
	p.lock.RUnlock()
	if sp == st {
		return nil
	}
	return fmt.Errorf("Unable to connect to %v, from %v", t, s)
}

func (p *Partitioner) PostRPC(s, t string, req *raft.RPC, res *raft.RPCResponse) error {
	return nil
}

func (p *Partitioner) PreRequestVote(src, target string, v *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	return nil, nil
}

func (p *Partitioner) PreAppendEntries(src, target string, v *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	return nil, nil
}
