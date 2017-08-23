package fuzzy

import (
	"fmt"
	"sync"
	"testing"

	"github.com/hashicorp/raft"
)

// AppendEntriesVerifier looks at all the AppendEntry RPC request and verifies that only one node sends AE requests for any given term
// it also verifies that the request only comes from the node indicated as the leader in the AE message.
type appendEntriesVerifier struct {
	sync.RWMutex
	leaderForTerm map[uint64]string
	errors        []string
}

func (v *appendEntriesVerifier) Report(t *testing.T) {
	v.Lock()
	defer v.Unlock()
	for _, e := range v.errors {
		t.Error(e)
	}
}

func (v *appendEntriesVerifier) Init() {
	v.Lock()
	defer v.Unlock()
	v.leaderForTerm = make(map[uint64]string)
	v.errors = make([]string, 0, 10)
}

func (v *appendEntriesVerifier) PreRPC(src, target string, r *raft.RPC) error {
	return nil
}

func (v *appendEntriesVerifier) PostRPC(src, target string, req *raft.RPC, res *raft.RPCResponse) error {
	return nil
}

func (v *appendEntriesVerifier) PreRequestVote(src, target string, rv *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	return nil, nil
}

func (v *appendEntriesVerifier) PreAppendEntries(src, target string, req *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	term := req.Term
	ldr := string(req.Leader)
	if ldr != src {
		v.Lock()
		defer v.Unlock()
		v.errors = append(v.errors, fmt.Sprintf("Node %v sent an appendEnties request for term %d that said the leader was some other node %v", src, term, ldr))
	}
	v.RLock()
	tl, exists := v.leaderForTerm[term]
	v.RUnlock()
	if exists && tl != ldr {
		v.Lock()
		defer v.Unlock()
		v.errors = append(v.errors, fmt.Sprintf("Node %v sent an AppendEntries request for term %d, but node %v had already done some, multiple leaders for same term!", src, term, tl))
	}
	if !exists {
		v.Lock()
		tl, exists := v.leaderForTerm[term]
		if exists && tl != ldr {
			v.errors = append(v.errors, fmt.Sprintf("Node %v sent an AppendEntries request for term %d, but node %v had already done some, multiple leaders for same term!", src, term, tl))
		}
		if !exists {
			v.leaderForTerm[term] = ldr
		}
		v.Unlock()
	}
	return nil, nil
}
