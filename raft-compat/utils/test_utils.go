package utils

import (
	"fmt"
	"github.com/hashicorp/raft"
	raftrs "github.com/hashicorp/raft-latest"
	"github.com/hashicorp/raft/compat/testcluster"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func WaitForNewLeader[T testcluster.RaftNode](t *testing.T, oldLeader string, c testcluster.RaftCluster[T]) {

	leader := func() string {
		for i := 0; i < c.Len(); i++ {
			switch r := c.Raft(i).(type) {
			case *raft.Raft:
				if r.State() == raft.Leader {
					return c.ID(i)
				}
			case *raftrs.Raft:
				if r.State() == raftrs.Leader {
					return c.ID(i)
				}
			}
		}
		return ""
	}
	after := time.After(5 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	for {
		select {
		case <-after:
			t.Fatalf("timedout")
		case <-ticker.C:
			id := leader()
			if id != "" {
				if id != oldLeader || oldLeader == "" {
					return
				}
			}
		}
	}
}

type future interface {
	Error() error
}

func WaitFuture(t *testing.T, f future) {
	timer := time.AfterFunc(1000*time.Millisecond, func() {
		panic(fmt.Errorf("timeout waiting for future %v", f))
	})
	defer timer.Stop()
	require.NoError(t, f.Error())
}
