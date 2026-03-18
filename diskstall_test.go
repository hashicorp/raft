// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLeaderHeartbeatsWithStalledDisk(t *testing.T) {
	c := MakeClusterCustom(t, &MakeClusterOpts{
		Peers:               3,
		Bootstrap:           true,
		LogstoreWrapperFunc: newBlockingLogStore,
	})
	t.Cleanup(c.Close)

	go func() {
		i := 0
		ticker := time.NewTicker(500 * time.Millisecond)
		for {
			select {
			case <-t.Context().Done():
				return
			case <-ticker.C:
				i++
				leader := c.Leader()
				fut := leader.Apply(fmt.Appendf([]byte{}, "test%d", i), 0)
				if err := fut.Error(); err != nil {
					t.Logf("got error trying to write: %v", err)
				} else {
					t.Logf("write %d ok", i)
				}
			}
		}
	}()

	t.Log("waiting for 5 seconds before partitioning leader")
	time.Sleep(time.Second * 5)

	oldLeader := c.Leader()
	oldLeaderID := oldLeader.leaderID
	oldLeaderTerm := oldLeader.getCurrentTerm()
	c.Partition([]ServerAddress{c.Leader().localAddr})

	var newLeaderTerm uint64
	ctx, cancel := context.WithTimeout(t.Context(), 10*c.propagateTimeout)
	t.Cleanup(cancel)
DONE:
	for {
		select {
		case <-ctx.Done():
			t.Fatal("election didn't happen!")
		default:
			newLeader := c.Leader()
			if newLeader.leaderID != oldLeaderID {
				t.Log("leader has stepped down!")
				newLeaderTerm = newLeader.getCurrentTerm()
				require.NotEqual(t, newLeaderTerm, oldLeaderTerm)
				cancel()
				break DONE
			}
		}
	}

	require.Len(t, c.WaitForFollowers(1), 1)

	t.Log("leader was elected. healing parition")

	// reconnect the partitioned node
	c.FullyConnect()
	time.Sleep(3 * c.propagateTimeout)

	leaderTerm := c.Leader().getCurrentTerm()
	require.Equal(t, newLeaderTerm, leaderTerm)

	t.Log("blocking disk on leader")

	leader := c.Leader()
	leaderID := leader.leaderID
	leaderStore := leader.logs.(*blockingLogStore)
	leaderStore.block()
	t.Cleanup(leaderStore.unblock)

	ctx, cancel = context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	for {
		select {
		case <-ctx.Done():
			t.Fatal("leader did not step down!")
		default:
			if c.Leader().leaderID != leaderID {
				t.Log("leader has stepped down!")
				return
			}
		}
	}
}

// blockingLogStore wraps a LogStore and blocks GetLog calls on demand,
// simulating disk IO stalls.
type blockingLogStore struct {
	LogStore
	mu       sync.Mutex
	blocked  atomic.Bool
	unblockC chan struct{}
}

func newBlockingLogStore(inner LogStore) LogStore {
	return &blockingLogStore{
		LogStore: inner,
		unblockC: make(chan struct{}),
	}
}

func (b *blockingLogStore) block() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.blocked.Store(true)
	b.unblockC = make(chan struct{})
}

func (b *blockingLogStore) unblock() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.blocked.Load() {
		b.blocked.Store(false)
		close(b.unblockC)
	}
}

func (b *blockingLogStore) GetLog(index uint64, log *Log) error {
	if b.blocked.Load() {
		b.mu.Lock()
		ch := b.unblockC
		b.mu.Unlock()
		<-ch
	}
	return b.LogStore.GetLog(index, log)
}
