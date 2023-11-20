// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
)

func BenchmarkStoreLogInMem(b *testing.B) {
	conf := DefaultConfig()
	conf.LocalID = "first"
	conf.HeartbeatTimeout = 50 * time.Millisecond
	conf.ElectionTimeout = 50 * time.Millisecond
	conf.LeaderLeaseTimeout = 50 * time.Millisecond
	conf.CommitTimeout = 5 * time.Millisecond
	conf.SnapshotThreshold = 100
	conf.TrailingLogs = 10
	conf.LogLevel = "OFF"
	raft := MakeRaft(b, conf, true)
	raft.logger.SetLevel(hclog.Off)

	NoErr(WaitFor(raft, Leader), b)

	applyAndWait := func(leader *RaftEnv, n, sz int) {
		// Do some commits
		var futures []ApplyFuture
		for i := 0; i < n; i++ {
			futures = append(futures, leader.raft.Apply(logBytes(i, sz), 0))
		}
		for _, f := range futures {
			NoErr(WaitFuture(f), b)
			leader.logger.Debug("applied", "index", f.Index(), "size", sz)
		}
	}

	for i := 0; i < b.N; i++ {
		// Do some commits
		applyAndWait(raft, 100, 10)
		// Do a snapshot
		NoErr(WaitFuture(raft.raft.Snapshot()), b)
	}
}
