// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package fuzzy

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	rdb "github.com/hashicorp/raft-boltdb"
)

type raftNode struct {
	transport *transport
	store     *rdb.BoltStore
	raft      *raft.Raft
	log       hclog.Logger
	fsm       *fuzzyFSM
	name      string
	dir       string
}

func newRaftNode(logger hclog.Logger, tc *transports, h TransportHooks, nodes []string, name string) (*raftNode, error) {
	var err error
	var datadir string
	datadir, err = resolveDirectory(fmt.Sprintf("data/%v", name), true)
	if err != nil {
		return nil, err
	}
	logger.Info("[INFO] Creating new raft Node with data in dir %v", datadir)
	var ss *raft.FileSnapshotStore
	ss, err = raft.NewFileSnapshotStoreWithLogger(datadir, 5, logger)

	if err != nil {
		return nil, fmt.Errorf("unable to initialize snapshots %v", err.Error())
	}
	transport := tc.AddNode(name, h)

	config := raft.DefaultConfig()
	config.SnapshotThreshold = 1409600
	config.SnapshotInterval = time.Hour
	config.Logger = logger
	config.ShutdownOnRemove = false
	config.LocalID = raft.ServerID(name)

	var store *rdb.BoltStore
	store, err = rdb.NewBoltStore(filepath.Join(datadir, "store.bolt"))
	if err != nil {
		return nil, fmt.Errorf("unable to initialize log %v", err.Error())
	}

	if len(nodes) > 0 {
		c := make([]raft.Server, 0, len(nodes))
		for _, n := range nodes {
			c = append(c, raft.Server{Suffrage: raft.Voter, ID: raft.ServerID(n), Address: raft.ServerAddress(n)})
		}
		configuration := raft.Configuration{Servers: c}

		if err = raft.BootstrapCluster(config, store, store, ss, transport, configuration); err != nil {
			return nil, err
		}
	}
	fsm := &fuzzyFSM{}
	var r *raft.Raft
	r, err = raft.NewRaft(config, fsm, store, store, ss, transport)
	if err != nil {
		return nil, err
	}
	n := raftNode{
		transport: transport,
		store:     store,
		raft:      r,
		fsm:       fsm,
		log:       logger,
		name:      name,
		dir:       datadir,
	}
	return &n, nil
}
