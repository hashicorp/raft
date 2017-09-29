package fuzzy

import (
	"fmt"
	"log"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	rdb "github.com/hashicorp/raft-boltdb"
)

type raftNode struct {
	transport *transport
	store     *rdb.BoltStore
	raft      *raft.Raft
	log       *log.Logger
	fsm       *fuzzyFSM
	name      string
	dir       string
}

func newRaftNode(logger *log.Logger, tc *transports, h TransportHooks, nodes []string, name string) (*raftNode, error) {
	datadir, err := resolveDirectory(fmt.Sprintf("data/%v", name), true)
	if err != nil {
		return nil, err
	}
	logger.Printf("[INFO] Creating new raft Node with data in dir %v", datadir)
	ss, err := raft.NewFileSnapshotStoreWithLogger(datadir, 5, logger)
	if err != nil {
		return nil, fmt.Errorf("Unable to initialize snapshots %v\n", err.Error())
	}
	transport := tc.AddNode(name, h)

	config := raft.DefaultConfig()
	config.SnapshotThreshold = 1409600
	config.SnapshotInterval = time.Hour
	config.Logger = logger
	config.ShutdownOnRemove = false
	config.LocalID = raft.ServerID(name)

	store, err := rdb.NewBoltStore(filepath.Join(datadir, "store.bolt"))
	if err != nil {
		return nil, fmt.Errorf("Unable to initialize log %v\n", err.Error())
	}

	if len(nodes) > 0 {
		c := make([]raft.Server, 0, len(nodes))
		for _, n := range nodes {
			c = append(c, raft.Server{Suffrage: raft.Voter, ID: raft.ServerID(n), Address: raft.ServerAddress(n)})
		}
		configuration := raft.Configuration{c}
		if err := raft.BootstrapCluster(config, store, store, ss, transport, configuration); err != nil {
			return nil, err
		}
	}
	fsm := &fuzzyFSM{}
	raft, err := raft.NewRaft(config, fsm, store, store, ss, transport)
	if err != nil {
		return nil, err
	}
	n := raftNode{
		transport: transport,
		store:     store,
		raft:      raft,
		fsm:       fsm,
		log:       logger,
		name:      name,
		dir:       datadir,
	}
	return &n, nil
}
