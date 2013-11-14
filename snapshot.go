package raft

import (
	"io"
	"net"
)

// SnapshotMeta is for meta data of a snaphot.
type SnapshotMeta struct {
	ID    string // ID is opaque to the store, and is used for opening
	Index uint64
	Term  uint64
	Peers []net.Addr
}

// SnapshotStore interface is used to allow for flexible implementations
// of snapshot storage and retrieval. For example, a client could implement
// a shared state store such as S3, allowing new nodes to restore snapshots
// without steaming from the leader.
type SnapshotStore interface {
	// StartSnapshot is used to begin a snapshot at a given index and term,
	// with the current peer set.
	StartSnapshot(index, term uint64, peers []net.Addr) (SnapshotSink, error)

	// ListSnapshots is used to list the available snapshots in the store
	// It should return then in descending order, with the highest index first.
	ListSnapshots() []*SnapshotMeta

	// OpenSnapshot takes a snapshot ID and provides a ReadCloser. Once close is
	// called it is assumed the snapshot is no longer needed.
	OpenSnapshot(id string) (io.ReadCloser, error)
}

// SnapshotSink is returned by StartSnapshot. The FSM will Write state
// to the sink and call Close on completion. On error, Cancel will be invoked
type SnapshotSink interface {
	io.WriteCloser
	Cancel()
}
