package raft

import (
	"bytes"
	"io"
	"io/ioutil"
)

// InmemSnapshotStore implements the SnapshotStore interface and
// retains only the most recent snapshot
type InmemSnapshotStore struct {
	latest *InmemSnapshotSink
}

// InmemSnapshotSink implements SnapshotSink in memory
type InmemSnapshotSink struct {
	meta     SnapshotMeta
	contents []byte
}

// NewInmemSnapshotStore creates a blank new InmemSnapshotStore
func NewInmemSnapshotStore() *InmemSnapshotStore {
	return &InmemSnapshotStore{
		latest: &InmemSnapshotSink{
			contents: []byte{},
		},
	}
}

// Create replaces the stored snapshot with a new one using the given args
func (m *InmemSnapshotStore) Create(index, term uint64, peers []byte) (SnapshotSink, error) {
	name := snapshotName(term, index)

	sink := m.latest
	sink.meta.ID = name
	sink.meta.Index = index
	sink.meta.Term = term
	sink.meta.Peers = peers
	sink.contents = []byte{}

	return sink, nil
}

// List returns the latest snapshot taken
func (m *InmemSnapshotStore) List() ([]*SnapshotMeta, error) {
	return []*SnapshotMeta{&m.latest.meta}, nil
}

// Open wraps an io.ReadCloser around the snapshot contents
func (m *InmemSnapshotStore) Open(id string) (*SnapshotMeta, io.ReadCloser, error) {
	return &m.latest.meta, ioutil.NopCloser(bytes.NewReader(m.latest.contents)), nil
}

// Write appends the given bytes to the snapshot contents
func (s *InmemSnapshotSink) Write(p []byte) (n int, err error) {
	s.contents = append(s.contents, p...)
	return len(p), nil
}

// Close updates the Size and is otherwise a no-op
func (s *InmemSnapshotSink) Close() error {
	s.meta.Size = int64(len(s.contents))
	return nil
}

func (s *InmemSnapshotSink) ID() string {
	return s.meta.ID
}

func (s *InmemSnapshotSink) Cancel() error {
	return nil
}
