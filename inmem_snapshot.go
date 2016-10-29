package raft

import (
	"bytes"
	"fmt"
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
	contents *bytes.Buffer
}

// NewInmemSnapshotStore creates a blank new InmemSnapshotStore
func NewInmemSnapshotStore() *InmemSnapshotStore {
	return &InmemSnapshotStore{
		latest: &InmemSnapshotSink{
			contents: &bytes.Buffer{},
		},
	}
}

// Create replaces the stored snapshot with a new one using the given args
func (m *InmemSnapshotStore) Create(index, term uint64, peers []byte) (SnapshotSink, error) {
	name := snapshotName(term, index)

	sink := m.latest
	sink.meta = SnapshotMeta{
		ID:    name,
		Index: index,
		Term:  term,
		Peers: peers,
	}
	sink.contents = &bytes.Buffer{}

	return sink, nil
}

// List returns the latest snapshot taken
func (m *InmemSnapshotStore) List() ([]*SnapshotMeta, error) {
	return []*SnapshotMeta{&m.latest.meta}, nil
}

// Open wraps an io.ReadCloser around the snapshot contents
func (m *InmemSnapshotStore) Open(id string) (*SnapshotMeta, io.ReadCloser, error) {
	if m.latest.meta.ID != id {
		return nil, nil, fmt.Errorf("[ERR] snapshot: failed to open snapshot id: %s", id)
	}

	return &m.latest.meta, ioutil.NopCloser(m.latest.contents), nil
}

// Write appends the given bytes to the snapshot contents
func (s *InmemSnapshotSink) Write(p []byte) (n int, err error) {
	written, err := io.Copy(s.contents, bytes.NewReader(p))
	s.meta.Size += written
	return int(written), err
}

// Close updates the Size and is otherwise a no-op
func (s *InmemSnapshotSink) Close() error {
	return nil
}

func (s *InmemSnapshotSink) ID() string {
	return s.meta.ID
}

func (s *InmemSnapshotSink) Cancel() error {
	return nil
}
