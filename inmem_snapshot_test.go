package raft

import (
	"bytes"
	"io"
	"reflect"
	"testing"
)

func TestInmemSnapshotStoreImpl(t *testing.T) {
	var impl interface{} = &InmemSnapshotStore{}
	if _, ok := impl.(SnapshotStore); !ok {
		t.Fatalf("InmemSnapshotStore not a SnapshotStore")
	}
}

func TestInmemSnapshotSinkImpl(t *testing.T) {
	var impl interface{} = &InmemSnapshotSink{}
	if _, ok := impl.(SnapshotSink); !ok {
		t.Fatalf("InmemSnapshotSink not a SnapshotSink")
	}
}

func TestInmemSS_CreateSnapshot(t *testing.T) {
	snap := NewInmemSnapshotStore()

	// Check no snapshots
	snaps, err := snap.List()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(snaps) != 0 {
		t.Fatalf("did not expect any snapshots: %v", snaps)
	}

	// Create a new sink
	var configuration Configuration
	configuration.Servers = append(configuration.Servers, Server{
		Suffrage: Voter,
		ID:       ServerID("my id"),
		Address:  ServerAddress("over here"),
	})
	_, trans := NewInmemTransport(NewInmemAddr())
	sink, err := snap.Create(SnapshotVersionMax, 10, 3, configuration, 2, trans)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// The sink is not done, should not be in a list!
	snaps, err = snap.List()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(snaps) != 1 {
		t.Fatalf("should always be 1 snapshot: %v", snaps)
	}

	// Write to the sink
	_, err = sink.Write([]byte("first\n"))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	_, err = sink.Write([]byte("second\n"))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Done!
	err = sink.Close()
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Should have a snapshot!
	snaps, err = snap.List()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(snaps) != 1 {
		t.Fatalf("expect a snapshots: %v", snaps)
	}

	// Check the latest
	latest := snaps[0]
	if latest.Index != 10 {
		t.Fatalf("bad snapshot: %v", *latest)
	}
	if latest.Term != 3 {
		t.Fatalf("bad snapshot: %v", *latest)
	}
	if !reflect.DeepEqual(latest.Configuration, configuration) {
		t.Fatalf("bad snapshot: %v", *latest)
	}
	if latest.ConfigurationIndex != 2 {
		t.Fatalf("bad snapshot: %v", *latest)
	}
	if latest.Size != 13 {
		t.Fatalf("bad snapshot: %v", *latest)
	}

	// Read the snapshot
	_, r, err := snap.Open(latest.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Read out everything
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Ensure a match
	if bytes.Compare(buf.Bytes(), []byte("first\nsecond\n")) != 0 {
		t.Fatalf("content mismatch")
	}
}
