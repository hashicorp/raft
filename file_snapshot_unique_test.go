package raft

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"
)

// reserveCollisionWindow creates owned snapshot directories around the clock
// used by snapshotName, so the test does not depend on winning a nanosecond
// scheduling race between two calls to Create.
func reserveCollisionWindow(t *testing.T, store *FileSnapshotStore, term, index uint64, completed bool) map[string]string {
	t.Helper()
	paths := make(map[string]string)
	first := time.Now().UnixNano()/int64(time.Millisecond) - 5
	const window = 400
	for ms := first; ms < first+window; ms++ {
		name := fmt.Sprintf("%d-%d-%d", term, index, ms)
		path := filepath.Join(store.path, name)
		if !completed {
			path += tmpSuffix
		}
		if err := os.Mkdir(path, 0o755); err != nil {
			t.Fatal(err)
		}
		state := filepath.Join(path, stateFilePath)
		marker := fmt.Sprintf("existing-state-%d", ms)
		if err := os.WriteFile(state, []byte(marker), 0o600); err != nil {
			t.Fatal(err)
		}
		paths[state] = marker
	}
	return paths
}

func verifyReservedSnapshotState(t *testing.T, paths map[string]string) {
	t.Helper()
	for path, expected := range paths {
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("snapshot state %q disappeared: %v", path, err)
		}
		if string(contents) != expected {
			t.Fatalf("snapshot state %q changed: %q, want %q", path, contents, expected)
		}
	}
}

func testExclusiveSnapshotCreate(t *testing.T, completed bool) {
	t.Helper()
	base := t.TempDir()
	store, err := NewFileSnapshotStoreWithLogger(base, 3, newTestLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	const term, index = uint64(5), uint64(42)
	reserved := reserveCollisionWindow(t, store, term, index, completed)

	_, transport := NewInmemTransport(NewInmemAddr())
	sink, err := store.Create(SnapshotVersionMax, index, term, Configuration{}, 0, transport)
	if err != nil {
		t.Fatalf("Create during ID collision = %v", err)
	}
	if !regexp.MustCompile(`^5-42-[0-9]+$`).MatchString(sink.ID()) {
		t.Fatalf("snapshot ID format changed: %q", sink.ID())
	}
	state := filepath.Join(store.path, sink.ID(), stateFilePath)
	if !completed {
		state = filepath.Join(store.path, sink.ID()+tmpSuffix, stateFilePath)
	}
	if _, exists := reserved[state]; exists {
		t.Fatalf("snapshot sink reused existing reserved state file %q", state)
	}
	if _, err := sink.Write([]byte("new-snapshot")); err != nil {
		t.Fatal(err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("failed to finish uniquely reserved snapshot: %v", err)
	}
	meta, reader, err := store.Open(sink.ID())
	if err != nil {
		t.Fatalf("cannot open newly created snapshot: %v", err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "new-snapshot" || meta.Index != index || meta.Term != term {
		t.Fatalf("restored snapshot = %q, metadata %v", data, meta)
	}
	verifyReservedSnapshotState(t, reserved)
}

func TestFileSS_CreateDoesNotTruncateActiveCollision(t *testing.T) {
	testExclusiveSnapshotCreate(t, false)
}

func TestFileSS_CreateDoesNotReuseFinalizedID(t *testing.T) {
	testExclusiveSnapshotCreate(t, true)
}
