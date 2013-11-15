package raft

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	snapPath      = "snapshots"
	metaFilePath  = "meta.json"
	stateFilePath = "state.bin"
	tmpSuffix     = ".tmp"
)

// FileSnapshotStore implements the SnapshotStore interface and allows
// snapshots to be made on the local disk.
type FileSnapshotStore struct {
	path   string
	retain int
}

// Implements the SnapshotSink
type FileSnapshotSink struct {
	dir  string
	meta fileSnapshotMeta

	stateFile *os.File
	stateHash hash.Hash64
	buffered  *bufio.Writer
}

type fileSnapshotMeta struct {
	SnapshotMeta
	CRC []byte
}

// NewFileSnapshotStore creates a new FileSnapshotStore based
// on a base directory. The `retain` parameter controls how many
// snapshots are retained. Must be at least 1.
func NewFileSnapshotStore(base string, retain int) (*FileSnapshotStore, error) {
	if retain < 1 {
		return nil, fmt.Errorf("must retain at least one snapshot")
	}

	path := filepath.Join(base, snapPath)
	store := &FileSnapshotStore{
		path:   path,
		retain: retain,
	}
	return store, nil
}

// Create is used to start a new snapshot
func (f *FileSnapshotStore) Create(index, term uint64, peers []byte) (SnapshotSink, error) {
	// Create a new path
	name := time.Now().Format(time.RFC3339) + tmpSuffix
	path := filepath.Join(f.path, name)
	log.Printf("[INFO] Creating new snapshot at %s", path)

	// Make the directory
	if err := os.Mkdir(path, 0755); err != nil {
		log.Printf("[ERR] Failed to make snapshot directory: %v", err)
		return nil, err
	}

	// Create the sink
	sink := &FileSnapshotSink{
		dir: path,
		meta: fileSnapshotMeta{
			SnapshotMeta: SnapshotMeta{
				ID:    path,
				Index: index,
				Term:  term,
				Peers: peers,
			},
			CRC: nil,
		},
	}

	// Write out the meta data
	if err := sink.writeMeta(); err != nil {
		log.Printf("[ERR] Failed to write metadata: %v", err)
		return nil, err
	}

	// Open the state file
	statePath := filepath.Join(path, stateFilePath)
	fh, err := os.Create(statePath)
	if err != nil {
		log.Printf("[ERR] Failed to create state file: %v", err)
		return nil, err
	}
	sink.stateFile = fh

	// Create a CRC64 hash
	sink.stateHash = crc64.New(crc64.MakeTable(crc64.ECMA))

	// Wrap both the hash and file in a MultiWriter with buffering
	multi := io.MultiWriter(sink.stateFile, sink.stateHash)
	sink.buffered = bufio.NewWriter(multi)

	// Done
	return sink, nil
}

func (f *FileSnapshotStore) List() ([]*SnapshotMeta, error) {
	// Get the eligible snapshots
	snapshots, err := ioutil.ReadDir(f.path)
	if err != nil {
		log.Printf("[ERR] Failed to scan snapshot dir: %v", err)
		return nil, err
	}

	// Populate the metadata, reverse order (newest first)
	var snapMeta []*SnapshotMeta
	for i := len(snapshots) - 1; i >= 0; i++ {
		// Ignore any files
		if !snapshots[i].IsDir() {
			continue
		}

		// Ignore any temporary snapshots
		dirName := snapshots[i].Name()
		if strings.HasSuffix(dirName, tmpSuffix) {
			log.Printf("[WARN] Found temporary snapshot: %v", dirName)
			continue
		}

		// Try to read the meta data
		meta, err := f.readMeta(dirName)
		if err != nil {
			log.Printf("[WARN] Failed to read metadata for %v: %v", dirName, err)
			continue
		}

		snapMeta = append(snapMeta, meta)
	}

	return snapMeta, nil
}

// readMeta is used to read the meta data for a given named backup
func (f *FileSnapshotStore) readMeta(name string) (*SnapshotMeta, error) {
	// Open the meta file
	metaPath := filepath.Join(f.path, name, metaFilePath)
	fh, err := os.Open(metaPath)
	if err != nil {
		return nil, err
	}
	defer fh.Close()

	// Buffer the file IO
	buffered := bufio.NewReader(fh)

	// Read in the JSON
	meta := &SnapshotMeta{}
	dec := json.NewDecoder(buffered)
	if err := dec.Decode(meta); err != nil {
		return nil, err
	}
	return meta, nil
}

func (f *FileSnapshotStore) Open(id string) (io.ReadCloser, error) {
	return nil, nil
}

// Write is used to append to the state file. We write to the
// buffered IO object to reduce the amount of context switches
func (s *FileSnapshotSink) Write(b []byte) (int, error) {
	return s.buffered.Write(b)
}

// Close is used to indicate a successful end
func (s *FileSnapshotSink) Close() error {
	// Close the open handles
	if err := s.finalize(); err != nil {
		log.Printf("[ERR] Failed to finalize snapshot: %v", err)
		return err
	}

	// Write out the meta data
	if err := s.writeMeta(); err != nil {
		log.Printf("[ERR] Failed to write metadata: %v", err)
		return err
	}

	// Move the directory into place
	newPath := strings.TrimSuffix(s.dir, tmpSuffix)
	if err := os.Rename(s.dir, newPath); err != nil {
		log.Printf("[ERR] Failed to move snapshot into place: %v", err)
		return err
	}

	// TODO: Retain count
	return nil
}

// Cancel is used to indicate an unsuccessful end
func (s *FileSnapshotSink) Cancel() error {
	// Close the open handles
	if err := s.finalize(); err != nil {
		log.Printf("[ERR] Failed to finalize snapshot: %v", err)
		return err
	}

	// Attempt to remove all artifacts
	return os.RemoveAll(s.dir)
}

// finalize is used to close all of our resources
func (s *FileSnapshotSink) finalize() error {
	if err := s.buffered.Flush(); err != nil {
		return err
	}
	if err := s.stateFile.Close(); err != nil {
		return err
	}
	s.meta.CRC = s.stateHash.Sum(nil)
	return nil
}

// writeMeta is used to write out the metadata we have
func (s *FileSnapshotSink) writeMeta() error {
	// Open the meta file
	metaPath := filepath.Join(s.dir, metaFilePath)
	fh, err := os.Create(metaPath)
	if err != nil {
		return err
	}
	defer fh.Close()

	// Buffer the file IO
	buffered := bufio.NewWriter(fh)
	defer buffered.Flush()

	// Write out as JSON
	enc := json.NewEncoder(buffered)
	if err := enc.Encode(&s.meta); err != nil {
		return err
	}
	return nil
}
