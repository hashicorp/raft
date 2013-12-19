package raft

import (
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"log"
	"path/filepath"
)

const logPath = "logs"
const confPath = "conf"
const maxOpenFiles = 128

type levelDBStore struct {
	db *leveldb.DB
}

func (l *levelDBStore) Close() error {
	return l.db.Close()
}

// LevelDBLogStore provides an implementation of LogStore
type LevelDBLogStore struct {
	levelDBStore
}

// LevelDBStableStore provides an implementation of StableStore
type LevelDBStableStore struct {
	levelDBStore
}

// newLevelDBStore is used to initialize a levelDB store
func newLevelDBStore(path string, store *levelDBStore) error {
	// LevelDB options
	opts := &opt.Options{
		Compression:  opt.SnappyCompression,
		MaxOpenFiles: maxOpenFiles,
		Strict:       opt.StrictAll,
	}

	// Open the DBs
	db, err := leveldb.OpenFile(path, opts)
	if err != nil {
		log.Printf("[ERR] Failed to open leveldb at %v: %v", path, err)
		return err
	}
	store.db = db
	return nil
}

// NewLevelDBLogStore returns a new LevelDBLogStore and potential
// error. Requres a base directory from which to operate.
func NewLevelDBLogStore(base string) (*LevelDBLogStore, error) {
	// Get the paths
	logLoc := filepath.Join(base, logPath)

	// Create the struct
	ldb := &LevelDBLogStore{}

	// Initialize the db
	if err := newLevelDBStore(logLoc, &ldb.levelDBStore); err != nil {
		return nil, err
	}
	return ldb, nil
}

func (l *LevelDBLogStore) FirstIndex() (uint64, error) {
	// Get an iterator
	it := l.db.NewIterator(nil)
	defer it.Release()

	// Seek to the first value
	it.First()

	// Check if there is a key
	key := it.Key()
	if key == nil {
		// Nothing written yet
		return 0, it.Error()
	}

	// Convert the key to the index
	return bytesToUint64(key), it.Error()
}

func (l *LevelDBLogStore) LastIndex() (uint64, error) {
	// Get an iterator
	it := l.db.NewIterator(nil)
	defer it.Release()

	// Seek to the last value
	it.Last()

	// Check if there is a key
	key := it.Key()
	if key == nil {
		// Nothing written yet
		return 0, it.Error()
	}

	// Convert the key to the index
	return bytesToUint64(key), it.Error()
}

// Gets a log entry at a given index
func (l *LevelDBLogStore) GetLog(index uint64, logOut *Log) error {
	key := uint64ToBytes(index)

	// Get an iterator
	snap, err := l.db.GetSnapshot()
	if err != nil {
		return err
	}
	defer snap.Release()

	// Look for the key
	val, err := snap.Get(key, nil)
	if err == leveldb.ErrNotFound {
		return LogNotFound
	} else if err != nil {
		return err
	}

	// Convert the value to a log
	return decodeMsgPack(val, logOut)
}

// Stores a log entry
func (l *LevelDBLogStore) StoreLog(log *Log) error {
	// Convert to an on-disk format
	key := uint64ToBytes(log.Index)
	val, err := encodeMsgPack(log)
	if err != nil {
		return err
	}

	// Write it out
	opts := &opt.WriteOptions{Sync: true}
	return l.db.Put(key, val.Bytes(), opts)
}

// Deletes a range of log entries. The range is inclusive.
func (l *LevelDBLogStore) DeleteRange(minIdx, maxIdx uint64) error {
	// Get lower and upper index bounds
	firstIdx, err := l.FirstIndex()
	if err != nil {
		return err
	}
	lastIdx, err := l.LastIndex()
	if err != nil {
		return err
	}

	// Optimize the index ranges
	minIdx = max(minIdx, firstIdx)
	maxIdx = min(maxIdx, lastIdx)

	// Create a batch operation
	batch := &leveldb.Batch{}
	for i := minIdx; i <= maxIdx; i++ {
		key := uint64ToBytes(i)
		batch.Delete(key)
	}

	// Apply the batch
	opts := &opt.WriteOptions{Sync: true}
	return l.db.Write(batch, opts)
}

// NewLevelDBStableStore returns a new LevelDBStableStore and potential
// error. Requres a base directory from which to operate.
func NewLevelDBStableStore(base string) (*LevelDBStableStore, error) {
	// Get the paths
	confLoc := filepath.Join(base, confPath)

	// Create the struct
	ldb := &LevelDBStableStore{}

	// Initialize the db
	if err := newLevelDBStore(confLoc, &ldb.levelDBStore); err != nil {
		return nil, err
	}
	return ldb, nil
}

// Set a K/V pair
func (l *LevelDBStableStore) Set(key []byte, val []byte) error {
	opts := &opt.WriteOptions{Sync: true}
	return l.db.Put(key, val, opts)
}

// Get a K/V pair
func (l *LevelDBStableStore) Get(key []byte) ([]byte, error) {
	// Get a snapshot view
	snap, err := l.db.GetSnapshot()
	if err != nil {
		return nil, err
	}
	defer snap.Release()

	// Look for the key
	val, err := snap.Get(key, nil)
	if err == leveldb.ErrNotFound {
		return nil, fmt.Errorf("not found")
	} else if err != nil {
		return nil, err
	}

	// Copy it to a new buffer
	buf := make([]byte, len(val))
	copy(buf, val)
	return buf, nil
}

func (l *LevelDBStableStore) SetUint64(key []byte, val uint64) error {
	return l.Set(key, uint64ToBytes(val))
}

func (l *LevelDBStableStore) GetUint64(key []byte) (uint64, error) {
	buf, err := l.Get(key)
	if err != nil {
		return 0, err
	}
	return bytesToUint64(buf), nil
}
