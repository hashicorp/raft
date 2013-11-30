package raft

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
)

var dbPath = "state.db"

// SQLiteStore implements the LogStore and StableStore
// interfaces using a sqlite db
type SQLiteStore struct {
	db *sql.DB
}

// NewSQLite store creates a new sqlite store and potential
// error. Requires a base directory from which to operate.
func NewSQLiteStore(base string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite3", "./foo.db")
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %v", err)
	}

	store := &SQLiteStore{
		db: db,
	}

	// Ensure we can initialize
	if err := store.initialize(); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

// Close is used to safely shutdown the sqlite store
func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

// initialize is used to setup the sqlite store for use
func (s *SQLiteStore) initialize() error {
	// Set the pragma first
	pragmas := []string{
		"pragma locking_mode=exclusive;",
		"pragma journal_mode=wal;",
	}
	for _, p := range pragmas {
		if _, err := s.db.Exec(p); err != nil {
			return fmt.Errorf("Failed to set '%s': %v", p, err)
		}
	}

	// Create the tables
	tables := []string{
		`CREATE TABLE IF NOT EXISTS logs (idx integer primary key,
term integer, type integer, data blob);`,
		`CREATE TABLE IF NOT EXISTS kv_str (idx text primary key,
data blob);`,
		`CREATE TABLE IF NOT EXISTS kv_int (idx text primary key,
data integer);`,
	}
	for _, t := range tables {
		if _, err := s.db.Exec(t); err != nil {
			return fmt.Errorf("Failed to call '%s': %v", t, err)
		}
	}
	return nil
}

func (s *SQLiteStore) Set(key []byte, val []byte) error {
	return nil
}

func (s *SQLiteStore) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (s *SQLiteStore) SetUint64(key []byte, val uint64) error {
	return nil
}

func (s *SQLiteStore) GetUint64(key []byte) (uint64, error) {
	return 0, nil
}

func (s *SQLiteStore) FirstIndex() (uint64, error) {
	return 0, nil
}

func (s *SQLiteStore) LastIndex() (uint64, error) {
	return 0, nil
}

func (s *SQLiteStore) GetLog(index uint64, log *Log) error {
	return nil
}

func (s *SQLiteStore) StoreLog(log *Log) error {
	return nil
}

func (s *SQLiteStore) DeleteRange(min, max uint64) error {
	return nil
}
