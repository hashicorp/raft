package raft

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"path/filepath"
)

var dbPath = "state.db"

type namedQuery uint8

const (
	confSetKVStr namedQuery = iota
	confGetKVStr
	confSetKVInt
	confGetKVInt
	logsFirstIndex
	logsLastIndex
	logsGet
	logsStore
	logsDelete
)

var (
	notFound = fmt.Errorf("not found")
)

// SQLiteStore implements the LogStore and StableStore
// interfaces using a sqlite db
type SQLiteStore struct {
	db       *sql.DB
	prepared map[namedQuery]*sql.Stmt
}

// NewSQLite store creates a new sqlite store and potential
// error. Requires a base directory from which to operate.
func NewSQLiteStore(base string) (*SQLiteStore, error) {
	// Get the paths
	dbLoc := filepath.Join(base, dbPath)

	// Open the db
	db, err := sql.Open("sqlite3", dbLoc)
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %v", err)
	}

	store := &SQLiteStore{
		db:       db,
		prepared: make(map[namedQuery]*sql.Stmt),
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

	// Prepare the queries
	queries := map[namedQuery]string{
		confSetKVStr:   "INSERT OR REPLACE INTO kv_str(idx, data) VALUES(?, ?)",
		confGetKVStr:   "SELECT data FROM kv_str WHERE idx=?",
		confSetKVInt:   "INSERT OR REPLACE INTO kv_int(idx, data) VALUES(?, ?)",
		confGetKVInt:   "SELECT data FROM kv_int WHERE idx=?",
		logsFirstIndex: "SELECT idx FROM logs ORDER BY idx ASC LIMIT 1",
		logsLastIndex:  "SELECT idx FROM logs ORDER BY idx DESC LIMIT 1",
		logsGet:        "SELECT * FROM logs WHERE idx=?",
		logsStore:      "INSERT INTO logs VALUES (?,?,?,?)",
		logsDelete:     "DELETE FROM logs WHERE idx >=? and idx <=?",
	}
	for name, query := range queries {
		stmt, err := s.db.Prepare(query)
		if err != nil {
			return fmt.Errorf("Failed to prepare '%s': %v", query, err)
		}
		s.prepared[name] = stmt
	}
	return nil
}

func (s *SQLiteStore) Set(key []byte, val []byte) error {
	stmt := s.prepared[confSetKVStr]
	res, err := stmt.Exec(key, val)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("Failed to set key: %s", key)
	}
	return nil
}

func (s *SQLiteStore) Get(key []byte) ([]byte, error) {
	stmt := s.prepared[confGetKVStr]
	row := stmt.QueryRow(key)
	var val []byte
	err := row.Scan(&val)
	if err == sql.ErrNoRows {
		return nil, notFound
	}
	return val, err
}

func (s *SQLiteStore) SetUint64(key []byte, val uint64) error {
	stmt := s.prepared[confSetKVInt]
	res, err := stmt.Exec(key, val)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("Failed to set key: %s", key)
	}
	return nil
}

func (s *SQLiteStore) GetUint64(key []byte) (uint64, error) {
	stmt := s.prepared[confGetKVInt]
	row := stmt.QueryRow(key)
	var val uint64
	err := row.Scan(&val)
	if err == sql.ErrNoRows {
		return 0, notFound
	}
	return val, err
}

func (s *SQLiteStore) FirstIndex() (uint64, error) {
	stmt := s.prepared[logsFirstIndex]
	row := stmt.QueryRow()
	var val uint64
	err := row.Scan(&val)
	if err == sql.ErrNoRows {
		err = nil
	}
	return val, err
}

func (s *SQLiteStore) LastIndex() (uint64, error) {
	stmt := s.prepared[logsLastIndex]
	row := stmt.QueryRow()
	var val uint64
	err := row.Scan(&val)
	if err == sql.ErrNoRows {
		err = nil
	}
	return val, err
}

func (s *SQLiteStore) GetLog(index uint64, log *Log) error {
	stmt := s.prepared[logsGet]
	row := stmt.QueryRow(index)
	err := row.Scan(&log.Index, &log.Term, &log.Type, &log.Data)
	if err == sql.ErrNoRows {
		err = LogNotFound
	}
	return err
}

func (s *SQLiteStore) StoreLog(log *Log) error {
	stmt := s.prepared[logsStore]
	res, err := stmt.Exec(log.Index, log.Term, log.Type, log.Data)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("Failed to insert log: %#v", *log)
	}
	return nil
}

func (s *SQLiteStore) DeleteRange(min, max uint64) error {
	stmt := s.prepared[logsDelete]
	res, err := stmt.Exec(min, max)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("Failed to delete any logs between: %d %d", min, max)
	}
	return nil
}
