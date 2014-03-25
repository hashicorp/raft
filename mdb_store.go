package raft

import (
	"fmt"
	"github.com/armon/gomdb"
	"os"
	"path/filepath"
)

const (
	dbLogs       = "logs"
	dbConf       = "conf"
	dbMaxMapSize = 128 * 1024 * 1024 // 128MB default max map size
)

// Sub-dir used for MDB
var mdbPath = "mdb/"

// MDBStore provides an implementation of LogStore and StableStore,
// all backed by a single MDB database.
type MDBStore struct {
	env     *mdb.Env
	path    string
	maxSize uint64
}

// NewMDBStore returns a new MDBStore and potential
// error. Requres a base directory from which to operate.
// Uses the default maximum size.
func NewMDBStore(base string) (*MDBStore, error) {
	return NewMDBStoreWithSize(base, 0)
}

// NewMDBStore returns a new MDBStore and potential
// error. Requres a base directory from which to operate,
// and a maximum size. If maxSize is not 0, a default value is used.
func NewMDBStoreWithSize(base string, maxSize uint64) (*MDBStore, error) {
	// Get the paths
	path := filepath.Join(base, mdbPath)
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	// Set the maxSize if not given
	if maxSize == 0 {
		maxSize = dbMaxMapSize
	}

	// Create the env
	env, err := mdb.NewEnv()
	if err != nil {
		return nil, err
	}

	// Create the struct
	store := &MDBStore{
		env:     env,
		path:    path,
		maxSize: maxSize,
	}

	// Initialize the db
	if err := store.initialize(); err != nil {
		env.Close()
		return nil, err
	}
	return store, nil
}

// initialize is used to setup the mdb store
func (m *MDBStore) initialize() error {
	// Allow up to 16 sub-dbs
	if err := m.env.SetMaxDBs(mdb.DBI(16)); err != nil {
		return err
	}

	// Increase the maximum map size
	if err := m.env.SetMapSize(m.maxSize); err != nil {
		return err
	}

	// Open the DB
	if err := m.env.Open(m.path, mdb.NOTLS, 0755); err != nil {
		return err
	}

	// Create all the tables
	tx, _, err := m.startTxn(false, dbLogs, dbConf)
	if err != nil {
		tx.Abort()
		return err
	}
	return tx.Commit()
}

// Close is used to gracefully shutdown the MDB store
func (m *MDBStore) Close() error {
	m.env.Close()
	return nil
}

// startTxn is used to start a transaction and open all the associated sub-databases
func (m *MDBStore) startTxn(readonly bool, open ...string) (*mdb.Txn, []mdb.DBI, error) {
	var txFlags uint = 0
	var dbFlags uint = 0
	if readonly {
		txFlags |= mdb.RDONLY
	} else {
		dbFlags |= mdb.CREATE
	}

	tx, err := m.env.BeginTxn(nil, txFlags)
	if err != nil {
		return nil, nil, err
	}

	var dbs []mdb.DBI
	for _, name := range open {
		dbi, err := tx.DBIOpen(name, dbFlags)
		if err != nil {
			tx.Abort()
			return nil, nil, err
		}
		dbs = append(dbs, dbi)
	}

	return tx, dbs, nil
}

func (m *MDBStore) FirstIndex() (uint64, error) {
	tx, dbis, err := m.startTxn(true, dbLogs)
	if err != nil {
		return 0, err
	}
	defer tx.Abort()

	cursor, err := tx.CursorOpen(dbis[0])
	if err != nil {
		return 0, err
	}

	key, _, err := cursor.Get(nil, mdb.FIRST)
	if err == mdb.NotFound {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	// Convert the key to the index
	return bytesToUint64(key), nil
}

func (m *MDBStore) LastIndex() (uint64, error) {
	tx, dbis, err := m.startTxn(true, dbLogs)
	if err != nil {
		return 0, err
	}
	defer tx.Abort()

	cursor, err := tx.CursorOpen(dbis[0])
	if err != nil {
		return 0, err
	}

	key, _, err := cursor.Get(nil, mdb.LAST)
	if err == mdb.NotFound {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	// Convert the key to the index
	return bytesToUint64(key), nil
}

// Gets a log entry at a given index
func (m *MDBStore) GetLog(index uint64, logOut *Log) error {
	key := uint64ToBytes(index)

	tx, dbis, err := m.startTxn(true, dbLogs)
	if err != nil {
		return err
	}
	defer tx.Abort()

	val, err := tx.Get(dbis[0], key)
	if err == mdb.NotFound {
		return LogNotFound
	} else if err != nil {
		return err
	}

	// Convert the value to a log
	return decodeMsgPack(val, logOut)
}

// Stores a log entry
func (m *MDBStore) StoreLog(log *Log) error {
	return m.StoreLogs([]*Log{log})
}

// Stores multiple log entries
func (m *MDBStore) StoreLogs(logs []*Log) error {
	// Start write txn
	tx, dbis, err := m.startTxn(false, dbLogs)
	if err != nil {
		return err
	}

	for _, log := range logs {
		// Convert to an on-disk format
		key := uint64ToBytes(log.Index)
		val, err := encodeMsgPack(log)
		if err != nil {
			tx.Abort()
			return err
		}

		// Write to the table
		if err := tx.Put(dbis[0], key, val.Bytes(), 0); err != nil {
			tx.Abort()
			return err
		}
	}
	return tx.Commit()
}

// Deletes a range of log entries. The range is inclusive.
func (m *MDBStore) DeleteRange(minIdx, maxIdx uint64) error {
	// Start write txn
	tx, dbis, err := m.startTxn(false, dbLogs)
	if err != nil {
		return err
	}

	// Open a cursor
	cursor, err := tx.CursorOpen(dbis[0])
	if err != nil {
		tx.Abort()
		return err
	}

	var key []byte
	didDelete := false
	for {
		if didDelete {
			key, _, err = cursor.Get(nil, 0)
			didDelete = false
		} else {
			key, _, err = cursor.Get(nil, mdb.NEXT)
		}
		if err == mdb.NotFound {
			break
		} else if err != nil {
			tx.Abort()
			return err
		}

		// Check if the key is in the range
		keyVal := bytesToUint64(key)
		if keyVal < minIdx {
			continue
		}
		if keyVal > maxIdx {
			break
		}

		// Attempt delete
		if err := cursor.Del(0); err != nil {
			tx.Abort()
			return err
		}
		didDelete = true
	}
	return tx.Commit()
}

// Set a K/V pair
func (m *MDBStore) Set(key []byte, val []byte) error {
	// Start write txn
	tx, dbis, err := m.startTxn(false, dbConf)
	if err != nil {
		return err
	}

	if err := tx.Put(dbis[0], key, val, 0); err != nil {
		tx.Abort()
		return err
	}
	return tx.Commit()
}

// Get a K/V pair
func (m *MDBStore) Get(key []byte) ([]byte, error) {
	// Start read txn
	tx, dbis, err := m.startTxn(true, dbConf)
	if err != nil {
		return nil, err
	}
	defer tx.Abort()

	val, err := tx.Get(dbis[0], key)
	if err == mdb.NotFound {
		return nil, fmt.Errorf("not found")
	} else if err != nil {
		return nil, err
	}
	return sliceCopy(val), nil
}

func (m *MDBStore) SetUint64(key []byte, val uint64) error {
	return m.Set(key, uint64ToBytes(val))
}

func (m *MDBStore) GetUint64(key []byte) (uint64, error) {
	buf, err := m.Get(key)
	if err != nil {
		return 0, err
	}
	return bytesToUint64(buf), nil
}

func sliceCopy(inp []byte) []byte {
	c := make([]byte, len(inp))
	copy(c, inp)
	return c
}
