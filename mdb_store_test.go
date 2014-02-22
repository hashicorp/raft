package raft

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
)

func MDBTestStore(t *testing.T) (string, *MDBStore) {
	// Create a test dir
	dir, err := ioutil.TempDir("", "raft")
	if err != nil {
		t.Fatalf("err: %v ", err)
	}

	// New level
	store, err := NewMDBStore(dir)
	if err != nil {
		t.Fatalf("err: %v ", err)
	}

	return dir, store
}

func TestMDB_StableStore(t *testing.T) {
	var l interface{} = &MDBStore{}
	_, ok := l.(StableStore)
	if !ok {
		t.Fatalf("MDBStore is not StableStore")
	}
}

func TestMDB_SetGet(t *testing.T) {
	// Create a test dir
	dir, err := ioutil.TempDir("", "raft")
	if err != nil {
		t.Fatalf("err: %v ", err)
	}
	defer os.RemoveAll(dir)

	// New level
	l, err := NewMDBStore(dir)
	if err != nil {
		t.Fatalf("err: %v ", err)
	}
	defer l.Close()

	// Get a bad key
	key := []byte("foobar")
	_, err = l.Get(key)
	if err.Error() != "not found" {
		t.Fatalf("err: %v ", err)
	}

	val := []byte("this is a test value")
	if err := l.Set(key, val); err != nil {
		t.Fatalf("err: %v ", err)
	}

	out, err := l.Get(key)
	if err != nil {
		t.Fatalf("err: %v ", err)
	}

	if bytes.Compare(val, out) != 0 {
		t.Fatalf("did not get result back: %v %v", val, out)
	}
}

func TestMDB_SetGetUint64(t *testing.T) {
	// Create a test dir
	dir, err := ioutil.TempDir("", "raft")
	if err != nil {
		t.Fatalf("err: %v ", err)
	}
	defer os.RemoveAll(dir)

	// New level
	l, err := NewMDBStore(dir)
	if err != nil {
		t.Fatalf("err: %v ", err)
	}
	defer l.Close()

	// Get a bad key
	key := []byte("dolla bills")
	_, err = l.GetUint64(key)
	if err.Error() != "not found" {
		t.Fatalf("err: %v ", err)
	}

	var val uint64 = 42000
	if err := l.SetUint64(key, val); err != nil {
		t.Fatalf("err: %v ", err)
	}

	out, err := l.GetUint64(key)
	if err != nil {
		t.Fatalf("err: %v ", err)
	}

	if out != val {
		t.Fatalf("did not get result back: %v %v", val, out)
	}
}

func TestMDB_LogStore(t *testing.T) {
	var l interface{} = &MDBStore{}
	_, ok := l.(LogStore)
	if !ok {
		t.Fatalf("MDBStore is not a LogStore")
	}
}

func TestMDB_Logs(t *testing.T) {
	// Create a test dir
	dir, err := ioutil.TempDir("", "raft")
	if err != nil {
		t.Fatalf("err: %v ", err)
	}
	defer os.RemoveAll(dir)

	// New level
	l, err := NewMDBStore(dir)
	if err != nil {
		t.Fatalf("err: %v ", err)
	}
	defer l.Close()

	// Should be no first index
	idx, err := l.FirstIndex()
	if err != nil {
		t.Fatalf("err: %v ", err)
	}
	if idx != 0 {
		t.Fatalf("bad idx: %d", idx)
	}

	// Should be no last index
	idx, err = l.LastIndex()
	if err != nil {
		t.Fatalf("err: %v ", err)
	}
	if idx != 0 {
		t.Fatalf("bad idx: %d", idx)
	}

	// Try a filed fetch
	var out Log
	if err := l.GetLog(10, &out); err.Error() != "log not found" {
		t.Fatalf("err: %v ", err)
	}

	// Write out a log
	log := Log{
		Index: 1,
		Term:  1,
		Type:  LogCommand,
		Data:  []byte("first"),
	}
	for i := 1; i <= 10; i++ {
		log.Index = uint64(i)
		log.Term = uint64(i)
		if err := l.StoreLog(&log); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	// Attempt to write multiple logs
	var logs []*Log
	for i := 11; i <= 20; i++ {
		nl := &Log{
			Index: uint64(i),
			Term:  uint64(i),
			Type:  LogCommand,
			Data:  []byte("first"),
		}
		logs = append(logs, nl)
	}
	if err := l.StoreLogs(logs); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Try to fetch
	if err := l.GetLog(10, &out); err != nil {
		t.Fatalf("err: %v ", err)
	}

	// Try to fetch
	if err := l.GetLog(20, &out); err != nil {
		t.Fatalf("err: %v ", err)
	}

	// Check the lowest index
	idx, err = l.FirstIndex()
	if err != nil {
		t.Fatalf("err: %v ", err)
	}
	if idx != 1 {
		t.Fatalf("bad idx: %d", idx)
	}

	// Check the highest index
	idx, err = l.LastIndex()
	if err != nil {
		t.Fatalf("err: %v ", err)
	}
	if idx != 20 {
		t.Fatalf("bad idx: %d", idx)
	}

	// Delete a suffix
	if err := l.DeleteRange(5, 20); err != nil {
		t.Fatalf("err: %v ", err)
	}

	// Index should be one
	idx, err = l.FirstIndex()
	if err != nil {
		t.Fatalf("err: %v ", err)
	}
	if idx != 1 {
		t.Fatalf("bad idx: %d", idx)
	}
	idx, err = l.LastIndex()
	if err != nil {
		t.Fatalf("err: %v ", err)
	}
	if idx != 4 {
		t.Fatalf("bad idx: %d", idx)
	}

	// Should not be able to fetch
	if err := l.GetLog(5, &out); err.Error() != "log not found" {
		t.Fatalf("err: %v ", err)
	}
}
