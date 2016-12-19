package raft

import (
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// FileStore implements the LogStore and StableStore interface.
type FileStore struct {
	l         sync.RWMutex
	lowIndex  uint64
	highIndex uint64
	dir       string
}

type uint64Slice []uint64

func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(filepath.Join(dir, "logs"), 0700); err != nil {
		return nil, err
	}

	if err := os.MkdirAll(filepath.Join(dir, "stable"), 0700); err != nil {
		return nil, err
	}

	return &FileStore{
		dir: dir,
	}, nil
}

// GetAll returns all indexes that are currently present in the log store. This
// is NOT part of the LogStore interface — we use it when snapshotting.
func (s *FileStore) GetAll() ([]uint64, error) {
	var indexes []uint64
	dir, err := os.Open(filepath.Join(s.dir, "logs"))
	if err != nil {
		return indexes, err
	}
	defer dir.Close()

	names, err := dir.Readdirnames(-1)
	if err != nil {
		return indexes, err
	}

	for _, name := range names {
		if !strings.HasPrefix(name, "entry.") {
			continue
		}

		dot := strings.LastIndex(name, ".")
		if dot == -1 {
			continue
		}

		index, err := strconv.ParseInt(name[dot+1:], 0, 64)
		if err != nil {
			return indexes, fmt.Errorf("Unexpected filename, does not confirm to entry.%%d: %q. Parse error: %v", name, err)
		}

		indexes = append(indexes, uint64(index))
	}

	sort.Sort(uint64Slice(indexes))

	return indexes, nil
}

// FirstIndex implements the LogStore interface.
func (s *FileStore) FirstIndex() (uint64, error) {
	s.l.RLock()
	defer s.l.RUnlock()
	return s.lowIndex, nil
}

// LastIndex implements the LogStore interface.
func (s *FileStore) LastIndex() (uint64, error) {
	s.l.RLock()
	defer s.l.RUnlock()
	return s.highIndex, nil
}

// GetLog implements the LogStore interface.
func (s *FileStore) GetLog(index uint64, rlog *Log) error {
	s.l.RLock()
	defer s.l.RUnlock()
	f, err := os.Open(filepath.Join(s.dir, "logs/entry."+strconv.FormatUint(index, 10)))
	if err != nil {
		if os.IsNotExist(err) {
			return ErrLogNotFound
		}
		return err
	}
	defer f.Close()

	var elog Log
	if err := gob.NewDecoder(f).Decode(&elog); err != nil {
		return err
	}
	*rlog = elog
	return nil
}

// StoreLog implements the LogStore interface.
func (s *FileStore) StoreLog(log *Log) error {
	return s.StoreLogs([]*Log{log})
}

// StoreLogs implements the LogStore interface.
func (s *FileStore) StoreLogs(logs []*Log) error {
	s.l.Lock()
	defer s.l.Unlock()

	for _, entry := range logs {
		f, err := os.Create(filepath.Join(s.dir, "logs/entry."+strconv.FormatUint(entry.Index, 10)))
		if err != nil {
			return err
		}
		if err := gob.NewEncoder(f).Encode(entry); err != nil {
			f.Close()
			return err
		}
		if entry.Index < s.lowIndex || s.lowIndex == 0 {
			s.lowIndex = entry.Index
		}
		if entry.Index > s.highIndex {
			s.highIndex = entry.Index
		}
		f.Close()
	}

	return nil
}

// DeleteRange implements the LogStore interface.
func (s *FileStore) DeleteRange(min, max uint64) error {
	s.l.Lock()
	defer s.l.Unlock()
	for i := min; i <= max; i++ {
		if err := os.Remove(filepath.Join(s.dir, "logs/entry."+strconv.FormatUint(i, 10))); err != nil {
			return err
		}
	}
	s.lowIndex = max + 1
	return nil
}

func (s *FileStore) DeleteAll() error {
	s.l.Lock()
	defer s.l.Unlock()
	if err := os.RemoveAll(filepath.Join(s.dir, "logs")); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(s.dir, "logs"), 0700); err != nil {
		return err
	}
	s.lowIndex = 0
	s.highIndex = 0
	return nil
}

// Set implements the StableStore interface.
func (s *FileStore) Set(key []byte, val []byte) error {
	s.l.Lock()
	defer s.l.Unlock()
	return ioutil.WriteFile(filepath.Join(s.dir, "stable", "key."+string(key)), val, 0600)
}

// Get implements the StableStore interface.
func (s *FileStore) Get(key []byte) ([]byte, error) {
	s.l.RLock()
	defer s.l.RUnlock()
	b, err := ioutil.ReadFile(filepath.Join(s.dir, "stable", "key."+string(key)))
	if err != nil && os.IsNotExist(err) {
		return []byte{}, fmt.Errorf("not found")
	}
	return b, err
}

// SetUint64 implements the StableStore interface.
func (s *FileStore) SetUint64(key []byte, val uint64) error {
	return s.Set(key, []byte(strconv.FormatUint(val, 10)))
}

// GetUint64 implements the StableStore interface.
func (s *FileStore) GetUint64(key []byte) (uint64, error) {
	b, err := s.Get(key)
	if err != nil {
		return 0, err
	}
	i, err := strconv.ParseInt(string(b), 0, 64)
	if err != nil {
		return 0, err
	}
	return uint64(i), nil
}
