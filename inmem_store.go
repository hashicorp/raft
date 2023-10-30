// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"errors"
	"sync"
	"sync/atomic"
)

// InmemStore implements the LogStore and StableStore interface.
// It should NOT EVER be used for production. It is used only for
// unit tests. Use the MDBStore implementation instead.
type InmemStore struct {
	storeFail uint32 // accessed atomically as a bool 0/1

	// storeSem lets the test control exactly when s StoreLog(s) call takes
	// effect.
	storeSem chan struct{}

	l         sync.RWMutex
	lowIndex  uint64
	highIndex uint64
	logs      map[uint64]*Log
	kv        map[string][]byte
	kvInt     map[string]uint64
}

// NewInmemStore returns a new in-memory backend. Do not ever
// use for production. Only for testing.
func NewInmemStore() *InmemStore {
	i := &InmemStore{
		storeSem: make(chan struct{}, 1),
		logs:     make(map[uint64]*Log),
		kv:       make(map[string][]byte),
		kvInt:    make(map[string]uint64),
	}
	return i
}

// BlockStore will cause further calls to StoreLog(s) to block indefinitely
// until the returned cancel func is called. Note that if the code or test is
// buggy this could cause a deadlock
func (i *InmemStore) BlockStore() func() {
	i.storeSem <- struct{}{}
	cancelled := false
	return func() {
		// Allow multiple calls, subsequent ones are a no op
		if !cancelled {
			<-i.storeSem
			cancelled = true
		}
	}
}

// FailNext signals that the next call to StoreLog(s) should return an error
// without modifying the log contents. Subsequent calls will succeed again.
func (i *InmemStore) FailNext() {
	atomic.StoreUint32(&i.storeFail, 1)
}

// FirstIndex implements the LogStore interface.
func (i *InmemStore) FirstIndex() (uint64, error) {
	i.l.RLock()
	defer i.l.RUnlock()
	return i.lowIndex, nil
}

// LastIndex implements the LogStore interface.
func (i *InmemStore) LastIndex() (uint64, error) {
	i.l.RLock()
	defer i.l.RUnlock()
	return i.highIndex, nil
}

// GetLog implements the LogStore interface.
func (i *InmemStore) GetLog(index uint64, log *Log) error {
	i.l.RLock()
	defer i.l.RUnlock()
	l, ok := i.logs[index]
	if !ok {
		return ErrLogNotFound
	}
	*log = *l
	return nil
}

// StoreLog implements the LogStore interface.
func (i *InmemStore) StoreLog(log *Log) error {
	return i.StoreLogs([]*Log{log})
}

// StoreLogs implements the LogStore interface.
func (i *InmemStore) StoreLogs(logs []*Log) error {
	// Block waiting for the semaphore slot if BlockStore has been called. We must
	// do this before we take the lock because otherwise we'll block GetLog and
	// others too by holding the lock while blocked.
	i.storeSem <- struct{}{}
	defer func() {
		<-i.storeSem
	}()

	// Switch out fail if it is set so we only fail once
	shouldFail := atomic.SwapUint32(&i.storeFail, 0)
	if shouldFail == 1 {
		return errors.New("IO error")
	}

	i.l.Lock()
	defer i.l.Unlock()

	for _, l := range logs {
		i.logs[l.Index] = l
		if i.lowIndex == 0 {
			i.lowIndex = l.Index
		}
		if l.Index > i.highIndex {
			i.highIndex = l.Index
		}
	}
	return nil
}

// DeleteRange implements the LogStore interface.
func (i *InmemStore) DeleteRange(min, max uint64) error {
	i.l.Lock()
	defer i.l.Unlock()
	for j := min; j <= max; j++ {
		delete(i.logs, j)
	}
	if min <= i.lowIndex {
		i.lowIndex = max + 1
	}
	if max >= i.highIndex {
		i.highIndex = min - 1
	}
	if i.lowIndex > i.highIndex {
		i.lowIndex = 0
		i.highIndex = 0
	}
	return nil
}

// Set implements the StableStore interface.
func (i *InmemStore) Set(key []byte, val []byte) error {
	i.l.Lock()
	defer i.l.Unlock()
	i.kv[string(key)] = val
	return nil
}

// Get implements the StableStore interface.
func (i *InmemStore) Get(key []byte) ([]byte, error) {
	i.l.RLock()
	defer i.l.RUnlock()
	val := i.kv[string(key)]
	if val == nil {
		return nil, errors.New("not found")
	}
	return val, nil
}

// SetUint64 implements the StableStore interface.
func (i *InmemStore) SetUint64(key []byte, val uint64) error {
	i.l.Lock()
	defer i.l.Unlock()
	i.kvInt[string(key)] = val
	return nil
}

// GetUint64 implements the StableStore interface.
func (i *InmemStore) GetUint64(key []byte) (uint64, error) {
	i.l.RLock()
	defer i.l.RUnlock()
	return i.kvInt[string(key)], nil
}
