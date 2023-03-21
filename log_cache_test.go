// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"errors"
	"strings"
	"sync"
	"testing"
)

func TestLogCache(t *testing.T) {
	store := NewInmemStore()
	c, _ := NewLogCache(16, store)

	// Insert into the in-mem store
	for i := 0; i < 32; i++ {
		log := &Log{Index: uint64(i) + 1}
		store.StoreLog(log)
	}

	// Check the indexes
	if idx, _ := c.FirstIndex(); idx != 1 {
		t.Fatalf("bad: %d", idx)
	}
	if idx, _ := c.LastIndex(); idx != 32 {
		t.Fatalf("bad: %d", idx)
	}

	// Try get log with a miss
	var out Log
	err := c.GetLog(1, &out)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out.Index != 1 {
		t.Fatalf("bad: %#v", out)
	}

	// Store logs
	l1 := &Log{Index: 33}
	l2 := &Log{Index: 34}
	err = c.StoreLogs([]*Log{l1, l2})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if idx, _ := c.LastIndex(); idx != 34 {
		t.Fatalf("bad: %d", idx)
	}

	// Check that it wrote-through
	err = store.GetLog(33, &out)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = store.GetLog(34, &out)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Delete in the backend
	err = store.DeleteRange(33, 34)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Should be in the ring buffer
	err = c.GetLog(33, &out)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = c.GetLog(34, &out)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Purge the ring buffer
	err = c.DeleteRange(33, 34)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Should not be in the ring buffer
	err = c.GetLog(33, &out)
	if err != ErrLogNotFound {
		t.Fatalf("err: %v", err)
	}
	err = c.GetLog(34, &out)
	if err != ErrLogNotFound {
		t.Fatalf("err: %v", err)
	}
}

type errorStore struct {
	LogStore
	mu      sync.Mutex
	fail    bool
	failed  int
	failMax int
}

func (e *errorStore) StoreLogs(logs []*Log) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.fail {
		e.failed++
		if e.failed <= e.failMax {
			return errors.New("some error")
		}
		e.fail = false
	}
	return e.LogStore.StoreLogs(logs)
}

func (e *errorStore) failNext(count int) {
	e.mu.Lock()
	e.fail = true
	e.failMax = count
	e.mu.Unlock()
}

func TestLogCacheWithBackendStoreError(t *testing.T) {
	var err error
	store := NewInmemStore()
	errStore := &errorStore{LogStore: store}
	c, _ := NewLogCache(16, errStore)

	for i := 0; i < 4; i++ {
		log := &Log{Index: uint64(i) + 1}
		store.StoreLog(log)
	}
	errStore.failNext(1)
	log := &Log{Index: 5}
	err = c.StoreLog(log)
	if !strings.Contains(err.Error(), "some error") {
		t.Fatalf("wanted: some error,  got err=%v", err)
	}

	var out Log
	for i := 1; i < 5; i++ {
		if err := c.GetLog(uint64(i), &out); err != nil {
			t.Fatalf("err: %v", err)
		}
	}
	out = Log{}
	if err = c.GetLog(5, &out); err != ErrLogNotFound {
		t.Fatalf("Should have returned not found, got err=%v out=%+v", err, out)
	}
}
