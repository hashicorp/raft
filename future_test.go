// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"errors"
	"testing"
)

func TestDeferFutureSuccess(t *testing.T) {
	var f deferError
	f.init()
	f.respond(nil)
	if err := f.Error(); err != nil {
		t.Fatalf("unexpected error result; got %#v want nil", err)
	}
	if err := f.Error(); err != nil {
		t.Fatalf("unexpected error result; got %#v want nil", err)
	}
}

func TestDeferFutureError(t *testing.T) {
	want := errors.New("x")
	var f deferError
	f.init()
	f.respond(want)
	if got := f.Error(); got != want {
		t.Fatalf("unexpected error result; got %#v want %#v", got, want)
	}
	if got := f.Error(); got != want {
		t.Fatalf("unexpected error result; got %#v want %#v", got, want)
	}
}

func TestDeferFutureConcurrent(t *testing.T) {
	// Food for the race detector.
	want := errors.New("x")
	var f deferError
	f.init()
	go f.respond(want)
	if got := f.Error(); got != want {
		t.Errorf("unexpected error result; got %#v want %#v", got, want)
	}
}

func TestLogFutureWaitCommittedError(t *testing.T) {
	assert := func(t *testing.T, want error, fn func() error) {
		t.Helper()
		if got := fn(); got != want {
			t.Fatalf("unexpected error result; got %#v want %#v", got, want)
		}
	}

	t.Run("ErrorBeforeCommitted", func(t *testing.T) {
		want := errors.New("x")
		var f logFuture
		f.init()
		f.respond(want)
		assert(t, want, f.WaitCommitted)
		assert(t, want, f.WaitCommitted)
		assert(t, want, f.Error)
		assert(t, want, f.Error)
	})

	t.Run("ErrorAfterCommitted", func(t *testing.T) {
		want := errors.New("x")
		var f logFuture
		f.init()
		close(f.committed)
		f.respond(want)
		assert(t, nil, f.WaitCommitted)
		assert(t, want, f.Error)
		assert(t, nil, f.WaitCommitted)
		assert(t, want, f.Error)
	})
}
