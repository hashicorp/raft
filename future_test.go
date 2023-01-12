package raft

import (
	"errors"
	"testing"
)

func TestDeferFutureSuccess(t *testing.T) {
	var f DeferError
	f.Init()
	f.Respond(nil)
	if err := f.Error(); err != nil {
		t.Fatalf("unexpected error result; got %#v want nil", err)
	}
	if err := f.Error(); err != nil {
		t.Fatalf("unexpected error result; got %#v want nil", err)
	}
}

func TestDeferFutureError(t *testing.T) {
	want := errors.New("x")
	var f DeferError
	f.Init()
	f.Respond(want)
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
	var f DeferError
	f.Init()
	go f.Respond(want)
	if got := f.Error(); got != want {
		t.Errorf("unexpected error result; got %#v want %#v", got, want)
	}
}
