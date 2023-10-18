// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"bytes"
	"regexp"
	"testing"
	"time"
)

// TestMsgpackEncodeTime ensures that we don't break backwards compatibility when updating go-msgpack with
// Raft binary formats.
func TestMsgpackEncodeTimeDefaultFormat(t *testing.T) {
	stamp := "2006-01-02T15:04:05Z"
	tm, err := time.Parse(time.RFC3339, stamp)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := encodeMsgPack(tm)

	expected := []byte{175, 1, 0, 0, 0, 14, 187, 75, 55, 229, 0, 0, 0, 0, 255, 255}

	if !bytes.Equal(buf.Bytes(), expected) {
		t.Errorf("Expected time %s to encode as %+v but got %+v", stamp, expected, buf.Bytes())
	}
}

func TestRandomTimeout(t *testing.T) {
	start := time.Now()
	timeout := randomTimeout(time.Millisecond)

	select {
	case <-timeout:
		diff := time.Now().Sub(start)
		if diff < time.Millisecond {
			t.Fatalf("fired early")
		}
	case <-time.After(3 * time.Millisecond):
		t.Fatalf("timeout")
	}
}

func TestNewSeed(t *testing.T) {
	vals := make(map[int64]bool)
	for i := 0; i < 1000; i++ {
		seed := newSeed()
		if _, exists := vals[seed]; exists {
			t.Fatal("newSeed() return a value it'd previously returned")
		}
		vals[seed] = true
	}
}

func TestRandomTimeout_NoTime(t *testing.T) {
	timeout := randomTimeout(0)
	if timeout != nil {
		t.Fatalf("expected nil channel")
	}
}

func TestMin(t *testing.T) {
	if min(1, 1) != 1 {
		t.Fatalf("bad min")
	}
	if min(2, 1) != 1 {
		t.Fatalf("bad min")
	}
	if min(1, 2) != 1 {
		t.Fatalf("bad min")
	}
}

func TestMax(t *testing.T) {
	if max(1, 1) != 1 {
		t.Fatalf("bad max")
	}
	if max(2, 1) != 2 {
		t.Fatalf("bad max")
	}
	if max(1, 2) != 2 {
		t.Fatalf("bad max")
	}
}

func TestGenerateUUID(t *testing.T) {
	prev := generateUUID()
	for i := 0; i < 100; i++ {
		id := generateUUID()
		if prev == id {
			t.Fatalf("Should get a new ID!")
		}

		matched, err := regexp.MatchString(
			`[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}`, id)
		if !matched || err != nil {
			t.Fatalf("expected match %s %v %s", id, matched, err)
		}
	}
}

func TestBackoff(t *testing.T) {
	b := backoff(10*time.Millisecond, 1, 8)
	if b != 10*time.Millisecond {
		t.Fatalf("bad: %v", b)
	}

	b = backoff(20*time.Millisecond, 2, 8)
	if b != 20*time.Millisecond {
		t.Fatalf("bad: %v", b)
	}

	b = backoff(10*time.Millisecond, 8, 8)
	if b != 640*time.Millisecond {
		t.Fatalf("bad: %v", b)
	}

	b = backoff(10*time.Millisecond, 9, 8)
	if b != 640*time.Millisecond {
		t.Fatalf("bad: %v", b)
	}
}

func TestOverrideNotifyBool(t *testing.T) {
	ch := make(chan bool, 1)

	// sanity check - buffered channel don't have any values
	select {
	case v := <-ch:
		t.Fatalf("unexpected receive: %v", v)
	default:
	}

	// simple case of a single push
	overrideNotifyBool(ch, false)
	select {
	case v := <-ch:
		if v != false {
			t.Fatalf("expected false but got %v", v)
		}
	default:
		t.Fatalf("expected a value but is not ready")
	}

	// assert that function never blocks and only last item is received
	overrideNotifyBool(ch, false)
	overrideNotifyBool(ch, false)
	overrideNotifyBool(ch, false)
	overrideNotifyBool(ch, false)
	overrideNotifyBool(ch, true)

	select {
	case v := <-ch:
		if v != true {
			t.Fatalf("expected true but got %v", v)
		}
	default:
		t.Fatalf("expected a value but is not ready")
	}

	// no further value is available
	select {
	case v := <-ch:
		t.Fatalf("unexpected receive: %v", v)
	default:
	}
}
