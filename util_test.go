package raft

import (
	"regexp"
	"testing"
	"time"
)

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
