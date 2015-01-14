package raft

import (
	"testing"
)

func TestEmptyCache(t *testing.T) {
	store := NewInmemStore()
	c := NewLogCache(10, store)

	for i := 0; i < 20; i++ {
		if _, ok := c.getLogFromCache(uint64(i)); ok {
			t.Fatalf("getLogFromCache(%d): got true, want false", i)
		}
	}
}

func TestSingleEntry(t *testing.T) {
	store := NewInmemStore()
	c := NewLogCache(10, store)

	c.cacheLogs([]*Log{&Log{Index: 1}})

	for i := 0; i < 20; i++ {
		want := (i == 1)
		if _, got := c.getLogFromCache(uint64(i)); got != want {
			t.Fatalf("getLogFromCache(%d): got %v, want %v", i, got, want)
		}
	}
}

func TestMultipleEntries(t *testing.T) {
	store := NewInmemStore()
	c := NewLogCache(10, store)

	for i := 0; i < 40; i++ {
		c.cacheLogs([]*Log{&Log{Index: uint64(i)}})
	}

	for i := 0; i < 50; i++ {
		want := (i >= 30 && i <= 39)
		if _, got := c.getLogFromCache(uint64(i)); got != want {
			t.Fatalf("getLogFromCache(%d): got %v, want %v", i, got, want)
		}
	}

}
