package raft

import (
	"bytes"
	crand "crypto/rand"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"sync"
	"time"

	"github.com/hashicorp/go-msgpack/codec"
)

func init() {
	// Ensure we use a high-entropy seed for the psuedo-random generator
	rand.Seed(newSeed())
}

// returns an int64 from a crypto random source
// can be used to seed a source for a math/rand.
func newSeed() int64 {
	r, err := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		panic(fmt.Errorf("failed to read random bytes: %v", err))
	}
	return r.Int64()
}

// randomTimeout returns a value that is between the minVal and 2x minVal.
func randomTimeout(minVal time.Duration) <-chan time.Time {
	if minVal == 0 {
		return nil
	}
	extra := (time.Duration(rand.Int63()) % minVal)
	return time.After(minVal + extra)
}

// min returns the minimum.
func min(a, b uint64) uint64 {
	if a <= b {
		return a
	}
	return b
}

// max returns the maximum.
func max(a, b uint64) uint64 {
	if a >= b {
		return a
	}
	return b
}

// generateUUID is used to generate a random UUID.
func generateUUID() string {
	buf := make([]byte, 16)
	if _, err := crand.Read(buf); err != nil {
		panic(fmt.Errorf("failed to read random bytes: %v", err))
	}

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%12x",
		buf[0:4],
		buf[4:6],
		buf[6:8],
		buf[8:10],
		buf[10:16])
}

// asyncNotifyCh is used to do an async channel send
// to a single channel without blocking.
func asyncNotifyCh(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// drainNotifyCh empties out a single-item notification channel without
// blocking, and returns whether it received anything.
func drainNotifyCh(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// asyncNotifyBool is used to do an async notification
// on a bool channel.
func asyncNotifyBool(ch chan bool, v bool) {
	select {
	case ch <- v:
	default:
	}
}

// Decode reverses the encode operation on a byte slice input.
func decodeMsgPack(buf []byte, out interface{}) error {
	r := bytes.NewBuffer(buf)
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(r, &hd)
	return dec.Decode(out)
}

// Encode writes an encoded object to a new bytes buffer.
func encodeMsgPack(in interface{}) (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(nil)
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(buf, &hd)
	err := enc.Encode(in)
	return buf, err
}

// backoff scales base exponentially up until limit. round should be an
// increasing counter starting at 1.
func backoff(round uint64, base time.Duration, limit time.Duration) time.Duration {
	if round == 0 {
		return 0
	}
	wait := base
	for i := uint64(1); i < round && wait < limit; i++ {
		wait *= 2
	}
	if wait < limit {
		return wait
	}
	return limit
}

// Needed for sorting []uint64, used to determine commitment
type uint64Slice []uint64

func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// Tracks running goroutines
type waitGroup struct {
	group sync.WaitGroup
}

// Start a goroutine and properly handle the race between a routine
// starting and incrementing, and exiting and decrementing.
func (wg *waitGroup) spawn(f func()) {
	wg.group.Add(1)
	go func() {
		defer wg.group.Done()
		f()
	}()
}

func (wg *waitGroup) waitShutdown() {
	wg.group.Wait()
}
