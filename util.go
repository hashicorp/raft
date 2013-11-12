package raft

import (
	crand "crypto/rand"
	"fmt"
	"math/rand"
	"net"
	"time"
)

// randomTimeout returns a value that is between the minVal and 2x minVal
func randomTimeout(minVal time.Duration) <-chan time.Time {
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

// max returns the maximum
func max(a, b uint64) uint64 {
	if a >= b {
		return a
	}
	return b
}

// generateUUID is used to generate a random UUID
func generateUUID() string {
	buf := make([]byte, 16)
	if _, err := crand.Read(buf); err != nil {
		panic(fmt.Errorf("Failed to read random bytes: %v", err))
	}

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%12x",
		buf[0:4],
		buf[4:6],
		buf[6:8],
		buf[8:10],
		buf[10:16])
}

// asyncNotify is used to do an async channel send to
// a list of channels. This will not block.
func asyncNotify(chans []chan struct{}) {
	for _, ch := range chans {
		asyncNotifyCh(ch)
	}
}

// asyncNotifyCh is used to do an async channel send
// to a singel channel without blocking.
func asyncNotifyCh(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// excludePeer is used to exclude a single peer from a list of peers
func excludePeer(peers []net.Addr, peer net.Addr) []net.Addr {
	otherPeers := make([]net.Addr, 0, len(peers))
	for _, p := range peers {
		if p.String() != peer.String() {
			otherPeers = append(otherPeers, p)
		}
	}
	return otherPeers
}

// peerContained checks if a given peer is contained in a list
func peerContained(peers []net.Addr, peer net.Addr) bool {
	for _, p := range peers {
		if p.String() == peer.String() {
			return true
		}
	}
	return false
}

// addUniquePeer is used to add a peer to a list of existing
// peers only if it is not already contained
func addUniquePeer(peers []net.Addr, peer net.Addr) []net.Addr {
	if peerContained(peers, peer) {
		return peers
	} else {
		return append(peers, peer)
	}
}
