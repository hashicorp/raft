package raft

import (
	"net"
)

// PeerStore provides an interface for persistent storage and
// retrieval of peers. We use a seperate interface than StableStore
// since the peers may need to be editted by a human operator. For example,
// in a two node cluster, the failure of either node requires human intervention
// since consensus is impossible.
type PeerStore interface {
	// Returns the list of known peers
	Peers() ([]net.Addr, error)

	// Sets the list of known peers. This is invoked when
	// a peer is added or removed
	SetPeers([]net.Addr) error
}

// StatisPeers is used to provide a static list of peers
type StaticPeers struct {
	StaticPeers []net.Addr
}

func (s *StaticPeers) Peers() ([]net.Addr, error) {
	return s.StaticPeers, nil
}

func (s *StaticPeers) SetPeers(p []net.Addr) error {
	s.StaticPeers = p
	return nil
}
