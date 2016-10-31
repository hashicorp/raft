package raft

import (
	"fmt"
	"strings"
)

// Membership changes follow the single-server algorithm described in Diego
// Ongaro's PhD dissertation. The Membership struct defines a cluster membership
// configuration, which is a set of servers, each of which is either a Voter,
// Nonvoter, or Staging (defined below).
//
// All changes to the membership configuration is done by writing a new
// membership configuration to the log, which the server does in
// appendMembershipEntry(). The new configuration will be in affect as soon as
// it is appended to the log (not when it is committed like a normal state
// machine command). Note that, for safety purposes, there can be at most one
// uncommitted configuration at a time (the next configuration may not be
// created until the prior one has been committed). It's not strictly necessary
// to follow these same rules for the nonvoter/staging servers, but those
// changes are treated uniformly for simplicity.
//
// Each server tracks two membership configurations (in its "memberships"
// struct, defined below):
// 1. the committed configuration: the latest configuration in the
//    log/snapshot that has been committed, along with its index.
// 2. the latest configuration: the latest configuration in the log/snapshot
//    (may be committed or uncommitted), along with its index.
//
// When there's no membership change happening, these two will be the same.
// The latest membership configuration is almost always the one used, except:
// - When followers truncate the suffix of their logs, they may need to fall
//   back to the committed configuration.
// - When snapshotting, the committed configuration is written, to correspond
//   with the committed log prefix that is being snapshotted.

// ServerSuffrage determines whether a Server in a Configuration gets a vote.
type ServerSuffrage int

// Note: Don't renumber these, since the numbers are written into the log.
const (
	// Voter is a server whose vote is counted in elections and whose match index
	// is used in advancing the leader's commit index.
	Voter ServerSuffrage = iota
	// Nonvoter is a server that receives log entries but is not considered for
	// elections or commitment purposes.
	Nonvoter
	// Staging is a server that acts like a nonvoter with one exception: once a
	// staging server receives enough log entries to be sufficiently caught up to
	// the leader's log, the leader will invoke a  membership change to change
	// the Staging server to a Voter.
	Staging
)

func (s ServerSuffrage) String() string {
	switch s {
	case Voter:
		return "Voter"
	case Nonvoter:
		return "Nonvoter"
	case Staging:
		return "Staging"
	}
	return "ServerSuffrage"
}

// ServerID is a unique string identifying a server for all time.
type ServerID string

// ServerAddress is a network address for a server that a transport can contact.
type ServerAddress string

// Server tracks the information about a single server in a configuration.
type Server struct {
	// Suffrage determines whether the server gets a vote.
	Suffrage ServerSuffrage
	// ID is a unique string identifying this server for all time.
	ID ServerID
	// Address is its network address that a transport can contact.
	Address ServerAddress
}

// Configuration tracks which servers are in the cluster, and whether they have
// votes. This should include the local server, if it's a member of the cluster.
// The servers are listed no particular order, but each should only appear once.
// These entries are appended to the log during membership changes.
type Membership struct {
	Servers []Server
}

func (m Membership) String() string {
	vec := make([]string, 0, len(m.Servers))
	for _, server := range m.Servers {
		vec = append(vec, fmt.Sprintf("%s at %s (%s)",
			server.ID, server.Address, server.Suffrage))
	}
	return fmt.Sprintf("[%s]", strings.Join(vec, ", "))
}

// Clone makes a deep copy of a Membership.
func (m *Membership) Clone() (copy Membership) {
	copy.Servers = append(copy.Servers, m.Servers...)
	return
}

// MembershipChangeCommand is the different ways to change the cluster
// configuration, as illustrated in the following diagram:
//
//                       Start ->  +--------+
//             ,------<------------|        |
//            /                    | absent |
//           /     RemoveServer--> |        | <-RemoveServer
//          /            |         +--------+               \
//         /             |            |                      \
//    AddNonvoter        |        AddStaging                  \
//        |       ,->---' `--<-.      |                        \
//        v      /              \     v                         \
//   +----------+                +----------+                    +----------+
//   |          | --AddStaging-> |          | -----Promote-----> |          |
//   | nonvoter |                | staging  |                    |  voter   |
//   |          | <-DemoteVoter- |          |                 ,- |          |
//   +----------+         \      +----------+                /   +----------+
//                         \                                /
//                          `--------------<---------------'
//
// Note that these are the internal commands the leader places in the log, which
// differ from client requests when adding voters. Specifically, when clients
// request AddVoter, the leader will append an AddStaging command. Once the
// server is ready, the leader will append a Promote command.
type MembershipChangeCommand uint8

const (
	// AddStaging makes a server Staging unless its Voter.
	AddStaging MembershipChangeCommand = iota
	// AddNonvoter makes a server Nonvoter unless its Staging or Voter.
	AddNonvoter
	// DemoteVoter makes a server Nonvoter unless its absent.
	DemoteVoter
	// RemoveServer removes a server entirely from the cluster membership.
	RemoveServer
	// Promote is created automatically by a leader; it turns a Staging server
	// into a Voter.
	Promote
)

func (m MembershipChangeCommand) String() string {
	switch m {
	case AddStaging:
		return "AddStaging"
	case AddNonvoter:
		return "AddNonvoter"
	case DemoteVoter:
		return "DemoteVoter"
	case RemoveServer:
		return "RemoveServer"
	case Promote:
		return "Promote"
	}
	return "MembershipChangeCommand"
}

// membershipChangeRequest describes a change that a leader would like to
// make to its current membership configuration. It's used only within a
// single server (never serialized into the log), as part of
// `membershipChangeFuture`.
type membershipChangeRequest struct {
	command       MembershipChangeCommand
	serverID      ServerID
	serverAddress ServerAddress // only present for AddStaging, AddNonvoter
	// prevIndex, if nonzero, is the index of the only configuration upon which
	// this change may be applied; if another configuration entry has been
	// added in the meantime, this request will fail.
	prevIndex Index
}

// memberships is state tracked on every server about its Cluster Membership.
// Note that, per Diego's dissertation, there can be at most one uncommitted
// membership at a time (the next configuration may not be created until the
// prior one has been committed).
//
// One downside to storing just two memberships is that if you try to take a
// snapshot when your state machine hasn't yet applied the committedIndex, we
// have no record of the membership that would logically fit into that
// snapshot. We disallow snapshots in that case now. An alternative approach,
// which LogCabin uses, is to track every membership change in the
// log.
// Unless there's a membership change in progress, commited & latest will
// be the same
type memberships struct {
	// committed is the latest membership in the log/snapshot that has been
	// committed (the one with the largest index).
	committed Membership
	// committedIndex is the log index where 'committed' was written.
	committedIndex Index
	// latest is the latest configuration in the log/snapshot (may be committed
	// or uncommitted)
	latest Membership
	// latestIndex is the log index where 'latest' was written.
	latestIndex Index
}

// Clone makes a deep copy of a memberships object.
func (m *memberships) Clone() (copy memberships) {
	copy.committed = m.committed.Clone()
	copy.committedIndex = m.committedIndex
	copy.latest = m.latest.Clone()
	copy.latestIndex = m.latestIndex
	return
}

// hasVote returns true if the server identified by 'id' is a Voter in the
// provided Membership.
func hasVote(membership Membership, id ServerID) bool {
	for _, server := range membership.Servers {
		if server.ID == id {
			return server.Suffrage == Voter
		}
	}
	return false
}

// check tests a cluster membership configuration for common errors.
func (membership *Membership) check() error {
	idSet := make(map[ServerID]bool)
	addressSet := make(map[ServerAddress]bool)
	var voters int
	for _, server := range membership.Servers {
		if server.ID == "" {
			return fmt.Errorf("Empty ID in membership: %v", membership)
		}
		if server.Address == "" {
			return fmt.Errorf("Empty address in membership: %v", server)
		}
		if idSet[server.ID] {
			return fmt.Errorf("Found duplicate ID in membership: %v", server.ID)
		}
		idSet[server.ID] = true
		if addressSet[server.Address] {
			return fmt.Errorf("Found duplicate address in membership: %v", server.Address)
		}
		addressSet[server.Address] = true
		if server.Suffrage == Voter {
			voters++
		}
	}
	if voters == 0 {
		return fmt.Errorf("Need at least one voter in membership: %v", membership)
	}
	return nil
}

// nextMembership generates a new Membership from the current one and a
// membership change request. It's split from appendMembershipEntry so
// that it can be unit tested easily.
func nextMembership(current Membership, currentIndex Index, change membershipChangeRequest) (Membership, error) {
	if change.prevIndex > 0 && change.prevIndex != currentIndex {
		return Membership{}, fmt.Errorf("Membership changed since %v (latest is %v)", change.prevIndex, currentIndex)
	}

	membership := current.Clone()
	switch change.command {
	case AddStaging:
		newServer := Server{
			// TODO: This should add the server as Staging, to be automatically
			// promoted to Voter later. However, the promoton to Voter is not yet
			// implemented, and doing so is not trivial with the way the leader loop
			// coordinates with the replication goroutines today. So, for now, the
			// server will have a vote right away, and the Promote case below is
			// unused.
			Suffrage: Voter,
			ID:       change.serverID,
			Address:  change.serverAddress,
		}
		found := false
		for i, server := range membership.Servers {
			if server.ID == change.serverID {
				if server.Address != change.serverAddress {
					return Membership{}, fmt.Errorf("May not change address of server %v (was %v, given %v)",
						server.ID, server.Address, change.serverAddress)
				}
				membership.Servers[i].Suffrage = Voter
				found = true
				break
			}
		}
		if !found {
			membership.Servers = append(membership.Servers, newServer)
		}
	case AddNonvoter:
		newServer := Server{
			Suffrage: Nonvoter,
			ID:       change.serverID,
			Address:  change.serverAddress,
		}
		found := false
		for _, server := range membership.Servers {
			if server.ID == change.serverID {
				if server.Address != change.serverAddress {
					return Membership{}, fmt.Errorf("May not change address of server %v (was %v, given %v)",
						server.ID, server.Address, change.serverAddress)
				}
				found = true
				break
			}
		}
		if !found {
			membership.Servers = append(membership.Servers, newServer)
		}
	case DemoteVoter:
		for i, server := range membership.Servers {
			if server.ID == change.serverID {
				membership.Servers[i].Suffrage = Nonvoter
				break
			}
		}
	case RemoveServer:
		for i, server := range membership.Servers {
			if server.ID == change.serverID {
				membership.Servers = append(membership.Servers[:i], membership.Servers[i+1:]...)
				break
			}
		}
	case Promote:
		for i, server := range membership.Servers {
			if server.ID == change.serverID && server.Suffrage == Staging {
				membership.Servers[i].Suffrage = Voter
				break
			}
		}
	}

	// Make sure we didn't do something bad like remove the last voter
	if err := membership.check(); err != nil {
		return Membership{}, err
	}

	return membership, nil
}

// encodePeers is used to serialize a Membership into the old peers format.
// This is here for backwards compatibility when operating with a mix of old
// servers and should be removed once we deprecate support for protocol version 1.
func encodePeers(membership Membership, trans Transport) []byte {
	// Gather up all the voters, other suffrage types are not supported by
	// this data format.
	var encPeers [][]byte
	for _, server := range membership.Servers {
		if server.Suffrage == Voter {
			encPeers = append(encPeers, trans.EncodePeer(server.Address))
		}
	}

	// Encode the entire array.
	buf, err := encodeMsgPack(encPeers)
	if err != nil {
		panic(fmt.Errorf("failed to encode peers: %v", err))
	}

	return buf.Bytes()
}

// decodePeers is used to deserialize an old list of peers into a Membership.
// This is here for backwards compatibility with old log entries and snapshots;
// it should be removed eventually.
func decodePeers(buf []byte, trans Transport) Membership {
	// Decode the buffer first.
	var encPeers [][]byte
	if err := decodeMsgPack(buf, &encPeers); err != nil {
		panic(fmt.Errorf("failed to decode peers: %v", err))
	}

	// Deserialize each peer.
	var servers []Server
	for _, enc := range encPeers {
		p := trans.DecodePeer(enc)
		servers = append(servers, Server{
			Suffrage: Voter,
			ID:       ServerID(p),
			Address:  ServerAddress(p),
		})
	}

	return Membership{
		Servers: servers,
	}
}

// encodeMembership serializes a Membership using MsgPack, or panics on
// errors.
func encodeMembership(membership Membership) []byte {
	buf, err := encodeMsgPack(membership)
	if err != nil {
		panic(fmt.Errorf("failed to encode membership: %v", err))
	}
	return buf.Bytes()
}

// decodeMembership deserializes a Membership using MsgPack, or panics on
// errors.
func decodeMembership(buf []byte) Membership {
	var membership Membership
	if err := decodeMsgPack(buf, &membership); err != nil {
		panic(fmt.Errorf("failed to decode membership: %v", err))
	}
	return membership
}
