package raft

import "fmt"

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
type Configuration struct {
	Servers []Server
}

// Clone makes a deep copy of a Configuration.
func (c *Configuration) Clone() (copy Configuration) {
	copy.Servers = append(copy.Servers, c.Servers...)
	return
}

// ConfigurationChangeCommand is the different ways to change the cluster
// configuration.
type ConfigurationChangeCommand uint8

const (
	// AddStaging makes a server Staging unless its Voter.
	AddStaging ConfigurationChangeCommand = iota
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

func (c ConfigurationChangeCommand) String() string {
	switch c {
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
	return "ConfigurationChangeCommand"
}

// configurationChangeRequest describes a change that a leader would like to
// make to its current configuration. It's used only within a single server
// (never serialized into the log), as part of `configurationChangeFuture`.
type configurationChangeRequest struct {
	command       ConfigurationChangeCommand
	serverID      ServerID
	serverAddress ServerAddress // only present for AddStaging, AddNonvoter
	// prevIndex, if nonzero, is the index of the only configuration upon which
	// this change may be applied; if another configuration entry has been
	// added in the meantime, this request will fail.
	prevIndex uint64
}

// configurations is state tracked on every server about its Configurations.
// Note that, per Diego's dissertation, there can be at most one uncommitted
// configuration at a time (the next configuration may not be created until the
// prior one has been committed).
//
// One downside to storing just two configurations is that if you try to take a
// snahpsot when your state machine hasn't yet applied the committedIndex, we
// have no record of the configuration that would logically fit into that
// snapshot. We disallow snapshots in that case now. An alternative approach,
// which LogCabin uses, is to track every configuration change in the
// log.
type configurations struct {
	// committed is the latest configuration in the log/snapshot that has been
	// committed (the one with the largest index).
	committed Configuration
	// committedIndex is the log index where 'committed' was written.
	committedIndex uint64
	// latest is the latest configuration in the log/snapshot (may be committed
	// or uncommitted)
	latest Configuration
	// latestIndex is the log index where 'latest' was written.
	latestIndex uint64
}

// Clone makes a deep copy of a configurations object.
func (c *configurations) Clone() (copy configurations) {
	copy.committed = c.committed.Clone()
	copy.committedIndex = c.committedIndex
	copy.latest = c.latest.Clone()
	copy.latestIndex = c.latestIndex
	return
}

// hasVote returns true if the server identified by 'id' is a Voter in the
// provided Configuration.
func hasVote(configuration Configuration, id ServerID) bool {
	for _, server := range configuration.Servers {
		if server.ID == id {
			return server.Suffrage == Voter
		}
	}
	return false
}

// checkConfiguration tests a cluster membership configuration for common
// errors.
func checkConfiguration(configuration Configuration) error {
	idSet := make(map[ServerID]bool)
	addressSet := make(map[ServerAddress]bool)
	var voters int
	for _, server := range configuration.Servers {
		if server.ID == "" {
			return fmt.Errorf("Empty ID in configuration: %v", configuration)
		}
		if server.Address == "" {
			return fmt.Errorf("Empty address in configuration: %v", server)
		}
		if idSet[server.ID] {
			return fmt.Errorf("Found duplicate ID in configuration: %v", server.ID)
		}
		idSet[server.ID] = true
		if addressSet[server.Address] {
			return fmt.Errorf("Found duplicate address in configuration: %v", server.Address)
		}
		addressSet[server.Address] = true
		if server.Suffrage == Voter {
			voters++
		}
	}
	if voters == 0 {
		return fmt.Errorf("Need at least one voter in configuration: %v", configuration)
	}
	return nil
}

// decodePeers is used to deserialize an old list of peers into a Configuration.
// This is here for backwards compatibility with old log entries and snapshots;
// it should be removed eventually.
func decodePeers(buf []byte, trans Transport) Configuration {
	// Decode the buffer first
	var encPeers [][]byte
	if err := decodeMsgPack(buf, &encPeers); err != nil {
		panic(fmt.Errorf("failed to decode peers: %v", err))
	}

	// Deserialize each peer
	var servers []Server
	for _, enc := range encPeers {
		p := trans.DecodePeer(enc)
		servers = append(servers, Server{
			Suffrage: Voter,
			ID:       ServerID(p),
			Address:  ServerAddress(p),
		})
	}

	return Configuration{
		Servers: servers,
	}
}

// encodeConfiguration serializes a Configuration using MsgPack, or panics on
// errors.
func encodeConfiguration(configuration Configuration) []byte {
	buf, err := encodeMsgPack(configuration)
	if err != nil {
		panic(fmt.Errorf("failed to encode peers: %v", err))
	}
	return buf.Bytes()
}

// decodeConfiguration deserializes a Configuration using MsgPack, or panics on
// errors.
func decodeConfiguration(buf []byte) Configuration {
	var configuration Configuration
	if err := decodeMsgPack(buf, &configuration); err != nil {
		panic(fmt.Errorf("failed to decode configuration: %v", err))
	}
	return configuration
}
