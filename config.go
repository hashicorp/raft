package raft

import (
	"fmt"
	"io"
	"log"
	"time"
)

// These are the versions of the protocol (which includes RPC messages as
// well as Raft-specific log entries) that this server can _understand_. Use
// the ProtocolVersion member of the Config object to control the version of
// the protocol to use when _speaking_ to other servers. This is not currently
// written into snapshots so they are unversioned. Note that depending on the
// protocol version being spoken, some otherwise understood RPC messages may be
// refused. See isVersionCompatible for details of this logic.
//
// There are notes about the upgrade path in the description of the versions
// below. If you are starting a fresh cluster then there's no reason not to
// jump right to the latest protocol version. If you need to interoperate with
// older, unversioned Raft servers you'll need to drive the cluster through the
// different versions in order. This may best be done by moving forward across a
// series of releases of your application.
//
// Version History
//
// 0: Unversioned, original protocol, used to interoperate with unversioned
//    Raft servers. Under this version all configuration changes are propagated
//    using the deprecated RemovePeerDeprecated Raft log entry. This means that
//    server IDs are always set to be the same as the server addresses (since
//    the old log entry type cannot transmit an ID), and only AddPeer/RemovePeer
//    APIs are supported. This version can understand the new LogConfiguration
//    Raft log entry but it will never generate one.
// 1: Transitional protocol used when migrating an existing cluster to the new
//    server ID system. Server IDs are still set to be the same as server
//    addresses, but all configuration changes are propagated using the new
//    LogConfiguration Raft log entry type, which can carry full ID information.
//    This version supports the old AddPeer/RemovePeer APIs as well as the new
//    ID-based AddVoter/RemoveServer APIs which should be used when adding
//    version 2 servers to the cluster later. Note that non-voting roles are not
//    yet supported. This version sheds all interoperability with unversioned
//    Raft servers, but can interoperate with newer Raft servers running with
//    protocol version 0 since they can understand the new LogConfiguration Raft
//    log entry, and this version can still understand their RemovePeerDeprecated
//    Raft log entries. We need this protocol version as an intermediate step
//    between 0 and 2 so that servers will propagate the ID information that
//    will come from newly-added (or -cycled) servers using protocol version 2,
//    but since they are still using their address-based IDs from the previous
//    step they will still be able to track commitments and their own voting
//    status properly. If we skipped this step, servers would be started with
//    their new IDs, but they wouldn't see themselves in the old address-based
//    configuration, so none of the servers would think they had a vote.
// 2: Protocol adding full support for server IDs and new ID-based server APIs
//    (AddVoter, AddNonvoter, etc.), old AddPeer/RemovePeer APIs are no longer
//    supported. Version 1 servers should be swapped out by removing them from
//    the cluster one-by-one and re-adding them with updated configuration for
//    this protocol version, along with their server ID. The remove/add cycle
//    is required to populate their server ID. Note that removing must be done
//    by ID, which will be the old server's address.
const (
	ProtocolVersionMin = 0
	ProtocolVersionMax = 2
)

// Config provides any necessary configuration to
// the Raft server
type Config struct {
	// ProtocolVersion allows a Raft server to inter-operate with older
	// Raft servers running an older version of the code. This is used to
	// version the wire protocol as well as Raft-specific log entries that
	// the server uses when _speaking_ to other servers. There is currently
	// no auto-negotiation of versions so all servers must be manually
	// configured with compatible versions. See ProtocolVersionMin and
	// ProtocolVersionMax for the versions of the protocol that this server
	// can _understand_.
	ProtocolVersion int

	// HeartbeatTimeout specifies the time in follower state without
	// a leader before we attempt an election.
	HeartbeatTimeout time.Duration

	// ElectionTimeout specifies the time in candidate state without
	// a leader before we attempt an election.
	ElectionTimeout time.Duration

	// CommitTimeout controls the time without an Apply() operation
	// before we heartbeat to ensure a timely commit. Due to random
	// staggering, may be delayed as much as 2x this value.
	CommitTimeout time.Duration

	// MaxAppendEntries controls the maximum number of append entries
	// to send at once. We want to strike a balance between efficiency
	// and avoiding waste if the follower is going to reject because of
	// an inconsistent log.
	MaxAppendEntries int

	// If we are a member of a cluster, and RemovePeer is invoked for the
	// local node, then we forget all peers and transition into the follower state.
	// If ShutdownOnRemove is is set, we additional shutdown Raft. Otherwise,
	// we can become a leader of a cluster containing only this node.
	ShutdownOnRemove bool

	// TrailingLogs controls how many logs we leave after a snapshot. This is
	// used so that we can quickly replay logs on a follower instead of being
	// forced to send an entire snapshot.
	TrailingLogs uint64

	// SnapshotInterval controls how often we check if we should perform a snapshot.
	// We randomly stagger between this value and 2x this value to avoid the entire
	// cluster from performing a snapshot at once.
	SnapshotInterval time.Duration

	// SnapshotThreshold controls how many outstanding logs there must be before
	// we perform a snapshot. This is to prevent excessive snapshots when we can
	// just replay a small set of logs.
	SnapshotThreshold uint64

	// LeaderLeaseTimeout is used to control how long the "lease" lasts
	// for being the leader without being able to contact a quorum
	// of nodes. If we reach this interval without contact, we will
	// step down as leader.
	LeaderLeaseTimeout time.Duration

	// StartAsLeader forces Raft to start in the leader state. This should
	// never be used except for testing purposes, as it can cause a split-brain.
	StartAsLeader bool

	// The unique ID for this server across all time. If using protocol
	// version 0 this is optional and will be populated with the server's
	// network address if not given. For protocol version > 0 this is
	// required.
	LocalID ServerID

	// NotifyCh is used to provide a channel that will be notified of leadership
	// changes. Raft will block writing to this channel, so it should either be
	// buffered or aggressively consumed.
	NotifyCh chan<- bool

	// LogOutput is used as a sink for logs, unless Logger is specified.
	// Defaults to os.Stderr.
	LogOutput io.Writer

	// Logger is a user-provided logger. If nil, a logger writing to LogOutput
	// is used.
	Logger *log.Logger
}

// DefaultConfig returns a Config with usable defaults.
func DefaultConfig() *Config {
	return &Config{
		ProtocolVersion:    ProtocolVersionMax,
		HeartbeatTimeout:   1000 * time.Millisecond,
		ElectionTimeout:    1000 * time.Millisecond,
		CommitTimeout:      50 * time.Millisecond,
		MaxAppendEntries:   64,
		ShutdownOnRemove:   true,
		TrailingLogs:       10240,
		SnapshotInterval:   120 * time.Second,
		SnapshotThreshold:  8192,
		LeaderLeaseTimeout: 500 * time.Millisecond,
	}
}

// ValidateConfig is used to validate a sane configuration
func ValidateConfig(config *Config) error {
	if config.ProtocolVersion < ProtocolVersionMin ||
		config.ProtocolVersion > ProtocolVersionMax {
		return fmt.Errorf("Protocol version %d must be >= %d and <= %d",
			config.ProtocolVersion, ProtocolVersionMin, ProtocolVersionMax)
	}
	if config.ProtocolVersion > 0 && len(config.LocalID) == 0 {
		return fmt.Errorf("LocalID cannot be empty")
	}
	if config.HeartbeatTimeout < 5*time.Millisecond {
		return fmt.Errorf("Heartbeat timeout is too low")
	}
	if config.ElectionTimeout < 5*time.Millisecond {
		return fmt.Errorf("Election timeout is too low")
	}
	if config.CommitTimeout < time.Millisecond {
		return fmt.Errorf("Commit timeout is too low")
	}
	if config.MaxAppendEntries <= 0 {
		return fmt.Errorf("MaxAppendEntries must be positive")
	}
	if config.MaxAppendEntries > 1024 {
		return fmt.Errorf("MaxAppendEntries is too large")
	}
	if config.SnapshotInterval < 5*time.Millisecond {
		return fmt.Errorf("Snapshot interval is too low")
	}
	if config.LeaderLeaseTimeout < 5*time.Millisecond {
		return fmt.Errorf("Leader lease timeout is too low")
	}
	if config.LeaderLeaseTimeout > config.HeartbeatTimeout {
		return fmt.Errorf("Leader lease timeout cannot be larger than heartbeat timeout")
	}
	if config.ElectionTimeout < config.HeartbeatTimeout {
		return fmt.Errorf("Election timeout must be equal or greater than Heartbeat Timeout")
	}
	return nil
}
