package raft

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
)

// Recovery is an interface for a recovery manager that runs at startup in
// order to let operators manually put the cluster's configuration into a
// known state after a loss of quorum outage.
type Recovery interface {
	// Override is called when Raft is starting up. If an override is
	// requested, this returns true along with the new configuration. We
	// include the latest configuration and index so that the recovery
	// manager can examine those and decide whether or not to run, such as
	// if we want to inject a configuration only under a specific index.
	Override(latest Configuration, latestIndex uint64) (Configuration, bool)

	// Disarm is called whenever the recovery configuration becomes durable
	// in the Raft system, such as when another configuration change is
	// committed or when a snapshot has taken place. This should disarm the
	// override so it will not interfere with future configuration changes.
	// Some recovery managers may not need this, such as if they look at the
	// latest index when deciding to act.
	Disarm() error
}

// PeersJSONRecovery is a recovery manager that reads the old-style peers.json
// file. It does not support server IDs, so it should only be used along with
// ProtocolVersion 0. Upon disarming, it'll delete the peers.json file.
type PeersJSONRecovery struct {
	// path is the full path to the peers.json file.
	path string

	// configuration is the override configuration.
	configuration Configuration
}

// NewPeersJSONRecovery takes the given base directory and parses the peers.json
// file at that location, returning a recovery manager to apply this
// configuration at startup. If there's no recovery defined then this will return
// nil, which is a valid thing to pass to NewRaft().
func NewPeersJSONRecovery(base string) (*PeersJSONRecovery, error) {
	// Read in the file.
	path := filepath.Join(base, "peers.json")
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	// Parse it as JSON.
	var peers []string
	dec := json.NewDecoder(bytes.NewReader(buf))
	if err := dec.Decode(&peers); err != nil {
		return nil, err
	}

	// Map it into the new-style configuration structure. We can only specify
	// voter roles here, and the ID has to be the same as the address.
	var configuration Configuration
	for _, peer := range peers {
		server := Server{
			Suffrage: Voter,
			ID:       ServerID(peer),
			Address:  ServerAddress(peer),
		}
		configuration.Servers = append(configuration.Servers, server)
	}

	// We should only ingest valid configurations.
	if err := checkConfiguration(configuration); err != nil {
		return nil, err
	}

	// Looks good - give them a manager!
	recovery := &PeersJSONRecovery{
		path:          path,
		configuration: configuration,
	}
	return recovery, nil
}

// See the Recovery interface documentation for Override.
func (r *PeersJSONRecovery) Override(latest Configuration, latestIndex uint64) (Configuration, bool) {
	return r.configuration, true
}

// See the Recovery interface documentation for Disarm. In this case, we attempt
// delete the peers.json file.
func (r *PeersJSONRecovery) Disarm() error {
	return os.Remove(r.path)
}
