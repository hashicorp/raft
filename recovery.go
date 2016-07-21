package raft

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
)

// PeersJSONRecovery is a recovery manager that reads the old-style peers.json
// file. It does not support server IDs, so it should only be used along with
// ProtocolVersion 0. Upon disarming, it'll delete the peers.json file.
type PeersJSONRecovery struct {
	// path is the full path to the peers.json file.
	path string

	// Configuration is the override configuration.
	Configuration Configuration
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
		Configuration: configuration,
	}
	return recovery, nil
}

// Disarm removes the peers.json file so that this won't override configuration
// changes that occur after the recovery.
func (r *PeersJSONRecovery) Disarm() error {
	return os.Remove(r.path)
}
