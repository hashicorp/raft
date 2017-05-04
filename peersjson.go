package raft

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
)

// ReadPeersJSON consumes a legacy peers.json file in the format of the old JSON
// peer store and creates a new-style Membership structure. This can be used
// to migrate this data or perform manual recovery when running protocol versions
// that can interoperate with older, unversioned Raft servers. This should not be
// used once server IDs are in use, because the old peers.json file didn't have
// support for these, nor non-voter suffrage types.
func ReadPeersJSON(path string) (Membership, error) {
	// Read in the file.
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		return Membership{}, err
	}

	// Parse it as JSON.
	var peers []string
	dec := json.NewDecoder(bytes.NewReader(buf))
	if err := dec.Decode(&peers); err != nil {
		return Membership{}, err
	}

	// Map it into the new-style membership structure. We can only specify
	// voter roles here, and the ID has to be the same as the address.
	membership := Membership{
		Servers: make([]Server, len(peers)),
	}
	for idx, peer := range peers {
		membership.Servers[idx] = Server{
			Suffrage: Voter,
			ID:       ServerID(peer),
			Address:  ServerAddress(peer),
		}
	}

	// We should only ingest valid memberships.
	if err := membership.check(); err != nil {
		return Membership{}, err
	}
	return membership, nil
}

// configEntry is used when decoding a new-style peers.json.
type configEntry struct {
	// ID is the ID of the server (a UUID, usually).
	ID ServerID `json:"id"`

	// Address is the host:port of the server.
	Address ServerAddress `json:"address"`

	// NonVoter controls the suffrage. We choose this sense so people
	// can leave this out and get a Voter by default.
	NonVoter bool `json:"non_voter"`
}

// ReadConfigJSON reads a new-style peers.json and returns a membership
// structure. This can be used to perform manual recovery when running protocol
// versions that use server IDs.
func ReadConfigJSON(path string) (Membership, error) {
	// Read in the file.
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		return Membership{}, err
	}

	// Parse it as JSON.
	var peers []configEntry
	dec := json.NewDecoder(bytes.NewReader(buf))
	if err := dec.Decode(&peers); err != nil {
		return Membership{}, err
	}

	// Map it into the new-style membership structure.
	var membership Membership
	for _, peer := range peers {
		suffrage := Voter
		if peer.NonVoter {
			suffrage = Nonvoter
		}
		server := Server{
			Suffrage: suffrage,
			ID:       peer.ID,
			Address:  peer.Address,
		}
		membership.Servers = append(membership.Servers, server)
	}

	// We should only ingest valid memberships.
	if err := membership.check(); err != nil {
		return Membership{}, err
	}
	return membership, nil
}
