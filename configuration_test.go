package raft

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var sampleConfiguration Configuration = Configuration{
	Servers: []Server{
		Server{
			Suffrage: Nonvoter,
			ID:       ServerID("id0"),
			Address:  ServerAddress("addr0"),
		},
		Server{
			Suffrage: Voter,
			ID:       ServerID("id1"),
			Address:  ServerAddress("addr1"),
		},
		Server{
			Suffrage: Staging,
			ID:       ServerID("id2"),
			Address:  ServerAddress("addr2"),
		},
	},
}

func TestConfiguration_Clone(t *testing.T) {
	{
		cloned := sampleConfiguration.Clone()
		if !reflect.DeepEqual(sampleConfiguration, cloned) {
			t.Fatalf("mismatch %v %v", sampleConfiguration, cloned)
		}
		cloned.Servers[1].ID = "scribble"
		if sampleConfiguration.Servers[1].ID == "scribble" {
			t.Fatalf("cloned configuration shouldn't alias Servers")
		}
	}

	{
		configurations := configurations{
			committed:      sampleConfiguration,
			committedIndex: 1,
			latest:         sampleConfiguration,
			latestIndex:    2,
		}
		cloned := configurations.Clone()
		if !reflect.DeepEqual(configurations, cloned) {
			t.Fatalf("mismatch %v %v", configurations, cloned)
		}
		cloned.committed.Servers[1].ID = "scribble"
		cloned.latest.Servers[1].ID = "scribble"
		if configurations.committed.Servers[1].ID == "scribble" ||
			configurations.latest.Servers[1].ID == "scribble" {
			t.Fatalf("cloned configuration shouldn't alias Servers")
		}
	}
}

func TestConfiguration_hasVote(t *testing.T) {
	if hasVote(sampleConfiguration, "id0") {
		t.Fatalf("id0 should not have vote")
	}
	if !hasVote(sampleConfiguration, "id1") {
		t.Fatalf("id1 should have vote")
	}
	if hasVote(sampleConfiguration, "id2") {
		t.Fatalf("id2 should not have vote")
	}
	if hasVote(sampleConfiguration, "someotherid") {
		t.Fatalf("someotherid should not have vote")
	}
}

func TestConfiguration_checkConfiguration(t *testing.T) {
	var configuration Configuration
	if checkConfiguration(configuration) == nil {
		t.Fatalf("empty configuration should be error")
	}

	configuration.Servers = append(configuration.Servers, Server{
		Suffrage: Nonvoter,
		ID:       ServerID("id0"),
		Address:  ServerAddress("addr0"),
	})
	if checkConfiguration(configuration) == nil {
		t.Fatalf("lack of voter should be error")
	}

	configuration.Servers = append(configuration.Servers, Server{
		Suffrage: Voter,
		ID:       ServerID("id1"),
		Address:  ServerAddress("addr1"),
	})
	if err := checkConfiguration(configuration); err != nil {
		t.Fatalf("should be OK: %v", err)
	}

	configuration.Servers[1].ID = "id0"
	err := checkConfiguration(configuration)
	if err == nil {
		t.Fatalf("duplicate ID should be error")
	}
	if !strings.Contains(err.Error(), "duplicate ID") {
		t.Fatalf("unexpected error: %v", err)
	}
	configuration.Servers[1].ID = "id1"

	configuration.Servers[1].Address = "addr0"
	err = checkConfiguration(configuration)
	if err == nil {
		t.Fatalf("duplicate address should be error")
	}
	if !strings.Contains(err.Error(), "duplicate address") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestConfiguration_decodePeers(t *testing.T) {
	var configuration Configuration
	_, trans := NewInmemTransport("")

	// Set up configuration and encode into old format
	var encPeers [][]byte
	for i := 0; i < 3; i++ {
		address := NewInmemAddr()
		configuration.Servers = append(configuration.Servers, Server{
			Suffrage: Voter,
			ID:       ServerID(address),
			Address:  ServerAddress(address),
		})
		encPeers = append(encPeers, trans.EncodePeer(address))
	}
	buf, err := encodeMsgPack(encPeers)
	if err != nil {
		panic(fmt.Errorf("failed to encode peers: %v", err))
	}

	// Decode from old format, as if reading an old log entry
	decoded := decodePeers(buf.Bytes(), trans)

	if !reflect.DeepEqual(configuration, decoded) {
		t.Fatalf("mismatch %v %v", configuration, decoded)
	}
}

func TestConfiguration_encodeDecodeConfiguration(t *testing.T) {
	decoded := decodeConfiguration(encodeConfiguration(sampleConfiguration))
	if !reflect.DeepEqual(sampleConfiguration, decoded) {
		t.Fatalf("mismatch %v %v", sampleConfiguration, decoded)
	}
}
