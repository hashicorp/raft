package raft

import (
	"fmt"
	"reflect"
	"testing"
)

func TestCheckConfiguration(t *testing.T) {
	var configuration Configuration
	if checkConfiguration(configuration) == nil {
		t.Fatalf("empty configuration should be error")
	}

	configuration.Servers = append(configuration.Servers, Server{
		Suffrage: Nonvoter,
		GUID:     "guid0",
		Address:  "addr0",
	})
	if checkConfiguration(configuration) == nil {
		t.Fatalf("lack of voter should be error")
	}

	configuration.Servers = append(configuration.Servers, Server{
		Suffrage: Voter,
		GUID:     "guid1",
		Address:  "addr1",
	})
	if err := checkConfiguration(configuration); err != nil {
		t.Fatalf("should be OK: %v", err)
	}

	configuration.Servers[1].GUID = "guid0"
	if checkConfiguration(configuration) == nil {
		t.Fatalf("duplicate GUID should be error")
	}
	configuration.Servers[1].GUID = "guid1"

	configuration.Servers[1].Address = "addr0"
	if checkConfiguration(configuration) == nil {
		t.Fatalf("duplicate address should be error")
	}
}

func TestDecodePeers(t *testing.T) {
	var configuration Configuration
	_, trans := NewInmemTransport("")

	// Set up configuration and encode into old format
	var encPeers [][]byte
	for i := 0; i < 3; i++ {
		address := NewInmemAddr()
		configuration.Servers = append(configuration.Servers, Server{
			Suffrage: Voter,
			GUID:     address,
			Address:  address,
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
