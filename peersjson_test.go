// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestPeersJSON_BadConfiguration(t *testing.T) {
	var err error
	var base string
	base, err = os.MkdirTemp("", "")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer os.RemoveAll(base)

	peers := filepath.Join(base, "peers.json")
	if err = os.WriteFile(peers, []byte("null"), 0o666); err != nil {
		t.Fatalf("err: %v", err)
	}

	_, err = ReadPeersJSON(peers)
	if err == nil || !strings.Contains(err.Error(), "at least one voter") {
		t.Fatalf("err: %v", err)
	}
}

func TestPeersJSON_ReadPeersJSON(t *testing.T) {
	var err error
	var base string
	base, err = os.MkdirTemp("", "")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer os.RemoveAll(base)

	content := []byte(`
["127.0.0.1:123",
 "127.0.0.2:123",
 "127.0.0.3:123"]
`)
	peers := filepath.Join(base, "peers.json")
	if err = os.WriteFile(peers, content, 0o666); err != nil {
		t.Fatalf("err: %v", err)
	}
	var configuration Configuration
	configuration, err = ReadPeersJSON(peers)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	expected := Configuration{
		Servers: []Server{
			{
				Suffrage: Voter,
				ID:       ServerID("127.0.0.1:123"),
				Address:  ServerAddress("127.0.0.1:123"),
			},
			{
				Suffrage: Voter,
				ID:       ServerID("127.0.0.2:123"),
				Address:  ServerAddress("127.0.0.2:123"),
			},
			{
				Suffrage: Voter,
				ID:       ServerID("127.0.0.3:123"),
				Address:  ServerAddress("127.0.0.3:123"),
			},
		},
	}
	if !reflect.DeepEqual(configuration, expected) {
		t.Fatalf("bad configuration: %+v != %+v", configuration, expected)
	}
}

func TestPeersJSON_ReadConfigJSON(t *testing.T) {
	var err error
	var base string
	base, err = os.MkdirTemp("", "")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer os.RemoveAll(base)

	content := []byte(`
[
  {
    "id": "adf4238a-882b-9ddc-4a9d-5b6758e4159e",
    "address": "127.0.0.1:123",
    "non_voter": false
  },
  {
    "id": "8b6dda82-3103-11e7-93ae-92361f002671",
    "address": "127.0.0.2:123"
  },
  {
    "id": "97e17742-3103-11e7-93ae-92361f002671",
    "address": "127.0.0.3:123",
    "non_voter": true
  }
]
`)
	peers := filepath.Join(base, "peers.json")
	if err = os.WriteFile(peers, content, 0o666); err != nil {
		t.Fatalf("err: %v", err)
	}

	var configuration Configuration
	configuration, err = ReadConfigJSON(peers)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	expected := Configuration{
		Servers: []Server{
			{
				Suffrage: Voter,
				ID:       ServerID("adf4238a-882b-9ddc-4a9d-5b6758e4159e"),
				Address:  ServerAddress("127.0.0.1:123"),
			},
			{
				Suffrage: Voter,
				ID:       ServerID("8b6dda82-3103-11e7-93ae-92361f002671"),
				Address:  ServerAddress("127.0.0.2:123"),
			},
			{
				Suffrage: Nonvoter,
				ID:       ServerID("97e17742-3103-11e7-93ae-92361f002671"),
				Address:  ServerAddress("127.0.0.3:123"),
			},
		},
	}
	if !reflect.DeepEqual(configuration, expected) {
		t.Fatalf("bad configuration: %+v != %+v", configuration, expected)
	}
}
