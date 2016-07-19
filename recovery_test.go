package raft

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestRecovery_PeersJSON_BadConfiguration(t *testing.T) {
	base, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer os.RemoveAll(base)

	peers := filepath.Join(base, "peers.json")
	if err := ioutil.WriteFile(peers, []byte("null"), 0666); err != nil {
		t.Fatalf("err: %v", err)
	}

	_, err = NewPeersJSONRecovery(base)
	if err == nil || !strings.Contains(err.Error(), "at least one voter") {
		t.Fatalf("err: %v", err)
	}
}

func TestRecovery_PeersJSON(t *testing.T) {
	base, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer os.RemoveAll(base)

	content := []byte("[\"127.0.0.1:123\", \"127.0.0.2:123\", \"127.0.0.3:123\"]")
	peers := filepath.Join(base, "peers.json")
	if err := ioutil.WriteFile(peers, content, 0666); err != nil {
		t.Fatalf("err: %v", err)
	}

	recovery, err := NewPeersJSONRecovery(base)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	configuration, ok := recovery.Override(Configuration{}, 0)
	if !ok {
		t.Fatalf("bad: should have an override")
	}

	expected := Configuration{
		Servers: []Server{
			Server{
				Suffrage: Voter,
				ID:       ServerID("127.0.0.1:123"),
				Address:  ServerAddress("127.0.0.1:123"),
			},
			Server{
				Suffrage: Voter,
				ID:       ServerID("127.0.0.2:123"),
				Address:  ServerAddress("127.0.0.2:123"),
			},
			Server{
				Suffrage: Voter,
				ID:       ServerID("127.0.0.3:123"),
				Address:  ServerAddress("127.0.0.3:123"),
			},
		},
	}
	if !reflect.DeepEqual(configuration, expected) {
		t.Fatalf("bad configuration: %+v != %+v", configuration, expected)
	}

	peersExists := func() bool {
		_, err := os.Stat(peers)
		if err == nil {
			return true
		} else if os.IsNotExist(err) {
			return false
		} else {
			t.Fatalf("bad: problem checking peers file: %v", err)
			return false
		}
	}
	if !peersExists() {
		t.Fatalf("peers file should exist")
	}
	if err := recovery.Disarm(); err != nil {
		t.Fatalf("err: %v", err)
	}
	if peersExists() {
		t.Fatalf("peers file should no longer exist")
	}
}
