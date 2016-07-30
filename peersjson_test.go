package raft

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestPeersJSON_BadConfiguration(t *testing.T) {
	base, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer os.RemoveAll(base)

	peers := filepath.Join(base, "peers.json")
	if err := ioutil.WriteFile(peers, []byte("null"), 0666); err != nil {
		t.Fatalf("err: %v", err)
	}

	_, err = ReadPeersJSON(peers)
	if err == nil || !strings.Contains(err.Error(), "at least one voter") {
		t.Fatalf("err: %v", err)
	}
}

func Test_PeersJSON(t *testing.T) {
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

	configuration, err := ReadPeersJSON(peers)
	if err != nil {
		t.Fatalf("err: %v", err)
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
}
