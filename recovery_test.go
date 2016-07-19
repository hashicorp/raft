package raft

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

type MockRecovery struct {
	errors  int
	eventCh chan string
}

func (m *MockRecovery) Override(latest Configuration, latestIndex uint64) (Configuration, bool) {
	return Configuration{}, false
}

func (m *MockRecovery) Disarm() error {
	if m.errors > 0 {
		m.errors--
		m.eventCh <- "error"
		return fmt.Errorf("error")
	}

	m.eventCh <- "disarm"
	return nil
}

func TestRecovery_Goroutine(t *testing.T) {
	eventCh := make(chan string, 10)
	mock := &MockRecovery{
		errors:  1,
		eventCh: eventCh,
	}

	logger := log.New(os.Stderr, "", log.LstdFlags)
	recoveryCh := make(chan struct{})
	shutdownCh := make(chan struct{})
	waitForEvent := func(expected string) {
		deadline := time.Now().Add(1 * time.Second)
		for {
			select {
			case event := <-eventCh:
				if event != expected {
					t.Fatalf("bad: expected %s, got %s", expected, event)
				}
				return

			case <-time.After(10 * time.Millisecond):
				if time.Now().After(deadline) {
					t.Fatalf("bad: timed out waiting for %s", expected)
				}
				asyncNotifyCh(recoveryCh)
			}
		}
	}

	// Try a case where the first disarm fails, then finally works.
	go func() {
		runRecovery(logger, mock, recoveryCh, shutdownCh)
		eventCh <- "exit"
	}()
	waitForEvent("error")
	waitForEvent("disarm")
	waitForEvent("exit")

	// Try the shutdown channel.
	go func() {
		runRecovery(logger, mock, recoveryCh, shutdownCh)
		eventCh <- "exit"
	}()
	close(shutdownCh)
	waitForEvent("exit")
}

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
