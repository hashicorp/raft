package raft

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var sampleMembership Membership = Membership{
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

func TestMembership_Membership_Clone(t *testing.T) {
	cloned := sampleMembership.Clone()
	if !reflect.DeepEqual(sampleMembership, cloned) {
		t.Fatalf("mismatch %v %v", sampleMembership, cloned)
	}
	cloned.Servers[1].ID = "scribble"
	if sampleMembership.Servers[1].ID == "scribble" {
		t.Fatalf("cloned configuration shouldn't alias Servers")
	}
}

func TestMembership_Memberships_Clone(t *testing.T) {
	memberships := memberships{
		committed:      sampleMembership,
		committedIndex: 1,
		latest:         sampleMembership,
		latestIndex:    2,
	}
	cloned := memberships.Clone()
	if !reflect.DeepEqual(memberships, cloned) {
		t.Fatalf("mismatch %v %v", memberships, cloned)
	}
	cloned.committed.Servers[1].ID = "scribble"
	cloned.latest.Servers[1].ID = "scribble"
	if memberships.committed.Servers[1].ID == "scribble" ||
		memberships.latest.Servers[1].ID == "scribble" {
		t.Fatalf("cloned configuration shouldn't alias Servers")
	}
}

func TestMembership_hasVote(t *testing.T) {
	if hasVote(sampleMembership, "id0") {
		t.Fatalf("id0 should not have vote")
	}
	if !hasVote(sampleMembership, "id1") {
		t.Fatalf("id1 should have vote")
	}
	if hasVote(sampleMembership, "id2") {
		t.Fatalf("id2 should not have vote")
	}
	if hasVote(sampleMembership, "someotherid") {
		t.Fatalf("someotherid should not have vote")
	}
}

func TestMembership_checkMembership(t *testing.T) {
	var membership Membership
	if membership.check() == nil {
		t.Fatalf("empty configuration should be error")
	}

	membership.Servers = append(membership.Servers, Server{
		Suffrage: Nonvoter,
		ID:       ServerID("id0"),
		Address:  ServerAddress("addr0"),
	})
	if membership.check() == nil {
		t.Fatalf("lack of voter should be error")
	}

	membership.Servers = append(membership.Servers, Server{
		Suffrage: Voter,
		ID:       ServerID("id1"),
		Address:  ServerAddress("addr1"),
	})
	if err := membership.check(); err != nil {
		t.Fatalf("should be OK: %v", err)
	}

	membership.Servers[1].ID = "id0"
	err := membership.check()
	if err == nil {
		t.Fatalf("duplicate ID should be error")
	}
	if !strings.Contains(err.Error(), "duplicate ID") {
		t.Fatalf("unexpected error: %v", err)
	}
	membership.Servers[1].ID = "id1"

	membership.Servers[1].Address = "addr0"
	err = membership.check()
	if err == nil {
		t.Fatalf("duplicate address should be error")
	}
	if !strings.Contains(err.Error(), "duplicate address") {
		t.Fatalf("unexpected error: %v", err)
	}
}

var singleServer = Membership{
	Servers: []Server{
		Server{
			Suffrage: Voter,
			ID:       ServerID("id1"),
			Address:  ServerAddress("addr1"),
		},
	},
}

var oneOfEach = Membership{
	Servers: []Server{
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
		Server{
			Suffrage: Nonvoter,
			ID:       ServerID("id3"),
			Address:  ServerAddress("addr3"),
		},
	},
}

var voterPair = Membership{
	Servers: []Server{
		Server{
			Suffrage: Voter,
			ID:       ServerID("id1"),
			Address:  ServerAddress("addr1"),
		},
		Server{
			Suffrage: Voter,
			ID:       ServerID("id2"),
			Address:  ServerAddress("addr2"),
		},
	},
}

var nextMembershipTests = []struct {
	current  Membership
	command  MembershipChangeCommand
	serverID int
	next     string
}{
	// AddStaging: was missing.
	{Membership{}, AddStaging, 1, "[id1 at addr1 (Voter)]"},
	{singleServer, AddStaging, 2, "[id1 at addr1 (Voter), id2 at addr2 (Voter)]"},
	// AddStaging: was Voter.
	{singleServer, AddStaging, 1, "[id1 at addr1 (Voter)]"},
	// AddStaging: was Staging.
	{oneOfEach, AddStaging, 2, "[id1 at addr1 (Voter), id2 at addr2 (Voter), id3 at addr3 (Nonvoter)]"},
	// AddStaging: was Nonvoter.
	{oneOfEach, AddStaging, 3, "[id1 at addr1 (Voter), id2 at addr2 (Staging), id3 at addr3 (Voter)]"},

	// AddNonvoter: was missing.
	{singleServer, AddNonvoter, 2, "[id1 at addr1 (Voter), id2 at addr2 (Nonvoter)]"},
	// AddNonvoter: was Voter.
	{singleServer, AddNonvoter, 1, "[id1 at addr1 (Voter)]"},
	// AddNonvoter: was Staging.
	{oneOfEach, AddNonvoter, 2, "[id1 at addr1 (Voter), id2 at addr2 (Staging), id3 at addr3 (Nonvoter)]"},
	// AddNonvoter: was Nonvoter.
	{oneOfEach, AddNonvoter, 3, "[id1 at addr1 (Voter), id2 at addr2 (Staging), id3 at addr3 (Nonvoter)]"},

	// DemoteVoter: was missing.
	{singleServer, DemoteVoter, 2, "[id1 at addr1 (Voter)]"},
	// DemoteVoter: was Voter.
	{voterPair, DemoteVoter, 2, "[id1 at addr1 (Voter), id2 at addr2 (Nonvoter)]"},
	// DemoteVoter: was Staging.
	{oneOfEach, DemoteVoter, 2, "[id1 at addr1 (Voter), id2 at addr2 (Nonvoter), id3 at addr3 (Nonvoter)]"},
	// DemoteVoter: was Nonvoter.
	{oneOfEach, DemoteVoter, 3, "[id1 at addr1 (Voter), id2 at addr2 (Staging), id3 at addr3 (Nonvoter)]"},

	// RemoveServer: was missing.
	{singleServer, RemoveServer, 2, "[id1 at addr1 (Voter)]"},
	// RemoveServer: was Voter.
	{voterPair, RemoveServer, 2, "[id1 at addr1 (Voter)]"},
	// RemoveServer: was Staging.
	{oneOfEach, RemoveServer, 2, "[id1 at addr1 (Voter), id3 at addr3 (Nonvoter)]"},
	// RemoveServer: was Nonvoter.
	{oneOfEach, RemoveServer, 3, "[id1 at addr1 (Voter), id2 at addr2 (Staging)]"},

	// Promote: was missing.
	{singleServer, Promote, 2, "[id1 at addr1 (Voter)]"},
	// Promote: was Voter.
	{singleServer, Promote, 1, "[id1 at addr1 (Voter)]"},
	// Promote: was Staging.
	{oneOfEach, Promote, 2, "[id1 at addr1 (Voter), id2 at addr2 (Voter), id3 at addr3 (Nonvoter)]"},
	// Promote: was Nonvoter.
	{oneOfEach, Promote, 3, "[id1 at addr1 (Voter), id2 at addr2 (Staging), id3 at addr3 (Nonvoter)]"},
}

func TestMembership_nextMembership_table(t *testing.T) {
	for i, tt := range nextMembershipTests {
		req := membershipChangeRequest{
			command:       tt.command,
			serverID:      ServerID(fmt.Sprintf("id%d", tt.serverID)),
			serverAddress: ServerAddress(fmt.Sprintf("addr%d", tt.serverID)),
		}
		next, err := nextMembership(tt.current, 1, req)
		if err != nil {
			t.Errorf("nextMembership %d should have succeeded, got %v", i, err)
			continue
		}
		if fmt.Sprintf("%v", next) != tt.next {
			t.Errorf("nextMembership %d returned %v, expected %s", i, next, tt.next)
			continue
		}
	}
}

func TestMembership_nextMembership_prevIndex(t *testing.T) {
	// Stale prevIndex.
	req := membershipChangeRequest{
		command:       AddStaging,
		serverID:      ServerID("id1"),
		serverAddress: ServerAddress("addr1"),
		prevIndex:     1,
	}
	_, err := nextMembership(singleServer, 2, req)
	if err == nil || !strings.Contains(err.Error(), "changed") {
		t.Fatalf("nextMembership should have failed due to intervening configuration change")
	}

	// Current prevIndex.
	req = membershipChangeRequest{
		command:       AddStaging,
		serverID:      ServerID("id2"),
		serverAddress: ServerAddress("addr2"),
		prevIndex:     2,
	}
	_, err = nextMembership(singleServer, 2, req)
	if err != nil {
		t.Fatalf("nextMembership should have succeeded, got %v", err)
	}

	// Zero prevIndex.
	req = membershipChangeRequest{
		command:       AddStaging,
		serverID:      ServerID("id3"),
		serverAddress: ServerAddress("addr3"),
		prevIndex:     0,
	}
	_, err = nextMembership(singleServer, 2, req)
	if err != nil {
		t.Fatalf("nextMembership should have succeeded, got %v", err)
	}
}

func TestMembership_nextMembership_changeAddress_addStaging(t *testing.T) {
	req := membershipChangeRequest{
		command:       AddStaging,
		serverID:      ServerID("id1"),
		serverAddress: ServerAddress("addr1x"),
	}
	_, err := nextMembership(singleServer, 1, req)
	if err == nil || !strings.Contains(err.Error(), "May not change address") {
		t.Fatalf("nextMembership should have failed for attempting to change address")
	}
}

func TestMembership_nextMembership_changeAddress_addNonvoter(t *testing.T) {
	req := membershipChangeRequest{
		command:       AddNonvoter,
		serverID:      ServerID("id2"),
		serverAddress: ServerAddress("addr2x"),
	}
	_, err := nextMembership(voterPair, 1, req)
	if err == nil || !strings.Contains(err.Error(), "May not change address") {
		t.Fatalf("nextMembership should have failed for attempting to change address")
	}
}

func TestMembership_nextMembership_checkMembership(t *testing.T) {
	req := membershipChangeRequest{
		command:       AddNonvoter,
		serverID:      ServerID("id1"),
		serverAddress: ServerAddress("addr1"),
	}
	_, err := nextMembership(Membership{}, 1, req)
	if err == nil || !strings.Contains(err.Error(), "at least one voter") {
		t.Fatalf("nextMembership should have failed for not having a voter")
	}
}

func TestMembership_encodeDecodePeers(t *testing.T) {
	// Set up membership.
	var membership Membership
	for i := 0; i < 3; i++ {
		address := NewInmemAddr()
		membership.Servers = append(membership.Servers, Server{
			Suffrage: Voter,
			ID:       ServerID(address),
			Address:  ServerAddress(address),
		})
	}

	// Encode into the old format.
	_, trans := NewInmemTransport("")
	buf := encodePeers(membership, trans)

	// Decode from old format, as if reading an old log entry.
	decoded := decodePeers(buf, trans)
	if !reflect.DeepEqual(membership, decoded) {
		t.Fatalf("mismatch %v %v", membership, decoded)
	}
}

func TestMembership_encodeDecodeMembership(t *testing.T) {
	decoded := decodeMembership(encodeMembership(sampleMembership))
	if !reflect.DeepEqual(sampleMembership, decoded) {
		t.Fatalf("mismatch %v %v", sampleMembership, decoded)
	}
}
