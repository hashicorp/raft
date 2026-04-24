package main

import (
	"os"
	"strings"

	"github.com/hashicorp/raft"
	cbg "github.com/whyrusleeping/cbor-gen"
)

func main() {
	err := cbg.WriteTupleEncodersToFile("cbor_gen.go", "raft",
		raft.AppendEntriesRequest{},
		raft.AppendEntriesResponse{},
		raft.RequestVoteRequest{},
		raft.RequestVoteResponse{},
		raft.RequestPreVoteRequest{},
		raft.RequestPreVoteResponse{},
		raft.InstallSnapshotRequest{},
		raft.InstallSnapshotResponse{},
		raft.TimeoutNowRequest{},
		raft.TimeoutNowResponse{},
		raft.RPCHeader{},
		raft.Log{},
		raft.TimeBytes{},
		raft.String{},
	)
	if err != nil {
		panic(err)
	}

	// Apply time.patch replacements after generating cbor_gen.go
	content, err := os.ReadFile("cbor_gen.go")
	if err != nil {
		panic(err)
	}

	// Apply the two replacements from time.patch
	modified := string(content)
	modified = strings.ReplaceAll(modified, "if err := t.AppendedAt.MarshalCBOR(cw); err != nil {", "if err := TimeMarshalCBOR(&t.AppendedAt, cw); err != nil {")
	modified = strings.ReplaceAll(modified, "if err := t.AppendedAt.UnmarshalCBOR(cr); err != nil {", "if err := TimeUnmarshalCBOR(&t.AppendedAt, cr); err != nil {")

	err = os.WriteFile("cbor_gen.go", []byte(modified), 0644)
	if err != nil {
		panic(err)
	}
}
