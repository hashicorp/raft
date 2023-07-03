// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

//go:build batchtest
// +build batchtest

package fuzzy

import "github.com/hashicorp/raft"

// ApplyBatch enables fuzzyFSM to satisfy the BatchingFSM interface. This
// function is gated by the batchtest build flag.
func (f *fuzzyFSM) ApplyBatch(logs []*raft.Log) []interface{} {
	ret := make([]interface{}, len(logs))

	for _, l := range logs {
		f.Apply(l)
	}

	return ret
}
