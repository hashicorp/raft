// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"testing"
	"time"
)

// TestPipelineDecode_EarlyReturnUnblocksSender checks that when pipelineDecode
// stops consuming responses (on an unsuccessful response, or on a newer term),
// a sender blocked in AppendEntries is released instead of waiting forever.
//
// With NetworkTransport and the default MaxRPCsInFlight (2), the pipeline's
// in-progress channel has no buffer, so a send needs decodeResponses to be
// receiving; once pipelineDecode returns, decodeResponses blocks handing the
// next response to doneCh, and nothing closes the pipeline except
// pipelineReplicate's deferred Close, which runs only after the send returns.
// See hashicorp/raft#612.
func TestPipelineDecode_EarlyReturnUnblocksSender(t *testing.T) {
	cases := []struct {
		name string
		resp func(n int) *AppendEntriesResponse // the follower's answer to its n-th AppendEntries
	}{
		{"not successful", func(n int) *AppendEntriesResponse {
			return &AppendEntriesResponse{Term: 1, Success: n > 1}
		}},
		{"newer term", func(n int) *AppendEntriesResponse {
			if n == 1 {
				return &AppendEntriesResponse{Term: 2, Success: false}
			}
			return &AppendEntriesResponse{Term: 1, Success: true}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fol, err := NewTCPTransportWithLogger("127.0.0.1:0", nil, 2, time.Second, newTestLogger(t))
			if err != nil {
				t.Fatal(err)
			}
			defer fol.Close()
			go func() {
				n := 0
				for rpc := range fol.Consumer() {
					n++
					rpc.Respond(tc.resp(n), nil)
				}
			}()

			lead, err := NewTCPTransportWithLogger("127.0.0.1:0", nil, 2, time.Second, newTestLogger(t))
			if err != nil {
				t.Fatal(err)
			}
			defer lead.Close()
			p, err := lead.AppendEntriesPipeline("follower", fol.LocalAddr())
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()

			r := &Raft{logger: newTestLogger(t)}
			s := &followerReplication{
				peer:     Server{ID: "follower", Address: fol.LocalAddr()},
				notify:   make(map[*verifyFuture]struct{}),
				stepDown: make(chan struct{}, 1),
			}
			stopCh, finishCh := make(chan struct{}), make(chan struct{})
			defer close(stopCh)
			go r.pipelineDecode(s, p, stopCh, finishCh)

			// A sender shaped like pipelineReplicate's loop: it checks
			// finishCh only between sends.
			done := make(chan struct{})
			go func() {
				defer close(done)
				for i := uint64(1); i <= 50; i++ {
					select {
					case <-finishCh:
						return
					default:
					}
					req := &AppendEntriesRequest{RPCHeader: RPCHeader{Addr: []byte(lead.LocalAddr())},
						Term: 1, PrevLogEntry: i - 1, PrevLogTerm: 1}
					if _, err := p.AppendEntries(req, new(AppendEntriesResponse)); err != nil {
						return
					}
				}
			}()

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("sender still blocked in AppendEntries 5s after pipelineDecode returned")
			}
		})
	}
}
