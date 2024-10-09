// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	metrics "github.com/armon/go-metrics"
)

func TestOldestLog(t *testing.T) {
	cases := []struct {
		Name    string
		Logs    []*Log
		WantIdx uint64
		WantErr bool
	}{
		{
			Name:    "empty logs",
			Logs:    nil,
			WantIdx: 0,
			WantErr: true,
		},
		{
			Name: "simple case",
			Logs: []*Log{
				{
					Index: 1234,
					Term:  1,
				},
				{
					Index: 1235,
					Term:  1,
				},
				{
					Index: 1236,
					Term:  2,
				},
			},
			WantIdx: 1234,
			WantErr: false,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			s := NewInmemStore()
			if err := s.StoreLogs(tc.Logs); err != nil {
				t.Fatalf("expected store logs not to fail: %s", err)
			}

			got, err := oldestLog(s)
			switch {
			case tc.WantErr && err == nil:
				t.Fatalf("wanted error got nil")
			case !tc.WantErr && err != nil:
				t.Fatalf("wanted no error got: %s", err)
			}

			if got.Index != tc.WantIdx {
				t.Fatalf("got index %v, want %v", got.Index, tc.WantIdx)
			}
		})
	}
}

func TestEmitsLogStoreMetrics(t *testing.T) {
	sink := testSetupMetrics(t)

	start := time.Now()

	s := NewInmemStore()
	logs := []*Log{
		{
			Index:      1234,
			Term:       1,
			AppendedAt: time.Now(),
		},
		{
			Index: 1235,
			Term:  1,
		},
		{
			Index: 1236,
			Term:  2,
		},
	}
	if err := s.StoreLogs(logs); err != nil {
		t.Fatalf("expected store logs not to fail: %s", err)
	}

	stopCh := make(chan struct{})
	defer close(stopCh)

	go emitLogStoreMetrics(s, []string{"foo"}, time.Millisecond, stopCh)

	// Wait for at least one interval
	time.Sleep(5 * time.Millisecond)

	got := getCurrentGaugeValue(t, sink, "raft.test.foo.oldestLogAge")

	// Assert the age is in a reasonable range.
	if got > float32(time.Since(start).Milliseconds()) {
		t.Fatalf("max age before test start: %v", got)
	}

	if got < 1 {
		t.Fatalf("max age less than interval: %v", got)
	}
}

func testSetupMetrics(t *testing.T) *metrics.InmemSink {
	// Record for ages (5 mins) so we can be confident that our assertions won't
	// fail on silly long test runs due to dropped data.
	s := metrics.NewInmemSink(10*time.Second, 300*time.Second)
	cfg := metrics.DefaultConfig("raft.test")
	cfg.EnableHostname = false
	metrics.NewGlobal(cfg, s)
	return s
}

func getCurrentGaugeValue(t *testing.T, sink *metrics.InmemSink, name string) float32 {
	t.Helper()

	data := sink.Data()

	// Loop backward through intervals until there is a non-empty one
	// Addresses flakiness around recording to one interval but accessing during the next
	for i := len(data) - 1; i >= 0; i-- {
		currentInterval := data[i]

		currentInterval.RLock()
		if gv, ok := currentInterval.Gauges[name]; ok {
			currentInterval.RUnlock()
			return gv.Value
		}
		currentInterval.RUnlock()
	}

	// Debug print all the gauges
	buf := bytes.NewBuffer(nil)
	for _, intv := range data {
		intv.RLock()
		for name, val := range intv.Gauges {
			fmt.Fprintf(buf, "[%v][G] '%s': %0.3f\n", intv.Interval, name, val.Value)
		}
		intv.RUnlock()
	}
	t.Log(buf.String())

	t.Fatalf("didn't find gauge %q", name)
	return 0
}
