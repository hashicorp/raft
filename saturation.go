package raft

import (
	"math"
	"time"

	"github.com/armon/go-metrics"
)

// saturationMetric measures the saturation (percentage of time spent working vs
// waiting for work) of an event processing loop, such as runFSM. It reports the
// saturation as a gauge metric (at most) once every reportInterval.
//
// Callers must instrument their loop with calls to sleeping and working, starting
// with a call to sleeping.
//
// Note: it is expected that the caller is single-threaded and is not safe for
// concurrent use by multiple goroutines.
type saturationMetric struct {
	// reportInterval is the maximum frequency at which the gauge will be
	// updated (it may be less frequent if the caller is idle).
	reportInterval time.Duration

	// samples is a fixed-size array of samples (similar to a circular buffer)
	// where elements at even-numbered indexes contain time spent sleeping, and
	// elements at odd-numbered indexes contain time spent working.
	samples [math.MaxUint8 + 1]sample

	sleepBegan time.Time
	lastReport time.Time

	// These are overwritten in tests.
	nowFn    func() time.Time
	reportFn func(float32)
}

type sample struct {
	v time.Duration // measurement
	t time.Time     // time at which the measurement was captured
}

// newSaturationMetric creates a saturationMetric that will update the gauge
// with the given name at the given reportInterval.
func newSaturationMetric(name []string, reportInterval time.Duration) *saturationMetric {
	return &saturationMetric{
		reportInterval: reportInterval,
		lastReport:     time.Now(), // Set to now to avoid reporting immediately.
		nowFn:          time.Now,
		reportFn:       func(sat float32) { metrics.SetGauge(name, sat) },
	}
}

// sleeping records the time at which the caller began waiting for work. After
// the initial call it must always be proceeded by a call to working.
func (s *saturationMetric) sleeping() {
	s.report()
	s.sleepBegan = s.nowFn()
}

// working records the time at which the caller began working. It must always
// be proceeded by a call to sleeping.
func (s *saturationMetric) working() {
	now := s.nowFn()

	index := now.UnixNano() / int64(10*time.Millisecond) % math.MaxUint8
	sample := &s.samples[index]
	sample.v += now.Sub(s.sleepBegan)
	if sample.t.IsZero() {
		sample.t = s.sleepBegan
	}
}

// report updates the gauge if reportInterval has passed since our last report.
func (s *saturationMetric) report() {
	now := s.nowFn()
	if now.Sub(s.lastReport) < s.reportInterval {
		return
	}

	// TODO: if we use time we don't need to iterate, we can build a slice
	// using indexes.
	var first sample
	var sleepSamples []sample
	for _, sleepSample := range s.samples {
		if sleepSample.t.After(s.lastReport) {
			sleepSamples = append(sleepSamples, sleepSample)

			if first.t.IsZero() || sleepSample.t.Before(first.t) {
				first = sleepSample
			}
		}
	}

	var saturation float32

	if first.v != 0 {
		var sleep time.Duration
		for _, s := range sleepSamples {
			sleep += s.v
		}
		total := now.Sub(first.t)
		saturation = float32(total-sleep) / float32(total)
	}

	s.reportFn(saturation)
	s.lastReport = s.nowFn()
}
