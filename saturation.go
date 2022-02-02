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

	// index of the current sample. We rely on it wrapping-around on overflow and
	// underflow, to implement a circular buffer (note the matching size of the
	// samples array).
	index uint8

	// samples is a fixed-size array of samples (similar to a circular buffer)
	// where elements at even-numbered indexes contain time spent sleeping, and
	// elements at odd-numbered indexes contain time spent working.
	samples [math.MaxUint8 + 1]struct {
		v time.Duration // measurement
		t time.Time     // time at which the measurement was captured
	}

	sleepBegan, workBegan time.Time
	lastReport            time.Time

	// These are overwritten in tests.
	nowFn    func() time.Time
	reportFn func(float32)
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
	s.sleepBegan = s.nowFn()

	// Caller should've called working (we've probably missed a branch of a
	// select). Reset sleepBegan without recording a sample to "lose" time
	// instead of recording nonsense data.
	if s.index%2 == 1 {
		return
	}

	if !s.workBegan.IsZero() {
		sample := &s.samples[s.index-1]
		sample.v = s.sleepBegan.Sub(s.workBegan)
		sample.t = s.sleepBegan
	}

	s.index++
	s.report()
}

// working records the time at which the caller began working. It must always
// be proceeded by a call to sleeping.
func (s *saturationMetric) working() {
	s.workBegan = s.nowFn()

	// Caller should've called sleeping. Reset workBegan without recording a
	// sample to "lose" time instead of recording nonsense data.
	if s.index%2 == 0 {
		return
	}

	sample := &s.samples[s.index-1]
	sample.v = s.workBegan.Sub(s.sleepBegan)
	sample.t = s.workBegan

	s.index++
	s.report()
}

// report updates the gauge if reportInterval has passed since our last report.
func (s *saturationMetric) report() {
	if s.nowFn().Sub(s.lastReport) < s.reportInterval {
		return
	}

	workSamples := make([]time.Duration, 0, len(s.samples)/2)
	sleepSamples := make([]time.Duration, 0, len(s.samples)/2)

	for idx, sample := range s.samples {
		if !sample.t.After(s.lastReport) {
			continue
		}

		if idx%2 == 0 {
			sleepSamples = append(sleepSamples, sample.v)
		} else {
			workSamples = append(workSamples, sample.v)
		}
	}

	// Ensure we take an equal number of work and sleep samples to avoid
	// over/under reporting.
	take := len(workSamples)
	if len(sleepSamples) < take {
		take = len(sleepSamples)
	}

	var saturation float32
	if take != 0 {
		var work, sleep time.Duration
		for _, s := range workSamples[0:take] {
			work += s
		}
		for _, s := range sleepSamples[0:take] {
			sleep += s
		}
		saturation = float32(work) / float32(work+sleep)
	}

	s.reportFn(saturation)
	s.lastReport = s.nowFn()
}
