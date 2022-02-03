package raft

import (
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
	reportInterval time.Duration

	// buckets contains measurements for both the current, and a configurable
	// number of previous periods (to smooth out spikes).
	buckets       []saturationMetricBucket
	currentBucket int

	sleepBegan, workBegan time.Time

	// These are overwritten in tests.
	nowFn    func() time.Time
	reportFn func(float32)
}

// saturationMetricBucket contains the measurements for a period.
type saturationMetricBucket struct {
	// beginning of the period for which this bucket contains measurements.
	beginning time.Time

	// slept contains time for which the event processing loop was sleeping rather
	// than working.
	slept time.Duration

	// lost contains time that was lost due to incorrect instrumentation of the
	// event processing loop (e.g. calling sleeping() or working() multiple times
	// in succession).
	lost time.Duration
}

// newSaturationMetric creates a saturationMetric that will update the gauge
// with the given name at the given reportInterval. keepPrev determines the
// number of previous measurements that will be used to smooth out spikes.
func newSaturationMetric(name []string, reportInterval time.Duration, keepPrev uint) *saturationMetric {
	m := &saturationMetric{
		reportInterval: reportInterval,
		buckets:        make([]saturationMetricBucket, 1+keepPrev),
		nowFn:          time.Now,
		reportFn:       func(sat float32) { metrics.SetGauge(name, sat) },
	}
	m.buckets[0].beginning = time.Now()
	return m
}

// sleeping records the time at which the loop began waiting for work. After the
// initial call it must always be proceeded by a call to working.
func (s *saturationMetric) sleeping() {
	now := s.nowFn()

	if !s.sleepBegan.IsZero() {
		// sleeping called twice in succession. Count that time as lost rather than
		// measuring nonsense.
		s.buckets[s.currentBucket].lost += now.Sub(s.sleepBegan)
	}

	s.sleepBegan = now
	s.workBegan = time.Time{}
	s.report()
}

// working records the time at which the loop began working. It must always be
// proceeded by a call to sleeping.
func (s *saturationMetric) working() {
	now := s.nowFn()
	bucket := &s.buckets[s.currentBucket]

	if s.workBegan.IsZero() {
		if s.sleepBegan.IsZero() {
			// working called before the initial call to sleeping. Count that time as
			// lost rather than measuring nonsense.
			bucket.lost += now.Sub(bucket.beginning)
		} else {
			bucket.slept += now.Sub(s.sleepBegan)
		}
	} else {
		// working called twice in succession. Count that time as lost rather than
		// measuring nonsense.
		bucket.lost += now.Sub(s.workBegan)
	}

	s.workBegan = now
	s.sleepBegan = time.Time{}
	s.report()
}

// report updates the gauge if reportInterval has passed since our last report.
func (s *saturationMetric) report() {
	now := s.nowFn()
	timeSinceLastReport := now.Sub(s.buckets[s.currentBucket].beginning)

	if timeSinceLastReport < s.reportInterval {
		return
	}

	var (
		beginning   time.Time
		slept, lost time.Duration
	)
	for _, bucket := range s.buckets {
		if bucket.beginning.IsZero() {
			continue
		}

		if beginning.IsZero() || bucket.beginning.Before(beginning) {
			beginning = bucket.beginning
		}

		slept += bucket.slept
		lost += bucket.lost
	}

	total := now.Sub(beginning) - lost

	var saturation float32
	if total != 0 {
		saturation = float32(total-slept) / float32(total)
	}
	s.reportFn(saturation)

	s.currentBucket++
	if s.currentBucket >= len(s.buckets) {
		s.currentBucket = 0
	}

	bucket := &s.buckets[s.currentBucket]
	bucket.slept = 0
	bucket.lost = 0
	bucket.beginning = now
}
