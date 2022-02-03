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
	// reportInterval is the maximum frequency at which the gauge will be
	// updated (it may be less frequent if the caller is idle).
	reportInterval time.Duration

	totalSleep, lostTime  time.Duration
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
	now := s.nowFn()

	if !s.sleepBegan.IsZero() {
		s.lostTime += now.Sub(s.sleepBegan)
	}

	s.sleepBegan = now
	s.workBegan = time.Time{}
	s.report()
}

// working records the time at which the caller began working. It must always
// be proceeded by a call to sleeping.
func (s *saturationMetric) working() {
	now := s.nowFn()

	if s.workBegan.IsZero() {
		if s.sleepBegan.IsZero() {
			s.lostTime += now.Sub(s.lastReport)
		} else {
			s.totalSleep += now.Sub(s.sleepBegan)
		}
	} else {
		s.lostTime += now.Sub(s.workBegan)
	}

	s.workBegan = now
	s.sleepBegan = time.Time{}
	s.report()
}

// report updates the gauge if reportInterval has passed since our last report.
func (s *saturationMetric) report() {
	now := s.nowFn()
	timeElapsed := now.Sub(s.lastReport)

	if timeElapsed < s.reportInterval {
		return
	}

	total := timeElapsed - s.lostTime

	var saturation float32
	if total != 0 {
		saturation = float32(total-s.totalSleep) / float32(total)
	}
	s.reportFn(saturation)

	s.lastReport = now
	s.totalSleep = 0
	s.lostTime = 0
}
