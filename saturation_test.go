package raft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSaturationMetric(t *testing.T) {
	t.Run("without smoothing", func(t *testing.T) {
		sat := newSaturationMetric([]string{"metric"}, 100*time.Millisecond)

		now := sat.lastReport
		sat.nowFn = func() time.Time { return now }

		var reported float32
		sat.reportFn = func(val float32) { reported = val }

		sat.sleeping()

		// First window: 50ms sleeping + 75ms working.
		now = now.Add(50 * time.Millisecond)
		sat.working()

		now = now.Add(75 * time.Millisecond)
		sat.sleeping()

		// Should be 60% saturation.
		require.Equal(t, float32(0.6), reported)

		// Second window: 90ms sleeping + 10ms working.
		now = now.Add(90 * time.Millisecond)
		sat.working()

		now = now.Add(10 * time.Millisecond)
		sat.sleeping()

		// Should be 10% saturation.
		require.Equal(t, float32(0.1), reported)

		// Third window: 100ms sleeping + 0ms working.
		now = now.Add(100 * time.Millisecond)
		sat.working()

		// Should be 0% saturation.
		require.Equal(t, float32(0), reported)
	})
}

func TestSaturationMetric_IncorrectUsage(t *testing.T) {
	t.Run("calling sleeping() consecutively", func(t *testing.T) {
		sat := newSaturationMetric([]string{"metric"}, 50*time.Millisecond)

		now := sat.lastReport
		sat.nowFn = func() time.Time { return now }

		var reported float32
		sat.reportFn = func(v float32) { reported = v }

		// Calling sleeping() consecutively should reset sleepBegan without recording
		// a sample, such that we "lose" time rather than recording nonsense data.
		//
		//   0   | sleeping() |
		//                     => Sleeping (10ms)
		// +10ms |  working() |
		//                     => Working  (10ms)
		// +20ms | sleeping() |
		//                     => [!] LOST [!] (10ms)
		// +30ms | sleeping() |
		//                     => Sleeping (10ms)
		// +40ms |  working() |
		//                     => Working (10ms)
		// +50ms | sleeping() |
		//
		// Total reportable time: 40ms. Saturation: 50%.
		sat.sleeping()
		now = now.Add(10 * time.Millisecond)
		sat.working()
		now = now.Add(10 * time.Millisecond)
		sat.sleeping()
		now = now.Add(10 * time.Millisecond)
		sat.sleeping()
		now = now.Add(10 * time.Millisecond)
		sat.working()
		now = now.Add(10 * time.Millisecond)
		sat.sleeping()

		require.Equal(t, float32(0.5), reported)
	})

	t.Run("calling working() consecutively", func(t *testing.T) {
		sat := newSaturationMetric([]string{"metric"}, 30*time.Millisecond)

		now := sat.lastReport
		sat.nowFn = func() time.Time { return now }

		var reported float32
		sat.reportFn = func(v float32) { reported = v }

		// Calling working() consecutively should reset workBegan without recording
		// a sample, such that we "lose" time rather than recording nonsense data.
		//
		//   0   | sleeping() |
		//                     => Sleeping (10ms)
		// +10ms |  working() |
		//                     => [!] LOST [!] (10ms)
		// +20ms |  working() |
		//                     => Working (10ms)
		// +30ms | sleeping() |
		//
		// Total reportable time: 20ms. Saturation: 50%.
		sat.sleeping()
		now = now.Add(10 * time.Millisecond)
		sat.working()
		now = now.Add(10 * time.Millisecond)
		sat.working()
		now = now.Add(10 * time.Millisecond)
		sat.sleeping()

		require.Equal(t, float32(0.5), reported)
	})

	t.Run("calling working() first", func(t *testing.T) {
		sat := newSaturationMetric([]string{"metric"}, 10*time.Millisecond)

		now := sat.lastReport
		sat.nowFn = func() time.Time { return now }

		var reported float32
		sat.reportFn = func(v float32) { reported = v }

		// Time from start until working() is treated as lost.
		sat.working()
		require.Equal(t, float32(0), reported)

		sat.sleeping()
		now = now.Add(5 * time.Millisecond)
		sat.working()
		now = now.Add(5 * time.Millisecond)
		sat.sleeping()
		require.Equal(t, float32(0.5), reported)
	})
}
