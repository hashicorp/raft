package raft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSaturationMetric(t *testing.T) {
	sat := newSaturationMetric([]string{"metric"}, 100*time.Millisecond)

	var now time.Time
	sat.nowFn = func() time.Time { return now }

	var reported float32
	sat.reportFn = func(val float32) { reported = val }

	now = time.Now()
	sat.sleeping()

	now = now.Add(50 * time.Millisecond)
	sat.working()

	now = now.Add(75 * time.Millisecond)
	sat.sleeping()

	require.Equal(t, float32(0.6), reported)

	now = now.Add(90 * time.Millisecond)
	sat.working()

	now = now.Add(10 * time.Millisecond)
	sat.sleeping()

	require.Equal(t, float32(0.1), reported)

	now = now.Add(100 * time.Millisecond)
	sat.working()

	require.Equal(t, float32(0), reported)
}

func TestSaturationMetric_IndexWraparound(t *testing.T) {
	sat := newSaturationMetric([]string{"metric"}, 100*time.Millisecond)

	now := time.Now()
	sat.nowFn = func() time.Time { return now }

	for i := 0; i < 1024; i++ {
		now = now.Add(25 * time.Millisecond)

		if i%2 == 0 {
			require.NotPanics(t, sat.sleeping)
		} else {
			require.NotPanics(t, sat.working)
		}
	}
}

func TestSaturationMetric_IncorrectUsage(t *testing.T) {
	t.Run("calling sleeping() consecutively", func(t *testing.T) {
		sat := newSaturationMetric([]string{"metric"}, 50*time.Millisecond)

		now := time.Now()
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

		now := time.Now()
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
}
