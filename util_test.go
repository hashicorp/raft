package raft

import (
	"regexp"
	"testing"
	"time"
)

func TestRandomTimeout(t *testing.T) {
	start := time.Now()
	timeout := randomTimeout(time.Millisecond)
	defer timeout.Stop()

	select {
	case <-timeout.C:
		diff := time.Now().Sub(start)
		if diff < time.Millisecond {
			t.Fatalf("fired early")
		}
	case <-time.After(3 * time.Millisecond):
		t.Fatalf("timeout")
	}
}

func TestNewSeed(t *testing.T) {
	vals := make(map[int64]bool)
	for i := 0; i < 1000; i++ {
		seed := newSeed()
		if _, exists := vals[seed]; exists {
			t.Fatal("newSeed() return a value it'd previously returned")
		}
		vals[seed] = true
	}
}

func TestMin(t *testing.T) {
	if min(1, 1) != 1 {
		t.Fatalf("bad min")
	}
	if min(2, 1) != 1 {
		t.Fatalf("bad min")
	}
	if min(1, 2) != 1 {
		t.Fatalf("bad min")
	}
}

func TestMax(t *testing.T) {
	if max(1, 1) != 1 {
		t.Fatalf("bad max")
	}
	if max(2, 1) != 2 {
		t.Fatalf("bad max")
	}
	if max(1, 2) != 2 {
		t.Fatalf("bad max")
	}
}

func TestGenerateUUID(t *testing.T) {
	prev := generateUUID()
	for i := 0; i < 100; i++ {
		id := generateUUID()
		if prev == id {
			t.Fatalf("Should get a new ID!")
		}

		matched, err := regexp.MatchString(
			`[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}`, id)
		if !matched || err != nil {
			t.Fatalf("expected match %s %v %s", id, matched, err)
		}
	}
}

func TestBackoff(t *testing.T) {
	b := backoff(10*time.Millisecond, 1, 8)
	if b != 10*time.Millisecond {
		t.Fatalf("bad: %v", b)
	}

	b = backoff(20*time.Millisecond, 2, 8)
	if b != 20*time.Millisecond {
		t.Fatalf("bad: %v", b)
	}

	b = backoff(10*time.Millisecond, 8, 8)
	if b != 640*time.Millisecond {
		t.Fatalf("bad: %v", b)
	}

	b = backoff(10*time.Millisecond, 9, 8)
	if b != 640*time.Millisecond {
		t.Fatalf("bad: %v", b)
	}
}

func TestOverrideNotifyBool(t *testing.T) {
	ch := make(chan bool, 1)

	// sanity check - buffered channel don't have any values
	select {
	case v := <-ch:
		t.Fatalf("unexpected receive: %v", v)
	default:
	}

	// simple case of a single push
	overrideNotifyBool(ch, false)
	select {
	case v := <-ch:
		if v != false {
			t.Fatalf("expected false but got %v", v)
		}
	default:
		t.Fatalf("expected a value but is not ready")
	}

	// assert that function never blocks and only last item is received
	overrideNotifyBool(ch, false)
	overrideNotifyBool(ch, false)
	overrideNotifyBool(ch, false)
	overrideNotifyBool(ch, false)
	overrideNotifyBool(ch, true)

	select {
	case v := <-ch:
		if v != true {
			t.Fatalf("expected true but got %v", v)
		}
	default:
		t.Fatalf("expected a value but is not ready")
	}

	// no further value is available
	select {
	case v := <-ch:
		t.Fatalf("unexpected receive: %v", v)
	default:
	}
}

func TestTimer(t *testing.T) {
	t.Run("NoTimeout", func(t *testing.T) {
		timer := newTimer(0)
		defer timer.Stop()

		select {
		case <-timer.C:
			t.Fatalf("timer is invalid")
		case <-time.After(50 * time.Millisecond):
		}
	})

	t.Run("Timeout", func(t *testing.T) {
		timer := newTimer(10 * time.Millisecond)
		defer timer.Stop()
		defer timer.Stop() // check if we able to stop twice

		select {
		case <-timer.C:
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("invalid timeout")
		}

		timer.Reset(100 * time.Millisecond)
		if timer.C != timer.t.C { // check the reference after reset
			t.FailNow()
		}

		select {
		case <-timer.C:
			t.Fatalf("invalid timeout")
		case <-time.After(20 * time.Millisecond):
		}
	})
}

const timerBenchAttempt = 10000

// goos: linux
// goarch: amd64
// pkg: github.com/hashicorp/raft
// BenchmarkTimeAfter-16                295           3968892 ns/op         2100006 B/op      30000 allocs/op
// BenchmarkTimer-16                    451           2738084 ns/op         2000056 B/op      30000 allocs/op

func BenchmarkTimeAfter(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for attempt := 0; attempt < timerBenchAttempt; attempt++ {
			select {
			case <-time.After(50 * time.Millisecond):
			default:
			}
		}
	}
}

func BenchmarkTimer(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for attempt := 0; attempt < timerBenchAttempt; attempt++ {
			timer := newTimer(50 * time.Millisecond)
			select {
			case <-timer.C:
			default:
				timer.Stop()
			}
		}
	}
}
