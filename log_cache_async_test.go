package raft

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLogCacheAsyncBasics(t *testing.T) {
	underlying := NewInmemStore()

	c, err := NewLogCacheAsync(32, underlying)
	require.NoError(t, err)

	// Starts in sync mode and can write some logs in sync mode.
	require.NoError(t, c.StoreLogs(makeLogs(1, 5)))
	assertLogContents(t, c, 1, 5)

	// Async appends should error
	require.ErrorContains(t, c.StoreLogsAsync(makeLogs(6, 10)), "in sync mode")
	assertLogContents(t, c, 1, 5)

	// Switch to Async mode
	compCh := make(chan LogWriteCompletion, 1)
	c.EnableAsync(compCh)

	// Sync appends should error
	require.ErrorContains(t, c.StoreLogs(makeLogs(6, 10)), "in async mode")
	assertLogContents(t, c, 1, 5)

	// Block up the underlying store indefinitely
	blockCancel := underlying.BlockStore()

	timeout := time.AfterFunc(100*time.Millisecond, func() {
		blockCancel()
		t.Fatal("timeout")
	})

	// (multiple) Async appends should work and return even though the underlying store is
	// not done syncing to disk yet.
	require.NoError(t, c.StoreLogsAsync(makeLogs(6, 8)))
	require.NoError(t, c.StoreLogsAsync(makeLogs(9, 10)))
	timeout.Stop()

	// The async log should "see" those updates
	assertLogContents(t, c, 1, 10)

	// But the underlying log doesn't have them yet
	assertLogContents(t, underlying, 1, 5)

	// Unblock the write on the underlying log
	blockCancel()

	// Wait for the completion event
	select {
	case lwc := <-compCh:
		require.NoError(t, lwc.Error)
		// Should get a single completion for all logs up to 10
		require.Equal(t, 10, int(lwc.PersistentIndex))
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for IO completion")
	}

	// Now the underlying should have all the logs
	assertLogContents(t, underlying, 1, 10)

	// Write some more async
	blockCancel = underlying.BlockStore()
	timeout = time.AfterFunc(100*time.Millisecond, func() {
		blockCancel()
		t.Fatal("timeout")
	})

	// (multiple) Async appends should work and return even though the underlying store is
	// not done syncing to disk yet.
	require.NoError(t, c.StoreLogsAsync(makeLogs(11, 12)))
	require.NoError(t, c.StoreLogsAsync(makeLogs(13, 15)))
	timeout.Stop()

	assertLogContents(t, c, 1, 15)

	// Fail the underlying write
	underlying.FailNext()
	blockCancel()
}

// TODO:
//   yt

func assertLogContents(t *testing.T, s LogStore, min, max int) {
	t.Helper()

	// It's easier to debug if we can see the actual contents visually so instead
	// of just asserting things match peicemeal, build a humaan-readable dump and
	// check it matches expectations.
	var expected, got []string

	var log Log
	for idx := min; idx <= max; idx++ {
		expected = append(expected, fmt.Sprintf("%d => op-%d", idx, idx))

		var gotS string
		if err := s.GetLog(uint64(idx), &log); err != nil {
			gotS = fmt.Sprintf("%d => <err: %s>", idx, err)
		} else {
			gotS = fmt.Sprintf("%d => %s", log.Index, log.Data)
		}
		got = append(got, gotS)
	}
	require.Equal(t, expected, got)
}

func makeLog(idx int) *Log {
	return &Log{
		Index: uint64(idx),
		Data:  []byte(fmt.Sprintf("op-%d", idx)),
	}
}

func makeLogs(min, max int) []*Log {
	logs := make([]*Log, 0, max-min)
	for idx := min; idx <= max; idx++ {
		logs = append(logs, makeLog(idx))
	}
	return logs
}
