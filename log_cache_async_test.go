package raft

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLogCacheAsyncBasics(t *testing.T) {
	underlying := NewInmemStore()

	// Set it to behave monotonically to ensure we only ever write the necessary
	// logs in correct sequence and not re-write the old ones.
	underlying.SetMonotonic(true)

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

	// Wait for the completion event (note that we might get one completion for
	// both writes or separate ones depending on timing).
	assertOKCompletions(t, compCh, 8, 10)

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

	// We should get the write error reported on the next completion.
	lwc := expectCompletion(t, compCh)
	require.ErrorContains(t, lwc.Error, "IO error")
	// Persistent index should be unchanged since the flush failed
	require.Equal(t, 10, int(lwc.PersistentIndex))

	// But then eventually the write should succeed even if no more writes happen.
	// We might see just one or both writes flush together.
	assertOKCompletions(t, compCh, 12, 15)

	assertLogContents(t, underlying, 1, 15)
}

func expectCompletion(t *testing.T, compCh <-chan LogWriteCompletion) LogWriteCompletion {
	t.Helper()
	select {
	case lwc := <-compCh:
		return lwc
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for IO completion")
	}
	return LogWriteCompletion{}
}

// assertOKCompletions helps to assert that one or more non-error completions
// are received. The max index written in  each call to StoreLogsAsync should be
// passed since it's non deterministic which ones are persisted together. They
// must be in ascending order. We will assert that a completion arrives in a
// timely manner, and that it's for a valid prefix of the batches written. If
// it's not all of them, we keep waiting for the rest recursively.
func assertOKCompletions(t *testing.T, compCh <-chan LogWriteCompletion, maxBatchIndexes ...int) {
	t.Helper()
	lwc := expectCompletion(t, compCh)
	require.NoError(t, lwc.Error)

	foundBatchIdx := -1
	for i, idx := range maxBatchIndexes {
		if int(lwc.PersistentIndex) == idx {
			foundBatchIdx = i
			break
		}
	}
	require.GreaterOrEqual(t, foundBatchIdx, 0,
		"unexpected persistent index in completion: %d, wanted one of %v",
		lwc.PersistentIndex,
		maxBatchIndexes,
	)

	if foundBatchIdx < len(maxBatchIndexes)-1 {
		// We didn't get all the batches acknowledged yet, keep waiting.
		assertOKCompletions(t, compCh, maxBatchIndexes[foundBatchIdx+1:]...)
	}
}

func assertLogContents(t *testing.T, s LogStore, min, max int) {
	t.Helper()

	// It's easier to debug if we can see the actual contents visually so instead
	// of just asserting things match peicemeal, build a humaan-readable dump and
	// check it matches expectations.
	var expected, got []string

	// Ensure that the min and max are the actual range the log contains!
	first, err := s.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, min, int(first))
	last, err := s.LastIndex()
	require.NoError(t, err)
	require.Equal(t, max, int(last))

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
