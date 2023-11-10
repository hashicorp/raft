package raft

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// LogCacheAsync is like LogCache but implements LogStoreAsync, allowing for
// writing and flush to disk in background and in larger batches than written
// through group commit.
type LogCacheAsync struct {
	// These first fields are accessed atomically and are first to ensure they
	// stay 64 bit aligned. They are only updated with state locked but are read
	// without holding the mutex by concurrent readers.

	// lastIndex is the highest readable Log index.
	lastIndex uint64

	// persistentIndex is the highest index known to be safely synced to the
	// underlying store.
	persistentIndex uint64

	// Static params
	store     LogStore
	monotonic bool
	size      uint64
	sizeMask  uint64

	// mu protects the mutable state
	state struct {
		sync.Mutex

		// cache is a ring buffer with static size which must be a power of two.
		cache []*Log

		// completionCh is non-nil after EnableAsync is called and canonically
		// defines whether we are in async or sync mode.
		completionCh chan<- LogWriteCompletion

		// triggerChan is 1-buffered to be used as an edge trigger for the
		// background sync.
		triggerChan chan syncRequest
	}
}

type syncRequest struct {
	startTime time.Time
	stop      bool
	doneCh    chan struct{}
}

// NewLogCacheAsync creates an async cache on top of store. The capacity if not
// a power of two will be rounded up to the nearest power of two. The capacity
// MUST be at least 2x the Config.MaxAppendEntries. The maximum allowed for that
// config is 1024 so a size or 2048 or greater will always be safe with current
// Config validation code. If capacity is lower than MaxAppendEntries in the
// raft config then writes could eventually stall. Since this is unrecoverable
// the server will panic. Take care if you allow user configurable
// MaxAppendEntries!
func NewLogCacheAsync(capacity int, store LogStore) (*LogCacheAsync, error) {
	last, err := store.LastIndex()
	if err != nil {
		return nil, err
	}

	size := nextPowerOf2(uint64(capacity))

	c := &LogCacheAsync{
		lastIndex:       last,
		persistentIndex: last,
		store:           store,
		size:            size,
		sizeMask:        size - 1, // 0b10000 -> 0b01111
	}
	c.state.cache = make([]*Log, size)
	c.state.triggerChan = make(chan syncRequest, 1)

	if ms, ok := store.(MonotonicLogStore); ok {
		c.monotonic = ms.IsMonotonic()
	}

	return c, nil
}

func nextPowerOf2(cap uint64) uint64 {
	res := uint64(1)
	for res < cap {
		res = res << 1
	}
	return res
}

// FirstIndex returns the first index written. 0 for no entries.
func (c *LogCacheAsync) FirstIndex() (uint64, error) {
	return c.store.FirstIndex()
}

// LastIndex returns the last index written. 0 for no entries.
func (c *LogCacheAsync) LastIndex() (uint64, error) {
	return atomic.LoadUint64(&c.lastIndex), nil
}

// minPossibleIdx is the lowest log we could possibly have cached. We might
// not have that low because we just started or because we are currently
// writing a batch over the top, but it's a lower bound.
func (c *LogCacheAsync) minPossibleIdx() uint64 {
	lastIdx := atomic.LoadUint64(&c.lastIndex)
	minPossibleIdx := uint64(1)
	if lastIdx > c.size {
		minPossibleIdx = lastIdx - c.size
	}
	return minPossibleIdx
}

// GetLog gets a log entry at a given index.
func (c *LogCacheAsync) GetLog(index uint64, log *Log) error {
	// Quick check to see if it's even possibly in the cache so we avoid locking
	// at all in the case of scanning through old records.
	if index < c.minPossibleIdx() {
		return c.store.GetLog(index, log)
	}

	// Check cache
	c.state.Lock()
	cached := c.state.cache[index&c.sizeMask] // equivalent to index % size
	c.state.Unlock()

	if cached != nil && cached.Index == index {
		*log = *cached
		return nil
	}
	return c.store.GetLog(index, log)
}

// StoreLog stores a log entry.
func (c *LogCacheAsync) StoreLog(log *Log) error {
	return c.StoreLogs([]*Log{log})
}

// StoreLogs stores multiple log entries.
func (c *LogCacheAsync) StoreLogs(logs []*Log) error {
	// Shortcut in this unlikely case to avoid panics below.
	if len(logs) < 1 {
		return nil
	}

	c.state.Lock()
	isAsync := c.state.completionCh != nil
	c.state.Unlock()
	if isAsync {
		return errors.New("call to sync StoreLog(s) when in async mode")
	}

	// Pass through sync writes to the underlying store. Don't hold lock while
	// doing IO since there can only be a single writer anyway.
	err := c.store.StoreLogs(logs)
	if err != nil {
		return err
	}

	// On success, populate the cache
	c.state.Lock()
	for _, l := range logs {
		c.state.cache[l.Index&c.sizeMask] = l
	}
	lastIdx := logs[len(logs)-1].Index
	atomic.StoreUint64(&c.lastIndex, lastIdx)
	atomic.StoreUint64(&c.persistentIndex, lastIdx)
	c.state.Unlock()
	return nil
}

// DeleteRange deletes a range of log entries. The range is inclusive. We need
// to support deletions in both sync and async mode since leader needs to be
// able to truncate logs while in Async mode. In async mode though we can't
// clear the cache as it may have unflushed logs. It has always been OK to call
// DeleteRange and StoreLogs concurrently though since Raft truncates in a
// background goroutine so we don't have to involve the flusher thread in the
// delete. We do need to be mindful though that the underlying LogStore probably
// used a lock to serialise DeleteRange and StoreLogs so our call here could
// deadlock with the flusher thread if we delete while holding the cache state
// lock.
func (c *LogCacheAsync) DeleteRange(min uint64, max uint64) error {
	c.state.Lock()

	if c.state.completionCh != nil {
		// ASYNC MODE!

		// Release the state lock while we delete to reduce contention if this is
		// slow.
		c.state.Unlock()
		err := c.store.DeleteRange(min, max)
		if err != nil {
			return err
		}

		// First check if the truncate could have any impact on any on the cached
		// records.
		if max < c.minPossibleIdx() {
			// range deleted was entirely below the cached range so we are done.
			return nil
		}

		// It's possible (but not certain) that some of the deleted range could be
		// in cache. Check if any logs in the deleted range are present and remove
		// them if they are.
		c.state.Lock()
		for idx := min; idx <= max; idx++ {
			l := c.state.cache[idx&c.sizeMask]
			if l != nil && l.Index == idx {
				// Delete it!
				c.state.cache[idx&c.sizeMask] = nil
			}
		}
		c.state.Unlock()
		return nil
	}
	// Invalidate the cache
	c.state.cache = make([]*Log, c.size)
	c.state.Unlock()

	return c.store.DeleteRange(min, max)
}

// IsMonotonic implement MonotonicLogStore
func (c *LogCacheAsync) IsMonotonic() bool {
	return c.monotonic
}

// EnableAsync implements AsyncLogStore
func (c *LogCacheAsync) EnableAsync(cc chan<- LogWriteCompletion) {
	c.state.Lock()
	defer c.state.Unlock()

	// Check we are in sync mode to enforce correct state machine usage.
	if c.state.completionCh != nil {
		panic("already in async mode")
	}

	c.state.completionCh = cc

	go c.runFlusher()
}

func (c *LogCacheAsync) runFlusher() {
	var batch []*Log

	for {
		syncReq := <-c.state.triggerChan

		// Load the state under lock
		c.state.Lock()
		persistedIdx := atomic.LoadUint64(&c.persistentIndex)
		lastIdx := atomic.LoadUint64(&c.lastIndex)

		for idx := persistedIdx + 1; idx <= lastIdx; idx++ {
			batch = append(batch, c.state.cache[idx&c.sizeMask])
		}

		// If we are stopping that means the serial writer is blocked in
		// DisableAsync waiting for us to be done syncing. Keep hold of the lock
		// while we sync everything that is unflushed and then stop.
		if syncReq.stop {
			lwc := c.doFlush(batch, syncReq.startTime)
			c.deliverCompletionLocked(lwc)

			// Teardown async state here while we hold the lock to make it simpler to
			// reason about possible races.
			c.state.completionCh = nil
			close(syncReq.doneCh)

			// Release the lock or we'll deadlock this node next time it's elected!
			c.state.Unlock()
			return
		}

		// Not stopping, unlock to allow concurrent writes while we flush this batch
		// to disk.
		c.state.Unlock()

		// Might be a no-op if batch is empty
		lwc := c.doFlush(batch, syncReq.startTime)

		// Note: if the flush failed we might retry it on the next loop. This is
		// safe assuming that the LogStore is atomic and not left in an invalid
		// state (which Raft assumes in general already). It might loop and retry
		// the write of the same logs next time which may fail again or may even
		// succeed before the leaderloop notices the error and steps down. But
		// either way it's fine because we don't advance the persistedIndex if it
		// fails so we'll keep trying to write the same logs at least not leave a
		// gap. Actually if we do error, even if there is no immediate sync trigger
		// waiting, the leader will step down and disable async which will mean we
		// attempt to flush again anyway. If that fails though (in the stop case
		// above) we won't keep retrying and will just re-report the error.

		// Need a lock to deliver the completion and update persistent index
		c.state.Lock()
		c.deliverCompletionLocked(lwc)
		c.state.Unlock()

		close(syncReq.doneCh)

		// Loop around, if more writes have happened, triggerChan will already have
		// a syncReq buffered and we'll immediately start flushing again.
	}
}

func (c *LogCacheAsync) deliverCompletionLocked(lwc *LogWriteCompletion) {
	if lwc == nil {
		return
	}
	if lwc.Error == nil {
		atomic.StoreUint64(&c.persistentIndex, lwc.PersistentIndex)
	}
	c.state.completionCh <- *lwc
}

func (c *LogCacheAsync) doFlush(logs []*Log, start time.Time) *LogWriteCompletion {
	if len(logs) < 1 {
		return nil
	}

	// Write to the underlying store
	err := c.store.StoreLogs(logs)

	lwc := LogWriteCompletion{
		// Initialize this to the current persistentIndex in case we fail to write.
		// We could Load but no need since we know it must be the one before the
		// first log we are writing now.
		PersistentIndex: logs[0].Index - 1,
		Error:           err,
		Duration:        time.Since(start),
	}
	if err == nil {
		lwc.PersistentIndex = logs[len(logs)-1].Index
		atomic.StoreUint64(&c.persistentIndex, logs[len(logs)-1].Index)
	}
	return &lwc
}

// DisableAsync implements AsyncLogStore
func (c *LogCacheAsync) DisableAsync() {
	c.state.Lock()
	// Check we are in sync mode to enforce correct state machine usage.
	if c.state.completionCh == nil {
		panic("already in sync mode")
	}
	// unlock again since the flusher will need the lock to stop and clear state.
	// This method can only be called from the serial leader loop so we don't have
	// to worry about races from other callers of Append, Enable* or Disable*.
	c.state.Unlock()

	// First, wait for any pending writes to flush. In this case we do wait even
	// if the buffer is ful since we need to see the ack.
	doneCh := make(chan struct{})
	c.state.triggerChan <- syncRequest{
		startTime: time.Now(),
		stop:      true, // Tell the flusher to exit and cleanup async state
		doneCh:    doneCh,
	}
	// Wait for the sync to be done
	<-doneCh
}

// StoreLogsAsync implements AsyncLogStore
func (c *LogCacheAsync) StoreLogsAsync(logs []*Log) error {
	c.state.Lock()
	defer c.state.Unlock()

	if c.state.completionCh == nil {
		return errors.New("call to StoreLogsAsync when in sync mode")
	}

	start := time.Now()

	persistedIdx := atomic.LoadUint64(&c.persistentIndex)
	lastIdx := atomic.LoadUint64(&c.lastIndex)

	// Make sure there is room in the cache for all the logs we need to write.
	// It's very unlikely there won't be, but if we are writing really fast into a
	// small cache and the flusher is blocked on IO for a while then we need to
	// ensure we don't overwrite cache entries that are not persistent yet!
	for !hasSpaceFor(len(logs), lastIdx, persistedIdx, c.size) {
		// We need to block and wait for they sync thread to persist some more
		// stuff! We do that by sending a sync request even though it's already busy
		// this lets us get notified about when it's free. Note that even though we
		// unlock and it's _possible_ for another StoreLogsAsync call to be made,
		doneCh := make(chan struct{})
		// Unlock before we send since we might block if the flusher is busy but it
		// won't be able to complete without the lock.
		c.state.Unlock()
		c.state.triggerChan <- syncRequest{startTime: start, doneCh: doneCh}
		<-doneCh
		c.state.Lock()
		// Reload the indexes now sync is done so we can check if there is space
		// now.
		persistedIdx = atomic.LoadUint64(&c.persistentIndex)
		lastIdx = atomic.LoadUint64(&c.lastIndex)
	}

	writeIdx := lastIdx + 1

	// Write the logs into the buffer. We know we aren't overwriting un-persisted
	// entries thanks to the check above and the fact that we stayed locked since
	// hasSpaceFor returned true.
	for _, log := range logs {
		c.state.cache[writeIdx&c.sizeMask] = log
		lastIdx = log.Index
		writeIdx++
	}
	atomic.StoreUint64(&c.lastIndex, lastIdx)

	// Trigger a sync in the background
	doneCh := make(chan struct{})

	// Don't wait, if the trigger buffer is full then there is already a sync
	// queued which will pick up our most recent changes.
	select {
	case c.state.triggerChan <- syncRequest{startTime: start, doneCh: doneCh}:
	default:
	}

	return nil
}

// hasSpaceFor works out if there are enough free slots in the buffer that are
// already persisted (or empty) for n more logs to be added.
func hasSpaceFor(n int, lastIdx, persistIdx, size uint64) bool {
	if uint64(n) > size {
		// This should not be possible if the user of the library follows the
		// guidance in the type's Doc comment!
		panic("trying to write more logs than will ever fit in the cache")
	}

	// If we add n logs, the new lastIdx will be
	newLastIdx := lastIdx + uint64(n)

	// See how far ahead of or persistIdx that is in the buffer
	unpersisted := newLastIdx - persistIdx

	// As long as the new number of unpersisted records is no larger than the
	// whole buffer, we have space. If it is greater, that implies that we'd have
	// to overwrite the oldest logs even though they still haven't been persisted
	// yet!
	return unpersisted <= size
}
