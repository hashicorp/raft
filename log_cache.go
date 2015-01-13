package raft

import (
	"sync"
)

// logCache wraps a logstore with a ring buffer providing fast access to the
// last n raft log entries.
type logCache struct {
	store      LogStore
	cache      []*Log
	current    int
	lastlogidx uint64
	l          sync.RWMutex
}

func newLogCache(capacity int, logstore LogStore) *logCache {
	return &logCache{
		cache: make([]*Log, 0, capacity),
		store: logstore,
	}
}

func (c *logCache) getLogFromCache(logidx uint64) (*Log, bool) {
	c.l.RLock()
	defer c.l.RUnlock()

	// Return if we have not seen that log entry yet, or
	// if it was pushed out of the cache already.
	if logidx > c.lastlogidx || (c.lastlogidx-logidx) >= uint64(len(c.cache)) {
		return nil, false
	}

	// 'last' is the index of the element we cached last,
	// its raft log index is 'lastlogidx'
	last := (c.current - 1)
	m := last - int(c.lastlogidx-logidx)

	// See https://golang.org/issue/448 for why (m % n) is not enough.
	n := cap(c.cache)
	log := c.cache[((m%n)+n)%n]
	// If the index does not match, cacheLog’s expected access pattern was
	// violated and we need to fall back to reading from the LogStore.
	return log, log.Index == logidx
}

// cacheLog should be called with strictly monotonically increasing logidx
// values, otherwise the cache will not be effective.
func (c *logCache) cacheLog(logidx uint64, log *Log) {
	c.l.Lock()
	defer c.l.Unlock()

	if len(c.cache) < cap(c.cache) {
		c.cache = append(c.cache, log)
	} else {
		c.cache[c.current] = log
	}
	c.lastlogidx = logidx
	c.current = (c.current + 1) % cap(c.cache)
}

func (c *logCache) GetLog(logidx uint64, log *Log) error {
	if cached, ok := c.getLogFromCache(logidx); ok {
		*log = *cached
		return nil
	}
	return c.store.GetLog(logidx, log)
}

func (c *logCache) StoreLog(log *Log) error {
	return c.StoreLogs([]*Log{log})
}

func (c *logCache) StoreLogs(logs []*Log) error {
	for _, l := range logs {
		c.cacheLog(l.Index, l)
	}
	return c.store.StoreLogs(logs)
}

func (c *logCache) FirstIndex() (uint64, error) {
	return c.store.FirstIndex()
}

func (c *logCache) LastIndex() (uint64, error) {
	return c.store.LastIndex()
}

func (c *logCache) DeleteRange(min, max uint64) error {
	c.l.Lock()
	defer c.l.Unlock()

	c.lastlogidx = 0
	c.current = 0
	c.cache = make([]*Log, 0, cap(c.cache))

	return c.store.DeleteRange(min, max)
}
