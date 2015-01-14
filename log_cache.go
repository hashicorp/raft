package raft

import (
	"sync"
)

// LogCache wraps any LogStore implementation to provide an
// in-memory ring buffer. This is used to cache access to
// the recently written entries. For implementations that do not
// cache themselves, this can provide a substantial boost by
// avoiding disk I/O on recent entries.
type LogCache struct {
	store LogStore

	cache      []*Log
	current    int
	lastLogIdx uint64
	l          sync.RWMutex
}

// NewLogCache is used to create a new LogCache with the
// given capacity and backend store.
func NewLogCache(capacity int, store LogStore) *LogCache {
	return &LogCache{
		store: store,
		cache: make([]*Log, capacity),
	}
}

func (c *LogCache) getLogFromCache(logidx uint64) (*Log, bool) {
	c.l.RLock()
	defer c.l.RUnlock()

	// 'last' is the index of the element we cached last,
	// its raft log index is 'lastLogIdx'
	last := (c.current - 1)
	m := last - int(c.lastLogIdx-logidx)

	// See https://golang.org/issue/448 for why (m % n) is not enough.
	n := len(c.cache)
	log := c.cache[((m%n)+n)%n]
	if log == nil {
		return nil, false
	}
	// If the index does not match, cacheLog’s expected access pattern was
	// violated and we need to fall back to reading from the LogStore.
	return log, log.Index == logidx
}

// cacheLogs should be called with strictly monotonically increasing logidx
// values, otherwise the cache will not be effective.
func (c *LogCache) cacheLogs(logs []*Log) {
	c.l.Lock()
	defer c.l.Unlock()

	for _, l := range logs {
		c.cache[c.current] = l
		c.lastLogIdx = l.Index
		c.current = (c.current + 1) % len(c.cache)
	}
}

func (c *LogCache) GetLog(idx uint64, log *Log) error {
	if cached, ok := c.getLogFromCache(idx); ok {
		*log = *cached
		return nil
	}
	return c.store.GetLog(idx, log)
}

func (c *LogCache) StoreLog(log *Log) error {
	return c.StoreLogs([]*Log{log})
}

func (c *LogCache) StoreLogs(logs []*Log) error {
	c.cacheLogs(logs)
	return c.store.StoreLogs(logs)
}

func (c *LogCache) FirstIndex() (uint64, error) {
	return c.store.FirstIndex()
}

func (c *LogCache) LastIndex() (uint64, error) {
	return c.store.LastIndex()
}

func (c *LogCache) DeleteRange(min, max uint64) error {
	c.l.Lock()
	defer c.l.Unlock()

	// Invalidate the cache on deletes
	c.lastLogIdx = 0
	c.current = 0
	c.cache = make([]*Log, len(c.cache))

	return c.store.DeleteRange(min, max)
}
