package raft

import (
	"context"
	"io"
	"sync"
	"time"

	hclog "github.com/hashicorp/go-hclog"
)

const (
	snapshotRestoreMonitorInterval = 10 * time.Second
)

type snapshotRestoreMonitor struct {
	logger hclog.Logger
	cr     CountingReader
	size   int64

	once   sync.Once
	cancel func()
	doneCh chan struct{}
}

func startSnapshotRestoreMonitor(logger hclog.Logger, cr CountingReader, size int64) *snapshotRestoreMonitor {
	ctx, cancel := context.WithCancel(context.Background())

	m := &snapshotRestoreMonitor{
		logger: logger,
		cr:     cr,
		size:   size,
		cancel: cancel,
		doneCh: make(chan struct{}),
	}
	go m.run(ctx)
	return m
}

func (m *snapshotRestoreMonitor) run(ctx context.Context) {
	defer close(m.doneCh)

	ticker := time.NewTicker(snapshotRestoreMonitorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.runOnce()
		}
	}
}

func (m *snapshotRestoreMonitor) runOnce() {
	readBytes := m.cr.Count()
	pct := float64(100*readBytes) / float64(m.size)

	m.logger.Info("snapshot restoration progress",
		"read-bytes", readBytes,
		"percent-complete", hclog.Fmt("%0.2f%%", pct),
	)
}

func (m *snapshotRestoreMonitor) StopAndWait() {
	m.once.Do(func() {
		m.cancel()
		<-m.doneCh
	})
}

func reportProgress(
	ctx context.Context,
	interval time.Duration,
	reportOnceFn func(last bool),
) <-chan struct{} {
	done := make(chan struct{})

	go func() {
		defer func() {
			reportOnceFn(true)
			close(done)
		}()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}

			reportOnceFn(false)
		}
	}()

	return done
}

type CountingReader interface {
	io.Reader
	Count() int64
}

type countingReader struct {
	reader io.Reader

	mu    sync.Mutex
	bytes int64
}

func (r *countingReader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	r.mu.Lock()
	r.bytes += int64(n)
	r.mu.Unlock()
	return n, err
}

func (r *countingReader) Count() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.bytes
}

func newCountingReader(r io.Reader) *countingReader {
	return &countingReader{reader: r}
}

type countingReadCloser struct {
	*countingReader
	io.Closer
}

func newCountingReadCloser(rc io.ReadCloser) *countingReadCloser {
	return &countingReadCloser{
		countingReader: newCountingReader(rc),
		Closer:         rc,
	}
}
