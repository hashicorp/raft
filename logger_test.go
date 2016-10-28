package raft

import (
	"bytes"
	"strings"
	"testing"
)

// Keep this test up here, as it hard-codes the line number of the Debug() call.
func TestLogger_NewRaftLoggerForTesting(t *testing.T) {
	dest := bytes.Buffer{}
	logger := NewRaftLoggerForTesting(&dest, "tag")
	logger.Debug("a debug msg")
	if !strings.Contains(dest.String(), "(tag) logger_test.go:13") {
		t.Errorf("Log message in wrong format: %q", dest.String())
	}
}

func TestLogger_LogLevel(t *testing.T) {
	dest := bytes.Buffer{}
	logger := DefaultStdLogger(&dest)
	logger.Debug("a debug msg")
	if dest.Len() > 0 {
		t.Errorf("Log message at DEBUG level not expected to appear in log when logging set to Info: %q", dest.String())
	}
	logger.Info("info")
	if strings.Index(dest.String(), "[INFO ] info") == -1 {
		t.Errorf("Log message at INFO level expected to be in log, but couldn't find it: %q", dest.String())
	}
	logger.Warn("warn")
	if strings.Index(dest.String(), "[WARN ] warn") == -1 {
		t.Errorf("Log message at WARN level expected to be in log, but couldn't find it: %q", dest.String())
	}
	logger.Error("error")
	if strings.Index(dest.String(), "[ERROR] error") == -1 {
		t.Errorf("Log message at ERROR level expected to be in log, but couldn't find it: %q", dest.String())
	}
}
