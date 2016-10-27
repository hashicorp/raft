package raft

import (
	"bytes"
	"log"
	"strings"
	"testing"
)

// Keep this test up here, as it hard-codes the line number of the Debug() call.
func TestStdLogger_filename(t *testing.T) {
	dest := bytes.Buffer{}
	logger := NewStdLogger(log.New(&dest, "", log.Lshortfile), LogDebug)
	logger.Debug("a debug msg")
	if dest.String() != "logger_test.go:14: [DEBUG] a debug msg\n" {
		t.Errorf("Log Message in wrong format. Got: '%s'", dest.String())
	}
}

func TestLogLevel_String(t *testing.T) {
	lvls := map[LogLevel]string{
		LogDebug: "DEBUG",
		LogInfo:  "INFO",
		LogWarn:  "WARN",
		LogError: "ERROR",
	}
	for lvl, str := range lvls {
		if lvl.String() != str {
			t.Errorf("LogLevel %d String() expected to return %s, but got %s", lvl, str, lvl.String())
		}
	}
}

func TestStdLogger_LogLevel(t *testing.T) {
	dest := bytes.Buffer{}
	logger := DefaultStdLogger(&dest)
	logger.Debug("a debug msg")
	if dest.Len() > 0 {
		t.Errorf("Log Message at Debug level not expected to appear in log when logging set to Info: got %s", dest.String())
	}
	logger.Info("info")
	if strings.Index(dest.String(), "[INFO] info") == -1 {
		t.Errorf("Log Message at Info level expected to be in log, but couldn't find it, log contains %s", dest.String())
	}
	logger.Warn("warn %d", 5)
	if strings.Index(dest.String(), "[WARN] warn 5") == -1 {
		t.Errorf("Log Message at Warn level expected to be in log, but couldn't find it, log contains %s", dest.String())
	}
	logger.Error("error")
	if strings.Index(dest.String(), "[ERROR] error") == -1 {
		t.Errorf("Log Message at Warn level expected to be in log, but couldn't find it, log contains %s", dest.String())
	}
}
