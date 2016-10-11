package raft

import (
	"bytes"
	"strings"
	"testing"
)

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
	logger.Logf(LogDebug, "a debug msg")
	if dest.Len() > 0 {
		t.Errorf("Log Message at Debug level not expected to appear in log when logging set to Info: got %s", dest.String())
	}
	logger.Logf(LogInfo, "info")
	if strings.Index(dest.String(), "[INFO] info") == -1 {
		t.Errorf("Log Message at Info level expected to be in log, but couldn't find it, log contains %s", dest.String())
	}
	logger.Logf(LogWarn, "warn %d", 5)
	if strings.Index(dest.String(), "[WARN] warn 5") == -1 {
		t.Errorf("Log Message at Warn level expected to be in log, but couldn't find it, log contains %s", dest.String())
	}
	logger.Logf(LogError, "error")
	if strings.Index(dest.String(), "[ERROR] error") == -1 {
		t.Errorf("Log Message at Warn level expected to be in log, but couldn't find it, log contains %s", dest.String())
	}
}
