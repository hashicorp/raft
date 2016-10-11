package raft

import (
	"fmt"
	"io"
	"log"
)

// LogLevel desribes the verbosity of a log item
type LogLevel int

const (
	LogDebug LogLevel = iota
	LogInfo
	LogWarn
	LogError
)

// Logger provides a way to capture log messages from the library
type Logger interface {
	Logf(lvl LogLevel, msg string, values ...interface{})
}

type stdLogger struct {
	dest  *log.Logger
	level LogLevel
}

// NewStdLogger provides an adapter from a golang std log.Logger to
// a raft.Logger, log messages at or above levelToLog will be logged
// to the supplied logger.
func NewStdLogger(l *log.Logger, levelToLog LogLevel) Logger {
	return &stdLogger{dest: l, level: levelToLog}
}

// DefaultStdLogger returns a Logger that logs to the supplied writer
// using the defaults for logger [this is often a fallback way to
// get a logger inside the library when one isn't supplied by the caller]
func DefaultStdLogger(dest io.Writer) Logger {
	return NewStdLogger(log.New(dest, "", log.LstdFlags), LogInfo)
}

func (s *stdLogger) Logf(lvl LogLevel, msg string, values ...interface{}) {
	if lvl >= s.level {
		s.dest.Printf("[%v] %s", lvl, fmt.Sprintf(msg, values...))
	}
}

// panicf is a helper func that logs the panic message to the logger, then panics
func panicf(logger Logger, msg string, values ...interface{}) {
	logger.Logf(LogError, msg, values...)
	panic(fmt.Sprintf(msg, values...))
}

func (lvl LogLevel) String() string {
	switch lvl {
	case LogDebug:
		return "DEBUG"
	case LogInfo:
		return "INFO"
	case LogWarn:
		return "WARN"
	case LogError:
		return "ERROR"
	default:
		return fmt.Sprintf("LogLevel[%d]", lvl)
	}
}
