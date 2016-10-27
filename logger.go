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

// Logger provides a way to capture log messages from the library. calldepth
// should be 0 for direct callers, 1 for callers of direct callers, etc.
type Logger func(calldepth int, level LogLevel, msg string, values ...interface{})

// NewStdLogger provides an adapter from a golang std log.Logger to
// a raft.Logger, log messages at or above levelToLog will be logged
// to the supplied logger.
func NewStdLogger(l *log.Logger, levelToLog LogLevel) Logger {
	return func(calldepth int, level LogLevel, msg string, values ...interface{}) {
		if level >= levelToLog {
			s := fmt.Sprintf("[%v] %s", level, fmt.Sprintf(msg, values...))
			l.Output(calldepth+3, s)
		}
	}
}

// DefaultStdLogger returns a Logger that logs to the supplied writer
// using the defaults for logger [this is often a fallback way to
// get a logger inside the library when one isn't supplied by the caller]
func DefaultStdLogger(dest io.Writer) Logger {
	return NewStdLogger(log.New(dest, "", log.LstdFlags), LogInfo)
}

func (output *Logger) Debug(msg string, values ...interface{}) {
	(*output)(0, LogDebug, msg, values...)
}

func (output *Logger) Info(msg string, values ...interface{}) {
	(*output)(0, LogInfo, msg, values...)
}

func (output *Logger) Warn(msg string, values ...interface{}) {
	(*output)(0, LogWarn, msg, values...)
}

func (output *Logger) Error(msg string, values ...interface{}) {
	(*output)(0, LogError, msg, values...)
}

// Panic logs the panic message to the logger, then panics.
func (output *Logger) Panic(msg string, values ...interface{}) {
	str := fmt.Sprintf(msg, values...)
	(*output)(0, LogError, str)
	panic(str)
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
