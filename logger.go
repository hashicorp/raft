package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	log "github.com/mgutz/logxi/v1"
)

const (
	styleDefault = iota
	styleJSON
)

// DefaultStdLogger returns a Logger that logs to the supplied writer using the
// defaults for logger (this is often a fallback way to get a logger inside the
// library when one isn't supplied by the caller).
func DefaultStdLogger(w io.Writer) log.Logger {
	return NewRaftLoggerWithWriter(w, log.LevelInfo)
}

// NewRaftLogger creates a new logger with the specified level and a Raft
// formatter.
func NewRaftLogger(level int) log.Logger {
	logger := log.New("raft")
	return setLevelFormatter(logger, level, createRaftFormatter("", false))
}

// NewRaftLoggerWithWriter creates a new logger with the specified level and
// writer and a Raft formatter.
func NewRaftLoggerWithWriter(w io.Writer, level int) log.Logger {
	logger := log.NewLogger(w, "raft")
	return setLevelFormatter(logger, level, createRaftFormatter("", false))
}

// NewRaftLoggerForTesting creates a new logger with the specified tag, for use
// in unit tests.
func NewRaftLoggerForTesting(w io.Writer, tag string) log.Logger {
	logger := log.NewLogger(w, "raft")
	level := log.LevelDebug
	return setLevelFormatter(logger, level, createRaftFormatter(tag, true))
}

// Sets the level and formatter on the log, which must be a DefaultLogger.
func setLevelFormatter(logger log.Logger, level int, formatter log.Formatter) log.Logger {
	logger.(*log.DefaultLogger).SetLevel(level)
	logger.(*log.DefaultLogger).SetFormatter(formatter)
	return logger
}

// createRaftFormatter builds a formatter and looks via environment variables
// for style customization.
func createRaftFormatter(tag string, withLine bool) log.Formatter {
	ret := &raftFormatter{
		Mutex:    &sync.Mutex{},
		tag:      tag,
		withLine: withLine,
	}
	logFormat := os.Getenv("RAFT_LOG_FORMAT")
	if logFormat == "" {
		logFormat = os.Getenv("LOGXI_FORMAT")
	}
	switch strings.ToLower(logFormat) {
	case "json", "raft_json", "raft-json", "raftjson":
		ret.style = styleJSON
	default:
		ret.style = styleDefault
	}
	return ret
}

// raftFormatter is a thread-safe formatter.
type raftFormatter struct {
	*sync.Mutex
	style    int
	module   string
	tag      string
	withLine bool
}

// Format preps a log message for the logger.
func (v *raftFormatter) Format(writer io.Writer, level int, msg string, args []interface{}) {
	v.Lock()
	defer v.Unlock()
	switch v.style {
	case styleJSON:
		v.formatJSON(writer, level, msg, args)
	default:
		v.formatDefault(writer, level, msg, args)
	}
}

// formatDefault is the default text-based log format.
func (v *raftFormatter) formatDefault(writer io.Writer, level int, msg string, args []interface{}) {
	// Write a trailing newline.
	defer writer.Write([]byte("\n"))

	writer.Write([]byte(time.Now().Local().Format("2006/01/02 15:04:05.000000")))

	switch level {
	case log.LevelCritical:
		writer.Write([]byte(" [CRIT ] "))
	case log.LevelError:
		writer.Write([]byte(" [ERROR] "))
	case log.LevelWarn:
		writer.Write([]byte(" [WARN ] "))
	case log.LevelInfo:
		writer.Write([]byte(" [INFO ] "))
	case log.LevelDebug:
		writer.Write([]byte(" [DEBUG] "))
	case log.LevelTrace:
		writer.Write([]byte(" [TRACE] "))
	default:
		writer.Write([]byte(" [ALL  ] "))
	}

	// Tag is used during unit tests when we might have multiple Rafts all
	// outputting to the same log.
	if v.tag != "" {
		writer.Write([]byte(fmt.Sprintf("(%s) ", v.tag)))
	}

	// This is a little jank because the depth depends on the internals of
	// the logxi library, but it's only used during unit tests.
	if v.withLine {
		_, file, line, ok := runtime.Caller(4)
		if !ok {
			file = "???"
			line = 0
		}
		writer.Write([]byte(fmt.Sprintf("%s:%d ", path.Base(file), line)))
	}

	if v.module != "" {
		writer.Write([]byte(fmt.Sprintf("(%s) ", v.module)))
	}

	writer.Write([]byte(msg))

	if args != nil && len(args) > 0 {
		if len(args)%2 != 0 {
			args = append(args, "[unknown!]")
		}

		writer.Write([]byte(":"))
		for i := 0; i < len(args); i = i + 2 {
			v := fmt.Sprintf("%v", args[i+1])
			if v == "" || strings.ContainsRune(v, ' ') {
				writer.Write([]byte(fmt.Sprintf(` %s=%q`, args[i], v)))
			} else {
				writer.Write([]byte(fmt.Sprintf(` %s=%s`, args[i], v)))
			}
		}
	}
}

// formatJSON is a machine-readable JSON log format.
func (v *raftFormatter) formatJSON(writer io.Writer, level int, msg string, args []interface{}) {
	vals := map[string]interface{}{
		"@message":   msg,
		"@timestamp": time.Now().Format("2006-01-02T15:04:05.000000Z07:00"),
	}

	var levelStr string
	switch level {
	case log.LevelCritical:
		levelStr = "critical"
	case log.LevelError:
		levelStr = "error"
	case log.LevelWarn:
		levelStr = "warn"
	case log.LevelInfo:
		levelStr = "info"
	case log.LevelDebug:
		levelStr = "debug"
	case log.LevelTrace:
		levelStr = "trace"
	default:
		levelStr = "all"
	}

	vals["@level"] = levelStr

	if v.module != "" {
		vals["@module"] = v.module
	}

	if args != nil && len(args) > 0 {

		if len(args)%2 != 0 {
			args = append(args, "[unknown!]")
		}

		for i := 0; i < len(args); i = i + 2 {
			if _, ok := args[i].(string); !ok {
				// As this is the logging function not much we can do here
				// without injecting into logs...
				continue
			}
			vals[args[i].(string)] = args[i+1]
		}
	}

	enc := json.NewEncoder(writer)
	enc.Encode(vals)
}

/*

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

*/
