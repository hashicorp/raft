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

// logLock is used to synchronize access to the formatter across all log
// instances.
var logLock sync.Mutex

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

// raftFormatter is a thread-safe formatter that synchronizes across all
// instances so we properly render logs with different prefixes.
type raftFormatter struct {
	style    int
	module   string
	tag      string
	withLine bool
}

// Format preps a log message for the logger.
func (v *raftFormatter) Format(writer io.Writer, level int, msg string, args []interface{}) {
	logLock.Lock()
	defer logLock.Unlock()
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

	// This is a little jank, but it's only used during unit tests.
	if v.withLine {
		file := "???"
		line := 0
		depth := 0
		for {
			var ok bool
			_, file, line, ok = runtime.Caller(depth)
			if ok {
				if strings.Contains(file, "/logxi/") ||
					strings.HasSuffix(file, "/raft/logger.go") {
					depth++
					continue
				}
				file = path.Base(file)
			}
			break
		}
		writer.Write([]byte(fmt.Sprintf("%s:%d ", file, line)))
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
