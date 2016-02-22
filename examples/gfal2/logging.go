package main

import (
	"fmt"
	"gitlab.cern.ch/dmc/go-gfal2"
	"os"
)

// Struct that implements the listener methods
type LogListener struct {
}

// Represent the log level as a string
func levelString(level int) string {
	switch level {
	case gfal2.LogLevelCritical:
		return "CRITICAL"
	case gfal2.LogLevelError:
		return "ERROR"
	case gfal2.LogLevelWarning:
		return "WARNING"
	case gfal2.LogLevelMessage:
		return "MESSAGE"
	case gfal2.LogLevelInfo:
		return "INFO"
	case gfal2.LogLevelDebug:
		return "DEBUG"
	}
	return "??"
}

// Called by gfal2 for the logging.
func (_ LogListener) Log(domain string, level int, msg string) {
	fmt.Fprintf(os.Stderr, "[%-8s] %s => %s\n", levelString(level), domain, msg)
}

// Print a new log entry.
func Log(domain string, level int, str string, args ...interface{}) {
	msg := fmt.Sprintf(str, args...)
	fmt.Fprintf(os.Stderr, "[%-8s] %s => %s\n", levelString(level), domain, msg)
}
