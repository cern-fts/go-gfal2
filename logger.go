package gfal2

// #cgo pkg-config: gfal2 gfal_transfer
// #include <gfal_api.h>
//
// void logCallback(const gchar *log_domain, GLogLevelFlags log_level, const gchar *message, gpointer user_data);
import "C"
import (
	"unsafe"
)

// Log levels.
const (
	LogLevelError    = C.G_LOG_LEVEL_ERROR
	LogLevelCritical = C.G_LOG_LEVEL_CRITICAL
	LogLevelWarning  = C.G_LOG_LEVEL_WARNING
	LogLevelMessage  = C.G_LOG_LEVEL_MESSAGE
	LogLevelInfo     = C.G_LOG_LEVEL_INFO
	LogLevelDebug    = C.G_LOG_LEVEL_DEBUG
)

// Interface to be implemented by the handlers.
type LogListener interface {
	Log(domain string, level int, msg string)
}

// Set log level.
func SetLogLevel(level int) {
	C.gfal2_log_set_level(C.GLogLevelFlags(level))
}

// Get log level.
func GetLogLevel() int {
	return int(C.gfal2_log_get_level())
}

//export logHandlerWrapper
func logHandlerWrapper(domain *C.char, level C.GLogLevelFlags, msg *C.char, udata C.gpointer) {
	logCallback := *(*LogListener)(udata)
	logCallback.Log(C.GoString(domain), int(level), C.GoString(msg))
}

// Set a callback rather than printing to stdout.
func SetLogHandler(handler LogListener) {
	C.gfal2_log_set_handler(C.GLogFunc(C.logCallback), C.gpointer(unsafe.Pointer(&handler)))
}
