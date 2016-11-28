/*
 * Copyright (c) CERN 2016
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

// LogListener is the interface to be implemented by the log handlers.
type LogListener interface {
	Log(domain string, level int, msg string)
}

// SetLogLevel set the logging level.
func SetLogLevel(level int) {
	C.gfal2_log_set_level(C.GLogLevelFlags(level))
}

// GetLogLevel returns the logging level.
func GetLogLevel() int {
	return int(C.gfal2_log_get_level())
}

//export logHandlerWrapper
func logHandlerWrapper(domain *C.char, level C.GLogLevelFlags, msg *C.char, udata C.gpointer) {
	logCallback := *(*LogListener)(udata)
	logCallback.Log(C.GoString(domain), int(level), C.GoString(msg))
}

// SetLogHandler sets a callback rather than printing to stdout.
func SetLogHandler(handler LogListener) {
	C.gfal2_log_set_handler(C.GLogFunc(C.logCallback), C.gpointer(unsafe.Pointer(&handler)))
}
