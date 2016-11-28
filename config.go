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
import "C"
import (
	"unsafe"
)

// SetOptString sets a string parameter under group:key.
func (context Context) SetOptString(group string, key string, value string) GError {
	var err *C.GError

	cGroup := (*C.gchar)(C.CString(group))
	defer C.free(unsafe.Pointer(cGroup))
	cKey := (*C.gchar)(C.CString(key))
	defer C.free(unsafe.Pointer(cKey))
	cValue := (*C.gchar)(C.CString(value))
	defer C.free(unsafe.Pointer(cValue))

	ret := C.gfal2_set_opt_string(context.cContext, cGroup, cKey, cValue, &err)
	if ret < 0 {
		return errorCtoGo(err)
	}

	return nil
}

// GetOptString gets the value of group:key as a string.
func (context Context) GetOptString(group string, key string) string {
	var err *C.GError

	cGroup := (*C.gchar)(C.CString(group))
	defer C.free(unsafe.Pointer(cGroup))
	cKey := (*C.gchar)(C.CString(key))
	defer C.free(unsafe.Pointer(cKey))

	ret := C.gfal2_get_opt_string(context.cContext, cGroup, cKey, &err)
	if ret == nil {
		return ""
	}

	value := C.GoString((*C.char)(ret))
	C.g_free(C.gpointer(ret))
	return value
}

// SetOptInteger sets an integer parameter under group:key.
func (context Context) SetOptInteger(group string, key string, value int) GError {
	var err *C.GError

	cGroup := (*C.gchar)(C.CString(group))
	defer C.free(unsafe.Pointer(cGroup))
	cKey := (*C.gchar)(C.CString(key))
	defer C.free(unsafe.Pointer(cKey))

	ret := C.gfal2_set_opt_integer(context.cContext, cGroup, cKey, C.gint(value), &err)
	if ret < 0 {
		return errorCtoGo(err)
	}

	return nil
}

// GetOptInteger gets the value of group:key as an integer.
func (context Context) GetOptInteger(group string, key string) int {
	var err *C.GError

	cGroup := (*C.gchar)(C.CString(group))
	defer C.free(unsafe.Pointer(cGroup))
	cKey := (*C.gchar)(C.CString(key))
	defer C.free(unsafe.Pointer(cKey))

	ret := C.gfal2_get_opt_integer(context.cContext, cGroup, cKey, &err)
	if err != nil {
		return 0
	}

	return int(ret)
}

// SetOptBoolean sets a boolean parameter under group:key.
func (context Context) SetOptBoolean(group string, key string, value bool) GError {
	var err *C.GError

	cGroup := (*C.gchar)(C.CString(group))
	defer C.free(unsafe.Pointer(cGroup))
	cKey := (*C.gchar)(C.CString(key))
	defer C.free(unsafe.Pointer(cKey))

	var cValue C.gboolean
	if value {
		cValue = 1
	}

	ret := C.gfal2_set_opt_boolean(context.cContext, cGroup, cKey, C.gboolean(cValue), &err)
	if ret < 0 {
		return errorCtoGo(err)
	}

	return nil
}

// GetOptBoolean gets the value of group:key as a boolean.
func (context Context) GetOptBoolean(group string, key string) bool {
	var err *C.GError

	cGroup := (*C.gchar)(C.CString(group))
	defer C.free(unsafe.Pointer(cGroup))
	cKey := (*C.gchar)(C.CString(key))
	defer C.free(unsafe.Pointer(cKey))

	ret := C.gfal2_get_opt_boolean(context.cContext, cGroup, cKey, &err)
	if err != nil {
		return false
	}

	return ret != 0
}

// SetOptStringList sets a string list parameter under group:key.
func (context Context) SetOptStringList(group string, key string, values []string) GError {
	var err *C.GError

	cGroup := (*C.gchar)(C.CString(group))
	defer C.free(unsafe.Pointer(cGroup))
	cKey := (*C.gchar)(C.CString(key))
	defer C.free(unsafe.Pointer(cKey))

	nValues := len(values)
	cValues := make([]*C.gchar, nValues)

	for i := 0; i < nValues; i++ {
		cValues[i] = (*C.gchar)(C.CString(values[i]))
		defer C.free(unsafe.Pointer(cValues[i]))
	}

	ret := C.gfal2_set_opt_string_list(context.cContext, cGroup, cKey, &cValues[0], C.gsize(nValues), &err)
	if ret < 0 {
		return errorCtoGo(err)
	}

	return nil
}

// GetOptStringList gets the value of group:key as a string list.
func (context Context) GetOptStringList(group string, key string) []string {
	var err *C.GError

	cGroup := (*C.gchar)(C.CString(group))
	defer C.free(unsafe.Pointer(cGroup))
	cKey := (*C.gchar)(C.CString(key))
	defer C.free(unsafe.Pointer(cKey))

	var nItems C.gsize
	ret := C.gfal2_get_opt_string_list(context.cContext, cGroup, cKey, &nItems, &err)
	if ret == nil {
		return nil
	}

	slice := (*[1 << 30]*C.gchar)(unsafe.Pointer(ret))[:nItems:nItems]
	array := make([]string, nItems)

	for index, name := range slice {
		array[index] = C.GoString((*C.char)(name))
	}

	C.g_strfreev(ret)

	return array
}

// LoadOptsFromFile loads configuration parameters from a file.
// It overwrites existing conflicting values, but keeps those that aren't superseded.
func (context Context) LoadOptsFromFile(path string) GError {
	var err *C.GError

	cPath := (*C.char)(C.CString(path))
	defer C.free(unsafe.Pointer(cPath))

	ret := C.gfal2_load_opts_from_file(context.cContext, cPath, &err)
	if ret < 0 {
		return errorCtoGo(err)
	}

	return nil
}

// SetUserAgent sets the user agent. Not all protocols implement this.
func (context Context) SetUserAgent(agent string, version string) GError {
	var err *C.GError

	cAgent := (*C.char)(C.CString(agent))
	defer C.free(unsafe.Pointer(cAgent))
	cVersion := (*C.char)(C.CString(version))
	defer C.free(unsafe.Pointer(cVersion))

	ret := C.gfal2_set_user_agent(context.cContext, cAgent, cVersion, &err)
	if ret < 0 {
		return errorCtoGo(err)
	}

	return nil
}

// GetUserAgent returns the configured user agent.
func (context Context) GetUserAgent() (agent string, version string) {
	var cAgent *C.char
	var cVersion *C.char

	C.gfal2_get_user_agent(context.cContext, &cAgent, &cVersion)

	if cAgent != nil {
		agent = C.GoString(cAgent)
		C.g_free(C.gpointer(cAgent))
	}
	if cVersion != nil {
		version = C.GoString(cVersion)
		C.g_free(C.gpointer(cVersion))
	}

	return
}

// AddClientInfo adds additional client info to be sent to the remote server.
// For instance, using HTTP this will be sent as part of the headers.
func (context Context) AddClientInfo(key string, value string) GError {
	var err *C.GError

	cKey := (*C.char)(C.CString(key))
	defer C.free(unsafe.Pointer(cKey))
	cValue := (*C.char)(C.CString(value))
	defer C.free(unsafe.Pointer(cValue))

	ret := C.gfal2_add_client_info(context.cContext, cKey, cValue, &err)
	if ret < 0 {
		return errorCtoGo(err)
	}

	return nil
}

// RemoveClientInfo removes additional client info that was previously set with AddClientInfo.
func (context Context) RemoveClientInfo(key string) GError {
	var err *C.GError

	cKey := (*C.char)(C.CString(key))
	defer C.free(unsafe.Pointer(cKey))

	ret := C.gfal2_remove_client_info(context.cContext, cKey, &err)
	if ret < 0 {
		return errorCtoGo(err)
	}

	return nil
}

// ClearClientInfo clears the additional client info.
func (context Context) ClearClientInfo() GError {
	var err *C.GError

	ret := C.gfal2_clear_client_info(context.cContext, &err)
	if ret < 0 {
		return errorCtoGo(err)
	}
	return nil
}

// GetClientInfoString returns the additional client info as key1:value1;key2:value2;...
func (context Context) GetClientInfoString() (repr string) {
	ret := C.gfal2_get_client_info_string(context.cContext)
	if ret != nil {
		repr = C.GoString(ret)
		C.g_free(C.gpointer(ret))
	}
	return
}
