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
// int helper_strvlengh(gchar **array) {
// 		int i = 0;
//		for (i = 0; array[i] != NULL; ++i);
//		return i;
// }
//
import "C"
import (
	"unsafe"
)

// Context is a handle to a gfal2 instantiation.
type Context struct {
	cContext C.gfal2_context_t
}

// Version returns the underlying gfal2 version.
func Version() string {
	return C.GoString(C.gfal2_version())
}

// NewContext creates a new gfal2 context.
func NewContext() (*Context, GError) {
	var context Context
	var err *C.GError
	context.cContext = C.gfal2_context_new(&err)

	if context.cContext == nil {
		return nil, errorCtoGo(err)
	}

	return &context, nil
}

// Close destroys the gfal2 context.
func (context Context) Close() {
	C.gfal2_context_free(context.cContext)
	context.cContext = nil
}

// GetPluginNames returns a list with the names and version of the loaded plugins.
func (context Context) GetPluginNames() ([]string, GError) {
	cArray := C.gfal2_get_plugin_names(context.cContext)
	cLength := C.helper_strvlengh(cArray)
	slice := (*[1 << 30]*C.char)(unsafe.Pointer(cArray))[:cLength:cLength]

	array := make([]string, cLength)

	for index, name := range slice {
		array[index] = C.GoString(name)
	}

	C.g_strfreev(cArray)
	return array, nil
}

// Cancel running operations, if any.
// Return the number of cancelled operations.
func (context Context) Cancel() int {
	return int(C.gfal2_cancel(context.cContext))
}
