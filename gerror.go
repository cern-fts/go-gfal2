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
	"syscall"
)

// GError contains error information coming from gfal2.
type GError interface {
	// Domain of the error. For instance, the plugin that triggered it.
	Domain() string
	// Error code. Values can be the same as those for errno.
	Code() syscall.Errno
	// Error description.
	Error() string
}

type gErrorImpl struct {
	domain  string
	code    syscall.Errno
	message string
}

// Get the error domain.
func (e gErrorImpl) Domain() string {
	return e.domain
}

// Code returns the error code (see errno).
func (e gErrorImpl) Code() syscall.Errno {
	return e.code
}

// Error returns the error message.
func (e gErrorImpl) Error() string {
	return e.message
}

// Convert a C GError to a Go GError.
// Frees the C error .
func errorCtoGo(e *C.GError) gErrorImpl {
	var err gErrorImpl
	err.code = syscall.Errno(e.code)
	err.message = C.GoString((*C.char)(e.message))
	C.g_clear_error(&e)
	return err
}
