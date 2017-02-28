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
	"bytes"
	"syscall"
	"unsafe"
)

// BringOnline performs a bring online operation. Return the token, or nil and an error.
// If async is false, this method will block until the file is brought online.
// If async if true, this method return immediately, and the caller should use BringOnlinePoll to check for the termination.
func (context Context) BringOnline(url string, pintime int, timeout int, async bool) (string, GError) {
	var err *C.GError

	cURL := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cURL))

	buffer := make([]byte, 256)
	bufferPtr := (*C.char)(unsafe.Pointer(&buffer[0]))

	var cAsync C.int
	if async {
		cAsync = 1
	}

	ret := C.gfal2_bring_online(context.cContext, cURL, C.time_t(pintime), C.time_t(timeout), bufferPtr, C.size_t(len(buffer)), cAsync, &err)
	if ret < 0 {
		return "", errorCtoGo(err)
	}

	n := bytes.IndexByte(buffer, 0)
	return string(buffer[:n]), nil
}

// BringOnlinePoll checks the status of a bring online operation.
// The token was returned by BringOnline.
func (context Context) BringOnlinePoll(url string, token string) GError {
	var err *C.GError

	cURL := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cURL))
	cToken := (*C.char)(C.CString(token))
	defer C.free(unsafe.Pointer(cToken))

	ret := C.gfal2_bring_online_poll(context.cContext, cURL, cToken, &err)
	if ret < 0 {
		return errorCtoGo(err)
	}

	return nil
}

// ReleaseFile releases a file, so the storage can remove it from disk.
// The token was returned by BringOnline.
func (context Context) ReleaseFile(url string, token string) GError {
	var err *C.GError

	cURL := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cURL))
	cToken := (*C.char)(C.CString(token))
	defer C.free(unsafe.Pointer(cToken))

	ret := C.gfal2_release_file(context.cContext, cURL, cToken, &err)
	if ret < 0 {
		return errorCtoGo(err)
	}

	return nil
}

// BringOnlineList request the staging of a list of files.
// See BringOnline for the meaning of the parameters.
// Return the token *and* a list of errors, one per url. The error will be nil if the file is online,
// or EAGAIN is queued. Other error codes are definite errors.
func (context Context) BringOnlineList(urls []string, pintime int, timeout int, async bool) (string, []GError) {
	nItems := len(urls)

	cErrs := make([]*C.GError, nItems)
	cUrls := make([]*C.char, nItems)

	for i := 0; i < nItems; i++ {
		cUrls[i] = (*C.char)(C.CString(urls[i]))
	}

	buffer := make([]byte, 256)
	bufferPtr := (*C.char)(unsafe.Pointer(&buffer[0]))

	var cAsync C.int
	if async {
		cAsync = 1
	}

	ret := C.gfal2_bring_online_list(context.cContext, C.int(nItems),
		(**C.char)(&cUrls[0]), C.time_t(pintime), C.time_t(timeout),
		bufferPtr, C.size_t(len(buffer)), cAsync, &cErrs[0])

	n := bytes.IndexByte(buffer, 0)
	token := string(buffer[:n])
	errors := make([]GError, nItems)

	for i := 0; i < nItems; i++ {
		C.free(unsafe.Pointer(cUrls[i]))
		if ret == 0 {
			errors[i] = &gErrorImpl{code: syscall.EAGAIN}
		} else if cErrs[i] == nil {
			errors[i] = nil
		} else {
			errors[i] = errorCtoGo(cErrs[i])
		}
	}

	return token, errors
}

// BringOnlinePollList polls a list of files. See BringOnlinePoll.
func (context Context) BringOnlinePollList(urls []string, token string) []GError {
	nItems := len(urls)

	cErrs := make([]*C.GError, nItems)
	cUrls := make([]*C.char, nItems)

	for i := 0; i < nItems; i++ {
		cUrls[i] = (*C.char)(C.CString(urls[i]))
	}

	cToken := (*C.char)(C.CString(token))
	defer C.free(unsafe.Pointer(cToken))

	ret := C.gfal2_bring_online_poll_list(context.cContext, C.int(nItems),
		(**C.char)(&cUrls[0]), cToken, &cErrs[0])

	errors := make([]GError, nItems)
	for i := 0; i < nItems; i++ {
		C.free(unsafe.Pointer(cUrls[i]))
		if ret == 0 {
			errors[i] = &gErrorImpl{code: syscall.EAGAIN}
		} else if cErrs[i] == nil {
			errors[i] = nil
		} else {
			errors[i] = errorCtoGo(cErrs[i])
		}
	}

	return errors
}

// ReleaseFileList releases a list of files.
func (context Context) ReleaseFileList(urls []string, token string) []GError {
	nItems := len(urls)

	errs := make([]*C.GError, nItems)
	cUrls := make([]*C.char, nItems)

	cToken := (*C.char)(C.CString(token))
	defer C.free(unsafe.Pointer(cToken))

	C.gfal2_release_file_list(context.cContext, C.int(nItems),
		(**C.char)(&cUrls[0]), cToken, &errs[0])

	errors := make([]GError, nItems)
	for i := 0; i < nItems; i++ {
		if errs[i] == nil {
			errors[i] = nil
		} else {
			errors[i] = errorCtoGo(errs[i])
		}
	}

	return errors
}

// AbortFiles aborts a set of files that are queued for staging.
func (context Context) AbortFiles(urls []string, token string) []GError {
	nItems := len(urls)

	errs := make([]*C.GError, nItems)
	cUrls := make([]*C.char, nItems)

	cToken := (*C.char)(C.CString(token))
	defer C.free(unsafe.Pointer(cToken))

	C.gfal2_abort_files(context.cContext, C.int(nItems),
		(**C.char)(&cUrls[0]), cToken, &errs[0])

	errors := make([]GError, nItems)
	for i := 0; i < nItems; i++ {
		if errs[i] == nil {
			errors[i] = nil
		} else {
			errors[i] = errorCtoGo(errs[i])
		}
	}

	return errors
}
