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
	"os"
	"unsafe"
)

// File contains the required data to operate on a file.
type File struct {
	cFd      C.int
	cContext C.gfal2_context_t
}

// Open a file in read only mode.
func (context Context) Open(url string) (*File, GError) {
	return context.OpenFile(url, os.O_RDONLY, 0)
}

// OpenFile opens a file allowing to specify if want to read, write, create, etc.
// flag is a combination of os.RD_ONLY, os.WR_ONLY, etc.)
// perm must be set to the posix permissions desired if the file is to be created.
func (context Context) OpenFile(url string, flag int, perm os.FileMode) (*File, GError) {
	var err *C.GError

	cURL := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cURL))

	var fd File
	fd.cContext = context.cContext
	fd.cFd = C.gfal2_open2(context.cContext, cURL, C.int(flag), C.mode_t(perm), &err)
	if fd.cFd < 0 {
		return nil, errorCtoGo(err)
	}

	return &fd, nil
}

// Close a file and frees the associated memory.
func (fd File) Close() GError {
	var err *C.GError

	ret := C.gfal2_close(fd.cContext, fd.cFd, &err)
	if ret < 0 {
		return errorCtoGo(err)
	}

	return nil
}

// Create is a shortcut for creating and open to write a file.
func (context Context) Create(url string) (*File, GError) {
	return context.OpenFile(url, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0777)
}

// Read reads up to len(b) bytes from the Gfal2File.
// It returns the number of bytes read and an error, if any.
// EOF is signaled by a zero count.
// On error the count is negative.
func (fd File) Read(b []byte) (int, GError) {
	var err *C.GError

	bufferPtr := (*C.void)(unsafe.Pointer(&b[0]))

	ret := C.gfal2_read(fd.cContext, fd.cFd, unsafe.Pointer(bufferPtr), C.size_t(len(b)), &err)
	if ret < 0 {
		return -1, errorCtoGo(err)
	}

	return int(ret), nil
}

// Write len(b) bytes from b into the Gfal2File.
// It returns the number of bytes written and an error, if any.
// On error the count is negative.
func (fd File) Write(b []byte) (int, GError) {
	var err *C.GError

	bufferPtr := (*C.void)(unsafe.Pointer(&b[0]))

	ret := C.gfal2_write(fd.cContext, fd.cFd, unsafe.Pointer(bufferPtr), C.size_t(len(b)), &err)
	if ret < 0 {
		return -1, errorCtoGo(err)
	}

	return int(ret), nil
}

// Seek changes the cursor position in the Gfal2File.
// whence: 0 means relative to the origin of the file, 1 means relative to the current offset,
// and 2 means relative to the end.
// It returns the new offset and an error, if any.
func (fd File) Seek(offset int64, whence int) (int64, GError) {
	var err *C.GError

	ret := C.gfal2_lseek(fd.cContext, fd.cFd, C.off_t(offset), C.int(whence), &err)
	if ret < 0 {
		return -1, errorCtoGo(err)
	}

	return int64(ret), nil
}

// Flush the Gfal2File.
func (fd File) Flush() GError {
	var err *C.GError

	ret := C.gfal2_flush(fd.cContext, fd.cFd, &err)
	if ret < 0 {
		return errorCtoGo(err)
	}

	return nil
}
