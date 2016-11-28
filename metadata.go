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
	"os"
	"path"
	"strings"
	"time"
	"unsafe"
)

// Stat is a superset of os.FileInfo
type Stat interface {
	Name() string       // base name of the file
	Size() int64        // length in bytes for regular files; system-dependent for others
	Mode() os.FileMode  // file mode bits
	ModTime() time.Time // modification time
	IsDir() bool        // abbreviation for Mode().IsDir()
	Sys() interface{}   // underlying data source (can return nil)

	Nlink() int // number of files in a directory/number of hard links
	Uid() int   // owner id
	Gid() int   // group id

	AccessTime() time.Time // access time
	ChangeTime() time.Time // modification time
}

// StatAndName is a join of C stat and file name
// Implements gfal2.Stat and, therefore os.FileInfo
type StatAndName struct {
	stat C.struct_stat
	name string
}

// Name returns the file name.
func (stat StatAndName) Name() string {
	return stat.name
}

// Size returns the file size.
func (stat StatAndName) Size() int64 {
	return int64(stat.stat.st_size)
}

func mapMode(posixMode C.mode_t) os.FileMode {
	mode := (os.FileMode(posixMode) & 0777)

	switch posixMode & 0170000 {
	case 0040000:
		mode = mode | os.ModeDir
	case 0120000:
		mode = mode | os.ModeSymlink
	case 0060000:
		mode = mode | os.ModeDevice
	case 0020000:
		mode = mode | os.ModeCharDevice
	case 0140000:
		mode = mode | os.ModeSocket
	}

	return mode
}

// Mode returns the permission bits.
func (stat StatAndName) Mode() os.FileMode {
	return mapMode(C.mode_t(stat.stat.st_mode))
}

// ModTime returns the modification time of the file.
func (stat StatAndName) ModTime() time.Time {
	return time.Unix(int64(stat.stat.st_mtim.tv_sec), int64(stat.stat.st_mtim.tv_nsec))
}

// IsDir returns true if the file is a directory.
func (stat StatAndName) IsDir() bool {
	return stat.Mode().IsDir()
}

// Sys returns the internal representation.
func (stat StatAndName) Sys() interface{} {
	return stat
}

// Nlink returns the number of links that point to this file.
func (stat StatAndName) Nlink() int {
	return int(stat.stat.st_nlink)
}

// Uid returns the user id that owns the file.
func (stat StatAndName) Uid() int {
	return int(stat.stat.st_uid)
}

// Gid returns the group id.
func (stat StatAndName) Gid() int {
	return int(stat.stat.st_gid)
}

// AccessTime returns the access time of the file.
func (stat StatAndName) AccessTime() time.Time {
	return time.Unix(int64(stat.stat.st_atim.tv_sec), int64(stat.stat.st_atim.tv_nsec))
}

// ChangeTime returns the modification time of the file.
func (stat StatAndName) ChangeTime() time.Time {
	return time.Unix(int64(stat.stat.st_ctim.tv_sec), int64(stat.stat.st_ctim.tv_nsec))
}

// Checksum returns the checksum of a url.
// chktype is the algorithm to use (md5, adler32, sha1...). Support depends on the underlying protocol and storage.
// The checksum can be calculated with an offset and length. If both are 0, then the checksum is for the whole file.
func (context Context) Checksum(url string, chktype string, offset uint64, length uint64) (string, GError) {
	var err *C.GError

	cURL := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cURL))
	cType := (*C.char)(C.CString(chktype))
	defer C.free(unsafe.Pointer(cType))

	buffer := make([]byte, 256)
	bufferPtr := (*C.char)(unsafe.Pointer(&buffer[0]))

	ret := C.gfal2_checksum(context.cContext, cURL, cType, C.off_t(offset), C.size_t(length), bufferPtr, C.size_t(len(buffer)), &err)
	if ret < 0 {
		return "", errorCtoGo(err)
	}

	n := bytes.IndexByte(buffer, 0)
	return string(buffer[:n]), nil
}

// Access checks if the user has permission for the given file.
// For mode, check the values of F_OK, R_OK, W_OK and X_OK.
func (context Context) Access(url string, mode int) GError {
	var err *C.GError

	cURL := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cURL))

	ret := C.gfal2_access(context.cContext, cURL, C.int(mode), &err)
	if ret < 0 {
		return errorCtoGo(err)
	}

	return nil
}

// Chmod changes  the mode of a file.
func (context Context) Chmod(url string, mode os.FileMode) GError {
	var err *C.GError

	cURL := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cURL))

	ret := C.gfal2_chmod(context.cContext, cURL, C.mode_t(mode), &err)
	if ret < 0 {
		return errorCtoGo(err)
	}

	return nil
}

// Rename a file. Source and destination must be on the same endpoint.
func (context Context) Rename(oldName string, newName string) GError {
	var err *C.GError

	cOld := (*C.char)(C.CString(oldName))
	defer C.free(unsafe.Pointer(cOld))
	cNew := (*C.char)(C.CString(newName))
	defer C.free(unsafe.Pointer(cNew))

	ret := C.gfal2_rename(context.cContext, cOld, cNew, &err)
	if ret < 0 {
		return errorCtoGo(err)
	}

	return nil
}

// Stat a file.
func (context Context) Stat(url string) (Stat, GError) {
	var err *C.GError

	cURL := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cURL))

	var stat StatAndName
	ret := C.gfal2_stat(context.cContext, cURL, &stat.stat, &err)
	if ret < 0 {
		return nil, errorCtoGo(err)
	}

	stat.name = path.Base(url)

	return stat, nil
}

// Lstat stats a file, but if url is a symlink, stat it rather than the target.
func (context Context) Lstat(url string) (Stat, GError) {
	var err *C.GError

	cURL := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cURL))

	var stat StatAndName
	ret := C.gfal2_lstat(context.cContext, cURL, &stat.stat, &err)
	if ret < 0 {
		return nil, errorCtoGo(err)
	}

	stat.name = path.Base(url)

	return stat, nil
}

// Mkdir creates a directory. Do not create intermediate parents.
func (context Context) Mkdir(url string, mode os.FileMode) GError {
	var err *C.GError

	cURL := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cURL))

	ret := C.gfal2_mkdir(context.cContext, cURL, C.mode_t(mode), &err)
	if ret < 0 {
		return errorCtoGo(err)
	}

	return nil
}

// MkdirAll creates a directory and intermediate parents if required.
func (context Context) MkdirAll(url string, mode os.FileMode) GError {
	var err *C.GError

	cURL := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cURL))

	ret := C.gfal2_mkdir_rec(context.cContext, cURL, C.mode_t(mode), &err)
	if ret < 0 {
		return errorCtoGo(err)
	}

	return nil
}

// Remove deletes a file or directory.
func (context Context) Remove(url string) GError {
	info, gerr := context.Stat(url)
	if gerr != nil {
		return gerr
	}

	cURL := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cURL))

	var err *C.GError
	var ret C.int
	if info.IsDir() {
		ret = C.gfal2_rmdir(context.cContext, cURL, &err)
	} else {
		ret = C.gfal2_unlink(context.cContext, cURL, &err)
	}

	if ret < 0 {
		return errorCtoGo(err)
	}
	return nil
}

// Symlink creates a symlink.
func (context Context) Symlink(source string, target string) GError {
	var err *C.GError

	cSource := (*C.char)(C.CString(source))
	defer C.free(unsafe.Pointer(cSource))
	cTarget := (*C.char)(C.CString(target))
	defer C.free(unsafe.Pointer(cTarget))

	ret := C.gfal2_symlink(context.cContext, cSource, cTarget, &err)
	if ret < 0 {
		return errorCtoGo(err)
	}

	return nil
}

// Readlink returns the target of a symbolic link.
func (context Context) Readlink(url string) (string, GError) {
	var err *C.GError

	cURL := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cURL))

	buffer := make([]byte, 256)
	bufferPtr := (*C.char)(unsafe.Pointer(&buffer[0]))

	ret := C.gfal2_readlink(context.cContext, cURL, bufferPtr, C.size_t(len(buffer)), &err)
	if ret < 0 {
		return "", errorCtoGo(err)
	}

	n := bytes.IndexByte(buffer, 0)
	return string(buffer[:n]), nil
}

// Listxattr returns the list of extended attributes of a file.
func (context Context) Listxattr(url string) ([]string, GError) {
	var err *C.GError

	cURL := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cURL))

	buffer := make([]byte, 1024)
	bufferPtr := (*C.char)(unsafe.Pointer(&buffer[0]))

	ret := C.gfal2_listxattr(context.cContext, cURL, bufferPtr, C.size_t(len(buffer)), &err)
	if ret < 0 {
		return nil, errorCtoGo(err)
	}

	allXattr := string(buffer[:ret])

	return strings.Split(allXattr, "\x00"), nil
}

// Getxattr returns an extended attribute of a file.
func (context Context) Getxattr(url string, name string) (string, GError) {
	var err *C.GError

	cURL := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cURL))
	cName := (*C.char)(C.CString(name))
	defer C.free(unsafe.Pointer(cName))

	buffer := make([]byte, 1024)
	bufferPtr := (*C.void)(unsafe.Pointer(&buffer[0]))

	ret := C.gfal2_getxattr(context.cContext, cURL, cName, unsafe.Pointer(bufferPtr), C.size_t(len(buffer)), &err)
	if ret < 0 {
		return "", errorCtoGo(err)
	}

	n := bytes.IndexByte(buffer, 0)
	return string(buffer[:n]), nil
}

// Setxattr sets an extended attribute of a file.
// If flags is 1 (create), fail if the attribute already exists. If 2 (replace), fail if the attribute does not exist.
// If 0, the attribute will be set either way.
func (context Context) Setxattr(url string, name string, value string, flags int) GError {
	var err *C.GError

	cURL := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cURL))
	cName := (*C.char)(C.CString(name))
	defer C.free(unsafe.Pointer(cName))
	cValue := (*C.char)(C.CString(value))
	defer C.free(unsafe.Pointer(cValue))

	ret := C.gfal2_setxattr(context.cContext, cURL, cName, unsafe.Pointer(cValue), C.size_t(len(value)), C.int(flags), &err)
	if ret < 0 {
		return errorCtoGo(err)
	}

	return nil
}
