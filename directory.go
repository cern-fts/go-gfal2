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
	"time"
	"unsafe"
)

// Dir contains information about a directory entry.
type Dir struct {
	cDir     *C.DIR
	cContext C.gfal2_context_t
}

// DirEntry models a directory entry.
type DirEntry struct {
	cDirent *C.struct_dirent
	cStat   C.struct_stat
}

// Name returns the file name/
func (entry DirEntry) Name() string {
	return C.GoString(&entry.cDirent.d_name[0])
}

// Size returns the file size.
func (entry DirEntry) Size() int64 {
	return int64(entry.cStat.st_size)
}

// Mode returns the permission bits for the file.
func (entry DirEntry) Mode() os.FileMode {
	return mapMode(C.mode_t(entry.cStat.st_mode))
}

// ModTime returns the modification time of the file.
func (entry DirEntry) ModTime() time.Time {
	return time.Unix(int64(entry.cStat.st_mtim.tv_sec), int64(entry.cStat.st_mtim.tv_nsec))
}

// IsDir returns true if the file is a directory.
func (entry DirEntry) IsDir() bool {
	return entry.Mode().IsDir()
}

// Sys returns the internal representation.
func (entry DirEntry) Sys() interface{} {
	return entry
}

// Nlink returns the number of hard links, or files in a directory.
func (entry DirEntry) Nlink() int {
	return int(entry.cStat.st_nlink)
}

// Uid returns the owner's user id.
func (entry DirEntry) Uid() int {
	return int(entry.cStat.st_uid)
}

// Gid returns the group id.
func (entry DirEntry) Gid() int {
	return int(entry.cStat.st_gid)
}

// AccessTime returns the access time of the file.
func (entry DirEntry) AccessTime() time.Time {
	return time.Unix(int64(entry.cStat.st_atim.tv_sec), int64(entry.cStat.st_atim.tv_nsec))
}

// ChangeTime returns the modification time of the file.
func (entry DirEntry) ChangeTime() time.Time {
	return time.Unix(int64(entry.cStat.st_ctim.tv_sec), int64(entry.cStat.st_ctim.tv_nsec))
}

// Opendir opens a directory.
func (context Context) Opendir(url string) (*Dir, GError) {
	var err *C.GError

	cURL := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cURL))

	var dir Dir
	dir.cContext = context.cContext
	dir.cDir = C.gfal2_opendir(context.cContext, cURL, &err)
	if dir.cDir == nil {
		return nil, errorCtoGo(err)
	}

	return &dir, nil
}

// Readdir reads a single entry from the directory.
// For the last entry, it returns nil, nil
func (dir Dir) Readdir() (Stat, GError) {
	var err *C.GError
	var entry DirEntry

	entry.cDirent = C.gfal2_readdirpp(dir.cContext, dir.cDir, &entry.cStat, &err)
	if entry.cDirent == nil && err != nil {
		return nil, errorCtoGo(err)
	} else if entry.cDirent == nil {
		return nil, nil
	}
	return entry, nil
}

// Close the directory and frees the associated memory.
func (dir Dir) Close() GError {
	var err *C.GError
	ret := C.gfal2_closedir(dir.cContext, dir.cDir, &err)
	if ret < 0 {
		return errorCtoGo(err)
	}
	return nil
}
