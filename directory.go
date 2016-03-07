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

// Contains information about a directory entry.
type Gfal2Dir struct {
	cDir     *C.DIR
	cContext C.gfal2_context_t
}

// Directory entry.
type Gfal2DirEntry struct {
	cDirent *C.struct_dirent
	cStat   C.struct_stat
}

// File name.
func (entry Gfal2DirEntry) Name() string {
	return C.GoString(&entry.cDirent.d_name[0])
}

// File size.
func (entry Gfal2DirEntry) Size() int64 {
	return int64(entry.cStat.st_size)
}

// File mode.
func (entry Gfal2DirEntry) Mode() os.FileMode {
	return mapMode(C.mode_t(entry.cStat.st_mode))
}

// Modification time.
func (entry Gfal2DirEntry) ModTime() time.Time {
	return time.Unix(int64(entry.cStat.st_mtim.tv_sec), int64(entry.cStat.st_mtim.tv_nsec))
}

// Return true if the file is a directory.
func (entry Gfal2DirEntry) IsDir() bool {
	return entry.Mode().IsDir()
}

// Internal representation.
func (entry Gfal2DirEntry) Sys() interface{} {
	return entry
}

// Number of files in a directory/hard links.
func (entry Gfal2DirEntry) Nlink() int {
	return int(entry.cStat.st_nlink)
}

// Onwer uid.
func (entry Gfal2DirEntry) Uid() int {
	return int(entry.cStat.st_uid)
}

// Group gid.
func (entry Gfal2DirEntry) Gid() int {
	return int(entry.cStat.st_gid)
}

// Access time.
func (entry Gfal2DirEntry) AccessTime() time.Time {
	return time.Unix(int64(entry.cStat.st_atim.tv_sec), int64(entry.cStat.st_atim.tv_nsec))
}

// Change time.
func (entry Gfal2DirEntry) ChangeTime() time.Time {
	return time.Unix(int64(entry.cStat.st_ctim.tv_sec), int64(entry.cStat.st_ctim.tv_nsec))
}

// Open a directory.
func (context Context) Opendir(url string) (*Gfal2Dir, GError) {
	var err *C.GError

	cUrl := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cUrl))

	var dir Gfal2Dir
	dir.cContext = context.cContext
	dir.cDir = C.gfal2_opendir(context.cContext, cUrl, &err)
	if dir.cDir == nil {
		return nil, errorCtoGo(err)
	}

	return &dir, nil
}

// Read a single entry from the directory.
// For the last entry, it returns nil, nil
func (dir Gfal2Dir) Readdir() (Stat, GError) {
	var err *C.GError
	var entry Gfal2DirEntry

	entry.cDirent = C.gfal2_readdirpp(dir.cContext, dir.cDir, &entry.cStat, &err)
	if entry.cDirent == nil && err != nil {
		return nil, errorCtoGo(err)
	} else if entry.cDirent == nil {
		return nil, nil
	}
	return entry, nil
}

// Close the directory and frees the associated memory.
func (dir Gfal2Dir) Close() GError {
	var err *C.GError
	ret := C.gfal2_closedir(dir.cContext, dir.cDir, &err)
	if ret < 0 {
		return errorCtoGo(err)
	}
	return nil
}
