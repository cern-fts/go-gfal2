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

// Superset of os.FileInfo
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

// Join of C stat and file name
// Implements gfal2.Stat and, therefore os.FileInfo
type StatAndName struct {
	stat C.struct_stat
	name string
}

// Always return the empty string.
func (stat StatAndName) Name() string {
	return stat.name
}

// File size.
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

// File mode.
func (stat StatAndName) Mode() os.FileMode {
	return mapMode(C.mode_t(stat.stat.st_mode))
}

// Modification time.
func (stat StatAndName) ModTime() time.Time {
	return time.Unix(int64(stat.stat.st_mtim.tv_sec), int64(stat.stat.st_mtim.tv_nsec))
}

// Return true if the file is a directory.
func (stat StatAndName) IsDir() bool {
	return stat.Mode().IsDir()
}

// Internal representation.
func (stat StatAndName) Sys() interface{} {
	return stat
}

// Map nlink.
func (stat StatAndName) Nlink() int {
	return int(stat.stat.st_nlink)
}

// Owner id.
func (stat StatAndName) Uid() int {
	return int(stat.stat.st_uid)
}

// Group id.
func (stat StatAndName) Gid() int {
	return int(stat.stat.st_gid)
}

// Access time.
func (stat StatAndName) AccessTime() time.Time {
	return time.Unix(int64(stat.stat.st_atim.tv_sec), int64(stat.stat.st_atim.tv_nsec))
}

// Change time.
func (stat StatAndName) ChangeTime() time.Time {
	return time.Unix(int64(stat.stat.st_ctim.tv_sec), int64(stat.stat.st_ctim.tv_nsec))
}

// Get the checksum of a url.
// chktype is the algorithm to use (md5, adler32, sha1...). Support depends on the underlying protocol and storage.
// The checksum can be calculated with an offset and length. If both are 0, then the checksum is for the whole file.
func (context Context) Checksum(url string, chktype string, offset uint64, length uint64) (string, GError) {
	var err *C.GError

	cUrl := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cUrl))
	cType := (*C.char)(C.CString(chktype))
	defer C.free(unsafe.Pointer(cType))

	buffer := make([]byte, 256)
	bufferPtr := (*C.char)(unsafe.Pointer(&buffer[0]))

	ret := C.gfal2_checksum(context.cContext, cUrl, cType, C.off_t(offset), C.size_t(length), bufferPtr, C.size_t(len(buffer)), &err)
	if ret < 0 {
		return "", errorCtoGo(err)
	}

	n := bytes.IndexByte(buffer, 0)
	return string(buffer[:n]), nil
}

// Check if the user has permission for the given file.
// For mode, check the values of F_OK, R_OK, W_OK and X_OK.
func (context Context) Access(url string, mode int) GError {
	var err *C.GError

	cUrl := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cUrl))

	ret := C.gfal2_access(context.cContext, cUrl, C.int(mode), &err)
	if ret < 0 {
		return errorCtoGo(err)
	}

	return nil
}

// Change the mode of a file.
func (context Context) Chmod(url string, mode os.FileMode) GError {
	var err *C.GError

	cUrl := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cUrl))

	ret := C.gfal2_chmod(context.cContext, cUrl, C.mode_t(mode), &err)
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

	cUrl := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cUrl))

	var stat StatAndName
	ret := C.gfal2_stat(context.cContext, cUrl, &stat.stat, &err)
	if ret < 0 {
		return nil, errorCtoGo(err)
	}

	stat.name = path.Base(url)

	return stat, nil
}

// Stat a file, but if url is a symlink, stat it rather than the target.
func (context Context) Lstat(url string) (Stat, GError) {
	var err *C.GError

	cUrl := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cUrl))

	var stat StatAndName
	ret := C.gfal2_lstat(context.cContext, cUrl, &stat.stat, &err)
	if ret < 0 {
		return nil, errorCtoGo(err)
	}
	
	stat.name = path.Base(url)

	return stat, nil
}

// Create a directory. Do not create intermediate parents.
func (context Context) Mkdir(url string, mode os.FileMode) GError {
	var err *C.GError

	cUrl := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cUrl))

	ret := C.gfal2_mkdir(context.cContext, cUrl, C.mode_t(mode), &err)
	if ret < 0 {
		return errorCtoGo(err)
	}

	return nil
}

// Create a directory and intermediate parents if required.
func (context Context) MkdirAll(url string, mode os.FileMode) GError {
	var err *C.GError

	cUrl := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cUrl))

	ret := C.gfal2_mkdir_rec(context.cContext, cUrl, C.mode_t(mode), &err)
	if ret < 0 {
		return errorCtoGo(err)
	}

	return nil
}

// Remove the file or directory.
func (context Context) Remove(url string) GError {
	info, gerr := context.Stat(url)
	if gerr != nil {
		return gerr
	}

	cUrl := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cUrl))

	var err *C.GError
	var ret C.int
	if info.IsDir() {
		ret = C.gfal2_rmdir(context.cContext, cUrl, &err)
	} else {
		ret = C.gfal2_unlink(context.cContext, cUrl, &err)
	}

	if ret < 0 {
		return errorCtoGo(err)
	}
	return nil
}

// Create a symlink.
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

// Get the target of a symbolic link.
func (context Context) Readlink(url string) (string, GError) {
	var err *C.GError

	cUrl := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cUrl))

	buffer := make([]byte, 256)
	bufferPtr := (*C.char)(unsafe.Pointer(&buffer[0]))

	ret := C.gfal2_readlink(context.cContext, cUrl, bufferPtr, C.size_t(len(buffer)), &err)
	if ret < 0 {
		return "", errorCtoGo(err)
	}

	n := bytes.IndexByte(buffer, 0)
	return string(buffer[:n]), nil
}

// Get the list of extended attributes of a file.
func (context Context) Listxattr(url string) ([]string, GError) {
	var err *C.GError

	cUrl := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cUrl))

	buffer := make([]byte, 1024)
	bufferPtr := (*C.char)(unsafe.Pointer(&buffer[0]))

	ret := C.gfal2_listxattr(context.cContext, cUrl, bufferPtr, C.size_t(len(buffer)), &err)
	if ret < 0 {
		return nil, errorCtoGo(err)
	}

	allXattr := string(buffer[:ret])

	return strings.Split(allXattr, "\x00"), nil
}

// Get an extended attribute of a file.
func (context Context) Getxattr(url string, name string) (string, GError) {
	var err *C.GError

	cUrl := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cUrl))
	cName := (*C.char)(C.CString(name))
	defer C.free(unsafe.Pointer(cName))

	buffer := make([]byte, 1024)
	bufferPtr := (*C.void)(unsafe.Pointer(&buffer[0]))

	ret := C.gfal2_getxattr(context.cContext, cUrl, cName, unsafe.Pointer(bufferPtr), C.size_t(len(buffer)), &err)
	if ret < 0 {
		return "", errorCtoGo(err)
	}

	n := bytes.IndexByte(buffer, 0)
	return string(buffer[:n]), nil
}

// Set an extended attribute of a file.
// If flags is 1 (create), fail if the attribute already exists. If 2 (replace), fail if the attribute does not exist.
// If 0, the attribute will be set either way.
func (context Context) Setxattr(url string, name string, value string, flags int) GError {
	var err *C.GError

	cUrl := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cUrl))
	cName := (*C.char)(C.CString(name))
	defer C.free(unsafe.Pointer(cName))
	cValue := (*C.char)(C.CString(value))
	defer C.free(unsafe.Pointer(cValue))

	ret := C.gfal2_setxattr(context.cContext, cUrl, cName, unsafe.Pointer(cValue), C.size_t(len(value)), C.int(flags), &err)
	if ret < 0 {
		return errorCtoGo(err)
	}

	return nil
}
