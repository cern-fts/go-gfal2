package gfal2

// #cgo pkg-config: gfal2 gfal_transfer
// #include <gfal_api.h>
import "C"
import (
	"bytes"
	"unsafe"
)

// Perform a bring online operation. Return the token, or nil and an error.
// If async is false, this method will block until the file is brought online.
// If async if true, this method return immediately, and the caller should use BringOnlinePoll to check for the termination.
func (context Context) BringOnline(url string, pintime int, timeout int, async bool) (string, GError) {
	var err *C.GError

	cUrl := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cUrl))

	buffer := make([]byte, 256)
	bufferPtr := (*C.char)(unsafe.Pointer(&buffer[0]))

	var cAsync C.int = 0
	if async {
		cAsync = 1
	}

	ret := C.gfal2_bring_online(context.cContext, cUrl, C.time_t(pintime), C.time_t(timeout), bufferPtr, C.size_t(len(buffer)), cAsync, &err)
	if ret < 0 {
		return "", errorCtoGo(err)
	}

	n := bytes.IndexByte(buffer, 0)
	return string(buffer[:n]), nil
}

// Check the status of a bring online operation.
// The token was returned by BringOnline.
func (context Context) BringOnlinePoll(url string, token string) GError {
	var err *C.GError

	cUrl := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cUrl))
	cToken := (*C.char)(C.CString(token))
	defer C.free(unsafe.Pointer(cToken))

	ret := C.gfal2_bring_online_poll(context.cContext, cUrl, cToken, &err)
	if ret < 0 {
		return errorCtoGo(err)
	}

	return nil
}

// Release a file, so the storage can remove it from disk.
// The token was returned by BringOnline.
func (context Context) ReleaseFile(url string, token string) GError {
	var err *C.GError

	cUrl := (*C.char)(C.CString(url))
	defer C.free(unsafe.Pointer(cUrl))
	cToken := (*C.char)(C.CString(token))
	defer C.free(unsafe.Pointer(cToken))

	ret := C.gfal2_release_file(context.cContext, cUrl, cToken, &err)
	if ret < 0 {
		return errorCtoGo(err)
	}

	return nil
}

// Bring online a list of files.
// See BringOnline for the meaning of the parameters.
// Return the token *and* a list of errors, one per url. The error will be nil if the file is online,
// or EAGAIN is queued. Other error codes are definite errors.
func (context Context) BringOnlineList(urls []string, pintime int, timeout int, async bool) (string, []GError) {
	nItems := len(urls)

	errs := make([]*C.GError, nItems)
	cUrls := make([]*C.char, nItems)

	buffer := make([]byte, 256)
	bufferPtr := (*C.char)(unsafe.Pointer(&buffer[0]))

	var cAsync C.int = 0
	if async {
		cAsync = 1
	}

	C.gfal2_bring_online_list(context.cContext, C.int(nItems),
		(**C.char)(&cUrls[0]), C.time_t(pintime), C.time_t(timeout),
		bufferPtr, C.size_t(len(buffer)), cAsync, &errs[0])

	n := bytes.IndexByte(buffer, 0)
	token := string(buffer[:n])
	errors := make([]GError, nItems)

	for i := 0; i < nItems; i++ {
		if errs[i] == nil {
			errors[i] = nil
		} else {
			errors[i] = errorCtoGo(errs[i])
		}
	}

	return token, errors
}

// Poll a list of files. See BringOnlinePoll.
func (context Context) BringOnlinePollList(urls []string, token string) []GError {
	nItems := len(urls)

	errs := make([]*C.GError, nItems)
	cUrls := make([]*C.char, nItems)

	cToken := (*C.char)(C.CString(token))
	defer C.free(unsafe.Pointer(cToken))

	C.gfal2_bring_online_poll_list(context.cContext, C.int(nItems),
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

// Release a list of files.
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

// Abort a set of files that are queued for staging.
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
