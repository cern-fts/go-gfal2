package gfal2

// #cgo pkg-config: gfal2 gfal_transfer
// #include <gfal_api.h>
import "C"

// Contains error information coming from gfal2.
type GError interface {
	// Domain of the error. For instance, the plugin that triggered it.
	Domain() string
	// Error code. Values can be the same as those for errno.
	Code() int32
	// Error description.
	Error() string
}

type GErrorImpl struct {
	domain  string
	code    int32
	message string
}

// Get the error domain.
func (e GErrorImpl) Domain() string {
	return e.domain
}

// Get the error code (see errno).
func (e GErrorImpl) Code() int32 {
	return e.code
}

// Get the error message.
func (e GErrorImpl) Error() string {
	return e.message
}

// Convert a C GError to a Go GError.
// Frees the C error .
func errorCtoGo(e *C.GError) GErrorImpl {
	var err GErrorImpl
	err.code = int32(e.code)
	err.message = C.GoString((*C.char)(e.message))
	C.g_clear_error(&e)
	return err
}
