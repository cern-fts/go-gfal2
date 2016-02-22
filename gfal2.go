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

type Context struct {
	cContext C.gfal2_context_t
}

// Return the underyling gfal2 version.
func Version() string {
	return C.GoString(C.gfal2_version())
}

// Create a new gfal2 context.
func NewContext() (*Context, GError) {
	var context Context
	var err *C.GError
	context.cContext = C.gfal2_context_new(&err)

	if context.cContext == nil {
		return nil, errorCtoGo(err)
	}

	return &context, nil
}

// Destroy the gfal2 context.
func (context Context) Close() {
	C.gfal2_context_free(context.cContext)
	context.cContext = nil
}

// Get a list with the names and version of the loaded plugins.
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
