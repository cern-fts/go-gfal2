package gfal2

// #cgo pkg-config: gfal2 gfal_transfer
// #include <gfal_api.h>
//
// void monitorCallback(gfalt_transfer_status_t h, const char* src, const char* dst, gpointer user_data);
// void eventCallback(const gfalt_event_t e, gpointer user_data);
import "C"
import (
	"bytes"
	"container/list"
	"unsafe"
)

// Side constants.
const (
	EventSource      = 0
	EventDestination = 1
	EventNone        = 2
)

// Data passed to the event listener.
type Event struct {
	Side        int
	Timestamp   uint64
	Stage       string
	Domain      string
	Description string
}

// Event listener interface.
type EventListener interface {
	NotifyEvent(event Event)
}

// Data passed to the monitor listener.
type Marker struct {
	AvgThroughput     uint64
	InstantThroughput uint64
	BytesTransferred  uint64
	ElapsedTime       uint64
}

// Monitor listener interface.
type MonitorListener interface {
	NotifyPerformanceMarker(marker Marker)
}

// Struct that holds the data required to run the transfers.
type TransferParameters struct {
	cParams  C.gfalt_params_t
	cContext C.gfal2_context_t

	// Keep references to the listener to avoid them being freed
	monitorListeners list.List
	eventListeners list.List
}

// Create a new TransferParameters struct.
func (context Context) NewCopy() (*TransferParameters, GError) {
	var params TransferParameters
	var err *C.GError

	params.cContext = context.cContext
	params.cParams = C.gfalt_params_handle_new(&err)
	if params.cParams == nil {
		return nil, errorCtoGo(err)
	}

	return &params, nil
}

// Copy the TransferParameters struct.
func (params TransferParameters) Copy() (*TransferParameters, GError) {
	var paramsCopy TransferParameters
	var err *C.GError

	paramsCopy.cContext = params.cContext
	paramsCopy.cParams = C.gfalt_params_handle_copy(params.cParams, &err)
	if paramsCopy.cParams == nil {
		return nil, errorCtoGo(err)
	}

	return &paramsCopy, nil
}

// Destroy the TransferParameters.
func (params TransferParameters) Close() GError {
	var err *C.GError

	C.gfalt_params_handle_delete(params.cParams, &err)
	if err != nil {
		errorCtoGo(err)
	}
	return nil
}

// Define the maximum time acceptable for the file tranfer.
func (params TransferParameters) SetTimeout(timeout int) GError {
	var err *C.GError

	ret := C.gfalt_set_timeout(params.cParams, C.guint64(timeout), &err)
	if ret < 0 {
		return errorCtoGo(err)
	}
	return nil
}

// Get the maximum time acceptable for the file transfer.
func (params TransferParameters) GetTimeout() (int, GError) {
	var err *C.GError

	ret := C.gfalt_get_timeout(params.cParams, &err)
	if err != nil {
		return 0, errorCtoGo(err)
	}
	return int(ret), nil
}

// Define the maximum number of parallels connexion to use for the file tranfer.
// Note that not all protocols implement this.
func (params TransferParameters) SetNoStreams(nostreams int) GError {
	var err *C.GError

	ret := C.gfalt_set_nbstreams(params.cParams, C.guint(nostreams), &err)
	if ret < 0 {
		return errorCtoGo(err)
	}
	return nil
}

// Get the number of streams to be used for the transfer.
func (params TransferParameters) GetNoStreams() (int, GError) {
	var err *C.GError

	ret := C.gfalt_get_nbstreams(params.cParams, &err)
	if err != nil {
		return 0, errorCtoGo(err)
	}
	return int(ret), nil
}

// Set the TCP buffer size.
// Note that not all protocols implement this.
func (params TransferParameters) SetTcpBuffersize(size int) GError {
	var err *C.GError

	ret := C.gfalt_set_tcp_buffer_size(params.cParams, C.guint64(size), &err)
	if ret < 0 {
		return errorCtoGo(err)
	}
	return nil
}

// Get the TCP buffer size.
func (params TransferParameters) GetTcpBuffersize() (int, GError) {
	var err *C.GError

	ret := C.gfalt_get_tcp_buffer_size(params.cParams, &err)
	if err != nil {
		return 0, errorCtoGo(err)
	}
	return int(ret), nil
}

// Set source space token.
// Note that not all protocols implement this.
func (params TransferParameters) SetSourceSpacetoken(token string) GError {
	var err *C.GError

	cToken := (*C.char)(C.CString(token))
	defer C.free(unsafe.Pointer(cToken))

	ret := C.gfalt_set_src_spacetoken(params.cParams, cToken, &err)
	if ret < 0 {
		return errorCtoGo(err)
	}
	return nil
}

// Get the source space token.
func (params TransferParameters) GetSourceSpaceToken() (string, GError) {
	var err *C.GError

	ret := C.gfalt_get_src_spacetoken(params.cParams, &err)
	if ret == nil {
		return "", errorCtoGo(err)
	}
	return C.GoString((*C.char)(ret)), nil
}

// Set the destination space token.
// Note that not all protocols implement this.
func (params TransferParameters) SetDestinationSpaceToken(token string) GError {
	var err *C.GError

	cToken := (*C.char)(C.CString(token))
	defer C.free(unsafe.Pointer(cToken))

	ret := C.gfalt_set_dst_spacetoken(params.cParams, cToken, &err)
	if ret < 0 {
		return errorCtoGo(err)
	}
	return nil
}

// Get the destination space token.
func (params TransferParameters) GetDestinationSpaceToken() (string, GError) {
	var err *C.GError

	ret := C.gfalt_get_dst_spacetoken(params.cParams, &err)
	if ret == nil {
		return "", errorCtoGo(err)
	}
	return C.GoString((*C.char)(ret)), nil
}

// If true, if the destination file exists, it will be overwritten.
// If false, if the destination file exists, the transfer will fail.
// If the destination file does not exist, there is, obviously, no difference.
func (params TransferParameters) SetOverwrite(overwrite bool) GError {
	var err *C.GError

	var cOverwrite C.gboolean = 0
	if overwrite {
		cOverwrite = 1
	}

	ret := C.gfalt_set_replace_existing_file(params.cParams, cOverwrite, &err)
	if ret < 0 {
		return errorCtoGo(err)
	}
	return nil
}

// Return the value of the Overwrite flag.
func (params TransferParameters) GetOverwrite() (bool, GError) {
	var err *C.GError

	ret := C.gfalt_get_replace_existing_file(params.cParams, &err)
	if err != nil {
		return false, errorCtoGo(err)
	}
	return ret != 0, nil
}

// If true, only the transfer will be done. Any preparatory work will be skipped.
// For instance, parent directory creation, checking the destination exists, checksum/size validation...
func (params TransferParameters) SetStrictCopy(strict bool) GError {
	var err *C.GError

	var cStrict C.gboolean = 0
	if strict {
		cStrict = 1
	}

	ret := C.gfalt_set_strict_copy_mode(params.cParams, cStrict, &err)
	if ret < 0 {
		return errorCtoGo(err)
	}
	return nil
}

// Return the value of the StrictCopy flag.
func (params TransferParameters) GetStrictCopy() (bool, GError) {
	var err *C.GError

	ret := C.gfalt_get_strict_copy_mode(params.cParams, &err)
	if err != nil {
		return false, errorCtoGo(err)
	}
	return ret != 0, nil
}

// If true, a checksum validation will be done after the transfer.
// If SetChecksum is used, then the source will be validated against that
// value before the transfer takes places.
func (params TransferParameters) EnableChecksum(enable bool) GError {
	var err *C.GError

	var cEnable C.gboolean = 0
	if enable {
		cEnable = 1
	}

	ret := C.gfalt_set_checksum_check(params.cParams, cEnable, &err)
	if ret < 0 {
		return errorCtoGo(err)
	}
	return nil
}

// Return the value of the Checksum flag.
func (params TransferParameters) IsChecksumEnabled() (bool, GError) {
	var err *C.GError

	ret := C.gfalt_get_checksum_check(params.cParams, &err)
	if err != nil {
		return false, errorCtoGo(err)
	}
	return ret != 0, nil
}

// Set a custom checksum type and value. If chkvalue is *not* empty, the source file will
// be validated prior to the transfer.
func (params TransferParameters) SetChecksum(chktype string, chkvalue string) GError {
	var err *C.GError

	cType := (*C.gchar)(C.CString(chktype))
	defer C.free(unsafe.Pointer(cType))
	cValue := (*C.gchar)(C.CString(chkvalue))
	defer C.free(unsafe.Pointer(cValue))

	ret := C.gfalt_set_user_defined_checksum(params.cParams, cType, cValue, &err)
	if ret < 0 {
		return errorCtoGo(err)
	}
	return nil
}

// Get the configured checksum type and value.
func (params TransferParameters) GetChecksum() (string, string, GError) {
	var err *C.GError

	typeBuffer := make([]byte, 256)
	typeBufferPtr := (*C.gchar)(unsafe.Pointer(&typeBuffer[0]))
	valueBuffer := make([]byte, 256)
	valueBufferPtr := (*C.gchar)(unsafe.Pointer(&valueBuffer[0]))

	ret := C.gfalt_get_user_defined_checksum(params.cParams, typeBufferPtr, C.size_t(len(typeBuffer)), valueBufferPtr, C.size_t(len(valueBuffer)), &err)
	if ret < 0 {
		return "", "", errorCtoGo(err)
	}

	nType := bytes.IndexByte(typeBuffer, 0)
	nValue := bytes.IndexByte(valueBuffer, 0)

	return string(typeBuffer[:nType]), string(valueBuffer[:nValue]), nil
}

// If true, the destination parent directory will be created if it does not exist.
// If false, the transfer will fail if the destination parent directory does not exist.
func (params TransferParameters) SetCreateParentDir(create bool) GError {
	var err *C.GError

	var cCreate C.gboolean = 0
	if create {
		cCreate = 1
	}

	ret := C.gfalt_set_create_parent_dir(params.cParams, cCreate, &err)
	if ret < 0 {
		return errorCtoGo(err)
	}
	return nil
}

// Get the value of the CreateParentDir flag.
func (params TransferParameters) GetCreateParentDir() (bool, GError) {
	var err *C.GError

	ret := C.gfalt_get_create_parent_dir(params.cParams, &err)
	if err != nil {
		return false, errorCtoGo(err)
	}
	return ret != 0, nil
}

// Wrapper for callbacks
//export monitorCallbackWrapper
func monitorCallbackWrapper(h C.gfalt_transfer_status_t, src *C.char, dst *C.char, user_data C.gpointer) {
	var err *C.GError

	listener := *(*MonitorListener)(user_data)

	var marker Marker
	marker.AvgThroughput = uint64(C.gfalt_copy_get_average_baudrate(h, &err))
	C.g_clear_error(&err)
	marker.InstantThroughput = uint64(C.gfalt_copy_get_instant_baudrate(h, &err))
	C.g_clear_error(&err)
	marker.BytesTransferred = uint64(C.gfalt_copy_get_bytes_transfered(h, &err))
	C.g_clear_error(&err)
	marker.ElapsedTime = uint64(C.gfalt_copy_get_elapsed_time(h, &err))
	C.g_clear_error(&err)

	listener.NotifyPerformanceMarker(marker)
}

// Add a function to be called with the performance markers data.
func (params TransferParameters) AddMonitorCallback(listener MonitorListener) GError {
	var err *C.GError

	ret := C.gfalt_add_monitor_callback(params.cParams, C.gfalt_monitor_func(C.monitorCallback), C.gpointer(unsafe.Pointer(&listener)), nil, &err)
	if ret < 0 {
		return errorCtoGo(err)
	}
	
	params.monitorListeners.PushBack(listener)

	return nil
}

// Wrapper for callbacks
//export eventCallbackWrapper
func eventCallbackWrapper(cEvent C.gfalt_event_t, user_data C.gpointer) {
	listener := *(*EventListener)(user_data)

	var event Event
	event.Description = C.GoString(cEvent.description)
	event.Domain = C.GoString((*C.char)(C.g_quark_to_string(cEvent.domain)))
	event.Side = int(cEvent.side)
	event.Stage = C.GoString((*C.char)(C.g_quark_to_string(cEvent.stage)))
	event.Timestamp = uint64(cEvent.timestamp)

	listener.NotifyEvent(event)
}

// Add a function to be called when there are events triggered by the plugins.
func (params TransferParameters) AddEventCallback(listener EventListener) GError {
	var err *C.GError

	ret := C.gfalt_add_event_callback(params.cParams, C.gfalt_event_func(C.eventCallback), C.gpointer(unsafe.Pointer(&listener)), nil, &err)
	if ret < 0 {
		return errorCtoGo(err)
	}
	
	params.eventListeners.PushBack(listener)

	return nil
}

// Perform the copy from source into destination.
// If the protocol supports it, it will be a third party copy.
// If the protocol does not support third party copies, then the data will be streamed via the local node.
func (params TransferParameters) CopyFile(source string, destination string) GError {
	var err *C.GError

	cSource := C.CString(source)
	defer C.free(unsafe.Pointer(cSource))
	cDestination := C.CString(destination)
	defer C.free(unsafe.Pointer(cDestination))

	ret := C.gfalt_copy_file(params.cContext, params.cParams, cSource, cDestination, &err)
	if ret < 0 {
		return errorCtoGo(err)
	}
	return nil
}
