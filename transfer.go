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
//
// void monitorCallback(gfalt_transfer_status_t h, const char* src, const char* dst, gpointer user_data);
// void eventCallback(const gfalt_event_t e, gpointer user_data);
import "C"
import (
	"bytes"
	"unsafe"
)

// Side constants.
const (
	EventSource      = 0
	EventDestination = 1
	EventNone        = 2
)

// Checksum type constants
const (
	ChecksumNone   = 0x00
	ChecksumSource = 0x01
	ChecksumTarget = 0x02
	ChecksumBoth   = (ChecksumSource | ChecksumTarget)
)

// Event stores the data passed to the event listener.
type Event struct {
	Side        int
	Timestamp   uint64
	Stage       string
	Domain      string
	Description string
}

// EventListener must be implemented by callbacks that want to be notified by
// events triggered inside gfal2.
type EventListener interface {
	NotifyEvent(event Event)
}

// Marker stores the data passed to the monitor listener.
type Marker struct {
	AvgThroughput     uint64
	InstantThroughput uint64
	BytesTransferred  uint64
	ElapsedTime       uint64
}

// MonitorListener must be implemented by callbacks that want to be notified by the
// transfer progress.
type MonitorListener interface {
	NotifyPerformanceMarker(marker Marker)
}

// TransferHandler holds the data required to run the transfers.
type TransferHandler struct {
	cParams  C.gfalt_params_t
	cContext C.gfal2_context_t
}

// Global references to the listeners
var monitorListeners []MonitorListener
var eventListeners []EventListener

// NewTransferHandler creates a new TransferParameters struct.
func (context Context) NewTransferHandler() (*TransferHandler, GError) {
	var params TransferHandler
	var err *C.GError

	params.cContext = context.cContext
	params.cParams = C.gfalt_params_handle_new(&err)
	if params.cParams == nil {
		return nil, errorCtoGo(err)
	}

	return &params, nil
}

// Copy the TransferParameters struct.
func (params TransferHandler) Copy() (*TransferHandler, GError) {
	var paramsCopy TransferHandler
	var err *C.GError

	paramsCopy.cContext = params.cContext
	paramsCopy.cParams = C.gfalt_params_handle_copy(params.cParams, &err)
	if paramsCopy.cParams == nil {
		return nil, errorCtoGo(err)
	}

	return &paramsCopy, nil
}

// Close destroys the TransferParameters.
func (params TransferHandler) Close() GError {
	var err *C.GError

	C.gfalt_params_handle_delete(params.cParams, &err)
	if err != nil {
		errorCtoGo(err)
	}
	return nil
}

// SetTimeout sets the maximum time acceptable for the file transfer.
func (params TransferHandler) SetTimeout(timeout int) GError {
	var err *C.GError

	ret := C.gfalt_set_timeout(params.cParams, C.guint64(timeout), &err)
	if ret < 0 {
		return errorCtoGo(err)
	}
	return nil
}

// GetTimeout returns the maximum time acceptable for the file transfer.
func (params TransferHandler) GetTimeout() int {
	var err *C.GError

	ret := C.gfalt_get_timeout(params.cParams, &err)
	if err != nil {
		return 0
	}
	return int(ret)
}

// SetNoStreams sets the maximum number of parallels connexion to use for the file transfer.
// Note that not all protocols implement this.
func (params TransferHandler) SetNoStreams(nostreams int) GError {
	var err *C.GError

	ret := C.gfalt_set_nbstreams(params.cParams, C.guint(nostreams), &err)
	if ret < 0 {
		return errorCtoGo(err)
	}
	return nil
}

// GetNoStreams returns the configured number of streams to be used for the transfer.
func (params TransferHandler) GetNoStreams() int {
	var err *C.GError

	ret := C.gfalt_get_nbstreams(params.cParams, &err)
	if err != nil {
		return 0
	}
	return int(ret)
}

// SetTCPBuffersize sets the TCP buffer size. 0 for system default.
// Note that not all protocols implement this.
func (params TransferHandler) SetTCPBuffersize(size int) GError {
	var err *C.GError

	ret := C.gfalt_set_tcp_buffer_size(params.cParams, C.guint64(size), &err)
	if ret < 0 {
		return errorCtoGo(err)
	}
	return nil
}

// GetTCPBuffersize returns the configured TCP buffer size.
func (params TransferHandler) GetTCPBuffersize() int {
	var err *C.GError

	ret := C.gfalt_get_tcp_buffer_size(params.cParams, &err)
	if err != nil {
		return 0
	}
	return int(ret)
}

// SetSourceSpacetoken sets source space token.
// Note that not all protocols implement this.
func (params TransferHandler) SetSourceSpacetoken(token string) GError {
	var err *C.GError

	cToken := (*C.char)(C.CString(token))
	defer C.free(unsafe.Pointer(cToken))

	ret := C.gfalt_set_src_spacetoken(params.cParams, cToken, &err)
	if ret < 0 {
		return errorCtoGo(err)
	}
	return nil
}

// GetSourceSpaceToken returns the source space token.
func (params TransferHandler) GetSourceSpaceToken() string {
	var err *C.GError

	ret := C.gfalt_get_src_spacetoken(params.cParams, &err)
	if ret == nil {
		return ""
	}
	return C.GoString((*C.char)(ret))
}

// SetDestinationSpaceToken sets the destination space token.
// Note that not all protocols implement this.
func (params TransferHandler) SetDestinationSpaceToken(token string) GError {
	var err *C.GError

	cToken := (*C.char)(C.CString(token))
	defer C.free(unsafe.Pointer(cToken))

	ret := C.gfalt_set_dst_spacetoken(params.cParams, cToken, &err)
	if ret < 0 {
		return errorCtoGo(err)
	}
	return nil
}

// GetDestinationSpaceToken returns the destination space token.
func (params TransferHandler) GetDestinationSpaceToken() string {
	var err *C.GError

	ret := C.gfalt_get_dst_spacetoken(params.cParams, &err)
	if ret == nil {
		return ""
	}
	return C.GoString((*C.char)(ret))
}

// SetOverwrite sets if the destination file should be overwritten if it exists.
// If false, if the destination file exists, the transfer will fail.
func (params TransferHandler) SetOverwrite(overwrite bool) GError {
	var err *C.GError

	var cOverwrite C.gboolean
	if overwrite {
		cOverwrite = 1
	}

	ret := C.gfalt_set_replace_existing_file(params.cParams, cOverwrite, &err)
	if ret < 0 {
		return errorCtoGo(err)
	}
	return nil
}

// GetOverwrite returns the value of the Overwrite flag.
func (params TransferHandler) GetOverwrite() bool {
	var err *C.GError

	ret := C.gfalt_get_replace_existing_file(params.cParams, &err)
	if err != nil {
		return false
	}
	return ret != 0
}

// SetStrictCopy sets if the transfer should do additional validation steps(true) or not.
// For instance, parent directory creation, checking the destination exists, checksum/size validation...
func (params TransferHandler) SetStrictCopy(strict bool) GError {
	var err *C.GError

	var cStrict C.gboolean
	if strict {
		cStrict = 1
	}

	ret := C.gfalt_set_strict_copy_mode(params.cParams, cStrict, &err)
	if ret < 0 {
		return errorCtoGo(err)
	}
	return nil
}

// GetStrictCopy returns the value of the StrictCopy flag.
func (params TransferHandler) GetStrictCopy() bool {
	var err *C.GError

	ret := C.gfalt_get_strict_copy_mode(params.cParams, &err)
	if err != nil {
		return false
	}
	return ret != 0
}

// GetChecksumMode returns the checksum mode to be used.
func (params TransferHandler) GetChecksumMode() (int, GError) {
	var err *C.GError

	ret := C.gfalt_get_checksum_mode(params.cParams, &err)
	if err != nil {
		return -1, errorCtoGo(err)
	}
	return int(ret), nil
}

// SetChecksum sets a custom checksum type and value. If chkvalue is *not* empty, the source file will
// be validated prior to the transfer.
func (params TransferHandler) SetChecksum(mode int, chktype string, chkvalue string) GError {
	var err *C.GError

	cType := (*C.gchar)(C.CString(chktype))
	defer C.free(unsafe.Pointer(cType))
	cValue := (*C.gchar)(C.CString(chkvalue))
	defer C.free(unsafe.Pointer(cValue))

	ret := C.gfalt_set_checksum(params.cParams, C.gfalt_checksum_mode_t(mode), cType, cValue, &err)
	if ret < 0 {
		return errorCtoGo(err)
	}
	return nil
}

// GetChecksum returns the configured checksum type and value.
func (params TransferHandler) GetChecksum() (int, string, string) {
	var err *C.GError

	typeBuffer := make([]byte, 256)
	typeBufferPtr := (*C.gchar)(unsafe.Pointer(&typeBuffer[0]))
	valueBuffer := make([]byte, 256)
	valueBufferPtr := (*C.gchar)(unsafe.Pointer(&valueBuffer[0]))

	mode := C.gfalt_get_checksum(params.cParams, typeBufferPtr, C.size_t(len(typeBuffer)), valueBufferPtr, C.size_t(len(valueBuffer)), &err)
	if mode < 0 {
		return -1, "", ""
	}

	nType := bytes.IndexByte(typeBuffer, 0)
	nValue := bytes.IndexByte(valueBuffer, 0)

	return int(mode), string(typeBuffer[:nType]), string(valueBuffer[:nValue])
}

// SetCreateParentDir sets if the parent directory should be created or not if it doesn't exist.
func (params TransferHandler) SetCreateParentDir(create bool) GError {
	var err *C.GError

	var cCreate C.gboolean
	if create {
		cCreate = 1
	}

	ret := C.gfalt_set_create_parent_dir(params.cParams, cCreate, &err)
	if ret < 0 {
		return errorCtoGo(err)
	}
	return nil
}

// GetCreateParentDir returns the value of the CreateParentDir flag.
func (params TransferHandler) GetCreateParentDir() bool {
	var err *C.GError

	ret := C.gfalt_get_create_parent_dir(params.cParams, &err)
	if err != nil {
		return false
	}
	return ret != 0
}

// Wrapper for callbacks
//export monitorCallbackWrapper
func monitorCallbackWrapper(h C.gfalt_transfer_status_t, src *C.char, dst *C.char, userData C.gpointer) {
	var err *C.GError

	listener := uintptr(userData)

	var marker Marker
	marker.AvgThroughput = uint64(C.gfalt_copy_get_average_baudrate(h, &err))
	C.g_clear_error(&err)
	marker.InstantThroughput = uint64(C.gfalt_copy_get_instant_baudrate(h, &err))
	C.g_clear_error(&err)
	marker.BytesTransferred = uint64(C.gfalt_copy_get_bytes_transfered(h, &err))
	C.g_clear_error(&err)
	marker.ElapsedTime = uint64(C.gfalt_copy_get_elapsed_time(h, &err))
	C.g_clear_error(&err)

	monitorListeners[listener].NotifyPerformanceMarker(marker)
}

// AddMonitorCallback adds a function to be called with the performance markers data.
func (params TransferHandler) AddMonitorCallback(listener MonitorListener) GError {
	var err *C.GError

	monitorListeners = append(monitorListeners, listener)

	ret := C.gfalt_add_monitor_callback(
		params.cParams,
		C.gfalt_monitor_func(C.monitorCallback),
		uintptr(len(monitorListeners)-1),
		nil,
		&err,
	)
	if ret < 0 {
		return errorCtoGo(err)
	}

	return nil
}

// Wrapper for callbacks
//export eventCallbackWrapper
func eventCallbackWrapper(cEvent C.gfalt_event_t, userData C.gpointer) {
	listener := uintptr(userData)

	var event Event
	event.Description = C.GoString(cEvent.description)
	event.Domain = C.GoString((*C.char)(C.g_quark_to_string(cEvent.domain)))
	event.Side = int(cEvent.side)
	event.Stage = C.GoString((*C.char)(C.g_quark_to_string(cEvent.stage)))
	event.Timestamp = uint64(cEvent.timestamp)

	eventListeners[listener].NotifyEvent(event)
}

// AddEventCallback adds a function to be called when there are events triggered by the plugins.
func (params TransferHandler) AddEventCallback(listener EventListener) GError {
	var err *C.GError

	eventListeners = append(eventListeners, listener)

	ret := C.gfalt_add_event_callback(
		params.cParams,
		C.gfalt_event_func(C.eventCallback),
		uintptr(len(eventListeners)-1),
		nil,
		&err,
	)
	if ret < 0 {
		return errorCtoGo(err)
	}

	return nil
}

// CopyFile copies the source file into destination.
// If the protocol supports it, it will be a third party copy.
// If the protocol does not support third party copies, then the data will be streamed via the local node.
func (params TransferHandler) CopyFile(source string, destination string) GError {
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
