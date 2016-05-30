package main

import (
	"gitlab.cern.ch/dmc/go-gfal2"
)

var cmdCopy = &Command{
	Name:        "copy",
	Description: "copy a file",
}

func init() {
	cmdCopy.Run = runCopy
}

var overwriteFlag = cmdCopy.Flag.Bool("f", false, "overwrite destination file")
var createParentFlag = cmdCopy.Flag.Bool("p", false, "create destination parent directory")
var checksumFlag = cmdCopy.Flag.Bool("K", false, "enable checksum validation")
var checksumType = cmdCopy.Flag.String("checksum-algo", "adler32", "use the checksum algorithm to validate the copy")
var checksumValue = cmdCopy.Flag.String("checksum-value", "", "user defined checksum")

type CopyListener struct {
	// Just to trigger "cgo argument has Go pointer to Go pointer"
	// Underlying implementation should allow this
	p *string
}

func (_ *CopyListener) NotifyEvent(event gfal2.Event) {
	Log("MAIN", gfal2.LogLevelInfo, "EVENT %s %s %s", event.Domain, event.Stage, event.Description)
}

func (_ *CopyListener) NotifyPerformanceMarker(marker gfal2.Marker) {
	Log("MAIN", gfal2.LogLevelInfo, "MARKER %ds %.2f KB/s %d bytes", marker.ElapsedTime, float32(marker.AvgThroughput)/1024.0, marker.BytesTransferred)
}

func runCopy(context *gfal2.Context, cmd *Command, args []string) int {
	if len(args) < 2 {
		Log("MAIN", gfal2.LogLevelCritical, "Missing source and/or destination surls")
		return -1
	}

	copyHandler, err := context.NewTransferHandler()
	if err != nil {
		Log("MAIN", gfal2.LogLevelCritical, "Failed to create the copy handler")
		return -1
	}

	copyHandler.SetOverwrite(*overwriteFlag)
	copyHandler.SetCreateParentDir(*createParentFlag)
	copyHandler.EnableChecksum(*checksumFlag)
	if *checksumFlag {
		copyHandler.SetChecksum(*checksumType, *checksumValue)
	}

	var listener CopyListener
	copyHandler.AddEventCallback(&listener)
	copyHandler.AddMonitorCallback(&listener)

	err = copyHandler.CopyFile(args[0], args[1])
	if err != nil {
		Log("MAIN", gfal2.LogLevelCritical, "Failed to copy the file: %s", err.Error())
		return -1
	}

	return 0
}
