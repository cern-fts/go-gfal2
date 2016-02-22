package main

import (
	"gitlab.cern.ch/dmc/go-gfal2"
	"os"
)

var cmdCat = &Command{
	Name:        "cat",
	Description: "dump the content of a file into the standard output",
}

func init() {
	cmdCat.Run = runCat
}

func runCat(context *gfal2.Context, cmd *Command, args []string) int {
	if len(args) < 1 {
		Log("MAIN", gfal2.LogLevelCritical, "Missing surl")
		return -1
	}

	fd, err := context.Open(args[0])
	if err != nil {
		Log("MAIN", gfal2.LogLevelCritical, "Could not open the file: %s", err.Error())
		return -1
	}
	defer fd.Close()

	buffer := make([]byte, 1024)
	nBytes, err := fd.Read(buffer)

	for ; nBytes > 0 && err == nil; nBytes, err = fd.Read(buffer) {
		os.Stdout.Write(buffer[:nBytes])
	}

	if err != nil {
		Log("MAIN", gfal2.LogLevelCritical, "Could not read the file: %s", err.Error())
		return -1
	}

	return 0
}
