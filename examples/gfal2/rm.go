package main

import (
	"gitlab.cern.ch/dmc/go-gfal2"
)

var cmdRm = &Command{
	Name:        "rm",
	Description: "remove a file or directory",
}

func init() {
	cmdRm.Run = runRm
}

func runRm(context *gfal2.Context, cmd *Command, args []string) int {
	if len(args) < 1 {
		Log("MAIN", gfal2.LogLevelCritical, "Missing surl")
		return -1
	}

	err := context.Remove(args[0])
	if err != nil {
		Log("MAIN", gfal2.LogLevelCritical, "Could not remove the file: %s", err.Error())
		return -1
	}

	return 0
}
