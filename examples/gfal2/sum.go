package main

import (
	"fmt"
	"gitlab.cern.ch/dmc/go-gfal2"
)

var cmdSum = &Command{
	Name:        "sum",
	Description: "calculate the checksum of the file",
}

func init() {
	cmdSum.Run = runSum
}

func runSum(context *gfal2.Context, cmd *Command, args []string) int {
	if len(args) < 2 {
		Log("MAIN", gfal2.LogLevelCritical, "Missing surl and/or checksum type")
		return -1
	}

	checksum, err := context.Checksum(args[0], args[1], 0, 0)
	if err != nil {
		Log("MAIN", gfal2.LogLevelCritical, "Failed to calculate the checksum: %s", err.Error())
		return -1
	}

	fmt.Printf("%s\t%s\n", checksum, args[0])

	return 0
}
