package main

import (
	"fmt"
	"gitlab.cern.ch/dmc/go-gfal2"
)

var cmdVersion = &Command{
	Name:        "version",
	Run:         runVersion,
	Description: "print gfal2 version",
}

func runVersion(_ *gfal2.Context, cmd *Command, args []string) int {
	fmt.Printf("%s\n", gfal2.Version())
	return 0
}
