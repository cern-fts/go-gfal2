package main

import (
	"fmt"
	"gitlab.cern.ch/dmc/go-gfal2"
)

var cmdLs = &Command{
	Name:        "ls",
	Description: "list the content of a directory",
}

func init() {
	cmdLs.Run = runLs
}

var fullList = cmdLs.Flag.Bool("l", false, "long list format")

func printShort(info gfal2.Stat) {
	fmt.Printf("%s\n", info.Name())
}

func printLong(info gfal2.Stat) {
	fmt.Printf("%s %#5d %#6d %#6d %#6d %s %s\n", info.Mode().String(), info.Nlink(), info.Uid(), info.Gid(), info.Size(), info.ModTime().String(), info.Name())
}

func runLs(context *gfal2.Context, cmd *Command, args []string) int {
	if len(args) < 1 {
		Log("MAIN", gfal2.LogLevelCritical, "Missing surl")
		return -1
	}

	dir, err := context.Opendir(args[0])
	if err != nil {
		Log("MAIN", gfal2.LogLevelCritical, "Could not open the directory: %s", err.Error())
		return -1
	}
	defer dir.Close()

	info, err := dir.Readdir()
	for ; info != nil && err == nil; info, err = dir.Readdir() {
		if *fullList {
			printLong(info)
		} else {
			printShort(info)
		}
	}

	if err != nil {
		Log("MAIN", gfal2.LogLevelCritical, "Could not read the directory: %s", err.Error())
	}

	return 0
}
