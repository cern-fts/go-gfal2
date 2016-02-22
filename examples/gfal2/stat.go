package main

import (
	"fmt"
	"gitlab.cern.ch/dmc/go-gfal2"
)

var cmdStat = &Command{
	Name:        "stat",
	Description: "stat a file or directory",
}

func init() {
	cmdStat.Run = runStat
}

func runStat(context *gfal2.Context, cmd *Command, args []string) int {
	if len(args) < 1 {
		Log("MAIN", gfal2.LogLevelCritical, "Missing surl")
		return -1
	}

	stat, err := context.Lstat(args[0])
	if err != nil {
		Log("MAIN", gfal2.LogLevelCritical, "Could not stat the file: %s", err.Error())
		return -1
	}
	
 	fmt.Printf("File: ‘%s’\n", stat.Name())
  	fmt.Printf("Size: %d", stat.Size())
  	if stat.IsDir() {
  		fmt.Printf("\tdirectory")
  	}
  	fmt.Printf("\n")
  	fmt.Printf("Access: %s\n", stat.Mode().String())
  	fmt.Printf("Access: %s\n", stat.AccessTime().String())
  	fmt.Printf("Modify: %s\n", stat.ModTime().String())
  	fmt.Printf("Change: %s\n", stat.ChangeTime().String())

	return 0
}
