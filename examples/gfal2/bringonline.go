package main

import (
	"fmt"
	"gitlab.cern.ch/dmc/go-gfal2"
	"os"
	"syscall"
	"time"
)

var cmdBringOnline = &Command{
	Name:        "bringonline",
	Description: "request a file to be brought online",
}

var pinLifetime = cmdBringOnline.Flag.Int("pin-lifetime", 600, "Pin lifetime")
var timeout = cmdBringOnline.Flag.Int("timeout", 300, "Timeout")
var poll = cmdBringOnline.Flag.Bool("poll", false, "Keep polling until done")

func init() {
	cmdBringOnline.Run = runBringOnline
}

func runBringOnline(context *gfal2.Context, cmd *Command, args []string) int {
	cmd.Flag.Parse(args)
	if cmd.Flag.NArg() == 0 {
		Log("MAIN", gfal2.LogLevelCritical, "Missing surl")
		return -1
	}

	urls := cmd.Flag.Args()
	token, errors := context.BringOnlineList(urls, *pinLifetime, *timeout, true)
	fmt.Fprint(os.Stdout, "Token: ", token, "\n")

	sleep := time.Second * 2

	for {
		remaining := make([]string, 0, len(urls))
		for i, error := range errors {
			if error == nil {
				fmt.Fprint(os.Stdout, "OK     ", urls[i], "\n")
			} else if error.Code() == syscall.EAGAIN {
				fmt.Fprint(os.Stdout, "QUEUED ", urls[i], "\n")
				remaining = append(remaining, urls[i])
			} else {
				fmt.Fprint(os.Stdout, "FAILED  ", urls[i], "\n")
				fmt.Fprint(os.Stdout, "\t", error, "\n")
			}
		}
		urls = remaining

		if !*poll || len(urls) == 0 {
			break
		}
		Log("MAIN", gfal2.LogLevelMessage, "Next attempt in %s", sleep)
		time.Sleep(sleep)
		errors = context.BringOnlinePollList(urls, token)
		if sleep < 30 * time.Minute {
			sleep *= 2
		}
	}
	return 0
}
