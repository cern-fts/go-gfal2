package main

import (
	"flag"
	"fmt"
	"gitlab.cern.ch/dmc/go-gfal2"
	"log"
	"os"
)

// Based on
// https://golang.org/src/cmd/go/main.go
type Command struct {
	Run         func(context *gfal2.Context, cmd *Command, args []string) int
	Name        string
	Flag        flag.FlagSet
	Description string
}

// Command list
var commands = []*Command{
	cmdCat,
	cmdCopy,
	cmdLs,
	cmdRm,
	cmdStat,
	cmdSum,
	cmdVersion,
	cmdBringOnline,
}

// Print a flag
func printFlag(flag *flag.Flag) {
	fmt.Fprintf(os.Stderr, "\t-%s\t%s (Default: %q)\n", flag.Name, flag.Usage, flag.Value)
}

// Print usage
func usage() {
	fmt.Fprintf(os.Stderr, "Usage:\n\t%s command [options]\n\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "General options:\n\n")
	flag.VisitAll(printFlag)
	fmt.Fprintf(os.Stderr, "\nPossible commands are:\n\n")
	for _, cmd := range commands {
		fmt.Fprintf(os.Stderr, "\t%-15s%s\n", cmd.Name, cmd.Description)
	}
	fmt.Fprintf(os.Stderr, "\n")
	os.Exit(1)
}

// Usage for a single command
func (cmd *Command) Usage() {
	fmt.Fprintf(os.Stderr, "Usage:\n\t%s %s [options] [args]\n\n", os.Args[0], cmd.Name)
	fmt.Fprintf(os.Stderr, "Available options:\n\n")
	cmd.Flag.VisitAll(printFlag)
	fmt.Fprintf(os.Stderr, "\n")
	os.Exit(2)
}

// Entry point
func main() {
	// General options
	optLogLevel := flag.Int("log-level", 0, "Set the logging level")
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		usage()
	}

	// Logging
	if *optLogLevel >= 2 {
		gfal2.SetLogLevel(gfal2.LogLevelDebug)
	} else if *optLogLevel >= 1 {
		gfal2.SetLogLevel(gfal2.LogLevelInfo)
	} else {
		gfal2.SetLogLevel(gfal2.LogLevelWarning)
	}

	var logger LogListener
	gfal2.SetLogHandler(logger)

	// Create gfal2 context
	context, err := gfal2.NewContext()
	if err != nil {
		log.Fatal(err)
	}
	defer context.Close()

	// Run command
	for _, cmd := range commands {
		if cmd.Name == args[0] {
			cmd.Flag.Usage = func() { cmd.Usage() }
			cmd.Flag.Parse(args[1:])
			os.Exit(cmd.Run(context, cmd, cmd.Flag.Args()))
		}
	}

	// Unknown!
	fmt.Fprintf(os.Stderr, "Unknown command: %s\n", args[0])
	usage()
}
