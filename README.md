# go-gfal2
This package provides bindings for gfal2 for the Go language. It requires gfal2 to be already installed, and must be localizable via pkg-config.

## Example usage
Have a look at the [examples/gfal2](examples/gfal2) directory to see some examples.

```golang
package main

import (
	"gitlab.cern.ch/dmc/go-gfal2"
)

func main() {
	context, err := gfal2.NewContext()
	if err != nil {
		log.Fatal(err)
	}
	defer context.Close()

	stat, err := gfal2.Stat("gsiftp://host/path")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Mode: %s\n", stat.Mode().String())
}
```