// Command driftwatch is the driftwatch command-line interface (§11).
package main

import (
	"fmt"
	"os"

	"github.com/nabrahma/driftwatch/internal/buildinfo"
)

func main() {
	if _, err := fmt.Fprintln(os.Stdout, buildinfo.String()); err != nil {
		os.Exit(1)
	}
}
