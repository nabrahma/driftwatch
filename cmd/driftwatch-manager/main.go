// Command driftwatch-manager is the Kubernetes operator entrypoint (§10.3).
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
