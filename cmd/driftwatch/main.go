// Command driftwatch is the driftwatch command-line interface (§11).
//
// This file is deliberately almost empty. §1.1.4 forbids reading the wall clock
// anywhere but main and the clock implementation itself, so main is where the
// real clock is created and injected, and everything below it is testable
// against a fake one.
package main

import (
	"context"
	"os"

	"github.com/nabrahma/driftwatch/internal/buildinfo"
	"github.com/nabrahma/driftwatch/internal/cli"
	"github.com/nabrahma/driftwatch/pkg/clock"
)

func main() {
	env := &cli.Env{
		Stdout:  os.Stdout,
		Stderr:  os.Stderr,
		Args:    os.Args[1:],
		Clock:   clock.Real(),
		Version: buildinfo.Version,
		Commit:  buildinfo.Commit,
		Date:    buildinfo.Date,
	}

	os.Exit(cli.ExecuteContext(context.Background(), env))
}
