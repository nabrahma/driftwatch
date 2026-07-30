// Package buildinfo exposes the version, commit and build date injected via ldflags (§8.5).
package buildinfo

import (
	"fmt"
	"runtime"
)

// Version is the semantic version of this build, injected via -ldflags.
// It is "dev" for builds that did not go through the release pipeline.
var Version = "dev"

// Commit is the git commit this build was produced from, injected via -ldflags.
var Commit = "unknown"

// Date is the RFC3339 build timestamp, injected via -ldflags.
var Date = "unknown"

// String renders the build information as a single human-readable line.
func String() string {
	return fmt.Sprintf("driftwatch %s (commit %s, built %s, %s %s/%s)",
		Version, Commit, Date, runtime.Version(), runtime.GOOS, runtime.GOARCH)
}
