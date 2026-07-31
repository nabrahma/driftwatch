//go:build !dev

package cli

import "github.com/spf13/cobra"

// addDevCommands is a no-op in a release build.
//
// `driftwatch inject` publishes synthetic events through the fault injector,
// which means the shipped binary would otherwise carry a way to write to the
// event stream it audits. NG1 says a detector that can also mutate is a
// detector nobody deploys, and the build tag is how that stays true of the
// artifact rather than only of the code path.
//
// Build it with `-tags dev` when a human wants to watch drift appear on a
// dashboard.
func addDevCommands(*cobra.Command, *Env, *globalFlags) {}
