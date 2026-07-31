package cli

import (
	"context"
	"time"

	"github.com/spf13/cobra"

	"github.com/nabrahma/driftwatch/pkg/check"
	"github.com/nabrahma/driftwatch/pkg/differ"
)

// diffFlags are `driftwatch diff`'s own options.
type diffFlags struct {
	warmup time.Duration
	extras bool
}

func newDiffCommand(env *Env, g *globalFlags) *cobra.Command {
	f := &diffFlags{}

	cmd := &cobra.Command{
		Use:   "diff",
		Short: "Run one full comparison and exit",
		Long: trim(`
Bootstraps, waits for the oracle to fill, then runs a complete comparison: one
oracle-to-target sweep, a confirmation cycle a settlement window later, and one
target-to-oracle extras scan. Prints the report and exits.

This is the command for CI pipelines. Exit code 3 means drift was confirmed,
which is distinct from exit code 1, an actual failure — a pipeline needs to
tell "your cache index is wrong" from "driftwatch could not run".`),
		Example: trim(`
  # assert a cache index is consistent, as a test
  driftwatch diff -f examples/local.yaml || exit 1

  # machine-readable, for a report artifact
  driftwatch diff -f examples/local.yaml -o json > drift.json`),
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runDiff(cmd.Context(), env, g, f)
		},
	}

	cmd.Flags().DurationVar(&f.warmup, "warmup", 0,
		"how long to fill the oracle before comparing, zero for 2x the settlement window")
	cmd.Flags().BoolVar(&f.extras, "extras", true,
		"also run the target-to-oracle scan for keys no event created")

	return cmd
}

func runDiff(ctx context.Context, env *Env, g *globalFlags, f *diffFlags) error {
	spec, err := loadSpec(g)
	if err != nil {
		return err
	}
	warn(env, &spec)

	log, flush, err := newLogger(env, g)
	if err != nil {
		return err
	}
	defer flush() //nolint:errcheck // nothing useful to do if the final flush fails

	c, err := check.New(spec, check.Deps{Clock: env.Clock, Logger: log})
	if err != nil {
		return exitWith(ExitConfigInvalid, err)
	}
	defer c.Close() //nolint:errcheck // the comparison result is what matters here

	ctx, stop := withSignals(ctx)
	defer stop()

	defer startCheck(ctx, c, stop)()

	report, err := compareOnce(ctx, env, c, &spec, f)
	if err != nil {
		return err
	}

	if err := emitReport(env, g, report); err != nil {
		return err
	}

	if report.Alertable() > 0 {
		return exitWith(ExitDriftFound, &errDriftFound{count: report.Alertable()})
	}
	return nil
}

// compareOnce runs the whole two-phase cycle plus the extras scan.
//
// The full cycle, not one sweep. A single sweep reports candidates, and a
// candidate is not a finding: §5.4's entire point is that one disagreeing read
// is as likely to be a slow materializer as real drift. A CI gate built on the
// first read would fail builds at random.
func compareOnce(
	ctx context.Context, env *Env, c *check.Check, spec *check.Spec, f *diffFlags,
) (*differ.Report, error) {
	window := spec.EffectiveWindow()

	warmup := f.warmup
	if warmup <= 0 {
		warmup = 2 * window
	}

	select {
	case <-c.Bootstrapped():
	case <-ctx.Done():
		return nil, exitWith(ExitFatal, ctx.Err())
	}

	if err := env.Clock.Sleep(ctx, warmup); err != nil {
		return nil, exitWith(ExitFatal, err)
	}

	if _, err := c.SweepNow(ctx); err != nil {
		return nil, exitWith(ExitFatal, err)
	}

	// Wait out the settlement window, then re-read. This is the second phase,
	// and it is what turns a candidate into something worth reporting.
	if err := env.Clock.Sleep(ctx, window); err != nil {
		return nil, exitWith(ExitFatal, err)
	}
	c.ConfirmDue(ctx)

	report, err := c.SweepNow(ctx)
	if err != nil {
		return nil, exitWith(ExitFatal, err)
	}

	if f.extras {
		if err := addExtras(ctx, env, c, report, window); err != nil {
			// An extras scan that fails does not invalidate the sweep that
			// succeeded. Reporting half a comparison and saying so beats
			// discarding a result that is already useful.
			env.errln("warning: the extras scan failed:", err)
		}
	}
	return report, nil
}

// addExtras runs both passes of §5.5's target-to-oracle scan.
//
// Both, separated by a settlement window, because one pass reports nothing by
// design. A key present in the store and absent from the oracle may simply be a
// key whose event has not arrived yet, and a scan of a live keyspace is not
// atomic — so only the intersection of two passes a window apart is an extra
// rather than a race. Calling the scan once and reporting its output would
// report every in-flight key in the store as drift.
func addExtras(
	ctx context.Context, env *Env, c *check.Check, report *differ.Report, window time.Duration,
) error {
	if _, err := c.ScanExtras(ctx); err != nil {
		return err
	}

	if err := env.Clock.Sleep(ctx, window); err != nil {
		return err
	}

	extras, err := c.ScanExtras(ctx)
	if err != nil {
		return err
	}
	for i := range extras.Findings {
		report.Add(&extras.Findings[i])
	}
	return nil
}

// emitReport writes a report to stdout in the requested format.
func emitReport(env *Env, g *globalFlags, report *differ.Report) error {
	if g.output == OutputJSON {
		doc, err := report.JSON()
		if err != nil {
			return exitWith(ExitFatal, err)
		}
		if err := writeJSON(env, doc); err != nil {
			return exitWith(ExitFatal, err)
		}
		return nil
	}

	env.print(report.Text())
	return nil
}
