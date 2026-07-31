//go:build dev

package cli

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/spf13/cobra"

	"github.com/nabrahma/driftwatch/pkg/check"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/source"
	"github.com/nabrahma/driftwatch/test/harness/faultinjector"
	"github.com/nabrahma/driftwatch/test/harness/publisher"
)

// scenario is one named fault from the §15.3 matrix.
//
// The names match the matrix rows exactly, so a row that fails in the automated
// suite can be reproduced by hand against a live dashboard with the same word.
type scenario struct {
	name  string
	row   string
	about string
	build func(seed int64) []faultinjector.Fault
}

var scenarios = []scenario{
	{
		name:  "drop-burst",
		row:   "§15.1",
		about: "drop 500 consecutive events, the shape a subscriber reconnect leaves",
		build: func(int64) []faultinjector.Fault {
			return []faultinjector.Fault{faultinjector.DropBurst(1000, 500)}
		},
	},
	{
		name:  "drop-random",
		row:   "§15.1",
		about: "drop 1% of events at random, the shape a lossy transport leaves",
		build: func(seed int64) []faultinjector.Fault {
			return []faultinjector.Fault{faultinjector.Drop(0.01, seed)}
		},
	},
	{
		name:  "reorder",
		row:   "§15.1",
		about: "reorder within a window of 50, which a non-commutative projection notices",
		build: func(seed int64) []faultinjector.Fault {
			return []faultinjector.Fault{faultinjector.Reorder(50, seed)}
		},
	},
	{
		name:  "duplicate",
		row:   "§15.1",
		about: "duplicate 5% of events, which sequence tracking must absorb silently",
		build: func(seed int64) []faultinjector.Fault {
			return []faultinjector.Fault{faultinjector.Duplicate(0.05, time.Second, seed)}
		},
	},
	{
		name:  "delay",
		row:   "§15.1",
		about: "hold every event for a uniform second, which the settlement window absorbs",
		build: func(int64) []faultinjector.Fault {
			return []faultinjector.Fault{faultinjector.Delay(time.Second, time.Second, 1)}
		},
	},
	{
		name:  "partition",
		row:   "§15.1",
		about: "a 30-second silence, the shape a network partition leaves",
		build: func(int64) []faultinjector.Fault {
			return []faultinjector.Fault{faultinjector.Partition(10*time.Second, 30*time.Second)}
		},
	},
	{
		name:  "publisher-restart",
		row:   "§15.1",
		about: "reset the sequence mid-stream without an epoch bump",
		build: func(int64) []faultinjector.Fault {
			return []faultinjector.Fault{faultinjector.SeqReset(2000)}
		},
	},
	{
		name:  "epoch-bump",
		row:   "§15.1",
		about: "a declared restart, which must not be mistaken for loss",
		build: func(int64) []faultinjector.Fault {
			return []faultinjector.Fault{faultinjector.EpochBump(2000)}
		},
	},
	{
		name:  "clock-skew",
		row:   "§15.1",
		about: "a publisher four minutes ahead, which settlement must ignore",
		build: func(int64) []faultinjector.Fault {
			return []faultinjector.Fault{faultinjector.ClockSkew("pub-0", 4*time.Minute)}
		},
	},
	{
		name:  "malformed",
		row:   "§15.1",
		about: "corrupt 2% of payloads, which the codec must reject without stalling",
		build: func(seed int64) []faultinjector.Fault {
			return []faultinjector.Fault{faultinjector.Corrupt(0.02, seed)}
		},
	},
}

// injectFlags are `driftwatch inject`'s own options.
type injectFlags struct {
	scenario string
	list     bool
	duration time.Duration
	rate     int
	seed     int64
	keys     int
}

// addDevCommands registers the test-only helper.
//
// Built only with `-tags dev`, because it publishes into the event stream. A
// release binary that could write to the system it audits is the thing NG1
// says nobody will deploy, and the tag is what keeps that true of the artifact.
func addDevCommands(root *cobra.Command, env *Env, g *globalFlags) {
	root.AddCommand(newInjectCommand(env, g))
}

func newInjectCommand(env *Env, g *globalFlags) *cobra.Command {
	f := &injectFlags{}

	cmd := &cobra.Command{
		Use:   "inject",
		Short: "Publish a synthetic event stream through the fault injector (dev builds only)",
		Long: trim(`
Generates a deterministic synthetic event stream, perturbs it with a named
fault, and feeds it to a running check so a human can watch drift appear on a
dashboard and then watch it resolve.

Scenario names match the fault matrix rows in §15.3, so a row that fails in the
automated suite can be reproduced by hand with the same word.

This command exists in dev builds only. It writes to the event stream, and a
release binary that could do that is a detector nobody will deploy.`),
		Example: trim(`
  driftwatch inject --list-scenarios
  driftwatch inject -f examples/local.yaml --scenario drop-burst --duration 2m`),
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runInject(cmd.Context(), env, g, f)
		},
	}

	cmd.Flags().StringVar(&f.scenario, "scenario", "", "which fault to inject")
	cmd.Flags().BoolVar(&f.list, "list-scenarios", false, "list the available scenarios and exit")
	cmd.Flags().DurationVar(&f.duration, "duration", time.Minute, "how long to publish for")
	cmd.Flags().IntVar(&f.rate, "rate", 100, "events per second")
	cmd.Flags().Int64Var(&f.seed, "seed", 1, "makes the stream and the faults reproducible")
	cmd.Flags().IntVar(&f.keys, "keys", 500, "size of the synthetic key space")

	return cmd
}

func runInject(ctx context.Context, env *Env, g *globalFlags, f *injectFlags) error {
	if f.list {
		listScenarios(env)
		return nil
	}

	sc, ok := findScenario(f.scenario)
	if !ok {
		return exitWith(ExitConfigInvalid, fmt.Errorf(
			"unknown scenario %q; run --list-scenarios", f.scenario))
	}

	spec, err := loadSpec(g)
	if err != nil {
		return err
	}
	if spec.Source.Type != "memory" {
		return exitWith(ExitConfigInvalid, errors.New(
			"inject needs source.type=memory: it publishes into the check's own "+
				"source rather than into a real transport"))
	}

	log, flush, err := newLogger(env, g)
	if err != nil {
		return err
	}
	defer flush() //nolint:errcheck // nothing useful to do if the final flush fails

	c, err := check.New(spec, check.Deps{Clock: env.Clock, Logger: log})
	if err != nil {
		return exitWith(ExitConfigInvalid, err)
	}
	defer c.Close() //nolint:errcheck // the injected run is what matters here

	ctx, stop := withSignals(ctx)
	defer stop()

	running := make(chan error, 1)
	go func() { running <- c.Run(ctx) }()

	env.printf("injecting %s (%s): %s\n", sc.name, sc.row, sc.about)
	if err := publishThrough(ctx, env, c, &sc, f); err != nil {
		return exitWith(ExitFatal, err)
	}

	status := c.Status()
	env.println(status.Summary())
	stop()
	<-running
	return nil
}

// publishThrough generates the stream, perturbs it and hands it to the check.
func publishThrough(
	ctx context.Context, env *Env, c *check.Check, sc *scenario, f *injectFlags,
) error {
	dest, ok := c.Source().(*source.MemorySource)
	if !ok {
		return errors.New("inject needs a memory source")
	}

	total := int(f.duration.Seconds()) * f.rate
	if total <= 0 {
		total = f.rate
	}

	pub := publisher.New(publisher.Config{
		Publishers: 3,
		Keys:       f.keys,
		Shape:      projection.ShapeSet,
		Seed:       f.seed,
		Clock:      env.Clock,
	})

	// Perturb through a real Injector rather than by calling the faults
	// directly, so a timed fault gets the same treatment it would in a live
	// pipeline and what a human watches is what the test suite exercises.
	staging := source.NewMemory(env.Clock, source.WithCapacity(total+64))
	for _, msg := range pub.Emit(total) {
		if !staging.Publish(msg) {
			break
		}
	}

	inj := faultinjector.Wrap(staging, env.Clock, sc.build(f.seed)...)
	out := make(chan source.RawMessage, 4096)

	injected := make(chan error, 1)
	go func() { injected <- inj.Run(ctx, out) }()

	interval := time.Second / time.Duration(f.rate)
	sent := 0

	for {
		select {
		case <-ctx.Done():
			_ = staging.Close() //nolint:errcheck // shutting down
			<-injected
			return nil

		case msg := <-out:
			if !dest.Publish(msg) {
				env.errln("warning: the check's ingest buffer is full")
			}
			sent++
			if sent >= total {
				_ = staging.Close() //nolint:errcheck // the stream is exhausted
				<-injected
				env.printf("published %d events\n", sent)
				return nil
			}
			// The context ended mid-stream. That is the operator pressing
			// ctrl-c, not a failure: the events already published stay
			// published and the check reports on them.
			if err := env.Clock.Sleep(ctx, interval); err != nil {
				return nil //nolint:nilerr // see above
			}
		}
	}
}

func findScenario(name string) (scenario, bool) {
	for _, sc := range scenarios {
		if sc.name == name {
			return sc, true
		}
	}
	return scenario{}, false
}

func listScenarios(env *Env) {
	names := make([]string, 0, len(scenarios))
	byName := map[string]scenario{}
	for _, sc := range scenarios {
		names = append(names, sc.name)
		byName[sc.name] = sc
	}
	sort.Strings(names)

	env.println("scenarios (names match the fault matrix rows in §15.3)")
	env.println()
	for _, name := range names {
		sc := byName[name]
		env.printf("  %-20s %-8s %s\n", sc.name, sc.row, sc.about)
	}
}
