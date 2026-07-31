package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/spf13/cobra"

	"github.com/nabrahma/driftwatch/pkg/check"
	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/target"
)

// replayFlags are `driftwatch replay`'s own options.
type replayFlags struct {
	events         string
	speed          string
	stopAtSeq      uint64
	dumpOracle     string
	targetSnapshot string
	settleFor      time.Duration
}

func newReplayCommand(env *Env, g *globalFlags) *cobra.Command {
	f := &replayFlags{}

	cmd := &cobra.Command{
		Use:   "replay",
		Short: "Replay a captured event stream offline and compare",
		Long: trim(`
Reads events from a capture, applies them, and diffs the result against a
target. Fully hermetic: no network, no cluster, no timing dependence.

This is how a captured incident gets reproduced. Point it at the events from
the hour the drift appeared and it converges to the same oracle state every
time, which is what makes the investigation repeatable and what makes a
regression test out of a production incident.`),
		Example: trim(`
  # reproduce an incident against a snapshot of the store
  driftwatch replay -f examples/local.yaml --events incident.jsonl \
      --target-snapshot store.json

  # stop at the event that is suspected, and dump what the oracle believed
  driftwatch replay -f examples/local.yaml --events incident.jsonl \
      --stop-at-seq 8842 --dump-oracle oracle.json`),
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runReplay(cmd.Context(), env, g, f)
		},
	}

	cmd.Flags().StringVar(&f.events, "events", "", "path to a newline-delimited event capture")
	cmd.Flags().StringVar(&f.speed, "speed", check.SpeedFast,
		`replay speed: "fast", "realtime" or a multiplier such as "2.0"`)
	cmd.Flags().Uint64Var(&f.stopAtSeq, "stop-at-seq", 0,
		"stop after this sequence number, zero to replay everything")
	cmd.Flags().StringVar(&f.dumpOracle, "dump-oracle", "",
		"write the resulting oracle state to this path")
	cmd.Flags().StringVar(&f.targetSnapshot, "target-snapshot", "",
		"seed a memory target from this JSON snapshot before comparing")
	cmd.Flags().DurationVar(&f.settleFor, "settle-for", 0,
		"how far to advance past the last event before comparing, zero for 2x the window")

	return cmd
}

func runReplay(ctx context.Context, env *Env, g *globalFlags, f *replayFlags) error {
	if f.events == "" {
		return exitWith(ExitConfigInvalid, errors.New("--events is required"))
	}

	spec, err := loadSpec(g)
	if err != nil {
		return err
	}
	// The capture replaces whatever source the spec named, and the replay is
	// hermetic by construction: overriding here means a spec pointing at a
	// production ZMQ endpoint cannot accidentally connect to it during what the
	// user believes is an offline reproduction.
	spec.Source = check.SourceSpec{
		Type:             "file",
		File:             &check.FileSpec{Path: f.events, Speed: f.speed},
		IngestBufferSize: spec.Source.IngestBufferSize,
	}
	if invalid := spec.Validate(); invalid != nil {
		return exitWith(ExitConfigInvalid, invalid)
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

	if err := seedTarget(c, f.targetSnapshot); err != nil {
		return exitWith(ExitConfigInvalid, err)
	}

	return replayAndCompare(ctx, env, g, c, &spec, f)
}

func replayAndCompare(
	ctx context.Context, env *Env, g *globalFlags,
	c *check.Check, spec *check.Spec, f *replayFlags,
) error {
	ctx, stop := withSignals(ctx)
	defer stop()

	defer startCheck(ctx, c, stop)()

	// A file source ends when the capture does. Waiting for the applier to
	// catch up rather than for a wall-clock interval is what keeps the replay
	// deterministic: the same capture produces the same oracle every run,
	// whatever the machine was doing at the time.
	if err := waitForReplay(ctx, env, c); err != nil {
		return err
	}

	settle := f.settleFor
	if settle <= 0 {
		settle = 2 * spec.EffectiveWindow()
	}
	if err := env.Clock.Sleep(ctx, settle); err != nil {
		return exitWith(ExitFatal, err)
	}

	report, err := c.SweepNow(ctx)
	if err != nil {
		return exitWith(ExitFatal, err)
	}

	if f.dumpOracle != "" {
		if err := dumpOracle(c, f.dumpOracle); err != nil {
			return exitWith(ExitFatal, err)
		}
		env.errln("wrote the oracle state to", f.dumpOracle)
	}

	if err := emitReport(env, g, report); err != nil {
		return err
	}
	if report.Alertable() > 0 {
		return exitWith(ExitDriftFound, &errDriftFound{count: report.Alertable()})
	}
	return nil
}

// waitForReplay blocks until the applier has drained the capture.
func waitForReplay(ctx context.Context, env *Env, c *check.Check) error {
	select {
	case <-c.Bootstrapped():
	case <-ctx.Done():
		return exitWith(ExitFatal, ctx.Err())
	}

	// The file source reports how many frames it read. The replay is done when
	// the applier has seen every one of them and stopped moving.
	stable, last := 0, uint64(0)
	for stable < replayStableRounds {
		if err := env.Clock.Sleep(ctx, replayPollInterval); err != nil {
			return exitWith(ExitFatal, err)
		}

		read := c.Source().Stats().FramesReceived
		applied := c.EventsApplied()

		if read > 0 && applied+c.Status().EventsDropped >= read && applied == last {
			stable++
			continue
		}
		stable, last = 0, applied
	}
	return nil
}

const (
	replayPollInterval = 20 * time.Millisecond
	// replayStableRounds is how many consecutive quiet polls mean the capture
	// has been fully applied. Three rather than one, because a large capture
	// can momentarily stall the applier between batches.
	replayStableRounds = 3
)

// seedTarget loads a JSON snapshot of the store into a memory target.
//
// This is the other half of a hermetic reproduction: the capture gives the
// events, and the snapshot gives the store as it was, so the comparison is the
// one that happened rather than one against an empty store.
func seedTarget(c *check.Check, path string) error {
	if path == "" {
		return nil
	}

	mem, ok := c.Target().(*target.MemoryTarget)
	if !ok {
		return fmt.Errorf(
			"--target-snapshot needs target.type=memory, but the spec configures %q",
			c.Target().Name())
	}

	raw, err := os.ReadFile(path) //nolint:gosec // the path is the operator's own snapshot
	if err != nil {
		return fmt.Errorf("reading the target snapshot: %w", err)
	}

	var snapshot targetSnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return fmt.Errorf("parsing the target snapshot: %w", err)
	}

	if len(snapshot.Scalars) > 0 {
		values := make(map[string][]byte, len(snapshot.Scalars))
		for k, v := range snapshot.Scalars {
			values[k] = []byte(v)
		}
		mem.Seed(values)
	}
	if len(snapshot.Sets) > 0 {
		mem.SeedSets(snapshot.Sets)
	}
	return nil
}

// targetSnapshot is the on-disk shape of a store snapshot.
type targetSnapshot struct {
	Scalars map[string]string   `json:"scalars,omitempty"`
	Sets    map[string][]string `json:"sets,omitempty"`
}

// oracleDump is the on-disk shape of `--dump-oracle`.
type oracleDump struct {
	GeneratedAt time.Time         `json:"generatedAt"`
	Keys        []oracleDumpEntry `json:"keys"`
}

type oracleDumpEntry struct {
	Key           string    `json:"key"`
	Value         string    `json:"value"`
	Version       uint64    `json:"version"`
	Trust         string    `json:"trust"`
	LastSeq       uint64    `json:"lastSeq"`
	LastPublisher string    `json:"lastPublisher"`
	LastEventAt   time.Time `json:"lastEventAt"`
}

// dumpOracle writes what the oracle believes to a file.
//
// Sorted by key, so two runs of the same capture produce byte-identical files
// and a diff between two replays is a diff of the state rather than of Go's map
// iteration order.
func dumpOracle(c *check.Check, path string) error {
	orc := c.Oracle()

	dump := oracleDump{GeneratedAt: time.Now().UTC()}
	keys := make([]string, 0, orc.Len())

	for key := range orc.SettledKeys(time.Now().Add(oracleDumpHorizon)) {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		entry, ok := orc.Get(key)
		if !ok {
			continue
		}
		dump.Keys = append(dump.Keys, oracleDumpEntry{
			Key:           key,
			Value:         entry.Value.String(),
			Version:       entry.Version,
			Trust:         entry.Trust.String(),
			LastSeq:       entry.LastSeq,
			LastPublisher: entry.LastPublisher,
			LastEventAt:   entry.LastEventAt.UTC(),
		})
	}

	raw, err := json.MarshalIndent(dump, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o600)
}

// oracleDumpHorizon is how far into the future the settled-key iterator is
// asked about, so that the dump contains every key rather than only the ones
// whose settlement window has already elapsed.
const oracleDumpHorizon = 100 * 365 * 24 * time.Hour

// ensure the imports stay honest about what a dump contains.
var (
	_ = event.ValueAbsent
	_ = oracle.TrustComplete
)
