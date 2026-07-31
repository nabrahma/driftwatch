// Package cli implements the driftwatch command-line subcommands (§11).
//
// Every command works without Kubernetes: the CLI reads a YAML file with the
// same schema as `DriftCheck.spec`, so the file that runs here is the file that
// goes into the cluster unchanged. That is not only convenience — it is what
// makes the tool testable end to end without a cluster, and what makes the demo
// something a reader can run.
//
// Two rules shape the output, both from §11 and both easy to get wrong:
//
// Errors go to stderr and data goes to stdout, so every command is pipeable. A
// log line in the middle of a JSON document is the difference between
// `driftwatch diff -o json | jq` working and not.
//
// Nothing here reads the wall clock directly. Env carries a clock, which is
// what lets the golden-file tests produce byte-identical output on every
// machine and in every timezone.
package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/go-logr/logr"
	"github.com/spf13/cobra"

	"github.com/nabrahma/driftwatch/pkg/check"
	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/logging"
)

// Exit codes from §11. They are a contract: CI pipelines branch on them, so
// changing one is a breaking change.
const (
	// ExitOK is a clean run.
	ExitOK = 0
	// ExitFatal is an unexpected failure.
	ExitFatal = 1
	// ExitConfigInvalid is a spec that did not validate.
	ExitConfigInvalid = 2
	// ExitDriftFound is confirmed drift. It is separate from ExitFatal because
	// finding drift means driftwatch worked, and a pipeline has to be able to
	// tell that from driftwatch falling over.
	ExitDriftFound = 3
)

// The output formats.
const (
	OutputText = "text"
	OutputJSON = "json"
)

// Env is everything the CLI needs from the outside world.
//
// It exists so tests can supply a fake clock and buffers instead of the real
// clock and the real terminal. Without it the golden-file tests §11 requires
// could not be written.
type Env struct {
	Stdout io.Writer
	Stderr io.Writer
	Args   []string
	// Clock is the injected clock. Defaults to the real one.
	Clock clock.Clock
	// Version, Commit and Date come from internal/buildinfo via ldflags.
	Version string
	Commit  string
	Date    string
}

func (e *Env) applyDefaults() {
	if e.Stdout == nil {
		e.Stdout = os.Stdout
	}
	if e.Stderr == nil {
		e.Stderr = os.Stderr
	}
	if e.Clock == nil {
		e.Clock = clock.Real()
	}
}

// globalFlags are the options every command shares.
type globalFlags struct {
	file       string
	output     string
	logLevel   string
	logFormat  string
	verbosity  int
	redactKeys bool
}

// exitCoder carries an exit code out of a command.
//
// cobra has no notion of one, and mapping error text onto a code at the top
// level would be guesswork. Wrapping is explicit and testable.
type exitCoder struct {
	code int
	err  error
}

func (e *exitCoder) Error() string { return e.err.Error() }
func (e *exitCoder) Unwrap() error { return e.err }

func exitWith(code int, err error) error {
	if err == nil {
		return nil
	}
	return &exitCoder{code: code, err: err}
}

// errDriftFound is returned when a command confirmed divergence. It is an
// outcome rather than a failure, which is why it has its own exit code.
type errDriftFound struct{ count int }

func (e *errDriftFound) Error() string {
	return fmt.Sprintf("%d divergent keys confirmed", e.count)
}

// Root builds the command tree.
func Root(env *Env) *cobra.Command {
	env.applyDefaults()

	g := &globalFlags{}

	root := &cobra.Command{
		Use:   "driftwatch",
		Short: "Detect silent divergence between an event stream and a target store",
		Long: strings.TrimSpace(`
driftwatch derives what a store should contain from the event stream that feeds
it, then compares that expectation against what the store actually holds. It
never writes to the store it audits.

It is built to distinguish three things existing monitoring cannot: the store is
wrong, the store is merely behind, and driftwatch itself missed events. Only the
first is worth waking someone for.`),
		SilenceUsage:  true,
		SilenceErrors: true,

		// Validate the shared flags once, here, rather than in each command.
		// A command that forgot to check --log-level would accept a typo and
		// run at the default level, which is the kind of quiet wrongness this
		// tool exists to complain about in other systems.
		PersistentPreRunE: func(*cobra.Command, []string) error {
			if err := validateOutput(g); err != nil {
				return err
			}
			return validateLogging(g)
		},
	}

	root.SetOut(env.Stdout)
	root.SetErr(env.Stderr)
	if env.Args != nil {
		root.SetArgs(env.Args)
	}

	pf := root.PersistentFlags()
	pf.StringVarP(&g.file, "file", "f", "", `path to a check specification ("-" for stdin)`)
	pf.StringVarP(&g.output, "output", "o", OutputText, "output format: text or json")
	pf.StringVar(&g.logLevel, "log-level", logging.LevelInfo, "log level: error, warn, info or debug")
	pf.StringVar(&g.logFormat, "log-format", logging.FormatConsole, "log format: console or json")
	pf.IntVarP(&g.verbosity, "verbosity", "v", 0, "log verbosity: 1 for debug, 2 for trace")
	pf.BoolVar(&g.redactKeys, "redact-keys", false, "hash key names in logs and output")

	root.AddCommand(
		newWatchCommand(env, g),
		newDiffCommand(env, g),
		newExplainCommand(env, g),
		newReplayCommand(env, g),
		newVersionCommand(env, g),
	)
	addDevCommands(root, env, g)

	return root
}

// Execute runs the CLI and returns the process exit code.
func Execute(env *Env) int { return ExecuteContext(context.Background(), env) }

// ExecuteContext runs the CLI under ctx and returns the process exit code.
func ExecuteContext(ctx context.Context, env *Env) int {
	env.applyDefaults()

	err := Root(env).ExecuteContext(ctx) //nolint:contextcheck // cobra threads ctx into every RunE
	if err == nil {
		return ExitOK
	}

	// Errors to stderr, always. A command whose data already went to stdout has
	// to stay pipeable even when it ends up failing.
	code := ExitFatal
	var coded *exitCoder
	if errors.As(err, &coded) {
		code = coded.code
	}

	env.errln("driftwatch:", err.Error())
	return code
}

// loadSpec reads and validates the spec named by --file.
//
// Every command starts here, and every one maps a failure onto exit code 2. A
// configuration error and a runtime failure are different things to a CI
// pipeline: the first means fix the file, the second means look at the system.
func loadSpec(g *globalFlags) (check.Spec, error) {
	if g.file == "" {
		return check.Spec{}, exitWith(ExitConfigInvalid, errNoSpec)
	}

	spec, err := check.LoadFile(g.file)
	if err != nil {
		return spec, exitWith(ExitConfigInvalid, err)
	}
	if err := spec.Validate(); err != nil {
		return spec, exitWith(ExitConfigInvalid, err)
	}
	return spec, nil
}

var errNoSpec = errors.New(
	"--file is required: pass a check specification, for example -f examples/local.yaml")

// newLogger builds the logger from the global flags.
//
// It always writes to stderr, whatever --output says. That is the whole
// mechanism behind §11's requirement that `--output json` emits one well-formed
// document even at `--log-level=debug`: the two streams never meet.
func newLogger(env *Env, g *globalFlags) (logr.Logger, func() error, error) {
	log, flush, err := logging.New(logging.Options{
		Level:      g.logLevel,
		V:          g.verbosity,
		Format:     g.logFormat,
		Out:        env.Stderr,
		RedactKeys: g.redactKeys,
	})
	if err != nil {
		return log, flush, exitWith(ExitConfigInvalid, err)
	}
	return log, flush, nil
}

// validateLogging rejects unusable logging flags before any work is done.
func validateLogging(g *globalFlags) error {
	_, flush, err := logging.New(logging.Options{
		Level:  g.logLevel,
		V:      g.verbosity,
		Format: g.logFormat,
		Out:    io.Discard,
	})
	if err != nil {
		return exitWith(ExitConfigInvalid, err)
	}
	return flush()
}

// validateOutput rejects an unknown --output before any work is done.
func validateOutput(g *globalFlags) error {
	switch g.output {
	case OutputText, OutputJSON:
		return nil
	default:
		return exitWith(ExitConfigInvalid,
			fmt.Errorf("--output %q: must be %s or %s", g.output, OutputText, OutputJSON))
	}
}

// warn writes configuration warnings to stderr.
//
// stderr rather than the log, because they have to appear even when the log
// level hides info lines: a warning about a non-commutative counter is
// something the operator must see once, not something to find in a log search
// after the report was already believed.
func warn(env *Env, spec *check.Spec) {
	for _, w := range spec.Warnings() {
		env.errln("warning:", w)
	}
}

// printf writes to stdout, the stream carrying data.
//
// The error is deliberately dropped, in one place rather than at every call
// site. A failed write to stdout is almost always EPIPE from a reader that has
// gone away — `driftwatch diff | head` — and there is nothing useful to do
// about it: reporting the failure requires writing somewhere, and the process
// is already on its way out. A genuine disk-full on a redirected stdout shows
// up as a truncated file, which is visible.
func (e *Env) printf(format string, args ...any) {
	_, _ = fmt.Fprintf(e.Stdout, format, args...) //nolint:errcheck // see above
}

// println writes one line to stdout.
func (e *Env) println(args ...any) {
	_, _ = fmt.Fprintln(e.Stdout, args...) //nolint:errcheck // see printf
}

// print writes to stdout without a trailing newline.
func (e *Env) print(s string) {
	_, _ = fmt.Fprint(e.Stdout, s) //nolint:errcheck // see printf
}

// errln writes one line to stderr, the stream carrying diagnostics.
func (e *Env) errln(args ...any) {
	_, _ = fmt.Fprintln(e.Stderr, args...) //nolint:errcheck // see printf
}

// errf writes to stderr.
func (e *Env) errf(format string, args ...any) {
	_, _ = fmt.Fprintf(e.Stderr, format, args...) //nolint:errcheck // see printf
}

// startCheck runs a check in the background and returns a function that stops
// it and blocks until every one of its goroutines has exited.
//
// The waiting is not tidiness. A command that returns while its check is still
// running leaves goroutines writing to stderr after the process believes it has
// finished: a truncated log line in production, and a data race under -race,
// which is how this was found.
func startCheck(ctx context.Context, c *check.Check, stop context.CancelFunc) func() {
	running := make(chan error, 1)
	go func() { running <- c.Run(ctx) }()

	return func() {
		stop()
		<-running
	}
}

// writeJSON emits one document to stdout, followed by a newline.
func writeJSON(env *Env, doc []byte) error {
	if _, err := env.Stdout.Write(doc); err != nil {
		return err
	}
	env.println()
	return nil
}
