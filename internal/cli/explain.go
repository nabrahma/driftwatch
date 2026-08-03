package cli

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/nabrahma/driftwatch/pkg/check"
	"github.com/nabrahma/driftwatch/pkg/explain"
)

// explainFlags are `driftwatch explain`'s own options.
type explainFlags struct {
	key     string
	keyHex  string
	wait    time.Duration
	fromURL string
}

func newExplainCommand(env *Env, g *globalFlags) *cobra.Command {
	f := &explainFlags{}

	cmd := &cobra.Command{
		Use:   "explain",
		Short: "Explain what happened to one key",
		Long: trim(`
Runs a check, waits for the key to be observed, and prints everything driftwatch
knows about it: what the oracle expects, what the target holds, the events that
produced the expectation, the publishers' sequence positions, and a diagnosis of
what most likely happened.

This is the command that makes driftwatch a debugging tool rather than an alarm.
A gauge tells an operator that twelve keys diverged; this tells them which event
the materializer did not apply.`),
		Example: trim(`
  # explain one key
  driftwatch explain -f examples/local.yaml --key block:9f3a

  # a binary key
  driftwatch explain -f examples/local.yaml --key-hex 626c6f636b3a00ff

  # machine-readable
  driftwatch explain -f examples/local.yaml --key block:9f3a -o json | jq .diagnosis`),
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runExplain(cmd.Context(), env, g, f)
		},
	}

	cmd.Flags().StringVar(&f.key, "key", "", "the key to explain")
	cmd.Flags().StringVar(&f.keyHex, "key-hex", "",
		"the key to explain, hex-encoded, for keys that are not valid UTF-8")
	cmd.Flags().DurationVar(&f.wait, "wait", 30*time.Second,
		"how long to wait for the key to be observed")
	cmd.Flags().StringVar(&f.fromURL, "from-running", "",
		"query a running watch process instead of starting a check (not yet implemented)")

	return cmd
}

// errKeyRequired names both spellings, because a user who passed neither has
// usually not noticed that --key-hex exists.
var errKeyRequired = errors.New("one of --key or --key-hex is required")

func runExplain(ctx context.Context, env *Env, g *globalFlags, f *explainFlags) error {
	key, err := resolveKey(f)
	if err != nil {
		return exitWith(ExitConfigInvalid, err)
	}

	if f.fromURL != "" {
		// Honest rather than silently ignoring the flag. The endpoint it talks
		// to is the operator's job, and saying so beats
		// starting a second subscriber the user did not ask for.
		return exitWith(ExitConfigInvalid, errors.New(
			"--from-running needs the HTTP endpoint served by the operator, which is "+
				"not in this build; run explain against the same spec instead"))
	}

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
	defer c.Close() //nolint:errcheck // the explanation is what matters here

	ctx, stop := withSignals(ctx)
	defer stop()

	defer startCheck(ctx, c, stop)()

	exp, err := waitForKey(ctx, env, c, key, f.wait)
	if err != nil {
		return err
	}
	return emitExplanation(env, g, exp)
}

// resolveKey decodes whichever spelling of the key was given.
func resolveKey(f *explainFlags) (string, error) {
	switch {
	case f.key != "" && f.keyHex != "":
		return "", errors.New("pass only one of --key and --key-hex")
	case f.keyHex != "":
		raw, err := hex.DecodeString(f.keyHex)
		if err != nil {
			return "", fmt.Errorf("--key-hex %q: %w", f.keyHex, err)
		}
		return string(raw), nil
	case f.key != "":
		return f.key, nil
	default:
		return "", errKeyRequired
	}
}

// waitForKey polls until the key is observed or the deadline passes.
//
// A timeout is not a failure. §23 A5 again: a key driftwatch has never seen is
// a fact worth reporting, and the explanation says so — usually because the
// keyTemplate does not produce the shape the user typed, which is exactly what
// the output helps them work out.
func waitForKey(
	ctx context.Context, env *Env, c *check.Check, key string, wait time.Duration,
) (*explain.Explanation, error) {
	select {
	case <-c.Bootstrapped():
	case <-ctx.Done():
		return nil, exitWith(ExitFatal, ctx.Err())
	}

	deadline := env.Clock.Now().Add(wait)
	for {
		exp, err := c.Explain(ctx, key)
		if err != nil {
			return nil, exitWith(ExitFatal, err)
		}
		// Wait for the key to be observed and then to settle. Explaining an
		// in-flight key is technically correct and rarely what the user wants:
		// it reports that a disagreement inside the settlement window is
		// expected, which they already knew, when a moment's wait would have
		// told them whether it is real.
		ready := exp.Verdict != explain.VerdictUnknownKey && exp.Settled
		if ready || !env.Clock.Now().Before(deadline) {
			return exp, nil
		}

		if err := env.Clock.Sleep(ctx, explainPollInterval); err != nil {
			// The context ended while waiting. Answer with what is known rather
			// than with an error: a partial answer beats none.
			return c.Explain(context.Background(), key) //nolint:contextcheck // deliberate, see above
		}
	}
}

// explainPollInterval is how often the key is re-checked while waiting.
const explainPollInterval = 250 * time.Millisecond

func emitExplanation(env *Env, g *globalFlags, exp *explain.Explanation) error {
	if g.output == OutputJSON {
		doc, err := exp.JSON()
		if err != nil {
			return exitWith(ExitFatal, err)
		}
		return exitWith(ExitFatal, writeJSON(env, doc))
	}

	env.print(exp.Text())
	return nil
}
