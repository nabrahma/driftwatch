package cli

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/spf13/cobra"

	"github.com/nabrahma/driftwatch/pkg/check"
	"github.com/nabrahma/driftwatch/pkg/metrics"
)

// watchFlags are `driftwatch watch`'s own options.
type watchFlags struct {
	metricsAddr    string
	statusInterval time.Duration
	once           bool
	timeout        time.Duration
	failOnDrift    bool
}

func newWatchCommand(env *Env, g *globalFlags) *cobra.Command {
	f := &watchFlags{}

	cmd := &cobra.Command{
		Use:   "watch",
		Short: "Run a check in the foreground and serve /metrics",
		Long: trim(`
Runs a check continuously: ingest the event stream, sweep on the configured
interval, confirm candidates, and serve Prometheus metrics.

This is the long-running mode the operator deploys. A status line is printed
every --status-interval, and the full sweep report at -v 1.`),
		Example: trim(`
  # audit a local Redis against a live event stream
  driftwatch watch -f examples/local.yaml

  # one sweep, then exit non-zero if anything diverged (a CI gate)
  driftwatch watch -f examples/local.yaml --once --fail-on-drift`),
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runWatch(cmd.Context(), env, g, f)
		},
	}

	cmd.Flags().StringVar(&f.metricsAddr, "metrics-addr", ":9090",
		"address to serve /metrics on, empty to disable")
	cmd.Flags().DurationVar(&f.statusInterval, "status-interval", 10*time.Second,
		"how often to print a status line")
	cmd.Flags().BoolVar(&f.once, "once", false, "run a single sweep and exit")
	cmd.Flags().DurationVar(&f.timeout, "timeout", 0,
		"stop after this long, zero to run until interrupted")
	cmd.Flags().BoolVar(&f.failOnDrift, "fail-on-drift", false, "exit 3 if drift is confirmed")

	return cmd
}

func runWatch(ctx context.Context, env *Env, g *globalFlags, f *watchFlags) error {
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

	met := metrics.New(metrics.Options{Registry: processRegistry(), Logger: log})
	met.SetBuildInfo(env.Version, env.Commit, runtime.Version())
	met.SetChecksActive(1)

	c, err := check.New(spec, check.Deps{Clock: env.Clock, Logger: log, Metrics: met})
	if err != nil {
		return exitWith(ExitConfigInvalid, err)
	}
	defer c.Close() //nolint:errcheck // the run error is the one worth returning

	ctx, stop := withSignals(ctx)
	defer stop()

	if f.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, f.timeout)
		defer cancel()
	}

	// serveMetrics deliberately does not take ctx: the listener has to outlive
	// the check's cancellation so a final scrape can still read the counters
	// that explain why it stopped.
	serve, shutdown := serveMetrics(env, log, met, f.metricsAddr) //nolint:contextcheck // see above
	defer shutdown()
	go serve()

	running := make(chan error, 1)
	go func() { running <- c.Run(ctx) }()

	if f.once {
		err = runWatchOnce(ctx, env, g, c, f)

		// stop() cancels the context signal.NotifyContext returned, which is
		// what tells the check to wind down. Waiting on running without it
		// would block forever: --once means one sweep, not one sweep and then
		// keep watching.
		stop()
		<-running
		return err
	}
	return watchUntilDone(ctx, env, c, f, running)
}

// runWatchOnce waits for bootstrap, sweeps once and exits.
//
// The `--once` shape exists so a CI job can gate on the same configuration file
// the deployment runs, rather than a second one that has drifted from it.
func runWatchOnce(
	ctx context.Context, env *Env, g *globalFlags, c *check.Check, f *watchFlags,
) error {
	select {
	case <-c.Bootstrapped():
	case <-ctx.Done():
		return nil
	}

	report, err := c.SweepNow(ctx)
	if err != nil {
		return exitWith(ExitFatal, err)
	}

	if err := emitReport(env, g, report); err != nil {
		return err
	}
	if f.failOnDrift && report.Alertable() > 0 {
		return exitWith(ExitDriftFound, &errDriftFound{count: report.Alertable()})
	}
	return nil
}

// watchUntilDone prints a status line on its interval until the check stops.
func watchUntilDone(
	ctx context.Context, env *Env, c *check.Check, f *watchFlags, running <-chan error,
) error {
	ticker := env.Clock.NewTicker(f.statusInterval)
	defer ticker.Stop()

	for {
		select {
		case err := <-running:
			return finishWatch(env, c, f, err)

		case <-ticker.C():
			status := c.Status()
			env.println(status.Summary())

		case <-ctx.Done():
			return finishWatch(env, c, f, <-running)
		}
	}
}

func finishWatch(env *Env, c *check.Check, f *watchFlags, err error) error {
	if err != nil && !errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded) {
		return exitWith(ExitFatal, err)
	}

	status := c.Status()
	env.println(status.Summary())

	if f.failOnDrift && status.DivergentKeys > 0 {
		return exitWith(ExitDriftFound, &errDriftFound{count: status.DivergentKeys})
	}
	return nil
}

// withSignals cancels ctx on SIGINT or SIGTERM.
//
// §11 requires graceful shutdown on both, and it matters more here than in most
// tools: a check killed mid-sweep leaves confirmed findings unreported, so the
// operator sees the drift vanish from the dashboard rather than the process
// vanish from the cluster.
func withSignals(ctx context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
}

// serveMetrics starts the /metrics listener, returning a runner and a shutdown.
func serveMetrics(
	env *Env, log logr.Logger, met *metrics.Metrics, addr string,
) (run, shutdown func()) {
	if addr == "" {
		return func() {}, func() {}
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", met.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, "ok") //nolint:errcheck // a health probe that hung up needs nothing
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	run = func() {
		log.Info("serving metrics", "addr", addr, "path", "/metrics")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			// Not fatal. A metrics port already in use must not stop the check
			// doing its job: the operator loses the dashboard, not the audit.
			log.Error(err, "the metrics listener stopped; the check keeps running", "addr", addr)
			env.errf("warning: could not serve metrics on %s: %v\n", addr, err)
		}
	}

	// A fresh context, deliberately: the caller's is already canceled by the
	// time shutdown runs, and one that inherited it would abort immediately
	// instead of draining the scrape in flight.
	shutdown = func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second) //nolint:contextcheck // see above
		defer cancel()
		_ = srv.Shutdown(ctx) //nolint:errcheck // shutting down on the way out
	}
	return run, shutdown
}

// processRegistry returns a registry carrying the Go and process collectors
// alongside driftwatch's own.
//
// A fresh registry rather than prometheus.DefaultRegisterer, deliberately: a
// library somewhere in the dependency tree that registers on the default
// registry would otherwise appear in driftwatch's output, and every metric
// exported here should be one this repository declared.
func processRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return reg
}

// trim removes the leading and trailing blank lines from a raw-string help
// block, so the help text can be written readably in the source.
func trim(s string) string { return strings.TrimSpace(s) }
