// Command driftwatch-manager is the Kubernetes operator entrypoint (§10.3).
//
// It holds the real clock, the process-wide metric registry and the manager,
// and nothing else. §1.1.4 forbids reading the wall clock anywhere but main and
// the clock implementation, so this is where the real one is created and
// injected; everything below it is testable against a fake.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	"github.com/nabrahma/driftwatch/api/v1alpha1"
	"github.com/nabrahma/driftwatch/internal/buildinfo"
	"github.com/nabrahma/driftwatch/internal/controller"
	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/logging"
	"github.com/nabrahma/driftwatch/pkg/metrics"
)

// options are the manager's flags.
type options struct {
	metricsAddr    string
	probeAddr      string
	webhookPort    int
	certDir        string
	enableWebhooks bool
	leaderElect    bool
	leaseNamespace string
	namespace      string

	logLevel  string
	logFormat string
	verbosity int

	statusRefresh   time.Duration
	restartInterval time.Duration
	shutdownGrace   time.Duration

	maxPublisherLabels int

	showVersion bool
}

func (o *options) bind(fs *flag.FlagSet) {
	fs.StringVar(&o.metricsAddr, "metrics-bind-address", ":8080",
		"address the metric endpoint binds to")
	fs.StringVar(&o.probeAddr, "health-probe-bind-address", ":8081",
		"address the health and readiness probes bind to")
	fs.IntVar(&o.webhookPort, "webhook-port", 9443, "port the admission webhook serves on")
	fs.StringVar(&o.certDir, "webhook-cert-dir", "/tmp/k8s-webhook-server/serving-certs",
		"directory holding tls.crt and tls.key for the webhook")
	fs.BoolVar(&o.enableWebhooks, "enable-webhooks", true,
		"serve the defaulting and validating webhooks")

	fs.BoolVar(&o.leaderElect, "leader-elect", true,
		"elect a leader, so exactly one manager runs the checks")
	fs.StringVar(&o.leaseNamespace, "leader-election-namespace", "",
		"namespace holding the leader-election lease; defaults to the pod's own")
	fs.StringVar(&o.namespace, "namespace", "",
		"restrict the manager to one namespace; empty watches all of them")

	fs.StringVar(&o.logLevel, "log-level", logging.LevelInfo, "error, warn, info or debug")
	fs.StringVar(&o.logFormat, "log-format", logging.FormatJSON, "console or json")
	fs.IntVar(&o.verbosity, "v", 0, "verbosity; higher is more")

	fs.DurationVar(&o.statusRefresh, "status-refresh-interval",
		controller.DefaultStatusRefreshInterval,
		"how often a running check's status is rewritten")
	fs.DurationVar(&o.restartInterval, "restart-interval",
		controller.DefaultRestartInterval,
		"minimum time between restarts of one check, so a spec edited repeatedly "+
			"does not rebuild the oracle on every edit")
	fs.DurationVar(&o.shutdownGrace, "shutdown-grace", controller.DefaultShutdownGrace,
		"how long to wait for checks to stop before giving up on them")

	fs.IntVar(&o.maxPublisherLabels, "max-publisher-labels", metrics.DefaultMaxPublisherLabels,
		"distinct publisher label values before the rest collapse into __other__")

	fs.BoolVar(&o.showVersion, "version", false, "print the build information and exit")
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "driftwatch-manager:", err)
		os.Exit(1)
	}
}

//nolint:funlen // an entrypoint that wires everything once; splitting it hides the order
func run() error {
	var opts options

	fs := flag.NewFlagSet("driftwatch-manager", flag.ContinueOnError)
	opts.bind(fs)

	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}
	if opts.showVersion {
		_, err := fmt.Fprintln(os.Stdout, buildinfo.String())
		return err
	}

	log, flush, err := logging.New(logging.Options{
		Level:  opts.logLevel,
		Format: opts.logFormat,
		V:      opts.verbosity,
		Out:    os.Stderr,
	})
	if err != nil {
		return err
	}
	defer flush() //nolint:errcheck // a failed flush on the way out is not actionable

	ctrl.SetLogger(log)
	log.Info("starting", "build", buildinfo.String())

	scheme, err := buildScheme()
	if err != nil {
		return err
	}

	m, err := buildMetrics(&opts, log)
	if err != nil {
		return err
	}

	clk := clock.Real()

	runners := controller.NewRunnerRegistry(controller.RegistryOptions{
		Logger:          log.WithName("runners"),
		Clock:           clk,
		Metrics:         m,
		RestartInterval: opts.restartInterval,
		ShutdownGrace:   opts.shutdownGrace,
	})

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                  scheme,
		Metrics:                 metricsserver.Options{BindAddress: opts.metricsAddr},
		HealthProbeBindAddress:  opts.probeAddr,
		LeaderElection:          opts.leaderElect,
		LeaderElectionID:        "driftwatch-manager.driftwatch.io",
		LeaderElectionNamespace: opts.leaseNamespace,
		// Release the lease on the way out, so a rolling update hands over in
		// seconds rather than waiting for the lease to expire — during which
		// nothing is auditing anything.
		LeaderElectionReleaseOnCancel: true,
		GracefulShutdownTimeout:       &opts.shutdownGrace,
		WebhookServer: webhook.NewServer(webhook.Options{
			Port:    opts.webhookPort,
			CertDir: opts.certDir,
		}),
		Cache: cacheOptions(opts.namespace),
	})
	if err != nil {
		return fmt.Errorf("building the manager: %w", err)
	}

	reconciler := &controller.DriftCheckReconciler{
		Client:                mgr.GetClient(),
		Scheme:                mgr.GetScheme(),
		Clock:                 clk,
		Metrics:               m,
		Runners:               runners,
		Recorder:              mgr.GetEventRecorderFor("driftwatch"),
		Log:                   log.WithName("driftcheck"),
		StatusRefreshInterval: opts.statusRefresh,
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("registering the DriftCheck controller: %w", err)
	}

	// Stopping the runners is a leader-elected runnable rather than a defer,
	// which is what makes §10.3's "on leadership loss, all runners stop" true
	// rather than aspirational. A manager that lost the lease and kept sweeping
	// would have two processes auditing the same store, both writing metrics
	// under the same check label.
	if err := mgr.Add(controller.NewRunnerStopper(runners, log)); err != nil {
		return fmt.Errorf("registering the runner stopper: %w", err)
	}

	if opts.enableWebhooks {
		if err := v1alpha1.SetupWebhookWithManager(mgr); err != nil {
			return fmt.Errorf("registering the DriftCheck webhook: %w", err)
		}
	} else {
		log.Info("webhooks are disabled; the CRD schema's defaults and enums still apply")
	}

	if err := mgr.AddHealthzCheck("ping", healthz.Ping); err != nil {
		return err
	}
	if err := mgr.AddReadyzCheck("ping", healthz.Ping); err != nil {
		return err
	}

	log.Info("running", "leaderElect", opts.leaderElect, "webhooks", opts.enableWebhooks)

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		return fmt.Errorf("manager exited: %w", err)
	}

	log.Info("stopped cleanly")
	return nil
}

// buildScheme registers the core types and driftwatch's.
func buildScheme() (*runtime.Scheme, error) {
	scheme := runtime.NewScheme()

	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("registering the core types: %w", err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("registering the driftwatch types: %w", err)
	}
	return scheme, nil
}

// buildMetrics puts driftwatch's metrics on controller-runtime's registry.
//
// One registry, so one scrape of one port returns both the check metrics and
// the controller's. Two endpoints would mean two ServiceMonitors and an
// operator working out which of them has the drift count on it.
func buildMetrics(opts *options, log logr.Logger) (*metrics.Metrics, error) {
	registry, ok := ctrlmetrics.Registry.(*prometheus.Registry)
	if !ok {
		return nil, errors.New(
			"controller-runtime's metric registry is not a *prometheus.Registry")
	}

	// The Go and process collectors are deliberately not registered here.
	// controller-runtime puts them on this registry itself, and registering
	// them again is not a duplicate-metric warning — it is a panic at startup,
	// before the first reconcile, in a binary that had passed every test.
	//
	// Nothing in the unit or envtest suites could have caught it: neither
	// builds this registry. It took running the real image in a cluster, which
	// is the argument for `make deploy` being part of the phase rather than a
	// nice-to-have.
	return metrics.New(metrics.Options{
		Registry:           registry,
		Logger:             log,
		MaxPublisherLabels: opts.maxPublisherLabels,
	}), nil
}

// cacheOptions restricts the cache to one namespace when asked.
//
// §18 prefers a namespaced deployment where the environment allows it: a
// manager that can read secrets in every namespace is a far larger blast radius
// than one that can read them only where it audits.
func cacheOptions(namespace string) ctrlcache.Options {
	if namespace == "" {
		return ctrlcache.Options{}
	}
	return ctrlcache.Options{
		DefaultNamespaces: map[string]ctrlcache.Config{namespace: {}},
	}
}
