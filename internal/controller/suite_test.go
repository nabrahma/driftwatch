package controller

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/nabrahma/driftwatch/api/v1alpha1"
	"github.com/nabrahma/driftwatch/pkg/check"
	"github.com/nabrahma/driftwatch/pkg/clock"
)

// The envtest suite runs against a real API server and a real etcd, which is
// the only way to test the things that make a controller a controller.
//
// A fake client would let every one of these tests pass while the controller
// was broken in production: fake clients do not enforce optimistic concurrency,
// so the conflict-retry path would never execute; they do not run finalizers,
// so a deletion would complete regardless of whether the runner stopped; and
// they do not validate against the CRD schema, so a status field that does not
// exist would be silently accepted. Those three are exactly what §10.3 asks
// this suite to prove.

var (
	// testEnv is the shared API server. One per package: starting etcd costs
	// several seconds and nothing here needs isolation between servers, only
	// between namespaces.
	testEnv *envtest.Environment
	// testCfg is the connection to it.
	testCfg *rest.Config
	// testScheme carries the core types and driftwatch's.
	testScheme *k8sruntime.Scheme
)

func TestMain(m *testing.M) {
	assets, err := envtestAssets()
	if err != nil {
		fmt.Fprintf(os.Stderr, "controller suite: %v\n", err)
		fmt.Fprintln(os.Stderr,
			"run: setup-envtest use 1.31.0, then export KUBEBUILDER_ASSETS")
		os.Exit(1)
	}

	// controller-runtime writes a stack trace to stderr on first use if no
	// logger was ever set, which buries the first real failure in this suite.
	ctrl.SetLogger(logr.Discard())

	testScheme = k8sruntime.NewScheme()
	if err = clientgoscheme.AddToScheme(testScheme); err != nil {
		panic(err)
	}
	if err = v1alpha1.AddToScheme(testScheme); err != nil {
		panic(err)
	}

	testEnv = &envtest.Environment{
		// The committed CRD, not a hand-written one. That makes every test here
		// also a test that the generated schema accepts the objects the
		// controller writes — including the status subresource, which is where
		// a mistyped field would otherwise be silently dropped.
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
		BinaryAssetsDirectory: assets,
	}

	testCfg, err = testEnv.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "controller suite: starting envtest: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()

	if err := testEnv.Stop(); err != nil {
		fmt.Fprintf(os.Stderr, "controller suite: stopping envtest: %v\n", err)
	}
	os.Exit(code)
}

// envtestAssets finds the API server and etcd binaries.
//
// KUBEBUILDER_ASSETS wins, which is what the Makefile sets. Falling back to
// setup-envtest's own store means `go test ./internal/controller/...` works
// without the Makefile, and a developer who has run setup-envtest once does not
// have to remember an environment variable.
func envtestAssets() (string, error) {
	if path := os.Getenv("KUBEBUILDER_ASSETS"); path != "" {
		return path, nil
	}

	store := setupEnvtestStore()
	if store == "" {
		return "", fmt.Errorf("KUBEBUILDER_ASSETS is unset and no envtest store was found")
	}

	pattern := filepath.Join(store, "*-"+runtime.GOOS+"-"+runtime.GOARCH)
	matches, err := filepath.Glob(pattern)
	if err != nil || len(matches) == 0 {
		return "", fmt.Errorf("no envtest binaries under %s", store)
	}

	// Newest version last, so a machine with several installed uses the most
	// recent rather than whichever the filesystem happened to list first.
	sort.Strings(matches)
	return matches[len(matches)-1], nil
}

func setupEnvtestStore() string {
	if runtime.GOOS == "windows" {
		if local := os.Getenv("LOCALAPPDATA"); local != "" {
			return filepath.Join(local, "kubebuilder-envtest", "k8s")
		}
		return ""
	}

	if xdg := os.Getenv("XDG_DATA_HOME"); xdg != "" {
		return filepath.Join(xdg, "kubebuilder-envtest", "k8s")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".local", "share", "kubebuilder-envtest", "k8s")
}

// ---------------------------------------------------------------------------
// The fixture every reconciler test builds on.
// ---------------------------------------------------------------------------

// fixture is one test's namespace, client, reconciler and recorder.
type fixture struct {
	t          *testing.T
	ctx        context.Context
	client     client.Client
	reconciler *DriftCheckReconciler
	registry   *testRegistry
	recorder   *record.FakeRecorder
	clock      clock.FakeClock
	namespace  string
}

// fixtureOptions customizes the reconciler under test.
type fixtureOptions struct {
	// configure customizes each stub runnable as it is built.
	configure func(spec check.Spec, s *stubRunnable)
	// restartInterval overrides the debounce. Negative disables it.
	restartInterval time.Duration
	// realChecks builds real *check.Check instances instead of stubs, which is
	// what the tests about construction failure need: no stub can reproduce
	// check.New rejecting a spec, and that is the behavior under test.
	realChecks bool
}

// newFixture starts a reconciler against the shared API server, in its own
// namespace.
//
// Per-namespace rather than per-server, because starting etcd costs seconds and
// the isolation these tests need is between objects, not between API servers.
func newFixture(t *testing.T, opts fixtureOptions) *fixture {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cl, err := client.New(testCfg, client.Options{Scheme: testScheme})
	require.NoError(t, err)

	namespace := uniqueNamespace(t)
	require.NoError(t, cl.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: namespace},
	}))

	clk := newFakeClock()

	registryOpts := RegistryOptions{
		Logger:          testLogger(t),
		Clock:           clk,
		RestartInterval: opts.restartInterval,
	}
	if opts.realChecks {
		registryOpts.Build = buildCheck
	}

	registry := newTestRegistry(t, registryOpts)
	registry.configure = opts.configure

	// A buffered fake recorder, because the real one talks to the API server
	// and asserting on what an operator would see in `kubectl describe` is
	// easier against a channel than against an Event list that is written
	// asynchronously.
	recorder := record.NewFakeRecorder(64)

	return &fixture{
		t:   t,
		ctx: ctx,
		reconciler: &DriftCheckReconciler{
			Client:                cl,
			Scheme:                testScheme,
			Clock:                 clk,
			Runners:               registry.RunnerRegistry,
			Recorder:              recorder,
			Log:                   testLogger(t),
			StatusRefreshInterval: 15 * time.Second,
			SecretRetryInterval:   30 * time.Second,
		},
		client:    cl,
		registry:  registry,
		recorder:  recorder,
		clock:     clk,
		namespace: namespace,
	}
}

// uniqueNamespace derives a legal namespace name from the test's own.
//
// A DNS-1123 label: at most 63 characters, lower case, and starting and ending
// with an alphanumeric. The last of those is the one worth being careful about
// — an earlier version truncated a long name from the left and occasionally
// produced a leading hyphen, which failed one test in four with an error about
// label syntax rather than about the controller.
func uniqueNamespace(t *testing.T) string {
	t.Helper()

	const (
		prefix    = "t-"
		maxLabel  = 63
		suffixLen = 6 // "-" plus five digits
	)

	lowered := make([]rune, 0, len(t.Name()))
	for _, r := range t.Name() {
		switch {
		case r >= 'A' && r <= 'Z':
			lowered = append(lowered, r+('a'-'A'))
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'):
			lowered = append(lowered, r)
		default:
			lowered = append(lowered, '-')
		}
	}

	// Truncate the middle, keeping the prefix and the suffix intact, so what is
	// lost is part of the test's name rather than the parts that make the
	// result legal and unique.
	body := string(lowered)
	if budget := maxLabel - len(prefix) - suffixLen; len(body) > budget {
		body = body[:budget]
	}
	body = strings.Trim(body, "-")

	return fmt.Sprintf("%s%s-%05d", prefix, body, time.Now().UnixNano()%100000)
}

// create applies a DriftCheck through the same defaulting the webhook performs.
//
// envtest does not run the webhooks — they need certificates and a service — so
// the defaults are applied here explicitly. The CRD's schema defaults still
// apply, because those are the API server's job; this covers the sub-object
// creation only the webhook can do.
func (f *fixture) create(name string, mutate ...func(*v1alpha1.DriftCheck)) *v1alpha1.DriftCheck {
	f.t.Helper()

	dc := &v1alpha1.DriftCheck{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: f.namespace},
		Spec: v1alpha1.DriftCheckSpec{
			Source: v1alpha1.SourceSpec{
				Type: "memory",
			},
			Projection: v1alpha1.ProjectionSpec{Type: "scalar"},
			Target:     v1alpha1.TargetSpec{Type: "memory"},
		},
	}
	for _, m := range mutate {
		m(dc)
	}
	dc.Spec.Default()

	require.NoError(f.t, f.client.Create(f.ctx, dc))
	return dc
}

// reconcile runs one pass and returns the result.
func (f *fixture) reconcile(name string) ctrl.Result {
	f.t.Helper()

	result, err := f.reconciler.Reconcile(f.ctx, ctrl.Request{NamespacedName: f.key(name)})
	require.NoError(f.t, err)
	return result
}

// reconcileErr runs one pass and returns the error.
func (f *fixture) reconcileErr(name string) error {
	f.t.Helper()

	_, err := f.reconciler.Reconcile(f.ctx, ctrl.Request{NamespacedName: f.key(name)})
	return err
}

func (f *fixture) key(name string) types.NamespacedName {
	return types.NamespacedName{Namespace: f.namespace, Name: name}
}

// get re-reads a DriftCheck from the API server.
func (f *fixture) get(name string) *v1alpha1.DriftCheck {
	f.t.Helper()

	var dc v1alpha1.DriftCheck
	require.NoError(f.t, f.client.Get(f.ctx, f.key(name), &dc))
	return &dc
}

// update applies a spec change through the API server.
func (f *fixture) update(name string, mutate func(*v1alpha1.DriftCheck)) *v1alpha1.DriftCheck {
	f.t.Helper()

	dc := f.get(name)
	mutate(dc)
	dc.Spec.Default()

	require.NoError(f.t, f.client.Update(f.ctx, dc))
	return dc
}

// secret creates a Secret in the fixture's namespace.
func (f *fixture) secret(name, key, value string) {
	f.t.Helper()

	require.NoError(f.t, f.client.Create(f.ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: f.namespace},
		Data:       map[string][]byte{key: []byte(value)},
	}))
}

// events drains everything the recorder has buffered.
func (f *fixture) events() []string {
	f.t.Helper()

	var out []string
	for {
		select {
		case e := <-f.recorder.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

// setStatus makes the running check report a given state, so the reconciler's
// rendering and event emission can be driven without a real drift episode.
func (f *fixture) setStatus(name string, mutate func(*check.Status)) {
	f.t.Helper()

	runner := f.reconciler.Runners.Get(f.key(name))
	require.NotNil(f.t, runner, "no runner for %s", name)

	stub, ok := runner.Check().(*stubRunnable)
	require.True(f.t, ok, "the fixture builds stub runnables")

	stub.setStatus(mutate)
}
