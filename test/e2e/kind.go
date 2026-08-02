// Package e2e holds the Kind-based end-to-end suite (PRD §14).
//
// The test files in this package are behind the e2e build tag so that they
// never run in the unit suite; the helpers below are not, so that the package
// still builds under ./... .
package e2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// The names and images the suite works with.
//
// The cluster name is fixed rather than generated: DRIFTWATCH_E2E_REUSE_CLUSTER
// has to be able to find the cluster a previous run left, and a generated name
// would make local iteration — which is the whole point of that variable —
// impossible.
const (
	ClusterName = "driftwatch-e2e"

	ManagerImage   = "driftwatch/manager:e2e"
	HarnessImage   = "driftwatch/e2e-harness:e2e"
	RedisImage     = "redis:7.4-alpine"
	ToxiproxyImage = "ghcr.io/shopify/toxiproxy:2.11.0"

	// ManagerNamespace is where the operator runs. One manager watches every
	// scenario's namespace, which is what §14.4 E8 needs — two DriftChecks in
	// one process, separable only by the `check` metric label.
	ManagerNamespace = "driftwatch-system"
)

// Timeouts. Generous but bounded, per §14.5. Every one of them is a wait on
// something the cluster is doing rather than on a test assertion.
const (
	clusterCreateTimeout = 5 * time.Minute
	imageBuildTimeout    = 10 * time.Minute
	imageLoadTimeout     = 5 * time.Minute
	deployTimeout        = 3 * time.Minute
	kubectlTimeout       = 2 * time.Minute
)

// Env reads the suite's environment switches.
type Env struct {
	// ReuseCluster skips creation and teardown, so a local edit-run cycle
	// costs the scenario's runtime rather than two minutes of Kind.
	ReuseCluster bool
	// Keep leaves the cluster standing after the suite, for post-mortem.
	Keep bool
	// SkipBuild reuses whatever image is already loaded. Only sound with
	// ReuseCluster, and only when the Go code has not changed.
	SkipBuild bool
}

// ReadEnv reads the switches §14.2 and §14.5 name.
func ReadEnv() Env {
	return Env{
		ReuseCluster: truthy(os.Getenv("DRIFTWATCH_E2E_REUSE_CLUSTER")),
		Keep:         truthy(os.Getenv("DRIFTWATCH_E2E_KEEP")),
		SkipBuild:    truthy(os.Getenv("DRIFTWATCH_E2E_SKIP_BUILD")),
	}
}

func truthy(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// RepoRoot returns the repository root, derived from this file's own path.
//
// Not the working directory: `go test ./test/e2e/...` runs with the package
// directory as its working directory, and every path the suite needs — the
// Dockerfile, config/crd, the manifests — is relative to the root. Deriving it
// from runtime.Caller is what makes `make e2e` work identically from a clean
// clone in any directory, which §14.5 requires and which a hardcoded relative
// path quietly breaks the first time somebody runs the suite from elsewhere.
func RepoRoot() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.New("cannot determine this file's path")
	}

	root, err := filepath.Abs(filepath.Join(filepath.Dir(file), "..", ".."))
	if err != nil {
		return "", err
	}

	// A sanity check, so a future move of this file fails loudly here rather
	// than as a confusing "no such file" three steps later.
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		return "", fmt.Errorf("derived repo root %s has no go.mod: %w", root, err)
	}
	return root, nil
}

// ArtifactsDir is where diagnostics land.
func ArtifactsDir() (string, error) {
	root, err := RepoRoot()
	if err != nil {
		return "", err
	}
	return filepath.Join(root, "test", "e2e", "_artifacts"), nil
}

// ---------------------------------------------------------------------------
// Running commands.
// ---------------------------------------------------------------------------

// Result is one command's outcome.
type Result struct {
	Args   []string
	Stdout string
	Stderr string
	Err    error
}

// Output returns stdout, or stderr when stdout is empty — which is where kind
// and docker put most of what they have to say.
func (r Result) Output() string {
	if strings.TrimSpace(r.Stdout) != "" {
		return r.Stdout
	}
	return r.Stderr
}

func (r Result) String() string {
	var b strings.Builder

	fmt.Fprintf(&b, "$ %s\n", strings.Join(r.Args, " "))
	if r.Stdout != "" {
		b.WriteString(r.Stdout)
	}
	if r.Stderr != "" {
		b.WriteString(r.Stderr)
	}
	if r.Err != nil {
		fmt.Fprintf(&b, "\nerror: %v\n", r.Err)
	}
	return b.String()
}

// Run executes a command with a timeout and captures both streams.
func Run(ctx context.Context, timeout time.Duration, name string, args ...string) Result {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, name, args...) //nolint:gosec // fixed tool names
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if root, err := RepoRoot(); err == nil {
		cmd.Dir = root
	}

	err := cmd.Run()
	res := Result{
		Args:   append([]string{name}, args...),
		Stdout: stdout.String(),
		Stderr: stderr.String(),
		Err:    err,
	}

	if err != nil && ctx.Err() != nil {
		res.Err = fmt.Errorf("%w (timed out after %s)", err, timeout)
	}
	return res
}

// RunStdin executes a command with input on stdin.
//
// Manifests are piped rather than written to a file. A file would have to live
// somewhere the suite owns, be cleaned up on every exit path including a
// panicking scenario, and carry a name unique enough that two scenarios running
// in parallel do not overwrite each other's.
func RunStdin(ctx context.Context, timeout time.Duration, stdin, name string, args ...string) Result {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, name, args...) //nolint:gosec // fixed tool names
	cmd.Stdin = strings.NewReader(stdin)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if root, err := RepoRoot(); err == nil {
		cmd.Dir = root
	}

	err := cmd.Run()
	res := Result{
		Args:   append([]string{name}, args...),
		Stdout: stdout.String(),
		Stderr: stderr.String(),
		Err:    err,
	}

	if err != nil && ctx.Err() != nil {
		res.Err = fmt.Errorf("%w (timed out after %s)", err, timeout)
	}
	return res
}

// KubectlApply pipes a manifest into `kubectl apply -f -`.
func KubectlApply(ctx context.Context, namespace, manifest string) error {
	res := RunStdin(ctx, kubectlTimeout, manifest, "kubectl",
		"--context", "kind-"+ClusterName, "-n", namespace, "apply", "-f", "-")

	if res.Err != nil {
		return fmt.Errorf("applying to %s: %w\n%s", namespace, res.Err, res)
	}
	return nil
}

// MustRun runs a command and returns an error carrying both streams.
func MustRun(ctx context.Context, timeout time.Duration, name string, args ...string) (string, error) {
	res := Run(ctx, timeout, name, args...)
	if res.Err != nil {
		return res.Output(), fmt.Errorf("%s failed: %w\n%s", name, res.Err, res)
	}
	return res.Output(), nil
}

// Kubectl runs kubectl against the e2e cluster's context.
//
// The context is always passed explicitly. A suite that used whatever context
// the developer's kubeconfig happened to have selected would, on its first bad
// day, delete a namespace in a real cluster.
func Kubectl(ctx context.Context, args ...string) Result {
	full := append([]string{"--context", "kind-" + ClusterName}, args...)
	return Run(ctx, kubectlTimeoutFor(args), "kubectl", full...)
}

// kubectlTimeoutFor gives a command that waits its own budget plus a margin.
//
// `kubectl wait` and `kubectl rollout status` take a --timeout of their own, and
// wrapping one in a shorter deadline means killing it just before it would have
// reported something useful. That happened: a `rollout status --timeout=3m`
// inside a 2m wrapper died at two minutes with "timed out after 2m0s", which
// reads as the deployment being stuck rather than as the harness cutting it off.
//
// So a waiting command gets the longest deadline the suite uses, and everything
// else keeps the short one — a `kubectl get` that has not answered in two
// minutes is not going to.
func kubectlTimeoutFor(args []string) time.Duration {
	for _, arg := range args {
		switch arg {
		case "wait", "rollout":
			return deployTimeout + 30*time.Second
		}
	}
	return kubectlTimeout
}

// KubectlOut runs kubectl and returns an error carrying both streams.
func KubectlOut(ctx context.Context, args ...string) (string, error) {
	res := Kubectl(ctx, args...)
	if res.Err != nil {
		return res.Output(), fmt.Errorf("kubectl failed: %w\n%s", res.Err, res)
	}
	return res.Output(), nil
}

// ---------------------------------------------------------------------------
// Retry, for setup only.
// ---------------------------------------------------------------------------

// retry runs a setup step until it succeeds or the attempts run out.
//
// §14.5 is explicit that this is for cluster setup and never for assertions.
// The distinction is not stylistic: retrying `kind load docker-image` papers
// over a containerd hiccup that has nothing to do with driftwatch, while
// retrying an assertion papers over the bug the assertion exists to find. There
// is deliberately no exported form of this — nothing in a scenario file can
// reach it.
func retry(ctx context.Context, attempts int, gap time.Duration, what string, fn func() error) error {
	var last error

	for attempt := 1; attempt <= attempts; attempt++ {
		last = fn()
		if last == nil {
			return nil
		}
		if attempt == attempts {
			break
		}

		fmt.Printf("e2e: %s failed (attempt %d/%d), retrying in %s: %v\n",
			what, attempt, attempts, gap, last)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(gap):
		}
	}
	return fmt.Errorf("%s failed after %d attempts: %w", what, attempts, last)
}

// ---------------------------------------------------------------------------
// Cluster lifecycle.
// ---------------------------------------------------------------------------

// Cluster is the e2e cluster and what the suite built into it.
type Cluster struct {
	Env Env
	// Created reports whether this run made the cluster, which decides whether
	// teardown may remove it. Reusing somebody else's cluster and then deleting
	// it would be a genuinely unpleasant surprise.
	Created bool
}

// EnsureCluster brings up the cluster, the images and the operator.
//
// Every step is idempotent, because DRIFTWATCH_E2E_REUSE_CLUSTER means a run
// can begin against a cluster that already has all of this in it.
func EnsureCluster(ctx context.Context) (*Cluster, error) {
	env := ReadEnv()
	c := &Cluster{Env: env}

	if err := checkPrerequisites(ctx); err != nil {
		return nil, err
	}

	exists, err := clusterExists(ctx)
	if err != nil {
		return nil, err
	}

	switch {
	case exists && env.ReuseCluster:
		fmt.Printf("e2e: reusing the existing %s cluster\n", ClusterName)
	case exists:
		// A cluster left by a previous run that did not reuse is stale: it may
		// hold an older CRD, an older image, or a half-deleted namespace.
		// Deleting is slower and is the only way to be sure what is being
		// tested. DRIFTWATCH_E2E_REUSE_CLUSTER=1 is the opt-out.
		fmt.Printf("e2e: deleting the stale %s cluster (set "+
			"DRIFTWATCH_E2E_REUSE_CLUSTER=1 to keep it)\n", ClusterName)
		if err := DeleteCluster(ctx); err != nil {
			return nil, err
		}
		if err := createCluster(ctx); err != nil {
			return nil, err
		}
		c.Created = true
	default:
		if err := createCluster(ctx); err != nil {
			return nil, err
		}
		c.Created = true
	}

	if !env.SkipBuild {
		if err := buildAndLoadImages(ctx); err != nil {
			return nil, err
		}
	}
	if err := deployOperator(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

// checkPrerequisites fails early with an actionable message.
//
// §14.5 requires `make e2e` to work from a clean clone with only Docker and Go.
// This exists so that a machine missing one of them is told which, rather than
// finding out from `exec: "kind": executable file not found` forty seconds into
// a build.
func checkPrerequisites(ctx context.Context) error {
	if res := Run(ctx, 30*time.Second, "docker", "info", "--format",
		"{{.ServerVersion}}"); res.Err != nil {
		return fmt.Errorf(
			"docker is not reachable; the e2e suite needs a running daemon:\n%s", res)
	}

	for _, tool := range []string{"kind", "kubectl"} {
		if _, err := exec.LookPath(tool); err != nil {
			return fmt.Errorf("%s is not on PATH; run 'make install-tools'", tool)
		}
	}
	return nil
}

func clusterExists(ctx context.Context) (bool, error) {
	out, err := MustRun(ctx, 60*time.Second, "kind", "get", "clusters")
	if err != nil {
		return false, err
	}

	for _, line := range strings.Split(out, "\n") {
		if strings.TrimSpace(line) == ClusterName {
			return true, nil
		}
	}
	return false, nil
}

func createCluster(ctx context.Context) error {
	root, err := RepoRoot()
	if err != nil {
		return err
	}
	config := filepath.Join(root, "test", "e2e", "manifests", "kind-config.yaml")

	fmt.Printf("e2e: creating the %s cluster\n", ClusterName)

	return retry(ctx, 2, 10*time.Second, "kind create cluster", func() error {
		_, err := MustRun(ctx, clusterCreateTimeout,
			"kind", "create", "cluster",
			"--name", ClusterName,
			"--config", config,
			"--wait", "120s")
		return err
	})
}

// DeleteCluster removes the cluster.
func DeleteCluster(ctx context.Context) error {
	_, err := MustRun(ctx, clusterCreateTimeout,
		"kind", "delete", "cluster", "--name", ClusterName)
	return err
}

// Teardown removes the cluster unless it was reused or DRIFTWATCH_E2E_KEEP is
// set.
func (c *Cluster) Teardown(ctx context.Context) error {
	switch {
	case c.Env.Keep:
		fmt.Printf("e2e: DRIFTWATCH_E2E_KEEP is set; leaving %s standing\n", ClusterName)
		fmt.Printf("     kubectl --context kind-%s get driftchecks -A\n", ClusterName)
		return nil
	case !c.Created:
		fmt.Println("e2e: the cluster was not created by this run; leaving it standing")
		return nil
	default:
		fmt.Printf("e2e: deleting the %s cluster\n", ClusterName)
		return DeleteCluster(ctx)
	}
}

// ---------------------------------------------------------------------------
// Images.
// ---------------------------------------------------------------------------

func buildAndLoadImages(ctx context.Context) error {
	root, err := RepoRoot()
	if err != nil {
		return err
	}

	images := []struct {
		tag        string
		dockerfile string
	}{
		{ManagerImage, filepath.Join(root, "Dockerfile")},
		{HarnessImage, filepath.Join(root, "test", "e2e", "Dockerfile")},
	}

	for _, img := range images {
		fmt.Printf("e2e: building %s\n", img.tag)

		// The build is not retried. A build failure is a compile error or a bad
		// Dockerfile, and retrying it spends ten minutes arriving at the same
		// message.
		if _, err := MustRun(ctx, imageBuildTimeout,
			"docker", "build", "-t", img.tag, "-f", img.dockerfile, root); err != nil {
			return err
		}

		fmt.Printf("e2e: loading %s into the cluster\n", img.tag)

		// The load is retried: it copies a tarball into containerd over the
		// docker socket, which is exactly the kind of step that fails once on a
		// loaded machine and works immediately afterwards.
		tag := img.tag
		if err := retry(ctx, 3, 5*time.Second, "kind load "+tag, func() error {
			_, err := MustRun(ctx, imageLoadTimeout,
				"kind", "load", "docker-image", tag, "--name", ClusterName)
			return err
		}); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// The operator.
// ---------------------------------------------------------------------------

func deployOperator(ctx context.Context) error {
	root, rootErr := RepoRoot()
	if rootErr != nil {
		return rootErr
	}

	fmt.Println("e2e: installing the CRD")
	if err := retry(ctx, 3, 5*time.Second, "apply the CRD", func() error {
		_, err := KubectlOut(ctx, "apply", "-f", filepath.Join(root, "config", "crd", "bases"))
		return err
	}); err != nil {
		return err
	}

	// Waiting for Established rather than assuming: a CRD the API server has
	// accepted but not yet served makes the first DriftCheck create fail with
	// "no matches for kind", which reads like a broken manifest.
	if _, err := KubectlOut(ctx, "wait", "--for=condition=Established",
		"crd/driftchecks.driftwatch.io", "--timeout=60s"); err != nil {
		return err
	}

	fmt.Println("e2e: deploying the manager")
	manifest := filepath.Join(root, "test", "e2e", "manifests", "manager.yaml")

	if err := retry(ctx, 3, 5*time.Second, "apply the manager", func() error {
		_, err := KubectlOut(ctx, "apply", "-f", manifest)
		return err
	}); err != nil {
		return err
	}

	// Always roll the deployment, even though the manifest was just applied.
	//
	// `kubectl apply` is a no-op when the spec has not changed, and the spec
	// never changes: the image tag is fixed at driftwatch/manager:e2e and the
	// pull policy is Never. `kind load docker-image` puts the newly built
	// layers into the node's containerd, but a pod that is already running does
	// not restart because of that. So on a reused cluster the manager keeps
	// serving whatever binary it started with.
	//
	// This cost most of an afternoon. `make e2e-reuse` is the documented
	// fast-iteration path, and it was silently exercising a seventeen-hour-old
	// build — so a fix would be made, the suite re-run, the same failure seen,
	// and the search would move somewhere the bug was not. The failure looks
	// exactly like a fix that did not work.
	//
	// A rollout costs a few seconds and removes the whole category.
	fmt.Println("e2e: rolling the manager onto the image just built")
	if _, err := KubectlOut(ctx, "-n", ManagerNamespace,
		"rollout", "restart", "deployment/driftwatch-manager"); err != nil {
		return err
	}

	fmt.Println("e2e: waiting for the manager")
	_, waitErr := KubectlOut(ctx, "-n", ManagerNamespace, "rollout", "status",
		"deployment/driftwatch-manager", "--timeout="+deployTimeout.String())
	return waitErr
}

// RestartManager rolls the manager and waits for it.
//
// E6 needs a manager whose goroutine count is comparable before and after a
// lifecycle, which means starting from a known state rather than from whatever
// the scenarios before it left behind.
func RestartManager(ctx context.Context) error {
	if _, err := KubectlOut(ctx, "-n", ManagerNamespace,
		"rollout", "restart", "deployment/driftwatch-manager"); err != nil {
		return err
	}
	_, err := KubectlOut(ctx, "-n", ManagerNamespace, "rollout", "status",
		"deployment/driftwatch-manager", "--timeout="+deployTimeout.String())
	return err
}

// ManagerPod returns the name of the running manager pod.
func ManagerPod(ctx context.Context) (string, error) {
	out, err := KubectlOut(ctx, "-n", ManagerNamespace, "get", "pods",
		"-l", "app.kubernetes.io/name=driftwatch",
		"--field-selector=status.phase=Running",
		"-o", "jsonpath={.items[0].metadata.name}")
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(out) == "" {
		return "", errors.New("no running manager pod")
	}
	return strings.TrimSpace(out), nil
}
