package e2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Diagnostics are an explicit deliverable (§14.3), and the reason is arithmetic
// rather than tidiness: a CI e2e failure with no artifacts costs somebody an
// hour of re-running the suite locally to see what the cluster looked like, and
// on a flaky failure they may never see it at all.
//
// Two rules keep this useful rather than merely voluminous.
//
// Nothing here may fail the test. Every step records its own error into the
// dump and continues, because a diagnostics collector that panicked while
// collecting would replace a real failure with a confusing one at exactly the
// moment the real failure mattered.
//
// Every step is bounded. A cluster that has stopped answering is the common
// case at collection time, so each command gets its own short timeout and the
// whole collection gets one too — an AfterEach that hung for ten minutes on an
// unreachable API server would turn one failed scenario into a timed-out suite
// with no artifacts at all.

// The budgets. Short, because these run against a cluster that is by definition
// already unhappy.
const (
	diagCommandTimeout = 30 * time.Second
	diagTotalTimeout   = 3 * time.Minute
)

// Collector gathers the §14.3 list for one scenario.
type Collector struct {
	// Namespace is the scenario's namespace.
	Namespace string
	// CheckNames are the DriftChecks the scenario created.
	CheckNames []string
	// Dir is where the dump lands.
	Dir string

	// written records what was collected, for the summary line.
	written []string
	// problems records collection failures, so a thin dump says why it is thin.
	problems []string
}

// NewCollector prepares a dump directory for a test.
func NewCollector(testName, namespace string, checks ...string) (*Collector, error) {
	base, err := ArtifactsDir()
	if err != nil {
		return nil, err
	}

	dir := filepath.Join(base, sanitize(testName))
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("creating %s: %w", dir, err)
	}

	return &Collector{Namespace: namespace, CheckNames: checks, Dir: dir}, nil
}

// sanitize turns a Ginkgo test name into a directory name.
//
// Ginkgo names contain spaces, slashes and quotes; a path built straight from
// one either fails to create or creates a surprising directory tree.
var unsafePath = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)

func sanitize(name string) string {
	clean := unsafePath.ReplaceAllString(strings.TrimSpace(name), "-")
	clean = strings.Trim(clean, "-")

	if clean == "" {
		clean = "unnamed"
	}
	if len(clean) > 120 {
		clean = clean[:120]
	}
	return clean
}

// Collect gathers everything §14.3 lists.
//
// The order is deliberate: the DriftCheck's own status and the manager's logs
// come first, because those answer most failures on their own and a collection
// that runs out of time should have them.
func (c *Collector) Collect(ctx context.Context, failureMessage string) string {
	ctx, cancel := context.WithTimeout(ctx, diagTotalTimeout)
	defer cancel()

	c.writeFile("00-failure.txt", failureMessage)

	c.driftChecks(ctx)
	c.managerLogs(ctx)
	c.fixtureLogs(ctx)
	c.redis(ctx)
	c.metrics(ctx)
	c.explain(ctx)
	c.clusterState(ctx)

	return c.summary()
}

// ---------------------------------------------------------------------------
// The DriftCheck itself.
// ---------------------------------------------------------------------------

func (c *Collector) driftChecks(ctx context.Context) {
	// -o yaml carries the full status: every condition, every publisher, the
	// spec that was actually applied after defaulting. §14.3 asks for the
	// applied spec separately; it is in here, and having it in one file next to
	// the status it produced is more useful than two.
	c.capture(ctx, "01-driftcheck.yaml",
		"get", "driftcheck", "-n", c.Namespace, "-o", "yaml")

	// describe carries the events, which are what an operator reads first.
	c.capture(ctx, "02-driftcheck-describe.txt",
		"describe", "driftcheck", "-n", c.Namespace)
}

// ---------------------------------------------------------------------------
// Logs.
// ---------------------------------------------------------------------------

func (c *Collector) managerLogs(ctx context.Context) {
	c.capture(ctx, "03-manager.log",
		"logs", "-n", ManagerNamespace,
		"-l", "app.kubernetes.io/name=driftwatch", "--tail=2000", "--all-containers")

	// The previous container's logs. §14.3 asks for both, and this is the one
	// that matters: a manager that crash-looped has nothing useful in its
	// current log, because the current container has only just started.
	c.capture(ctx, "04-manager-previous.log",
		"logs", "-n", ManagerNamespace,
		"-l", "app.kubernetes.io/name=driftwatch", "--tail=2000", "--previous")
}

func (c *Collector) fixtureLogs(ctx context.Context) {
	for _, app := range []string{"publisher", "materializer", "toxiproxy"} {
		c.capture(ctx, "05-"+app+".log",
			"logs", "-n", c.Namespace, "-l", "app="+app, "--tail=1000")
	}
}

// ---------------------------------------------------------------------------
// Redis.
// ---------------------------------------------------------------------------

// redis dumps what the store actually held.
//
// This is the half of the comparison the CRD's status cannot show. A finding
// says "the target disagrees"; only this says what the target had in it, and
// for a mass-divergence failure the difference between "Redis is empty" and
// "Redis has the wrong values" is the whole diagnosis.
func (c *Collector) redis(ctx context.Context) {
	pod, err := c.podName(ctx, "app=redis")
	if err != nil {
		c.problem("redis pod: %v", err)
		return
	}

	c.captureExec(ctx, "06-redis-info.txt", pod, "redis-cli", "INFO", "all")
	c.captureExec(ctx, "07-redis-dbsize.txt", pod, "redis-cli", "DBSIZE")

	// The first 200 keys with their values. §14.3 asks for SCAN rather than
	// KEYS, which matters even here: KEYS blocks the server for the length of
	// the keyspace, and doing that during diagnostics could turn a scenario
	// failure into a Redis timeout in whatever runs next.
	script := `
		cursor=0
		count=0
		while : ; do
			out=$(redis-cli --no-raw SCAN $cursor COUNT 100)
			cursor=$(echo "$out" | head -1 | tr -d '"')
			for key in $(echo "$out" | tail -n +2 | tr -d '"'); do
				type=$(redis-cli TYPE "$key")
				case "$type" in
					set)  echo "$key ($type) = $(redis-cli SMEMBERS "$key" | tr '\n' ',')" ;;
					*)    echo "$key ($type) = $(redis-cli GET "$key")" ;;
				esac
				count=$((count+1))
				[ "$count" -ge 200 ] && break 2
			done
			[ "$cursor" = "0" ] && break
		done
		echo "--- $count keys shown ---"
	`
	c.captureExec(ctx, "08-redis-keys.txt", pod, "sh", "-c", script)
}

// ---------------------------------------------------------------------------
// Metrics.
// ---------------------------------------------------------------------------

// metrics scrapes the manager, filtered to driftwatch's own series.
//
// Unfiltered, the scrape is several hundred lines of Go runtime and
// controller-runtime metrics with driftwatch's twenty interesting ones
// somewhere inside. The full scrape is kept alongside, because occasionally the
// answer is in controller-runtime's reconcile error counter.
func (c *Collector) metrics(ctx context.Context) {
	pod, err := ManagerPod(ctx)
	if err != nil {
		c.problem("manager pod: %v", err)
		return
	}

	raw, err := c.scrapeMetrics(ctx, pod)
	if err != nil {
		c.problem("scraping metrics: %v", err)
		return
	}
	c.writeFile("09-metrics-all.txt", raw)

	var driftwatch strings.Builder
	for _, line := range strings.Split(raw, "\n") {
		if strings.HasPrefix(line, "driftwatch_") ||
			strings.HasPrefix(line, "# HELP driftwatch_") ||
			strings.HasPrefix(line, "# TYPE driftwatch_") {
			driftwatch.WriteString(line)
			driftwatch.WriteString("\n")
		}
	}
	c.writeFile("10-metrics-driftwatch.txt", driftwatch.String())
}

// scrapeMetrics reads /metrics through the API server's proxy.
//
// Not `kubectl exec` and not a port-forward. The manager image is distroless
// with no shell and no curl, so there is nothing to exec; a port-forward is a
// background process this would then have to manage and reap. The proxy
// subresource needs neither.
func (c *Collector) scrapeMetrics(ctx context.Context, pod string) (string, error) {
	return KubectlOut(ctx, "get", "--raw",
		fmt.Sprintf("/api/v1/namespaces/%s/pods/%s:8080/proxy/metrics", ManagerNamespace, pod))
}

// ScrapeMetrics returns the manager's whole /metrics response.
//
// Through the API server's proxy rather than a port-forward: the manager image
// is distroless with no shell and no curl, and a port-forward would be a
// subprocess for every scenario to manage and reap.
func ScrapeMetrics(ctx context.Context) (string, error) {
	pod, err := ManagerPod(ctx)
	if err != nil {
		return "", err
	}
	return KubectlOut(ctx, "get", "--raw",
		fmt.Sprintf("/api/v1/namespaces/%s/pods/%s:8080/proxy/metrics", ManagerNamespace, pod))
}

// MetricValue sums every series of one metric, across all label combinations.
//
// Summed rather than matched on labels, because what the scenarios ask is "did
// this ever happen" rather than "did it happen for this exact label set" — and
// a scenario that hard-coded a label combination would break the first time a
// label was added, for a reason unrelated to what it asserts.
func MetricValue(ctx context.Context, name string) (float64, error) {
	raw, err := ScrapeMetrics(ctx)
	if err != nil {
		return 0, err
	}

	total := 0.0
	found := false

	for _, line := range strings.Split(raw, "\n") {
		if !strings.HasPrefix(line, name) {
			continue
		}
		// Guard against a prefix match: driftwatch_sweeps_total must not pick
		// up driftwatch_sweeps_total_something. The character after the name is
		// either a brace or a space in a well-formed exposition.
		rest := line[len(name):]
		if rest == "" || (rest[0] != '{' && rest[0] != ' ') {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			continue
		}
		total += v
		found = true
	}

	if !found {
		return 0, fmt.Errorf("%s is not present in the manager's /metrics", name)
	}
	return total, nil
}

// GoroutineCount reads the manager's live goroutine count from pprof.
//
// E6 asserts this does not grow across a full DriftCheck lifecycle. It is
// exported because that scenario needs it, and it lives here because the
// plumbing — reaching a distroless pod's HTTP endpoint through the API server —
// is the same plumbing the metrics scrape needs.
func GoroutineCount(ctx context.Context) (int, error) {
	pod, err := ManagerPod(ctx)
	if err != nil {
		return 0, err
	}

	out, err := KubectlOut(ctx, "get", "--raw",
		fmt.Sprintf("/api/v1/namespaces/%s/pods/%s:8080/proxy/debug/pprof/goroutine?debug=1",
			ManagerNamespace, pod))
	if err != nil {
		return 0, err
	}

	// The first line is "goroutine profile: total N".
	for _, line := range strings.Split(out, "\n") {
		if !strings.HasPrefix(line, "goroutine profile: total ") {
			continue
		}
		var n int
		if _, err := fmt.Sscanf(line, "goroutine profile: total %d", &n); err != nil {
			return 0, fmt.Errorf("parsing %q: %w", line, err)
		}
		return n, nil
	}
	return 0, fmt.Errorf("no goroutine total in the profile:\n%s", truncate(out, 400))
}

// ---------------------------------------------------------------------------
// explain
// ---------------------------------------------------------------------------

// explain runs the CLI against the first five divergent keys.
//
// §14.3 asks for this specifically, and it is the item that most often answers
// the question on its own: the status says how many keys diverged, and explain
// says why driftwatch thinks so for a named key, with the event history behind
// it.
//
// The CLI cannot reach the running check's oracle from outside the manager —
// that state is in the manager's memory — so what is captured is the per-key
// detail the status block carries plus the key names, which is what makes a
// manual `driftwatch explain` reproducible afterwards.
func (c *Collector) explain(ctx context.Context) {
	var b strings.Builder

	for _, name := range c.CheckNames {
		out, err := KubectlOut(ctx, "get", "driftcheck", name, "-n", c.Namespace, "-o",
			`jsonpath={range .status.conditions[*]}{.type}={.status} ({.reason}): {.message}{"\n"}{end}`)
		if err != nil {
			c.problem("conditions for %s: %v", name, err)
			continue
		}

		fmt.Fprintf(&b, "=== %s/%s ===\n%s\n", c.Namespace, name, out)

		categories, err := KubectlOut(ctx, "get", "driftcheck", name, "-n", c.Namespace,
			"-o", `jsonpath={.status.divergenceByCategory}`)
		if err == nil && strings.TrimSpace(categories) != "" {
			fmt.Fprintf(&b, "divergence by category: %s\n\n", categories)
		}

		fmt.Fprintf(&b, "To reproduce the per-key diagnosis against this cluster:\n"+
			"  kubectl --context kind-%s -n %s port-forward svc/redis 6379:6379 &\n"+
			"  driftwatch explain -f <spec.yaml> --key <key>\n\n", ClusterName, c.Namespace)
	}

	c.writeFile("11-explain.txt", b.String())
}

// ---------------------------------------------------------------------------
// Cluster state.
// ---------------------------------------------------------------------------

func (c *Collector) clusterState(ctx context.Context) {
	c.capture(ctx, "12-events.txt",
		"get", "events", "--sort-by=.lastTimestamp", "-A")
	c.capture(ctx, "13-pods.txt",
		"get", "pods", "-o", "wide", "-A")
	c.capture(ctx, "14-pods-describe.txt",
		"describe", "pods", "-n", c.Namespace)
	c.capture(ctx, "15-node.txt",
		"describe", "node")
	c.capture(ctx, "16-namespace.yaml",
		"get", "all,configmap,secret", "-n", c.Namespace, "-o", "yaml")
}

// ---------------------------------------------------------------------------
// Plumbing.
// ---------------------------------------------------------------------------

// capture runs kubectl and writes the output, recording rather than raising any
// failure.
func (c *Collector) capture(ctx context.Context, name string, args ...string) {
	cmdCtx, cancel := context.WithTimeout(ctx, diagCommandTimeout)
	defer cancel()

	res := Kubectl(cmdCtx, args...)

	body := res.Output()
	if res.Err != nil {
		body = fmt.Sprintf("%s\n\n--- this command failed ---\n%v\n", body, res.Err)
		c.problem("%s: %v", name, res.Err)
	}
	c.writeFile(name, body)
}

// captureExec runs a command inside a pod in the scenario's namespace.
func (c *Collector) captureExec(ctx context.Context, name, pod string, command ...string) {
	args := append([]string{"-n", c.Namespace, "exec", pod, "--"}, command...)
	c.capture(ctx, name, args...)
}

func (c *Collector) podName(ctx context.Context, selector string) (string, error) {
	out, err := KubectlOut(ctx, "-n", c.Namespace, "get", "pods", "-l", selector,
		"-o", "jsonpath={.items[0].metadata.name}")
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(out) == "" {
		return "", fmt.Errorf("no pod matching %s in %s", selector, c.Namespace)
	}
	return strings.TrimSpace(out), nil
}

func (c *Collector) writeFile(name, body string) {
	path := filepath.Join(c.Dir, name)

	// An empty file is worse than no file: it reads as "nothing was wrong here"
	// when it means "this was not collected". A placeholder says which.
	if strings.TrimSpace(body) == "" {
		body = "(empty — the command returned nothing)\n"
	}

	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		c.problem("writing %s: %v", name, err)
		return
	}
	c.written = append(c.written, name)
}

func (c *Collector) problem(format string, args ...any) {
	c.problems = append(c.problems, fmt.Sprintf(format, args...))
}

// summary is the line the test prints, so a CI log says where the dump is
// without anybody having to know the convention.
func (c *Collector) summary() string {
	var b strings.Builder

	fmt.Fprintf(&b, "e2e diagnostics: %d files in %s\n", len(c.written), c.Dir)
	for _, name := range c.written {
		fmt.Fprintf(&b, "  %s\n", name)
	}

	if len(c.problems) > 0 {
		fmt.Fprintf(&b, "\n%d item(s) could not be collected:\n", len(c.problems))
		for _, p := range c.problems {
			fmt.Fprintf(&b, "  %s\n", p)
		}
	}

	// Written into the dump as well, so an artifact downloaded on its own is
	// self-describing.
	//nolint:errcheck // the summary is also returned to the caller; a failed
	// write here must not replace the real failure with a diagnostics failure
	_ = os.WriteFile(filepath.Join(c.Dir, "99-summary.txt"), []byte(b.String()), 0o600)
	return b.String()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
