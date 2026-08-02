package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Every scenario gets its own namespace with a generated name (§14.5), holding
// its own Redis, publisher, materializer and — where the scenario needs one —
// toxiproxy. Nothing is shared between scenarios except the cluster and the
// manager.
//
// That is more expensive than one shared fixture set. It is also the only way
// eight scenarios can run without interfering: E4 flushes Redis, E5 fills it
// until it evicts, E7 deletes the publisher pod. Any one of those against a
// shared fixture would fail the other seven, and the failure would land in
// whichever scenario happened to run next.

// Fixture is one scenario's namespace and everything in it.
type Fixture struct {
	Namespace string

	// UsesProxy reports whether driftwatch reaches the publisher through
	// toxiproxy. Only E3 needs it, and it costs an extra pod and an extra hop,
	// so the rest connect directly.
	UsesProxy bool

	// checks are the DriftChecks this scenario created, so cleanup and the
	// diagnostics collector both know what to look at.
	checks []string
}

// FixtureOptions configures the stack a scenario wants.
type FixtureOptions struct {
	// Rate is the publisher's events per second.
	Rate int
	// Keys is the size of the keyspace.
	Keys int
	// Publisher is the identity the publisher declares. Defaults to replica-0.
	Publisher string
	// SkipFrom and SkipTo make the materializer drop a sequence range, which is
	// how E2 produces divergence driftwatch can see and the store cannot.
	SkipFrom int
	SkipTo   int
	// RedisMaxMemory sets Redis's `maxmemory`, for E5. Empty leaves it
	// unbounded.
	RedisMaxMemory string
	// RedisEvictionPolicy sets `maxmemory-policy`. Empty means noeviction.
	RedisEvictionPolicy string
	// WithProxy puts toxiproxy between the publisher and driftwatch. E3 only.
	WithProxy bool
}

func (o *FixtureOptions) applyDefaults() {
	if o.Rate <= 0 {
		o.Rate = 400
	}
	if o.Keys <= 0 {
		o.Keys = 400
	}
	if o.Publisher == "" {
		o.Publisher = "replica-0"
	}
	if o.RedisEvictionPolicy == "" {
		o.RedisEvictionPolicy = "noeviction"
	}
}

// The endpoints inside a scenario's namespace, for pods that live in it.
//
// A bare service name resolves through the pod's own search domain, so these
// work for the materializer and the throwaway curl pod and nowhere else.
const (
	// PublisherDirect is what the materializer connects to.
	PublisherDirect = "tcp://publisher:5557"
	// ToxiproxyAPI is toxiproxy's control endpoint.
	ToxiproxyAPI = "http://toxiproxy:8474"
	// RedisLocal is the store, from a pod in the same namespace.
	RedisLocal = "redis:6379"
)

// The endpoints the *manager* uses, which have to be fully qualified.
//
// This cost the first full run of the suite. The operator lives in
// driftwatch-system and every scenario's fixtures live in their own generated
// namespace, so a DriftCheck saying `addr: redis:6379` sends the manager
// looking for a Service called `redis` in `driftwatch-system` — which does not
// exist. What comes back is not a clean refusal but a DNS timeout, so the check
// sat in Bootstrapping retrying a scan while its Redis was up and full three
// namespaces away.
//
// The same trap is waiting for any operator who writes a DriftCheck in their
// own namespace against a cluster-scoped manager, which is the deployment
// config/default ships. Fully qualifying is the fix in both cases.

// RedisAddr is the store as the manager must address it.
func (f *Fixture) RedisAddr() string {
	return fmt.Sprintf("redis.%s.svc.cluster.local:6379", f.Namespace)
}

// PublisherEndpoint is the publisher as the manager must address it.
func (f *Fixture) PublisherEndpoint() string {
	return fmt.Sprintf("tcp://publisher.%s.svc.cluster.local:5557", f.Namespace)
}

// ProxyEndpoint is the publisher via toxiproxy, as the manager must address it.
func (f *Fixture) ProxyEndpoint() string {
	return fmt.Sprintf("tcp://toxiproxy.%s.svc.cluster.local:5557", f.Namespace)
}

// NewFixture creates the namespace and brings the stack up.
//
// Every step here is setup, so every step may be retried (§14.5). Nothing in
// this file asserts anything about driftwatch's behavior — that is the
// scenarios' job, and retrying one of those would hide the bug it exists to
// find.
func NewFixture(ctx context.Context, name string, opts *FixtureOptions) (*Fixture, error) {
	opts.applyDefaults()

	namespace := generateNamespace(name)
	f := &Fixture{Namespace: namespace, UsesProxy: opts.WithProxy}

	if err := retry(ctx, 3, 2*time.Second, "create namespace "+namespace, func() error {
		_, err := KubectlOut(ctx, "create", "namespace", namespace)
		return err
	}); err != nil {
		return nil, err
	}

	manifest := f.render(opts)
	if err := retry(ctx, 3, 2*time.Second, "apply fixtures to "+namespace, func() error {
		return KubectlApply(ctx, namespace, manifest)
	}); err != nil {
		return nil, err
	}

	// Waiting on Available rather than on the pods, because a Deployment whose
	// pod is Running but whose readiness probe has not passed yet is a
	// publisher that is not listening — and the materializer would connect to
	// nothing and silently produce an empty Redis for the whole scenario.
	deployments := []string{"redis", "publisher", "materializer"}
	if opts.WithProxy {
		deployments = append(deployments, "toxiproxy")
	}

	for _, d := range deployments {
		if _, err := KubectlOut(ctx, "-n", namespace, "wait",
			"--for=condition=Available", "deployment/"+d, "--timeout=120s"); err != nil {
			return f, fmt.Errorf("waiting for %s in %s: %w", d, namespace, err)
		}
	}

	if opts.WithProxy {
		if err := f.configureProxy(ctx); err != nil {
			return f, err
		}
	}
	return f, nil
}

// generateNamespace derives a legal, unique namespace name.
//
// A DNS-1123 label: lower case, at most 63 characters, starting and ending with
// an alphanumeric. The suffix is what makes it unique, which matters more than
// it looks — a rerun against a reused cluster must not collide with the
// namespace the previous run left behind while its finalizers drain.
func generateNamespace(name string) string {
	var b strings.Builder
	b.WriteString("e2e-")

	for _, r := range strings.ToLower(name) {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'):
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}

	base := strings.Trim(b.String(), "-")
	if len(base) > 44 {
		base = strings.Trim(base[:44], "-")
	}
	return fmt.Sprintf("%s-%d", base, time.Now().UnixNano()%1_000_000)
}

// Checks returns the DriftChecks this scenario created.
func (f *Fixture) Checks() []string { return f.checks }

// ---------------------------------------------------------------------------
// The manifest.
// ---------------------------------------------------------------------------

func (f *Fixture) render(opts *FixtureOptions) string {
	var b strings.Builder

	b.WriteString(renderRedis(opts))
	b.WriteString(renderPublisher(opts))

	// driftwatch connects through the proxy when the scenario wants to be able
	// to cut it; the materializer always connects directly. That asymmetry is
	// the whole of E3: severing driftwatch's subscription while the store keeps
	// being written correctly is what produces suspect keys rather than drift.
	if opts.WithProxy {
		b.WriteString(renderToxiproxy())
	}

	b.WriteString(renderMaterializer(opts))
	return b.String()
}

func renderRedis(opts *FixtureOptions) string {
	args := []string{
		`"redis-server"`, `"--save"`, `""`, `"--appendonly"`, `"no"`,
		`"--maxmemory-policy"`, strconv.Quote(opts.RedisEvictionPolicy),
	}
	if opts.RedisMaxMemory != "" {
		args = append(args, `"--maxmemory"`, strconv.Quote(opts.RedisMaxMemory))
	}

	return fmt.Sprintf(`
apiVersion: v1
kind: Service
metadata:
  name: redis
spec:
  selector: { app: redis }
  ports: [{ port: 6379, targetPort: 6379 }]
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: redis
spec:
  replicas: 1
  selector: { matchLabels: { app: redis } }
  template:
    metadata:
      labels: { app: redis }
    spec:
      containers:
        - name: redis
          image: %s
          imagePullPolicy: IfNotPresent
          command: [%s]
          ports: [{ containerPort: 6379 }]
          readinessProbe:
            exec: { command: ["redis-cli", "PING"] }
            initialDelaySeconds: 1
            periodSeconds: 2
          resources:
            requests: { cpu: 50m, memory: 64Mi }
            limits: { cpu: "1", memory: 512Mi }
---
`, RedisImage, strings.Join(args, ", "))
}

func renderPublisher(opts *FixtureOptions) string {
	return fmt.Sprintf(`
apiVersion: v1
kind: Service
metadata:
  name: publisher
spec:
  selector: { app: publisher }
  ports:
    - { name: zmq, port: 5557, targetPort: 5557 }
    - { name: status, port: 8090, targetPort: 8090 }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: publisher
spec:
  replicas: 1
  selector: { matchLabels: { app: publisher } }
  template:
    metadata:
      labels: { app: publisher }
    spec:
      containers:
        - name: publisher
          image: %s
          imagePullPolicy: Never
          args:
            - publish
            - --bind=tcp://0.0.0.0:5557
            - --topic=kv-events
            - --rate=%d
            - --keys=%d
            - --publisher=%s
            - --status-addr=:8090
          ports:
            - { containerPort: 5557 }
            - { containerPort: 8090 }
          readinessProbe:
            httpGet: { path: /healthz, port: 8090 }
            initialDelaySeconds: 1
            periodSeconds: 2
          resources:
            requests: { cpu: 50m, memory: 64Mi }
            limits: { cpu: "1", memory: 256Mi }
---
`, HarnessImage, opts.Rate, opts.Keys, opts.Publisher)
}

func renderMaterializer(opts *FixtureOptions) string {
	skip := ""
	if opts.SkipFrom > 0 {
		skip = fmt.Sprintf(`
            - --skip-from=%d
            - --skip-to=%d`, opts.SkipFrom, opts.SkipTo)
	}

	return fmt.Sprintf(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: materializer
spec:
  replicas: 1
  selector: { matchLabels: { app: materializer } }
  template:
    metadata:
      labels: { app: materializer }
    spec:
      containers:
        - name: materializer
          image: %s
          imagePullPolicy: Never
          args:
            - materialize
            - --connect=%s
            - --topic=kv-events
            - --redis=%s
            - --status-addr=:8090%s
          ports: [{ containerPort: 8090 }]
          readinessProbe:
            httpGet: { path: /healthz, port: 8090 }
            initialDelaySeconds: 1
            periodSeconds: 2
          resources:
            requests: { cpu: 50m, memory: 64Mi }
            limits: { cpu: "1", memory: 256Mi }
---
`, HarnessImage, PublisherDirect, RedisLocal, skip)
}

func renderToxiproxy() string {
	return fmt.Sprintf(`
apiVersion: v1
kind: Service
metadata:
  name: toxiproxy
spec:
  selector: { app: toxiproxy }
  ports:
    - { name: proxy, port: 5557, targetPort: 5557 }
    - { name: api, port: 8474, targetPort: 8474 }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: toxiproxy
spec:
  replicas: 1
  selector: { matchLabels: { app: toxiproxy } }
  template:
    metadata:
      labels: { app: toxiproxy }
    spec:
      containers:
        - name: toxiproxy
          image: %s
          imagePullPolicy: IfNotPresent
          # -host 0.0.0.0 so the API is reachable from the test, which
          # configures the proxy after the pod is up rather than from a config
          # file — E3 needs to add and remove a toxic mid-scenario.
          args: ["-host", "0.0.0.0"]
          ports:
            - { containerPort: 5557 }
            - { containerPort: 8474 }
          readinessProbe:
            tcpSocket: { port: 8474 }
            initialDelaySeconds: 1
            periodSeconds: 2
          resources:
            requests: { cpu: 50m, memory: 32Mi }
            limits: { cpu: 500m, memory: 128Mi }
---
`, ToxiproxyImage)
}

// ---------------------------------------------------------------------------
// toxiproxy.
// ---------------------------------------------------------------------------

// configureProxy creates the publisher proxy.
//
// Done over the API after the pod is running rather than from a config file,
// because E3 has to be able to add and remove a toxic mid-scenario and the file
// form is read once at startup.
func (f *Fixture) configureProxy(ctx context.Context) error {
	body := `{"name":"publisher","listen":"0.0.0.0:5557","upstream":"publisher:5557","enabled":true}`

	return retry(ctx, 5, 2*time.Second, "create the toxiproxy proxy", func() error {
		out, err := f.curl(ctx, "POST", ToxiproxyAPI+"/proxies", body)
		if err != nil {
			return err
		}
		// 409 means it already exists, which a retry makes likely and which is
		// success as far as the scenario is concerned.
		if strings.Contains(out, "already exists") || strings.Contains(out, `"name":"publisher"`) {
			return nil
		}
		return fmt.Errorf("unexpected response creating the proxy: %s", out)
	})
}

// CutSubscription severs driftwatch's link to the publisher.
//
// The publisher keeps emitting and the materializer keeps writing throughout,
// so the store stays correct while driftwatch's view of the stream develops a
// hole. That asymmetry is the whole point of E3.
//
// The timeout is 1ms rather than 0, and the difference is the difference
// between this scenario working and silently proving nothing.
//
// toxiproxy's `timeout` toxic with `timeout: 0` does not partition anything:
// its documented behavior is that "the connection won't close, and data will
// be delayed until the toxic is removed." So the frames driftwatch would have
// missed are held by the proxy and delivered in full the moment the toxic is
// deleted. driftwatch loses nothing, has nothing to be suspect about, and E3
// asserts on a fault that never happened.
//
// A non-zero timeout closes the connection instead. ZMQ PUB/SUB has no replay,
// so everything published while the socket is down is gone — which is the
// actual fault this scenario is about. 1ms rather than something larger so that
// each reconnection attempt is cut promptly too, keeping the partition closed
// for as long as the toxic is present.
func (f *Fixture) CutSubscription(ctx context.Context) error {
	body := `{"name":"cut","type":"timeout","stream":"downstream","toxicity":1.0,"attributes":{"timeout":1}}`

	_, err := f.curl(ctx, "POST", ToxiproxyAPI+"/proxies/publisher/toxics", body)
	return err
}

// RestoreSubscription removes the partition.
func (f *Fixture) RestoreSubscription(ctx context.Context) error {
	_, err := f.curl(ctx, "DELETE", ToxiproxyAPI+"/proxies/publisher/toxics/cut", "")
	return err
}

// curl runs an HTTP request from inside the namespace.
//
// From a throwaway pod rather than from the test process: toxiproxy's API is a
// ClusterIP service with no ingress, and a port-forward would be a subprocess
// to manage, reap, and race against.
func (f *Fixture) curl(ctx context.Context, method, url, body string) (string, error) {
	args := []string{
		"-n", f.Namespace, "run",
		fmt.Sprintf("curl-%d", time.Now().UnixNano()%1_000_000),
		"--image=curlimages/curl:8.11.1", "--restart=Never", "--rm", "-i",
		"--quiet", "--command", "--",
		"curl", "-sS", "-X", method, url,
	}
	if body != "" {
		args = append(args, "-H", "Content-Type: application/json", "-d", body)
	}

	res := Kubectl(ctx, args...)
	if res.Err != nil {
		return res.Output(), fmt.Errorf("%s %s: %w\n%s", method, url, res.Err, res)
	}
	return res.Output(), nil
}

// ---------------------------------------------------------------------------
// Redis.
// ---------------------------------------------------------------------------

// RedisCommand runs redis-cli inside the scenario's Redis pod.
func (f *Fixture) RedisCommand(ctx context.Context, args ...string) (string, error) {
	pod, err := f.PodName(ctx, "app=redis")
	if err != nil {
		return "", err
	}

	full := append([]string{"-n", f.Namespace, "exec", pod, "--", "redis-cli"}, args...)
	out, err := KubectlOut(ctx, full...)
	return strings.TrimSpace(out), err
}

// RedisDBSize returns how many keys the store holds.
func (f *Fixture) RedisDBSize(ctx context.Context) (int, error) {
	out, err := f.RedisCommand(ctx, "DBSIZE")
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(strings.TrimSpace(out))
}

// ---------------------------------------------------------------------------
// The publisher.
// ---------------------------------------------------------------------------

// PublisherStats reads the publisher's /stats.
//
// A scenario waits on `emitted` before asserting anything, because a scenario
// that began against a stream with nothing in it would fail on an empty oracle
// and look like a detection bug.
func (f *Fixture) PublisherStats(ctx context.Context) (map[string]any, error) {
	pod, err := f.PodName(ctx, "app=publisher")
	if err != nil {
		return nil, err
	}

	out, err := KubectlOut(ctx, "get", "--raw",
		fmt.Sprintf("/api/v1/namespaces/%s/pods/%s:8090/proxy/stats", f.Namespace, pod))
	if err != nil {
		return nil, err
	}

	var stats map[string]any
	if err := json.Unmarshal([]byte(out), &stats); err != nil {
		return nil, fmt.Errorf("parsing publisher stats %q: %w", out, err)
	}
	return stats, nil
}

// PublisherEmitted returns how many events the publisher has sent.
func (f *Fixture) PublisherEmitted(ctx context.Context) (int, error) {
	stats, err := f.PublisherStats(ctx)
	if err != nil {
		return 0, err
	}
	// A missing or wrongly-typed field reads as zero rather than as an error.
	// The publisher is a test harness, and a scenario that failed on a stats
	// shape would report a harness problem as a driftwatch problem.
	n, ok := stats["emitted"].(float64)
	if !ok {
		return 0, nil
	}
	return int(n), nil
}

// PublisherEpoch returns the incarnation the publisher currently declares.
func (f *Fixture) PublisherEpoch(ctx context.Context) (int64, error) {
	stats, err := f.PublisherStats(ctx)
	if err != nil {
		return 0, err
	}
	e, ok := stats["epoch"].(float64)
	if !ok {
		return 0, nil
	}
	return int64(e), nil
}

// RestartPublisher deletes the publisher pod so the Deployment reschedules it.
//
// E7's fault. The replacement comes back with a sequence number that restarts
// at 1 and a higher epoch, which driftwatch has to read as a restart rather
// than as several hundred thousand missing events.
func (f *Fixture) RestartPublisher(ctx context.Context) error {
	pod, err := f.PodName(ctx, "app=publisher")
	if err != nil {
		return err
	}

	if _, delErr := KubectlOut(ctx, "-n", f.Namespace, "delete", "pod", pod,
		"--grace-period=0", "--force"); delErr != nil {
		return delErr
	}

	_, err = KubectlOut(ctx, "-n", f.Namespace, "wait",
		"--for=condition=Available", "deployment/publisher", "--timeout=120s")
	return err
}

// ---------------------------------------------------------------------------
// Pods.
// ---------------------------------------------------------------------------

// PodName returns the first running pod matching a selector.
func (f *Fixture) PodName(ctx context.Context, selector string) (string, error) {
	out, err := KubectlOut(ctx, "-n", f.Namespace, "get", "pods",
		"-l", selector, "--field-selector=status.phase=Running",
		"-o", "jsonpath={.items[0].metadata.name}")
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(out) == "" {
		return "", fmt.Errorf("no running pod matching %s in %s", selector, f.Namespace)
	}
	return strings.TrimSpace(out), nil
}

// ScaleMaterializer stops or starts the materializer.
//
// E4 uses it to hold the store still while a flush is observed: with the
// materializer running, a flushed keyspace refills within seconds and the drift
// resolves before it can be confirmed.
func (f *Fixture) ScaleMaterializer(ctx context.Context, replicas int) error {
	if _, err := KubectlOut(ctx, "-n", f.Namespace, "scale",
		"deployment/materializer", fmt.Sprintf("--replicas=%d", replicas)); err != nil {
		return err
	}

	if replicas == 0 {
		_, err := KubectlOut(ctx, "-n", f.Namespace, "wait", "--for=delete",
			"pod", "-l", "app=materializer", "--timeout=60s")
		return err
	}
	_, err := KubectlOut(ctx, "-n", f.Namespace, "wait",
		"--for=condition=Available", "deployment/materializer", "--timeout=120s")
	return err
}
