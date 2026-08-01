package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// The DriftCheck side of a fixture: applying one, reading its status, and
// taking it away again without racing the finalizer.

// CheckOptions configures the DriftCheck a scenario applies.
type CheckOptions struct {
	// Name defaults to "check".
	Name string
	// KeyPattern is the keyspace this check owns. Defaults to "block:*", which
	// is what the harness publisher produces.
	KeyPattern string
	// Projection defaults to keysetOwnership.
	Projection string
	// SettlementWindow defaults to 3s.
	//
	// Static rather than adaptive throughout the suite. Adaptive is right in
	// production — it tracks a materializer whose latency moves — but it starts
	// at the floor and widens as evidence arrives, so a ninety-second scenario
	// would spend most of its time watching the window climb rather than
	// watching the property under test.
	SettlementWindow time.Duration
	// SweepInterval defaults to 5s, against a production default of 30s. Eight
	// scenarios share an eight-minute budget; a 30s sweep would spend it all
	// waiting.
	SweepInterval time.Duration
	// ExtraScanInterval defaults to 20s.
	ExtraScanInterval time.Duration
	// Paused starts the check paused.
	Paused bool
	// MaxTrackedKeys defaults to 100,000.
	MaxTrackedKeys int
	// ViaProxy points the check at toxiproxy rather than straight at the
	// publisher, so the scenario can sever its subscription.
	ViaProxy bool
}

func (o *CheckOptions) applyDefaults() {
	if o.Name == "" {
		o.Name = "check"
	}
	if o.KeyPattern == "" {
		o.KeyPattern = "block:*"
	}
	if o.Projection == "" {
		o.Projection = "keysetOwnership"
	}
	if o.SettlementWindow <= 0 {
		o.SettlementWindow = 3 * time.Second
	}
	if o.SweepInterval <= 0 {
		o.SweepInterval = 5 * time.Second
	}
	if o.ExtraScanInterval <= 0 {
		o.ExtraScanInterval = 20 * time.Second
	}
	if o.MaxTrackedKeys <= 0 {
		o.MaxTrackedKeys = 100_000
	}
}

// checkTemplate is the DriftCheck the scenarios apply.
//
// Spelled out rather than defaulted, because a scenario that failed would
// otherwise leave the reader guessing which policy was in force — and the
// policy is usually the answer.
const checkTemplate = `
apiVersion: driftwatch.io/v1alpha1
kind: DriftCheck
metadata:
  name: %s
  namespace: %s
spec:
  source:
    type: zmq
    zmq:
      endpoints: ["%s"]
      topics: ["kv-events"]
      recvHWM: 100000
    ingestBufferSize: 200000
  codec:
    type: json
  projection:
    type: %s
    keyTemplate: "{{.Key}}"
    memberTemplate: "{{.Member}}"
  target:
    type: redis
    redis:
      addr: %s
      keyPattern: "%s"
      readBatchSize: 500
      scanCount: 1000
  policy:
    settlementWindow:
      mode: static
      static: %s
      min: 1s
      max: 30s
    sweepInterval: %s
    extraScanInterval: %s
    bootstrap: Adopt
    expiryPolicy: Strict
    requirePrimary: false
    maxTrackedKeys: %d
    paused: %t
`

// CreateCheck applies a DriftCheck into the scenario's namespace.
func (f *Fixture) CreateCheck(ctx context.Context, opts CheckOptions) (string, error) {
	opts.applyDefaults()

	// Fully qualified, always. The manager lives in another namespace and a
	// bare service name would resolve against its own — see the comment on
	// Fixture.RedisAddr.
	endpoint := f.PublisherEndpoint()
	if opts.ViaProxy {
		endpoint = f.ProxyEndpoint()
	}

	manifest := fmt.Sprintf(checkTemplate,
		opts.Name, f.Namespace, endpoint, opts.Projection, f.RedisAddr(), opts.KeyPattern,
		opts.SettlementWindow, opts.SweepInterval, opts.ExtraScanInterval,
		opts.MaxTrackedKeys, opts.Paused)

	if err := KubectlApply(ctx, f.Namespace, manifest); err != nil {
		return "", err
	}

	f.checks = append(f.checks, opts.Name)
	return opts.Name, nil
}

// PatchCheck applies a merge patch to a DriftCheck.
func (f *Fixture) PatchCheck(ctx context.Context, name, patch string) error {
	_, err := KubectlOut(ctx, "-n", f.Namespace, "patch", "driftcheck", name,
		"--type=merge", "-p", patch)
	return err
}

// Condition is one status condition.
type Condition struct {
	Type    string `json:"type"`
	Status  string `json:"status"`
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

// PublisherState is one publisher's sequence position, as the status reports it.
type PublisherState struct {
	ID            string `json:"id"`
	Epoch         int64  `json:"epoch"`
	HighWaterMark int64  `json:"highWaterMark"`
	MissingEvents int64  `json:"missingEvents"`
	Restarts      int64  `json:"restarts"`
}

// CheckStatus is the DriftCheck status the scenarios assert on.
type CheckStatus struct {
	Phase                string           `json:"phase"`
	ObservedGeneration   int64            `json:"observedGeneration"`
	Message              string           `json:"message"`
	DivergentKeys        int              `json:"divergentKeys"`
	SuspectDivergentKeys int              `json:"suspectDivergentKeys"`
	DivergenceByCategory map[string]int   `json:"divergenceByCategory"`
	TrackedKeys          int              `json:"trackedKeys"`
	SettledKeys          int              `json:"settledKeys"`
	InFlightKeys         int              `json:"inFlightKeys"`
	CoverageRatio        string           `json:"coverageRatio"`
	TargetReachable      bool             `json:"targetReachable"`
	TargetKeyspaceSize   int64            `json:"targetKeyspaceSize"`
	EventsApplied        int64            `json:"eventsApplied"`
	EventsDropped        int64            `json:"eventsDropped"`
	Publishers           []PublisherState `json:"publishers"`
	Conditions           []Condition      `json:"conditions"`
}

// Condition returns one condition by type, or nil.
func (s *CheckStatus) Condition(kind string) *Condition {
	for i := range s.Conditions {
		if s.Conditions[i].Type == kind {
			return &s.Conditions[i]
		}
	}
	return nil
}

// ConditionIs reports whether a condition currently has a given status.
func (s *CheckStatus) ConditionIs(kind, status string) bool {
	c := s.Condition(kind)
	return c != nil && c.Status == status
}

// TotalMissingEvents sums the sequence numbers unaccounted for.
func (s *CheckStatus) TotalMissingEvents() int64 {
	var total int64
	for i := range s.Publishers {
		total += s.Publishers[i].MissingEvents
	}
	return total
}

// Summary renders the status in one line, for a failure message.
//
// Gomega prints the value an Eventually gave up on, and a raw struct dump of
// this is forty lines of mostly zeroes. This is the six numbers that say what
// the check was actually doing.
func (s *CheckStatus) Summary() string {
	return fmt.Sprintf(
		"phase=%s drift=%d suspect=%d tracked=%d settled=%d coverage=%s "+
			"targetKeys=%d applied=%d",
		s.Phase, s.DivergentKeys, s.SuspectDivergentKeys, s.TrackedKeys,
		s.SettledKeys, s.CoverageRatio, s.TargetKeyspaceSize, s.EventsApplied)
}

// Status reads a DriftCheck's status.
func (f *Fixture) Status(ctx context.Context, name string) (*CheckStatus, error) {
	out, err := KubectlOut(ctx, "-n", f.Namespace, "get", "driftcheck", name,
		"-o", "jsonpath={.status}")
	if err != nil {
		return nil, err
	}

	trimmed := strings.TrimSpace(out)
	if trimmed == "" {
		// The controller has not written status yet. An empty struct rather
		// than an error: a scenario polling for a phase should keep polling
		// rather than fail on the first read.
		return &CheckStatus{}, nil
	}

	var status CheckStatus
	if err := json.Unmarshal([]byte(trimmed), &status); err != nil {
		return nil, fmt.Errorf("parsing status of %s: %w\n%s", name, err, trimmed)
	}
	return &status, nil
}

// Events returns the Kubernetes Events for a DriftCheck, oldest first.
func (f *Fixture) Events(ctx context.Context, name string) (string, error) {
	return KubectlOut(ctx, "-n", f.Namespace, "get", "events",
		"--field-selector", "involvedObject.name="+name,
		"--sort-by=.lastTimestamp",
		"-o", `jsonpath={range .items[*]}{.type}/{.reason}: {.message}{"\n"}{end}`)
}

// DeleteCheck removes a DriftCheck and confirms the finalizer cleared.
//
// §14.5 asks for this rather than a bare delete, and the finalizer is why: the
// controller stops the runner before releasing the object, so a cleanup that
// did not wait would let the next scenario's namespace teardown race a check
// still sweeping — which surfaces as an unrelated scenario failing against a
// Redis that has gone.
func (f *Fixture) DeleteCheck(ctx context.Context, name string) error {
	res := Kubectl(ctx, "-n", f.Namespace, "delete", "driftcheck", name,
		"--ignore-not-found", "--timeout=90s")
	if res.Err != nil {
		return fmt.Errorf("deleting driftcheck %s: %w\n%s", name, res.Err, res)
	}

	// kubectl delete already blocks until the object is gone, which cannot
	// happen while a finalizer is on it. This confirms rather than assumes,
	// because the interesting failure — a finalizer that never clears — ends in
	// a timeout whose message is easy to lose.
	out, err := KubectlOut(ctx, "-n", f.Namespace, "get", "driftcheck", name,
		"--ignore-not-found", "-o", "name")
	if err != nil {
		return err
	}
	if strings.TrimSpace(out) != "" {
		return fmt.Errorf(
			"driftcheck %s still exists after delete: the finalizer %s did not clear",
			name, "driftwatch.io/cleanup")
	}
	return nil
}

// Teardown removes every check and then the namespace.
func (f *Fixture) Teardown(ctx context.Context) error {
	var problems []string

	for _, name := range f.checks {
		if err := f.DeleteCheck(ctx, name); err != nil {
			problems = append(problems, err.Error())
		}
	}

	// The namespace goes without waiting. Kubernetes reclaims it in the
	// background, and a suite that waited would spend up to thirty seconds per
	// scenario watching an empty namespace disappear — four minutes of an
	// eight-minute budget, for nothing.
	if res := Kubectl(ctx, "delete", "namespace", f.Namespace,
		"--ignore-not-found", "--wait=false"); res.Err != nil {
		problems = append(problems, res.Err.Error())
	}

	if len(problems) > 0 {
		return fmt.Errorf("tearing down %s: %s", f.Namespace, strings.Join(problems, "; "))
	}
	return nil
}
