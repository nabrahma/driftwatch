// Package check assembles a complete, running audit from a spec (M14).
//
// This is the composition root: source to codec to sequence tracking to
// projection to oracle to sweeper to reporter, with the metrics and the logging
// wired through. Every other package in driftwatch is deliberately ignorant of
// the ones around it, and this is the one place that knows the shape of the
// whole thing.
//
// Two rules govern how it is built, both from §9 M14 and both learned the hard
// way in systems that did the opposite:
//
// The whole spec is validated before anything is constructed. A check that
// fails halfway up has already opened a socket and started a goroutine, and
// unwinding that correctly from an error path is the kind of code nobody tests
// until it matters. Validate first, construct second, and construction cannot
// fail for a reason the operator could have been told about earlier.
//
// A source that cannot connect is not a construction failure. The endpoint may
// come up in thirty seconds; refusing to start means an operator's rollout
// fails because a dependency was slow, which teaches them to remove the check
// rather than fix the dependency. It starts, reports Degraded, and says so.
package check

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/sync/errgroup"

	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/codec"
	"github.com/nabrahma/driftwatch/pkg/differ"
	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/explain"
	"github.com/nabrahma/driftwatch/pkg/lag"
	"github.com/nabrahma/driftwatch/pkg/logging"
	"github.com/nabrahma/driftwatch/pkg/metrics"
	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/seqtrack"
	"github.com/nabrahma/driftwatch/pkg/source"
	"github.com/nabrahma/driftwatch/pkg/sweeper"
	"github.com/nabrahma/driftwatch/pkg/target"
)

// Phase is the lifecycle state reported in status (§10.1).
type Phase string

// The phases. Degraded is the interesting one: it means the check is running
// and honest about not being able to see everything, which is a different thing
// from Failed and must not page anyone the same way.
const (
	PhasePending       Phase = "Pending"
	PhaseBootstrapping Phase = "Bootstrapping"
	PhaseWatching      Phase = "Watching"
	PhaseDegraded      Phase = "Degraded"
	PhasePaused        Phase = "Paused"
	PhaseFailed        Phase = "Failed"
)

// Deps are the injected dependencies a check cannot build for itself.
type Deps struct {
	// Clock is the injected clock. Defaults to the real one.
	Clock clock.Clock
	// Logger receives every line. The zero value is a working no-op.
	Logger logr.Logger
	// Metrics is the process-wide metric set. Nil means metrics are not
	// recorded, which is what most tests want.
	Metrics *metrics.Metrics
}

// Check is one running audit.
type Check struct {
	spec Spec
	clk  clock.Clock
	log  logr.Logger
	m    *metrics.CheckMetrics

	src     source.Source
	cdc     codec.Codec
	proj    projection.Projection
	tgt     target.Target
	orc     *oracle.Oracle
	tracker *seqtrack.Tracker
	swp     *sweeper.Sweeper
	est     *lag.Estimator

	// raw carries frames from the source to the applier. It is sized from
	// policy.ingestBufferSize, and that size is validated against the socket's
	// high-water mark: loss has to happen here, where it is counted, rather
	// than inside the transport where it is invisible.
	raw chan source.RawMessage

	// sampler bounds repetitive error logging. A malformed stream arrives at
	// the full event rate, and a log line per event turns one publisher's bad
	// deploy into a disk-full outage (§12.3).
	sampler *logging.Sampler

	// bootstrapped closes when the oracle is ready to be swept against.
	bootstrapped chan struct{}

	// closers are released by Close in reverse order, including after a failed
	// New. Partial construction has to clean up after itself or a rejected spec
	// leaks a connection pool.
	closers []io.Closer

	mu         sync.RWMutex
	phase      Phase
	message    string
	lastReport *differ.Report
	lastSweep  time.Time
	closed     bool

	// confirmedCats and lastResolved turn the sweeper's confirmed set into the
	// two episode counters. See publishEpisodes.
	confirmedCats map[string]differ.Category
	lastResolved  int64

	// saturated is sticky: an oracle that could not hold the whole keyspace
	// stays degraded for the life of the check, because every report it
	// produces afterwards covers only part of the store. It is not something a
	// later clean sweep can clear.
	saturated  atomic.Bool
	saturation string

	// sourceFailed is sticky for the same reason, and for a sharper one: a
	// source that has stopped delivering makes every subsequent sweep look
	// perfectly clean, because the oracle stops changing and the target stops
	// being written to. That is the most dangerous reading this tool can
	// produce, so it must not be reported as Watching.
	sourceFailed  atomic.Bool
	sourceFailure string

	// Counters read by Status. Atomic because Status is called from the CRD
	// controller and the CLI's status line while the applier is running.
	eventsApplied atomic.Uint64
	eventsDropped atomic.Uint64
	bytesRead     atomic.Uint64
	gapSignals    atomic.Uint64
	decodeErrors  atomic.Uint64
}

// Sentinel errors.
var (
	// ErrClosed reports use of a check after Close.
	ErrClosed = errors.New("check is closed")
	// ErrNotBootstrapped reports a sweep requested before the oracle is ready.
	ErrNotBootstrapped = errors.New("check has not finished bootstrapping")
)

// samplerBurst and samplerInterval implement §12.3's rate limit: the first ten
// of a given reason, then one every ten seconds.
const (
	samplerBurst    = 10
	samplerInterval = 10 * time.Second
)

// New assembles a check from a spec.
//
// The spec is validated in full before anything is built, so a configuration
// error names the offending field and leaves nothing behind. If construction
// does fail partway, Close on the returned check releases whatever was already
// built — which is why it returns a non-nil check alongside some errors.
// the copy rather than the caller's struct, and New runs once per check.
//
//nolint:gocritic // hugeParam: a spec is passed by value so ApplyDefaults mutates
func New(spec Spec, deps Deps) (*Check, error) {
	spec.ApplyDefaults()

	if err := spec.Validate(); err != nil {
		return nil, err
	}

	clk := deps.Clock
	if clk == nil {
		clk = clock.Real()
	}

	c := &Check{
		spec:          spec,
		clk:           clk,
		log:           deps.Logger.WithValues("check", spec.ID()),
		bootstrapped:  make(chan struct{}),
		phase:         PhasePending,
		sampler:       logging.NewSampler(clk, samplerBurst, samplerInterval, 64),
		raw:           make(chan source.RawMessage, spec.Source.IngestBufferSize),
		confirmedCats: map[string]differ.Category{},
	}
	if deps.Metrics != nil {
		c.m = deps.Metrics.ForCheck(spec.ID())
	}

	if err := c.build(); err != nil {
		// Close, not leak. Everything built before the failure is released
		// here rather than left for a caller who has no handle on it.
		_ = c.Close() //nolint:errcheck // the construction error is the one worth returning
		return nil, err
	}

	c.logEffectiveConfig()
	return c, nil
}

// build constructs the pipeline in dependency order.
func (c *Check) build() error {
	var err error

	if c.cdc, err = codec.New(c.spec.Codec.Type, c.spec.CodecConfig()); err != nil {
		return fmt.Errorf("%w: codec: %w", ErrInvalidSpec, err)
	}
	if c.proj, err = projection.New(c.spec.Projection.Type, c.spec.ProjectionConfig()); err != nil {
		return fmt.Errorf("%w: projection: %w", ErrInvalidSpec, err)
	}

	c.tgt, err = target.New(c.spec.Target.Type, target.Config{
		Settings: c.spec.TargetSettings(),
		Clock:    c.clk,
	})
	if err != nil {
		return fmt.Errorf("%w: target: %w", ErrInvalidSpec, err)
	}
	c.closers = append(c.closers, c.tgt)

	// The source is the one component allowed to fail at connect time without
	// failing construction; see the package comment. Only a configuration error
	// stops us here.
	c.src, err = source.New(c.spec.Source.Type, c.spec.SourceConfig(), c.clk)
	if err != nil {
		return fmt.Errorf("%w: source: %w", ErrInvalidSpec, err)
	}
	c.closers = append(c.closers, c.src)

	c.tracker = seqtrack.New(seqtrack.Config{
		MaxPublishers: c.spec.Policy.MaxPublishers,
		Clock:         c.clk,
	})

	c.orc = oracle.New(oracle.Config{
		Shards:              c.spec.Policy.OracleShards,
		MaxTrackedKeys:      c.spec.Policy.MaxTrackedKeys,
		RingSize:            c.spec.Policy.RingSize,
		RetainRaw:           c.spec.Codec.RetainRaw,
		SettlementWindow:    c.spec.EffectiveWindow(),
		NeverSettledFactor:  c.spec.Policy.NeverSettledThreshold,
		MaxSettlementWindow: c.spec.Policy.SettlementWindow.Max.Duration(),
		Clock:               c.clk,
	})

	c.buildLag()
	c.buildSweeper()
	return nil
}

func (c *Check) buildLag() {
	w := &c.spec.Policy.SettlementWindow

	cfg := lag.Config{
		Oracle:       c.orc,
		Target:       c.tgt,
		Shape:        c.proj.TargetShape(),
		Clock:        c.clk,
		MinWindow:    w.Min.Duration(),
		MaxWindow:    w.Max.Duration(),
		SafetyFactor: w.SafetyFactor.Float(),
		OnWindowChange: func(old, next time.Duration, s lag.Stats) {
			// §9 M11 requires every change to be visible. A window that moved
			// without saying so turns "why did this alert fire" into an
			// unanswerable question.
			c.log.Info("settlement window changed",
				"from", old, "to", next, "p99", s.P99,
				"observations", s.Observations, "clamped", s.Clamped)
			c.orc.SetSettlementWindow(next)
			if c.m != nil {
				c.m.SetSettlementWindow(next)
			}
		},
	}

	// The distribution is measured either way. Knowing how far a static window
	// is from the truth is useful even when it is not driving anything, and it
	// is what tells an operator their configured 5s should have been 30s.
	if w.Mode == WindowStatic {
		static := w.Static.Duration()
		cfg.Static = &static
	}

	c.est = lag.New(cfg)
}

func (c *Check) buildSweeper() {
	c.swp = sweeper.New(sweeper.Config{
		Oracle:            c.orc,
		Target:            c.tgt,
		Shape:             c.proj.TargetShape(),
		Clock:             c.clk,
		DifferOptions:     c.spec.DifferOptions(),
		SweepInterval:     c.spec.Policy.SweepInterval.Duration(),
		ExtraScanInterval: c.spec.Policy.ExtraScanInterval.Duration(),
		ExtraScanPattern:  c.spec.ExtraScanPattern(),
		SettlementWindow:  c.orc.SettlementWindow,
		ReadBatchSize:     c.readBatchSize(),
		MaxConfirmQueue:   c.spec.Policy.MaxConfirmQueue,
		MaxExtrasTracked:  c.spec.Policy.MaxExtrasTracked,
		RequirePrimary:    c.spec.Policy.RequirePrimary,
		OnReport:          c.onReport,
	})
	c.closers = append(c.closers, c.swp)
}

func (c *Check) readBatchSize() int {
	if c.spec.Target.Redis != nil {
		return c.spec.Target.Redis.ReadBatchSize
	}
	return DefaultReadBatchSize
}

// logEffectiveConfig writes the running configuration once, with secrets
// replaced.
//
// §12.3 calls this out specifically, and it earns its place: half of all
// "driftwatch is reporting nonsense" reports are a keyTemplate or a keyPattern
// that does not match what the operator thought they configured, and this one
// line answers that without a round trip.
func (c *Check) logEffectiveConfig() {
	c.log.Info("check configured",
		"source", c.spec.Source.Type,
		"codec", c.spec.Codec.Type,
		"projection", c.spec.Projection.Type,
		"target", c.spec.Target.Type,
		"settlementWindow", c.spec.EffectiveWindow(),
		"sweepInterval", c.spec.Policy.SweepInterval,
		"bootstrap", c.spec.Policy.Bootstrap,
		"effectiveConfig", c.spec.YAML())

	for _, w := range c.spec.Warnings() {
		c.log.Info("configuration warning", "warning", w)
	}
}

// ---------------------------------------------------------------------------
// Accessors.
// ---------------------------------------------------------------------------

// Spec returns the effective configuration, with defaults applied.
func (c *Check) Spec() Spec { return c.spec }

// Source returns the event transport. Exposed so a test or `driftwatch replay`
// can drive it directly.
func (c *Check) Source() source.Source { return c.src }

// Target returns the audited store.
func (c *Check) Target() target.Target { return c.tgt }

// Oracle returns the expectation store, which `driftwatch replay --dump-oracle`
// serializes.
func (c *Check) Oracle() *oracle.Oracle { return c.orc }

// Sweeper returns the sweeper.
func (c *Check) Sweeper() *sweeper.Sweeper { return c.swp }

// Bootstrapped returns a channel closed once the oracle is ready to sweep.
func (c *Check) Bootstrapped() <-chan struct{} { return c.bootstrapped }

// EventsApplied returns how many events reached the oracle.
func (c *Check) EventsApplied() uint64 { return c.eventsApplied.Load() }

// ---------------------------------------------------------------------------
// Run.
// ---------------------------------------------------------------------------

// Run blocks until ctx is done or a fatal error occurs.
//
// Every goroutine runs under one errgroup, so the first fatal error cancels the
// rest and Run returns only once all of them have exited. A check that returned
// while its applier was still writing to the oracle would be a use-after-close
// waiting to happen in the operator, which restarts checks on every spec change.
func (c *Check) Run(ctx context.Context) error {
	c.mu.RLock()
	closed := c.closed
	c.mu.RUnlock()
	if closed {
		return ErrClosed
	}

	c.setPhase(PhaseBootstrapping, "starting")

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error { return c.runSource(ctx) })
	g.Go(func() error { return c.guard(ctx, metrics.ComponentIngest, c.ingest) })
	g.Go(func() error { return c.guard(ctx, metrics.ComponentSource, c.watchGaps) })
	g.Go(func() error { return c.guard(ctx, metrics.ComponentLag, c.est.Run) })
	g.Go(func() error { return c.guard(ctx, metrics.ComponentSweeper, c.runSweeper) })
	g.Go(func() error { return c.refreshMetrics(ctx) })

	// A deadline is as clean a shutdown as a cancellation: `--timeout` and
	// `--once` both end the run deliberately, and reporting Failed for them
	// would put a red phase in the status of every successful CI gate.
	err := g.Wait()
	if err != nil && !errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded) {
		c.setPhase(PhaseFailed, err.Error())
		return err
	}

	c.setPhase(PhasePending, "stopped")
	return nil
}

// runSource drives the transport, tolerating a failure to connect.
//
// A source that cannot reach its endpoint puts the check into Degraded rather
// than killing it: the endpoint may come up, and a check that refuses to start
// because a dependency was slow is a check operators remove.
func (c *Check) runSource(ctx context.Context) error {
	err := c.src.Run(ctx, c.raw)

	switch {
	case err == nil,
		errors.Is(err, context.Canceled),
		errors.Is(err, context.DeadlineExceeded):
		return nil
	default:
		message := "source: " + err.Error()

		c.mu.Lock()
		c.sourceFailure = message
		c.mu.Unlock()
		c.sourceFailed.Store(true)

		c.log.Error(err, "the source stopped; the check keeps running and reports "+
			"degraded, because sweeps against a stalled stream look clean")
		c.setPhase(PhaseDegraded, message)

		<-ctx.Done()
		return nil
	}
}

// guard runs fn with a panic recovery that names the component.
//
// §19.2 and §12's panics_total exist because a panic in one goroutine takes the
// whole process down, and in a multi-check operator that means every other
// check stops too. Recovering, counting and failing this one check is the
// containment.
func (c *Check) guard(
	ctx context.Context, component metrics.Component, fn func(context.Context) error,
) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if c.m != nil {
				c.m.Panic(component)
			}
			err = fmt.Errorf("panic in %s: %v\n%s", component, r, debug.Stack())
			c.log.Error(err, "recovered a panic", "component", component)
		}
	}()

	err = fn(ctx)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// ingest is the applier: the single goroutine that writes to the oracle.
//
// Single by design. The oracle's version bumping and settlement index are
// correct without a global lock precisely because exactly one goroutine writes,
// and that discipline is enforced here by there being one of these.
func (c *Check) ingest(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-c.raw:
			if !ok {
				return nil
			}
			c.applyMessage(&msg)
		}
	}
}

func (c *Check) applyMessage(msg *source.RawMessage) {
	c.bytesRead.Add(uint64(len(msg.Payload)))
	if c.m != nil {
		c.m.AddBytesReceived(int64(len(msg.Payload)))
		c.m.SetQueueDepth(metrics.StageRaw, len(c.raw))
	}

	var e event.Event
	e.ObservedAt = msg.ObservedAt
	if e.ObservedAt.IsZero() {
		e.ObservedAt = c.clk.Now()
	}

	if err := c.cdc.Decode(msg.Payload, msg.Topic, &e); err != nil {
		c.onDecodeError(err)
		return
	}
	if err := e.Validate(); err != nil {
		c.drop("", metrics.DropValidationError, "invalid event", err)
		return
	}

	verdict, _ := c.tracker.Observe(&e)
	switch verdict {
	case seqtrack.DropDuplicate:
		c.drop(e.Publisher, metrics.DropDuplicate, "", nil)
		return
	case seqtrack.DropStaleEpoch:
		c.drop(e.Publisher, metrics.DropStaleEpoch, "", nil)
		return
	case seqtrack.AcceptWithGap:
		c.onGap(&e)
	case seqtrack.AcceptAfterRestart:
		c.onRestart(&e)
	case seqtrack.Accept, seqtrack.AcceptLateFill, seqtrack.AcceptFirstSeen:
	}

	c.fold(&e, verdict)
}

// fold applies one event through the projection into the oracle.
func (c *Check) fold(e *event.Event, verdict seqtrack.Verdict) {
	if c.m != nil {
		c.m.EventReceived(e.Publisher, metrics.Op(e.Op.String()))
	}

	// The store key is the projection's key template applied to the event, not
	// the event's own Key field. Looking up the raw key would miss on every
	// keyTemplate that rewrites it, so every event would fold onto an empty
	// previous value and the oracle would be quietly wrong.
	key, err := c.proj.TargetKey(e)
	if err != nil {
		c.onProjectionError(err, e.Key)
		return
	}
	prev, _ := c.orc.Get(key)

	mutation, err := c.proj.Apply(prev.Value, e)
	if err != nil {
		c.onProjectionError(err, e.Key)
		return
	}

	started := c.clk.Now()
	res := c.orc.Apply(mutation, e, verdict, c.tracker.Trust(e.Publisher))
	if c.m != nil {
		c.m.ObserveApplyDuration(c.clk.Now().Sub(started))
	}

	if res.Applied {
		c.eventsApplied.Add(1)

		// The convergence measurement starts here, at the moment the oracle
		// learned the value, because that is the instant the target begins to
		// be behind. Measuring from anywhere later understates the lag and so
		// understates the settlement window derived from it.
		c.est.OfferKey(mutation.Key)
		c.est.Observe(mutation.Key, res.Version, started)
	}
}

func (c *Check) onProjectionError(err error, key string) {
	// Counted as a drop as well as a projection error, because it is one: the
	// event was received and never reached the oracle. Without this,
	// events_received stops equalling applied plus dropped, and the difference
	// is exactly the events an operator most wants to find.
	c.eventsDropped.Add(1)

	if c.m != nil {
		c.m.EventDropped("", metrics.DropValidationError)
		c.m.ProjectionError(c.proj.Name(), projectionReason(err))
	}
	if allow, suppressed := c.sampler.Allow("projection_error"); allow {
		c.log.Error(err, "projection refused an event",
			"key", logging.Redact(key), "suppressed", suppressed)
	}
}

func (c *Check) onDecodeError(err error) {
	c.decodeErrors.Add(1)
	c.drop("", metrics.DropDecodeError, "", nil)

	// A message driftwatch could not decode is a message it did not see, and it
	// has no publisher or sequence number to scope the loss with — the fields
	// that would have told it were in the payload it could not read. §5.2's
	// answer is to widen suspicion to everything.
	//
	// That sounds drastic and is not, because it decays: the mark is a
	// generation counter, and every key touched by a later event is written at
	// the current generation and becomes trustworthy again. One malformed frame
	// suppresses assertions only about keys nothing has updated since.
	c.orc.MarkSuspect("", "undecodable payload")
	c.gapSignals.Add(1)

	if allow, suppressed := c.sampler.Allow("decode_error"); allow {
		c.log.Error(err, "could not decode a message; keys are marked suspect until "+
			"a later event refreshes them", "suppressed", suppressed)
	}
}

func (c *Check) onGap(e *event.Event) {
	c.gapSignals.Add(1)
	if c.m != nil {
		c.m.SeqGap(e.Publisher)
	}

	// driftwatch cannot know which keys the missing events touched — that
	// information was in the events it did not receive. §5.2's answer is to
	// widen suspicion as far as the projection's ownership model allows: a
	// partitioned keyspace loses trust only in that publisher's partition, and
	// everything else loses it everywhere.
	pattern := c.suspectPattern(e.Publisher)
	c.orc.MarkSuspect(pattern, "sequence gap on "+e.Publisher)

	if allow, suppressed := c.sampler.Allow("seq_gap"); allow {
		c.log.Info("sequence gap; affected keys are suspect and will not be alerted on",
			"publisher", e.Publisher, "seq", e.Seq,
			"scope", scopeOf(pattern), "suppressed", suppressed)
	}
}

func (c *Check) onRestart(e *event.Event) {
	c.gapSignals.Add(1)
	if c.m != nil {
		c.m.PublisherRestart(e.Publisher, metrics.RestartImplicit)
	}

	c.orc.MarkSuspect(c.suspectPattern(e.Publisher), "restart of "+e.Publisher)
	c.log.Info("publisher restarted", "publisher", e.Publisher, "epoch", e.Epoch, "seq", e.Seq)
}

// suspectPattern returns the keyspace a publisher's loss casts doubt over.
//
// An empty pattern means every key, which is the right answer whenever the
// projection does not promise publishers own disjoint keyspaces. It is a blunt
// instrument and deliberately so: suppressing findings too widely costs
// coverage, and suppressing them too narrowly costs correctness.
func (c *Check) suspectPattern(publisher string) string {
	ownership := c.proj.KeyOwnership()
	if !ownership.Partitioned || ownership.KeyPattern == "" {
		return ""
	}
	return strings.ReplaceAll(ownership.KeyPattern, "{{.Publisher}}", publisher)
}

func scopeOf(pattern string) string {
	if pattern == "" {
		return "all keys"
	}
	return pattern
}

func (c *Check) drop(publisher string, reason metrics.DropReason, msg string, err error) {
	c.eventsDropped.Add(1)
	if c.m != nil {
		c.m.EventDropped(publisher, reason)
	}
	if msg == "" {
		return
	}
	if allow, suppressed := c.sampler.Allow(string(reason)); allow {
		c.log.Error(err, msg, "reason", reason, "suppressed", suppressed)
	}
}

// projectionReason maps a projection error onto the closed metric enum, so an
// error string never becomes a label value.
func projectionReason(err error) metrics.ProjectionErrorReason {
	switch {
	case errors.Is(err, projection.ErrUnsupportedOp):
		return metrics.ProjectionUnsupportedOp
	case errors.Is(err, projection.ErrShapeMismatch):
		return metrics.ProjectionInvalidEvent
	default:
		return metrics.ProjectionInvalidEvent
	}
}

// watchGaps relays the source's possible-loss signals into the oracle.
//
// This is the wire §9 M14 calls out explicitly, and forgetting it is silent:
// everything keeps working, the reconnect is counted, and driftwatch goes on
// asserting at full confidence about keys whose events it missed while the
// socket was down.
func (c *Check) watchGaps(ctx context.Context) error {
	signaller, ok := c.src.(source.GapSignaller)
	if !ok {
		<-ctx.Done()
		return ctx.Err()
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sig, ok := <-signaller.Gaps():
			if !ok {
				<-ctx.Done()
				return ctx.Err()
			}
			c.gapSignals.Add(1)
			c.orc.MarkSuspect("", string(sig.Reason))
			c.log.Info("the source may have missed messages; every key is suspect "+
				"until a later event refreshes it",
				"reason", sig.Reason, "detail", sig.Detail)
		}
	}
}

// runSweeper bootstraps and then sweeps.
func (c *Check) runSweeper(ctx context.Context) error {
	if err := c.bootstrap(ctx); err != nil {
		return err
	}

	if c.spec.Policy.Paused {
		c.setPhase(PhasePaused, "policy.paused is set: ingesting, not sweeping")
		<-ctx.Done()
		return ctx.Err()
	}

	c.setPhase(c.steadyPhase())
	return c.swp.Run(ctx)
}

// refreshMetrics republishes the gauges that describe state rather than events.
func (c *Check) refreshMetrics(ctx context.Context) error {
	if c.m == nil {
		<-ctx.Done()
		return nil
	}

	ticker := c.clk.NewTicker(metricsRefreshInterval)
	defer ticker.Stop()

	c.publishGauges()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C():
			c.publishGauges()
		}
	}
}

// metricsRefreshInterval is how often the state gauges are recomputed. Counting
// a million oracle keys is not free, so it is deliberately slower than a
// Prometheus scrape rather than tied to one.
const metricsRefreshInterval = 15 * time.Second

func (c *Check) publishGauges() {
	if c.m == nil {
		return
	}

	counts := c.orc.Counts(c.clk.Now())
	c.m.SetOracleKeys(metrics.TrustComplete, counts.ByTrust[oracle.TrustComplete])
	c.m.SetOracleKeys(metrics.TrustSuspect, counts.ByTrust[oracle.TrustSuspect])
	c.m.SetOracleKeys(metrics.TrustAdopted, counts.ByTrust[oracle.TrustAdopted])
	c.m.SetSettledKeys(counts.Settled)
	c.m.SetInflightKeys(counts.InFlight)
	c.m.SetNeverSettledKeys(counts.NeverSettled)
	c.m.SetOracleEvictionsTotal(c.orc.Evictions())
	c.m.SetSettlementWindow(c.orc.SettlementWindow())
	c.m.SetQueueDepth(metrics.StageRaw, len(c.raw))

	publishers := c.tracker.Publishers()
	c.m.SetPublishersTracked(len(publishers))
	for _, ps := range publishers {
		if ps.Gaps == nil {
			continue
		}
		c.m.SetMissingEvents(ps.ID, ps.Gaps.Count())
		c.m.SetGapsetTruncated(ps.ID, ps.Gaps.Truncated())
	}

	stats := c.swp.Stats()
	c.m.SetConfirmQueueDepth(stats.PendingConfirmations)
	c.m.SetConfirmQueueDroppedTotal(uint64(stats.ConfirmQueueDropped))

	lagStats := c.est.Stats()
	c.m.SetLagProbeTimeoutsTotal(uint64(lagStats.TimedOut))

	srcStats := c.src.Stats()
	c.m.SetSourceConnected(0, srcStats.Connected)
	c.m.SetSourceReconnectsTotal(srcStats.Reconnects)
}

// ---------------------------------------------------------------------------
// Bootstrap (§5.6).
// ---------------------------------------------------------------------------

// bootstrapRetryInterval is how long a failed adopt scan waits before trying
// again. Bounded retry rather than giving up: the store being down at startup
// is common and temporary.
const bootstrapRetryInterval = 5 * time.Second

// bootstrap prepares the oracle before the first sweep.
//
// The three modes answer the same question differently: what does driftwatch
// believe about keys that existed before it attached? Adopt reads them and
// marks them advisory, Wait ignores them until an event proves they exist, and
// Strict refuses to assert anything until a publisher retransmits its state.
func (c *Check) bootstrap(ctx context.Context) error {
	defer close(c.bootstrapped)

	switch c.spec.Policy.Bootstrap {
	case BootstrapAdopt:
		return c.bootstrapAdopt(ctx)
	case BootstrapWait:
		// Nothing to do: the oracle starts empty and fills from events. Keys
		// the store already holds are reported as extras and treated
		// conservatively by the two-pass scan (§5.5).
		c.log.Info("bootstrap complete", "mode", BootstrapWait,
			"note", "the oracle starts empty; pre-existing keys appear as extras")
		return nil
	default: // BootstrapStrict
		c.log.Info("bootstrap complete", "mode", BootstrapStrict,
			"note", "no key is asserted on until its publisher completes a snapshot cycle")
		return nil
	}
}

// bootstrapAdopt reads the target's keyspace into the oracle as a baseline.
func (c *Check) bootstrapAdopt(ctx context.Context) error {
	for attempt := 1; ; attempt++ {
		adopted, err := c.adoptScan(ctx)
		if err == nil {
			c.log.Info("bootstrap complete", "mode", BootstrapAdopt, "adopted", adopted)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		c.setPhase(PhaseBootstrapping, "adopt scan failed: "+err.Error())
		c.log.Error(err, "bootstrap scan failed, retrying",
			"attempt", attempt, "retryIn", bootstrapRetryInterval)

		if err := c.clk.Sleep(ctx, bootstrapRetryInterval); err != nil {
			return err
		}
	}
}

// adoptScan walks the keyspace once, adopting as much as fits.
//
// A store with more keys than maxTrackedKeys is not an error and must not be
// treated as one. driftwatch adopts what it can, says how much it left, and
// carries on with reduced coverage — because a check that refuses to start on a
// large keyspace is a check that is never used on the systems that need it.
func (c *Check) adoptScan(ctx context.Context) (int, error) {
	c.setPhase(PhaseBootstrapping, "scanning the target keyspace")

	iter := c.tgt.Scan(ctx, c.spec.ExtraScanPattern(), c.readBatchSize())
	defer iter.Close() //nolint:errcheck // the scan error is reported through iter.Err

	adopted, skipped := 0, 0
	now := c.clk.Now()

	for iter.Next(ctx) {
		keys := iter.Keys()

		values, err := c.tgt.GetMany(ctx, keys, c.proj.TargetShape())
		if err != nil {
			return adopted, fmt.Errorf("reading a batch during the adopt scan: %w", err)
		}

		batch := make(map[string]event.Value, len(keys))
		for i, key := range keys {
			if adopted+len(batch) >= c.spec.Policy.MaxTrackedKeys {
				skipped++
				continue
			}
			batch[key] = values[i]
		}

		c.orc.AdoptSnapshot(batch, now)
		adopted += len(batch)
	}

	if err := iter.Err(); err != nil {
		return adopted, fmt.Errorf("scanning the target keyspace: %w", err)
	}

	if skipped > 0 {
		// Stated explicitly rather than left to a gauge. Coverage is a
		// correctness property here: findings are only ever about the keys
		// driftwatch is tracking, and an operator reading a clean report needs
		// to know it covers a fraction of their keyspace.
		c.log.Info("the target holds more keys than maxTrackedKeys allows; "+
			"coverage is partial and findings are incomplete",
			"adopted", adopted, "skipped", skipped,
			"maxTrackedKeys", c.spec.Policy.MaxTrackedKeys)
		message := fmt.Sprintf(
			"oracle saturated: adopted %d keys, skipped %d; findings cover only "+
				"the keys driftwatch is tracking", adopted, skipped)

		c.mu.Lock()
		c.saturation = message
		c.mu.Unlock()
		c.saturated.Store(true)

		c.setPhase(PhaseDegraded, message)
	}
	return adopted, nil
}

// ---------------------------------------------------------------------------
// Sweeps, status and explain.
// ---------------------------------------------------------------------------

// SweepNow runs one sweep out of band and returns its report.
func (c *Check) SweepNow(ctx context.Context) (*differ.Report, error) {
	c.mu.RLock()
	closed := c.closed
	c.mu.RUnlock()
	if closed {
		return nil, ErrClosed
	}

	rep, err := c.swp.SweepOnce(ctx)
	c.onReport(rep, err)
	return rep, err
}

// ScanExtras runs one target-to-oracle pass, which is the second half of a
// full comparison (§5.5).
func (c *Check) ScanExtras(ctx context.Context) (*differ.Report, error) {
	rep, err := c.swp.ScanExtrasOnce(ctx)
	if err != nil {
		return nil, err
	}
	if c.m != nil {
		c.m.Sweep(metrics.SweepTargetToOracle, metrics.SweepSuccess)
	}
	return rep, nil
}

// ConfirmDue drains the confirmation queue for candidates whose window has
// elapsed. Exposed so `driftwatch diff` can run a complete two-phase cycle in
// one process rather than waiting for the sweeper's own timer.
func (c *Check) ConfirmDue(ctx context.Context) int {
	return c.swp.ConfirmDue(ctx, c.clk.Now())
}

// onReport records a sweep's outcome in the status and the metrics.
func (c *Check) onReport(rep *differ.Report, err error) {
	c.mu.Lock()
	if rep != nil {
		c.lastReport = rep
		c.lastSweep = rep.FinishedAt
	}
	c.mu.Unlock()

	c.recordSweepMetrics(rep, err)

	switch {
	case errors.Is(err, sweeper.ErrTargetUnavailable):
		// Not a failure of driftwatch, and it must not read as one. §23 A5:
		// absence of data is not evidence of divergence.
		c.setPhase(PhaseDegraded, "the target could not be reached; nothing was compared")
	case err != nil:
		c.log.Error(err, "sweep failed")
	case c.phaseIs(PhaseDegraded):
		c.setPhase(c.steadyPhase())
	}
}

func (c *Check) recordSweepMetrics(rep *differ.Report, err error) {
	if c.m == nil {
		return
	}

	result := metrics.SweepSuccess
	switch {
	case errors.Is(err, sweeper.ErrTargetUnavailable):
		result = metrics.SweepTargetUnavailable
	case errors.Is(err, context.Canceled):
		result = metrics.SweepAborted
	case err != nil:
		result = metrics.SweepError
	}
	c.m.Sweep(metrics.SweepOracleToTarget, result)

	if rep == nil {
		return
	}

	c.m.ObserveSweepDuration(metrics.SweepOracleToTarget, rep.Duration())
	c.m.SetSweepKeysCompared(rep.KeysCompared)
	c.m.SetTargetReachable(rep.TargetHealth.Reachable)
	c.m.SetTargetKeyspaceSize(rep.TargetHealth.KeyspaceSize)
	c.m.SetTargetEvictionsObserved(rep.TargetHealth.EvictedKeys)
	c.m.SetTargetExpirationsObserved(rep.TargetHealth.ExpiredKeys)
	c.m.SetTargetRole(metrics.Role(rep.TargetHealth.Role).Normalize())
	c.m.SetCoverageRatio(coverage(rep, c.orc.Len()))

	c.publishDivergence(rep)
	c.publishEpisodes()

	stats := c.swp.Stats()
	c.m.SetConfirmQueueDepth(stats.PendingConfirmations)
	c.m.SetDriftDuration(c.oldestEpisodeAge())
}

// publishDivergence republishes the three divergence gauges from scratch.
//
// From scratch matters: a category that had findings last sweep and none this
// one must go to zero rather than keep its old value, or a resolved drift stays
// on the dashboard forever and the alert never clears.
func (c *Check) publishDivergence(rep *differ.Report) {
	confirmed := c.swp.Confirmed()

	byCategory := map[metrics.Category]int{}
	for key := range confirmed {
		f := confirmed[key]
		byCategory[metrics.Category(f.Category.String()).Normalize()]++
	}

	suspect := map[metrics.Category]int{}
	advisory := map[metrics.Category]int{}
	for i := range rep.Findings {
		f := &rep.Findings[i]
		cat := metrics.Category(f.Category.String()).Normalize()
		switch f.Trust {
		case oracle.TrustSuspect:
			suspect[cat]++
		case oracle.TrustAdopted:
			advisory[cat]++
		case oracle.TrustComplete:
		}
	}

	for _, cat := range metrics.Categories() {
		c.m.SetDivergentKeys(cat, byCategory[cat])
		c.m.SetSuspectDivergentKeys(cat, suspect[cat])
		c.m.SetAdvisoryDivergentKeys(cat, advisory[cat])
	}
}

// publishEpisodes turns the sweeper's confirmed set into the two episode
// counters.
//
// The sweeper owns a set, and Prometheus wants counters, so the difference
// between one sweep's set and the next is what gets counted. A key that
// appeared is one episode. A key that left is only a resolution if the sweeper
// also says a drift was resolved — findings are withdrawn for other reasons
// too, and a superseded expectation or an evicted key is the question being
// reopened rather than the store being repaired. Counting those as repairs
// would make the resolution rate a comfortable fiction.
func (c *Check) publishEpisodes() {
	confirmed := c.swp.Confirmed()
	resolvedTotal := c.swp.Stats().DriftResolved

	c.mu.Lock()
	previous := c.confirmedCats
	budget := resolvedTotal - c.lastResolved
	c.lastResolved = resolvedTotal

	current := make(map[string]differ.Category, len(confirmed))
	for key := range confirmed {
		cat := confirmed[key].Category
		current[key] = cat
		if _, existed := previous[key]; !existed {
			c.m.DriftEpisode(metrics.Category(cat.String()).Normalize())
		}
	}

	// Sorted, so which categories absorb the budget does not depend on map
	// iteration order and two identical runs produce identical counters.
	departed := make([]string, 0, len(previous))
	for key := range previous {
		if _, still := current[key]; !still {
			departed = append(departed, key)
		}
	}
	sort.Strings(departed)

	for _, key := range departed {
		if budget <= 0 {
			break
		}
		budget--
		c.m.DriftResolved(metrics.Category(previous[key].String()).Normalize())
	}

	c.confirmedCats = current
	c.mu.Unlock()
}

func (c *Check) oldestEpisodeAge() time.Duration {
	oldest := time.Duration(0)
	now := c.clk.Now()

	episodes := c.swp.Episodes()
	for key := range episodes {
		if age := now.Sub(episodes[key].FirstSeenAt); age > oldest {
			oldest = age
		}
	}
	return oldest
}

func coverage(rep *differ.Report, tracked int) float64 {
	if tracked == 0 {
		return 1
	}
	return float64(rep.KeysCompared) / float64(tracked)
}

// Explain answers "what happened to this key?".
func (c *Check) Explain(ctx context.Context, key string) (*explain.Explanation, error) {
	c.mu.RLock()
	closed := c.closed
	lastHealth := target.Health{}
	if c.lastReport != nil {
		lastHealth = c.lastReport.TargetHealth
	}
	c.mu.RUnlock()

	if closed {
		return nil, ErrClosed
	}

	// The eviction delta is what TARGET_EVICTION_LIKELY fires on, and it is
	// owned by whoever runs the sweeps rather than by the explain engine.
	evictions := uint64(0)
	if health, err := c.tgt.Health(ctx); err == nil && health.EvictedKeys > lastHealth.EvictedKeys {
		evictions = health.EvictedKeys - lastHealth.EvictedKeys
	}

	return explain.Explain(ctx, explain.Input{
		Key:                     key,
		Oracle:                  c.orc,
		Target:                  c.tgt,
		SeqTrack:                c.tracker,
		Shape:                   c.proj.TargetShape(),
		Window:                  c.orc.SettlementWindow(),
		Clock:                   c.clk,
		RingSize:                c.spec.Policy.RingSize,
		EvictionsSinceLastSweep: evictions,
	})
}

// ---------------------------------------------------------------------------
// Status.
// ---------------------------------------------------------------------------

// Status is a snapshot for the CRD status block and the CLI status line.
type Status struct {
	Phase   Phase  `json:"phase"`
	Message string `json:"message"`

	TrackedKeys  int `json:"trackedKeys"`
	SettledKeys  int `json:"settledKeys"`
	InFlightKeys int `json:"inFlightKeys"`
	SuspectKeys  int `json:"suspectKeys"`
	AdoptedKeys  int `json:"adoptedKeys"`
	NeverSettled int `json:"neverSettledKeys"`
	// OracleSaturated reports that the keyspace did not fit, so every finding
	// is partial. It maps onto the CRD condition of the same name (§10.1).
	OracleSaturated bool    `json:"oracleSaturated"`
	OracleEvictions uint64  `json:"oracleEvictions"`
	CoverageRatio   float64 `json:"coverageRatio"`

	DivergentKeys        int            `json:"divergentKeys"`
	SuspectDivergentKeys int            `json:"suspectDivergentKeys"`
	DivergenceByCategory map[string]int `json:"divergenceByCategory"`
	DriftDurationSeconds float64        `json:"driftDurationSeconds"`

	EventsApplied uint64 `json:"eventsApplied"`
	EventsDropped uint64 `json:"eventsDropped"`
	BytesReceived uint64 `json:"bytesReceived"`
	GapSignals    uint64 `json:"gapSignals"`

	SettlementWindowSeconds float64 `json:"settlementWindowSeconds"`
	ConvergenceP99Seconds   float64 `json:"convergenceP99Seconds"`
	WindowIsAdaptive        bool    `json:"windowIsAdaptive"`

	Publishers []PublisherStatus `json:"publishers"`

	LastSweepTime            time.Time `json:"lastSweepTime,omitempty"`
	LastSweepDurationSeconds float64   `json:"lastSweepDurationSeconds"`
	LastSweepKeysCompared    int       `json:"lastSweepKeysCompared"`
	SweepsSkipped            int64     `json:"sweepsSkipped"`

	TargetReachable    bool   `json:"targetReachable"`
	TargetRole         string `json:"targetRole,omitempty"`
	TargetKeyspaceSize int64  `json:"targetKeyspaceSize"`
}

// PublisherStatus is one publisher's sequence position.
type PublisherStatus struct {
	ID               string  `json:"id"`
	Epoch            uint64  `json:"epoch"`
	HighWaterMark    uint64  `json:"highWaterMark"`
	MissingEvents    uint64  `json:"missingEvents"`
	Restarts         uint64  `json:"restarts"`
	LastSeenSeconds  float64 `json:"lastSeenSeconds"`
	ClockSkewSeconds float64 `json:"clockSkewSeconds"`
}

// Status returns a snapshot of everything the check currently knows.
func (c *Check) Status() Status {
	now := c.clk.Now()
	counts := c.orc.Counts(now)

	c.mu.RLock()
	phase, message, rep, lastSweep := c.phase, c.message, c.lastReport, c.lastSweep
	c.mu.RUnlock()

	lagStats := c.est.Stats()
	sweepStats := c.swp.Stats()

	st := Status{
		Phase:                   phase,
		Message:                 message,
		TrackedKeys:             counts.Total,
		SettledKeys:             counts.Settled,
		InFlightKeys:            counts.InFlight,
		SuspectKeys:             counts.ByTrust[oracle.TrustSuspect],
		AdoptedKeys:             counts.ByTrust[oracle.TrustAdopted],
		NeverSettled:            counts.NeverSettled,
		OracleEvictions:         c.orc.Evictions(),
		OracleSaturated:         c.saturated.Load(),
		DivergenceByCategory:    map[string]int{},
		EventsApplied:           c.eventsApplied.Load(),
		EventsDropped:           c.eventsDropped.Load(),
		BytesReceived:           c.bytesRead.Load(),
		GapSignals:              c.gapSignals.Load(),
		SettlementWindowSeconds: c.orc.SettlementWindow().Seconds(),
		ConvergenceP99Seconds:   lagStats.P99.Seconds(),
		WindowIsAdaptive:        lagStats.Adaptive,
		SweepsSkipped:           sweepStats.SweepsSkipped,
		LastSweepTime:           lastSweep,
		DriftDurationSeconds:    c.oldestEpisodeAge().Seconds(),
	}

	confirmed := c.swp.Confirmed()
	for key := range confirmed {
		st.DivergentKeys++
		st.DivergenceByCategory[confirmed[key].Category.String()]++
	}

	if rep != nil {
		st.SuspectDivergentKeys = rep.ByTrust[oracle.TrustSuspect]
		st.LastSweepDurationSeconds = rep.Duration().Seconds()
		st.LastSweepKeysCompared = rep.KeysCompared
		st.CoverageRatio = coverage(rep, counts.Total)
		st.TargetReachable = rep.TargetHealth.Reachable
		st.TargetRole = rep.TargetHealth.Role
		st.TargetKeyspaceSize = rep.TargetHealth.KeyspaceSize
	}

	for _, ps := range c.tracker.Publishers() {
		p := PublisherStatus{
			ID:              ps.ID,
			Epoch:           ps.Epoch,
			HighWaterMark:   ps.HWM,
			Restarts:        ps.RestartCount,
			LastSeenSeconds: now.Sub(ps.LastSeen).Seconds(),
		}
		if ps.Gaps != nil {
			p.MissingEvents = ps.Gaps.Count()
		}
		st.Publishers = append(st.Publishers, p)
	}

	return st
}

// Summary renders the one-line status the CLI prints on its interval.
func (s *Status) Summary() string {
	var b strings.Builder

	fmt.Fprintf(&b, "%-14s keys %d (%d settled, %d in flight)",
		s.Phase, s.TrackedKeys, s.SettledKeys, s.InFlightKeys)
	fmt.Fprintf(&b, "  events %d", s.EventsApplied)

	if s.EventsDropped > 0 {
		fmt.Fprintf(&b, " (%d dropped)", s.EventsDropped)
	}
	fmt.Fprintf(&b, "  drift %d", s.DivergentKeys)
	if s.SuspectDivergentKeys > 0 {
		fmt.Fprintf(&b, " (+%d suspect)", s.SuspectDivergentKeys)
	}
	fmt.Fprintf(&b, "  W %.1fs", s.SettlementWindowSeconds)

	if !s.TargetReachable && !s.LastSweepTime.IsZero() {
		b.WriteString("  target unreachable")
	}
	return b.String()
}

func (c *Check) setPhase(p Phase, message string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.phase != p {
		c.log.Info("phase changed", "from", c.phase, "to", p, "message", message)
	}
	c.phase, c.message = p, message
}

// steadyPhase is what the check reports when nothing is currently wrong.
//
// Not always Watching: a saturated oracle is a permanent, partial view, and a
// check that went back to reporting Watching would be telling an operator its
// clean reports cover a keyspace they do not.
func (c *Check) steadyPhase() (phase Phase, message string) {
	if !c.saturated.Load() && !c.sourceFailed.Load() {
		return PhaseWatching, "steady state"
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.sourceFailed.Load() {
		return PhaseDegraded, c.sourceFailure
	}
	return PhaseDegraded, c.saturation
}

func (c *Check) phaseIs(p Phase) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.phase == p
}

// Close releases every resource. It is idempotent and safe after a failed New.
func (c *Check) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	closers := c.closers
	c.closers = nil
	c.mu.Unlock()

	// Reverse order: the source stops delivering before the target it feeds is
	// closed underneath it.
	var errs []error
	for i := len(closers) - 1; i >= 0; i-- {
		if err := closers[i].Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
