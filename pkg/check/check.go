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
	// PhaseAwaitingSnapshot is bootstrap Strict before any publisher has
	// retransmitted. §10.1's phase list omits it and §15 row 46 names it
	// explicitly; the more specific requirement wins. See ADR-0010.
	PhaseAwaitingSnapshot Phase = "AwaitingSnapshot"
	PhaseWatching         Phase = "Watching"
	PhaseDegraded         Phase = "Degraded"
	PhasePaused           Phase = "Paused"
	PhaseFailed           Phase = "Failed"
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

	// reorder restores sequence order within a bounded window, which §9 M6
	// requires whenever the projection is not commutative. See reorder.go.
	reorder *reorderBuffer

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

	// awaitingSnapshot is bootstrap Strict's state: nothing is asserted on
	// until a publisher retransmits. snapshotsSeen counts completed cycles.
	awaitingSnapshot atomic.Bool
	snapshotsSeen    atomic.Uint64

	// multiWriter records that two publishers have written the same key under a
	// projection whose fold is order-dependent. See checkMultiWriter.
	multiWriter    atomic.Bool
	multiWriterKey atomic.Pointer[string]

	// skewMu guards clockSkew, the last measured offset between each
	// publisher's wall clock and driftwatch's. Bounded by the same publisher
	// limit seqtrack uses, so a stream of one-off publisher ids cannot grow it.
	skewMu    sync.Mutex
	clockSkew map[string]time.Duration

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
		raw:           make(chan source.RawMessage, ingestBufferFor(&spec)),
		confirmedCats: map[string]differ.Category{},
		clockSkew:     map[string]time.Duration{},
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

// inProcessIngestBuffer caps the ingest channel for sources that cannot drop.
//
// Generous for a transport whose producer blocks, and three orders of magnitude
// smaller than the default below.
const inProcessIngestBuffer = 4096

// ingestBufferFor sizes the channel between the source and the applier.
//
// The buffer exists to absorb bursts a socket would otherwise discard: §10.2
// requires it to exceed recvHWM precisely so loss happens here, where it is
// counted, rather than inside the transport where it is invisible. That
// argument only applies to a transport that can drop.
//
// A file source blocks its reader and a memory source is in-process, so neither
// can lose a message no matter how far behind the applier falls — and sizing
// their channel for a socket costs 12.8 MiB of empty channel per check. §15 row
// 60 measured that: fifty idle checks held 640 MB, essentially all of it this
// allocation. See docs/DISCOVERIES.md D-016.
func ingestBufferFor(spec *Spec) int {
	switch spec.Source.Type {
	case "zmq", "nats":
		return spec.Source.IngestBufferSize
	default:
		return min(spec.Source.IngestBufferSize, inProcessIngestBuffer)
	}
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

	// A commutative projection reaches the same state whatever order it folds
	// in, so buffering would be latency bought for nothing.
	window := c.spec.Policy.ReorderWindow.Duration()
	if c.proj.Commutative() {
		window = 0
	}
	c.reorder = newReorderBuffer(window, defaultMaxHeldPerPublisher)

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
	// The reorder buffer needs someone to notice that a held event's wait has
	// run out. A stream that goes quiet mid-sequence would otherwise leave that
	// event unapplied indefinitely, and every sweep after it would compare an
	// oracle missing an update driftwatch had actually received.
	//
	// It ticks here rather than in the sweeper because applying an event writes
	// the oracle, and the applier is the only goroutine allowed to do that.
	flush := c.clk.NewTicker(reorderFlushInterval)
	defer flush.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-c.raw:
			if !ok {
				return nil
			}
			c.applyMessage(&msg)
		case <-flush.C():
			c.FlushReorder()
		}
	}
}

// reorderFlushInterval is how often the applier checks for held events whose
// wait has expired. Well inside the reorder window, so the delay a held event
// actually sees is the window rather than the window plus a tick.
const reorderFlushInterval = 250 * time.Millisecond

// Ingest applies one raw message on the caller's goroutine.
//
// Run's applier is the only caller in production. It is exported because a test
// that drives the pipeline one message at a time is deterministic in a way a
// goroutine and a channel are not — and because the alternative, a second copy
// of the applier living in the test harness, is exactly how a key-template bug
// survived four phases undetected (D-013). The fault matrix in §15 runs through
// this, so every row exercises the real pipeline rather than a replica of it.
//
// It must not be called concurrently with Run: the oracle's single-writer
// discipline is what makes version bumping correct without a global lock.
func (c *Check) Ingest(msg source.RawMessage) { c.applyMessage(&msg) }

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

	// Restore sequence order before anything downstream looks at the event.
	// seqtrack, the projection and the oracle all assume they see a publisher's
	// stream in order, and every one of them is wrong in a different way when
	// they do not.
	released := c.reorder.offer(&e, e.ObservedAt)
	for i := range released {
		c.applyOrdered(&released[i])
	}
}

// applyOrdered runs one in-sequence event through the rest of the pipeline.
func (c *Check) applyOrdered(e *event.Event) {
	verdict, _ := c.tracker.Observe(e)
	switch verdict {
	case seqtrack.DropDuplicate:
		c.drop(e.Publisher, metrics.DropDuplicate, "", nil)
		return
	case seqtrack.DropStaleEpoch:
		c.drop(e.Publisher, metrics.DropStaleEpoch, "", nil)
		return
	case seqtrack.AcceptWithGap:
		c.onGap(e)
	case seqtrack.AcceptAfterRestart:
		c.onRestart(e)
	case seqtrack.Accept, seqtrack.AcceptLateFill, seqtrack.AcceptFirstSeen:
	}

	c.onSnapshotOp(e)
	c.fold(e, verdict)
}

// onSnapshotOp restores trust when a publisher finishes retransmitting.
//
// This is the other half of §5.2, and the half that was missing: MarkSuspect
// has no counterpart unless something clears it. A publisher that has just
// resent its whole state has made whatever driftwatch missed irrelevant — the
// oracle now holds what the publisher says is true, so continuing to refuse to
// assert on those keys costs coverage for no reason.
//
// It is also what makes bootstrap Strict terminate, and what makes a declared
// restart followed by a snapshot clean rather than suspect (§15 rows 20, 46).
func (c *Check) onSnapshotOp(e *event.Event) {
	switch e.Op {
	case event.OpSnapshotBegin:
		c.log.Info("publisher began a snapshot", "publisher", e.Publisher, "seq", e.Seq)

	case event.OpSnapshotEnd:
		pattern := c.suspectPattern(e.Publisher)
		c.tracker.ClearGaps(e.Publisher)
		c.orc.ClearSuspect(pattern)
		c.snapshotsSeen.Add(1)

		c.log.Info("publisher completed a snapshot; its keys are trustworthy again",
			"publisher", e.Publisher, "scope", scopeOf(pattern))

		if c.awaitingSnapshot.CompareAndSwap(true, false) {
			c.setPhase(c.steadyPhase())
		}

	case event.OpUnknown, event.OpSet, event.OpDelete, event.OpAdd,
		event.OpRemove, event.OpIncr, event.OpHeartbeat:
	}
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

	c.checkMultiWriter(&prev, e)
	c.recordSkew(e)

	trust := c.tracker.Trust(e.Publisher)
	if c.awaitingSnapshot.Load() {
		// Bootstrap Strict, before any publisher has retransmitted. Marking the
		// existing keys suspect at startup is not enough on its own: a key
		// created by an event arriving now would be written at the current
		// generation and come out trustworthy, so the mode would leak
		// assertions about exactly the keyspace it was told not to assert on.
		trust = event.TrustSuspect
	}

	started := c.clk.Now()
	res := c.orc.Apply(mutation, e, verdict, trust)
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

// recordSkew measures how far a publisher's clock is from driftwatch's.
//
// Diagnostic only, and the comment is the point: nothing in the pipeline reads
// this back. Settlement, ordering and gap detection all run on ObservedAt, the
// local receive time, precisely so that a publisher with a wrong clock cannot
// change what driftwatch decides (§5.3, fault F5). What skew is for is the
// operator reading `explain` output full of publisher timestamps, who needs to
// know those timestamps are five minutes off before drawing a conclusion from
// them — and the §12 metric that was declared for it and, until §15 rows 23
// and 24 asked, never written.
func (c *Check) recordSkew(e *event.Event) {
	if e.PublishedAt.IsZero() || e.ObservedAt.IsZero() {
		return
	}

	c.skewMu.Lock()
	defer c.skewMu.Unlock()

	if _, known := c.clockSkew[e.Publisher]; !known &&
		len(c.clockSkew) >= c.spec.Policy.MaxPublishers {
		return
	}
	c.clockSkew[e.Publisher] = e.PublishedAt.Sub(e.ObservedAt)
}

// skewOf returns a publisher's last measured clock offset.
func (c *Check) skewOf(publisher string) time.Duration {
	c.skewMu.Lock()
	defer c.skewMu.Unlock()
	return c.clockSkew[publisher]
}

// checkMultiWriter notices two publishers writing the same key.
//
// §15 row 25's point is that "last write wins" is not globally meaningful when
// the writes come from different publishers: sequence numbers only order events
// within one publisher's stream, so there is no fact of the matter about which
// of two concurrent writes came second. For a set projection this is harmless —
// adds and removes of distinct members commute, so any interleaving reaches the
// same place. For a scalar or a counter it is not, and driftwatch's expectation
// is then one arbitrary choice among several equally valid ones.
//
// It cannot be fixed here, only declared. Reporting drift would be wrong, and
// silently picking a winner would be worse, so the check records that its
// answers for this keyspace are unreliable and names the key that showed it.
//
// The comparison is free: the oracle already stores each key's last publisher,
// so this costs a string compare on a value that was fetched anyway.
func (c *Check) checkMultiWriter(prev *oracle.Entry, e *event.Event) {
	if prev.LastPublisher == "" || prev.LastPublisher == e.Publisher {
		return
	}

	// A set folds per member: two publishers adding different members reach the
	// same set whichever order they arrive in, so there is no ambiguity to
	// declare. A scalar replaces the whole value and an absolute counter
	// overwrites the running total, so those genuinely conflict — which is the
	// distinction §15 row 25 draws, and why Commutative alone is the wrong
	// test. keysetOwnership reports false because add and remove of the *same*
	// member is order-dependent, which is a different question.
	if c.proj.TargetShape() == projection.ShapeSet || c.proj.Commutative() {
		return
	}
	if c.proj.KeyOwnership().Partitioned {
		// Publishers own disjoint keyspaces, so two of them touching one key is
		// a misconfiguration the operator has already been warned about rather
		// than an inherent ambiguity.
		return
	}

	key := e.Key
	c.multiWriterKey.Store(&key)

	if c.multiWriter.CompareAndSwap(false, true) {
		c.log.Info("two publishers wrote the same key under a projection whose fold "+
			"depends on order; driftwatch's expectation for this keyspace is one "+
			"valid interleaving among several, and findings on it are unreliable",
			"key", logging.Redact(key),
			"publishers", []string{prev.LastPublisher, e.Publisher},
			"projection", c.proj.Name())
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
	c.drop("", decodeReason(err), "", nil)

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

// decodeReason maps a codec failure onto the §12 drop reason it belongs to.
//
// The three are not interchangeable, and reporting all of them as decode_error
// — which is what happened until §15 rows 18 and 19 asked — sends an operator
// to the wrong system. A malformed payload is a serializer or a wire-format
// mismatch. An unknown op is a producer that started emitting an event type
// nobody configured, with everything else about the message fine. An oversized
// frame is a producer bug or an attack, and is the only one of the three that
// says nothing at all about the format.
func decodeReason(err error) metrics.DropReason {
	switch {
	case errors.Is(err, codec.ErrTooLarge):
		return metrics.DropTooLarge
	case errors.Is(err, codec.ErrUnknownOp):
		return metrics.DropUnknownOp
	default:
		return metrics.DropDecodeError
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
			c.SignalGap(sig)
		}
	}
}

// SignalGap records that the source may have missed messages.
//
// Run's gap watcher is the only caller in production. It is exported for the
// same reason Ingest is: a scenario that needs a reconnect to have happened
// should say so directly rather than build a socket and break it, and the
// handler under test is then the one that really runs.
//
// Every key becomes suspect, because a PUB/SUB subscriber that reconnects
// cannot find out what it missed — or even whether it missed anything. The
// suspicion decays as later events refresh each key, and a snapshot clears it
// outright.
func (c *Check) SignalGap(sig source.GapSignal) {
	c.gapSignals.Add(1)
	c.orc.MarkSuspect("", string(sig.Reason))

	c.log.Info("the source may have missed messages; every key is suspect "+
		"until a later event refreshes it",
		"reason", sig.Reason, "detail", sig.Detail)
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
		c.m.SetClockSkew(ps.ID, c.skewOf(ps.ID))
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
		// Strict has to do something rather than say something. Marking every
		// key suspect up front is what makes the promise real: a suspect key
		// produces no alertable finding, so nothing is asserted until a
		// publisher's snapshotEnd clears it. Without this the mode was a log
		// line that behaved exactly like Wait (§15 row 46).
		c.awaitingSnapshot.Store(true)
		c.orc.MarkSuspect("", "awaiting a snapshot cycle")
		c.setPhase(PhaseAwaitingSnapshot,
			"no key is asserted on until a publisher completes a snapshot cycle")

		c.log.Info("bootstrap complete", "mode", BootstrapStrict,
			"note", "awaiting a snapshot cycle before asserting on any key")
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

// FlushReorder applies anything whose wait for a predecessor has run out.
//
// Run drives this on a ticker from the applier goroutine. It is exported for
// the same reason Ingest is: a synchronous driver — the fault matrix, a replay
// — needs to end the wait deliberately rather than hope a goroutine notices the
// clock moved.
//
// It must be called from whichever goroutine calls Ingest, and from no other.
// Applying an event writes the oracle, and the oracle's version bumping and
// settlement index are correct without a global lock precisely because exactly
// one goroutine writes them. Calling this from a sweep — which is what an
// earlier version did, to make sure a sweep never compared against an oracle
// with an update still sitting in the buffer — put a second writer on it.
func (c *Check) FlushReorder() {
	released := c.reorder.expire(c.clk.Now())
	for i := range released {
		c.applyOrdered(&released[i])
	}
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

// PollLag runs one round of the convergence estimator: poll the outstanding
// probes, rotate the sample, and recompute the settlement window.
//
// Run drives this on its own ticker. It is exported for the same reason Ingest
// is — a test that advances a fake clock and then asks the estimator to look is
// deterministic, where waiting for a goroutine to notice the clock moved is a
// race dressed up as a test.
func (c *Check) PollLag(ctx context.Context) { c.est.Tick(ctx, c.clk.Now()) }

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

	// Recompute the state gauges here as well as on the refresh ticker. A sweep
	// has just walked the oracle, so this is the cheapest moment to publish
	// what it found — and without it a process that only ever sweeps out of
	// band, which is what `driftwatch diff` and `watch --once` do, would export
	// the gauges exactly once at startup and never again.
	c.publishGauges()

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

	// The reachability gauge has to be set on the failure path too. It is fed
	// from the report's health, and a sweep that could not reach the store
	// produces no report — so without this it holds its last value at exactly
	// the moment it is supposed to change, and the alert on target availability
	// never fires.
	if result == metrics.SweepTargetUnavailable {
		c.m.SetTargetReachable(false)
	}

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
	OracleSaturated bool `json:"oracleSaturated"`
	// SnapshotsSeen counts completed snapshot cycles, which is what clears
	// suspicion after a gap and what bootstrap Strict waits for.
	SnapshotsSeen uint64 `json:"snapshotsSeen"`
	// AwaitingSnapshot reports that bootstrap Strict has not yet seen a
	// publisher retransmit, so nothing is being asserted about any key. It is a
	// fact about the check rather than about its run loop, which is why it is
	// here as well as in Phase.
	AwaitingSnapshot bool `json:"awaitingSnapshot"`
	// ReorderHeld counts events waiting for a predecessor. A number that stays
	// high means the stream is arriving badly out of order, or that a publisher
	// stopped mid-sequence.
	ReorderHeld int `json:"reorderHeld"`
	// MultiWriterUnsafe reports that two publishers have written the same key
	// under an order-dependent projection, so findings on that keyspace reflect
	// one arbitrary interleaving (§15 row 25). It maps onto the CRD condition of
	// the same name.
	MultiWriterUnsafe bool `json:"multiWriterUnsafe"`
	// MultiWriterKey names the most recent key that showed it.
	MultiWriterKey  string  `json:"multiWriterKey,omitempty"`
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
		SnapshotsSeen:           c.snapshotsSeen.Load(),
		AwaitingSnapshot:        c.awaitingSnapshot.Load(),
		ReorderHeld:             c.reorder.heldCount(),
		MultiWriterUnsafe:       c.multiWriter.Load(),
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

	if key := c.multiWriterKey.Load(); key != nil {
		st.MultiWriterKey = *key
	}

	for _, ps := range c.tracker.Publishers() {
		p := PublisherStatus{
			ID:               ps.ID,
			Epoch:            ps.Epoch,
			HighWaterMark:    ps.HWM,
			Restarts:         ps.RestartCount,
			LastSeenSeconds:  now.Sub(ps.LastSeen).Seconds(),
			ClockSkewSeconds: c.skewOf(ps.ID).Seconds(),
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
	if c.awaitingSnapshot.Load() {
		return PhaseAwaitingSnapshot,
			"no key is asserted on until a publisher completes a snapshot cycle"
	}
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
