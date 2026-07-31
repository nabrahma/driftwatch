// Package sweeper runs the settle, diff and two-phase confirm cycle (§5.3-§5.5, M10).
//
// This is where the correctness mechanisms are composed, and composing them is
// the whole job. Any one of them alone still produces a tool that cries wolf:
// the settlement window handles a materializer that is behind, version fencing
// handles driftwatch's own concurrency, two-phase confirmation handles the
// single unlucky read, and the two-pass extras scan handles a keyspace that
// changes underneath a non-atomic scan. They defeat different faults and none
// substitutes for another.
//
// The rule governing every decision here is §23 A5: absence of data is not
// evidence of divergence. When driftwatch cannot find out, it reports nothing
// and says why. A tool that reports drift because it could not read the store
// is worse than no tool, because it teaches the operator to ignore it.
package sweeper

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/differ"
	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/target"
)

// Sentinel errors. Each means driftwatch declined to assert anything, and they
// are distinct because the operator's next move differs for each.
var (
	// ErrTargetUnavailable reports that the store could not be reached. No
	// findings are produced, ever: §23 A5.
	ErrTargetUnavailable = errors.New("target unavailable, nothing compared")

	// ErrNotPrimary reports that the store is a replica while RequirePrimary is
	// set. A replica is legitimately behind, so comparing against one
	// manufactures drift that does not exist.
	ErrNotPrimary = errors.New("target is not the primary")

	// ErrClosed reports use of a sweeper after Close.
	ErrClosed = errors.New("sweeper is closed")
)

// The Prometheus metric names these counters feed, from §12. They live here so
// that M12 wires the exporter to a named constant rather than to a string typed
// twice, which is how a counter and its metric quietly stop meaning the same
// thing.
const (
	MetricDriftResolvedTotal       = "driftwatch_drift_resolved_total"
	MetricDriftDurationSeconds     = "driftwatch_drift_duration_seconds"
	MetricDivergentKeys            = "driftwatch_divergent_keys"
	MetricInflightKeys             = "driftwatch_inflight_keys"
	MetricTransientDivergenceTotal = "driftwatch_transient_divergence_total"
	MetricConfirmQueueDroppedTotal = "driftwatch_confirm_queue_dropped_total"
	MetricSweepsSkippedTotal       = "driftwatch_sweeps_skipped_total"
	MetricExtrasReportedTotal      = "driftwatch_extras_reported_total"
)

// Defaults for Config.
const (
	defaultSweepInterval     = 30 * time.Second
	defaultExtraScanInterval = 5 * time.Minute
	defaultConfirmInterval   = time.Second
	defaultReadBatchSize     = 500
	defaultMaxConfirmQueue   = 10_000
	defaultMaxExtrasTracked  = 100_000
	defaultExtraScanPattern  = "*"
	defaultSubscriberBuffer  = 256
)

// Config configures a Sweeper.
type Config struct {
	Oracle *oracle.Oracle
	Target target.Target
	Shape  projection.Shape
	Clock  clock.Clock

	// DifferOptions carries the expiry policy and the reporting limits. Now is
	// overwritten per sweep; this package owns the clock reading.
	DifferOptions differ.Options

	// SweepInterval is how often the oracle→target pass runs. Default 30s.
	SweepInterval time.Duration
	// ExtraScanInterval is how often the target→oracle pass runs. Default 5m,
	// because extras move slowly and the scan is the expensive half (§5.5).
	ExtraScanInterval time.Duration
	// ConfirmInterval is how often the confirm queue is drained. Default 1s.
	// It is not the confirmation delay — that is W, held per candidate.
	ConfirmInterval time.Duration
	// ExtraScanPattern limits the keyspace scan. Default "*".
	ExtraScanPattern string

	// SettlementWindow returns the current W, so an adaptive estimator can move
	// it underneath a running sweeper. Read once per sweep, never per key.
	SettlementWindow func() time.Duration

	// ReadBatchSize is how many keys are read from the target at a time.
	// Default 500.
	ReadBatchSize int
	// MaxConfirmQueue bounds the candidates awaiting confirmation.
	// Default 10,000.
	MaxConfirmQueue int
	// MaxExtrasTracked bounds the first extras pass. Default 100,000.
	MaxExtrasTracked int

	// RequirePrimary refuses to sweep anything but a primary.
	RequirePrimary bool

	// OnReport receives the outcome of every sweep the Run loop drives,
	// including the errors. Optional.
	OnReport func(*differ.Report, error)
}

func (c *Config) applyDefaults() {
	if c.SweepInterval <= 0 {
		c.SweepInterval = defaultSweepInterval
	}
	if c.ExtraScanInterval <= 0 {
		c.ExtraScanInterval = defaultExtraScanInterval
	}
	if c.ConfirmInterval <= 0 {
		c.ConfirmInterval = defaultConfirmInterval
	}
	if c.ReadBatchSize <= 0 {
		c.ReadBatchSize = defaultReadBatchSize
	}
	if c.MaxConfirmQueue <= 0 {
		c.MaxConfirmQueue = defaultMaxConfirmQueue
	}
	if c.MaxExtrasTracked <= 0 {
		c.MaxExtrasTracked = defaultMaxExtrasTracked
	}
	if c.ExtraScanPattern == "" {
		c.ExtraScanPattern = defaultExtraScanPattern
	}
	if c.Clock == nil {
		c.Clock = clock.Real()
	}
	if c.SettlementWindow == nil {
		w := c.Oracle.SettlementWindow()
		c.SettlementWindow = func() time.Duration { return w }
	}
}

// Episode is one drift episode: a key that disagreed, was confirmed to still
// disagree a settlement window later, and has not yet been repaired.
//
// It carries both read times and the window they were judged against, which is
// what makes invariant I7 checkable from outside the package rather than a
// claim about code nobody can see.
type Episode struct {
	Key     string
	Finding differ.Finding

	// FirstSeenAt is when a sweep first saw the disagreement, ConfirmedAt is
	// when the re-read confirmed it, and Window is the W in force when the
	// candidate was raised. ConfirmedAt.Sub(FirstSeenAt) >= Window, always.
	FirstSeenAt time.Time
	ConfirmedAt time.Time
	Window      time.Duration
}

// Stats is a snapshot of the sweeper's counters.
type Stats struct {
	Sweeps        int64
	SweepsSkipped int64
	KeysCompared  int64
	FenceFailures int64

	CandidatesEnqueued  int64
	ConfirmQueueDropped int64
	ConfirmCycles       int64
	Confirmations       int64

	// The three ways a candidate stops being one without becoming a finding.
	// They are counted apart because they mean different things: the first says
	// the materializer is slow relative to W, the second says driftwatch's own
	// timing was unlucky, the third says the oracle is too small.
	TransientResolved       int64
	TransientOracleAdvanced int64
	TransientKeyEvicted     int64

	// ConfirmReadFailed counts confirmations abandoned because the target could
	// not be read. Deliberately not counted as a transient: the disagreement is
	// unresolved, not resolved.
	ConfirmReadFailed int64

	DriftResolved           int64
	ConfirmedDroppedEvicted int64
	// ConfirmedSuperseded counts findings withdrawn because a new event
	// replaced the expectation they were raised against. Not a repair: the
	// question is simply open again.
	ConfirmedSuperseded int64
	// SuspectNotConfirmed counts disagreements left unconfirmed because
	// driftwatch does not trust its own view of the key. A rising count here is
	// driftwatch declining to blame the store for its own lost events.
	SuspectNotConfirmed int64
	// LastDriftDuration is how long the most recently repaired episode lasted,
	// measured from its first disagreeing read.
	LastDriftDuration time.Duration

	ExtraScans         int64
	ExtrasReported     int64
	ExtrasSelfResolved int64
	ExtrasTruncated    int64

	TargetUnavailable int64
	SubscriberDropped int64

	// CurrentlyConfirmed is the live gauge behind driftwatch_divergent_keys.
	CurrentlyConfirmed int
	// PendingConfirmations is the depth of the confirm queue.
	PendingConfirmations int
	// PendingExtras is the size of the extras set awaiting its second pass.
	PendingExtras int
}

// counters holds the raw atomics, split from Stats so that a snapshot is a
// plain value the caller can keep.
type counters struct {
	sweeps        atomic.Int64
	sweepsSkipped atomic.Int64
	keysCompared  atomic.Int64
	fenceFailures atomic.Int64

	candidatesEnqueued  atomic.Int64
	confirmQueueDropped atomic.Int64
	confirmCycles       atomic.Int64
	confirmations       atomic.Int64

	transientResolved       atomic.Int64
	transientOracleAdvanced atomic.Int64
	transientKeyEvicted     atomic.Int64
	confirmReadFailed       atomic.Int64

	driftResolved           atomic.Int64
	confirmedDroppedEvicted atomic.Int64
	confirmedSuperseded     atomic.Int64
	suspectNotConfirmed     atomic.Int64
	lastDriftDuration       atomic.Int64

	extraScans         atomic.Int64
	extrasReported     atomic.Int64
	extrasSelfResolved atomic.Int64
	extrasTruncated    atomic.Int64

	targetUnavailable atomic.Int64
	subscriberDropped atomic.Int64
}

// Sweeper compares the oracle against the target and confirms what it finds.
type Sweeper struct {
	cfg Config

	// sweepMu serializes sweeps. SweepOnce blocks on it and TrySweepOnce skips,
	// which is the difference between the CLI asking for a sweep and a ticker
	// arriving while one is already running (§9 M10).
	sweepMu sync.Mutex

	mu        sync.Mutex
	queue     []*candidate
	queued    map[string]struct{}
	confirmed map[string]Episode
	requeued  map[string]struct{}
	extras    *extrasPass
	subs      []chan differ.Finding
	closed    bool

	c counters
}

// New returns a Sweeper configured by cfg.
//
// Config is by value to match the other packages' constructors, and New is
// called once at startup.
//
//nolint:gocritic // hugeParam: deliberate, see above.
func New(cfg Config) *Sweeper {
	cfg.applyDefaults()

	return &Sweeper{
		cfg:       cfg,
		queued:    map[string]struct{}{},
		confirmed: map[string]Episode{},
		requeued:  map[string]struct{}{},
	}
}

// Run drives the sweep, confirm and extras loops until ctx is done.
func (s *Sweeper) Run(ctx context.Context) error {
	sweeps := s.cfg.Clock.NewTicker(s.cfg.SweepInterval)
	defer sweeps.Stop()
	confirms := s.cfg.Clock.NewTicker(s.cfg.ConfirmInterval)
	defer confirms.Stop()
	extras := s.cfg.Clock.NewTicker(s.cfg.ExtraScanInterval)
	defer extras.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sweeps.C():
			s.TrySweepOnce(ctx)
		case now := <-confirms.C():
			s.ConfirmDue(ctx, now)
		case <-extras.C():
			s.report(s.ScanExtrasOnce(ctx))
		}
	}
}

// SweepOnce performs one oracle→target pass, blocking if another sweep holds
// the guard. Exposed for the diff CLI and for deterministic tests.
func (s *Sweeper) SweepOnce(ctx context.Context) (*differ.Report, error) {
	s.sweepMu.Lock()
	defer s.sweepMu.Unlock()
	return s.sweep(ctx)
}

// TrySweepOnce runs a sweep unless one is already in progress, in which case it
// counts the skip and returns false.
//
// Skipping rather than queueing is deliberate. A sweep that overruns its
// interval is a sweeper that cannot keep up, and stacking the next one behind
// it turns a slow sweeper into an unbounded queue of sweeps. The counter is how
// the operator finds out (§9 M10).
func (s *Sweeper) TrySweepOnce(ctx context.Context) bool {
	if !s.sweepMu.TryLock() {
		s.c.sweepsSkipped.Add(1)
		return false
	}
	defer s.sweepMu.Unlock()

	s.report(s.sweep(ctx))
	return true
}

// sweep is the algorithm from §9 M10, in its numbered order.
func (s *Sweeper) sweep(ctx context.Context) (*differ.Report, error) {
	if s.isClosed() {
		return nil, ErrClosed
	}

	// 1. The store must be reachable before anything else happens. Every step
	//    below produces findings, and a finding derived from a store that did
	//    not answer is a fabrication.
	health, err := s.checkHealth(ctx)
	if err != nil {
		return nil, err
	}
	evictedBefore := health.EvictedKeys

	// 2. The time and the window are captured once each. A W that moved
	//    mid-sweep would mean the first half of the keyspace was judged by one
	//    rule and the second half by another.
	now := s.cfg.Clock.Now()
	w := s.cfg.SettlementWindow()

	opts := s.cfg.DifferOptions
	opts.Now = now

	rep := differ.NewReport(now, opts)
	rep.SettlementWindow = w
	rep.KeysSkippedInFlight = s.cfg.Oracle.Counts(now).InFlight

	// 3 and 4. Walk the settled keys, batching the reads.
	if err := s.walk(ctx, rep, opts, now, w); err != nil {
		return nil, err
	}

	// 5. A sweep that found mass absence while the store was evicting has an
	//    obvious explanation, and saying so saves the operator an hour (§5.7).
	if after, err := s.cfg.Target.Health(ctx); err == nil {
		rep.EvictionSuspected = after.EvictedKeys > evictedBefore
		rep.TargetHealth = after
	} else {
		rep.TargetHealth = health
	}

	// Withdraw any confirmed finding the oracle has forgotten or moved past, so
	// the gauge and the report agree with what driftwatch can still support.
	s.liveEpisodes()

	rep.FinishedAt = s.cfg.Clock.Now()
	s.c.sweeps.Add(1)
	return rep, nil
}

// checkHealth performs step 1, distinguishing the three ways it goes wrong.
func (s *Sweeper) checkHealth(ctx context.Context) (target.Health, error) {
	health, err := s.cfg.Target.Health(ctx)

	// Our own cancellation is not the store being unreachable, and conflating
	// them would make a clean shutdown look like an outage in the metrics.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return health, ctxErr
	}
	if err != nil {
		s.c.targetUnavailable.Add(1)
		return health, fmt.Errorf("%w: %w", ErrTargetUnavailable, err)
	}
	if !health.Reachable {
		s.c.targetUnavailable.Add(1)
		return health, ErrTargetUnavailable
	}
	if s.cfg.RequirePrimary && health.Role != "master" {
		return health, fmt.Errorf("%w: role is %q", ErrNotPrimary, health.Role)
	}
	return health, nil
}

// walk performs step 3: iterate the settled keys and hand them to processBatch
// in batches of ReadBatchSize.
func (s *Sweeper) walk(
	ctx context.Context,
	rep *differ.Report,
	opts differ.Options,
	now time.Time,
	w time.Duration,
) error {
	s.drainRequeued()

	batch := make([]string, 0, s.cfg.ReadBatchSize)
	versions := make(map[string]uint64, s.cfg.ReadBatchSize)
	var walkErr error

	for key := range s.cfg.Oracle.SettledKeys(now) {
		if err := ctx.Err(); err != nil {
			walkErr = err
			break
		}

		e, ok := s.cfg.Oracle.Get(key)
		if !ok {
			// Evicted between the iterator yielding the key and this read. Not
			// an error: the key simply no longer exists to compare.
			continue
		}
		if e.Trust == oracle.TrustAdopted {
			rep.KeysSkippedAdopted++
			continue
		}

		versions[key] = e.Version
		batch = append(batch, key)

		if len(batch) < s.cfg.ReadBatchSize {
			continue
		}
		if err := s.processBatch(ctx, rep, opts, batch, versions, now, w); err != nil {
			walkErr = err
			break
		}
		batch = batch[:0]
		versions = make(map[string]uint64, s.cfg.ReadBatchSize)
	}

	if walkErr != nil {
		return walkErr
	}
	if len(batch) == 0 {
		return nil
	}
	return s.processBatch(ctx, rep, opts, batch, versions, now, w)
}

// processBatch performs step 4: read the batch, re-check the fence, compare.
func (s *Sweeper) processBatch(
	ctx context.Context,
	rep *differ.Report,
	opts differ.Options,
	keys []string,
	versions map[string]uint64,
	now time.Time,
	w time.Duration,
) error {
	reads, err := s.cfg.Target.ReadMany(ctx, keys, s.cfg.Shape)
	if err != nil {
		// A transport failure sinks the batch and the sweep. It is not a
		// finding, and continuing would report every key in the batch as
		// absent — the exact fabrication §23 A5 forbids.
		return fmt.Errorf("reading %d keys: %w", len(keys), err)
	}

	for i, key := range keys {
		// FENCE. Read the version again; if the oracle moved while the target
		// was being read, the comparison is against a value already superseded
		// (§5.5, invariant I12) and the only correct answer is to try again.
		v2, ok := s.cfg.Oracle.Version(key)
		if !ok || v2 != versions[key] {
			s.c.fenceFailures.Add(1)
			s.markRequeued(key)
			continue
		}
		e, ok := s.cfg.Oracle.Get(key)
		if !ok || e.Version != versions[key] {
			s.c.fenceFailures.Add(1)
			s.markRequeued(key)
			continue
		}

		rep.KeysCompared++
		s.c.keysCompared.Add(1)

		f, err := compareRead(key, &e, reads[i], opts)
		if err != nil {
			return err
		}
		s.handleComparison(rep, f, e.Version, key, now, w)
	}
	return nil
}

// compareRead turns one batch slot into a finding, or nil if the key agrees.
func compareRead(
	key string,
	e *oracle.Entry,
	read target.Read,
	opts differ.Options,
) (*differ.Finding, error) {
	if read.Err == nil {
		return differ.Compare(key, *e, read.Value, opts), nil
	}

	// A key holding the wrong type is drift, not a read failure: something
	// wrote a different shape into the index (§9 M8).
	var wrongType *target.WrongTypeError
	if errors.As(read.Err, &wrongType) {
		return differ.CompareUnreadable(key, *e, wrongType.Got, opts), nil
	}
	return nil, fmt.Errorf("reading %q: %w", key, read.Err)
}

// handleComparison routes one comparison outcome: agreement, which may resolve
// an episode; a repeat of an already-confirmed finding; or a fresh candidate.
func (s *Sweeper) handleComparison(
	rep *differ.Report,
	f *differ.Finding,
	version uint64,
	key string,
	now time.Time,
	w time.Duration,
) {
	if f == nil {
		s.resolve(key, now)
		return
	}

	// A key driftwatch does not trust itself about is reported but never
	// confirmed. Confirming is a positive claim that the target is wrong, and
	// driftwatch cannot make that claim about a key whose expectation it built
	// from a stream it knows had holes in it (§5.2, §23 A7).
	//
	// The finding still goes in the report: an operator looking at a sweep
	// should see the disagreement and see that it is not trusted. What it must
	// not do is reach Confirmed(), which is what drives divergent_keys and the
	// subscriber channel — that is the path that ends in somebody being paged.
	if f.Trust == oracle.TrustSuspect {
		s.c.suspectNotConfirmed.Add(1)
		rep.Add(f)
		return
	}

	if episode, ok := s.episode(key); ok {
		// Already confirmed and still wrong. The episode keeps its original
		// first-seen time, so the drift duration measures the whole episode
		// rather than restarting on every sweep.
		f.Confirmed = true
		f.FirstSeenAt = episode.FirstSeenAt
		s.refresh(key, f)
		rep.Add(f)
		return
	}

	f.FirstSeenAt = now
	s.enqueue(f, version, now, w)
	rep.Add(f)
}

// report hands a sweep outcome to the configured callback, if any.
func (s *Sweeper) report(rep *differ.Report, err error) {
	if s.cfg.OnReport != nil {
		s.cfg.OnReport(rep, err)
	}
}

// Confirmed returns the currently-confirmed findings, keyed by key.
func (s *Sweeper) Confirmed() map[string]differ.Finding {
	episodes := s.liveEpisodes()

	out := make(map[string]differ.Finding, len(episodes))
	for key := range episodes {
		f := episodes[key].Finding
		out[key] = f
	}
	return out
}

// Episodes returns the confirmed findings with their timing, which is what
// invariant I7 is asserted against.
func (s *Sweeper) Episodes() map[string]Episode { return s.liveEpisodes() }

// liveEpisodes returns the episodes that still stand, withdrawing the ones that
// no longer do.
//
// Two things retire a confirmed finding without it having been repaired.
//
// The oracle may have forgotten the key, in which case driftwatch can no longer
// say what the target should hold and has to stop claiming anything.
//
// Or the oracle may have moved past the version the finding was raised against.
// A finding is a statement about one specific expectation — "at version 7 the
// target should have held x and held y" — and a new event replaces that
// expectation with a different one. Whether the target is wrong about the new
// value is a question driftwatch has not asked yet, so the old answer is
// withdrawn and the next sweep establishes a fresh one. This is the same rule
// the fence applies during a sweep and the confirm loop applies to a waiting
// candidate (§5.5, invariant I12), enforced at the last place a stale claim
// could survive.
//
// It matters for invariant I11 in particular. Without it a key confirmed while
// settled would stay confirmed after the next event arrived, leaving it
// simultaneously in flight and reported as divergent — which is precisely the
// false positive the settlement window exists to prevent.
//
// The pruning happens here, at the read, because an event arriving is not
// something the sweeper is told about. Doing it lazily is what makes the
// invariant hold at every instant rather than only just after a sweep.
func (s *Sweeper) liveEpisodes() map[string]Episode {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make(map[string]Episode, len(s.confirmed))
	for key := range s.confirmed {
		version, known := s.cfg.Oracle.Version(key)
		switch {
		case !known:
			delete(s.confirmed, key)
			s.c.confirmedDroppedEvicted.Add(1)
		case version != s.confirmed[key].Finding.OracleVersion:
			delete(s.confirmed, key)
			s.c.confirmedSuperseded.Add(1)
		default:
			out[key] = s.confirmed[key]
		}
	}
	return out
}

// Requeued returns the keys deferred to a later sweep, either because the fence
// failed or because the oracle advanced while a candidate was waiting.
func (s *Sweeper) Requeued() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]string, 0, len(s.requeued))
	for key := range s.requeued {
		out = append(out, key)
	}
	return out
}

// Subscribe returns a channel of confirmed findings for the reporter.
//
// The channel is buffered, and a send that would block is dropped and counted
// rather than stalling the confirm loop: a slow reporter must not be able to
// stop driftwatch detecting drift.
func (s *Sweeper) Subscribe() <-chan differ.Finding {
	s.mu.Lock()
	defer s.mu.Unlock()

	ch := make(chan differ.Finding, defaultSubscriberBuffer)
	if s.closed {
		close(ch)
		return ch
	}
	s.subs = append(s.subs, ch)
	return ch
}

// Stats returns a snapshot of the counters.
func (s *Sweeper) Stats() Stats {
	confirmed := len(s.liveEpisodes())

	s.mu.Lock()
	pending := len(s.queue)
	pendingExtras := 0
	if s.extras != nil {
		pendingExtras = len(s.extras.keys)
	}
	s.mu.Unlock()

	return Stats{
		Sweeps:        s.c.sweeps.Load(),
		SweepsSkipped: s.c.sweepsSkipped.Load(),
		KeysCompared:  s.c.keysCompared.Load(),
		FenceFailures: s.c.fenceFailures.Load(),

		CandidatesEnqueued:  s.c.candidatesEnqueued.Load(),
		ConfirmQueueDropped: s.c.confirmQueueDropped.Load(),
		ConfirmCycles:       s.c.confirmCycles.Load(),
		Confirmations:       s.c.confirmations.Load(),

		TransientResolved:       s.c.transientResolved.Load(),
		TransientOracleAdvanced: s.c.transientOracleAdvanced.Load(),
		TransientKeyEvicted:     s.c.transientKeyEvicted.Load(),
		ConfirmReadFailed:       s.c.confirmReadFailed.Load(),

		DriftResolved:           s.c.driftResolved.Load(),
		ConfirmedDroppedEvicted: s.c.confirmedDroppedEvicted.Load(),
		ConfirmedSuperseded:     s.c.confirmedSuperseded.Load(),
		SuspectNotConfirmed:     s.c.suspectNotConfirmed.Load(),
		LastDriftDuration:       time.Duration(s.c.lastDriftDuration.Load()),

		ExtraScans:         s.c.extraScans.Load(),
		ExtrasReported:     s.c.extrasReported.Load(),
		ExtrasSelfResolved: s.c.extrasSelfResolved.Load(),
		ExtrasTruncated:    s.c.extrasTruncated.Load(),

		TargetUnavailable: s.c.targetUnavailable.Load(),
		SubscriberDropped: s.c.subscriberDropped.Load(),

		CurrentlyConfirmed:   confirmed,
		PendingConfirmations: pending,
		PendingExtras:        pendingExtras,
	}
}

// Close releases the sweeper and closes every subscriber channel. Idempotent.
func (s *Sweeper) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true
	for _, ch := range s.subs {
		close(ch)
	}
	s.subs = nil
	return nil
}

func (s *Sweeper) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// markRequeued notes a key to be revisited. The set is bounded by the oracle's
// own key budget, since only tracked keys ever enter it.
func (s *Sweeper) markRequeued(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requeued[key] = struct{}{}
}

// drainRequeued clears the set at the start of a sweep.
//
// The keys do not need re-adding to a work list: the next sweep walks every
// settled key anyway, so the set exists to make the deferral visible rather
// than to schedule it.
func (s *Sweeper) drainRequeued() {
	s.mu.Lock()
	defer s.mu.Unlock()
	clear(s.requeued)
}

// episode returns the confirmed episode for a key.
func (s *Sweeper) episode(key string) (Episode, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	episode, ok := s.confirmed[key]
	return episode, ok
}

// refresh replaces a confirmed episode's finding while keeping its timing.
func (s *Sweeper) refresh(key string, f *differ.Finding) {
	s.mu.Lock()
	defer s.mu.Unlock()

	episode, ok := s.confirmed[key]
	if !ok {
		return
	}
	episode.Finding = *f
	s.confirmed[key] = episode
}

// resolve removes a confirmed episode because the key now agrees.
//
// This is the half of the design that makes the other half usable. Without it
// driftwatch_divergent_keys never returns to zero after a repair, the alert
// never clears, and whoever is on call learns that the metric lies (§9 M10).
func (s *Sweeper) resolve(key string, now time.Time) {
	s.mu.Lock()
	episode, ok := s.confirmed[key]
	if ok {
		delete(s.confirmed, key)
	}
	s.mu.Unlock()

	if !ok {
		return
	}
	s.c.driftResolved.Add(1)
	s.c.lastDriftDuration.Store(int64(now.Sub(episode.FirstSeenAt)))
}

// publish delivers a confirmed finding to every subscriber.
func (s *Sweeper) publish(f *differ.Finding) {
	s.mu.Lock()
	subs := make([]chan differ.Finding, len(s.subs))
	copy(subs, s.subs)
	s.mu.Unlock()

	for _, ch := range subs {
		select {
		case ch <- *f:
		default:
			s.c.subscriberDropped.Add(1)
		}
	}
}
