package sweeper_test

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/differ"
	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/seqtrack"
	"github.com/nabrahma/driftwatch/pkg/sweeper"
	"github.com/nabrahma/driftwatch/pkg/target"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// go-redis starts a process-wide time cache at package init and never
		// stops it. This package links the client transitively through
		// pkg/target, so the goroutine exists even though no test here opens a
		// connection.
		//
		// §16.5 permits an ignore for a third-party goroutine with a reason.
		// None of driftwatch's own goroutines is ignored here, and one of those
		// outliving a test would be a bug to fix rather than an entry to add.
		goleak.IgnoreAnyFunction("github.com/redis/go-redis/v9/internal/pool.startGlobalTimeCache.func1"),

		// go-winio's IO completion processor, started once by the Docker client
		// and never stopped. Reachable only from testcontainers, so it appears
		// under the integration build tag — BenchmarkFullSweep1M — on Windows,
		// and the matcher finds nothing in a normal unit run.
		//
		// Same reasoning and same wording as pkg/target/recording_test.go,
		// where it was first needed. IgnoreAnyFunction rather than
		// IgnoreTopFunction because the goroutine parks in syscall.syscalln and
		// the name worth matching is several frames down.
		goleak.IgnoreAnyFunction("github.com/Microsoft/go-winio.ioCompletionProcessor"),
	)
}

// window is the settlement window every test in this file runs with. It is a
// round number so that "just inside it" and "just outside it" read as intent at
// the call site rather than as arithmetic.
const window = 5 * time.Second

func epoch() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }

// harnessConfig is what a test may vary. Both halves are here because several
// tests need to change the oracle and the sweeper together.
type harnessConfig struct {
	oracle  oracle.Config
	sweeper sweeper.Config
	memory  []target.MemoryOption
	// wrap inserts a decorator between the RecordingTarget and the sweeper, so
	// a test can fail one operation without failing all of them.
	wrap func(target.Target) target.Target
}

// harness wires an oracle, an in-memory target and a sweeper over one fake
// clock.
//
// The target is wrapped in a RecordingTarget for every test in this package, so
// invariant I13 is asserted continuously rather than in one dedicated test: a
// sweep that issued a write would fail whichever test happened to run it.
type harness struct {
	t   *testing.T
	clk clock.FakeClock
	orc *oracle.Oracle
	mem *target.MemoryTarget
	rec *target.RecordingTarget
	swp *sweeper.Sweeper

	seq uint64
}

func newHarness(t *testing.T, tweaks ...func(*harnessConfig)) *harness {
	t.Helper()

	clk := clock.Fake(epoch())

	hc := harnessConfig{
		oracle: oracle.Config{Clock: clk, SettlementWindow: window},
		sweeper: sweeper.Config{
			Shape:            projection.ShapeScalar,
			Clock:            clk,
			SettlementWindow: func() time.Duration { return window },
		},
	}
	for _, tweak := range tweaks {
		tweak(&hc)
	}

	mem := target.NewMemory(append([]target.MemoryOption{target.WithClock(clk)}, hc.memory...)...)
	rec := target.Recording(t, mem)

	hc.sweeper.Target = target.Target(rec)
	if hc.wrap != nil {
		hc.sweeper.Target = hc.wrap(rec)
	}

	orc := oracle.New(hc.oracle)
	hc.sweeper.Oracle = orc

	h := &harness{t: t, clk: clk, orc: orc, mem: mem, rec: rec, swp: sweeper.New(hc.sweeper)}
	t.Cleanup(func() { require.NoError(t, h.swp.Close()) })
	return h
}

// apply feeds one event into the oracle, as the applier would.
func (h *harness) apply(key, value string) uint64 {
	h.t.Helper()
	h.seq++

	e := &event.Event{
		Publisher: "p", Epoch: 1, Seq: h.seq, Op: event.OpSet,
		Key: key, Value: []byte(value), ObservedAt: h.clk.Now(),
	}
	res := h.orc.Apply(projection.Mutation{
		Key:    key,
		Action: projection.ActionUpsert,
		Value:  event.Value{Kind: event.ValueScalar, Scalar: []byte(value)},
	}, e, seqtrack.Accept, oracle.TrustComplete)
	return res.Version
}

// applyDelete feeds a delete into the oracle, which leaves a tombstone rather
// than removing the entry.
func (h *harness) applyDelete(key string) uint64 {
	h.t.Helper()
	h.seq++

	e := &event.Event{
		Publisher: "p", Epoch: 1, Seq: h.seq, Op: event.OpDelete,
		Key: key, ObservedAt: h.clk.Now(),
	}
	res := h.orc.Apply(projection.Mutation{
		Key:    key,
		Action: projection.ActionDelete,
	}, e, seqtrack.Accept, oracle.TrustComplete)
	return res.Version
}

// materialize writes to the store, standing in for the real materializer. It
// runs inside Fixture because it is the test's write, not driftwatch's.
func (h *harness) materialize(key, value string) {
	h.t.Helper()
	h.rec.Fixture(func() { h.mem.Seed(map[string][]byte{key: []byte(value)}) })
}

// unmaterialize deletes from the store, standing in for a dropped write.
func (h *harness) unmaterialize(keys ...string) {
	h.t.Helper()
	h.rec.Fixture(func() { h.mem.Remove(keys...) })
}

func (h *harness) setHealth(fn func(*target.Health)) {
	h.t.Helper()

	got, err := h.mem.Health(context.Background())
	require.NoError(h.t, err)
	fn(&got)
	h.rec.Fixture(func() { h.mem.SetHealth(got) })
}

func (h *harness) advance(d time.Duration) { h.clk.Advance(d) }

// settle moves past the settlement window, so keys touched so far become
// eligible for comparison.
func (h *harness) settle() { h.advance(window + time.Second) }

func (h *harness) sweep() *differ.Report {
	h.t.Helper()

	rep, err := h.swp.SweepOnce(context.Background())
	require.NoError(h.t, err)
	return rep
}

// confirmDue runs the confirmation pass for every candidate whose wait has
// elapsed, which is what the Run loop's ticker does.
func (h *harness) confirmDue() int {
	h.t.Helper()
	return h.swp.ConfirmDue(context.Background(), h.clk.Now())
}

// ---------------------------------------------------------------------------
// The four behaviors §9 M10 names explicitly.
// ---------------------------------------------------------------------------

func TestSweeper_TransientDivergenceIsNeverReported(t *testing.T) {
	// This is the point of the whole design. A materializer that is merely
	// behind must never be indistinguishable from one that is wrong, and two
	// independent mechanisms stand between the two: the settlement window keeps
	// a recently-changed key out of the comparison entirely, and two-phase
	// confirmation discards a candidate that has caught up by the time it is
	// re-read. Either one failing alone produces exactly the false positive
	// §23 A7 says makes the tool worthless, so both are tested.

	t.Run("a key inside its settlement window is not compared at all", func(t *testing.T) {
		h := newHarness(t)

		h.apply("k", "v1")
		// The target has not caught up. Sweeping now would find a disagreement.
		h.advance(window - time.Millisecond)

		rep := h.sweep()

		assert.Equal(t, 0, rep.KeysCompared, "a key inside W must not be compared")
		assert.Equal(t, 1, rep.KeysSkippedInFlight)
		assert.Empty(t, rep.Findings)
		assert.Empty(t, h.swp.Confirmed())
		assert.Zero(t, h.swp.Stats().CandidatesEnqueued)
	})

	t.Run("a candidate that catches up before confirmation is discarded", func(t *testing.T) {
		h := newHarness(t)

		h.materialize("k", "stale")
		h.apply("k", "v1")
		h.settle()

		// The sweep finds the disagreement and raises a candidate. Nothing is
		// reported yet.
		rep := h.sweep()
		require.Equal(t, 1, rep.KeysCompared)
		require.Len(t, rep.Findings, 1)
		require.False(t, rep.Findings[0].Confirmed)
		require.Empty(t, h.swp.Confirmed(), "one disagreeing read is never a report")

		// The materializer catches up while the candidate is waiting.
		h.materialize("k", "v1")
		h.advance(window)

		require.Equal(t, 1, h.confirmDue())

		assert.Empty(t, h.swp.Confirmed(), "a resolved candidate is not drift")
		assert.Equal(t, int64(1), h.swp.Stats().TransientResolved)
		assert.Zero(t, h.swp.Stats().Confirmations)
	})
}

func TestSweeper_RealDivergenceIsReportedAfterExactlyOneConfirmCycle(t *testing.T) {
	// The counterpart to the test above: the caution must not be so thorough
	// that genuine drift is never reported either.
	h := newHarness(t)
	findings := h.swp.Subscribe()

	h.materialize("k", "wrong")
	h.apply("k", "right")
	h.settle()

	rep := h.sweep()
	require.Len(t, rep.Findings, 1)
	require.Empty(t, h.swp.Confirmed(), "not reported on the strength of one read")
	require.Equal(t, int64(1), h.swp.Stats().CandidatesEnqueued)

	// Confirmation is not due until a full W after the candidate was raised.
	h.advance(window - time.Millisecond)
	require.Zero(t, h.confirmDue(), "the wait is not over yet")
	require.Empty(t, h.swp.Confirmed())

	h.advance(time.Millisecond)
	require.Equal(t, 1, h.confirmDue())

	confirmed := h.swp.Confirmed()
	require.Len(t, confirmed, 1)
	got := confirmed["k"]
	assert.True(t, got.Confirmed)
	assert.Equal(t, differ.CatValueMismatch, got.Category)
	assert.Equal(t, []byte("right"), got.OracleValue.Scalar)
	assert.Equal(t, []byte("wrong"), got.TargetValue.Scalar)

	assert.Equal(t, int64(1), h.swp.Stats().ConfirmCycles,
		"exactly one confirm cycle, not a re-confirmation loop")
	assert.Equal(t, int64(1), h.swp.Stats().Confirmations)

	select {
	case f := <-findings:
		assert.Equal(t, "k", f.Key)
		assert.True(t, f.Confirmed)
	default:
		t.Fatal("a confirmed finding must reach subscribers")
	}
}

func TestSweeper_DriftResolutionRemovesTheFindingAndCountsIt(t *testing.T) {
	// Without this the alert never clears. driftwatch_divergent_keys stays
	// above zero after the drift is repaired, whoever is on call learns the
	// metric lies, and every later alert is ignored. §9 M10 is blunt about the
	// consequence: "which makes the whole tool useless".
	h := newHarness(t)

	h.materialize("k", "wrong")
	h.apply("k", "right")
	h.settle()

	h.sweep()
	h.advance(window)
	require.Equal(t, 1, h.confirmDue())
	require.Len(t, h.swp.Confirmed(), 1)

	episode, ok := h.swp.Episodes()["k"]
	require.True(t, ok)
	firstSeen := episode.FirstSeenAt

	// Somebody repairs the target. There is no new event: the oracle was right
	// all along, so it does not change and the key stays settled.
	h.materialize("k", "right")
	h.advance(time.Minute)

	rep := h.sweep()

	assert.Empty(t, rep.Findings)
	assert.Empty(t, h.swp.Confirmed(), "a repaired key must leave Confirmed()")
	assert.Equal(t, int64(1), h.swp.Stats().DriftResolved)
	assert.Equal(t, h.clk.Now().Sub(firstSeen), h.swp.Stats().LastDriftDuration,
		"the episode is measured from the first disagreeing read")
}

func TestSweeper_NothingIsReportedWhileTheTargetIsUnreachable(t *testing.T) {
	// §23 A5: absence of data is not evidence of divergence. An unreachable
	// store answers nothing, and answering nothing looks exactly like answering
	// "the key is gone" to any code that forgets to check.
	h := newHarness(t)

	h.materialize("a", "v")
	h.materialize("b", "v")
	h.apply("a", "v")
	h.apply("b", "v")
	h.settle()

	t.Run("no candidate is raised", func(t *testing.T) {
		h.setHealth(func(hl *target.Health) { hl.Reachable = false })

		rep, err := h.swp.SweepOnce(context.Background())

		require.ErrorIs(t, err, sweeper.ErrTargetUnavailable)
		assert.Nil(t, rep, "an unreachable target produces no report at all")
		assert.Zero(t, h.swp.Stats().CandidatesEnqueued)
		assert.Zero(t, h.swp.Stats().KeysCompared)
		assert.Equal(t, int64(1), h.swp.Stats().TargetUnavailable)
	})

	t.Run("an existing finding is neither confirmed nor resolved", func(t *testing.T) {
		// The dangerous direction. A confirmed finding must not be quietly
		// resolved because the store stopped answering, and a waiting candidate
		// must not be confirmed on the strength of a failed read.
		h.setHealth(func(hl *target.Health) { hl.Reachable = true })
		h.unmaterialize("a")
		h.advance(time.Minute)

		h.sweep()
		h.advance(window)
		require.Equal(t, 1, h.confirmDue())
		require.Len(t, h.swp.Confirmed(), 1)

		h.setHealth(func(hl *target.Health) { hl.Reachable = false })
		h.advance(time.Minute)

		_, err := h.swp.SweepOnce(context.Background())
		require.ErrorIs(t, err, sweeper.ErrTargetUnavailable)

		assert.Len(t, h.swp.Confirmed(), 1, "the finding survives the outage")
		assert.Zero(t, h.swp.Stats().DriftResolved,
			"an unreachable store must never look like a repair")
	})
}

// ---------------------------------------------------------------------------
// The numbered steps of the SweepOnce algorithm, and their failure modes.
// ---------------------------------------------------------------------------

func TestSweeper_RefusesToSweepAReplicaWhenPrimaryIsRequired(t *testing.T) {
	// Reading a replica means reading state that is legitimately behind, which
	// manufactures drift that does not exist.
	h := newHarness(t, func(c *harnessConfig) { c.sweeper.RequirePrimary = true })

	h.setHealth(func(hl *target.Health) { hl.Role = "replica" })

	_, err := h.swp.SweepOnce(context.Background())

	require.ErrorIs(t, err, sweeper.ErrNotPrimary)
	assert.Zero(t, h.swp.Stats().KeysCompared)
}

func TestSweeper_FenceFailureRequeuesRatherThanReporting(t *testing.T) {
	// §5.5: the version is read, the target is read, the version is read again.
	// If the oracle moved in between, the comparison would be against a value
	// driftwatch has already superseded — invariant I12 — and the only correct
	// answer is to try again next sweep.
	h := newHarness(t)

	h.materialize("k", "v1")
	h.apply("k", "v1")
	h.settle()

	// A new event lands between the two version reads. The hook fires while the
	// batch read is in flight, which is exactly the race the fence exists for.
	h.mem.ObserveCommands(func(name string) {
		if name == "GET" {
			h.apply("k", "v2")
		}
	})

	rep := h.sweep()

	assert.Equal(t, int64(1), h.swp.Stats().FenceFailures)
	assert.Empty(t, rep.Findings, "a fenced-off comparison is not a finding")
	assert.Contains(t, h.swp.Requeued(), "k")
}

func TestSweeper_AdoptedKeysAreNotCompared(t *testing.T) {
	// §5.6: an adopted key was read out of the target at startup, so comparing
	// it against the target proves only that the target agrees with itself.
	h := newHarness(t)

	h.orc.AdoptSnapshot(map[string]event.Value{
		"adopted": {Kind: event.ValueScalar, Scalar: []byte("v")},
	}, h.clk.Now())
	h.apply("evented", "v")
	h.materialize("evented", "v")
	h.settle()

	rep := h.sweep()

	assert.Equal(t, 1, rep.KeysCompared, "only the evented key is comparable")
	assert.Empty(t, rep.Findings)
}

func TestSweeper_ZeroSettledKeysIsAValidSweep(t *testing.T) {
	// An empty answer and a broken sweep must not look alike.
	h := newHarness(t)

	rep := h.sweep()

	require.NotNil(t, rep)
	assert.Equal(t, 0, rep.KeysCompared)
	assert.Empty(t, rep.Findings)
	assert.Equal(t, int64(1), h.swp.Stats().Sweeps)
}

func TestSweeper_MassAbsenceDuringEvictionSaysSo(t *testing.T) {
	// §5.7: a sweep that finds mass missing_in_target at the same moment the
	// store's eviction counter jumped has an obvious explanation, and saying so
	// in the output saves the operator an hour.
	//
	// The eviction has to happen *during* the sweep, because that is the only
	// thing the before/after counters can detect. A store that evicted before
	// the sweep started shows the same counter at both ends and is
	// indistinguishable from one that never evicted at all — which is a real
	// limit of the mechanism, and the reason this test injects mid-sweep rather
	// than up front.
	const keys = 20
	h := newHarness(t, func(c *harnessConfig) { c.sweeper.ReadBatchSize = 5 })

	for i := 0; i < keys; i++ {
		key := "k" + strconv.Itoa(i)
		h.materialize(key, "v")
		h.apply(key, "v")
	}
	h.settle()

	// Let the first batch read cleanly, then take the store out from under the
	// remaining three.
	var batches int
	h.mem.ObserveCommands(func(name string) {
		if name != "GET" {
			return
		}
		batches++
		if batches == 2 {
			h.rec.Fixture(func() { h.mem.SimulateEvict(keys) })
		}
	})

	rep := h.sweep()

	assert.Equal(t, keys, rep.KeysCompared)
	assert.Len(t, rep.Findings, 15, "the three batches read after the eviction")
	assert.True(t, rep.EvictionSuspected,
		"the eviction counter moved during the sweep and the report must say so")
	for _, f := range rep.Findings {
		assert.Equal(t, differ.CatMissingInTarget, f.Category)
	}
}

func TestSweeper_ReadsAreBatchedAtTheConfiguredSize(t *testing.T) {
	h := newHarness(t, func(c *harnessConfig) { c.sweeper.ReadBatchSize = 4 })

	for i := 0; i < 10; i++ {
		key := "k" + strconv.Itoa(i)
		h.materialize(key, "v")
		h.apply(key, "v")
	}
	h.settle()

	before := h.rec.Calls()["ReadMany"]
	rep := h.sweep()

	assert.Equal(t, 10, rep.KeysCompared)
	assert.Equal(t, 3, h.rec.Calls()["ReadMany"]-before, "10 keys in batches of 4")
}

func TestSweeper_ContextCancellationAbortsPromptly(t *testing.T) {
	h := newHarness(t)

	for i := 0; i < 100; i++ {
		key := "k" + strconv.Itoa(i)
		h.materialize(key, "v")
		h.apply(key, "v")
	}
	h.settle()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := h.swp.SweepOnce(ctx)

	require.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled),
		"a canceled sweep reports the cancellation, got %v", err)
	assert.Zero(t, h.swp.Stats().TargetUnavailable,
		"our own cancellation is not the store being unreachable")
}

func TestSweeper_ConfirmQueueIsBounded(t *testing.T) {
	// §5.4: under mass divergence you do not need to individually confirm every
	// key, you need to know the magnitude. Dropping is correct; dropping
	// silently is not.
	h := newHarness(t, func(c *harnessConfig) { c.sweeper.MaxConfirmQueue = 5 })

	for i := 0; i < 20; i++ {
		key := "k" + strconv.Itoa(i)
		h.materialize(key, "wrong")
		h.apply(key, "right")
	}
	h.settle()

	rep := h.sweep()

	assert.Len(t, rep.Findings, 20, "the report keeps the full magnitude")
	assert.Equal(t, int64(5), h.swp.Stats().CandidatesEnqueued)
	assert.Equal(t, int64(15), h.swp.Stats().ConfirmQueueDropped)
}

func TestSweeper_ConfirmationDiscardsAKeyTheOracleMovedOn(t *testing.T) {
	h := newHarness(t)

	h.materialize("k", "wrong")
	h.apply("k", "right")
	h.settle()
	h.sweep()

	// A new event arrives while the candidate waits. What was queued is a
	// disagreement about a value that is no longer current.
	h.apply("k", "newer")
	h.advance(window)

	require.Equal(t, 1, h.confirmDue())

	assert.Empty(t, h.swp.Confirmed())
	assert.Equal(t, int64(1), h.swp.Stats().TransientOracleAdvanced)
	assert.Contains(t, h.swp.Requeued(), "k")
}

func TestSweeper_ConfirmationDiscardsAKeyEvictedFromTheOracle(t *testing.T) {
	h := newHarness(t, func(c *harnessConfig) {
		c.oracle.MaxTrackedKeys = 64
		c.oracle.Shards = 1
	})

	h.materialize("k", "wrong")
	h.apply("k", "right")
	h.settle()
	h.sweep()
	require.Equal(t, int64(1), h.swp.Stats().CandidatesEnqueued)

	// Fill the oracle well past its budget so the candidate's key is evicted.
	for i := 0; i < 256; i++ {
		h.apply("filler"+strconv.Itoa(i), "v")
	}
	h.advance(window)
	require.Equal(t, 1, h.confirmDue())

	assert.Empty(t, h.swp.Confirmed())
	assert.Equal(t, int64(1), h.swp.Stats().TransientKeyEvicted)
}

func TestSweeper_AConfirmedKeyEvictedFromTheOracleIsDropped(t *testing.T) {
	// Keeping it would mean alerting forever about a key driftwatch can no
	// longer form an expectation for, which is a claim it cannot support.
	h := newHarness(t, func(c *harnessConfig) {
		c.oracle.MaxTrackedKeys = 64
		c.oracle.Shards = 1
	})

	h.materialize("k", "wrong")
	h.apply("k", "right")
	h.settle()
	h.sweep()
	h.advance(window)
	require.Equal(t, 1, h.confirmDue())
	require.Len(t, h.swp.Confirmed(), 1)

	for i := 0; i < 256; i++ {
		h.apply("filler"+strconv.Itoa(i), "v")
	}
	h.advance(time.Minute)
	h.sweep()

	assert.Empty(t, h.swp.Confirmed())
	assert.Equal(t, int64(1), h.swp.Stats().ConfirmedDroppedEvicted)
	assert.Zero(t, h.swp.Stats().DriftResolved,
		"an evicted key is not a repaired key and must not be counted as one")
}

func TestSweeper_OverlappingSweepsAreSkippedRatherThanQueued(t *testing.T) {
	h := newHarness(t)

	h.apply("k", "v")
	h.materialize("k", "v")
	h.settle()

	started := make(chan struct{})
	release := make(chan struct{})
	var once bool

	h.mem.ObserveCommands(func(name string) {
		if name != "GET" || once {
			return
		}
		once = true
		close(started)
		<-release
	})

	done := make(chan struct{})
	var sweepErr error
	go func() {
		defer close(done)
		_, sweepErr = h.swp.SweepOnce(context.Background())
	}()

	<-started
	ran := h.swp.TrySweepOnce(context.Background())
	close(release)
	<-done
	require.NoError(t, sweepErr)

	assert.False(t, ran, "a sweep already in progress must be skipped, not queued")
	assert.Equal(t, int64(1), h.swp.Stats().SweepsSkipped)
}

func TestSweeper_WIsCapturedOnceAtSweepStart(t *testing.T) {
	// §9 M10 edge case. A W that changed mid-sweep would mean the first half of
	// the keyspace was judged by one rule and the second half by another.
	var w atomic.Int64
	w.Store(int64(window))

	h := newHarness(t, func(c *harnessConfig) {
		c.sweeper.SettlementWindow = func() time.Duration { return time.Duration(w.Load()) }
	})

	h.apply("k", "v")
	h.materialize("k", "v")
	h.settle()

	var reads int
	h.mem.ObserveCommands(func(name string) {
		if name == "GET" {
			reads++
			w.Store(int64(time.Hour))
		}
	})

	rep := h.sweep()

	require.Positive(t, reads)
	assert.Equal(t, window, rep.SettlementWindow,
		"the report states the window the sweep actually used")
}
