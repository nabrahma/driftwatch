package faults

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/differ"
	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/lag"
	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/seqtrack"
	"github.com/nabrahma/driftwatch/pkg/sweeper"
	"github.com/nabrahma/driftwatch/pkg/target"
)

// §15.3 — driftwatch's own faults.
//
// Every row of the matrix in §15 is one test, and the "Expected" column is the
// assertion: if the implementation does something else, the implementation is
// wrong. This file holds rows 47 to 54.
//
// Each test registers its row number so TestFaultMatrix_Coverage can prove the
// range is actually covered rather than merely claimed.

const faultWindow = 5 * time.Second

func faultEpoch() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }

// faultRows maps every row of §15 to the test that covers it.
//
// The values are the test functions themselves rather than their names, so
// deleting or renaming a test breaks the build here. A table of strings would
// let a row quietly become uncovered while still claiming to be covered, which
// is worse than having no table at all.
//
// The name is checked too, in TestFaultMatrix_Coverage: without that, a
// copy-paste slip could point two rows at one test and the table would still
// look complete.
var faultRows = map[int]func(*testing.T){
	// §15.1 — event-stream faults.
	1:  TestFault01_SingleEventDroppedFromTheMaterializer,
	2:  TestFault02_SingleEventDroppedFromDriftwatch,
	3:  TestFault03_BurstOfAHundredDroppedFromTheMaterializer,
	4:  TestFault04_BurstOfAHundredDroppedFromDriftwatch,
	5:  TestFault05_SustainedDropRateStaysBoundedAndMonotonic,
	6:  TestFault06_AdjacentPairReorderedOnTheMaterializer,
	7:  TestFault07_AdjacentPairReorderedOnDriftwatch,
	8:  TestFault08_WindowShuffleOverTenThousandEvents,
	9:  TestFault09_ImmediateDuplicateIsAbsorbed,
	10: TestFault10_DelayedDuplicateDoesNotResetSettlement,
	11: TestFault11_UniformDelayBeyondTheStaticWindow,
	12: TestFault12_OnePublisherDelayedAffectsOnlyItsOwnKeys,
	13: TestFault13_PartitionOfDriftwatchsSource,
	14: TestFault14_PartitionOfTheMaterializersSource,
	15: TestFault15_OnePercentCorruptPayloads,
	16: TestFault16_FiftyPercentCorruptPayloads,
	17: TestFault17_TruncatedPayload,
	18: TestFault18_OversizedPayloadIsRefusedWithoutAllocating,
	19: TestFault19_UnknownOpCode,
	20: TestFault20_ExplicitRestartWithAnEpochBump,
	21: TestFault21_ImplicitRestart,
	22: TestFault22_StaleEventFromAPreviousEpoch,
	23: TestFault23_PublisherClockFiveMinutesAhead,
	24: TestFault24_PublisherClockFiveMinutesBehind,
	25: TestFault25_TwoPublishersWritingTheSameKey,
	26: TestFault26_HeartbeatOnlyStream,
	27: TestFault27_FifteenHundredPublishers,

	// §15.2 — target-store faults.
	28: TestFault28_FlushdbMidRun,
	29: TestFault29_EvictionUnderMaxmemory,
	30: TestFault30_TTLExpiryUnderStrictPolicy,
	31: TestFault31_TTLExpiryUnderIgnorePolicy,
	32: TestFault32_TTLExpiryUnderModelPolicy,
	33: TestFault33_OutOfBandWrite,
	34: TestFault34_OutOfBandDelete,
	35: TestFault35_WrongTypeIsDriftRatherThanAnError,
	36: TestFault36_TargetUnreachable,
	37: TestFault37_TargetHighLatency,
	38: TestFault38_TargetRestartWithoutPersistence,
	39: TestFault39_FailoverToALaggingReplica,
	40: TestFault40_ScanCursorResetByAConcurrentFlush,
	41: TestFault41_KeyAddedMidScanIsNotAnExtra,
	42: TestFault42_KeyRemovedMidScanIsNotAnExtra,
	43: TestFault43_EmptyTargetAndAFullOracle,
	44: TestFault44_FullTargetEmptyOracleUnderAdopt,
	45: TestFault45_FullTargetEmptyOracleUnderWait,
	46: TestFault46_FullTargetEmptyOracleUnderStrict,

	// §15.3 — driftwatch's own faults.
	47: TestFault47_DroppedEventsMakeKeysSuspectRatherThanWrong,
	48: TestFault48_OracleSaturationDegradesCoverageRatherThanReporting,
	49: TestFault49_ConfirmQueueFullDropsCandidatesAndKeepsFindings,
	50: TestFault50_ASweepThatOverrunsIsSkippedNotStacked,
	51: TestFault51_AHotKeyIsEventuallyComparedRatherThanSkippedForever,
	52: TestFault52_AdaptiveWindowClampsAtItsMaximumAndSaysSo,
	53: TestFault53_NoObservationsMeansTheFloorAndNoBusyLoop,
	54: TestFault54_ContextCancellationAbortsASweepPromptly,
	55: TestFault55_PanicInTheApplierIsContained,
	56: TestFault56_ProjectionRejectsEveryEvent,
	57: TestFault57_CloseCalledTwice,
	58: TestFault58_CloseDuringBootstrap,
	59: TestFault59_ImmutableFieldsCannotChange,
	60: TestFault60_FiftyConcurrentChecksInOneManager,
}

// funcName returns a test function's unqualified name, so the table can be
// checked against t.Name().
func funcName(fn func(*testing.T)) string {
	full := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
	if i := strings.LastIndex(full, "."); i >= 0 {
		return full[i+1:]
	}
	return full
}

// rig is the smallest thing that can exhibit an internal fault: an oracle, a
// store, and a sweeper over one fake clock.
type rig struct {
	t   *testing.T
	clk clock.FakeClock
	orc *oracle.Oracle
	mem *target.MemoryTarget
	rec *target.RecordingTarget
	swp *sweeper.Sweeper
	seq uint64
}

// newRig builds the rig. Both configs are by value so each test's copy is
// independent, matching the constructors they are handed to.
//
//nolint:gocritic // hugeParam: deliberate, see above.
func newRig(t *testing.T, oc oracle.Config, sc sweeper.Config) *rig {
	t.Helper()

	clk := clock.Fake(faultEpoch())
	mem := target.NewMemory(target.WithClock(clk))
	rec := target.Recording(t, mem)

	oc.Clock = clk
	if oc.SettlementWindow == 0 {
		oc.SettlementWindow = faultWindow
	}
	orc := oracle.New(oc)

	sc.Oracle = orc
	sc.Target = rec
	sc.Clock = clk
	sc.Shape = projection.ShapeScalar
	if sc.SettlementWindow == nil {
		sc.SettlementWindow = func() time.Duration { return faultWindow }
	}

	r := &rig{t: t, clk: clk, orc: orc, mem: mem, rec: rec, swp: sweeper.New(sc)}
	t.Cleanup(func() { require.NoError(t, r.swp.Close()) })
	return r
}

func (r *rig) apply(key, value string) {
	r.t.Helper()
	r.seq++

	e := &event.Event{
		Publisher: "p", Epoch: 1, Seq: r.seq, Op: event.OpSet,
		Key: key, Value: []byte(value), ObservedAt: r.clk.Now(),
	}
	r.orc.Apply(projection.Mutation{
		Key:    key,
		Action: projection.ActionUpsert,
		Value:  event.Value{Kind: event.ValueScalar, Scalar: []byte(value)},
	}, e, seqtrack.Accept, oracle.TrustComplete)
}

func (r *rig) materialize(key, value string) {
	r.t.Helper()
	r.rec.Fixture(func() { r.mem.Seed(map[string][]byte{key: []byte(value)}) })
}

func (r *rig) sweep() *differ.Report {
	r.t.Helper()

	rep, err := r.swp.SweepOnce(context.Background())
	require.NoError(r.t, err)
	return rep
}

func (r *rig) confirm() int {
	r.t.Helper()
	return r.swp.ConfirmDue(context.Background(), r.clk.Now())
}

func (r *rig) settle() { r.clk.Advance(faultWindow + time.Second) }

// ---------------------------------------------------------------------------

func TestFault47_DroppedEventsMakeKeysSuspectRatherThanWrong(t *testing.T) {
	// The full row — publishing ten times faster than the decoder and watching
	// events_dropped_total rise — needs pkg/source. What is
	// testable now is the half that matters for correctness: once driftwatch
	// knows it lost events, it must stop claiming the target is wrong.
	//
	// A dropped event is a lost event, and a lost event means the oracle's
	// expectation is built from an incomplete stream. Reporting drift on that
	// basis is reporting driftwatch's own gap as the target's fault.
	r := newRig(t, oracle.Config{}, sweeper.Config{})

	for i := 0; i < 10; i++ {
		key := "k" + strconv.Itoa(i)
		r.apply(key, "right")
		r.materialize(key, "wrong")
	}
	r.settle()

	// The ingest buffer overflowed and events were dropped.
	r.orc.MarkSuspect("", "buffer full")

	rep := r.sweep()

	require.Equal(t, 10, len(rep.Findings), "the disagreement is still visible")
	assert.Zero(t, rep.Alertable(),
		"but none of it is alertable, because driftwatch knows its view is incomplete")
	assert.Equal(t, 10, rep.ByTrust[oracle.TrustSuspect])
}

func TestFault48_OracleSaturationDegradesCoverageRatherThanReporting(t *testing.T) {
	// The oracle is bounded, so a keyspace larger than expected costs coverage.
	// What it must never cost is correctness: an evicted key is one driftwatch
	// has no expectation for, and a key with no expectation cannot be drift.
	const budget = 64
	r := newRig(t, oracle.Config{MaxTrackedKeys: budget, Shards: 1}, sweeper.Config{})

	for i := 0; i < 500; i++ {
		key := "k" + strconv.Itoa(i)
		r.apply(key, "v")
		// Deliberately never materialized: every tracked key disagrees.
	}
	r.settle()

	rep := r.sweep()

	assert.LessOrEqual(t, r.orc.Len(), budget, "memory stays bounded")
	assert.Positive(t, r.orc.Evictions(), "and the eviction counter says so")
	assert.LessOrEqual(t, len(rep.Findings), budget,
		"findings are bounded by what the oracle still remembers, not by the keyspace")
	assert.Equal(t, len(rep.Findings), rep.KeysCompared)
}

func TestFault49_ConfirmQueueFullDropsCandidatesAndKeepsFindings(t *testing.T) {
	// Under mass divergence, individually confirming every key is not useful:
	// the operator needs the magnitude, which the report already carries. What
	// must not happen is losing the findings already established.
	r := newRig(t, oracle.Config{}, sweeper.Config{MaxConfirmQueue: 3})

	for i := 0; i < 3; i++ {
		key := "early" + strconv.Itoa(i)
		r.apply(key, "right")
		r.materialize(key, "wrong")
	}
	r.settle()
	r.sweep()
	r.clk.Advance(faultWindow)
	require.Equal(t, 3, r.confirm())
	require.Len(t, r.swp.Confirmed(), 3)

	// Now the flood.
	for i := 0; i < 50; i++ {
		key := "flood" + strconv.Itoa(i)
		r.apply(key, "right")
		r.materialize(key, "wrong")
	}
	r.settle()

	rep := r.sweep()

	assert.Positive(t, r.swp.Stats().ConfirmQueueDropped)
	assert.Equal(t, 53, len(rep.Findings), "the magnitude survives the queue bound")
	assert.Len(t, r.swp.Confirmed(), 3, "already-confirmed findings are retained")
}

func TestFault50_ASweepThatOverrunsIsSkippedNotStacked(t *testing.T) {
	// A sweeper that cannot keep up must shed load rather than accumulate it.
	// Stacking sweeps behind a slow one turns a performance problem into an
	// unbounded queue, and two sweeps running at once would double-count every
	// finding.
	r := newRig(t, oracle.Config{}, sweeper.Config{})

	r.apply("k", "v")
	r.materialize("k", "v")
	r.settle()

	var concurrent, maxConcurrent atomic.Int64
	blocked := make(chan struct{})
	release := make(chan struct{})
	var once bool

	r.mem.ObserveCommands(func(name string) {
		if name != "GET" {
			return
		}
		if n := concurrent.Add(1); n > maxConcurrent.Load() {
			maxConcurrent.Store(n)
		}
		defer concurrent.Add(-1)

		if !once {
			once = true
			close(blocked)
			<-release
		}
	})

	done := make(chan struct{})
	var sweepErr error
	go func() {
		defer close(done)
		_, sweepErr = r.swp.SweepOnce(context.Background())
	}()

	<-blocked
	// Five ticks arrive while the first sweep is stuck.
	for i := 0; i < 5; i++ {
		assert.False(t, r.swp.TrySweepOnce(context.Background()))
	}
	close(release)
	<-done
	require.NoError(t, sweepErr)

	assert.Equal(t, int64(5), r.swp.Stats().SweepsSkipped)
	assert.Equal(t, int64(1), r.swp.Stats().Sweeps)
	assert.Equal(t, int64(1), maxConcurrent.Load(),
		"two sweeps must never be inside the store at the same time")
}

func TestFault51_AHotKeyIsEventuallyComparedRatherThanSkippedForever(t *testing.T) {
	// §5.3's blind spot. A key updated every 100ms against a 5s window is
	// permanently in flight, and a hot key is exactly the key most worth
	// auditing — it would be the one key driftwatch silently never looked at.
	r := newRig(t, oracle.Config{NeverSettledFactor: 10}, sweeper.Config{})

	// A minute of events every 100ms, with the value genuinely changing, which
	// is past the fifty-second never-settled threshold (10 x W).
	for i := 0; i < 600; i++ {
		r.clk.Advance(100 * time.Millisecond)
		r.apply("hot", "changing"+strconv.Itoa(i))
	}

	require.Equal(t, 0, r.sweep().KeysCompared, "still in flight, so still not compared")
	assert.Equal(t, 1, r.orc.Counts(r.clk.Now()).NeverSettled,
		"and counted as a key driftwatch has never managed to compare")

	// The value stops changing. The events do not.
	r.materialize("hot", "wrong")
	for i := 0; i < 600; i++ {
		r.clk.Advance(100 * time.Millisecond)
		r.apply("hot", "stable")
	}

	rep := r.sweep()

	assert.Equal(t, 1, rep.KeysCompared, "the stability-window check makes it comparable")
	assert.Zero(t, r.orc.Counts(r.clk.Now()).NeverSettled)
	require.Len(t, rep.Findings, 1)
	assert.Equal(t, differ.CatValueMismatch, rep.Findings[0].Category)
}

func TestFault52_AdaptiveWindowClampsAtItsMaximumAndSaysSo(t *testing.T) {
	// A window that grew without bound would mean never asserting anything.
	// Clamping is right, but past the clamp driftwatch is knowingly using a
	// window it has measured to be too small, and that has to be visible.
	const (
		maxWindow    = 120 * time.Second
		maxPollDelay = 90 * time.Second
		probes       = 200
	)

	clk := clock.Fake(faultEpoch())
	orc := oracle.New(oracle.Config{Clock: clk, SettlementWindow: faultWindow})
	mem := target.NewMemory(target.WithClock(clk))
	rec := target.Recording(t, mem)

	est := lag.New(lag.Config{
		Oracle:     orc,
		Target:     rec,
		Shape:      projection.ShapeScalar,
		Clock:      clk,
		ProbeCount: probes,
		// Long enough that the sample is not rotated out from under the
		// measurements this test is waiting on.
		ProbeRotation: time.Hour,
		MaxPollDelay:  maxPollDelay,
		MinWindow:     time.Second,
		MaxWindow:     maxWindow,
		SafetyFactor:  3,
		WindowSize:    500,
		Seed:          1,
	})

	// A materializer that never applies anything. Every probe runs to its
	// deadline, which is the state W most needs to represent honestly.
	ctx := context.Background()
	for i := 0; i < probes; i++ {
		key := "probe" + strconv.Itoa(i)
		est.OfferKey(key)

		e := &event.Event{
			Publisher: "p", Epoch: 1, Seq: uint64(i + 1), //nolint:gosec // loop counter
			Op: event.OpSet, Key: key, Value: []byte("v"), ObservedAt: clk.Now(),
		}
		res := orc.Apply(projection.Mutation{
			Key:    key,
			Action: projection.ActionUpsert,
			Value:  event.Value{Kind: event.ValueScalar, Scalar: []byte("v")},
		}, e, seqtrack.Accept, oracle.TrustComplete)
		est.Observe(key, res.Version, clk.Now())
	}
	require.Equal(t, probes, est.PendingCount())

	for i := 0; i < 95; i++ {
		clk.Advance(time.Second)
		est.Tick(ctx, clk.Now())
	}

	stats := est.Stats()
	require.Equal(t, probes, stats.TimedOut, "every probe ran to its deadline")
	require.Equal(t, maxPollDelay, stats.P99,
		"and the p99 reflects them rather than excluding them (D-008)")

	// p99 x 3 wants 270s against a 120s ceiling.
	assert.Equal(t, maxWindow, est.SettlementWindow(), "clamped at the ceiling")
	assert.True(t, stats.Clamped, "and the clamp is reported, not silent")

	// Time passing with the same measurements must not move it further.
	clk.Advance(10 * time.Minute)
	est.Tick(ctx, clk.Now())
	assert.Equal(t, maxWindow, est.SettlementWindow(), "no unbounded growth")
	assert.Empty(t, rec.Violations())
}

func TestFault53_NoObservationsMeansTheFloorAndNoBusyLoop(t *testing.T) {
	// Before any measurement exists, W sits on its floor and the status says
	// the window is not being driven by data. Adapting to three samples would
	// be adapting to noise, and a busy loop looking for samples that are not
	// there would burn a core to learn nothing.
	const minWindow = 2 * time.Second

	clk := clock.Fake(faultEpoch())
	est := lag.New(lag.Config{
		Clock:        clk,
		MinWindow:    minWindow,
		MaxWindow:    120 * time.Second,
		SafetyFactor: 3,
		MaxPollDelay: 30 * time.Second,
	})

	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		clk.Advance(10 * time.Millisecond)
		est.Tick(ctx, clk.Now())
	}

	stats := est.Stats()
	assert.Equal(t, minWindow, est.SettlementWindow(), "W sits on its floor")
	assert.False(t, stats.Adaptive, "and the status is honest that it is not adapting")
	assert.Zero(t, stats.Observations)
	assert.Zero(t, est.Polls(),
		"ten seconds of ticks with nothing to measure cost no reads at all")
}

func TestFault54_ContextCancellationAbortsASweepPromptly(t *testing.T) {
	// Shutdown must not wait for a sweep over a million keys. The abort has to
	// be visible mid-walk, not only between sweeps.
	r := newRig(t, oracle.Config{}, sweeper.Config{ReadBatchSize: 10})

	for i := 0; i < 1000; i++ {
		key := "k" + strconv.Itoa(i)
		r.apply(key, "v")
		r.materialize(key, "v")
	}
	r.settle()

	ctx, cancel := context.WithCancel(context.Background())

	var batches atomic.Int64
	r.mem.ObserveCommands(func(name string) {
		if name == "GET" && batches.Add(1) == 3 {
			cancel()
		}
	})

	rep, err := r.swp.SweepOnce(ctx)

	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, rep, "an aborted sweep reports nothing rather than a partial answer")
	assert.Less(t, batches.Load(), int64(100),
		"the sweep stopped where it was canceled rather than running to the end")
	assert.Zero(t, r.swp.Stats().TargetUnavailable,
		"our own cancellation is not the store being unreachable")
	// goleak in TestMain covers the "all goroutines exit" half of the row.
}

func TestFaultMatrix_Coverage(t *testing.T) {
	// The matrix is only a specification if something checks that it is
	// followed. A row with no test is a row where the implementation can do
	// whatever it likes, and the gap is invisible unless it is asserted.
	//
	// Rows 47 to 54 were written before the rest, so the range is now the
	// whole matrix. hack/verify-fault-matrix.sh checks the same property from
	// outside the compiler by reflecting over test names; this table checks it
	// from inside, where deleting or renaming a test is a build failure rather
	// than a grep that quietly stops matching.
	const firstRow, lastRow = 1, 60

	for n := firstRow; n <= lastRow; n++ {
		fn, ok := faultRows[n]
		if !assert.True(t, ok, "fault matrix row %d (§15.3) has no test", n) {
			continue
		}

		// The name has to carry the row number too, zero-padded to two digits.
		// Without it a copy-paste slip could point two rows at the same test and
		// the table would still look complete; the padding is what makes a
		// sorted list of test names read in matrix order.
		name := funcName(fn)
		assert.True(t, strings.HasPrefix(name, fmt.Sprintf("TestFault%02d_", n)),
			"row %d is covered by %s, whose name claims a different row", n, name)
		t.Logf("row %2d covered by %s", n, name)
	}
}
