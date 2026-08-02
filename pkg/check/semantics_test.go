package check_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/check"
	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/metrics"
	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/source"
	"github.com/nabrahma/driftwatch/pkg/target"
)

// The declarations a check makes about its own answers.
//
// Everything in this file is a case where driftwatch cannot fix a problem and
// has to say so instead: two publishers racing on a key it cannot order, a
// publisher whose clock disagrees with its own, a snapshot that restores trust,
// a decode failure whose cause decides which team gets paged. Each of these is
// a place where the wrong behavior is silence, and silence is what a test has
// to be written to catch.

// multiWriterSpec is a projection where write order matters. keysetOwnership
// folds per member, so two publishers adding different members commute and
// there is no ambiguity to declare; a scalar replaces the whole value, so
// there is.
const multiWriterSpec = `
source: {type: memory}
projection: {type: scalar}
target: {type: memory}
policy:
  settlementWindow: {mode: static, static: 1s, min: 1s, max: 60s}
  sweepInterval: 10s
  bootstrap: Wait
`

func TestSemantics_TwoPublishersOnOneScalarKeyIsDeclaredUnreliable(t *testing.T) {
	// §15 row 25. Sequence numbers order events within one publisher's stream
	// and say nothing across streams, so when two publishers write the same
	// scalar key there is no fact of the matter about which write came second.
	//
	// driftwatch cannot resolve that. Reporting drift would be wrong and
	// silently picking a winner would be worse, so the only honest move is to
	// declare that its answers for this keyspace are one valid interleaving
	// among several — and to name the key that showed it, because "somewhere in
	// your keyspace" is not actionable.
	c := newCheck(t, multiWriterSpec)
	stop := running(t, c)
	defer stop()

	require.False(t, c.Status().MultiWriterUnsafe,
		"nothing has happened yet")

	publish(t, c, setEvent("replica-0", 1, "contended", "v1"))
	require.False(t, c.Status().MultiWriterUnsafe,
		"one publisher writing its own key is the ordinary case")

	publish(t, c, setEvent("replica-1", 1, "contended", "v2"))

	status := c.Status()
	assert.True(t, status.MultiWriterUnsafe,
		"two publishers wrote block:contended under a projection whose fold "+
			"depends on order, and the check did not say so")
	assert.Equal(t, "contended", status.MultiWriterKey,
		"the flag without the key sends an operator looking through the whole "+
			"keyspace for the one that tripped it")
}

func TestSemantics_TwoPublishersOnOneSetKeyIsNotDeclaredUnsafe(t *testing.T) {
	// The other half, and the reason Commutative() alone is the wrong test.
	//
	// A set folds per member: replica-0 adding itself and replica-1 adding
	// itself reach the same set whichever order they arrive in. Declaring that
	// unsafe would fire the warning on the single most common shape driftwatch
	// is deployed against — a KV-cache index where every replica announces its
	// own blocks — and a warning that is always on is a warning nobody reads.
	c := newCheck(t, inProcessSpec)
	stop := running(t, c)
	defer stop()

	publish(t, c,
		addEventJSON("replica-0", 1, "shared", "replica-0"),
		addEventJSON("replica-1", 1, "shared", "replica-1"),
	)

	status := c.Status()
	assert.False(t, status.MultiWriterUnsafe,
		"two publishers adding distinct members to one set commute, so there "+
			"is no ambiguity to declare: key=%q", status.MultiWriterKey)
}

func TestSemantics_PublisherClockSkewIsMeasuredAndReported(t *testing.T) {
	// The skew is not used to make decisions — §5.3 is explicit that the
	// settlement window runs on local receive time precisely so that a
	// publisher's clock cannot move it. It is reported because a publisher four
	// seconds ahead explains an operator's "why did this key take so long to
	// settle" without anyone having to guess.
	clk := clock.Fake(epoch())
	c := newCheckWith(t, inProcessSpec, clk)
	stop := running(t, c)
	defer stop()

	// ts is the publisher's clock; the check's clock is at epoch().
	ahead := epoch().Add(4 * time.Second).Format(time.RFC3339Nano)
	publish(t, c, fmt.Sprintf(
		`{"publisher":"replica-0","epoch":1,"seq":1,"op":"add","key":"0",`+
			`"member":"replica-0","ts":%q}`, ahead))

	status := c.Status()
	require.Len(t, status.Publishers, 1)
	assert.InDelta(t, 4.0, status.Publishers[0].ClockSkewSeconds, 1.0,
		"a publisher 4s ahead of the check should be reported as roughly 4s of skew")
}

func TestSemantics_SkewTrackingStopsAtTheePublisherCap(t *testing.T) {
	// §19.2. A misconfigured producer that stamps a fresh identity on every
	// message would otherwise grow this map without limit, and it is the one
	// collection in the check that a stream of one-off publisher names can
	// reach directly.
	//
	// The cap is the same maxPublishers seqtrack uses, so the two cannot
	// disagree about how many publishers exist.
	const maxPublishers = 3
	spec := strings.Replace(inProcessSpec,
		"  bootstrap: Wait",
		fmt.Sprintf("  bootstrap: Wait\n  maxPublishers: %d", maxPublishers), 1)

	clk := clock.Fake(epoch())
	c := newCheckWith(t, spec, clk)
	stop := running(t, c)
	defer stop()

	ts := epoch().Add(time.Second).Format(time.RFC3339Nano)
	for i := 0; i < maxPublishers*3; i++ {
		publish(t, c, fmt.Sprintf(
			`{"publisher":"ephemeral-%d","epoch":1,"seq":1,"op":"add","key":"0",`+
				`"member":"m","ts":%q}`, i, ts))
	}

	withSkew := 0
	for _, p := range c.Status().Publishers {
		if p.ClockSkewSeconds != 0 {
			withSkew++
		}
	}
	assert.LessOrEqual(t, withSkew, maxPublishers,
		"skew was tracked for %d publishers against a cap of %d", withSkew, maxPublishers)
}

func TestSemantics_ASnapshotRestoresTrustThatAGapTookAway(t *testing.T) {
	// A snapshot is a publisher re-declaring its whole state, which is the one
	// thing that can make a suspect key trustworthy again without waiting for
	// an event to touch it. Without this the only route out of suspicion is
	// per-key traffic, and a key nobody writes stays suspect forever — losing
	// coverage permanently over a gap that has since been repaired.
	clk := clock.Fake(epoch())
	c := newCheckWith(t, inProcessSpec, clk)
	stop := running(t, c)
	defer stop()

	publish(t, c, addEventJSON("replica-0", 1, "0", "replica-0"))
	assertTrust(t, c, "block:0", oracle.TrustComplete)

	// A gap, which suspects the key.
	publish(t, c, addEventJSON("replica-0", 40, "1", "replica-0"))
	clk.Advance(5 * time.Second)
	waitForGap(t, c, 1)
	_, err := c.SweepNow(context.Background())
	require.NoError(t, err)
	assertTrust(t, c, "block:0", oracle.TrustSuspect)

	before := c.Status().SnapshotsSeen

	// Published straight at the source rather than through publish(), which
	// waits on applied+dropped. A snapshot marker carries no key, so the
	// projection refuses it and it lands in neither bucket in the way that
	// helper expects — the observable that matters here is SnapshotsSeen.
	src, ok := c.Source().(*source.MemorySource)
	require.True(t, ok, "the spec configures a memory source")
	for _, payload := range []string{
		`{"publisher":"replica-0","epoch":1,"seq":41,"op":"snapshotBegin"}`,
		addEventJSON("replica-0", 42, "0", "replica-0"),
		`{"publisher":"replica-0","epoch":1,"seq":43,"op":"snapshotEnd"}`,
	} {
		require.True(t, src.PublishPayload([]byte(payload)))
	}

	require.Eventually(t, func() bool { return c.Status().SnapshotsSeen == before+1 },
		eventuallyFor, eventuallyPoll,
		"the completed snapshot cycle was not counted")
	assertTrust(t, c, "block:0", oracle.TrustComplete,
		"a completed snapshot re-declares the publisher's whole state, so its "+
			"keys are trustworthy again")
}

func TestSemantics_ADecodeFailureIsAttributedToTheRightCause(t *testing.T) {
	// Three failures that all look like "driftwatch could not read it" and send
	// an operator to three different systems:
	//
	//   decode_error  the payload is malformed — a serializer or wire-format
	//                 mismatch, so look at the producer's encoder
	//   unknown_op    everything parsed; the producer started emitting an event
	//                 type nobody configured, so look at the producer's release
	//   too_large     the frame exceeded the cap — a producer bug or an attack,
	//                 and the only one of the three that says nothing about
	//                 format at all
	//
	// Reporting all three as decode_error, which is what happened until §15
	// rows 18 and 19 asked, sends two of those three investigations to the
	// wrong place.
	reg := prometheus.NewRegistry()
	c := newCheckWithRegistry(t, inProcessSpec, reg)
	stop := running(t, c)
	defer stop()

	publish(t, c, `{"publisher":"replica-0",,,`)
	publish(t, c,
		`{"publisher":"replica-0","epoch":1,"seq":2,"op":"teleport","key":"0"}`)

	assert.Equal(t, 1.0, dropCount(t, reg, string(metrics.DropDecodeError)),
		"a malformed payload should be decode_error")
	assert.Equal(t, 1.0, dropCount(t, reg, string(metrics.DropUnknownOp)),
		"a parseable message with an unconfigured op should be unknown_op, not "+
			"decode_error — the payload was fine")
}

// dropCount reads driftwatch_events_dropped_total for one reason.
func dropCount(t *testing.T, reg *prometheus.Registry, reason string) float64 {
	t.Helper()

	families, err := reg.Gather()
	require.NoError(t, err)

	for _, f := range families {
		if f.GetName() != "driftwatch_events_dropped_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "reason" && l.GetValue() == reason {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

func TestSemantics_PollLagDrivesTheEstimator(t *testing.T) {
	// PollLag is what the controller's ticker calls, and it is the only route by
	// which the measured convergence distribution ever fills. An estimator that
	// is never ticked reports no observations forever, which looks exactly like
	// a system where nothing has converged yet — so nothing else in the check
	// would notice this being disconnected.
	//
	// The assertion is on the p99 rather than on WindowIsAdaptive: the window
	// only starts being driven by measurement after the controller has enough
	// samples to trust, and requiring that here would be testing the
	// controller's warm-up rule rather than the wiring.
	clk := clock.Fake(epoch())
	c := newCheckWith(t, adaptiveSpec, clk)
	stop := running(t, c)
	defer stop()

	ctx := context.Background()
	require.Zero(t, c.Status().ConvergenceP99Seconds,
		"nothing has converged yet")

	store, ok := c.Target().(*target.MemoryTarget)
	require.True(t, ok, "the spec configures a memory target")

	// Each round is one key the oracle learns and the store then catches up on,
	// which is exactly one convergence measurement. The probe only sees it when
	// PollLag runs, so the tick is the thing under test.
	for i := 0; i < 8; i++ {
		block := strconv.Itoa(i)
		publish(t, c, addEventJSON("replica-0", uint64(i+1), block, "replica-0"))
		store.SeedSets(map[string][]string{"block:" + block: {"replica-0"}})

		clk.Advance(250 * time.Millisecond)
		c.PollLag(ctx)
	}

	status := c.Status()
	assert.Positive(t, status.ConvergenceP99Seconds,
		"eight keys converged and the estimator recorded none of them, so "+
			"PollLag is not reaching it")
	assert.Positive(t, status.SettlementWindowSeconds,
		"the window should never be reported as zero")
}

// adaptiveSpec drives the lag estimator rather than a fixed window.
const adaptiveSpec = `
source: {type: memory}
projection:
  type: keysetOwnership
  keyTemplate: "block:{{.Key}}"
target: {type: memory}
policy:
  settlementWindow: {mode: adaptive, min: 1s, max: 30s}
  sweepInterval: 10s
  bootstrap: Wait
`

// newCheckWithRegistry builds a check whose metrics land in reg, so a test can
// assert on the label values rather than only on the counts in Status.
func newCheckWithRegistry(t *testing.T, spec string, reg *prometheus.Registry) *check.Check {
	t.Helper()

	parsed, err := check.Load(strings.NewReader(spec))
	require.NoError(t, err)

	c, err := check.New(parsed, check.Deps{
		Clock:   clock.Fake(epoch()),
		Metrics: metrics.New(metrics.Options{Registry: reg}),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	return c
}
