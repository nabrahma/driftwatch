package sweeper_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/differ"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/target"
)

// What the sweeper does when the store stops cooperating.
//
// §23 A5 is the rule these all serve: an outage in the thing being audited must
// not become a wall of drift reports about it. A checker that confirms findings
// on the strength of reads that failed turns someone else's incident into its
// own, at the exact moment its output is least useful and most likely to be
// believed.

func TestFailure_AKeyHoldingTheWrongTypeIsDriftRatherThanAReadError(t *testing.T) {
	// §9 M8. A WRONGTYPE reply is not a transport failure — the store answered,
	// and the answer was that something wrote a different shape into the index.
	// That is precisely the kind of silent corruption driftwatch exists to
	// find, so it has to be categorized rather than counted as an error and
	// dropped.
	h := newHarness(t, func(hc *harnessConfig) {
		hc.wrap = func(inner target.Target) target.Target {
			return &wrongTypeTarget{Target: inner, key: "confused"}
		}
	})

	h.apply("confused", "v1")
	h.settle()

	rep := h.sweep()

	require.Equal(t, 1, rep.Total(),
		"a wrong-type read is a finding, not an error to swallow")
	assert.Equal(t, differ.CatTypeMismatch, rep.Findings[0].Category,
		"got %s; a key of the wrong shape should be named as such rather than "+
			"reported as missing, which sends the operator to the wrong place",
		rep.Findings[0].Category)
}

func TestFailure_AReadErrorFailsTheSweepRatherThanReportingDrift(t *testing.T) {
	// The other half. A read that genuinely failed says nothing at all about
	// whether the key agrees, and a sweep that treated an unreadable key as a
	// disagreeing one would report drift proportional to the outage.
	boom := errors.New("connection reset by peer")

	h := newHarness(t, func(hc *harnessConfig) {
		hc.wrap = func(inner target.Target) target.Target {
			return &failingReadTarget{Target: inner, err: boom}
		}
	})

	h.apply("k1", "v1")
	h.settle()

	rep, err := h.swp.SweepOnce(context.Background())
	require.Error(t, err, "a failed read must fail the sweep")
	assert.ErrorIs(t, err, boom, "the cause should survive to the caller")

	if rep != nil {
		assert.Zero(t, rep.Total(),
			"a sweep that could not read must not report findings: %s", rep.Summary())
	}
}

func TestFailure_AnUnreachableStoreRequeuesCandidatesInsteadOfConfirmingThem(t *testing.T) {
	// The §23 A5 case in the place it is easiest to get wrong. Confirmation is
	// a read. If the store is unreachable when the confirmation comes due, the
	// honest answer is "still unknown", and every candidate goes back on the
	// queue to be re-read the moment the store answers again.
	//
	// Confirming them instead would produce a burst of drift findings whose
	// size is proportional to the outage and whose content is entirely
	// fictional.
	h := newHarness(t)

	h.apply("missing", "v1")
	h.settle()

	rep := h.sweep()
	require.Equal(t, 1, rep.Total())
	require.Equal(t, 1, h.swp.PendingConfirmations())

	// The store goes away after the candidate was raised but before it is due.
	h.setHealth(func(hh *target.Health) { hh.Reachable = false })
	h.advance(window + time.Second)

	decided := h.swp.ConfirmDue(context.Background(), h.clk.Now())

	assert.Zero(t, decided,
		"nothing may be decided while the store is unreachable")
	assert.Empty(t, h.swp.Confirmed(),
		"a candidate must not be promoted on the strength of a read that "+
			"never happened")
	assert.Equal(t, 1, h.swp.PendingConfirmations(),
		"the candidate must go back on the queue, not be discarded — the wait "+
			"has already elapsed, so it is due the moment the store returns")
	assert.Positive(t, h.swp.Stats().TargetUnavailable,
		"the operator needs to know why confirmations stopped")

	// And when the store comes back, the same candidate is decided normally.
	h.setHealth(func(hh *target.Health) { hh.Reachable = true })
	h.advance(time.Second)

	require.Positive(t, h.swp.ConfirmDue(context.Background(), h.clk.Now()),
		"the requeued candidate should be decided once the store answers again")
	assert.Len(t, h.swp.Confirmed(), 1)
}

func TestFailure_SubscribersReceiveConfirmedFindings(t *testing.T) {
	h := newHarness(t)

	findings := h.swp.Subscribe()

	h.apply("missing", "v1")
	h.settle()
	h.sweep()

	h.advance(window + time.Second)
	require.Positive(t, h.swp.ConfirmDue(context.Background(), h.clk.Now()))

	select {
	case f := <-findings:
		assert.Equal(t, "missing", f.Key)
		assert.True(t, f.Confirmed,
			"only confirmed findings are published; a candidate reaching a "+
				"subscriber is the false positive two-phase confirmation exists "+
				"to prevent")
	case <-time.After(30 * time.Second):
		t.Fatal("a confirmed finding never reached the subscriber")
	}
}

func TestFailure_ASubscriberThatStopsReadingIsDroppedRatherThanBlockingTheSweeper(t *testing.T) {
	// A subscriber is a convenience; the sweeper is not. Blocking a sweep on a
	// consumer that has stopped reading would let any subscriber halt the
	// audit, so the send is non-blocking and the drop is counted.
	//
	// Counting it is the part that matters. A silently dropped finding is
	// indistinguishable from no finding, which is the exact failure mode this
	// whole project is about.
	h := newHarness(t)

	sub := h.swp.Subscribe() // never read from
	require.NotNil(t, sub)

	// Enough confirmed findings to overrun the subscriber's buffer several
	// times over.
	const keys = 4096
	for i := 0; i < keys; i++ {
		h.apply(keyN(i), "v1")
	}
	h.settle()
	h.sweep()

	h.advance(window + time.Second)
	h.swp.ConfirmDue(context.Background(), h.clk.Now())

	assert.Positive(t, h.swp.Stats().SubscriberDropped,
		"a subscriber that stopped reading should have had findings dropped "+
			"and counted, not blocked the sweeper")
}

func TestFailure_ASweepAfterCloseReportsClosedRatherThanPanicking(t *testing.T) {
	h := newHarness(t)
	h.apply("k1", "v1")
	h.settle()

	require.NoError(t, h.swp.Close())

	_, err := h.swp.SweepOnce(context.Background())
	assert.Error(t, err, "a sweep after Close must not succeed")

	// Close is idempotent: the controller's shutdown path can reach it twice
	// when a context cancellation and an explicit stop race, and the second
	// call must not panic.
	assert.NoError(t, h.swp.Close(), "Close should be idempotent")

	// A subscriber taken after Close gets a closed channel rather than one that
	// never delivers, so a range over it terminates.
	ch := h.swp.Subscribe()
	select {
	case _, open := <-ch:
		assert.False(t, open, "a post-Close subscription should be closed")
	case <-time.After(30 * time.Second):
		t.Fatal("a post-Close subscription neither delivered nor closed")
	}
}

// keyN names one of many keys.
func keyN(i int) string { return "block-" + strconv.Itoa(i) }

// wrongTypeTarget answers one key with a WRONGTYPE error, which is what Redis
// returns when a SMEMBERS lands on a string.
type wrongTypeTarget struct {
	target.Target
	key string
}

func (w *wrongTypeTarget) ReadMany(
	ctx context.Context, keys []string, shape projection.Shape,
) ([]target.Read, error) {
	reads, err := w.Target.ReadMany(ctx, keys, shape)
	if err != nil {
		return nil, err
	}
	for i, k := range keys {
		if k == w.key {
			reads[i].Err = &target.WrongTypeError{Key: k, Want: shape, Got: "string"}
		}
	}
	return reads, nil
}

// failingReadTarget fails every batched read with one error.
type failingReadTarget struct {
	target.Target
	err error
}

func (f *failingReadTarget) ReadMany(
	_ context.Context, _ []string, _ projection.Shape,
) ([]target.Read, error) {
	return nil, f.err
}
