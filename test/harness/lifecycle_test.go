package harness_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/differ"
	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/seqtrack"
	"github.com/nabrahma/driftwatch/pkg/sweeper"
	"github.com/nabrahma/driftwatch/pkg/target"
)

// TestDriftLifecycle_InjectConfirmRepairResolve is the Phase 3 demo.
//
// It runs the whole correctness cycle end to end against a real Redis protocol
// implementation, in process, with a fake clock: a materializer falls behind
// and catches up (and is not reported), then genuinely loses a write (and is
// reported, but only after a second read a settlement window later), then the
// write is restored (and the finding clears).
//
// The wall-clock cost is zero. Every "wait" in the story below is a clock
// advance, which is what makes the timing assertions exact rather than
// approximate — the test can state that confirmation happened after exactly one
// window, not after roughly one.
func TestDriftLifecycle_InjectConfirmRepairResolve(t *testing.T) {
	const (
		keys = 50
		w    = 5 * time.Second
	)

	clk := clock.Fake(epoch())
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	tgt := target.NewRedisFromClient(client, 0, 0)
	t.Cleanup(func() { require.NoError(t, tgt.Close()) })

	// Every read the sweeper makes goes through the recorder, so this demo also
	// asserts that a full drift lifecycle never writes to the store it audits.
	rec := target.Recording(t, tgt)

	orc := oracle.New(oracle.Config{Clock: clk, SettlementWindow: w})
	tracker := seqtrack.New(seqtrack.Config{Clock: clk})

	swp := sweeper.New(sweeper.Config{
		Oracle:           orc,
		Target:           rec,
		Shape:            projection.ShapeScalar,
		Clock:            clk,
		SettlementWindow: func() time.Duration { return w },
		ReadBatchSize:    16,
	})
	t.Cleanup(func() { require.NoError(t, swp.Close()) })

	confirmed := swp.Subscribe()
	ctx := context.Background()

	// materialize stands in for the real materializer: it applies an event to
	// the store the way the system under audit would.
	materialize := func(key, value string) {
		rec.Fixture(func() { require.NoError(t, server.Set(key, value)) })
	}

	var seq uint64
	publish := func(key, value string) {
		seq++
		e := &event.Event{
			Publisher: "p", Epoch: 1, Seq: seq, Op: event.OpSet,
			Key: key, Value: []byte(value), ObservedAt: clk.Now(),
		}
		verdict, _ := tracker.Observe(e)
		orc.Apply(projection.Mutation{
			Key:    key,
			Action: projection.ActionUpsert,
			Value:  event.Value{Kind: event.ValueScalar, Scalar: []byte(value)},
		}, e, verdict, tracker.Trust(e.Publisher))
	}

	key := func(i int) string { return "user:" + strconv.Itoa(i) }

	// ---------------------------------------------------------------------
	// 1. A healthy system. Events arrive; the materializer applies them.
	// ---------------------------------------------------------------------
	for i := 0; i < keys; i++ {
		publish(key(i), "v1")
		materialize(key(i), "v1")
	}
	clk.Advance(w + time.Second)

	rep, err := swp.SweepOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, keys, rep.KeysCompared)
	require.Empty(t, rep.Findings, "a healthy system produces no findings")

	// ---------------------------------------------------------------------
	// 2. The materializer falls behind on one key. It is slow, not wrong.
	// ---------------------------------------------------------------------
	publish(key(7), "v2")
	clk.Advance(w + time.Second)

	rep, err = swp.SweepOnce(ctx)
	require.NoError(t, err)
	require.Len(t, rep.Findings, 1, "the disagreement is visible")
	require.False(t, rep.Findings[0].Confirmed)
	require.Empty(t, swp.Confirmed(), "but one read is never enough to report it")

	// It catches up while the candidate waits out its window.
	materialize(key(7), "v2")
	clk.Advance(w)
	require.Equal(t, 1, swp.ConfirmDue(ctx, clk.Now()))

	require.Empty(t, swp.Confirmed(), "lag is not drift")
	require.Equal(t, int64(1), swp.Stats().TransientResolved)

	// ---------------------------------------------------------------------
	// 3. The materializer genuinely loses a write. This is real drift.
	// ---------------------------------------------------------------------
	publish(key(23), "v2")
	// ... and nothing writes it to the store.
	clk.Advance(w + time.Second)

	rep, err = swp.SweepOnce(ctx)
	require.NoError(t, err)
	require.Len(t, rep.Findings, 1)
	require.Empty(t, swp.Confirmed(), "still not on the strength of one read")

	clk.Advance(w)
	require.Equal(t, 1, swp.ConfirmDue(ctx, clk.Now()))

	finding := swp.Confirmed()[key(23)]
	assert.True(t, finding.Confirmed)
	assert.Equal(t, differ.CatValueMismatch, finding.Category)
	assert.Equal(t, []byte("v2"), finding.OracleValue.Scalar)
	assert.Equal(t, []byte("v1"), finding.TargetValue.Scalar)

	select {
	case f := <-confirmed:
		assert.Equal(t, key(23), f.Key)
	default:
		t.Fatal("a confirmed finding must reach the reporter")
	}

	// The two disagreeing reads really were a window apart — invariant I7,
	// stated as a number rather than assumed.
	episode := swp.Episodes()[key(23)]
	assert.Equal(t, w, episode.ConfirmedAt.Sub(episode.FirstSeenAt))

	// It stays reported for as long as it is wrong, keeping one episode rather
	// than starting a new one each sweep.
	for i := 0; i < 3; i++ {
		clk.Advance(30 * time.Second)
		_, err = swp.SweepOnce(ctx)
		require.NoError(t, err)
		require.Len(t, swp.Confirmed(), 1)
	}
	assert.Equal(t, episode.FirstSeenAt, swp.Episodes()[key(23)].FirstSeenAt)

	// ---------------------------------------------------------------------
	// 4. Somebody repairs it. The alert has to clear, or nobody will ever
	//    trust the next one.
	// ---------------------------------------------------------------------
	materialize(key(23), "v2")
	clk.Advance(30 * time.Second)

	rep, err = swp.SweepOnce(ctx)
	require.NoError(t, err)

	assert.Empty(t, rep.Findings)
	assert.Empty(t, swp.Confirmed(), "the repaired key leaves Confirmed()")
	assert.Equal(t, int64(1), swp.Stats().DriftResolved)
	assert.Equal(t, clk.Now().Sub(episode.FirstSeenAt), swp.Stats().LastDriftDuration)

	// ---------------------------------------------------------------------
	// And the whole lifecycle read the store without ever writing to it.
	// ---------------------------------------------------------------------
	assert.Empty(t, rec.Violations())

	// Two and a half minutes of system time passed, in a test that runs in
	// milliseconds. Every wait above was a clock advance, which is what lets
	// the assertions state exact durations — confirmation after exactly one
	// window — instead of approximate ones.
	assert.Greater(t, clk.Now().Sub(epoch()), 2*time.Minute)
}
