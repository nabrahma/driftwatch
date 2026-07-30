package oracle_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/seqtrack"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func newOracle(t *testing.T, cfg oracle.Config) (*oracle.Oracle, clock.FakeClock) {
	t.Helper()
	clk := clock.Fake(epoch)
	cfg.Clock = clk
	return oracle.New(cfg), clk
}

func setOf(members ...string) event.Value {
	m := make(map[string]struct{}, len(members))
	for _, s := range members {
		m[s] = struct{}{}
	}
	return event.Value{Kind: event.ValueSet, Members: m}
}

// upsert builds an upsert mutation and the event that produced it.
func upsert(key string, at time.Time, members ...string) (projection.Mutation, *event.Event) {
	return projection.Mutation{Key: key, Action: projection.ActionUpsert, Value: setOf(members...)},
		&event.Event{Publisher: "p", Epoch: 1, Seq: 1, Op: event.OpAdd, Key: key, ObservedAt: at}
}

func del(key string, at time.Time) (projection.Mutation, *event.Event) {
	return projection.Mutation{Key: key, Action: projection.ActionDelete},
		&event.Event{Publisher: "p", Epoch: 1, Seq: 2, Op: event.OpDelete, Key: key, ObservedAt: at}
}

//nolint:gocritic // hugeParam: mirrors the Apply signature under test
func apply(o *oracle.Oracle, m projection.Mutation, e *event.Event) oracle.ApplyResult {
	return o.Apply(m, e, seqtrack.Accept, oracle.TrustComplete)
}

func TestOracle_ApplyStoresTheValueAndReportsCreation(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{})

	m, e := upsert("k", epoch, "replica-0")
	res := apply(o, m, e)

	assert.Equal(t, "k", res.Key)
	assert.Equal(t, uint64(1), res.Version)
	assert.True(t, res.Created)
	assert.True(t, res.Applied)
	assert.False(t, res.Deleted)
	assert.Empty(t, res.Evicted)

	got, ok := o.Get("k")
	require.True(t, ok)
	assert.True(t, got.Value.Equal(setOf("replica-0")))
	assert.Equal(t, uint64(1), got.Version)
	assert.Equal(t, epoch, got.LastEventAt)
	assert.Equal(t, epoch, got.CreatedAt)
	assert.Equal(t, "p", got.LastPublisher)
	assert.Equal(t, uint64(1), got.LastSeq)
	assert.Equal(t, oracle.TrustComplete, got.Trust)
}

func TestOracle_ApplyOfANoOpMutationChangesNothing(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{})

	res := apply(o, projection.Mutation{Key: "k", Action: projection.ActionNone}, &event.Event{Publisher: "p"})

	// A no-op must not create the key, bump a version, or restart the
	// settlement window for something that did not happen.
	assert.False(t, res.Applied)
	assert.False(t, res.Created)
	_, ok := o.Get("k")
	assert.False(t, ok)
}

func TestOracle_VersionIsMonotonicAndSurvivesADeleteAndRecreate(t *testing.T) {
	// A recreated key must not restart at 1. A sweeper that read version 3
	// before a delete and 1 after a recreate would conclude the oracle had gone
	// backwards, and fencing depends on that never happening.
	o, _ := newOracle(t, oracle.Config{})

	m, e := upsert("k", epoch, "a")
	require.Equal(t, uint64(1), apply(o, m, e).Version)

	m, e = upsert("k", epoch, "a", "b")
	require.Equal(t, uint64(2), apply(o, m, e).Version)

	dm, de := del("k", epoch)
	res := apply(o, dm, de)
	assert.Equal(t, uint64(3), res.Version)
	assert.True(t, res.Deleted)

	m, e = upsert("k", epoch, "c")
	assert.Equal(t, uint64(4), apply(o, m, e).Version,
		"a recreated key must continue from the version it had, not restart")
}

func TestOracle_ADeletedKeyRemainsAsATombstone(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{})

	m, e := upsert("k", epoch, "a")
	apply(o, m, e)
	dm, de := del("k", epoch)
	apply(o, dm, de)

	// The entry has to stay: the version must survive for fencing, and the
	// differ needs "the target should not have this key" to be distinguishable
	// from "driftwatch has never heard of this key".
	got, ok := o.Get("k")
	require.True(t, ok)
	assert.True(t, got.IsAbsent())
	assert.True(t, got.Value.IsAbsent())

	v, ok := o.Version("k")
	require.True(t, ok)
	assert.Equal(t, uint64(2), v)
}

func TestOracle_DeleteOfAKeyThatWasNeverSeenStillCreatesATombstone(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{})

	dm, de := del("k", epoch)
	res := apply(o, dm, de)

	assert.True(t, res.Created)
	assert.True(t, res.Deleted)
	got, ok := o.Get("k")
	require.True(t, ok)
	assert.True(t, got.IsAbsent())
}

func TestOracle_GetReturnsADeepCopy(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{})
	m, e := upsert("k", epoch, "a", "b")
	apply(o, m, e)

	got, ok := o.Get("k")
	require.True(t, ok)

	got.Value.Members["injected-by-the-caller"] = struct{}{}
	delete(got.Value.Members, "a")

	// Handing out a reference into a shard would be a data race waiting to
	// happen: the applier mutates entries while the sweeper reads them.
	again, _ := o.Get("k")
	assert.True(t, again.Value.Equal(setOf("a", "b")), "Get leaked a reference into the shard")
}

func TestOracle_GetAndVersionReportAbsenceForAnUnknownKey(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{})

	_, ok := o.Get("never-seen")
	assert.False(t, ok)

	_, ok = o.Version("never-seen")
	assert.False(t, ok)
}

func TestOracle_ObservedAtFallsBackToTheClockWhenTheSourceDidNotStampOne(t *testing.T) {
	o, clk := newOracle(t, oracle.Config{})
	clk.Advance(90 * time.Second)

	m := projection.Mutation{Key: "k", Action: projection.ActionUpsert, Value: setOf("a")}
	apply(o, m, &event.Event{Publisher: "p", Op: event.OpAdd, Key: "k"})

	got, ok := o.Get("k")
	require.True(t, ok)
	assert.Equal(t, epoch.Add(90*time.Second), got.LastEventAt)
}

func TestOracle_SettlementUsesLocalReceiveTimeNotThePublisherClock(t *testing.T) {
	// A producer whose clock is an hour fast would otherwise make its keys
	// settle an hour early, and the resulting divergence reports would be
	// entirely driftwatch's own fault.
	o, _ := newOracle(t, oracle.Config{SettlementWindow: 5 * time.Second})

	m := projection.Mutation{Key: "k", Action: projection.ActionUpsert, Value: setOf("a")}
	apply(o, m, &event.Event{
		Publisher:   "p",
		Op:          event.OpAdd,
		Key:         "k",
		ObservedAt:  epoch,
		PublishedAt: epoch.Add(-time.Hour),
	})

	assert.Empty(t, settledKeys(o, epoch.Add(time.Second)),
		"the key is in flight by local time and must not settle on the publisher's clock")
	assert.Equal(t, []string{"k"}, settledKeys(o, epoch.Add(6*time.Second)))
}

func settledKeys(o *oracle.Oracle, now time.Time) []string {
	var out []string
	for k := range o.SettledKeys(now) {
		out = append(out, k)
	}
	return out
}

func TestOracle_SettledKeys(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{SettlementWindow: 5 * time.Second})

	m, e := upsert("old", epoch, "a")
	apply(o, m, e)
	m, e = upsert("recent", epoch.Add(10*time.Second), "a")
	apply(o, m, e)

	tests := []struct {
		name string
		now  time.Time
		want []string
	}{
		{
			name: "nothing is settled before the window elapses",
			now:  epoch.Add(time.Second),
			want: nil,
		},
		{
			name: "a key settles once its last event is older than the window",
			now:  epoch.Add(6 * time.Second),
			want: []string{"old"},
		},
		{
			name: "a key still inside the window is excluded even when older ones are not",
			now:  epoch.Add(12 * time.Second),
			want: []string{"old"},
		},
		{
			name: "every key settles eventually",
			now:  epoch.Add(20 * time.Second),
			want: []string{"old", "recent"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.ElementsMatch(t, tc.want, settledKeys(o, tc.now))
		})
	}
}

func TestOracle_SettledKeysIteratorStopsWhenTheCallerBreaks(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{SettlementWindow: time.Second})
	for i := 0; i < 200; i++ {
		m, e := upsert("k"+strconv.Itoa(i), epoch, "a")
		apply(o, m, e)
	}

	seen := 0
	for range o.SettledKeys(epoch.Add(time.Hour)) {
		seen++
		if seen == 5 {
			break
		}
	}

	assert.Equal(t, 5, seen, "the iterator must honor an early break")
}

func TestOracle_ANewEventMovesAKeyBackIntoFlight(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{SettlementWindow: 5 * time.Second})

	m, e := upsert("k", epoch, "a")
	apply(o, m, e)
	require.Equal(t, []string{"k"}, settledKeys(o, epoch.Add(10*time.Second)))

	m, e = upsert("k", epoch.Add(10*time.Second), "a", "b")
	apply(o, m, e)

	assert.Empty(t, settledKeys(o, epoch.Add(11*time.Second)),
		"a fresh event restarts the settlement window")
}

func TestOracle_SetSettlementWindow(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{
		SettlementWindow:    30 * time.Second,
		MaxSettlementWindow: 60 * time.Second,
	})

	m, e := upsert("k", epoch, "a")
	apply(o, m, e)

	require.Empty(t, settledKeys(o, epoch.Add(10*time.Second)))

	t.Run("shrinking the window settles keys immediately", func(t *testing.T) {
		o.SetSettlementWindow(time.Second)
		assert.Equal(t, time.Second, o.SettlementWindow())
		assert.Equal(t, []string{"k"}, settledKeys(o, epoch.Add(10*time.Second)))
	})

	t.Run("growing the window puts them back in flight", func(t *testing.T) {
		// The adaptive estimator raises W when the materializer slows down.
		// A key that was settled under the old window has to stop being
		// eligible, or driftwatch reports drift it has already decided to be
		// patient about.
		o.SetSettlementWindow(30 * time.Second)
		assert.Empty(t, settledKeys(o, epoch.Add(10*time.Second)))
	})

	t.Run("a window beyond the maximum is clamped", func(t *testing.T) {
		o.SetSettlementWindow(time.Hour)
		assert.Equal(t, 60*time.Second, o.SettlementWindow())
	})

	t.Run("a negative window is clamped to zero", func(t *testing.T) {
		o.SetSettlementWindow(-time.Second)
		assert.Equal(t, time.Duration(0), o.SettlementWindow())
	})

	t.Run("a zero window settles everything the instant after its event", func(t *testing.T) {
		// PRD §5.3 defines settled as t - lastEventAt > W, strictly. With W=0
		// that makes a key settled at any time after its event but not at the
		// same instant, which is the behavior tests want when they advance the
		// fake clock by any amount at all.
		assert.Empty(t, settledKeys(o, epoch))
		assert.Equal(t, []string{"k"}, settledKeys(o, epoch.Add(time.Nanosecond)))
	})
}

func TestOracle_KeysOlderThanTheMaximumWindowStaySettledWhateverWIsSetTo(t *testing.T) {
	// The settlement index coalesces old buckets on the maximum window rather
	// than the current one, precisely so that raising W cannot un-settle a key
	// whose bucket has already been folded away.
	o, _ := newOracle(t, oracle.Config{
		SettlementWindow:    time.Second,
		MaxSettlementWindow: 10 * time.Second,
	})

	m, e := upsert("k", epoch, "a")
	apply(o, m, e)

	// Force the promotion by iterating well past the horizon.
	require.Equal(t, []string{"k"}, settledKeys(o, epoch.Add(time.Hour)))

	o.SetSettlementWindow(10 * time.Second)
	assert.Equal(t, []string{"k"}, settledKeys(o, epoch.Add(time.Hour)))
}

func TestOracle_Counts(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{SettlementWindow: 5 * time.Second})

	m, e := upsert("settled", epoch, "a")
	apply(o, m, e)
	m, e = upsert("inflight", epoch.Add(10*time.Second), "a")
	apply(o, m, e)

	got := o.Counts(epoch.Add(11 * time.Second))

	assert.Equal(t, 2, got.Total)
	assert.Equal(t, 1, got.Settled)
	assert.Equal(t, 1, got.InFlight)
	assert.Equal(t, 2, got.ByTrust[oracle.TrustComplete])
	assert.Zero(t, got.ByTrust[oracle.TrustSuspect])
	assert.Zero(t, got.Truncated)
}

func TestOracle_CountsTracksTruncatedKeys(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{})

	m, e := upsert("k", epoch, "a")
	m.Truncated = true
	apply(o, m, e)

	assert.Equal(t, 1, o.Counts(epoch).Truncated)

	// Clearing the flag on a later mutation must decrement the count, or the
	// metric ratchets upward forever.
	m, e = upsert("k", epoch, "a", "b")
	apply(o, m, e)
	assert.Zero(t, o.Counts(epoch).Truncated)

	got, ok := o.Get("k")
	require.True(t, ok)
	assert.False(t, got.Truncated)
}

func TestOracle_HistoryReplaysWhatWasObserved(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{RingSize: 4})

	for i := 0; i < 3; i++ {
		m, e := upsert("k", epoch.Add(time.Duration(i)*time.Second), "replica-"+strconv.Itoa(i))
		e.Seq = uint64(i + 1) //nolint:gosec // loop counter
		apply(o, m, e)
	}

	got := o.History("k")

	require.Len(t, got, 3)
	for i, h := range got {
		assert.Equal(t, uint64(i+1), h.Event.Seq, "history must be oldest first") //nolint:gosec // loop counter
		assert.Equal(t, uint64(i+1), h.Version)                                   //nolint:gosec // loop counter
		assert.Equal(t, seqtrack.Accept, h.Verdict)
		assert.Equal(t, epoch.Add(time.Duration(i)*time.Second), h.AppliedAt)
	}
	assert.True(t, got[2].ResultValue.Equal(setOf("replica-2")))
}

func TestOracle_HistoryRingNeverGrowsPastItsSize(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{RingSize: 3})

	for i := 0; i < 20; i++ {
		m, e := upsert("k", epoch, "a")
		e.Seq = uint64(i + 1) //nolint:gosec // loop counter
		apply(o, m, e)
	}

	got := o.History("k")

	// A per-key list of every event ever seen is an out-of-memory kill in a
	// system emitting millions of events per key per day.
	require.Len(t, got, 3)
	assert.Equal(t, uint64(18), got[0].Event.Seq, "the oldest entries are overwritten")
	assert.Equal(t, uint64(20), got[2].Event.Seq)
}

func TestOracle_HistoryOnAnUnknownKeyIsEmptyRatherThanAnError(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{})

	assert.Nil(t, o.History("never-seen"))
}

func TestOracle_HistoryReturnsCopies(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{RingSize: 2})
	m, e := upsert("k", epoch, "a")
	apply(o, m, e)

	got := o.History("k")
	require.Len(t, got, 1)
	got[0].ResultValue.Members["injected"] = struct{}{}

	again := o.History("k")
	assert.True(t, again[0].ResultValue.Equal(setOf("a")))
}

func TestOracle_RetainRawIsOffByDefaultBecauseItIsTenTimesTheMemory(t *testing.T) {
	raw := []byte(`{"publisher":"p","seq":1,"op":"add","key":"k","member":"a"}`)

	t.Run("the raw payload is dropped by default", func(t *testing.T) {
		o, _ := newOracle(t, oracle.Config{RingSize: 2})
		m, e := upsert("k", epoch, "a")
		e.Raw = raw
		apply(o, m, e)

		got := o.History("k")
		require.Len(t, got, 1)
		assert.Nil(t, got[0].Event.Raw,
			"retaining raw payloads costs roughly 3 GB against 300 MB at a million keys")
	})

	t.Run("the raw payload is kept when asked for", func(t *testing.T) {
		o, _ := newOracle(t, oracle.Config{RingSize: 2, RetainRaw: true})
		m, e := upsert("k", epoch, "a")
		e.Raw = raw
		apply(o, m, e)

		got := o.History("k")
		require.Len(t, got, 1)
		assert.Equal(t, raw, got[0].Event.Raw)
	})
}

func TestOracle_AdoptSnapshotLoadsBaselineStateAsUnconfirmed(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{})

	o.AdoptSnapshot(map[string]event.Value{
		"a": setOf("replica-0"),
		"b": setOf("replica-1"),
	}, epoch)

	got, ok := o.Get("a")
	require.True(t, ok)
	assert.True(t, got.Value.Equal(setOf("replica-0")))

	// Adopted keys were read from the target rather than derived from events,
	// so they cannot be used to assert the target is wrong. Adopt mode's
	// guarantee is only ever "no new drift since I started".
	assert.Equal(t, oracle.TrustAdopted, got.Trust)
	assert.Equal(t, 2, o.Counts(epoch).ByTrust[oracle.TrustAdopted])
}

func TestOracle_AnEventOnAnAdoptedKeyPromotesItToATrustedState(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{})
	o.AdoptSnapshot(map[string]event.Value{"k": setOf("replica-0")}, epoch)

	m, e := upsert("k", epoch, "replica-0", "replica-1")
	apply(o, m, e)

	got, ok := o.Get("k")
	require.True(t, ok)
	assert.Equal(t, oracle.TrustComplete, got.Trust)
	assert.Equal(t, 0, o.Counts(epoch).ByTrust[oracle.TrustAdopted])
}

func TestOracle_AdoptSnapshotCopiesTheValuesItIsGiven(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{})
	value := setOf("replica-0")

	o.AdoptSnapshot(map[string]event.Value{"k": value}, epoch)
	value.Members["injected-by-the-caller"] = struct{}{}

	got, ok := o.Get("k")
	require.True(t, ok)
	assert.True(t, got.Value.Equal(setOf("replica-0")))
}

func TestOracle_AdoptSnapshotRespectsTheKeyBudget(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{Shards: 1, MaxTrackedKeys: 10})

	entries := map[string]event.Value{}
	for i := 0; i < 50; i++ {
		entries["k"+strconv.Itoa(i)] = setOf("a")
	}
	o.AdoptSnapshot(entries, epoch)

	assert.LessOrEqual(t, o.Len(), 10, "adoption must not be a way around the bound")
	assert.Positive(t, o.Evictions())
}
