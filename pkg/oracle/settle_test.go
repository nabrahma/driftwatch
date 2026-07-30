package oracle_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/seqtrack"
)

func TestOracle_MarkSuspectWithAnEmptyPatternCoversEveryKey(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{})
	for i := 0; i < 20; i++ {
		m, e := upsert("k"+strconv.Itoa(i), epoch, "a")
		apply(o, m, e)
	}
	require.Equal(t, 20, o.Counts(epoch).ByTrust[oracle.TrustComplete])

	o.MarkSuspect("", "sequence gap on publisher p")

	// driftwatch cannot know which keys the lost events touched — that
	// information was in the lost events — so without key ownership every key
	// becomes suspect and the check stops asserting until trust is restored.
	assert.Equal(t, 20, o.Counts(epoch).ByTrust[oracle.TrustSuspect])
	assert.Equal(t, 0, o.Counts(epoch).ByTrust[oracle.TrustComplete])

	got, ok := o.Get("k0")
	require.True(t, ok)
	assert.Equal(t, oracle.TrustSuspect, got.Trust)
}

func TestOracle_KeysWrittenAfterAMarkAreTrustedAgain(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{})
	m, e := upsert("before", epoch, "a")
	apply(o, m, e)

	o.MarkSuspect("", "gap")

	m, e = upsert("after", epoch, "a")
	apply(o, m, e)

	before, _ := o.Get("before")
	after, _ := o.Get("after")

	// The generation floor is a point in time, not a permanent verdict: an
	// entry written after the gap was recorded is derived from events
	// driftwatch actually saw.
	assert.Equal(t, oracle.TrustSuspect, before.Trust)
	assert.Equal(t, oracle.TrustComplete, after.Trust)

	counts := o.Counts(epoch)
	assert.Equal(t, 1, counts.ByTrust[oracle.TrustSuspect])
	assert.Equal(t, 1, counts.ByTrust[oracle.TrustComplete])
}

func TestOracle_MarkSuspectWithAPatternScopesSuspicionToOnePartition(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{})

	for _, key := range []string{"replica:0:a", "replica:0:b", "replica:1:a", "other"} {
		m, e := upsert(key, epoch, "x")
		apply(o, m, e)
	}

	o.MarkSuspect("replica:0:*", "gap on replica 0")

	// When a projection declares that publishers own disjoint keyspaces, only
	// the affected partition loses trust. The alternative is suppressing every
	// finding in the store because one publisher dropped an event.
	for _, key := range []string{"replica:0:a", "replica:0:b"} {
		got, ok := o.Get(key)
		require.True(t, ok, key)
		assert.Equal(t, oracle.TrustSuspect, got.Trust, key)
	}
	for _, key := range []string{"replica:1:a", "other"} {
		got, ok := o.Get(key)
		require.True(t, ok, key)
		assert.Equal(t, oracle.TrustComplete, got.Trust, key)
	}

	counts := o.Counts(epoch)
	assert.Equal(t, 2, counts.ByTrust[oracle.TrustSuspect])
	assert.Equal(t, 2, counts.ByTrust[oracle.TrustComplete])
}

func TestOracle_AMalformedPatternMatchesNothingRatherThanPanicking(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{})
	m, e := upsert("k", epoch, "a")
	apply(o, m, e)

	// A bad pattern in a DriftCheck must not take the check down at the first
	// gap, which is the moment it is least affordable.
	o.MarkSuspect("[", "gap")

	got, ok := o.Get("k")
	require.True(t, ok)
	assert.Equal(t, oracle.TrustComplete, got.Trust)
}

func TestOracle_MarkSuspectLeavesAdoptedKeysAlone(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{})
	o.AdoptSnapshot(map[string]event.Value{"adopted": setOf("a")}, epoch)

	o.MarkSuspect("", "gap")

	got, ok := o.Get("adopted")
	require.True(t, ok)

	// Adopted already means "never confirmed by an event", which is a weaker
	// claim than Suspect. Overwriting it would lose the distinction between a
	// key driftwatch never verified and one it verified and then lost track of.
	assert.Equal(t, oracle.TrustAdopted, got.Trust)
}

func TestOracle_ClearSuspectRestoresTrustAfterASnapshotCycle(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{})
	for i := 0; i < 10; i++ {
		m, e := upsert("k"+strconv.Itoa(i), epoch, "a")
		apply(o, m, e)
	}
	o.MarkSuspect("", "gap")
	require.Equal(t, 10, o.Counts(epoch).ByTrust[oracle.TrustSuspect])

	o.ClearSuspect("")

	// A completed snapshot means the publisher retransmitted its whole state,
	// so what was missed before it no longer affects the oracle. This is
	// invariant I10.
	assert.Equal(t, 10, o.Counts(epoch).ByTrust[oracle.TrustComplete])
	assert.Equal(t, 0, o.Counts(epoch).ByTrust[oracle.TrustSuspect])
}

func TestOracle_ClearSuspectClearsBothTheFloorAndThePerKeyFlags(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{})

	m, e := upsert("floor-marked", epoch, "a")
	apply(o, m, e)
	o.MarkSuspect("", "gap") // suspect via the generation floor

	m, e = upsert("flag-marked", epoch, "a")
	apply(o, m, e)
	o.MarkSuspect("flag-marked", "gap") // suspect via a per-key flag

	require.Equal(t, 2, o.Counts(epoch).ByTrust[oracle.TrustSuspect])

	o.ClearSuspect("")

	// The two mechanisms have to be cleared together, or a snapshot would
	// restore trust for some keys and silently leave others suspect forever.
	assert.Equal(t, 2, o.Counts(epoch).ByTrust[oracle.TrustComplete])
	assert.Equal(t, 0, o.Counts(epoch).ByTrust[oracle.TrustSuspect])
}

func TestOracle_ClearSuspectWithAPatternIsScoped(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{})
	for _, key := range []string{"replica:0:a", "replica:1:a"} {
		m, e := upsert(key, epoch, "x")
		apply(o, m, e)
	}
	o.MarkSuspect("", "gap")

	o.ClearSuspect("replica:0:*")

	cleared, _ := o.Get("replica:0:a")
	stillSuspect, _ := o.Get("replica:1:a")
	assert.Equal(t, oracle.TrustComplete, cleared.Trust)
	assert.Equal(t, oracle.TrustSuspect, stillSuspect.Trust)
}

func TestOracle_ClearSuspectLeavesAdoptedKeysAdopted(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{})
	o.AdoptSnapshot(map[string]event.Value{"adopted": setOf("a")}, epoch)

	o.ClearSuspect("")

	got, ok := o.Get("adopted")
	require.True(t, ok)
	assert.Equal(t, oracle.TrustAdopted, got.Trust,
		"a snapshot cycle does not turn an unverified key into a verified one")
}

func TestOracle_ApplyCarriesTheTrustStateTheTrackerDecided(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{})

	m, e := upsert("k", epoch, "a")
	o.Apply(m, e, seqtrack.AcceptWithGap, oracle.TrustSuspect)

	got, ok := o.Get("k")
	require.True(t, ok)
	assert.Equal(t, oracle.TrustSuspect, got.Trust)
	assert.Equal(t, 1, o.Counts(epoch).ByTrust[oracle.TrustSuspect])
}

func TestOracle_SettlementIndexSurvivesManyRoundsOfMovement(t *testing.T) {
	// The index moves keys between buckets on every event and coalesces old
	// buckets on iteration. A bookkeeping slip shows up as a key that is
	// permanently settled or permanently in flight, so the accounting is
	// checked after a lot of churn rather than after one move.
	o, _ := newOracle(t, oracle.Config{
		Shards:              4,
		SettlementWindow:    5 * time.Second,
		MaxSettlementWindow: 20 * time.Second,
		BucketWidth:         time.Second,
	})

	now := epoch
	for round := 0; round < 50; round++ {
		now = now.Add(time.Second)
		for i := 0; i < 20; i++ {
			m, e := upsert("k"+strconv.Itoa(i), now, "a")
			apply(o, m, e)
		}
		settledKeys(o, now)
	}

	counts := o.Counts(now)
	assert.Equal(t, 20, counts.Total)
	assert.Equal(t, 0, counts.Settled, "every key was just touched")
	assert.Equal(t, 20, counts.InFlight)

	later := now.Add(time.Hour)
	assert.Len(t, settledKeys(o, later), 20)
	assert.Equal(t, 20, o.Counts(later).Settled)
}

func TestOracle_ConfigDefaultsProduceAUsableOracle(t *testing.T) {
	o := oracle.New(oracle.Config{})

	m, e := upsert("k", epoch, "a")
	res := o.Apply(m, e, seqtrack.Accept, oracle.TrustComplete)

	require.True(t, res.Applied)
	assert.Equal(t, 120*time.Second, oracle.New(oracle.Config{SettlementWindow: time.Hour}).SettlementWindow(),
		"a window beyond the default maximum is clamped at construction")
	assert.Len(t, o.History("k"), 1)
}

func TestOracle_ARingSizeOfZeroUsesTheDefaultRatherThanDisablingHistory(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{RingSize: 0})

	m, e := upsert("k", epoch, "a")
	apply(o, m, e)

	assert.Len(t, o.History("k"), 1)
}
