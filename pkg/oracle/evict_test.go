package oracle_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/oracle"
)

func TestOracle_EvictsRatherThanGrowingPastTheKeyBudget(t *testing.T) {
	// A monitoring tool that dies when the system it watches grows is worse
	// than no monitoring tool, so the oracle degrades coverage instead.
	o, _ := newOracle(t, oracle.Config{Shards: 1, MaxTrackedKeys: 10})

	for i := 0; i < 100; i++ {
		m, e := upsert("k"+strconv.Itoa(i), epoch.Add(time.Duration(i)*time.Second), "a")
		apply(o, m, e)
	}

	assert.Equal(t, 10, o.Len())
	assert.Equal(t, uint64(90), o.Evictions())
}

func TestOracle_EvictionNamesTheKeyItDropped(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{Shards: 1, MaxTrackedKeys: 2})

	m, e := upsert("first", epoch, "a")
	apply(o, m, e)
	m, e = upsert("second", epoch.Add(time.Second), "a")
	apply(o, m, e)

	m, e = upsert("third", epoch.Add(2*time.Second), "a")
	res := apply(o, m, e)

	// The caller has to be told coverage shrank rather than left to infer it
	// from a quiet dashboard.
	assert.NotEmpty(t, res.Evicted)
	assert.NotEqual(t, "third", res.Evicted, "the key being inserted must never be the victim")
	_, stillThere := o.Get(res.Evicted)
	assert.False(t, stillThere)
	_, inserted := o.Get("third")
	assert.True(t, inserted)
}

func TestOracle_EvictionPrefersKeysPastTheCoalescingHorizon(t *testing.T) {
	// The victim choice is approximate on purpose: within the coalesced settled
	// set, and within any single time bucket, the pick is arbitrary. Asserting
	// a total ordering over the three would be asserting something the
	// implementation does not provide — an earlier version of this test did
	// exactly that and passed only on the luck of Go's map iteration order.
	//
	// What is guaranteed, and what this checks, is the tier: a key old enough
	// to have been folded into the settled set goes before one still sitting in
	// a live bucket.
	o, _ := newOracle(t, oracle.Config{
		Shards: 1, MaxTrackedKeys: 3,
		BucketWidth:         time.Second,
		MaxSettlementWindow: 10 * time.Second,
	})

	m, e := upsert("ancient", epoch, "a")
	apply(o, m, e)
	m, e = upsert("older", epoch.Add(55*time.Second), "a")
	apply(o, m, e)
	m, e = upsert("newer", epoch.Add(56*time.Second), "a")
	apply(o, m, e)

	// Iterating at 60s coalesces every bucket older than 50s, which is
	// "ancient" alone; the other two stay in live buckets.
	settledKeys(o, epoch.Add(60*time.Second))

	m, e = upsert("newest", epoch.Add(60*time.Second), "a")
	res := apply(o, m, e)

	assert.True(t, res.DidEvict)
	assert.Equal(t, "ancient", res.Evicted,
		"the only key past the coalescing horizon must be the one evicted")

	for _, kept := range []string{"older", "newer", "newest"} {
		_, ok := o.Get(kept)
		assert.True(t, ok, "%s should have survived", kept)
	}
}

func TestOracle_UpdatingAnExistingKeyAtCapacityEvictsNothing(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{Shards: 1, MaxTrackedKeys: 2})

	m, e := upsert("a", epoch, "x")
	apply(o, m, e)
	m, e = upsert("b", epoch, "x")
	apply(o, m, e)
	require.Equal(t, uint64(0), o.Evictions())

	m, e = upsert("a", epoch.Add(time.Second), "x", "y")
	res := apply(o, m, e)

	assert.Empty(t, res.Evicted)
	assert.Equal(t, uint64(0), o.Evictions())
	assert.Equal(t, 2, o.Len())
}

func TestOracle_TheKeyBudgetIsDistributedSoTheTotalNeverExceedsIt(t *testing.T) {
	// Rounding the per-shard share up would let 64 shards hold 64 keys more
	// than the configured limit, which invariant I8 forbids outright.
	tests := []struct {
		shards, maxKeys int
	}{
		{shards: 4, maxKeys: 10},
		{shards: 8, maxKeys: 3},
		{shards: 64, maxKeys: 100},
		{shards: 3, maxKeys: 1},
	}

	for _, tc := range tests {
		t.Run(strconv.Itoa(tc.shards)+"shards/"+strconv.Itoa(tc.maxKeys)+"keys", func(t *testing.T) {
			o, _ := newOracle(t, oracle.Config{Shards: tc.shards, MaxTrackedKeys: tc.maxKeys})

			for i := 0; i < tc.maxKeys*20; i++ {
				m, e := upsert("key-"+strconv.Itoa(i), epoch, "a")
				apply(o, m, e)
			}

			assert.LessOrEqual(t, o.Len(), tc.maxKeys)
		})
	}
}

func TestOracle_AnEvictedKeyLosesItsHistoryTooRatherThanLeakingIt(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{Shards: 1, MaxTrackedKeys: 1, RingSize: 4})

	m, e := upsert("first", epoch, "a")
	apply(o, m, e)
	require.Len(t, o.History("first"), 1)

	m, e = upsert("second", epoch.Add(time.Second), "a")
	apply(o, m, e)

	assert.Nil(t, o.History("first"), "history must not outlive the entry it belongs to")
}

func TestOracle_EvictionKeepsTheTrustCountsHonest(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{Shards: 1, MaxTrackedKeys: 3})

	for i := 0; i < 20; i++ {
		m, e := upsert("k"+strconv.Itoa(i), epoch.Add(time.Duration(i)*time.Second), "a")
		apply(o, m, e)
	}

	counts := o.Counts(epoch.Add(time.Hour))

	// The cached cardinalities are maintained incrementally, so a bookkeeping
	// slip on eviction would show up here as a total that disagrees with
	// reality rather than as a crash.
	assert.Equal(t, 3, counts.Total)
	assert.Equal(t, 3, counts.ByTrust[oracle.TrustComplete]+
		counts.ByTrust[oracle.TrustSuspect]+counts.ByTrust[oracle.TrustAdopted])
}

func TestOracle_TheEmptyKeyIsEvictableLikeAnyOther(t *testing.T) {
	// Redis accepts the empty string as a key, so "" cannot double as a "no
	// victim found" sentinel inside eviction. It did, and a shard whose only
	// evictable entry was the empty key silently declined to evict and let the
	// key budget be exceeded. TestProp_MemoryBounded found it.
	o, _ := newOracle(t, oracle.Config{Shards: 1, MaxTrackedKeys: 1})

	m, e := upsert("", epoch, "a")
	apply(o, m, e)
	require.Equal(t, 1, o.Len())

	m, e = upsert("next", epoch.Add(time.Second), "a")
	res := apply(o, m, e)

	assert.True(t, res.DidEvict, "an eviction happened and must be reported as one")
	assert.Equal(t, "", res.Evicted, "the empty key is the key that was evicted")
	assert.Equal(t, 1, o.Len())

	_, stillThere := o.Get("")
	assert.False(t, stillThere)
}

func TestOracle_NoEvictionIsReportedWhenNoneHappened(t *testing.T) {
	o, _ := newOracle(t, oracle.Config{Shards: 1, MaxTrackedKeys: 10})

	m, e := upsert("k", epoch, "a")
	res := apply(o, m, e)

	assert.False(t, res.DidEvict)
	assert.Empty(t, res.Evicted)
}

func TestOracle_PerShardBudgetsCostSomeCapacityToHashImbalance(t *testing.T) {
	// The key budget is enforced per shard so eviction never needs two locks,
	// and hashing does not divide keys evenly. Filling an oracle to exactly its
	// configured budget therefore evicts a small fraction of keys while the
	// oracle is globally not full.
	//
	// This is measured rather than assumed, so that a change to the hash or the
	// shard count shows up as a number instead of a surprise. See
	// docs/DISCOVERIES.md D-003.
	const (
		shards  = 64
		maxKeys = 100_000
	)

	o, _ := newOracle(t, oracle.Config{Shards: shards, MaxTrackedKeys: maxKeys})

	for i := 0; i < maxKeys; i++ {
		m, e := upsert("block-"+strconv.Itoa(i), epoch, "a")
		apply(o, m, e)
	}

	tracked := o.Len()
	lossRatio := float64(maxKeys-tracked) / float64(maxKeys)

	assert.LessOrEqual(t, tracked, maxKeys, "the bound is an upper bound and must hold")
	assert.Less(t, lossRatio, 0.05,
		"lost %.2f%% of capacity to shard imbalance (%d of %d keys); "+
			"more than a few percent means the hash or the shard count regressed",
		lossRatio*100, maxKeys-tracked, maxKeys)
	t.Logf("64 shards, %d key budget: %d tracked, %.2f%% lost to imbalance",
		maxKeys, tracked, lossRatio*100)
}
