package check_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/check"
	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/source"
	"github.com/nabrahma/driftwatch/pkg/target"
)

// The three answers to "what do I do about the keys that were already there?".
//
// §5.6. A checker attaching to a running system finds a store full of keys no
// event has explained, and every one of the three defensible answers has a
// different cost. Getting the mode wrong is not a crash — it is a check that
// reports drift for the entire pre-existing keyspace, or one that vouches for
// keys it has no evidence about. Both look like working software.

func specWithBootstrap(mode string) string {
	return strings.Replace(inProcessSpec, "  bootstrap: Wait", "  bootstrap: "+mode, 1)
}

func TestBootstrap_WaitStartsEmptyAndTreatsExistingKeysAsExtras(t *testing.T) {
	// The conservative default. The oracle starts empty and fills from events,
	// so a key the store already held is something driftwatch has no
	// expectation for — which is what the target→oracle pass calls an extra,
	// not what the oracle→target pass would call drift.
	c := newCheck(t, specWithBootstrap("Wait"))

	store, ok := c.Target().(*target.MemoryTarget)
	require.True(t, ok)
	store.SeedSets(map[string][]string{
		"block:pre-existing-0": {"replica-0"},
		"block:pre-existing-1": {"replica-1"},
	})

	stop := running(t, c)
	defer stop()

	status := c.Status()
	assert.Zero(t, status.TrackedKeys,
		"Wait starts with an empty oracle; adopting silently would be Adopt")
	assert.Zero(t, status.AdoptedKeys)

	// And a sweep reports no drift, because there is nothing to compare: the
	// oracle has no expectation for either key.
	rep, err := c.SweepNow(context.Background())
	require.NoError(t, err)
	assert.Zero(t, rep.Total(),
		"two keys the oracle never heard of are not drift: %s", rep.Summary())
}

func TestBootstrap_AdoptReadsTheKeyspaceInAsABaseline(t *testing.T) {
	// The pragmatic mode, for attaching to a system that has been running for
	// months. driftwatch reads what the store holds and adopts it as the
	// starting expectation — which trades a real guarantee for immediate
	// coverage, and the trade has to be visible.
	//
	// The load-bearing part is that an adopted key is *marked*: comparing one
	// against the target proves only that the target agrees with itself, so the
	// sweeper skips it until an event touches it (§5.6).
	clk := clock.Fake(epoch())
	c := newCheckWith(t, specWithBootstrap("Adopt"), clk)

	store, ok := c.Target().(*target.MemoryTarget)
	require.True(t, ok)
	store.SeedSets(map[string][]string{
		"block:existing-0": {"replica-0"},
		"block:existing-1": {"replica-1"},
		"block:existing-2": {"replica-0", "replica-1"},
	})

	stop := running(t, c)
	defer stop()

	// Past the settlement window, so the adopted keys are eligible for
	// comparison and the sweeper has to decide what to do with them. Before
	// this they are skipped as in-flight, which is a different reason.
	clk.Advance(5 * time.Second)

	status := c.Status()
	assert.Equal(t, 3, status.TrackedKeys,
		"Adopt should have read the whole keyspace in")
	assert.Equal(t, 3, status.AdoptedKeys,
		"adopted keys have to be counted separately, or an operator cannot "+
			"tell an expectation backed by events from one backed by a guess")

	rep, err := c.SweepNow(context.Background())
	require.NoError(t, err)
	assert.Zero(t, rep.Total())
	assert.Equal(t, 3, rep.KeysSkippedAdopted,
		"comparing an adopted key against the target it was read from proves "+
			"only that the target agrees with itself")
}

func TestBootstrap_StrictAssertsNothingUntilASnapshotArrives(t *testing.T) {
	// The mode that means what it says. §15 row 46 caught this being a log line
	// that behaved exactly like Wait: it announced that it would not assert on
	// anything, and then asserted on everything.
	//
	// Marking every key suspect up front is what makes the promise real,
	// because a suspect key produces no alertable finding. Nothing is asserted
	// until a publisher completes a snapshot cycle and clears it.
	clk := clock.Fake(epoch())
	c := newCheckWith(t, specWithBootstrap("Strict"), clk)
	stop := running(t, c)
	defer stop()

	assert.True(t, c.Status().AwaitingSnapshot,
		"Strict should report that it is waiting")
	assert.Equal(t, check.PhaseAwaitingSnapshot, c.Status().Phase)

	// An event arriving now must not become a trustworthy key. This is the
	// subtle half: marking the *existing* keys suspect is not enough, because a
	// key created after bootstrap would be written at the current generation
	// and come out complete — leaking assertions about exactly the keyspace the
	// mode was told not to assert on.
	publish(t, c, addEventJSON("replica-0", 1, "0", "replica-0"))
	assertTrust(t, c, "block:0", oracle.TrustSuspect,
		"a key created during Strict bootstrap must not be trustworthy")

	// A key that disagrees produces no alertable finding while suspect.
	clk.Advance(5 * time.Second)
	rep, err := c.SweepNow(context.Background())
	require.NoError(t, err)
	assert.Zero(t, rep.Alertable(),
		"Strict must not raise an alertable finding before a snapshot: %s",
		rep.Summary())

	// The snapshot cycle is what ends it.
	src := mustMemorySource(t, c)
	for _, payload := range []string{
		`{"publisher":"replica-0","epoch":1,"seq":2,"op":"snapshotBegin"}`,
		addEventJSON("replica-0", 3, "0", "replica-0"),
		`{"publisher":"replica-0","epoch":1,"seq":4,"op":"snapshotEnd"}`,
	} {
		require.True(t, src.PublishPayload([]byte(payload)))
	}

	require.Eventually(t, func() bool { return !c.Status().AwaitingSnapshot },
		eventuallyFor, eventuallyPoll,
		"a completed snapshot should end the Strict wait")

	assert.Equal(t, check.PhaseWatching, c.Status().Phase,
		"after the snapshot the check is in steady state")
	assertTrust(t, c, "block:0", oracle.TrustComplete)
}

func TestBootstrap_AdoptOnAKeyspaceLargerThanTheOracleReducesCoverageRatherThanFailing(t *testing.T) {
	// §5.6 again, and the reason it is spelled out: a check that refuses to
	// start on a large keyspace is a check that is never used on the systems
	// that most need it. driftwatch adopts what fits, says how much it left,
	// and carries on with a coverage ratio that is honest about the shortfall.
	// 1,000 is the configured floor — the CRD refuses anything smaller, on the
	// grounds that an oracle too small to hold a useful keyspace is a
	// misconfiguration rather than a tuning choice.
	const capacity = 1000
	const present = 3000

	spec := strings.Replace(
		specWithBootstrap("Adopt"),
		"  bootstrap: Adopt",
		fmt.Sprintf("  bootstrap: Adopt\n  maxTrackedKeys: %d", capacity), 1)

	c := newCheck(t, spec)

	store, ok := c.Target().(*target.MemoryTarget)
	require.True(t, ok)
	seed := map[string][]string{}
	for i := 0; i < present; i++ {
		seed[fmt.Sprintf("block:k%05d", i)] = []string{"replica-0"}
	}
	store.SeedSets(seed)

	stop := running(t, c)
	defer stop()

	status := c.Status()
	assert.LessOrEqual(t, status.TrackedKeys, capacity,
		"the oracle must not exceed its cap during adoption")
	// The lower bound is 80%, not 95%, and D-003 is why.
	//
	// The key budget is enforced per shard so that eviction stays shard-local.
	// Hashing 1,000 keys across 64 shards gives a fair share of 15.6 per shard,
	// and at bins that small the relative deviation is large — the observed
	// loss here is around 11%, against the 0.31% D-003 measured at a million
	// keys. That is the same effect, not a different one: D-003 states plainly
	// that the deviation grows as the bins shrink.
	//
	// A tighter bound here would be a test that fails on a hash change while
	// telling nobody anything about adoption.
	assert.Greater(t, status.TrackedKeys, capacity*8/10,
		"adoption should fill most of the available capacity, not a token "+
			"amount; D-003 accounts for the shortfall")
	assert.Positive(t, status.TrackedKeys,
		"adoption should take what fits rather than giving up")
	assert.NotEqual(t, check.PhaseFailed, status.Phase,
		"a keyspace larger than the oracle is a coverage problem, not a failure")
}

func TestStatus_CoverageRatioIsHonestAboutWhatWasNotCompared(t *testing.T) {
	// The single most important number driftwatch reports, because every other
	// number is scoped to it. "0 divergent keys" means nothing without it: a
	// check comparing 3% of the keyspace and finding nothing wrong has not
	// found that the keyspace is fine.
	clk := clock.Fake(epoch())
	c := newCheckWith(t, inProcessSpec, clk)
	stop := running(t, c)
	defer stop()

	const keys = 10
	for i := 0; i < keys; i++ {
		publish(t, c, addEventJSON("replica-0", uint64(i+1), fmt.Sprintf("k%d", i), "replica-0"))
	}

	// Nothing has settled yet, so nothing can be compared.
	rep, err := c.SweepNow(context.Background())
	require.NoError(t, err)
	require.Zero(t, rep.KeysCompared)
	assert.Zero(t, c.Status().CoverageRatio,
		"before anything settles, driftwatch has verified nothing and must "+
			"say so rather than reporting full coverage")

	// Once they settle, all of them are compared.
	clk.Advance(5 * time.Second)
	rep, err = c.SweepNow(context.Background())
	require.NoError(t, err)
	require.Equal(t, keys, rep.KeysCompared)
	assert.InDelta(t, 1.0, c.Status().CoverageRatio, 0.001,
		"every tracked key was compared, so coverage is complete")
}

func TestStatus_SummaryNamesTheSuspectCountSeparately(t *testing.T) {
	// Summary is the one line that goes into a log and a CLI header, and the
	// distinction it has to preserve is confirmed-versus-suspect. Collapsing
	// them into one number is the §23 A7 mistake in its cheapest form: it reads
	// as "driftwatch found N problems" when some of those N are driftwatch
	// admitting it does not know.
	var s check.Status
	s.Phase = check.PhaseWatching
	s.TrackedKeys = 100
	s.SettledKeys = 90
	s.DivergentKeys = 2
	s.SuspectDivergentKeys = 5
	s.EventsApplied = 1000
	s.EventsDropped = 3
	s.SettlementWindowSeconds = 5

	line := s.Summary()

	assert.Contains(t, line, "drift 2", "the confirmed count: %s", line)
	assert.Contains(t, line, "+5 suspect",
		"the suspect count must be visible and marked as separate, not folded "+
			"into the drift number: %s", line)
	assert.Contains(t, line, "3 dropped", "%s", line)
	assert.Contains(t, line, "W 5.0s", "%s", line)
}

func TestStatus_SummaryOmitsWhatIsZero(t *testing.T) {
	// The healthy line is the one read thousands of times a day, so it carries
	// only what is true: no "(0 dropped)", no "(+0 suspect)".
	var s check.Status
	s.Phase = check.PhaseWatching
	s.TrackedKeys = 100
	s.SettledKeys = 100
	s.SettlementWindowSeconds = 5
	s.TargetReachable = true

	line := s.Summary()

	assert.NotContains(t, line, "dropped", "%s", line)
	assert.NotContains(t, line, "suspect", "%s", line)
	assert.NotContains(t, line, "unreachable", "%s", line)
}

func mustMemorySource(t *testing.T, c *check.Check) *source.MemorySource {
	t.Helper()
	src, ok := c.Source().(*source.MemorySource)
	require.True(t, ok, "the spec configures a memory source")
	return src
}
