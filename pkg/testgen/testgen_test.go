package testgen_test

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"pgregory.net/rapid"

	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/testgen"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// The generators are the foundation every property test stands on. A generator
// that stops producing interesting inputs weakens every property built on it
// without failing anything, so each one is checked here directly.

func TestProp_GeneratedEventsAreAlwaysStructurallyValid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		e := testgen.Event(t, "pub-0", rapid.Uint64().Draw(t, "seq"))

		require.NoError(t, e.Validate(),
			"a generator that emits invalid events makes every property test report defects that are not there")
	})
}

func TestProp_GeneratedKeyEventsAlwaysTouchAKey(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		e := testgen.KeyEvent(t, "pub-0", 1)

		require.NoError(t, e.Validate())
		assert.True(t, e.Op.TouchesKey())
	})
}

func TestProp_EventStreamsHaveContiguousAscendingSeqsPerPublisher(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		publishers := rapid.IntRange(1, 4).Draw(t, "publishers")
		count := rapid.IntRange(0, 40).Draw(t, "count")

		evs := testgen.EventStream(t, publishers, count)
		require.Len(t, evs, count)

		// A gap in a generated stream would make the gap-detection property
		// tests pass for the wrong reason.
		next := map[string]uint64{}
		for _, e := range evs {
			want, seen := next[e.Publisher]
			if !seen {
				want = 1
			}
			assert.Equal(t, want, e.Seq, "publisher %s must be contiguous", e.Publisher)
			next[e.Publisher] = e.Seq + 1
		}
	})
}

func TestProp_KeyEventStreamsOnlyContainKeyTouchingOps(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		evs := testgen.KeyEventStream(t, rapid.IntRange(1, 3).Draw(t, "publishers"), 20)

		for _, e := range evs {
			assert.True(t, e.Op.TouchesKey(), "op %s must touch a key", e.Op)
		}
	})
}

func TestEventStream_ClampsNonsensicalArguments(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		assert.Empty(t, testgen.EventStream(t, 0, -1))
		assert.Empty(t, testgen.KeyEventStream(t, -3, -1))
		assert.Len(t, testgen.EventStream(t, 0, 3), 3)
		assert.Len(t, testgen.KeyEventStream(t, -1, 3), 3)
	})
}

func TestProp_PermutationPreservesTheMultisetAndLeavesTheInputAlone(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		evs := testgen.EventStream(t, 2, rapid.IntRange(0, 20).Draw(t, "count"))
		before := fingerprints(evs)

		got := testgen.Permutation(t, evs)

		assert.Equal(t, before, fingerprints(evs), "Permutation must not modify its input")
		assert.ElementsMatch(t, before, fingerprints(got),
			"Permutation must reorder, never add or drop")
	})
}

func TestProp_WithdrawSubsetPartitionsTheStreamExactly(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		evs := testgen.EventStream(t, 2, rapid.IntRange(0, 30).Draw(t, "count"))

		kept, withheld := testgen.WithdrawSubset(t, evs)

		// Every event goes to exactly one side. If this ever failed, the gap
		// detection property would be counting the wrong thing.
		assert.Len(t, kept, len(evs)-len(withheld))
		assert.ElementsMatch(t, fingerprints(evs),
			append(fingerprints(kept), fingerprints(withheld)...))
	})
}

func TestProp_GeneratedValuesMatchTheRequestedKind(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		kinds := []event.ValueKind{
			event.ValueAbsent, event.ValueScalar, event.ValueSet, event.ValueCounter,
		}
		kind := rapid.SampledFrom(kinds).Draw(t, "kind")

		v := testgen.Value(t, kind)

		assert.Equal(t, kind, v.Kind)
		assert.True(t, v.Equal(v.Clone()), "a generated value must survive a round trip through Clone")
	})
}

func TestValue_UnrecognizedKindsGenerateAnAbsentValue(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		assert.True(t, testgen.Value(t, event.ValueKind(9)).IsAbsent())
	})
}

func TestProp_AnyValueProducesEveryKindOverEnoughDraws(t *testing.T) {
	seen := map[event.ValueKind]bool{}

	rapid.Check(t, func(t *rapid.T) {
		v := testgen.AnyValue(t)
		seen[v.Kind] = true
	})

	assert.Len(t, seen, 4, "AnyValue must reach every value kind, got %v", seen)
}

func TestProp_KeyGeneratorReachesTheAwkwardCases(t *testing.T) {
	var sawEmpty, sawBinary, sawGlob, sawLong bool

	rapid.Check(t, func(t *rapid.T) {
		k := testgen.Key(t)
		switch {
		case k == "":
			sawEmpty = true
		case len(k) >= 64:
			sawLong = true
		}
		for i := 0; i < len(k); i++ {
			switch {
			case k[i] >= 0x80:
				sawBinary = true
			case k[i] == '*' || k[i] == '?' || k[i] == '[':
				sawGlob = true
			}
		}
	})

	// These are exactly the shapes that break naive key handling: an empty key
	// is legal in Redis, glob metacharacters matter to MarkSuspect patterns and
	// SCAN MATCH, and binary keys break anything that assumes UTF-8.
	assert.True(t, sawEmpty, "Key must generate the empty key")
	assert.True(t, sawBinary, "Key must generate non-ASCII bytes")
	assert.True(t, sawGlob, "Key must generate glob metacharacters")
	assert.True(t, sawLong, "Key must generate keys long enough to exercise size caps")
}

func TestProp_MemberGeneratorNeverProducesTheEmptyString(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// An empty member fails Validate, so generating one would produce
		// property failures that are the generator's fault.
		assert.NotEmpty(t, testgen.Member(t))
	})
}

func TestOpGeneratorsCoverEveryOperation(t *testing.T) {
	// Asserted against the generators' declared range rather than by sampling
	// them, because "can produce every operation" is a fact about a slice and
	// sampling only ever approximates it.
	//
	// It used to collect draws across rapid's hundred runs and assert eight
	// distinct operations had appeared. That is sound in expectation and fails
	// on the run where one does not come up — which it did, in CI, missing
	// snapshot_begin. rapid does not draw uniformly at random on purpose: it
	// biases towards values it thinks are interesting and shrinks towards the
	// front of the slice, so the tail of a SampledFrom is genuinely less
	// likely, and no number of runs makes that guarantee rather than merely
	// probable.
	//
	// A flaky test that guards a real property is worse than either a solid
	// test or none, because the failure teaches everyone to re-run CI.
	want := []event.Op{
		event.OpSet, event.OpDelete, event.OpAdd, event.OpRemove, event.OpIncr,
		event.OpSnapshotBegin, event.OpSnapshotEnd, event.OpHeartbeat,
	}
	assert.ElementsMatch(t, want, testgen.AllOps(),
		"Op's range must be every wire operation; a missing one is a whole class "+
			"of event no property test in this repository would ever generate")

	assert.ElementsMatch(t,
		[]event.Op{event.OpSet, event.OpDelete, event.OpAdd, event.OpRemove, event.OpIncr},
		testgen.KeyTouchingOps(),
		"KeyTouchingOp's range must be exactly the operations that affect a key")

	assert.NotContains(t, testgen.AllOps(), event.OpUnknown,
		"OpUnknown is the zero value, not a wire operation: generating it would "+
			"produce validation failures that are the generator's fault")
}

func TestProp_OpGeneratorsStayInsideTheirRange(t *testing.T) {
	// The half that is genuinely a property. The range is checked above; what
	// random draws are good for is confirming nothing outside it ever escapes.
	all := map[event.Op]bool{}
	for _, op := range testgen.AllOps() {
		all[op] = true
	}
	keyTouching := map[event.Op]bool{}
	for _, op := range testgen.KeyTouchingOps() {
		keyTouching[op] = true
	}

	rapid.Check(t, func(t *rapid.T) {
		assert.Contains(t, all, testgen.Op(t))
		assert.Contains(t, keyTouching, testgen.KeyTouchingOp(t))
	})
}

func fingerprints(evs []event.Event) []string {
	out := make([]string, 0, len(evs))
	for i := range evs {
		out = append(out, evs[i].Fingerprint().String()+"/"+evs[i].Op.String()+"/"+evs[i].Key)
	}
	sort.Strings(out)
	return out
}
