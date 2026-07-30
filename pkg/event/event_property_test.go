package event_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/testgen"
)

// Value.Equal is the comparison every divergence finding ultimately rests on.
// If it is not an equivalence relation, the differ can report a key as
// divergent in one sweep and equal in the next with nothing having changed.

func TestProp_ValueEqualIsReflexive(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		v := testgen.AnyValue(t)

		// Comparing a value with itself is the whole point of reflexivity, so
		// the duplicated argument here is deliberate.
		//nolint:gocritic // dupArg: self-comparison is the property under test
		assert.True(t, v.Equal(v), "%s must equal itself", v)
	})
}

func TestProp_ValueEqualIsSymmetric(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		a := testgen.AnyValue(t)
		b := testgen.AnyValue(t)

		assert.Equal(t, a.Equal(b), b.Equal(a), "Equal(%s, %s) is not symmetric", a, b)
	})
}

func TestProp_ValueEqualIsTransitive(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		a := testgen.AnyValue(t)
		b := testgen.AnyValue(t)
		c := testgen.AnyValue(t)

		// The empty-set-equals-absent rule is the one that could plausibly break
		// transitivity, so this property is what keeps that rule honest.
		if a.Equal(b) && b.Equal(c) {
			assert.True(t, a.Equal(c), "Equal is not transitive: %s == %s == %s", a, b, c)
		}
	})
}

func TestProp_CloneIsIndependentOfTheOriginal(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		original := testgen.AnyValue(t)

		// A second clone records what the original looked like before the first
		// one is vandalized.
		snapshot := original.Clone()
		clone := original.Clone()
		require.True(t, clone.Equal(original))

		// Mutating every mutable field of the clone must leave the original
		// untouched. The oracle hands out clones precisely so that a caller
		// cannot reach back into a shard and mutate live state.
		if clone.Members != nil {
			clone.Members["injected-by-the-test"] = struct{}{}
		}
		for i := range clone.Scalar {
			clone.Scalar[i] ^= 0xff
		}
		clone.Counter++

		assert.True(t, original.Equal(snapshot),
			"mutating a clone changed the original: %s became %s", snapshot, original)
		assert.NotContains(t, original.Members, "injected-by-the-test")
	})
}

func TestProp_AbsentAndEmptySetAreInterchangeable(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		v := testgen.AnyValue(t)
		absent := event.Value{}
		emptySet := event.Value{Kind: event.ValueSet, Members: map[string]struct{}{}}

		// Whatever v is, it cannot tell the two apart. This is the property that
		// makes "the last member was removed" and "the key never existed"
		// indistinguishable, which is what Redis actually does.
		assert.Equal(t, v.Equal(absent), v.Equal(emptySet),
			"%s distinguishes an absent value from an empty set", v)
	})
}

func TestProp_IsAbsentAgreesWithEqualAgainstAbsent(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		v := testgen.AnyValue(t)

		assert.Equal(t, v.IsAbsent(), v.Equal(event.Value{}),
			"IsAbsent and Equal(absent) must agree for %s", v)
	})
}

func TestProp_ValidGeneratedEventsRoundTripThroughFingerprint(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		e := testgen.Event(t, "pub-0", rapid.Uint64().Draw(t, "seq"))

		f := e.Fingerprint()

		assert.Equal(t, e.Publisher, f.Publisher)
		assert.Equal(t, e.Epoch, f.Epoch)
		assert.Equal(t, e.Seq, f.Seq)
	})
}
