package projection_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/testgen"
)

// fold applies a projection to a stream the way the oracle does, keeping only
// keys that end up present. Anything absent is dropped rather than stored,
// because "the key holds an empty value" and "the target does not have the key"
// are the same observable state.
func fold(_ require.TestingT, p projection.Projection, events []event.Event) map[string]event.Value {
	state := map[string]event.Value{}

	for i := range events {
		e := &events[i]
		prev := state[e.Key]

		m, err := p.Apply(prev, e)
		if err != nil {
			// An operation this projection has no meaning for is counted and
			// dropped by the pipeline, so the fold skips it too.
			continue
		}

		switch m.Action {
		case projection.ActionUpsert:
			if m.Value.IsAbsent() {
				delete(state, m.Key)
				continue
			}
			state[m.Key] = m.Value
		case projection.ActionDelete:
			delete(state, m.Key)
		case projection.ActionNone:
		}
	}
	return state
}

func assertStatesEqual(t *rapid.T, want, got map[string]event.Value, msg string) {
	t.Helper()

	require.Equal(t, len(want), len(got), "%s: key count differs\nwant %v\ngot  %v", msg, want, got)
	for k, wantValue := range want {
		gotValue, ok := got[k]
		require.True(t, ok, "%s: key %q missing", msg, k)
		require.True(t, wantValue.Equal(gotValue),
			"%s: key %q: want %s, got %s", msg, k, wantValue, gotValue)
	}
}

// keyEvents generates a stream restricted to one shape's operations, over a
// small key space so that keys actually collide and the interesting transitions
// happen.
func keyEvents(t *rapid.T, ops []event.Op, keys, members []string, count int) []event.Event {
	out := make([]event.Event, 0, count)
	for i := 0; i < count; i++ {
		e := event.Event{
			Publisher: "p",
			Epoch:     1,
			Seq:       uint64(i + 1), //nolint:gosec // loop counter, never negative
			Op:        rapid.SampledFrom(ops).Draw(t, "op"),
			Key:       rapid.SampledFrom(keys).Draw(t, "key"),
		}
		switch e.Op {
		case event.OpAdd, event.OpRemove:
			e.Member = rapid.SampledFrom(members).Draw(t, "member")
		case event.OpSet:
			e.Value = []byte(rapid.SampledFrom([]string{"", "a", "a,b", "b,c", "x"}).Draw(t, "value"))
		case event.OpIncr:
			e.Delta = rapid.Int64Range(-50, 50).Draw(t, "delta")
			if e.Delta == 0 {
				e.Delta = 1
			}
		default:
		}
		out = append(out, e)
	}
	return out
}

var (
	setOps    = []event.Op{event.OpAdd, event.OpRemove, event.OpDelete, event.OpSet}
	scalarOps = []event.Op{event.OpSet, event.OpDelete}
	incrOps   = []event.Op{event.OpIncr}
	testKeys  = []string{"", "k1", "k2", "k3"}
	testMems  = []string{"replica-0", "replica-1", "replica-2"}
)

// TestProp_ApplyTwiceEqualsOnce is invariant I1: applying an event twice yields
// the same state as applying it once.
//
// The oracle relies on this because a lossy broadcast redelivers as readily as
// it drops. Sequence dedup catches most repeats, but a projection that is not
// itself idempotent would still corrupt state on any repeat that slips through
// — a late fill, say, or a snapshot replay.
func TestProp_ApplyTwiceEqualsOnce(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		p := newRapidProjection(t, "keysetOwnership", nil)

		events := keyEvents(t, setOps, testKeys, testMems, rapid.IntRange(0, 25).Draw(t, "count"))

		once := fold(t, p, events)

		// Replay the same stream a second time. Every operation here is
		// idempotent on its own, so the state must not move.
		twice := fold(t, p, append(append([]event.Event{}, events...), events...))

		assertStatesEqual(t, once, twice, "replaying the stream changed the state")
	})
}

// TestProp_ApplyIsPureAndDoesNotTouchItsInput asserts the contract the whole
// property suite depends on: same inputs, same output, and prev unmodified.
func TestProp_ApplyIsPureAndDoesNotTouchItsInput(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		p := newRapidProjection(t, "keysetOwnership", nil)

		prev := testgen.Value(t, event.ValueSet)
		before := prev.Clone()
		e := testgen.KeyEvent(t, "p", 1)

		first, firstErr := p.Apply(prev, &e)
		for i := 0; i < 5; i++ {
			again, againErr := p.Apply(prev, &e)

			require.Equal(t, firstErr == nil, againErr == nil, "Apply is not deterministic")
			if firstErr == nil {
				require.Equal(t, first.Key, again.Key)
				require.Equal(t, first.Action, again.Action)
				require.True(t, first.Value.Equal(again.Value),
					"Apply returned different values for identical inputs")
			}
		}

		require.True(t, before.Equal(prev),
			"Apply modified the value it was given: %s became %s", before, prev)
	})
}

// TestProp_CommutativePermutationInvariant is invariant I2: for a commutative
// projection, any permutation of the same event set yields identical state.
func TestProp_CommutativePermutationInvariant(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		p := newRapidProjection(t, "counter", map[string]string{"incrOnly": "true"})
		require.True(t, p.Commutative())

		events := keyEvents(t, incrOps, testKeys, testMems, rapid.IntRange(0, 30).Draw(t, "count"))

		inOrder := fold(t, p, events)
		shuffled := fold(t, p, testgen.Permutation(t, events))

		// Addition commutes, so the totals must match whatever order the events
		// arrived in. This is what lets driftwatch assert on a counter index
		// without ordering the stream first.
		assertStatesEqual(t, inOrder, shuffled, "a commutative projection depended on order")
	})
}

// TestProp_NonCommutativeProjectionsAreHonestAboutIt is the other half of I2.
// A projection that declares itself order-dependent must actually be — if a
// permutation could never change the outcome, the declaration is costing the
// oracle work for nothing.
func TestProp_NonCommutativeProjectionsAreHonestAboutIt(t *testing.T) {
	p := newProjection(t, "keysetOwnership", nil)
	require.False(t, p.Commutative())

	addThenRemove := []event.Event{
		{Publisher: "p", Seq: 1, Op: event.OpAdd, Key: "k", Member: "m"},
		{Publisher: "p", Seq: 2, Op: event.OpRemove, Key: "k", Member: "m"},
	}
	removeThenAdd := []event.Event{addThenRemove[1], addThenRemove[0]}

	assert.Empty(t, fold(t, p, addThenRemove), "the key should have been emptied and deleted")
	assert.Len(t, fold(t, p, removeThenAdd), 1, "the add should have survived")
}

// TestProp_ConvergesToReference is invariant I3: applying events in sequence
// order reaches the canonical state, which is defined by the deliberately naive
// reference implementation.
//
// This is differential testing. The optimized projection clones maps, special
// cases identity templates, and short-circuits empty sets; the reference does
// none of that. Any disagreement is a bug in the optimized one.
func TestProp_ConvergesToReference(t *testing.T) {
	shapes := []struct {
		name      string
		projName  string
		cfg       map[string]string
		shape     projection.Shape
		ops       []event.Op
		incrOnly  bool
		delimiter string
	}{
		{name: "keysetOwnership", projName: "keysetOwnership", shape: projection.ShapeSet, ops: setOps, delimiter: ","},
		{name: "scalar", projName: "scalar", shape: projection.ShapeScalar, ops: scalarOps},
		{name: "counter", projName: "counter", shape: projection.ShapeCounter, ops: []event.Op{event.OpIncr, event.OpSet, event.OpDelete}},
	}

	for _, sh := range shapes {
		t.Run(sh.name, func(t *testing.T) {
			rapid.Check(t, func(t *rapid.T) {
				p := newRapidProjection(t, sh.projName, sh.cfg)

				ref := projection.NewReference(sh.shape)
				if sh.delimiter != "" {
					ref = ref.WithDelimiter(sh.delimiter)
				}

				events := keyEvents(t, sh.ops, testKeys, testMems,
					rapid.IntRange(0, 40).Draw(t, "count"))

				assertStatesEqual(t, ref.Fold(events), fold(t, p, events),
					"the optimized projection disagrees with the reference")
			})
		})
	}
}

// TestProp_OutOfOrderDeliveryConvergesOnceEverythingArrives is the second half
// of I3: a non-commutative projection reaches the canonical state once the
// stream is sorted by sequence and replayed, however it arrived.
//
// This is exactly what the oracle does. Events arrive in whatever order the
// broadcast delivers them; ordering by seq before folding is what makes the
// expectation well-defined at all.
func TestProp_OutOfOrderDeliveryConvergesOnceEverythingArrives(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		p := newRapidProjection(t, "keysetOwnership", nil)

		events := keyEvents(t, setOps, testKeys, testMems, rapid.IntRange(0, 30).Draw(t, "count"))
		canonical := fold(t, p, events)

		shuffled := testgen.Permutation(t, events)
		sortBySeq(shuffled)

		assertStatesEqual(t, canonical, fold(t, p, shuffled),
			"sorting a permuted stream by seq did not recover the canonical state")
	})
}

func sortBySeq(events []event.Event) {
	for i := 1; i < len(events); i++ {
		for j := i; j > 0 && events[j].Seq < events[j-1].Seq; j-- {
			events[j], events[j-1] = events[j-1], events[j]
		}
	}
}

// TestProp_EmptySetsNeverSurviveAsUpserts guards the empty-set trap at the property
// level rather than only in the named table test: whatever the stream, no key
// is ever left holding a value that is equivalent to absence.
func TestProp_EmptySetsNeverSurviveAsUpserts(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		p := newRapidProjection(t, "keysetOwnership", nil)

		events := keyEvents(t, setOps, testKeys, testMems, rapid.IntRange(0, 30).Draw(t, "count"))

		state := map[string]event.Value{}
		for i := range events {
			e := &events[i]
			m, err := p.Apply(state[e.Key], e)
			if err != nil {
				continue
			}
			if m.Action == projection.ActionUpsert {
				require.False(t, m.Value.IsAbsent(),
					"an upsert produced a value equivalent to absence, which the target can never hold")
				state[m.Key] = m.Value
				continue
			}
			if m.Action == projection.ActionDelete {
				delete(state, m.Key)
			}
		}
	})
}

func newRapidProjection(t *rapid.T, name string, cfg map[string]string) projection.Projection {
	t.Helper()
	p, err := projection.New(name, cfg)
	require.NoError(t, err)
	return p
}
