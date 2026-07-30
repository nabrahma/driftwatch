package projection_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/projection"
)

func TestRegistry_NamesIncludesEveryBuiltInProjection(t *testing.T) {
	names := projection.Names()

	assert.Contains(t, names, "keysetOwnership")
	assert.Contains(t, names, "scalar")
	assert.Contains(t, names, "counter")

	for i := 1; i < len(names); i++ {
		assert.LessOrEqual(t, names[i-1], names[i], "Names must be sorted")
	}
}

func TestRegistry_NewReportsAnUnknownProjectionWithTheAvailableNames(t *testing.T) {
	_, err := projection.New("hyperloglog", nil)

	require.ErrorIs(t, err, projection.ErrUnknownProjection)
	assert.Contains(t, err.Error(), "keysetOwnership", "the error must say what is available")
}

func TestRegistry_RegisterRejectsDuplicateAndEmptyNames(t *testing.T) {
	ctor := func(map[string]string) (projection.Projection, error) {
		return projection.New("scalar", nil)
	}

	projection.Register("projection-test-stub", ctor)
	assert.Contains(t, projection.Names(), "projection-test-stub")

	// A silently shadowed projection would compute a different expectation than
	// the operator configured, and every finding after that would be
	// driftwatch's own fault.
	assert.Panics(t, func() { projection.Register("projection-test-stub", ctor) })
	assert.Panics(t, func() { projection.Register("", ctor) })
	assert.Panics(t, func() { projection.Register("projection-test-nil", nil) })
}

func TestAction_String(t *testing.T) {
	tests := []struct {
		name string
		a    projection.Action
		want string
	}{
		{name: "the zero action is none", a: projection.ActionNone, want: "none"},
		{name: "upsert", a: projection.ActionUpsert, want: "upsert"},
		{name: "delete", a: projection.ActionDelete, want: "delete"},
		{name: "an out-of-range action reports its number", a: projection.Action(9), want: "Action(9)"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.a.String())
		})
	}
}

func TestShape_String(t *testing.T) {
	tests := []struct {
		name string
		s    projection.Shape
		want string
	}{
		{name: "the zero shape is scalar", s: projection.ShapeScalar, want: "scalar"},
		{name: "set", s: projection.ShapeSet, want: "set"},
		{name: "counter", s: projection.ShapeCounter, want: "counter"},
		{name: "an out-of-range shape reports its number", s: projection.Shape(9), want: "Shape(9)"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.s.String())
		})
	}
}

func TestReference_FoldMatchesTheDocumentedSemantics(t *testing.T) {
	tests := []struct {
		name   string
		shape  projection.Shape
		events []event.Event
		want   map[string]event.Value
	}{
		{
			name:  "a set fold drops a key whose last member was removed",
			shape: projection.ShapeSet,
			events: []event.Event{
				{Op: event.OpAdd, Key: "k", Member: "m"},
				{Op: event.OpRemove, Key: "k", Member: "m"},
			},
			want: map[string]event.Value{},
		},
		{
			name:  "a set fold keeps remaining members",
			shape: projection.ShapeSet,
			events: []event.Event{
				{Op: event.OpAdd, Key: "k", Member: "a"},
				{Op: event.OpAdd, Key: "k", Member: "b"},
				{Op: event.OpRemove, Key: "k", Member: "a"},
			},
			want: map[string]event.Value{"k": setOf("b")},
		},
		{
			name:  "a set fold replaces the whole set on OpSet",
			shape: projection.ShapeSet,
			events: []event.Event{
				{Op: event.OpAdd, Key: "k", Member: "a"},
				{Op: event.OpSet, Key: "k", Value: []byte("x,y")},
			},
			want: map[string]event.Value{"k": setOf("x", "y")},
		},
		{
			name:  "a set fold deletes on OpDelete",
			shape: projection.ShapeSet,
			events: []event.Event{
				{Op: event.OpAdd, Key: "k", Member: "a"},
				{Op: event.OpDelete, Key: "k"},
			},
			want: map[string]event.Value{},
		},
		{
			name:  "a set fold ignores operations the shape has no meaning for",
			shape: projection.ShapeSet,
			events: []event.Event{
				{Op: event.OpAdd, Key: "k", Member: "a"},
				{Op: event.OpIncr, Key: "k", Delta: 1},
			},
			want: map[string]event.Value{"k": setOf("a")},
		},
		{
			name:  "a scalar fold is last write wins",
			shape: projection.ShapeScalar,
			events: []event.Event{
				{Op: event.OpSet, Key: "k", Value: []byte("a")},
				{Op: event.OpSet, Key: "k", Value: []byte("b")},
			},
			want: map[string]event.Value{"k": scalarOf("b")},
		},
		{
			name:  "a scalar fold deletes",
			shape: projection.ShapeScalar,
			events: []event.Event{
				{Op: event.OpSet, Key: "k", Value: []byte("a")},
				{Op: event.OpDelete, Key: "k"},
			},
			want: map[string]event.Value{},
		},
		{
			name:  "a scalar fold ignores operations the shape has no meaning for",
			shape: projection.ShapeScalar,
			events: []event.Event{
				{Op: event.OpSet, Key: "k", Value: []byte("a")},
				{Op: event.OpAdd, Key: "k", Member: "m"},
			},
			want: map[string]event.Value{"k": scalarOf("a")},
		},
		{
			name:  "a counter fold accumulates",
			shape: projection.ShapeCounter,
			events: []event.Event{
				{Op: event.OpIncr, Key: "k", Delta: 5},
				{Op: event.OpIncr, Key: "k", Delta: -2},
			},
			want: map[string]event.Value{"k": counterOf(3)},
		},
		{
			name:  "a counter fold accepts absolute writes",
			shape: projection.ShapeCounter,
			events: []event.Event{
				{Op: event.OpIncr, Key: "k", Delta: 5},
				{Op: event.OpSet, Key: "k", Value: []byte("100")},
			},
			want: map[string]event.Value{"k": counterOf(100)},
		},
		{
			name:  "a counter fold ignores an unparseable absolute write",
			shape: projection.ShapeCounter,
			events: []event.Event{
				{Op: event.OpIncr, Key: "k", Delta: 5},
				{Op: event.OpSet, Key: "k", Value: []byte("nope")},
			},
			want: map[string]event.Value{"k": counterOf(5)},
		},
		{
			name:  "a counter fold deletes",
			shape: projection.ShapeCounter,
			events: []event.Event{
				{Op: event.OpIncr, Key: "k", Delta: 5},
				{Op: event.OpDelete, Key: "k"},
			},
			want: map[string]event.Value{},
		},
		{
			name:  "a counter fold ignores operations the shape has no meaning for",
			shape: projection.ShapeCounter,
			events: []event.Event{
				{Op: event.OpIncr, Key: "k", Delta: 5},
				{Op: event.OpAdd, Key: "k", Member: "m"},
			},
			want: map[string]event.Value{"k": counterOf(5)},
		},
		{
			name:  "markers and heartbeats are skipped entirely",
			shape: projection.ShapeSet,
			events: []event.Event{
				{Op: event.OpSnapshotBegin},
				{Op: event.OpAdd, Key: "k", Member: "a"},
				{Op: event.OpHeartbeat},
				{Op: event.OpSnapshotEnd},
			},
			want: map[string]event.Value{"k": setOf("a")},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := projection.NewReference(tc.shape).Fold(tc.events)

			require.Len(t, got, len(tc.want))
			for k, want := range tc.want {
				gotValue, ok := got[k]
				require.True(t, ok, "key %q missing", k)
				assert.True(t, want.Equal(gotValue), "key %q: want %s, got %s", k, want, gotValue)
			}
		})
	}
}

func TestReference_IncrOnlyIgnoresAbsoluteWrites(t *testing.T) {
	// The reference has to mirror the optimized projection's refusal, or the
	// commutativity property would compare two different semantics.
	ref := projection.NewReference(projection.ShapeCounter).WithIncrOnly(true)

	got := ref.Fold([]event.Event{
		{Op: event.OpIncr, Key: "k", Delta: 5},
		{Op: event.OpSet, Key: "k", Value: []byte("100")},
	})

	require.Len(t, got, 1)
	assert.True(t, counterOf(5).Equal(got["k"]))
}

func TestReference_CustomDelimiter(t *testing.T) {
	ref := projection.NewReference(projection.ShapeSet).WithDelimiter("|")

	got := ref.Fold([]event.Event{{Op: event.OpSet, Key: "k", Value: []byte("a|b")}})

	require.Len(t, got, 1)
	assert.True(t, setOf("a", "b").Equal(got["k"]))
}
