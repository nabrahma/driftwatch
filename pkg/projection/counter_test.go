package projection_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/projection"
)

func incrEvent(key string, delta int64) *event.Event {
	return &event.Event{Publisher: "p", Op: event.OpIncr, Key: key, Delta: delta}
}

func TestCounter_Apply(t *testing.T) {
	tests := []struct {
		name    string
		prev    event.Value
		event   *event.Event
		want    projection.Mutation
		wantErr error
	}{
		{
			name:  "incrementing an absent key creates it at the delta",
			prev:  event.Value{},
			event: incrEvent("k", 5),
			want:  projection.Mutation{Key: "k", Action: projection.ActionUpsert, Value: counterOf(5)},
		},
		{
			name:  "incrementing adds to the existing value",
			prev:  counterOf(10),
			event: incrEvent("k", 5),
			want:  projection.Mutation{Key: "k", Action: projection.ActionUpsert, Value: counterOf(15)},
		},
		{
			name:  "a negative delta decrements",
			prev:  counterOf(10),
			event: incrEvent("k", -3),
			want:  projection.Mutation{Key: "k", Action: projection.ActionUpsert, Value: counterOf(7)},
		},
		{
			name:  "a decrement below zero is a real value, not an absence",
			prev:  counterOf(1),
			event: incrEvent("k", -5),
			want:  projection.Mutation{Key: "k", Action: projection.ActionUpsert, Value: counterOf(-4)},
		},
		{
			name:  "a set writes an absolute value",
			prev:  counterOf(10),
			event: setEvent("k", "42"),
			want:  projection.Mutation{Key: "k", Action: projection.ActionUpsert, Value: counterOf(42)},
		},
		{
			name:  "a set to zero is a real value",
			prev:  counterOf(10),
			event: setEvent("k", "0"),
			want:  projection.Mutation{Key: "k", Action: projection.ActionUpsert, Value: counterOf(0)},
		},
		{
			name:  "a set to a negative value works",
			prev:  event.Value{},
			event: setEvent("k", "-7"),
			want:  projection.Mutation{Key: "k", Action: projection.ActionUpsert, Value: counterOf(-7)},
		},
		{
			name:  "deleting an existing key yields a delete",
			prev:  counterOf(10),
			event: &event.Event{Publisher: "p", Op: event.OpDelete, Key: "k"},
			want:  projection.Mutation{Key: "k", Action: projection.ActionDelete},
		},
		{
			name:  "deleting an absent key is a no-op",
			prev:  event.Value{},
			event: &event.Event{Publisher: "p", Op: event.OpDelete, Key: "k"},
			want:  projection.Mutation{Key: "k", Action: projection.ActionNone},
		},
		{
			name:  "a heartbeat touches nothing",
			prev:  counterOf(10),
			event: &event.Event{Publisher: "p", Op: event.OpHeartbeat},
			want:  projection.Mutation{Action: projection.ActionNone},
		},
		{
			name:    "an unparseable set value is reported",
			prev:    event.Value{},
			event:   setEvent("k", "not-a-number"),
			wantErr: projection.ErrBadValue,
		},
		{
			name:    "a set value beyond int64 is reported rather than wrapping",
			prev:    event.Value{},
			event:   setEvent("k", "99999999999999999999"),
			wantErr: projection.ErrBadValue,
		},
		{
			name:    "an add is not a counter operation",
			prev:    event.Value{},
			event:   addEvent("k", "m"),
			wantErr: projection.ErrUnsupportedOp,
		},
		{
			name:    "a remove is not a counter operation",
			prev:    event.Value{},
			event:   removeEvent("k", "m"),
			wantErr: projection.ErrUnsupportedOp,
		},
		{
			name:    "a previous value of the wrong shape is reported rather than panicking",
			prev:    scalarOf("10"),
			event:   incrEvent("k", 1),
			wantErr: projection.ErrShapeMismatch,
		},
	}

	p := newProjection(t, "counter", nil)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := p.Apply(tc.prev, tc.event)

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want.Key, got.Key)
			assert.Equal(t, tc.want.Action, got.Action)
			assert.True(t, tc.want.Value.Equal(got.Value),
				"value: want %s, got %s", tc.want.Value, got.Value)
		})
	}
}

func TestCounter_OverflowSaturatesRatherThanWrapping(t *testing.T) {
	// Wrapping would turn an overflowing counter into a large negative number
	// that reads as a plausible value and would be compared against the target
	// as if it were real. Clamping is also wrong, but it is visibly wrong.
	tests := []struct {
		name  string
		prev  event.Value
		delta int64
		want  int64
	}{
		{
			name:  "adding past the top of int64 clamps at the maximum",
			prev:  counterOf(math.MaxInt64 - 1),
			delta: 10,
			want:  math.MaxInt64,
		},
		{
			name:  "subtracting past the bottom of int64 clamps at the minimum",
			prev:  counterOf(math.MinInt64 + 1),
			delta: -10,
			want:  math.MinInt64,
		},
		{
			name:  "adding at the maximum stays at the maximum",
			prev:  counterOf(math.MaxInt64),
			delta: 1,
			want:  math.MaxInt64,
		},
	}

	p := newProjection(t, "counter", nil)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := p.Apply(tc.prev, incrEvent("k", tc.delta))

			require.NoError(t, err)
			assert.Equal(t, tc.want, got.Value.Counter)
			assert.True(t, got.Saturated, "the mutation must record that the value is a bound")
		})
	}
}

func TestCounter_OrdinaryArithmeticIsNotFlaggedAsSaturated(t *testing.T) {
	p := newProjection(t, "counter", nil)

	got, err := p.Apply(counterOf(10), incrEvent("k", 5))

	require.NoError(t, err)
	assert.False(t, got.Saturated)
}

// TestCounter_CommutativityIsPerInstanceNotPerType is the point of this
// projection. Addition commutes, so an increment-only stream reaches the same
// total in any order. One absolute write breaks that, and driftwatch cannot
// verify the producer's behavior on its own — so it is a declaration, and the
// declaration is enforced.
func TestCounter_CommutativityIsPerInstanceNotPerType(t *testing.T) {
	mixed := newProjection(t, "counter", nil)
	incrOnly := newProjection(t, "counter", map[string]string{"incrOnly": "true"})

	assert.False(t, mixed.Commutative(),
		"a counter that accepts absolute writes is not commutative")
	assert.True(t, incrOnly.Commutative(),
		"a counter that only ever increments is")

	// The declaration has to be enforced, or the property test that relies on it
	// would be testing a premise the code does not hold to.
	_, err := incrOnly.Apply(event.Value{}, setEvent("k", "42"))
	assert.ErrorIs(t, err, projection.ErrUnsupportedOp)
}

func TestCounter_Metadata(t *testing.T) {
	p := newProjection(t, "counter", nil)

	assert.Equal(t, "counter", p.Name())
	assert.Equal(t, projection.ShapeCounter, p.TargetShape())
	assert.False(t, p.KeyOwnership().Partitioned)
}

func TestCounter_ConfigurationErrors(t *testing.T) {
	t.Run("a non-boolean incrOnly is rejected", func(t *testing.T) {
		_, err := projection.New("counter", map[string]string{"incrOnly": "sometimes"})
		assert.ErrorIs(t, err, projection.ErrBadConfig)
	})

	t.Run("a bad key template is rejected", func(t *testing.T) {
		_, err := projection.New("counter", map[string]string{"keyTemplate": "{{.Nope}}"})
		assert.ErrorIs(t, err, projection.ErrBadConfig)
	})
}

func TestCounter_KeyTemplateIsApplied(t *testing.T) {
	p := newProjection(t, "counter", map[string]string{"keyTemplate": "c:{{.Key}}"})

	got, err := p.Apply(event.Value{}, incrEvent("k", 1))

	require.NoError(t, err)
	assert.Equal(t, "c:k", got.Key)
}
