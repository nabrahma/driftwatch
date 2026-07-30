package projection_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/projection"
)

func setEvent(key, value string) *event.Event {
	return &event.Event{Publisher: "p", Op: event.OpSet, Key: key, Value: []byte(value)}
}

func TestScalar_Apply(t *testing.T) {
	tests := []struct {
		name    string
		prev    event.Value
		event   *event.Event
		want    projection.Mutation
		wantErr error
	}{
		{
			name:  "setting an absent key creates it",
			prev:  event.Value{},
			event: setEvent("k", "v"),
			want:  projection.Mutation{Key: "k", Action: projection.ActionUpsert, Value: scalarOf("v")},
		},
		{
			name:  "setting an existing key overwrites it, last write wins",
			prev:  scalarOf("old"),
			event: setEvent("k", "new"),
			want:  projection.Mutation{Key: "k", Action: projection.ActionUpsert, Value: scalarOf("new")},
		},
		{
			name:  "setting the empty string is a real value, not an absence",
			prev:  scalarOf("old"),
			event: setEvent("k", ""),
			want:  projection.Mutation{Key: "k", Action: projection.ActionUpsert, Value: scalarOf("")},
		},
		{
			name:  "deleting an existing key yields a delete",
			prev:  scalarOf("v"),
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
			prev:  scalarOf("v"),
			event: &event.Event{Publisher: "p", Op: event.OpHeartbeat},
			want:  projection.Mutation{Action: projection.ActionNone},
		},
		{
			name:    "an add is not a scalar operation",
			prev:    event.Value{},
			event:   addEvent("k", "m"),
			wantErr: projection.ErrUnsupportedOp,
		},
		{
			name:    "a remove is not a scalar operation",
			prev:    event.Value{},
			event:   removeEvent("k", "m"),
			wantErr: projection.ErrUnsupportedOp,
		},
		{
			name:    "an increment is not a scalar operation",
			prev:    event.Value{},
			event:   &event.Event{Publisher: "p", Op: event.OpIncr, Key: "k", Delta: 1},
			wantErr: projection.ErrUnsupportedOp,
		},
		{
			name:    "a previous value of the wrong shape is reported rather than panicking",
			prev:    setOf("m"),
			event:   setEvent("k", "v"),
			wantErr: projection.ErrShapeMismatch,
		},
		{
			name:  "a binary value is preserved byte for byte",
			prev:  event.Value{},
			event: setEvent("k", "\x00\xff\xfe"),
			want:  projection.Mutation{Key: "k", Action: projection.ActionUpsert, Value: scalarOf("\x00\xff\xfe")},
		},
	}

	p := newProjection(t, "scalar", nil)
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

func TestScalar_CopiesTheValueBytesOutOfTheSourceBuffer(t *testing.T) {
	// Event.Value may point into a buffer the transport is free to reuse the
	// moment Decode returns, so a projection that kept the slice would end up
	// comparing whatever arrived next against the target.
	p := newProjection(t, "scalar", nil)
	payload := []byte("original")
	e := &event.Event{Publisher: "p", Op: event.OpSet, Key: "k", Value: payload}

	got, err := p.Apply(event.Value{}, e)
	require.NoError(t, err)

	copy(payload, "OVERWRIT")

	assert.Equal(t, []byte("original"), got.Value.Scalar)
}

func TestScalar_Metadata(t *testing.T) {
	p := newProjection(t, "scalar", nil)

	assert.Equal(t, "scalar", p.Name())
	assert.Equal(t, projection.ShapeScalar, p.TargetShape())
	assert.False(t, p.Commutative(), "with last-write-wins, order is the whole semantics")
	assert.False(t, p.KeyOwnership().Partitioned)
}

func TestScalar_RejectsABadKeyTemplateAtConstruction(t *testing.T) {
	_, err := projection.New("scalar", map[string]string{"keyTemplate": "{{.Nope}}"})

	assert.ErrorIs(t, err, projection.ErrBadConfig)
}

func TestScalar_KeyTemplateIsApplied(t *testing.T) {
	p := newProjection(t, "scalar", map[string]string{"keyTemplate": "app:{{.Key}}"})

	got, err := p.Apply(event.Value{}, setEvent("k", "v"))

	require.NoError(t, err)
	assert.Equal(t, "app:k", got.Key)
}
