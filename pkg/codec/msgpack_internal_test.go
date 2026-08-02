package codec

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The msgpack conversion helpers, called directly.
//
// These are internal rather than round-trip tests for a specific reason: the
// msgpack library encodes an integer into the narrowest form that holds it and
// decodes it back into int64 or uint64, so a round-trip can never produce a
// uint8 or an int16 — but a different encoder, or a future version of this one,
// can. The wide type switches exist for that, and a switch arm nothing
// exercises is a switch arm nobody has checked.
//
// They are pure functions with a stated contract, so testing them directly is
// not reaching into implementation detail; it is testing the units the contract
// is written about.

func TestMsgpackUint_AcceptsEveryIntegerWidth(t *testing.T) {
	tests := []struct {
		name string
		raw  any
		want uint64
	}{
		{name: "uint64", raw: uint64(42), want: 42},
		{name: "uint", raw: uint(42), want: 42},
		{name: "uint32", raw: uint32(42), want: 42},
		{name: "uint16", raw: uint16(42), want: 42},
		{name: "uint8", raw: uint8(42), want: 42},
		{name: "int64", raw: int64(42), want: 42},
		{name: "int", raw: 42, want: 42},
		{name: "int32", raw: int32(42), want: 42},
		{name: "int16", raw: int16(42), want: 42},
		{name: "int8", raw: int8(42), want: 42},
		{name: "a quoted number", raw: "42", want: 42},
		{name: "a quoted number with space", raw: "  42  ", want: 42},
		{name: "absent", raw: nil, want: 0},
		{
			// The value D-002 is about, in the width that carries it exactly.
			name: "2^53 + 1",
			raw:  uint64(9007199254740993),
			want: 9007199254740993,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := msgpackUint(tc.raw, "seq")
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestMsgpackUint_RefusesWhatCannotBeOne(t *testing.T) {
	tests := []struct {
		name string
		raw  any
		want string
	}{
		{name: "a negative int64", raw: int64(-1), want: "negative"},
		{name: "a negative int", raw: -1, want: "negative"},
		{name: "a negative int8", raw: int8(-1), want: "negative"},
		{name: "a float64", raw: float64(1), want: "D-002"},
		{name: "a float32", raw: float32(1), want: "D-002"},
		{name: "a non-numeric string", raw: "soon", want: "not an unsigned integer"},
		{name: "a boolean", raw: true, want: "must be an integer"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := msgpackUint(tc.raw, "seq")
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrMalformed)
			assert.Contains(t, err.Error(), tc.want)
			assert.Contains(t, err.Error(), "seq",
				"the message must name the field: %v", err)
		})
	}
}

func TestMsgpackInt_AcceptsEveryIntegerWidth(t *testing.T) {
	tests := []struct {
		name string
		raw  any
		want int64
	}{
		{name: "int64", raw: int64(-7), want: -7},
		{name: "int", raw: -7, want: -7},
		{name: "int32", raw: int32(-7), want: -7},
		{name: "int16", raw: int16(-7), want: -7},
		{name: "int8", raw: int8(-7), want: -7},
		{name: "uint64", raw: uint64(7), want: 7},
		{name: "uint", raw: uint(7), want: 7},
		{name: "uint32", raw: uint32(7), want: 7},
		{name: "uint16", raw: uint16(7), want: 7},
		{name: "uint8", raw: uint8(7), want: 7},
		{name: "a quoted number", raw: "-7", want: -7},
		{name: "absent", raw: nil, want: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := msgpackInt(tc.raw, "delta")
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestMsgpackInt_RefusesWhatCannotBeOne(t *testing.T) {
	tests := []struct {
		name string
		raw  any
	}{
		{name: "a uint64 past int64", raw: uint64(math.MaxUint64)},
		{name: "a float", raw: 1.5},
		{name: "a float32", raw: float32(1.5)},
		{name: "a non-numeric string", raw: "soon"},
		{name: "a boolean", raw: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := msgpackInt(tc.raw, "delta")
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrMalformed)
		})
	}
}

func TestMsgpackString_AcceptsStringsBytesAndAbsence(t *testing.T) {
	got, err := msgpackString("replica-0", "publisher")
	require.NoError(t, err)
	assert.Equal(t, "replica-0", got)

	// bin8 rather than str8, which is what a producer that treats identifiers
	// as opaque bytes emits.
	got, err = msgpackString([]byte("replica-0"), "publisher")
	require.NoError(t, err)
	assert.Equal(t, "replica-0", got)

	got, err = msgpackString(nil, "publisher")
	require.NoError(t, err)
	assert.Empty(t, got)

	_, err = msgpackString(42, "publisher")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrMalformed)
	assert.Contains(t, err.Error(), "publisher")
}

func TestMsgpackValue_RendersEveryScalarCanonically(t *testing.T) {
	tests := []struct {
		name string
		raw  any
		want string
	}{
		{name: "nil is absent", raw: nil, want: ""},
		{name: "a string", raw: "v1", want: "v1"},
		{name: "bytes", raw: []byte("v1"), want: "v1"},
		{name: "true", raw: true, want: "true"},
		{name: "false", raw: false, want: "false"},
		{name: "int64", raw: int64(-42), want: "-42"},
		{name: "uint64", raw: uint64(42), want: "42"},
		{name: "int", raw: 42, want: "42"},
		{name: "float64", raw: 1.5, want: "1.5"},
		{name: "float32", raw: float32(1.5), want: "1.5"},
		{
			// The reason a number is rendered rather than kept: a scalar
			// projection comparing against a store holding "42" has to see "42".
			name: "a whole float is not rendered in exponent form",
			raw:  float64(42),
			want: "42",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := msgpackValue(tc.raw)
			require.NoError(t, err)
			assert.Equal(t, tc.want, string(got))
		})
	}
}

func TestMsgpackValue_CopiesRatherThanAliasingBytes(t *testing.T) {
	// The Decode contract lets the caller reuse the payload buffer, and a value
	// that aliased it would change underneath the oracle.
	raw := []byte("v1")
	got, err := msgpackValue(raw)
	require.NoError(t, err)

	raw[0] = 'X'
	assert.Equal(t, "v1", string(got))
}

func TestMsgpackTime_AcceptsEveryFormAProducerSends(t *testing.T) {
	want := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		raw  any
		want time.Time
	}{
		{name: "the native extension type", raw: want, want: want},
		{name: "RFC 3339", raw: "2026-01-01T00:00:00Z", want: want},
		{name: "a quoted Unix integer", raw: "1767225600", want: time.Unix(1767225600, 0).UTC()},
		{name: "a Unix integer", raw: int64(1767225600), want: time.Unix(1767225600, 0).UTC()},
		{name: "absent", raw: nil, want: time.Time{}},
		{name: "an empty string", raw: "   ", want: time.Time{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := msgpackTime(tc.raw)
			require.NoError(t, err)
			assert.True(t, tc.want.Equal(got), "want %s, got %s", tc.want, got)
		})
	}
}

func TestMsgpackTime_RefusesWhatIsNeither(t *testing.T) {
	for _, raw := range []any{"tomorrow", 1.5, float32(1.5), true} {
		_, err := msgpackTime(raw)
		require.Error(t, err, "raw %v", raw)
		assert.ErrorIs(t, err, ErrMalformed)
	}
}

func TestMsgpackTTL_DistinguishesAbsentFromZero(t *testing.T) {
	got, err := msgpackTTL(nil)
	require.NoError(t, err)
	assert.Nil(t, got, "absent means the event says nothing about expiry")

	got, err = msgpackTTL("")
	require.NoError(t, err)
	assert.Nil(t, got, "an empty string is an absence, not a zero")

	got, err = msgpackTTL(int64(0))
	require.NoError(t, err)
	require.NotNil(t, got, "an explicit zero means expire immediately")
	assert.Zero(t, *got)

	got, err = msgpackTTL("90s")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, 90*time.Second, *got)

	_, err = msgpackTTL("soon")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrMalformed)
}
