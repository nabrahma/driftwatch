package codec_test

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/nabrahma/driftwatch/pkg/codec"
	"github.com/nabrahma/driftwatch/pkg/event"
)

func newMsgpack(t *testing.T, cfg map[string]string) codec.Codec {
	t.Helper()
	c, err := codec.New("msgpack", cfg)
	require.NoError(t, err)
	return c
}

// pack encodes a map the way a producer would.
func pack(t *testing.T, fields map[string]any) []byte {
	t.Helper()
	raw, err := msgpack.Marshal(fields)
	require.NoError(t, err)
	return raw
}

func decodeMsgpack(t *testing.T, c codec.Codec, payload []byte) (event.Event, error) {
	t.Helper()
	var e event.Event
	err := c.Decode(payload, "topic", &e)
	return e, err
}

func TestMsgpack_DecodesTheCanonicalShape(t *testing.T) {
	c := newMsgpack(t, nil)

	got, err := decodeMsgpack(t, c, pack(t, map[string]any{
		"publisher": "replica-0",
		"epoch":     uint64(1),
		"seq":       uint64(8847),
		"op":        "add",
		"key":       "block:9f3a",
		"member":    "replica-2",
		"ts":        "2026-01-01T00:00:00Z",
	}))
	require.NoError(t, err)

	assert.Equal(t, "replica-0", got.Publisher)
	assert.Equal(t, uint64(1), got.Epoch)
	assert.Equal(t, uint64(8847), got.Seq)
	assert.Equal(t, event.OpAdd, got.Op)
	assert.Equal(t, "block:9f3a", got.Key)
	assert.Equal(t, "replica-2", got.Member)
	assert.Equal(t, "topic", got.Topic)
	assert.Equal(t, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), got.PublishedAt)
}

func TestMsgpack_CarriesASequenceNumberAbove2To53Exactly(t *testing.T) {
	// D-002's whole point, in the format where it should be impossible.
	//
	// msgpack encodes a uint64 as a uint64, so the value survives the wire. The
	// assertion is that driftwatch does not undo that by routing it through a
	// float on the way in — which is exactly what encoding/json does by default
	// and what cost a day to find the first time.
	const seq = uint64(9007199254740993) // 2^53 + 1

	c := newMsgpack(t, nil)
	got, err := decodeMsgpack(t, c, pack(t, map[string]any{
		"publisher": "replica-0", "epoch": uint64(1), "seq": seq,
		"op": "add", "key": "k", "member": "m",
	}))
	require.NoError(t, err)

	assert.Equal(t, seq, got.Seq,
		"the sequence number changed in transit; if it came back as %d the "+
			"decoder went through float64", uint64(9007199254740992))
}

func TestMsgpack_RefusesASequenceNumberSentAsAFloat(t *testing.T) {
	// The producer already lost the precision before the bytes were written.
	// Accepting it would launder a corrupted number into a plausible one, and
	// the resulting gap report would send someone looking for a lost event that
	// was never lost.
	c := newMsgpack(t, nil)

	_, err := decodeMsgpack(t, c, pack(t, map[string]any{
		"publisher": "replica-0", "epoch": uint64(1),
		"seq": float64(9007199254740993),
		"op":  "add", "key": "k", "member": "m",
	}))
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrMalformed)
	assert.Contains(t, err.Error(), "D-002",
		"the message should point at the finding that explains why: %v", err)
}

func TestMsgpack_HonoursTheSameFieldRenamingAsJSON(t *testing.T) {
	// The compatibility claim: an operator who has configured the json codec
	// for their producer should be able to switch `type` and change nothing
	// else. A KV-cache producer emitting replica_id means that replica as both
	// the publisher and the set member, so the alias has to work here too.
	c := newMsgpack(t, map[string]string{
		"publisherField": "replica_id",
		"memberField":    "replica_id",
		"seqField":       "event_id",
		"opField":        "event_type",
		"keyField":       "block_hash",
		"opMapping":      "BLOCK_STORED=add,BLOCK_EVICTED=remove",
	})

	got, err := decodeMsgpack(t, c, pack(t, map[string]any{
		"replica_id": "vllm-2",
		"event_id":   uint64(41),
		"event_type": "BLOCK_STORED",
		"block_hash": "8e2b41ff",
	}))
	require.NoError(t, err)

	assert.Equal(t, "vllm-2", got.Publisher)
	assert.Equal(t, "vllm-2", got.Member,
		"one wire field may fill two event fields; forbidding it makes the "+
			"most common foreign format inexpressible")
	assert.Equal(t, uint64(41), got.Seq)
	assert.Equal(t, event.OpAdd, got.Op)
	assert.Equal(t, "8e2b41ff", got.Key)
}

func TestMsgpack_ReportsTypedSentinels(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		want    error
	}{
		{
			name:    "an empty payload",
			payload: []byte{},
			want:    codec.ErrMalformed,
		},
		{
			name:    "bytes that are not msgpack",
			payload: []byte("this is not msgpack at all"),
			want:    codec.ErrMalformed,
		},
		{
			name:    "a truncated map header",
			payload: []byte{0x82, 0xa3, 'o', 'p'},
			want:    codec.ErrMalformed,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newMsgpack(t, nil)
			_, err := decodeMsgpack(t, c, tc.payload)
			require.Error(t, err)
			assert.ErrorIs(t, err, tc.want)
		})
	}
}

func TestMsgpack_AnUnknownOpIsItsOwnSentinel(t *testing.T) {
	// Counted apart from malformed input because it usually means the producer
	// shipped a new event type, not that the bytes are corrupt — and those two
	// send an operator to different teams.
	c := newMsgpack(t, nil)

	_, err := decodeMsgpack(t, c, pack(t, map[string]any{
		"publisher": "replica-0", "epoch": uint64(1), "seq": uint64(1),
		"op": "teleport", "key": "k",
	}))
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrUnknownOp)
	assert.NotErrorIs(t, err, codec.ErrMalformed,
		"an unrecognized operation is a version mismatch, not corruption")
}

func TestMsgpack_AMissingOpIsAMissingField(t *testing.T) {
	c := newMsgpack(t, nil)

	_, err := decodeMsgpack(t, c, pack(t, map[string]any{
		"publisher": "replica-0", "seq": uint64(1), "key": "k",
	}))
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrMissingField)
}

func TestMsgpack_RejectsAnOversizedPayloadRatherThanTruncatingIt(t *testing.T) {
	// A truncated event is a wrong event, and a wrong event is worse than a
	// missing one: the missing one is counted.
	c := newMsgpack(t, map[string]string{"maxPayloadBytes": "64"})

	big := make([]byte, 256)
	for i := range big {
		big[i] = 'x'
	}
	_, err := decodeMsgpack(t, c, pack(t, map[string]any{
		"publisher": "replica-0", "epoch": uint64(1), "seq": uint64(1),
		"op": "set", "key": "k", "value": string(big),
	}))
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrTooLarge)
}

func TestMsgpack_RejectsAnOversizedKey(t *testing.T) {
	c := newMsgpack(t, map[string]string{"maxKeyBytes": "8"})

	_, err := decodeMsgpack(t, c, pack(t, map[string]any{
		"publisher": "replica-0", "epoch": uint64(1), "seq": uint64(1),
		"op": "set", "key": "a-key-far-longer-than-eight-bytes",
	}))
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrTooLarge)
}

func TestMsgpack_ValueKeepsItsWireShapeAsBytes(t *testing.T) {
	// The projection decides what a value means. A number is rendered in
	// canonical decimal rather than kept as a float, so a scalar projection
	// comparing against a store holding "42" sees "42" rather than "4.2e+01".
	tests := []struct {
		name string
		raw  any
		want string
	}{
		{name: "a string", raw: "replica-0", want: "replica-0"},
		{name: "raw bytes", raw: []byte{'a', 'b'}, want: "ab"},
		{name: "an integer", raw: int64(42), want: "42"},
		{name: "a boolean", raw: true, want: "true"},
		{name: "a float", raw: float64(1.5), want: "1.5"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newMsgpack(t, nil)
			got, err := decodeMsgpack(t, c, pack(t, map[string]any{
				"publisher": "replica-0", "epoch": uint64(1), "seq": uint64(1),
				"op": "set", "key": "k", "value": tc.raw,
			}))
			require.NoError(t, err)
			assert.Equal(t, tc.want, string(got.Value))
		})
	}
}

func TestMsgpack_ANestedValueIsRefusedWithAReason(t *testing.T) {
	// A map has no single representation the target could be compared against,
	// and inventing one — JSON-encoding it, say — would make the comparison
	// depend on key ordering. Refusing is the honest answer.
	c := newMsgpack(t, nil)

	_, err := decodeMsgpack(t, c, pack(t, map[string]any{
		"publisher": "replica-0", "epoch": uint64(1), "seq": uint64(1),
		"op": "set", "key": "k", "value": map[string]any{"nested": true},
	}))
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrMalformed)
	assert.Contains(t, err.Error(), "compared against")
}

func TestMsgpack_TTLDistinguishesAbsentFromZero(t *testing.T) {
	// Three states, and the difference matters: absent means the event says
	// nothing about expiry, zero means expire immediately. Collapsing them
	// makes every event look like it set a TTL of zero.
	base := map[string]any{
		"publisher": "replica-0", "epoch": uint64(1), "seq": uint64(1),
		"op": "set", "key": "k",
	}

	t.Run("absent", func(t *testing.T) {
		c := newMsgpack(t, nil)
		got, err := decodeMsgpack(t, c, pack(t, base))
		require.NoError(t, err)
		assert.Nil(t, got.TTL, "no ttl field means the event says nothing about expiry")
	})

	t.Run("zero", func(t *testing.T) {
		c := newMsgpack(t, nil)
		fields := map[string]any{"ttl": int64(0)}
		for k, v := range base {
			fields[k] = v
		}
		got, err := decodeMsgpack(t, c, pack(t, fields))
		require.NoError(t, err)
		require.NotNil(t, got.TTL, "an explicit zero is a TTL, not an absence")
		assert.Zero(t, *got.TTL)
	})

	t.Run("a number is seconds", func(t *testing.T) {
		c := newMsgpack(t, nil)
		fields := map[string]any{"ttl": int64(30)}
		for k, v := range base {
			fields[k] = v
		}
		got, err := decodeMsgpack(t, c, pack(t, fields))
		require.NoError(t, err)
		require.NotNil(t, got.TTL)
		assert.Equal(t, 30*time.Second, *got.TTL)
	})

	t.Run("a string is a Go duration", func(t *testing.T) {
		c := newMsgpack(t, nil)
		fields := map[string]any{"ttl": "2m30s"}
		for k, v := range base {
			fields[k] = v
		}
		got, err := decodeMsgpack(t, c, pack(t, fields))
		require.NoError(t, err)
		require.NotNil(t, got.TTL)
		assert.Equal(t, 150*time.Second, *got.TTL)
	})
}

func TestMsgpack_AcceptsMsgpacksNativeTimestampType(t *testing.T) {
	// The one form json has no equivalent of. The library decodes the ext type
	// straight to time.Time, so there is nothing to interpret and no ambiguity
	// about the unit — which is the reason to prefer it over a Unix integer.
	want := time.Date(2026, 7, 30, 11, 2, 31, 0, time.UTC)

	c := newMsgpack(t, nil)
	got, err := decodeMsgpack(t, c, pack(t, map[string]any{
		"publisher": "replica-0", "epoch": uint64(1), "seq": uint64(1),
		"op": "add", "key": "k", "member": "m", "ts": want,
	}))
	require.NoError(t, err)
	assert.True(t, want.Equal(got.PublishedAt),
		"want %s, got %s", want, got.PublishedAt)
}

func TestMsgpack_UnixTimestampUnitIsDetectedByMagnitude(t *testing.T) {
	tests := []struct {
		name string
		raw  int64
		want time.Time
	}{
		{
			name: "seconds",
			raw:  1767225600,
			want: time.Unix(1767225600, 0).UTC(),
		},
		{
			name: "milliseconds",
			raw:  1767225600000,
			want: time.UnixMilli(1767225600000).UTC(),
		},
		{
			name: "nanoseconds",
			raw:  1767225600000000000,
			want: time.Unix(0, 1767225600000000000).UTC(),
		},
		{
			name: "zero is unset rather than 1970",
			raw:  0,
			want: time.Time{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newMsgpack(t, nil)
			got, err := decodeMsgpack(t, c, pack(t, map[string]any{
				"publisher": "replica-0", "epoch": uint64(1), "seq": uint64(1),
				"op": "add", "key": "k", "member": "m", "ts": tc.raw,
			}))
			require.NoError(t, err)
			assert.True(t, tc.want.Equal(got.PublishedAt),
				"want %s, got %s", tc.want, got.PublishedAt)
		})
	}
}

func TestMsgpack_ADeltaOverflowingInt64IsRefused(t *testing.T) {
	c := newMsgpack(t, nil)

	_, err := decodeMsgpack(t, c, pack(t, map[string]any{
		"publisher": "replica-0", "epoch": uint64(1), "seq": uint64(1),
		"op": "incr", "key": "k", "delta": uint64(math.MaxUint64),
	}))
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrMalformed)
}

func TestMsgpack_RetainRawKeepsACopyRatherThanTheCallersBuffer(t *testing.T) {
	// The Decode contract says the payload may be reused by the caller. A codec
	// that retained the slice would produce events whose Raw changes underneath
	// them — and `explain`, which is the only consumer, would print whatever
	// arrived most recently rather than the event being explained.
	c := newMsgpack(t, map[string]string{"retainRaw": "true"})

	payload := pack(t, map[string]any{
		"publisher": "replica-0", "epoch": uint64(1), "seq": uint64(1),
		"op": "add", "key": "k", "member": "m",
	})
	got, err := decodeMsgpack(t, c, payload)
	require.NoError(t, err)
	require.NotEmpty(t, got.Raw)

	before := append([]byte(nil), got.Raw...)
	for i := range payload {
		payload[i] = 0
	}
	assert.Equal(t, before, got.Raw,
		"Raw aliased the caller's buffer and changed when it was reused")
}

func TestMsgpack_RejectsAnEmptyFieldNameOverride(t *testing.T) {
	_, err := codec.New("msgpack", map[string]string{"keyField": ""})
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrBadConfig)
}

func TestMsgpack_RejectsAnUnparseableOpMapping(t *testing.T) {
	_, err := codec.New("msgpack", map[string]string{"opMapping": "not-a-pair"})
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrBadConfig)
}

func TestMsgpack_IsRegisteredUnderItsName(t *testing.T) {
	assert.Contains(t, codec.Names(), "msgpack",
		"a codec the CRD accepts must be in the registry, or a spec that "+
			"validates fails at startup instead")
}
