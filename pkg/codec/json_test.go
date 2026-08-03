package codec_test

import (
	"bufio"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/nabrahma/driftwatch/pkg/codec"
	"github.com/nabrahma/driftwatch/pkg/event"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func newJSON(t *testing.T, cfg map[string]string) codec.Codec {
	t.Helper()
	c, err := codec.New("json", cfg)
	require.NoError(t, err)
	return c
}

func decode(t *testing.T, c codec.Codec, payload string) (event.Event, error) {
	t.Helper()
	var e event.Event
	err := c.Decode([]byte(payload), "topic", &e)
	return e, err
}

func ptr[T any](v T) *T { return &v }

func TestJSON_DecodesTheCanonicalGoldenPayload(t *testing.T) {
	payload, err := os.ReadFile("testdata/canonical_add.json")
	require.NoError(t, err)

	var got event.Event
	require.NoError(t, newJSON(t, nil).Decode(payload, "kv-events", &got))

	assert.Equal(t, "replica-2", got.Publisher)
	assert.Equal(t, uint64(1), got.Epoch)
	assert.Equal(t, uint64(8847), got.Seq)
	assert.Equal(t, event.OpAdd, got.Op)
	assert.Equal(t, "9f3a2c1e", got.Key)
	assert.Equal(t, "replica-2", got.Member)
	assert.Equal(t, "kv-events", got.Topic)
	assert.Equal(t,
		time.Date(2026, 7, 30, 11, 2, 31, 412000000, time.UTC),
		got.PublishedAt)
}

func TestJSON_DecodesAForeignFormatThroughFieldAndOpMapping(t *testing.T) {
	payload, err := os.ReadFile("testdata/foreign_block_stored.json")
	require.NoError(t, err)

	// This is the realistic case: a producer nobody is going to change, whose
	// field and operation vocabulary is entirely its own.
	c := newJSON(t, map[string]string{
		"publisherField": "replica_id",
		"epochField":     "incarnation",
		"seqField":       "event_id",
		"opField":        "event_type",
		"keyField":       "block_hash",
		// The producer names the replica once and means it as both the
		// publisher and the set member, so both event fields read replica_id.
		"memberField": "replica_id",
		"opMapping":   "BLOCK_STORED=add,BLOCK_EVICTED=remove",
	})

	var got event.Event
	require.NoError(t, c.Decode(payload, "", &got))

	assert.Equal(t, "vllm-2", got.Publisher)
	assert.Equal(t, uint64(8847), got.Seq)
	assert.Equal(t, event.OpAdd, got.Op)
	assert.Equal(t, "9f3a2c1e", got.Key)
	assert.Equal(t, "vllm-2", got.Member)
	assert.Equal(t, time.UnixMilli(1785412951412).UTC(), got.PublishedAt)
}

func TestJSON_DecodesTheSnapshotCycleGoldenPayloads(t *testing.T) {
	f, err := os.Open("testdata/snapshot_cycle.jsonl")
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()

	c := newJSON(t, nil)

	var ops []event.Op
	var seqs []uint64
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var got event.Event
		require.NoError(t, c.Decode([]byte(line), "", &got), "line %q", line)
		ops = append(ops, got.Op)
		seqs = append(seqs, got.Seq)
	}
	require.NoError(t, scanner.Err())

	// The markers arrive as camelCase here, which is one of the four spellings
	// that occur in the wild.
	assert.Equal(t, []event.Op{
		event.OpSnapshotBegin, event.OpAdd, event.OpAdd, event.OpSnapshotEnd,
	}, ops)
	assert.Equal(t, []uint64{9000, 9001, 9002, 9003}, seqs)
}

func TestJSON_Decode(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    event.Event
		wantErr error
	}{
		{
			name:    "a minimal delete decodes",
			payload: `{"publisher":"p","epoch":1,"seq":2,"op":"delete","key":"k"}`,
			want:    event.Event{Publisher: "p", Epoch: 1, Seq: 2, Op: event.OpDelete, Key: "k"},
		},
		{
			name:    "an empty key is preserved, because Redis accepts it",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":""}`,
			want:    event.Event{Publisher: "p", Seq: 1, Op: event.OpDelete},
		},
		{
			name:    "an absent epoch defaults to zero",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"k"}`,
			want:    event.Event{Publisher: "p", Seq: 1, Op: event.OpDelete, Key: "k"},
		},
		{
			name:    "a set carries its value bytes",
			payload: `{"publisher":"p","seq":1,"op":"set","key":"k","value":"hello"}`,
			want:    event.Event{Publisher: "p", Seq: 1, Op: event.OpSet, Key: "k", Value: []byte("hello")},
		},
		{
			name:    "a set with an empty value is valid, since the empty string is a real Redis value",
			payload: `{"publisher":"p","seq":1,"op":"set","key":"k","value":""}`,
			want:    event.Event{Publisher: "p", Seq: 1, Op: event.OpSet, Key: "k", Value: []byte{}},
		},
		{
			name:    "a negative delta decodes",
			payload: `{"publisher":"p","seq":1,"op":"incr","key":"k","delta":-5}`,
			want:    event.Event{Publisher: "p", Seq: 1, Op: event.OpIncr, Key: "k", Delta: -5},
		},
		{
			name:    "unknown fields are skipped",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"k","extra":{"a":[1,2,{"b":null}]}}`,
			want:    event.Event{Publisher: "p", Seq: 1, Op: event.OpDelete, Key: "k"},
		},
		{
			name:    "a duplicate JSON key resolves to the last occurrence, matching encoding/json",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"first","key":"last"}`,
			want:    event.Event{Publisher: "p", Seq: 1, Op: event.OpDelete, Key: "last"},
		},
		{
			name:    "a null field is treated exactly like an absent one",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"k","ttl":null,"member":null}`,
			want:    event.Event{Publisher: "p", Seq: 1, Op: event.OpDelete, Key: "k"},
		},
		{
			name:    "unicode escapes are decoded",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"café"}`,
			want:    event.Event{Publisher: "p", Seq: 1, Op: event.OpDelete, Key: "café"},
		},
		{
			name:    "a surrogate pair is joined into one rune",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"😀"}`,
			want:    event.Event{Publisher: "p", Seq: 1, Op: event.OpDelete, Key: "😀"},
		},
		{
			name:    "a lone surrogate becomes the replacement rune rather than invalid UTF-8",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"\ud83d"}`,
			want:    event.Event{Publisher: "p", Seq: 1, Op: event.OpDelete, Key: "�"},
		},
		{
			name:    "every simple escape is decoded",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"\"\\\/\b\f\n\r\t"}`,
			want:    event.Event{Publisher: "p", Seq: 1, Op: event.OpDelete, Key: "\"\\/\b\f\n\r\t"},
		},
		{
			name:    "an escaped field name still matches its mapping",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"k"}`,
			want:    event.Event{Publisher: "p", Seq: 1, Op: event.OpDelete, Key: "k"},
		},
		{
			name:    "a field name with an invalid escape simply does not match, rather than failing the decode",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"k","\q":1}`,
			want:    event.Event{Publisher: "p", Seq: 1, Op: event.OpDelete, Key: "k"},
		},
		{
			name:    "whitespace between tokens is ignored",
			payload: "{ \"publisher\" : \"p\" ,\n\t\"seq\" : 1 , \"op\" : \"delete\" , \"key\" : \"k\" }",
			want:    event.Event{Publisher: "p", Seq: 1, Op: event.OpDelete, Key: "k"},
		},
		{
			name:    "a seq above 2^53 survives, because it is parsed from its digits and never through float64",
			payload: `{"publisher":"p","seq":9007199254740993,"op":"delete","key":"k"}`,
			want:    event.Event{Publisher: "p", Seq: 9007199254740993, Op: event.OpDelete, Key: "k"},
		},
		{
			name:    "a seq sent as a string is accepted, which is what a careful producer does",
			payload: `{"publisher":"p","seq":"9007199254740993","op":"delete","key":"k"}`,
			want:    event.Event{Publisher: "p", Seq: 9007199254740993, Op: event.OpDelete, Key: "k"},
		},
		{
			name:    "the maximum uint64 seq decodes",
			payload: `{"publisher":"p","seq":18446744073709551615,"op":"delete","key":"k"}`,
			want:    event.Event{Publisher: "p", Seq: 18446744073709551615, Op: event.OpDelete, Key: "k"},
		},
		{
			name:    "a seq written as a float is rejected rather than silently rounded",
			payload: `{"publisher":"p","seq":1.0,"op":"delete","key":"k"}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "a seq in exponent form is rejected for the same reason",
			payload: `{"publisher":"p","seq":1e300,"op":"delete","key":"k"}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "a negative seq is rejected",
			payload: `{"publisher":"p","seq":-1,"op":"delete","key":"k"}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "a seq of the wrong JSON type is rejected",
			payload: `{"publisher":"p","seq":true,"op":"delete","key":"k"}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "a non-numeric seq string is rejected",
			payload: `{"publisher":"p","seq":"abc","op":"delete","key":"k"}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "a delta written as a float is rejected",
			payload: `{"publisher":"p","seq":1,"op":"incr","key":"k","delta":1.5}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "a delta sent as a string is accepted",
			payload: `{"publisher":"p","seq":1,"op":"incr","key":"k","delta":"-7"}`,
			want:    event.Event{Publisher: "p", Seq: 1, Op: event.OpIncr, Key: "k", Delta: -7},
		},
		{
			name:    "a delta of the wrong JSON type is rejected",
			payload: `{"publisher":"p","seq":1,"op":"incr","key":"k","delta":[1]}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "a delta string that is not an integer is rejected",
			payload: `{"publisher":"p","seq":1,"op":"incr","key":"k","delta":"x"}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "a publisher of the wrong JSON type is rejected",
			payload: `{"publisher":7,"seq":1,"op":"delete","key":"k"}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "a key of the wrong JSON type is rejected",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":7}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "an op of the wrong JSON type is rejected",
			payload: `{"publisher":"p","seq":1,"op":7,"key":"k"}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "a missing op field is a missing field, not an unknown op",
			payload: `{"publisher":"p","seq":1,"key":"k"}`,
			wantErr: codec.ErrMissingField,
		},
		{
			name:    "an unrecognized op is reported as an unknown op, since it usually means a version mismatch",
			payload: `{"publisher":"p","seq":1,"op":"teleport","key":"k"}`,
			wantErr: codec.ErrUnknownOp,
		},
		{
			name:    "an op in the wrong case still resolves",
			payload: `{"publisher":"p","seq":1,"op":"ADD","key":"k","member":"m"}`,
			want:    event.Event{Publisher: "p", Seq: 1, Op: event.OpAdd, Key: "k", Member: "m"},
		},
		{
			name:    "a missing publisher is rejected, because sequence tracking is per publisher",
			payload: `{"seq":1,"op":"delete","key":"k"}`,
			wantErr: codec.ErrMissingField,
		},
		{
			name:    "an add without a member is rejected by validation",
			payload: `{"publisher":"p","seq":1,"op":"add","key":"k"}`,
			wantErr: codec.ErrMissingField,
		},
		{
			name:    "a snapshot marker carrying a key is rejected by validation",
			payload: `{"publisher":"p","seq":1,"op":"snapshot_begin","key":"k"}`,
			wantErr: event.ErrUnexpectedField,
		},
		{
			name:    "an empty payload is malformed",
			payload: ``,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "valid JSON that is not an object is malformed",
			payload: `[1,2,3]`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "a bare null is malformed",
			payload: `null`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "an empty object is missing its op",
			payload: `{}`,
			wantErr: codec.ErrMissingField,
		},
		{
			name:    "an unterminated object is malformed",
			payload: `{"publisher":"p"`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "an unterminated string is malformed",
			payload: `{"publisher":"p}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "a missing colon is malformed",
			payload: `{"publisher" "p"}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "a non-string object key is malformed",
			payload: `{publisher:"p"}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "trailing bytes after the object are malformed, since two concatenated events are not one event",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"k"} {"publisher":"q"}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "a raw control byte inside a string is malformed",
			payload: "{\"publisher\":\"p\x01\"}",
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "an unknown escape is malformed",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"\q"}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "a truncated unicode escape is malformed",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"\u00"}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "a non-hex unicode escape is malformed",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"\uZZZZ"}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "a string ending inside an escape is malformed",
			payload: "{\"publisher\":\"p\",\"seq\":1,\"op\":\"delete\",\"key\":\"a\\\\\\\"}",
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "NaN is not JSON",
			payload: `{"publisher":"p","seq":NaN,"op":"delete","key":"k"}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "Infinity is not JSON",
			payload: `{"publisher":"p","seq":Infinity,"op":"delete","key":"k"}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "a number with an empty fraction is malformed",
			payload: `{"publisher":"p","seq":1.,"op":"delete","key":"k"}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "a number with an empty exponent is malformed",
			payload: `{"publisher":"p","seq":1e,"op":"delete","key":"k"}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "a bare minus sign is malformed",
			payload: `{"publisher":"p","seq":-,"op":"delete","key":"k"}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "a truncated true literal is malformed",
			payload: `{"publisher":"p","extra":tru}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "an unterminated array is malformed",
			payload: `{"publisher":"p","extra":[1,2}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "an unterminated nested object is malformed",
			payload: `{"publisher":"p","extra":{"a":1]}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "a nested object missing its colon is malformed",
			payload: `{"publisher":"p","extra":{"a" 1}}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "an empty nested container is skipped cleanly",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"k","a":{},"b":[]}`,
			want:    event.Event{Publisher: "p", Seq: 1, Op: event.OpDelete, Key: "k"},
		},
		{
			name:    "a value that ends the payload abruptly is malformed",
			payload: `{"publisher":`,
			wantErr: codec.ErrMalformed,
		},
	}

	c := newJSON(t, nil)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decode(t, c, tc.payload)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)

			want := tc.want
			want.Topic = "topic"
			assert.Equal(t, want, got)
		})
	}
}

func TestJSON_TimestampAutoDetection(t *testing.T) {
	// The detection rule is documented on unixSecondsCeiling: below 1e11 is
	// seconds, below 1e14 is milliseconds, larger is nanoseconds.
	tests := []struct {
		name string
		ts   string
		want time.Time
	}{
		{
			name: "RFC3339 parses",
			ts:   `"2026-07-30T11:02:31Z"`,
			want: time.Date(2026, 7, 30, 11, 2, 31, 0, time.UTC),
		},
		{
			name: "RFC3339Nano parses with its fractional seconds",
			ts:   `"2026-07-30T11:02:31.123456789Z"`,
			want: time.Date(2026, 7, 30, 11, 2, 31, 123456789, time.UTC),
		},
		{
			name: "an RFC3339 offset is normalized to UTC",
			ts:   `"2026-07-30T13:02:31+02:00"`,
			want: time.Date(2026, 7, 30, 11, 2, 31, 0, time.UTC),
		},
		{
			name: "Unix seconds are detected below the 1e11 boundary",
			ts:   `1785412951`,
			want: time.Unix(1785412951, 0).UTC(),
		},
		{
			name: "Unix milliseconds are detected below the 1e14 boundary",
			ts:   `1785412951412`,
			want: time.UnixMilli(1785412951412).UTC(),
		},
		{
			name: "Unix nanoseconds are detected above the 1e14 boundary",
			ts:   `1785412951412000000`,
			want: time.Unix(0, 1785412951412000000).UTC(),
		},
		{
			name: "the ambiguous 1e10 boundary resolves to seconds",
			ts:   `10000000000`,
			want: time.Unix(10000000000, 0).UTC(),
		},
		{
			name: "one below the seconds ceiling is still seconds",
			ts:   `99999999999`,
			want: time.Unix(99999999999, 0).UTC(),
		},
		{
			name: "the seconds ceiling itself is milliseconds",
			ts:   `100000000000`,
			want: time.UnixMilli(100000000000).UTC(),
		},
		{
			name: "the milliseconds ceiling itself is nanoseconds",
			ts:   `100000000000000`,
			want: time.Unix(0, 100000000000000).UTC(),
		},
		{
			name: "a negative Unix value predates the epoch and keeps its unit",
			ts:   `-1000`,
			want: time.Unix(-1000, 0).UTC(),
		},
		{
			name: "zero means unset rather than the Unix epoch",
			ts:   `0`,
			want: time.Time{},
		},
		{
			name: "an empty timestamp string is unset",
			ts:   `""`,
			want: time.Time{},
		},
		{
			name: "a numeric string is still a Unix value",
			ts:   `"1785412951"`,
			want: time.Unix(1785412951, 0).UTC(),
		},
	}

	c := newJSON(t, nil)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decode(t, c,
				`{"publisher":"p","seq":1,"op":"delete","key":"k","ts":`+tc.ts+`}`)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got.PublishedAt)
		})
	}
}

func TestJSON_RejectsUnparseableTimestamps(t *testing.T) {
	tests := []struct {
		name string
		ts   string
	}{
		{name: "a float timestamp is rejected", ts: `1785412951.5`},
		{name: "an unparseable string is rejected", ts: `"yesterday"`},
		{name: "a boolean timestamp is rejected", ts: `true`},
		{name: "a timestamp too large for int64 is rejected", ts: `99999999999999999999999`},
	}

	c := newJSON(t, nil)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decode(t, c,
				`{"publisher":"p","seq":1,"op":"delete","key":"k","ts":`+tc.ts+`}`)
			assert.ErrorIs(t, err, codec.ErrMalformed)
		})
	}
}

func TestJSON_TTL(t *testing.T) {
	tests := []struct {
		name    string
		ttl     string
		want    *time.Duration
		wantErr bool
	}{
		{name: "an absent TTL means the event says nothing about expiry", ttl: "", want: nil},
		{name: "a null TTL is the same as absent", ttl: `null`, want: nil},
		{
			name: "zero means expire immediately and is distinct from absent",
			ttl:  `0`,
			want: ptr(time.Duration(0)),
		},
		{name: "a number is read as seconds", ttl: `30`, want: ptr(30 * time.Second)},
		{name: "a fractional number is read as fractional seconds", ttl: `1.5`, want: ptr(1500 * time.Millisecond)},
		{name: "a duration string is parsed", ttl: `"90s"`, want: ptr(90 * time.Second)},
		{name: "a compound duration string is parsed", ttl: `"1h30m"`, want: ptr(90 * time.Minute)},
		{name: "a negative TTL is rejected by validation", ttl: `-1`, wantErr: true},
		{name: "an unparseable duration string is rejected", ttl: `"soon"`, wantErr: true},
		{name: "a boolean TTL is rejected", ttl: `true`, wantErr: true},
	}

	c := newJSON(t, nil)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			payload := `{"publisher":"p","seq":1,"op":"delete","key":"k"`
			if tc.ttl != "" {
				payload += `,"ttl":` + tc.ttl
			}
			payload += `}`

			got, err := decode(t, c, payload)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got.TTL)
		})
	}
}

func TestJSON_RejectsOversizedInputRatherThanTruncatingIt(t *testing.T) {
	// A truncated event is a wrong event, and a wrong event produces a
	// divergence finding that is entirely driftwatch's own fault.
	c := newJSON(t, map[string]string{"maxPayloadBytes": "128", "maxKeyBytes": "8"})

	t.Run("an oversized payload is rejected", func(t *testing.T) {
		big := `{"publisher":"p","seq":1,"op":"delete","key":"` + strings.Repeat("k", 200) + `"}`
		_, err := decode(t, c, big)
		assert.ErrorIs(t, err, codec.ErrTooLarge)
	})

	t.Run("an oversized key is rejected", func(t *testing.T) {
		_, err := decode(t, c, `{"publisher":"p","seq":1,"op":"delete","key":"kkkkkkkkkkkk"}`)
		assert.ErrorIs(t, err, codec.ErrTooLarge)
	})

	t.Run("an oversized member is rejected", func(t *testing.T) {
		_, err := decode(t, c, `{"publisher":"p","seq":1,"op":"add","key":"k","member":"mmmmmmmmmmmm"}`)
		assert.ErrorIs(t, err, codec.ErrTooLarge)
	})

	t.Run("an escaped key that expands past the cap is rejected after unescaping", func(t *testing.T) {
		// Six escapes are 36 raw bytes but only six decoded ones, so the check
		// has to happen on both sides of the unescape.
		_, err := decode(t, c,
			`{"publisher":"p","seq":1,"op":"delete","key":"aaaaaa"}`)
		assert.NoError(t, err)

		_, err = decode(t, c, `{"publisher":"p","seq":1,"op":"delete","key":"`+
			strings.Repeat(`a`, 10)+`"}`)
		assert.ErrorIs(t, err, codec.ErrTooLarge)
	})

	t.Run("nesting deeper than the limit is rejected, because an arbitrarily deep document is a DoS vector", func(t *testing.T) {
		shallow := newJSON(t, map[string]string{"maxDepth": "4"})
		nested := `{"publisher":"p","seq":1,"op":"delete","key":"k","x":` +
			strings.Repeat("[", 10) + strings.Repeat("]", 10) + `}`
		_, err := decode(t, shallow, nested)
		assert.ErrorIs(t, err, codec.ErrTooLarge)
	})
}

func TestJSON_RetainRawCopiesThePayloadOnlyWhenAskedTo(t *testing.T) {
	payload := []byte(`{"publisher":"p","seq":1,"op":"delete","key":"k"}`)

	var without event.Event
	require.NoError(t, newJSON(t, nil).Decode(payload, "", &without))
	assert.Nil(t, without.Raw, "retaining raw bytes by default would cost ~2 KB per history entry")

	var with event.Event
	require.NoError(t, newJSON(t, map[string]string{"retainRaw": "true"}).Decode(payload, "", &with))
	assert.Equal(t, payload, with.Raw)

	// The copy must not alias the caller's buffer, which sources reuse.
	payload[2] = 'X'
	assert.NotEqual(t, payload, with.Raw)
}

func TestJSON_DecodeDoesNotRetainTheCallersBuffer(t *testing.T) {
	payload := []byte(`{"publisher":"p","seq":1,"op":"set","key":"k","value":"v"}`)

	var got event.Event
	require.NoError(t, newJSON(t, nil).Decode(payload, "", &got))

	for i := range payload {
		payload[i] = 'Z'
	}

	assert.Equal(t, "p", got.Publisher)
	assert.Equal(t, "k", got.Key)
	assert.Equal(t, []byte("v"), got.Value)
}

func TestJSON_DecodePreservesObservedAtAndClearsEverythingElse(t *testing.T) {
	observed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Reusing a dirty event is the normal case on the ingest hot path, and a
	// field left over from the previous decode would be attributed to this one.
	dst := event.Event{
		ObservedAt: observed,
		Publisher:  "stale", Key: "stale", Member: "stale",
		Value: []byte("stale"), Delta: 99, Epoch: 9, Seq: 9,
		TTL: ptr(time.Hour), Raw: []byte("stale"),
	}

	require.NoError(t, newJSON(t, nil).Decode(
		[]byte(`{"publisher":"p","seq":1,"op":"delete","key":"k"}`), "t", &dst))

	assert.Equal(t, observed, dst.ObservedAt, "ObservedAt belongs to the source, not the codec")
	assert.Equal(t, event.Event{
		ObservedAt: observed, Topic: "t",
		Publisher: "p", Seq: 1, Op: event.OpDelete, Key: "k",
	}, dst)
}

func TestJSON_ConfigurationErrors(t *testing.T) {
	tests := []struct {
		name string
		cfg  map[string]string
	}{
		{name: "an empty field name is rejected", cfg: map[string]string{"keyField": ""}},
		{name: "an opMapping entry without an equals sign is rejected", cfg: map[string]string{"opMapping": "BLOCK_STORED"}},
		{name: "an opMapping entry with an empty name is rejected", cfg: map[string]string{"opMapping": "=add"}},
		{name: "an opMapping entry with an empty target is rejected", cfg: map[string]string{"opMapping": "X="}},
		{name: "an opMapping target that is not an op is rejected", cfg: map[string]string{"opMapping": "X=teleport"}},
		{name: "a non-numeric maxPayloadBytes is rejected", cfg: map[string]string{"maxPayloadBytes": "big"}},
		{name: "a zero maxPayloadBytes is rejected", cfg: map[string]string{"maxPayloadBytes": "0"}},
		{name: "a negative maxKeyBytes is rejected", cfg: map[string]string{"maxKeyBytes": "-1"}},
		{name: "a non-numeric maxDepth is rejected", cfg: map[string]string{"maxDepth": "deep"}},
		{name: "a non-boolean retainRaw is rejected", cfg: map[string]string{"retainRaw": "yes please"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := codec.New("json", tc.cfg)
			assert.ErrorIs(t, err, codec.ErrBadConfig)
		})
	}
}

func TestJSON_OneJSONFieldCanFeedSeveralEventFields(t *testing.T) {
	// Forbidding the alias would make the most common foreign format
	// inexpressible: a producer that names its replica once means it as both
	// the publisher and the set member.
	c := newJSON(t, map[string]string{"memberField": "publisher"})

	got, err := decode(t, c, `{"publisher":"replica-7","seq":1,"op":"add","key":"k"}`)

	require.NoError(t, err)
	assert.Equal(t, "replica-7", got.Publisher)
	assert.Equal(t, "replica-7", got.Member)
}

func TestJSON_OpMappingTakesPrecedenceOverTheBuiltInNames(t *testing.T) {
	// A producer whose "set" means something else entirely must be able to say
	// so without the built-in vocabulary quietly winning.
	c := newJSON(t, map[string]string{"opMapping": "set=delete"})

	got, err := decode(t, c, `{"publisher":"p","seq":1,"op":"set","key":"k"}`)

	require.NoError(t, err)
	assert.Equal(t, event.OpDelete, got.Op)
}

func TestJSON_NameIsTheRegistryName(t *testing.T) {
	assert.Equal(t, "json", newJSON(t, nil).Name())
}

func TestJSON_IsSafeForConcurrentUse(t *testing.T) {
	c := newJSON(t, nil)

	const goroutines = 16
	done := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			var e event.Event
			for j := 0; j < 200; j++ {
				payload := `{"publisher":"p","seq":1,"op":"add","key":"k","member":"m"}`
				if err := c.Decode([]byte(payload), "t", &e); err != nil {
					done <- err
					return
				}
			}
			done <- nil
		}()
	}
	for i := 0; i < goroutines; i++ {
		require.NoError(t, <-done)
	}
}

// The scanner has to walk fields it does not care about in order to find the
// ones it does, so every JSON shape has to be traversable even when nothing in
// it is ever read. These are the shapes that were never exercised: a decoder
// that mishandles one of them either rejects a valid event or, worse, walks off
// the end of a field and reads the next one as though it were the value.
//
// The fuzzer covers "does not panic". This covers "gets the right answer", which
// is a different claim and the one that matters for an ignored field: a scanner
// that stops in the wrong place produces a plausible event rather than an error.
func TestJSON_IgnoredFieldsOfEveryShapeAreWalkedCorrectly(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    event.Event
		wantErr error
	}{
		{
			name: "a false literal in an ignored field",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"k",` +
				`"ignored":false,"alsoIgnored":true}`,
			want: event.Event{Publisher: "p", Seq: 1, Op: event.OpDelete, Key: "k"},
		},
		{
			name: "a null in an ignored field",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"k",` +
				`"ignored":null}`,
			want: event.Event{Publisher: "p", Seq: 1, Op: event.OpDelete, Key: "k"},
		},
		{
			name: "an exponent with an explicit plus, which is legal JSON",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"k",` +
				`"ignored":1e+5}`,
			want: event.Event{Publisher: "p", Seq: 1, Op: event.OpDelete, Key: "k"},
		},
		{
			name: "an exponent with a minus and a capital E",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"k",` +
				`"ignored":1.5E-3}`,
			want: event.Event{Publisher: "p", Seq: 1, Op: event.OpDelete, Key: "k"},
		},
		{
			name: "a nested object several fields deep",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"k",` +
				`"meta":{"a":1,"b":{"c":"d","e":[1,2]},"f":null}}`,
			want: event.Event{Publisher: "p", Seq: 1, Op: event.OpDelete, Key: "k"},
		},
		{
			name: "an array of mixed shapes",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"k",` +
				`"trace":[1,"two",{"three":3},[4],true,null]}`,
			want: event.Event{Publisher: "p", Seq: 1, Op: event.OpDelete, Key: "k"},
		},
		{
			name: "an empty nested object and an empty array",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"k",` +
				`"a":{},"b":[]}`,
			want: event.Event{Publisher: "p", Seq: 1, Op: event.OpDelete, Key: "k"},
		},

		// The malformed half. Each one stops the scanner somewhere it cannot
		// recover from, and the decoder has to say so rather than guess.
		{
			name: "a nested object whose key is not a string",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"k",` +
				`"meta":{1:"a"}}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name: "a nested object missing the colon after its key",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"k",` +
				`"meta":{"a" 1}}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name: "an unterminated nested object",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"k",` +
				`"meta":{"a":1`,
			wantErr: codec.ErrMalformed,
		},
		{
			name: "an unterminated array",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"k",` +
				`"trace":[1,2`,
			wantErr: codec.ErrMalformed,
		},
		{
			name: "a nested object with a trailing separator and no value",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"k",` +
				`"meta":{"a":1,}}`,
			wantErr: codec.ErrMalformed,
		},
	}

	c := newJSON(t, nil)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var got event.Event
			err := c.Decode([]byte(tc.payload), "kv-events", &got)

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.want.Publisher, got.Publisher)
			assert.Equal(t, tc.want.Seq, got.Seq)
			assert.Equal(t, tc.want.Op, got.Op)
			assert.Equal(t, tc.want.Key, got.Key)
		})
	}
}

// A key longer than maxKeyBytes is rejected rather than truncated.
//
// Truncating would be worse than rejecting: two distinct keys that share a
// prefix would fold into one, and driftwatch would compare the wrong pair
// forever without anything looking wrong.
func TestJSON_AnOversizedKeyIsRejectedRatherThanTruncated(t *testing.T) {
	c := newJSON(t, map[string]string{"maxKeyBytes": "16"})

	within := `{"publisher":"p","seq":1,"op":"delete","key":"0123456789abcdef"}`
	var ok event.Event
	require.NoError(t, c.Decode([]byte(within), "kv-events", &ok))
	assert.Equal(t, "0123456789abcdef", ok.Key, "sixteen bytes is the limit, not past it")

	over := `{"publisher":"p","seq":1,"op":"delete","key":"0123456789abcdefg"}`
	var got event.Event
	err := c.Decode([]byte(over), "kv-events", &got)
	require.ErrorIs(t, err, codec.ErrTooLarge)
	assert.Contains(t, err.Error(), "17 bytes", "the message should say how far over it was")
}
