package codec_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/codec"
	"github.com/nabrahma/driftwatch/pkg/event"
)

// The hot path deliberately reads strings straight out of the payload without
// unescaping, and falls back to the slow path only when a backslash is present.
// That split is exactly where a decoder grows a class of bugs that only appear
// against producers who escape things, so every field's slow path is exercised
// here directly.

func TestJSON_EscapedFieldsTakeTheSlowPathAndStillDecodeCorrectly(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    event.Event
	}{
		{
			name:    "an escaped op resolves through the built-in vocabulary",
			payload: `{"publisher":"p","seq":1,"op":"add","key":"k","member":"m"}`,
			want:    event.Event{Publisher: "p", Seq: 1, Op: event.OpAdd, Key: "k", Member: "m"},
		},
		{
			name:    "an escaped publisher decodes",
			payload: `{"publisher":"replica-2","seq":1,"op":"delete","key":"k"}`,
			want:    event.Event{Publisher: "replica-2", Seq: 1, Op: event.OpDelete, Key: "k"},
		},
		{
			name:    "an escaped key decodes",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"a\tb"}`,
			want:    event.Event{Publisher: "p", Seq: 1, Op: event.OpDelete, Key: "a\tb"},
		},
		{
			name:    "an escaped member decodes",
			payload: `{"publisher":"p","seq":1,"op":"add","key":"k","member":"a\"b"}`,
			want:    event.Event{Publisher: "p", Seq: 1, Op: event.OpAdd, Key: "k", Member: `a"b`},
		},
		{
			name:    "an escaped value decodes",
			payload: `{"publisher":"p","seq":1,"op":"set","key":"k","value":"a\nb"}`,
			want:    event.Event{Publisher: "p", Seq: 1, Op: event.OpSet, Key: "k", Value: []byte("a\nb")},
		},
		{
			name:    "an escaped duration string decodes",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"k","ttl":"90s"}`,
			want:    event.Event{Publisher: "p", Seq: 1, Op: event.OpDelete, Key: "k", TTL: ptr(90 * time.Second)},
		},
		{
			name:    "an escaped RFC3339 timestamp falls back to time.Parse",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"k","ts":"2026-07-30T11:02:31Z"}`,
			want: event.Event{
				Publisher: "p", Seq: 1, Op: event.OpDelete, Key: "k",
				PublishedAt: time.Date(2026, 7, 30, 11, 2, 31, 0, time.UTC),
			},
		},
		{
			name:    "an escaped numeric timestamp string decodes",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"k","ts":"1785412951"}`,
			want: event.Event{
				Publisher: "p", Seq: 1, Op: event.OpDelete, Key: "k",
				PublishedAt: time.Unix(1785412951, 0).UTC(),
			},
		},
	}

	c := newJSON(t, nil)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decode(t, c, tc.payload)
			require.NoError(t, err)

			want := tc.want
			want.Topic = "topic"
			assert.Equal(t, want, got)
		})
	}
}

func TestJSON_EscapedFieldErrorsAreStillReported(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		wantErr error
	}{
		{
			name:    "an escaped op that is not a known operation is an unknown op",
			payload: `{"publisher":"p","seq":1,"op":"teleport","key":"k"}`,
			wantErr: codec.ErrUnknownOp,
		},
		{
			name:    "a bad escape inside the op is malformed",
			payload: `{"publisher":"p","seq":1,"op":"\q","key":"k"}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "a bad escape inside the publisher is malformed",
			payload: `{"publisher":"\uZZZZ","seq":1,"op":"delete","key":"k"}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "a bad escape inside the value is malformed",
			payload: `{"publisher":"p","seq":1,"op":"set","key":"k","value":"\q"}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "a bad escape inside the ttl is malformed",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"k","ttl":"\q"}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "a bad escape inside the timestamp is malformed",
			payload: `{"publisher":"p","seq":1,"op":"delete","key":"k","ts":"\q"}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "a value of the wrong JSON type is malformed",
			payload: `{"publisher":"p","seq":1,"op":"set","key":"k","value":42}`,
			wantErr: codec.ErrMalformed,
		},
		{
			name:    "a member of the wrong JSON type is malformed",
			payload: `{"publisher":"p","seq":1,"op":"add","key":"k","member":42}`,
			wantErr: codec.ErrMalformed,
		},
	}

	c := newJSON(t, nil)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decode(t, c, tc.payload)
			assert.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestJSON_AnEscapedKeyIsSizeCheckedAfterUnescaping(t *testing.T) {
	// Six \u escapes are 36 raw bytes but six decoded ones. Checking only the
	// raw length would reject a legal key; checking only the decoded length
	// would let a payload of escapes past the cap on the way in.
	c := newJSON(t, map[string]string{"maxKeyBytes": "8"})

	got, err := decode(t, c,
		`{"publisher":"p","seq":1,"op":"delete","key":"abcdef"}`)
	require.NoError(t, err)
	assert.Equal(t, "abcdef", got.Key)

	_, err = decode(t, c, `{"publisher":"p","seq":1,"op":"delete","key":"`+
		strings.Repeat(`a`, 12)+`"}`)
	assert.ErrorIs(t, err, codec.ErrTooLarge)
}

func TestJSON_LoneSurrogatesBecomeTheReplacementRune(t *testing.T) {
	tests := []struct {
		name    string
		escaped string
		want    string
	}{
		{
			name:    "a high surrogate with nothing after it",
			escaped: `\ud83d`,
			want:    "�",
		},
		{
			name:    "a high surrogate followed by an ordinary character",
			escaped: `\ud83dx`,
			want:    "�x",
		},
		{
			name:    "a high surrogate followed by a non-surrogate escape",
			escaped: `\ud83da`,
			want:    "�a",
		},
		{
			name:    "a high surrogate followed by another high surrogate",
			escaped: `\ud83d\ud83d`,
			want:    "��",
		},
		{
			name:    "a lone low surrogate",
			escaped: `\udc00`,
			want:    "�",
		},
		{
			name:    "a well-formed surrogate pair is joined",
			escaped: `😀`,
			want:    "😀",
		},
	}

	c := newJSON(t, nil)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decode(t, c,
				`{"publisher":"p","seq":1,"op":"delete","key":"`+tc.escaped+`"}`)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got.Key)
		})
	}
}

func TestJSON_ASurrogatePairWithABadSecondEscapeIsMalformed(t *testing.T) {
	_, err := decode(t, newJSON(t, nil),
		`{"publisher":"p","seq":1,"op":"delete","key":"\ud83d\uZZZZ"}`)

	assert.ErrorIs(t, err, codec.ErrMalformed)
}

func TestJSON_RFC3339FastPathAgreesWithTimeParse(t *testing.T) {
	// The fast path exists only to avoid an allocation, so anything it accepts
	// must match what time.Parse would have produced. Anything it rejects falls
	// through to time.Parse, which is why the rejections below still decode.
	accepted := []string{
		"2026-07-30T11:02:31Z",
		"2026-07-30T11:02:31.4Z",
		"2026-07-30T11:02:31.412Z",
		"2026-07-30T11:02:31.123456789Z",
		"2026-07-30T11:02:31.1234567891234Z",
		"2026-07-30T13:02:31+02:00",
		"2026-07-30T09:02:31-02:00",
		"2000-02-29T00:00:00Z",
	}

	c := newJSON(t, nil)
	for _, ts := range accepted {
		t.Run(ts, func(t *testing.T) {
			want, err := time.Parse(time.RFC3339Nano, ts)
			require.NoError(t, err)

			got, err := decode(t, c,
				`{"publisher":"p","seq":1,"op":"delete","key":"k","ts":"`+ts+`"}`)
			require.NoError(t, err)
			assert.True(t, want.Equal(got.PublishedAt),
				"fast path produced %s, time.Parse produced %s", got.PublishedAt, want)
		})
	}
}

func TestJSON_RFC3339RejectionsFallThroughAndAreReportedHonestly(t *testing.T) {
	tests := []struct {
		name string
		ts   string
	}{
		{name: "too short to be a timestamp", ts: "2026-07-30T11:02"},
		{name: "the date separator is wrong", ts: "2026/07/30T11:02:31Z"},
		{name: "the time separator is missing", ts: "2026-07-30 11:02:31Z"},
		{name: "the hour separator is wrong", ts: "2026-07-30T11.02:31Z"},
		{name: "the minute separator is wrong", ts: "2026-07-30T11:02.31Z"},
		{name: "a component is not a number", ts: "20x6-07-30T11:02:31Z"},
		{name: "the month is out of range", ts: "2026-13-30T11:02:31Z"},
		{name: "the day is out of range for the month", ts: "2026-02-30T11:02:31Z"},
		{name: "the hour is out of range", ts: "2026-07-30T25:02:31Z"},
		{name: "the fraction is empty", ts: "2026-07-30T11:02:31.Z"},
		{name: "the zone is missing", ts: "2026-07-30T11:02:31"},
		{name: "the zone is not recognized", ts: "2026-07-30T11:02:31X"},
		{
			// RFC 3339 §5.6 permits these, but Go's time.RFC3339 layout does
			// not. The fast path has to reject them too, or the same timestamp
			// would decode differently depending on whether the payload
			// happened to contain a backslash. See docs/DISCOVERIES.md D-001.
			name: "the lowercase date-time separator that RFC 3339 allows but time.Parse does not",
			ts:   "2026-07-30t11:02:31Z",
		},
		{
			name: "the lowercase zulu marker that RFC 3339 allows but time.Parse does not",
			ts:   "2026-07-30T11:02:31z",
		},
		{name: "there are trailing bytes after the zulu marker", ts: "2026-07-30T11:02:31Zextra"},
		{name: "the offset is the wrong width", ts: "2026-07-30T11:02:31+0200"},
		{name: "the offset separator is wrong", ts: "2026-07-30T11:02:31+02-00"},
		{name: "the offset is not numeric", ts: "2026-07-30T11:02:31+xx:00"},
		{name: "the offset minutes are not numeric", ts: "2026-07-30T11:02:31+02:xx"},
	}

	c := newJSON(t, nil)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decode(t, c,
				`{"publisher":"p","seq":1,"op":"delete","key":"k","ts":"`+tc.ts+`"}`)

			// Whatever the fast path declines, time.Parse gets the final say.
			// Every one of these is genuinely not a timestamp, so the decode
			// must fail rather than silently producing a wrong time.
			assert.ErrorIs(t, err, codec.ErrMalformed, "ts %q", tc.ts)
		})
	}
}

func TestJSON_ErrorsNameTheConfiguredFieldRatherThanTheInternalOne(t *testing.T) {
	c := newJSON(t, map[string]string{"opField": "event_type"})

	_, err := decode(t, c, `{"publisher":"p","seq":1,"key":"k"}`)

	require.ErrorIs(t, err, codec.ErrMissingField)
	assert.Contains(t, err.Error(), "event_type",
		"an operator reading this needs the name they configured")
}
