package codec

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The template codec's per-field parsers, called directly.
//
// Everything arrives as text here, so every field is a parse and every parse is
// a place a malformed line could become a plausible event rather than an error.
// Driving them through the regex would need a different pattern per case and
// would be testing the regex rather than the conversion.

func TestTemplateUint_ReadsDigitsAndNothingElse(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want uint64
	}{
		{name: "a small number", raw: "42", want: 42},
		{name: "surrounding space", raw: "  42  ", want: 42},
		{name: "empty is zero", raw: "", want: 0},
		{
			// D-002, in the codec where it is easiest to get right and would be
			// invisible if it were wrong.
			name: "above 2^53",
			raw:  "9007199254740993",
			want: 9007199254740993,
		},
		{name: "the largest uint64", raw: "18446744073709551615", want: 1<<64 - 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := templateUint(tc.raw, "seq")
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestTemplateUint_RefusesWhatIsNotOne(t *testing.T) {
	for _, raw := range []string{"-1", "1.5", "soon", "0x2a", "1e3"} {
		_, err := templateUint(raw, "seq")
		require.Error(t, err, "raw %q", raw)
		assert.ErrorIs(t, err, ErrMalformed)
		assert.Contains(t, err.Error(), "seq",
			"the message must name the field, not just the line")
	}
}

func TestTemplateTime_AcceptsRFC3339AndUnixIntegers(t *testing.T) {
	want := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		raw  string
		want time.Time
	}{
		{name: "RFC 3339", raw: "2026-01-01T00:00:00Z", want: want},
		{name: "RFC 3339 with nanoseconds", raw: "2026-01-01T00:00:00.000Z", want: want},
		{name: "Unix seconds", raw: "1767225600", want: time.Unix(1767225600, 0).UTC()},
		{name: "Unix millis", raw: "1767225600000", want: time.UnixMilli(1767225600000).UTC()},
		{name: "empty is unset", raw: "", want: time.Time{}},
		{name: "space is unset", raw: "   ", want: time.Time{}},
		{name: "zero is unset rather than 1970", raw: "0", want: time.Time{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := templateTime(tc.raw)
			require.NoError(t, err)
			assert.True(t, tc.want.Equal(got), "want %s, got %s", tc.want, got)
		})
	}
}

func TestTemplateTime_RefusesWhatIsNeither(t *testing.T) {
	for _, raw := range []string{"tomorrow", "2026-01-01", "1.5"} {
		_, err := templateTime(raw)
		require.Error(t, err, "raw %q", raw)
		assert.ErrorIs(t, err, ErrMalformed)
	}
}

func TestTemplateTTL_AcceptsDurationsAndBareSeconds(t *testing.T) {
	got, err := templateTTL("")
	require.NoError(t, err)
	assert.Nil(t, got, "an empty capture is an absence, not a zero")

	got, err = templateTTL("2m30s")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, 150*time.Second, *got)

	got, err = templateTTL("30")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, 30*time.Second, *got)

	got, err = templateTTL("0")
	require.NoError(t, err)
	require.NotNil(t, got, "an explicit zero means expire immediately")
	assert.Zero(t, *got)

	_, err = templateTTL("soon")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrMalformed)
	assert.Contains(t, err.Error(), "neither a duration nor a number of seconds")
}

func TestTrimTrailingNewline_RemovesOneLineEndingAtMost(t *testing.T) {
	// One, not all. A payload ending in two newlines has a genuinely empty last
	// line, and eating both would silently change what the pattern matches.
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "LF", raw: "line\n", want: "line"},
		{name: "CRLF", raw: "line\r\n", want: "line"},
		{name: "none", raw: "line", want: "line"},
		{name: "only the last of two", raw: "line\n\n", want: "line\n"},
		{name: "a bare CR is not a line ending", raw: "line\r", want: "line\r"},
		{name: "empty", raw: "", want: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, string(trimTrailingNewline([]byte(tc.raw))))
		})
	}
}

func TestCodecNames_AreWhatTheyRegisteredAs(t *testing.T) {
	// The registry name and the Name() method have to agree, or a metric
	// labeled by one and a config validated against the other disagree about
	// which codec is running.
	for _, name := range []string{"json", "msgpack", "template"} {
		cfg := map[string]string{}
		if name == "template" {
			cfg["pattern"] = `^(?P<op>\w+) (?P<key>\S+)$`
		}
		c, err := New(name, cfg)
		require.NoError(t, err, "codec %q", name)
		assert.Equal(t, name, c.Name())
	}
}
