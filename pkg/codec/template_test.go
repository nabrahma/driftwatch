package codec_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/codec"
	"github.com/nabrahma/driftwatch/pkg/event"
)

// The template codec is §7's compatibility escape hatch: a regex with named
// capture groups, for a producer emitting a line format nobody is going to
// change. It is slow by construction and documented as such; what it must be is
// unsurprising, and every test here is about a way it could quietly do the
// wrong thing rather than fail.

func newTemplate(t *testing.T, cfg map[string]string) codec.Codec {
	t.Helper()
	c, err := codec.New("template", cfg)
	require.NoError(t, err)
	return c
}

// logLine is the format the tests use: a syslog-ish line from a producer that
// predates anyone thinking about auditing it.
const logLinePattern = `^(?P<ts>\S+) (?P<publisher>\S+) (?P<seq>\d+) ` +
	`(?P<op>\w+) (?P<key>\S+)(?: (?P<member>\S+))?$`

func TestTemplate_DecodesALineIntoAnEvent(t *testing.T) {
	c := newTemplate(t, map[string]string{"pattern": logLinePattern})

	got, err := decode(t, c, "2026-01-01T00:00:00Z replica-0 8847 ADD block:9f3a replica-2")
	require.NoError(t, err)

	assert.Equal(t, "replica-0", got.Publisher)
	assert.Equal(t, uint64(8847), got.Seq)
	assert.Equal(t, event.OpAdd, got.Op)
	assert.Equal(t, "block:9f3a", got.Key)
	assert.Equal(t, "replica-2", got.Member)
	assert.Equal(t, "topic", got.Topic)
	assert.Equal(t, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), got.PublishedAt)
}

func TestTemplate_AnOptionalGroupThatDidNotParticipateLeavesItsFieldEmpty(t *testing.T) {
	// The same pattern with the member omitted. A group inside an optional
	// section reports (-1, -1) rather than an empty span, and treating those the
	// same would write "" over a field another group had set.
	c := newTemplate(t, map[string]string{"pattern": logLinePattern})

	got, err := decode(t, c, "2026-01-01T00:00:00Z replica-0 8848 DELETE block:9f3a")
	require.NoError(t, err)

	assert.Equal(t, event.OpDelete, got.Op)
	assert.Equal(t, "block:9f3a", got.Key)
	assert.Empty(t, got.Member)
}

func TestTemplate_ATrailingNewlineIsNotPartOfTheLine(t *testing.T) {
	// Every event read from a file source arrives with one. A pattern anchored
	// with $ would fail on all of them, and the operator would be left
	// debugging a regex that is correct.
	c := newTemplate(t, map[string]string{"pattern": logLinePattern})

	for _, suffix := range []string{"\n", "\r\n"} {
		got, err := decode(t, c,
			"2026-01-01T00:00:00Z replica-0 1 ADD block:9f3a replica-2"+suffix)
		require.NoError(t, err, "suffix %q", suffix)
		assert.Equal(t, "replica-2", got.Member, "suffix %q", suffix)
	}
}

func TestTemplate_APayloadThatDoesNotMatchIsMalformed(t *testing.T) {
	c := newTemplate(t, map[string]string{"pattern": logLinePattern})

	_, err := decode(t, c, "this line is not in the configured format")
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrMalformed)
	assert.Contains(t, err.Error(), "pattern",
		"the message should say what it failed to match: %v", err)
}

func TestTemplate_RefusesAPatternThatMatchesTheEmptyString(t *testing.T) {
	// The failure this prevents is the quiet one. A pattern of all-optional
	// groups matches every payload — including a truncated frame — and produces
	// an event of empty fields rather than an error. driftwatch would then fold
	// a stream of empty keys into the oracle and report the entire real
	// keyspace as missing.
	//
	// Catching it at construction turns that into a startup failure.
	_, err := codec.New("template", map[string]string{
		"pattern": `(?P<op>\w*)(?P<key>\S*)`,
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrBadConfig)
	assert.Contains(t, err.Error(), "empty string")
}

func TestTemplate_RefusesAPatternWithNoOpGroup(t *testing.T) {
	// Every event has an operation. Without an op group every line decodes as
	// OpUnknown, which the pipeline counts as a version mismatch and reports as
	// the producer's problem. It is neither.
	_, err := codec.New("template", map[string]string{
		"pattern": `^(?P<key>\S+) (?P<member>\S+)$`,
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrBadConfig)
	assert.Contains(t, err.Error(), "op")
}

func TestTemplate_RefusesACaptureGroupThatNamesNothing(t *testing.T) {
	// A typo in a group name is otherwise invisible: the group captures, the
	// codec ignores it, and the field it was meant to fill stays empty forever.
	// The operator sees a check whose publisher is always "" and no reason why.
	_, err := codec.New("template", map[string]string{
		"pattern": `^(?P<op>\w+) (?P<keyy>\S+)$`,
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrBadConfig)
	assert.Contains(t, err.Error(), "keyy")
	assert.Contains(t, err.Error(), "key",
		"listing the valid names is what turns this from a rejection into a fix: %v", err)
}

func TestTemplate_AnUnnamedGroupIsAllowed(t *testing.T) {
	// A pattern needs grouping for alternation whether or not it wants to
	// capture, so an unnamed group must not be an error.
	c := newTemplate(t, map[string]string{
		"pattern": `^(?:ADD|SET) (?P<op>\w+) (?P<key>\S+)$`,
	})

	got, err := decode(t, c, "ADD set block:9f3a")
	require.NoError(t, err)
	assert.Equal(t, event.OpSet, got.Op)
	assert.Equal(t, "block:9f3a", got.Key)
}

func TestTemplate_RefusesAnInvalidRegularExpression(t *testing.T) {
	_, err := codec.New("template", map[string]string{"pattern": `^(?P<op>\w+`})
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrBadConfig)
}

func TestTemplate_RefusesAnEmptyPattern(t *testing.T) {
	// The default configuration is not a working one, and saying so at
	// construction is better than matching nothing at runtime.
	_, err := codec.New("template", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrBadConfig)
	assert.Contains(t, err.Error(), "named capture groups",
		"the message should show the shape of what is wanted: %v", err)
}

func TestTemplate_HonoursAConfiguredOpMapping(t *testing.T) {
	c := newTemplate(t, map[string]string{
		"pattern":   `^(?P<op>\S+) (?P<key>\S+)$`,
		"opMapping": "BLOCK_STORED=add,BLOCK_EVICTED=remove",
	})

	got, err := decode(t, c, "BLOCK_STORED block:9f3a")
	require.NoError(t, err)
	assert.Equal(t, event.OpAdd, got.Op)

	got, err = decode(t, c, "BLOCK_EVICTED block:9f3a")
	require.NoError(t, err)
	assert.Equal(t, event.OpRemove, got.Op)
}

func TestTemplate_AnUnknownOpIsItsOwnSentinel(t *testing.T) {
	c := newTemplate(t, map[string]string{"pattern": `^(?P<op>\w+) (?P<key>\S+)$`})

	_, err := decode(t, c, "teleport block:9f3a")
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrUnknownOp)
	assert.NotErrorIs(t, err, codec.ErrMalformed)
}

func TestTemplate_ASequenceNumberIsReadFromItsDigits(t *testing.T) {
	// D-002 again, in the place it is easiest to get right and would be
	// invisible if it were wrong: straight from text to uint64, never through
	// a float.
	c := newTemplate(t, map[string]string{
		"pattern": `^(?P<seq>\d+) (?P<op>\w+) (?P<key>\S+)$`,
	})

	got, err := decode(t, c, "9007199254740993 add block:9f3a")
	require.NoError(t, err)
	assert.Equal(t, uint64(9007199254740993), got.Seq)
}

func TestTemplate_RejectsNumericFieldsThatAreNotNumbers(t *testing.T) {
	// A pattern loose enough to capture a non-number is the operator's to fix,
	// but the failure has to name the field rather than the whole line.
	c := newTemplate(t, map[string]string{
		"pattern": `^(?P<seq>\S+) (?P<op>\w+) (?P<key>\S+)$`,
	})

	_, err := decode(t, c, "not-a-number add block:9f3a")
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrMalformed)
	assert.Contains(t, err.Error(), "seq")
}

func TestTemplate_TTLAcceptsBothADurationAndBareSeconds(t *testing.T) {
	c := newTemplate(t, map[string]string{
		"pattern": `^(?P<op>\w+) (?P<key>\S+) (?P<ttl>\S+)$`,
	})

	got, err := decode(t, c, "set block:9f3a 2m30s")
	require.NoError(t, err)
	require.NotNil(t, got.TTL)
	assert.Equal(t, 150*time.Second, *got.TTL)

	got, err = decode(t, c, "set block:9f3a 30")
	require.NoError(t, err)
	require.NotNil(t, got.TTL)
	assert.Equal(t, 30*time.Second, *got.TTL)
}

func TestTemplate_RejectsAnOversizedPayload(t *testing.T) {
	c := newTemplate(t, map[string]string{
		"pattern":         `^(?P<op>\w+) (?P<key>\S+)$`,
		"maxPayloadBytes": "32",
	})

	long := "set block:"
	for len(long) < 64 {
		long += "9f3a"
	}
	_, err := decode(t, c, long)
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrTooLarge)
}

func TestTemplate_RejectsAnEmptyPayload(t *testing.T) {
	c := newTemplate(t, map[string]string{"pattern": `^(?P<op>\w+) (?P<key>\S+)$`})

	_, err := decode(t, c, "")
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrMalformed)
}

func TestTemplate_NeverPanicsOnArbitraryInput(t *testing.T) {
	// The property every codec in this package guarantees. There is no separate
	// fuzz target for template because the regex is operator-supplied and a
	// fuzzer would be exercising Go's regexp package rather than this code —
	// but the guarantee still has to hold, so the adversarial corpus from
	// §25.1 is run through it directly.
	c := newTemplate(t, map[string]string{"pattern": logLinePattern})

	for _, payload := range []string{
		"", "\n", "\r\n", "\x00", "\xff\xfe",
		"2026-01-01T00:00:00Z", " ", "                    ",
		"2026-01-01T00:00:00Z replica-0 99999999999999999999999 ADD k m",
		"2026-01-01T00:00:00Z replica-0 -1 ADD k m",
		"\x00\x00\x00 \x00 0 ADD \x00 \x00",
	} {
		assert.NotPanics(t, func() {
			var e event.Event
			// The error is deliberately discarded: what is under test is that
			// the call returns at all, whatever it returns.
			err := c.Decode([]byte(payload), "topic", &e)
			_ = err
		}, "payload %q", payload)
	}
}

func TestTemplate_RetainRawKeepsACopy(t *testing.T) {
	c := newTemplate(t, map[string]string{
		"pattern":   `^(?P<op>\w+) (?P<key>\S+)$`,
		"retainRaw": "true",
	})

	payload := []byte("set block:9f3a")
	var got event.Event
	require.NoError(t, c.Decode(payload, "topic", &got))
	require.NotEmpty(t, got.Raw)

	before := append([]byte(nil), got.Raw...)
	for i := range payload {
		payload[i] = 0
	}
	assert.Equal(t, before, got.Raw,
		"Raw aliased the caller's buffer and changed when it was reused")
}

func TestTemplate_IsRegisteredUnderItsName(t *testing.T) {
	assert.Contains(t, codec.Names(), "template")
}

func TestTemplate_FillsEveryFieldAPatternCanName(t *testing.T) {
	// One pattern with a group for all ten fields. Each `assign` arm is a
	// separate parse, and an arm nothing exercises is one nobody has checked —
	// a codec that silently never populated `epoch` would produce a check that
	// treats every publisher restart as a gap.
	c := newTemplate(t, map[string]string{
		"pattern": `^(?P<ts>\S+) (?P<publisher>\S+) (?P<epoch>\d+) (?P<seq>\d+) ` +
			`(?P<op>\w+) (?P<key>\S+) (?P<member>\S+) (?P<value>\S+) ` +
			`(?P<ttl>\S+) (?P<delta>-?\d+)$`,
	})

	got, err := decode(t, c,
		"2026-01-01T00:00:00Z replica-0 3 8847 set block:9f3a replica-2 v1 90s -5")
	require.NoError(t, err)

	assert.Equal(t, "replica-0", got.Publisher)
	assert.Equal(t, uint64(3), got.Epoch)
	assert.Equal(t, uint64(8847), got.Seq)
	assert.Equal(t, event.OpSet, got.Op)
	assert.Equal(t, "block:9f3a", got.Key)
	assert.Equal(t, "replica-2", got.Member)
	assert.Equal(t, "v1", string(got.Value))
	require.NotNil(t, got.TTL)
	assert.Equal(t, 90*time.Second, *got.TTL)
	assert.Equal(t, int64(-5), got.Delta)
	assert.Equal(t, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), got.PublishedAt)
}

func TestTemplate_RejectsANonNumericEpochAndDelta(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		line    string
		field   string
	}{
		{
			name:    "epoch",
			pattern: `^(?P<epoch>\S+) (?P<op>\w+) (?P<key>\S+)$`,
			line:    "soon set block:9f3a",
			field:   "epoch",
		},
		{
			name:    "delta",
			pattern: `^(?P<delta>\S+) (?P<op>\w+) (?P<key>\S+)$`,
			line:    "soon incr counter:hits",
			field:   "delta",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newTemplate(t, map[string]string{"pattern": tc.pattern})
			_, err := decode(t, c, tc.line)
			require.Error(t, err)
			assert.ErrorIs(t, err, codec.ErrMalformed)
			assert.Contains(t, err.Error(), tc.field)
		})
	}
}

func TestTemplate_RejectsMalformedLimitsAndFlags(t *testing.T) {
	// A configuration error has to fail at construction. A codec built with a
	// silently ignored limit is a codec whose bounds are not what the manifest
	// says they are.
	tests := []struct {
		name string
		cfg  map[string]string
	}{
		{
			name: "a non-numeric payload limit",
			cfg:  map[string]string{"maxPayloadBytes": "lots"},
		},
		{
			name: "a zero payload limit",
			cfg:  map[string]string{"maxPayloadBytes": "0"},
		},
		{
			name: "a non-numeric key limit",
			cfg:  map[string]string{"maxKeyBytes": "big"},
		},
		{
			name: "a non-boolean retainRaw",
			cfg:  map[string]string{"retainRaw": "sometimes"},
		},
		{
			name: "an op mapping that is not name=op",
			cfg:  map[string]string{"opMapping": "BLOCK_STORED"},
		},
		{
			name: "an op mapping onto an operation that does not exist",
			cfg:  map[string]string{"opMapping": "BLOCK_STORED=teleport"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := map[string]string{"pattern": `^(?P<op>\w+) (?P<key>\S+)$`}
			for k, v := range tc.cfg {
				cfg[k] = v
			}
			_, err := codec.New("template", cfg)
			require.Error(t, err)
			assert.ErrorIs(t, err, codec.ErrBadConfig)
		})
	}
}

func TestTemplate_AnOpGroupThatDidNotParticipateIsAMissingField(t *testing.T) {
	// The op group is required at construction, but a pattern can make it
	// optional inside an alternation. A line that matches without it decodes to
	// OpUnknown, which is a missing field rather than an unknown operation —
	// the producer sent nothing, it did not send something unrecognized.
	c := newTemplate(t, map[string]string{
		"pattern": `^(?P<key>\S+)(?: (?P<op>\w+))?$`,
	})

	_, err := decode(t, c, "block:9f3a")
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrMissingField)
	assert.NotErrorIs(t, err, codec.ErrUnknownOp,
		"an absent op is not an unrecognized one; they point at different problems")
}

func TestTemplate_RejectsAnOversizedKey(t *testing.T) {
	c := newTemplate(t, map[string]string{
		"pattern":     `^(?P<op>\w+) (?P<key>\S+)$`,
		"maxKeyBytes": "8",
	})

	_, err := decode(t, c, "set a-key-far-longer-than-eight-bytes")
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrTooLarge)
}
