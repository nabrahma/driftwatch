package logging_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/logging"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func epoch() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }

// lines parses the buffer as one JSON object per line.
func lines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()

	out := []map[string]any{}
	for _, raw := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if raw == "" {
			continue
		}
		var obj map[string]any
		require.NoError(t, json.Unmarshal([]byte(raw), &obj), "line %q", raw)
		out = append(out, obj)
	}
	return out
}

func TestNew_JSONFormatEmitsOneObjectPerLine(t *testing.T) {
	var buf bytes.Buffer
	log, flush, err := logging.New(logging.Options{Format: "json", Out: &buf})
	require.NoError(t, err)

	log.Info("sweep finished", "keys", 12)
	log.Error(assert.AnError, "target unreachable")
	require.NoError(t, flush())

	got := lines(t, &buf)
	require.Len(t, got, 2)
	assert.Equal(t, "sweep finished", got[0]["msg"])
	assert.InDelta(t, 12.0, got[0]["keys"], 0)
	assert.Equal(t, "target unreachable", got[1]["msg"])
}

func TestNew_LevelGatesVerbosity(t *testing.T) {
	tests := []struct {
		name    string
		opts    logging.Options
		wantMsg []string
	}{
		{
			name:    "info is the default and hides debug",
			opts:    logging.Options{},
			wantMsg: []string{"error", "info"},
		},
		{
			name:    "debug shows V(1)",
			opts:    logging.Options{Level: "debug"},
			wantMsg: []string{"error", "info", "debug"},
		},
		{
			name:    "warn hides info but keeps errors",
			opts:    logging.Options{Level: "warn"},
			wantMsg: []string{"error"},
		},
		{
			name:    "error hides everything but errors",
			opts:    logging.Options{Level: "error"},
			wantMsg: []string{"error"},
		},
		{
			name:    "-v 2 opens trace regardless of level",
			opts:    logging.Options{V: 2},
			wantMsg: []string{"error", "info", "debug", "trace"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			opts := tc.opts
			opts.Format, opts.Out = "json", &buf

			log, flush, err := logging.New(opts)
			require.NoError(t, err)

			log.Error(assert.AnError, "error")
			log.Info("info")
			log.V(1).Info("debug")
			log.V(2).Info("trace")
			require.NoError(t, flush())

			got := make([]string, 0, 4)
			for _, obj := range lines(t, &buf) {
				msg, ok := obj["msg"].(string)
				require.True(t, ok, "every line carries a msg field")
				got = append(got, msg)
			}
			assert.Equal(t, tc.wantMsg, got)
		})
	}
}

func TestNew_RejectsAnUnknownFormat(t *testing.T) {
	_, _, err := logging.New(logging.Options{Format: "yaml"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "log-format")
	assert.Contains(t, err.Error(), "console")
}

func TestNew_RejectsAnUnknownLevel(t *testing.T) {
	_, _, err := logging.New(logging.Options{Level: "verbose"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "log-level")
}

func TestNew_ConsoleFormatIsPlainText(t *testing.T) {
	var buf bytes.Buffer
	log, flush, err := logging.New(logging.Options{Format: "console", Out: &buf})
	require.NoError(t, err)

	log.WithValues("check", "inference/kvcache").Info("watching")
	require.NoError(t, flush())

	assert.Contains(t, buf.String(), "watching")
	assert.Contains(t, buf.String(), "inference/kvcache")

	var obj map[string]any
	assert.Error(t, json.Unmarshal(buf.Bytes(), &obj), "console output is not a JSON document")
}

func TestRedact_IsIdentityUntilEnabled(t *testing.T) {
	// The default has to be off. A tool whose logs never name a key is much
	// harder to debug with, so redaction is opt-in per §12.3.
	assert.Equal(t, "block:9f3a", logging.Redact("block:9f3a"))
}

func TestRedact_HashesStablyWhenEnabled(t *testing.T) {
	logging.SetRedactKeys(true)
	t.Cleanup(func() { logging.SetRedactKeys(false) })

	got := logging.Redact("block:9f3a")

	assert.NotContains(t, got, "block", "the key must not survive redaction")
	assert.Equal(t, got, logging.Redact("block:9f3a"), "the same key hashes the same way")
	assert.NotEqual(t, got, logging.Redact("block:9f3b"), "distinct keys stay distinguishable")
	assert.True(t, strings.HasPrefix(got, "sha256:"), "the hash announces what it is: %q", got)
}

func TestRedact_LeavesTheEmptyStringAlone(t *testing.T) {
	logging.SetRedactKeys(true)
	t.Cleanup(func() { logging.SetRedactKeys(false) })

	// Hashing "" would turn "no key" into something that looks like a key.
	assert.Empty(t, logging.Redact(""))
}

func TestSampler_AllowsTheFirstBurstThenOnePerInterval(t *testing.T) {
	clk := clock.Fake(epoch())
	s := logging.NewSampler(clk, 10, 10*time.Second, 64)

	allowed := 0
	for i := 0; i < 100; i++ {
		if ok, _ := s.Allow("decode_error"); ok {
			allowed++
		}
	}
	assert.Equal(t, 10, allowed, "the burst is the first 10 and nothing more")

	clk.Advance(10 * time.Second)
	ok, suppressed := s.Allow("decode_error")
	assert.True(t, ok, "one line is allowed through once the interval elapses")
	assert.Equal(t, 90, suppressed, "the caller is told how many it did not see")

	ok, suppressed = s.Allow("decode_error")
	assert.False(t, ok)
	assert.Zero(t, suppressed)
}

func TestSampler_CountsEachReasonSeparately(t *testing.T) {
	// An unbounded error log is its own outage (§12.3), but a burst of decode
	// errors must not silence the first target error.
	clk := clock.Fake(epoch())
	s := logging.NewSampler(clk, 2, time.Minute, 64)

	for i := 0; i < 20; i++ {
		s.Allow("decode_error")
	}

	ok, _ := s.Allow("target_error")
	assert.True(t, ok, "a different reason has its own budget")
}

func TestSampler_BoundsTheReasonMap(t *testing.T) {
	// Reasons come from closed enums in the code, but a sampler that trusts
	// that is one refactor away from being a memory leak keyed by error string.
	clk := clock.Fake(epoch())
	s := logging.NewSampler(clk, 1, time.Minute, 8)

	for i := 0; i < 1000; i++ {
		s.Allow(strings.Repeat("r", i%997))
	}

	assert.LessOrEqual(t, s.Tracked(), 8, "the sampler kept every reason it was handed")
}

func TestSampler_NilIsUsable(t *testing.T) {
	// Call sites should not have to nil-check before logging.
	var s *logging.Sampler

	ok, suppressed := s.Allow("anything")

	assert.True(t, ok)
	assert.Zero(t, suppressed)
}
