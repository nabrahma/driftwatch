package codec_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/codec"
	"github.com/nabrahma/driftwatch/pkg/event"
)

// FuzzDecodeJSON asserts the one property that matters against untrusted input:
// Decode never panics. A decoder that panics on a malformed frame takes down
// the auditor, and it does so at exactly the moment the system it is auditing
// is misbehaving.
//
// Any crash this finds is committed to testdata/fuzz/ by the Go toolchain and
// becomes a permanent regression case (§16.2).
func FuzzDecodeJSON(f *testing.F) {
	for _, seed := range fuzzSeeds(f) {
		f.Add([]byte(seed))
	}

	plain, err := codec.New("json", nil)
	require.NoError(f, err)

	// A second codec with a foreign mapping exercises the aliasing and
	// opMapping paths, which the default configuration never reaches.
	mapped, err := codec.New("json", map[string]string{
		"publisherField": "replica_id",
		"memberField":    "replica_id",
		"seqField":       "event_id",
		"opField":        "event_type",
		"keyField":       "block_hash",
		"opMapping":      "BLOCK_STORED=add,BLOCK_EVICTED=remove",
		"retainRaw":      "true",
		"maxDepth":       "6",
		"maxKeyBytes":    "32",
	})
	require.NoError(f, err)

	f.Fuzz(func(t *testing.T, payload []byte) {
		for _, c := range []codec.Codec{plain, mapped} {
			var got event.Event
			if err := c.Decode(payload, "topic", &got); err != nil {
				continue
			}
			// A decode that reports success must produce an event the rest of
			// the pipeline can rely on, so the postcondition is checked too.
			require.NoError(t, got.Validate(),
				"Decode returned an invalid event for %q", payload)
		}
	})
}

// fuzzSeeds returns the adversarial corpus from §25.1 plus the golden
// payloads, so the fuzzer starts from inputs that are already interesting
// rather than discovering JSON from scratch.
func fuzzSeeds(f *testing.F) []string {
	f.Helper()

	seeds := []string{
		``,
		`{}`,
		`null`,
		`[]`,
		`{"seq":1e300}`,
		`{"seq":"9007199254740993"}`,
		`{"seq":9007199254740993}`,
		`{"op":"ADD"}`,
		`{"ttl":-1}`,
		`{"publisher":"p","seq":1,"op":"delete","key":"\xff\xfe"}`,
		`{"key":"a","key":"b"}`,
		`{"publisher":"p","seq":1,"op":"add","key":"k","member":"m"}`,
		`{"publisher":"p","seq":1,"op":"snapshotBegin"}`,
		`{"publisher":"p","seq":1,"op":"incr","key":"k","delta":-9223372036854775808}`,
		`{"publisher":"p","seq":1,"op":"set","key":"k","value":"😀"}`,
		`{"publisher":"p","seq":1,"op":"set","key":"k","ts":"2026-07-30T11:02:31.412Z"}`,
		strings.Repeat("[", 200) + strings.Repeat("]", 200),
		`{"a":` + strings.Repeat(`{"a":`, 100) + `1` + strings.Repeat(`}`, 100) + `}`,
	}

	matches, err := filepath.Glob(filepath.Join("testdata", "*.json"))
	require.NoError(f, err)
	for _, path := range matches {
		content, readErr := os.ReadFile(path)
		require.NoError(f, readErr)
		seeds = append(seeds, string(content))
	}

	lines, err := os.ReadFile(filepath.Join("testdata", "snapshot_cycle.jsonl"))
	require.NoError(f, err)
	for _, line := range strings.Split(string(lines), "\n") {
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			seeds = append(seeds, trimmed)
		}
	}
	return seeds
}
