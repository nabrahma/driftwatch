package explain_test

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/explain"
	"github.com/nabrahma/driftwatch/pkg/target"
)

func TestMain(m *testing.M) {
	// Every entry below is a third-party goroutine started at package init and
	// never stopped. No ignore here is ever for one of driftwatch's own
	// goroutines — one of those is a bug to fix, not an entry to add.
	goleak.VerifyTestMain(m,
		goleak.IgnoreAnyFunction("github.com/redis/go-redis/v9/internal/pool.startGlobalTimeCache.func1"),
	)
}

var update = flag.Bool("update", false, "rewrite the golden files in testdata/")

// goldenCases are the nine renderings §9 M13 requires a golden file for.
//
// They are the outputs an operator actually sees, so they are checked byte for
// byte: a change to the wording or the layout has to be looked at by a person,
// because this text is the product. A diff here is not a test failure to be
// silenced with --update, it is a review request.
var goldenCases = []struct {
	name  string
	build func(t *testing.T) *explain.Explanation
}{
	{"agree", buildAgree},
	{"in-flight", buildInFlight},
	{"missing-with-gaps", buildMissingWithGaps},
	{"missing-without-gaps", buildMissingWithoutGaps},
	{"extra", buildExtra},
	{"member-subset", buildMemberSubset},
	{"type-mismatch", buildTypeMismatch},
	{"truncated-history", buildTruncatedHistory},
	{"unknown-key", buildUnknownKey},
}

func TestText_MatchesTheGoldenFiles(t *testing.T) {
	for _, tc := range goldenCases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.build(t).Text()
			path := filepath.Join("testdata", tc.name+".golden")

			if *update {
				require.NoError(t, os.WriteFile(path, []byte(got), 0o600))
				return
			}

			want, err := os.ReadFile(path)
			require.NoError(t, err, "run: go test ./pkg/explain/ -update")
			assert.Equal(t, strings.ReplaceAll(string(want), "\r\n", "\n"), got)
		})
	}
}

func TestText_NeverExceeds120Columns(t *testing.T) {
	// §9 M13's hard limit. This output is read in a terminal beside a
	// dashboard, and a wrapped line inside the history table destroys the
	// alignment that makes the table worth having.
	const hardLimit = 120

	for _, tc := range goldenCases {
		t.Run(tc.name, func(t *testing.T) {
			for i, line := range strings.Split(tc.build(t).Text(), "\n") {
				width := utf8.RuneCountInString(line)
				assert.LessOrEqualf(t, width, hardLimit,
					"line %d is %d columns:\n%s", i+1, width, line)
			}
		})
	}
}

func TestText_StaysInsideTheLimitWithPathologicalInput(t *testing.T) {
	// The golden cases are well-behaved by construction. The limit has to hold
	// for a 400-byte key and a member name nobody sane would choose, because
	// those are what a real keyspace contains.
	const hardLimit = 120

	longKey := "block:" + strings.Repeat("f", 400)
	longMember := strings.Repeat("replica-with-an-absurd-name-", 20)

	f := newFixture(t)
	f.add(longKey, longMember, 1)
	f.add(longKey, "replica-0", 2)
	f.mem.SeedSets(map[string][]string{longKey: {"replica-0"}})
	f.advance(time.Minute)

	for i, line := range strings.Split(f.explain(longKey).Text(), "\n") {
		assert.LessOrEqualf(t, utf8.RuneCountInString(line), hardLimit,
			"line %d is too wide:\n%s", i+1, line)
	}
}

func TestJSON_IsWellFormedAndCarriesTheDiagnosis(t *testing.T) {
	e := buildMissingWithoutGaps(t)

	raw, err := e.JSON()
	require.NoError(t, err)

	var doc struct {
		Key       string `json:"key"`
		Verdict   string `json:"verdict"`
		Settled   bool   `json:"settled"`
		Diagnosis []struct {
			Code       string   `json:"code"`
			Confidence string   `json:"confidence"`
			Statement  string   `json:"statement"`
			Evidence   []string `json:"evidence"`
		} `json:"diagnosis"`
		History []struct {
			Seq uint64 `json:"seq"`
			Op  string `json:"op"`
		} `json:"history"`
		Publishers []struct {
			ID            string `json:"id"`
			MissingEvents uint64 `json:"missingEvents"`
		} `json:"publishers"`
	}
	require.NoError(t, json.Unmarshal(raw, &doc))

	assert.Equal(t, "block:9f3a", doc.Key)
	assert.Equal(t, "DIVERGED", doc.Verdict)
	assert.True(t, doc.Settled)
	require.NotEmpty(t, doc.Diagnosis)
	assert.Equal(t, explain.CodeMissingInTargetNoGaps, doc.Diagnosis[0].Code)
	assert.Equal(t, "high", doc.Diagnosis[0].Confidence)
	assert.NotEmpty(t, doc.Diagnosis[0].Evidence)
	assert.Len(t, doc.History, 2)
	assert.Equal(t, "add", doc.History[0].Op)
	require.Len(t, doc.Publishers, 1)
	assert.Equal(t, "replica-0", doc.Publishers[0].ID)
}

func TestJSON_HexEncodesAKeyThatIsNotValidUTF8(t *testing.T) {
	// The JSON encoder replaces invalid UTF-8 with U+FFFD, which would silently
	// corrupt the one field the whole document is about.
	binary := "block:\xff\xfe"

	f := newFixture(t, withProjection("scalar"))
	f.set(binary, "v1", 1)
	f.advance(time.Minute)

	raw, err := f.explain(binary).JSON()
	require.NoError(t, err)

	var doc struct {
		Key    string `json:"key"`
		KeyHex string `json:"keyHex"`
	}
	require.NoError(t, json.Unmarshal(raw, &doc))

	assert.Empty(t, doc.Key)
	assert.Equal(t, "626c6f636b3afffe", doc.KeyHex)
}

func TestJSON_NeverCarriesAStoredValueVerbatim(t *testing.T) {
	// §18: a report is a diagnostic, not a backup. Values go through
	// Value.String, which truncates and quotes, so the contents of the audited
	// store do not leave the cluster in whatever the output is piped into.
	secret := strings.Repeat("A", 4096)

	f := newFixture(t, withProjection("scalar"))
	f.set("session:9f3a", secret, 1)
	f.advance(time.Minute)

	raw, err := f.explain("session:9f3a").JSON()
	require.NoError(t, err)

	assert.NotContains(t, string(raw), secret)
	assert.Less(t, len(raw), 4096, "the whole value was not embedded")
}

// ---------------------------------------------------------------------------
// The nine golden scenarios.
// ---------------------------------------------------------------------------

func buildAgree(t *testing.T) *explain.Explanation {
	f := newFixture(t)
	f.add("block:9f3a", "replica-0", 8801)
	f.advance(2 * time.Second)
	f.add("block:9f3a", "replica-2", 8802)
	f.materialize("block:9f3a")
	f.advance(47 * time.Second)

	return f.explain("block:9f3a")
}

func buildInFlight(t *testing.T) *explain.Explanation {
	f := newFixture(t, withWindow(5*time.Second))
	f.add("block:9f3a", "replica-0", 8801)
	f.materialize("block:9f3a")
	f.advance(time.Second)
	f.add("block:9f3a", "replica-2", 8802) // the target has not caught up yet
	f.advance(1500 * time.Millisecond)

	return f.explain("block:9f3a")
}

func buildMissingWithGaps(t *testing.T) *explain.Explanation {
	f := newFixture(t)
	f.add("block:9f3a", "replica-0", 8801)
	f.advance(3 * time.Second)
	f.add("block:9f3a", "replica-0", 8847) // 8802..8846 never arrived
	f.advance(47 * time.Second)

	return f.explain("block:9f3a")
}

func buildMissingWithoutGaps(t *testing.T) *explain.Explanation {
	f := newFixture(t)
	f.add("block:9f3a", "replica-0", 8801)
	f.advance(3 * time.Second)
	f.add("block:9f3a", "replica-0", 8802)
	f.advance(47 * time.Second) // the materializer never wrote it

	return f.explain("block:9f3a")
}

func buildExtra(t *testing.T) *explain.Explanation {
	f := newFixture(t)
	f.mem.SeedSets(map[string][]string{"block:orphan": {"replica-3", "replica-7"}})
	f.advance(time.Minute)

	return f.explain("block:orphan")
}

func buildMemberSubset(t *testing.T) *explain.Explanation {
	// The §9 M13 sketch, reproduced: the publisher's sequence runs across its
	// whole keyspace, so the events touching this key are 8839, 8841, 8842 and
	// 8847 with no gap anywhere — the intervening numbers went to other keys.
	f := newFixture(t)
	f.add("block:9f3a", "replica-0", 8839)
	f.materialize("block:9f3a")
	f.advance(4 * time.Second)

	f.fill(8840, 8840)
	f.add("block:9f3a", "replica-2", 8841)
	f.advance(2 * time.Second)
	f.apply(event.Event{Op: event.OpRemove, Key: "block:9f3a", Member: "replica-2", Seq: 8842, Epoch: 1})
	f.advance(2 * time.Second)

	f.fill(8843, 8846)
	f.add("block:9f3a", "replica-2", 8847)
	f.advance(47 * time.Second)

	return f.explain("block:9f3a")
}

func buildTypeMismatch(t *testing.T) *explain.Explanation {
	f := newFixture(t)
	f.add("block:9f3a", "replica-0", 8801)
	f.mem.Seed(map[string][]byte{"block:9f3a": []byte("replica-0")})
	f.advance(time.Minute)

	return f.explain("block:9f3a")
}

func buildTruncatedHistory(t *testing.T) *explain.Explanation {
	f := newFixture(t, withRingSize(4))
	for seq := uint64(8801); seq <= 8812; seq++ {
		f.add("block:9f3a", "replica-0", seq)
		f.advance(2 * time.Second)
	}
	f.advance(time.Minute)

	return f.explain("block:9f3a")
}

func buildUnknownKey(t *testing.T) *explain.Explanation {
	f := newFixture(t)
	f.add("block:9f3a", "replica-0", 8801)
	f.advance(time.Minute)

	return f.explain("block:typo-in-the-key-name")
}

// buildTargetUnavailable is not a golden case; it backs the assertion that an
// unreadable store still produces a usable rendering rather than an error.
func buildTargetUnavailable(t *testing.T) *explain.Explanation {
	f := newFixture(t)
	f.add("block:9f3a", "replica-0", 8801)
	f.advance(time.Minute)

	return f.explainWithoutTarget("block:9f3a")
}

func TestText_RendersAnUnreadableStoreWithoutPretendingItIsEmpty(t *testing.T) {
	// "absent" and "could not be read" must never look alike in the output:
	// the first is a finding and the second is driftwatch admitting it does not
	// know (§23 A5).
	got := buildTargetUnavailable(t).Text()

	assert.Contains(t, got, "TARGET UNAVAILABLE")
	assert.Contains(t, got, "the store could not be reached")
	assert.NotContains(t, got, "TARGET    absent")
}

func TestText_RendersHealthDrivenDiagnosesReadably(t *testing.T) {
	f := newFixture(t)
	f.add("block:9f3a", "replica-0", 8801)
	f.advance(time.Minute)
	f.evictions = 4218
	f.mem.SetHealth(target.Health{
		Reachable:       true,
		EvictedKeys:     4218,
		UsedMemoryBytes: 3_900_000_000,
		MaxMemoryBytes:  4_000_000_000,
	})

	got := f.explain("block:9f3a").Text()

	assert.Contains(t, got, "3.6GiB of 3.7GiB")
	assert.Contains(t, got, "97% of maxmemory")
}
