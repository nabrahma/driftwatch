package differ_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/differ"
	"github.com/nabrahma/driftwatch/pkg/oracle"
)

// A report has two renderings and they answer to different readers: Text goes
// to a terminal where a human is deciding what to do next, JSON goes to
// whatever is going to store or query it. Both are part of the interface, and
// the JSON one more so — a field that quietly changes name breaks a consumer
// that has no way to notice until its query returns nothing.

func TestReport_TextNamesBothTTLsWhenTheyDisagree(t *testing.T) {
	// A TTL mismatch is the one finding where the two values are the whole
	// story: "block:9f3a expires in 60s, the store says never" is actionable,
	// and "block:9f3a has a TTL mismatch" is not. Both sides have to appear,
	// including the side that is absent — a missing TTL rendered as an empty
	// string reads as a formatting bug rather than as "no expiry set".
	oracleTTL := 60 * time.Second

	rep := differ.NewReport(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), differ.Options{})
	rep.Add(&differ.Finding{
		Key:       "block:9f3a",
		Category:  differ.CatTTLMismatch,
		Trust:     oracle.TrustComplete,
		OracleTTL: &oracleTTL,
		TargetTTL: nil, // the store has no expiry at all
	})
	rep.FinishedAt = rep.StartedAt.Add(time.Second)

	text := rep.Text()

	assert.Contains(t, text, "1m0s",
		"the oracle's expected TTL should appear: %s", text)
	assert.Contains(t, text, "none",
		"a target with no TTL must render as 'none' rather than as an empty "+
			"string, which reads as a formatting bug: %s", text)
}

func TestReport_TextRendersBothTTLsWhenBothArePresent(t *testing.T) {
	oracleTTL := 60 * time.Second
	targetTTL := 5 * time.Minute

	rep := differ.NewReport(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), differ.Options{})
	rep.Add(&differ.Finding{
		Key:       "block:9f3a",
		Category:  differ.CatTTLMismatch,
		Trust:     oracle.TrustComplete,
		OracleTTL: &oracleTTL,
		TargetTTL: &targetTTL,
	})
	rep.FinishedAt = rep.StartedAt.Add(time.Second)

	text := rep.Text()
	assert.Contains(t, text, "1m0s")
	assert.Contains(t, text, "5m0s")
}

func TestReport_JSONCarriesTheFieldsAConsumerQueriesOn(t *testing.T) {
	// The wire shape. Categories and trust states are keyed by *name* rather
	// than by their numeric constants precisely so that inserting a category
	// does not silently renumber every stored report — so the test asserts on
	// the names, which is the guarantee.
	oracleTTL := 90 * time.Second
	started := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	rep := differ.NewReport(started, differ.Options{})
	rep.SettlementWindow = 5 * time.Second
	rep.KeysCompared = 1200
	rep.KeysSkippedInFlight = 37
	rep.KeysSkippedSuspect = 4
	rep.EvictionSuspected = true
	rep.Add(&differ.Finding{
		Key:            "block:9f3a",
		Category:       differ.CatMissingInTarget,
		Trust:          oracle.TrustComplete,
		MissingMembers: []string{"replica-0", "replica-2"},
		OracleVersion:  7,
		LastSeq:        8847,
		LastPublisher:  "replica-0",
		LastEventAt:    started.Add(-2 * time.Second),
		OracleTTL:      &oracleTTL,
		Confirmed:      true,
	})
	rep.FinishedAt = started.Add(1500 * time.Millisecond)

	raw, err := rep.JSON()
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(raw, &got),
		"a report that does not round-trip through encoding/json is not a wire "+
			"format: %s", raw)

	assert.EqualValues(t, 1200, got["keysCompared"])
	assert.EqualValues(t, 37, got["keysSkippedInFlight"])
	assert.EqualValues(t, 1500, got["durationMs"])
	assert.Equal(t, "5s", got["settlementWindow"],
		"the window is stated because a finding means little without it: the "+
			"same disagreement is a false positive under 1s and real drift "+
			"under 60s")
	assert.Equal(t, true, got["evictionSuspected"])

	byCategory, ok := got["byCategory"].(map[string]any)
	require.True(t, ok, "byCategory should be an object keyed by name: %s", raw)
	assert.EqualValues(t, 1, byCategory["missing_in_target"],
		"categories are keyed by name, not by their numeric constant, so that "+
			"renumbering one does not rewrite the meaning of stored reports")

	findings, ok := got["findings"].([]any)
	require.True(t, ok)
	require.Len(t, findings, 1)

	f, ok := findings[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "block:9f3a", f["key"])
	assert.Equal(t, "missing_in_target", f["category"])
	assert.Equal(t, "complete", f["trust"],
		"the trust state is what separates a confirmed finding from a suspect "+
			"one, and a consumer that cannot read it cannot tell them apart")
	assert.EqualValues(t, 8847, f["lastSeq"],
		"the sequence number is the thing an operator greps the materializer's "+
			"logs for")
	assert.Equal(t, "1m30s", f["oracleTTL"])
	assert.Equal(t, true, f["confirmed"])

	// A finding with no TTL must omit the field rather than emit "0s", which a
	// consumer would read as "expires immediately".
	assert.NotContains(t, f, "targetTTL",
		"an absent TTL should be omitted, not rendered as a zero duration")
}

func TestReport_JSONOfAnEmptyReportIsStillValid(t *testing.T) {
	// The overwhelmingly common case: a healthy sweep. It has to serialize to
	// something a consumer can parse without special-casing, with the arrays
	// present and empty rather than null — a consumer iterating null crashes,
	// and the healthy path is the one that runs thousands of times a day.
	started := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	rep := differ.NewReport(started, differ.Options{})
	rep.FinishedAt = started.Add(time.Second)

	raw, err := rep.JSON()
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(raw, &got))

	findings, ok := got["findings"].([]any)
	require.True(t, ok, "findings must be an array even when empty: %s", raw)
	assert.Empty(t, findings)
	assert.EqualValues(t, 0, got["total"])
	assert.Equal(t, false, got["truncated"])
}
