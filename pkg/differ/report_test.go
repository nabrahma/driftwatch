package differ_test

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/differ"
	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/target"
)

// sampleReport builds a report with one of each interesting finding, used by
// the rendering tests so their expected output is worth reading.
func sampleReport(t *testing.T) *differ.Report {
	t.Helper()

	r := differ.NewReport(epoch, differ.Options{})
	r.FinishedAt = epoch.Add(1500 * time.Millisecond)
	r.KeysCompared = 5000
	r.KeysSkippedInFlight = 120
	r.TargetHealth = target.Health{
		Reachable: true, Version: "7.2.15", Role: "master",
		KeyspaceSize: 5120, EvictedKeys: 3,
	}

	r.Add(differ.Compare("block-9f3a",
		entry(setOf("replica-0", "replica-1")), setOf("replica-0"), differ.Options{}))
	r.Add(differ.Compare("block-1c2d",
		entry(scalarOf("expected")), scalarOf("actual"), differ.Options{}))
	r.Add(differ.Compare("block-77aa",
		entry(counterOf(1000)), counterOf(999), differ.Options{}))
	r.Add(differ.CompareUnreadable("block-bad", entry(setOf("replica-0")), "hash", differ.Options{}))

	suspect := entry(setOf("replica-2"))
	suspect.Trust = oracle.TrustSuspect
	r.Add(differ.Compare("block-susp", suspect, event.Value{}, differ.Options{}))

	return r
}

func TestReport_SummaryIsOneLineAndSaysWhatMatters(t *testing.T) {
	r := sampleReport(t)

	got := r.Summary()

	assert.NotContains(t, got, "\n", "a log line must be one line")
	assert.Contains(t, got, "5000 keys")
	assert.Contains(t, got, "5 findings")
	assert.Contains(t, got, "4 alertable")
	assert.Contains(t, got, "1 suspect")
	assert.Contains(t, got, "120 in flight")
}

func TestReport_SummaryOnACleanSweep(t *testing.T) {
	r := differ.NewReport(epoch, differ.Options{})
	r.FinishedAt = epoch.Add(time.Second)
	r.KeysCompared = 1000

	got := r.Summary()

	assert.Contains(t, got, "1000 keys")
	assert.Contains(t, got, "0 findings")
	assert.NotContains(t, got, "suspect")
}

func TestReport_TextRendersEveryFindingWithItsEvidence(t *testing.T) {
	got := sampleReport(t).Text()

	t.Log("\n" + got)

	// The header answers "what did this sweep do".
	assert.Contains(t, got, "driftwatch sweep report")
	assert.Contains(t, got, "compared   5000 keys")
	assert.Contains(t, got, "in flight  120 keys, not yet settled")

	// The counts separate what driftwatch will stand behind from what it will
	// not, which is the distinction that keeps it honest.
	assert.Contains(t, got, "findings   5 total, 4 alertable")
	assert.Contains(t, got, "1 on keys driftwatch cannot vouch for")

	// Every category present is named and counted.
	for _, want := range []string{
		"member_mismatch", "value_mismatch", "counter_mismatch",
		"type_mismatch", "missing_in_target",
	} {
		assert.Contains(t, got, want)
	}

	// Each finding carries the evidence needed to act on it.
	assert.Contains(t, got, "block-9f3a")
	assert.Contains(t, got, "missing  replica-1")
	assert.Contains(t, got, "holds    hash")
	assert.Contains(t, got, "last event")
	assert.Contains(t, got, "trust    suspect")

	// The target section explains the sweep rather than just describing it.
	assert.Contains(t, got, "version          7.2.15")
	assert.Contains(t, got, "evicted keys     3")
}

func TestReport_TextOnACleanSweepSaysSoPlainly(t *testing.T) {
	r := differ.NewReport(epoch, differ.Options{})
	r.FinishedAt = epoch.Add(time.Second)
	r.KeysCompared = 1000
	r.TargetHealth = target.Health{Reachable: true, Version: "7.2.15", KeyspaceSize: 1000}

	got := r.Text()

	assert.Contains(t, got, "no divergence found")
	assert.NotContains(t, got, "by category")
}

func TestReport_TextExplainsAnEvictionRatherThanLeavingItToBeGuessed(t *testing.T) {
	// A sweep finding mass absence at the same moment the store was evicting
	// has an obvious explanation, and saying so saves an hour of looking in the
	// wrong place (§5.7).
	r := differ.NewReport(epoch, differ.Options{})
	r.FinishedAt = epoch.Add(time.Second)
	r.EvictionSuspected = true
	r.TargetHealth = target.Health{Reachable: true, EvictedKeys: 4821, Version: "7.2.15"}
	r.Add(differ.Compare("k", entry(scalarOf("v")), event.Value{}, differ.Options{}))

	got := r.Text()

	assert.Contains(t, got, "eviction counter moved during this sweep")
	assert.Contains(t, got, "without any drift having occurred")
	assert.Contains(t, r.Summary(), "eviction suspected")
}

func TestReport_TextWarnsWhenReadsCameFromAReplica(t *testing.T) {
	// A replica can serve stale data and produce findings that resolve
	// themselves, which the operator has to be told before they go hunting.
	r := differ.NewReport(epoch, differ.Options{})
	r.FinishedAt = epoch.Add(time.Second)
	r.TargetHealth = target.Health{Reachable: true, Role: "replica", Version: "7.2.15"}

	got := r.Text()

	assert.Contains(t, got, "reads came from a replica")
	assert.Contains(t, got, "stale data")
}

func TestReport_TruncationKeepsTheMagnitudeAndSaysItDidSo(t *testing.T) {
	r := differ.NewReport(epoch, differ.Options{MaxFindings: 10})
	r.FinishedAt = epoch.Add(time.Second)

	for i := 0; i < 1000; i++ {
		r.Add(&differ.Finding{
			Key: "k" + strconv.Itoa(i), Category: differ.CatMissingInTarget,
			Trust: oracle.TrustComplete,
		})
	}

	assert.True(t, r.Truncated)
	assert.Len(t, r.Findings, 10, "the list is capped so the reporter cannot exhaust memory")
	assert.Equal(t, 1000, r.Total(), "the count is not capped")

	text := r.Text()
	assert.Contains(t, text, "findings   1000 total")
	assert.Contains(t, text, "truncated at 10; the counts above are complete")
	assert.Contains(t, r.Summary(), "list truncated at 10")
}

func TestReport_MemberDiffTruncationShowsTheRemainder(t *testing.T) {
	members := make(map[string]struct{}, 100)
	for i := 0; i < 100; i++ {
		members["replica-"+strconv.Itoa(i)] = struct{}{}
	}

	r := differ.NewReport(epoch, differ.Options{})
	r.FinishedAt = epoch.Add(time.Second)
	r.Add(differ.Compare("k",
		entry(event.Value{Kind: event.ValueSet, Members: members}), setOf(), differ.Options{MaxMembersReported: 3}))

	got := r.Text()

	assert.Contains(t, got, "and 97 more (100 total)",
		"the magnitude is the part an operator acts on")
}

func TestReport_JSONIsMachineReadableAndKeyedByName(t *testing.T) {
	raw, err := sampleReport(t).JSON()
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(raw, &decoded))

	assert.EqualValues(t, 5000, decoded["keysCompared"])
	assert.EqualValues(t, 5, decoded["total"])
	assert.EqualValues(t, 4, decoded["alertable"])
	assert.EqualValues(t, 1500, decoded["durationMs"])

	// Keyed by name rather than by the numeric constants, so the output
	// survives a category being renumbered.
	byCategory, ok := decoded["byCategory"].(map[string]any)
	require.True(t, ok)
	assert.EqualValues(t, 1, byCategory["member_mismatch"])
	assert.EqualValues(t, 1, byCategory["type_mismatch"])

	byTrust, ok := decoded["byTrust"].(map[string]any)
	require.True(t, ok)
	assert.EqualValues(t, 1, byTrust["suspect"])
	assert.EqualValues(t, 4, byTrust["complete"])

	findings, ok := decoded["findings"].([]any)
	require.True(t, ok)
	require.Len(t, findings, 5)

	first, ok := findings[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "block-9f3a", first["key"])
	assert.Equal(t, "member_mismatch", first["category"])
	assert.Equal(t, "complete", first["trust"])
}

func TestReport_JSONRendersValuesThroughTheTruncatingFormatter(t *testing.T) {
	// A report is a diagnostic, not a backup. Emitting whole values would put
	// the contents of the audited store into logs, which §18 forbids.
	secret := strings.Repeat("s", 500)

	r := differ.NewReport(epoch, differ.Options{})
	r.Add(differ.Compare("k", entry(scalarOf(secret)), scalarOf("other"), differ.Options{}))

	raw, err := r.JSON()
	require.NoError(t, err)

	assert.NotContains(t, string(raw), secret, "the whole value must not reach the output")
	assert.Contains(t, string(raw), "500B", "its length still should")
}

func TestReport_JSONOnACleanSweep(t *testing.T) {
	r := differ.NewReport(epoch, differ.Options{})
	r.FinishedAt = epoch.Add(time.Second)
	r.KeysCompared = 10

	raw, err := r.JSON()
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(raw, &decoded))
	assert.EqualValues(t, 0, decoded["total"])
	assert.Empty(t, decoded["findings"])
}

func TestReport_AddIgnoresANilFinding(t *testing.T) {
	// Compare returns nil for agreement, and the sweeper adds whatever it gets
	// rather than branching at every call site.
	r := differ.NewReport(epoch, differ.Options{})

	r.Add(nil)

	assert.Equal(t, 0, r.Total())
	assert.Empty(t, r.Findings)
}

func TestReport_DurationIsZeroUntilTheSweepFinishes(t *testing.T) {
	r := differ.NewReport(epoch, differ.Options{})

	assert.Equal(t, time.Duration(0), r.Duration())

	r.FinishedAt = epoch.Add(2 * time.Second)
	assert.Equal(t, 2*time.Second, r.Duration())
}

func TestReport_AlertableExcludesSuspectFindings(t *testing.T) {
	// The distinction that keeps driftwatch honest: it never claims the target
	// is broken while it knows its own view is incomplete (§23 A7).
	r := differ.NewReport(epoch, differ.Options{})

	for i := 0; i < 3; i++ {
		r.Add(&differ.Finding{Category: differ.CatMissingInTarget, Trust: oracle.TrustComplete})
	}
	for i := 0; i < 7; i++ {
		r.Add(&differ.Finding{Category: differ.CatMissingInTarget, Trust: oracle.TrustSuspect})
	}

	assert.Equal(t, 10, r.Total())
	assert.Equal(t, 3, r.Alertable())
}
