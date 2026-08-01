package check_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/nabrahma/driftwatch/pkg/check"
	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/differ"
	"github.com/nabrahma/driftwatch/pkg/explain"
	"github.com/nabrahma/driftwatch/pkg/metrics"
	"github.com/nabrahma/driftwatch/pkg/source"
	"github.com/nabrahma/driftwatch/pkg/target"
)

func TestMain(m *testing.M) {
	// Every entry is a third-party goroutine started at package init and never
	// stopped. No ignore here is ever for one of driftwatch's own goroutines —
	// one of those is a bug to fix, not an entry to add.
	goleak.VerifyTestMain(m,
		goleak.IgnoreAnyFunction("github.com/redis/go-redis/v9/internal/pool.startGlobalTimeCache.func1"),
	)
}

func epoch() time.Time { return time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC) }

// inProcessSpec is the whole system with nothing outside the process: a memory
// source, a memory target and a static settlement window.
const inProcessSpec = `
name: kvcache-index
namespace: inference
source:
  type: memory
codec:
  type: json
projection:
  type: keysetOwnership
  keyTemplate: "block:{{.Key}}"
target:
  type: memory
policy:
  settlementWindow: {mode: static, static: 2s}
  sweepInterval: 10s
  extraScanInterval: 1m
  bootstrap: Wait
`

// TestCheck_EndToEnd_InProcess is the flagship composition test (§9 M14).
//
// It drives the whole pipeline — source, codec, sequence tracking, projection,
// oracle, sweeper, two-phase confirmation, metrics — through a complete drift
// lifecycle, with a fake clock and no real time elapsed anywhere. Every other
// test in this repository proves one module behaves; this one is the only proof
// that they compose, which is a different claim and the one most likely to be
// false.
//
// The scenario is the story the tool exists to tell:
//
//	events arrive        the oracle learns what the store should hold
//	sweep                clean, because the materializer kept up
//	drift injected       a key is removed from the store behind driftwatch's back
//	sweep                a candidate is raised, not a finding
//	wait W, confirm      the re-read still disagrees, so it becomes a finding
//	repair               the key is put back
//	sweep                the finding is withdrawn and drift_resolved_total moves
func TestCheck_EndToEnd_InProcess(t *testing.T) {
	clk := clock.Fake(epoch())
	reg := prometheus.NewRegistry()
	met := metrics.New(metrics.Options{Registry: reg})

	spec, err := check.Load(strings.NewReader(inProcessSpec))
	require.NoError(t, err)

	c, err := check.New(spec, check.Deps{Clock: clk, Metrics: met})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })

	ctx, cancel := context.WithCancel(context.Background())
	running := make(chan error, 1)
	go func() { running <- c.Run(ctx) }()

	src, ok := c.Source().(*source.MemorySource)
	require.True(t, ok, "the spec configures a memory source")
	store, ok := c.Target().(*target.MemoryTarget)
	require.True(t, ok, "the spec configures a memory target")

	// ---- events arrive -------------------------------------------------
	//
	// Three blocks, each held by two replicas: the KV-cache index shape from
	// §0.2, which is the case the whole tool is built around.
	const blocks = 3
	seq := uint64(0)
	for b := 0; b < blocks; b++ {
		for _, replica := range []string{"replica-0", "replica-1"} {
			seq++
			require.True(t, src.PublishPayload(addEvent("replica-0", seq, b, replica)))
		}
	}

	require.Eventually(t, func() bool { return c.EventsApplied() == uint64(blocks*2) },
		5*time.Second, time.Millisecond, "the applier never drained the source")

	status := c.Status()
	assert.Equal(t, blocks, status.TrackedKeys)
	assert.Equal(t, uint64(6), status.EventsApplied)
	assert.Zero(t, status.EventsDropped)

	// ---- the materializer keeps up, so a sweep is clean ----------------
	//
	// Writing through the store's own fixture path rather than through
	// driftwatch: driftwatch never writes to what it audits, and the read-only
	// enforcement in pkg/target would fail this test if it tried.
	for b := 0; b < blocks; b++ {
		store.SeedSets(map[string][]string{
			blockKey(b): {"replica-0", "replica-1"},
		})
	}

	clk.Advance(3 * time.Second) // past the 2s settlement window

	report, err := c.SweepNow(ctx)
	require.NoError(t, err)
	assert.Equal(t, blocks, report.KeysCompared)
	assert.Zero(t, report.Total(), "a healthy pipeline produces no findings: %s", report.Summary())
	assert.Empty(t, c.Sweeper().Confirmed())

	// ---- drift is injected ---------------------------------------------
	//
	// One block disappears from the store with no event to explain it. This is
	// exactly the silent divergence §0.1 describes: nothing errors, nothing
	// logs, and every existing monitor stays green.
	lost := blockKey(1)
	store.Remove(lost)

	// ---- the first sweep raises a candidate, not a finding --------------
	//
	// The distinction is the whole of §5.4. One disagreeing read is not
	// evidence: it could be a slow materializer, an unlucky moment in a
	// non-atomic write, or driftwatch's own read racing an update.
	report, err = c.SweepNow(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, report.Total(), "the missing key should have been noticed")
	assert.Equal(t, 1, report.ByCategory[differ.CatMissingInTarget])
	assert.Empty(t, c.Sweeper().Confirmed(),
		"one disagreeing read is a candidate; confirming it here would be the false "+
			"positive every mechanism in §5 exists to prevent")
	assert.Equal(t, 1, c.Sweeper().PendingConfirmations())

	// ---- a settlement window later, the re-read confirms it -------------
	//
	// Nothing here calls the confirmation directly: advancing the clock fires
	// the sweeper's own confirm ticker inside Run, which is the path that runs
	// in production. The candidate becomes a finding because a second read,
	// taken a settlement window after the first, still disagreed.
	clk.Advance(3 * time.Second)

	require.Eventually(t, func() bool { return len(c.Sweeper().Confirmed()) == 1 },
		5*time.Second, time.Millisecond,
		"the sweeper's confirm cycle never promoted the candidate")

	confirmed := c.Sweeper().Confirmed()
	finding := confirmed[lost]
	assert.Equal(t, differ.CatMissingInTarget, finding.Category)
	assert.True(t, finding.Confirmed)

	// ---- explain answers the question the gauge cannot ------------------
	exp, err := c.Explain(ctx, lost)
	require.NoError(t, err)
	assert.Equal(t, explain.VerdictDiverged, exp.Verdict)
	assert.True(t, exp.Has(explain.CodeMissingInTargetNoGaps),
		"driftwatch saw a complete sequence, so it can name the materializer: %v", exp.Diagnosis)
	assert.Contains(t, exp.Text(), "no gaps")

	// The status block, which is what the CRD and the CLI status line render.
	status = c.Status()
	assert.Equal(t, 1, status.DivergentKeys)
	assert.Equal(t, 1, status.DivergenceByCategory["missing_in_target"])
	assert.Zero(t, status.SuspectDivergentKeys, "driftwatch missed no events here")

	_, err = c.SweepNow(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, valueOf(t, reg, "driftwatch_drift_episodes_total"),
		"a confirmed divergence is one episode")
	assert.Equal(t, 1, valueOf(t, reg, "driftwatch_divergent_keys"))

	// ---- the operator repairs the store, and the claim is withdrawn -----
	//
	// A confirmed finding is a claim about one oracle version. Without this
	// step it would sit on the dashboard forever and the alert would never
	// clear, which is how a detector stops being believed.
	store.SeedSets(map[string][]string{lost: {"replica-0", "replica-1"}})
	clk.Advance(3 * time.Second)

	report, err = c.SweepNow(ctx)
	require.NoError(t, err)
	assert.Zero(t, report.Total(), "the store agrees again: %s", report.Summary())
	assert.Empty(t, c.Sweeper().Confirmed(), "the finding must be withdrawn, not just stop recurring")
	assert.Equal(t, int64(1), c.Sweeper().Stats().DriftResolved)
	assert.Equal(t, 1, valueOf(t, reg, "driftwatch_drift_resolved_total"))
	assert.Equal(t, 0, valueOf(t, reg, "driftwatch_divergent_keys"),
		"the gauge has to fall back to zero or the alert never clears")

	assert.Zero(t, c.Status().DivergentKeys)

	// ---- and none of it took any real time -----------------------------
	cancel()
	select {
	case err := <-running:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}

	assert.Equal(t, 9*time.Second, clk.Now().Sub(epoch()),
		"the whole lifecycle ran on the fake clock: nine simulated seconds, no real sleeps")
}

// addEvent renders one `add` event in the canonical driftwatch JSON format.
func addEvent(publisher string, seq uint64, block int, member string) []byte {
	return []byte(fmt.Sprintf(
		`{"publisher":%q,"epoch":1,"seq":%d,"op":"add","key":"%d","member":%q}`,
		publisher, seq, block, member))
}

// blockKey is the store key the keyTemplate produces for a block.
func blockKey(block int) string { return fmt.Sprintf("block:%d", block) }

// valueOf sums a metric family's samples, so an assertion does not have to name
// every label combination.
func valueOf(t *testing.T, reg *prometheus.Registry, name string) int {
	t.Helper()

	families, err := reg.Gather()
	require.NoError(t, err)

	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		total := 0.0
		for _, m := range mf.GetMetric() {
			total += sampleValue(m)
		}
		return int(total)
	}
	return 0
}

func sampleValue(m *dto.Metric) float64 {
	switch {
	case m.GetCounter() != nil:
		return m.GetCounter().GetValue()
	case m.GetGauge() != nil:
		return m.GetGauge().GetValue()
	default:
		return 0
	}
}

// TestCheck_ExtrasScanDoesNotClobberSweepMetrics is a regression test for a
// defect the whole unit suite missed and thirty seconds of watching the demo's
// dashboard found.
//
// Both halves of §5.5's comparison reach the metrics through one callback. The
// extras scan walks the *store* looking for keys no event produced, so its
// KeysCompared has nothing to do with how much of the oracle was verified — and
// coverage was being recomputed from it anyway. Every extraScanInterval the
// coverage gauge dropped to zero and came back, which on the dashboard is the
// one panel that exists to stop a zero-divergence verdict from overstating
// itself, flashing red on a timer.
//
// The same fall-through counted each extras scan as an oracle_to_target sweep
// and mixed its duration into that histogram, so `sweeps_total` and the sweep
// duration p99 were both measuring two different operations at once.
func TestCheck_ExtrasScanDoesNotClobberSweepMetrics(t *testing.T) {
	clk := clock.Fake(epoch())
	reg := prometheus.NewRegistry()
	met := metrics.New(metrics.Options{Registry: reg})

	spec, err := check.Load(strings.NewReader(inProcessSpec))
	require.NoError(t, err)

	c, err := check.New(spec, check.Deps{Clock: clk, Metrics: met})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })

	ctx := context.Background()
	store, ok := c.Target().(*target.MemoryTarget)
	require.True(t, ok, "the spec configures a memory target")

	// A keyspace both sides agree on, settled and swept, so coverage is real.
	for b := 0; b < 4; b++ {
		msg := source.RawMessage{
			Payload:    addEvent("replica-0", uint64(b+1), b, "replica-0"),
			ObservedAt: clk.Now(),
		}
		c.Ingest(msg)
		store.Seed(map[string][]byte{blockKey(b): []byte("replica-0")})
	}

	clk.Advance(30 * time.Second)
	_, err = c.SweepNow(ctx)
	require.NoError(t, err)

	coverageAfterSweep := gaugeValue(t, reg, "driftwatch_coverage_ratio")
	require.Positive(t, coverageAfterSweep,
		"the sweep compared keys, so coverage must be above zero")

	sweepsBefore := valueOf(t, reg, "driftwatch_sweeps_total")

	// Now the other half of the comparison, exactly as the Run loop drives it.
	_, err = c.ScanExtras(ctx)
	require.NoError(t, err)

	assert.InDelta(t, coverageAfterSweep,
		gaugeValue(t, reg, "driftwatch_coverage_ratio"), 1e-9,
		"the extras scan walks the store, not the oracle: it knows nothing "+
			"about coverage and must leave the gauge where the last sweep put it")

	assert.Equal(t, sweepsBefore+1, valueOf(t, reg, "driftwatch_sweeps_total"),
		"the scan is counted once")
	assert.Equal(t, 1, labelledValue(t, reg, "driftwatch_sweeps_total",
		"kind", "target_to_oracle"),
		"and counted as what it is")
	assert.Equal(t, 1, labelledValue(t, reg, "driftwatch_sweeps_total",
		"kind", "oracle_to_target"),
		"the sweep count is untouched by it")
}

// gaugeValue reads a single gauge, summed across labels.
func gaugeValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()

	families, err := reg.Gather()
	require.NoError(t, err)

	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		total := 0.0
		for _, m := range mf.GetMetric() {
			total += sampleValue(m)
		}
		return total
	}
	return 0
}

// labelledValue sums the samples carrying one label value.
func labelledValue(t *testing.T, reg *prometheus.Registry, name, label, value string) int {
	t.Helper()

	families, err := reg.Gather()
	require.NoError(t, err)

	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		total := 0.0
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == label && lp.GetValue() == value {
					total += sampleValue(m)
				}
			}
		}
		return int(total)
	}
	return 0
}
