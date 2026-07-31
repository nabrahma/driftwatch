package check_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/check"
	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/source"
	"github.com/nabrahma/driftwatch/pkg/target"
)

// The ingest path is where §5.2 lives: the decisions about whether driftwatch
// can vouch for its own view. Every test below drives a real check through its
// Run loop, because the mechanism only exists once the pieces are wired
// together — a gap detected by seqtrack that nobody relays to the oracle is a
// gap that changes nothing.

// running starts a check and returns a stop function.
func running(t *testing.T, c *check.Check) func() {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	select {
	case <-c.Bootstrapped():
	case <-time.After(5 * time.Second):
		t.Fatal("bootstrap never completed")
	}

	return func() {
		cancel()
		require.NoError(t, <-done)
	}
}

// publish feeds payloads through the check's memory source and waits for the
// applier to have seen all of them.
func publish(t *testing.T, c *check.Check, payloads ...string) {
	t.Helper()

	src, ok := c.Source().(*source.MemorySource)
	require.True(t, ok, "the spec configures a memory source")

	before := c.Status().EventsApplied + c.Status().EventsDropped
	for _, p := range payloads {
		require.True(t, src.PublishPayload([]byte(p)))
	}

	require.Eventually(t, func() bool {
		s := c.Status()
		return s.EventsApplied+s.EventsDropped >= before+uint64(len(payloads))
	}, 5*time.Second, time.Millisecond, "the applier never drained the source")
}

func setEvent(pub string, seq uint64, key, value string) string {
	return fmt.Sprintf(
		`{"publisher":%q,"epoch":1,"seq":%d,"op":"set","key":%q,"value":%q}`,
		pub, seq, key, value)
}

func TestIngest_AnUndecodablePayloadMakesKeysSuspectAndThenLetsThemRecover(t *testing.T) {
	// A message driftwatch could not decode is a message it did not see, and it
	// has no publisher or sequence number to scope the loss with — those fields
	// were in the payload it could not read. §5.2's answer is to suspect
	// everything, and the interesting half is that the suspicion decays: a key
	// touched by a later event is trustworthy again.
	c := newCheck(t, inProcessSpec)
	stop := running(t, c)
	defer stop()

	publish(t, c, addEventJSON("replica-0", 1, "0", "replica-0"))
	assertTrust(t, c, "block:0", oracle.TrustComplete)

	publish(t, c, `{"publisher":"replica-0",,,`)

	assert.Equal(t, uint64(1), c.Status().EventsDropped)
	assert.Positive(t, c.Status().GapSignals, "an undecodable frame is possible loss")
	assertTrust(t, c, "block:0", oracle.TrustSuspect)

	publish(t, c, addEventJSON("replica-0", 2, "0", "replica-1"))
	assertTrust(t, c, "block:0", oracle.TrustComplete,
		"a later event restores trust in the key it touched")
}

func TestIngest_ASequenceGapMakesTheAffectedKeysSuspect(t *testing.T) {
	c := newCheck(t, inProcessSpec)
	stop := running(t, c)
	defer stop()

	publish(t, c,
		addEventJSON("replica-0", 1, "0", "replica-0"),
		addEventJSON("replica-0", 40, "1", "replica-0"), // seq 2..39 never arrived
	)

	assertTrust(t, c, "block:0", oracle.TrustSuspect)
	assertTrust(t, c, "block:1", oracle.TrustSuspect)

	status := c.Status()
	assert.Positive(t, status.GapSignals)
	require.Len(t, status.Publishers, 1)
	assert.Equal(t, uint64(38), status.Publishers[0].MissingEvents)
}

func TestIngest_APartitionedProjectionScopesTheSuspicionToOnePublisher(t *testing.T) {
	// The whole point of §5.2's ownership model. driftwatch cannot know which
	// keys the missing events touched, but if publishers own disjoint
	// keyspaces it knows which ones they could not have touched — and
	// suspecting those too would cost coverage for nothing.
	c := newCheck(t, `
source: {type: memory}
projection:
  type: scalar
  keyTemplate: "{{.Publisher}}:{{.Key}}"
  ownership: {partitioned: true, keyPattern: "{{.Publisher}}:*"}
target: {type: memory}
policy:
  settlementWindow: {mode: static, static: 1s, min: 1s, max: 60s}
  sweepInterval: 10s
  bootstrap: Wait
`)
	stop := running(t, c)
	defer stop()

	publish(t, c,
		setEvent("replica-0", 1, "a", "v1"),
		setEvent("replica-1", 1, "b", "v1"),
		setEvent("replica-0", 90, "c", "v1"), // replica-0 lost 2..89
	)

	assertTrust(t, c, "replica-0:a", oracle.TrustSuspect)
	assertTrust(t, c, "replica-1:b", oracle.TrustComplete,
		"replica-1 owns a disjoint partition, so its keys keep their trust")
}

func TestIngest_DropsAreCountedByReasonRatherThanApplied(t *testing.T) {
	tests := []struct {
		name    string
		stream  []string
		applied uint64
		dropped uint64
	}{
		{
			name: "a duplicate is absorbed silently",
			stream: []string{
				setEvent("replica-0", 1, "a", "v1"),
				setEvent("replica-0", 1, "a", "v1"),
			},
			applied: 1,
			dropped: 1,
		},
		{
			name: "an event from a superseded epoch is refused",
			stream: []string{
				`{"publisher":"replica-0","epoch":3,"seq":10,"op":"set","key":"a","value":"v1"}`,
				`{"publisher":"replica-0","epoch":1,"seq":11,"op":"set","key":"a","value":"stale"}`,
			},
			applied: 1,
			dropped: 1,
		},
		{
			name: "an event with no publisher cannot be sequence-tracked",
			stream: []string{
				`{"epoch":1,"seq":1,"op":"set","key":"a","value":"v1"}`,
			},
			applied: 0,
			dropped: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newCheck(t, scalarSpec)
			stop := running(t, c)
			defer stop()

			publish(t, c, tc.stream...)

			status := c.Status()
			assert.Equal(t, tc.applied, status.EventsApplied)
			assert.Equal(t, tc.dropped, status.EventsDropped)
		})
	}
}

func TestIngest_AProjectionErrorIsCountedRatherThanFatal(t *testing.T) {
	// One event the projection cannot fold must not stop the check: a producer
	// that emits one wrong-shaped message would otherwise take down the audit
	// of everything else it emits.
	c := newCheck(t, scalarSpec)
	stop := running(t, c)
	defer stop()

	publish(t, c,
		setEvent("replica-0", 1, "a", "v1"),
		// An `add` against a scalar projection: the shape does not fit.
		`{"publisher":"replica-0","epoch":1,"seq":2,"op":"add","key":"a","member":"m"}`,
		setEvent("replica-0", 3, "b", "v1"),
	)

	assert.Equal(t, 2, c.Status().TrackedKeys, "the events either side were still applied")
}

func TestIngest_ARestartMakesKeysSuspect(t *testing.T) {
	c := newCheck(t, scalarSpec)
	stop := running(t, c)
	defer stop()

	publish(t, c,
		setEvent("replica-0", 1, "a", "v1"),
		`{"publisher":"replica-0","epoch":2,"seq":1,"op":"set","key":"b","value":"v1"}`,
	)

	assertTrust(t, c, "a", oracle.TrustSuspect,
		"anything in flight at the moment of a restart is unaccounted for")
	require.Len(t, c.Status().Publishers, 1)
	assert.Equal(t, uint64(2), c.Status().Publishers[0].Epoch)
}

// TestIngest_ASourceGapSignalMarksEverythingSuspect exercises the wire §9 M14
// calls out explicitly.
//
// Forgetting it is silent: everything keeps working, the reconnect is counted,
// and driftwatch goes on asserting at full confidence about keys whose events
// it missed while the socket was down. So the test uses a source that really
// signals, registered here rather than mocked, to prove the relay exists.
func TestIngest_ASourceGapSignalMarksEverythingSuspect(t *testing.T) {
	c := newCheck(t, strings.Replace(scalarSpec, "type: memory", "type: "+gappySourceName, 1))
	stop := running(t, c)
	defer stop()

	src, ok := c.Source().(*gappySource)
	require.True(t, ok)

	src.publish(setEvent("replica-0", 1, "a", "v1"))
	require.Eventually(t, func() bool { return c.Status().EventsApplied == 1 },
		5*time.Second, time.Millisecond)
	assertTrust(t, c, "a", oracle.TrustComplete)

	src.signalGap()

	require.Eventually(t, func() bool { return c.Status().GapSignals > 0 },
		5*time.Second, time.Millisecond, "the gap signal never reached the pipeline")
	assertTrust(t, c, "a", oracle.TrustSuspect)
}

func TestCheck_ScanExtrasReportsOnlyWhatSurvivesBothPasses(t *testing.T) {
	// §5.5: a scan of a live keyspace is not atomic, so one pass proves
	// nothing. Only a key present in both passes, a settlement window apart, is
	// an extra rather than a race with an event still in flight.
	clk := clock.Fake(epoch())
	c := newCheckWith(t, scalarSpec, clk)
	stop := running(t, c)
	defer stop()

	store, ok := c.Target().(*target.MemoryTarget)
	require.True(t, ok, "the spec configures a memory target")
	store.Seed(map[string][]byte{"orphan": []byte("nobody wrote me")})

	first, err := c.ScanExtras(context.Background())
	require.NoError(t, err)
	assert.Zero(t, first.Total(), "one pass is a candidate, not a finding")

	clk.Advance(3 * time.Second)

	second, err := c.ScanExtras(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, second.Total(), "the key survived both passes")
}

func TestCheck_ConfirmDueDrainsTheQueue(t *testing.T) {
	clk := clock.Fake(epoch())
	c := newCheckWith(t, scalarSpec, clk)
	stop := running(t, c)
	defer stop()

	publish(t, c, setEvent("replica-0", 1, "a", "v1"))
	clk.Advance(3 * time.Second)

	_, err := c.SweepNow(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, c.Sweeper().PendingConfirmations())

	// ConfirmDue is the out-of-band entry point `driftwatch diff` uses so a CI
	// run completes a two-phase cycle without waiting for the sweeper's own
	// timer. The assertion is on the outcome rather than the return value,
	// because the running sweeper's confirm ticker may legitimately get there
	// first — both paths lead to the same place, which is the point.
	clk.Advance(3 * time.Second)
	c.ConfirmDue(context.Background())

	require.Eventually(t, func() bool { return len(c.Sweeper().Confirmed()) == 1 },
		5*time.Second, time.Millisecond, "the candidate was never confirmed")
}

func TestCheck_ExposesTheEffectiveSpecAndOracle(t *testing.T) {
	// Both are load-bearing rather than conveniences: `driftwatch replay
	// --dump-oracle` serializes the oracle, and the CRD status renders the
	// effective spec so an operator can see what is actually running.
	c := newCheck(t, scalarSpec)
	defer func() { require.NoError(t, c.Close()) }()

	spec := c.Spec()
	assert.Equal(t, check.DefaultMaxTrackedKeys, spec.Policy.MaxTrackedKeys,
		"the spec handed back is the defaulted one, not the sparse file")
	assert.NotNil(t, c.Oracle())
	assert.Zero(t, c.Oracle().Len())
}

// scalarSpec is the simplest complete configuration: no key template, so the
// store key and the event key are the same string.
const scalarSpec = `
source: {type: memory}
projection: {type: scalar}
target: {type: memory}
policy:
  settlementWindow: {mode: static, static: 1s, min: 1s, max: 60s}
  sweepInterval: 10s
  bootstrap: Wait
`

func addEventJSON(pub string, seq uint64, block, member string) string {
	return fmt.Sprintf(
		`{"publisher":%q,"epoch":1,"seq":%d,"op":"add","key":%q,"member":%q}`,
		pub, seq, block, member)
}

func assertTrust(t *testing.T, c *check.Check, key string, want oracle.TrustState, msg ...any) {
	t.Helper()

	entry, ok := c.Oracle().Get(key)
	require.True(t, ok, "the oracle has no entry for %q", key)
	assert.Equal(t, want, entry.Trust, msg...)
}

// ---------------------------------------------------------------------------
// A source that signals possible loss, so the relay can be tested end to end.
// ---------------------------------------------------------------------------

const gappySourceName = "test_gappy"

func init() { source.Register(gappySourceName, newGappySource) }

// gappySource is a memory source that can also report a gap, which is what a
// reconnecting transport does and what pkg/source's memory source deliberately
// cannot: a source that never loses anything has no business claiming it might.
type gappySource struct {
	clk  clock.Clock
	gaps chan source.GapSignal

	mu      sync.Mutex
	pending [][]byte
	out     chan<- source.RawMessage
}

func newGappySource(_ source.Config, clk clock.Clock) (source.Source, error) {
	return &gappySource{clk: clk, gaps: make(chan source.GapSignal, 8)}, nil
}

func (g *gappySource) Name() string { return gappySourceName }

func (g *gappySource) Run(ctx context.Context, out chan<- source.RawMessage) error {
	g.mu.Lock()
	g.out = out
	queued := g.pending
	g.pending = nil
	g.mu.Unlock()

	for _, payload := range queued {
		g.deliver(ctx, payload)
	}

	<-ctx.Done()
	return ctx.Err()
}

func (g *gappySource) deliver(ctx context.Context, payload []byte) {
	select {
	case g.out <- source.RawMessage{Payload: payload, ObservedAt: g.clk.Now()}:
	case <-ctx.Done():
	}
}

func (g *gappySource) publish(payload string) {
	g.mu.Lock()
	out := g.out
	if out == nil {
		g.pending = append(g.pending, []byte(payload))
	}
	g.mu.Unlock()

	if out != nil {
		out <- source.RawMessage{Payload: []byte(payload), ObservedAt: g.clk.Now()}
	}
}

func (g *gappySource) signalGap() {
	g.gaps <- source.GapSignal{
		Source: gappySourceName,
		Reason: source.GapReconnect,
		At:     g.clk.Now(),
		Detail: "the socket was down for an unknown interval",
	}
}

func (g *gappySource) Gaps() <-chan source.GapSignal { return g.gaps }

func (g *gappySource) Stats() source.Stats { return source.Stats{Connected: true} }

func (g *gappySource) Close() error { return nil }
