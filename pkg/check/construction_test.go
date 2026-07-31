package check_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/check"
	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/target"
)

// TestCheck_ConstructionTable is §9 M14's requirement that a misconfigured
// check fails at construction with a precise error naming the offending field.
//
// The alternative is what makes it worth a table of its own: a check that
// fails halfway up has already opened a socket and started a goroutine, and
// unwinding that from an error path is code nobody exercises until an operator
// hits it during an incident.
func TestCheck_ConstructionTable(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name:    "an unregistered source",
			yaml:    "source: {type: kafka}",
			wantErr: "source.type",
		},
		{
			name:    "an unregistered codec",
			yaml:    "source: {type: memory}\ncodec: {type: avro}",
			wantErr: "codec.type",
		},
		{
			name:    "an unregistered projection",
			yaml:    "source: {type: memory}\nprojection: {type: bitmap}",
			wantErr: "projection.type",
		},
		{
			name:    "an unregistered target",
			yaml:    "source: {type: memory}\ntarget: {type: dynamodb}",
			wantErr: "target.type",
		},
		{
			name: "a key template that does not parse",
			yaml: `source: {type: memory}
projection: {type: scalar, keyTemplate: "block:{{.Nope}}"}`,
			wantErr: "projection",
		},
		{
			name: "an ingest buffer below the socket high-water mark",
			yaml: `source:
  type: zmq
  ingestBufferSize: 100
  zmq: {endpoints: ["tcp://a:5557"], recvHWM: 100000}`,
			wantErr: "invisibly in the socket",
		},
		{
			name: "a nats queue group",
			yaml: `source:
  type: nats
  nats: {url: "nats://n:4222", subjects: ["a.>"], queueGroup: g}`,
			wantErr: "corrupt the oracle",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spec, err := check.Load(strings.NewReader(tc.yaml))
			require.NoError(t, err)

			c, err := check.New(spec, check.Deps{Clock: clock.Fake(epoch())})

			require.Error(t, err)
			assert.ErrorIs(t, err, check.ErrInvalidSpec)
			assert.Contains(t, err.Error(), tc.wantErr)
			assert.Nil(t, c, "a rejected spec must not hand back a half-built check")
		})
	}
}

func TestCheck_ListsTheValidNamesWhenOneIsWrong(t *testing.T) {
	// An error that says a name is unknown without saying what is known makes
	// the operator go and read the source.
	spec, err := check.Load(strings.NewReader("source: {type: kafka}"))
	require.NoError(t, err)

	_, err = check.New(spec, check.Deps{})

	assert.Contains(t, err.Error(), "memory")
	assert.Contains(t, err.Error(), "zmq")
}

func TestCheck_CloseIsIdempotent(t *testing.T) {
	c := newCheck(t, inProcessSpec)

	require.NoError(t, c.Close())
	require.NoError(t, c.Close(), "Close is called from a defer and from a shutdown path")
}

func TestCheck_RunAfterCloseIsRefused(t *testing.T) {
	c := newCheck(t, inProcessSpec)
	require.NoError(t, c.Close())

	assert.ErrorIs(t, c.Run(context.Background()), check.ErrClosed)
	_, err := c.SweepNow(context.Background())
	assert.ErrorIs(t, err, check.ErrClosed)
}

func TestCheck_AnUnreachableSourceDegradesRatherThanFailing(t *testing.T) {
	// §9 M14's edge case, and the one most likely to be got wrong. The endpoint
	// may come up in thirty seconds; a check that refuses to start because a
	// dependency was slow is a check operators delete from their manifests.
	clk := clock.Fake(epoch())
	c := newCheckWith(t, `
source:
  type: file
  file: {path: /nonexistent/capture.ndjson}
target: {type: memory}
policy:
  settlementWindow: {mode: static, static: 2s}
  sweepInterval: 10s
  bootstrap: Wait
`, clk)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	require.Eventually(t, func() bool { return c.Status().Phase == check.PhaseDegraded },
		5*time.Second, time.Millisecond, "an unreadable source should degrade the check")

	assert.Contains(t, c.Status().Message, "capture.ndjson",
		"the status says which dependency is missing")

	cancel()
	require.NoError(t, <-done)
}

func TestCheck_AdoptBootstrapReadsTheKeyspaceAsABaseline(t *testing.T) {
	// §5.6 Adopt: keys that predate the subscription are read in and marked
	// advisory, so they are not reported as extras forever.
	clk := clock.Fake(epoch())
	c := newCheckWith(t, `
source: {type: memory}
projection: {type: scalar}
target: {type: memory}
policy:
  settlementWindow: {mode: static, static: 2s}
  sweepInterval: 10s
  bootstrap: Adopt
`, clk)

	store, ok := c.Target().(*target.MemoryTarget)
	require.True(t, ok, "the spec configures a memory target")
	store.Seed(map[string][]byte{"pre:1": []byte("a"), "pre:2": []byte("b")})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	select {
	case <-c.Bootstrapped():
	case <-time.After(5 * time.Second):
		t.Fatal("bootstrap never completed")
	}

	status := c.Status()
	assert.Equal(t, 2, status.TrackedKeys)
	assert.Equal(t, 2, status.AdoptedKeys,
		"adopted keys are advisory: they were read from the target, so comparing "+
			"one against the target proves only that the target agrees with itself")

	cancel()
	require.NoError(t, <-done)
}

func TestCheck_WaitBootstrapStartsWithAnEmptyOracle(t *testing.T) {
	clk := clock.Fake(epoch())
	c := newCheckWith(t, inProcessSpec, clk)

	store, ok := c.Target().(*target.MemoryTarget)
	require.True(t, ok, "the spec configures a memory target")
	store.Seed(map[string][]byte{"pre:1": []byte("a")})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	select {
	case <-c.Bootstrapped():
	case <-time.After(5 * time.Second):
		t.Fatal("bootstrap never completed")
	}

	assert.Zero(t, c.Status().TrackedKeys,
		"Wait means the oracle only learns keys events tell it about")

	cancel()
	require.NoError(t, <-done)
}

func TestCheck_ASaturatedOracleDegradesAndSaysHowMuchItMissed(t *testing.T) {
	// A store with more keys than maxTrackedKeys is not an error and must not
	// be treated as one — a check that refuses to start on a large keyspace is
	// never used on the systems that need it. It adopts what fits and states
	// the shortfall, because a clean report over 1% of a keyspace is not clean.
	clk := clock.Fake(epoch())
	c := newCheckWith(t, `
source: {type: memory}
projection: {type: scalar}
target: {type: memory}
policy:
  settlementWindow: {mode: static, static: 2s}
  sweepInterval: 10s
  bootstrap: Adopt
  maxTrackedKeys: 1000
`, clk)

	store, ok := c.Target().(*target.MemoryTarget)
	require.True(t, ok, "the spec configures a memory target")
	seed := make(map[string][]byte, 2500)
	for i := 0; i < 2500; i++ {
		seed["key:"+itoa(i)] = []byte("v")
	}
	store.Seed(seed)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	select {
	case <-c.Bootstrapped():
	case <-time.After(10 * time.Second):
		t.Fatal("bootstrap never completed")
	}

	status := c.Status()
	assert.Equal(t, check.PhaseDegraded, status.Phase)
	assert.Contains(t, status.Message, "oracle saturated")
	assert.LessOrEqual(t, status.TrackedKeys, 1000)

	cancel()
	require.NoError(t, <-done)
}

func TestCheck_StatusSummaryReadsAsAStatusLine(t *testing.T) {
	c := newCheck(t, inProcessSpec)
	defer func() { require.NoError(t, c.Close()) }()

	status := c.Status()
	summary := status.Summary()

	assert.Contains(t, summary, "keys 0")
	assert.Contains(t, summary, "drift 0")
	assert.Contains(t, summary, "W 2.0s")
	assert.NotContains(t, summary, "\n", "a status line is one line")
}

// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

func newCheck(t *testing.T, yaml string) *check.Check {
	t.Helper()
	return newCheckWith(t, yaml, clock.Fake(epoch()))
}

func newCheckWith(t *testing.T, yaml string, clk clock.Clock) *check.Check {
	t.Helper()

	spec, err := check.Load(strings.NewReader(yaml))
	require.NoError(t, err)

	c, err := check.New(spec, check.Deps{Clock: clk})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })

	return c
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [12]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}
