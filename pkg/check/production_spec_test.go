package check_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/check"
)

// A spec shaped like one an operator would actually deploy: a real transport, a
// real store, an adaptive window and a password.
//
// Everything else in this package builds checks over memory sources and memory
// targets, which is right for testing behavior and wrong for testing the
// construction paths those specs never take — the Redis batch size, the
// transport-sized ingest buffer, the redaction that keeps a password out of the
// logs.
const productionSpec = `
name: kvcache
namespace: inference
source:
  type: zmq
  zmq:
    endpoints: ["tcp://vllm-0.vllm.inference.svc.cluster.local:5557"]
    topics: ["kv-events"]
  ingestBufferSize: 131072
codec:
  type: json
projection:
  type: keysetOwnership
  keyTemplate: "block:{{.Key}}"
target:
  type: redis
  redis:
    addr: redis.inference.svc.cluster.local:6379
    keyPattern: "block:*"
    password: "hunter2"
    readBatchSize: 250
policy:
  settlementWindow: {mode: adaptive, min: 2s, max: 45s, safetyFactor: 1.5}
  sweepInterval: 30s
  extraScanInterval: 5m
  bootstrap: Wait
`

func TestProductionSpec_BuildsWithoutTouchingTheNetwork(t *testing.T) {
	// Construction must not dial. A check whose New() connected would make the
	// controller's reconcile block on an unreachable store, and a DriftCheck
	// pointed at a store that is down would fail to be created at all rather
	// than being created and reporting the store as unreachable — which is the
	// whole point of having a status.
	spec, err := check.Load(strings.NewReader(productionSpec))
	require.NoError(t, err)

	c, err := check.New(spec, check.Deps{})
	require.NoError(t, err, "New must not require the endpoints to be reachable")
	t.Cleanup(func() { require.NoError(t, c.Close()) })

	assert.Equal(t, "zmq", c.Source().Name())
	assert.Equal(t, "redis", c.Target().Name())
}

func TestProductionSpec_TheEffectiveConfigDumpRedactsThePassword(t *testing.T) {
	// §12.3. The effective config is logged once at startup because half of all
	// "driftwatch is reporting nonsense" reports are a keyTemplate or a
	// keyPattern that is not what the operator thought they configured, and one
	// line answers that without a round trip.
	//
	// It is also the single most likely place for a credential to end up in a
	// log aggregator, which is why the redaction is tested rather than assumed.
	spec, err := check.Load(strings.NewReader(productionSpec))
	require.NoError(t, err)

	dump := spec.YAML()

	assert.NotContains(t, dump, "hunter2",
		"the password reached the effective-config dump, which is written to "+
			"the log at startup:\n%s", dump)
	assert.Contains(t, dump, "block:{{.Key}}",
		"the keyTemplate must survive — it is the field this dump exists for")
	assert.Contains(t, dump, "block:*",
		"and so must the keyPattern")
}

func TestProductionSpec_DefaultsAreFilledAndVisible(t *testing.T) {
	// The defaulting contract: every optional field is filled in and written
	// back, so `kubectl get driftcheck -o yaml` shows the whole effective
	// configuration rather than the eight lines the operator typed. A default
	// that stays implicit is a default nobody can see they are relying on.
	minimal := `
source:
  type: zmq
  zmq:
    endpoints: ["tcp://vllm-0.vllm.inference.svc.cluster.local:5557"]
projection:
  type: keysetOwnership
  keyTemplate: "block:{{.Key}}"
target:
  type: redis
  redis:
    addr: redis.inference.svc.cluster.local:6379
    keyPattern: "block:*"
`
	spec, err := check.Load(strings.NewReader(minimal))
	require.NoError(t, err)

	assert.Equal(t, "json", spec.Codec.Type, "the codec should default")
	assert.Positive(t, spec.Policy.SweepInterval.Duration(),
		"the sweep interval should default rather than being zero, which "+
			"would mean 'sweep continuously'")
	assert.Positive(t, spec.Target.Redis.ReadBatchSize,
		"an unset batch size would mean reading one key per round trip")
	assert.NotEmpty(t, spec.Policy.Bootstrap,
		"bootstrap has no safe zero value; §5.6's three modes behave "+
			"differently enough that guessing is not an option")
}

func TestProductionSpec_AnAdaptiveWindowStartsAtItsFloor(t *testing.T) {
	// Before any convergence has been measured there is no p99 to derive W
	// from, and the only defensible starting value is the configured minimum.
	// Starting at zero would compare every key the instant it was written;
	// starting at the maximum would delay the first real finding by 45s for no
	// reason.
	spec, err := check.Load(strings.NewReader(productionSpec))
	require.NoError(t, err)

	c, err := check.New(spec, check.Deps{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })

	status := c.Status()
	assert.InDelta(t, 2.0, status.SettlementWindowSeconds, 0.001,
		"an adaptive window with no observations yet should sit at its floor")
	assert.False(t, status.WindowIsAdaptive,
		"it is configured adaptive but not yet driven by measurement, and the "+
			"status should say which of the two is true right now")
}
