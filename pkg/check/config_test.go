package check_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/check"
)

// minimalSpec is a valid configuration every test can start from and break in
// exactly one place, so a failure names one field rather than a list.
const minimalSpec = `
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
  bootstrap: Wait
`

func load(t *testing.T, yaml string) check.Spec {
	t.Helper()

	spec, err := check.Load(strings.NewReader(yaml))
	require.NoError(t, err)
	return spec
}

func TestLoad_ReadsTheMinimalConfigFromTheSpec(t *testing.T) {
	spec := load(t, minimalSpec)

	assert.Equal(t, "inference/kvcache-index", spec.ID())
	assert.Equal(t, "memory", spec.Source.Type)
	assert.Equal(t, "keysetOwnership", spec.Projection.Type)
	assert.Equal(t, 2*time.Second, spec.Policy.SettlementWindow.Static.Duration())
	assert.Equal(t, 10*time.Second, spec.Policy.SweepInterval.Duration())
	require.NoError(t, spec.Validate())
}

func TestLoad_ReadsTheCommittedExample(t *testing.T) {
	// examples/local.yaml is what the README tells a reader to run. If it stops
	// parsing, the first thing anyone tries with this tool fails.
	spec, err := check.LoadFile("../../examples/local.yaml")

	require.NoError(t, err)
	require.NoError(t, spec.Validate())
	assert.Equal(t, "redis", spec.Target.Type)
	assert.Equal(t, "localhost:6379", spec.Target.Redis.Addr)
	assert.Equal(t, "block:*", spec.ExtraScanPattern())
	assert.Empty(t, spec.Warnings(), "the example configuration should be exemplary")
}

func TestLoad_RejectsAnUnknownField(t *testing.T) {
	// A typo in a policy key that silently kept the default means an operator
	// believing they raised a bound that is still where it was.
	_, err := check.Load(strings.NewReader(`
source: {type: memory}
policy:
  sweepIntervl: 10s
`))

	require.Error(t, err)
	assert.ErrorIs(t, err, check.ErrInvalidSpec)
	assert.Contains(t, err.Error(), "sweepIntervl")
}

func TestLoad_RejectsAnEmptyFile(t *testing.T) {
	_, err := check.Load(strings.NewReader(""))

	require.Error(t, err)
	assert.ErrorIs(t, err, check.ErrInvalidSpec)
	assert.Contains(t, err.Error(), "empty")
}

func TestLoad_FillsEveryOptionalField(t *testing.T) {
	// §10.1's defaulting requirement: the effective configuration must be
	// visible, so `kubectl get driftcheck -o yaml` shows what is running rather
	// than what was typed.
	spec := load(t, "source: {type: memory}\n")

	assert.Equal(t, "json", spec.Codec.Type)
	assert.Equal(t, "scalar", spec.Projection.Type)
	assert.Equal(t, check.DefaultMaxTrackedKeys, spec.Policy.MaxTrackedKeys)
	assert.Equal(t, check.DefaultOracleShards, spec.Policy.OracleShards)
	assert.Equal(t, check.DefaultRingSize, spec.Policy.RingSize)
	assert.Equal(t, check.BootstrapAdopt, spec.Policy.Bootstrap)
	assert.Equal(t, check.ExpiryStrict, spec.Policy.ExpiryPolicy)
	assert.InDelta(t, check.DefaultSafetyFactor, spec.Policy.SettlementWindow.SafetyFactor.Float(), 0)
	assert.Equal(t, check.DefaultSweepInterval, spec.Policy.SweepInterval.Duration())
}

func TestDuration_AcceptsBothSpellings(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want time.Duration
	}{
		{"go syntax", `policy: {sweepInterval: 2m30s}`, 150 * time.Second},
		{"quoted", `policy: {sweepInterval: "45s"}`, 45 * time.Second},
		{"bare seconds", `policy: {sweepInterval: 30}`, 30 * time.Second},
		{"fractional seconds", `policy: {sweepInterval: 1.5}`, 1500 * time.Millisecond},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spec := load(t, "source: {type: memory}\n"+tc.yaml)
			assert.Equal(t, tc.want, spec.Policy.SweepInterval.Duration())
		})
	}
}

func TestDuration_ReportsWhatItExpected(t *testing.T) {
	_, err := check.Load(strings.NewReader("source: {type: memory}\npolicy: {sweepInterval: soon}"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), `"soon" is not a duration`)
	assert.Contains(t, err.Error(), `"5s"`, "the error shows the form that works")
}

func TestDecimal_AcceptsANumberOrAQuotedNumber(t *testing.T) {
	// The CRD spells decimals as strings because Kubernetes API conventions
	// avoid floats; a hand-written local file spells them as numbers. Both have
	// to work or the same file cannot be used in both places.
	quoted := load(t, `
source: {type: memory}
policy: {settlementWindow: {safetyFactor: "2.5"}}
`)
	bare := load(t, `
source: {type: memory}
policy: {settlementWindow: {safetyFactor: 2.5}}
`)

	assert.InDelta(t, 2.5, quoted.Policy.SettlementWindow.SafetyFactor.Float(), 0)
	assert.InDelta(t, 2.5, bare.Policy.SettlementWindow.SafetyFactor.Float(), 0)
}

// TestSpec_ValidationTable is the §10.2 rule table, one case per row.
//
// Every case takes the minimal valid spec and breaks exactly one thing, so the
// assertion is that the error names that field and not that validation
// happened at all.
func TestSpec_ValidationTable(t *testing.T) {
	tests := []struct {
		name      string
		yaml      string
		wantField string
		wantMsg   string
	}{
		{
			name:      "an unregistered source type",
			yaml:      "source: {type: kafka}",
			wantField: "source.type",
			wantMsg:   `unknown source type "kafka"`,
		},
		{
			name:      "zmq with no endpoints",
			yaml:      "source: {type: zmq}",
			wantField: "source.zmq.endpoints",
			wantMsg:   "at least one endpoint",
		},
		{
			name: "an unparseable zmq endpoint",
			yaml: `source:
  type: zmq
  zmq: {endpoints: ["vllm-0:5557"]}`,
			wantField: "source.zmq.endpoints[0]",
			wantMsg:   "invalid endpoint",
		},
		{
			name: "a zmq endpoint with no port",
			yaml: `source:
  type: zmq
  zmq: {endpoints: ["tcp://vllm-0"]}`,
			wantField: "source.zmq.endpoints[0]",
			wantMsg:   "host and a port",
		},
		{
			name: "an ingest buffer smaller than the socket high-water mark",
			yaml: `source:
  type: zmq
  ingestBufferSize: 1000
  zmq: {endpoints: ["tcp://vllm-0:5557"], recvHWM: 100000}`,
			wantField: "policy",
			wantMsg:   "otherwise event loss occurs invisibly in the socket",
		},
		{
			name: "a nats queue group",
			yaml: `source:
  type: nats
  nats: {url: "nats://nats:4222", subjects: ["kv.>"], queueGroup: driftwatch}`,
			wantField: "source.nats.queueGroup",
			wantMsg:   "would distribute events across replicas and corrupt the oracle",
		},
		{
			name:      "an unregistered codec",
			yaml:      "source: {type: memory}\ncodec: {type: protobuf}",
			wantField: "codec.type",
			wantMsg:   `unknown codec type "protobuf"`,
		},
		{
			name:      "an unknown field mapping",
			yaml:      "source: {type: memory}\ncodec: {fieldMapping: {replica: replica_id}}",
			wantField: "codec.fieldMapping",
			wantMsg:   `unknown field "replica"`,
		},
		{
			name:      "an unknown op mapping target",
			yaml:      "source: {type: memory}\ncodec: {opMapping: {BLOCK_STORED: upsert}}",
			wantField: `codec.opMapping["BLOCK_STORED"]`,
			wantMsg:   `unknown op "upsert"`,
		},
		{
			name:      "an unregistered projection",
			yaml:      "source: {type: memory}\nprojection: {type: bitmap}",
			wantField: "projection.type",
			wantMsg:   `unknown projection type "bitmap"`,
		},
		{
			name:      "a partitioned projection with no key pattern",
			yaml:      "source: {type: memory}\nprojection: {ownership: {partitioned: true}}",
			wantField: "projection.ownership.keyPattern",
			wantMsg:   "cannot be scoped to one publisher",
		},
		{
			name:      "an unregistered target",
			yaml:      "source: {type: memory}\ntarget: {type: postgres}",
			wantField: "target.type",
			wantMsg:   `unknown target type "postgres"`,
		},
		{
			name:      "redis cluster mode with no addresses",
			yaml:      "source: {type: memory}\ntarget: {type: redis, redis: {mode: cluster}}",
			wantField: "target.redis.addrs",
			wantMsg:   "required in cluster mode",
		},
		{
			name: "redis cluster mode with a single address",
			yaml: `source: {type: memory}
target:
  type: redis
  redis: {mode: cluster, addr: "redis:6379", addrs: ["a:6379", "b:6379"]}`,
			wantField: "target.redis.addr",
			wantMsg:   "must be empty in cluster mode",
		},
		{
			name:      "sentinel mode without a master name",
			yaml:      "source: {type: memory}\ntarget: {type: redis, redis: {mode: sentinel, addrs: [\"s:26379\"]}}",
			wantField: "target.redis.masterName",
			wantMsg:   "required in sentinel mode",
		},
		{
			name: "an invalid key pattern",
			yaml: `source: {type: memory}
target:
  type: redis
  redis: {addr: "redis:6379", keyPattern: "block:[0-9"}`,
			wantField: "target.redis.keyPattern",
			wantMsg:   "invalid glob",
		},
		{
			name: "a settlement window that is not ordered",
			yaml: `source: {type: memory}
policy:
  settlementWindow: {min: 10s, static: 5s, max: 60s}`,
			wantField: "policy.settlementWindow",
			wantMsg:   "does not hold",
		},
		{
			name: "a safety factor below one",
			yaml: `source: {type: memory}
policy:
  settlementWindow: {safetyFactor: "0.5"}`,
			wantField: "policy.settlementWindow.safetyFactor",
			wantMsg:   "must be >= 1.0",
		},
		{
			name:      "a sweep interval below a second",
			yaml:      "source: {type: memory}\npolicy: {sweepInterval: 200ms}",
			wantField: "policy.sweepInterval",
			wantMsg:   "at least 1s",
		},
		{
			name:      "maxTrackedKeys below the floor",
			yaml:      "source: {type: memory}\npolicy: {maxTrackedKeys: 10}",
			wantField: "policy.maxTrackedKeys",
			wantMsg:   "between 1,000 and 100,000,000",
		},
		{
			name:      "oracleShards that is not a power of two",
			yaml:      "source: {type: memory}\npolicy: {oracleShards: 100}",
			wantField: "policy.oracleShards",
			wantMsg:   "power of two",
		},
		{
			name:      "a ring size beyond the bound",
			yaml:      "source: {type: memory}\npolicy: {ringSize: 4096}",
			wantField: "policy.ringSize",
			wantMsg:   "between 1 and 1024",
		},
		{
			name:      "strict bootstrap without snapshot ops",
			yaml:      "source: {type: memory}\npolicy: {bootstrap: Strict}",
			wantField: "policy.bootstrap",
			wantMsg:   "requires codec.opMapping to define snapshotBegin and snapshotEnd",
		},
		{
			name:      "an unknown bootstrap mode",
			yaml:      "source: {type: memory}\npolicy: {bootstrap: Guess}",
			wantField: "policy.bootstrap",
			wantMsg:   "must be one of",
		},
		{
			name:      "expiryPolicy Ignore without an assumed TTL",
			yaml:      "source: {type: memory}\npolicy: {expiryPolicy: Ignore}",
			wantField: "policy.assumedTTL",
			wantMsg:   "must be positive when expiryPolicy is Ignore",
		},
		{
			name:      "an unknown settlement window mode",
			yaml:      "source: {type: memory}\npolicy: {settlementWindow: {mode: guessy}}",
			wantField: "policy.settlementWindow.mode",
			wantMsg:   "must be static or adaptive",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spec := load(t, tc.yaml)

			err := spec.Validate()

			require.Error(t, err, "this configuration should not have been accepted")
			assert.ErrorIs(t, err, check.ErrInvalidSpec)
			assert.Contains(t, err.Error(), tc.wantField, "the error must name the offending field")
			assert.Contains(t, err.Error(), tc.wantMsg)

			var ve *check.ValidationError
			require.ErrorAs(t, err, &ve)
			assert.Contains(t, ve.Fields(), tc.wantField)
		})
	}
}

func TestSpec_StrictBootstrapIsAcceptedWithSnapshotOpsMapped(t *testing.T) {
	spec := load(t, `
source: {type: memory}
codec:
  opMapping:
    SNAPSHOT_START: snapshotBegin
    SNAPSHOT_END: snapshotEnd
policy: {bootstrap: Strict}
`)

	assert.NoError(t, spec.Validate())
}

func TestSpec_ReportsEveryProblemAtOnce(t *testing.T) {
	// An operator fixing a config file one error per run is an operator who
	// gives up on the tool.
	spec := load(t, `
source: {type: kafka}
codec: {type: protobuf}
projection: {type: bitmap}
policy: {ringSize: 9999, oracleShards: 100}
`)

	err := spec.Validate()

	var ve *check.ValidationError
	require.ErrorAs(t, err, &ve)
	assert.Len(t, ve.Errors, 5)
	assert.Contains(t, err.Error(), "5 problems")
}

func TestSpec_WarnsWithoutRefusingToStart(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "a counter that is not increment-only",
			yaml: "source: {type: memory}\nprojection: {type: counter}",
			want: "ProjectionNotCommutative",
		},
		{
			name: "a sweep interval tighter than two windows",
			yaml: `source: {type: memory}
policy: {sweepInterval: 3s, settlementWindow: {static: 5s, min: 1s, max: 60s}}`,
			want: "SweepIntervalTight",
		},
		{
			name: "alerting on suspect keys",
			yaml: "source: {type: memory}\nalert: {includeSuspect: true}",
			want: "includeSuspect",
		},
		{
			name: "an explicit multipart convention",
			yaml: `source:
  type: zmq
  zmq: {endpoints: ["tcp://a:5557"], multipart: singleFrame, recvHWM: 1000}`,
			want: "advisory",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spec := load(t, tc.yaml)

			assert.NoError(t, spec.Validate(), "a warning must not block startup")
			assert.Contains(t, strings.Join(spec.Warnings(), "\n"), tc.want)
		})
	}
}

func TestSpec_RedactsSecretsInEveryRendering(t *testing.T) {
	// §12.3 asks for the effective configuration to be logged once at startup,
	// which is only safe if this is the only way a spec is ever rendered.
	spec := load(t, `
source: {type: memory}
target:
  type: redis
  redis: {addr: "redis:6379", username: driftwatch, password: hunter2}
`)

	rendered := spec.YAML()

	assert.NotContains(t, rendered, "hunter2")
	assert.Contains(t, rendered, check.Redacted)
	assert.Contains(t, rendered, "driftwatch", "the username is not a secret and is worth seeing")
	assert.Equal(t, "hunter2", spec.Target.Redis.Password,
		"redaction must not mutate the spec the check is actually running")
}

func TestSpec_TranslatesIntoEachSubsystemsConfiguration(t *testing.T) {
	spec := load(t, `
source:
  type: zmq
  ingestBufferSize: 200000
  zmq:
    endpoints: ["tcp://vllm-0:5557", "tcp://vllm-1:5557"]
    topics: ["kv_events"]
    recvHWM: 100000
codec:
  type: json
  fieldMapping: {publisher: replica_id, key: block_hash}
  opMapping: {BLOCK_STORED: add, BLOCK_EVICTED: remove}
projection:
  type: keysetOwnership
  keyTemplate: "block:{{.Key}}"
  maxMembersPerKey: 64
target:
  type: redis
  redis: {addr: "redis:6379", db: 3, readBatchSize: 250, keyPattern: "block:*"}
`)
	require.NoError(t, spec.Validate())

	src := spec.SourceConfig()
	assert.Equal(t, "tcp://vllm-0:5557,tcp://vllm-1:5557", src.Settings["endpoints"])
	assert.Equal(t, "kv_events", src.Settings["topics"])
	assert.Equal(t, "100000", src.Settings["recvHWM"])

	cdc := spec.CodecConfig()
	assert.Equal(t, "replica_id", cdc["publisherField"])
	assert.Equal(t, "block_hash", cdc["keyField"])
	assert.Equal(t, "BLOCK_EVICTED=remove,BLOCK_STORED=add", cdc["opMapping"],
		"rendered in a stable order, so the effective config is comparable across restarts")

	proj := spec.ProjectionConfig()
	assert.Equal(t, "block:{{.Key}}", proj["keyTemplate"])
	assert.Equal(t, "64", proj["maxMembersPerKey"])

	tgt := spec.TargetSettings()
	assert.Equal(t, "redis:6379", tgt["addrs"])
	assert.Equal(t, "3", tgt["db"])
	assert.Equal(t, "250", tgt["batchSize"])
	assert.Equal(t, "block:*", spec.ExtraScanPattern())
}

func TestSpec_AdaptiveModeStartsAtTheFloor(t *testing.T) {
	// The estimator has no measurements at startup. Starting from the floor
	// means the window widens as evidence arrives, rather than narrowing from a
	// guess — and a window that is too wide only costs detection latency, while
	// one that is too narrow manufactures findings.
	spec := load(t, `
source: {type: memory}
policy:
  settlementWindow: {mode: adaptive, min: 2s, static: 5s, max: 60s}
`)

	assert.Equal(t, 2*time.Second, spec.EffectiveWindow())
}

func TestSpec_IDNamesTheCheckForLogsAndMetrics(t *testing.T) {
	tests := []struct {
		yaml string
		want string
	}{
		{"source: {type: memory}\nname: kv\nnamespace: inference", "inference/kv"},
		{"source: {type: memory}\nname: kv", "kv"},
		{"source: {type: memory}", "default"},
	}

	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			spec := load(t, tc.yaml)
			assert.Equal(t, tc.want, spec.ID())
		})
	}
}
