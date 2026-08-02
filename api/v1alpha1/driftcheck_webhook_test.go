package v1alpha1

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/nabrahma/driftwatch/pkg/check"
)

// One test per row of §10.2's table, each asserting the exact sentence the
// operator will read.
//
// Asserting the message rather than "an error occurred" is the whole point. A
// rule that fires with a message nobody can act on is only marginally better
// than no rule: the operator knows something is wrong and still has to read the
// source to find out what. Several of these messages are the only place the
// reasoning is written down at all — the recvHWM one especially, where the
// consequence of getting it wrong is silence rather than failure.

// valid returns a DriftCheck that passes every rule, so each test can break
// exactly one thing and attribute the error to it.
func valid() *DriftCheck {
	dc := &DriftCheck{
		ObjectMeta: metav1.ObjectMeta{Name: "kvcache-index", Namespace: "inference"},
		Spec: DriftCheckSpec{
			Source: SourceSpec{
				Type: "zmq",
				ZMQ: &ZMQSpec{
					Endpoints: []string{"tcp://vllm-0.vllm.inference.svc:5557"},
					Topics:    []string{"kv_events"},
				},
			},
			Projection: ProjectionSpec{Type: "keysetOwnership", KeyTemplate: "block:{{.Key}}"},
			Target: TargetSpec{
				Type:  "redis",
				Redis: &RedisSpec{Addr: "redis.inference.svc:6379", KeyPattern: "block:*"},
			},
		},
	}
	dc.Spec.Default()
	return dc
}

// admit runs the create path exactly as the API server would: default, then
// validate.
func admit(t *testing.T, dc *DriftCheck) error {
	t.Helper()

	require.NoError(t, (&DriftCheckDefaulter{}).Default(context.Background(), dc))
	_, err := (&DriftCheckValidator{}).ValidateCreate(context.Background(), dc)
	return err
}

// admitUpdate runs the update path against a stored object.
func admitUpdate(t *testing.T, previous, updated *DriftCheck) error {
	t.Helper()

	require.NoError(t, (&DriftCheckDefaulter{}).Default(context.Background(), updated))
	_, err := (&DriftCheckValidator{}).ValidateUpdate(context.Background(), previous, updated)
	return err
}

func warningsOf(t *testing.T, dc *DriftCheck) []string {
	t.Helper()

	require.NoError(t, (&DriftCheckDefaulter{}).Default(context.Background(), dc))
	warnings, err := (&DriftCheckValidator{}).ValidateCreate(context.Background(), dc)
	require.NoError(t, err)
	return warnings
}

func TestWebhook_ValidSpecIsAdmitted(t *testing.T) {
	// The control. Without it every test below could pass because the fixture
	// is broken in some unrelated way.
	require.NoError(t, admit(t, valid()))
}

// --- Rule 1: source.type must be a registered name -------------------------

func TestWebhookRule01_UnknownSourceType(t *testing.T) {
	dc := valid()
	dc.Spec.Source.Type = "kafka"

	err := admit(t, dc)

	require.Error(t, err)
	assert.Contains(t, err.Error(),
		`spec.source.type: Invalid value: "": unknown source type "kafka", `+
			`valid: [file memory nats zmq]`)
}

// --- Rule 2: zmq requires >= 1 parseable endpoint ---------------------------

func TestWebhookRule02_ZMQEndpointsRequiredAndParseable(t *testing.T) {
	t.Run("none at all", func(t *testing.T) {
		dc := valid()
		dc.Spec.Source.ZMQ.Endpoints = nil

		err := admit(t, dc)

		require.Error(t, err)
		assert.Contains(t, err.Error(),
			"spec.source.zmq.endpoints: Invalid value: \"\": at least one endpoint is required")
	})

	t.Run("one that is not a ZMQ URI", func(t *testing.T) {
		dc := valid()
		dc.Spec.Source.ZMQ.Endpoints = []string{"vllm-0:5557"}

		err := admit(t, dc)

		require.Error(t, err)
		assert.Contains(t, err.Error(),
			"spec.source.zmq.endpoints[0]: Invalid value: \"\": invalid endpoint: "+
				"expected a form like tcp://host:port")
	})
}

// --- Rule 3: ingestBufferSize >= recvHWM ------------------------------------

func TestWebhookRule03_IngestBufferMustCoverRecvHWM(t *testing.T) {
	// The rule that matters more than it looks. A socket whose high-water mark
	// exceeds the buffer behind it drops frames inside ZMQ, where driftwatch
	// cannot see them, cannot count them, and cannot mark the affected keys
	// suspect. The check keeps reporting a complete view of a stream it is
	// quietly missing pieces of — which is precisely the failure driftwatch
	// exists to catch, happening to driftwatch.
	dc := valid()
	dc.Spec.Source.IngestBufferSize = 1000
	dc.Spec.Source.ZMQ.RecvHWM = 100_000

	err := admit(t, dc)

	require.Error(t, err)
	assert.Contains(t, err.Error(),
		"spec.policy: Invalid value: \"\": ingestBufferSize (1000) must be >= "+
			"source.zmq.recvHWM (100000), otherwise event loss occurs invisibly in the socket")
}

func TestWebhookRule03_DefaultingDoesNotHideTheMismatch(t *testing.T) {
	// Defaulting could make this rule unreachable by quietly raising the buffer
	// to match, and that would be the wrong kindness: the operator would keep
	// the recvHWM they chose and lose the buffer size they chose, without being
	// told either happened.
	dc := valid()
	dc.Spec.Source.IngestBufferSize = 1000
	dc.Spec.Source.ZMQ.RecvHWM = 100_000

	require.NoError(t, (&DriftCheckDefaulter{}).Default(context.Background(), dc))

	assert.Equal(t, 1000, dc.Spec.Source.IngestBufferSize,
		"defaulting left the operator's value alone so validation could reject it")
}

// --- Rule 4: nats queueGroup must be empty ----------------------------------

func TestWebhookRule04_NATSQueueGroupMustBeEmpty(t *testing.T) {
	// The one misconfiguration in the whole spec that corrupts results instead
	// of failing. A queue group looks like sensible horizontal scaling and
	// silently gives every replica a different subset of the stream, so each
	// computes a different incomplete expectation and reports the store wrong.
	dc := valid()
	dc.Spec.Source = SourceSpec{
		Type: "nats",
		NATS: &NATSSpec{
			URL:        "nats://nats.default.svc:4222",
			Subjects:   []string{"kv.events.>"},
			QueueGroup: "driftwatch",
		},
	}

	err := admit(t, dc)

	require.Error(t, err)
	assert.Contains(t, err.Error(),
		"spec.source.nats.queueGroup: Invalid value: \"\": source.nats.queueGroup "+
			"must be empty: a queue group would distribute events across replicas "+
			"and corrupt the oracle")
}

// --- Rule 5: codec.type registered, fieldMapping keys known -----------------

func TestWebhookRule05_UnknownCodecTypeAndFieldMapping(t *testing.T) {
	t.Run("codec type", func(t *testing.T) {
		dc := valid()
		dc.Spec.Codec.Type = "protobuf"

		err := admit(t, dc)

		require.Error(t, err)
		assert.Contains(t, err.Error(),
			`spec.codec.type: Invalid value: "": unknown codec type "protobuf", valid: [json msgpack template]`)
	})

	t.Run("field mapping key", func(t *testing.T) {
		dc := valid()
		dc.Spec.Codec.FieldMapping = map[string]string{"replica": "replica_id"}

		err := admit(t, dc)

		require.Error(t, err)
		assert.Contains(t, err.Error(),
			`spec.codec.fieldMapping: Invalid value: "": unknown field "replica", `+
				`valid: [delta epoch key member op publisher seq timestamp ttl value]`)
	})
}

// --- Rule 6: opMapping values must be valid ops -----------------------------

func TestWebhookRule06_UnknownOpInOpMapping(t *testing.T) {
	dc := valid()
	dc.Spec.Codec.OpMapping = map[string]string{"BLOCK_STORED": "upsert"}

	err := admit(t, dc)

	require.Error(t, err)
	assert.Contains(t, err.Error(),
		`spec.codec.opMapping["BLOCK_STORED"]: Invalid value: "": unknown op "upsert"`)
}

// --- Rule 7: projection.type registered -------------------------------------

func TestWebhookRule07_UnknownProjectionType(t *testing.T) {
	dc := valid()
	dc.Spec.Projection.Type = "histogram"

	err := admit(t, dc)

	require.Error(t, err)
	assert.Contains(t, err.Error(),
		`spec.projection.type: Invalid value: "": unknown projection type "histogram", `+
			`valid: [counter keysetOwnership scalar]`)
}

// --- Rule 8: non-commutative counter warns rather than failing ---------------

func TestWebhookRule08_NonCommutativeCounterWarnsRatherThanFails(t *testing.T) {
	// A counter fed anything but increments is order-dependent, so a reordered
	// stream converges to a different total. Real risk, not always avoidable —
	// so it surfaces as a warning and a condition rather than a refusal, and
	// the operator decides.
	dc := valid()
	dc.Spec.Projection.Type = "counter"
	dc.Spec.Projection.IncrOnly = false

	warnings := warningsOf(t, dc)

	require.NotEmpty(t, warnings)
	assert.Contains(t, warnings[0],
		"a counter that accepts absolute sets is not commutative, so a reordered "+
			"stream converges to a different total (condition ProjectionNotCommutative)")

	dc = valid()
	dc.Spec.Projection.Type = "counter"
	dc.Spec.Projection.IncrOnly = true

	assert.Empty(t, warningsOf(t, dc), "an increment-only counter is commutative")
}

// --- Rule 9: cluster mode requires addrs and forbids addr -------------------

func TestWebhookRule09_RedisClusterModeAddressing(t *testing.T) {
	t.Run("addrs required", func(t *testing.T) {
		dc := valid()
		dc.Spec.Target.Redis.Mode = check.RedisCluster
		dc.Spec.Target.Redis.Addr = ""

		err := admit(t, dc)

		require.Error(t, err)
		assert.Contains(t, err.Error(),
			"spec.target.redis.addrs: Invalid value: \"\": required in cluster mode")
	})

	t.Run("addr forbidden", func(t *testing.T) {
		dc := valid()
		dc.Spec.Target.Redis.Mode = check.RedisCluster
		dc.Spec.Target.Redis.Addrs = []string{"redis-0:6379", "redis-1:6379"}

		err := admit(t, dc)

		require.Error(t, err)
		assert.Contains(t, err.Error(),
			"spec.target.redis.addr: Invalid value: \"\": must be empty in cluster mode; use addrs")
	})
}

// --- Rule 10: keyPattern must be a valid Redis glob -------------------------

func TestWebhookRule10_InvalidKeyPatternGlob(t *testing.T) {
	// Worth catching at apply time rather than at the first extras scan: SCAN
	// with an unmatched pattern returns nothing, which reads exactly like a
	// store with no keys in it.
	dc := valid()
	dc.Spec.Target.Redis.KeyPattern = "block:[a-"

	err := admit(t, dc)

	require.Error(t, err)
	assert.Contains(t, err.Error(),
		"spec.target.redis.keyPattern: Invalid value: \"\": invalid glob: unmatched '['")
}

// --- Rule 11: min <= static <= max ------------------------------------------

func TestWebhookRule11_SettlementWindowOrdering(t *testing.T) {
	dc := valid()
	dc.Spec.Policy.SettlementWindow.Min = metav1.Duration{Duration: 30 * time.Second}
	dc.Spec.Policy.SettlementWindow.Static = metav1.Duration{Duration: 5 * time.Second}
	dc.Spec.Policy.SettlementWindow.Max = metav1.Duration{Duration: 120 * time.Second}

	err := admit(t, dc)

	require.Error(t, err)
	assert.Contains(t, err.Error(),
		"spec.policy.settlementWindow: Invalid value: \"\": min (30s) <= static (5s) "+
			"<= max (2m0s) does not hold")
}

// --- Rule 12: safetyFactor >= 1.0 -------------------------------------------

func TestWebhookRule12_SafetyFactorAtLeastOne(t *testing.T) {
	// Below 1.0 the window would be narrower than the convergence delay it was
	// derived from, so normal operation gets reported as drift.
	dc := valid()
	dc.Spec.Policy.SettlementWindow.SafetyFactor = "0.5"

	err := admit(t, dc)

	require.Error(t, err)
	assert.Contains(t, err.Error(),
		"spec.policy.settlementWindow.safetyFactor: Invalid value: \"\": must be >= 1.0, got 0.5")
}

func TestWebhookRule12_SafetyFactorMustBeANumber(t *testing.T) {
	// The CRD carries this as a string because Kubernetes convention avoids
	// floats. Without an explicit parse an unparseable value would convert to
	// zero and be reported as violating the bound, sending the operator looking
	// at a number they never wrote.
	dc := valid()
	dc.Spec.Policy.SettlementWindow.SafetyFactor = "3.O"

	err := admit(t, dc)

	require.Error(t, err)
	assert.Contains(t, err.Error(),
		`spec.policy.settlementWindow.safetyFactor: Invalid value: "3.O": "3.O" is not a number`)
}

// --- Rule 13: sweepInterval at least 1s -------------------------------------

func TestWebhookRule13_SweepIntervalLowerBound(t *testing.T) {
	dc := valid()
	dc.Spec.Policy.SweepInterval = metav1.Duration{Duration: 200 * time.Millisecond}

	err := admit(t, dc)

	require.Error(t, err)
	assert.Contains(t, err.Error(),
		"spec.policy.sweepInterval: Invalid value: \"\": must be at least 1s, got 200ms")
}

// --- Rule 14: sweepInterval below 2W warns ----------------------------------

func TestWebhookRule14_TightSweepIntervalWarns(t *testing.T) {
	// Sweeping faster than twice the settlement window means the next sweep
	// raises candidates the previous one has not finished confirming, so the
	// confirm queue grows and the same key is re-raised on every pass.
	dc := valid()
	dc.Spec.Policy.SettlementWindow.Mode = check.WindowStatic
	dc.Spec.Policy.SettlementWindow.Static = metav1.Duration{Duration: 30 * time.Second}
	dc.Spec.Policy.SettlementWindow.Max = metav1.Duration{Duration: 120 * time.Second}
	dc.Spec.Policy.SweepInterval = metav1.Duration{Duration: 10 * time.Second}

	warnings := warningsOf(t, dc)

	require.NotEmpty(t, warnings)
	assert.Contains(t, warnings[0],
		"policy.sweepInterval (10s) is less than twice the settlement window (30s): "+
			"candidates will still be awaiting confirmation when the next sweep "+
			"raises them again (condition SweepIntervalTight)")
}

// --- Rule 15: maxTrackedKeys bounds -----------------------------------------

func TestWebhookRule15_MaxTrackedKeysBounds(t *testing.T) {
	dc := valid()
	dc.Spec.Policy.MaxTrackedKeys = 500

	err := admit(t, dc)

	require.Error(t, err)
	assert.Contains(t, err.Error(),
		"spec.policy.maxTrackedKeys: Invalid value: \"\": must be between 1,000 and "+
			"100,000,000, got 500")
}

// --- Rule 16: oracleShards a power of two -----------------------------------

func TestWebhookRule16_OracleShardsPowerOfTwo(t *testing.T) {
	dc := valid()
	dc.Spec.Policy.OracleShards = 100

	err := admit(t, dc)

	require.Error(t, err)
	assert.Contains(t, err.Error(),
		"spec.policy.oracleShards: Invalid value: \"\": must be a power of two between "+
			"1 and 1024, got 100")
}

// --- Rule 17: ringSize bounds -----------------------------------------------

func TestWebhookRule17_RingSizeBounds(t *testing.T) {
	dc := valid()
	dc.Spec.Policy.RingSize = 4096

	err := admit(t, dc)

	require.Error(t, err)
	assert.Contains(t, err.Error(),
		"spec.policy.ringSize: Invalid value: \"\": must be between 1 and 1024, got 4096")
}

// --- Rule 18: bootstrap Strict needs the snapshot markers -------------------

func TestWebhookRule18_StrictBootstrapNeedsSnapshotOps(t *testing.T) {
	// Strict asserts nothing until a publisher retransmits its whole state. If
	// the operator had to teach driftwatch what the producer's op names mean,
	// they have to teach it the snapshot markers too, or Strict waits forever
	// for a cycle it cannot recognize and the check never asserts anything.
	dc := valid()
	dc.Spec.Policy.Bootstrap = check.BootstrapStrict
	dc.Spec.Codec.OpMapping = map[string]string{
		"BLOCK_STORED":  "add",
		"BLOCK_EVICTED": "remove",
	}

	err := admit(t, dc)

	require.Error(t, err)
	assert.Contains(t, err.Error(),
		"spec.policy.bootstrap: Invalid value: \"\": policy.bootstrap=Strict requires "+
			"codec.opMapping to define snapshotBegin and snapshotEnd")

	// With the markers mapped it is admitted.
	dc = valid()
	dc.Spec.Policy.Bootstrap = check.BootstrapStrict
	dc.Spec.Codec.OpMapping = map[string]string{
		"BLOCK_STORED":   "add",
		"SNAPSHOT_START": "snapshotBegin",
		"SNAPSHOT_END":   "snapshotEnd",
	}
	require.NoError(t, admit(t, dc))
}

// --- Rule 19: expiryPolicy Ignore needs assumedTTL ---------------------------

func TestWebhookRule19_IgnoreExpiryNeedsAssumedTTL(t *testing.T) {
	dc := valid()
	dc.Spec.Policy.ExpiryPolicy = check.ExpiryIgnore

	err := admit(t, dc)

	require.Error(t, err)
	assert.Contains(t, err.Error(),
		"spec.policy.assumedTTL: Invalid value: \"\": must be positive when "+
			"expiryPolicy is Ignore")

	dc = valid()
	dc.Spec.Policy.ExpiryPolicy = check.ExpiryIgnore
	dc.Spec.Policy.AssumedTTL = metav1.Duration{Duration: time.Hour}
	require.NoError(t, admit(t, dc))
}

// --- Rule 20: secret refs are the controller's job, not the webhook's -------

func TestWebhookRule20_SecretRefsAreNotResolvedByTheWebhook(t *testing.T) {
	// §10.2 puts this rule on the controller and gives the reason: a webhook
	// must not depend on other objects existing. Applying a manifest that
	// creates the Secret and the DriftCheck together would be rejected on
	// ordering alone, and a rotated Secret would be re-rejected later, when
	// nothing about the DriftCheck had changed.
	//
	// So the webhook admits a reference to a secret that does not exist, and
	// SecretRefs tells the controller what to go and look for. The controller's
	// side of this rule is TestReconcile_SecretResolutionFailureAndRecovery.
	dc := valid()
	dc.Spec.Target.Redis.PasswordSecretRef = &SecretKeyRef{Name: "absent", Key: "password"}

	require.NoError(t, admit(t, dc))

	refs := dc.Spec.SecretRefs()
	require.Len(t, refs, 1)
	assert.Equal(t, "absent", refs["target.redis.passwordSecretRef"].Name)
}

// --- Rule 21: projection.type and target.type are immutable -----------------

func TestWebhookRule21_ImmutableFields(t *testing.T) {
	// Not tidiness. The oracle holds values in the projection's shape and the
	// sweeper reads the store in it; changing either under a running check
	// leaves every tracked key holding something the new projection cannot fold
	// and the new target cannot read. Every key would report a shape mismatch,
	// which reads exactly like the store having been rewritten by something.
	t.Run("projection.type", func(t *testing.T) {
		previous := valid()
		updated := valid()
		updated.Spec.Projection.Type = "scalar"

		err := admitUpdate(t, previous, updated)

		require.Error(t, err)
		assert.Contains(t, err.Error(),
			"spec.projection.type: Invalid value: \"\": field is immutable; delete and "+
				"recreate the DriftCheck (was \"keysetOwnership\", now \"scalar\")")
	})

	t.Run("target.type", func(t *testing.T) {
		previous := valid()
		updated := valid()
		updated.Spec.Target.Type = "memory"
		updated.Spec.Target.Redis = nil

		err := admitUpdate(t, previous, updated)

		require.Error(t, err)
		assert.Contains(t, err.Error(),
			"spec.target.type: Invalid value: \"\": field is immutable; delete and "+
				"recreate the DriftCheck (was \"redis\", now \"memory\")")
	})

	t.Run("a mutable change is admitted", func(t *testing.T) {
		previous := valid()
		updated := valid()
		updated.Spec.Policy.SweepInterval = metav1.Duration{Duration: 90 * time.Second}
		updated.Spec.Policy.Paused = true

		require.NoError(t, admitUpdate(t, previous, updated))
	})
}

// --- The defaulting webhook -------------------------------------------------

func TestWebhook_DefaultingFillsEveryOptionalField(t *testing.T) {
	// §10.2 asks the defaulting webhook to fill everything so that
	// `kubectl get driftcheck -o yaml` shows what is actually running rather
	// than the sparse thing the operator typed. A half-defaulted object makes
	// the status a worse source of truth than the source file.
	dc := &DriftCheck{
		ObjectMeta: metav1.ObjectMeta{Name: "minimal", Namespace: "inference"},
		Spec: DriftCheckSpec{
			Source: SourceSpec{
				Type: "zmq",
				ZMQ:  &ZMQSpec{Endpoints: []string{"tcp://vllm-0:5557"}},
			},
			Target: TargetSpec{Type: "redis", Redis: &RedisSpec{Addr: "redis:6379"}},
		},
	}

	require.NoError(t, (&DriftCheckDefaulter{}).Default(context.Background(), dc))

	spec := dc.Spec
	assert.Equal(t, check.DefaultIngestBufferSize, spec.Source.IngestBufferSize)
	assert.Equal(t, check.DefaultRecvHWM, spec.Source.ZMQ.RecvHWM)
	assert.Equal(t, check.MultipartAuto, spec.Source.ZMQ.Multipart)
	assert.Equal(t, check.DefaultConnectTimeout, spec.Source.ZMQ.ConnectTimeout.Duration)
	assert.Equal(t, "json", spec.Codec.Type)
	assert.Equal(t, check.DefaultMaxPayloadBytes, spec.Codec.MaxPayloadBytes)
	assert.Equal(t, "keysetOwnership", spec.Projection.Type)
	assert.Equal(t, check.DefaultKeyTemplate, spec.Projection.KeyTemplate)
	assert.Equal(t, check.DefaultMemberTemplate, spec.Projection.MemberTemplate)
	assert.Equal(t, check.DefaultMaxMembersPerKey, spec.Projection.MaxMembersPerKey)
	assert.Equal(t, check.RedisStandalone, spec.Target.Redis.Mode)
	assert.Equal(t, check.DefaultKeyPatternGlob, spec.Target.Redis.KeyPattern)
	assert.Equal(t, check.DefaultReadBatchSize, spec.Target.Redis.ReadBatchSize)
	assert.Equal(t, check.DefaultScanCount, spec.Target.Redis.ScanCount)
	assert.Equal(t, check.DefaultRedisPoolSize, spec.Target.Redis.PoolSize)
	assert.Equal(t, check.WindowAdaptive, spec.Policy.SettlementWindow.Mode)
	assert.Equal(t, "3", spec.Policy.SettlementWindow.SafetyFactor)
	assert.Equal(t, check.DefaultSweepInterval, spec.Policy.SweepInterval.Duration)
	assert.Equal(t, check.DefaultExtraScanInterval, spec.Policy.ExtraScanInterval.Duration)
	assert.Equal(t, check.DefaultReorderWindow, spec.Policy.ReorderWindow.Duration)
	assert.Equal(t, check.BootstrapAdopt, spec.Policy.Bootstrap)
	assert.Equal(t, check.ExpiryStrict, spec.Policy.ExpiryPolicy)
	assert.Equal(t, check.DefaultMaxTrackedKeys, spec.Policy.MaxTrackedKeys)
	assert.Equal(t, check.DefaultRingSize, spec.Policy.RingSize)
	assert.Equal(t, check.DefaultOracleShards, spec.Policy.OracleShards)
	assert.Equal(t, check.DefaultNeverSettledThreshold, spec.Policy.NeverSettledThreshold)
	assert.Equal(t, check.DefaultDivergentKeys, spec.Alert.DivergentKeysThreshold)
	assert.Equal(t, "0.001", spec.Alert.DivergentRatioThreshold)
	assert.Equal(t, check.DefaultForDuration, spec.Alert.ForDuration.Duration)
}

func TestWebhook_DefaultingIsIdempotent(t *testing.T) {
	// The defaulting webhook runs on every update as well as on create, so a
	// non-idempotent default would rewrite the object on every reconcile and
	// bump the generation, which restarts the runner in a loop.
	dc := valid()
	first := dc.DeepCopy()

	require.NoError(t, (&DriftCheckDefaulter{}).Default(context.Background(), dc))

	assert.Equal(t, first.Spec, dc.Spec)
}

func TestWebhook_DefaultingCreatesTheRedisBlock(t *testing.T) {
	// The one default a CRD schema cannot express: redis exists only because
	// target.type selected it, and the API server does not default the children
	// of an absent field.
	dc := valid()
	dc.Spec.Target.Redis = nil

	require.NoError(t, (&DriftCheckDefaulter{}).Default(context.Background(), dc))

	require.NotNil(t, dc.Spec.Target.Redis)
	assert.Equal(t, check.RedisStandalone, dc.Spec.Target.Redis.Mode)
}

// --- The conversion the whole webhook rests on ------------------------------

func TestWebhook_ConversionCarriesEveryField(t *testing.T) {
	// Every rule above is enforced by pkg/check against a converted spec, so a
	// field the conversion drops is a rule that silently stops being enforced.
	// This is the test that catches that, and it is why it asserts on the
	// values rather than merely on the absence of an error.
	dc := valid()
	dc.Spec.Codec.RetainRaw = true
	dc.Spec.Codec.FieldMapping = map[string]string{"publisher": "replica_id"}
	dc.Spec.Codec.OpMapping = map[string]string{"BLOCK_STORED": "add"}
	dc.Spec.Projection.Ownership = &OwnershipSpec{
		Partitioned: true, KeyPattern: "replica:{{.Publisher}}:*",
	}
	dc.Spec.Target.Redis.PasswordSecretRef = &SecretKeyRef{Name: "redis-creds", Key: "password"}
	dc.Spec.Target.Redis.TLS = &TLSSpec{Enabled: true}
	dc.Spec.Policy.RequirePrimary = true
	dc.Spec.Policy.Paused = true
	dc.Spec.Alert.IncludeSuspect = true
	dc.Spec.Default()

	spec := dc.Spec.ToCheckSpec(dc.Name, dc.Namespace)

	assert.Equal(t, "inference/kvcache-index", spec.ID())
	assert.Equal(t, "zmq", spec.Source.Type)
	assert.Equal(t, []string{"tcp://vllm-0.vllm.inference.svc:5557"}, spec.Source.ZMQ.Endpoints)
	assert.Equal(t, []string{"kv_events"}, spec.Source.ZMQ.Topics)
	assert.True(t, spec.Codec.RetainRaw)
	assert.Equal(t, "replica_id", spec.Codec.FieldMapping["publisher"])
	assert.Equal(t, "add", spec.Codec.OpMapping["BLOCK_STORED"])
	assert.Equal(t, "block:{{.Key}}", spec.Projection.KeyTemplate)
	require.NotNil(t, spec.Projection.Ownership)
	assert.Equal(t, "replica:{{.Publisher}}:*", spec.Projection.Ownership.KeyPattern)
	require.NotNil(t, spec.Target.Redis.PasswordSecretRef)
	assert.Equal(t, "redis-creds", spec.Target.Redis.PasswordSecretRef.Name)
	require.NotNil(t, spec.Target.Redis.TLS)
	assert.True(t, spec.Target.Redis.TLS.Enabled)
	assert.Equal(t, "block:*", spec.ExtraScanPattern())
	assert.True(t, spec.Policy.RequirePrimary)
	assert.True(t, spec.Policy.Paused)
	assert.True(t, spec.Alert.IncludeSuspect)
	assert.InDelta(t, 3.0, spec.Policy.SettlementWindow.SafetyFactor.Float(), 1e-9)
	assert.InDelta(t, 0.001, spec.Alert.DivergentRatioThreshold.Float(), 1e-12)

	assert.Empty(t, spec.Target.Redis.Password,
		"the conversion carries the reference, never a resolved secret: a spec is "+
			"rendered into the startup log line §12.3 asks for")
}

func TestWebhook_ValidateRejectsTheWrongObjectType(t *testing.T) {
	// A defensive branch, but the alternative to testing it is a panic inside
	// an admission handler, which takes out every apply for the resource.
	_, err := (&DriftCheckValidator{}).ValidateCreate(context.Background(), &DriftCheckList{})
	require.ErrorContains(t, err, "expected a DriftCheck")

	err = (&DriftCheckDefaulter{}).Default(context.Background(), &DriftCheckList{})
	require.ErrorContains(t, err, "expected a DriftCheck")

	warnings, err := (&DriftCheckValidator{}).ValidateDelete(context.Background(), valid())
	require.NoError(t, err)
	assert.Empty(t, warnings)
}

func TestWebhook_EveryProblemIsReportedAtOnce(t *testing.T) {
	// Fixing a config file one error per apply is how an operator gives up on a
	// tool. §9 M14 requires the whole spec to be validated before anything
	// starts, and the webhook has to preserve that.
	dc := valid()
	dc.Spec.Policy.RingSize = 4096
	dc.Spec.Policy.OracleShards = 100
	dc.Spec.Policy.MaxTrackedKeys = 10

	err := admit(t, dc)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "spec.policy.ringSize")
	assert.Contains(t, err.Error(), "spec.policy.oracleShards")
	assert.Contains(t, err.Error(), "spec.policy.maxTrackedKeys")
}
