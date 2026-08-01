package v1alpha1

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/nabrahma/driftwatch/pkg/check"
)

// Defaulting runs twice over a DriftCheck, and that is deliberate rather than
// redundant. The kubebuilder markers on the type put defaults in the CRD's
// structural schema, so the API server fills them even when the webhook is not
// installed — which is how `kubectl apply -f config/crd/` alone produces a
// usable object. This webhook then fills what a schema default cannot express:
// a whole sub-object that only exists once another field selects it, and the
// couple of values that depend on a second field.
//
// The numbers themselves are pkg/check's exported constants. Two copies of
// "the default sweep interval is 30s" would eventually disagree, and the one
// that wins would depend on whether the operator went through the CLI or the
// cluster.

// Default fills every optional field, so that a `kubectl get driftcheck -o yaml`
// shows the configuration that is actually running rather than the sparse thing
// the operator typed (§10.2).
func (in *DriftCheckSpec) Default() {
	in.defaultSource()
	in.defaultCodec()
	in.defaultProjection()
	in.defaultTarget()
	in.defaultPolicy()
	in.defaultAlert()
}

func (in *DriftCheckSpec) defaultSource() {
	if in.Source.Type == "" {
		in.Source.Type = "zmq"
	}
	if in.Source.IngestBufferSize <= 0 {
		in.Source.IngestBufferSize = check.DefaultIngestBufferSize
	}

	if z := in.Source.ZMQ; z != nil {
		if z.RecvHWM <= 0 {
			z.RecvHWM = check.DefaultRecvHWM
		}
		setDuration(&z.ConnectTimeout, check.DefaultConnectTimeout)
		setDuration(&z.ReconnectIntervalMax, check.DefaultReconnectMax)
		if z.Multipart == "" {
			z.Multipart = check.MultipartAuto
		}

		// Note what is deliberately not defaulted here: ingestBufferSize is left
		// alone even when it is below recvHWM. Quietly raising it would make the
		// object valid and the operator's mistake invisible, which is the exact
		// failure mode the rule exists to prevent — the whole point of that rule
		// is that under-sizing the buffer moves event loss inside the socket
		// where nothing counts it. It is worth an admission error naming both
		// numbers rather than a silent correction.
	}
	if f := in.Source.File; f != nil && f.Speed == "" {
		f.Speed = check.SpeedFast
	}
}

func (in *DriftCheckSpec) defaultCodec() {
	if in.Codec.Type == "" {
		in.Codec.Type = "json"
	}
	if in.Codec.MaxPayloadBytes <= 0 {
		in.Codec.MaxPayloadBytes = check.DefaultMaxPayloadBytes
	}
}

func (in *DriftCheckSpec) defaultProjection() {
	if in.Projection.Type == "" {
		in.Projection.Type = "keysetOwnership"
	}
	if in.Projection.KeyTemplate == "" {
		in.Projection.KeyTemplate = check.DefaultKeyTemplate
	}
	if in.Projection.MemberTemplate == "" {
		in.Projection.MemberTemplate = check.DefaultMemberTemplate
	}
	if in.Projection.MaxMembersPerKey <= 0 {
		in.Projection.MaxMembersPerKey = check.DefaultMaxMembersPerKey
	}
}

func (in *DriftCheckSpec) defaultTarget() {
	if in.Target.Type == "" {
		in.Target.Type = "redis"
	}
	if in.Target.Type != "redis" {
		return
	}

	// The sub-object a schema default cannot create: redis exists only because
	// target.type selected it, and the API server will not default the children
	// of a field that is absent.
	if in.Target.Redis == nil {
		in.Target.Redis = &RedisSpec{}
	}

	r := in.Target.Redis
	if r.Mode == "" {
		r.Mode = check.RedisStandalone
	}
	if r.KeyPattern == "" {
		r.KeyPattern = check.DefaultKeyPatternGlob
	}
	if r.ReadBatchSize <= 0 {
		r.ReadBatchSize = check.DefaultReadBatchSize
	}
	if r.ScanCount <= 0 {
		r.ScanCount = check.DefaultScanCount
	}
	if r.PoolSize <= 0 {
		r.PoolSize = check.DefaultRedisPoolSize
	}
	setDuration(&r.DialTimeout, check.DefaultDialTimeout)
	setDuration(&r.ReadTimeout, check.DefaultReadTimeout)
}

//nolint:gocyclo // one branch per defaulted field; splitting it would only move the list
func (in *DriftCheckSpec) defaultPolicy() {
	p := &in.Policy
	w := &p.SettlementWindow

	if w.Mode == "" {
		w.Mode = check.WindowAdaptive
	}
	setDuration(&w.Static, check.DefaultStaticWindow)
	setDuration(&w.Min, check.DefaultMinWindow)
	setDuration(&w.Max, check.DefaultMaxWindow)
	if w.SafetyFactor == "" {
		w.SafetyFactor = check.Decimal(check.DefaultSafetyFactor).String()
	}

	setDuration(&p.SweepInterval, check.DefaultSweepInterval)
	setDuration(&p.ExtraScanInterval, check.DefaultExtraScanInterval)
	setDuration(&p.ReorderWindow, check.DefaultReorderWindow)
	setDuration(&p.TTLTolerance, check.DefaultTTLTolerance)

	if p.Bootstrap == "" {
		p.Bootstrap = check.BootstrapAdopt
	}
	if p.ExpiryPolicy == "" {
		p.ExpiryPolicy = check.ExpiryStrict
	}
	if p.MaxTrackedKeys <= 0 {
		p.MaxTrackedKeys = check.DefaultMaxTrackedKeys
	}
	if p.RingSize <= 0 {
		p.RingSize = check.DefaultRingSize
	}
	if p.MaxConfirmQueue <= 0 {
		p.MaxConfirmQueue = check.DefaultMaxConfirmQueue
	}
	if p.MaxFindings <= 0 {
		p.MaxFindings = check.DefaultMaxFindings
	}
	if p.MaxExtrasTracked <= 0 {
		p.MaxExtrasTracked = check.DefaultMaxExtrasTracked
	}
	if p.MaxPublishers <= 0 {
		p.MaxPublishers = check.DefaultMaxPublishers
	}
	if p.OracleShards <= 0 {
		p.OracleShards = check.DefaultOracleShards
	}
	if p.NeverSettledThreshold <= 0 {
		p.NeverSettledThreshold = check.DefaultNeverSettledThreshold
	}
}

func (in *DriftCheckSpec) defaultAlert() {
	if in.Alert.DivergentKeysThreshold <= 0 {
		in.Alert.DivergentKeysThreshold = check.DefaultDivergentKeys
	}
	if in.Alert.DivergentRatioThreshold == "" {
		in.Alert.DivergentRatioThreshold = check.Decimal(check.DefaultDivergentRatio).String()
	}
	setDuration(&in.Alert.ForDuration, check.DefaultForDuration)
}

func setDuration(d *metav1.Duration, fallback time.Duration) {
	if d.Duration <= 0 {
		d.Duration = fallback
	}
}
