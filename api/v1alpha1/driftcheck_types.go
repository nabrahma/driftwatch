package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Every field below carries a doc comment, and that is a requirement rather
// than a courtesy: controller-gen copies them into the CRD's OpenAPI schema, so
// they are what `kubectl explain driftcheck.spec.policy.settlementWindow`
// prints. An undocumented field is one an operator has to read the source to
// configure, and §20 Phase 7 makes "descriptions for every field" an exit
// criterion for exactly that reason.

// DriftCheckSpec describes one audit: where the events come from, how to fold
// them into an expectation, which store to compare that expectation against,
// and how carefully to do it.
type DriftCheckSpec struct {
	// Source is the event transport driftwatch subscribes to. This is the
	// stream the audited store is itself built from — driftwatch is a second,
	// independent consumer of it.
	Source SourceSpec `json:"source"`

	// Codec decodes the transport's frames into events. Use fieldMapping and
	// opMapping when the producer's wire format is not driftwatch's own.
	//
	// The empty-object default is what makes the fields inside it default at
	// all: structural-schema defaulting does not descend into a field that is
	// absent, so a spec with no codec block would come back from the API server
	// with no codec block, and `kubectl get driftcheck -o yaml` would show
	// nothing about how events are being decoded.
	// +kubebuilder:default={}
	// +optional
	Codec CodecSpec `json:"codec,omitempty"`

	// Projection folds the event stream into the expected state of each key.
	// It is immutable: the oracle holds values in this shape and the sweeper
	// reads the store in it, so changing it under a running check would make
	// every tracked key unreadable at once.
	Projection ProjectionSpec `json:"projection"`

	// Target is the store being audited. driftwatch never writes to it; the
	// client refuses any command outside a read-only allowlist. Immutable, for
	// the same reason as projection.
	Target TargetSpec `json:"target"`

	// Policy decides when a disagreement counts as drift, and bounds every
	// queue and map so the check degrades rather than exhausting memory.
	// +kubebuilder:default={}
	// +optional
	Policy PolicySpec `json:"policy,omitempty"`

	// Alert configures the thresholds a PrometheusRule reads. driftwatch does
	// not page anyone itself; these values describe when it believes it should.
	// +kubebuilder:default={}
	// +optional
	Alert AlertSpec `json:"alert,omitempty"`
}

// SourceSpec configures the event transport.
type SourceSpec struct {
	// Type selects the transport. Only the block matching it is read.
	// +kubebuilder:validation:Enum=zmq;nats;file;memory
	// +kubebuilder:default=zmq
	Type string `json:"type"`

	// ZMQ configures a ZeroMQ SUB subscription. Required when type is zmq.
	// +optional
	ZMQ *ZMQSpec `json:"zmq,omitempty"`

	// NATS configures a core NATS subscription. Required when type is nats.
	// +optional
	NATS *NATSSpec `json:"nats,omitempty"`

	// File configures replay from a captured stream, which is how a past
	// incident is reproduced. Required when type is file.
	// +optional
	File *FileSpec `json:"file,omitempty"`

	// IngestBufferSize is the queue between the transport and the applier, in
	// messages. It must be at least as large as source.zmq.recvHWM: if the
	// socket's high-water mark is the smaller of the two, the socket discards
	// frames before driftwatch ever sees them, and the loss is invisible rather
	// than counted.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=200000
	// +optional
	IngestBufferSize int `json:"ingestBufferSize,omitempty"`
}

// ZMQSpec configures a ZeroMQ SUB subscription.
type ZMQSpec struct {
	// Endpoints are the publishers to connect to, as ZMQ URIs such as
	// tcp://vllm-0.vllm.svc:5557. One subscription is opened per endpoint and
	// DNS is re-resolved on every reconnect, so a rescheduled pod is found
	// again at its new address.
	// +kubebuilder:validation:MinItems=1
	Endpoints []string `json:"endpoints"`

	// Topics filters the subscription by topic prefix. An empty list
	// subscribes to everything, which is what a publisher that does not use
	// topics requires.
	// +optional
	Topics []string `json:"topics,omitempty"`

	// RecvHWM is the socket's receive high-water mark, in messages. Frames
	// beyond it are dropped by the transport, so ingestBufferSize must be at
	// least this large for loss to be countable.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=100000
	// +optional
	RecvHWM int `json:"recvHWM,omitempty"`

	// ConnectTimeout bounds a single connection attempt.
	// +kubebuilder:default="5s"
	// +optional
	ConnectTimeout metav1.Duration `json:"connectTimeout,omitempty"`

	// ReconnectIntervalMax caps the exponential backoff between reconnect
	// attempts.
	// +kubebuilder:default="30s"
	// +optional
	ReconnectIntervalMax metav1.Duration `json:"reconnectIntervalMax,omitempty"`

	// Multipart declares the publisher's framing convention. The default,
	// auto, detects topic-then-payload and single-frame per message, so a
	// producer that changes convention mid-stream is still read correctly; the
	// explicit values are advisory.
	// +kubebuilder:validation:Enum=auto;topicThenPayload;singleFrame
	// +kubebuilder:default=auto
	// +optional
	Multipart string `json:"multipart,omitempty"`
}

// NATSSpec configures a core NATS subscription.
type NATSSpec struct {
	// URL is the NATS server to connect to, such as nats://nats.default.svc:4222.
	// +kubebuilder:validation:MinLength=1
	URL string `json:"url"`

	// Subjects are the subjects to subscribe to, such as kv.events.>.
	// +kubebuilder:validation:MinItems=1
	Subjects []string `json:"subjects"`

	// QueueGroup must be empty. It exists so that setting it is rejected with
	// an explanation rather than silently accepted: a queue group distributes
	// messages across subscribers, so each driftwatch replica would see a
	// different subset of the stream and every one of them would compute a
	// different, incomplete expectation.
	// +optional
	QueueGroup string `json:"queueGroup,omitempty"`

	// CredentialsSecretRef names a secret holding NATS credentials. The
	// controller resolves it; its contents never reach status, events or logs.
	// +optional
	CredentialsSecretRef *SecretKeyRef `json:"credentialsSecretRef,omitempty"`
}

// FileSpec configures replay from a captured event stream.
type FileSpec struct {
	// Path is the newline-delimited capture to replay.
	// +kubebuilder:validation:MinLength=1
	Path string `json:"path"`

	// Speed is realtime to honor the capture's own timestamps, fast to replay
	// as quickly as possible, or a multiplier such as "2.0".
	// +kubebuilder:default="fast"
	// +optional
	Speed string `json:"speed,omitempty"`

	// Loop replays the capture endlessly. It cannot be used with stdin, which
	// has nothing to rewind to.
	// +optional
	Loop bool `json:"loop,omitempty"`
}

// SecretKeyRef names one key inside a secret in the same namespace.
//
// Cross-namespace references are deliberately not supported: they would let a
// DriftCheck in one namespace read a secret in another, which is a privilege
// escalation dressed as a convenience.
type SecretKeyRef struct {
	// Name is the secret's name, in the DriftCheck's own namespace.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Key is the entry within the secret.
	// +kubebuilder:validation:MinLength=1
	Key string `json:"key"`
}

// CodecSpec configures how transport frames become events.
type CodecSpec struct {
	// Type selects the decoder.
	//
	// The enum lists what the binary can actually decode rather than what the
	// design names, because an operator who sets an unregistered codec should
	// be told at apply time and not by a runner that fails to start after the
	// rollout has already begun.
	//
	// json is the fast path: a hand-written scanner, no allocation per event on
	// the happy path. msgpack takes the same field mapping and configuration
	// and costs one map allocation per event. template is a regular expression
	// with named capture groups, for line-oriented formats — a compatibility
	// escape hatch roughly an order of magnitude slower than json, and not
	// something to run at high throughput.
	// +kubebuilder:validation:Enum=json;msgpack;template
	// +kubebuilder:default=json
	// +optional
	Type string `json:"type,omitempty"`

	// MaxPayloadBytes rejects any frame larger than this, before it is
	// buffered. A producer able to make the auditor allocate arbitrarily per
	// frame is a denial-of-service vector.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1048576
	// +optional
	MaxPayloadBytes int `json:"maxPayloadBytes,omitempty"`

	// RetainRaw keeps the original wire bytes in each key's history, which
	// `driftwatch explain` can then show. Off by default because it costs
	// roughly 2 KB per retained event and because it puts payloads —
	// potentially sensitive — into memory driftwatch did not need to hold.
	// +optional
	RetainRaw bool `json:"retainRaw,omitempty"`

	// FieldMapping renames the producer's JSON fields onto driftwatch's, for
	// example publisher: replica_id. Keys must be driftwatch field names:
	// publisher, epoch, seq, timestamp, op, key, member, value, ttl, delta.
	// +optional
	FieldMapping map[string]string `json:"fieldMapping,omitempty"`

	// OpMapping translates the producer's operation names onto driftwatch's,
	// for example BLOCK_STORED: add. Values must be one of set, delete, add,
	// remove, incr, snapshotBegin, snapshotEnd or heartbeat.
	// +optional
	OpMapping map[string]string `json:"opMapping,omitempty"`
}

// ProjectionSpec configures how events fold into expected state.
type ProjectionSpec struct {
	// Type selects the fold. keysetOwnership maintains key to set-of-members,
	// which is the KV-cache index shape; scalar is last-write-wins; counter is
	// additive. Immutable after creation.
	// +kubebuilder:validation:Enum=keysetOwnership;scalar;counter
	// +kubebuilder:default=keysetOwnership
	Type string `json:"type"`

	// KeyTemplate builds the store key from the event, for example
	// "block:{{.Key}}". The available fields are .Key, .Member and .Publisher.
	// +kubebuilder:default="{{.Key}}"
	// +optional
	KeyTemplate string `json:"keyTemplate,omitempty"`

	// MemberTemplate builds a set member from the event. Used by the
	// keysetOwnership projection only.
	// +kubebuilder:default="{{.Member}}"
	// +optional
	MemberTemplate string `json:"memberTemplate,omitempty"`

	// MaxMembersPerKey bounds one key's member set. A set that can grow without
	// limit is an out-of-memory kill; past the bound the key is marked
	// truncated and its expectation is treated as approximate.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=100000
	// +optional
	MaxMembersPerKey int `json:"maxMembersPerKey,omitempty"`

	// IncrOnly declares that the producer only ever increments, which makes a
	// counter projection commutative and therefore insensitive to event order.
	// It is a promise about the producer that driftwatch cannot verify.
	// +optional
	IncrOnly bool `json:"incrOnly,omitempty"`

	// Ownership declares whether publishers write disjoint keyspaces. It exists
	// to scope the damage of a sequence gap: when a publisher's events go
	// missing, driftwatch cannot know which keys they touched, so without a
	// partition every key becomes suspect.
	// +optional
	Ownership *OwnershipSpec `json:"ownership,omitempty"`
}

// OwnershipSpec declares whether publishers own disjoint keyspaces.
type OwnershipSpec struct {
	// Partitioned states that each publisher writes only its own keys.
	// +optional
	Partitioned bool `json:"partitioned,omitempty"`

	// KeyPattern is the glob a given publisher may write, with {{.Publisher}}
	// expanded, for example "replica:{{.Publisher}}:*". Required when
	// partitioned is set, because it is what scopes suspicion to one publisher.
	// +optional
	KeyPattern string `json:"keyPattern,omitempty"`
}

// TargetSpec configures the store being audited.
type TargetSpec struct {
	// Type selects the store. Immutable after creation.
	// +kubebuilder:validation:Enum=redis;memory
	// +kubebuilder:default=redis
	Type string `json:"type"`

	// Redis configures the Redis client. Required when type is redis.
	// +optional
	Redis *RedisSpec `json:"redis,omitempty"`
}

// RedisSpec configures a Redis target. Every command driftwatch issues is on a
// read-only allowlist enforced by a client hook, so this connection cannot
// mutate the store even if something tried.
type RedisSpec struct {
	// Mode selects the deployment topology.
	// +kubebuilder:validation:Enum=standalone;sentinel;cluster
	// +kubebuilder:default=standalone
	// +optional
	Mode string `json:"mode,omitempty"`

	// Addr is the single server address, for standalone mode.
	// +optional
	Addr string `json:"addr,omitempty"`

	// Addrs are the cluster node or sentinel addresses. Required for cluster
	// and sentinel modes, and must be empty for standalone.
	// +optional
	Addrs []string `json:"addrs,omitempty"`

	// MasterName is the monitored primary's name. Required in sentinel mode.
	// +optional
	MasterName string `json:"masterName,omitempty"`

	// DB is the database index. Ignored in cluster mode, which has only one.
	// +kubebuilder:validation:Minimum=0
	// +optional
	DB int `json:"db,omitempty"`

	// Username for ACL authentication. The password comes from a secret.
	// +optional
	Username string `json:"username,omitempty"`

	// PasswordSecretRef names the secret holding the password. The value is
	// resolved by the controller and never copied into status, events or logs.
	// +optional
	PasswordSecretRef *SecretKeyRef `json:"passwordSecretRef,omitempty"`

	// TLS configures transport security to the store.
	// +optional
	TLS *TLSSpec `json:"tls,omitempty"`

	// KeyPattern is the glob the target-to-oracle scan walks, for example
	// "block:*". Narrow it to the keyspace this check owns: a pattern matching
	// keys no event produces reports every one of them as an extra.
	// +kubebuilder:default="*"
	// +optional
	KeyPattern string `json:"keyPattern,omitempty"`

	// ReadBatchSize is how many keys one pipelined read carries.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=500
	// +optional
	ReadBatchSize int `json:"readBatchSize,omitempty"`

	// ScanCount is the COUNT hint passed to SCAN. driftwatch never uses KEYS,
	// which blocks the server for the length of the keyspace.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1000
	// +optional
	ScanCount int `json:"scanCount,omitempty"`

	// DialTimeout bounds establishing a connection.
	// +kubebuilder:default="5s"
	// +optional
	DialTimeout metav1.Duration `json:"dialTimeout,omitempty"`

	// ReadTimeout bounds a single read.
	// +kubebuilder:default="3s"
	// +optional
	ReadTimeout metav1.Duration `json:"readTimeout,omitempty"`

	// PoolSize is the maximum number of connections.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=10
	// +optional
	PoolSize int `json:"poolSize,omitempty"`
}

// TLSSpec configures transport security to the target.
type TLSSpec struct {
	// Enabled turns on TLS.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// InsecureSkipVerify disables certificate verification. Supported because
	// internal CAs are sometimes unavoidable, and logged at WARN on every
	// startup so it cannot be forgotten.
	// +optional
	InsecureSkipVerify bool `json:"insecureSkipVerify,omitempty"`

	// CASecretRef names the secret holding a CA bundle to trust.
	// +optional
	CASecretRef *SecretKeyRef `json:"caSecretRef,omitempty"`
}

// PolicySpec decides when a disagreement counts as drift.
type PolicySpec struct {
	// SettlementWindow is how long a key must go unchanged before driftwatch
	// will compare it. It is the single most important setting here: too short
	// and a materializer that is merely behind is reported as broken, too long
	// and real drift takes longer to surface.
	// +kubebuilder:default={}
	// +optional
	SettlementWindow SettlementWindowSpec `json:"settlementWindow,omitempty"`

	// SweepInterval is how often the oracle-to-target comparison runs.
	// +kubebuilder:default="30s"
	// +optional
	SweepInterval metav1.Duration `json:"sweepInterval,omitempty"`

	// ExtraScanInterval is how often the target-to-oracle scan runs, looking
	// for keys no event created. It is slower than the sweep because extras
	// move slowly and the scan is the expensive half.
	// +kubebuilder:default="5m"
	// +optional
	ExtraScanInterval metav1.Duration `json:"extraScanInterval,omitempty"`

	// ReorderWindow is how long an event waits for its predecessor before
	// driftwatch concludes the predecessor was genuinely lost. Transports
	// reorder adjacent messages routinely, and folding them in arrival order
	// leaves the expectation permanently wrong for an order-dependent
	// projection.
	// +kubebuilder:default="2s"
	// +optional
	ReorderWindow metav1.Duration `json:"reorderWindow,omitempty"`

	// Bootstrap decides what driftwatch believes about keys that existed before
	// it attached. Adopt reads them in as a baseline and never asserts on them;
	// Wait ignores them until an event proves they exist; Strict asserts
	// nothing at all until a publisher retransmits its whole state.
	// +kubebuilder:validation:Enum=Adopt;Strict;Wait
	// +kubebuilder:default=Adopt
	// +optional
	Bootstrap string `json:"bootstrap,omitempty"`

	// ExpiryPolicy decides whether a key missing from the store is drift.
	// Strict reports every absence, which is correct for an index with no TTLs.
	// Ignore suppresses absences once the oracle's copy is older than
	// assumedTTL. Model expects the events to carry TTLs so the oracle expires
	// keys itself.
	// +kubebuilder:validation:Enum=Ignore;Model;Strict
	// +kubebuilder:default=Strict
	// +optional
	ExpiryPolicy string `json:"expiryPolicy,omitempty"`

	// AssumedTTL is the age past which the Ignore policy stops reporting a
	// missing key. Required when expiryPolicy is Ignore.
	// +optional
	AssumedTTL metav1.Duration `json:"assumedTTL,omitempty"`

	// TTLTolerance is how far two expiries may differ before it is a finding.
	// +kubebuilder:default="5s"
	// +optional
	TTLTolerance metav1.Duration `json:"ttlTolerance,omitempty"`

	// RequirePrimary refuses to compare against a replica. A replica is
	// legitimately behind its primary, so comparing against one manufactures
	// drift that does not exist.
	// +optional
	RequirePrimary bool `json:"requirePrimary,omitempty"`

	// MaxTrackedKeys bounds the oracle. On reaching it driftwatch evicts rather
	// than growing: a keyspace larger than expected degrades coverage instead
	// of killing the process. Budget roughly 700 bytes per key.
	// +kubebuilder:validation:Minimum=1000
	// +kubebuilder:validation:Maximum=100000000
	// +kubebuilder:default=1000000
	// +optional
	MaxTrackedKeys int `json:"maxTrackedKeys,omitempty"`

	// RingSize is how many recent events are retained per key for
	// `driftwatch explain`. Larger histories explain more and cost memory on
	// every tracked key.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1024
	// +kubebuilder:default=16
	// +optional
	RingSize int `json:"ringSize,omitempty"`

	// MaxConfirmQueue bounds the candidates awaiting a second read. Under mass
	// divergence the magnitude matters more than the per-key detail.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=10000
	// +optional
	MaxConfirmQueue int `json:"maxConfirmQueue,omitempty"`

	// MaxFindings caps the finding list in a report. The counts stay complete
	// past the cap, so magnitude is never lost even when detail is.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=10000
	// +optional
	MaxFindings int `json:"maxFindings,omitempty"`

	// MaxExtrasTracked bounds the first pass of the target-to-oracle scan.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=100000
	// +optional
	MaxExtrasTracked int `json:"maxExtrasTracked,omitempty"`

	// MaxPublishers bounds the per-publisher sequence state. Past it the oldest
	// publishers are evicted rather than memory growing without limit.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1024
	// +optional
	MaxPublishers int `json:"maxPublishers,omitempty"`

	// OracleShards is how many independent lock domains the oracle uses. Must
	// be a power of two.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1024
	// +kubebuilder:default=64
	// +optional
	OracleShards int `json:"oracleShards,omitempty"`

	// NeverSettledThreshold is how many multiples of the settlement window a
	// permanently busy key may stay in flight before the stability check
	// rescues it. Without it a hot key would never be compared at all.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=10
	// +optional
	NeverSettledThreshold int `json:"neverSettledThreshold,omitempty"`

	// Paused stops sweeping while continuing to ingest. It exists so alerting
	// can be silenced without discarding the oracle: stopping ingestion instead
	// would mean every key is suspect for as long as it takes to refill.
	// +optional
	Paused bool `json:"paused,omitempty"`
}

// SettlementWindowSpec configures how long a key must be quiet before it is
// compared.
type SettlementWindowSpec struct {
	// Mode selects a fixed window or one derived from measured convergence.
	// Adaptive is preferable when the materializer's latency varies, because a
	// static window has to be set for the worst case and then reports late.
	// +kubebuilder:validation:Enum=static;adaptive
	// +kubebuilder:default=adaptive
	// +optional
	Mode string `json:"mode,omitempty"`

	// Static is the window used in static mode, and the starting point in
	// adaptive mode.
	// +kubebuilder:default="5s"
	// +optional
	Static metav1.Duration `json:"static,omitempty"`

	// Min floors the adaptive window. An adaptive check starts here, because
	// with no measurements yet a window that widens as evidence arrives is
	// safer than one that narrows from a guess.
	// +kubebuilder:default="1s"
	// +optional
	Min metav1.Duration `json:"min,omitempty"`

	// Max ceilings the adaptive window. Reaching it means the materializer is
	// slower than driftwatch can meaningfully audit, and the condition says so.
	// +kubebuilder:default="120s"
	// +optional
	Max metav1.Duration `json:"max,omitempty"`

	// SafetyFactor multiplies the measured p99 convergence to give the window.
	// Must be at least 1.0; below that the window would be narrower than the
	// lag it was derived from, which reports normal operation as drift.
	// +kubebuilder:default="3.0"
	// +optional
	SafetyFactor string `json:"safetyFactor,omitempty"`
}

// AlertSpec describes when driftwatch believes divergence is worth paging for.
// It does not page anyone itself; a PrometheusRule reads these values.
type AlertSpec struct {
	// DivergentKeysThreshold is the absolute number of confirmed divergent keys
	// that should fire an alert.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=10
	// +optional
	DivergentKeysThreshold int `json:"divergentKeysThreshold,omitempty"`

	// DivergentRatioThreshold is the same as a fraction of tracked keys, which
	// scales with the keyspace where an absolute count does not.
	// +kubebuilder:default="0.001"
	// +optional
	DivergentRatioThreshold string `json:"divergentRatioThreshold,omitempty"`

	// ForDuration is how long a threshold must be exceeded before alerting.
	// +kubebuilder:default="60s"
	// +optional
	ForDuration metav1.Duration `json:"forDuration,omitempty"`

	// IncludeSuspect alerts on keys driftwatch knows it has an incomplete view
	// of. Leave it false: those findings measure driftwatch's own event loss
	// rather than the store's correctness, and paging on them trains an
	// operator to ignore the alert.
	// +optional
	IncludeSuspect bool `json:"includeSuspect,omitempty"`
}

// DriftCheckStatus reports what the check is doing and what it has found.
type DriftCheckStatus struct {
	// Phase is the check's lifecycle state. Degraded means it is running and
	// honest about not seeing everything, which is different from Failed and
	// must not page anyone the same way.
	// +kubebuilder:validation:Enum=Pending;Bootstrapping;AwaitingSnapshot;Watching;Degraded;Paused;Failed
	// +optional
	Phase string `json:"phase,omitempty"`

	// ObservedGeneration is the spec generation this status describes. A status
	// older than metadata.generation has not caught up with the last edit.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Message explains the current phase in one sentence.
	// +optional
	Message string `json:"message,omitempty"`

	// DivergentKeys is the number of confirmed divergent keys driftwatch will
	// stand behind. This is the number to alert on.
	// +optional
	DivergentKeys int `json:"divergentKeys,omitempty"`

	// SuspectDivergentKeys is the number of divergent keys whose event stream
	// driftwatch knows it partly missed. Never alert on it: it measures
	// driftwatch, not the store.
	// +optional
	SuspectDivergentKeys int `json:"suspectDivergentKeys,omitempty"`

	// DivergenceByCategory breaks the confirmed count down by kind of
	// disagreement, which is usually enough to guess the cause before opening
	// `driftwatch explain`.
	// +optional
	DivergenceByCategory map[string]int `json:"divergenceByCategory,omitempty"`

	// DriftDurationSeconds is the age of the oldest unresolved drift episode.
	// +optional
	DriftDurationSeconds string `json:"driftDurationSeconds,omitempty"`

	// TrackedKeys is how many keys the oracle currently holds.
	// +optional
	TrackedKeys int `json:"trackedKeys,omitempty"`

	// SettledKeys is how many of those are quiet enough to compare.
	// +optional
	SettledKeys int `json:"settledKeys,omitempty"`

	// InFlightKeys is how many changed inside the settlement window.
	// Disagreement on these is expected rather than meaningful.
	// +optional
	InFlightKeys int `json:"inFlightKeys,omitempty"`

	// CoverageRatio is the fraction of tracked keys the last sweep compared. A
	// high divergence count under a low coverage ratio is not what it looks
	// like.
	// +optional
	CoverageRatio string `json:"coverageRatio,omitempty"`

	// SettlementWindowSeconds is the window currently in force, which in
	// adaptive mode moves as convergence is measured.
	// +optional
	SettlementWindowSeconds string `json:"settlementWindowSeconds,omitempty"`

	// ConvergenceP99Seconds is the measured 99th-percentile delay between the
	// oracle learning a value and the store holding it. The adaptive window is
	// derived from it.
	// +optional
	ConvergenceP99Seconds string `json:"convergenceP99Seconds,omitempty"`

	// Publishers reports each publisher's sequence position, which is how an
	// operator sees whether driftwatch's own view is complete.
	// +optional
	Publishers []PublisherStatus `json:"publishers,omitempty"`

	// LastSweepTime is when the last comparison finished.
	// +optional
	LastSweepTime *metav1.Time `json:"lastSweepTime,omitempty"`

	// LastSweepDurationSeconds is how long it took. Approaching sweepInterval
	// means sweeps are about to start being skipped.
	// +optional
	LastSweepDurationSeconds string `json:"lastSweepDurationSeconds,omitempty"`

	// LastSweepKeysCompared is how many keys that sweep actually read.
	// +optional
	LastSweepKeysCompared int `json:"lastSweepKeysCompared,omitempty"`

	// SweepsSkipped counts sweeps skipped because the previous one was still
	// running.
	// +optional
	SweepsSkipped int64 `json:"sweepsSkipped,omitempty"`

	// TargetReachable reports whether the last health probe reached the store.
	// While it is false driftwatch reports no new findings at all, because
	// absence of data is not evidence of divergence.
	// +optional
	TargetReachable bool `json:"targetReachable,omitempty"`

	// TargetRole is the store's replication role, master or replica.
	// +optional
	TargetRole string `json:"targetRole,omitempty"`

	// TargetKeyspaceSize is how many keys the store reports holding.
	// +optional
	TargetKeyspaceSize int64 `json:"targetKeyspaceSize,omitempty"`

	// EventsApplied counts events folded into the oracle.
	// +optional
	EventsApplied int64 `json:"eventsApplied,omitempty"`

	// EventsDropped counts events that never reached the oracle. Every
	// increment is a hole in driftwatch's own view.
	// +optional
	EventsDropped int64 `json:"eventsDropped,omitempty"`

	// Conditions carry the standard Kubernetes condition set. Ready is the one
	// to gate on; the others explain why it is what it is.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// PublisherStatus reports one publisher's sequence position.
type PublisherStatus struct {
	// ID is the publisher's identity, as it appears in the events.
	ID string `json:"id"`

	// Epoch is the incarnation the publisher declared. It changes when the
	// publisher restarts and says so.
	// +optional
	Epoch int64 `json:"epoch,omitempty"`

	// HighWaterMark is the highest sequence number seen from this publisher.
	// +optional
	HighWaterMark int64 `json:"highWaterMark,omitempty"`

	// MissingEvents is how many of its sequence numbers are unaccounted for. A
	// non-zero value is why the affected keys are suspect rather than reported.
	// +optional
	MissingEvents int64 `json:"missingEvents,omitempty"`

	// Restarts counts how many times this publisher has restarted.
	// +optional
	Restarts int64 `json:"restarts,omitempty"`

	// LastSeenSeconds is how long ago its last event arrived.
	// +optional
	LastSeenSeconds string `json:"lastSeenSeconds,omitempty"`

	// ClockSkewSeconds is its wall clock minus driftwatch's. Diagnostic only:
	// settlement uses driftwatch's local receive time, so a skewed publisher
	// cannot affect what is compared or when.
	// +optional
	ClockSkewSeconds string `json:"clockSkewSeconds,omitempty"`
}

// The condition types a DriftCheck reports. Ready is the one to gate on; the
// rest exist so that "not ready" always comes with a reason an operator can act
// on rather than a bare false.
const (
	// ConditionReady is true when the check is running and asserting.
	ConditionReady = "Ready"
	// ConditionSourceConnected is true when every configured endpoint is
	// connected.
	ConditionSourceConnected = "SourceConnected"
	// ConditionTargetAvailable is true when the store answered the last probe.
	ConditionTargetAvailable = "TargetAvailable"
	// ConditionDriftDetected is true when confirmed divergence exists.
	ConditionDriftDetected = "DriftDetected"
	// ConditionOracleSaturated is true when the keyspace did not fit, so every
	// finding covers only part of the store.
	ConditionOracleSaturated = "OracleSaturated"
	// ConditionSequenceIntegrity is true when driftwatch has observed a
	// complete event sequence from every publisher.
	ConditionSequenceIntegrity = "SequenceIntegrity"
	// ConditionProjectionNotCommutative warns that a counter accepts absolute
	// writes, so a reordered stream converges to a different total.
	ConditionProjectionNotCommutative = "ProjectionNotCommutative"
	// ConditionSweepIntervalTight warns that sweeps are scheduled closer
	// together than two settlement windows, so candidates are re-raised before
	// the previous ones have finished confirming.
	ConditionSweepIntervalTight = "SweepIntervalTight"
	// ConditionMultiWriterUnsafe warns that two publishers have written the
	// same key under an order-dependent projection, so findings on that
	// keyspace reflect one arbitrary interleaving.
	ConditionMultiWriterUnsafe = "MultiWriterUnsafe"
	// ConditionAwaitingSnapshot is true under bootstrap Strict before any
	// publisher has retransmitted.
	ConditionAwaitingSnapshot = "AwaitingSnapshot"
)

// Finalizer is the finalizer driftwatch places on every DriftCheck, so that a
// deleted check stops its runner cleanly before the object disappears.
const Finalizer = "driftwatch.io/cleanup"

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=dc;dcheck,categories=driftwatch
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Drift",type=integer,JSONPath=`.status.divergentKeys`
// +kubebuilder:printcolumn:name="Suspect",type=integer,JSONPath=`.status.suspectDivergentKeys`
// +kubebuilder:printcolumn:name="Keys",type=integer,JSONPath=`.status.trackedKeys`
// +kubebuilder:printcolumn:name="Window",type=string,JSONPath=`.status.settlementWindowSeconds`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// DriftCheck audits one store against the event stream that feeds it.
//
// driftwatch derives what the store should contain by folding the events
// independently, then compares that expectation against what the store actually
// holds. It never writes to the store it audits.
type DriftCheck struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec describes the audit to run.
	Spec DriftCheckSpec `json:"spec,omitempty"`

	// Status reports what the check is doing and what it has found.
	// +optional
	Status DriftCheckStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DriftCheckList is a list of DriftCheck resources.
type DriftCheckList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	// Items are the checks.
	Items []DriftCheck `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DriftCheck{}, &DriftCheckList{})
}
