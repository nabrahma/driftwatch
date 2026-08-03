package check

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/nabrahma/driftwatch/pkg/codec"
	"github.com/nabrahma/driftwatch/pkg/differ"
	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/source"
	"github.com/nabrahma/driftwatch/pkg/target"
)

// Redacted is what replaces a secret in any rendering of a spec.
const Redacted = "[REDACTED]"

// ErrInvalidSpec wraps every configuration failure, so a caller can map the
// whole class onto the exit code §11 reserves for it without matching strings.
var ErrInvalidSpec = errors.New("invalid check specification")

// Spec is one audit's configuration.
//
// It is the same schema as `DriftCheck.spec` (§10.1), deliberately: a YAML file
// the CLI runs must be the file that goes into the cluster unchanged, or the
// demo proves something different from what gets deployed. The yaml tags match
// the CRD's json tags one for one.
type Spec struct {
	// Name and Namespace identify the check. Every log line and every metric
	// carries them, which is the only thing that makes a multi-check process
	// readable (§12.3).
	Name      string `yaml:"name"`
	Namespace string `yaml:"namespace"`

	Source     SourceSpec     `yaml:"source"`
	Codec      CodecSpec      `yaml:"codec"`
	Projection ProjectionSpec `yaml:"projection"`
	Target     TargetSpec     `yaml:"target"`
	Policy     PolicySpec     `yaml:"policy"`
	Alert      AlertSpec      `yaml:"alert"`
}

// SourceSpec configures the event transport.
type SourceSpec struct {
	Type string    `yaml:"type"`
	ZMQ  *ZMQSpec  `yaml:"zmq,omitempty"`
	NATS *NATSSpec `yaml:"nats,omitempty"`
	File *FileSpec `yaml:"file,omitempty"`

	// IngestBufferSize is the channel between the source and the applier. It
	// must exceed the socket's high-water mark, or loss happens inside the
	// socket where driftwatch can neither see it nor count it.
	IngestBufferSize int `yaml:"ingestBufferSize"`
}

// ZMQSpec configures the ZeroMQ SUB source.
type ZMQSpec struct {
	Endpoints            []string `yaml:"endpoints"`
	Topics               []string `yaml:"topics"`
	RecvHWM              int      `yaml:"recvHWM"`
	ConnectTimeout       Duration `yaml:"connectTimeout"`
	ReconnectIntervalMax Duration `yaml:"reconnectIntervalMax"`
	// IdleTimeout ends a session that has received nothing for this long, so
	// the source reconnects and re-resolves. See D-025.
	IdleTimeout Duration `yaml:"idleTimeout"`
	// Multipart is auto, topicThenPayload or singleFrame.
	Multipart string `yaml:"multipart"`
}

// NATSSpec configures the NATS source.
type NATSSpec struct {
	URL      string   `yaml:"url"`
	Subjects []string `yaml:"subjects"`
	// QueueGroup must be empty. It is present so that setting it is rejected
	// with an explanation rather than ignored; see §10.2.
	QueueGroup           string     `yaml:"queueGroup"`
	CredentialsSecretRef *SecretRef `yaml:"credentialsSecretRef,omitempty"`
}

// FileSpec configures replay from a capture.
type FileSpec struct {
	Path  string `yaml:"path"`
	Speed string `yaml:"speed"`
	Loop  bool   `yaml:"loop"`
}

// SecretRef names a Kubernetes secret. The CLI records it and does not resolve
// it; the controller does.
type SecretRef struct {
	Name string `yaml:"name"`
	Key  string `yaml:"key"`
}

// CodecSpec configures event decoding.
type CodecSpec struct {
	Type            string `yaml:"type"`
	MaxPayloadBytes int    `yaml:"maxPayloadBytes"`
	RetainRaw       bool   `yaml:"retainRaw"`
	// FieldMapping renames a producer's fields onto driftwatch's.
	FieldMapping map[string]string `yaml:"fieldMapping,omitempty"`
	// OpMapping maps a producer's operation names onto driftwatch's.
	OpMapping map[string]string `yaml:"opMapping,omitempty"`
}

// ProjectionSpec configures how events fold into expected state.
type ProjectionSpec struct {
	Type             string            `yaml:"type"`
	KeyTemplate      string            `yaml:"keyTemplate"`
	MemberTemplate   string            `yaml:"memberTemplate"`
	MaxMembersPerKey int               `yaml:"maxMembersPerKey"`
	IncrOnly         bool              `yaml:"incrOnly"`
	Ownership        *OwnershipSpec    `yaml:"ownership,omitempty"`
	Extra            map[string]string `yaml:"extra,omitempty"`
}

// OwnershipSpec declares whether publishers own disjoint keyspaces.
type OwnershipSpec struct {
	Partitioned bool   `yaml:"partitioned"`
	KeyPattern  string `yaml:"keyPattern"`
}

// TargetSpec configures the audited store.
type TargetSpec struct {
	Type  string     `yaml:"type"`
	Redis *RedisSpec `yaml:"redis,omitempty"`
	// Settings passes target-specific options straight through, which is how
	// the in-process memory target is told to fail or to be slow. Only the
	// memory target reads them; the Redis target is configured by RedisSpec.
	Settings map[string]string `yaml:"settings,omitempty"`
}

// RedisSpec configures the Redis target.
type RedisSpec struct {
	Mode       string   `yaml:"mode"`
	Addr       string   `yaml:"addr"`
	Addrs      []string `yaml:"addrs"`
	MasterName string   `yaml:"masterName"`
	DB         int      `yaml:"db"`
	Username   string   `yaml:"username"`
	// Password is for standalone use. In the cluster it comes from
	// PasswordSecretRef, and it is replaced by Redacted in every rendering.
	Password          string     `yaml:"password"`
	PasswordSecretRef *SecretRef `yaml:"passwordSecretRef,omitempty"`
	TLS               *TLSSpec   `yaml:"tls,omitempty"`

	KeyPattern    string   `yaml:"keyPattern"`
	ReadBatchSize int      `yaml:"readBatchSize"`
	ScanCount     int      `yaml:"scanCount"`
	DialTimeout   Duration `yaml:"dialTimeout"`
	ReadTimeout   Duration `yaml:"readTimeout"`
	PoolSize      int      `yaml:"poolSize"`
}

// TLSSpec configures transport security to the store.
type TLSSpec struct {
	Enabled            bool       `yaml:"enabled"`
	InsecureSkipVerify bool       `yaml:"insecureSkipVerify"`
	CASecretRef        *SecretRef `yaml:"caSecretRef,omitempty"`
}

// PolicySpec is everything about when and how to compare.
type PolicySpec struct {
	SettlementWindow SettlementWindowSpec `yaml:"settlementWindow"`

	SweepInterval     Duration `yaml:"sweepInterval"`
	ExtraScanInterval Duration `yaml:"extraScanInterval"`

	// Bootstrap is Adopt, Strict or Wait (§5.6).
	Bootstrap string `yaml:"bootstrap"`
	// ExpiryPolicy is Ignore, Model or Strict (§5.7).
	ExpiryPolicy string   `yaml:"expiryPolicy"`
	AssumedTTL   Duration `yaml:"assumedTTL"`
	TTLTolerance Duration `yaml:"ttlTolerance"`

	RequirePrimary bool `yaml:"requirePrimary"`

	// ReorderWindow is how long an event waits for its predecessor before
	// driftwatch concludes the predecessor is genuinely lost (§9 M6). Zero or
	// negative disables the buffer, which means a reordered stream folds in
	// arrival order and a non-commutative projection ends up wrong.
	ReorderWindow Duration `yaml:"reorderWindow"`

	MaxTrackedKeys        int  `yaml:"maxTrackedKeys"`
	RingSize              int  `yaml:"ringSize"`
	MaxConfirmQueue       int  `yaml:"maxConfirmQueue"`
	MaxFindings           int  `yaml:"maxFindings"`
	MaxExtrasTracked      int  `yaml:"maxExtrasTracked"`
	MaxPublishers         int  `yaml:"maxPublishers"`
	OracleShards          int  `yaml:"oracleShards"`
	NeverSettledThreshold int  `yaml:"neverSettledThreshold"`
	Paused                bool `yaml:"paused"`
}

// SettlementWindowSpec configures W (§5.3).
type SettlementWindowSpec struct {
	// Mode is static or adaptive.
	Mode   string   `yaml:"mode"`
	Static Duration `yaml:"static"`
	Min    Duration `yaml:"min"`
	Max    Duration `yaml:"max"`
	// SafetyFactor multiplies the measured p99. Never below 1.0.
	SafetyFactor Decimal `yaml:"safetyFactor"`
}

// AlertSpec configures when divergence is worth waking someone.
type AlertSpec struct {
	DivergentKeysThreshold  int      `yaml:"divergentKeysThreshold"`
	DivergentRatioThreshold Decimal  `yaml:"divergentRatioThreshold"`
	ForDuration             Duration `yaml:"forDuration"`
	// IncludeSuspect must stay false in any sane configuration. It exists so
	// that turning it on is a recorded decision rather than an accident.
	IncludeSuspect bool `yaml:"includeSuspect"`
}

// The bootstrap modes from §5.6.
const (
	BootstrapAdopt  = "Adopt"
	BootstrapStrict = "Strict"
	BootstrapWait   = "Wait"
)

// The expiry policies from §5.7.
const (
	ExpiryIgnore = "Ignore"
	ExpiryModel  = "Model"
	ExpiryStrict = "Strict"
)

// The settlement window modes.
const (
	WindowStatic   = "static"
	WindowAdaptive = "adaptive"
)

// The Redis deployment modes.
const (
	RedisStandalone = "standalone"
	RedisSentinel   = "sentinel"
	RedisCluster    = "cluster"
)

// The ZMQ multipart conventions from §10.1.
const (
	MultipartAuto          = "auto"
	MultipartTopicThenLoad = "topicThenPayload"
	MultipartSingleFrame   = "singleFrame"
)

// ---------------------------------------------------------------------------
// Scalars that YAML does not have.
// ---------------------------------------------------------------------------

// Duration is a time.Duration that reads Go's duration syntax from YAML.
//
// The CRD writes durations as "5s" and "2m", which yaml.v3 hands over as a
// string. Without this every duration in the spec would have to be an integer
// count of some unit the reader has to remember.
type Duration time.Duration

// Duration returns the underlying time.Duration.
func (d Duration) Duration() time.Duration { return time.Duration(d) }

// String renders the duration in Go's syntax.
func (d Duration) String() string { return time.Duration(d).String() }

// UnmarshalYAML parses "5s", "2m30s" and a bare number of seconds.
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	var raw string
	if err := node.Decode(&raw); err != nil {
		return fmt.Errorf("expected a duration such as \"5s\", got %s", node.Tag)
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		*d = 0
		return nil
	}

	parsed, err := time.ParseDuration(raw)
	if err != nil {
		// A bare number is accepted as seconds, because "30" for half a minute
		// is what people write and rejecting it teaches nothing.
		if secs, numErr := strconv.ParseFloat(raw, 64); numErr == nil {
			*d = Duration(time.Duration(secs * float64(time.Second)))
			return nil
		}
		return fmt.Errorf("%q is not a duration: use a form like \"5s\" or \"2m30s\"", raw)
	}
	*d = Duration(parsed)
	return nil
}

// MarshalYAML renders the duration as a string.
func (d Duration) MarshalYAML() (any, error) { return d.String(), nil }

// Decimal is a fractional value that reads from YAML as either a quoted string
// or a bare number.
//
// The CRD spells these as strings ("3.0", "0.001") because Kubernetes API
// conventions avoid floats, and a hand-written local file spells them as
// numbers. Both have to work or the same file cannot be used in both places.
type Decimal float64

// Float returns the value.
func (d Decimal) Float() float64 { return float64(d) }

// String renders the value without a trailing zero run.
func (d Decimal) String() string { return strconv.FormatFloat(float64(d), 'g', -1, 64) }

// ParseDecimal reads a Decimal from its string form.
//
// The CRD carries these as strings, so the webhook needs the same parse the
// YAML loader performs — and needs it to fail loudly, because a safetyFactor of
// "3.O" silently becoming zero would be reported as a bound violation on a
// number the operator never wrote.
func ParseDecimal(raw string) (Decimal, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0, nil
	}

	v, err := strconv.ParseFloat(trimmed, 64)
	if err != nil {
		return 0, fmt.Errorf("%q is not a number", raw)
	}
	return Decimal(v), nil
}

// UnmarshalYAML accepts a number or a quoted number.
func (d *Decimal) UnmarshalYAML(node *yaml.Node) error {
	var raw string
	if err := node.Decode(&raw); err != nil {
		return fmt.Errorf("expected a number, got %s", node.Tag)
	}
	if strings.TrimSpace(raw) == "" {
		*d = 0
		return nil
	}

	v, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		return fmt.Errorf("%q is not a number", raw)
	}
	*d = Decimal(v)
	return nil
}

// MarshalYAML renders the value as a string, matching the CRD.
func (d Decimal) MarshalYAML() (any, error) { return d.String(), nil }

// ---------------------------------------------------------------------------
// Loading.
// ---------------------------------------------------------------------------

// Load reads a spec from YAML.
//
// Unknown fields are an error. A typo in a policy key that silently kept the
// default would mean an operator believing they had raised a bound that is
// still at its default, and they would only find out from a metric.
func Load(r io.Reader) (Spec, error) {
	var spec Spec

	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)

	if err := dec.Decode(&spec); err != nil {
		if errors.Is(err, io.EOF) {
			return spec, fmt.Errorf("%w: the file is empty", ErrInvalidSpec)
		}
		return spec, fmt.Errorf("%w: %w", ErrInvalidSpec, err)
	}

	spec.ApplyDefaults()
	return spec, nil
}

// LoadFile reads a spec from a path, or from stdin when path is "-".
func LoadFile(path string) (Spec, error) {
	if path == "-" {
		return Load(os.Stdin)
	}

	f, err := os.Open(path) //nolint:gosec // the path is the operator's own config file
	if err != nil {
		return Spec{}, fmt.Errorf("%w: %w", ErrInvalidSpec, err)
	}
	defer f.Close() //nolint:errcheck // read-only

	return Load(f)
}

// ID returns the check's identity for logs and the `check` metric label.
func (s *Spec) ID() string {
	switch {
	case s.Name == "":
		return "default"
	case s.Namespace == "":
		return s.Name
	default:
		return s.Namespace + "/" + s.Name
	}
}

// Redact returns a copy with every secret replaced.
//
// §12.3 asks for the effective configuration to be logged once at startup,
// which saves an enormous amount of debugging time and is only safe if this
// exists and is the only way the spec is ever rendered.
func (s *Spec) Redact() Spec {
	out := *s

	if s.Target.Redis != nil {
		redis := *s.Target.Redis
		if redis.Password != "" {
			redis.Password = Redacted
		}
		out.Target.Redis = &redis
	}
	if s.Source.NATS != nil {
		nats := *s.Source.NATS
		out.Source.NATS = &nats
	}
	return out
}

// YAML renders the spec, with secrets redacted.
func (s *Spec) YAML() string {
	redacted := s.Redact()
	out, err := yaml.Marshal(&redacted)
	if err != nil {
		return fmt.Sprintf("<spec could not be rendered: %v>", err)
	}
	return string(out)
}

// ---------------------------------------------------------------------------
// Defaults.
// ---------------------------------------------------------------------------

// The defaults from §10.1. They are named rather than written inline so that
// the documentation, the defaulting webhook and the tests can all cite one
// number.
const (
	DefaultIngestBufferSize      = 200_000
	DefaultRecvHWM               = 100_000
	DefaultMaxPayloadBytes       = 1 << 20
	DefaultMaxMembersPerKey      = 100_000
	DefaultReadBatchSize         = 500
	DefaultScanCount             = 1000
	DefaultMaxTrackedKeys        = 1_000_000
	DefaultRingSize              = 16
	DefaultMaxConfirmQueue       = 10_000
	DefaultMaxFindings           = 10_000
	DefaultMaxExtrasTracked      = 100_000
	DefaultMaxPublishers         = 1024
	DefaultOracleShards          = 64
	DefaultNeverSettledThreshold = 10
	DefaultDivergentKeys         = 10

	DefaultSweepInterval     = 30 * time.Second
	DefaultExtraScanInterval = 5 * time.Minute
	DefaultStaticWindow      = 5 * time.Second
	DefaultMinWindow         = time.Second
	DefaultMaxWindow         = 120 * time.Second
	DefaultTTLTolerance      = 5 * time.Second
	DefaultReorderWindow     = 2 * time.Second
	DefaultDialTimeout       = 5 * time.Second
	DefaultReadTimeout       = 3 * time.Second
	DefaultConnectTimeout    = 5 * time.Second
	DefaultReconnectMax      = 30 * time.Second
	DefaultIdleTimeout       = 60 * time.Second
	DefaultForDuration       = 60 * time.Second

	DefaultSafetyFactor   = 3.0
	DefaultDivergentRatio = 0.001
	DefaultRedisPoolSize  = 10
	DefaultKeyPatternGlob = "*"
	DefaultKeyTemplate    = "{{.Key}}"
	DefaultMemberTemplate = "{{.Member}}"
)

// ApplyDefaults fills every optional field.
//
// Everything downstream then reads a fully-specified struct, and
// `driftwatch diff --output json` can print the effective configuration that is
// actually running rather than the sparse thing the operator typed.
func (s *Spec) ApplyDefaults() {
	if s.Name == "" {
		s.Name = "default"
	}

	s.defaultSource()
	s.defaultCodec()
	s.defaultProjection()
	s.defaultTarget()
	s.defaultPolicy()
	s.defaultAlert()
}

func (s *Spec) defaultSource() {
	if s.Source.Type == "" {
		s.Source.Type = "memory"
	}
	if s.Source.IngestBufferSize <= 0 {
		s.Source.IngestBufferSize = DefaultIngestBufferSize
	}

	if z := s.Source.ZMQ; z != nil {
		if z.RecvHWM <= 0 {
			z.RecvHWM = DefaultRecvHWM
		}
		if z.ConnectTimeout <= 0 {
			z.ConnectTimeout = Duration(DefaultConnectTimeout)
		}
		if z.ReconnectIntervalMax <= 0 {
			z.ReconnectIntervalMax = Duration(DefaultReconnectMax)
		}
		if z.IdleTimeout <= 0 {
			z.IdleTimeout = Duration(DefaultIdleTimeout)
		}
		if z.Multipart == "" {
			z.Multipart = MultipartAuto
		}
	}
	if f := s.Source.File; f != nil && f.Speed == "" {
		f.Speed = "fast"
	}
}

func (s *Spec) defaultCodec() {
	if s.Codec.Type == "" {
		s.Codec.Type = "json"
	}
	if s.Codec.MaxPayloadBytes <= 0 {
		s.Codec.MaxPayloadBytes = DefaultMaxPayloadBytes
	}
}

func (s *Spec) defaultProjection() {
	if s.Projection.Type == "" {
		s.Projection.Type = "scalar"
	}
	if s.Projection.KeyTemplate == "" {
		s.Projection.KeyTemplate = DefaultKeyTemplate
	}
	if s.Projection.MemberTemplate == "" {
		s.Projection.MemberTemplate = DefaultMemberTemplate
	}
	if s.Projection.MaxMembersPerKey <= 0 {
		s.Projection.MaxMembersPerKey = DefaultMaxMembersPerKey
	}
}

func (s *Spec) defaultTarget() {
	if s.Target.Type == "" {
		s.Target.Type = "memory"
	}
	if s.Target.Type != "redis" {
		return
	}
	if s.Target.Redis == nil {
		s.Target.Redis = &RedisSpec{}
	}

	r := s.Target.Redis
	if r.Mode == "" {
		r.Mode = RedisStandalone
	}
	if r.KeyPattern == "" {
		r.KeyPattern = DefaultKeyPatternGlob
	}
	if r.ReadBatchSize <= 0 {
		r.ReadBatchSize = DefaultReadBatchSize
	}
	if r.ScanCount <= 0 {
		r.ScanCount = DefaultScanCount
	}
	if r.DialTimeout <= 0 {
		r.DialTimeout = Duration(DefaultDialTimeout)
	}
	if r.ReadTimeout <= 0 {
		r.ReadTimeout = Duration(DefaultReadTimeout)
	}
	if r.PoolSize <= 0 {
		r.PoolSize = DefaultRedisPoolSize
	}
}

//nolint:gocyclo // one branch per defaulted field; splitting it would only move the list
func (s *Spec) defaultPolicy() {
	p := &s.Policy

	if p.SettlementWindow.Mode == "" {
		p.SettlementWindow.Mode = WindowStatic
	}
	if p.SettlementWindow.Static <= 0 {
		p.SettlementWindow.Static = Duration(DefaultStaticWindow)
	}
	if p.SettlementWindow.Min <= 0 {
		p.SettlementWindow.Min = Duration(DefaultMinWindow)
	}
	if p.SettlementWindow.Max <= 0 {
		p.SettlementWindow.Max = Duration(DefaultMaxWindow)
	}
	if p.SettlementWindow.SafetyFactor <= 0 {
		p.SettlementWindow.SafetyFactor = DefaultSafetyFactor
	}

	if p.SweepInterval <= 0 {
		p.SweepInterval = Duration(DefaultSweepInterval)
	}
	if p.ExtraScanInterval <= 0 {
		p.ExtraScanInterval = Duration(DefaultExtraScanInterval)
	}
	if p.Bootstrap == "" {
		p.Bootstrap = BootstrapAdopt
	}
	if p.ExpiryPolicy == "" {
		p.ExpiryPolicy = ExpiryStrict
	}
	if p.TTLTolerance <= 0 {
		p.TTLTolerance = Duration(DefaultTTLTolerance)
	}
	if p.ReorderWindow == 0 {
		p.ReorderWindow = Duration(DefaultReorderWindow)
	}
	if p.MaxTrackedKeys <= 0 {
		p.MaxTrackedKeys = DefaultMaxTrackedKeys
	}
	if p.RingSize <= 0 {
		p.RingSize = DefaultRingSize
	}
	if p.MaxConfirmQueue <= 0 {
		p.MaxConfirmQueue = DefaultMaxConfirmQueue
	}
	if p.MaxFindings <= 0 {
		p.MaxFindings = DefaultMaxFindings
	}
	if p.MaxExtrasTracked <= 0 {
		p.MaxExtrasTracked = DefaultMaxExtrasTracked
	}
	if p.MaxPublishers <= 0 {
		p.MaxPublishers = DefaultMaxPublishers
	}
	if p.OracleShards <= 0 {
		p.OracleShards = DefaultOracleShards
	}
	if p.NeverSettledThreshold <= 0 {
		p.NeverSettledThreshold = DefaultNeverSettledThreshold
	}
}

func (s *Spec) defaultAlert() {
	if s.Alert.DivergentKeysThreshold <= 0 {
		s.Alert.DivergentKeysThreshold = DefaultDivergentKeys
	}
	if s.Alert.DivergentRatioThreshold <= 0 {
		s.Alert.DivergentRatioThreshold = DefaultDivergentRatio
	}
	if s.Alert.ForDuration <= 0 {
		s.Alert.ForDuration = Duration(DefaultForDuration)
	}
}

// ---------------------------------------------------------------------------
// Translation into the subsystems' own configuration.
// ---------------------------------------------------------------------------

// SourceSettings renders the source-specific settings.
func (s *Spec) SourceSettings() map[string]string {
	out := map[string]string{}

	switch s.Source.Type {
	case "zmq":
		if z := s.Source.ZMQ; z != nil {
			out["endpoints"] = strings.Join(z.Endpoints, ",")
			out["topics"] = strings.Join(z.Topics, ",")
			out["recvHWM"] = strconv.Itoa(z.RecvHWM)
			out["connectTimeout"] = z.ConnectTimeout.String()
			out["reconnectIntervalMax"] = z.ReconnectIntervalMax.String()
			out["idleTimeout"] = z.IdleTimeout.String()
		}
	case "nats":
		if n := s.Source.NATS; n != nil {
			out["url"] = n.URL
			out["subjects"] = strings.Join(n.Subjects, ",")
			out["queueGroup"] = n.QueueGroup
		}
	case "file":
		if f := s.Source.File; f != nil {
			out["path"] = f.Path
			out["speed"] = fileSpeed(f.Speed)
			out["loop"] = strconv.FormatBool(f.Loop)
		}
	case "memory":
		out["buffer"] = strconv.Itoa(s.Source.IngestBufferSize)
	}
	return out
}

// SourceConfig renders the full source configuration.
func (s *Spec) SourceConfig() source.Config {
	return source.Config{
		Settings:        s.SourceSettings(),
		MaxPayloadBytes: s.Codec.MaxPayloadBytes,
	}
}

// CodecConfig renders the codec configuration.
func (s *Spec) CodecConfig() map[string]string {
	out := map[string]string{
		"maxPayloadBytes": strconv.Itoa(s.Codec.MaxPayloadBytes),
		"retainRaw":       strconv.FormatBool(s.Codec.RetainRaw),
	}

	for field, name := range s.Codec.FieldMapping {
		if key, ok := codecFieldKeys[field]; ok {
			out[key] = name
		}
	}
	if len(s.Codec.OpMapping) > 0 {
		pairs := make([]string, 0, len(s.Codec.OpMapping))
		for from, to := range s.Codec.OpMapping {
			pairs = append(pairs, from+"="+to)
		}
		sort.Strings(pairs) // stable, so the rendered config is comparable
		out["opMapping"] = strings.Join(pairs, ",")
	}
	return out
}

// The replay speeds §10.1 names. They are the operator-facing spelling; the
// file source's own vocabulary differs, and fileSpeed translates between them.
const (
	SpeedRealtime = "realtime"
	SpeedFast     = "fast"
)

// fileSpeed maps the spec's speed onto the file source's.
//
// §10.1 spells "as fast as possible" as `fast`, and pkg/source spells it
// `asFastAsPossible`. Two vocabularies for one concept is a mistake, but the
// CRD's is the published one and the source's is the descriptive one, so the
// translation lives here rather than either side changing to match the other.
func fileSpeed(speed string) string {
	if speed == SpeedFast || speed == "" {
		return source.SpeedAsFastAsPossible
	}
	return speed
}

// codecFieldKeys maps a spec fieldMapping key onto the codec's config key.
//
// The spec names the event field ("publisher") and the codec takes the option
// that renames it ("publisherField"). Keeping the translation in one table is
// what lets validation reject an unknown field by name rather than letting it
// through as a silently ignored option.
var codecFieldKeys = map[string]string{
	"publisher": "publisherField",
	"epoch":     "epochField",
	"seq":       "seqField",
	"timestamp": "timestampField",
	"op":        "opField",
	"key":       "keyField",
	"member":    "memberField",
	"value":     "valueField",
	"ttl":       "ttlField",
	"delta":     "deltaField",
}

// CodecFields returns the event field names a fieldMapping may rename, sorted.
func CodecFields() []string {
	out := make([]string, 0, len(codecFieldKeys))
	for field := range codecFieldKeys {
		out = append(out, field)
	}
	sort.Strings(out)
	return out
}

// ProjectionConfig renders the projection configuration.
func (s *Spec) ProjectionConfig() map[string]string {
	out := map[string]string{
		"keyTemplate":      s.Projection.KeyTemplate,
		"memberTemplate":   s.Projection.MemberTemplate,
		"maxMembersPerKey": strconv.Itoa(s.Projection.MaxMembersPerKey),
		"incrOnly":         strconv.FormatBool(s.Projection.IncrOnly),
	}
	if o := s.Projection.Ownership; o != nil && o.Partitioned {
		out["keyPattern"] = o.KeyPattern
	}
	for k, v := range s.Projection.Extra {
		out[k] = v
	}
	return out
}

// TargetSettings renders the target-specific settings.
func (s *Spec) TargetSettings() map[string]string {
	out := map[string]string{}

	for k, v := range s.Target.Settings {
		out[k] = v
	}

	if s.Target.Type != "redis" || s.Target.Redis == nil {
		return out
	}

	r := s.Target.Redis
	addrs := r.Addrs
	if r.Addr != "" {
		addrs = append([]string{r.Addr}, addrs...)
	}

	out["addrs"] = strings.Join(addrs, ",")
	out["masterName"] = r.MasterName
	out["username"] = r.Username
	out["password"] = r.Password
	out["db"] = strconv.Itoa(r.DB)
	out["batchSize"] = strconv.Itoa(r.ReadBatchSize)
	out["scanCount"] = strconv.Itoa(r.ScanCount)
	return out
}

// ExtraScanPattern returns the keyspace glob the target-to-oracle scan walks.
func (s *Spec) ExtraScanPattern() string {
	if s.Target.Redis != nil && s.Target.Redis.KeyPattern != "" {
		return s.Target.Redis.KeyPattern
	}
	return DefaultKeyPatternGlob
}

// DifferOptions renders the comparison policy.
func (s *Spec) DifferOptions() differ.Options {
	return differ.Options{
		ExpiryPolicy: s.expiryPolicy(),
		AssumedTTL:   s.Policy.AssumedTTL.Duration(),
		TTLTolerance: s.Policy.TTLTolerance.Duration(),
		MaxFindings:  s.Policy.MaxFindings,
	}
}

func (s *Spec) expiryPolicy() differ.ExpiryPolicy {
	switch s.Policy.ExpiryPolicy {
	case ExpiryIgnore:
		return differ.ExpiryIgnore
	case ExpiryModel:
		return differ.ExpiryModel
	default:
		return differ.ExpiryStrict
	}
}

// ---------------------------------------------------------------------------
// Validation.
// ---------------------------------------------------------------------------

// FieldError names one invalid field.
type FieldError struct {
	Field   string
	Message string
}

// Error renders the field and its problem.
func (e FieldError) Error() string { return e.Field + ": " + e.Message }

// ValidationError collects every problem found in one spec.
//
// Every problem, not the first. An operator fixing a config file one error per
// run is an operator who gives up on the tool, and §9 M14 requires the whole
// spec to be validated before anything is started.
type ValidationError struct {
	Errors []FieldError
}

// Error renders every field error, one per line.
func (e *ValidationError) Error() string {
	if len(e.Errors) == 1 {
		return "invalid check specification: " + e.Errors[0].Error()
	}

	var b strings.Builder
	fmt.Fprintf(&b, "invalid check specification: %d problems", len(e.Errors))
	for _, fe := range e.Errors {
		b.WriteString("\n  - ")
		b.WriteString(fe.Error())
	}
	return b.String()
}

// Is reports a match against ErrInvalidSpec so callers can map the whole class
// onto one exit code.
func (e *ValidationError) Is(err error) bool { return err == ErrInvalidSpec }

// Fields returns the offending field paths, sorted.
func (e *ValidationError) Fields() []string {
	out := make([]string, 0, len(e.Errors))
	for _, fe := range e.Errors {
		out = append(out, fe.Field)
	}
	sort.Strings(out)
	return out
}

// validator accumulates field errors and warnings.
type validator struct {
	errs     []FieldError
	warnings []string
}

func (v *validator) fail(field, format string, args ...any) {
	v.errs = append(v.errs, FieldError{Field: field, Message: fmt.Sprintf(format, args...)})
}

func (v *validator) warn(format string, args ...any) {
	v.warnings = append(v.warnings, fmt.Sprintf(format, args...))
}

// Validate checks the whole spec, returning every problem at once.
//
// It must be called before anything is constructed. §9 M14 is explicit that a
// misconfigured check has to fail with a precise error naming the offending
// field, rather than fail halfway up and leave sockets open.
func (s *Spec) Validate() error {
	v := &validator{}

	s.validateSource(v)
	s.validateCodec(v)
	s.validateProjection(v)
	s.validateTarget(v)
	s.validatePolicy(v)

	if len(v.errs) == 0 {
		return nil
	}
	return &ValidationError{Errors: v.errs}
}

// ValidateUpdate checks a spec against the one it replaces.
//
// §10.2 makes projection.type and target.type immutable, and the reason is not
// tidiness. The oracle holds values in the projection's shape and the sweeper
// reads the store in it; changing either under a running check leaves every
// tracked key holding a value the new projection cannot fold and the new target
// cannot read. Every key would report a shape mismatch, which reads exactly
// like the store having been rewritten by something.
//
// The webhook is a thin adapter over this. The rule lives here so it
// is enforced by the same code whether the spec arrives through kubectl or
// through -f, and so it is testable without a cluster (§15 row 59).
func (s *Spec) ValidateUpdate(previous *Spec) error {
	if previous == nil {
		return nil
	}

	v := &validator{}
	if previous.Projection.Type != "" && previous.Projection.Type != s.Projection.Type {
		v.fail("projection.type",
			"field is immutable; delete and recreate the DriftCheck (was %q, now %q)",
			previous.Projection.Type, s.Projection.Type)
	}
	if previous.Target.Type != "" && previous.Target.Type != s.Target.Type {
		v.fail("target.type",
			"field is immutable; delete and recreate the DriftCheck (was %q, now %q)",
			previous.Target.Type, s.Target.Type)
	}

	if len(v.errs) == 0 {
		return nil
	}
	return &ValidationError{Errors: v.errs}
}

// Warnings returns configurations that are legal but probably not what the
// operator meant. They surface as CRD conditions in the cluster and as stderr
// lines in the CLI, and never block startup.
func (s *Spec) Warnings() []string {
	v := &validator{}

	s.validateSource(v)
	s.validateCodec(v)
	s.validateProjection(v)
	s.validateTarget(v)
	s.validatePolicy(v)

	return v.warnings
}

func (s *Spec) validateSource(v *validator) {
	if !contains(source.Registered(), s.Source.Type) {
		v.fail("source.type", "unknown source type %q, valid: %v",
			s.Source.Type, source.Registered())
		return
	}

	switch s.Source.Type {
	case "zmq":
		s.validateZMQ(v)
	case "nats":
		s.validateNATS(v)
	case "file":
		s.validateFile(v)
	}
}

func (s *Spec) validateFile(v *validator) {
	f := s.Source.File
	if f == nil || f.Path == "" {
		v.fail("source.file.path", "required when source.type is file")
		return
	}

	switch f.Speed {
	case SpeedRealtime, SpeedFast, "":
	default:
		if rate, err := strconv.ParseFloat(f.Speed, 64); err != nil || rate <= 0 {
			v.fail("source.file.speed",
				"must be %q, %q or a positive multiplier such as \"2.0\", got %q",
				SpeedRealtime, SpeedFast, f.Speed)
		}
	}

	if f.Loop && f.Path == "-" {
		v.fail("source.file.loop", "cannot be used with stdin, which cannot be rewound")
	}
}

func (s *Spec) validateZMQ(v *validator) {
	z := s.Source.ZMQ
	if z == nil || len(z.Endpoints) == 0 {
		v.fail("source.zmq.endpoints", "at least one endpoint is required")
		return
	}

	for i, ep := range z.Endpoints {
		if err := validateZMQEndpoint(ep); err != nil {
			v.fail(fmt.Sprintf("source.zmq.endpoints[%d]", i), "invalid endpoint: %v", err)
		}
	}

	// The rule §10.2 spells out in full, because getting it wrong is invisible.
	// A socket whose high-water mark exceeds the buffer behind it drops frames
	// inside the transport, where driftwatch cannot see them, cannot count them
	// and cannot mark the affected keys suspect.
	if s.Source.IngestBufferSize < z.RecvHWM {
		v.fail("policy",
			"ingestBufferSize (%d) must be >= source.zmq.recvHWM (%d), "+
				"otherwise event loss occurs invisibly in the socket",
			s.Source.IngestBufferSize, z.RecvHWM)
	}

	switch z.Multipart {
	case MultipartAuto:
	case MultipartTopicThenLoad, MultipartSingleFrame:
		v.warn("source.zmq.multipart=%s is advisory: the ZMQ source detects both "+
			"conventions per frame, so a producer that changes convention mid-stream "+
			"is still read correctly", z.Multipart)
	default:
		v.fail("source.zmq.multipart", "must be one of %s, %s, %s",
			MultipartAuto, MultipartTopicThenLoad, MultipartSingleFrame)
	}
}

func (s *Spec) validateNATS(v *validator) {
	n := s.Source.NATS
	if n == nil || n.URL == "" {
		v.fail("source.nats.url", "required when source.type is nats")
		return
	}
	if len(n.Subjects) == 0 {
		v.fail("source.nats.subjects", "at least one subject is required")
	}

	// The rule worth failing loudly on. A queue group looks like sensible
	// horizontal scaling and quietly destroys the oracle: each replica would
	// see a different subset of events, so every one of them would compute a
	// different, incomplete expectation and report the store as wrong.
	if n.QueueGroup != "" {
		v.fail("source.nats.queueGroup", "%s", source.ErrQueueGroupSet.Error())
	}
}

// validateZMQEndpoint checks the transport and address of a ZMQ URI.
func validateZMQEndpoint(endpoint string) error {
	scheme, rest, found := strings.Cut(endpoint, "://")
	if !found {
		return errors.New("expected a form like tcp://host:port")
	}

	switch scheme {
	case "tcp":
		host, port, ok := strings.Cut(rest, ":")
		if !ok || host == "" || port == "" {
			return errors.New("tcp endpoints need a host and a port")
		}
		if _, err := strconv.Atoi(port); err != nil {
			return fmt.Errorf("port %q is not a number", port)
		}
	case "ipc", "inproc":
		if rest == "" {
			return fmt.Errorf("%s endpoints need a path", scheme)
		}
	default:
		return fmt.Errorf("unsupported transport %q, use tcp, ipc or inproc", scheme)
	}
	return nil
}

func (s *Spec) validateCodec(v *validator) {
	if !contains(codec.Names(), s.Codec.Type) {
		v.fail("codec.type", "unknown codec type %q, valid: %v", s.Codec.Type, codec.Names())
	}

	for field := range s.Codec.FieldMapping {
		if _, ok := codecFieldKeys[field]; !ok {
			v.fail("codec.fieldMapping", "unknown field %q, valid: %v", field, CodecFields())
		}
	}
	for from, to := range s.Codec.OpMapping {
		if _, err := event.ParseOp(to); err != nil {
			v.fail(fmt.Sprintf("codec.opMapping[%q]", from), "unknown op %q", to)
		}
	}

	if s.Codec.MaxPayloadBytes <= 0 {
		v.fail("codec.maxPayloadBytes", "must be positive")
	}
}

func (s *Spec) validateProjection(v *validator) {
	if !contains(projection.Names(), s.Projection.Type) {
		v.fail("projection.type", "unknown projection type %q, valid: %v",
			s.Projection.Type, projection.Names())
		return
	}

	if _, err := projection.New(s.Projection.Type, s.ProjectionConfig()); err != nil {
		v.fail("projection", "%v", err)
	}

	// §10.2 makes this a warning rather than an error, and rightly: a counter
	// fed anything but increments is order-dependent, so a reordered stream
	// reaches a different total. That is a real risk and not always avoidable,
	// so it is surfaced as a condition rather than a refusal to start.
	if s.Projection.Type == "counter" && !s.Projection.IncrOnly {
		v.warn("projection: a counter that accepts absolute sets is not commutative, " +
			"so a reordered stream converges to a different total (condition " +
			"ProjectionNotCommutative); set projection.incrOnly if the producer only increments")
	}

	if o := s.Projection.Ownership; o != nil && o.Partitioned && o.KeyPattern == "" {
		v.fail("projection.ownership.keyPattern",
			"required when partitioned is set, or a gap cannot be scoped to one publisher")
	}
}

func (s *Spec) validateTarget(v *validator) {
	if !contains(target.Names(), s.Target.Type) {
		v.fail("target.type", "unknown target type %q, valid: %v",
			s.Target.Type, target.Names())
		return
	}
	if s.Target.Type != "redis" {
		return
	}

	r := s.Target.Redis
	if r == nil {
		v.fail("target.redis", "required when target.type is redis")
		return
	}

	switch r.Mode {
	case RedisStandalone:
		if r.Addr == "" && len(r.Addrs) == 0 {
			v.fail("target.redis.addr", "required in standalone mode")
		}
	case RedisCluster:
		if len(r.Addrs) == 0 {
			v.fail("target.redis.addrs", "required in cluster mode")
		}
		if r.Addr != "" {
			v.fail("target.redis.addr", "must be empty in cluster mode; use addrs")
		}
	case RedisSentinel:
		if len(r.Addrs) == 0 {
			v.fail("target.redis.addrs", "required in sentinel mode: the sentinel addresses")
		}
		if r.MasterName == "" {
			v.fail("target.redis.masterName", "required in sentinel mode")
		}
	default:
		v.fail("target.redis.mode", "must be one of %s, %s, %s",
			RedisStandalone, RedisSentinel, RedisCluster)
	}

	if err := validateGlob(r.KeyPattern); err != nil {
		v.fail("target.redis.keyPattern", "invalid glob: %v", err)
	}
	if r.Password != "" && r.PasswordSecretRef != nil {
		v.fail("target.redis.password",
			"set together with passwordSecretRef; use one or the other")
	}
}

// validateGlob rejects a pattern Redis cannot match, which is a pattern with an
// unclosed character class.
//
// A bad glob is worth catching at startup rather than at the first extras scan:
// SCAN with an unmatched pattern returns nothing, which reads exactly like a
// store with no keys in it.
func validateGlob(pattern string) error {
	if pattern == "" {
		return errors.New("must not be empty")
	}

	depth := 0
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '\\':
			i++ // the next byte is literal
		case '[':
			depth++
		case ']':
			depth--
			if depth < 0 {
				return errors.New("unmatched ']'")
			}
		}
	}
	if depth != 0 {
		return errors.New("unmatched '['")
	}
	return nil
}

//nolint:gocyclo // a validation table reads better as one list than as five functions
func (s *Spec) validatePolicy(v *validator) {
	p := &s.Policy
	w := &p.SettlementWindow

	switch w.Mode {
	case WindowStatic, WindowAdaptive:
	default:
		v.fail("policy.settlementWindow.mode", "must be %s or %s", WindowStatic, WindowAdaptive)
	}

	if w.Min > w.Static || w.Static > w.Max {
		v.fail("policy.settlementWindow",
			"min (%s) <= static (%s) <= max (%s) does not hold", w.Min, w.Static, w.Max)
	}
	if w.SafetyFactor < 1.0 {
		v.fail("policy.settlementWindow.safetyFactor", "must be >= 1.0, got %s", w.SafetyFactor)
	}

	if p.SweepInterval < Duration(time.Second) {
		v.fail("policy.sweepInterval", "must be at least 1s, got %s", p.SweepInterval)
	}
	if p.ExtraScanInterval < Duration(time.Second) {
		v.fail("policy.extraScanInterval", "must be at least 1s, got %s", p.ExtraScanInterval)
	}

	// A sweep interval shorter than two settlement windows means the next sweep
	// starts before the previous one's candidates have finished confirming, so
	// the confirm queue grows and the same key is raised repeatedly.
	if effective := s.EffectiveWindow(); p.SweepInterval.Duration() < 2*effective {
		v.warn("policy.sweepInterval (%s) is less than twice the settlement window (%s): "+
			"candidates will still be awaiting confirmation when the next sweep raises them "+
			"again (condition SweepIntervalTight)", p.SweepInterval, effective)
	}

	switch p.Bootstrap {
	case BootstrapAdopt, BootstrapWait:
	case BootstrapStrict:
		// Strict waits for a snapshot cycle, which it can only recognize if the
		// codec knows how to decode one.
		if !s.mapsSnapshotOps() {
			v.fail("policy.bootstrap",
				"policy.bootstrap=Strict requires codec.opMapping to define "+
					"snapshotBegin and snapshotEnd")
		}
	default:
		v.fail("policy.bootstrap", "must be one of %s, %s, %s",
			BootstrapAdopt, BootstrapStrict, BootstrapWait)
	}

	switch p.ExpiryPolicy {
	case ExpiryModel, ExpiryStrict:
	case ExpiryIgnore:
		if p.AssumedTTL <= 0 {
			v.fail("policy.assumedTTL", "must be positive when expiryPolicy is Ignore")
		}
	default:
		v.fail("policy.expiryPolicy", "must be one of %s, %s, %s",
			ExpiryIgnore, ExpiryModel, ExpiryStrict)
	}

	if p.MaxTrackedKeys < 1_000 || p.MaxTrackedKeys > 100_000_000 {
		v.fail("policy.maxTrackedKeys", "must be between 1,000 and 100,000,000, got %d",
			p.MaxTrackedKeys)
	}
	if p.RingSize < 1 || p.RingSize > 1024 {
		v.fail("policy.ringSize", "must be between 1 and 1024, got %d", p.RingSize)
	}
	if p.OracleShards < 1 || p.OracleShards > 1024 || p.OracleShards&(p.OracleShards-1) != 0 {
		v.fail("policy.oracleShards", "must be a power of two between 1 and 1024, got %d",
			p.OracleShards)
	}
	if p.MaxPublishers < 1 {
		v.fail("policy.maxPublishers", "must be positive")
	}
	if p.NeverSettledThreshold < 1 {
		v.fail("policy.neverSettledThreshold", "must be at least 1 (multiples of W)")
	}

	if s.Alert.IncludeSuspect {
		v.warn("alert.includeSuspect is set: alerts will fire on keys driftwatch " +
			"knows it has an incomplete view of, which measures driftwatch rather " +
			"than the store (§23 A7)")
	}
}

// mapsSnapshotOps reports whether the codec can recognize a snapshot cycle,
// either through the canonical op names or through an explicit mapping.
func (s *Spec) mapsSnapshotOps() bool {
	// With no opMapping the codec uses driftwatch's own vocabulary, which
	// includes both snapshot markers — so there is nothing to configure and
	// §10.2's requirement is already met. The rule exists for a producer whose
	// operation names are foreign: if the operator had to teach driftwatch what
	// "BLOCK_STORED" means, they have to teach it the snapshot markers too, or
	// Strict would wait for a cycle it cannot recognize and never assert
	// anything at all.
	if len(s.Codec.OpMapping) == 0 {
		return true
	}

	begin, end := false, false

	for _, to := range s.Codec.OpMapping {
		op, err := event.ParseOp(to)
		if err != nil {
			continue
		}
		switch op {
		case event.OpSnapshotBegin:
			begin = true
		case event.OpSnapshotEnd:
			end = true
		case event.OpUnknown, event.OpSet, event.OpDelete, event.OpAdd,
			event.OpRemove, event.OpIncr, event.OpHeartbeat:
		}
	}
	return begin && end
}

// EffectiveWindow returns the settlement window the check starts with.
//
// In adaptive mode it is the floor rather than the static value: the estimator
// has no measurements yet at startup, and starting from the floor means the
// window widens as evidence arrives instead of narrowing from a guess.
func (s *Spec) EffectiveWindow() time.Duration {
	if s.Policy.SettlementWindow.Mode == WindowAdaptive {
		return s.Policy.SettlementWindow.Min.Duration()
	}
	return s.Policy.SettlementWindow.Static.Duration()
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
