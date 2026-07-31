package metrics

// The closed label enums.
//
// §9 M12 requires that `reason` and `category` labels come from a closed set
// defined in code and never from user input or an error string. That rule is
// enforced here by construction: every label value is a constant of a defined
// type, so a call site cannot pass an arbitrary string without a conversion
// visible in review, and a conversion of an unrecognized value is normalized
// back into the enum before it reaches Prometheus.
//
// The failure this prevents is specific. `err.Error()` as a label value looks
// harmless in a code review and is unbounded in production: a dial error
// carries a port number, a decode error carries an offset, and one bad
// publisher then mints a new time series per event.

// Op is the event operation label.
type Op string

// The operations, mirroring pkg/event's wire names.
const (
	OpUnknown       Op = "unknown"
	OpSet           Op = "set"
	OpDelete        Op = "delete"
	OpAdd           Op = "add"
	OpRemove        Op = "remove"
	OpIncr          Op = "incr"
	OpSnapshotBegin Op = "snapshot_begin"
	OpSnapshotEnd   Op = "snapshot_end"
	OpHeartbeat     Op = "heartbeat"
)

func opValues() []string {
	return []string{
		string(OpUnknown), string(OpSet), string(OpDelete), string(OpAdd),
		string(OpRemove), string(OpIncr), string(OpSnapshotBegin),
		string(OpSnapshotEnd), string(OpHeartbeat),
	}
}

// Normalize returns o if it is a known operation, and OpUnknown otherwise.
func (o Op) Normalize() Op { return normalize(string(o), opValues(), OpUnknown) }

// DropReason says why an event never reached the oracle.
type DropReason string

// The drop reasons from §12.
const (
	DropDecodeError     DropReason = "decode_error"
	DropUnknownOp       DropReason = "unknown_op"
	DropTooLarge        DropReason = "too_large"
	DropBufferFull      DropReason = "buffer_full"
	DropDuplicate       DropReason = "duplicate"
	DropStaleEpoch      DropReason = "stale_epoch"
	DropValidationError DropReason = "validation_error"
)

func dropReasonValues() []string {
	return []string{
		string(DropDecodeError), string(DropUnknownOp), string(DropTooLarge),
		string(DropBufferFull), string(DropDuplicate), string(DropStaleEpoch),
		string(DropValidationError),
	}
}

// Normalize returns r if it is a known reason, and DropValidationError
// otherwise.
func (r DropReason) Normalize() DropReason {
	return normalize(string(r), dropReasonValues(), DropValidationError)
}

// Stage names a point in the ingest pipeline.
type Stage string

// The ingest stages.
const (
	StageRaw     Stage = "raw"
	StageDecoded Stage = "decoded"
)

func stageValues() []string { return []string{string(StageRaw), string(StageDecoded)} }

// Normalize returns s if it is a known stage, and StageRaw otherwise.
func (s Stage) Normalize() Stage { return normalize(string(s), stageValues(), StageRaw) }

// RestartKind distinguishes a declared publisher restart from an inferred one.
type RestartKind string

// The restart kinds.
const (
	RestartExplicit RestartKind = "explicit"
	RestartImplicit RestartKind = "implicit"
)

func restartKindValues() []string {
	return []string{string(RestartExplicit), string(RestartImplicit)}
}

// Normalize returns k if it is a known kind, and RestartImplicit otherwise.
func (k RestartKind) Normalize() RestartKind {
	return normalize(string(k), restartKindValues(), RestartImplicit)
}

// Trust is the oracle trust state label.
type Trust string

// The trust states.
const (
	TrustComplete Trust = "complete"
	TrustSuspect  Trust = "suspect"
	TrustAdopted  Trust = "adopted"
)

func trustValues() []string {
	return []string{string(TrustComplete), string(TrustSuspect), string(TrustAdopted)}
}

// Normalize returns t if it is a known state, and TrustSuspect otherwise —
// the conservative direction, since an unrecognized trust state is exactly the
// case where driftwatch should not be asserting anything.
func (t Trust) Normalize() Trust { return normalize(string(t), trustValues(), TrustSuspect) }

// ProjectionErrorReason says why a projection refused an event.
type ProjectionErrorReason string

// The projection error reasons.
const (
	ProjectionUnsupportedOp    ProjectionErrorReason = "unsupported_op"
	ProjectionInvalidEvent     ProjectionErrorReason = "invalid_event"
	ProjectionMemberLimit      ProjectionErrorReason = "member_limit"
	ProjectionCounterSaturated ProjectionErrorReason = "counter_saturated"
	ProjectionTemplateError    ProjectionErrorReason = "template_error"
)

func projectionErrorValues() []string {
	return []string{
		string(ProjectionUnsupportedOp), string(ProjectionInvalidEvent),
		string(ProjectionMemberLimit), string(ProjectionCounterSaturated),
		string(ProjectionTemplateError),
	}
}

// Normalize returns r if it is a known reason, and ProjectionInvalidEvent
// otherwise.
func (r ProjectionErrorReason) Normalize() ProjectionErrorReason {
	return normalize(string(r), projectionErrorValues(), ProjectionInvalidEvent)
}

// TargetOp names a store operation.
type TargetOp string

// The store operations driftwatch issues. All of them read.
const (
	TargetGet     TargetOp = "get"
	TargetGetMany TargetOp = "get_many"
	TargetScan    TargetOp = "scan"
	TargetTTL     TargetOp = "ttl"
	TargetHealth  TargetOp = "health"
)

func targetOpValues() []string {
	return []string{
		string(TargetGet), string(TargetGetMany), string(TargetScan),
		string(TargetTTL), string(TargetHealth),
	}
}

// Normalize returns o if it is a known operation, and TargetGet otherwise.
func (o TargetOp) Normalize() TargetOp {
	return normalize(string(o), targetOpValues(), TargetGet)
}

// Category is the divergence category label, mirroring differ.Category.
type Category string

// The divergence categories.
const (
	CatMissingInTarget Category = "missing_in_target"
	CatExtraInTarget   Category = "extra_in_target"
	CatValueMismatch   Category = "value_mismatch"
	CatMemberMismatch  Category = "member_mismatch"
	CatTypeMismatch    Category = "type_mismatch"
	CatTTLMismatch     Category = "ttl_mismatch"
	CatCounterMismatch Category = "counter_mismatch"
)

// Categories returns every divergence category, in the order §9 M9 lists them.
func Categories() []Category {
	return []Category{
		CatMissingInTarget, CatExtraInTarget, CatValueMismatch, CatMemberMismatch,
		CatTypeMismatch, CatTTLMismatch, CatCounterMismatch,
	}
}

func categoryValues() []string {
	cats := Categories()
	out := make([]string, 0, len(cats))
	for _, c := range cats {
		out = append(out, string(c))
	}
	return out
}

// Normalize returns c if it is a known category, and CatValueMismatch
// otherwise.
func (c Category) Normalize() Category {
	return normalize(string(c), categoryValues(), CatValueMismatch)
}

// TransientReason says why a candidate divergence never became a finding.
type TransientReason string

// The transient reasons from §12. They are counted apart because the operator's
// reading differs: `resolved` is the settlement window doing its job,
// `fence_failed` is driftwatch's own concurrency, and a rising `key_evicted`
// says the store is under memory pressure.
const (
	TransientResolved       TransientReason = "resolved"
	TransientOracleAdvanced TransientReason = "oracle_advanced"
	TransientKeyEvicted     TransientReason = "key_evicted"
	TransientFenceFailed    TransientReason = "fence_failed"
)

func transientReasonValues() []string {
	return []string{
		string(TransientResolved), string(TransientOracleAdvanced),
		string(TransientKeyEvicted), string(TransientFenceFailed),
	}
}

// Normalize returns r if it is a known reason, and TransientResolved otherwise.
func (r TransientReason) Normalize() TransientReason {
	return normalize(string(r), transientReasonValues(), TransientResolved)
}

// SweepKind names the direction of a sweep.
type SweepKind string

// The sweep directions.
const (
	SweepOracleToTarget SweepKind = "oracle_to_target"
	SweepTargetToOracle SweepKind = "target_to_oracle"
)

func sweepKindValues() []string {
	return []string{string(SweepOracleToTarget), string(SweepTargetToOracle)}
}

// Normalize returns k if it is a known direction, and SweepOracleToTarget
// otherwise.
func (k SweepKind) Normalize() SweepKind {
	return normalize(string(k), sweepKindValues(), SweepOracleToTarget)
}

// SweepResult is the outcome of a sweep.
type SweepResult string

// The sweep outcomes. `target_unavailable` is separate from `error` because it
// is not a failure of driftwatch and must not page anyone as one.
const (
	SweepSuccess           SweepResult = "success"
	SweepTargetUnavailable SweepResult = "target_unavailable"
	SweepError             SweepResult = "error"
	SweepAborted           SweepResult = "aborted"
)

func sweepResultValues() []string {
	return []string{
		string(SweepSuccess), string(SweepTargetUnavailable),
		string(SweepError), string(SweepAborted),
	}
}

// Normalize returns r if it is a known outcome, and SweepError otherwise.
func (r SweepResult) Normalize() SweepResult {
	return normalize(string(r), sweepResultValues(), SweepError)
}

// Role is the store's replication role.
type Role string

// The replication roles.
const (
	RoleMaster  Role = "master"
	RoleReplica Role = "replica"
	RoleUnknown Role = "unknown"
)

func roleValues() []string {
	return []string{string(RoleMaster), string(RoleReplica), string(RoleUnknown)}
}

// Normalize returns r if it is a known role, and RoleUnknown otherwise.
func (r Role) Normalize() Role { return normalize(string(r), roleValues(), RoleUnknown) }

// Component names the part of a check a panic was recovered in.
type Component string

// The components that run their own goroutine and so can panic independently.
const (
	ComponentIngest    Component = "ingest"
	ComponentApplier   Component = "applier"
	ComponentSweeper   Component = "sweeper"
	ComponentLag       Component = "lag"
	ComponentSource    Component = "source"
	ComponentBootstrap Component = "bootstrap"
)

func componentValues() []string {
	return []string{
		string(ComponentIngest), string(ComponentApplier), string(ComponentSweeper),
		string(ComponentLag), string(ComponentSource), string(ComponentBootstrap),
	}
}

// Normalize returns c if it is a known component, and ComponentIngest
// otherwise.
func (c Component) Normalize() Component {
	return normalize(string(c), componentValues(), ComponentIngest)
}

// normalize maps an unrecognized value onto a fallback.
//
// The linear scan is deliberate: every enum here has fewer than ten members,
// a map would allocate one per enum at init, and this runs off the hot path.
func normalize[T ~string](v string, allowed []string, fallback T) T {
	for _, a := range allowed {
		if v == a {
			return T(v)
		}
	}
	return fallback
}
