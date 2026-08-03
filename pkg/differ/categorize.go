// Package differ compares oracle state against target state and categorizes findings (M9).
//
// It is pure: no I/O, no clock, no randomness. Compare is given one key's
// expected value and one key's observed value and returns a Finding or nil.
// Everything about when to compare, whether to trust the answer, and whether to
// report it belongs to the sweeper.
//
// The one rule worth stating up front is that Compare returns nil whenever the
// two agree, and "agree" is decided by event.Value.Equal — which treats an
// empty member set as equal to an absent key, because that is what Redis does.
// Every false positive this tool could produce starts with getting that wrong.
package differ

import (
	"strconv"
	"time"

	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/oracle"
)

// Category names the kind of disagreement a Finding records.
type Category uint8

// The categories, in the order §9 M9 lists them.
const (
	// CatMissingInTarget means the oracle expects a value the target does not
	// have.
	CatMissingInTarget Category = iota
	// CatExtraInTarget means the target holds a key the oracle does not expect.
	// The sweeper decides whether to report it; see §5.5.
	CatExtraInTarget
	// CatValueMismatch means both hold a scalar and the bytes differ.
	CatValueMismatch
	// CatMemberMismatch means both hold a set and the membership differs.
	CatMemberMismatch
	// CatTypeMismatch means the target holds a type the projection's shape
	// cannot read.
	CatTypeMismatch
	// CatTTLMismatch means the expiry differs beyond tolerance.
	CatTTLMismatch
	// CatCounterMismatch means both hold a counter and the values differ.
	CatCounterMismatch
)

var categoryNames = [...]string{
	CatMissingInTarget: "missing_in_target",
	CatExtraInTarget:   "extra_in_target",
	CatValueMismatch:   "value_mismatch",
	CatMemberMismatch:  "member_mismatch",
	CatTypeMismatch:    "type_mismatch",
	CatTTLMismatch:     "ttl_mismatch",
	CatCounterMismatch: "counter_mismatch",
}

// String returns the metric-friendly name of the category.
func (c Category) String() string {
	if int(c) >= len(categoryNames) {
		return "Category(" + strconv.Itoa(int(c)) + ")"
	}
	return categoryNames[c]
}

// ExpiryPolicy decides whether a key missing from the target counts as drift.
//
// Whether an expired key is drift is domain-dependent, which is why this is a
// policy rather than a rule (§5.7).
type ExpiryPolicy uint8

const (
	// ExpiryStrict treats any absence as drift. Correct when the target has no
	// TTLs, which is the common case for an index, and the default for that
	// reason.
	ExpiryStrict ExpiryPolicy = iota
	// ExpiryIgnore suppresses a missing key once the oracle's copy is older
	// than AssumedTTL. Blunt, but useful against a keyspace whose TTLs
	// driftwatch cannot see.
	ExpiryIgnore
	// ExpiryModel expects the projection to track TTL from events, so the
	// oracle expires keys itself and divergence is meaningful both ways.
	ExpiryModel
)

var expiryPolicyNames = [...]string{
	ExpiryStrict: "strict",
	ExpiryIgnore: "ignore",
	ExpiryModel:  "model",
}

// String returns the name of the policy.
func (p ExpiryPolicy) String() string {
	if int(p) >= len(expiryPolicyNames) {
		return "ExpiryPolicy(" + strconv.Itoa(int(p)) + ")"
	}
	return expiryPolicyNames[p]
}

// Defaults for Options.
const (
	defaultTTLTolerance       = 5 * time.Second
	defaultMaxMembersReported = 20
	defaultMaxFindings        = 10_000
)

// Options configures a comparison.
type Options struct {
	// TTLTolerance is how far two expiries may differ before it is a finding.
	// Default 5s.
	TTLTolerance time.Duration
	// ExpiryPolicy decides whether a missing key is drift. Default strict.
	ExpiryPolicy ExpiryPolicy
	// AssumedTTL is the age past which ExpiryIgnore stops reporting a missing
	// key.
	AssumedTTL time.Duration
	// MaxMembersReported truncates member diffs. Default 20.
	MaxMembersReported int
	// MaxFindings caps a report's finding list. Default 10,000.
	MaxFindings int
	// Now is the comparison time, used only by ExpiryIgnore to age a key.
	// Supplied by the caller because this package never reads a clock.
	Now time.Time
}

func (o *Options) applyDefaults() {
	if o.TTLTolerance <= 0 {
		o.TTLTolerance = defaultTTLTolerance
	}
	if o.MaxMembersReported <= 0 {
		o.MaxMembersReported = defaultMaxMembersReported
	}
	if o.MaxFindings <= 0 {
		o.MaxFindings = defaultMaxFindings
	}
}

// Finding records one key's disagreement.
type Finding struct {
	Key      string
	Category Category
	Trust    oracle.TrustState

	OracleValue event.Value
	TargetValue event.Value

	// MissingMembers are in the oracle and absent from the target;
	// ExtraMembers are the other way round. Both are truncated at
	// MaxMembersReported, with the remainder counted rather than listed.
	MissingMembers []string
	ExtraMembers   []string
	// MissingMemberCount and ExtraMemberCount are the untruncated totals, so a
	// report says "and 99,980 more" rather than losing the magnitude.
	MissingMemberCount int
	ExtraMemberCount   int

	OracleVersion uint64
	LastEventAt   time.Time
	LastSeq       uint64
	LastPublisher string

	// TargetType names what the target actually held, for CatTypeMismatch.
	TargetType string

	// OracleTTL and TargetTTL are set for CatTTLMismatch.
	OracleTTL *time.Duration
	TargetTTL *time.Duration

	// FirstSeenAt is when this finding first appeared, and Confirmed reports
	// whether it survived a re-read a settlement window later. Both are filled
	// in by the sweeper, which is the only thing that sees a key twice.
	FirstSeenAt time.Time
	Confirmed   bool
}
