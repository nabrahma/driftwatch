package differ

import (
	"sort"
	"time"

	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/oracle"
)

// Compare produces a Finding, or nil if the values agree.
//
// The comparison is exactly event.Value.Equal, and the categories exist only to
// explain a disagreement Equal has already found. Doing it the other way round
// — deciding per category whether something counts — is how a differ ends up
// with two definitions of equality that disagree at the edges.
//
// Trust is carried onto the Finding but never suppresses it. A suspect key is
// still compared; the sweeper reports it under a separate metric that does not
// feed alerting, because driftwatch knowing its own view is incomplete is a
// different statement from the target being wrong (§5.2).
//
// oracle.Entry is passed by value throughout this package rather than by
// pointer. The struct is 176 bytes and a pointer would save the copy, but
// Compare's contract is that it cannot modify what it is given — the sweeper
// keeps using the snapshot afterwards — and a value parameter is how that is
// stated in the type rather than only in a comment.
//
//nolint:gocritic // hugeParam: by value so the purity contract is in the signature
func Compare(key string, oe oracle.Entry, tv event.Value, opts Options) *Finding {
	opts.applyDefaults()

	oracleAbsent := oe.Value.IsAbsent()
	targetAbsent := tv.IsAbsent()

	switch {
	case oracleAbsent && targetAbsent:
		// Neither has it. Not a finding, and specifically not an
		// extra_in_target: a key driftwatch has never heard of and a key it
		// expects to be gone are both "the target should not have this".
		return nil

	case oracleAbsent && !targetAbsent:
		// An adopted key was read out of the target at bootstrap rather than
		// derived from events, so it cannot be evidence that the target is
		// wrong. Adopt mode's guarantee is only ever "no new drift since I
		// started" (§5.6).
		if oe.Trust == oracle.TrustAdopted {
			return nil
		}
		return newFinding(key, CatExtraInTarget, oe, tv, opts)

	case !oracleAbsent && targetAbsent:
		if suppressedByExpiry(oe, opts) {
			return nil
		}
		return newFinding(key, CatMissingInTarget, oe, tv, opts)
	}

	// Both present.
	if oe.Value.Equal(tv) {
		return nil
	}
	if oe.Value.Kind != tv.Kind {
		f := newFinding(key, CatTypeMismatch, oe, tv, opts)
		f.TargetType = tv.Kind.String()
		return f
	}

	switch oe.Value.Kind {
	case event.ValueSet:
		return newFinding(key, CatMemberMismatch, oe, tv, opts)
	case event.ValueCounter:
		return newFinding(key, CatCounterMismatch, oe, tv, opts)
	case event.ValueScalar:
		return newFinding(key, CatValueMismatch, oe, tv, opts)
	case event.ValueAbsent:
		// Unreachable: both absent was handled above.
		return nil
	default:
		return newFinding(key, CatValueMismatch, oe, tv, opts)
	}
}

// CompareUnreadable produces the finding for a key the target holds in a shape
// the projection cannot read.
//
// It is a separate entry point because Compare takes an event.Value, and there
// is no event.Value that means "a hash". Swallowing the distinction would turn
// the most informative finding a sweep can produce — something wrote a
// different shape into the index — into an ordinary missing key.
//
// oracle.Entry is passed by value throughout this package rather than by
// pointer. The struct is 176 bytes and a pointer would save the copy, but
// Compare's contract is that it cannot modify what it is given — the sweeper
// keeps using the snapshot afterwards — and a value parameter is how that is
// stated in the type rather than only in a comment.
//
//nolint:gocritic // hugeParam: by value so the purity contract is in the signature
func CompareUnreadable(key string, oe oracle.Entry, targetType string, opts Options) *Finding {
	opts.applyDefaults()

	f := newFinding(key, CatTypeMismatch, oe, event.Value{}, opts)
	f.TargetType = targetType
	return f
}

// CompareTTL produces a finding when two expiries differ beyond tolerance.
//
// A tolerance is required rather than nice to have: a TTL read at one instant
// and compared against another read at a different instant is a moving target,
// and an exact comparison would report every key with an expiry on every sweep.
//
// oracle.Entry is passed by value throughout this package rather than by
// pointer. The struct is 176 bytes and a pointer would save the copy, but
// Compare's contract is that it cannot modify what it is given — the sweeper
// keeps using the snapshot afterwards — and a value parameter is how that is
// stated in the type rather than only in a comment.
//
//nolint:gocritic // hugeParam: by value so the purity contract is in the signature
func CompareTTL(key string, oe oracle.Entry, targetTTL *time.Duration, opts Options) *Finding {
	opts.applyDefaults()

	oracleTTL := oe.TTL
	switch {
	case oracleTTL == nil && targetTTL == nil:
		return nil
	case oracleTTL != nil && targetTTL != nil:
		delta := *oracleTTL - *targetTTL
		if delta < 0 {
			delta = -delta
		}
		if delta <= opts.TTLTolerance {
			return nil
		}
	}

	// Under ExpiryIgnore driftwatch has already accepted that it cannot see the
	// target's expiries, so comparing them would contradict the policy.
	if opts.ExpiryPolicy == ExpiryIgnore {
		return nil
	}

	f := newFinding(key, CatTTLMismatch, oe, event.Value{}, opts)
	f.OracleTTL = oracleTTL
	f.TargetTTL = targetTTL
	return f
}

// suppressedByExpiry reports whether a missing key is explained by expiry
// rather than by drift.
//
// oracle.Entry is passed by value throughout this package rather than by
// pointer. The struct is 176 bytes and a pointer would save the copy, but
// Compare's contract is that it cannot modify what it is given — the sweeper
// keeps using the snapshot afterwards — and a value parameter is how that is
// stated in the type rather than only in a comment.
//
//nolint:gocritic // hugeParam: by value so the purity contract is in the signature
func suppressedByExpiry(oe oracle.Entry, opts Options) bool {
	if opts.ExpiryPolicy != ExpiryIgnore {
		// Strict reports every absence. Model expects the oracle to have
		// expired the key itself, so if the oracle still holds it, the absence
		// is real drift.
		return false
	}
	if opts.AssumedTTL <= 0 || opts.Now.IsZero() || oe.LastEventAt.IsZero() {
		return false
	}
	return opts.Now.Sub(oe.LastEventAt) > opts.AssumedTTL
}

// newFinding builds a Finding, filling in the member diff for set comparisons.
//
// oracle.Entry is passed by value throughout this package rather than by
// pointer. The struct is 176 bytes and a pointer would save the copy, but
// Compare's contract is that it cannot modify what it is given — the sweeper
// keeps using the snapshot afterwards — and a value parameter is how that is
// stated in the type rather than only in a comment.
//
//nolint:gocritic // hugeParam: by value so the purity contract is in the signature
func newFinding(key string, cat Category, oe oracle.Entry, tv event.Value, opts Options) *Finding {
	f := &Finding{
		Key:           key,
		Category:      cat,
		Trust:         oe.Trust,
		OracleValue:   oe.Value,
		TargetValue:   tv,
		OracleVersion: oe.Version,
		LastEventAt:   oe.LastEventAt,
		LastSeq:       oe.LastSeq,
		LastPublisher: oe.LastPublisher,
		OracleTTL:     oe.TTL,
	}

	if oe.Value.Kind == event.ValueSet || tv.Kind == event.ValueSet {
		f.MissingMembers, f.MissingMemberCount = diffMembers(oe.Value.Members, tv.Members, opts.MaxMembersReported)
		f.ExtraMembers, f.ExtraMemberCount = diffMembers(tv.Members, oe.Value.Members, opts.MaxMembersReported)
	}
	return f
}

// diffMembers returns the members of a that are absent from b, truncated to
// limit, along with the untruncated count.
//
// The truncation is the point. A key whose member set diverges by a hundred
// thousand entries must not produce a hundred-thousand-line report — but the
// magnitude still has to survive, because "and 99,980 more" is the part an
// operator acts on.
func diffMembers(a, b map[string]struct{}, limit int) (listed []string, total int) {
	if len(a) == 0 {
		return nil, 0
	}

	listed = make([]string, 0, min(limit, len(a)))
	for member := range a {
		if _, present := b[member]; present {
			continue
		}
		total++
		if len(listed) < limit {
			listed = append(listed, member)
		}
	}
	if total == 0 {
		return nil, 0
	}

	// Sorted so a report is stable across runs; map iteration order is not.
	sort.Strings(listed)
	return listed, total
}
