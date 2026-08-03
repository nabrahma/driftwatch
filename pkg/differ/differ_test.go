package differ_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/nabrahma/driftwatch/pkg/differ"
	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/oracle"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// go-redis starts a process-wide time cache and a connection-pool reaper
		// at package init and never stops them. They are reachable from any
		// package that links the client, which now includes this one.
		//
		// §16.5 anticipates exactly this and permits an ignore for a
		// third-party goroutine with a reason. Neither of these is driftwatch's,
		// and one of driftwatch's own would be a bug to fix rather than an entry
		// to add here.
		goleak.IgnoreAnyFunction("github.com/redis/go-redis/v9/internal/pool.startGlobalTimeCache.func1"),
		goleak.IgnoreAnyFunction("github.com/redis/go-redis/v9/internal/pool.(*ConnPool).reaper"),
	)
}

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func setOf(members ...string) event.Value {
	m := make(map[string]struct{}, len(members))
	for _, s := range members {
		m[s] = struct{}{}
	}
	return event.Value{Kind: event.ValueSet, Members: m}
}

func scalarOf(s string) event.Value {
	return event.Value{Kind: event.ValueScalar, Scalar: []byte(s)}
}

func counterOf(n int64) event.Value {
	return event.Value{Kind: event.ValueCounter, Counter: n}
}

// entry builds a settled, trusted oracle entry holding v.
func entry(v event.Value) oracle.Entry {
	return oracle.Entry{
		Key: "k", Value: v, Version: 7, Trust: oracle.TrustComplete,
		LastEventAt: epoch, LastSeq: 42, LastPublisher: "replica-0",
	}
}

func ptr[T any](v T) *T { return &v }

// TestCompare is the comparison table from §9 M9: every reachable
// combination of oracle kind, target kind, trust and expiry policy, with the
// category it must produce or nil.
func TestCompare(t *testing.T) {
	tests := []struct {
		name   string
		oracle oracle.Entry
		target event.Value
		opts   differ.Options
		want   *differ.Category
	}{
		// Agreement.
		{
			name:   "both absent is not a finding",
			oracle: entry(event.Value{}),
			target: event.Value{},
		},
		{
			name:   "identical scalars agree",
			oracle: entry(scalarOf("v")),
			target: scalarOf("v"),
		},
		{
			name:   "identical sets agree",
			oracle: entry(setOf("a", "b")),
			target: setOf("a", "b"),
		},
		{
			name:   "sets differing only in iteration order agree",
			oracle: entry(setOf("a", "b", "c")),
			target: setOf("c", "a", "b"),
		},
		{
			name:   "identical counters agree",
			oracle: entry(counterOf(5)),
			target: counterOf(5),
		},
		{
			name:   "an empty oracle set matches an absent target key, per the Redis decision",
			oracle: entry(setOf()),
			target: event.Value{},
		},
		{
			name:   "an absent oracle value matches an empty target set",
			oracle: entry(event.Value{}),
			target: setOf(),
		},
		{
			name:   "identical binary scalars agree",
			oracle: entry(scalarOf("\x00\xff")),
			target: scalarOf("\x00\xff"),
		},

		// Missing in target.
		{
			name:   "a scalar the target does not have is missing",
			oracle: entry(scalarOf("v")),
			target: event.Value{},
			want:   cat(differ.CatMissingInTarget),
		},
		{
			name:   "a set the target does not have is missing",
			oracle: entry(setOf("a")),
			target: event.Value{},
			want:   cat(differ.CatMissingInTarget),
		},
		{
			name:   "a counter the target does not have is missing",
			oracle: entry(counterOf(1)),
			target: event.Value{},
			want:   cat(differ.CatMissingInTarget),
		},
		{
			name:   "an empty scalar the target does not have is missing, because an empty string is a value",
			oracle: entry(scalarOf("")),
			target: event.Value{},
			want:   cat(differ.CatMissingInTarget),
		},
		{
			name:   "a zero counter the target does not have is missing, because Redis stores the string 0",
			oracle: entry(counterOf(0)),
			target: event.Value{},
			want:   cat(differ.CatMissingInTarget),
		},

		// Extra in target.
		{
			name:   "a key the oracle does not expect is extra",
			oracle: entry(event.Value{}),
			target: scalarOf("v"),
			want:   cat(differ.CatExtraInTarget),
		},
		{
			name: "a tombstoned key the target still holds is extra",
			// The oracle saw a delete, so it expects absence. That is a
			// finding, and a different one from never having heard of the key.
			oracle: entry(event.Value{}),
			target: setOf("a"),
			want:   cat(differ.CatExtraInTarget),
		},
		{
			name: "an adopted key is never extra",
			// It was read out of the target at bootstrap rather than derived
			// from events, so it cannot be evidence the target is wrong (§5.6).
			oracle: func() oracle.Entry {
				e := entry(event.Value{})
				e.Trust = oracle.TrustAdopted
				return e
			}(),
			target: scalarOf("v"),
		},

		// Value, member and counter mismatches.
		{
			name:   "differing scalars are a value mismatch",
			oracle: entry(scalarOf("expected")),
			target: scalarOf("actual"),
			want:   cat(differ.CatValueMismatch),
		},
		{
			name:   "an empty scalar against a non-empty one is a value mismatch",
			oracle: entry(scalarOf("")),
			target: scalarOf("v"),
			want:   cat(differ.CatValueMismatch),
		},
		{
			name:   "differing set membership is a member mismatch",
			oracle: entry(setOf("a", "b")),
			target: setOf("a", "c"),
			want:   cat(differ.CatMemberMismatch),
		},
		{
			name:   "a set the target has fewer members of is a member mismatch",
			oracle: entry(setOf("a", "b")),
			target: setOf("a"),
			want:   cat(differ.CatMemberMismatch),
		},
		{
			name:   "differing counters are a counter mismatch",
			oracle: entry(counterOf(100)),
			target: counterOf(99),
			want:   cat(differ.CatCounterMismatch),
		},
		{
			name: "a counter off by exactly one is reported, with no fudge factor",
			// One lost increment is one lost increment. A tolerance here would
			// hide precisely the smallest real bug.
			oracle: entry(counterOf(1000)),
			target: counterOf(999),
			want:   cat(differ.CatCounterMismatch),
		},

		// Type mismatches.
		{
			name:   "a scalar where a set is expected is a type mismatch",
			oracle: entry(setOf("a")),
			target: scalarOf("v"),
			want:   cat(differ.CatTypeMismatch),
		},
		{
			name:   "a counter where a scalar is expected is a type mismatch",
			oracle: entry(scalarOf("5")),
			target: counterOf(5),
			want:   cat(differ.CatTypeMismatch),
		},

		// Trust is carried, never suppressive.
		{
			name: "a suspect key is still compared",
			oracle: func() oracle.Entry {
				e := entry(scalarOf("v"))
				e.Trust = oracle.TrustSuspect
				return e
			}(),
			target: event.Value{},
			want:   cat(differ.CatMissingInTarget),
		},
		{
			name: "a suspect key that agrees is still not a finding",
			oracle: func() oracle.Entry {
				e := entry(scalarOf("v"))
				e.Trust = oracle.TrustSuspect
				return e
			}(),
			target: scalarOf("v"),
		},
		{
			name: "an adopted key that disagrees is still a finding",
			oracle: func() oracle.Entry {
				e := entry(scalarOf("expected"))
				e.Trust = oracle.TrustAdopted
				return e
			}(),
			target: scalarOf("actual"),
			want:   cat(differ.CatValueMismatch),
		},

		// Expiry policies against a missing key.
		{
			name:   "strict reports a missing key however old",
			oracle: entry(scalarOf("v")),
			target: event.Value{},
			opts: differ.Options{
				ExpiryPolicy: differ.ExpiryStrict,
				AssumedTTL:   time.Minute,
				Now:          epoch.Add(time.Hour),
			},
			want: cat(differ.CatMissingInTarget),
		},
		{
			name:   "ignore suppresses a missing key older than the assumed TTL",
			oracle: entry(scalarOf("v")),
			target: event.Value{},
			opts: differ.Options{
				ExpiryPolicy: differ.ExpiryIgnore,
				AssumedTTL:   time.Minute,
				Now:          epoch.Add(time.Hour),
			},
		},
		{
			name:   "ignore still reports a missing key younger than the assumed TTL",
			oracle: entry(scalarOf("v")),
			target: event.Value{},
			opts: differ.Options{
				ExpiryPolicy: differ.ExpiryIgnore,
				AssumedTTL:   time.Hour,
				Now:          epoch.Add(time.Minute),
			},
			want: cat(differ.CatMissingInTarget),
		},
		{
			name:   "ignore without an assumed TTL configured suppresses nothing",
			oracle: entry(scalarOf("v")),
			target: event.Value{},
			opts: differ.Options{
				ExpiryPolicy: differ.ExpiryIgnore,
				Now:          epoch.Add(time.Hour),
			},
			want: cat(differ.CatMissingInTarget),
		},
		{
			name:   "ignore without a comparison time suppresses nothing",
			oracle: entry(scalarOf("v")),
			target: event.Value{},
			opts: differ.Options{
				ExpiryPolicy: differ.ExpiryIgnore,
				AssumedTTL:   time.Minute,
			},
			want: cat(differ.CatMissingInTarget),
		},
		{
			name: "model reports a missing key the oracle still holds",
			// Under model the oracle expires keys itself, so if it still holds
			// the key the absence is real drift rather than an expiry.
			oracle: entry(scalarOf("v")),
			target: event.Value{},
			opts: differ.Options{
				ExpiryPolicy: differ.ExpiryModel,
				AssumedTTL:   time.Minute,
				Now:          epoch.Add(time.Hour),
			},
			want: cat(differ.CatMissingInTarget),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := differ.Compare("k", tc.oracle, tc.target, tc.opts)

			if tc.want == nil {
				assert.Nil(t, got, "expected agreement, got %v", got)
				return
			}
			require.NotNil(t, got, "expected a %s finding", *tc.want)
			assert.Equal(t, *tc.want, got.Category)
			assert.Equal(t, "k", got.Key)
			assert.Equal(t, tc.oracle.Trust, got.Trust, "trust must be carried onto the finding")
			assert.Equal(t, uint64(7), got.OracleVersion)
			assert.Equal(t, uint64(42), got.LastSeq)
			assert.Equal(t, "replica-0", got.LastPublisher)
		})
	}
}

func cat(c differ.Category) *differ.Category { return &c }

func TestCompare_MemberDiffNamesWhatIsMissingAndExtra(t *testing.T) {
	got := differ.Compare("k",
		entry(setOf("a", "b", "c")), setOf("b", "c", "d"), differ.Options{})

	require.NotNil(t, got)
	assert.Equal(t, differ.CatMemberMismatch, got.Category)
	assert.Equal(t, []string{"a"}, got.MissingMembers, "in the oracle, absent from the target")
	assert.Equal(t, []string{"d"}, got.ExtraMembers, "in the target, absent from the oracle")
	assert.Equal(t, 1, got.MissingMemberCount)
	assert.Equal(t, 1, got.ExtraMemberCount)
}

func TestCompare_MemberDiffIsTruncatedButTheCountSurvives(t *testing.T) {
	// A key whose set diverges by a hundred thousand members must not produce a
	// hundred-thousand-line report. The magnitude is the part an operator acts
	// on, so it has to outlive the truncation.
	const members = 1000

	oracleSet := make(map[string]struct{}, members)
	for i := 0; i < members; i++ {
		oracleSet["m"+strconv.Itoa(i)] = struct{}{}
	}

	got := differ.Compare("k",
		entry(event.Value{Kind: event.ValueSet, Members: oracleSet}),
		setOf("unrelated"),
		differ.Options{MaxMembersReported: 20})

	require.NotNil(t, got)
	assert.Len(t, got.MissingMembers, 20, "the listing is capped")
	assert.Equal(t, members, got.MissingMemberCount, "the count is not")
	assert.Equal(t, []string{"unrelated"}, got.ExtraMembers)
}

func TestCompare_MemberDiffIsSortedSoReportsAreStable(t *testing.T) {
	// Map iteration order is randomized, so an unsorted diff would produce a
	// different report on every run of the same sweep.
	first := differ.Compare("k", entry(setOf("c", "a", "b")), setOf(), differ.Options{})
	second := differ.Compare("k", entry(setOf("b", "c", "a")), setOf(), differ.Options{})

	require.NotNil(t, first)
	require.NotNil(t, second)
	assert.Equal(t, []string{"a", "b", "c"}, first.MissingMembers)
	assert.Equal(t, first.MissingMembers, second.MissingMembers)
}

func TestCompareUnreadable(t *testing.T) {
	// The target holds something the projection's shape cannot read. That is
	// the most informative finding a sweep can produce, so it must not collapse
	// into an ordinary missing key.
	got := differ.CompareUnreadable("k", entry(setOf("a")), "hash", differ.Options{})

	require.NotNil(t, got)
	assert.Equal(t, differ.CatTypeMismatch, got.Category)
	assert.Equal(t, "hash", got.TargetType)
	assert.Equal(t, "k", got.Key)
}

func TestCompareTTL(t *testing.T) {
	withTTL := func(d *time.Duration) oracle.Entry {
		e := entry(scalarOf("v"))
		e.TTL = d
		return e
	}

	tests := []struct {
		name      string
		oracle    oracle.Entry
		targetTTL *time.Duration
		opts      differ.Options
		wantFind  bool
	}{
		{
			name:      "neither has a TTL",
			oracle:    withTTL(nil),
			targetTTL: nil,
		},
		{
			name:      "TTLs within tolerance agree",
			oracle:    withTTL(ptr(60 * time.Second)),
			targetTTL: ptr(58 * time.Second),
			opts:      differ.Options{TTLTolerance: 5 * time.Second},
		},
		{
			name:      "TTLs differing beyond tolerance are a finding",
			oracle:    withTTL(ptr(60 * time.Second)),
			targetTTL: ptr(10 * time.Second),
			opts:      differ.Options{TTLTolerance: 5 * time.Second},
			wantFind:  true,
		},
		{
			name:      "the tolerance is symmetric",
			oracle:    withTTL(ptr(10 * time.Second)),
			targetTTL: ptr(60 * time.Second),
			opts:      differ.Options{TTLTolerance: 5 * time.Second},
			wantFind:  true,
		},
		{
			name:      "a TTL the oracle expects and the target does not have is a finding",
			oracle:    withTTL(ptr(60 * time.Second)),
			targetTTL: nil,
			wantFind:  true,
		},
		{
			name:      "a TTL the target has and the oracle does not expect is a finding",
			oracle:    withTTL(nil),
			targetTTL: ptr(60 * time.Second),
			wantFind:  true,
		},
		{
			name:      "ignore suppresses TTL comparison entirely",
			oracle:    withTTL(ptr(60 * time.Second)),
			targetTTL: nil,
			opts:      differ.Options{ExpiryPolicy: differ.ExpiryIgnore},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := differ.CompareTTL("k", tc.oracle, tc.targetTTL, tc.opts)

			if !tc.wantFind {
				assert.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			assert.Equal(t, differ.CatTTLMismatch, got.Category)
			assert.Equal(t, tc.oracle.TTL, got.OracleTTL)
			assert.Equal(t, tc.targetTTL, got.TargetTTL)
		})
	}
}

func TestCategory_String(t *testing.T) {
	tests := []struct {
		name string
		c    differ.Category
		want string
	}{
		{name: "missing in target", c: differ.CatMissingInTarget, want: "missing_in_target"},
		{name: "extra in target", c: differ.CatExtraInTarget, want: "extra_in_target"},
		{name: "value mismatch", c: differ.CatValueMismatch, want: "value_mismatch"},
		{name: "member mismatch", c: differ.CatMemberMismatch, want: "member_mismatch"},
		{name: "type mismatch", c: differ.CatTypeMismatch, want: "type_mismatch"},
		{name: "ttl mismatch", c: differ.CatTTLMismatch, want: "ttl_mismatch"},
		{name: "counter mismatch", c: differ.CatCounterMismatch, want: "counter_mismatch"},
		{name: "an out-of-range category reports its number", c: differ.Category(99), want: "Category(99)"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.c.String())
		})
	}
}

func TestExpiryPolicy_String(t *testing.T) {
	assert.Equal(t, "strict", differ.ExpiryStrict.String())
	assert.Equal(t, "ignore", differ.ExpiryIgnore.String())
	assert.Equal(t, "model", differ.ExpiryModel.String())
	assert.Equal(t, "ExpiryPolicy(9)", differ.ExpiryPolicy(9).String())
}

func TestCompare_ToleratesAnEvictedOracleEntry(t *testing.T) {
	// The sweeper may hand over an entry whose oracle copy was evicted between
	// the read and the compare. That is a zero Entry, and it must produce a
	// clean answer rather than a panic; the sweeper re-queues the key.
	got := differ.Compare("k", oracle.Entry{}, event.Value{}, differ.Options{})

	assert.Nil(t, got)
}

func TestCompare_DefaultsAreAppliedToAZeroOptions(t *testing.T) {
	oracleSet := make(map[string]struct{}, 100)
	for i := 0; i < 100; i++ {
		oracleSet["m"+strconv.Itoa(i)] = struct{}{}
	}

	got := differ.Compare("k",
		entry(event.Value{Kind: event.ValueSet, Members: oracleSet}),
		event.Value{}, differ.Options{})

	require.NotNil(t, got)
	assert.Len(t, got.MissingMembers, 20, "the default member cap is 20")
	assert.Equal(t, 100, got.MissingMemberCount)
}
