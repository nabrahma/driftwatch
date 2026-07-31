package differ_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/nabrahma/driftwatch/pkg/differ"
	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/testgen"
)

// TestProp_EmptyIffEqual is invariant I6: for a settled key driftwatch has a
// complete view of, Compare reports nothing exactly when the oracle and the
// target agree.
//
// Both halves matter and they fail differently. A finding where the values
// agree is a false positive, and enough of those make the tool something people
// silence. No finding where the values disagree is a false negative — silent,
// and indistinguishable from the healthy case, which is the failure this whole
// project exists to prevent in somebody else's system.
//
// The generated values deliberately include the awkward cases: binary bytes,
// the empty string, empty sets, large sets, and counters at the limits of
// int64.
func TestProp_EmptyIffEqual(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		oracleValue := testgen.AnyValue(t)
		targetValue := testgen.AnyValue(t)

		oe := oracle.Entry{
			Key:         "k",
			Value:       oracleValue,
			Version:     1,
			Trust:       oracle.TrustComplete,
			LastEventAt: epoch,
		}

		got := differ.Compare("k", oe, targetValue, differ.Options{})
		equal := oracleValue.Equal(targetValue)

		if equal {
			require.Nil(t, got,
				"reported %v for values that are equal:\n  oracle %s\n  target %s",
				got, oracleValue, targetValue)
			return
		}
		require.NotNil(t, got,
			"reported nothing for values that differ:\n  oracle %s\n  target %s",
			oracleValue, targetValue)
	})
}

// TestProp_TheCategoryExplainsTheDisagreement checks that whatever category
// Compare picks is consistent with the values it was given. A finding whose
// category contradicts its own values would send an operator to the wrong place.
func TestProp_TheCategoryExplainsTheDisagreement(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		oracleValue := testgen.AnyValue(t)
		targetValue := testgen.AnyValue(t)

		oe := oracle.Entry{Key: "k", Value: oracleValue, Trust: oracle.TrustComplete}
		got := differ.Compare("k", oe, targetValue, differ.Options{})
		if got == nil {
			return
		}

		switch got.Category {
		case differ.CatMissingInTarget:
			assert.False(t, oracleValue.IsAbsent())
			assert.True(t, targetValue.IsAbsent())
		case differ.CatExtraInTarget:
			assert.True(t, oracleValue.IsAbsent())
			assert.False(t, targetValue.IsAbsent())
		case differ.CatTypeMismatch:
			assert.NotEqual(t, oracleValue.Kind, targetValue.Kind)
			assert.False(t, oracleValue.IsAbsent())
			assert.False(t, targetValue.IsAbsent())
		case differ.CatValueMismatch:
			assert.Equal(t, event.ValueScalar, oracleValue.Kind)
			assert.Equal(t, event.ValueScalar, targetValue.Kind)
		case differ.CatMemberMismatch:
			assert.Equal(t, event.ValueSet, oracleValue.Kind)
			assert.Equal(t, event.ValueSet, targetValue.Kind)
		case differ.CatCounterMismatch:
			assert.Equal(t, event.ValueCounter, oracleValue.Kind)
			assert.Equal(t, event.ValueCounter, targetValue.Kind)
		case differ.CatTTLMismatch:
			t.Fatalf("Compare must not produce a TTL finding; CompareTTL does")
		}
	})
}

// TestProp_AMemberDiffAccountsForEveryDifference asserts the member lists and
// their counts describe the same disagreement the values do.
func TestProp_AMemberDiffAccountsForEveryDifference(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		oracleValue := testgen.Value(t, event.ValueSet)
		targetValue := testgen.Value(t, event.ValueSet)

		oe := oracle.Entry{Key: "k", Value: oracleValue, Trust: oracle.TrustComplete}
		got := differ.Compare("k", oe, targetValue, differ.Options{MaxMembersReported: 3})
		if got == nil {
			return
		}

		wantMissing := countAbsent(oracleValue.Members, targetValue.Members)
		wantExtra := countAbsent(targetValue.Members, oracleValue.Members)

		assert.Equal(t, wantMissing, got.MissingMemberCount)
		assert.Equal(t, wantExtra, got.ExtraMemberCount)

		// The listing is capped but must never exceed the true count, and must
		// only name members that are genuinely on the side it claims.
		assert.LessOrEqual(t, len(got.MissingMembers), 3)
		assert.LessOrEqual(t, len(got.MissingMembers), wantMissing)
		for _, m := range got.MissingMembers {
			_, inOracle := oracleValue.Members[m]
			_, inTarget := targetValue.Members[m]
			assert.True(t, inOracle, "%q is listed as missing but is not in the oracle", m)
			assert.False(t, inTarget, "%q is listed as missing but is in the target", m)
		}
		for _, m := range got.ExtraMembers {
			_, inOracle := oracleValue.Members[m]
			_, inTarget := targetValue.Members[m]
			assert.True(t, inTarget, "%q is listed as extra but is not in the target", m)
			assert.False(t, inOracle, "%q is listed as extra but is in the oracle", m)
		}

		// A disagreement between two present sets has to show up somewhere.
		if !oracleValue.IsAbsent() && !targetValue.IsAbsent() {
			assert.Positive(t, wantMissing+wantExtra,
				"two sets were reported as differing with no differing members")
		}
	})
}

// TestProp_CompareIsPureAndDoesNotTouchItsInputs guards the contract the
// sweeper relies on: it hands Compare a snapshot and keeps using it afterwards.
func TestProp_CompareIsPureAndDoesNotTouchItsInputs(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		oracleValue := testgen.AnyValue(t)
		targetValue := testgen.AnyValue(t)

		oracleBefore := oracleValue.Clone()
		targetBefore := targetValue.Clone()

		oe := oracle.Entry{Key: "k", Value: oracleValue, Trust: oracle.TrustComplete}

		first := differ.Compare("k", oe, targetValue, differ.Options{})
		for i := 0; i < 3; i++ {
			again := differ.Compare("k", oe, targetValue, differ.Options{})
			require.Equal(t, first == nil, again == nil, "Compare is not deterministic")
			if first != nil {
				require.Equal(t, first.Category, again.Category)
			}
		}

		assert.True(t, oracleBefore.Equal(oracleValue), "Compare modified the oracle value")
		assert.True(t, targetBefore.Equal(targetValue), "Compare modified the target value")
	})
}

// TestProp_AnAdoptedKeyIsNeverReportedAsExtra pins the §5.6 rule at the
// property level. Adopt mode's whole guarantee is "no new drift since I
// started"; reporting adopted keys as extra would make it report the entire
// pre-existing keyspace on the first sweep.
func TestProp_AnAdoptedKeyIsNeverReportedAsExtra(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		targetValue := testgen.AnyValue(t)

		oe := oracle.Entry{Key: "k", Value: event.Value{}, Trust: oracle.TrustAdopted}
		got := differ.Compare("k", oe, targetValue, differ.Options{})

		assert.Nil(t, got, "an adopted key produced %v", got)
	})
}

// TestProp_TrustNeverChangesWhetherSomethingIsAFinding separates the two
// questions driftwatch must keep apart: whether the values disagree, and
// whether driftwatch is a reliable witness to it. Trust answers the second and
// must not leak into the first.
func TestProp_TrustNeverChangesWhetherSomethingIsAFinding(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		oracleValue := testgen.AnyValue(t)
		targetValue := testgen.AnyValue(t)

		// Adopted is excluded: it genuinely does suppress extras, which is the
		// §5.6 rule the previous property covers.
		complete := oracle.Entry{Key: "k", Value: oracleValue, Trust: oracle.TrustComplete}
		suspect := oracle.Entry{Key: "k", Value: oracleValue, Trust: oracle.TrustSuspect}

		fromComplete := differ.Compare("k", complete, targetValue, differ.Options{})
		fromSuspect := differ.Compare("k", suspect, targetValue, differ.Options{})

		require.Equal(t, fromComplete == nil, fromSuspect == nil,
			"trust changed whether a disagreement was reported at all")
		if fromComplete != nil {
			assert.Equal(t, fromComplete.Category, fromSuspect.Category)
			assert.Equal(t, oracle.TrustComplete, fromComplete.Trust)
			assert.Equal(t, oracle.TrustSuspect, fromSuspect.Trust,
				"the finding must carry the trust so the sweeper can route it")
		}
	})
}

// TestProp_ReportCountsSurviveTruncation asserts the magnitude outlives the
// detail. Under mass divergence an operator needs to know whether the real
// number is ten thousand or a million.
func TestProp_ReportCountsSurviveTruncation(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		limit := rapid.IntRange(1, 20).Draw(t, "maxFindings")
		findings := rapid.IntRange(0, 100).Draw(t, "findings")

		r := differ.NewReport(epoch, differ.Options{MaxFindings: limit})
		for i := 0; i < findings; i++ {
			trust := oracle.TrustComplete
			if rapid.Bool().Draw(t, "suspect") {
				trust = oracle.TrustSuspect
			}
			r.Add(&differ.Finding{
				Key: "k", Category: differ.CatMissingInTarget, Trust: trust,
			})
		}

		assert.Equal(t, findings, r.Total(), "the count must not be capped")
		assert.LessOrEqual(t, len(r.Findings), limit, "the list must be")
		assert.Equal(t, findings > limit, r.Truncated)
		assert.Equal(t, findings-r.ByTrust[oracle.TrustSuspect], r.Alertable())
	})
}

func countAbsent(a, b map[string]struct{}) int {
	n := 0
	for member := range a {
		if _, present := b[member]; !present {
			n++
		}
	}
	return n
}
