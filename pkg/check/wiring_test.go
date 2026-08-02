package check_test

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/check"
	"github.com/nabrahma/driftwatch/pkg/source"
)

// Configuration decisions that only show up once the pipeline is built.
//
// Each of these was a defect before it was a test: a buffer sized for a
// transport that cannot drop, an immutable field that was not, a decimal that
// parsed differently depending on whether someone had quoted it.

func TestSpec_ImmutableFieldsAreRefusedOnUpdate(t *testing.T) {
	// Changing the projection or the target type under a running check does not
	// migrate anything — the oracle holds values in the old projection's shape,
	// and the new one would fold onto them. The result is a check that reports
	// drift for the whole keyspace and is entirely wrong about all of it.
	//
	// Refusing the update is the only safe answer, and the message has to say
	// what to do instead, because "field is immutable" on its own leaves an
	// operator with a broken check and no next step.
	base, err := check.Load(strings.NewReader(inProcessSpec))
	require.NoError(t, err)

	t.Run("an unchanged spec is accepted", func(t *testing.T) {
		next, err := check.Load(strings.NewReader(inProcessSpec))
		require.NoError(t, err)
		assert.NoError(t, next.ValidateUpdate(&base))
	})

	t.Run("no previous spec is accepted", func(t *testing.T) {
		assert.NoError(t, base.ValidateUpdate(nil),
			"a first apply has nothing to be immutable against")
	})

	t.Run("changing the projection type is refused", func(t *testing.T) {
		changed, err := check.Load(strings.NewReader(
			strings.Replace(inProcessSpec, "type: keysetOwnership", "type: scalar", 1)))
		require.NoError(t, err)

		err = changed.ValidateUpdate(&base)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "immutable")
		assert.Contains(t, err.Error(), "delete and recreate",
			"the message must say what to do instead: %v", err)
	})

	t.Run("a policy change is accepted", func(t *testing.T) {
		// Only the two structural fields are immutable. Tuning the window or
		// the sweep interval under a running check is the ordinary case, and
		// refusing it would make every adjustment a delete-and-recreate.
		changed, err := check.Load(strings.NewReader(
			strings.Replace(inProcessSpec, "sweepInterval: 10s", "sweepInterval: 45s", 1)))
		require.NoError(t, err)
		assert.NoError(t, changed.ValidateUpdate(&base))
	})
}

func TestSpec_DecimalAcceptsBothQuotedAndBareNumbers(t *testing.T) {
	// YAML will hand back a string or a float depending on quoting, and an
	// operator writing safetyFactor: "1.5" rather than safetyFactor: 1.5 has
	// not made a mistake. Accepting only one of the two produces a validation
	// error that reads as though the value is wrong when the quoting is.
	tests := []struct {
		name string
		raw  string
		want float64
		bad  bool
	}{
		{name: "bare", raw: "1.5", want: 1.5},
		{name: "integer", raw: "2", want: 2},
		{name: "surrounding space", raw: "  1.25  ", want: 1.25},
		{name: "empty is zero", raw: "", want: 0},
		{name: "not a number", raw: "soon", bad: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := check.ParseDecimal(tc.raw)
			if tc.bad {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "not a number",
					"the message should name the problem: %v", err)
				return
			}
			require.NoError(t, err)
			assert.InDelta(t, tc.want, got.Float(), 0.0001)
		})
	}
}

func TestCheck_AProjectionErrorIsAttributedToACloseEnum(t *testing.T) {
	// The reason label on driftwatch_projection_errors_total is a closed enum
	// and not the error string. An error string as a label value is how a
	// metric acquires unbounded cardinality — one series per distinct message,
	// including the ones that embed a key.
	reg := prometheus.NewRegistry()
	c := newCheckWithRegistry(t, scalarSpec, reg)
	stop := running(t, c)
	defer stop()

	// An `add` against a scalar projection: parseable, valid, and the wrong
	// shape for the fold.
	publish(t, c,
		`{"publisher":"replica-0","epoch":1,"seq":1,"op":"add","key":"a","member":"m"}`)

	families, err := reg.Gather()
	require.NoError(t, err)

	var found bool
	for _, f := range families {
		if f.GetName() != "driftwatch_projection_errors_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() != "reason" {
					continue
				}
				found = true
				assert.NotContains(t, l.GetValue(), " ",
					"the reason label is an enum member, not a sentence: %q",
					l.GetValue())
			}
		}
	}
	assert.True(t, found,
		"a projection error should have been recorded with a reason label")
}

func TestCheck_TheIdleTimeoutReachesTheSource(t *testing.T) {
	// D-025's fix is only worth anything if the value survives the trip from
	// the CRD, through the spec, through the settings map, into the source.
	// That path has four hops and one of them silently drops an unknown key,
	// so the assertion is on the source's own reading rather than on the spec.
	//
	// D-010 is the precedent: an option accepted and ignored looks exactly like
	// an option that works.
	spec, err := check.Load(strings.NewReader(`
source:
  type: zmq
  zmq:
    endpoints: ["tcp://publisher.default.svc.cluster.local:5557"]
projection: {type: scalar}
target: {type: memory}
`))
	require.NoError(t, err)

	c, err := check.New(spec, check.Deps{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })

	z, ok := c.Source().(*source.ZMQSource)
	require.True(t, ok, "the spec configures a zmq source")

	assert.Equal(t, check.DefaultIdleTimeout, z.IdleTimeout(),
		"the deadline did not survive the trip from the spec into the source, "+
			"so a publisher that vanishes would still leave driftwatch deaf")
}
