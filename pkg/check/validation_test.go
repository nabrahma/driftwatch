package check_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/check"
)

// The messages an operator sees when a spec is wrong.
//
// These are worth testing for the same reason the diagnosis line in `explain`
// is: a rejection that names the field and says what was expected costs the
// operator a minute, and one that says "invalid configuration" costs an hour
// and a trip through the source. Every case below asserts on the *content* of
// the message, not merely that one was produced.

func TestValidation_EachSourceTypeNamesItsOwnMissingField(t *testing.T) {
	tests := []struct {
		name string
		spec string
		want string
	}{
		{
			name: "zmq without endpoints",
			spec: "source: {type: zmq}\n" + minimalTail,
			want: "source.zmq.endpoints",
		},
		{
			name: "nats without a url",
			spec: "source: {type: nats}\n" + minimalTail,
			want: "source.nats.url",
		},
		{
			name: "nats without subjects",
			spec: "source:\n  type: nats\n  nats: {url: \"nats://nats:4222\"}\n" + minimalTail,
			want: "source.nats.subjects",
		},
		{
			name: "file without a path",
			spec: "source: {type: file}\n" + minimalTail,
			want: "source.file.path",
		},
		{
			name: "an unknown source type lists the ones that exist",
			spec: "source: {type: kafka}\n" + minimalTail,
			want: "valid:",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spec, err := check.Load(strings.NewReader(tc.spec))
			require.NoError(t, err, "the spec parses; it is Validate that must refuse it")

			err = spec.Validate()
			require.Error(t, err, "this spec should not have been accepted")
			assert.Contains(t, err.Error(), tc.want,
				"the message must name the field an operator has to fix: %v", err)
		})
	}
}

func TestValidation_ARedisTargetWithoutItsBlockIsRejectedByName(t *testing.T) {
	spec, err := check.Load(strings.NewReader(`
source: {type: memory}
projection: {type: scalar}
target: {type: redis}
`))
	require.NoError(t, err)

	err = spec.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "target.redis",
		"naming the missing block is the difference between a one-line fix "+
			"and a search: %v", err)
}

func TestValidation_ABareNumberIsSecondsRatherThanNanoseconds(t *testing.T) {
	// `sweepInterval: 30` is what people write for half a minute, and Go's
	// zero-value convention would read it as 30 nanoseconds — a check that
	// sweeps continuously and burns a core.
	//
	// Neither rejecting it nor taking it literally is the right answer: it is
	// accepted as seconds, because that is unambiguously what was meant.
	spec, err := check.Load(strings.NewReader(`
source: {type: memory}
projection: {type: scalar}
target: {type: memory}
policy:
  sweepInterval: 30
`))
	require.NoError(t, err)
	assert.Equal(t, 30*time.Second, spec.Policy.SweepInterval.Duration(),
		"a bare number is seconds, not nanoseconds")
}

func TestValidation_ADurationRejectsWhatIsNotOne(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{name: "a word", raw: `sweepInterval: "soon"`},
		{name: "a list", raw: "sweepInterval: [30s]"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := check.Load(strings.NewReader(`
source: {type: memory}
projection: {type: scalar}
target: {type: memory}
policy:
  ` + tc.raw + `
`))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "duration",
				"the message should say what a duration looks like: %v", err)
		})
	}
}

func TestValidation_AnEmptyDurationIsZeroRatherThanAnError(t *testing.T) {
	// The other half. An explicitly empty value is how a Kubernetes manifest
	// expresses "unset" when a field is templated out, and treating that as a
	// parse failure would break every chart that leaves a value blank.
	spec, err := check.Load(strings.NewReader(`
source: {type: memory}
projection: {type: scalar}
target: {type: memory}
policy:
  reorderWindow: ""
`))
	require.NoError(t, err, "an explicitly empty duration must parse")

	// It does not stay zero: defaulting fills it afterwards, which is the
	// behavior a templated-out field wants. The point is that the empty string
	// reaches defaulting rather than failing the parse.
	assert.Positive(t, spec.Policy.ReorderWindow.Duration(),
		"an empty value should be defaulted, not left at zero")
}

func TestValidation_AKeyPatternWithUnbalancedBracketsIsRejected(t *testing.T) {
	// A Redis glob with an unmatched bracket does not error at the server — it
	// matches nothing. A check configured with one would scan a keyspace,
	// find zero keys, compare nothing, and report a clean bill of health for a
	// store it never looked at. That is the worst possible failure for this
	// tool, so the pattern is parsed before it is ever sent.
	spec, err := check.Load(strings.NewReader(`
source: {type: memory}
projection: {type: scalar}
target:
  type: redis
  redis:
    addr: redis:6379
    keyPattern: "block:[abc"
`))
	require.NoError(t, err)

	err = spec.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "keyPattern",
		"the message must name the pattern: %v", err)
}

func TestValidation_ABackslashEscapesTheBracketAfterIt(t *testing.T) {
	// And the pattern parser has to understand escaping, or it rejects legal
	// globs. `block:\[` matches a literal bracket and is balanced.
	_, err := check.Load(strings.NewReader(`
source: {type: memory}
projection: {type: scalar}
target:
  type: redis
  redis:
    addr: redis:6379
    keyPattern: "block:\\[literal"
`))
	require.NoError(t, err,
		"an escaped bracket is a literal, not the start of a class")
}

func TestValidation_TheEffectiveConfigDumpCoversEverySourceType(t *testing.T) {
	// The dump is written once at startup and is the fastest answer to "what is
	// this check actually configured with". A source type it does not know how
	// to render produces a dump missing the fields that matter most for that
	// transport — which is exactly when someone is reading it.
	tests := []struct {
		name string
		spec string
		want string
	}{
		{
			name: "zmq renders its endpoints",
			spec: `
source:
  type: zmq
  zmq: {endpoints: ["tcp://vllm-0:5557"], topics: ["kv-events"]}
` + minimalTail,
			want: "tcp://vllm-0:5557",
		},
		{
			name: "nats renders its url and subjects",
			spec: `
source:
  type: nats
  nats: {url: "nats://nats:4222", subjects: ["kv.events"], queueGroup: "driftwatch"}
` + minimalTail,
			want: "kv.events",
		},
		{
			name: "file renders its path",
			spec: `
source:
  type: file
  file: {path: "/captures/events.jsonl"}
` + minimalTail,
			want: "/captures/events.jsonl",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spec, err := check.Load(strings.NewReader(tc.spec))
			require.NoError(t, err)

			dump := spec.YAML()
			assert.Contains(t, dump, tc.want,
				"the effective-config dump dropped the field this transport is "+
					"configured by:\n%s", dump)
		})
	}
}

// minimalTail completes a spec whose source section is the part under test.
const minimalTail = `projection: {type: scalar}
target: {type: memory}
`
