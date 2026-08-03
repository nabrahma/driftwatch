// Package testgen provides the rapid generators the property tests share (§16.2).
//
// It is a non-test package so that every package's property tests can import
// the same generators. A generator that quietly stops producing interesting
// inputs weakens every property built on it without failing anything, so the
// generators have tests of their own in testgen_test.go.
//
// The bias here is deliberate: keys and members over-sample the inputs that
// break naive implementations — the empty string, binary bytes, glob
// metacharacters, and lengths near the buffer sizes elsewhere in driftwatch.
package testgen

import (
	"strconv"

	"pgregory.net/rapid"

	"github.com/nabrahma/driftwatch/pkg/event"
)

// awkwardKeys are the key shapes that have historically broken key handling:
// the empty key (legal in Redis), glob metacharacters (which matter to
// MarkSuspect patterns and to SCAN MATCH), separators that appear in key
// templates, and non-UTF8 bytes.
var awkwardKeys = []string{
	"",
	"*",
	"?",
	"[a-z]",
	"[",
	"]",
	"\\",
	"a*b",
	":",
	"replica:0:block",
	"key with spaces",
	"\x00",
	"\xff\xfe",
	"\x00embedded\x00nul",
	"ünïcødé",
	"\n",
}

// Key generates a key, over-sampling the awkward ones.
func Key(t *rapid.T) string {
	return rapid.OneOf(
		rapid.SampledFrom(awkwardKeys),
		rapid.StringN(0, 16, 16),
		binaryString(0, 8),
		longString(),
	).Draw(t, "key")
}

// Member generates a set member. Members are never empty, because OpAdd and
// OpRemove reject an empty member at validation.
func Member(t *rapid.T) string {
	return rapid.OneOf(
		rapid.SampledFrom([]string{"replica-0", "replica-1", "*", "[", "\xff", "ünïcødé"}),
		rapid.StringN(1, 12, 12),
		binaryString(1, 6),
	).Draw(t, "member")
}

// binaryString generates a string of arbitrary bytes, including bytes that are
// not valid UTF-8.
func binaryString(minLen, maxLen int) *rapid.Generator[string] {
	return rapid.Custom(func(t *rapid.T) string {
		b := rapid.SliceOfN(rapid.Byte(), minLen, maxLen).Draw(t, "bytes")
		return string(b)
	})
}

// longString generates a key long enough to exercise the codec's key-size cap.
func longString() *rapid.Generator[string] {
	return rapid.Custom(func(t *rapid.T) string {
		n := rapid.IntRange(64, 512).Draw(t, "len")
		b := make([]byte, n)
		for i := range b {
			b[i] = 'k'
		}
		return string(b)
	})
}

// keyTouchingOps are the operations that affect target state.
var keyTouchingOps = []event.Op{
	event.OpSet, event.OpDelete, event.OpAdd, event.OpRemove, event.OpIncr,
}

// allOps includes the markers and heartbeats that advance a sequence without
// touching a key.
var allOps = append(append([]event.Op{}, keyTouchingOps...),
	event.OpSnapshotBegin, event.OpSnapshotEnd, event.OpHeartbeat)

// AllOps returns the operations Op can produce.
//
// Exported so a test can assert the generator's range directly rather than by
// sampling it. The two are not the same claim: "this generator can produce
// every operation" is a fact about the slice, and checking it by drawing until
// all eight have appeared is a probabilistic approximation that fails on the
// run where one of them happens not to come up.
//
// The returned slice is a copy, so a caller cannot narrow what every property
// test in the repository generates.
func AllOps() []event.Op { return append([]event.Op(nil), allOps...) }

// KeyTouchingOps returns the operations KeyTouchingOp can produce, as a copy.
func KeyTouchingOps() []event.Op { return append([]event.Op(nil), keyTouchingOps...) }

// Op generates any operation, including the ones that touch no key.
func Op(t *rapid.T) event.Op {
	return rapid.SampledFrom(allOps).Draw(t, "op")
}

// KeyTouchingOp generates only operations that affect target state.
func KeyTouchingOp(t *rapid.T) event.Op {
	return rapid.SampledFrom(keyTouchingOps).Draw(t, "keyTouchingOp")
}

// Event generates a structurally valid event for the given publisher and
// sequence number. The returned event always passes Validate, so a property
// test that fails is reporting a real defect rather than a malformed input.
func Event(t *rapid.T, pub string, seq uint64) event.Event {
	return eventWithOp(t, pub, seq, Op(t))
}

// KeyEvent generates a structurally valid event that touches a key.
func KeyEvent(t *rapid.T, pub string, seq uint64) event.Event {
	return eventWithOp(t, pub, seq, KeyTouchingOp(t))
}

func eventWithOp(t *rapid.T, pub string, seq uint64, op event.Op) event.Event {
	e := event.Event{
		Publisher: pub,
		Epoch:     1,
		Seq:       seq,
		Op:        op,
	}

	if op.TouchesKey() {
		e.Key = Key(t)
	}

	switch op {
	case event.OpSet:
		e.Value = []byte(rapid.StringN(0, 12, 12).Draw(t, "value"))
	case event.OpAdd, event.OpRemove:
		e.Member = Member(t)
	case event.OpIncr:
		// Zero is excluded because Validate rejects a no-op increment.
		d := rapid.Int64Range(-1000, 1000).Draw(t, "delta")
		if d == 0 {
			d = 1
		}
		e.Delta = d
	case event.OpUnknown, event.OpDelete, event.OpSnapshotBegin, event.OpSnapshotEnd, event.OpHeartbeat:
		// No additional fields.
	}
	return e
}

// EventStream generates count events spread across the given number of
// publishers. Each publisher's sequence numbers are contiguous and ascending
// starting at 1, and the returned slice preserves that per-publisher order
// while interleaving publishers arbitrarily — which is what a real multi-
// publisher subscription looks like.
func EventStream(t *rapid.T, publishers, count int) []event.Event {
	if publishers < 1 {
		publishers = 1
	}
	if count < 0 {
		count = 0
	}

	next := make([]uint64, publishers)
	for i := range next {
		next[i] = 1
	}

	out := make([]event.Event, 0, count)
	for i := 0; i < count; i++ {
		p := rapid.IntRange(0, publishers-1).Draw(t, "publisher")
		pub := "pub-" + strconv.Itoa(p)
		out = append(out, Event(t, pub, next[p]))
		next[p]++
	}
	return out
}

// KeyEventStream is EventStream restricted to operations that touch a key.
// Projection properties use it, since markers and heartbeats are no-ops there
// and would only dilute the generated cases.
func KeyEventStream(t *rapid.T, publishers, count int) []event.Event {
	if publishers < 1 {
		publishers = 1
	}
	if count < 0 {
		count = 0
	}

	next := make([]uint64, publishers)
	for i := range next {
		next[i] = 1
	}

	out := make([]event.Event, 0, count)
	for i := 0; i < count; i++ {
		p := rapid.IntRange(0, publishers-1).Draw(t, "publisher")
		pub := "pub-" + strconv.Itoa(p)
		out = append(out, KeyEvent(t, pub, next[p]))
		next[p]++
	}
	return out
}

// Permutation returns a shuffled copy of evs. The input is not modified, so a
// caller can compare the permuted result against the original.
func Permutation(t *rapid.T, evs []event.Event) []event.Event {
	out := make([]event.Event, len(evs))
	copy(out, evs)

	// Fisher-Yates with rapid-drawn indices, so a failing permutation shrinks
	// toward the identity ordering rather than staying random.
	for i := len(out) - 1; i > 0; i-- {
		j := rapid.IntRange(0, i).Draw(t, "swap")
		out[i], out[j] = out[j], out[i]
	}
	return out
}

// WithdrawSubset splits evs into the events that are delivered and the events
// that are withheld, modeling a lossy channel. Neither slice aliases the input.
func WithdrawSubset(t *rapid.T, evs []event.Event) (kept, withheld []event.Event) {
	kept = make([]event.Event, 0, len(evs))
	withheld = make([]event.Event, 0)
	for i := range evs {
		if rapid.Bool().Draw(t, "withhold") {
			withheld = append(withheld, evs[i])
			continue
		}
		kept = append(kept, evs[i])
	}
	return kept, withheld
}

// Value generates a value of the requested kind. ValueSet may produce an empty
// set, which is the case that must compare equal to an absent value.
func Value(t *rapid.T, kind event.ValueKind) event.Value {
	switch kind {
	case event.ValueScalar:
		return event.Value{
			Kind:   event.ValueScalar,
			Scalar: []byte(rapid.OneOf(rapid.StringN(0, 12, 12), binaryString(0, 8)).Draw(t, "scalar")),
		}
	case event.ValueSet:
		n := rapid.IntRange(0, 6).Draw(t, "members")
		members := make(map[string]struct{}, n)
		for i := 0; i < n; i++ {
			members[Member(t)] = struct{}{}
		}
		return event.Value{Kind: event.ValueSet, Members: members}
	case event.ValueCounter:
		return event.Value{Kind: event.ValueCounter, Counter: rapid.Int64().Draw(t, "counter")}
	case event.ValueAbsent:
		return event.Value{}
	default:
		return event.Value{}
	}
}

// AnyValue generates a value of any kind, including absent.
func AnyValue(t *rapid.T) event.Value {
	kind := rapid.SampledFrom([]event.ValueKind{
		event.ValueAbsent, event.ValueScalar, event.ValueSet, event.ValueCounter,
	}).Draw(t, "kind")
	return Value(t, kind)
}
