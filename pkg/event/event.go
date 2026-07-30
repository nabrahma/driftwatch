// Package event defines the immutable Event record and its value types (§4, M2).
//
// There is no logic here beyond validation and cheap accessors. An Event is
// treated as immutable once constructed: nothing in driftwatch mutates a
// received event, and projections return new values rather than editing the
// ones they are given.
package event

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Sentinel errors returned by this package. They are typed so the ingest
// pipeline can categorize a rejection instead of counting every failure the
// same way — an unknown op is a version mismatch, a missing field is a codec
// misconfiguration, and the two deserve different diagnoses.
var (
	// ErrUnknownOp reports an operation this build does not recognize.
	ErrUnknownOp = errors.New("unknown op")
	// ErrMissingField reports a field an operation requires but does not have.
	ErrMissingField = errors.New("missing required field")
	// ErrUnexpectedField reports a field set on an operation that forbids it.
	ErrUnexpectedField = errors.New("unexpected field")
	// ErrInvalidField reports a field that is present but structurally invalid.
	ErrInvalidField = errors.New("invalid field")
)

// Op is the kind of state change an event describes.
type Op uint8

// The operations driftwatch understands. OpUnknown is the zero value so an
// uninitialized Event fails validation rather than being read as a delete.
const (
	OpUnknown       Op = iota
	OpSet              // scalar assignment: Key = Value
	OpDelete           // remove Key entirely
	OpAdd              // add Member to the set at Key
	OpRemove           // remove Member from the set at Key
	OpIncr             // add Delta to the counter at Key
	OpSnapshotBegin    // publisher begins a full resync
	OpSnapshotEnd      // publisher completes a full resync
	OpHeartbeat        // liveness only; advances seq, touches no key
)

var opNames = [...]string{
	OpUnknown:       "unknown",
	OpSet:           "set",
	OpDelete:        "delete",
	OpAdd:           "add",
	OpRemove:        "remove",
	OpIncr:          "incr",
	OpSnapshotBegin: "snapshot_begin",
	OpSnapshotEnd:   "snapshot_end",
	OpHeartbeat:     "heartbeat",
}

// String returns the wire name of the operation.
func (o Op) String() string {
	if int(o) >= len(opNames) {
		return "Op(" + strconv.Itoa(int(o)) + ")"
	}
	return opNames[o]
}

// TouchesKey reports whether the operation affects target state. It is false
// for the snapshot markers and for heartbeats, which advance a publisher's
// sequence number without changing anything.
func (o Op) TouchesKey() bool {
	switch o {
	case OpSet, OpDelete, OpAdd, OpRemove, OpIncr:
		return true
	case OpUnknown, OpSnapshotBegin, OpSnapshotEnd, OpHeartbeat:
		return false
	default:
		return false
	}
}

// ParseOp resolves a wire name to an Op. Matching ignores case and surrounding
// whitespace, and accepts either "snapshot_begin" or "snapshot-begin", because
// both spellings occur in the wild. It returns ErrUnknownOp for anything else,
// including the literal string "unknown".
func ParseOp(s string) (Op, error) {
	normalized := strings.ReplaceAll(strings.ToLower(strings.TrimSpace(s)), "-", "_")
	for op, name := range opNames {
		if op == int(OpUnknown) {
			continue
		}
		if name == normalized {
			return Op(op), nil //nolint:gosec // op indexes a fixed table of Op values
		}
	}
	return OpUnknown, fmt.Errorf("%w: %q", ErrUnknownOp, s)
}

// Event is an immutable observed state change.
type Event struct {
	Publisher string
	Epoch     uint64
	Seq       uint64

	// PublishedAt is the producer's wall clock. Diagnostic only: never used for
	// settlement decisions, because producer clocks skew and a skew larger than
	// the settlement window would make settlement unsound (§5.3, F5).
	PublishedAt time.Time

	// ObservedAt is driftwatch's local receive time, monotonic. This is the
	// authoritative time for all elapsed-time logic.
	ObservedAt time.Time

	Topic  string
	Op     Op
	Key    string
	Member string
	Value  []byte
	Delta  int64

	// TTL distinguishes three states: nil means the event says nothing about
	// expiry, a pointer to zero means expire immediately, and a pointer to a
	// positive duration is a lifetime.
	TTL *time.Duration

	// Raw is the original wire bytes, retained only if retainRaw is enabled
	// (memory cost). Used by `explain`.
	Raw []byte
}

// Validate returns an error if the event violates a structural invariant for
// its Op.
//
// Validate is stateless, so it deliberately does not enforce the rule that Seq
// may be zero only as the first event of an epoch: that needs the publisher's
// history and belongs to pkg/seqtrack. An empty Key is likewise accepted,
// because Redis treats the empty string as a legal key.
func (e *Event) Validate() error {
	if e.Publisher == "" {
		return fmt.Errorf("%w: publisher (sequence tracking is per publisher)", ErrMissingField)
	}
	if e.TTL != nil && *e.TTL < 0 {
		return fmt.Errorf("%w: ttl %s is negative", ErrInvalidField, *e.TTL)
	}

	switch e.Op {
	case OpSet:
		if e.Value == nil {
			return fmt.Errorf("%w: value for op %s", ErrMissingField, e.Op)
		}
	case OpAdd, OpRemove:
		if e.Member == "" {
			return fmt.Errorf("%w: member for op %s", ErrMissingField, e.Op)
		}
	case OpIncr:
		if e.Delta == 0 {
			return fmt.Errorf("%w: delta for op %s must be non-zero", ErrMissingField, e.Op)
		}
	case OpDelete:
		// A key is all a delete needs, and the empty key is a legal key.
	case OpSnapshotBegin, OpSnapshotEnd, OpHeartbeat:
		if e.Key != "" {
			return fmt.Errorf("%w: key on op %s, which touches no key", ErrUnexpectedField, e.Op)
		}
		if e.Member != "" {
			return fmt.Errorf("%w: member on op %s, which touches no key", ErrUnexpectedField, e.Op)
		}
	case OpUnknown:
		return fmt.Errorf("%w: %s", ErrUnknownOp, e.Op)
	default:
		return fmt.Errorf("%w: %s", ErrUnknownOp, e.Op)
	}
	return nil
}

// Fingerprint returns a stable identity for dedup: publisher/epoch/seq.
func (e *Event) Fingerprint() Fingerprint {
	return Fingerprint{Publisher: e.Publisher, Epoch: e.Epoch, Seq: e.Seq}
}

// Fingerprint identifies one event within a publisher's sequence space.
type Fingerprint struct {
	Publisher string
	Epoch     uint64
	Seq       uint64
}

// String renders the fingerprint as publisher/epoch/seq.
func (f Fingerprint) String() string {
	return f.Publisher + "/" + strconv.FormatUint(f.Epoch, 10) + "/" + strconv.FormatUint(f.Seq, 10)
}

// ValueKind discriminates the shape of a Value.
type ValueKind uint8

// The value shapes a projection can produce. ValueAbsent is the zero value so
// that an unset Value reads as "this key does not exist", which is what every
// caller means by an empty value.
const (
	ValueAbsent ValueKind = iota
	ValueScalar
	ValueSet
	ValueCounter
)

var valueKindNames = [...]string{
	ValueAbsent:  "absent",
	ValueScalar:  "scalar",
	ValueSet:     "set",
	ValueCounter: "counter",
}

// String returns the name of the value kind.
func (k ValueKind) String() string {
	if int(k) >= len(valueKindNames) {
		return "ValueKind(" + strconv.Itoa(int(k)) + ")"
	}
	return valueKindNames[k]
}

// Value is the oracle-side representation of a key's state.
type Value struct {
	Kind    ValueKind
	Scalar  []byte
	Members map[string]struct{}
	Counter int64
}

// Equal reports whether two values describe the same observable target state.
//
// An empty member set equals an absent value. This is a deliberate choice, not
// an oversight: Redis deletes a set key when its last member is removed, so a
// set that has emptied and a key that never existed are indistinguishable from
// the target's side. Treating them as different would report drift on every key
// that legitimately empties — the most common transition in a KV-cache
// ownership index — which is enough false positives to make driftwatch
// unusable. See docs/DISCOVERIES.md D-001.
//
// The symmetry does not extend to scalars or counters: an empty string and the
// integer zero are real Redis values, distinct from a missing key.
func (v Value) Equal(other Value) bool {
	if v.IsAbsent() || other.IsAbsent() {
		return v.IsAbsent() && other.IsAbsent()
	}
	if v.Kind != other.Kind {
		return false
	}
	switch v.Kind {
	case ValueScalar:
		return bytes.Equal(v.Scalar, other.Scalar)
	case ValueSet:
		return maps.Equal(v.Members, other.Members)
	case ValueCounter:
		return v.Counter == other.Counter
	default:
		// ValueAbsent cannot reach here: the IsAbsent guard above returns
		// first. Anything else is an unrecognized kind, and two values of an
		// unrecognized shape are not comparable, so they are not equal.
		return false
	}
}

// Clone returns a deep copy. The member map and scalar bytes are copied, so the
// result shares no mutable state with the receiver.
func (v Value) Clone() Value {
	out := Value{Kind: v.Kind, Counter: v.Counter}
	if v.Scalar != nil {
		out.Scalar = bytes.Clone(v.Scalar)
	}
	if v.Members != nil {
		out.Members = maps.Clone(v.Members)
	}
	return out
}

// IsAbsent reports whether the value is equivalent to the key not existing in
// the target. An empty member set is absent, for the reason given on Equal.
func (v Value) IsAbsent() bool {
	switch v.Kind {
	case ValueAbsent:
		return true
	case ValueSet:
		return len(v.Members) == 0
	case ValueScalar, ValueCounter:
		return false
	default:
		return false
	}
}

// maxValueStringBytes bounds how much of a scalar String renders. Log lines and
// error messages are the only consumers; comparisons never go through String.
const maxValueStringBytes = 64

// maxValueStringMembers bounds how many set members String renders.
const maxValueStringMembers = 8

// String returns a truncated, log-safe rendering. Non-UTF8 bytes are hex
// encoded so a binary key cannot corrupt a log line or a terminal.
//
// String must never be used for comparison: it is lossy by construction.
func (v Value) String() string {
	switch v.Kind {
	case ValueAbsent:
		return "absent"
	case ValueScalar:
		return "scalar(" + quoteBytes(v.Scalar) + ")"
	case ValueSet:
		members := make([]string, 0, len(v.Members))
		for m := range v.Members {
			members = append(members, m)
		}
		sort.Strings(members)

		var b strings.Builder
		b.WriteString("set(")
		b.WriteString(strconv.Itoa(len(members)))
		b.WriteString(")[")
		for i, m := range members {
			if i == maxValueStringMembers {
				b.WriteString(" ...")
				break
			}
			if i > 0 {
				b.WriteByte(' ')
			}
			b.WriteString(safeString(m))
		}
		b.WriteByte(']')
		return b.String()
	case ValueCounter:
		return "counter(" + strconv.FormatInt(v.Counter, 10) + ")"
	default:
		return "ValueKind(" + strconv.Itoa(int(v.Kind)) + ")"
	}
}

// quoteBytes renders b for logs, truncating and reporting the original length
// when it is too long to show in full.
func quoteBytes(b []byte) string {
	if !utf8.Valid(b) {
		if len(b) > maxValueStringBytes {
			return "hex:" + hex.EncodeToString(b[:maxValueStringBytes]) + "..." +
				strconv.Itoa(len(b)) + "B"
		}
		return "hex:" + hex.EncodeToString(b)
	}
	if len(b) > maxValueStringBytes {
		return `"` + string(b[:maxValueStringBytes]) + `"...` + strconv.Itoa(len(b)) + "B"
	}
	return `"` + string(b) + `"`
}

// safeString renders a set member for logs, hex encoding it if it is not valid
// UTF-8.
func safeString(s string) string {
	if !utf8.ValidString(s) {
		return "hex:" + hex.EncodeToString([]byte(s))
	}
	if len(s) > maxValueStringBytes {
		return s[:maxValueStringBytes] + "..."
	}
	return s
}

// TrustState records whether driftwatch's own view of a key is complete.
//
// It lives here, in the leaf package, because both pkg/seqtrack (which decides
// when trust is lost) and pkg/oracle (which stores it per key) need the same
// type, and seqtrack must not import oracle.
//
// This distinction is what keeps driftwatch honest: it never claims the target
// is wrong while it knows its own subscription dropped events (§5.2).
type TrustState uint8

// The trust states a key can be in. TrustComplete is the zero value, so a key
// with no recorded problem is trusted.
const (
	// TrustComplete means no known gap affects this key.
	TrustComplete TrustState = iota
	// TrustSuspect means a gap was observed that may have affected this key.
	// Suspect keys are reported separately and never feed alerting.
	TrustSuspect
	// TrustAdopted means the key was loaded from the target at bootstrap and
	// has never been confirmed by an event.
	TrustAdopted
)

var trustStateNames = [...]string{
	TrustComplete: "complete",
	TrustSuspect:  "suspect",
	TrustAdopted:  "adopted",
}

// String returns the name of the trust state.
func (s TrustState) String() string {
	if int(s) >= len(trustStateNames) {
		return "TrustState(" + strconv.Itoa(int(s)) + ")"
	}
	return trustStateNames[s]
}
