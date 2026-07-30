package event_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/nabrahma/driftwatch/pkg/event"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func ptr[T any](v T) *T { return &v }

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

func TestOp_StringNamesEveryOp(t *testing.T) {
	tests := []struct {
		name string
		op   event.Op
		want string
	}{
		{name: "the zero value renders as unknown", op: event.OpUnknown, want: "unknown"},
		{name: "set renders as set", op: event.OpSet, want: "set"},
		{name: "delete renders as delete", op: event.OpDelete, want: "delete"},
		{name: "add renders as add", op: event.OpAdd, want: "add"},
		{name: "remove renders as remove", op: event.OpRemove, want: "remove"},
		{name: "incr renders as incr", op: event.OpIncr, want: "incr"},
		{name: "snapshot begin renders as snapshot_begin", op: event.OpSnapshotBegin, want: "snapshot_begin"},
		{name: "snapshot end renders as snapshot_end", op: event.OpSnapshotEnd, want: "snapshot_end"},
		{name: "heartbeat renders as heartbeat", op: event.OpHeartbeat, want: "heartbeat"},
		{name: "an out-of-range op renders with its numeric value", op: event.Op(200), want: "Op(200)"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.op.String())
		})
	}
}

func TestOp_TouchesKeyIsFalseForMarkersAndHeartbeat(t *testing.T) {
	tests := []struct {
		name string
		op   event.Op
		want bool
	}{
		{name: "set touches a key", op: event.OpSet, want: true},
		{name: "delete touches a key", op: event.OpDelete, want: true},
		{name: "add touches a key", op: event.OpAdd, want: true},
		{name: "remove touches a key", op: event.OpRemove, want: true},
		{name: "incr touches a key", op: event.OpIncr, want: true},
		{name: "snapshot begin touches no key", op: event.OpSnapshotBegin, want: false},
		{name: "snapshot end touches no key", op: event.OpSnapshotEnd, want: false},
		{name: "heartbeat touches no key", op: event.OpHeartbeat, want: false},
		{name: "unknown touches no key", op: event.OpUnknown, want: false},
		{name: "an op outside the defined range touches no key", op: event.Op(200), want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.op.TouchesKey())
		})
	}
}

func TestParseOp_AcceptsWireNamesAndRejectsAnythingElse(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    event.Op
		wantErr bool
	}{
		{name: "the canonical name parses", in: "add", want: event.OpAdd},
		{name: "parsing is case insensitive", in: "ADD", want: event.OpAdd},
		{name: "surrounding whitespace is ignored", in: "  remove\t", want: event.OpRemove},
		{name: "snapshot markers use their underscore form", in: "snapshot_begin", want: event.OpSnapshotBegin},
		{name: "the hyphenated snapshot form is also accepted", in: "snapshot-end", want: event.OpSnapshotEnd},
		{name: "an unrecognized name is rejected", in: "frobnicate", wantErr: true},
		{name: "the empty string is rejected", in: "", wantErr: true},
		{name: "the literal word unknown is rejected", in: "unknown", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := event.ParseOp(tc.in)
			if tc.wantErr {
				require.ErrorIs(t, err, event.ErrUnknownOp)
				assert.Equal(t, event.OpUnknown, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestEvent_ValidateCoversEveryOpAndMissingFieldCombination(t *testing.T) {
	// base is a structurally valid event; each case mutates exactly one thing so
	// that a failure names the field responsible.
	base := func(op event.Op) event.Event {
		return event.Event{Publisher: "replica-0", Epoch: 1, Seq: 7, Op: op, Key: "k"}
	}

	tests := []struct {
		name    string
		event   event.Event
		wantErr error
	}{
		{
			name:  "a well-formed set is valid",
			event: func() event.Event { e := base(event.OpSet); e.Value = []byte("v"); return e }(),
		},
		{
			name:  "a set with an empty but non-nil value is valid, since an empty string is a real Redis value",
			event: func() event.Event { e := base(event.OpSet); e.Value = []byte{}; return e }(),
		},
		{
			name:    "a set without a value is rejected",
			event:   base(event.OpSet),
			wantErr: event.ErrMissingField,
		},
		{
			name:  "a well-formed delete needs only a key",
			event: base(event.OpDelete),
		},
		{
			name:  "a well-formed add is valid",
			event: func() event.Event { e := base(event.OpAdd); e.Member = "m"; return e }(),
		},
		{
			name:    "an add without a member is rejected",
			event:   base(event.OpAdd),
			wantErr: event.ErrMissingField,
		},
		{
			name:  "a well-formed remove is valid",
			event: func() event.Event { e := base(event.OpRemove); e.Member = "m"; return e }(),
		},
		{
			name:    "a remove without a member is rejected",
			event:   base(event.OpRemove),
			wantErr: event.ErrMissingField,
		},
		{
			name:  "a well-formed incr is valid",
			event: func() event.Event { e := base(event.OpIncr); e.Delta = 3; return e }(),
		},
		{
			name:  "a negative delta is valid",
			event: func() event.Event { e := base(event.OpIncr); e.Delta = -3; return e }(),
		},
		{
			name:    "an incr with a zero delta is rejected, since it would be a no-op that still consumes a seq",
			event:   base(event.OpIncr),
			wantErr: event.ErrMissingField,
		},
		{
			name:  "a snapshot begin with no key is valid",
			event: func() event.Event { e := base(event.OpSnapshotBegin); e.Key = ""; return e }(),
		},
		{
			name:    "a snapshot begin carrying a key is rejected",
			event:   base(event.OpSnapshotBegin),
			wantErr: event.ErrUnexpectedField,
		},
		{
			name:  "a snapshot end with no key is valid",
			event: func() event.Event { e := base(event.OpSnapshotEnd); e.Key = ""; return e }(),
		},
		{
			name:    "a snapshot end carrying a key is rejected",
			event:   base(event.OpSnapshotEnd),
			wantErr: event.ErrUnexpectedField,
		},
		{
			name:  "a heartbeat with no key is valid",
			event: func() event.Event { e := base(event.OpHeartbeat); e.Key = ""; return e }(),
		},
		{
			name:    "a heartbeat carrying a key is rejected",
			event:   base(event.OpHeartbeat),
			wantErr: event.ErrUnexpectedField,
		},
		{
			name:    "a heartbeat carrying a member is rejected",
			event:   func() event.Event { e := base(event.OpHeartbeat); e.Key = ""; e.Member = "m"; return e }(),
			wantErr: event.ErrUnexpectedField,
		},
		{
			name:    "an unknown op is rejected",
			event:   base(event.OpUnknown),
			wantErr: event.ErrUnknownOp,
		},
		{
			name:    "an op outside the defined range is rejected",
			event:   base(event.Op(200)),
			wantErr: event.ErrUnknownOp,
		},
		{
			name:    "an event without a publisher is rejected, because sequence tracking is per publisher",
			event:   func() event.Event { e := base(event.OpDelete); e.Publisher = ""; return e }(),
			wantErr: event.ErrMissingField,
		},
		{
			name:  "an empty key is valid, because Redis accepts the empty string as a key",
			event: func() event.Event { e := base(event.OpDelete); e.Key = ""; return e }(),
		},
		{
			name:  "a binary key is valid",
			event: func() event.Event { e := base(event.OpDelete); e.Key = "\x00\xff\xfe"; return e }(),
		},
		{
			name:  "a binary member is valid",
			event: func() event.Event { e := base(event.OpAdd); e.Member = "\x00\xff"; return e }(),
		},
		{
			name:  "a seq of zero is valid, because seqtrack decides whether it is a legal epoch start",
			event: func() event.Event { e := base(event.OpDelete); e.Seq = 0; return e }(),
		},
		{
			name:  "a nil TTL means no expiry and is valid",
			event: func() event.Event { e := base(event.OpDelete); e.TTL = nil; return e }(),
		},
		{
			name:  "a zero TTL means expire immediately and is valid, distinct from nil",
			event: func() event.Event { e := base(event.OpDelete); e.TTL = ptr(time.Duration(0)); return e }(),
		},
		{
			name:    "a negative TTL is rejected",
			event:   func() event.Event { e := base(event.OpDelete); e.TTL = ptr(-time.Second); return e }(),
			wantErr: event.ErrInvalidField,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.event.Validate()
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestEvent_ValidateErrorNamesTheOffendingField(t *testing.T) {
	e := event.Event{Publisher: "p", Op: event.OpAdd, Key: "k"}

	err := e.Validate()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "member")
	assert.Contains(t, err.Error(), "add")
}

func TestEvent_FingerprintIdentifiesAnEventByPublisherEpochAndSeq(t *testing.T) {
	e := event.Event{Publisher: "p", Epoch: 2, Seq: 9, Key: "k", Op: event.OpDelete}

	assert.Equal(t, event.Fingerprint{Publisher: "p", Epoch: 2, Seq: 9}, e.Fingerprint())

	// Everything outside (publisher, epoch, seq) is payload, not identity.
	other := e
	other.Key = "different"
	other.Op = event.OpSet
	assert.Equal(t, e.Fingerprint(), other.Fingerprint())
}

func TestValue_EqualTreatsAnEmptySetAsAbsent(t *testing.T) {
	// This is the D-001 decision made explicit: Redis deletes a set key when its
	// last member is removed, so an empty set and an absent key are the same
	// observable state. Getting this wrong produces a permanent false
	// missing_in_target on every key that ever empties.
	empty := setOf()
	absent := event.Value{}

	assert.True(t, empty.Equal(absent))
	assert.True(t, absent.Equal(empty))

	nilMembers := event.Value{Kind: event.ValueSet, Members: nil}
	assert.True(t, nilMembers.Equal(absent))
	assert.True(t, nilMembers.Equal(empty))
}

func TestValue_Equal(t *testing.T) {
	tests := []struct {
		name string
		a, b event.Value
		want bool
	}{
		{name: "absent equals absent", a: event.Value{}, b: event.Value{}, want: true},
		{name: "identical scalars are equal", a: scalarOf("v"), b: scalarOf("v"), want: true},
		{name: "different scalars are not equal", a: scalarOf("v"), b: scalarOf("w"), want: false},
		{
			name: "a nil scalar equals an empty scalar, because Redis stores the empty string",
			a:    event.Value{Kind: event.ValueScalar, Scalar: nil},
			b:    event.Value{Kind: event.ValueScalar, Scalar: []byte{}},
			want: true,
		},
		{
			name: "an empty scalar does not equal an absent key, because an empty string is a value",
			a:    event.Value{Kind: event.ValueScalar, Scalar: []byte{}},
			b:    event.Value{},
			want: false,
		},
		{name: "sets with the same members are equal regardless of insertion order", a: setOf("a", "b"), b: setOf("b", "a"), want: true},
		{name: "sets of different sizes are not equal", a: setOf("a", "b"), b: setOf("a"), want: false},
		{name: "sets of the same size with different members are not equal", a: setOf("a"), b: setOf("b"), want: false},
		{name: "identical counters are equal", a: counterOf(5), b: counterOf(5), want: true},
		{name: "different counters are not equal", a: counterOf(5), b: counterOf(6), want: false},
		{
			name: "a zero counter does not equal an absent key, because Redis stores the string 0",
			a:    counterOf(0),
			b:    event.Value{},
			want: false,
		},
		{name: "a scalar never equals a set", a: scalarOf("a"), b: setOf("a"), want: false},
		{name: "a scalar never equals a counter", a: scalarOf("5"), b: counterOf(5), want: false},
		{name: "a non-empty set never equals an absent key", a: setOf("a"), b: event.Value{}, want: false},
		{name: "binary scalars compare byte-wise", a: scalarOf("\x00\xff"), b: scalarOf("\x00\xff"), want: true},
		{
			name: "two values of an unrecognized kind are never equal, because their shape is unknown",
			a:    event.Value{Kind: event.ValueKind(9)},
			b:    event.Value{Kind: event.ValueKind(9)},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.a.Equal(tc.b))
			assert.Equal(t, tc.want, tc.b.Equal(tc.a), "Equal must be symmetric")
		})
	}
}

func TestValue_IsAbsentIncludesTheEmptySet(t *testing.T) {
	tests := []struct {
		name string
		v    event.Value
		want bool
	}{
		{name: "the zero value is absent", v: event.Value{}, want: true},
		{name: "an empty set is absent, matching Redis set semantics", v: setOf(), want: true},
		{name: "a set with a nil member map is absent", v: event.Value{Kind: event.ValueSet}, want: true},
		{name: "a set with members is present", v: setOf("a"), want: false},
		{name: "an empty scalar is present", v: event.Value{Kind: event.ValueScalar, Scalar: []byte{}}, want: false},
		{name: "a zero counter is present", v: counterOf(0), want: false},
		{name: "an unrecognized kind is not treated as absent", v: event.Value{Kind: event.ValueKind(9)}, want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.v.IsAbsent())
		})
	}
}

func TestValue_CloneIsADeepCopy(t *testing.T) {
	original := event.Value{
		Kind:    event.ValueSet,
		Scalar:  []byte("scalar"),
		Members: map[string]struct{}{"a": {}, "b": {}},
		Counter: 7,
	}

	clone := original.Clone()
	require.True(t, clone.Equal(original))

	// Mutating every mutable field of the clone must leave the original intact.
	clone.Scalar[0] = 'X'
	clone.Members["c"] = struct{}{}
	delete(clone.Members, "a")
	clone.Counter = 99

	assert.Equal(t, []byte("scalar"), original.Scalar)
	assert.Len(t, original.Members, 2)
	assert.Contains(t, original.Members, "a")
	assert.NotContains(t, original.Members, "c")
	assert.Equal(t, int64(7), original.Counter)
}

func TestValue_CloneOfAnAbsentValueAllocatesNothing(t *testing.T) {
	absent := event.Value{}

	clone := absent.Clone()

	assert.Nil(t, clone.Scalar)
	assert.Nil(t, clone.Members)
	assert.True(t, clone.IsAbsent())
}

func TestValue_StringIsTruncatedAndSafeForLogs(t *testing.T) {
	long := make([]byte, 200)
	for i := range long {
		long[i] = 'a'
	}

	tests := []struct {
		name string
		v    event.Value
		want string
	}{
		{name: "an absent value renders as absent", v: event.Value{}, want: "absent"},
		{name: "a printable scalar renders verbatim", v: scalarOf("hello"), want: `scalar("hello")`},
		{
			name: "a non-UTF8 scalar is hex encoded so it cannot corrupt a log line",
			v:    scalarOf("\x00\xff\xfe"),
			want: `scalar(hex:00fffe)`,
		},
		{
			name: "a long scalar is truncated to 64 bytes with the original length reported",
			v:    event.Value{Kind: event.ValueScalar, Scalar: long},
			want: `scalar("` + string(long[:64]) + `"...200B)`,
		},
		{
			name: "set members render in sorted order so the output is stable",
			v:    setOf("c", "a", "b"),
			want: `set(3)[a b c]`,
		},
		{name: "an empty set renders with its cardinality", v: setOf(), want: `set(0)[]`},
		{
			// Members sort by their raw bytes, not by their rendering, so the
			// order is stable no matter how a member is displayed.
			name: "a set with a non-UTF8 member hex encodes that member only",
			v:    setOf("ok", "\xff"),
			want: `set(2)[ok hex:ff]`,
		},
		{name: "a counter renders its value", v: counterOf(-12), want: "counter(-12)"},
		{name: "an out-of-range kind renders defensively", v: event.Value{Kind: event.ValueKind(9)}, want: "ValueKind(9)"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.v.String())
		})
	}
}

func TestValue_StringTruncatesLongBinaryScalars(t *testing.T) {
	binary := make([]byte, 100)
	for i := range binary {
		binary[i] = 0xff
	}

	got := event.Value{Kind: event.ValueScalar, Scalar: binary}.String()

	assert.Contains(t, got, "hex:")
	assert.Contains(t, got, "...100B", "the original length must survive truncation")
	assert.NotContains(t, got, "\xff", "raw bytes must never reach a log line")
}

func TestValue_StringTruncatesAnOverlongMember(t *testing.T) {
	long := ""
	for i := 0; i < 100; i++ {
		long += "m"
	}

	got := setOf(long).String()

	assert.Contains(t, got, "...")
	assert.LessOrEqual(t, len(got), 128)
}

func TestFingerprint_StringRendersPublisherEpochSeq(t *testing.T) {
	f := event.Fingerprint{Publisher: "replica-3", Epoch: 2, Seq: 4471}

	assert.Equal(t, "replica-3/2/4471", f.String())
}

func TestValue_StringTruncatesLongMemberLists(t *testing.T) {
	members := make([]string, 0, 40)
	for i := 0; i < 40; i++ {
		members = append(members, string(rune('a'+i%26))+string(rune('0'+i/26)))
	}

	got := setOf(members...).String()

	assert.Contains(t, got, "set(40)")
	assert.Contains(t, got, "...")
	assert.LessOrEqual(t, len(got), 128, "String must stay bounded for logs")
}

func TestValueKind_String(t *testing.T) {
	tests := []struct {
		name string
		k    event.ValueKind
		want string
	}{
		{name: "the zero kind is absent", k: event.ValueAbsent, want: "absent"},
		{name: "scalar", k: event.ValueScalar, want: "scalar"},
		{name: "set", k: event.ValueSet, want: "set"},
		{name: "counter", k: event.ValueCounter, want: "counter"},
		{name: "an out-of-range kind reports its number", k: event.ValueKind(9), want: "ValueKind(9)"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.k.String())
		})
	}
}

func TestTrustState_String(t *testing.T) {
	tests := []struct {
		name string
		s    event.TrustState
		want string
	}{
		{name: "the zero trust state is complete", s: event.TrustComplete, want: "complete"},
		{name: "suspect", s: event.TrustSuspect, want: "suspect"},
		{name: "adopted", s: event.TrustAdopted, want: "adopted"},
		{name: "an out-of-range state reports its number", s: event.TrustState(9), want: "TrustState(9)"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.s.String())
		})
	}
}
