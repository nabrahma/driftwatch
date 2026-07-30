package projection_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/projection"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func newProjection(t *testing.T, name string, cfg map[string]string) projection.Projection {
	t.Helper()
	p, err := projection.New(name, cfg)
	require.NoError(t, err)
	return p
}

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

func addEvent(key, member string) *event.Event {
	return &event.Event{Publisher: "p", Seq: 1, Op: event.OpAdd, Key: key, Member: member}
}

func removeEvent(key, member string) *event.Event {
	return &event.Event{Publisher: "p", Seq: 1, Op: event.OpRemove, Key: key, Member: member}
}

// TestKeysetOwnership_LastMemberRemoval_YieldsDelete is the single most likely
// bug in the project, written before the implementation and named explicitly by
// PRD §9 M6.
//
// Redis deletes a set key when its final member is removed via SREM. A
// projection that emits an upsert with an empty member set instead of a delete
// therefore expects a key the target will never have, and every key that ever
// empties becomes a permanent false missing_in_target. Emptying is the most
// common transition in a KV-cache ownership index, so this one mistake would
// produce enough false positives to make driftwatch unusable.
func TestKeysetOwnership_LastMemberRemoval_YieldsDelete(t *testing.T) {
	p := newProjection(t, "keysetOwnership", nil)

	got, err := p.Apply(setOf("replica-0"), removeEvent("block-9f3a", "replica-0"))

	require.NoError(t, err)
	assert.Equal(t, projection.Mutation{Key: "block-9f3a", Action: projection.ActionDelete}, got)
	assert.NotEqual(t, projection.ActionUpsert, got.Action,
		"an upsert with an empty set expects a key Redis will never have")
}

func TestKeysetOwnership_Apply(t *testing.T) {
	tests := []struct {
		name    string
		prev    event.Value
		event   *event.Event
		want    projection.Mutation
		wantErr error
	}{
		{
			name:  "adding the first member creates the set",
			prev:  event.Value{},
			event: addEvent("k", "replica-0"),
			want:  projection.Mutation{Key: "k", Action: projection.ActionUpsert, Value: setOf("replica-0")},
		},
		{
			name:  "adding a second member extends the set",
			prev:  setOf("replica-0"),
			event: addEvent("k", "replica-1"),
			want:  projection.Mutation{Key: "k", Action: projection.ActionUpsert, Value: setOf("replica-0", "replica-1")},
		},
		{
			name: "adding a member that is already present still upserts, so the version advances",
			// The version bump is what lets the sweeper's fence tell a stale
			// read from a current one. Suppressing it to save a write would
			// make an idempotent republish look like no event at all.
			prev:  setOf("replica-0"),
			event: addEvent("k", "replica-0"),
			want:  projection.Mutation{Key: "k", Action: projection.ActionUpsert, Value: setOf("replica-0")},
		},
		{
			name:  "removing the last member yields a delete, matching Redis empty-set semantics",
			prev:  setOf("replica-0"),
			event: removeEvent("k", "replica-0"),
			want:  projection.Mutation{Key: "k", Action: projection.ActionDelete},
		},
		{
			name:  "removing one of several members shrinks the set",
			prev:  setOf("replica-0", "replica-1"),
			event: removeEvent("k", "replica-0"),
			want:  projection.Mutation{Key: "k", Action: projection.ActionUpsert, Value: setOf("replica-1")},
		},
		{
			name:  "removing a member that is not present is a no-op",
			prev:  setOf("replica-0"),
			event: removeEvent("k", "replica-1"),
			want:  projection.Mutation{Key: "k", Action: projection.ActionNone},
		},
		{
			name: "removing from an absent key is a no-op and must not create the key",
			prev: event.Value{},
			// Creating an empty set here would immediately be equal to absent
			// anyway, but it would also bump a version and reset a settlement
			// timer for a key nothing happened to.
			event: removeEvent("k", "replica-0"),
			want:  projection.Mutation{Key: "k", Action: projection.ActionNone},
		},
		{
			name:  "deleting an existing key yields a delete",
			prev:  setOf("replica-0"),
			event: &event.Event{Publisher: "p", Op: event.OpDelete, Key: "k"},
			want:  projection.Mutation{Key: "k", Action: projection.ActionDelete},
		},
		{
			name:  "deleting an absent key is a no-op",
			prev:  event.Value{},
			event: &event.Event{Publisher: "p", Op: event.OpDelete, Key: "k"},
			want:  projection.Mutation{Key: "k", Action: projection.ActionNone},
		},
		{
			name:  "a set replaces the whole member list from the delimited value",
			prev:  setOf("replica-9"),
			event: &event.Event{Publisher: "p", Op: event.OpSet, Key: "k", Value: []byte("replica-0,replica-1")},
			want:  projection.Mutation{Key: "k", Action: projection.ActionUpsert, Value: setOf("replica-0", "replica-1")},
		},
		{
			name:  "a set with one member replaces the list",
			prev:  setOf("replica-9"),
			event: &event.Event{Publisher: "p", Op: event.OpSet, Key: "k", Value: []byte("replica-0")},
			want:  projection.Mutation{Key: "k", Action: projection.ActionUpsert, Value: setOf("replica-0")},
		},
		{
			name:  "a set with an empty value yields a delete, for the same reason as the last removal",
			prev:  setOf("replica-9"),
			event: &event.Event{Publisher: "p", Op: event.OpSet, Key: "k", Value: []byte("")},
			want:  projection.Mutation{Key: "k", Action: projection.ActionDelete},
		},
		{
			name:  "empty fields between delimiters are dropped rather than becoming empty members",
			prev:  event.Value{},
			event: &event.Event{Publisher: "p", Op: event.OpSet, Key: "k", Value: []byte("a,,b,")},
			want:  projection.Mutation{Key: "k", Action: projection.ActionUpsert, Value: setOf("a", "b")},
		},
		{
			name:  "a heartbeat touches nothing",
			prev:  setOf("replica-0"),
			event: &event.Event{Publisher: "p", Op: event.OpHeartbeat},
			want:  projection.Mutation{Action: projection.ActionNone},
		},
		{
			name:  "a snapshot marker touches nothing",
			prev:  setOf("replica-0"),
			event: &event.Event{Publisher: "p", Op: event.OpSnapshotBegin},
			want:  projection.Mutation{Action: projection.ActionNone},
		},
		{
			name:    "an increment is not a set operation",
			prev:    setOf("replica-0"),
			event:   &event.Event{Publisher: "p", Op: event.OpIncr, Key: "k", Delta: 1},
			wantErr: projection.ErrUnsupportedOp,
		},
		{
			name: "a previous value of the wrong shape is reported rather than panicking",
			// Reachable in practice: changing a DriftCheck's projection without
			// clearing the oracle leaves scalars where sets are expected.
			prev:    scalarOf("not-a-set"),
			event:   addEvent("k", "replica-0"),
			wantErr: projection.ErrShapeMismatch,
		},
		{
			name:  "the empty key is supported, because Redis accepts it",
			prev:  event.Value{},
			event: addEvent("", "replica-0"),
			want:  projection.Mutation{Key: "", Action: projection.ActionUpsert, Value: setOf("replica-0")},
		},
		{
			name:  "a binary member is supported",
			prev:  event.Value{},
			event: addEvent("k", "\x00\xff"),
			want:  projection.Mutation{Key: "k", Action: projection.ActionUpsert, Value: setOf("\x00\xff")},
		},
	}

	p := newProjection(t, "keysetOwnership", nil)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := p.Apply(tc.prev, tc.event)

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want.Key, got.Key)
			assert.Equal(t, tc.want.Action, got.Action)
			assert.True(t, tc.want.Value.Equal(got.Value),
				"value: want %s, got %s", tc.want.Value, got.Value)
		})
	}
}

func TestKeysetOwnership_DoesNotMutateThePreviousValue(t *testing.T) {
	// The oracle passes its live value in. A projection that mutated it would
	// corrupt state that the sweeper may be reading concurrently, and would do
	// so without bumping the version that fencing depends on.
	p := newProjection(t, "keysetOwnership", nil)
	prev := setOf("replica-0", "replica-1")

	_, err := p.Apply(prev, addEvent("k", "replica-2"))
	require.NoError(t, err)
	_, err = p.Apply(prev, removeEvent("k", "replica-0"))
	require.NoError(t, err)

	assert.True(t, prev.Equal(setOf("replica-0", "replica-1")),
		"Apply modified the value it was given: %s", prev)
}

func TestKeysetOwnership_MemberLimitClampsRatherThanGrowingWithoutBound(t *testing.T) {
	p := newProjection(t, "keysetOwnership", map[string]string{"maxMembersPerKey": "3"})

	prev := setOf("a", "b", "c")
	got, err := p.Apply(prev, addEvent("k", "d"))

	// An unbounded member set is an out-of-memory kill. The add is refused, the
	// key is flagged, and the mutation is still applied so the oracle records
	// that this key's view is incomplete.
	require.NoError(t, err)
	assert.Equal(t, projection.ActionUpsert, got.Action)
	assert.True(t, got.Truncated, "the key must be flagged as truncated")
	assert.True(t, got.Value.Equal(setOf("a", "b", "c")))
}

func TestKeysetOwnership_MemberLimitDoesNotBlockUpdatesToExistingMembers(t *testing.T) {
	p := newProjection(t, "keysetOwnership", map[string]string{"maxMembersPerKey": "3"})

	// Re-adding a member already in a full set does not grow it, so it must
	// not be refused.
	got, err := p.Apply(setOf("a", "b", "c"), addEvent("k", "a"))

	require.NoError(t, err)
	assert.False(t, got.Truncated)
	assert.True(t, got.Value.Equal(setOf("a", "b", "c")))
}

func TestKeysetOwnership_SetIsTruncatedAtTheMemberLimit(t *testing.T) {
	p := newProjection(t, "keysetOwnership", map[string]string{"maxMembersPerKey": "2"})

	got, err := p.Apply(event.Value{}, &event.Event{
		Publisher: "p", Op: event.OpSet, Key: "k", Value: []byte("a,b,c,d"),
	})

	require.NoError(t, err)
	assert.True(t, got.Truncated)
	assert.Len(t, got.Value.Members, 2)
}

func TestKeysetOwnership_CustomDelimiter(t *testing.T) {
	p := newProjection(t, "keysetOwnership", map[string]string{"setDelimiter": "|"})

	got, err := p.Apply(event.Value{}, &event.Event{
		Publisher: "p", Op: event.OpSet, Key: "k", Value: []byte("a|b"),
	})

	require.NoError(t, err)
	assert.True(t, got.Value.Equal(setOf("a", "b")))
}

func TestKeysetOwnership_Templates(t *testing.T) {
	tests := []struct {
		name       string
		cfg        map[string]string
		event      *event.Event
		wantKey    string
		wantMember string
	}{
		{
			name:       "the default templates pass the key and member through unchanged",
			event:      addEvent("block-1", "replica-0"),
			wantKey:    "block-1",
			wantMember: "replica-0",
		},
		{
			name: "a key template can add the prefix the materializer uses",
			cfg:  map[string]string{"keyTemplate": "kv:{{.Key}}"},
			event: &event.Event{
				Publisher: "replica-2", Op: event.OpAdd, Key: "block-1", Member: "replica-0",
			},
			wantKey:    "kv:block-1",
			wantMember: "replica-0",
		},
		{
			name: "a member template can derive the member from the publisher",
			// This is the shape of the real KV-cache format: the producer names
			// its replica once and means it as the set member.
			cfg: map[string]string{"memberTemplate": "{{.Publisher}}"},
			event: &event.Event{
				Publisher: "replica-2", Op: event.OpAdd, Key: "block-1", Member: "ignored",
			},
			wantKey:    "block-1",
			wantMember: "replica-2",
		},
		{
			name: "templates can reference every identity field",
			cfg: map[string]string{
				"keyTemplate":    "{{.Topic}}/{{.Key}}/{{.Epoch}}/{{.Seq}}",
				"memberTemplate": "{{.Publisher}}:{{.Member}}",
			},
			event: &event.Event{
				Publisher: "replica-2", Topic: "kv", Epoch: 3, Seq: 7,
				Op: event.OpAdd, Key: "block-1", Member: "m",
			},
			wantKey:    "kv/block-1/3/7",
			wantMember: "replica-2:m",
		},
		{
			name:       "a template may expand to the empty key, which Redis accepts",
			cfg:        map[string]string{"keyTemplate": "{{.Member}}"},
			event:      addEvent("block-1", ""),
			wantKey:    "",
			wantMember: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := newProjection(t, "keysetOwnership", tc.cfg)

			got, err := p.Apply(event.Value{}, tc.event)

			require.NoError(t, err)
			assert.Equal(t, tc.wantKey, got.Key)
			assert.True(t, got.Value.Equal(setOf(tc.wantMember)),
				"members: want %q, got %s", tc.wantMember, got.Value)
		})
	}
}

func TestKeysetOwnership_TemplateErrors(t *testing.T) {
	t.Run("an unparseable template is rejected at construction, not at the first event", func(t *testing.T) {
		_, err := projection.New("keysetOwnership", map[string]string{"keyTemplate": "{{.Key"})

		assert.ErrorIs(t, err, projection.ErrBadConfig)
	})

	t.Run("a template referencing a field the event does not have is rejected at construction", func(t *testing.T) {
		_, err := projection.New("keysetOwnership", map[string]string{"keyTemplate": "{{.Nonexistent}}"})

		assert.ErrorIs(t, err, projection.ErrBadConfig)
	})

	t.Run("a non-numeric member limit is rejected", func(t *testing.T) {
		_, err := projection.New("keysetOwnership", map[string]string{"maxMembersPerKey": "lots"})

		assert.ErrorIs(t, err, projection.ErrBadConfig)
	})

	t.Run("an empty delimiter is rejected, since it would split every byte into a member", func(t *testing.T) {
		_, err := projection.New("keysetOwnership", map[string]string{"setDelimiter": ""})

		assert.ErrorIs(t, err, projection.ErrBadConfig)
	})
}

func TestKeysetOwnership_Metadata(t *testing.T) {
	p := newProjection(t, "keysetOwnership", nil)

	assert.Equal(t, "keysetOwnership", p.Name())
	assert.Equal(t, projection.ShapeSet, p.TargetShape())
	assert.False(t, p.Commutative(),
		"add-then-remove and remove-then-add reach different states, so order matters")
	assert.False(t, p.KeyOwnership().Partitioned)
}

func TestKeysetOwnership_KeyOwnershipNarrowsTheBlastRadiusOfAGap(t *testing.T) {
	// When a publisher only ever writes keys matching a declared pattern, a gap
	// in its stream can only have affected that partition. Without the
	// declaration every key in the store becomes suspect.
	p := newProjection(t, "keysetOwnership", map[string]string{
		"keyPattern": "replica:{{.Publisher}}:*",
	})

	ownership := p.KeyOwnership()

	assert.True(t, ownership.Partitioned)
	assert.Equal(t, "replica:{{.Publisher}}:*", ownership.KeyPattern)
}

func TestKeysetOwnership_ATemplateThatOnlyFailsOnSomeDataIsReportedPerEvent(t *testing.T) {
	// Templates are validated at construction against a zero value, which
	// catches the overwhelming majority of mistakes. A template can still be
	// data-dependent, and when it fails mid-stream the event has to be reported
	// rather than silently producing a wrong key.
	tests := []struct {
		name  string
		cfg   map[string]string
		event *event.Event
	}{
		{
			name:  "a key template that fails on a long key",
			cfg:   map[string]string{"keyTemplate": `{{if gt (len .Key) 3}}{{slice .Key 0 100}}{{end}}`},
			event: addEvent("this-key-is-long-enough-to-trip-it", "m"),
		},
		{
			name:  "a member template that fails on a long member",
			cfg:   map[string]string{"memberTemplate": `{{if gt (len .Member) 3}}{{slice .Member 0 100}}{{end}}`},
			event: addEvent("k", "this-member-is-long-enough-to-trip-it"),
		},
		{
			name:  "a member template that fails on a remove",
			cfg:   map[string]string{"memberTemplate": `{{if gt (len .Member) 3}}{{slice .Member 0 100}}{{end}}`},
			event: removeEvent("k", "this-member-is-long-enough-to-trip-it"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, err := projection.New("keysetOwnership", tc.cfg)
			require.NoError(t, err, "the template must be valid against the zero value")

			_, err = p.Apply(event.Value{}, tc.event)

			assert.ErrorIs(t, err, projection.ErrBadConfig)
		})
	}
}

func TestKeysetOwnership_DeletePropagatesTheEventTTL(t *testing.T) {
	p := newProjection(t, "keysetOwnership", nil)
	ttl := 30 * time.Second

	got, err := p.Apply(setOf("m"), &event.Event{
		Publisher: "p", Op: event.OpDelete, Key: "k", TTL: &ttl,
	})

	require.NoError(t, err)
	require.NotNil(t, got.TTL)
	assert.Equal(t, ttl, *got.TTL)
}
