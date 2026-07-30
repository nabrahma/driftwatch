package projection

import (
	"fmt"
	"strings"

	"github.com/nabrahma/driftwatch/pkg/event"
)

func init() { Register("keysetOwnership", newKeysetOwnership) }

// defaultMaxMembersPerKey bounds one key's member set. A set that can grow
// without limit is an out-of-memory kill, and the limit has to live in the
// projection because that is where members are added.
const defaultMaxMembersPerKey = 100_000

// keysetOwnership maintains key -> set of members. It is the KV-cache index
// shape and the flagship case for driftwatch: block hash to the set of replicas
// holding that block.
//
// The behavior that matters most is what happens when the last member is
// removed. Redis deletes a set key when its final member goes, so the
// projection emits ActionDelete rather than an upsert with an empty set.
// Getting this wrong would make every key that ever empties a permanent false
// missing_in_target, and emptying is the most common transition this index
// sees. PRD §9 M6 calls it the single most likely bug in the project, which is
// why TestKeysetOwnership_LastMemberRemoval_YieldsDelete was written first.
type keysetOwnership struct {
	keyTmpl    *expander
	memberTmpl *expander
	delimiter  string
	maxMembers int
	ownership  OwnershipModel
}

func newKeysetOwnership(cfg map[string]string) (Projection, error) {
	delimiter := stringConfig(cfg, "setDelimiter", ",")
	if delimiter == "" {
		return nil, fmt.Errorf("%w: setDelimiter must not be empty, or every byte becomes a member",
			ErrBadConfig)
	}

	maxMembers, err := intConfig(cfg, "maxMembersPerKey", defaultMaxMembersPerKey)
	if err != nil {
		return nil, err
	}

	pool := newBuilderPool()
	keyTmpl, err := newExpander("keyTemplate", stringConfig(cfg, "keyTemplate", "{{.Key}}"), pool)
	if err != nil {
		return nil, err
	}
	memberTmpl, err := newExpander("memberTemplate", stringConfig(cfg, "memberTemplate", "{{.Member}}"), pool)
	if err != nil {
		return nil, err
	}

	return &keysetOwnership{
		keyTmpl:    keyTmpl,
		memberTmpl: memberTmpl,
		delimiter:  delimiter,
		maxMembers: maxMembers,
		ownership:  ownershipFrom(cfg),
	}, nil
}

// Name returns the registry name.
func (k *keysetOwnership) Name() string { return "keysetOwnership" }

// Commutative reports false: adding then removing a member leaves a different
// set than removing then adding it.
func (k *keysetOwnership) Commutative() bool { return false }

// KeyOwnership reports the configured partitioning, if any.
func (k *keysetOwnership) KeyOwnership() OwnershipModel { return k.ownership }

// TargetShape reports that values map onto Redis sets.
func (k *keysetOwnership) TargetShape() Shape { return ShapeSet }

// Apply folds one event into the member set for its key.
func (k *keysetOwnership) Apply(prev event.Value, e *event.Event) (Mutation, error) {
	if !e.Op.TouchesKey() {
		return Mutation{Action: ActionNone}, nil
	}
	if err := checkShape(prev, event.ValueSet); err != nil {
		return Mutation{}, err
	}

	data := dataFor(e)
	key, err := k.keyTmpl.expand(data)
	if err != nil {
		return Mutation{}, err
	}

	switch e.Op {
	case event.OpAdd:
		member, memberErr := k.memberTmpl.expand(data)
		if memberErr != nil {
			return Mutation{}, memberErr
		}
		return k.add(key, member, prev), nil

	case event.OpRemove:
		member, memberErr := k.memberTmpl.expand(data)
		if memberErr != nil {
			return Mutation{}, memberErr
		}
		return k.remove(key, member, prev), nil

	case event.OpDelete:
		if prev.IsAbsent() {
			// Deleting a key that is already absent changes nothing. Emitting a
			// delete anyway would bump a version and restart a settlement timer
			// for a key nothing happened to.
			return Mutation{Key: key, Action: ActionNone}, nil
		}
		return Mutation{Key: key, Action: ActionDelete, TTL: e.TTL}, nil

	case event.OpSet:
		return k.replace(key, e), nil

	default:
		return Mutation{}, fmt.Errorf("%w: %s on keysetOwnership", ErrUnsupportedOp, e.Op)
	}
}

func (k *keysetOwnership) add(key, member string, prev event.Value) Mutation {
	_, already := prev.Members[member]

	// Re-adding an existing member does not grow the set, so the cap must not
	// refuse it. Only genuine growth is bounded.
	if !already && len(prev.Members) >= k.maxMembers {
		return Mutation{
			Key:       key,
			Action:    ActionUpsert,
			Value:     prev.Clone(),
			Truncated: true,
		}
	}

	next := cloneMembers(prev.Members, 1)
	next[member] = struct{}{}
	return Mutation{
		Key:    key,
		Action: ActionUpsert,
		Value:  event.Value{Kind: event.ValueSet, Members: next},
	}
}

func (k *keysetOwnership) remove(key, member string, prev event.Value) Mutation {
	if _, present := prev.Members[member]; !present {
		// Removing something that is not there is a no-op, including on a key
		// that does not exist. Creating the key to hold an empty set would
		// invent state the target never had.
		return Mutation{Key: key, Action: ActionNone}
	}

	next := cloneMembers(prev.Members, 0)
	delete(next, member)

	if len(next) == 0 {
		// The case this projection exists to get right: Redis deletes a set key
		// when its last member is removed, so the expectation must be absence,
		// not an empty set.
		return Mutation{Key: key, Action: ActionDelete}
	}
	return Mutation{
		Key:    key,
		Action: ActionUpsert,
		Value:  event.Value{Kind: event.ValueSet, Members: next},
	}
}

// replace rebuilds the whole member set from a delimited value.
func (k *keysetOwnership) replace(key string, e *event.Event) Mutation {
	members := make(map[string]struct{})
	truncated := false

	for _, field := range strings.Split(string(e.Value), k.delimiter) {
		if field == "" {
			// An empty field between delimiters is a formatting artifact, not a
			// member. Redis has no empty set members to speak of here, and
			// admitting one would make "a,b," differ from "a,b".
			continue
		}
		if len(members) >= k.maxMembers {
			truncated = true
			break
		}
		members[field] = struct{}{}
	}

	if len(members) == 0 {
		return Mutation{Key: key, Action: ActionDelete}
	}
	return Mutation{
		Key:       key,
		Action:    ActionUpsert,
		Value:     event.Value{Kind: event.ValueSet, Members: members},
		TTL:       e.TTL,
		Truncated: truncated,
	}
}

// cloneMembers copies a member set with room for extra additions. The oracle
// passes its live value in, so folding must never write through to it.
func cloneMembers(src map[string]struct{}, extra int) map[string]struct{} {
	out := make(map[string]struct{}, len(src)+extra)
	for m := range src {
		out[m] = struct{}{}
	}
	return out
}

var _ Projection = (*keysetOwnership)(nil)
