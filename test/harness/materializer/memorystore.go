package materializer

import (
	"context"
	"strconv"

	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/target"
)

// MemoryStore is the write side of a target.MemoryTarget.
//
// It reads through the ordinary read-only Target interface and writes through
// the memory target's test-only seeding methods. That split is deliberate: the
// harness is allowed to read the store it writes, and driftwatch is allowed to
// do neither except through Target.
type MemoryStore struct {
	mem *target.MemoryTarget
	// fixture wraps every write so a RecordingTarget in the same test files it
	// as harness setup rather than as driftwatch attempting a mutation.
	fixture func(func())
}

// NewMemoryStore returns a store backed by mem.
//
// If rec is non-nil, writes are made inside its fixture scope. Every scenario
// passes one: the recording target is what proves driftwatch never wrote, and
// the materializer's own writes must not be mistaken for driftwatch's.
func NewMemoryStore(mem *target.MemoryTarget, rec *target.RecordingTarget) *MemoryStore {
	fixture := func(fn func()) { fn() }
	if rec != nil {
		fixture = rec.Fixture
	}
	return &MemoryStore{mem: mem, fixture: fixture}
}

// Set assigns a scalar.
func (s *MemoryStore) Set(key string, value []byte) {
	s.fixture(func() { s.mem.Seed(map[string][]byte{key: value}) })
}

// Delete removes a key entirely.
func (s *MemoryStore) Delete(key string) {
	s.fixture(func() { s.mem.Remove(key) })
}

// AddMember adds one member to the set at key.
func (s *MemoryStore) AddMember(key, member string) {
	members := s.members(key)
	for _, m := range members {
		if m == member {
			return
		}
	}
	s.fixture(func() { s.mem.SeedSets(map[string][]string{key: append(members, member)}) })
}

// RemoveMember removes one member.
//
// Removing the last member removes the key, which is what Redis does and what
// the empty-set trap in D-007 is about: a set with no members is not an empty
// set, it is an absent key.
func (s *MemoryStore) RemoveMember(key, member string) {
	members := s.members(key)

	kept := make([]string, 0, len(members))
	for _, m := range members {
		if m != member {
			kept = append(kept, m)
		}
	}
	if len(kept) == len(members) {
		return
	}

	s.fixture(func() {
		if len(kept) == 0 {
			s.mem.Remove(key)
			return
		}
		s.mem.SeedSets(map[string][]string{key: kept})
	})
}

// Incr adds delta to the counter at key.
func (s *MemoryStore) Incr(key string, delta int64) {
	current := int64(0)
	if v, err := s.mem.Get(context.Background(), key, projection.ShapeCounter); err == nil {
		if v.Kind == event.ValueCounter {
			current = v.Counter
		}
	}

	next := strconv.FormatInt(current+delta, 10)
	s.fixture(func() { s.mem.Seed(map[string][]byte{key: []byte(next)}) })
}

// members reads the current set at key.
func (s *MemoryStore) members(key string) []string {
	v, err := s.mem.Get(context.Background(), key, projection.ShapeSet)
	if err != nil || v.Kind != event.ValueSet {
		return nil
	}

	out := make([]string, 0, len(v.Members))
	for m := range v.Members {
		out = append(out, m)
	}
	return out
}
