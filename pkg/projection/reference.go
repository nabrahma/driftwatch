package projection

import (
	"math"
	"strconv"
	"strings"

	"github.com/nabrahma/driftwatch/pkg/event"
)

// Reference is a deliberately naive, obviously-correct fold used as the
// differential-testing oracle in property tests.
//
// It is written to be read, not to be fast: plain maps, no cloning tricks, no
// pooling, no fast paths, no template compilation. If the optimized projection
// and this implementation ever disagree on generated input, the bug is in the
// optimized one — that is the whole point of having two.
//
// It lives in a regular file rather than a _test.go so that pkg/oracle and the
// fault-matrix suite can use the same oracle rather than each writing their own
// slightly different version of "obviously correct".
type Reference struct {
	shape     Shape
	delimiter string
	incrOnly  bool
}

// NewReference returns a reference fold for the given target shape.
func NewReference(shape Shape) *Reference {
	return &Reference{shape: shape, delimiter: ","}
}

// WithDelimiter sets the OpSet delimiter used by the set shape.
func (r *Reference) WithDelimiter(d string) *Reference {
	r.delimiter = d
	return r
}

// WithIncrOnly declares that the counter shape sees only increments.
func (r *Reference) WithIncrOnly(v bool) *Reference {
	r.incrOnly = v
	return r
}

// Fold applies every event in order and returns the resulting state.
//
// Keys that end up absent are omitted from the result rather than being stored
// as an empty value, so the returned map is exactly "what the target should
// contain".
func (r *Reference) Fold(events []event.Event) map[string]event.Value {
	state := map[string]event.Value{}

	for i := range events {
		e := &events[i]
		if !e.Op.TouchesKey() {
			continue
		}
		r.apply(state, e)
	}

	// Drop anything that ended up equivalent to absent. An empty member set is
	// the case that matters: Redis deletes a set key when its last member goes,
	// so a key holding an empty set is a key the target does not have.
	for k, v := range state {
		if v.IsAbsent() {
			delete(state, k)
		}
	}
	return state
}

func (r *Reference) apply(state map[string]event.Value, e *event.Event) {
	switch r.shape {
	case ShapeSet:
		r.applySet(state, e)
	case ShapeScalar:
		r.applyScalar(state, e)
	case ShapeCounter:
		r.applyCounter(state, e)
	}
}

func (r *Reference) applySet(state map[string]event.Value, e *event.Event) {
	current := map[string]struct{}{}
	if v, ok := state[e.Key]; ok {
		for m := range v.Members {
			current[m] = struct{}{}
		}
	}

	switch e.Op {
	case event.OpAdd:
		current[e.Member] = struct{}{}
	case event.OpRemove:
		delete(current, e.Member)
	case event.OpDelete:
		delete(state, e.Key)
		return
	case event.OpSet:
		current = map[string]struct{}{}
		for _, field := range strings.Split(string(e.Value), r.delimiter) {
			if field != "" {
				current[field] = struct{}{}
			}
		}
	default:
		// An operation this shape has no meaning for leaves state untouched;
		// the optimized implementation reports an error and the oracle drops
		// the event, so the observable outcome is the same.
		return
	}

	if len(current) == 0 {
		delete(state, e.Key)
		return
	}
	state[e.Key] = event.Value{Kind: event.ValueSet, Members: current}
}

func (r *Reference) applyScalar(state map[string]event.Value, e *event.Event) {
	switch e.Op {
	case event.OpSet:
		b := make([]byte, len(e.Value))
		copy(b, e.Value)
		state[e.Key] = event.Value{Kind: event.ValueScalar, Scalar: b}
	case event.OpDelete:
		delete(state, e.Key)
	default:
	}
}

func (r *Reference) applyCounter(state map[string]event.Value, e *event.Event) {
	switch e.Op {
	case event.OpIncr:
		var n int64
		if v, ok := state[e.Key]; ok {
			n = v.Counter
		}
		sum := n + e.Delta
		switch {
		case e.Delta > 0 && sum < n:
			sum = math.MaxInt64
		case e.Delta < 0 && sum > n:
			sum = math.MinInt64
		}
		state[e.Key] = event.Value{Kind: event.ValueCounter, Counter: sum}

	case event.OpSet:
		if r.incrOnly {
			return
		}
		n, err := strconv.ParseInt(string(e.Value), 10, 64)
		if err != nil {
			return
		}
		state[e.Key] = event.Value{Kind: event.ValueCounter, Counter: n}

	case event.OpDelete:
		delete(state, e.Key)

	default:
	}
}
