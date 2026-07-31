package projection

import (
	"fmt"
	"math"
	"strconv"

	"github.com/nabrahma/driftwatch/pkg/event"
)

func init() { Register("counter", newCounter) }

// counter maintains key -> int64, applying OpIncr additively.
//
// Commutativity is a property of the configured instance, not of the type, and
// that distinction is the interesting part of this projection. Addition
// commutes, so a stream of nothing but increments reaches the same total in any
// order. Mix in a single absolute OpSet and it does not: set-then-increment and
// increment-then-set differ. Commutative() therefore reports false unless the
// operator declares incrOnly, which is a promise about the producer that
// driftwatch cannot verify on its own.
type counter struct {
	keyTmpl   *expander
	incrOnly  bool
	ownership OwnershipModel
}

func newCounter(cfg map[string]string) (Projection, error) {
	keyTmpl, err := newExpander("keyTemplate", stringConfig(cfg, "keyTemplate", "{{.Key}}"), newBuilderPool())
	if err != nil {
		return nil, err
	}
	incrOnly, err := boolConfig(cfg, "incrOnly")
	if err != nil {
		return nil, err
	}
	return &counter{keyTmpl: keyTmpl, incrOnly: incrOnly, ownership: ownershipFrom(cfg)}, nil
}

// Name returns the registry name.
func (c *counter) Name() string { return "counter" }

// Commutative reports whether this instance was declared increment-only.
func (c *counter) Commutative() bool { return c.incrOnly }

// KeyOwnership reports the configured partitioning, if any.
func (c *counter) KeyOwnership() OwnershipModel { return c.ownership }

// TargetKey returns the store key this event affects.
func (c *counter) TargetKey(e *event.Event) (string, error) {
	return c.keyTmpl.expand(dataFor(e))
}

// TargetShape reports that values map onto Redis integer strings.
func (c *counter) TargetShape() Shape { return ShapeCounter }

// Apply folds one event into the counter at its key.
func (c *counter) Apply(prev event.Value, e *event.Event) (Mutation, error) {
	if !e.Op.TouchesKey() {
		return Mutation{Action: ActionNone}, nil
	}
	if err := checkShape(prev, event.ValueCounter); err != nil {
		return Mutation{}, err
	}

	data := dataFor(e)
	key, err := c.keyTmpl.expand(data)
	if err != nil {
		return Mutation{}, err
	}

	switch e.Op {
	case event.OpIncr:
		sum, saturated := addSaturating(prev.Counter, e.Delta)
		return Mutation{
			Key:       key,
			Action:    ActionUpsert,
			Value:     event.Value{Kind: event.ValueCounter, Counter: sum},
			TTL:       e.TTL,
			Saturated: saturated,
		}, nil

	case event.OpSet:
		if c.incrOnly {
			// An absolute write breaks the commutativity the operator promised,
			// so it is refused rather than silently making the property tests'
			// premise false.
			return Mutation{}, fmt.Errorf(
				"%w: %s on a counter declared incrOnly", ErrUnsupportedOp, e.Op)
		}
		n, parseErr := strconv.ParseInt(string(e.Value), 10, 64)
		if parseErr != nil {
			return Mutation{}, fmt.Errorf("%w: counter value %q is not an int64", ErrBadValue, e.Value)
		}
		return Mutation{
			Key:    key,
			Action: ActionUpsert,
			Value:  event.Value{Kind: event.ValueCounter, Counter: n},
			TTL:    e.TTL,
		}, nil

	case event.OpDelete:
		if prev.IsAbsent() {
			return Mutation{Key: key, Action: ActionNone}, nil
		}
		return Mutation{Key: key, Action: ActionDelete, TTL: e.TTL}, nil

	default:
		return Mutation{}, fmt.Errorf("%w: %s on counter", ErrUnsupportedOp, e.Op)
	}
}

// addSaturating adds delta to n, clamping at the limits of int64 rather than
// wrapping, and reports whether it clamped.
//
// Wrapping would turn an overflowing counter into a large negative number,
// which reads as a plausible value and would be compared against the target as
// if it were real. Clamping is wrong too, but it is visibly wrong and the
// Saturated flag says so.
func addSaturating(n, delta int64) (int64, bool) {
	sum := n + delta
	switch {
	case delta > 0 && sum < n:
		return math.MaxInt64, true
	case delta < 0 && sum > n:
		return math.MinInt64, true
	default:
		return sum, false
	}
}

var _ Projection = (*counter)(nil)
