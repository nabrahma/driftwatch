package projection

import (
	"fmt"

	"github.com/nabrahma/driftwatch/pkg/event"
)

func init() { Register("scalar", newScalar) }

// scalar maintains key -> bytes with last-write-wins semantics. It is the
// simplest useful projection and the one to copy when adding a new shape.
type scalar struct {
	keyTmpl   *expander
	ownership OwnershipModel
}

func newScalar(cfg map[string]string) (Projection, error) {
	keyTmpl, err := newExpander("keyTemplate", stringConfig(cfg, "keyTemplate", "{{.Key}}"), newBuilderPool())
	if err != nil {
		return nil, err
	}
	return &scalar{keyTmpl: keyTmpl, ownership: ownershipFrom(cfg)}, nil
}

// Name returns the registry name.
func (s *scalar) Name() string { return "scalar" }

// Commutative reports false: with last-write-wins, whichever write lands last
// decides the value, so order is the whole semantics.
func (s *scalar) Commutative() bool { return false }

// KeyOwnership reports the configured partitioning, if any.
func (s *scalar) KeyOwnership() OwnershipModel { return s.ownership }

// TargetKey returns the store key this event affects.
func (s *scalar) TargetKey(e *event.Event) (string, error) {
	return s.keyTmpl.expand(dataFor(e))
}

// TargetShape reports that values map onto Redis strings.
func (s *scalar) TargetShape() Shape { return ShapeScalar }

// Apply folds one event into the scalar at its key.
func (s *scalar) Apply(prev event.Value, e *event.Event) (Mutation, error) {
	if !e.Op.TouchesKey() {
		return Mutation{Action: ActionNone}, nil
	}
	if err := checkShape(prev, event.ValueScalar); err != nil {
		return Mutation{}, err
	}

	data := dataFor(e)
	key, err := s.keyTmpl.expand(data)
	if err != nil {
		return Mutation{}, err
	}

	switch e.Op {
	case event.OpSet:
		// The value bytes are copied because Event.Value may point into a
		// source buffer the transport is free to reuse.
		next := make([]byte, len(e.Value))
		copy(next, e.Value)
		return Mutation{
			Key:    key,
			Action: ActionUpsert,
			Value:  event.Value{Kind: event.ValueScalar, Scalar: next},
			TTL:    e.TTL,
		}, nil

	case event.OpDelete:
		if prev.IsAbsent() {
			return Mutation{Key: key, Action: ActionNone}, nil
		}
		return Mutation{Key: key, Action: ActionDelete, TTL: e.TTL}, nil

	default:
		return Mutation{}, fmt.Errorf("%w: %s on scalar", ErrUnsupportedOp, e.Op)
	}
}

var _ Projection = (*scalar)(nil)
