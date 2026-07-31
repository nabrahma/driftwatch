// Package materializer is a reference consumer that writes observed events to a target (§13).
//
// It is part of the harness and emphatically not part of driftwatch. That
// separation is the whole reason the fault matrix can test both directions:
//
//   - Put the fault injector in front of the materializer's subscription and
//     the target becomes genuinely wrong. driftwatch must report confirmed
//     divergence, because it is right and the store is not.
//   - Put it in front of driftwatch's subscription and driftwatch's own oracle
//     becomes wrong. It must report suspect divergence and never confirm,
//     because the disagreement is its own fault and it knows it.
//
// Nothing else in the repository writes to a target. pkg/target is read-only by
// construction and RecordingTarget fails a test that tries, so the writer lives
// here, behind its own interface, with no path back into the audited code.
//
// The implementation is deliberately simple-minded. Its job is to be obviously
// correct, not fast or clever: if the reference consumer had subtle bugs, every
// finding a scenario produced would be ambiguous.
package materializer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/nabrahma/driftwatch/pkg/codec"
	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/source"
)

// Store is the write side of a target.
//
// It is declared here rather than in pkg/target on purpose. A write interface
// living next to the read-only Target would be an invitation to implement both
// on one type, and NG1's guarantee is worth more than the convenience.
type Store interface {
	// Set assigns a scalar.
	Set(key string, value []byte)
	// Delete removes a key entirely.
	Delete(key string)
	// AddMember adds one member to the set at key.
	AddMember(key, member string)
	// RemoveMember removes one member. Removing the last member removes the
	// key, because Redis has no empty sets (D-007).
	RemoveMember(key, member string)
	// Incr adds delta to the counter at key.
	Incr(key string, delta int64)
}

// Config configures a Materializer.
type Config struct {
	Store Store
	Codec codec.Codec
	Shape projection.Shape
	// Projection supplies the key naming. A real materializer maintains an
	// index under the same key scheme the events describe, so when a keyTemplate
	// rewrites "9f3a" into "block:9f3a" it has to write to "block:9f3a" too.
	// Without this the reference consumer and the oracle name different keys and
	// every scenario with a template reports drift that is purely the harness
	// disagreeing with itself.
	Projection projection.Projection
	OnError    func(error)
}

// Materializer applies decoded events to a store.
type Materializer struct {
	cfg Config

	applied atomic.Int64
	failed  atomic.Int64

	mu sync.Mutex
}

// New returns a Materializer.
func New(cfg Config) (*Materializer, error) {
	if cfg.Store == nil {
		return nil, errors.New("materializer: a Store is required")
	}
	if cfg.Codec == nil {
		c, err := codec.New("json", nil)
		if err != nil {
			return nil, fmt.Errorf("materializer: %w", err)
		}
		cfg.Codec = c
	}
	return &Materializer{cfg: cfg}, nil
}

// Run consumes messages until the channel closes or ctx is done.
func (m *Materializer) Run(ctx context.Context, in <-chan source.RawMessage) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case msg, open := <-in:
			if !open {
				return nil
			}
			m.Apply(msg)
		}
	}
}

// Apply decodes one message and writes it to the store.
//
// A message that will not decode is counted and skipped. That is what a real
// materializer does — it has nowhere to put an event it cannot read — and it is
// what makes Corrupt and Truncate produce a genuinely wrong target rather than
// a crashed harness.
func (m *Materializer) Apply(msg source.RawMessage) {
	var e event.Event
	// ObservedAt belongs to the source that received the frame and is the only
	// time driftwatch trusts for elapsed-time decisions, so it is set before
	// the decode rather than taken from the payload.
	e.ObservedAt = msg.ObservedAt

	if err := m.cfg.Codec.Decode(msg.Payload, msg.Topic, &e); err != nil {
		m.failed.Add(1)
		if m.cfg.OnError != nil {
			m.cfg.OnError(err)
		}
		return
	}

	key := e.Key
	if m.cfg.Projection != nil {
		resolved, keyErr := m.cfg.Projection.TargetKey(&e)
		if keyErr != nil {
			m.failed.Add(1)
			if m.cfg.OnError != nil {
				m.cfg.OnError(keyErr)
			}
			return
		}
		key = resolved
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	switch e.Op {
	case event.OpSet:
		m.cfg.Store.Set(key, e.Value)
	case event.OpDelete:
		m.cfg.Store.Delete(key)
	case event.OpAdd:
		m.cfg.Store.AddMember(key, e.Member)
	case event.OpRemove:
		m.cfg.Store.RemoveMember(key, e.Member)
	case event.OpIncr:
		m.cfg.Store.Incr(key, e.Delta)
	case event.OpUnknown, event.OpSnapshotBegin, event.OpSnapshotEnd, event.OpHeartbeat:
		// These touch no key. A real materializer ignores them too.
		return
	}
	m.applied.Add(1)
}

// Applied returns how many events reached the store.
func (m *Materializer) Applied() int64 { return m.applied.Load() }

// Failed returns how many messages could not be decoded.
//
// A scenario asserts on this to prove a corruption fault actually corrupted
// something, rather than silently producing valid-but-different payloads.
func (m *Materializer) Failed() int64 { return m.failed.Load() }
