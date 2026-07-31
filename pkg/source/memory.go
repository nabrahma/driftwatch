package source

import (
	"context"
	"sync"

	"github.com/nabrahma/driftwatch/pkg/clock"
)

func init() { Register("memory", newMemory) }

// defaultMemoryBuffer is how many published messages the memory source holds
// before Publish starts refusing.
const defaultMemoryBuffer = 4096

// MemorySource is an in-process source used by every unit test and by the fault
// injector.
//
// It is the only source that can be driven synchronously, which is what makes
// the rest of the suite deterministic: a test publishes, advances a fake clock,
// and knows exactly what the pipeline has seen. No other source can promise
// that, because no other source controls when the transport delivers.
type MemorySource struct {
	clk clock.Clock
	c   counters

	mu     sync.Mutex
	buf    []RawMessage
	closed bool
	// waiting is signaled when a message arrives, so Run does not poll.
	waiting chan struct{}

	maxPayload int
	capacity   int
}

// MemoryOption configures a MemorySource.
type MemoryOption func(*MemorySource)

// WithCapacity bounds the backlog. Publish refuses beyond it rather than
// growing, so a test that over-publishes finds out instead of swelling.
func WithCapacity(n int) MemoryOption {
	return func(m *MemorySource) {
		if n > 0 {
			m.capacity = n
		}
	}
}

// WithMaxPayload bounds one frame.
func WithMaxPayload(n int) MemoryOption {
	return func(m *MemorySource) {
		if n > 0 {
			m.maxPayload = n
		}
	}
}

// NewMemory returns an in-process source.
func NewMemory(clk clock.Clock, opts ...MemoryOption) *MemorySource {
	if clk == nil {
		clk = clock.Real()
	}

	m := &MemorySource{
		clk:        clk,
		waiting:    make(chan struct{}, 1),
		maxPayload: defaultMaxPayloadBytes,
		capacity:   defaultMemoryBuffer,
	}
	for _, opt := range opts {
		opt(m)
	}
	m.c.connected(true)
	return m
}

func newMemory(cfg Config, clk clock.Clock) (Source, error) {
	capacity, err := cfg.SettingInt("buffer", defaultMemoryBuffer)
	if err != nil {
		return nil, err
	}
	return NewMemory(clk, WithCapacity(capacity), WithMaxPayload(cfg.MaxPayloadBytes)), nil
}

// Name returns the registry name.
func (m *MemorySource) Name() string { return "memory" }

// Publish queues a message for delivery, stamping ObservedAt if unset.
//
// It reports whether the message was accepted. A full backlog refuses rather
// than blocking, because the alternative is a test that deadlocks instead of
// failing, and a deadlocked test says nothing about what went wrong.
func (m *MemorySource) Publish(msg RawMessage) bool {
	if len(msg.Payload) > m.maxPayload {
		m.c.dropped()
		m.c.fail(ErrPayloadTooLarge)
		return false
	}
	if msg.ObservedAt.IsZero() {
		msg.ObservedAt = m.clk.Now()
	}

	m.mu.Lock()
	if m.closed || len(m.buf) >= m.capacity {
		full := !m.closed
		m.mu.Unlock()

		if full {
			m.c.dropped()
		}
		return false
	}
	m.buf = append(m.buf, msg)
	m.mu.Unlock()

	// Non-blocking wake: one pending notification is enough, because Run drains
	// the whole backlog when it wakes.
	select {
	case m.waiting <- struct{}{}:
	default:
	}
	return true
}

// PublishPayload is Publish for the common case of a topicless frame.
func (m *MemorySource) PublishPayload(payload []byte) bool {
	return m.Publish(RawMessage{Payload: payload})
}

// Backlog returns how many messages are queued but not yet delivered.
func (m *MemorySource) Backlog() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.buf)
}

// Run delivers queued messages until ctx is done.
func (m *MemorySource) Run(ctx context.Context, out chan<- RawMessage) error {
	for {
		batch := m.take()
		for _, msg := range batch {
			if !send(ctx, out, msg) {
				return ctx.Err()
			}
			m.c.frame(len(msg.Payload), msg.ObservedAt)
		}

		if len(batch) > 0 {
			// More may have arrived while that batch was being delivered.
			continue
		}
		if m.isClosed() {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-m.waiting:
		}
	}
}

// take removes and returns the whole backlog.
func (m *MemorySource) take() []RawMessage {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.buf) == 0 {
		return nil
	}
	batch := m.buf
	m.buf = nil
	return batch
}

func (m *MemorySource) isClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

// Stats returns transport-level counters.
func (m *MemorySource) Stats() Stats { return m.c.snapshot() }

// Close stops accepting publishes and lets Run finish the backlog. Idempotent.
func (m *MemorySource) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	m.mu.Unlock()

	m.c.connected(false)

	// Wake Run so it sees the close rather than waiting for a publish that will
	// never come.
	select {
	case m.waiting <- struct{}{}:
	default:
	}
	return nil
}
