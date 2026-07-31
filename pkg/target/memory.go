package target

import (
	"context"
	"errors"
	"path"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/projection"
)

func init() { Register("memory", newMemory) }

// defaultScanBatch is the batch size an iterator uses when the caller does not
// choose one.
const defaultScanBatch = 500

// MemoryTarget is an in-process store used by unit and fault tests.
//
// It is deliberately more controllable than a real store: latency and failures
// are injected deterministically rather than sampled, because a sweeper error
// path that only fires sometimes is a test that only passes sometimes. It also
// exposes SimulateEvict and SimulateFlush, which are the two store-side events
// the fault matrix needs and which no read-only interface can express.
type MemoryTarget struct {
	mu sync.RWMutex

	scalars  map[string][]byte
	sets     map[string]map[string]struct{}
	expiries map[string]time.Time

	health Health

	clock     Config
	latency   time.Duration
	failEvery int
	opCount   int
	closed    bool

	observers []func(string)
}

// MemoryOption configures a MemoryTarget.
type MemoryOption func(*MemoryTarget)

// WithLatency makes every operation wait, using the injected clock. A test that
// sets this must advance the clock, which is the point: the delay is visible in
// the test rather than hidden in a sleep.
func WithLatency(d time.Duration) MemoryOption {
	return func(m *MemoryTarget) { m.latency = d }
}

// WithFailureRate makes a deterministic fraction of operations fail with
// ErrInjected.
//
// It is a rate rather than a probability: 0.25 fails exactly every fourth
// operation, not roughly one in four. A flaky fault injector produces flaky
// tests, and a flaky test of an error path is worse than no test at all.
func WithFailureRate(rate float64) MemoryOption {
	return func(m *MemoryTarget) {
		switch {
		case rate <= 0:
			m.failEvery = 0
		case rate >= 1:
			m.failEvery = 1
		default:
			m.failEvery = int(1/rate + 0.5)
		}
	}
}

// NewMemory returns an empty in-process target.
func NewMemory(opts ...MemoryOption) *MemoryTarget {
	m := &MemoryTarget{
		scalars:  map[string][]byte{},
		sets:     map[string]map[string]struct{}{},
		expiries: map[string]time.Time{},
		health:   Health{Reachable: true, Role: "master", Version: "memory"},
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

func newMemory(cfg Config) (Target, error) {
	m := NewMemory()
	m.clock = cfg

	if raw := cfg.Setting("failureRate", ""); raw != "" {
		rate, err := strconv.ParseFloat(raw, 64)
		if err != nil || rate < 0 || rate > 1 {
			return nil, errors.New(ErrBadConfig.Error() + ": failureRate must be between 0 and 1, got " + raw)
		}
		WithFailureRate(rate)(m)
	}
	if raw := cfg.Setting("latency", ""); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d < 0 {
			return nil, errors.New(ErrBadConfig.Error() + ": latency must be a non-negative duration, got " + raw)
		}
		m.latency = d
	}
	return m, nil
}

// Name returns the registry name.
func (m *MemoryTarget) Name() string { return "memory" }

// ObserveCommands installs a command observer.
func (m *MemoryTarget) ObserveCommands(fn func(string)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.observers = append(m.observers, fn)
}

// emit reports a command to the observers. It is called before the operation
// happens, so an observer that fails the test stops the command.
func (m *MemoryTarget) emit(name string) {
	m.mu.RLock()
	observers := make([]func(string), len(m.observers))
	copy(observers, m.observers)
	m.mu.RUnlock()

	for _, fn := range observers {
		fn(name)
	}
}

// begin runs the injected latency and failure, and reports whether the target
// is usable.
func (m *MemoryTarget) begin(ctx context.Context, command string) error {
	m.emit(command)

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrClosed
	}
	m.opCount++
	shouldFail := m.failEvery > 0 && m.opCount%m.failEvery == 0
	latency := m.latency
	clk := m.clock.Clock
	m.mu.Unlock()

	if latency > 0 && clk != nil {
		if err := clk.Sleep(ctx, latency); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if shouldFail {
		return ErrInjected
	}
	return nil
}

// Seed loads scalar values. Test setup only; it is not part of Target and a
// RecordingTarget will report the writes it makes.
func (m *MemoryTarget) Seed(values map[string][]byte) {
	m.emit("SET")

	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range values {
		m.scalars[k] = append([]byte(nil), v...)
	}
}

// SeedSets loads member sets. Test setup only.
func (m *MemoryTarget) SeedSets(values map[string][]string) {
	m.emit("SADD")

	m.mu.Lock()
	defer m.mu.Unlock()
	for k, members := range values {
		set := make(map[string]struct{}, len(members))
		for _, member := range members {
			set[member] = struct{}{}
		}
		if len(set) == 0 {
			// Redis has no empty sets: adding no members leaves no key.
			delete(m.sets, k)
			continue
		}
		m.sets[k] = set
	}
}

// SetExpiry gives a key a deadline, so TTL and expiry policies can be tested.
func (m *MemoryTarget) SetExpiry(key string, at time.Time) {
	m.emit("EXPIREAT")

	m.mu.Lock()
	defer m.mu.Unlock()
	m.expiries[key] = at
}

// SetHealth replaces the reported diagnostics.
//
//nolint:gocritic // hugeParam: Health is a value type and this is test setup
func (m *MemoryTarget) SetHealth(h Health) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.health = h
}

// SimulateEvict drops n keys and advances the eviction counter, standing in for
// Redis discarding keys under maxmemory.
func (m *MemoryTarget) SimulateEvict(n int) []string {
	m.emit("DEBUG EVICT")

	m.mu.Lock()
	defer m.mu.Unlock()

	victims := make([]string, 0, n)
	for _, key := range m.allKeysLocked() {
		if len(victims) >= n {
			break
		}
		delete(m.scalars, key)
		delete(m.sets, key)
		delete(m.expiries, key)
		victims = append(victims, key)
	}
	m.health.EvictedKeys += uint64(len(victims)) //nolint:gosec // len is never negative
	return victims
}

// SimulateFlush empties the store, standing in for FLUSHDB.
func (m *MemoryTarget) SimulateFlush() {
	m.emit("FLUSHDB")

	m.mu.Lock()
	defer m.mu.Unlock()
	m.scalars = map[string][]byte{}
	m.sets = map[string]map[string]struct{}{}
	m.expiries = map[string]time.Time{}
}

// Len returns the number of keys currently stored.
func (m *MemoryTarget) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.allKeysLocked())
}

// allKeysLocked returns every live key, sorted.
//
// Sorted so tests are reproducible. Real Redis SCAN makes no ordering promise
// whatsoever, so nothing outside a test may depend on this.
func (m *MemoryTarget) allKeysLocked() []string {
	out := make([]string, 0, len(m.scalars)+len(m.sets))
	for k := range m.scalars {
		out = append(out, k)
	}
	for k := range m.sets {
		if _, dup := m.scalars[k]; !dup {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// expiredLocked reports whether a key has passed its deadline.
func (m *MemoryTarget) expiredLocked(key string) bool {
	deadline, ok := m.expiries[key]
	if !ok {
		return false
	}
	if m.clock.Clock == nil {
		return false
	}
	return !m.clock.Clock.Now().Before(deadline)
}

// Get reads one key.
func (m *MemoryTarget) Get(ctx context.Context, key string, shape projection.Shape) (event.Value, error) {
	if err := m.begin(ctx, commandFor(shape)); err != nil {
		return event.Value{}, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.readLocked(key, shape)
}

func (m *MemoryTarget) readLocked(key string, shape projection.Shape) (event.Value, error) {
	if m.expiredLocked(key) {
		return event.Value{}, nil
	}

	scalar, hasScalar := m.scalars[key]
	set, hasSet := m.sets[key]

	switch shape {
	case projection.ShapeSet:
		if hasScalar {
			return event.Value{}, &WrongTypeError{Key: key, Want: shape, Got: "string"}
		}
		if !hasSet || len(set) == 0 {
			// Redis deletes a set key when its last member goes, so an empty
			// set and a missing key are the same observable state (M2).
			return event.Value{}, nil
		}
		members := make(map[string]struct{}, len(set))
		for member := range set {
			members[member] = struct{}{}
		}
		return event.Value{Kind: event.ValueSet, Members: members}, nil

	case projection.ShapeScalar:
		if hasSet {
			return event.Value{}, &WrongTypeError{Key: key, Want: shape, Got: "set"}
		}
		if !hasScalar {
			return event.Value{}, nil
		}
		return event.Value{Kind: event.ValueScalar, Scalar: append([]byte(nil), scalar...)}, nil

	case projection.ShapeCounter:
		if hasSet {
			return event.Value{}, &WrongTypeError{Key: key, Want: shape, Got: "set"}
		}
		if !hasScalar {
			return event.Value{}, nil
		}
		n, err := strconv.ParseInt(string(scalar), 10, 64)
		if err != nil {
			return event.Value{}, &WrongTypeError{Key: key, Want: shape, Got: "non-integer string"}
		}
		return event.Value{Kind: event.ValueCounter, Counter: n}, nil

	default:
		return event.Value{}, &WrongTypeError{Key: key, Want: shape, Got: "unknown shape"}
	}
}

// GetMany reads a batch.
func (m *MemoryTarget) GetMany(ctx context.Context, keys []string, shape projection.Shape) ([]event.Value, error) {
	reads, err := m.ReadMany(ctx, keys, shape)
	if err != nil {
		return nil, err
	}
	return valuesFromReads(reads), nil
}

// ReadMany reads a batch, preserving per-key outcomes.
func (m *MemoryTarget) ReadMany(ctx context.Context, keys []string, shape projection.Shape) ([]Read, error) {
	if err := m.begin(ctx, commandFor(shape)); err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]Read, len(keys))
	for i, key := range keys {
		v, err := m.readLocked(key, shape)
		if err != nil {
			out[i] = Read{Err: err}
			continue
		}
		out[i] = Read{Value: v}
	}
	return out, nil
}

// TTL returns the remaining lifetime of a key.
func (m *MemoryTarget) TTL(ctx context.Context, key string) (*time.Duration, error) {
	if err := m.begin(ctx, "TTL"); err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	_, hasScalar := m.scalars[key]
	_, hasSet := m.sets[key]
	if (!hasScalar && !hasSet) || m.expiredLocked(key) {
		return nil, ErrNotFound
	}

	deadline, ok := m.expiries[key]
	if !ok || m.clock.Clock == nil {
		return nil, nil
	}
	remaining := deadline.Sub(m.clock.Clock.Now())
	return &remaining, nil
}

// Health returns the configured diagnostics.
func (m *MemoryTarget) Health(ctx context.Context) (Health, error) {
	if err := m.begin(ctx, "INFO"); err != nil {
		return Health{}, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	h := m.health
	h.KeyspaceSize = int64(len(m.allKeysLocked()))
	return h, nil
}

// Scan iterates keys matching a glob pattern.
func (m *MemoryTarget) Scan(ctx context.Context, pattern string, batch int) Iterator {
	if batch <= 0 {
		batch = defaultScanBatch
	}
	if err := m.begin(ctx, "SCAN"); err != nil {
		return &sliceIterator{err: err}
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	matched := make([]string, 0, len(m.scalars)+len(m.sets))
	for _, key := range m.allKeysLocked() {
		if m.expiredLocked(key) {
			continue
		}
		if pattern != "" && pattern != "*" {
			ok, err := path.Match(pattern, key)
			if err != nil || !ok {
				continue
			}
		}
		matched = append(matched, key)
	}
	return &sliceIterator{keys: matched, batch: batch}
}

// Close marks the target unusable. Idempotent.
func (m *MemoryTarget) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

// commandFor names the read command a shape implies, so the memory target
// reports the same command names a real store would.
func commandFor(shape projection.Shape) string {
	if shape == projection.ShapeSet {
		return "SMEMBERS"
	}
	return "GET"
}

// sliceIterator walks a fixed key list in batches.
type sliceIterator struct {
	keys    []string
	batch   int
	current []string
	err     error
}

func (it *sliceIterator) Next(ctx context.Context) bool {
	if it.err != nil {
		return false
	}
	if err := ctx.Err(); err != nil {
		it.err = err
		return false
	}
	if len(it.keys) == 0 {
		it.current = nil
		return false
	}

	n := min(it.batch, len(it.keys))
	it.current, it.keys = it.keys[:n], it.keys[n:]
	return true
}

func (it *sliceIterator) Keys() []string { return it.current }
func (it *sliceIterator) Err() error     { return it.err }
func (it *sliceIterator) Close() error   { return nil }

var (
	_ Target    = (*MemoryTarget)(nil)
	_ Commander = (*MemoryTarget)(nil)
)
