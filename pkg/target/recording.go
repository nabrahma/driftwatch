package target

import (
	"context"
	"sync"
	"time"

	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/projection"
)

// Failer is the part of testing.TB that RecordingTarget needs.
//
// Depending on this rather than on testing.TB keeps the testing package out of
// the production binary, which importing testing from a non-test file would
// otherwise drag in along with its flag registrations.
type Failer interface {
	Helper()
	Errorf(format string, args ...any)
	FailNow()
}

// RecordingTarget wraps a Target, records every command issued, and fails the
// test the instant one falls outside the read-only allowlist.
//
// This is the structural enforcement of NG1 and invariant I13. driftwatch is
// deployed beside production stores on the strength of a promise that it never
// writes; a promise kept by careful review is worth much less than one a test
// suite refuses to let you break.
//
// The check is on commands rather than on methods, and that distinction is the
// whole design. Every method on Target is already a read, so a wrapper watching
// method calls can only confirm what the type system guarantees. A write would
// appear as a command — issued from inside an implementation, past the
// interface — so the wrapper subscribes to the command stream via Commander and
// checks each name before it is sent.
//
// A target that does not implement Commander cannot be checked, and wrapping
// one is itself a test failure rather than a silent pass.
type RecordingTarget struct {
	tb    Failer
	inner Target

	mu       sync.Mutex
	commands []string
	calls    map[string]int
	// violations records refused commands so a test can assert on them
	// deliberately, which is how the enforcement itself is tested.
	violations []string
	// allowViolations makes a refused command recorded rather than fatal. Only
	// the test that proves the enforcement works sets it.
	allowViolations bool
}

// Recording wraps inner so that any command outside the read-only allowlist
// fails tb immediately.
func Recording(tb Failer, inner Target) *RecordingTarget {
	tb.Helper()

	r := &RecordingTarget{tb: tb, inner: inner, calls: map[string]int{}}

	commander, ok := inner.(Commander)
	if !ok {
		tb.Errorf("target %q does not implement Commander, so its read-only "+
			"guarantee cannot be enforced; RecordingTarget would be decorative", inner.Name())
		tb.FailNow()
		return r
	}
	commander.ObserveCommands(r.observe)
	return r
}

// AllowViolations stops a refused command from failing the test, recording it
// instead. It exists so the enforcement can be tested; nothing else should call
// it, and a test that does is opting out of the guarantee.
func (r *RecordingTarget) AllowViolations() *RecordingTarget {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.allowViolations = true
	return r
}

// observe is invoked for every command the wrapped target issues.
func (r *RecordingTarget) observe(name string) {
	r.mu.Lock()
	r.commands = append(r.commands, name)
	allowed := IsReadOnlyCommand(name)
	if !allowed {
		r.violations = append(r.violations, name)
	}
	tolerate := r.allowViolations
	r.mu.Unlock()

	if allowed {
		return
	}

	r.tb.Helper()
	if tolerate {
		return
	}

	// FailNow stops this goroutine, so the command never reaches the store.
	// The refusal is not advisory.
	r.tb.Errorf("driftwatch attempted the mutating command %q against the "+
		"audited store; the target must be read-only (NG1, invariant I13). "+
		"Permitted commands: %v", name, ReadOnlyCommands())
	r.tb.FailNow()
}

// Commands returns every command issued so far, in order.
func (r *RecordingTarget) Commands() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.commands...)
}

// Violations returns the commands that were refused.
func (r *RecordingTarget) Violations() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.violations...)
}

// Calls returns how many times each Target method was called.
func (r *RecordingTarget) Calls() map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make(map[string]int, len(r.calls))
	for name, n := range r.calls {
		out[name] = n
	}
	return out
}

func (r *RecordingTarget) record(method string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls[method]++
}

// Name returns the wrapped target's name.
func (r *RecordingTarget) Name() string { return r.inner.Name() }

// Get reads one key.
func (r *RecordingTarget) Get(ctx context.Context, key string, shape projection.Shape) (event.Value, error) {
	r.record("Get")
	return r.inner.Get(ctx, key, shape)
}

// GetMany reads a batch.
func (r *RecordingTarget) GetMany(ctx context.Context, keys []string, shape projection.Shape) ([]event.Value, error) {
	r.record("GetMany")
	return r.inner.GetMany(ctx, keys, shape)
}

// ReadMany reads a batch, preserving per-key outcomes.
func (r *RecordingTarget) ReadMany(ctx context.Context, keys []string, shape projection.Shape) ([]Read, error) {
	r.record("ReadMany")
	return r.inner.ReadMany(ctx, keys, shape)
}

// Scan iterates the keyspace.
func (r *RecordingTarget) Scan(ctx context.Context, pattern string, batch int) Iterator {
	r.record("Scan")
	return r.inner.Scan(ctx, pattern, batch)
}

// TTL returns the remaining lifetime of a key.
func (r *RecordingTarget) TTL(ctx context.Context, key string) (*time.Duration, error) {
	r.record("TTL")
	return r.inner.TTL(ctx, key)
}

// Health returns store diagnostics.
func (r *RecordingTarget) Health(ctx context.Context) (Health, error) {
	r.record("Health")
	return r.inner.Health(ctx)
}

// Close releases the wrapped target.
func (r *RecordingTarget) Close() error {
	r.record("Close")
	return r.inner.Close()
}

// ObserveCommands forwards to the wrapped target, so a RecordingTarget can be
// wrapped again or observed by a test.
func (r *RecordingTarget) ObserveCommands(fn func(string)) {
	if commander, ok := r.inner.(Commander); ok {
		commander.ObserveCommands(fn)
	}
}

var (
	_ Target    = (*RecordingTarget)(nil)
	_ Commander = (*RecordingTarget)(nil)
)
