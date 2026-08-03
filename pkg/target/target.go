// Package target reads the audited store; every implementation is read-only (M8).
//
// driftwatch never writes to the store it is auditing. That is not a policy
// applied by care, it is enforced structurally in two independent places: a
// command allowlist checked by the redis client's own hook, and RecordingTarget,
// which fails a test the instant a mutating command is attempted. NG1 says a
// detector that can also mutate is a detector nobody will deploy, and invariant
// I13 is what makes the claim checkable rather than aspirational.
//
// The other thing this package is careful about is the difference between "the
// key is not there" and "something went wrong". A missing key is a Value with
// Kind ValueAbsent and a nil error, always. An error means driftwatch could not
// find out — and §23 A5 is explicit that absence of data is not evidence of
// divergence, so the two must never be confused.
package target

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/projection"
)

// Sentinel errors. Read failures are typed so the sweeper can tell a store that
// is unreachable from a key that holds the wrong thing — the first suppresses
// findings, the second is one.
var (
	// ErrNotFound reports that a key does not exist, for the operations where
	// that is genuinely exceptional. Reads never use it: a missing key is an
	// absent Value.
	ErrNotFound = errors.New("key not found")

	// ErrWrongType reports that a key holds a type the projection's shape
	// cannot read. It is drift, not a failure; see WrongTypeError.
	ErrWrongType = errors.New("key holds an unexpected type")

	// ErrScanRestarted reports that a SCAN cursor went backwards, which means
	// the keyspace was reset underneath the iteration. Returned rather than
	// looping forever.
	ErrScanRestarted = errors.New("scan cursor restarted mid-iteration")

	// ErrClosed reports use of a target after Close.
	ErrClosed = errors.New("target is closed")

	// ErrReadOnlyViolation reports an attempt to issue a mutating command.
	ErrReadOnlyViolation = errors.New("mutating command attempted on a read-only target")

	// ErrUnknownTarget reports a target name that is not registered.
	ErrUnknownTarget = errors.New("unknown target")

	// ErrBadConfig reports a target configuration that cannot be honored.
	ErrBadConfig = errors.New("invalid target configuration")

	// ErrInjected is returned by the memory target's injected failures, so a
	// test can tell a simulated fault from a real one.
	ErrInjected = errors.New("injected failure")
)

// WrongTypeError reports a key whose stored type does not match the shape the
// projection expects — a Redis hash where a set was expected, say.
//
// This is surfaced rather than swallowed because it is a genuine form of
// divergence: something wrote a different shape into the index. The differ
// turns it into CatTypeMismatch.
type WrongTypeError struct {
	Key  string
	Want projection.Shape
	Got  string
}

func (e *WrongTypeError) Error() string {
	return fmt.Sprintf("key %q holds a %s, which cannot be read as a %s", e.Key, e.Got, e.Want)
}

// Is reports a match against ErrWrongType so callers can use errors.Is.
func (e *WrongTypeError) Is(err error) bool { return err == ErrWrongType }

// Health returns store-level diagnostics used to explain sweeps.
//
// EvictedKeys is the reason this exists. A sweep that finds mass
// missing_in_target at the same moment the store's eviction counter jumped has
// an obvious explanation, and saying so in the output saves the operator an
// hour of looking in the wrong place (§5.7).
type Health struct {
	Reachable        bool
	EvictedKeys      uint64
	ExpiredKeys      uint64
	UsedMemoryBytes  uint64
	MaxMemoryBytes   uint64
	KeyspaceSize     int64
	Role             string // master | replica
	ReplicationLagMs int64
	Version          string
}

// Read is one key's outcome from a batch read.
//
// It exists because a batch of 500 keys must not be sunk by one key holding the
// wrong type: that key is a finding, and the other 499 still need comparing.
type Read struct {
	Value event.Value
	// Err is nil, or a *WrongTypeError for a key that could not be read as the
	// requested shape. It is never a transport error — those fail the batch.
	Err error
}

// Iterator walks the keyspace in batches.
type Iterator interface {
	// Next advances to the next batch, returning false at the end or on error.
	Next(ctx context.Context) bool
	// Keys returns the current batch.
	Keys() []string
	// Err returns the error that stopped the iteration, if any.
	Err() error
	// Close releases iterator resources. Idempotent.
	Close() error
}

// Target reads state from the audited store. Implementations MUST NOT issue any
// mutating command. This is enforced in tests by RecordingTarget.
type Target interface {
	Name() string

	// Get reads one key, shaped per the projection. A missing key yields a
	// Value with Kind ValueAbsent and a nil error.
	Get(ctx context.Context, key string, shape projection.Shape) (event.Value, error)

	// GetMany reads a batch, pipelined. Order of results matches keys. A
	// missing key yields a Value with Kind ValueAbsent, not an error.
	//
	// A key holding the wrong type also yields ValueAbsent here, because this
	// signature has nowhere to put the distinction. Use ReadMany when it
	// matters, which is what the sweeper does.
	GetMany(ctx context.Context, keys []string, shape projection.Shape) ([]event.Value, error)

	// ReadMany is GetMany with per-key outcomes preserved.
	//
	// This is an addition to the M8 interface. Without it a single wrong-typed
	// key either sinks a 500-key batch or is silently reported as absent, and
	// "absent" is a different finding from "holds a hash" — one of them tells
	// the operator what actually happened.
	ReadMany(ctx context.Context, keys []string, shape projection.Shape) ([]Read, error)

	// Scan iterates the keyspace matching pattern. Must use a non-blocking
	// cursor (Redis SCAN), never KEYS.
	Scan(ctx context.Context, pattern string, batch int) Iterator

	// TTL returns the remaining TTL, nil if the key has none, or ErrNotFound.
	TTL(ctx context.Context, key string) (*time.Duration, error)

	// Health returns store-level diagnostics used to explain sweeps.
	Health(ctx context.Context) (Health, error)

	// Close releases resources. Idempotent.
	Close() error
}

// Commander is implemented by targets that can report the store commands they
// issue.
//
// Method-level checking is not enough to enforce read-only: every method on
// Target is already a read, so a wrapper watching methods can only ever confirm
// what the type system already guarantees. The commands are where a write would
// actually appear, so that is where the check has to be.
type Commander interface {
	// ObserveCommands installs a callback invoked with the name of every
	// command the target issues, before it is sent. A callback that panics or
	// calls runtime.Goexit — which is what a test failure does — prevents the
	// command from reaching the store.
	ObserveCommands(fn func(name string))
}

// readOnlyCommands is the data-plane allowlist from §5.8 I13, matched on
// the command verb. Everything here reads keyspace data; anything absent from
// both this and connectionCommands is refused.
var readOnlyCommands = map[string]struct{}{
	"GET":      {},
	"SMEMBERS": {},
	"SCAN":     {},
	"TYPE":     {},
	"TTL":      {},
	"PTTL":     {},
	"EXISTS":   {},
	"HGETALL":  {},
	"INFO":     {},
	"STRLEN":   {},
	"SCARD":    {},
	"MEMORY":   {},

	// One addition to the §5.8 list: DBSIZE, which is read-only and which
	// Health.KeyspaceSize needs, and §9 M8 requires Health to report it.
	"DBSIZE": {},
}

// connectionCommands are issued by the client library on driftwatch's behalf
// while establishing and maintaining a connection. They are matched in full,
// including the subcommand.
//
// This list exists because the allowlist in §5.8 does not survive contact with
// a real client. go-redis sends HELLO, CLIENT SETINFO and — since v9.17 —
// CLIENT MAINT_NOTIFICATIONS during the handshake, none of which driftwatch
// asks for and none of which it can decline. An allowlist containing only the
// data-plane reads refuses the handshake, so every read fails with "mutating
// command attempted" and the operator goes looking for a write that does not
// exist. See docs/DISCOVERIES.md D-004.
//
// Every entry is connection-scoped: none can read or modify a single byte of
// keyspace data. The subcommands are listed individually rather than allowing
// the CLIENT or CLUSTER verb wholesale, because CLIENT KILL and CLUSTER RESET
// share those verbs and neither is something driftwatch should be able to send.
var connectionCommands = map[string]struct{}{
	"HELLO":  {},
	"AUTH":   {},
	"SELECT": {},
	"PING":   {},
	"ECHO":   {},

	"CLIENT SETINFO":             {},
	"CLIENT SETNAME":             {},
	"CLIENT GETNAME":             {},
	"CLIENT ID":                  {},
	"CLIENT INFO":                {},
	"CLIENT MAINT_NOTIFICATIONS": {},

	// Cluster topology discovery, which ClusterClient.ForEachMaster needs to
	// find the masters a scan has to visit.
	"CLUSTER SLOTS":  {},
	"CLUSTER SHARDS": {},
	"CLUSTER NODES":  {},
	"CLUSTER INFO":   {},
	"CLUSTER MYID":   {},
}

// IsReadOnlyCommand reports whether a command may be issued against an audited
// store.
//
// Matching ignores case. A container command such as "MEMORY USAGE" is matched
// on its verb when the verb itself is a read; connection commands are matched
// in full, so "CLIENT SETINFO" is permitted while "CLIENT KILL" is not.
func IsReadOnlyCommand(name string) bool {
	upper := strings.ToUpper(strings.TrimSpace(name))
	if upper == "" {
		return false
	}
	if _, ok := connectionCommands[upper]; ok {
		return true
	}

	verb := upper
	if i := strings.IndexByte(verb, ' '); i >= 0 {
		verb = verb[:i]
	}
	_, ok := readOnlyCommands[verb]
	return ok
}

// ReadOnlyCommands returns the data-plane allowlist, sorted. Exported so a test
// or an operator can see exactly what driftwatch is permitted to read with.
func ReadOnlyCommands() []string {
	out := make([]string, 0, len(readOnlyCommands))
	for name := range readOnlyCommands {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// ConnectionCommands returns the connection-setup allowlist, sorted.
func ConnectionCommands() []string {
	out := make([]string, 0, len(connectionCommands))
	for name := range connectionCommands {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// Constructor builds a Target from configuration.
type Constructor func(cfg Config) (Target, error)

// Clock is the subset of clock.Clock a target needs.
//
// It is declared here rather than imported so that pkg/target does not depend
// on pkg/clock: the dependency would be one-directional and harmless, but this
// interface is two methods and stating them is cheaper than the coupling.
type Clock interface {
	Now() time.Time
	Sleep(ctx context.Context, d time.Duration) error
}

// Config is the string-keyed configuration a target is built from, plus the
// injected dependencies that cannot be expressed as strings.
type Config struct {
	// Settings holds the target-specific options, as they arrive from a
	// DriftCheck spec.
	Settings map[string]string
	// Clock is the injected clock. Targets that simulate latency need it;
	// targets that talk to a real store use it for timeouts.
	Clock Clock
}

// Setting returns a configured value or a default.
func (c Config) Setting(key, def string) string {
	if v, ok := c.Settings[key]; ok {
		return v
	}
	return def
}

var (
	registryMu sync.RWMutex
	registry   = map[string]Constructor{}
)

// Register adds a target constructor under name. It panics on a duplicate: a
// silently shadowed target would read a different store than the operator
// configured, and every finding after that would be meaningless.
func Register(name string, ctor Constructor) {
	if name == "" {
		panic("target: Register with an empty name")
	}
	if ctor == nil {
		panic("target: Register with a nil constructor for " + name)
	}

	registryMu.Lock()
	defer registryMu.Unlock()
	if _, exists := registry[name]; exists {
		panic("target: Register called twice for " + name)
	}
	registry[name] = ctor
}

// New constructs the named target.
func New(name string, cfg Config) (Target, error) {
	registryMu.RLock()
	ctor, ok := registry[name]
	registryMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("%w: %q (registered: %v)", ErrUnknownTarget, name, Names())
	}
	return ctor(cfg)
}

// Names returns the registered target names in sorted order.
func Names() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()

	out := make([]string, 0, len(registry))
	for name := range registry {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// valuesFromReads flattens per-key outcomes for the GetMany signature, mapping
// an unreadable key to absent.
func valuesFromReads(reads []Read) []event.Value {
	out := make([]event.Value, len(reads))
	for i, r := range reads {
		if r.Err == nil {
			out[i] = r.Value
		}
	}
	return out
}
