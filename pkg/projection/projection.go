// Package projection folds event streams into derived state with pure functions (M6).
//
// Every implementation here contains zero I/O, zero clock access and zero
// randomness. That purity is not stylistic: it is what makes the property tests
// in §16.2 possible. A projection that could read the network or the clock
// could not be replayed, permuted, or checked against a reference
// implementation, and the invariants those tests prove (I1, I2, I3) are the
// evidence that driftwatch's expectation is computed correctly rather than
// merely computed.
package projection

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/nabrahma/driftwatch/pkg/event"
)

// Sentinel errors. The pipeline counts them by reason rather than lumping every
// failure together, because a shape mismatch is a misconfiguration and an
// unsupported op is a codec or producer problem.
var (
	// ErrUnsupportedOp reports an operation this projection has no meaning for.
	ErrUnsupportedOp = errors.New("operation not supported by this projection")
	// ErrShapeMismatch reports a previous value whose shape this projection
	// cannot fold. It happens when a DriftCheck's projection is changed without
	// clearing the oracle.
	ErrShapeMismatch = errors.New("previous value has the wrong shape")
	// ErrBadValue reports a value that cannot be parsed for this projection.
	ErrBadValue = errors.New("value cannot be parsed")
	// ErrBadConfig reports a projection configuration that cannot be honored.
	ErrBadConfig = errors.New("invalid projection configuration")
	// ErrUnknownProjection reports a projection name that is not registered.
	ErrUnknownProjection = errors.New("unknown projection")
)

// Action is what a Mutation does to the oracle.
type Action uint8

// The actions a projection can request. ActionNone is the zero value, so a
// mutation nobody filled in changes nothing.
const (
	// ActionNone means the event does not affect target state.
	ActionNone Action = iota
	// ActionUpsert means the key takes the mutation's value.
	ActionUpsert
	// ActionDelete means the key is expected to be absent from the target.
	ActionDelete
)

var actionNames = [...]string{
	ActionNone:   "none",
	ActionUpsert: "upsert",
	ActionDelete: "delete",
}

// String returns the name of the action.
func (a Action) String() string {
	if int(a) >= len(actionNames) {
		return "Action(" + strconv.Itoa(int(a)) + ")"
	}
	return actionNames[a]
}

// Shape describes how a projection's values map onto the target store, so the
// Target adapter knows which read command to issue.
type Shape uint8

// The target shapes driftwatch can read.
const (
	ShapeScalar Shape = iota
	ShapeSet
	ShapeCounter
)

var shapeNames = [...]string{
	ShapeScalar:  "scalar",
	ShapeSet:     "set",
	ShapeCounter: "counter",
}

// String returns the name of the shape.
func (s Shape) String() string {
	if int(s) >= len(shapeNames) {
		return "Shape(" + strconv.Itoa(int(s)) + ")"
	}
	return shapeNames[s]
}

// Mutation is the result of applying one event to one key.
type Mutation struct {
	Key    string
	Action Action
	Value  event.Value // meaningful for ActionUpsert
	TTL    *time.Duration

	// Truncated reports that a bound was hit while computing this value — a
	// member set at its cap, for instance — so the oracle can mark the key as
	// holding an incomplete view. The mutation is still valid and must still be
	// applied; the flag records that driftwatch's expectation for this key is
	// approximate rather than exact.
	Truncated bool

	// Saturated reports that a counter clamped at the limits of int64 rather
	// than wrapping. Same contract as Truncated: apply the mutation, and know
	// that the value is a bound rather than the true count.
	Saturated bool
}

// OwnershipModel describes whether publishers own disjoint keyspaces.
//
// It exists to scope the damage of a sequence gap. When a publisher's events go
// missing, driftwatch cannot know which keys they touched — that information
// was in the lost events. If publishers own disjoint partitions, only that
// partition becomes Suspect; otherwise every key does, and the whole check
// stops asserting until a snapshot restores trust.
type OwnershipModel struct {
	Partitioned bool
	// KeyPattern, if Partitioned, is a template that expands to the key prefix
	// a given publisher may write, e.g. "replica:{{.Publisher}}:*".
	KeyPattern string
}

// Projection folds events into expected target state.
//
// Implementations MUST be pure: the same (prev, event) always yields the same
// Mutation, with no side effects, and prev is never modified.
type Projection interface {
	// Name returns the registry name.
	Name() string

	// Apply computes the new state for the key the event touches. prev is the
	// current value, with Kind ValueAbsent if the key does not exist.
	// Returning ActionNone means the event does not affect target state.
	Apply(prev event.Value, e *event.Event) (Mutation, error)

	// Commutative reports whether event order affects the final state. If
	// false, the oracle must order by seq before applying.
	Commutative() bool

	// KeyOwnership describes whether publishers own disjoint keyspaces.
	KeyOwnership() OwnershipModel

	// TargetShape describes how the value maps onto the target store.
	TargetShape() Shape
}

// Constructor builds a Projection from string configuration.
type Constructor func(cfg map[string]string) (Projection, error)

var (
	registryMu sync.RWMutex
	registry   = map[string]Constructor{}
)

// Register adds a projection constructor under name. It panics on a duplicate:
// a silently shadowed projection would compute a different expectation than the
// operator configured, and every finding after that would be driftwatch's own
// fault.
func Register(name string, ctor Constructor) {
	if name == "" {
		panic("projection: Register with an empty name")
	}
	if ctor == nil {
		panic("projection: Register with a nil constructor for " + name)
	}

	registryMu.Lock()
	defer registryMu.Unlock()
	if _, exists := registry[name]; exists {
		panic("projection: Register called twice for " + name)
	}
	registry[name] = ctor
}

// New constructs the named projection with the given configuration.
func New(name string, cfg map[string]string) (Projection, error) {
	registryMu.RLock()
	ctor, ok := registry[name]
	registryMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("%w: %q (registered: %v)", ErrUnknownProjection, name, Names())
	}
	return ctor(cfg)
}

// Names returns the registered projection names in sorted order.
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

// templateData is what a key or member template can reference. Templates are
// checked against it at construction, so a typo fails when the DriftCheck is
// applied rather than on the first event at 3am.
type templateData struct {
	Key       string
	Member    string
	Publisher string
	Topic     string
	Epoch     uint64
	Seq       uint64
}

// fieldRef names the single event field an identity template resolves to.
//
// It is an enum rather than a func value on purpose. A closure here would be an
// indirect call that the escape analysis cannot see through, which forces the
// templateData onto the heap and costs an allocation on every single event.
type fieldRef uint8

const (
	fieldRefTemplate fieldRef = iota
	fieldRefKey
	fieldRefMember
	fieldRefPublisher
	fieldRefTopic
)

// expander renders one configured template. The overwhelmingly common case is
// the identity template ("{{.Key}}"), which is recognized at construction and
// answered by returning the field directly — text/template execution allocates
// several times per call, and this runs once per event per key.
type expander struct {
	tmpl *template.Template
	ref  fieldRef
	pool *sync.Pool
}

// identityFields maps the templates worth special-casing to their field.
var identityFields = map[string]fieldRef{
	"{{.Key}}":       fieldRefKey,
	"{{.Member}}":    fieldRefMember,
	"{{.Publisher}}": fieldRefPublisher,
	"{{.Topic}}":     fieldRefTopic,
}

// newExpander compiles spec once. Compiling per event would dominate the
// projection benchmark and buy nothing.
func newExpander(what, spec string, pool *sync.Pool) (*expander, error) {
	if ref, ok := identityFields[spec]; ok {
		return &expander{ref: ref}, nil
	}

	tmpl, err := template.New(what).Option("missingkey=error").Parse(spec)
	if err != nil {
		return nil, fmt.Errorf("%w: %s %q: %w", ErrBadConfig, what, spec, err)
	}

	// Execute once against a zero value so a reference to a field that does not
	// exist is a configuration error rather than a per-event failure.
	ex := &expander{tmpl: tmpl, pool: pool}
	if _, err := ex.expand(templateData{}); err != nil {
		return nil, fmt.Errorf("%w: %s %q: %w", ErrBadConfig, what, spec, err)
	}
	return ex, nil
}

// expand takes its data by value rather than by pointer, which is deliberate
// and measured: a pointer here escapes to the heap because the template path
// stores it in an interface, costing one allocation on every event. Copying 80
// bytes on the stack is cheaper than that allocation, and the identity path —
// which is the overwhelming majority of calls — then does no work at all.
//
//nolint:gocritic // hugeParam: the copy is cheaper than the escape it prevents
func (e *expander) expand(d templateData) (string, error) {
	switch e.ref {
	case fieldRefKey:
		return d.Key, nil
	case fieldRefMember:
		return d.Member, nil
	case fieldRefPublisher:
		return d.Publisher, nil
	case fieldRefTopic:
		return d.Topic, nil
	case fieldRefTemplate:
	}

	buf, ok := e.pool.Get().(*strings.Builder)
	if !ok {
		// Unreachable: the pool's New only produces *strings.Builder.
		buf = new(strings.Builder)
	}
	buf.Reset()
	defer e.pool.Put(buf)

	if err := e.tmpl.Execute(buf, d); err != nil {
		return "", fmt.Errorf("%w: %w", ErrBadConfig, err)
	}
	return buf.String(), nil
}

func newBuilderPool() *sync.Pool {
	return &sync.Pool{New: func() any { return new(strings.Builder) }}
}

// dataFor fills the template inputs from an event without allocating.
func dataFor(e *event.Event) templateData {
	return templateData{
		Key:       e.Key,
		Member:    e.Member,
		Publisher: e.Publisher,
		Topic:     e.Topic,
		Epoch:     e.Epoch,
		Seq:       e.Seq,
	}
}

// intConfig reads a positive integer setting.
func intConfig(cfg map[string]string, key string, def int) (int, error) {
	raw, ok := cfg[key]
	if !ok {
		return def, nil
	}
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%w: %s must be a positive integer, got %q", ErrBadConfig, key, raw)
	}
	return n, nil
}

// boolConfig reads a boolean setting.
func boolConfig(cfg map[string]string, key string) (bool, error) {
	raw, ok := cfg[key]
	if !ok {
		return false, nil
	}
	v, err := strconv.ParseBool(strings.TrimSpace(raw))
	if err != nil {
		return false, fmt.Errorf("%w: %s must be a boolean, got %q", ErrBadConfig, key, raw)
	}
	return v, nil
}

// stringConfig reads a setting with a default.
func stringConfig(cfg map[string]string, key, def string) string {
	if raw, ok := cfg[key]; ok {
		return raw
	}
	return def
}

// ownershipFrom builds the ownership model from the optional keyPattern.
func ownershipFrom(cfg map[string]string) OwnershipModel {
	pattern := stringConfig(cfg, "keyPattern", "")
	return OwnershipModel{Partitioned: pattern != "", KeyPattern: pattern}
}

// checkShape rejects a previous value this projection cannot fold. An absent
// value is always acceptable: every key starts out not existing.
func checkShape(prev event.Value, want event.ValueKind) error {
	if prev.Kind == event.ValueAbsent || prev.Kind == want {
		return nil
	}
	return fmt.Errorf("%w: expected %s, got %s", ErrShapeMismatch, want, prev.Kind)
}
