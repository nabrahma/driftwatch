// Package codec decodes raw source bytes into events (M3).
//
// Codecs are pluggable because real producers have their own formats and
// nobody is going to change a production publisher to suit an auditing tool.
// The json codec's field names and operation names are configurable for the
// same reason.
//
// Two rules bind every implementation: Decode must never panic on arbitrary
// input, which the fuzz test enforces, and every failure must be reported as
// one of the sentinels below, so the pipeline can tell a version mismatch
// (ErrUnknownOp) from corruption (ErrMalformed) from a misconfiguration
// (ErrMissingField) instead of counting them all the same way.
package codec

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/nabrahma/driftwatch/pkg/event"
)

// Sentinel errors every codec reports through. ErrUnknownOp and ErrMissingField
// are shared with pkg/event so that a single errors.Is check works wherever the
// failure originated.
var (
	// ErrMalformed reports input that is not well-formed for this codec.
	ErrMalformed = errors.New("malformed payload")
	// ErrTooLarge reports a payload or field beyond its configured bound.
	// Oversized input is rejected, never truncated: a truncated event is a
	// wrong event, and a wrong event is worse than a missing one.
	ErrTooLarge = errors.New("payload too large")
	// ErrUnknownOp reports an operation name this build does not recognize.
	// Counted separately from ErrMalformed because it usually means the
	// producer is running a newer version, not that the bytes are corrupt.
	ErrUnknownOp = event.ErrUnknownOp
	// ErrMissingField reports a field the event requires but does not have.
	ErrMissingField = event.ErrMissingField
	// ErrUnknownCodec reports a codec name that is not registered.
	ErrUnknownCodec = errors.New("unknown codec")
	// ErrBadConfig reports a codec configuration that cannot be honored.
	ErrBadConfig = errors.New("invalid codec configuration")
)

// Codec decodes wire bytes into an Event. Implementations must be safe for
// concurrent use.
type Codec interface {
	// Name returns the registry name (e.g. "json").
	Name() string

	// Decode parses payload into dst. It must not retain payload unless
	// retainRaw is set; callers may reuse the buffer.
	//
	// Decode overwrites every field of dst except ObservedAt, which belongs to
	// the source that received the frame and is the only time driftwatch trusts
	// for elapsed-time decisions.
	Decode(payload []byte, topic string, dst *event.Event) error
}

// Constructor builds a Codec from string configuration.
type Constructor func(cfg map[string]string) (Codec, error)

var (
	registryMu sync.RWMutex
	registry   = map[string]Constructor{}
)

// Register adds a codec constructor under name. It panics if the name is
// already taken, because a silently shadowed codec would decode events with
// the wrong field mapping and produce divergence findings that are entirely
// driftwatch's own fault.
func Register(name string, ctor Constructor) {
	if name == "" {
		panic("codec: Register with an empty name")
	}
	if ctor == nil {
		panic("codec: Register with a nil constructor for " + name)
	}

	registryMu.Lock()
	defer registryMu.Unlock()
	if _, exists := registry[name]; exists {
		panic("codec: Register called twice for " + name)
	}
	registry[name] = ctor
}

// New constructs the named codec with the given configuration.
func New(name string, cfg map[string]string) (Codec, error) {
	registryMu.RLock()
	ctor, ok := registry[name]
	registryMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("%w: %q (registered: %v)", ErrUnknownCodec, name, Names())
	}
	return ctor(cfg)
}

// Names returns the registered codec names in sorted order.
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
