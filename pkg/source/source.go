// Package source provides event transports: memory, file replay, ZeroMQ and NATS (M4).
//
// A source has one job and one hard rule. The job is to hand the pipeline
// whatever the transport delivered, as fast as it arrives. The rule is that it
// must never make the pipeline wait: a source that blocks on a full channel
// turns a slow consumer into a stalled subscription, and on a PUB/SUB transport
// a stalled subscription becomes silent message loss upstream, where driftwatch
// can neither see it nor count it.
//
// The other thing this package is careful about is admitting when it has lost
// messages. Every transport here can drop — a reconnect misses whatever was
// published while the socket was down, a high-water mark discards frames the
// receiver was too slow to take. driftwatch's whole claim is that it knows the
// difference between "the target is wrong" and "I did not see everything", so a
// source that loses frames has to say so. That is what GapSignal is for, and
// §9 M4 is blunt that it is easy to forget.
package source

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nabrahma/driftwatch/pkg/clock"
)

// Sentinel errors.
var (
	// ErrUnknownSource reports a source name that is not registered.
	ErrUnknownSource = errors.New("unknown source")

	// ErrBadConfig reports a source configuration that cannot be honored.
	ErrBadConfig = errors.New("invalid source configuration")

	// ErrClosed reports use of a source after Close.
	ErrClosed = errors.New("source is closed")

	// ErrIdle reports a session ended because nothing arrived within the
	// configured idle timeout.
	//
	// It is a sentinel rather than a bare error because it is the one
	// "failure" that is often not one: a genuinely quiet publisher produces it
	// too. What the reconnection path does with it is the same either way —
	// back off, re-resolve, reconnect, signal possible loss — but an operator
	// reading LastError needs to be able to tell it from a refused connection.
	ErrIdle = errors.New("no frames received within the idle timeout")

	// ErrPayloadTooLarge reports a frame beyond MaxPayloadBytes. It is counted
	// and dropped rather than propagated: an oversized frame is a producer bug,
	// and allocating for it is how one bad publisher takes down the auditor.
	ErrPayloadTooLarge = errors.New("payload exceeds the configured maximum")
)

// defaultMaxPayloadBytes bounds a single frame. Generous for an event, small
// enough that a runaway producer cannot exhaust memory one frame at a time.
const defaultMaxPayloadBytes = 8 << 20 // 8 MiB

// defaultShutdownGrace matches §6.3.
const defaultShutdownGrace = 10 * time.Second

// RawMessage is one frame as it came off the transport.
type RawMessage struct {
	Topic   string
	Payload []byte
	// ObservedAt is driftwatch's local receive time, stamped here at the edge.
	//
	// It is never the publisher's clock, and this is the only place it can
	// honestly be set. Everything downstream — settlement above all — is built
	// on it, and §5.3 depends on it being local and monotonic so that a
	// publisher with a skewed clock cannot make settlement decisions unsound
	// (F5).
	ObservedAt time.Time
}

// GapReason names why a source may have missed messages.
type GapReason string

const (
	// GapReconnect means the transport reconnected. Whatever was published
	// while the socket was down was missed, and on PUB/SUB there is no way to
	// find out what.
	GapReconnect GapReason = "reconnect"

	// GapHighWaterMark means the transport discarded frames because the
	// receiver was too slow.
	GapHighWaterMark GapReason = "high_water_mark"

	// GapOversized means a frame was refused for exceeding MaxPayloadBytes.
	GapOversized GapReason = "oversized_frame"

	// GapIdle means the session was ended because nothing arrived within the
	// configured idle timeout.
	//
	// Separate from GapReconnect so the log says which of the two happened. A
	// reconnect after an error and a reconnect after silence look identical
	// downstream and mean different things upstream: the first is a peer that
	// went away noisily, the second is one that went away without saying so,
	// which is the case D-025 exists for.
	GapIdle GapReason = "idle_timeout"
)

// GapSignal reports that the source may have missed messages.
//
// "May have" is the honest phrasing and the reason this exists at all. On a
// PUB/SUB transport a subscriber cannot tell how much it missed while
// disconnected, or whether it missed anything at all. What it can tell is that
// a window existed in which loss was possible, and that is enough for the
// pipeline to mark the affected keys Suspect and stop asserting on them until
// trust is restored (§5.2).
type GapSignal struct {
	Source string
	Reason GapReason
	At     time.Time
	// Detail carries transport-specific context for the operator, such as which
	// endpoint dropped. Never parsed.
	Detail string
}

// Source delivers raw messages until ctx is canceled or Close is called.
type Source interface {
	Name() string

	// Run reads from the transport and sends to out until ctx is done.
	//
	// Run must return only after all its goroutines have exited, and must
	// return within ShutdownGrace of cancellation — which for a socket-backed
	// source means a receive timeout rather than an indefinite block (§9 M4).
	//
	// Implementations must not close out; the caller owns it.
	Run(ctx context.Context, out chan<- RawMessage) error

	// Stats returns transport-level counters for diagnostics.
	Stats() Stats

	// Close releases transport resources. Idempotent.
	Close() error
}

// GapSignaller is implemented by sources that can lose messages.
//
// It is a separate interface rather than a method on Source because not every
// source can: a file replay reads every byte or fails, and handing it a gap
// channel nobody ever writes to would suggest loss is possible where it is not.
type GapSignaller interface {
	// Gaps returns the channel on which possible-loss signals arrive. It is
	// buffered, and a signal that would block is dropped: the pipeline needs to
	// learn that a gap happened, not how many times.
	Gaps() <-chan GapSignal
}

// Stats returns transport-level counters for diagnostics.
type Stats struct {
	Connected      bool
	Reconnects     uint64
	FramesReceived uint64
	BytesReceived  uint64
	LastFrameAt    time.Time
	LastError      string

	// Dropped counts frames the source itself discarded — oversized ones, and
	// sends the pipeline could not accept. It is deliberately separate from the
	// transport's own invisible drops: what driftwatch discards it can count,
	// and §8.1 requires the ingest buffer to be sized so that loss lands here
	// rather than silently in the socket.
	Dropped uint64
	// Gaps counts possible-loss signals emitted.
	Gaps uint64
}

// Constructor builds a Source from configuration.
type Constructor func(cfg Config, clk clock.Clock) (Source, error)

// Config is the string-keyed configuration a source is built from.
type Config struct {
	// Settings holds the source-specific options, as they arrive from a
	// DriftCheck spec.
	Settings map[string]string

	// MaxPayloadBytes bounds one frame. Default 8 MiB.
	MaxPayloadBytes int

	// ShutdownGrace bounds how long Run may take to return after cancellation.
	// Default 10s, matching §6.3.
	ShutdownGrace time.Duration
}

func (c *Config) applyDefaults() {
	if c.MaxPayloadBytes <= 0 {
		c.MaxPayloadBytes = defaultMaxPayloadBytes
	}
	if c.ShutdownGrace <= 0 {
		c.ShutdownGrace = defaultShutdownGrace
	}
}

// Setting returns a configured value or a default.
func (c Config) Setting(key, def string) string {
	if v, ok := c.Settings[key]; ok && v != "" {
		return v
	}
	return def
}

// SettingList returns a comma-separated value as a slice, trimmed and with
// empties removed.
func (c Config) SettingList(key string) []string {
	raw := c.Setting(key, "")
	if raw == "" {
		return nil
	}

	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// SettingInt returns an integer setting, or an error naming the key.
func (c Config) SettingInt(key string, def int) (int, error) {
	raw := c.Setting(key, "")
	if raw == "" {
		return def, nil
	}

	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%w: %s must be an integer, got %q", ErrBadConfig, key, raw)
	}
	return n, nil
}

// SettingDuration returns a duration setting, or an error naming the key.
func (c Config) SettingDuration(key string, def time.Duration) (time.Duration, error) {
	raw := c.Setting(key, "")
	if raw == "" {
		return def, nil
	}

	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%w: %s must be a duration, got %q", ErrBadConfig, key, raw)
	}
	if d < 0 {
		return 0, fmt.Errorf("%w: %s must not be negative, got %q", ErrBadConfig, key, raw)
	}
	return d, nil
}

// SettingBool returns a boolean setting, or an error naming the key.
func (c Config) SettingBool(key string, def bool) (bool, error) {
	raw := c.Setting(key, "")
	if raw == "" {
		return def, nil
	}

	b, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%w: %s must be true or false, got %q", ErrBadConfig, key, raw)
	}
	return b, nil
}

var (
	registryMu sync.RWMutex
	registry   = map[string]Constructor{}
)

// Register adds a source constructor under name.
//
// It panics on a duplicate. Two sources answering to one name is a
// configuration that silently does the wrong thing, and a panic at init is the
// only moment it can be caught before it matters.
func Register(name string, ctor Constructor) {
	registryMu.Lock()
	defer registryMu.Unlock()

	if _, exists := registry[name]; exists {
		panic("source: duplicate registration for " + name)
	}
	registry[name] = ctor
}

// New builds a registered source.
func New(name string, cfg Config, clk clock.Clock) (Source, error) {
	registryMu.RLock()
	ctor, ok := registry[name]
	registryMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("%w: %q, registered: %v", ErrUnknownSource, name, Registered())
	}

	cfg.applyDefaults()
	if clk == nil {
		clk = clock.Real()
	}
	return ctor(cfg, clk)
}

// Registered returns the registered source names, sorted.
func Registered() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()

	out := make([]string, 0, len(registry))
	for name := range registry {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// counters holds a source's stats behind one mutex.
//
// A mutex rather than atomics because LastFrameAt and LastError are not machine
// words, and a Stats snapshot mixing a counter from one moment with a timestamp
// from another is a diagnostic that misleads at exactly the moment somebody is
// relying on it.
type counters struct {
	mu sync.Mutex
	s  Stats
}

func (c *counters) frame(n int, at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.s.FramesReceived++
	c.s.BytesReceived += uint64(n) //nolint:gosec // a frame length is never negative
	c.s.LastFrameAt = at
}

func (c *counters) connected(v bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.s.Connected = v
}

func (c *counters) reconnected() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.s.Reconnects++
	c.s.Connected = false
}

func (c *counters) dropped() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.s.Dropped++
}

func (c *counters) gap() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.s.Gaps++
}

func (c *counters) fail(err error) {
	if err == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.s.LastError = err.Error()
}

func (c *counters) snapshot() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.s
}

// gapChannel is the shared plumbing for sources that can lose messages.
type gapChannel struct {
	name string
	ch   chan GapSignal
	c    *counters
}

func newGapChannel(name string, c *counters) *gapChannel {
	// Buffered, because the pipeline learning that a gap happened is what
	// matters; learning it sixteen times is not. A full buffer means the
	// pipeline already has signals it has not read, so another adds nothing.
	return &gapChannel{name: name, ch: make(chan GapSignal, 16), c: c}
}

// Gaps returns the possible-loss channel.
func (g *gapChannel) Gaps() <-chan GapSignal { return g.ch }

// signal reports possible loss, without ever blocking the receive loop.
func (g *gapChannel) signal(reason GapReason, at time.Time, detail string) {
	g.c.gap()

	select {
	case g.ch <- GapSignal{Source: g.name, Reason: reason, At: at, Detail: detail}:
	default:
	}
}

// send delivers a message to the pipeline, or reports that it could not.
//
// The only two outcomes are "delivered" and "the context is done". It never
// blocks indefinitely on a full channel.
func send(ctx context.Context, out chan<- RawMessage, msg RawMessage) bool {
	select {
	case out <- msg:
		return true
	case <-ctx.Done():
		return false
	}
}

// trySend is send without the wait, for sources that must not stall.
//
// A socket-backed source uses this rather than send: if the pipeline cannot
// keep up, the frame is dropped and counted here, where the number is visible.
// Blocking instead would stop the receive loop, and a stopped receive loop on
// PUB/SUB means the socket drops frames nobody counts (§8.1).
func trySend(out chan<- RawMessage, msg RawMessage) bool {
	select {
	case out <- msg:
		return true
	default:
		return false
	}
}
