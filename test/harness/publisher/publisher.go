// Package publisher generates synthetic event streams for tests (§13).
//
// The stream is a pure function of the configuration: the same seed, publisher
// count and key count produce byte-identical messages every run. That is what
// lets a scenario name the message it wants perturbed — DropSeqRange(500, 500)
// removes a specific event touching a specific key, and KeyForSeq says which
// key that was, so the assertion reads as intent rather than as arithmetic.
//
// Sequence numbers are per publisher, because that is how a real fleet works
// and because it is the case sequence tracking gets wrong: one global counter
// looks fine with a single publisher and reports permanent gaps with three.
package publisher

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/source"
)

// Config configures a Publisher.
type Config struct {
	// Publishers is how many distinct publisher identities emit. Default 1.
	Publishers int
	// Keys is the size of the key space. Default 100.
	Keys int
	// Shape decides what kind of events are produced: scalar assignments, set
	// membership, or counter increments.
	Shape projection.Shape
	// Topic is the topic every message carries. Default "events".
	Topic string
	// Interval is how far apart consecutive events are stamped. Default 1ms.
	Interval time.Duration
	// Seed makes the stream reproducible. Default 1.
	Seed int64
	// Clock supplies the base time. Required for a scenario to control pacing.
	Clock clock.Clock
}

func (c *Config) applyDefaults() {
	if c.Publishers <= 0 {
		c.Publishers = 1
	}
	if c.Keys <= 0 {
		c.Keys = 100
	}
	if c.Topic == "" {
		c.Topic = "events"
	}
	if c.Interval <= 0 {
		c.Interval = time.Millisecond
	}
	if c.Seed == 0 {
		c.Seed = 1
	}
	if c.Clock == nil {
		c.Clock = clock.Real()
	}
}

// Publisher emits a deterministic synthetic event stream.
type Publisher struct {
	cfg Config

	mu sync.Mutex
	// rnd drives key selection and operation choice. One generator for the
	// whole stream, advanced once per message, so the Nth message is the same
	// whatever was asked for before it.
	rnd *rand.Rand
	// perPublisher holds each identity's next sequence number.
	perPublisher []uint64
	// emitted counts messages produced, which is the global position a fault
	// like Oversize(atMsg) counts in.
	emitted uint64
	// keyBySeq remembers which key each global position touched, so a scenario
	// can name the key a dropped message would have updated.
	keyBySeq map[uint64]string
	// at is the timestamp of the next message.
	at time.Time
}

// New returns a Publisher.
func New(cfg Config) *Publisher {
	cfg.applyDefaults()

	return &Publisher{
		cfg:          cfg,
		rnd:          rand.New(rand.NewSource(cfg.Seed)), //nolint:gosec // synthetic data, not security
		perPublisher: make([]uint64, cfg.Publishers),
		keyBySeq:     map[uint64]string{},
		at:           cfg.Clock.Now(),
	}
}

// Emit produces the next n messages.
func (p *Publisher) Emit(n int) []source.RawMessage {
	p.mu.Lock()
	defer p.mu.Unlock()

	out := make([]source.RawMessage, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, p.next())
	}
	return out
}

// next produces one message and advances every counter.
func (p *Publisher) next() source.RawMessage {
	p.emitted++

	// Round-robin across publishers rather than random selection: it makes the
	// per-publisher sequence spaces regular, so a scenario can reason about
	// which publisher owns a given global position without simulating the
	// generator.
	which := int((p.emitted - 1) % uint64(p.cfg.Publishers))
	p.perPublisher[which]++

	key := "key-" + strconv.Itoa(p.rnd.Intn(p.cfg.Keys))
	p.keyBySeq[p.emitted] = key

	msg := source.RawMessage{
		Topic:      p.cfg.Topic,
		Payload:    p.payload(which, p.perPublisher[which], key),
		ObservedAt: p.at,
	}
	p.at = p.at.Add(p.cfg.Interval)
	return msg
}

// payload renders one event in the harness wire format.
//
// The field names match what the fault injector's envelope helpers read and
// what pkg/codec decodes by default, so a message can be perturbed and then
// decoded without either side needing to be configured to match the other.
func (p *Publisher) payload(which int, seq uint64, key string) []byte {
	pub := "pub-" + strconv.Itoa(which)
	ts := p.at.Format(time.RFC3339Nano)

	switch p.cfg.Shape {
	case projection.ShapeSet:
		// Membership churns: mostly additions, with removals often enough that
		// a key can legitimately empty out — which is the Redis empty-set trap
		// from D-007 and worth exercising in every scenario rather than only in
		// the projection's own tests.
		op, member := "add", "member-"+strconv.Itoa(p.rnd.Intn(4))
		if p.rnd.Intn(4) == 0 {
			op = "remove"
		}
		return []byte(fmt.Sprintf(
			`{"publisher":%q,"epoch":1,"seq":%d,"op":%q,"key":%q,"member":%q,"ts":%q}`,
			pub, seq, op, key, member, ts))

	case projection.ShapeCounter:
		return []byte(fmt.Sprintf(
			`{"publisher":%q,"epoch":1,"seq":%d,"op":"incr","key":%q,"delta":%d,"ts":%q}`,
			pub, seq, key, p.rnd.Intn(9)+1, ts))

	default: // projection.ShapeScalar
		op, value := "set", "value-"+strconv.Itoa(p.rnd.Intn(1000))
		if p.rnd.Intn(10) == 0 {
			op, value = "delete", ""
		}
		return []byte(fmt.Sprintf(
			`{"publisher":%q,"epoch":1,"seq":%d,"op":%q,"key":%q,"value":%q,"ts":%q}`,
			pub, seq, op, key, value, ts))
	}
}

// KeyForSeq returns the key the message at a global position touched.
//
// The scenario DSL leans on this: a test drops one message and then asserts on
// the key that message would have updated, by name. Without it the assertion
// would have to be "some key diverged", which is a much weaker claim and one
// that would pass if the wrong key diverged.
func (p *Publisher) KeyForSeq(seq uint64) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.keyBySeq[seq]
}

// Emitted returns how many messages have been produced.
func (p *Publisher) Emitted() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.emitted
}

// Now returns the timestamp the next message will carry.
func (p *Publisher) Now() time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.at
}

// Reset returns the publisher to its starting state, so two runs of the same
// scenario produce the same stream.
func (p *Publisher) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.rnd = rand.New(rand.NewSource(p.cfg.Seed)) //nolint:gosec // synthetic data, not security
	p.perPublisher = make([]uint64, p.cfg.Publishers)
	p.emitted = 0
	p.keyBySeq = map[uint64]string{}
	p.at = p.cfg.Clock.Now()
}
