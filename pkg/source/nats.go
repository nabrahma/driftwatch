package source

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/nabrahma/driftwatch/pkg/clock"
)

func init() { Register("nats", newNATS) }

// ErrQueueGroupSet reports a configured queue group.
//
// It is a named error rather than a string because it is the one
// misconfiguration in this package that corrupts results instead of failing —
// see validateQueueGroup.
var ErrQueueGroupSet = errors.New(
	"source.nats.queueGroup must be empty: a queue group would distribute " +
		"events across replicas and corrupt the oracle")

// defaultNATSPending bounds the client's own per-subscription buffer.
const (
	defaultNATSPendingMsgs  = 100_000
	defaultNATSPendingBytes = 256 << 20 // 256 MiB
)

// NATSSource subscribes to core NATS subjects.
//
// Core NATS, not JetStream. JetStream has durable consumers and replay, which
// would make gap detection somebody else's problem — an easier problem, and a
// different one. Core NATS is fire-and-forget, so a subscriber that falls
// behind or disconnects loses messages, which is precisely the situation
// driftwatch has to stay honest about.
type NATSSource struct {
	url        string
	subjects   []string
	maxPayload int

	clk clock.Clock
	c   counters
	*gapChannel

	// connect is a seam so tests can drive reconnection and delivery without a
	// server.
	connect func(ctx context.Context) (natsConn, error)

	mu     sync.Mutex
	closed bool
	conn   natsConn
}

// natsConn is the part of *nats.Conn this source uses.
type natsConn interface {
	ChanSubscribe(subject string, ch chan *nats.Msg) (natsSubscription, error)
	Close()
}

// natsSubscription is the part of *nats.Subscription this source uses.
type natsSubscription interface {
	SetPendingLimits(msgs, bytes int) error
	Dropped() (int, error)
	Unsubscribe() error
}

// validateQueueGroup rejects a non-empty queue group.
//
// This is the one setting in this package worth a named error and a paragraph.
// A queue group tells NATS to load-balance a subject across the group's
// members, so two driftwatch replicas in one group each receive about half the
// events. Neither notices: every event either replica sees is well-formed and
// in order, the sequence numbers simply skip, and pkg/seqtrack reports gaps
// that never happened. Both replicas then mark keys Suspect and stop asserting,
// so the failure is a monitoring tool that silently checks nothing.
//
// It is also a plausible thing to configure. Queue groups are how you scale a
// NATS consumer, and adding one to a replicated deployment is the obvious move
// for anyone who has not thought about what driftwatch is doing. So it is
// rejected at construction, with a message that says why rather than only what.
func validateQueueGroup(queueGroup string) error {
	if strings.TrimSpace(queueGroup) == "" {
		return nil
	}
	return fmt.Errorf("%w (got %q)", ErrQueueGroupSet, queueGroup)
}

// NewNATS returns a core NATS source.
func NewNATS(url string, subjects []string, queueGroup string, clk clock.Clock) (*NATSSource, error) {
	if err := validateQueueGroup(queueGroup); err != nil {
		return nil, err
	}
	if strings.TrimSpace(url) == "" {
		return nil, fmt.Errorf("%w: nats.url is required", ErrBadConfig)
	}
	if len(subjects) == 0 {
		return nil, fmt.Errorf("%w: nats.subjects must list at least one subject", ErrBadConfig)
	}
	if clk == nil {
		clk = clock.Real()
	}

	n := &NATSSource{
		url:        url,
		subjects:   subjects,
		maxPayload: defaultMaxPayloadBytes,
		clk:        clk,
	}
	n.gapChannel = newGapChannel("nats", &n.c)
	n.connect = n.dial
	return n, nil
}

func newNATS(cfg Config, clk clock.Clock) (Source, error) {
	n, err := NewNATS(
		cfg.Setting("url", ""),
		cfg.SettingList("subjects"),
		cfg.Setting("queueGroup", ""),
		clk,
	)
	if err != nil {
		return nil, err
	}
	n.maxPayload = cfg.MaxPayloadBytes
	return n, nil
}

// dial opens a connection with reconnection handled by the client.
func (n *NATSSource) dial(_ context.Context) (natsConn, error) {
	conn, err := nats.Connect(n.url,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.ReconnectJitter(500*time.Millisecond, time.Second),

		// Every reconnect is a window in which messages were published and not
		// received. Core NATS will not replay them, so the only honest thing to
		// do is say a gap may have happened (§5.2).
		nats.ReconnectHandler(func(_ *nats.Conn) {
			n.c.reconnected()
			n.c.connected(true)
			n.signal(GapReconnect, n.clk.Now(), "nats reconnected to "+n.url)
		}),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			n.c.connected(false)
			n.c.fail(err)
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("connecting to %s: %w", n.url, err)
	}
	return &natsConnAdapter{Conn: conn}, nil
}

// natsConnAdapter narrows *nats.Conn to the interface this source uses.
type natsConnAdapter struct{ *nats.Conn }

func (a *natsConnAdapter) ChanSubscribe(subject string, ch chan *nats.Msg) (natsSubscription, error) {
	return a.Conn.ChanSubscribe(subject, ch)
}

// Name returns the registry name.
func (n *NATSSource) Name() string { return "nats" }

// Run subscribes and delivers messages until ctx is done.
func (n *NATSSource) Run(ctx context.Context, out chan<- RawMessage) error {
	conn, err := n.connect(ctx)
	if err != nil {
		n.c.fail(err)
		return err
	}

	n.mu.Lock()
	n.conn = conn
	n.mu.Unlock()

	defer func() {
		conn.Close()

		n.mu.Lock()
		n.conn = nil
		n.mu.Unlock()
		n.c.connected(false)
	}()

	// One channel across every subject, sized so the client buffers rather than
	// the pipeline. A per-subject channel would only add fan-in.
	msgs := make(chan *nats.Msg, 1024)

	subs := make([]natsSubscription, 0, len(n.subjects))
	defer func() {
		for _, sub := range subs {
			_ = sub.Unsubscribe() //nolint:errcheck // shutting down either way
		}
	}()

	for _, subject := range n.subjects {
		// Deliberately ChanSubscribe rather than QueueSubscribe. There is no
		// queue group here and there must not be; see validateQueueGroup.
		sub, err := conn.ChanSubscribe(subject, msgs)
		if err != nil {
			n.c.fail(err)
			return fmt.Errorf("subscribing to %q: %w", subject, err)
		}
		if err := sub.SetPendingLimits(defaultNATSPendingMsgs, defaultNATSPendingBytes); err != nil {
			n.c.fail(err)
			return fmt.Errorf("setting pending limits on %q: %w", subject, err)
		}
		subs = append(subs, sub)
	}
	n.c.connected(true)

	return n.deliver(ctx, subs, msgs, out)
}

// deliver forwards messages until the context is done.
func (n *NATSSource) deliver(
	ctx context.Context,
	subs []natsSubscription,
	msgs <-chan *nats.Msg,
	out chan<- RawMessage,
) error {
	var reportedDrops int

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case msg, open := <-msgs:
			if !open {
				return nil
			}
			if n.isClosed() {
				return nil
			}
			n.forward(msg, out)

			// The client drops for a slow consumer, silently, and only says so
			// if asked. Asking is what turns invisible loss into a gap signal
			// the pipeline can act on.
			if dropped := n.droppedCount(subs); dropped > reportedDrops {
				n.signal(GapHighWaterMark, n.clk.Now(),
					fmt.Sprintf("nats client dropped %d messages for a slow consumer", dropped))
				reportedDrops = dropped
			}
		}
	}
}

// forward turns one NATS message into a RawMessage.
func (n *NATSSource) forward(msg *nats.Msg, out chan<- RawMessage) {
	if len(msg.Data) > n.maxPayload {
		n.c.dropped()
		n.c.fail(ErrPayloadTooLarge)
		n.signal(GapOversized, n.clk.Now(),
			fmt.Sprintf("%d bytes on subject %q", len(msg.Data), msg.Subject))
		return
	}

	raw := RawMessage{Topic: msg.Subject, Payload: msg.Data, ObservedAt: n.clk.Now()}
	if trySend(out, raw) {
		n.c.frame(len(msg.Data), raw.ObservedAt)
		return
	}

	n.c.dropped()
	n.signal(GapHighWaterMark, raw.ObservedAt, "ingest buffer full on subject "+msg.Subject)
}

// droppedCount totals what the client discarded across subscriptions.
func (n *NATSSource) droppedCount(subs []natsSubscription) int {
	total := 0
	for _, sub := range subs {
		if dropped, err := sub.Dropped(); err == nil {
			total += dropped
		}
	}
	return total
}

func (n *NATSSource) isClosed() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.closed
}

// Stats returns transport-level counters.
func (n *NATSSource) Stats() Stats { return n.c.snapshot() }

// Close releases the connection. Idempotent.
func (n *NATSSource) Close() error {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil
	}
	n.closed = true
	conn := n.conn
	n.conn = nil
	n.mu.Unlock()

	if conn != nil {
		conn.Close()
	}
	n.c.connected(false)
	return nil
}
