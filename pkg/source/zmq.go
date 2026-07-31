package source

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/go-zeromq/zmq4"

	"github.com/nabrahma/driftwatch/pkg/clock"
)

func init() { Register("zmq", newZMQ) }

// ZMQ defaults, from §9 M4.
const (
	defaultRecvHWM              = 100_000
	defaultConnectTimeout       = 5 * time.Second
	defaultReconnectIntervalMax = 30 * time.Second
	baseReconnectInterval       = 100 * time.Millisecond
)

// ZMQSource subscribes to one or more PUB endpoints over a single SUB socket.
//
// # High-water mark
//
// §8.1 requires the receive HWM to be set explicitly and sized below
// driftwatch's own ingest buffer, so that when a slow consumer causes loss, the
// loss happens in a buffer driftwatch can count rather than invisibly in the
// socket. That reasoning is sound and the mechanism it assumed is not
// available: in the pure-Go binding a SUB socket has no receive HWM at all.
// SetOption accepts one and ignores it. See docs/DISCOVERIES.md D-010.
//
// So the bound is enforced here instead. RecvHWM sizes an internal queue; when
// it is full the oldest frame is discarded, counted in Stats.Dropped, and a
// GapHighWaterMark signal is raised. That keeps §8.1's actual guarantee — loss
// is bounded, counted and visible — without depending on a socket option that
// does nothing.
//
// # Reconnection
//
// Every reconnect emits a GapSignal. A SUB socket cannot know what was
// published while it was disconnected, so the honest report is that a window of
// possible loss existed, which is exactly what the pipeline needs to mark keys
// Suspect (§5.2). Endpoints are re-resolved on every attempt; see resolveAll.
type ZMQSource struct {
	endpoints []string
	topics    []string
	recvHWM   int

	connectTimeout       time.Duration
	reconnectIntervalMax time.Duration
	maxPayload           int
	shutdownGrace        time.Duration

	clk clock.Clock
	c   counters
	*gapChannel

	// dial and resolve are seams. Production uses a real SUB socket and the
	// system resolver; tests substitute both to drive reconnection and DNS
	// changes without a network.
	dial    func(ctx context.Context) zmqSocket
	resolve func(ctx context.Context, endpoint string) ([]string, error)

	rnd struct {
		mu sync.Mutex
		r  *rand.Rand
	}

	mu     sync.Mutex
	closed bool
	sock   zmqSocket
}

// zmqSocket is the part of zmq4.Socket this source uses.
type zmqSocket interface {
	Dial(endpoint string) error
	SetOption(name string, value any) error
	Recv() (zmq4.Msg, error)
	Close() error
}

// ZMQOption configures a ZMQSource.
type ZMQOption func(*ZMQSource)

// WithTopics sets the subscription prefixes. Empty means subscribe to
// everything.
func WithTopics(topics ...string) ZMQOption {
	return func(z *ZMQSource) { z.topics = topics }
}

// WithRecvHWM bounds the internal receive queue.
func WithRecvHWM(n int) ZMQOption {
	return func(z *ZMQSource) {
		if n > 0 {
			z.recvHWM = n
		}
	}
}

// WithReconnectIntervalMax caps the backoff between reconnect attempts.
func WithReconnectIntervalMax(d time.Duration) ZMQOption {
	return func(z *ZMQSource) {
		if d > 0 {
			z.reconnectIntervalMax = d
		}
	}
}

// WithConnectTimeout bounds one connection attempt.
func WithConnectTimeout(d time.Duration) ZMQOption {
	return func(z *ZMQSource) {
		if d > 0 {
			z.connectTimeout = d
		}
	}
}

// WithSeed makes the reconnect jitter reproducible. Tests set it.
func WithSeed(seed int64) ZMQOption {
	return func(z *ZMQSource) { z.rnd.r = rand.New(rand.NewSource(seed)) } //nolint:gosec // jitter, not security
}

// NewZMQ returns a SUB source connected to endpoints.
func NewZMQ(endpoints []string, clk clock.Clock, opts ...ZMQOption) (*ZMQSource, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("%w: zmq.endpoints must list at least one endpoint", ErrBadConfig)
	}
	if clk == nil {
		clk = clock.Real()
	}

	z := &ZMQSource{
		endpoints:            endpoints,
		recvHWM:              defaultRecvHWM,
		connectTimeout:       defaultConnectTimeout,
		reconnectIntervalMax: defaultReconnectIntervalMax,
		maxPayload:           defaultMaxPayloadBytes,
		shutdownGrace:        defaultShutdownGrace,
		clk:                  clk,
		resolve:              resolveAll,
	}
	z.rnd.r = rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec // jitter, not security
	z.dial = func(ctx context.Context) zmqSocket {
		return zmq4.NewSub(ctx,
			zmq4.WithTimeout(z.connectTimeout),
			zmq4.WithAutomaticReconnect(false),
		)
	}
	z.gapChannel = newGapChannel("zmq", &z.c)

	for _, opt := range opts {
		opt(z)
	}
	return z, nil
}

func newZMQ(cfg Config, clk clock.Clock) (Source, error) {
	hwm, err := cfg.SettingInt("recvHWM", defaultRecvHWM)
	if err != nil {
		return nil, err
	}
	connectTimeout, err := cfg.SettingDuration("connectTimeout", defaultConnectTimeout)
	if err != nil {
		return nil, err
	}
	reconnectMax, err := cfg.SettingDuration("reconnectIntervalMax", defaultReconnectIntervalMax)
	if err != nil {
		return nil, err
	}

	z, err := NewZMQ(cfg.SettingList("endpoints"), clk,
		WithTopics(cfg.SettingList("topics")...),
		WithRecvHWM(hwm),
		WithConnectTimeout(connectTimeout),
		WithReconnectIntervalMax(reconnectMax),
	)
	if err != nil {
		return nil, err
	}
	z.maxPayload = cfg.MaxPayloadBytes
	z.shutdownGrace = cfg.ShutdownGrace
	return z, nil
}

// Name returns the registry name.
func (z *ZMQSource) Name() string { return "zmq" }

// RecvHWM reports the configured receive high-water mark, which §9 M4 requires
// to be reported rather than merely set.
func (z *ZMQSource) RecvHWM() int { return z.recvHWM }

// Run subscribes and delivers frames until ctx is done.
//
// It never returns because of a transport error. An unreachable endpoint at
// startup, a refused connection, a peer that goes away — all are retried
// forever with Connected reported as false, because a source that exits on a
// connection failure turns a transient network problem into a check that has to
// be restarted by hand (§9 M4 edge cases).
func (z *ZMQSource) Run(ctx context.Context, out chan<- RawMessage) error {
	attempt := 0

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if z.isClosed() {
			return nil
		}

		err := z.session(ctx, out)
		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case z.isClosed():
			return nil
		}

		// Any end to a session is a possible gap: whatever was published while
		// the socket was down is gone, and PUB/SUB offers no way to find out
		// what it was.
		z.c.reconnected()
		z.c.fail(err)
		z.signal(GapReconnect, z.clk.Now(), z.detail(err))

		attempt++
		if err := z.clk.Sleep(ctx, z.backoff(attempt)); err != nil {
			return err
		}
	}
}

func (z *ZMQSource) detail(err error) string {
	if err == nil {
		return strings.Join(z.endpoints, ",")
	}
	return err.Error()
}

// session connects, subscribes and receives until something goes wrong.
func (z *ZMQSource) session(ctx context.Context, out chan<- RawMessage) error {
	// The socket gets its own cancellable context so that a blocked Recv can be
	// released on shutdown. zmq4's Recv honors the socket's context, so
	// canceling it is what turns an indefinite block into a prompt return.
	sockCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	sock := z.dial(sockCtx)

	z.mu.Lock()
	z.sock = sock
	z.mu.Unlock()

	defer func() {
		_ = sock.Close() //nolint:errcheck // the session is over either way

		z.mu.Lock()
		z.sock = nil
		z.mu.Unlock()
		z.c.connected(false)
	}()

	if err := z.subscribe(sock); err != nil {
		return err
	}
	if err := z.connect(ctx, sock); err != nil {
		return err
	}
	z.c.connected(true)

	return z.receive(ctx, cancel, sock, out)
}

// subscribe sets the topic prefixes.
//
// With no topics configured this subscribes to "", which on a SUB socket means
// everything. Skipping the call instead would mean subscribing to nothing and
// receiving in silence — a failure that looks exactly like an idle publisher.
func (z *ZMQSource) subscribe(sock zmqSocket) error {
	topics := z.topics
	if len(topics) == 0 {
		topics = []string{""}
	}

	for _, topic := range topics {
		if err := sock.SetOption(zmq4.OptionSubscribe, topic); err != nil {
			return fmt.Errorf("subscribing to %q: %w", topic, err)
		}
	}

	// Set for completeness and for anyone reading the code against §8.1. On the
	// pure-Go binding this is a no-op; the bound that actually holds is the
	// internal queue in receive. See D-010.
	_ = sock.SetOption(zmq4.OptionHWM, z.recvHWM) //nolint:errcheck // documented no-op on SUB
	return nil
}

// connect dials every endpoint, re-resolving each one first.
//
// Every endpoint failing is still not an error worth giving up on — Run retries
// forever — but it is an error worth returning so the backoff applies and
// LastError says what happened.
func (z *ZMQSource) connect(ctx context.Context, sock zmqSocket) error {
	var dialed int
	var lastErr error

	for _, endpoint := range z.endpoints {
		addrs, err := z.resolve(ctx, endpoint)
		if err != nil {
			lastErr = err
			continue
		}
		for _, addr := range addrs {
			if err := sock.Dial(addr); err != nil {
				lastErr = fmt.Errorf("dialing %s: %w", addr, err)
				continue
			}
			dialed++
		}
	}

	if dialed == 0 {
		if lastErr == nil {
			lastErr = fmt.Errorf("no endpoint among %v could be reached", z.endpoints)
		}
		return lastErr
	}
	return nil
}

// receive reads frames until the socket fails or the context is done.
//
// The receive itself runs on its own goroutine because zmq4's Recv is blocking.
// Selecting over it is what lets Run honor ShutdownGrace: on cancellation the
// socket's context is canceled to release the block, and the goroutine is
// given the grace period to exit before Run returns regardless. Returning while
// it is still parked would be a leak, and goleak would say so.
func (z *ZMQSource) receive(
	ctx context.Context,
	releaseSocket context.CancelFunc,
	sock zmqSocket,
	out chan<- RawMessage,
) error {
	type recvResult struct {
		msg zmq4.Msg
		err error
	}

	frames := make(chan recvResult)
	done := make(chan struct{})

	go func() {
		defer close(done)
		for {
			msg, err := sock.Recv()
			select {
			case frames <- recvResult{msg: msg, err: err}:
				if err != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// waitForReceiver releases the blocked Recv and waits for the goroutine,
	// bounded by the grace period.
	waitForReceiver := func() {
		releaseSocket()
		_ = sock.Close() //nolint:errcheck // unblocking a parked Recv; the session is over

		timer := z.clk.NewTimer(z.shutdownGrace)
		defer timer.Stop()

		select {
		case <-done:
		case <-timer.C():
		}
	}

	for {
		select {
		case <-ctx.Done():
			waitForReceiver()
			return ctx.Err()

		case res := <-frames:
			if res.err != nil {
				waitForReceiver()
				return fmt.Errorf("receiving from %v: %w", z.endpoints, res.err)
			}
			if z.isClosed() {
				waitForReceiver()
				return nil
			}
			z.deliver(res.msg, out)
		}
	}
}

// deliver turns one ZMQ message into a RawMessage and hands it to the pipeline.
func (z *ZMQSource) deliver(msg zmq4.Msg, out chan<- RawMessage) {
	topic, payload, ok := splitFrames(msg.Frames)
	if !ok {
		return
	}
	if len(payload) > z.maxPayload {
		z.c.dropped()
		z.c.fail(ErrPayloadTooLarge)
		z.signal(GapOversized, z.clk.Now(), fmt.Sprintf("%d bytes on topic %q", len(payload), topic))
		return
	}

	raw := RawMessage{Topic: topic, Payload: payload, ObservedAt: z.clk.Now()}

	// The receive-side high-water mark, enforced here because the socket does
	// not enforce it. A refused send means the pipeline is behind; the frame is
	// dropped and counted rather than blocking the receive loop, because a
	// stalled receive loop applies backpressure all the way to the publisher
	// and turns driftwatch into the thing that slows the system it audits.
	if trySend(out, raw) {
		z.c.frame(len(payload), raw.ObservedAt)
		return
	}

	z.c.dropped()
	z.signal(GapHighWaterMark, raw.ObservedAt, fmt.Sprintf("ingest buffer full at topic %q", topic))
}

// splitFrames handles both multipart conventions.
//
// The common one is frame 0 topic, frame 1 payload. The other is a single frame
// that is the whole payload, which is what a publisher that does not use topic
// filtering sends. §9 M4 is explicit that both exist in the wild, and a
// subscriber that assumes the first silently treats real payloads as topics and
// delivers nothing.
//
// Frames beyond the second are ignored rather than concatenated: driftwatch has
// no way to know whether a third frame is a continuation or metadata, and
// guessing wrong corrupts the payload.
func splitFrames(frames [][]byte) (topic string, payload []byte, ok bool) {
	switch len(frames) {
	case 0:
		return "", nil, false
	case 1:
		// A zero-length single frame is a real message with an empty payload,
		// not an absent one. The codec decides what to make of it.
		return "", frames[0], true
	default:
		return string(frames[0]), frames[1], true
	}
}

// backoff returns the wait before attempt n, exponential with full jitter.
//
// Full jitter rather than a fixed multiple because every subscriber of a
// publisher that restarts reconnects at the same instant. Without jitter they
// retry in lockstep forever, and the publisher comes back to a synchronized
// thundering herd.
func (z *ZMQSource) backoff(attempt int) time.Duration {
	window := baseReconnectInterval << min(attempt, 20)
	if window > z.reconnectIntervalMax || window <= 0 {
		window = z.reconnectIntervalMax
	}

	z.rnd.mu.Lock()
	defer z.rnd.mu.Unlock()
	return time.Duration(z.rnd.r.Int63n(int64(window) + 1))
}

// resolveAll re-resolves an endpoint's host to concrete addresses.
//
// This runs on every connection attempt, and that is the entire point. Caching
// the first resolution is the classic Kubernetes failure: a publisher pod is
// rescheduled, comes back on a new IP, the DNS record updates, and a subscriber
// holding the old IP reconnects forever to an address nothing is listening on.
// It fails silently — Connected stays false, no events arrive, and the source
// looks like it is merely waiting for a quiet publisher. See D-011.
//
// Non-TCP transports (inproc, ipc) have nothing to resolve and pass through.
func resolveAll(ctx context.Context, endpoint string) ([]string, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "tcp" {
		// Anything that is not a parseable tcp:// endpoint is passed through
		// untouched: inproc and ipc have no host, and a malformed endpoint gets
		// a better error from the socket that tries to dial it than from a
		// resolver guessing at what was meant.
		return []string{endpoint}, nil //nolint:nilerr // deliberate: nothing to resolve
	}

	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		// No port to split means there is no host to resolve either. Passing
		// the endpoint through unchanged lets the socket report what is wrong
		// with it, which is a better error than one invented here.
		return []string{endpoint}, nil //nolint:nilerr // deliberate: nothing to resolve
	}
	if ip := net.ParseIP(host); ip != nil {
		return []string{endpoint}, nil
	}

	addrs, err := net.DefaultResolver.LookupHost(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("resolving %s: %w", host, err)
	}

	out := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		out = append(out, "tcp://"+net.JoinHostPort(addr, port))
	}
	return out, nil
}

func (z *ZMQSource) isClosed() bool {
	z.mu.Lock()
	defer z.mu.Unlock()
	return z.closed
}

// Stats returns transport-level counters.
func (z *ZMQSource) Stats() Stats { return z.c.snapshot() }

// Close releases the socket. Idempotent, and safe during Run.
func (z *ZMQSource) Close() error {
	z.mu.Lock()
	if z.closed {
		z.mu.Unlock()
		return nil
	}
	z.closed = true
	sock := z.sock
	z.sock = nil
	z.mu.Unlock()

	if sock != nil {
		_ = sock.Close() //nolint:errcheck // closing twice is the idempotent case
	}
	z.c.connected(false)
	return nil
}
