package source

import (
	"context"
	"errors"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/clock"
)

// In-package because these drive the dial and resolve seams directly. Exporting
// them for the sake of a test would widen the API with two fields that exist
// only so a test can reach them, and every caller would then have to be told to
// leave them alone.

// stubSocket is a zmqSocket that never touches a network.
type stubSocket struct {
	onDial func(endpoint string) error
	recv   func() (zmq4.Msg, error)

	mu     sync.Mutex
	closed bool
}

func (s *stubSocket) Dial(endpoint string) error {
	if s.onDial != nil {
		return s.onDial(endpoint)
	}
	return nil
}

func (s *stubSocket) SetOption(string, any) error { return nil }

func (s *stubSocket) Recv() (zmq4.Msg, error) {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()

	if closed {
		return zmq4.Msg{}, errors.New("socket closed")
	}
	if s.recv != nil {
		return s.recv()
	}
	return zmq4.Msg{}, errors.New("no receive behavior configured")
}

func (s *stubSocket) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func TestZMQ_ReResolvesDNSOnEveryReconnect(t *testing.T) {
	// The Kubernetes failure from §9 M4's edge cases, written up as D-011. A
	// publisher pod is rescheduled onto a new IP; a subscriber that cached the
	// first resolution reconnects forever to an address nothing is listening
	// on, reporting only that it is not connected — which looks exactly like a
	// publisher that has not started yet.
	var resolutions atomic.Int64
	addrs := []string{"tcp://10.0.0.1:5555", "tcp://10.0.0.2:5555"}

	var dialed struct {
		mu sync.Mutex
		to []string
	}

	src, err := NewZMQ([]string{"tcp://publisher.default.svc:5555"}, clock.Real(),
		WithSeed(1), WithReconnectIntervalMax(time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, src.Close()) })

	// A rescheduled pod: the record changes between attempts.
	src.resolve = func(context.Context, string) ([]string, error) {
		n := resolutions.Add(1)
		return []string{addrs[min(int(n)-1, len(addrs)-1)]}, nil
	}
	src.dial = func(context.Context) zmqSocket {
		return &stubSocket{
			onDial: func(endpoint string) error {
				dialed.mu.Lock()
				dialed.to = append(dialed.to, endpoint)
				dialed.mu.Unlock()
				return nil
			},
			recv: func() (zmq4.Msg, error) { return zmq4.Msg{}, errors.New("connection reset") },
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan RawMessage, 16)
	done := make(chan error, 1)
	go func() { done <- src.Run(ctx, out) }()

	assert.Eventually(t, func() bool {
		dialed.mu.Lock()
		defer dialed.mu.Unlock()
		return len(dialed.to) >= 2
	}, 10*time.Second, time.Millisecond)

	cancel()
	<-done

	dialed.mu.Lock()
	defer dialed.mu.Unlock()

	assert.Equal(t, "tcp://10.0.0.1:5555", dialed.to[0])
	assert.Equal(t, "tcp://10.0.0.2:5555", dialed.to[1],
		"the second attempt must use the re-resolved address, not the cached one")
	assert.GreaterOrEqual(t, resolutions.Load(), int64(2),
		"resolution happens per attempt, not once at construction")
}

func TestZMQ_AnUnresolvableEndpointRetriesForeverRatherThanFailing(t *testing.T) {
	// §9 M4: an endpoint that does not resolve at startup must not fail
	// startup. A check that refuses to start because DNS was not ready yet is a
	// check that needs a human at the worst possible moment.
	var attempts atomic.Int64

	src, err := NewZMQ([]string{"tcp://nothing.invalid:5555"}, clock.Real(),
		WithSeed(1), WithReconnectIntervalMax(time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, src.Close()) })

	src.resolve = func(context.Context, string) ([]string, error) {
		attempts.Add(1)
		return nil, errors.New("no such host")
	}

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan RawMessage, 1)
	done := make(chan error, 1)
	go func() { done <- src.Run(ctx, out) }()

	assert.Eventually(t, func() bool { return attempts.Load() >= 5 },
		10*time.Second, time.Millisecond,
		"an unresolvable endpoint is retried, not fatal")

	assert.False(t, src.Stats().Connected, "and readiness reflects that it is not connected")
	assert.Contains(t, src.Stats().LastError, "no such host")

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestZMQ_RefusesAnOversizedFrame(t *testing.T) {
	// An oversized frame is a producer bug. Allocating for it is how one bad
	// publisher takes the auditor down, so it is refused, counted, and reported
	// as a gap — the pipeline has lost an event either way and needs to know.
	src, err := NewZMQ([]string{"tcp://127.0.0.1:1"}, clock.Real(), WithSeed(1))
	require.NoError(t, err)
	src.maxPayload = 8
	t.Cleanup(func() { require.NoError(t, src.Close()) })

	out := make(chan RawMessage, 4)
	src.deliver(zmq4.NewMsgFrom([]byte("t"), []byte("this payload is far too long")), out)

	assert.Empty(t, out, "an oversized frame is not delivered")
	assert.Equal(t, uint64(1), src.Stats().Dropped)
	assert.Contains(t, src.Stats().LastError, "exceeds the configured maximum")

	select {
	case gap := <-src.Gaps():
		assert.Equal(t, GapOversized, gap.Reason)
		assert.Contains(t, gap.Detail, "28 bytes")
	default:
		t.Fatal("an oversized frame must raise a gap signal")
	}
}

func TestSplitFrames(t *testing.T) {
	tests := []struct {
		name        string
		frames      [][]byte
		wantTopic   string
		wantPayload []byte
		wantOK      bool
	}{
		{
			name:   "no frames at all is not a message",
			frames: nil,
			wantOK: false,
		},
		{
			name:        "a single frame is the whole payload",
			frames:      [][]byte{[]byte(`{"seq":1}`)},
			wantTopic:   "",
			wantPayload: []byte(`{"seq":1}`),
			wantOK:      true,
		},
		{
			name:        "a single empty frame is an empty payload, not an absent one",
			frames:      [][]byte{{}},
			wantTopic:   "",
			wantPayload: []byte{},
			wantOK:      true,
		},
		{
			name:        "two frames are topic then payload",
			frames:      [][]byte{[]byte("orders"), []byte(`{"seq":1}`)},
			wantTopic:   "orders",
			wantPayload: []byte(`{"seq":1}`),
			wantOK:      true,
		},
		{
			name:        "a zero-length payload behind a topic is still a message",
			frames:      [][]byte{[]byte("orders"), {}},
			wantTopic:   "orders",
			wantPayload: []byte{},
			wantOK:      true,
		},
		{
			name: "frames beyond the second are ignored rather than guessed at",
			frames: [][]byte{
				[]byte("orders"), []byte(`{"seq":1}`), []byte("trailing metadata"),
			},
			wantTopic:   "orders",
			wantPayload: []byte(`{"seq":1}`),
			wantOK:      true,
		},
		{
			name:        "binary payloads survive intact",
			frames:      [][]byte{[]byte("t"), {0x00, 0xff, 0x1b, 0x00}},
			wantTopic:   "t",
			wantPayload: []byte{0x00, 0xff, 0x1b, 0x00},
			wantOK:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			topic, payload, ok := splitFrames(tc.frames)

			assert.Equal(t, tc.wantOK, ok)
			if !tc.wantOK {
				return
			}
			assert.Equal(t, tc.wantTopic, topic)
			assert.Equal(t, tc.wantPayload, payload)
		})
	}
}

func TestResolveAll(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		want     []string
	}{
		{
			name:     "an inproc endpoint has nothing to resolve",
			endpoint: "inproc://events",
			want:     []string{"inproc://events"},
		},
		{
			name:     "an ipc endpoint has nothing to resolve",
			endpoint: "ipc:///tmp/events.sock",
			want:     []string{"ipc:///tmp/events.sock"},
		},
		{
			name:     "a literal IPv4 address is already resolved",
			endpoint: "tcp://10.0.0.1:5555",
			want:     []string{"tcp://10.0.0.1:5555"},
		},
		{
			name:     "a literal IPv6 address is already resolved",
			endpoint: "tcp://[::1]:5555",
			want:     []string{"tcp://[::1]:5555"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveAll(context.Background(), tc.endpoint)

			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}

	t.Run("localhost resolves to a loopback address", func(t *testing.T) {
		got, err := resolveAll(context.Background(), "tcp://localhost:5555")

		require.NoError(t, err)
		require.NotEmpty(t, got)
		for _, addr := range got {
			host, port, splitErr := net.SplitHostPort(
				addr[len("tcp://"):])
			require.NoError(t, splitErr)
			assert.Equal(t, "5555", port, "the port survives resolution")
			assert.True(t, net.ParseIP(host).IsLoopback(), "resolved to %s", host)
		}
	})
}

func TestZMQ_BackoffIsBoundedAndJittered(t *testing.T) {
	// Full jitter rather than a fixed multiple, because every subscriber of a
	// publisher that restarts reconnects at the same instant. Without jitter
	// they retry in lockstep forever and the publisher comes back to a
	// synchronized thundering herd.
	src, err := NewZMQ([]string{"tcp://127.0.0.1:1"}, clock.Real(),
		WithSeed(7), WithReconnectIntervalMax(time.Second))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, src.Close()) })

	distinct := map[time.Duration]struct{}{}
	for attempt := 1; attempt <= 50; attempt++ {
		d := src.backoff(attempt)

		require.GreaterOrEqual(t, d, time.Duration(0))
		require.LessOrEqual(t, d, time.Second, "the backoff never exceeds its ceiling")
		distinct[d] = struct{}{}
	}

	assert.Greater(t, len(distinct), 10,
		"full jitter means the waits differ; identical waits would be a herd")
}

func TestZMQ_ASilentSocketEndsTheSessionRatherThanBlockingForever(t *testing.T) {
	// D-025, and the reason it is a product bug rather than a test bug.
	//
	// A SUB socket whose publisher disappears does not report an error. Recv
	// blocks, waiting for a peer that is never coming back. driftwatch's
	// session only ends on a Recv error, so without a deadline it never ends:
	// Run never retries, DNS is never re-resolved, and driftwatch sits
	// reporting itself connected and healthy while receiving nothing.
	//
	// That is the exact failure this project exists to make visible, happening
	// inside the tool. It is also invisible from every metric that was designed
	// to catch it — connected stays true, no error is recorded, and the drift
	// count stays at zero because a frozen oracle agrees with a frozen store.
	//
	// The assertion is that a silent socket produces a *new session*: the
	// deadline fires, the session ends, and Run dials again.
	var dials atomic.Int64

	src, err := NewZMQ([]string{"tcp://publisher.default.svc:5555"}, clock.Real(),
		WithSeed(1),
		WithReconnectIntervalMax(time.Millisecond),
		WithIdleTimeout(50*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, src.Close()) })

	src.resolve = func(context.Context, string) ([]string, error) {
		return []string{"tcp://10.0.0.1:5555"}, nil
	}

	// A socket that connects successfully and then says nothing at all — which
	// is what zmq4 does when the peer is gone.
	src.dial = func(ctx context.Context) zmqSocket {
		dials.Add(1)
		return &stubSocket{
			recv: func() (zmq4.Msg, error) {
				<-ctx.Done()
				return zmq4.Msg{}, ctx.Err()
			},
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := make(chan RawMessage, 1)
	done := make(chan error, 1)
	go func() { done <- src.Run(ctx, out) }()

	require.Eventually(t, func() bool { return dials.Load() >= 3 },
		30*time.Second, 10*time.Millisecond,
		"the socket went silent and the session never ended; only %d dial(s) "+
			"happened, so driftwatch would have stayed connected to a dead "+
			"publisher forever", dials.Load())

	// Every ended session must take its receive goroutine with it.
	//
	// This is the half the first version of the idle deadline got wrong, and
	// goleak would not have caught it: the goroutines do eventually exit, when
	// the check stops. What they do not do is exit per session, so a check
	// reconnecting once a minute accumulates one an hour, for as long as it
	// runs — and goleak only looks at the end.
	//
	// The cause was that the receiver selected on the *session* context while
	// the deadline cancels only the *socket* context, so it parked forever on a
	// send nobody was going to read.
	before := runtime.NumGoroutine()
	require.Eventually(t, func() bool { return dials.Load() >= int64(before)+20 },
		60*time.Second, 10*time.Millisecond,
		"not enough reconnects to make a leak visible")

	after := runtime.NumGoroutine()
	assert.Less(t, after, before+10,
		"goroutines went from %d to %d across at least 20 reconnects: each "+
			"ended session is leaving its receiver behind", before, after)

	// Every one of those sessions is a window of possible loss, and each has to
	// be reported as such — a reconnect that says nothing is how the pipeline
	// silently keeps asserting on a keyspace it can no longer see.
	assert.Positive(t, src.Stats().Gaps,
		"a session that ended on the idle deadline must still signal possible loss")

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

func TestZMQ_AZeroIdleTimeoutDisablesTheDeadline(t *testing.T) {
	// The escape hatch, and the reason it is worth having: a test that drives
	// reconnection by hand does not want a deadline firing underneath it. In
	// production nothing should set this.
	src, err := NewZMQ([]string{"tcp://publisher.default.svc:5555"}, clock.Real(),
		WithIdleTimeout(0))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, src.Close()) })

	assert.Zero(t, src.idleTimeout)

	// A disabled deadline must be a channel that never fires rather than one
	// that fires immediately, which is the failure mode of a nil timer read
	// without a guard.
	timer := src.newIdleTimer()
	defer timer.stop()

	select {
	case <-timer.expired():
		t.Fatal("a disabled idle timer fired")
	default:
	}
}

func TestZMQ_TheIdleTimeoutDefaultsToSomethingReconnectable(t *testing.T) {
	src, err := NewZMQ([]string{"tcp://publisher.default.svc:5555"}, clock.Real())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, src.Close()) })

	assert.Equal(t, defaultIdleTimeout, src.idleTimeout,
		"the deadline must be on by default; the failure it prevents is silent")
}
