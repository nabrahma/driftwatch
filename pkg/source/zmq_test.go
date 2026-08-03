package source_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/source"
)

// These run against a real in-process pure-Go PUB socket over TCP on loopback,
// so there is a real ZMTP handshake and a real socket, with only the network
// hop shortened.

// pubRig is a PUB socket bound to a loopback port.
type pubRig struct {
	t    *testing.T
	sock zmq4.Socket
	addr string
}

func newPubRig(t *testing.T, opts ...zmq4.Option) *pubRig {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	sock := zmq4.NewPub(ctx, opts...)

	// Port 0 lets the OS choose, so parallel tests never collide.
	require.NoError(t, sock.Listen("tcp://127.0.0.1:0"))

	t.Cleanup(func() {
		// A publisher already closed by a test that needed it gone is the
		// normal case here, so a second close failing is expected.
		_ = sock.Close() //nolint:errcheck // may already be closed by the test
		cancel()
	})
	return &pubRig{t: t, sock: sock, addr: "tcp://" + sock.Addr().String()}
}

// publish sends a two-frame message: topic then payload.
func (p *pubRig) publish(topic, payload string) {
	p.t.Helper()
	require.NoError(p.t, p.sock.Send(zmq4.NewMsgFrom([]byte(topic), []byte(payload))))
}

// publishSingle sends a one-frame message, the other convention in the wild.
func (p *pubRig) publishSingle(payload string) {
	p.t.Helper()
	require.NoError(p.t, p.sock.Send(zmq4.NewMsg([]byte(payload))))
}

// subRig runs a ZMQSource against an endpoint and collects what arrives.
type subRig struct {
	t    *testing.T
	src  *source.ZMQSource
	out  chan source.RawMessage
	done chan error
}

func newSubRig(t *testing.T, endpoint string, opts ...source.ZMQOption) *subRig {
	t.Helper()

	src, err := source.NewZMQ([]string{endpoint}, clock.Real(),
		append([]source.ZMQOption{source.WithSeed(1)}, opts...)...)
	require.NoError(t, err)

	return &subRig{t: t, src: src, out: make(chan source.RawMessage, 4096), done: make(chan error, 1)}
}

// start runs the source until the test ends.
func (s *subRig) start() {
	s.t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	go func() { s.done <- s.src.Run(ctx, s.out) }()

	s.t.Cleanup(func() {
		cancel()
		select {
		case <-s.done:
		case <-time.After(15 * time.Second):
			s.t.Error("ZMQSource.Run did not return after cancellation")
		}
		require.NoError(s.t, s.src.Close())
	})
}

// collect waits for n messages, or fails.
func (s *subRig) collect(n int, within time.Duration) []source.RawMessage {
	s.t.Helper()

	got := make([]source.RawMessage, 0, n)
	deadline := time.After(within)
	for len(got) < n {
		select {
		case msg := <-s.out:
			got = append(got, msg)
		case <-deadline:
			s.t.Fatalf("wanted %d messages, got %d", n, len(got))
		}
	}
	return got
}

// publishUntilSubscribed works around the ZMQ slow-joiner race.
//
// A SUB socket's subscription takes effect some time after Dial returns, and
// whatever the publisher sends before then is dropped with no error on either
// side. The fix is to keep publishing until the first message lands rather than
// to sleep and hope: a sleep is a guess that fails on a loaded CI machine, and
// §16.6 is explicit that this race must be handled with a handshake rather than
// a delay.
func publishUntilSubscribed(t *testing.T, pub func(), out <-chan source.RawMessage) source.RawMessage {
	t.Helper()

	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(20 * time.Second)

	for {
		pub()
		select {
		case msg := <-out:
			return msg
		case <-ticker.C:
		case <-deadline:
			t.Fatal("no message arrived; the subscription never took effect")
		}
	}
}

// drain empties a channel without blocking.
func drain(ch <-chan source.RawMessage) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func TestZMQ_DeliversFromARealPubSocket(t *testing.T) {
	pub := newPubRig(t)
	sub := newSubRig(t, pub.addr)
	sub.start()

	first := publishUntilSubscribed(t, func() { pub.publish("events", "hello") }, sub.out)

	assert.Equal(t, "events", first.Topic)
	assert.Equal(t, []byte("hello"), first.Payload)
	assert.False(t, first.ObservedAt.IsZero(), "the source stamps local receive time")

	drain(sub.out)
	for i := 0; i < 20; i++ {
		pub.publish("events", "payload-"+strconv.Itoa(i))
	}
	got := sub.collect(20, 20*time.Second)

	for i, msg := range got {
		assert.Equal(t, "payload-"+strconv.Itoa(i), string(msg.Payload),
			"PUB/SUB preserves order within a publisher")
	}
	assert.True(t, sub.src.Stats().Connected)
	assert.GreaterOrEqual(t, sub.src.Stats().FramesReceived, uint64(21))
	assert.Positive(t, sub.src.Stats().BytesReceived)
}

func TestZMQ_HandlesBothMultipartConventions(t *testing.T) {
	// §9 M4: both conventions exist in the wild. A subscriber that assumes
	// topic-then-payload treats a single-frame publisher's real payload as a
	// topic and delivers an empty message, which looks like an idle publisher
	// rather than a bug.
	pub := newPubRig(t)
	sub := newSubRig(t, pub.addr)
	sub.start()

	publishUntilSubscribed(t, func() { pub.publish("warmup", "warmup") }, sub.out)
	drain(sub.out)

	pub.publish("topic-a", "two-frame-payload")
	pub.publishSingle("single-frame-payload")

	got := sub.collect(2, 20*time.Second)

	assert.Equal(t, "topic-a", got[0].Topic)
	assert.Equal(t, []byte("two-frame-payload"), got[0].Payload)

	assert.Empty(t, got[1].Topic, "a single frame has no topic")
	assert.Equal(t, []byte("single-frame-payload"), got[1].Payload,
		"the whole frame is the payload, not the topic")
}

func TestZMQ_TopicPrefixFilteringExcludesOtherTopics(t *testing.T) {
	pub := newPubRig(t)
	sub := newSubRig(t, pub.addr, source.WithTopics("orders."))
	sub.start()

	publishUntilSubscribed(t, func() { pub.publish("orders.warmup", "w") }, sub.out)
	drain(sub.out)

	for i := 0; i < 10; i++ {
		pub.publish("orders.created", "kept-"+strconv.Itoa(i))
		pub.publish("shipments.created", "filtered-"+strconv.Itoa(i))
	}

	got := sub.collect(10, 20*time.Second)
	for _, msg := range got {
		assert.Equal(t, "orders.created", msg.Topic)
		assert.Contains(t, string(msg.Payload), "kept-")
	}

	// Nothing from the other topic should be queued behind them.
	select {
	case extra := <-sub.out:
		t.Fatalf("received a filtered topic: %q %q", extra.Topic, extra.Payload)
	case <-time.After(250 * time.Millisecond):
	}
}

func TestZMQ_ReconnectsAfterThePublisherGoesAwayAndSignalsAGap(t *testing.T) {
	// The gap signal is the point. A SUB socket cannot know what it missed
	// while disconnected, so the pipeline has to be told a window of possible
	// loss existed and mark the affected keys Suspect (§5.2).
	pub := newPubRig(t)
	sub := newSubRig(t, pub.addr, source.WithReconnectIntervalMax(50*time.Millisecond))
	sub.start()

	publishUntilSubscribed(t, func() { pub.publish("t", "before") }, sub.out)

	require.NoError(t, pub.sock.Close())

	select {
	case gap := <-sub.src.Gaps():
		assert.Equal(t, source.GapReconnect, gap.Reason)
		assert.Equal(t, "zmq", gap.Source)
		assert.False(t, gap.At.IsZero())
	case <-time.After(20 * time.Second):
		t.Fatal("losing the publisher must raise a gap signal")
	}

	assert.Eventually(t, func() bool { return sub.src.Stats().Reconnects > 0 },
		10*time.Second, 20*time.Millisecond,
		"the source must keep retrying rather than giving up")
}

func TestZMQ_DropsAtTheHighWaterMarkRatherThanGrowing(t *testing.T) {
	// §8.1 requires loss under a slow consumer to be bounded, counted and
	// visible. The socket does not enforce a receive HWM (D-010), so this
	// asserts the bound driftwatch enforces itself: a pipeline that is not
	// reading costs dropped frames and a gap signal, never unbounded memory and
	// never a stalled receive loop.
	pub := newPubRig(t)

	src, err := source.NewZMQ([]string{pub.addr}, clock.Real(), source.WithSeed(1))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, src.Close()) })

	// A deliberately slow consumer: one slot, and nobody reading it.
	out := make(chan source.RawMessage, 1)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- src.Run(ctx, out) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Error("Run did not return")
		}
	})

	// Publish until the drop counter moves. The first message may land in the
	// channel; every one after that has nowhere to go.
	assert.Eventually(t, func() bool {
		for i := 0; i < 50; i++ {
			pub.publish("t", "payload-"+strconv.Itoa(i))
		}
		return src.Stats().Dropped > 0
	}, 25*time.Second, 50*time.Millisecond,
		"a full ingest buffer must drop and count, not block or grow")

	select {
	case gap := <-src.Gaps():
		assert.Equal(t, source.GapHighWaterMark, gap.Reason)
		assert.Contains(t, gap.Detail, "ingest buffer full")
	case <-time.After(5 * time.Second):
		t.Fatal("a drop must raise a gap signal so the pipeline can mark keys Suspect")
	}

	assert.Equal(t, 100_000, src.RecvHWM(), "the configured HWM is reported, per §9 M4")
}

func TestZMQ_RunReturnsPromptlyWhenCancelledWhileBlockedInReceive(t *testing.T) {
	// §9 M4's shutdown requirement, and the reason the receive runs on its own
	// goroutine. zmq4's Recv blocks indefinitely; a source that simply called it
	// in a loop would hang until a frame arrived, which for a quiet publisher is
	// forever — and a check that cannot be stopped is a check that has to be
	// killed.
	pub := newPubRig(t)
	sub := newSubRig(t, pub.addr)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sub.src.Run(ctx, sub.out) }()

	// Get it connected, then go silent so Run is parked in Recv.
	publishUntilSubscribed(t, func() { pub.publish("t", "connected") }, sub.out)

	start := time.Now()
	cancel()

	select {
	case err := <-done:
		elapsed := time.Since(start)
		require.ErrorIs(t, err, context.Canceled)
		assert.Less(t, elapsed, 10*time.Second,
			"Run must return within shutdownGrace of cancellation, took %s", elapsed)
		t.Logf("Run returned %s after cancellation while blocked in receive", elapsed)
	case <-time.After(20 * time.Second):
		t.Fatal("Run did not return: a blocking Recv with no way to release it")
	}

	require.NoError(t, sub.src.Close())
}

func TestZMQ_SubscribesToEverythingWhenNoTopicsAreConfigured(t *testing.T) {
	// Skipping the SUBSCRIBE call for an empty topic list would mean
	// subscribing to nothing, and a SUB socket with no subscription receives in
	// silence — indistinguishable from a publisher with nothing to say.
	pub := newPubRig(t)
	sub := newSubRig(t, pub.addr)
	sub.start()

	first := publishUntilSubscribed(t,
		func() { pub.publish("any.topic.at.all", "delivered") }, sub.out)
	assert.Equal(t, []byte("delivered"), first.Payload)
}

func TestZMQ_ConfigurationIsValidated(t *testing.T) {
	tests := []struct {
		name     string
		settings map[string]string
		wantErr  string
	}{
		{
			name:     "no endpoints",
			settings: map[string]string{},
			wantErr:  "endpoints must list at least one",
		},
		{
			name:     "a recvHWM that is not a number",
			settings: map[string]string{"endpoints": "tcp://x:1", "recvHWM": "lots"},
			wantErr:  "recvHWM must be an integer",
		},
		{
			name:     "a connectTimeout that is not a duration",
			settings: map[string]string{"endpoints": "tcp://x:1", "connectTimeout": "soon"},
			wantErr:  "connectTimeout must be a duration",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := source.New("zmq", source.Config{Settings: tc.settings}, clock.Real())

			require.Error(t, err)
			assert.ErrorIs(t, err, source.ErrBadConfig)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestZMQ_CloseIsIdempotentAndSafeDuringRun(t *testing.T) {
	pub := newPubRig(t)
	sub := newSubRig(t, pub.addr, source.WithReconnectIntervalMax(10*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- sub.src.Run(ctx, sub.out) }()

	publishUntilSubscribed(t, func() { pub.publish("t", "x") }, sub.out)

	require.NoError(t, sub.src.Close())
	require.NoError(t, sub.src.Close(), "Close is idempotent")

	select {
	case err := <-done:
		assert.NoError(t, err, "a closed source ends its run cleanly rather than erroring")
	case <-time.After(20 * time.Second):
		t.Fatal("Close during Run must let Run return")
	}
}
