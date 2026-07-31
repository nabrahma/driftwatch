package source

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/clock"
)

// In-package because these drive the connect seam directly rather than standing
// up a NATS server for behavior that is entirely about this source's own
// logic. The one thing that genuinely needs a server — that core NATS drops for
// a slow consumer — is asserted through the client's own Dropped() counter,
// which is the same signal the source reads in production.

func natsEpoch() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }

// fakeConn is a natsConn that delivers whatever a test feeds it.
type fakeConn struct {
	mu      sync.Mutex
	subs    map[string]chan *nats.Msg
	closed  bool
	dropped int
	subErr  error
}

func newFakeConn() *fakeConn {
	return &fakeConn{subs: map[string]chan *nats.Msg{}}
}

func (f *fakeConn) ChanSubscribe(subject string, ch chan *nats.Msg) (natsSubscription, error) {
	if f.subErr != nil {
		return nil, f.subErr
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.subs[subject] = ch
	return &fakeSub{conn: f, subject: subject}, nil
}

func (f *fakeConn) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
}

// deliver pushes a message as the server would.
func (f *fakeConn) deliver(subject string, data []byte) {
	f.mu.Lock()
	ch := f.subs[subject]
	f.mu.Unlock()

	if ch != nil {
		ch <- &nats.Msg{Subject: subject, Data: data}
	}
}

func (f *fakeConn) setDropped(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dropped = n
}

func (f *fakeConn) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

type fakeSub struct {
	conn    *fakeConn
	subject string
}

func (s *fakeSub) SetPendingLimits(int, int) error { return nil }

func (s *fakeSub) Dropped() (int, error) {
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()
	return s.conn.dropped, nil
}

func (s *fakeSub) Unsubscribe() error { return nil }

// runNATS starts a source against a fake connection.
func runNATS(t *testing.T, n *NATSSource, conn *fakeConn, out chan RawMessage) {
	t.Helper()

	n.connect = func(context.Context) (natsConn, error) { return conn, nil }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- n.Run(ctx, out) }()

	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("NATSSource.Run did not return")
		}
		require.NoError(t, n.Close())
	})
}

func TestNATS_ANonEmptyQueueGroupIsRejected(t *testing.T) {
	// The one misconfiguration in this package that corrupts results instead of
	// failing. A queue group load-balances the subject across its members, so
	// two driftwatch replicas each see about half the events. Neither notices:
	// every event either replica sees is well-formed and in order, the sequence
	// numbers simply skip, and pkg/seqtrack reports gaps that never happened.
	// Both replicas then mark keys Suspect and stop asserting — a monitoring
	// tool that silently checks nothing.
	tests := []struct {
		name       string
		queueGroup string
	}{
		{name: "an ordinary group name", queueGroup: "driftwatch"},
		{name: "whitespace around a name is still a name", queueGroup: "  workers  "},
		{name: "a single character", queueGroup: "q"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewNATS("nats://localhost:4222", []string{"events.>"},
				tc.queueGroup, clock.Fake(natsEpoch()))

			require.Error(t, err)
			require.ErrorIs(t, err, ErrQueueGroupSet)

			// The exact message from §10.2. It is asserted verbatim because the
			// message is the mitigation: whoever configured this needs to be
			// told why it is refused, not just that it was.
			assert.Contains(t, err.Error(),
				"source.nats.queueGroup must be empty: a queue group would "+
					"distribute events across replicas and corrupt the oracle")
			assert.Contains(t, err.Error(), tc.queueGroup,
				"and what they actually configured, so the error names itself")
		})
	}
}

func TestNATS_AnEmptyQueueGroupIsAccepted(t *testing.T) {
	for _, queueGroup := range []string{"", "   ", "\t"} {
		_, err := NewNATS("nats://localhost:4222", []string{"events.>"},
			queueGroup, clock.Fake(natsEpoch()))
		assert.NoError(t, err, "queueGroup %q is empty and must be allowed", queueGroup)
	}
}

func TestNATS_TheQueueGroupRejectionSurvivesTheRegistry(t *testing.T) {
	// The registry path is how a DriftCheck spec actually reaches this source,
	// so the validation has to hold there too rather than only in the
	// constructor a test happens to call.
	_, err := New("nats", Config{Settings: map[string]string{
		"url":        "nats://localhost:4222",
		"subjects":   "events.>",
		"queueGroup": "driftwatch",
	}}, clock.Fake(natsEpoch()))

	require.ErrorIs(t, err, ErrQueueGroupSet)
}

func TestNATS_DeliversMessagesWithTheSubjectAsTopic(t *testing.T) {
	n, err := NewNATS("nats://localhost:4222", []string{"events.orders", "events.shipments"},
		"", clock.Fake(natsEpoch()))
	require.NoError(t, err)

	conn := newFakeConn()
	out := make(chan RawMessage, 64)
	runNATS(t, n, conn, out)

	assert.Eventually(t, func() bool {
		conn.mu.Lock()
		defer conn.mu.Unlock()
		return len(conn.subs) == 2
	}, 5*time.Second, time.Millisecond, "every configured subject is subscribed")

	conn.deliver("events.orders", []byte(`{"seq":1}`))
	conn.deliver("events.shipments", []byte(`{"seq":2}`))

	first := <-out
	assert.Equal(t, "events.orders", first.Topic, "the subject becomes the topic")
	assert.Equal(t, []byte(`{"seq":1}`), first.Payload)
	assert.Equal(t, natsEpoch(), first.ObservedAt, "stamped from the injected clock")

	second := <-out
	assert.Equal(t, "events.shipments", second.Topic)

	assert.Equal(t, uint64(2), n.Stats().FramesReceived)
	assert.True(t, n.Stats().Connected)
}

func TestNATS_ClientSideDropsBecomeGapSignals(t *testing.T) {
	// Core NATS drops for a slow consumer, silently, and only says so if asked.
	// Asking is what turns invisible loss into a signal the pipeline can act
	// on: without it driftwatch would keep asserting on an oracle built from a
	// stream it knows it did not fully receive.
	n, err := NewNATS("nats://localhost:4222", []string{"events"}, "", clock.Fake(natsEpoch()))
	require.NoError(t, err)

	conn := newFakeConn()
	out := make(chan RawMessage, 64)
	runNATS(t, n, conn, out)

	assert.Eventually(t, func() bool {
		conn.mu.Lock()
		defer conn.mu.Unlock()
		return len(conn.subs) == 1
	}, 5*time.Second, time.Millisecond)

	conn.setDropped(17)
	conn.deliver("events", []byte(`{"seq":1}`))
	<-out

	select {
	case gap := <-n.Gaps():
		assert.Equal(t, GapHighWaterMark, gap.Reason)
		assert.Equal(t, "nats", gap.Source)
		assert.Contains(t, gap.Detail, "17 messages")
	case <-time.After(5 * time.Second):
		t.Fatal("a client-side drop must raise a gap signal")
	}
}

func TestNATS_AFullIngestBufferDropsRatherThanBlocking(t *testing.T) {
	// A source that blocks on a full channel stops reading its subscription,
	// and a subscription that is not read is one the server starts dropping
	// for — moving the loss somewhere driftwatch cannot count it.
	n, err := NewNATS("nats://localhost:4222", []string{"events"}, "", clock.Fake(natsEpoch()))
	require.NoError(t, err)

	conn := newFakeConn()
	out := make(chan RawMessage, 1)
	runNATS(t, n, conn, out)

	assert.Eventually(t, func() bool {
		conn.mu.Lock()
		defer conn.mu.Unlock()
		return len(conn.subs) == 1
	}, 5*time.Second, time.Millisecond)

	for i := 0; i < 10; i++ {
		conn.deliver("events", []byte(`{"seq":`+strconv.Itoa(i)+`}`))
	}

	assert.Eventually(t, func() bool { return n.Stats().Dropped > 0 },
		5*time.Second, time.Millisecond,
		"a full ingest buffer drops and counts rather than stalling the subscription")

	select {
	case gap := <-n.Gaps():
		assert.Equal(t, GapHighWaterMark, gap.Reason)
	case <-time.After(5 * time.Second):
		t.Fatal("a drop must raise a gap signal")
	}
}

func TestNATS_RefusesAnOversizedMessage(t *testing.T) {
	n, err := NewNATS("nats://localhost:4222", []string{"events"}, "", clock.Fake(natsEpoch()))
	require.NoError(t, err)
	n.maxPayload = 8

	conn := newFakeConn()
	out := make(chan RawMessage, 8)
	runNATS(t, n, conn, out)

	assert.Eventually(t, func() bool {
		conn.mu.Lock()
		defer conn.mu.Unlock()
		return len(conn.subs) == 1
	}, 5*time.Second, time.Millisecond)

	conn.deliver("events", []byte("a payload well beyond the limit"))

	assert.Eventually(t, func() bool { return n.Stats().Dropped == 1 },
		5*time.Second, time.Millisecond)
	assert.Empty(t, out, "an oversized message is not delivered")

	select {
	case gap := <-n.Gaps():
		assert.Equal(t, GapOversized, gap.Reason)
	case <-time.After(5 * time.Second):
		t.Fatal("an oversized message must raise a gap signal")
	}
}

func TestNATS_ReportsAConnectionFailureRatherThanHanging(t *testing.T) {
	n, err := NewNATS("nats://localhost:4222", []string{"events"}, "", clock.Fake(natsEpoch()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, n.Close()) })

	n.connect = func(context.Context) (natsConn, error) {
		return nil, errors.New("no servers available for connection")
	}

	err = n.Run(context.Background(), make(chan RawMessage, 1))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no servers available")
	assert.Contains(t, n.Stats().LastError, "no servers available")
	assert.False(t, n.Stats().Connected)
}

func TestNATS_CloseIsIdempotentAndClosesTheConnection(t *testing.T) {
	n, err := NewNATS("nats://localhost:4222", []string{"events"}, "", clock.Fake(natsEpoch()))
	require.NoError(t, err)

	conn := newFakeConn()
	out := make(chan RawMessage, 8)

	n.connect = func(context.Context) (natsConn, error) { return conn, nil }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- n.Run(ctx, out) }()

	assert.Eventually(t, func() bool { return n.Stats().Connected },
		5*time.Second, time.Millisecond)

	require.NoError(t, n.Close())
	require.NoError(t, n.Close(), "Close is idempotent")
	assert.True(t, conn.isClosed())

	cancel()
	<-done
}

func TestNATS_ConfigurationIsValidated(t *testing.T) {
	tests := []struct {
		name     string
		settings map[string]string
		wantErr  string
	}{
		{
			name:     "no url",
			settings: map[string]string{"subjects": "events"},
			wantErr:  "nats.url is required",
		},
		{
			name:     "no subjects",
			settings: map[string]string{"url": "nats://localhost:4222"},
			wantErr:  "subjects must list at least one",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New("nats", Config{Settings: tc.settings}, clock.Fake(natsEpoch()))

			require.Error(t, err)
			assert.ErrorIs(t, err, ErrBadConfig)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}
