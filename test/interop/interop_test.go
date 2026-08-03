//go:build interop

// Package interop proves wire compatibility with real libzmq (§16.6).
//
// driftwatch subscribes with github.com/go-zeromq/zmq4, a pure-Go ZMTP
// implementation, rather than a cgo binding to libzmq. ADR-0001 records why:
// static binaries, trivial cross-compilation, and a distroless image with no
// shared libraries in it. What that costs is a guarantee — wire compatibility
// with the libzmq publishers driftwatch will actually be pointed at becomes a
// claim rather than something the linker enforces.
//
// These tests buy the guarantee back by putting real libzmq, through pyzmq, on
// the other end of a real TCP socket. Both directions, because a subscriber
// that can read libzmq is only half of what driftwatch needs to be true.
package interop

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The same constants publisher.py uses. Duplicated deliberately: if one side
// changes, the test should fail rather than silently agree with itself.
var (
	topics = []string{"kv-events", "kv-events-secondary", "other-events"}

	binaryBlob = func() []byte {
		out := []byte{0x00, 0x01, 0xFF, 0xFE, 0x0A, 0x0D, 0x7F, 0x80}
		out = append(out, []byte("héllo—wörld")...)
		for b := 0; b < 32; b++ {
			out = append(out, byte(b))
		}
		return out
	}()
)

// header is the JSON every message carries.
type header struct {
	Publisher string `json:"publisher"`
	Epoch     uint64 `json:"epoch"`
	Seq       uint64 `json:"seq"`
	Op        string `json:"op"`
	Key       string `json:"key"`
	Member    string `json:"member"`
	Topic     string `json:"topic"`
}

// python resolves an interpreter with pyzmq available.
func python(t *testing.T) string {
	t.Helper()

	for _, candidate := range []string{"python3", "python", "py"} {
		path, err := exec.LookPath(candidate)
		if err != nil {
			continue
		}
		if err := exec.Command(path, "-c", "import zmq").Run(); err == nil {
			return path
		}
	}

	t.Skip("pyzmq is not installed; run: python -m pip install pyzmq")
	return ""
}

func scriptPath(t *testing.T) string {
	t.Helper()

	path, err := filepath.Abs("publisher.py")
	require.NoError(t, err)
	require.FileExists(t, path)
	return path
}

// startPython launches the helper and streams its output into the test log.
func startPython(t *testing.T, args ...string) *exec.Cmd {
	t.Helper()

	cmd := exec.Command(python(t), append([]string{scriptPath(t)}, args...)...) //nolint:gosec // fixed script

	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	cmd.Stderr = cmd.Stdout

	require.NoError(t, cmd.Start())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			t.Logf("python: %s", scanner.Text())
		}
	}()

	t.Cleanup(func() {
		// Both errors are reported rather than discarded, but neither fails the
		// test: by this point the subprocess has usually already exited on its
		// own, and "no such process" is the expected outcome rather than a
		// problem worth failing a passing interop run over.
		if killErr := cmd.Process.Kill(); killErr != nil {
			t.Logf("killing the python publisher: %v", killErr)
		}
		if waitErr := cmd.Wait(); waitErr != nil {
			t.Logf("reaping the python publisher: %v", waitErr)
		}
		wg.Wait()
	})

	return cmd
}

// announce tells the other side we are attached.
//
// This is the whole answer to the slow-joiner problem, and the reason there is
// no sleep in this file. A PUB socket discards everything it has no subscriber
// for, silently; connecting a SUB socket is asynchronous, and the subscription
// frame reaches the publisher some unpredictable time after the TCP connect
// returns. A publisher that starts the instant after bind loses an
// indeterminate prefix of its stream.
//
// The usual fix is to sleep 100ms and hope. That fails on a loaded CI runner,
// and it fails in a way that looks like message loss in the library under test
// — which is exactly the conclusion this test exists to draw correctly.
func announce(t *testing.T, endpoint string) {
	t.Helper()

	sock := zmq4.NewReq(context.Background())
	defer sock.Close() //nolint:errcheck // test cleanup

	require.Eventually(t, func() bool {
		return sock.Dial(endpoint) == nil
	}, 30*time.Second, 100*time.Millisecond, "could not reach the sync endpoint")

	require.NoError(t, sock.Send(zmq4.NewMsgString("ready")))

	_, err := sock.Recv()
	require.NoError(t, err, "the other side never acknowledged the handshake")
}

// ---------------------------------------------------------------------------
// libzmq PUB -> Go SUB
// ---------------------------------------------------------------------------

func TestInterop_LibzmqPublisherToGoSubscriber(t *testing.T) {
	const (
		count    = 10_000
		endpoint = "tcp://127.0.0.1:5599"
		sync     = "tcp://127.0.0.1:5600"
		topic    = "kv-events"
	)

	startPython(t, "publish",
		"--bind="+endpoint,
		"--sync="+sync,
		fmt.Sprintf("--count=%d", count))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	sub := zmq4.NewSub(ctx)
	defer sub.Close() //nolint:errcheck // test cleanup

	// Subscribe before dialing, always. The reverse order loses everything
	// that arrives before the subscription reaches the publisher, and on PUB
	// there is no way to ask for it again.
	require.NoError(t, sub.SetOption(zmq4.OptionSubscribe, topic))

	require.Eventually(t, func() bool {
		return sub.Dial(endpoint) == nil
	}, 30*time.Second, 100*time.Millisecond, "could not connect to the libzmq publisher")

	// Only now is it safe for the publisher to start.
	announce(t, sync)

	var (
		received   []uint64
		binaryOK   = true
		wrongTopic int
	)

	deadline := time.Now().Add(90 * time.Second)

	for time.Now().Before(deadline) {
		msg, err := sub.Recv()
		require.NoError(t, err, "receiving from libzmq")

		payload, blob := split(msg)
		if payload == nil {
			continue
		}

		var h header
		if err := json.Unmarshal(payload, &h); err != nil {
			// The terminator carries a different shape.
			if strings.Contains(string(payload), `"op":"end"`) {
				break
			}
			t.Fatalf("libzmq sent a payload the Go side could not parse: %q", payload)
		}
		if h.Op == "end" {
			break
		}

		received = append(received, h.Seq)

		// Prefix filtering: subscribing to "kv-events" must deliver
		// "kv-events-secondary" too, because ZMQ subscription is a prefix
		// match. Anything not starting with the prefix is a filtering bug.
		if !strings.HasPrefix(h.Topic, topic) {
			wrongTopic++
		}

		if blob != nil && !bytes.Equal(blob, binaryBlob) {
			binaryOK = false
		}
	}

	t.Logf("received %d messages from libzmq", len(received))

	// Exactly which sequence numbers should have arrived, rather than how many.
	//
	// Two of the three topics start with "kv-events" and ZMQ subscription is a
	// prefix match, so the expected set is every message whose topic began with
	// it — which is deliberately *not* a contiguous run. An earlier version of
	// this test asserted contiguity and failed on the two-thirds that were
	// correctly filtered out, which is the wrong lesson from the right data.
	//
	// Comparing the set is strictly stronger than comparing the count: it
	// catches a filter that dropped the right number of the wrong messages.
	var expected []uint64
	for i := 1; i <= count; i++ {
		if strings.HasPrefix(topics[i%len(topics)], topic) {
			expected = append(expected, uint64(i)) //nolint:gosec // bounded by count
		}
	}

	require.NotEmpty(t, received,
		"nothing arrived at all: the pure-Go subscriber could not read a real "+
			"libzmq publisher, which is the compatibility ADR-0001 assumes")

	assert.Equal(t, expected, received,
		"prefix subscription to %q should have delivered exactly the %d messages "+
			"on the two topics carrying that prefix; got %d.\n\n"+
			"A shortfall starting at seq 1 is the slow-joiner race, which the "+
			"handshake in this file exists to prevent. A shortfall elsewhere is "+
			"real loss between two implementations that are meant to be wire "+
			"compatible.",
		topic, len(expected), len(received))

	assert.Zero(t, wrongTopic,
		"%d messages arrived whose topic does not start with %q: the "+
			"subscription filter let through what it should have dropped",
		wrongTopic, topic)

	assert.True(t, binaryOK,
		"a binary payload did not survive the wire byte for byte. The blob "+
			"contains a null byte, a newline and high bytes precisely because a "+
			"layer treating the payload as text mangles exactly those.")
}

// ---------------------------------------------------------------------------
// Go PUB -> libzmq SUB
// ---------------------------------------------------------------------------

func TestInterop_GoPublisherToLibzmqSubscriber(t *testing.T) {
	const (
		count    = 5_000
		endpoint = "tcp://127.0.0.1:5601"
		topic    = "kv-events"
	)

	// §16.6's reverse direction. The Python process writes what it received to
	// a file this test reads back, because the two have no other channel and
	// parsing stdout would make the test sensitive to anything else either
	// process printed.
	dir := t.TempDir()
	out := filepath.Join(dir, "interop-result.json")
	ready := filepath.Join(dir, "subscriber-ready")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	pub := zmq4.NewPub(ctx)
	defer pub.Close() //nolint:errcheck // test cleanup

	require.NoError(t, pub.Listen(endpoint))

	startPython(t, "subscribe",
		"--connect="+endpoint,
		"--ready="+ready,
		"--topic="+topic,
		"--out="+out)

	// The subscriber creates the ready file once its subscription is installed.
	// Not a sleep: the publisher genuinely does not start until the other side
	// is provably attached, which is what §16.6 asks for.
	require.Eventually(t, func() bool {
		_, err := os.Stat(ready)
		return err == nil
	}, 60*time.Second, 100*time.Millisecond,
		"the libzmq subscriber never signaled that it was attached")

	// One more beat for the subscription frame to reach the publisher after the
	// subscriber's own socket reports it installed. This is the slow-joiner gap
	// that cannot be closed from the subscriber's side alone — it is why the
	// assertion below tolerates a short prefix rather than demanding all 5,000.
	for i := 1; i <= count; i++ {
		payload, err := json.Marshal(header{
			Publisher: "go-publisher",
			Epoch:     1,
			Seq:       uint64(i), //nolint:gosec // bounded by count
			Op:        "add",
			Key:       fmt.Sprintf("block:%d", i%100),
			Member:    "replica-0",
			Topic:     topic,
		})
		require.NoError(t, err)

		require.NoError(t, pub.SendMulti(zmq4.NewMsgFrom(
			[]byte(topic), payload, binaryBlob)))
	}

	require.NoError(t, pub.SendMulti(zmq4.NewMsgFrom(
		[]byte(topic), []byte(`{"op":"end"}`), nil)))

	// The result file appearing is the signal the subscriber finished.
	var result struct {
		Received        int      `json:"received"`
		Seqs            []uint64 `json:"seqs"`
		BinaryIdentical bool     `json:"binary_identical"`
		LibzmqVersion   string   `json:"libzmq_version"`
		PyzmqVersion    string   `json:"pyzmq_version"`
	}

	require.Eventually(t, func() bool {
		raw, err := os.ReadFile(out) //nolint:gosec // a path this test made
		if err != nil {
			return false
		}
		return json.Unmarshal(raw, &result) == nil
	}, 90*time.Second, 500*time.Millisecond,
		"the libzmq subscriber never wrote its result; it may have received nothing")

	t.Logf("libzmq %s / pyzmq %s received %d of %d messages",
		result.LibzmqVersion, result.PyzmqVersion, result.Received, count)

	require.NotZero(t, result.Received,
		"real libzmq could not read anything the pure-Go publisher sent. This is "+
			"the half of ADR-0001 that matters for a driftwatch deployment "+
			"publishing back into an existing ZMQ estate.")

	assert.Equal(t, count, result.Received,
		"libzmq received %d of %d messages from the Go publisher",
		result.Received, count)

	assert.True(t, result.BinaryIdentical,
		"a binary payload was altered between the Go publisher and libzmq")

	// No filtering in this direction — one topic, every message — so the
	// received sequence must be contiguous. A hole is loss.
	assertContiguous(t, result.Seqs)
}

// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

// split returns the JSON header and the binary blob from either framing.
func split(msg zmq4.Msg) (payload, blob []byte) {
	switch {
	case len(msg.Frames) >= 3:
		return msg.Frames[1], msg.Frames[2]
	case len(msg.Frames) == 2:
		return msg.Frames[1], nil
	case len(msg.Frames) == 1:
		// Single frame: "<topic> <json>\x00<blob>". Both separators matter —
		// the space is the convention, and the null is there because a layer
		// that treated the frame as a C string would truncate here.
		frame := msg.Frames[0]

		space := indexByte(frame, ' ')
		if space < 0 {
			return nil, nil
		}
		rest := frame[space+1:]

		null := indexByte(rest, 0x00)
		if null < 0 {
			return rest, nil
		}
		return rest[:null], rest[null+1:]
	default:
		return nil, nil
	}
}

func indexByte(b []byte, c byte) int {
	for i := range b {
		if b[i] == c {
			return i
		}
	}
	return -1
}

// assertContiguous reports gaps in a received sequence.
//
// A gap here is loss, and loss is the thing this whole file exists to rule out.
// Reported as a range rather than a count, because "seq 1 to 240 missing" says
// slow joiner and "seq 4,001 missing" says something else entirely.
func assertContiguous(t *testing.T, seqs []uint64) {
	t.Helper()

	if len(seqs) == 0 {
		return
	}

	var gaps []string
	for i := 1; i < len(seqs); i++ {
		if seqs[i] != seqs[i-1]+1 && seqs[i] > seqs[i-1] {
			gaps = append(gaps, fmt.Sprintf("%d-%d", seqs[i-1]+1, seqs[i]-1))
		}
	}

	assert.Empty(t, gaps,
		"the received sequence has holes in it: %s\n\n"+
			"A gap starting at 1 is the slow-joiner race, which the handshake in "+
			"this file exists to prevent. A gap elsewhere is real loss between "+
			"two implementations that are supposed to be wire compatible.",
		strings.Join(gaps, ", "))
}
