// Command e2eharness is the publisher and materializer the e2e suite runs in
// the cluster (PRD §14.2).
//
// It is a separate binary from driftwatch on purpose. §18 requires the release
// image to be distroless with nothing in it but the two production binaries,
// and a test harness that shipped inside it would be a permanent invitation to
// run fault injection against production. This builds into its own image, which
// only the e2e suite ever loads.
//
// Two modes, one binary, because the alternative is two images to build and
// load for every run and the code they share — the event format above all — is
// most of both.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/redis/go-redis/v9"
)

func main() {
	// main only decides the exit code. Everything with a defer in it lives in
	// run, because os.Exit does not unwind the stack — a defer here would skip
	// the signal handler's own teardown on every non-zero exit.
	os.Exit(run())
}

func run() int {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: e2eharness publish|materialize|healthcheck [flags]")
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "publish":
		err = publish(ctx, os.Args[2:])
	case "materialize":
		err = materialize(ctx, os.Args[2:])
	case "healthcheck":
		err = healthcheck(ctx, os.Args[2:])
	default:
		err = fmt.Errorf("unknown mode %q", os.Args[1])
	}

	// A canceled context is how SIGTERM ends every mode, so it is a clean exit
	// rather than a failure. Compose sends one to every container on `down`.
	if err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "e2eharness:", err)
		return 1
	}
	return 0
}

// ---------------------------------------------------------------------------
// The event format.
// ---------------------------------------------------------------------------

// event is the canonical driftwatch wire format, which pkg/codec decodes with
// no fieldMapping at all. The e2e suite deliberately does not exercise a
// foreign format: the unit tests cover mapping thoroughly, and using the
// canonical one here keeps a failing e2e run from having two candidate causes.
type event struct {
	Publisher string `json:"publisher"`
	Epoch     uint64 `json:"epoch"`
	Seq       uint64 `json:"seq"`
	Op        string `json:"op"`
	Key       string `json:"key"`
	Member    string `json:"member,omitempty"`
	Value     string `json:"value,omitempty"`
	Timestamp string `json:"ts"`
}

// ---------------------------------------------------------------------------
// publish
// ---------------------------------------------------------------------------

func publish(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("publish", flag.ContinueOnError)

	bind := fs.String("bind", "tcp://0.0.0.0:5557", "PUB endpoint to bind")
	topic := fs.String("topic", "kv-events", "topic prefix on every message")
	rate := fs.Int("rate", 500, "events per second")
	keys := fs.Int("keys", 500, "size of the key space")
	publisherID := fs.String("publisher", "", "publisher identity; defaults to the hostname")
	epoch := fs.Uint64("epoch", 0, "declared epoch; 0 derives one from the start time")
	seed := fs.Int64("seed", 1, "makes the stream reproducible")
	statusAddr := fs.String("status-addr", ":8090", "address serving /healthz and /stats")
	multipart := fs.Bool("multipart", true, "send topic and payload as separate frames")

	if err := fs.Parse(args); err != nil {
		return err
	}

	id := *publisherID
	if id == "" {
		host, err := os.Hostname()
		if err != nil {
			return fmt.Errorf("deriving the publisher identity: %w", err)
		}
		id = host
	}

	// The epoch defaults to the process start time in seconds. That is what
	// makes E7 work: a rescheduled pod comes back with a higher epoch and a
	// sequence number that restarts at 1, and driftwatch has to read that as a
	// restart rather than as 900,000 missing events.
	incarnation := *epoch
	if incarnation == 0 {
		incarnation = uint64(time.Now().Unix()) //nolint:gosec // a timestamp, not a size
	}

	sock := zmq4.NewPub(ctx)
	defer sock.Close() //nolint:errcheck // process is exiting

	if err := sock.Listen(*bind); err != nil {
		return fmt.Errorf("binding %s: %w", *bind, err)
	}

	var emitted atomic.Uint64
	serveStatus(*statusAddr, func() map[string]any {
		return map[string]any{
			"mode":      "publish",
			"publisher": id,
			"epoch":     incarnation,
			"emitted":   emitted.Load(),
		}
	})

	fmt.Printf("publishing as %s epoch %d on %s topic %q at %d/s\n",
		id, incarnation, *bind, *topic, *rate)

	rnd := rand.New(rand.NewSource(*seed)) //nolint:gosec // synthetic data
	ticker := time.NewTicker(pacing(*rate))
	defer ticker.Stop()

	batch := batchFor(*rate)

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("stopping after %d events\n", emitted.Load())
			return nil
		case <-ticker.C:
			for i := 0; i < batch; i++ {
				seq := emitted.Add(1)

				payload, err := json.Marshal(event{
					Publisher: id,
					Epoch:     incarnation,
					Seq:       seq,
					Op:        "add",
					Key:       "block:" + strconv.Itoa(rnd.Intn(*keys)),
					Member:    id,
					Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
				})
				if err != nil {
					return err
				}

				if err := send(sock, *topic, payload, *multipart); err != nil {
					return fmt.Errorf("publishing seq %d: %w", seq, err)
				}
			}
		}
	}
}

// send writes one message in whichever framing convention was asked for.
//
// Both are exercised because §8.1 says the ZMQ source detects them per frame
// rather than being told which to expect, and an e2e run that only ever saw one
// would leave the detection untested where it actually matters.
func send(sock zmq4.Socket, topic string, payload []byte, multipart bool) error {
	if multipart {
		return sock.SendMulti(zmq4.NewMsgFrom([]byte(topic), payload))
	}
	return sock.Send(zmq4.NewMsg(append(append([]byte(topic), ' '), payload...)))
}

// pacing turns an event rate into a ticker interval.
//
// Above a few hundred a second the OS timer resolution stops being able to keep
// up one tick per event, so the loop sends a batch per tick instead. 5ms is
// short enough that the stream still looks continuous to a settlement window
// measured in seconds.
func pacing(rate int) time.Duration {
	if rate <= 200 {
		return time.Second / time.Duration(rate)
	}
	return 5 * time.Millisecond
}

func batchFor(rate int) int {
	if rate <= 200 {
		return 1
	}
	return max(1, rate/200)
}

// ---------------------------------------------------------------------------
// materialize
// ---------------------------------------------------------------------------

func materialize(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("materialize", flag.ContinueOnError)

	connect := fs.String("connect", "tcp://publisher:5557", "PUB endpoint to subscribe to")
	topic := fs.String("topic", "kv-events", "topic prefix to subscribe to")
	redisAddr := fs.String("redis", "redis:6379", "Redis address")
	statusAddr := fs.String("status-addr", ":8090", "address serving /healthz and /stats")

	// The fault E2 needs. §14.4 describes it as the publisher skipping a range
	// destined for the materializer; it is applied here instead, because from
	// the store's side the two are the same event never landing, and doing it
	// here keeps the publisher to one socket that driftwatch and the
	// materializer both read. What matters for the test is that driftwatch sees
	// the events and Redis does not.
	skipFrom := fs.Uint64("skip-from", 0, "first seq to drop, inclusive; 0 disables")
	skipTo := fs.Uint64("skip-to", 0, "last seq to drop, inclusive")

	if err := fs.Parse(args); err != nil {
		return err
	}

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	defer rdb.Close() //nolint:errcheck // process is exiting

	if err := waitForRedis(ctx, rdb); err != nil {
		return err
	}

	sock := zmq4.NewSub(ctx)
	defer sock.Close() //nolint:errcheck // process is exiting

	// Subscribe before connecting. The other order is the slow-joiner race:
	// a SUB socket with no subscription installed discards everything that
	// arrives before the subscription reaches the publisher, and on a PUB
	// socket there is no way to ask for it again. See docs/DISCOVERIES.md.
	if err := sock.SetOption(zmq4.OptionSubscribe, *topic); err != nil {
		return fmt.Errorf("subscribing to %q: %w", *topic, err)
	}
	if err := sock.Dial(*connect); err != nil {
		return fmt.Errorf("connecting to %s: %w", *connect, err)
	}

	var applied, skipped, failed atomic.Uint64
	serveStatus(*statusAddr, func() map[string]any {
		return map[string]any{
			"mode":    "materialize",
			"applied": applied.Load(),
			"skipped": skipped.Load(),
			"failed":  failed.Load(),
		}
	})

	fmt.Printf("materializing %s topic %q into %s (skip %d-%d)\n",
		*connect, *topic, *redisAddr, *skipFrom, *skipTo)

	for {
		if ctx.Err() != nil {
			return nil
		}

		msg, err := sock.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("receiving: %w", err)
		}

		var ev event
		if err := json.Unmarshal(payloadOf(msg), &ev); err != nil {
			failed.Add(1)
			continue
		}

		if *skipFrom > 0 && ev.Seq >= *skipFrom && ev.Seq <= *skipTo {
			// Deliberately not written. driftwatch saw this event; Redis will
			// not have it, which is exactly the divergence E2 asserts on.
			skipped.Add(1)
			continue
		}

		if err := apply(ctx, rdb, &ev); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			failed.Add(1)
			continue
		}
		applied.Add(1)
	}
}

// payloadOf returns the event bytes from either framing convention.
func payloadOf(msg zmq4.Msg) []byte {
	if len(msg.Frames) >= 2 {
		return msg.Frames[1]
	}
	if len(msg.Frames) == 1 {
		// Single-frame: "<topic> <payload>". Split on the first space, which is
		// the convention the publisher above uses and the one pkg/source
		// detects.
		frame := msg.Frames[0]
		for i, b := range frame {
			if b == ' ' {
				return frame[i+1:]
			}
		}
		return frame
	}
	return nil
}

// apply writes one event into Redis in the keysetOwnership shape.
func apply(ctx context.Context, rdb *redis.Client, ev *event) error {
	switch ev.Op {
	case "add":
		return rdb.SAdd(ctx, ev.Key, ev.Member).Err()
	case "remove":
		return rdb.SRem(ctx, ev.Key, ev.Member).Err()
	case "delete":
		return rdb.Del(ctx, ev.Key).Err()
	default:
		return nil
	}
}

// waitForRedis blocks until Redis answers or the context ends.
//
// Bounded polling rather than a fixed wait: the materializer and Redis start
// together, and a pod that exited because its dependency was two seconds late
// would restart-loop its way through the whole scenario.
func waitForRedis(ctx context.Context, rdb *redis.Client) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	deadline := time.Now().Add(2 * time.Minute)

	for {
		if err := rdb.Ping(ctx).Err(); err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("redis did not answer within 2m")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// ---------------------------------------------------------------------------
// healthcheck
// ---------------------------------------------------------------------------

// healthcheck GETs a URL and exits non-zero unless it answers 2xx.
//
// It exists because this image is distroless: there is no curl, no wget and no
// shell, so a Docker HEALTHCHECK has nothing to call except the binary already
// in the image. Compose needs one — the materializer must not start before the
// publisher is listening, or it connects to nothing and the demo comes up with
// an empty Redis.
func healthcheck(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("healthcheck", flag.ContinueOnError)
	url := fs.String("url", "http://127.0.0.1:8090/healthz", "URL to probe")

	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, *url, http.NoBody)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close() //nolint:errcheck // probe

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s answered %s", *url, resp.Status)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Status endpoint.
// ---------------------------------------------------------------------------

// serveStatus exposes /healthz and /stats.
//
// /stats is what the e2e suite polls to know the publisher has actually
// emitted something before it starts asserting — a readiness probe that
// reported only "the process is up" would let a scenario begin against an empty
// stream and fail for the wrong reason.
func serveStatus(addr string, stats func() map[string]any) {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok")) //nolint:errcheck // a probe endpoint; a failed write is the prober's problem
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(stats()) //nolint:errcheck // a probe endpoint; a failed write is the prober's problem
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(os.Stderr, "status server:", err)
		}
	}()
}
