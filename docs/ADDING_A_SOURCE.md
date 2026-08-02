# Adding a source

A source turns a transport into a stream of `RawMessage`. It is the smallest
interface in the project, and the one with the most rules attached — because
everything downstream depends on two claims a source makes and nothing else can
check: *when* a message arrived, and *whether any were lost*.

Four sources ship: `zmq`, `nats`, `file` and `memory`. This walks through adding
a fifth.

## The contract

```go
type Source interface {
    Name() string
    Run(ctx context.Context, out chan<- RawMessage) error
    Stats() Stats
    Close() error
}
```

`Run` reads from the transport and sends to `out` until the context is done.
Three obligations, each of which has bitten:

- **Do not close `out`.** The caller owns it and may have other writers.
- **Return only after every goroutine you started has exited.** goleak runs in
  every package and will fail the build otherwise, which is the point.
- **Return within `ShutdownGrace` of cancellation.** For a socket-backed source
  this means a *receive timeout*, not an indefinite block. A source that parks
  forever in `recv()` cannot be cancelled, and `kubectl delete driftcheck` hangs
  until someone reaches for `--force`.

## `RawMessage`, and the one field that matters

```go
type RawMessage struct {
    Topic      string
    Payload    []byte
    ObservedAt time.Time
}
```

**`ObservedAt` is driftwatch's local receive time, and the edge is the only place
it can honestly be set.** Every elapsed-time decision downstream is built on it —
the settlement window above all — and §5.3 depends on it being local and
monotonic. Stamp it with the injected clock the instant the frame arrives, before
any parsing.

Never set it from a timestamp inside the payload. A publisher whose clock is
skewed by more than the settlement window would make settlement unsound (fault
F5); the producer's clock is read later, by the codec, into `PublishedAt`, which
is diagnostic only.

## Signalling loss

If your transport can lose messages, implement one more interface:

```go
type GapSignaller interface {
    Gaps() <-chan GapSignal
}
```

It is separate from `Source` on purpose. A file replay reads every byte or fails,
and handing it a gap channel nobody writes to would suggest loss is possible where
it is not.

A gap signal does not say how much was lost. On a PUB/SUB transport a subscriber
cannot know that — it cannot even know whether anything was lost. What it *can*
say is that a window existed in which loss was possible, and that is enough: the
pipeline marks the affected keys suspect and stops asserting on them until trust
is restored.

Use the shared `gapChannel` helper rather than rolling your own. It is buffered
at 16 and drops rather than blocks, because the pipeline needs to learn that a
gap happened, not to learn it sixteen times.

**Signal on every reconnect**, unless your transport genuinely guarantees replay
from the last acknowledged position. A reconnect that does not signal is how a
checker silently starts asserting on a keyspace it has an incomplete view of.

## A worked example

`pkg/source/memory.go` is the shortest complete implementation; copy its shape.
The skeleton is:

```go
package source

func init() { Register("mytransport", newMyTransport) }

type myTransport struct {
    clk clock.Clock
    c   counters      // the shared Stats accumulator
    *gapChannel       // only if the transport can lose messages
    // ... your connection state
}

func newMyTransport(cfg Config, clk clock.Clock) (Source, error) {
    if cfg.MyTransport == nil || cfg.MyTransport.Endpoint == "" {
        return nil, fmt.Errorf("%w: source.mytransport.endpoint is required",
            ErrBadConfig)
    }
    s := &myTransport{clk: clk}
    s.gapChannel = newGapChannel("mytransport", &s.c)
    return s, nil
}

func (s *myTransport) Name() string { return "mytransport" }

func (s *myTransport) Run(ctx context.Context, out chan<- RawMessage) error {
    for attempt := 0; ; attempt++ {
        if err := ctx.Err(); err != nil {
            return err
        }

        conn, err := s.connect(ctx)   // re-resolve DNS here, every attempt
        if err != nil {
            s.c.setLastError(err)
            if sleepErr := s.clk.Sleep(ctx, backoff(attempt)); sleepErr != nil {
                return sleepErr
            }
            continue
        }

        // A reconnection is a window in which loss was possible.
        if attempt > 0 {
            s.c.reconnect()
            s.signal(GapReasonReconnect, s.clk.Now(), s.cfg.Endpoint)
        }

        err = s.receive(ctx, conn, out)
        _ = conn.Close()

        if ctx.Err() != nil {
            return ctx.Err()
        }
        // Any other error: loop and reconnect.
    }
}

func (s *myTransport) receive(ctx context.Context, conn *Conn,
    out chan<- RawMessage) error {

    for {
        // A deadline, not a blocking read. This is what makes cancellation work.
        conn.SetReadDeadline(s.clk.Now().Add(recvTimeout))

        frame, err := conn.Recv()
        if errors.Is(err, os.ErrDeadlineExceeded) {
            if ctx.Err() != nil {
                return ctx.Err()
            }
            continue
        }
        if err != nil {
            return err
        }

        // Stamp the arrival time here, at the edge, before anything else.
        msg := RawMessage{
            Topic:      frame.Topic,
            Payload:    frame.Body,
            ObservedAt: s.clk.Now(),
        }

        if len(msg.Payload) > s.maxPayload {
            s.c.drop()
            continue
        }
        s.c.frame(len(msg.Payload))

        // send() returns false only when the context is done. It never blocks
        // indefinitely on a full channel.
        if !send(ctx, out, msg) {
            return ctx.Err()
        }
    }
}
```

## Four traps that have already been paid for

Each of these is a real finding from building the existing sources. They cost
between an afternoon and a day each.

**Re-resolve DNS on every reconnection attempt.** Caching the first resolution
means that when the publisher pod is rescheduled onto a new IP, the source
reconnects successfully to nothing, reports itself healthy, and driftwatch goes
quiet without a single error in any log. See
[D-011](DISCOVERIES.md).

**Check that the library honours the options you set.** The pure-Go ZMQ binding
accepts a subscriber high-water mark and silently ignores it, so a buffer that
looked bounded was not. If an option matters, write a test that proves it took
effect rather than that it was accepted. See [D-010](DISCOVERIES.md).

**Subscribe before you connect,** if your transport has that distinction. ZMQ's
slow-joiner behaviour means messages published between connect and subscribe are
gone with no indication they existed.

**Know whether your topic filter is a prefix match.** ZMQ subscriptions are, and
a filter of `kv-events` also matches `kv-events-debug`. Two of three topics
matching a prefix produces exactly two thirds of the events, which looks like
33% packet loss and is not.

## Registering it

Add the config block to `pkg/check/config.go` (`SourceSpec`), the enum value to
`api/v1alpha1/driftcheck_types.go`, and a validation case to `validateSource`
that names your required fields:

```go
case "mytransport":
    s.validateMyTransport(v)
```

Then `make manifests` to regenerate the CRD, and add your field docs — they are
what `kubectl explain driftcheck.spec.source.mytransport` renders, and CI fails
if any field lacks them.

## What a new source must pass

Copy the table from an existing source's tests. The non-negotiable ones:

| Test | Why |
|---|---|
| `Run` returns `context.Canceled` promptly on cancellation | Otherwise `kubectl delete` hangs |
| goleak in `TestMain` finds nothing after `Run` returns | A leaked receive goroutine holds a socket forever |
| `Close` is idempotent | The shutdown path can reach it twice when a cancel and an explicit stop race |
| `ObservedAt` is set from the injected clock | Otherwise every settlement decision is wrong and no test would show it |
| A reconnect emits exactly one gap signal | Under-signalling loses the suspect state; over-signalling costs coverage |
| An oversized frame is dropped and counted, not truncated | A truncated event is a wrong event |
| `Stats().Dropped` moves when the pipeline cannot accept a message | §8.1 requires loss to land here rather than invisibly in the socket |

If your transport can be run in Docker, add an integration test under the
`integration` build tag. If it has a widely-used reference implementation in
another language, consider an interop test as well — `test/interop/` runs
driftwatch's pure-Go ZMQ against real libzmq in both directions, and that found
a wire-compatibility question no unit test could have.

## What not to do

- **Do not parse the payload.** That is the codec's job, and the split is what
  lets any codec pair with any transport.
- **Do not retry forever without backoff.** A tight reconnect loop against a
  down endpoint is a denial of service you are committing against yourself.
- **Do not swallow errors.** `Stats().LastError` is what an operator reads when
  a check has gone quiet, and an empty one means they have nothing to go on.
