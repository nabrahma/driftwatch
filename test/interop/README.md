# ZMQ interop test

driftwatch subscribes with a pure-Go ZMTP implementation
(`github.com/go-zeromq/zmq4`) rather than a cgo binding to libzmq. That choice
buys static binaries, trivial cross-compilation and distroless images — see
`docs/DECISIONS.md` ADR-0001 — and it costs a guarantee: wire compatibility with
real libzmq publishers becomes a claim rather than a given.

This directory buys the guarantee back by testing it. A real `pyzmq` publisher,
libzmq-backed, feeds the Go subscriber; the test asserts every message arrives
intact and in order. It runs in CI under the `interop` build tag.

The areas most likely to break are subscription-prefix filtering and multipart
framing conventions. Both must be exercised explicitly.

**Status:** publisher and test land in Phase 7. See PRD §16.6.

## Running it

```sh
make test-interop    # requires python3 and python3-zmq
```
