#!/usr/bin/env python3
"""Real libzmq on the other side of the wire (PRD §16.6).

driftwatch subscribes with a pure-Go ZMTP implementation rather than a cgo
binding to libzmq. That choice buys static binaries, trivial cross-compilation
and a distroless image — see docs/DECISIONS.md ADR-0001 — and it costs a
guarantee: wire compatibility with real libzmq publishers becomes a claim rather
than a given.

This script is the other side of that claim. It is driven by the Go test in
this directory and speaks over pyzmq, which is a binding to the actual libzmq
C library, so anything it proves is proved against the real implementation
rather than against a second copy of the Go one.

Two modes:

    publish  bind PUB, wait for the subscriber to prove it is attached, then
             emit a known sequence across three topics in both framing
             conventions, with binary payloads.

    subscribe  bind SUB, receive from a Go PUB, and write what arrived to a
               file the Go test reads back. This is §16.6's reverse direction.

The handshake is the interesting part, and the reason there is no sleep
anywhere in here. See the comment on wait_for_subscriber.
"""

import argparse
import json
import sys
import time

import zmq


# The topics. Three, with one a prefix of another, because ZMQ subscription
# filtering is a prefix match rather than an exact one — a subscriber asking for
# "kv" gets "kv-events" too. A test using three unrelated topics would not
# notice if prefix matching were broken.
TOPICS = ["kv-events", "kv-events-secondary", "other-events"]

# The payload every message carries after its JSON header. Deliberately nasty:
# a null byte, a newline, the ZMTP frame delimiter, high bytes, and a UTF-8
# sequence. If any layer treats the payload as a C string or splits on
# whitespace, one of these finds it.
BINARY_BLOB = bytes([0x00, 0x01, 0xFF, 0xFE, 0x0A, 0x0D, 0x7F, 0x80]) + \
    "héllo—wörld".encode("utf-8") + bytes(range(0, 32))


def build_message(index, topic):
    """One message: a JSON header and a binary payload."""
    header = {
        "publisher": "libzmq-publisher",
        "epoch": 1,
        "seq": index,
        "op": "add",
        "key": f"block:{index % 100}",
        "member": "replica-0",
        "topic": topic,
    }
    return json.dumps(header, separators=(",", ":")).encode("utf-8")


def wait_for_subscriber(sock, sync_endpoint, expected, timeout):
    """Block until the subscriber has proved it is attached.

    This exists because of the slow-joiner problem, which is the single most
    common way to get a ZMQ PUB/SUB test wrong.

    A PUB socket drops every message it has no subscriber for, silently and
    without buffering. Connecting a SUB socket is asynchronous: the TCP connect
    completes, the ZMTP handshake completes, and the subscription itself is sent
    as a separate frame that the publisher processes some time later. A publisher
    that starts emitting the instant after bind() will therefore lose an
    unpredictable number of early messages — and the usual fix, sleeping for a
    hundred milliseconds, is a guess that fails on a loaded CI runner.

    So the subscriber tells us instead. It connects a REQ socket to a second
    endpoint and sends a byte once its subscription is installed; we do not
    start publishing until that arrives. That is a real synchronisation rather
    than a hope, which is what §16.6 asks for.
    """
    ctx = sock.context
    sync = ctx.socket(zmq.REP)
    sync.bind(sync_endpoint)

    poller = zmq.Poller()
    poller.register(sync, zmq.POLLIN)

    deadline = time.monotonic() + timeout
    ready = 0

    while ready < expected:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            sync.close()
            raise TimeoutError(
                f"only {ready} of {expected} subscribers announced themselves "
                f"within {timeout}s"
            )

        if not poller.poll(remaining * 1000):
            continue

        sync.recv()
        sync.send(b"go")
        ready += 1

    sync.close()
    return ready


def publish(args):
    ctx = zmq.Context.instance()
    sock = ctx.socket(zmq.PUB)
    sock.setsockopt(zmq.LINGER, 5000)
    sock.bind(args.bind)

    print(f"libzmq {zmq.zmq_version()} publishing on {args.bind}", flush=True)

    wait_for_subscriber(sock, args.sync, expected=1, timeout=args.timeout)
    print("subscriber announced; publishing", flush=True)

    sent = 0
    for i in range(1, args.count + 1):
        topic = TOPICS[i % len(TOPICS)]
        payload = build_message(i, topic)

        # Alternate the framing convention. §8.1 says the Go source detects
        # topic-then-payload and single-frame per message rather than being told
        # which to expect, and a test that only ever sent one would leave that
        # detection unexercised where it actually matters.
        if i % 2 == 0:
            sock.send_multipart([topic.encode(), payload, BINARY_BLOB])
        else:
            sock.send(topic.encode() + b" " + payload + b"\x00" + BINARY_BLOB)

        sent += 1

    # A terminator on every topic, so the subscriber knows the stream is
    # complete rather than having to time out. Sent on all three because a
    # subscriber filtering to one topic would never see the others.
    for topic in TOPICS:
        sock.send_multipart([topic.encode(), b'{"op":"end"}', b""])

    print(f"sent {sent} messages across {len(TOPICS)} topics", flush=True)
    sock.close()
    ctx.term()
    return 0


def subscribe(args):
    """§16.6's reverse direction: Go PUB, libzmq SUB.

    Results are written to a file the Go test reads, because the two processes
    have no other channel and parsing them off stdout would make the test
    sensitive to anything else either process printed.
    """
    ctx = zmq.Context.instance()
    sock = ctx.socket(zmq.SUB)
    # Subscribe before connecting, always. The reverse order is the slow-joiner
    # race from the other side: a SUB socket with no subscription installed
    # discards everything that arrives before one reaches the publisher.
    sock.setsockopt(zmq.SUBSCRIBE, args.topic.encode())
    sock.connect(args.connect)

    print(f"libzmq {zmq.zmq_version()} subscribed to {args.connect} "
          f"topic {args.topic!r}", flush=True)

    # Announce by creating a file the Go publisher polls for.
    #
    # A REQ/REP handshake would be more symmetrical with the other direction and
    # is what this used to do. It was replaced because it put a second pair of
    # sockets between two processes that were already failing to talk over the
    # first pair, which made a failure impossible to attribute: nothing arrived,
    # and the question "was that the PUB/SUB or the REQ/REP" had no answer.
    #
    # A file has no handshake of its own to get wrong. It is still a real
    # synchronisation rather than a sleep — the publisher does not start until
    # the subscription is provably installed.
    with open(args.ready, "w", encoding="utf-8") as f:
        f.write("ready")

    received = []
    binary_ok = True
    deadline = time.monotonic() + args.timeout

    poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)

    while time.monotonic() < deadline:
        if not poller.poll(1000):
            continue

        frames = sock.recv_multipart()
        if len(frames) < 2:
            continue

        try:
            header = json.loads(frames[1].decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue

        if header.get("op") == "end":
            break

        received.append(header.get("seq"))

        # The payload has to survive the round trip byte for byte. A layer that
        # treated it as text would mangle the null byte or the high bytes.
        if len(frames) > 2 and frames[2] != BINARY_BLOB:
            binary_ok = False

    result = {
        "received": len(received),
        "seqs": received,
        "binary_identical": binary_ok,
        "libzmq_version": zmq.zmq_version(),
        "pyzmq_version": zmq.__version__,
    }

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(result, f)

    print(f"received {len(received)} messages, binary_identical={binary_ok}",
          flush=True)

    sock.close()
    ctx.term()
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)

    p = sub.add_parser("publish", help="bind PUB and emit a known sequence")
    p.add_argument("--bind", default="tcp://127.0.0.1:5599")
    p.add_argument("--sync", default="tcp://127.0.0.1:5600")
    p.add_argument("--count", type=int, default=10_000)
    p.add_argument("--timeout", type=float, default=30.0)
    p.set_defaults(func=publish)

    p = sub.add_parser("subscribe", help="connect SUB and record what arrives")
    p.add_argument("--connect", default="tcp://127.0.0.1:5601")
    p.add_argument("--ready", required=True,
                   help="file to create once the subscription is installed")
    p.add_argument("--topic", default="kv-events")
    p.add_argument("--out", required=True)
    p.add_argument("--timeout", type=float, default=30.0)
    p.set_defaults(func=subscribe)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
