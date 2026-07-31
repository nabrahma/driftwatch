package source_test

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// go-zeromq starts a per-socket connection reaper and, on the PUB side,
		// a dispatch goroutine. Both are tied to the context handed to
		// NewSub/NewPub and exit when it is canceled, but the cancellation is
		// asynchronous and goleak samples immediately after the last test.
		//
		// §16.5 permits an ignore for a third-party goroutine with a reason.
		// None of driftwatch's own is ignored here: the whole point of
		// TestZMQ_RunReturnsPromptlyWhenCancelledWhileBlockedInReceive is that
		// the source's own receive goroutine is accounted for, and an ignore
		// covering it would hollow that test out.
		goleak.IgnoreAnyFunction("github.com/go-zeromq/zmq4.(*socket).connReaper"),
		goleak.IgnoreAnyFunction("github.com/go-zeromq/zmq4.(*pubMWriter).run"),
		goleak.IgnoreAnyFunction("github.com/go-zeromq/zmq4.(*Conn).run"),
		goleak.IgnoreAnyFunction("github.com/go-zeromq/zmq4.newQReader.func1"),
		goleak.IgnoreAnyFunction("github.com/go-zeromq/zmq4.(*qreader).addConn.func1"),
	)
}
