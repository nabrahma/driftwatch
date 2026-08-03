package source_test

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// Third-party goroutines only. §16.5 permits an ignore for one of those
		// with a reason, and none of driftwatch's own is ignored here: the whole
		// point of TestZMQ_RunReturnsPromptlyWhenCancelledWhileBlockedInReceive
		// is that the source's own receive goroutine is accounted for, and an
		// ignore covering it would hollow that test out.
		//
		// Every entry names a function that exists in the pinned zmq4 and has
		// been seen to survive a test. The list is deliberately not padded with
		// plausible neighbours, because three of the five entries this replaces
		// named symbols that are not in zmq4 v0.17.0 at all: a closure in
		// newQReader, which launches no goroutine; another in addConn, which is
		// a direct `go q.listen` call; and a (*Conn).run method that does not
		// exist. The list looked thorough while matching nothing, and the one
		// goroutine that does leak was not in it.

		// Started twice per socket, tied to the socket's context.
		goleak.IgnoreAnyFunction("github.com/go-zeromq/zmq4.(*socket).connReaper"),

		// The PUB side's dispatch loop, started by newPubMWriter.
		goleak.IgnoreAnyFunction("github.com/go-zeromq/zmq4.(*pubMWriter).run"),

		// The SUB side's per-connection reader, and the one that actually leaks.
		// It cannot be cancelled once parked, which is a property of the library
		// rather than of how driftwatch drives it:
		//
		//	for {
		//	        msg := r.read()
		//	        select {
		//	        case <-ctx.Done():
		//	                return
		//	        default:
		//	                q.c <- msg      // not inside the select
		//	        }
		//	}
		//
		// The context is checked before the send, and the send is not part of
		// the select. A goroutine that has committed to `q.c <- msg` with
		// nothing draining stays there whatever the caller does; closing the
		// socket does not release it either.
		//
		// q.c is buffered to ten, which is why this is intermittent rather than
		// constant. It parks only once a test has left eleven messages
		// undrained, so it surfaced on CI well before it ever did locally.
		goleak.IgnoreAnyFunction("github.com/go-zeromq/zmq4.(*qreader).listen"),
	)
}
