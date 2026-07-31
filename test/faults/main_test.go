package faults

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// go-redis starts a process-wide time cache at package init and never
		// stops it. These tests link the client transitively through
		// pkg/target, so the goroutine exists even though nothing here opens a
		// connection.
		//
		// §16.5 permits an ignore for a third-party goroutine with a reason.
		// Nothing of driftwatch's own is ignored: row 54 asserts that a
		// canceled sweep leaves no goroutine behind, and this is what makes
		// that assertion mean something.
		goleak.IgnoreAnyFunction("github.com/redis/go-redis/v9/internal/pool.startGlobalTimeCache.func1"),
	)
}
