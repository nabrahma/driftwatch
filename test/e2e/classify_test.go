//go:build e2e

package e2e

import "testing"

func TestLooksLikeRegistryFailure(t *testing.T) {
	transient := []string{
		`failed to resolve source metadata for gcr.io/distroless/static-debian12:nonroot: ` +
			`failed to do request: Head "https://gcr.io/v2/...": dial tcp: lookup gcr.io: no such host`,
		"error pulling image: Get https://registry-1.docker.io/v2/: net/http: TLS handshake timeout",
		"toomanyrequests: You have reached your pull rate limit",
		"Get https://gcr.io/v2/: dial tcp 1.2.3.4:443: connect: connection refused",
	}
	for _, out := range transient {
		if !looksLikeRegistryFailure(out) {
			t.Errorf("registry failure not recognized, so a DNS blip would end a run:\n%s", out)
		}
	}

	// The half that matters more. A classifier that called these transient
	// would turn one build break into three, each ten minutes apart, and bury
	// the compiler error under retries.
	genuine := []string{
		"./pkg/check/check.go:42:2: undefined: notAThing",
		"Dockerfile:12: unknown instruction: COPYY",
		"failed to compute cache key: \"/cmd/manager\" not found: not found",
		"go: downloading ... \nbuild failed",
	}
	for _, out := range genuine {
		if looksLikeRegistryFailure(out) {
			t.Errorf("a real build failure was classed as transient:\n%s", out)
		}
	}
}
