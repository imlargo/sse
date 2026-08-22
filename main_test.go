package sse_test

import (
	"flag"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/imlargo/sse/internal/leak"
)

// RP-4: leak detection covers every test in the package.
func TestMain(m *testing.M) { leak.Main(m.Run) }

// Soak parameters, so the same test is a quick check in CI and a real one on a
// machine with room. See TestSoakManyConnectionsWithSlowConsumers.
var (
	soakConns    = flag.Int("sse.soak.conns", 2000, "connections held open by the soak test")
	soakDuration = flag.Duration("sse.soak.duration", 8*time.Second, "how long the soak test publishes for")
)

// requestFor builds a request for tests that drive Serve directly.
func requestFor(target string) *http.Request {
	return httptest.NewRequest("GET", target, nil)
}
