package sse_test

import (
	"testing"
	"testing/synctest"
)

// synctestRun runs fn inside a synctest bubble, where the clock is virtual and
// only advances once every goroutine is durably blocked. Retention windows,
// heartbeats and deadlines are therefore exact and instant instead of being
// approximated with real sleeps (RP-3).
func synctestRun(t *testing.T, fn func(t *testing.T)) {
	t.Helper()
	synctest.Test(t, fn)
}
