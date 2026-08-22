// Package leak detects goroutines that outlive a test run.
//
// RP-4 requires leak detection in every test, not just some, so this is wired
// from each package's TestMain rather than sprinkled per-test. At tens of
// thousands of connections, one leaked goroutine per dropped client is a
// permanent memory leak — which is exactly the documented failure of the
// existing SSE libraries on fasthttp.
package leak

import (
	"fmt"
	"os"
	"runtime"
	"slices"
	"strings"
	"time"
)

// Main runs the test suite and fails the process if goroutines outlive it.
//
// Usage, in every package under test:
//
//	func TestMain(m *testing.M) { leak.Main(m.Run) }
func Main(run func() int) {
	before := snapshot()

	if code := run(); code != 0 {
		// Tests already failed. A leak report here would only add noise, and
		// a failed test often leaves goroutines behind for uninteresting
		// reasons.
		os.Exit(code)
	}

	// Goroutines can take a moment to wind down after the last test returns.
	// Retry briefly: a real leak never clears, a slow shutdown does.
	deadline := time.Now().Add(2 * time.Second)
	var leaked []string
	for {
		leaked = appeared(before, snapshot())
		if len(leaked) == 0 {
			os.Exit(0)
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	fmt.Fprintf(os.Stderr, "\nleak: %d goroutine(s) outlived the test run\n\n", len(leaked))
	for _, stack := range leaked {
		fmt.Fprintf(os.Stderr, "%s\n\n", stack)
	}
	os.Exit(1)
}

// snapshot returns the live goroutine stacks, keyed by goroutine id.
func snapshot() map[string]string {
	buf := make([]byte, 1<<16)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}

	out := make(map[string]string)
	for _, stack := range strings.Split(string(buf), "\n\n") {
		stack = strings.TrimSpace(stack)
		if stack == "" {
			continue
		}
		header, _, ok := strings.Cut(stack, "\n")
		if !ok {
			continue
		}
		// "goroutine 42 [running]:" -> "42"
		id, _, _ := strings.Cut(strings.TrimPrefix(header, "goroutine "), " ")
		out[id] = stack
	}
	return out
}

// appeared returns the stacks of goroutines present in after but not in
// before. Comparing by id means long-lived harness goroutines that predate the
// run are excluded for free, so the ignore list below only has to cover
// goroutines the harness starts *during* the run.
func appeared(before, after map[string]string) []string {
	var out []string
	for id, stack := range after {
		if _, existed := before[id]; existed || harness(stack) {
			continue
		}
		out = append(out, stack)
	}
	slices.Sort(out)
	return out
}

// harnessFrames identify goroutines owned by the runtime or the test framework.
// Deliberately short and specific: ubiquitous frames like runtime.goexit or
// runtime.gopark appear in nearly every stack and would hide real leaks.
var harnessFrames = []string{
	"testing.(*M).Run",
	"testing.runFuzzing",
	"testing.runFuzzTests",
	"testing.(*F).Fuzz",
	"os/signal.signal_recv",
	"os/signal.loop",
	"runtime.ensureSigM",
	"internal/synctest.Run",
	"github.com/imlargo/sse/internal/leak.snapshot",
}

func harness(stack string) bool {
	for _, frame := range harnessFrames {
		if strings.Contains(stack, frame) {
			return true
		}
	}
	return false
}
