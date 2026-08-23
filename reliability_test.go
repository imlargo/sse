package sse_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/imlargo/sse"
	"github.com/imlargo/sse/ssetest"
)

// A metrics hook is application code. If it panics, does the library contain
// it, or does it take the process down?
func TestPanicInMetricsHook(t *testing.T) {
	c := ssetest.NewConn()
	defer c.Close()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("a panicking metrics hook escaped: %v", r)
		}
	}()

	_ = sse.Serve(context.Background(), c, httptest.NewRequest("GET", "/e", nil),
		func(ctx context.Context, s *sse.Session) error {
			return s.Send(ctx, sse.Text("x"))
		},
		sse.WithKeepAlive(0),
		sse.WithMetrics(&sse.Metrics{
			EventDelivered: func(string, int, time.Duration) { panic("hook exploded") },
		}))
	time.Sleep(100 * time.Millisecond)
}

// panicTransport is what a buggy framework adapter looks like.
type panicTransport struct{ *ssetest.Conn }

func (p panicTransport) Write(b []byte) (int, error) { panic("transport exploded") }

func TestPanicInTransport(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("a panicking transport escaped: %v", r)
		}
	}()

	c := ssetest.NewConn()
	defer c.Close()
	_ = sse.Serve(context.Background(), panicTransport{c},
		httptest.NewRequest("GET", "/e", nil),
		func(ctx context.Context, s *sse.Session) error { return nil },
		sse.WithKeepAlive(0))
	time.Sleep(100 * time.Millisecond)
}

// And if either does escape, is the session left in the registry forever?
func TestSessionNotLeakedOnHookPanic(t *testing.T) {
	lc := sse.NewLifecycle()
	c := ssetest.NewConn()
	defer c.Close()

	func() {
		defer func() { _ = recover() }()
		_ = sse.Serve(context.Background(), c, httptest.NewRequest("GET", "/e", nil),
			func(ctx context.Context, s *sse.Session) error { return nil },
			sse.WithLifecycle(lc), sse.WithKeepAlive(0),
			sse.WithMetrics(&sse.Metrics{
				SessionOpened: func(sse.SessionStats) { panic("hook exploded") },
			}))
	}()
	time.Sleep(100 * time.Millisecond)

	if n := lc.NodeSessionCount(); n != 0 {
		t.Errorf("%d sessions left in the registry after a hook panicked", n)
	}
}

var _ http.ResponseWriter

// Churning connections must not accumulate anything.
//
// A stream is short-lived far more often than not — a page navigation, a mobile
// network handover, a deploy — so the cost of a session that ends has to be
// zero. Anything retained per connection becomes a leak measured in days
// rather than in requests, which is exactly the kind nothing catches until a
// server falls over on a Sunday.
func TestChurningSessionsRetainNothing(t *testing.T) {
	if testing.Short() {
		t.Skip("leak: skipped under -short")
	}

	log := sse.NewMemoryLog(sse.Retention{Events: 256})
	defer log.Close()
	b := sse.NewBroker("events", log)
	lc := sse.NewLifecycle()

	round := func(n int) {
		var wg sync.WaitGroup
		for i := range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				c := ssetest.NewConn()
				defer c.Close()
				// A spread of client behaviour, because the paths that hold
				// state are the ones a healthy client never reaches: stalling
				// arms the write deadline, throttling fills the queue and makes
				// it discard, and a grant deadline arms a timer.
				switch i % 4 {
				case 0:
					c.Throttle(time.Millisecond)
				case 1:
					go func() { time.Sleep(3 * time.Millisecond); c.Stall() }()
				}
				var grant sse.Grant
				if i%2 == 0 {
					grant.Deadline = time.Now().Add(8 * time.Millisecond)
				}

				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
				defer cancel()
				_ = sse.ServeGrant(ctx, c, httptest.NewRequest("GET", "/e", nil),
					func(sctx context.Context, s *sse.Session) error {
						if i%8 == 0 {
							// The side channel keeps a reference to the session
							// from outside it, so it is worth churning too.
							go func() {
								time.Sleep(2 * time.Millisecond)
								_ = s.Resubscribe(sse.MustFilter(fmt.Sprintf("u.%d.>", i%16)))
							}()
						}
						return b.Subscribe(sctx, s, sse.MustFilter(fmt.Sprintf("u.%d.>", i)))
					}, grant,
					sse.WithLog("events", log), sse.WithLifecycle(lc),
					sse.WithKeepAlive(4*time.Millisecond),
					sse.WithWriteTimeout(20*time.Millisecond))
			}()
			if i%50 == 0 {
				_, _ = b.Publish(context.Background(), sse.MustTopic("u.1.x"), sse.Text("x"))
			}
		}
		wg.Wait()
	}

	// Warm up, so the measurement is not dominated by first-touch costs.
	round(400)
	runtime.GC()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	baseGoroutines := runtime.NumGoroutine()

	for range 6 {
		round(400)
	}
	// Let the last writers finish before measuring.
	deadline := time.Now().Add(5 * time.Second)
	for lc.NodeSessionCount() > 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	runtime.GC()
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	growth := int64(after.HeapInuse) - int64(before.HeapInuse)
	goroutines := runtime.NumGoroutine() - baseGoroutines
	t.Logf("after 2,400 more sessions: heap %+d KB, goroutines %+d, registry %d",
		growth>>10, goroutines, lc.NodeSessionCount())

	if lc.NodeSessionCount() != 0 {
		t.Errorf("%d sessions left in the registry", lc.NodeSessionCount())
	}
	if goroutines > 20 {
		t.Errorf("%d goroutines outlived their sessions", goroutines)
	}
	if growth > 16<<20 {
		t.Errorf("heap grew %d MB across 2,400 short sessions: something is retained per connection",
			growth>>20)
	}
}

// The wake buckets carry a waiter count that a reader increments before
// blocking. If any exit path forgets to decrement it, publishing keeps closing
// and reallocating channels for buckets nobody listens on — a slow, invisible
// tax that only shows up as the process ageing badly.
func TestWakeCountersReturnToZero(t *testing.T) {
	log := sse.NewMemoryLog(sse.Retention{Events: 64})
	defer log.Close()

	var wg sync.WaitGroup
	for i := range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			r, err := log.Read(ctx, 0, sse.ReadOptions{
				Filters: []sse.Filter{sse.MustFilter(fmt.Sprintf("u.%d.>", i))},
			})
			if err != nil {
				return
			}
			defer r.Close()
			for {
				if _, err := r.Next(ctx); err != nil {
					return
				}
			}
		}()
	}
	wg.Wait()

	// With every reader gone, an append must find no bucket worth waking, and
	// so must allocate nothing at all.
	f := sse.Frame{Body: []byte("data: x\n\n"), Topic: "u.7.x"}
	allocs := testing.AllocsPerRun(200, func() {
		if _, err := log.Append(context.Background(), f); err != nil {
			t.Fatal(err)
		}
	})
	t.Logf("appending with no readers left: %.1f allocations", allocs)
	if allocs > 1 {
		t.Errorf("appending allocates %.1f with no readers: wake counters were "+
			"not returned, so publishing keeps replacing channels nobody waits on", allocs)
	}
}
