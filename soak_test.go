package sse_test

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imlargo/sse"
	"github.com/imlargo/sse/ssetest"
)

// RP-7: many connections, deliberately slow consumers, and memory that settles.
//
// It is the only thing behind the claim that a node holds tens of thousands of
// connections. Everything else measures a single operation; this measures
// whether the thing stays standing.
//
// Skipped under -short, and scaled by -sse.soak.conns so the same test can be
// the quick check in CI and the real one on a machine with room:
//
//	go test -run TestSoak -sse.soak.conns=15000 -sse.soak.duration=60s
func TestSoakManyConnectionsWithSlowConsumers(t *testing.T) {
	if testing.Short() {
		t.Skip("soak test: skipped under -short")
	}

	conns := *soakConns
	duration := *soakDuration

	log := sse.NewMemoryLog(sse.Retention{Events: 4096, For: 30 * time.Second})
	defer log.Close()
	b := sse.NewBroker("events", log,
		// The realistic configuration for this shape of load: a subscriber that
		// cannot keep up is caught up rather than disconnected or blocked.
		sse.WithBackpressure(sse.Backpressure{
			Policy: sse.Coalesce, MaxEvents: 32, MaxBytes: 64 << 10,
		}))
	lc := sse.NewLifecycle()

	ctx, cancel := context.WithTimeout(context.Background(), duration+30*time.Second)
	defer cancel()

	var (
		wg        sync.WaitGroup
		delivered atomic.Int64
		failed    atomic.Int64
	)

	for i := range conns {
		c := ssetest.NewConn()
		// A third of them read slowly, which is the point: the healthy ones
		// must not be held back and memory must not grow without bound.
		if i%3 == 0 {
			c.Throttle(5 * time.Millisecond)
		}
		filter := sse.MustFilter(fmt.Sprintf("user.%d.>", i))

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer c.Close()
			err := sse.ServeGrant(ctx, c, requestFor(fmt.Sprintf("/events?u=%d", i)),
				func(ctx context.Context, s *sse.Session) error {
					return b.Subscribe(ctx, s, filter)
				},
				sse.Grant{Identity: fmt.Sprintf("u-%d", i)},
				sse.WithLog("events", b.Log()),
				sse.WithLifecycle(lc),
				sse.WithKeepAlive(5*time.Second),
				sse.WithWriteTimeout(30*time.Second),
				sse.WithMetrics(&sse.Metrics{
					EventDelivered: func(string, int, time.Duration) { delivered.Add(1) },
				}),
			)
			if err != nil {
				failed.Add(1)
			}
		}()
	}

	// Wait for everyone to be connected before measuring anything.
	deadline := time.Now().Add(60 * time.Second)
	for lc.NodeSessionCount() < conns && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if n := lc.NodeSessionCount(); n != conns {
		t.Fatalf("only %d of %d connections came up", n, conns)
	}

	runtime.GC()
	settled := heapInUse()
	baseGoroutines := runtime.NumGoroutine()
	t.Logf("connected: %d sessions, %d goroutines, heap %d MB",
		conns, baseGoroutines, settled>>20)

	// A steady mix: a broadcast everyone wants, and per-user events nobody else
	// does. Coalescing keys make the slow third catch up rather than queue.
	stop := make(chan struct{})
	var published atomic.Int64
	var pubWG sync.WaitGroup
	pubWG.Add(1)
	go func() {
		defer pubWG.Done()
		tick := time.NewTicker(2 * time.Millisecond)
		defer tick.Stop()
		for n := 0; ; n++ {
			select {
			case <-stop:
				return
			case <-tick.C:
			}
			topic := sse.MustTopic(fmt.Sprintf("user.%d.inbox", n%conns))
			if n%20 == 0 {
				topic = sse.MustTopic("broadcast.all")
			}
			if _, err := b.Publish(ctx, topic, sse.Text("payload"),
				sse.Name("e"), sse.Key(topic.String())); err != nil {
				return
			}
			published.Add(1)
		}
	}()

	// Sample the heap while it runs. It must stop growing, which is the whole
	// question: a bounded queue per subscriber means a bounded process.
	var samples []uint64
	sampleUntil := time.Now().Add(duration)
	for time.Now().Before(sampleUntil) {
		time.Sleep(duration / 8)
		runtime.GC()
		samples = append(samples, heapInUse())
	}
	close(stop)
	pubWG.Wait()

	t.Logf("published %d events, delivered %d, %d sessions still live",
		published.Load(), delivered.Load(), lc.NodeSessionCount())
	for i, s := range samples {
		t.Logf("  heap sample %d: %d MB", i+1, s>>20)
	}

	if published.Load() == 0 {
		t.Fatal("nothing was published")
	}
	if delivered.Load() == 0 {
		t.Fatal("nothing was delivered")
	}
	if n := failed.Load(); n > 0 {
		t.Errorf("%d sessions ended in error while merely being slow", n)
	}

	// Memory must settle rather than climb. Comparing the last sample against
	// the first half's peak catches a slow leak that a single reading misses.
	if len(samples) >= 4 {
		var peakEarly uint64
		for _, s := range samples[:len(samples)/2] {
			peakEarly = max(peakEarly, s)
		}
		last := samples[len(samples)-1]
		if last > peakEarly*2 {
			t.Errorf("heap grew from %d MB to %d MB over the run: memory is not settling",
				peakEarly>>20, last>>20)
		}
	}

	cancel()
	wg.Wait()

	// And everything must be released once the clients are gone. The package's
	// leak detector checks goroutines; this checks the registry.
	if n := lc.NodeSessionCount(); n != 0 {
		t.Errorf("%d sessions survived their connections", n)
	}
}

func heapInUse() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}
