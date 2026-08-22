package sse_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imlargo/sse"
	"github.com/imlargo/sse/ssetest"
)

// Everything that can happen to a stream, happening at once, under the race
// detector.
//
// Reading finds the bugs you thought of. This is for the ones you did not: the
// orderings between publishing, subscribing, resubscribing, stopping, draining
// and the log closing underneath all of it. Each of those paths is tested
// alone; none of that says anything about them interleaving.
func TestStressConcurrentEverything(t *testing.T) {
	if testing.Short() {
		t.Skip("stress: skipped under -short")
	}

	log := sse.NewMemoryLog(sse.Retention{Events: 512, For: 2 * time.Second})
	b := sse.NewBroker("events", log,
		sse.WithBackpressure(sse.Backpressure{Policy: sse.Coalesce, MaxEvents: 8, MaxBytes: 16 << 10}))
	lc := sse.NewLifecycle()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	var (
		opened    atomic.Int64
		published atomic.Int64
		resubbed  atomic.Int64
		stopped   atomic.Int64
	)

	// Publishers, on overlapping and disjoint topics.
	for p := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ctx.Err() == nil; i++ {
				topic := sse.MustTopic(fmt.Sprintf("t.%d.%d", p, i%16))
				if _, err := b.Publish(ctx, topic, sse.Text("x"),
					sse.Name("e"), sse.Key(topic.String())); err != nil {
					return
				}
				published.Add(1)
			}
		}()
	}

	// Subscribers that come and go, some slow, some stalled, some healthy.
	for c := range 40 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				conn := ssetest.NewConn()
				switch c % 4 {
				case 0:
					conn.Throttle(time.Millisecond)
				case 1:
					// Stalls partway through, which is the case that exercises
					// the write deadline and the drop paths together.
					go func() {
						time.Sleep(time.Duration(rand.IntN(50)) * time.Millisecond)
						conn.Stall()
					}()
				}

				sessCtx, done := context.WithTimeout(ctx, time.Duration(20+rand.IntN(200))*time.Millisecond)
				filter := sse.MustFilter(fmt.Sprintf("t.%d.>", c%8))
				opened.Add(1)
				_ = sse.Serve(sessCtx, conn, httptest.NewRequest("GET", "/events", nil),
					func(sctx context.Context, s *sse.Session) error {
						return b.Subscribe(sctx, s, filter)
					},
					sse.WithLog("events", log), sse.WithLifecycle(lc),
					sse.WithKeepAlive(5*time.Millisecond),
					sse.WithWriteTimeout(30*time.Millisecond))
				done()
				conn.Close()
			}
		}()
	}

	// Something reaching in from outside: resubscribing and stopping live
	// sessions by id, which is the side channel EventSource forces.
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				for _, s := range lc.NodeSessions() {
					switch rand.IntN(8) {
					case 0:
						if err := s.Resubscribe(sse.MustFilter(
							fmt.Sprintf("t.%d.>", rand.IntN(8)))); err == nil {
							resubbed.Add(1)
						}
					case 1:
						s.Stop()
						stopped.Add(1)
					}
				}
				time.Sleep(time.Millisecond)
			}
		}()
	}

	<-ctx.Done()
	wg.Wait()

	// Draining must terminate even with everything above having just happened.
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer drainCancel()
	if err := lc.Shutdown(drainCtx); err != nil {
		t.Errorf("Shutdown did not finish: %v", err)
	}
	if n := lc.NodeSessionCount(); n != 0 {
		t.Errorf("%d sessions survived the drain", n)
	}
	if err := log.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}

	t.Logf("opened %d sessions, published %d events, %d resubscriptions, %d stops",
		opened.Load(), published.Load(), resubbed.Load(), stopped.Load())
	if published.Load() == 0 || opened.Load() == 0 {
		t.Fatal("the stress test did not actually run")
	}
}

// Closing the log while subscribers are reading it must end them, not wedge
// them. A reader blocked in Next has no other way out.
func TestStressCloseLogUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("stress: skipped under -short")
	}

	for range 20 {
		log := sse.NewMemoryLog(sse.Retention{Events: 64})
		b := sse.NewBroker("events", log)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		var wg sync.WaitGroup

		for range 25 {
			conn := ssetest.NewConn()
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer conn.Close()
				_ = sse.Serve(ctx, conn, httptest.NewRequest("GET", "/events", nil),
					func(sctx context.Context, s *sse.Session) error {
						return b.Subscribe(sctx, s)
					}, sse.WithLog("events", log), sse.WithKeepAlive(0))
			}()
		}
		go func() {
			for i := 0; i < 200 && ctx.Err() == nil; i++ {
				_, _ = b.Publish(ctx, sse.MustTopic("t"), sse.Text("x"))
			}
		}()

		time.Sleep(5 * time.Millisecond)
		_ = log.Close()

		waited := make(chan struct{})
		go func() { wg.Wait(); close(waited) }()
		select {
		case <-waited:
		case <-time.After(5 * time.Second):
			t.Fatal("closing the log left subscribers wedged")
		}
		cancel()
	}
}

// A publisher must never be blocked by a subscriber, whatever the subscriber is
// doing. This runs the publisher against every policy at once, with consumers
// in every state, and asserts on wall-clock progress.
func TestStressPublisherIsNeverBlocked(t *testing.T) {
	if testing.Short() {
		t.Skip("stress: skipped under -short")
	}

	log := sse.NewMemoryLog(sse.Retention{Events: 1024})
	defer log.Close()

	policies := []sse.Policy{sse.DropOldest, sse.DropNewest, sse.Coalesce, sse.Block, sse.Disconnect}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for i := range 50 {
		b := sse.NewBroker("events", log, sse.WithBackpressure(sse.Backpressure{
			Policy: policies[i%len(policies)], MaxEvents: 2, MaxBytes: 4 << 10,
			BlockTimeout: 20 * time.Millisecond,
		}))
		conn := ssetest.NewConn()
		conn.Stall() // every one of them is a dead weight
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer conn.Close()
			_ = sse.Serve(ctx, conn, httptest.NewRequest("GET", "/events", nil),
				func(sctx context.Context, s *sse.Session) error { return b.Subscribe(sctx, s) },
				sse.WithLog("events", log), sse.WithKeepAlive(0),
				sse.WithWriteTimeout(50*time.Millisecond))
		}()
	}
	time.Sleep(50 * time.Millisecond)

	pub := sse.NewBroker("events", log)
	const events = 2000
	start := time.Now()
	for i := range events {
		if _, err := pub.Publish(context.Background(),
			sse.MustTopic(fmt.Sprintf("t.%d", i%4)), sse.Text("x")); err != nil {
			t.Fatal(err)
		}
	}
	elapsed := time.Since(start)

	t.Logf("published %d events in %v with 50 stalled subscribers across every policy",
		events, elapsed)
	if elapsed > 3*time.Second {
		t.Errorf("publishing took %v: a subscriber is holding the publisher up", elapsed)
	}
	cancel()
	wg.Wait()
}

// Resubscribing repeatedly while events flow must not lose the subscriber's
// place or wedge it. The position is kept across a change, so a subscriber that
// keeps changing its mind still sees a contiguous stream of what it asked for.
func TestStressResubscribeDoesNotWedge(t *testing.T) {
	if testing.Short() {
		t.Skip("stress: skipped under -short")
	}

	log := sse.NewMemoryLog(sse.Retention{Events: 8192})
	defer log.Close()
	b := sse.NewBroker("events", log)
	lc := sse.NewLifecycle()

	conn := ssetest.NewConn()
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = sse.Serve(ctx, conn, httptest.NewRequest("GET", "/events", nil),
			func(sctx context.Context, s *sse.Session) error {
				return b.Subscribe(sctx, s, sse.MustFilter("t.0.>"))
			}, sse.WithLog("events", log), sse.WithLifecycle(lc), sse.WithKeepAlive(0),
			sse.WithBackpressure(sse.Backpressure{MaxEvents: 4096, MaxBytes: 1 << 22}))
	}()

	deadline := time.Now().Add(2 * time.Second)
	var sess *sse.Session
	for time.Now().Before(deadline) {
		if ss := lc.NodeSessions(); len(ss) == 1 {
			sess = ss[0]
			break
		}
		time.Sleep(time.Millisecond)
	}
	if sess == nil {
		t.Fatal("the session never registered")
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 3000 && ctx.Err() == nil; i++ {
			_, _ = b.Publish(ctx, sse.MustTopic(fmt.Sprintf("t.%d.x", i%3)), sse.Text("x"), sse.Name("e"))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 400 && ctx.Err() == nil; i++ {
			if err := sess.Resubscribe(sse.MustFilter(fmt.Sprintf("t.%d.>", i%3))); err != nil &&
				!errors.Is(err, sse.ErrSessionClosed) {
				t.Errorf("Resubscribe: %v", err)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	wg.Wait()

	// It must still be alive and still delivering.
	before := len(dataOf(conn, "e"))
	for i := range 20 {
		if _, err := b.Publish(context.Background(),
			sse.MustTopic(fmt.Sprintf("t.%d.x", i%3)), sse.Text("final"), sse.Name("e")); err != nil {
			t.Fatal(err)
		}
	}
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && len(dataOf(conn, "e")) == before {
		time.Sleep(2 * time.Millisecond)
	}
	if len(dataOf(conn, "e")) == before {
		t.Error("the session stopped delivering after repeated resubscription")
	}
	cancel()
	<-done
}
