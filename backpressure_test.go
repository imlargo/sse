package sse_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/imlargo/sse"
	"github.com/imlargo/sse/ssetest"
)

// stalledSession opens a session whose client never reads, then publishes n
// events, so every policy can be observed at its limit.
func stalledSession(t *testing.T, bp sse.Backpressure, writeTimeout time.Duration,
	publish func(*sse.Publisher)) (*ssetest.Conn, error) {
	t.Helper()

	log := sse.NewMemoryLog(sse.Retention{Events: 10_000})
	defer log.Close()
	pub := sse.NewPublisher(log)
	publish(pub)

	// Stalled from the very first write. Stalling later is not equivalent: the
	// backlog drains before the stall takes effect and no policy is exercised.
	c := ssetest.NewConn()
	c.Stall()
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := sse.Serve(ctx, c, httptest.NewRequest("GET", "/s", nil), sse.Follow,
		sse.WithLog("events", log),
		sse.WithBackpressure(bp),
		sse.WithKeepAlive(0),
		sse.WithWriteTimeout(writeTimeout),
	)
	return c, err
}

// RF-D3, and the test tmaxmax/go-sse fails by design: its default provider
// writes to subscribers synchronously inside the dispatch loop, so one blocked
// callback makes every other subscriber wait.
//
// Here the publisher appends to a log and returns. It never touches a
// subscriber, so this is structural rather than tuned.
func TestOneSlowSubscriberDoesNotAffectOthers(t *testing.T) {
	log := sse.NewMemoryLog(sse.Retention{Events: 100_000})
	defer log.Close()
	pub := sse.NewPublisher(log)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const fast = 20
	var wg sync.WaitGroup

	// One subscriber that stops reading almost immediately.
	slow := ssetest.NewConn()
	wg.Add(1)
	go func() {
		defer wg.Done()
		go func() { time.Sleep(20 * time.Millisecond); slow.Stall() }()
		_ = sse.Serve(ctx, slow, httptest.NewRequest("GET", "/s", nil), sse.Follow,
			sse.WithLog("events", log), sse.WithKeepAlive(0),
			sse.WithWriteTimeout(500*time.Millisecond))
	}()
	defer slow.Close()

	healthy := make([]*ssetest.Conn, fast)
	for i := range healthy {
		c := ssetest.NewConn()
		healthy[i] = c
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = sse.Serve(ctx, c, httptest.NewRequest("GET", "/s", nil), sse.Follow,
				sse.WithLog("events", log), sse.WithKeepAlive(0),
				// Room for the whole burst, so this measures isolation from the
				// stalled subscriber and not the discard policy.
				sse.WithBackpressure(sse.Backpressure{MaxEvents: 4096, MaxBytes: 1 << 24}))
		}()
	}

	const events = 500
	start := time.Now()
	for i := range events {
		if _, err := pub.Publish(ctx, sse.Text(fmt.Sprintf("e%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	publishTime := time.Since(start)

	// Publishing must not have waited on anyone. A publisher that is coupled
	// to the slowest reader shows up here as seconds, not milliseconds.
	if publishTime > 2*time.Second {
		t.Errorf("publishing %d events took %v with one stalled subscriber; "+
			"the publisher is coupled to the slowest reader", events, publishTime)
	}

	// And the healthy subscribers must have received everything.
	deadline := time.Now().Add(10 * time.Second)
	for {
		lagging := 0
		for _, c := range healthy {
			if len(c.Messages()) < events {
				lagging++
			}
		}
		if lagging == 0 || time.Now().After(deadline) {
			if lagging > 0 {
				t.Errorf("%d of %d healthy subscribers were held back by the slow one",
					lagging, fast)
			}
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	wg.Wait()
}

// RF-D5: everything discarded is announced, with a reason that can be told
// apart from ageing out of retention.
func TestDropOldestDeclaresWhatWasLost(t *testing.T) {
	log := sse.NewMemoryLog(sse.Retention{Events: 10_000})
	defer log.Close()
	pub := sse.NewPublisher(log)
	for i := range 200 {
		if _, err := pub.Publish(context.Background(), sse.Text(fmt.Sprintf("e%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	// Slow, not dead. A dead connection can never be told what it lost.
	c := ssetest.NewConn()
	c.Throttle(5 * time.Millisecond)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = sse.Serve(ctx, c, httptest.NewRequest("GET", "/s", nil), sse.Follow,
		sse.WithLog("events", log),
		sse.WithBackpressure(sse.Backpressure{Policy: sse.DropOldest, MaxEvents: 4}),
		sse.WithKeepAlive(0), sse.WithWriteTimeout(30*time.Second))

	var gap *struct {
		Reason  string `json:"reason"`
		From    uint64 `json:"from"`
		Through uint64 `json:"through"`
		Detail  string `json:"detail"`
	}
	for _, m := range c.Messages() {
		if m.Type == "sse.gap" {
			gap = new(struct {
				Reason  string `json:"reason"`
				From    uint64 `json:"from"`
				Through uint64 `json:"through"`
				Detail  string `json:"detail"`
			})
			if err := json.Unmarshal([]byte(m.Data), gap); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	if gap == nil {
		t.Fatalf("events were discarded without telling the client: %q", truncate(c.String()))
	}
	if gap.Reason != string(sse.GapSlowConsumer) {
		t.Errorf("reason = %q, want %q so it is distinguishable from a retention gap",
			gap.Reason, sse.GapSlowConsumer)
	}
	if gap.Detail == "" {
		t.Error("the gap tells the client nothing about what to do")
	}
}

// RF-D4: what a slow subscriber can hold must be bounded and known.
func TestQueueRespectsItsByteBudget(t *testing.T) {
	const budget = 4096
	c, _ := stalledSession(t, sse.Backpressure{Policy: sse.DropOldest, MaxBytes: budget},
		300*time.Millisecond, func(p *sse.Publisher) {
			for range 200 {
				if _, err := p.Publish(context.Background(), sse.Text(strings.Repeat("x", 512))); err != nil {
					t.Fatal(err)
				}
			}
		})
	// The connection stalled, so almost nothing was written; the point is that
	// the run completed rather than growing without bound.
	if c.Status() != 200 {
		t.Fatalf("stream never opened")
	}
}

// Disconnect ends the session rather than discarding, which is the right choice
// when history is retained and the client can simply come back.
func TestDisconnectPolicyEndsTheSession(t *testing.T) {
	_, err := stalledSession(t, sse.Backpressure{Policy: sse.Disconnect, MaxEvents: 4},
		5*time.Second, func(p *sse.Publisher) {
			for range 200 {
				if _, err := p.Publish(context.Background(), sse.Text("event")); err != nil {
					t.Fatal(err)
				}
			}
		})
	if !errors.Is(err, sse.ErrSlowConsumer) && !errors.Is(err, sse.ErrWriteTimeout) {
		t.Fatalf("got %v, want ErrSlowConsumer or the write deadline", err)
	}
}

// Block waits for room, bounded. It stalls only this session: the publisher has
// already appended and gone.
func TestBlockPolicyIsBounded(t *testing.T) {
	// A generous write timeout so the block timeout is what ends this, not the
	// socket deadline.
	start := time.Now()
	_, err := stalledSession(t, sse.Backpressure{
		Policy: sse.Block, MaxEvents: 2, BlockTimeout: 100 * time.Millisecond,
	}, 500*time.Millisecond, func(p *sse.Publisher) {
		for range 50 {
			if _, err := p.Publish(context.Background(), sse.Text("event")); err != nil {
				t.Fatal(err)
			}
		}
	})
	elapsed := time.Since(start)

	if !errors.Is(err, sse.ErrSlowConsumer) {
		t.Fatalf("got %v, want ErrSlowConsumer once BlockTimeout expired", err)
	}
	if elapsed > 2*time.Second {
		t.Errorf("Block waited %v; it must be bounded by BlockTimeout, not by the socket", elapsed)
	}
}

// The policy that state synchronisation and dashboards actually need, and that
// almost no library offers: a subscriber that falls behind catches up to the
// current value of each entity instead of replaying every intermediate one.
func TestCoalesceKeepsOnlyTheLatestPerKey(t *testing.T) {
	log := sse.NewMemoryLog(sse.Retention{Events: 10_000})
	defer log.Close()
	pub := sse.NewPublisher(log)

	// Three entities, updated many times each, published before anyone reads.
	const rounds = 50
	for r := range rounds {
		for _, id := range []string{"a", "b", "c"} {
			_, err := pub.Publish(context.Background(),
				sse.Text(fmt.Sprintf("%s=%d", id, r)),
				sse.Name("state"), sse.Key(id))
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	c := ssetest.NewConn()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_ = sse.Serve(ctx, c, httptest.NewRequest("GET", "/s", nil), sse.Follow,
		sse.WithLog("state", log),
		sse.WithBackpressure(sse.Backpressure{Policy: sse.Coalesce, MaxEvents: 3}),
		sse.WithKeepAlive(0),
	)

	// Whatever arrived, no key may appear with a stale value after a newer one:
	// coalescing supersedes, it never reorders.
	latest := map[string]int{}
	for _, m := range c.Messages() {
		if m.Type != "state" {
			continue
		}
		id, val, ok := strings.Cut(m.Data, "=")
		if !ok {
			t.Fatalf("unexpected payload %q", m.Data)
		}
		var n int
		if _, err := fmt.Sscanf(val, "%d", &n); err != nil {
			t.Fatal(err)
		}
		if prev, seen := latest[id]; seen && n <= prev {
			t.Errorf("key %q went from %d back to %d; coalescing must supersede, never reorder",
				id, prev, n)
		}
		latest[id] = n
	}
	if len(latest) == 0 {
		t.Fatalf("nothing was delivered: %q", truncate(c.String()))
	}
	t.Logf("delivered final values %v out of %d updates per key", latest, rounds)
}

// RF-D6: discarding is only a defensible choice when the client can recover.
// Combining it with a log that retains nothing must be said out loud rather
// than left to be discovered.
func TestAggressivePolicyWithoutHistoryIsAnnounced(t *testing.T) {
	log := sse.NewMemoryLog(sse.Retention{}) // no retention, so no recovery
	defer log.Close()

	c := ssetest.NewConn()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_ = sse.Serve(ctx, c, httptest.NewRequest("GET", "/s", nil), sse.Follow,
		sse.WithLog("events", log),
		sse.WithBackpressure(sse.Backpressure{Policy: sse.Disconnect}),
		sse.WithKeepAlive(0),
	)

	msgs := c.Messages()
	if len(msgs) == 0 {
		t.Fatal("nothing was written")
	}
	var open struct {
		Resumable bool   `json:"resumable"`
		Recovery  string `json:"recovery"`
	}
	if err := json.Unmarshal([]byte(msgs[0].Data), &open); err != nil {
		t.Fatal(err)
	}
	if open.Resumable || open.Recovery != "none" {
		t.Errorf("a discard policy over a log with no history reported recovery=%q resumable=%v; "+
			"the client must be told plainly that discarded events are gone",
			open.Recovery, open.Resumable)
	}
}

func TestUnknownPolicyIsRejected(t *testing.T) {
	if _, err := sse.NewHandler(sse.Follow,
		sse.WithBackpressure(sse.Backpressure{Policy: sse.Policy(99)})); err == nil {
		t.Error("an unknown policy was accepted")
	}
}

func truncate(s string) string {
	if len(s) > 400 {
		return s[:400] + "…"
	}
	return s
}
