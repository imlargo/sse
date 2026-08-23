package redislog_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imlargo/sse"
	"github.com/imlargo/sse/logs/redislog"
	goredis "github.com/redis/go-redis/v9"
)

// A node holds one long-lived Redis connection, whatever the audience.
//
// This is the property the adapter is built around, and it was not true before:
// a blocking XREAD per subscriber meant a Redis connection per subscriber, so
// past the client's pool size — eighty by default — every further subscriber
// waited for a connection that never freed and the node stopped delivering
// entirely. At 250 subscribers the pool sat at 80 blocked connections with
// nothing getting through.
func TestOneRedisConnectionPerNode(t *testing.T) {
	c := client(t)
	ctx := context.Background()

	l, err := redislog.New(ctx, c, freshKey(t, c), sse.Retention{Events: 10_000},
		redislog.WithBlockTimeout(500*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	const subs = 400
	var delivered atomic.Int64
	var wg sync.WaitGroup
	for range subs {
		r, err := l.Read(readCtx, 0, sse.ReadOptions{})
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer r.Close()
			for {
				if _, err := r.Next(readCtx); err != nil {
					return
				}
				delivered.Add(1)
			}
		}()
	}
	time.Sleep(500 * time.Millisecond)

	blocked, err := blockedClients(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%d subscribers: %d blocked Redis clients, pool holds %d connections",
		subs, blocked, c.PoolStats().TotalConns)

	// One tailer. Allowing a couple for the test's own client and timing.
	if blocked > 3 {
		t.Errorf("%d Redis clients are blocked with %d subscribers: the adapter is "+
			"holding a connection per subscriber again", blocked, subs)
	}

	// And everyone must actually receive, promptly.
	start := time.Now()
	if _, err := l.Append(ctx, sse.Frame{Body: []byte("data: x\n\n")}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for delivered.Load() < int64(subs) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := delivered.Load(); got < int64(subs) {
		t.Fatalf("only %d of %d subscribers received the event", got, subs)
	}
	t.Logf("one event reached all %d subscribers in %v", subs, time.Since(start).Round(time.Millisecond))
}

// A subscriber whose position predates this node's local window is caught up
// from Redis and then joins the tail — with nothing missing and nothing twice
// at the join.
func TestCatchUpJoinsTheLocalTailExactly(t *testing.T) {
	c := client(t)
	ctx := context.Background()

	// A deliberately tiny local window, so most of the history is only in Redis.
	l, err := redislog.New(ctx, c, freshKey(t, c), sse.Retention{Events: 100_000},
		redislog.WithLocalWindow(sse.Retention{Events: 16}),
		redislog.WithBlockTimeout(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	const total = 600
	for i := range total {
		if _, err := l.Append(ctx, sse.Frame{Body: []byte(fmt.Sprintf("data: %d\n\n", i))}); err != nil {
			t.Fatal(err)
		}
	}
	// Let the tailer mirror the tail of it.
	time.Sleep(300 * time.Millisecond)

	readCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	r, err := l.Read(readCtx, 0, sse.ReadOptions{}) // from the very beginning
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	var last sse.Offset
	for i := range total {
		e, err := r.Next(readCtx)
		if err != nil {
			t.Fatalf("entry %d of %d: %v (the catch-up did not reach the local window)", i, total, err)
		}
		if e.Offset <= last {
			t.Fatalf("entry %d has offset %d, not past %d: the join duplicated or reordered",
				i, e.Offset, last)
		}
		last = e.Offset
		want := fmt.Sprintf("data: %d\n\n", i)
		if string(e.Frame.Body) != want {
			t.Fatalf("entry %d is %q, want %q: the join lost or repeated an event",
				i, e.Frame.Body, want)
		}
	}

	// And it continues live from there.
	if _, err := l.Append(ctx, sse.Frame{Body: []byte("data: live\n\n")}); err != nil {
		t.Fatal(err)
	}
	e, err := r.Next(readCtx)
	if err != nil {
		t.Fatalf("did not continue into live delivery: %v", err)
	}
	if string(e.Frame.Body) != "data: live\n\n" {
		t.Errorf("after catching up, got %q", e.Frame.Body)
	}
}

// blockedClients asks Redis how many of its clients are parked in a blocking
// command, which is the honest way to count what the adapter holds.
func blockedClients(ctx context.Context, c *goredis.Client) (int, error) {
	info, err := c.Info(ctx, "clients").Result()
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(info, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "blocked_clients:"); ok {
			return strconv.Atoi(v)
		}
	}
	return 0, fmt.Errorf("blocked_clients not reported")
}
