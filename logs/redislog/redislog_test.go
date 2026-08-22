package redislog_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/imlargo/sse"
	"github.com/imlargo/sse/logs/redislog"
	"github.com/imlargo/sse/ssetest"
	"github.com/imlargo/sse/wire"
	goredis "github.com/redis/go-redis/v9"
)

// addr is where the integration tests look for Redis. They skip rather than
// fail when it is absent, so the suite still runs on a machine without it.
func addr() string {
	if a := os.Getenv("SSE_REDIS_ADDR"); a != "" {
		return a
	}
	return "127.0.0.1:6399"
}

func client(t *testing.T) *goredis.Client {
	t.Helper()
	c := goredis.NewClient(&goredis.Options{Addr: addr()})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no Redis at %s: %v", addr(), err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// freshKey gives each test its own stream, and removes it afterwards.
func freshKey(t *testing.T, c *goredis.Client) string {
	t.Helper()
	key := fmt.Sprintf("ssetest:%s:%d", strings.ReplaceAll(t.Name(), "/", "_"), time.Now().UnixNano())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = c.Del(ctx, key, key+":epoch").Err()
	})
	return key
}

func TestAppendAndRead(t *testing.T) {
	c := client(t)
	ctx := context.Background()
	l, err := redislog.New(ctx, c, freshKey(t, c), sse.Retention{Events: 1000})
	if err != nil {
		t.Fatal(err)
	}

	for i := range 5 {
		if _, err := l.Append(ctx, sse.Frame{
			Body:  []byte(fmt.Sprintf("data: e%d\n\n", i)),
			Topic: "news",
		}); err != nil {
			t.Fatal(err)
		}
	}

	r, err := l.Read(ctx, 0, sse.ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.Gap() != nil {
		t.Fatalf("unexpected gap: %+v", r.Gap())
	}

	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var last sse.Offset
	for i := range 5 {
		e, err := r.Next(readCtx)
		if err != nil {
			t.Fatalf("Next %d: %v", i, err)
		}
		if e.Offset <= last {
			t.Fatalf("offset %d did not increase past %d", e.Offset, last)
		}
		last = e.Offset
		if e.Frame.Topic != "news" {
			t.Errorf("topic = %q, want news", e.Frame.Topic)
		}
		if !strings.Contains(string(e.Frame.Body), fmt.Sprintf("e%d", i)) {
			t.Errorf("entry %d body = %q", i, e.Frame.Body)
		}
	}
}

// Offsets must round-trip through a Redis id, because that packing is what lets
// a cursor be a plain integer across nodes.
func TestOffsetsPreserveOrder(t *testing.T) {
	c := client(t)
	ctx := context.Background()
	l, err := redislog.New(ctx, c, freshKey(t, c), sse.Retention{Events: 1000})
	if err != nil {
		t.Fatal(err)
	}

	var prev sse.Offset
	for range 200 {
		off, err := l.Append(ctx, sse.Frame{Body: []byte("data: x\n\n")})
		if err != nil {
			t.Fatal(err)
		}
		if off <= prev {
			t.Fatalf("offset %d did not increase past %d; ordering is not preserved by the packing", off, prev)
		}
		prev = off
	}
}

// A follower must see entries appended after it started waiting, without a
// separate catch-up mode to hand over from.
func TestReaderFollowsTheTail(t *testing.T) {
	c := client(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	l, err := redislog.New(ctx, c, freshKey(t, c), sse.Retention{Events: 1000},
		redislog.WithBlockTimeout(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}

	r, err := l.Read(ctx, 0, sse.ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	go func() {
		time.Sleep(100 * time.Millisecond)
		for i := range 10 {
			if _, err := l.Append(ctx, sse.Frame{Body: []byte(fmt.Sprintf("data: live-%d\n\n", i))}); err != nil {
				return
			}
		}
	}()

	for i := range 10 {
		e, err := r.Next(ctx)
		if err != nil {
			t.Fatalf("Next %d: %v", i, err)
		}
		if !strings.Contains(string(e.Frame.Body), fmt.Sprintf("live-%d", i)) {
			t.Errorf("entry %d = %q, out of order", i, e.Frame.Body)
		}
	}
}

// RF-C4 across the seam: a position Redis no longer holds is declared, not
// quietly started later.
func TestGapAfterTrimming(t *testing.T) {
	c := client(t)
	ctx := context.Background()
	key := freshKey(t, c)

	l, err := redislog.New(ctx, c, key, sse.Retention{Events: 5})
	if err != nil {
		t.Fatal(err)
	}
	first, err := l.Append(ctx, sse.Frame{Body: []byte("data: first\n\n")})
	if err != nil {
		t.Fatal(err)
	}
	for range 100 {
		if _, err := l.Append(ctx, sse.Frame{Body: []byte("data: x\n\n")}); err != nil {
			t.Fatal(err)
		}
	}
	// MAXLEN is approximate, so force the trim to be exact for the assertion.
	if err := c.XTrimMaxLen(ctx, key, 5).Err(); err != nil {
		t.Fatal(err)
	}

	r, err := l.Read(ctx, first, sse.ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	gap := r.Gap()
	if gap == nil {
		t.Fatal("a position behind the retention window was not declared as a gap")
	}
	if gap.Reason != sse.GapRetention {
		t.Errorf("reason = %q, want %q", gap.Reason, sse.GapRetention)
	}
}

// Every node that starts against the same stream must agree on the epoch,
// otherwise a client reconnecting to a different replica would be told its
// cursor belongs to another generation.
func TestEpochIsSharedAcrossNodes(t *testing.T) {
	c := client(t)
	ctx := context.Background()
	key := freshKey(t, c)

	a, err := redislog.New(ctx, c, key, sse.Retention{Events: 100})
	if err != nil {
		t.Fatal(err)
	}
	b, err := redislog.New(ctx, goredis.NewClient(&goredis.Options{Addr: addr()}), key,
		sse.Retention{Events: 100})
	if err != nil {
		t.Fatal(err)
	}

	ia, _ := a.Info(ctx)
	ib, _ := b.Info(ctx)
	if ia.Epoch != ib.Epoch {
		t.Fatalf("two nodes disagree on the epoch (%d vs %d); a client reconnecting to "+
			"the other one would be told its cursor is from an earlier generation",
			ia.Epoch, ib.Epoch)
	}
	if ia.Epoch == 0 {
		t.Error("epoch must never be zero, which reads as absent")
	}
}

// The one that matters: a client served by one node reconnects to a different
// node and resumes exactly where it stopped.
//
// This is what "no sticky sessions" means. The cursor names a log and an
// offset, and both nodes read the same log, so nothing about the first node is
// needed to serve the second connection.
func TestResumeAcrossNodes(t *testing.T) {
	c := client(t)
	ctx := context.Background()
	key := freshKey(t, c)

	// Two nodes: separate clients, separate logs, separate brokers, one stream.
	nodeA, err := redislog.New(ctx, goredis.NewClient(&goredis.Options{Addr: addr()}), key,
		sse.Retention{Events: 10_000}, redislog.WithBlockTimeout(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	nodeB, err := redislog.New(ctx, goredis.NewClient(&goredis.Options{Addr: addr()}), key,
		sse.Retention{Events: 10_000}, redislog.WithBlockTimeout(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}

	brokerA := sse.NewBroker("events", nodeA)
	brokerB := sse.NewBroker("events", nodeB)

	for i := range 5 {
		if _, err := brokerA.Publish(ctx, sse.MustTopic("job.demo"),
			sse.Text(fmt.Sprintf("step-%d", i)), sse.Name("step")); err != nil {
			t.Fatal(err)
		}
	}

	// First connection, on node A.
	first := subscribe(t, brokerA, "", 6)
	if got := collect(first, "step"); len(got) != 5 {
		t.Fatalf("node A delivered %v, want five steps", got)
	}
	cursor := lastID(t, first)
	if cursor == "" {
		t.Fatal("node A never sent a resumption cursor")
	}

	// Work continues while the client is away, published through node B.
	for i := 5; i < 9; i++ {
		if _, err := brokerB.Publish(ctx, sse.MustTopic("job.demo"),
			sse.Text(fmt.Sprintf("step-%d", i)), sse.Name("step")); err != nil {
			t.Fatal(err)
		}
	}

	// Reconnection lands on node B, which has never seen this client.
	second := subscribeWithCursor(t, brokerB, cursor, 5)
	got := collect(second, "step")

	for _, m := range second.Messages() {
		if m.Type == "sse.gap" {
			t.Fatalf("node B declared a gap for a cursor well inside retention: %s", m.Data)
		}
	}
	want := []string{"step-5", "step-6", "step-7", "step-8"}
	if len(got) != len(want) {
		t.Fatalf("node B delivered %v, want exactly %v — no duplicates, nothing lost", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("event %d = %q, want %q", i, got[i], want[i])
		}
	}
}

// Topic filtering works the same over Redis, because it happens above the seam.
func TestTopicFilteringOverRedis(t *testing.T) {
	c := client(t)
	ctx := context.Background()
	l, err := redislog.New(ctx, c, freshKey(t, c), sse.Retention{Events: 1000},
		redislog.WithBlockTimeout(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	b := sse.NewBroker("events", l)

	for _, tp := range []string{"tenant.acme.tickets", "tenant.globex.tickets", "tenant.acme.builds"} {
		if _, err := b.Publish(ctx, sse.MustTopic(tp), sse.Text(tp), sse.Name("e")); err != nil {
			t.Fatal(err)
		}
	}

	conn := subscribe(t, b, "tenant.acme.>", 3)
	for _, d := range collect(conn, "e") {
		if !strings.HasPrefix(d, "tenant.acme.") {
			t.Errorf("received another tenant's event: %q", d)
		}
	}
	if n := len(collect(conn, "e")); n != 2 {
		t.Errorf("received %d events, want the 2 for this tenant", n)
	}
}

// --- helpers ---------------------------------------------------------------

func subscribe(t *testing.T, b *sse.Broker, filter string, n int) *ssetest.Conn {
	return subscribeWith(t, b, filter, "", n)
}

func subscribeWithCursor(t *testing.T, b *sse.Broker, cursor string, n int) *ssetest.Conn {
	return subscribeWith(t, b, "", cursor, n)
}

func subscribeWith(t *testing.T, b *sse.Broker, filter, cursor string, n int) *ssetest.Conn {
	t.Helper()
	conn := ssetest.NewConn()

	r := httptest.NewRequest("GET", "/events", nil)
	if cursor != "" {
		r.Header.Set("Last-Event-ID", cursor)
	}
	var filters []sse.Filter
	if filter != "" {
		filters = append(filters, sse.MustFilter(filter))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = sse.Serve(ctx, conn, r, func(ctx context.Context, s *sse.Session) error {
			return b.Subscribe(ctx, s, filters...)
		}, sse.WithLog("events", b.Log()), sse.WithKeepAlive(0))
	}()

	deadline := time.Now().Add(8 * time.Second)
	for len(conn.Messages()) < n && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	conn.Close()
	return conn
}

func collect(c *ssetest.Conn, typ string) []string {
	var out []string
	for _, m := range c.Messages() {
		if m.Type == typ {
			out = append(out, m.Data)
		}
	}
	return out
}

func lastID(t *testing.T, c *ssetest.Conn) string {
	t.Helper()
	d := wire.NewDecoder(strings.NewReader(c.String()))
	for {
		if _, err := d.Next(); err != nil {
			break
		}
	}
	return d.LastEventID()
}
