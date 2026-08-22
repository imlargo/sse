package sse_test

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/imlargo/sse"
	"github.com/imlargo/sse/ssetest"
)

// recorder collects every hook so a test can assert on what was observed.
type recorder struct {
	mu sync.Mutex

	opened    []sse.SessionStats
	closed    []sse.SessionStats
	closeErrs []error
	active    []int
	published int
	delivered int
	latencies []time.Duration
	dropped   map[string]int
	gaps      []sse.GapReason
	depths    int
}

func newRecorder() *recorder { return &recorder{dropped: map[string]int{}} }

func (r *recorder) metrics() *sse.Metrics {
	return &sse.Metrics{
		SessionOpened: func(s sse.SessionStats) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.opened = append(r.opened, s)
		},
		SessionClosed: func(s sse.SessionStats, err error) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.closed = append(r.closed, s)
			r.closeErrs = append(r.closeErrs, err)
		},
		NodeSessionsActive: func(n int) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.active = append(r.active, n)
		},
		EventPublished: func(topic string, bytes int) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.published++
		},
		EventDelivered: func(topic string, bytes int, latency time.Duration) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.delivered++
			r.latencies = append(r.latencies, latency)
		},
		EventDropped: func(topic, reason string, count int) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.dropped[reason] += count
		},
		GapDeclared: func(reason sse.GapReason) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.gaps = append(r.gaps, reason)
		},
		QueueDepth: func(id string, events, bytes int) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.depths++
		},
	}
}

func (r *recorder) snapshot(fn func(*recorder)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	fn(r)
}

// RNF-10: the minimum set is actually observed, end to end.
func TestMetricsCoverTheRequiredSet(t *testing.T) {
	rec := newRecorder()

	log := sse.NewMemoryLog(sse.Retention{Events: 1000})
	defer log.Close()
	lc := sse.NewLifecycle()
	b := sse.NewBroker("events", log, sse.WithMetrics(rec.metrics()))

	for i := range 5 {
		if _, err := b.Publish(context.Background(), sse.MustTopic("news"),
			sse.Text(fmt.Sprintf("item-%d", i)), sse.Name("e")); err != nil {
			t.Fatal(err)
		}
	}

	c := ssetest.NewConn()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = sse.Serve(ctx, c, httptest.NewRequest("GET", "/events", nil),
			func(ctx context.Context, s *sse.Session) error { return b.Subscribe(ctx, s) },
			sse.WithLog("events", log), sse.WithMetrics(rec.metrics()),
			sse.WithLifecycle(lc), sse.WithKeepAlive(0))
	}()

	deadline := time.Now().Add(2 * time.Second)
	for len(c.Messages()) < 6 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	<-done
	c.Close()

	rec.snapshot(func(r *recorder) {
		if r.published != 5 {
			t.Errorf("published = %d, want 5", r.published)
		}
		if r.delivered < 5 {
			t.Errorf("delivered = %d, want at least the 5 published", r.delivered)
		}
		if len(r.opened) != 1 {
			t.Errorf("sessions opened = %d, want 1", len(r.opened))
		}
		if len(r.closed) != 1 {
			t.Fatalf("sessions closed = %d, want 1", len(r.closed))
		}
		if r.closed[0].Reason == "" {
			t.Error("a session closed without a reason")
		}
		if len(r.active) == 0 {
			t.Error("the active session count was never reported")
		}
		if r.depths == 0 {
			t.Error("queue occupancy was never reported; it is the leading indicator")
		}
		for _, l := range r.latencies {
			if l <= 0 {
				t.Error("delivery latency must be measured from publication, not from the write")
			}
		}
	})
}

// Reasons must be distinguishable, because "why did sessions end" is the first
// question an operator asks.
func TestSessionCloseReasonsAreDistinct(t *testing.T) {
	tests := []struct {
		name   string
		want   string
		stream sse.StreamFunc
		setup  func(*ssetest.Conn)
	}{
		{
			name:   "completed",
			want:   sse.ReasonCompleted,
			stream: func(ctx context.Context, s *sse.Session) error { return nil },
		},
		{
			name: "client gone",
			want: sse.ReasonClientGone,
			stream: func(ctx context.Context, s *sse.Session) error {
				var err error
				for range 500 {
					if err = s.Send(ctx, sse.Text("x")); err != nil {
						return err
					}
				}
				return nil
			},
			setup: func(c *ssetest.Conn) { c.Fail(errors.New("reset by peer")) },
		},
		{
			name: "panic",
			want: sse.ReasonPanic,
			stream: func(ctx context.Context, s *sse.Session) error {
				panic("boom")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := newRecorder()
			c := ssetest.NewConn()
			if tt.setup != nil {
				tt.setup(c)
			}
			defer c.Close()

			_ = sse.Serve(context.Background(), c,
				httptest.NewRequest("GET", "/s", nil), tt.stream,
				sse.WithMetrics(rec.metrics()), sse.WithKeepAlive(0))

			rec.snapshot(func(r *recorder) {
				if len(r.closed) != 1 {
					t.Fatalf("closed %d sessions, want 1", len(r.closed))
				}
				if r.closed[0].Reason != tt.want {
					t.Errorf("reason = %q, want %q", r.closed[0].Reason, tt.want)
				}
			})
		})
	}
}

// RNF-11: nothing may suggest a cluster-wide scope when the value is local to
// this process. The naming has to carry it, because a dashboard that sums
// per-replica numbers without saying so is describing something that does not
// exist.
func TestMetricNamesDeclareTheirScope(t *testing.T) {
	typ := reflect.TypeOf(sse.Metrics{})

	// Any hook reporting a total across sessions must say "Node".
	totals := map[string]bool{"NodeSessionsActive": true}
	for i := range typ.NumField() {
		name := typ.Field(i).Name
		if strings.Contains(name, "Sessions") && !strings.Contains(name, "Node") {
			t.Errorf("hook %q reports a total but does not say it is node-local", name)
		}
		delete(totals, name)
	}
	if len(totals) > 0 {
		t.Errorf("expected node-scoped hooks are missing: %v", totals)
	}

	// The same rule on the lifecycle accessor.
	lt := reflect.TypeOf(&sse.Lifecycle{})
	found := false
	for i := range lt.NumMethod() {
		if lt.Method(i).Name == "NodeSessionCount" {
			found = true
		}
		if lt.Method(i).Name == "SessionCount" {
			t.Error("Lifecycle.SessionCount reads as a global total; it is per process")
		}
	}
	if !found {
		t.Error("Lifecycle has no node-scoped session count")
	}
}

// RNF-4: a server that observes nothing must pay nothing.
func TestUnsetHooksCostNothing(t *testing.T) {
	log := sse.NewMemoryLog(sse.Retention{Events: 100})
	defer log.Close()

	// A Metrics with every hook nil, and no Metrics at all, must both work.
	for _, m := range []*sse.Metrics{nil, {}} {
		pub, err := sse.NewPublisherWith(log, sse.WithMetrics(m))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pub.Publish(context.Background(), sse.Text("x")); err != nil {
			t.Fatalf("publishing with metrics %v: %v", m, err)
		}
	}

	pub := sse.NewPublisher(log)
	allocs := testing.AllocsPerRun(50, func() {
		if _, err := pub.Publish(context.Background(), sse.Raw([]byte("payload"))); err != nil {
			t.Fatal(err)
		}
	})
	t.Logf("publish allocates %.0f times with no metrics configured", allocs)
}

// Drops are counted by reason, so "we are losing events" and "why" are the same
// dashboard (RF-D5).
func TestDropsAreCountedByReason(t *testing.T) {
	rec := newRecorder()

	log := sse.NewMemoryLog(sse.Retention{Events: 10_000})
	defer log.Close()
	b := sse.NewBroker("events", log, sse.WithMetrics(rec.metrics()))
	for i := range 200 {
		if _, err := b.Publish(context.Background(), sse.MustTopic("news"),
			sse.Text(fmt.Sprintf("e%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	c := ssetest.NewConn()
	c.Throttle(5 * time.Millisecond)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	_ = sse.Serve(ctx, c, httptest.NewRequest("GET", "/events", nil),
		func(ctx context.Context, s *sse.Session) error { return b.Subscribe(ctx, s) },
		sse.WithLog("events", log), sse.WithMetrics(rec.metrics()),
		sse.WithBackpressure(sse.Backpressure{Policy: sse.DropOldest, MaxEvents: 4}),
		sse.WithKeepAlive(0), sse.WithWriteTimeout(30*time.Second))

	rec.snapshot(func(r *recorder) {
		if r.dropped[string(sse.GapSlowConsumer)] == 0 {
			t.Errorf("events were discarded without being counted: %v", r.dropped)
		}
		if len(r.gaps) == 0 {
			t.Error("no gap was recorded for the discarded events")
		}
	})
}
