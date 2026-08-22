package sse_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/imlargo/sse"
	"github.com/imlargo/sse/ssetest"
)

// followed runs one session over a log and returns a function that publishes
// and waits for the session to have consumed the event.
func followed(tb testing.TB, filter string) (*sse.Broker, func()) {
	tb.Helper()

	log := sse.NewMemoryLog(sse.Retention{Events: 1 << 16, For: time.Hour})
	b := sse.NewBroker("events", log)

	conn := ssetest.NewConn()
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = sse.Serve(ctx, conn, httptest.NewRequest("GET", "/events", nil),
			func(ctx context.Context, s *sse.Session) error {
				return b.Subscribe(ctx, s, sse.MustFilter(filter))
			},
			sse.WithLog("events", log), sse.WithKeepAlive(0),
			sse.WithBackpressure(sse.Backpressure{MaxEvents: 1 << 14, MaxBytes: 1 << 24}))
	}()

	// Let the session reach its blocked state.
	deadline := time.Now().Add(2 * time.Second)
	for len(conn.Messages()) < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	return b, func() {
		cancel()
		<-done
		conn.Close()
		log.Close()
	}
}

// The cost a subscriber pays for an event it does not want.
//
// A narrow filter means most events are skipped, and until this was measured
// each one rebuilt and base64-encoded a resumption cursor that nobody would
// ever read — the position only has to reach the client at keep-alive
// granularity. Delivering costs a cursor because the id goes on the wire;
// skipping should cost an integer.
func BenchmarkFollowSkippedEvent(b *testing.B) {
	broker, stop := followed(b, "wanted.>")
	defer stop()

	topic := sse.MustTopic("unwanted.event")
	ctx := context.Background()
	payload := sse.Raw([]byte("payload"))

	b.ReportAllocs()
	for b.Loop() {
		if _, err := broker.Publish(ctx, topic, payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFollowDeliveredEvent(b *testing.B) {
	broker, stop := followed(b, "wanted.>")
	defer stop()

	topic := sse.MustTopic("wanted.event")
	ctx := context.Background()
	payload := sse.Raw([]byte("payload"))

	b.ReportAllocs()
	for b.Loop() {
		if _, err := broker.Publish(ctx, topic, payload); err != nil {
			b.Fatal(err)
		}
	}
}

// The budget, asserted rather than merely reported.
//
// Skipping an event must not allocate at all beyond what publishing itself
// costs. If this regresses, a subscriber with a selective filter is paying for
// events it never sees — which is the exact cost the topic-prefix wake-up
// buckets exist to avoid, given away again one layer up.
func TestSkippedEventCostsNoMoreThanPublishing(t *testing.T) {
	bare := sse.NewMemoryLog(sse.Retention{Events: 1 << 16})
	defer bare.Close()
	barePub := sse.NewBroker("events", bare)
	ctx := context.Background()
	topic := sse.MustTopic("unwanted.event")
	payload := sse.Raw([]byte("payload"))

	// What a publish costs with nobody listening at all.
	baseline := testing.AllocsPerRun(200, func() {
		if _, err := barePub.Publish(ctx, topic, payload); err != nil {
			t.Fatal(err)
		}
	})

	broker, stop := followed(t, "wanted.>")
	defer stop()

	withSkipping := testing.AllocsPerRun(200, func() {
		if _, err := broker.Publish(ctx, topic, payload); err != nil {
			t.Fatal(err)
		}
	})

	t.Logf("publish alone: %.1f allocs; with a subscriber skipping it: %.1f",
		baseline, withSkipping)

	if withSkipping > baseline+1 {
		t.Errorf("a skipped event costs %.1f allocations against a %.1f baseline: "+
			"the subscriber is doing work for an event it never receives",
			withSkipping, baseline)
	}
}
