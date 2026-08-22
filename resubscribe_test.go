package sse_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/imlargo/sse"
	"github.com/imlargo/sse/ssetest"
)

// RF-B5: a client changes what it is subscribed to without reconnecting.
//
// EventSource cannot send anything to the server, so the change has to arrive
// through a side channel — an ordinary request that finds the session by id.
// That is only possible because a session is addressable, which is the piece
// SSE itself does not provide.
func TestResubscribeWithoutReconnecting(t *testing.T) {
	log := sse.NewMemoryLog(sse.Retention{Events: 10_000})
	defer log.Close()
	b := sse.NewBroker("events", log)
	lc := sse.NewLifecycle()

	conn := ssetest.NewConn()
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = sse.Serve(ctx, conn, httptest.NewRequest("GET", "/events", nil),
			func(ctx context.Context, s *sse.Session) error {
				return b.Subscribe(ctx, s, sse.MustFilter("tenant.acme.tickets"))
			}, sse.WithLog("events", b.Log()), sse.WithLifecycle(lc), sse.WithKeepAlive(0))
	}()

	// The session id is in the connection event, so a client already has it.
	var id string
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, s := range lc.NodeSessions() {
			id = s.ID()
		}
		if id != "" {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if id == "" {
		t.Fatal("the session never registered")
	}

	publish := func(topic, body string) {
		t.Helper()
		if _, err := b.Publish(context.Background(), sse.MustTopic(topic),
			sse.Text(body), sse.Name("e")); err != nil {
			t.Fatal(err)
		}
	}
	waitFor := func(want string) bool {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			for _, d := range dataOf(conn, "e") {
				if d == want {
					return true
				}
			}
			time.Sleep(2 * time.Millisecond)
		}
		return false
	}

	publish("tenant.acme.tickets", "before-tickets")
	publish("tenant.acme.builds", "before-builds")
	if !waitFor("before-tickets") {
		t.Fatalf("the original subscription never delivered: %q", truncate(conn.String()))
	}

	// The side channel. This is what an application would expose as, say,
	// POST /subscriptions?session=<id>&topic=...
	s, ok := lc.Session(id)
	if !ok {
		t.Fatal("the session is not addressable by id")
	}
	if err := s.Resubscribe(sse.MustFilter("tenant.acme.builds")); err != nil {
		t.Fatalf("Resubscribe: %v", err)
	}

	publish("tenant.acme.builds", "after-builds")
	publish("tenant.acme.tickets", "after-tickets")

	if !waitFor("after-builds") {
		t.Fatalf("the new subscription never delivered: %q", truncate(conn.String()))
	}

	// The stream did not restart, so nothing was replayed, and what is no
	// longer subscribed no longer arrives.
	var got []string
	for _, d := range dataOf(conn, "e") {
		got = append(got, d)
	}
	for _, d := range got {
		if d == "before-builds" {
			t.Error("resubscribing replayed an event from before the change; the position was not kept")
		}
		if d == "after-tickets" {
			t.Error("an event for the old subscription arrived after the change")
		}
	}
	if len(got) != 2 || got[0] != "before-tickets" || got[1] != "after-builds" {
		t.Errorf("received %v, want exactly [before-tickets after-builds]", got)
	}

	cancel()
	<-done
}

// RF-F3 from the other direction: an application ends one session, and the
// client reconnects by itself with fresh credentials.
func TestStopEndsOneSession(t *testing.T) {
	log := sse.NewMemoryLog(sse.Retention{Events: 100})
	defer log.Close()
	b := sse.NewBroker("events", log)
	lc := sse.NewLifecycle()

	conns := make([]*ssetest.Conn, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := range conns {
		c := ssetest.NewConn()
		conns[i] = c
		go func() {
			_ = sse.Serve(ctx, c, httptest.NewRequest("GET", "/events", nil),
				func(ctx context.Context, s *sse.Session) error { return b.Subscribe(ctx, s) },
				sse.WithLog("events", b.Log()), sse.WithLifecycle(lc), sse.WithKeepAlive(0))
		}()
	}
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()

	deadline := time.Now().Add(3 * time.Second)
	for lc.NodeSessionCount() < 3 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if n := lc.NodeSessionCount(); n != 3 {
		t.Fatalf("%d sessions registered, want 3", n)
	}

	victim := lc.NodeSessions()[0]
	victim.Stop()

	deadline = time.Now().Add(3 * time.Second)
	for lc.NodeSessionCount() > 2 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if n := lc.NodeSessionCount(); n != 2 {
		t.Errorf("%d sessions live after stopping one, want 2", n)
	}

	// And it was told, with a retry, rather than dropped in silence.
	var closed int
	for _, c := range conns {
		msgs := c.Messages()
		if len(msgs) > 0 && msgs[len(msgs)-1].Type == "sse.closing" {
			closed++
		}
	}
	if closed != 1 {
		t.Errorf("%d clients were told the stream was closing, want exactly 1", closed)
	}
	cancel()
}

func TestResubscribeRejectsBadInput(t *testing.T) {
	log := sse.NewMemoryLog(sse.Retention{Events: 10})
	defer log.Close()
	b := sse.NewBroker("events", log)
	lc := sse.NewLifecycle()

	conn := ssetest.NewConn()
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	errs := make(chan error, 2)
	go func() {
		_ = sse.Serve(ctx, conn, httptest.NewRequest("GET", "/events", nil),
			func(ctx context.Context, s *sse.Session) error {
				errs <- s.Resubscribe(sse.Filter{}) // the zero Filter
				return b.Subscribe(ctx, s)
			}, sse.WithLog("events", b.Log()), sse.WithLifecycle(lc), sse.WithKeepAlive(0))
	}()

	select {
	case err := <-errs:
		if err == nil {
			t.Error("the zero Filter was accepted")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Resubscribe never returned")
	}
	cancel()
}

var _ = fmt.Sprintf
