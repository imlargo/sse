package sse_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/imlargo/sse"
	"github.com/imlargo/sse/ssetest"
)

// RF-E1: open connections are drained in an orderly way and clients are told
// before the close.
func TestGracefulShutdownDrainsAndNotifies(t *testing.T) {
	lc := sse.NewLifecycle()
	const sessions = 8

	conns := make([]*ssetest.Conn, sessions)
	ready := make(chan struct{}, sessions)
	finished := make(chan error, sessions)

	for i := range sessions {
		c := ssetest.NewConn()
		conns[i] = c
		go func() {
			finished <- sse.Serve(context.Background(), c,
				httptest.NewRequest("GET", "/stream", nil),
				func(ctx context.Context, s *sse.Session) error {
					if err := s.Send(ctx, sse.Text("hello")); err != nil {
						return err
					}
					ready <- struct{}{}
					<-s.Done() // stay open until the server drains us
					return nil
				},
				sse.WithLifecycle(lc), sse.WithKeepAlive(0))
		}()
	}
	for range sessions {
		<-ready
	}

	if got := lc.NodeSessionCount(); got != sessions {
		t.Fatalf("NodeSessionCount = %d, want %d", got, sessions)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := lc.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	for range sessions {
		<-finished
	}

	// Every client was told, and every one of them still received the event
	// that was already queued: draining is orderly, not a hang-up.
	var delays []int
	for i, c := range conns {
		msgs := c.Messages()
		if len(msgs) < 3 {
			t.Fatalf("session %d saw %d messages, want open + hello + closing: %q", i, len(msgs), c.String())
		}
		if msgs[1].Data != "hello" {
			t.Errorf("session %d lost its queued event: %+v", i, msgs)
		}
		last := msgs[len(msgs)-1]
		if last.Type != "sse.closing" {
			t.Errorf("session %d ended with %q, want sse.closing", i, last.Type)
		}
		delays = append(delays, retryOf(t, c.String()))
	}

	// RF-E2: if every client waited exactly the same delay they would all come
	// back at the same instant. The retry field is the only lever the protocol
	// gives, so the delays must actually differ.
	distinct := map[int]bool{}
	for _, d := range delays {
		distinct[d] = true
	}
	if len(distinct) < sessions/2 {
		t.Errorf("only %d distinct retry delays across %d sessions: %v", len(distinct), sessions, delays)
	}
}

// A draining node must never answer a reconnection with a non-200 status.
//
// A client that receives one stops reconnecting permanently, so 503 during a
// rolling deploy does not move the client to a healthy replica: it kills it.
func TestDrainingNodeStillAnswers200(t *testing.T) {
	lc := sse.NewLifecycle()
	if err := lc.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	c := ssetest.NewConn()
	err := sse.Serve(context.Background(), c,
		httptest.NewRequest("GET", "/stream", nil),
		func(ctx context.Context, s *sse.Session) error {
			t.Error("the stream function ran on a draining node")
			return nil
		},
		sse.WithLifecycle(lc))

	if !errors.Is(err, sse.ErrShuttingDown) {
		t.Errorf("Serve returned %v, want ErrShuttingDown", err)
	}
	if c.Status() != 200 {
		t.Fatalf("status = %d, want 200; a non-200 makes the client give up for good", c.Status())
	}

	msgs := c.Messages()
	if len(msgs) == 0 || msgs[len(msgs)-1].Type != "sse.closing" {
		t.Fatalf("draining node did not tell the client to come back: %q", c.String())
	}
	if retryOf(t, c.String()) <= 0 {
		t.Error("no retry delay offered, so the client would use its own default and stampede")
	}
}

// retryOf extracts the retry field from a wire dump.
func retryOf(t *testing.T, s string) int {
	t.Helper()
	best := 0
	for line := range strings.SplitSeq(s, "\n") {
		if v, ok := strings.CutPrefix(line, "retry: "); ok {
			n, err := strconv.Atoi(v)
			if err != nil {
				t.Fatalf("unparsable retry %q", v)
			}
			best = n
		}
	}
	return best
}
