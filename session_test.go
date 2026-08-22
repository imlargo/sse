package sse_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/imlargo/sse"
	"github.com/imlargo/sse/ssetest"
)

// serveOn runs fn over an in-memory transport and returns when the stream ends.
func serveOn(t *testing.T, c *ssetest.Conn, fn sse.StreamFunc, opts ...sse.Option) error {
	t.Helper()
	r := httptest.NewRequest("GET", "/stream", nil)
	return sse.Serve(context.Background(), c, r, fn, opts...)
}

func TestOpenEventDeclaresCapabilities(t *testing.T) {
	c := ssetest.NewConn()
	err := serveOn(t, c, func(ctx context.Context, s *sse.Session) error { return nil })
	if err != nil {
		t.Fatalf("Serve: %v", err)
	}

	msgs := c.Messages()
	if len(msgs) == 0 {
		t.Fatalf("no messages written; wire was %q", c.String())
	}
	if msgs[0].Type != "sse.open" {
		t.Fatalf("first event is %q, want sse.open", msgs[0].Type)
	}

	// RF-E3: the client must be told what it was actually offered. Without
	// history there is no resumption, and saying so is the point.
	var open struct {
		SessionID string `json:"sessionId"`
		Resumable bool   `json:"resumable"`
		Delivery  string `json:"delivery"`
		Recovery  string `json:"recovery"`
	}
	if err := json.Unmarshal([]byte(msgs[0].Data), &open); err != nil {
		t.Fatalf("open payload is not JSON: %v (%q)", err, msgs[0].Data)
	}
	if open.SessionID == "" {
		t.Error("open event carries no session id")
	}
	if open.Resumable {
		t.Error("resumable must be false with no history configured")
	}
	if open.Delivery != "at-most-once" || open.Recovery != "none" {
		t.Errorf("delivery=%q recovery=%q, want at-most-once/none", open.Delivery, open.Recovery)
	}
	if c.Status() != 200 {
		t.Errorf("status = %d, want 200", c.Status())
	}
}

func TestSendShapes(t *testing.T) {
	type ticket struct {
		ID    int    `json:"id"`
		Title string `json:"title"`
	}

	c := ssetest.NewConn()
	err := serveOn(t, c, func(ctx context.Context, s *sse.Session) error {
		if err := s.Send(ctx, ticket{1, "hello"}); err != nil {
			return err
		}
		if err := s.Send(ctx, sse.Text("a token"), sse.Name("token")); err != nil {
			return err
		}
		if err := s.Send(ctx, sse.Raw([]byte(`<li>row</li>`)), sse.Name("fragment")); err != nil {
			return err
		}
		return s.Send(ctx, sse.From(strings.NewReader("from a reader")))
	})
	if err != nil {
		t.Fatalf("Serve: %v", err)
	}

	msgs := c.Messages()[1:] // skip sse.open
	want := []struct{ typ, data string }{
		{"message", `{"id":1,"title":"hello"}`},
		{"token", "a token"},
		{"fragment", "<li>row</li>"},
		{"message", "from a reader"},
	}
	if len(msgs) != len(want) {
		t.Fatalf("got %d messages, want %d: %+v", len(msgs), len(want), msgs)
	}
	for i, w := range want {
		if msgs[i].Type != w.typ || msgs[i].Data != w.data {
			t.Errorf("message %d = {%q, %q}, want {%q, %q}",
				i, msgs[i].Type, msgs[i].Data, w.typ, w.data)
		}
	}
}

// RF-E4: application events cannot collide with the library's own.
func TestReservedNamespaceIsRefused(t *testing.T) {
	c := ssetest.NewConn()
	var sendErr error
	_ = serveOn(t, c, func(ctx context.Context, s *sse.Session) error {
		sendErr = s.Send(ctx, sse.Text("x"), sse.Name("sse.open"))
		return nil
	})
	if !errors.Is(sendErr, sse.ErrReservedName) {
		t.Fatalf("got %v, want ErrReservedName", sendErr)
	}
}

func TestEventSizeLimit(t *testing.T) {
	c := ssetest.NewConn()
	var sendErr error
	_ = serveOn(t, c, func(ctx context.Context, s *sse.Session) error {
		sendErr = s.Send(ctx, sse.Text(strings.Repeat("x", 2048)))
		return nil
	}, sse.WithMaxEventSize(1024))

	if !errors.Is(sendErr, sse.ErrEventTooLarge) {
		t.Fatalf("got %v, want ErrEventTooLarge", sendErr)
	}
}

// RF-E5: a panic in application code ends one session, not the process.
func TestPanicIsContained(t *testing.T) {
	c := ssetest.NewConn()
	err := serveOn(t, c, func(ctx context.Context, s *sse.Session) error {
		panic("boom")
	})

	var pe *sse.PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("got %v, want a *PanicError", err)
	}
	if pe.Value != "boom" {
		t.Errorf("panic value = %v, want boom", pe.Value)
	}
	if len(pe.Stack) == 0 {
		t.Error("PanicError carries no stack")
	}
	// The stream still opened and closed cleanly rather than being abandoned.
	if c.Status() != 200 {
		t.Errorf("status = %d, want 200", c.Status())
	}
}

// RF-A5 and T-02: the keep-alive is also the liveness probe, so it must fire on
// a silent stream. Run in a synctest bubble so this is instant and exact
// instead of a two-minute sleep (RP-3).
func TestKeepAliveCadence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := ssetest.NewConn()
		release := make(chan struct{})

		done := make(chan error, 1)
		go func() {
			done <- serveOn(t, c, func(ctx context.Context, s *sse.Session) error {
				<-release
				return nil
			}, sse.WithKeepAlive(15*time.Second))
		}()

		// Let the stream open, then stay silent for three keep-alive periods.
		synctest.Wait()
		time.Sleep(46 * time.Second)
		synctest.Wait()

		if got := len(c.Comments()); got != 3 {
			t.Errorf("got %d keep-alive comments after 46s at 15s, want 3 (wire: %q)",
				got, c.String())
		}

		close(release)
		if err := <-done; err != nil {
			t.Fatalf("Serve: %v", err)
		}
	})
}

// An event resets the keep-alive timer: the guarantee is "no silence longer
// than the interval", not "a comment every interval regardless of traffic".
func TestKeepAliveResetsOnTraffic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := ssetest.NewConn()
		release := make(chan struct{})

		done := make(chan error, 1)
		go func() {
			done <- serveOn(t, c, func(ctx context.Context, s *sse.Session) error {
				for range 4 {
					if err := s.Send(ctx, sse.Text("tick")); err != nil {
						return err
					}
					time.Sleep(10 * time.Second)
				}
				<-release
				return nil
			}, sse.WithKeepAlive(15*time.Second))
		}()

		synctest.Wait()
		time.Sleep(41 * time.Second)
		synctest.Wait()

		if got := len(c.Comments()); got != 0 {
			t.Errorf("got %d keep-alives on a stream sending every 10s at a 15s interval, want 0", got)
		}

		close(release)
		if err := <-done; err != nil {
			t.Fatalf("Serve: %v", err)
		}
	})
}

// RF-A8: a client that stops reading but never closes must not pin resources.
// This is the case the request context does not save you from — the write
// simply blocks.
func TestStalledClientHitsWriteDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := ssetest.NewConn()
		defer c.Close()

		done := make(chan error, 1)
		go func() {
			done <- serveOn(t, c, func(ctx context.Context, s *sse.Session) error {
				for {
					if err := s.Send(ctx, sse.Text("x")); err != nil {
						return err
					}
					time.Sleep(time.Second)
				}
			}, sse.WithWriteTimeout(5*time.Second), sse.WithKeepAlive(time.Minute))
		}()

		synctest.Wait()
		c.Stall()

		select {
		case err := <-done:
			if !errors.Is(err, sse.ErrWriteTimeout) {
				t.Fatalf("got %v, want ErrWriteTimeout", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("a stalled client did not trip the write deadline")
		}
	})
}

// A failing write is the primary disconnection signal, in every environment.
func TestWriteFailureEndsSession(t *testing.T) {
	c := ssetest.NewConn()
	err := serveOn(t, c, func(ctx context.Context, s *sse.Session) error {
		c.Fail(errors.New("connection reset by peer"))
		for range 100 {
			if err := s.Send(ctx, sse.Text("x")); err != nil {
				return err
			}
		}
		return nil
	})
	if !errors.Is(err, sse.ErrClientGone) {
		t.Fatalf("got %v, want ErrClientGone", err)
	}
}

// Send reports acceptance, never delivery (RNF-13). Once the writer is gone,
// Send must say so rather than silently succeed.
func TestSendAfterWriterFailureReports(t *testing.T) {
	c := ssetest.NewConn()
	_ = serveOn(t, c, func(ctx context.Context, s *sse.Session) error {
		c.Fail(errors.New("gone"))
		var last error
		for range 200 {
			if last = s.Send(ctx, sse.Text("x")); last != nil {
				break
			}
		}
		if last == nil {
			t.Error("Send kept succeeding after the connection failed")
		}
		return nil
	})
}

func TestStreamIsFlushed(t *testing.T) {
	c := ssetest.NewConn()
	_ = serveOn(t, c, func(ctx context.Context, s *sse.Session) error {
		return s.Send(ctx, sse.Text("x"))
	})
	// A stream that writes without flushing is not a stream: the bytes sit in
	// a buffer and the client sees nothing.
	if c.Flushes() < 2 {
		t.Errorf("flushed %d times for 2 events, want at least 2", c.Flushes())
	}
}
