package sse_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/imlargo/sse"
)

// dialAndAbort opens a raw connection, sends req, reads until the stream is
// clearly running, and then rips the socket away without a close handshake.
// That is what a browser tab being killed looks like.
func dialAndAbort(t *testing.T, addr, req string) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatalf("write request: %v", err)
	}

	br := bufio.NewReader(conn)
	deadline := time.Now().Add(5 * time.Second)
	_ = conn.SetReadDeadline(deadline)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("reading the stream: %v", err)
		}
		if strings.HasPrefix(line, "event: sse.open") || strings.HasPrefix(line, "data:") {
			break
		}
	}

	// SetLinger(0) makes Close send RST instead of FIN, so this is an abrupt
	// disappearance rather than a polite shutdown.
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetLinger(0)
	}
	_ = conn.Close()
}

// RF-A7 scenario 1: a plain GET whose client vanishes.
func TestDisconnectGET(t *testing.T) {
	ended := make(chan error, 1)

	srv := httptest.NewServer(sse.Handler(func(ctx context.Context, s *sse.Session) error {
		var err error
		for err == nil {
			err = s.Send(ctx, sse.Text("tick"))
			time.Sleep(5 * time.Millisecond)
		}
		ended <- err
		return err
	}))
	defer srv.Close()

	dialAndAbort(t, srv.Listener.Addr().String(),
		"GET /stream HTTP/1.1\r\nHost: x\r\nAccept: text/event-stream\r\n\r\n")

	select {
	case err := <-ended:
		if !errors.Is(err, sse.ErrClientGone) && !errors.Is(err, context.Canceled) {
			t.Fatalf("stream ended with %v, want ErrClientGone or context cancellation", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the session outlived its client")
	}
}

// RF-A7 scenario 2, the one that matters most.
//
// Streaming over POST is a first-class case: MCP uses it. In net/http the
// background read that cancels r.Context() on disconnection only starts once
// the request body has been consumed:
//
//	if requestBodyRemains(req.Body) { registerOnHitEOF(...) } else { startBackgroundRead() }
//
// With a body left undrained over HTTP/1.1 there is no reader watching the
// socket, so the disconnection is discovered only when something writes and the
// write fails. net/http then cancels the context as a *consequence* of that
// failure, which is why the context looks like it works here and is still not
// the mechanism: on a silent stream nothing writes, so nothing ever notices.
//
// That is the whole reason the keep-alive doubles as the liveness probe.
func TestDisconnectPOSTWithUndrainedBody(t *testing.T) {
	type ending struct {
		err       error
		ctxWasDue bool
	}
	ended := make(chan ending, 1)

	srv := httptest.NewServer(sse.Handler(func(ctx context.Context, s *sse.Session) error {
		// Deliberately do not read s.Request().Body.
		for {
			err := s.Send(ctx, sse.Text(strings.Repeat("x", 512)))
			if err != nil {
				// Sampled at the instant the write failed, so the ordering
				// between the two signals is not a race with handler teardown.
				ended <- ending{err: err, ctxWasDue: ctx.Err() != nil}
				return err
			}
			time.Sleep(2 * time.Millisecond)
		}
	}))
	defer srv.Close()

	body := strings.Repeat("q", 256)
	dialAndAbort(t, srv.Listener.Addr().String(), fmt.Sprintf(
		"POST /stream HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s",
		len(body), body))

	select {
	case e := <-ended:
		// Not accepting context.Canceled is the point: if this passes, the
		// write failure is what ended the session.
		if !errors.Is(e.err, sse.ErrClientGone) && !errors.Is(e.err, sse.ErrWriteTimeout) {
			t.Fatalf("stream ended with %v, want ErrClientGone or ErrWriteTimeout", e.err)
		}
		t.Logf("context already cancelled when the write failed: %v", e.ctxWasDue)
	case <-time.After(15 * time.Second):
		t.Fatal("the session outlived its client over POST with an undrained body")
	}
}

// The sharper version of the same point: a silent stream over POST with an
// undrained body. Nothing in the application writes, so if the library did not
// write a keep-alive of its own, nothing would ever discover that the client is
// gone and the session would hang for as long as the process lives.
//
// This is the case that leaks subscribers permanently in libraries that rely on
// the request context.
func TestSilentStreamStillDetectsDisconnect(t *testing.T) {
	ended := make(chan error, 1)

	srv := httptest.NewServer(sse.Handler(
		func(ctx context.Context, s *sse.Session) error {
			// Produces nothing at all: only the library's keep-alive touches
			// the socket. Waiting on s.Done() rather than ctx.Done() is
			// deliberate — it means the only thing that can end this test is
			// the writer noticing, so a pass proves the keep-alive write is
			// what detected the disconnection.
			select {
			case <-s.Done():
				err := s.Err()
				ended <- err
				return err
			case <-time.After(20 * time.Second):
				ended <- errors.New("never noticed the client was gone")
				return nil
			}
		},
		sse.WithKeepAlive(100*time.Millisecond),
		sse.WithWriteTimeout(time.Second),
	))
	defer srv.Close()

	dialAndAbort(t, srv.Listener.Addr().String(),
		"POST /stream HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\n\r\nping")

	select {
	case err := <-ended:
		if err == nil || strings.Contains(err.Error(), "never noticed") {
			t.Fatalf("silent stream did not detect the disconnection: %v", err)
		}
		t.Logf("detected via %v", err)
	case <-time.After(25 * time.Second):
		t.Fatal("silent stream leaked: the session outlived its client")
	}
}

// RF-A4: the handler must not assume GET anywhere.
func TestAnyMethod(t *testing.T) {
	h := sse.Handler(func(ctx context.Context, s *sse.Session) error {
		return s.Send(ctx, sse.Text(s.Request().Method))
	})

	for _, method := range []string{"GET", "POST", "PUT", "PATCH", "DELETE"} {
		t.Run(method, func(t *testing.T) {
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(method, "/stream", strings.NewReader("{}")))

			if w.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", w.Code)
			}
			if !strings.Contains(w.Body.String(), "data: "+method) {
				t.Errorf("body did not carry the method: %q", w.Body.String())
			}
		})
	}
}

func TestStreamHeaders(t *testing.T) {
	w := httptest.NewRecorder()
	sse.Handler(func(ctx context.Context, s *sse.Session) error { return nil }).
		ServeHTTP(w, httptest.NewRequest("GET", "/stream", nil))

	want := map[string]string{
		"Content-Type":  "text/event-stream",
		"Cache-Control": "no-cache",
		// RF-A6: without this, nginx buffers the response and the stream looks
		// broken until its buffer happens to fill.
		"X-Accel-Buffering": "no",
	}
	for k, v := range want {
		if got := w.Header().Get(k); got != v {
			t.Errorf("%s = %q, want %q", k, got, v)
		}
	}
}

// --- RF-A7 scenario 3 and RF-H4: middleware that hides the writer -----------

// blindWrapper wraps a ResponseWriter without exposing it. Because the embedded
// interface has no Flush and there is no Unwrap, http.ResponseController cannot
// reach the real writer. Plenty of third-party middleware looks like this.
type blindWrapper struct{ http.ResponseWriter }

// politeWrapper is the same thing done correctly.
type politeWrapper struct{ http.ResponseWriter }

func (w politeWrapper) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func TestRefusesWriterThatCannotFlush(t *testing.T) {
	var reached bool
	h := sse.Handler(func(ctx context.Context, s *sse.Session) error {
		reached = true
		return nil
	})

	rec := httptest.NewRecorder()
	h.ServeHTTP(blindWrapper{rec}, httptest.NewRequest("GET", "/stream", nil))

	if reached {
		t.Error("the stream function ran on a writer that can never be flushed")
	}
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
}

// RF-G4: the error has to teach. Naming the type that broke the chain is the
// difference between a five-minute fix and an afternoon.
func TestUnsupportedWriterErrorNamesTheCulprit(t *testing.T) {
	err := (&sse.UnsupportedWriterError{
		Chain:    []string{"sse_test.blindWrapper", "*httptest.ResponseRecorder"},
		BrokenAt: "sse_test.blindWrapper",
	}).Error()

	for _, want := range []string{"blindWrapper", "Unwrap() http.ResponseWriter", "writer chain"} {
		if !strings.Contains(err, want) {
			t.Errorf("error message does not mention %q:\n%s", want, err)
		}
	}
}

func TestAcceptsWrapperThatUnwraps(t *testing.T) {
	rec := httptest.NewRecorder()
	sse.Handler(func(ctx context.Context, s *sse.Session) error {
		return s.Send(ctx, sse.Text("through the wrapper"))
	}).ServeHTTP(politeWrapper{rec}, httptest.NewRequest("GET", "/stream", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "through the wrapper") {
		t.Errorf("nothing came through: %q", rec.Body.String())
	}
}

func TestInvalidOptionIsRejected(t *testing.T) {
	if _, err := sse.NewHandler(func(context.Context, *sse.Session) error { return nil },
		sse.WithRetryJitter(2)); err == nil {
		t.Error("NewHandler accepted a jitter outside [0, 1]")
	}
	if _, err := sse.NewHandler(nil); err == nil {
		t.Error("NewHandler accepted a nil stream function")
	}

	defer func() {
		if recover() == nil {
			t.Error("Handler did not panic on an invalid option")
		}
	}()
	sse.Handler(func(context.Context, *sse.Session) error { return nil }, sse.WithMaxEventSize(-1))
}
