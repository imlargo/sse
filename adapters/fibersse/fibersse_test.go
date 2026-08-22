package fibersse_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/imlargo/sse"
	fibersse "github.com/imlargo/sse/adapters/fibersse"
	"github.com/imlargo/sse/internal/leak"
)

// Leak detection still applies, with fasthttp's own goroutines excluded.
//
// Its worker pool keeps a janitor sleeping past Shutdown; that is fasthttp's
// business, not a subscriber leak. The exclusions name those specific frames
// and nothing broader, because catching a leak in this adapter is the entire
// reason the check is here — Fiber is where the ecosystem is documented to
// leak a subscriber per dropped client.
func TestMain(m *testing.M) {
	leak.MainIgnoring(m.Run,
		"fasthttp.(*workerPool).Start",
		"fasthttp.(*workerPool).getCh",
		"fasthttp.(*Server).Serve",
		// A package-level ticker that refreshes the Date header, started once
		// and never stopped.
		"fasthttp.updateServerDate",
	)
}

// serve starts a Fiber app on a real socket and returns its address.
func serve(t *testing.T, register func(*fiber.App)) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	app := fiber.New()
	register(app)

	go func() { _ = app.Listener(ln, fiber.ListenConfig{DisableStartupMessage: true}) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = app.ShutdownWithContext(ctx)
	})

	addr := ln.Addr().String()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond); err == nil {
			_ = c.Close()
			return addr
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("Fiber never came up")
	return ""
}

// dial opens a raw connection and sends a request, returning a reader over the
// response.
func dial(t *testing.T, addr, req string) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	return conn, bufio.NewReader(conn)
}

func TestStreamsOverFiber(t *testing.T) {
	addr := serve(t, func(app *fiber.App) {
		app.Get("/events", fibersse.Handler(func(ctx context.Context, s *sse.Session) error {
			for i := range 3 {
				if err := s.Send(ctx, sse.Text(fmt.Sprintf("tick-%d", i)), sse.Name("tick")); err != nil {
					return err
				}
			}
			return nil
		}, sse.WithKeepAlive(0)))
	})

	conn, br := dial(t, addr, "GET /events HTTP/1.1\r\nHost: x\r\n\r\n")
	defer conn.Close()

	var body strings.Builder
	for range 40 {
		line, err := br.ReadString('\n')
		body.WriteString(line)
		if err != nil {
			break
		}
		if strings.Contains(body.String(), "tick-2") {
			break
		}
	}

	got := body.String()
	for _, want := range []string{
		"text/event-stream", "X-Accel-Buffering: no",
		"event: sse.open", "event: tick", "data: tick-0", "data: tick-2",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("%q missing from the response:\n%s", want, got)
		}
	}
}

// The reason this adapter exists.
//
// fasthttp's RequestCtx.Done() fires at server shutdown, not on client
// disconnection, so every SSE library that relies on the request context leaks
// a subscriber per dropped client on Fiber — permanently, for the life of the
// process. That is the documented gap in the Go ecosystem.
//
// Nothing here depends on the context. The keep-alive doubles as a liveness
// probe and a failed write ends the session, so a vanished client is found the
// same way it is on net/http.
func TestVanishedClientDoesNotLeak(t *testing.T) {
	var live atomic.Int64
	ended := make(chan error, 8)

	addr := serve(t, func(app *fiber.App) {
		app.Get("/events", fibersse.Handler(func(ctx context.Context, s *sse.Session) error {
			live.Add(1)
			defer live.Add(-1)

			// Produces nothing at all. Only the library's keep-alive touches
			// the socket, so a pass proves the keep-alive write is what
			// noticed — there is nothing else that could.
			select {
			case <-s.Done():
				err := s.Err()
				ended <- err
				return err
			case <-time.After(20 * time.Second):
				ended <- errors.New("never noticed the client was gone")
				return nil
			}
		}, sse.WithKeepAlive(100*time.Millisecond), sse.WithWriteTimeout(2*time.Second)))
	})

	const clients = 5
	for range clients {
		conn, br := dial(t, addr, "GET /events HTTP/1.1\r\nHost: x\r\n\r\n")
		// Read until the stream is clearly running, then rip the socket away
		// without a close handshake.
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				t.Fatalf("reading the stream: %v", err)
			}
			if strings.HasPrefix(line, "event: sse.open") || strings.HasPrefix(line, "data:") {
				break
			}
		}
		if tcp, ok := conn.(*net.TCPConn); ok {
			_ = tcp.SetLinger(0)
		}
		_ = conn.Close()
	}

	for i := range clients {
		select {
		case err := <-ended:
			if err == nil || strings.Contains(err.Error(), "never noticed") {
				t.Fatalf("client %d: session outlived its client: %v", i, err)
			}
		case <-time.After(25 * time.Second):
			t.Fatalf("client %d: session outlived its client", i)
		}
	}

	deadline := time.Now().Add(5 * time.Second)
	for live.Load() > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := live.Load(); n != 0 {
		t.Errorf("%d zombie subscribers survived their clients", n)
	}
}

// A client that stops reading but never closes must still be bounded. fasthttp
// offers no per-response deadline, so the adapter reaches the connection
// itself; without that this hangs until the process ends.
func TestWriteDeadlineIsAvailable(t *testing.T) {
	caps := make(chan sse.Capabilities, 1)

	addr := serve(t, func(app *fiber.App) {
		app.Get("/events", fibersse.Handler(func(ctx context.Context, s *sse.Session) error {
			caps <- s.Capabilities()
			return s.Send(ctx, sse.Text("x"))
		}, sse.WithKeepAlive(0)))
	})

	conn, br := dial(t, addr, "GET /events HTTP/1.1\r\nHost: x\r\n\r\n")
	defer conn.Close()
	for {
		line, err := br.ReadString('\n')
		if err != nil || strings.HasPrefix(line, "data:") {
			break
		}
	}

	select {
	case c := <-caps:
		if !c.Flush {
			t.Error("the stream opened without flush support")
		}
		if !c.WriteDeadline {
			t.Error("no write deadline on Fiber: a client that stops reading " +
				"would hold a goroutine and a connection until the process ends")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the stream function never ran")
	}
}

// Topics, resumption, authorization and backpressure are the core's, so they
// behave identically here. This checks the wiring, not the features.
func TestBrokerOverFiber(t *testing.T) {
	log := sse.NewMemoryLog(sse.Retention{Events: 1000})
	defer log.Close()
	b := sse.NewBroker("events", log)

	for _, topic := range []string{"tenant.acme.tickets", "tenant.globex.tickets"} {
		if _, err := b.Publish(context.Background(), sse.MustTopic(topic),
			sse.Text(topic), sse.Name("e")); err != nil {
			t.Fatal(err)
		}
	}

	addr := serve(t, func(app *fiber.App) {
		app.Get("/events", fibersse.Handler(func(ctx context.Context, s *sse.Session) error {
			filters, err := sse.FiltersFromQuery(s.Request(), sse.TopicQueryParam)
			if err != nil {
				return err
			}
			return b.Subscribe(ctx, s, filters...)
		}, sse.WithLog("events", log), sse.WithKeepAlive(0)))
	})

	conn, br := dial(t, addr, "GET /events?topic=tenant.acme.%3E HTTP/1.1\r\nHost: x\r\n\r\n")
	defer conn.Close()

	var body strings.Builder
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	for range 60 {
		line, err := br.ReadString('\n')
		body.WriteString(line)
		if err != nil {
			break
		}
		if strings.Contains(body.String(), "tenant.acme.tickets") {
			break
		}
	}

	got := body.String()
	if !strings.Contains(got, "tenant.acme.tickets") {
		t.Errorf("the subscribed topic never arrived:\n%s", got)
	}
	if strings.Contains(got, "tenant.globex") {
		t.Error("another tenant's events leaked through the Fiber adapter")
	}
	// The resumption cursor must come through too.
	if !strings.Contains(got, "id: sse1.") {
		t.Error("no resumption cursor was sent over Fiber")
	}
}
