package compat_test

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/imlargo/sse"
	"github.com/labstack/echo/v4"
)

// stream is the same handler for every framework: whatever wraps the writer,
// the application code is identical.
func stream(caps chan<- sse.Capabilities) sse.StreamFunc {
	return func(ctx context.Context, s *sse.Session) error {
		select {
		case caps <- s.Capabilities():
		default:
		}
		for i := range 3 {
			if err := s.Send(ctx, sse.Text(string(rune('a'+i))), sse.Name("tick")); err != nil {
				return err
			}
		}
		return nil
	}
}

// readStream consumes an event stream until the body ends.
func readStream(t *testing.T, url string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("Content-Type = %q", ct)
	}

	var sb strings.Builder
	br := bufio.NewReader(resp.Body)
	for {
		line, err := br.ReadString('\n')
		sb.WriteString(line)
		if err != nil {
			return sb.String()
		}
	}
}

// The finding this package exists to record: Gin needs no adapter.
//
// gin.responseWriter implements Unwrap() http.ResponseWriter, so
// http.ResponseController reaches the standard library's writer underneath and
// both flushing and write deadlines work through it. An adapter would be code
// with no purpose.
func TestGinNeedsNoAdapter(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	caps := make(chan sse.Capabilities, 1)

	r := gin.New()
	r.GET("/events", gin.WrapH(sse.Handler(stream(caps), sse.WithKeepAlive(0))))

	srv := httptest.NewServer(r)
	defer srv.Close()

	body := readStream(t, srv.URL+"/events")
	assertStream(t, "gin", body, caps)
}

// Same for Echo: echo.Response implements Unwrap.
func TestEchoNeedsNoAdapter(t *testing.T) {
	caps := make(chan sse.Capabilities, 1)

	e := echo.New()
	e.HideBanner, e.HidePort = true, true
	e.GET("/events", echo.WrapHandler(sse.Handler(stream(caps), sse.WithKeepAlive(0))))

	srv := httptest.NewServer(e)
	defer srv.Close()

	body := readStream(t, srv.URL+"/events")
	assertStream(t, "echo", body, caps)
}

// And the standard library, as the reference the other two are compared against.
func TestNetHTTPBaseline(t *testing.T) {
	caps := make(chan sse.Capabilities, 1)

	mux := http.NewServeMux()
	mux.Handle("/events", sse.Handler(stream(caps), sse.WithKeepAlive(0)))

	srv := httptest.NewServer(mux)
	defer srv.Close()

	body := readStream(t, srv.URL+"/events")
	assertStream(t, "net/http", body, caps)
}

func assertStream(t *testing.T, name, body string, caps <-chan sse.Capabilities) {
	t.Helper()

	for _, want := range []string{"event: sse.open", "event: tick", "data: a", "data: c"} {
		if !strings.Contains(body, want) {
			t.Errorf("%s: %q missing from the stream:\n%s", name, want, body)
		}
	}

	select {
	case c := <-caps:
		if !c.Flush {
			t.Errorf("%s: the stream opened without flush support", name)
		}
		// The point of the test. The framework's writer does not implement
		// SetWriteDeadline itself; reaching it depends entirely on the wrapper
		// exposing Unwrap. If this ever goes false, that framework has stopped
		// unwrapping and a slow client on it can no longer be timed out.
		if !c.WriteDeadline {
			t.Errorf("%s: write deadlines are unavailable, so the wrapper no longer "+
				"implements Unwrap() http.ResponseWriter and a client that stops "+
				"reading cannot be timed out", name)
		}
	default:
		t.Errorf("%s: the stream function never ran", name)
	}
}

// A wrapper that hides the writer is refused with an error naming it, rather
// than opening a stream that can never be flushed. This is what the frameworks
// above avoid by unwrapping.
func TestWrapperWithoutUnwrapIsRefused(t *testing.T) {
	type blind struct{ http.ResponseWriter }

	rec := httptest.NewRecorder()
	sse.Handler(func(ctx context.Context, s *sse.Session) error {
		t.Error("the stream ran on a writer that can never be flushed")
		return nil
	}).ServeHTTP(blind{rec}, httptest.NewRequest("GET", "/events", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
}
