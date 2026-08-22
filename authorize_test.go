package sse_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/imlargo/sse"
	"github.com/imlargo/sse/ssetest"
)

// RF-F1: the decision happens before the stream opens, and a rejection is an
// ordinary HTTP response with its status.
func TestAuthorizerRejectsBeforeTheStreamOpens(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{"unauthorized", sse.Unauthorized("sign in first"), http.StatusUnauthorized},
		{"forbidden", sse.Forbidden("not your tenant"), http.StatusForbidden},
		{"bad request", sse.BadRequest("missing tenant"), http.StatusBadRequest},
		{"custom status", sse.Status(http.StatusTooManyRequests, "slow down"), http.StatusTooManyRequests},
		// An error that is not a StatusError is a server-side failure, not a
		// guess. Answering 403 where 500 was meant would tell the client to
		// stop trying forever.
		{"plain error", errors.New("database unreachable"), http.StatusInternalServerError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var reached bool
			h := sse.Handler(func(ctx context.Context, s *sse.Session) error {
				reached = true
				return nil
			}, sse.WithAuthorizer(func(*http.Request) (sse.Grant, error) {
				return sse.Grant{}, tt.err
			}))

			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest("GET", "/events", nil))

			if reached {
				t.Error("the stream function ran on a rejected request")
			}
			if w.Code != tt.want {
				t.Errorf("status = %d, want %d", w.Code, tt.want)
			}
			if ct := w.Header().Get("Content-Type"); strings.Contains(ct, "event-stream") {
				t.Errorf("a stream was opened anyway (Content-Type %q)", ct)
			}
		})
	}
}

// The authorizer sees the whole request, which is the point: EventSource cannot
// send an Authorization header, so credentials arrive as a cookie or in the
// query string.
func TestAuthorizerSeesTheWholeRequest(t *testing.T) {
	log := sse.NewMemoryLog(sse.Retention{Events: 100})
	defer log.Close()
	b := sse.NewBroker("events", log)

	auth := func(r *http.Request) (sse.Grant, error) {
		token := r.URL.Query().Get("token") // EventSource cannot set headers
		if c, err := r.Cookie("session"); err == nil {
			token = c.Value
		}
		switch token {
		case "acme-user":
			return sse.Grant{
				Identity: "u-1",
				Filters:  []sse.Filter{sse.MustFilter("tenant.acme.>")},
			}, nil
		case "":
			return sse.Grant{}, sse.Unauthorized("no credentials")
		default:
			return sse.Grant{}, sse.Forbidden("unknown token")
		}
	}

	if _, err := b.Publish(context.Background(),
		sse.MustTopic("tenant.acme.tickets"), sse.Text("mine"), sse.Name("e")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(context.Background(),
		sse.MustTopic("tenant.globex.tickets"), sse.Text("theirs"), sse.Name("e")); err != nil {
		t.Fatal(err)
	}

	// By query token.
	c := serveWithAuth(t, b, "/events?token=acme-user", auth, 2)
	got := dataOf(c, "e")
	if len(got) != 1 || got[0] != "mine" {
		t.Errorf("received %v, want only this tenant's event", got)
	}

	// By cookie.
	r := httptest.NewRequest("GET", "/events", nil)
	r.AddCookie(&http.Cookie{Name: "session", Value: "acme-user"})
	w := httptest.NewRecorder()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	b.Handler(sse.WithAuthorizer(auth), sse.WithKeepAlive(0)).ServeHTTP(w, r.WithContext(ctx))
	if w.Code != http.StatusOK {
		t.Errorf("cookie credentials were rejected with %d", w.Code)
	}

	// No credentials at all.
	w2 := httptest.NewRecorder()
	b.Handler(sse.WithAuthorizer(auth)).ServeHTTP(w2, httptest.NewRequest("GET", "/events", nil))
	if w2.Code != http.StatusUnauthorized {
		t.Errorf("an unauthenticated request got %d, want 401", w2.Code)
	}
}

// RF-F2: a denied topic is reported. A stream that simply never carries those
// events is indistinguishable from one where they are merely quiet, and the
// client waits forever.
func TestDeniedTopicsAreReported(t *testing.T) {
	log := sse.NewMemoryLog(sse.Retention{Events: 100})
	defer log.Close()
	b := sse.NewBroker("events", log)

	auth := func(r *http.Request) (sse.Grant, error) {
		return sse.Grant{
			Identity: "u-1",
			Filters:  []sse.Filter{sse.MustFilter("tenant.acme.>")},
			Denied: []sse.Denial{
				{Topic: "tenant.globex.>", Reason: "not-a-member"},
				{Topic: "admin.>", Reason: "insufficient-role"},
			},
		}, nil
	}

	c := serveWithAuth(t, b, "/events?topic=tenant.acme.>&topic=admin.>", auth, 1)
	msgs := c.Messages()
	if len(msgs) == 0 {
		t.Fatalf("nothing written: %q", truncate(c.String()))
	}

	var open struct {
		Identity string   `json:"identity"`
		Granted  []string `json:"granted"`
		Denied   []struct {
			Topic  string `json:"topic"`
			Reason string `json:"reason"`
		} `json:"denied"`
	}
	if err := json.Unmarshal([]byte(msgs[0].Data), &open); err != nil {
		t.Fatal(err)
	}
	if open.Identity != "u-1" {
		t.Errorf("identity = %q, want u-1", open.Identity)
	}
	if len(open.Granted) != 1 || open.Granted[0] != "tenant.acme.>" {
		t.Errorf("granted = %v, want the one allowed filter", open.Granted)
	}
	if len(open.Denied) != 2 {
		t.Fatalf("denied = %v, want both refusals reported", open.Denied)
	}
	for _, d := range open.Denied {
		if d.Reason == "" {
			t.Errorf("denial of %q carries no reason", d.Topic)
		}
	}
}

// RF-F3: a credential that expires mid-stream ends the session, and the client
// reconnects by itself with a fresh one. The protocol's own reconnection turns
// expiry into a non-event.
func TestGrantDeadlineEndsTheSession(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		log := sse.NewMemoryLog(sse.Retention{Events: 100})
		defer log.Close()
		b := sse.NewBroker("events", log)

		c := ssetest.NewConn()
		auth := func(*http.Request) (sse.Grant, error) {
			return sse.Grant{
				Identity: "u-1",
				Deadline: time.Now().Add(30 * time.Second),
			}, nil
		}

		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = sse.Serve(context.Background(), c,
				httptest.NewRequest("GET", "/events", nil),
				func(ctx context.Context, s *sse.Session) error {
					return b.Subscribe(ctx, s)
				},
				sse.WithLog("events", log), sse.WithAuthorizer(auth), sse.WithKeepAlive(0))
		}()

		synctest.Wait()
		time.Sleep(35 * time.Second)
		synctest.Wait()

		select {
		case <-done:
		default:
			t.Fatal("the session outlived its credentials")
		}

		// The client is told, and given a retry, so it comes back with fresh
		// credentials rather than being dropped silently.
		msgs := c.Messages()
		if len(msgs) == 0 || msgs[len(msgs)-1].Type != "sse.closing" {
			t.Fatalf("the client was dropped without being told: %q", truncate(c.String()))
		}
		if retryOf(t, c.String()) <= 0 {
			t.Error("no retry offered, so the client would use its own default")
		}
	})
}

// RF-D1: the policy is decidable per subscription, not only per server.
func TestGrantCanOverrideBackpressure(t *testing.T) {
	log := sse.NewMemoryLog(sse.Retention{Events: 1000})
	defer log.Close()
	b := sse.NewBroker("events", log,
		sse.WithBackpressure(sse.Backpressure{Policy: sse.DropOldest}))

	auth := func(*http.Request) (sse.Grant, error) {
		return sse.Grant{
			Backpressure: &sse.Backpressure{Policy: sse.Disconnect, MaxEvents: 2},
		}, nil
	}

	for range 500 {
		if _, err := b.Publish(context.Background(),
			sse.MustTopic("news"), sse.Text("x")); err != nil {
			t.Fatal(err)
		}
	}

	c := ssetest.NewConn()
	c.Stall()
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := sse.Serve(ctx, c, httptest.NewRequest("GET", "/events", nil),
		func(ctx context.Context, s *sse.Session) error { return b.Subscribe(ctx, s) },
		sse.WithLog("events", log), sse.WithAuthorizer(auth),
		sse.WithKeepAlive(0), sse.WithWriteTimeout(2*time.Second))

	if !errors.Is(err, sse.ErrSlowConsumer) {
		t.Fatalf("got %v, want the per-subscription Disconnect policy to apply", err)
	}
}

// RF-E5: a panic in an authorizer is contained like any other application code.
func TestAuthorizerPanicIsContained(t *testing.T) {
	h := sse.Handler(func(ctx context.Context, s *sse.Session) error { return nil },
		sse.WithAuthorizer(func(*http.Request) (sse.Grant, error) { panic("boom") }))

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/events", nil))

	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", w.Code)
	}
}

func TestGrantWithZeroFilterIsRejected(t *testing.T) {
	h := sse.Handler(func(ctx context.Context, s *sse.Session) error { return nil },
		sse.WithAuthorizer(func(*http.Request) (sse.Grant, error) {
			return sse.Grant{Filters: []sse.Filter{{}}}, nil
		}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/events", nil))
	if w.Code != http.StatusInternalServerError {
		t.Errorf("a malformed grant was accepted (status %d)", w.Code)
	}
}

// serveWithAuth drives a broker subscription with an authorizer over the
// in-memory transport.
func serveWithAuth(t *testing.T, b *sse.Broker, target string, auth sse.Authorizer, n int) *ssetest.Conn {
	t.Helper()
	c := ssetest.NewConn()
	r := httptest.NewRequest("GET", target, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		grant, err := auth(r)
		if err != nil {
			return
		}
		_ = sse.ServeGrant(ctx, c, r, func(ctx context.Context, s *sse.Session) error {
			return b.Subscribe(ctx, s, grant.Filters...)
		}, grant, sse.WithLog("events", b.Log()), sse.WithKeepAlive(0))
	}()

	deadline := time.Now().Add(2 * time.Second)
	for len(c.Messages()) < n && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	c.Close()
	return c
}

var _ = fmt.Sprintf
