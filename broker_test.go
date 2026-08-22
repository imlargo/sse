package sse_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/imlargo/sse"
	"github.com/imlargo/sse/ssetest"
	"github.com/imlargo/sse/wire"
)

// serveBroker drives a broker subscription over the in-memory transport.
func serveBroker(t *testing.T, b *sse.Broker, query string, n int) *ssetest.Conn {
	t.Helper()
	c := ssetest.NewConn()

	target := "/events"
	if query != "" {
		target += "?" + query
	}
	r := httptest.NewRequest("GET", target, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		filters, err := sse.FiltersFromQuery(r, sse.TopicQueryParam)
		if err != nil {
			t.Error(err)
			return
		}
		_ = sse.Serve(ctx, c, r, func(ctx context.Context, s *sse.Session) error {
			return b.Subscribe(ctx, s, filters...)
		}, sse.WithLog("events", b.Log()), sse.WithKeepAlive(0))
	}()

	deadline := time.Now().Add(2 * time.Second)
	for len(c.Messages()) < n && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	<-done
	c.Close()
	return c
}

func dataOf(c *ssetest.Conn, typ string) []string {
	var out []string
	for _, m := range c.Messages() {
		if m.Type == typ {
			out = append(out, m.Data)
		}
	}
	return out
}

// RF-B2: publish once, everyone connected receives it.
func TestBroadcastReachesEverySubscriber(t *testing.T) {
	log := sse.NewMemoryLog(sse.Retention{Events: 1000})
	defer log.Close()
	b := sse.NewBroker("events", log)

	for i := range 5 {
		if _, err := b.Publish(context.Background(), sse.MustTopic("news"),
			sse.Text(fmt.Sprintf("item-%d", i)), sse.Name("news")); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	conns := make([]*ssetest.Conn, 10)
	for i := range conns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conns[i] = serveBroker(t, b, "", 6)
		}()
	}
	wg.Wait()

	for i, c := range conns {
		if got := dataOf(c, "news"); len(got) != 5 {
			t.Errorf("subscriber %d received %d of 5 events: %v", i, len(got), got)
		}
	}
}

// RF-B3: subscribers declare what they want and receive only that.
func TestTopicSegmentation(t *testing.T) {
	log := sse.NewMemoryLog(sse.Retention{Events: 1000})
	defer log.Close()
	b := sse.NewBroker("events", log)

	published := []struct{ topic, body string }{
		{"tenant.acme.tickets", "acme-ticket"},
		{"tenant.acme.builds", "acme-build"},
		{"tenant.globex.tickets", "globex-ticket"},
		{"system.notices", "notice"},
	}
	for _, p := range published {
		if _, err := b.Publish(context.Background(), sse.MustTopic(p.topic),
			sse.Text(p.body), sse.Name("e")); err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{"one tenant, everything", "topic=tenant.acme.>", []string{"acme-ticket", "acme-build"}},
		{"one tenant, one kind", "topic=tenant.acme.tickets", []string{"acme-ticket"}},
		{"all tenants, one kind", "topic=tenant.*.tickets", []string{"acme-ticket", "globex-ticket"}},
		{"two filters", "topic=tenant.acme.builds&topic=system.notices", []string{"acme-build", "notice"}},
		{"no filter is everything", "", []string{"acme-ticket", "acme-build", "globex-ticket", "notice"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := serveBroker(t, b, tt.query, len(tt.want)+1)
			got := dataOf(c, "e")
			if len(got) != len(tt.want) {
				t.Fatalf("received %v, want %v", got, tt.want)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Errorf("event %d = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

// A tenant must never see another tenant's events. This is the property the
// whole segmentation exists for.
func TestTenantIsolation(t *testing.T) {
	log := sse.NewMemoryLog(sse.Retention{Events: 1000})
	defer log.Close()
	b := sse.NewBroker("events", log)

	for i := range 20 {
		tenant := "acme"
		if i%2 == 0 {
			tenant = "globex"
		}
		if _, err := b.Publish(context.Background(),
			sse.MustTopic(fmt.Sprintf("tenant.%s.item.%d", tenant, i)),
			sse.Text(tenant), sse.Name("e")); err != nil {
			t.Fatal(err)
		}
	}

	c := serveBroker(t, b, "topic=tenant.acme.>", 11)
	for _, d := range dataOf(c, "e") {
		if d != "acme" {
			t.Fatalf("a tenant subscriber received another tenant's event: %q", d)
		}
	}
	if n := len(dataOf(c, "e")); n != 10 {
		t.Errorf("received %d events, want the 10 belonging to this tenant", n)
	}
}

// The payoff of the specification detail that an id is committed before the
// empty-data check: a subscriber whose filter matches almost nothing still has
// its resumption position carried forward, instead of resuming far in the past
// and rescanning everything it already skipped.
func TestFilteredOutEventsStillAdvanceTheCursor(t *testing.T) {
	log := sse.NewMemoryLog(sse.Retention{Events: 10_000})
	defer log.Close()
	b := sse.NewBroker("events", log)

	// A hundred events the subscriber does not want, then one it does.
	for i := range 100 {
		if _, err := b.Publish(context.Background(),
			sse.MustTopic(fmt.Sprintf("other.%d", i)), sse.Text("no")); err != nil {
			t.Fatal(err)
		}
	}

	c := ssetest.NewConn()
	r := httptest.NewRequest("GET", "/events?topic=mine.>", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = sse.Serve(ctx, c, r, func(ctx context.Context, s *sse.Session) error {
			return b.Subscribe(ctx, s, sse.MustFilter("mine.>"))
		}, sse.WithLog("events", b.Log()), sse.WithKeepAlive(30*time.Millisecond))
	}()

	// Wait for a keep-alive tick to carry the checkpoint out.
	deadline := time.Now().Add(2 * time.Second)
	var cursor string
	for time.Now().Before(deadline) {
		d := wire.NewDecoder(strings.NewReader(c.String()))
		for {
			if _, err := d.Next(); err != nil {
				break
			}
		}
		if cursor = d.LastEventID(); cursor != "" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	c.Close()

	if cursor == "" {
		t.Fatalf("no checkpoint was ever sent, so this subscriber would resume "+
			"from the beginning and rescan all 100 skipped events: %q", truncate(c.String()))
	}

	parsed, err := sse.ParseCursor(cursor)
	if err != nil {
		t.Fatalf("checkpoint %q is not a valid cursor: %v", cursor, err)
	}
	e, ok := parsed.Lookup(sse.NewLogID("events"))
	if !ok {
		t.Fatal("checkpoint carries no position for the log")
	}
	if e.Offset < 90 {
		t.Errorf("checkpoint is at offset %d after 100 skipped events; it barely advanced", e.Offset)
	}
	t.Logf("cursor advanced to offset %d without delivering a single event", e.Offset)

	// And no event was actually dispatched to the client.
	if n := len(c.Messages()); n != 1 { // just sse.open
		t.Errorf("checkpoint dispatched %d events; it must dispatch none", n-1)
	}
}

// D-01: publishing names one concrete topic.
func TestBrokerRejectsAnUnaddressedPublish(t *testing.T) {
	log := sse.NewMemoryLog(sse.Retention{Events: 10})
	defer log.Close()
	b := sse.NewBroker("events", log)

	if _, err := b.Publish(context.Background(), sse.Topic{}, sse.Text("x")); err == nil {
		t.Error("an unaddressed publish was accepted")
	}
}

// RF-B6: a filter taken from a query string is validated before it reaches the
// matcher.
func TestBadFilterInQueryIsRejected(t *testing.T) {
	for _, q := range []string{"topic=org.>.bad", "topic=org%23frag", "topic=", "topic=" + strings.Repeat("a", 300)} {
		r := httptest.NewRequest("GET", "/events?"+q, nil)
		if _, err := sse.FiltersFromQuery(r, sse.TopicQueryParam); err == nil {
			t.Errorf("query %q was accepted", q)
		}
	}
}

// RF-B4: several logical streams over one connection is a consequence of having
// filters and a session, not a feature that needed building. The six-connection
// limit per domain in HTTP/1.1 is why it matters.
func TestOneConnectionCarriesSeveralStreams(t *testing.T) {
	log := sse.NewMemoryLog(sse.Retention{Events: 1000})
	defer log.Close()
	b := sse.NewBroker("events", log)

	for _, topic := range []string{"chat.room.1", "presence.room.1", "notifications.user.7"} {
		if _, err := b.Publish(context.Background(), sse.MustTopic(topic),
			sse.Text(topic), sse.Name("e")); err != nil {
			t.Fatal(err)
		}
	}

	c := serveBroker(t, b, "topic=chat.>&topic=presence.>&topic=notifications.user.7", 4)
	if got := len(dataOf(c, "e")); got != 3 {
		t.Errorf("one connection carried %d of 3 logical streams", got)
	}
}

// RF-F1: the decision to accept a connection happens before the stream opens,
// so a rejection is an ordinary HTTP response with a status code.
//
// Order matters more than usual here. Once a 200 text/event-stream response is
// committed there is no status left to send, and a client that receives a
// non-200 stops reconnecting permanently — so validating too late turns a typo
// in a query string into a client that never comes back.
func TestBadFilterIsRejectedBeforeTheStreamOpens(t *testing.T) {
	log := sse.NewMemoryLog(sse.Retention{Events: 10})
	defer log.Close()
	b := sse.NewBroker("events", log)

	bad := []string{
		"topic=org.%3E.bad",                 // tail wildcard in the middle
		"topic=org%23fragment",              // '#' truncates in a browser
		"topic=",                            // empty
		"topic=" + strings.Repeat("a", 300), // over the length limit
	}
	for _, q := range bad {
		t.Run(q[:min(len(q), 30)], func(t *testing.T) {
			w := httptest.NewRecorder()
			b.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/events?"+q, nil))

			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; the stream must not open on a bad filter", w.Code)
			}
			if ct := w.Header().Get("Content-Type"); strings.Contains(ct, "event-stream") {
				t.Errorf("a stream was opened anyway (Content-Type %q)", ct)
			}
			if w.Body.Len() == 0 {
				t.Error("the rejection explains nothing")
			}
		})
	}

	// And a good one still opens. The stream would otherwise run forever, so
	// it is given a deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/events?topic=org.42.%3E", nil).WithContext(ctx)
	b.Handler(sse.WithKeepAlive(0)).ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("a valid filter was rejected with %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); !strings.Contains(ct, "event-stream") {
		t.Errorf("Content-Type = %q, want an event stream", ct)
	}
}
