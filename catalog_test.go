package sse_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/imlargo/sse"
	"github.com/imlargo/sse/ssetest"
)

type Ticket struct {
	ID     int    `json:"id"`
	Title  string `json:"title"`
	Closed bool   `json:"closed"`
}

type Build struct {
	Ref      string        `json:"ref"`
	Duration time.Duration `json:"durationMs"`
}

var (
	TicketCreated = sse.Declare[Ticket]("ticket.created").
			WithDescription("A ticket was opened.").
			OnTopic("tenant.*.tickets")
	BuildFinished = sse.Declare[Build]("build.finished").
			OnTopic("tenant.*.builds")
)

// The payload type is checked by the compiler. This test exists to state the
// scope of that honestly: it covers the payload, not the name.
func TestDeclaredPublishIsTypeChecked(t *testing.T) {
	log := sse.NewMemoryLog(sse.Retention{Events: 100})
	defer log.Close()
	b := sse.NewBroker("events", log,
		sse.WithCatalog(sse.NewCatalog(TicketCreated, BuildFinished)))

	if _, err := TicketCreated.Publish(context.Background(), b,
		sse.MustTopic("tenant.acme.tickets"), Ticket{ID: 1, Title: "hello"}); err != nil {
		t.Fatal(err)
	}
	// TicketCreated.Publish(ctx, b, topic, Build{}) does not compile, which is
	// the compile-time half of RF-G6.
}

// The half that cannot be a compile-time check: Go cannot express "this string
// is a member of that set" in a type, so a raw publish naming an undeclared
// event is caught at runtime with an error that lists what is declared.
func TestUndeclaredEventIsRefused(t *testing.T) {
	log := sse.NewMemoryLog(sse.Retention{Events: 100})
	defer log.Close()
	b := sse.NewBroker("events", log,
		sse.WithCatalog(sse.NewCatalog(TicketCreated, BuildFinished)))

	_, err := b.Publish(context.Background(), sse.MustTopic("tenant.acme.tickets"),
		sse.Text("x"), sse.Name("ticket.deleted"))
	if !errors.Is(err, sse.ErrUndeclaredEvent) {
		t.Fatalf("got %v, want ErrUndeclaredEvent", err)
	}
	if !contains(err.Error(), "ticket.created") {
		t.Errorf("the error does not say what is declared: %v", err)
	}

	// Without a catalog nothing is constrained.
	plain := sse.NewBroker("events", log)
	if _, err := plain.Publish(context.Background(), sse.MustTopic("t"),
		sse.Text("x"), sse.Name("anything")); err != nil {
		t.Errorf("a stream with no catalog rejected an event: %v", err)
	}
}

// RF-E3 derived from the same declaration, so the two cannot drift.
func TestCatalogIsReportedWhenTheStreamOpens(t *testing.T) {
	log := sse.NewMemoryLog(sse.Retention{Events: 100})
	defer log.Close()

	c := ssetest.NewConn()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = sse.Serve(ctx, c, httptest.NewRequest("GET", "/events", nil),
		func(ctx context.Context, s *sse.Session) error { return nil },
		sse.WithCatalog(sse.NewCatalog(TicketCreated, BuildFinished)),
		sse.WithKeepAlive(0))

	msgs := c.Messages()
	if len(msgs) == 0 {
		t.Fatalf("nothing written: %q", c.String())
	}
	var open struct {
		Events []string `json:"events"`
	}
	if err := json.Unmarshal([]byte(msgs[0].Data), &open); err != nil {
		t.Fatal(err)
	}
	want := []string{"ticket.created", "build.finished"}
	if !slices.Equal(open.Events, want) {
		t.Errorf("the connection event announced %v, want %v", open.Events, want)
	}
}

func TestCatalogRejectsDuplicates(t *testing.T) {
	_, err := sse.NewCatalogWith(TicketCreated, TicketCreated)
	if err == nil {
		t.Fatal("a duplicate event name was accepted")
	}
	if !contains(err.Error(), "twice") {
		t.Errorf("the error does not explain the rule: %v", err)
	}
}

func TestNilCatalogConstrainsNothing(t *testing.T) {
	var c *sse.Catalog
	if !c.Declares("anything") {
		t.Error("a nil catalog must not constrain")
	}
	if c.Names() != nil || c.Events() != nil {
		t.Error("a nil catalog must report nothing")
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}
