package sse_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/imlargo/sse"
	"github.com/imlargo/sse/ssetest"
	"github.com/imlargo/sse/wire"
)

// followUntil streams a log into an in-memory connection until n messages have
// arrived or the deadline passes, then returns what the client saw.
func followUntil(t *testing.T, log sse.Log, lastEventID string, n int) *ssetest.Conn {
	t.Helper()
	c := ssetest.NewConn()

	r := httptest.NewRequest("GET", "/stream", nil)
	if lastEventID != "" {
		r.Header.Set("Last-Event-ID", lastEventID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = sse.Serve(ctx, c, r, sse.Follow,
			sse.WithLog("events", log), sse.WithKeepAlive(0))
	}()

	deadline := time.Now().Add(5 * time.Second)
	for len(c.Messages()) < n && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	<-done
	return c
}

func lastEventIDOf(t *testing.T, c *ssetest.Conn) string {
	t.Helper()
	d := wire.NewDecoder(strings.NewReader(c.String()))
	for {
		if _, err := d.Next(); err != nil {
			break
		}
	}
	return d.LastEventID()
}

// The end-to-end shape of the case the whole design targets: a producer keeps
// running while the client is away, and the client comes back where it left off.
func TestResumeContinuesWhereTheClientLeftOff(t *testing.T) {
	log := sse.NewMemoryLog(sse.Retention{Events: 1000, For: time.Hour})
	defer log.Close()
	pub := sse.NewPublisher(log)

	for i := range 5 {
		if _, err := pub.Publish(context.Background(), sse.Text(fmt.Sprintf("token-%d", i)),
			sse.Name("token")); err != nil {
			t.Fatal(err)
		}
	}

	// First connection: open event plus five tokens.
	first := followUntil(t, log, "", 6)
	msgs := first.Messages()
	if len(msgs) < 6 {
		t.Fatalf("first connection saw %d messages, want 6: %q", len(msgs), first.String())
	}
	cursor := lastEventIDOf(t, first)
	if cursor == "" {
		t.Fatal("no resumption cursor was ever sent, so the client cannot come back")
	}

	// Generation continued while the client was away.
	for i := 5; i < 9; i++ {
		if _, err := pub.Publish(context.Background(), sse.Text(fmt.Sprintf("token-%d", i)),
			sse.Name("token")); err != nil {
			t.Fatal(err)
		}
	}

	// Second connection resumes: open event plus exactly the four it missed.
	second := followUntil(t, log, cursor, 5)
	var got []string
	for _, m := range second.Messages() {
		if m.Type == "token" {
			got = append(got, m.Data)
		}
		if m.Type == "sse.gap" {
			t.Fatalf("a gap was declared inside the retention window: %s", m.Data)
		}
	}

	want := []string{"token-5", "token-6", "token-7", "token-8"}
	if len(got) != len(want) {
		t.Fatalf("resumed with %v, want exactly %v (no duplicates, nothing lost)", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("resumed event %d = %q, want %q", i, got[i], want[i])
		}
	}
}

// RF-C4: when history cannot be provided, say so — before anything else, with
// a reason the application can branch on.
func TestGapIsDeclaredBeforeAnyReplay(t *testing.T) {
	log := sse.NewMemoryLog(sse.Retention{Events: 3})
	defer log.Close()
	pub := sse.NewPublisher(log)

	for i := range 2 {
		if _, err := pub.Publish(context.Background(), sse.Text(fmt.Sprintf("early-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	first := followUntil(t, log, "", 3)
	cursor := lastEventIDOf(t, first)

	// Far more events than the window holds: the client's position is gone.
	for i := range 20 {
		if _, err := pub.Publish(context.Background(), sse.Text(fmt.Sprintf("later-%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	second := followUntil(t, log, cursor, 2)
	msgs := second.Messages()
	if len(msgs) < 2 {
		t.Fatalf("expected an open event and a gap, got %q", second.String())
	}

	if msgs[0].Type != "sse.open" {
		t.Fatalf("first message is %q, want sse.open", msgs[0].Type)
	}
	if msgs[1].Type != "sse.gap" {
		t.Fatalf("second message is %q, want the gap before anything else", msgs[1].Type)
	}

	var g struct {
		Reason  string `json:"reason"`
		From    uint64 `json:"from"`
		Through uint64 `json:"through"`
		Detail  string `json:"detail"`
	}
	if err := json.Unmarshal([]byte(msgs[1].Data), &g); err != nil {
		t.Fatalf("gap payload is not JSON: %v", err)
	}
	if g.Reason != string(sse.GapRetention) {
		t.Errorf("reason = %q, want %q", g.Reason, sse.GapRetention)
	}
	if g.Through <= g.From {
		t.Errorf("gap %+v does not describe a lost range", g)
	}
	if g.Detail == "" {
		t.Error("the gap tells the client nothing about what to do")
	}
}

// RF-C5, the silent-corruption case.
//
// A restart with volatile storage means the same offsets now point at
// completely different events. Resolving an old cursor against them would hand
// the client unrelated data and it would never know.
func TestCursorFromAnEarlierGenerationIsRefused(t *testing.T) {
	oldLog := sse.NewMemoryLog(sse.Retention{Events: 100})
	pub := sse.NewPublisher(oldLog)
	for i := range 5 {
		if _, err := pub.Publish(context.Background(), sse.Text(fmt.Sprintf("before-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	before := followUntil(t, oldLog, "", 6)
	staleCursor := lastEventIDOf(t, before)
	oldLog.Close()

	// The process restarted. Same name, same offsets, entirely different events.
	newLog := sse.NewMemoryLog(sse.Retention{Events: 100})
	defer newLog.Close()
	pub2 := sse.NewPublisher(newLog)
	for i := range 5 {
		if _, err := pub2.Publish(context.Background(), sse.Text(fmt.Sprintf("after-%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	c := followUntil(t, newLog, staleCursor, 2)
	msgs := c.Messages()
	if len(msgs) < 2 {
		t.Fatalf("expected an open event and a gap, got %q", c.String())
	}
	if msgs[1].Type != "sse.gap" {
		t.Fatalf("a cursor from an earlier generation was accepted: second message is %q", msgs[1].Type)
	}

	var g struct {
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal([]byte(msgs[1].Data), &g); err != nil {
		t.Fatal(err)
	}
	if g.Reason != string(sse.GapEpoch) {
		t.Errorf("reason = %q, want %q so the application can tell this apart from ageing out",
			g.Reason, sse.GapEpoch)
	}

	// And crucially: none of the new log's events were replayed as though they
	// were the ones the client missed.
	for _, m := range msgs {
		if strings.HasPrefix(m.Data, "before-") {
			t.Errorf("stale cursor resolved against the new generation and replayed %q", m.Data)
		}
	}
}

// RF-C1 and RF-C2: with no retention the stream promises nothing, and it says
// so both when it opens and when a client tries to resume anyway.
func TestNoRetentionPromisesNothing(t *testing.T) {
	log := sse.NewMemoryLog(sse.Retention{})
	defer log.Close()

	c := followUntil(t, log, "sse1.AYTH45YBlfip-pe33pueAcCEPQ", 2)
	msgs := c.Messages()
	if len(msgs) == 0 {
		t.Fatalf("nothing was written: %q", c.String())
	}

	var open struct {
		Resumable bool   `json:"resumable"`
		Delivery  string `json:"delivery"`
		Recovery  string `json:"recovery"`
	}
	if err := json.Unmarshal([]byte(msgs[0].Data), &open); err != nil {
		t.Fatal(err)
	}
	if open.Resumable {
		t.Error("a log with no retention claimed to be resumable")
	}
	if open.Delivery != "at-most-once" || open.Recovery != "none" {
		t.Errorf("delivery=%q recovery=%q, want the weaker guarantee stated plainly",
			open.Delivery, open.Recovery)
	}

	if len(msgs) < 2 || msgs[1].Type != "sse.gap" {
		t.Fatalf("a client that presented a cursor was not told history is unavailable: %q", c.String())
	}
}

// With retention the promise is at-least-once within the window, and never
// anything stronger.
func TestRetentionDeclaresItsWindow(t *testing.T) {
	log := sse.NewMemoryLog(sse.Retention{Events: 500, For: 10 * time.Minute})
	defer log.Close()

	c := followUntil(t, log, "", 1)
	msgs := c.Messages()
	if len(msgs) == 0 {
		t.Fatalf("nothing was written: %q", c.String())
	}

	var open struct {
		Resumable       bool   `json:"resumable"`
		Delivery        string `json:"delivery"`
		Recovery        string `json:"recovery"`
		RetentionMs     int64  `json:"retentionMs"`
		RetentionEvents int    `json:"retentionEvents"`
	}
	if err := json.Unmarshal([]byte(msgs[0].Data), &open); err != nil {
		t.Fatal(err)
	}
	if !open.Resumable {
		t.Error("a log with retention did not offer resumption")
	}
	if open.Delivery == "exactly-once" {
		t.Fatal("a guarantee was promised that cannot be kept")
	}
	if open.Delivery != "at-least-once-within-retention" {
		t.Errorf("delivery = %q, want the honest at-least-once-within-retention", open.Delivery)
	}
	if open.RetentionMs != (10*time.Minute).Milliseconds() || open.RetentionEvents != 500 {
		t.Errorf("window reported as %dms/%d events, want 600000/500",
			open.RetentionMs, open.RetentionEvents)
	}
}

// A session has one source of event ids. Sending directly while following a log
// would produce an event with no position, which a reconnecting client could
// never recover — a silent loss.
func TestSendIsRefusedWhileFollowing(t *testing.T) {
	log := sse.NewMemoryLog(sse.Retention{Events: 10})
	defer log.Close()

	c := ssetest.NewConn()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var sendErr error
	_ = sse.Serve(ctx, c, httptest.NewRequest("GET", "/s", nil),
		func(ctx context.Context, s *sse.Session) error {
			go func() {
				time.Sleep(20 * time.Millisecond)
				sendErr = s.Send(context.Background(), sse.Text("sneaky"))
				cancel()
			}()
			return s.Follow(ctx)
		},
		sse.WithLog("events", log), sse.WithKeepAlive(0))

	if !errors.Is(sendErr, sse.ErrFollowing) {
		t.Fatalf("Send during Follow returned %v, want ErrFollowing", sendErr)
	}
}

func TestFollowWithoutALogIsAnError(t *testing.T) {
	c := ssetest.NewConn()
	err := sse.Serve(context.Background(), c,
		httptest.NewRequest("GET", "/s", nil), sse.Follow)
	if err == nil || !strings.Contains(err.Error(), "WithLog") {
		t.Fatalf("got %v, want an error telling the caller to pass sse.WithLog", err)
	}
}
