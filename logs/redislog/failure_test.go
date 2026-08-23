package redislog_test

import (
	"context"
	"fmt"
	"os/exec"
	"testing"
	"time"

	"github.com/imlargo/sse"
	"github.com/imlargo/sse/logs/redislog"
)

// Redis dropping every connection, as a failover or a restart does. The data
// survives; the connections do not.
func TestSurvivesConnectionLoss(t *testing.T) {
	c := client(t)
	ctx := context.Background()

	l, err := redislog.New(ctx, c, freshKey(t, c), sse.Retention{Events: 10_000},
		redislog.WithBlockTimeout(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	r, err := l.Read(readCtx, 0, sse.ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if _, err := l.Append(ctx, sse.Frame{Body: []byte("data: before\n\n")}); err != nil {
		t.Fatal(err)
	}
	e, err := r.Next(readCtx)
	if err != nil {
		t.Fatalf("before the failure: %v", err)
	}
	if string(e.Frame.Body) != "data: before\n\n" {
		t.Fatalf("got %q", e.Frame.Body)
	}

	// Cut every client, including the tailer's.
	if err := c.Do(ctx, "CLIENT", "KILL", "TYPE", "normal").Err(); err != nil {
		t.Logf("CLIENT KILL: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	// The subscriber must recover without anybody restarting anything.
	var delivered bool
	for attempt := range 40 {
		if _, err := l.Append(ctx, sse.Frame{Body: []byte("data: after\n\n")}); err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		ctxTry, cancelTry := context.WithTimeout(readCtx, 500*time.Millisecond)
		got, err := r.Next(ctxTry)
		cancelTry()
		if err == nil && string(got.Frame.Body) == "data: after\n\n" {
			t.Logf("recovered after %d attempts", attempt+1)
			delivered = true
			break
		}
	}
	if !delivered {
		t.Fatal("the stream never recovered after Redis dropped its connections")
	}
}

// Redis restarted with nothing in it — a wipe, a failover to an empty replica,
// the wrong instance.
//
// The offsets a client holds now refer to events that no longer exist. Handing
// it whatever occupies those positions instead would be exactly the silent
// corruption the epoch exists to prevent, so it has to be reported.
func TestDetectsAWipedStream(t *testing.T) {
	if _, err := exec.LookPath("redis-cli"); err != nil {
		t.Skip("needs redis-cli to restart the server")
	}
	c := client(t)
	ctx := context.Background()
	key := freshKey(t, c)

	l, err := redislog.New(ctx, c, key, sse.Retention{Events: 10_000},
		redislog.WithBlockTimeout(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}

	for i := range 5 {
		if _, err := l.Append(ctx, sse.Frame{Body: []byte(fmt.Sprintf("data: %d\n\n", i))}); err != nil {
			t.Fatal(err)
		}
	}
	before, err := l.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	staleOffset := before.Newest
	l.Close()

	// Wipe it, as a restart without persistence would.
	if err := c.Del(ctx, key, key+":epoch").Err(); err != nil {
		t.Fatal(err)
	}

	// A node coming up against the wiped stream.
	l2, err := redislog.New(ctx, c, key, sse.Retention{Events: 10_000},
		redislog.WithBlockTimeout(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	after, err := l2.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if after.Epoch == before.Epoch {
		t.Fatalf("the generation did not change after the stream was wiped (%d): a "+
			"cursor from before would resolve against unrelated events", after.Epoch)
	}
	t.Logf("generation changed %d -> %d, so cursors from before are detectable",
		before.Epoch, after.Epoch)

	// And a client presenting the stale position must be told, not served.
	cur := sse.NewCursor(sse.CursorEntry{
		Log: sse.NewLogID("events"), Epoch: before.Epoch, Offset: staleOffset,
	})
	conn := subscribeWithCursor(t, sse.NewBroker("events", l2), cur.String(), 2)
	msgs := conn.Messages()
	if len(msgs) < 2 || msgs[1].Type != "sse.gap" {
		t.Fatalf("a cursor from the wiped generation was accepted: %q", conn.String())
	}
	t.Logf("client was told: %s", msgs[1].Data)
}

// The dangerous version of the same thing: the stream is wiped while a node is
// still running.
//
// The node holds the generation it read at startup. If it keeps reporting that
// one, a client reconnecting with a cursor from before the wipe is told its
// position is fine — and is then served whatever now occupies the stream. That
// is the silent corruption the epoch exists to catch, so the node has to notice
// on its own rather than only across a restart.
func TestNoticesAWipeWhileRunning(t *testing.T) {
	c := client(t)
	ctx := context.Background()
	key := freshKey(t, c)

	l, err := redislog.New(ctx, c, key, sse.Retention{Events: 10_000},
		redislog.WithBlockTimeout(100*time.Millisecond),
		redislog.WithEpochCheckInterval(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	if _, err := l.Append(ctx, sse.Frame{Body: []byte("data: before\n\n")}); err != nil {
		t.Fatal(err)
	}
	before, err := l.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Wipe it underneath the running node.
	if err := c.Del(ctx, key, key+":epoch").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Append(ctx, sse.Frame{Body: []byte("data: after\n\n")}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	var after sse.LogInfo
	for time.Now().Before(deadline) {
		after, err = l.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if after.Epoch != before.Epoch {
			t.Logf("the running node noticed: generation %d -> %d", before.Epoch, after.Epoch)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("a running node still reports generation %d after the stream was "+
		"wiped: a cursor from before would be accepted and resolved against "+
		"unrelated events", after.Epoch)
}
