package sse_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/imlargo/sse"
)

func frame(body string) sse.Frame { return sse.Frame{Body: []byte(body)} }

func mustAppend(t *testing.T, l *sse.MemoryLog, body string) sse.Offset {
	t.Helper()
	off, err := l.Append(context.Background(), frame(body))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	return off
}

func TestOffsetsIncreaseStrictly(t *testing.T) {
	l := sse.NewMemoryLog(sse.Retention{Events: 100})
	defer l.Close()

	var prev sse.Offset
	for i := range 50 {
		off := mustAppend(t, l, strconv.Itoa(i))
		if off <= prev {
			t.Fatalf("offset %d did not increase past %d", off, prev)
		}
		prev = off
	}
}

func TestReadReplaysRetainedHistory(t *testing.T) {
	l := sse.NewMemoryLog(sse.Retention{Events: 100})
	defer l.Close()
	for i := range 5 {
		mustAppend(t, l, strconv.Itoa(i))
	}

	r, err := l.Read(context.Background(), 0) // 0 means everything retained
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.Gap() != nil {
		t.Fatalf("unexpected gap: %+v", r.Gap())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for i := range 5 {
		e, err := r.Next(ctx)
		if err != nil {
			t.Fatalf("Next %d: %v", i, err)
		}
		if got := string(e.Frame.Body); got != strconv.Itoa(i) {
			t.Errorf("entry %d = %q, want %q", i, got, strconv.Itoa(i))
		}
	}
}

// RF-C4: when history is gone, say so — before replaying anything, and with the
// range that was lost.
func TestGapIsDeclaredBeforeReplay(t *testing.T) {
	l := sse.NewMemoryLog(sse.Retention{Events: 3})
	defer l.Close()
	for i := range 10 {
		mustAppend(t, l, strconv.Itoa(i))
	}

	// A client that was at offset 2 has missed events that are no longer held.
	r, err := l.Read(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	gap := r.Gap()
	if gap == nil {
		t.Fatal("no gap declared for a cursor behind the retention window")
	}
	if gap.Reason != sse.GapRetention {
		t.Errorf("reason = %q, want %q", gap.Reason, sse.GapRetention)
	}
	if gap.From != 2 || gap.Through < 2 {
		t.Errorf("gap = %+v, want it to cover everything after offset 2 that was evicted", gap)
	}

	// A client still inside the window gets no gap.
	r2, err := l.Read(context.Background(), 8)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	if r2.Gap() != nil {
		t.Errorf("gap declared for a cursor inside the window: %+v", r2.Gap())
	}
}

// RF-C6, the property no other library holds.
//
// Every implementation that replays history and then "switches to live" has a
// race at the switch. Here there is no switch: a reader holds a position and
// advances it. This asserts that under sustained concurrent publication a
// resuming reader observes every offset, exactly once, strictly in order.
func TestReplayToLiveHasNoSeam(t *testing.T) {
	const (
		preload  = 200
		streamed = 2000
		readers  = 8
	)

	l := sse.NewMemoryLog(sse.Retention{Events: preload + streamed + 1})
	defer l.Close()

	for i := range preload {
		mustAppend(t, l, fmt.Sprintf("pre-%d", i))
	}
	resumeAt := sse.Offset(preload / 2)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Publishers keep appending for the whole duration of the replay, which is
	// exactly the window where a naive implementation loses or duplicates.
	var pub sync.WaitGroup
	pub.Add(1)
	go func() {
		defer pub.Done()
		for i := range streamed {
			if _, err := l.Append(ctx, frame(fmt.Sprintf("live-%d", i))); err != nil {
				return
			}
		}
	}()

	total := sse.Offset(preload + streamed)
	var wg sync.WaitGroup
	errs := make(chan error, readers)

	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := l.Read(ctx, resumeAt)
			if err != nil {
				errs <- err
				return
			}
			defer r.Close()

			want := resumeAt + 1
			for want <= total {
				e, err := r.Next(ctx)
				if err != nil {
					errs <- fmt.Errorf("stopped at offset %d of %d: %w", want, total, err)
					return
				}
				if e.Offset != want {
					errs <- fmt.Errorf("offset %d arrived where %d was expected: "+
						"the replay-to-live handover lost, duplicated or reordered events",
						e.Offset, want)
					return
				}
				want++
			}
		}()
	}

	wg.Wait()
	pub.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// A subscriber holds a position, not a copy of the events. That is what keeps
// an extra subscriber cheap and memory bounded per subscription (RF-D4).
func TestSubscriberCostIsAPosition(t *testing.T) {
	l := sse.NewMemoryLog(sse.Retention{Events: 1000})
	defer l.Close()
	for i := range 1000 {
		mustAppend(t, l, fmt.Sprintf("event-%d", i))
	}

	readers := make([]sse.Reader, 500)
	for i := range readers {
		r, err := l.Read(context.Background(), 0)
		if err != nil {
			t.Fatal(err)
		}
		readers[i] = r
	}
	defer func() {
		for _, r := range readers {
			r.Close()
		}
	}()

	// The bytes held by the log must not depend on how many readers exist.
	allocs := testing.AllocsPerRun(50, func() {
		r, _ := l.Read(context.Background(), 0)
		_ = r.Close()
	})
	if allocs > 4 {
		t.Errorf("opening a reader allocates %.0f times; it should cost about a position", allocs)
	}
}

// A slow reader must not slow down the publisher. In a log model the publisher
// never touches a subscriber at all, so this is structural rather than tuned.
func TestSlowReaderDoesNotBlockPublisher(t *testing.T) {
	l := sse.NewMemoryLog(sse.Retention{Events: 10})
	defer l.Close()

	r, err := l.Read(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// The reader never calls Next. The publisher must still make progress and
	// simply leave it behind.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 10_000 {
			if _, err := l.Append(context.Background(), frame(strconv.Itoa(i))); err != nil {
				return
			}
		}
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("a reader that never reads blocked the publisher")
	}

	// And the one that fell behind is told, rather than silently resuming later.
	r2, err := l.Read(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	if r2.Gap() == nil {
		t.Error("a reader left far behind was not told it lost history")
	}
}

func TestRetentionByCountBytesAndAge(t *testing.T) {
	t.Run("count", func(t *testing.T) {
		l := sse.NewMemoryLog(sse.Retention{Events: 5})
		defer l.Close()
		for i := range 20 {
			mustAppend(t, l, strconv.Itoa(i))
		}
		info, _ := l.Info(context.Background())
		if info.Newest-info.Oldest+1 > 5 {
			t.Errorf("kept offsets %d..%d, want at most 5 entries", info.Oldest, info.Newest)
		}
	})

	t.Run("bytes", func(t *testing.T) {
		l := sse.NewMemoryLog(sse.Retention{Bytes: 100})
		defer l.Close()
		for range 50 {
			mustAppend(t, l, "0123456789") // 10 bytes each
		}
		info, _ := l.Info(context.Background())
		if info.EvictedThrough == 0 {
			t.Error("byte budget never evicted anything")
		}
	})

	t.Run("age", func(t *testing.T) {
		synctestRun(t, func(t *testing.T) {
			l := sse.NewMemoryLog(sse.Retention{For: time.Minute})
			defer l.Close()

			mustAppend(t, l, "old")
			time.Sleep(2 * time.Minute)
			mustAppend(t, l, "new")

			info, _ := l.Info(context.Background())
			if info.EvictedThrough < 1 {
				t.Errorf("an entry two minutes past a one-minute window was kept: %+v", info)
			}
		})
	})
}

// RF-C1: without configured retention the log promises nothing and says so.
func TestZeroRetentionIsNotResumable(t *testing.T) {
	l := sse.NewMemoryLog(sse.Retention{})
	defer l.Close()

	info, err := l.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if info.Resumable {
		t.Error("a log with no configured retention must not claim to be resumable")
	}

	withRetention := sse.NewMemoryLog(sse.Retention{Events: 10})
	defer withRetention.Close()
	info2, _ := withRetention.Info(context.Background())
	if !info2.Resumable {
		t.Error("a log with retention must report itself resumable")
	}
}

// RF-C5: a fresh log is a different generation, so cursors from the old one
// must not resolve against it.
func TestEachLogHasItsOwnEpoch(t *testing.T) {
	seen := map[sse.Epoch]bool{}
	for range 100 {
		l := sse.NewMemoryLog(sse.Retention{Events: 1})
		info, _ := l.Info(context.Background())
		if info.Epoch == 0 {
			t.Fatal("epoch must never be zero, which reads as absent")
		}
		if seen[info.Epoch] {
			t.Fatalf("epoch %d repeated across log generations", info.Epoch)
		}
		seen[info.Epoch] = true
		l.Close()
	}
}

// An ephemeral event is delivered but must not push retained history out.
func TestEphemeralDoesNotEvictHistory(t *testing.T) {
	synctestRun(t, func(t *testing.T) {
		l := sse.NewMemoryLog(sse.Retention{For: time.Hour})
		defer l.Close()

		mustAppend(t, l, "durable")
		for range 100 {
			if _, err := l.Append(context.Background(), sse.Frame{
				Body: []byte("tick"), Ephemeral: true,
			}); err != nil {
				t.Fatal(err)
			}
		}
		// Long enough for the ephemeral window, far short of the hour.
		time.Sleep(2 * time.Minute)
		mustAppend(t, l, "durable-2")

		r, err := l.Read(context.Background(), 0)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		e, err := r.Next(ctx)
		if err != nil {
			t.Fatalf("the durable event was evicted by a burst of ephemeral ones: %v", err)
		}
		if string(e.Frame.Body) != "durable" {
			t.Errorf("oldest retained entry is %q, want the durable one", e.Frame.Body)
		}
	})
}

func TestReadersUnblockOnClose(t *testing.T) {
	l := sse.NewMemoryLog(sse.Retention{Events: 10})
	r, err := l.Read(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	done := make(chan error, 1)
	go func() {
		_, err := r.Next(context.Background())
		done <- err
	}()

	time.Sleep(20 * time.Millisecond)
	_ = l.Close()

	select {
	case err := <-done:
		if !errors.Is(err, sse.ErrLogClosed) {
			t.Errorf("got %v, want ErrLogClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("closing the log left a reader blocked forever")
	}
}

func TestReaderRespectsContext(t *testing.T) {
	l := sse.NewMemoryLog(sse.Retention{Events: 10})
	defer l.Close()
	r, err := l.Read(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := r.Next(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("got %v, want the context deadline", err)
	}
}

func BenchmarkLogAppend(b *testing.B) {
	l := sse.NewMemoryLog(sse.Retention{Events: 10_000})
	defer l.Close()
	f := frame("event: token\ndata: the quick brown fox\n\n")
	ctx := context.Background()

	b.ReportAllocs()
	for b.Loop() {
		if _, err := l.Append(ctx, f); err != nil {
			b.Fatal(err)
		}
	}
}

// RNF-1: the cost of an extra subscriber must approach the cost of a write, not
// of another encoding. Fan-out here is N readers over one shared frame.
func BenchmarkLogFanout(b *testing.B) {
	for _, readers := range []int{1, 10, 100, 1000} {
		b.Run(strconv.Itoa(readers), func(b *testing.B) {
			l := sse.NewMemoryLog(sse.Retention{Events: 1 << 16})
			defer l.Close()
			ctx := context.Background()

			rs := make([]sse.Reader, readers)
			for i := range rs {
				r, _ := l.Read(ctx, 0)
				rs[i] = r
			}
			defer func() {
				for _, r := range rs {
					r.Close()
				}
			}()

			f := frame("event: token\ndata: the quick brown fox\n\n")
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := l.Append(ctx, f); err != nil {
					b.Fatal(err)
				}
				for _, r := range rs {
					if _, err := r.Next(ctx); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

// RNF-1, asserted rather than only benchmarked: delivering one event to N
// subscribers must not cost N encodings. Subscribers hold offsets into a single
// shared frame, so the per-event allocation must not grow with the audience.
func TestFanoutCostIsIndependentOfSubscriberCount(t *testing.T) {
	measure := func(readers int) float64 {
		l := sse.NewMemoryLog(sse.Retention{Events: 1 << 16})
		defer l.Close()
		ctx := context.Background()

		rs := make([]sse.Reader, readers)
		for i := range rs {
			r, err := l.Read(ctx, 0)
			if err != nil {
				t.Fatal(err)
			}
			rs[i] = r
		}
		defer func() {
			for _, r := range rs {
				r.Close()
			}
		}()

		f := frame("event: token\ndata: the quick brown fox\n\n")
		return testing.AllocsPerRun(100, func() {
			if _, err := l.Append(ctx, f); err != nil {
				t.Fatal(err)
			}
			for _, r := range rs {
				if _, err := r.Next(ctx); err != nil {
					t.Fatal(err)
				}
			}
		})
	}

	one, many := measure(1), measure(200)
	t.Logf("allocations per event: 1 subscriber = %.1f, 200 subscribers = %.1f", one, many)

	// Delivery itself must allocate nothing. Allow a small constant for the
	// log's own amortised slice growth, but nothing proportional to readers.
	if many > one+2 {
		t.Errorf("fan-out allocates %.1f per event at 200 subscribers versus %.1f at 1: "+
			"the cost is scaling with the audience, so the event is being re-encoded per subscriber",
			many, one)
	}
}
