package sse_test

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imlargo/sse"
)

// benchLarge opts into the sizes that take minutes to set up.
var benchLarge = flag.Bool("sse.bench.large", false, "include the 15,000-subscriber wake-up benchmark")

// The benchmark the repository was missing.
//
// BenchmarkLogFanout reads every subscriber sequentially from one goroutine, so
// it measures allocation faithfully and cannot measure the cost that actually
// decides a node's capacity: waking N goroutines that are genuinely blocked in
// Next. That cost only appears from a few thousand subscribers up, and it is
// paid whether or not the event is one those subscribers want.
//
// Two cases are measured side by side. "matches-all" is a broadcast every
// subscriber wants. "matches-none" is addressed to a topic no subscriber is
// following — a per-user notification, which is the shape of the most common
// level 2 use. If the two are the same speed, topic filtering is saving socket
// writes but no scheduler work, and a message for one user costs the same as a
// message for everybody.
func BenchmarkWakeup(b *testing.B) {
	// "unfiltered" is the baseline and the worst case: subscribers that ask for
	// everything must be woken by everything, so they all share one bucket.
	// That is what every subscriber used to do regardless of its filter, so
	// comparing against it measures exactly what the change bought.
	// 1k and 5k already show the shape; 15k costs minutes of setup on a small
	// runner and is reachable through TestSoak when the real number is wanted.
	sizes := []int{1_000, 5_000}
	if !testing.Short() && *benchLarge {
		sizes = append(sizes, 15_000)
	}
	for _, subscribers := range sizes {
		for _, mode := range []string{"unfiltered", "per-user-hit", "per-user-miss"} {
			b.Run(fmt.Sprintf("%s/%d", mode, subscribers), func(b *testing.B) {
				benchWakeup(b, subscribers, mode)
			})
		}
	}
}

func benchWakeup(b *testing.B, subscribers int, mode string) {
	log := sse.NewMemoryLog(sse.Retention{Events: 1 << 14})
	defer log.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Every subscriber follows its own user topic, which is what a
	// notification stream looks like.
	var wg sync.WaitGroup
	var delivered atomic.Int64
	for i := range subscribers {
		var filters []sse.Filter
		if mode != "unfiltered" {
			filters = []sse.Filter{sse.MustFilter(fmt.Sprintf("user.%d.>", i))}
		}
		r, err := log.Read(ctx, 0, sse.ReadOptions{Filters: filters})
		if err != nil {
			b.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer r.Close()
			for {
				e, err := r.Next(ctx)
				if err != nil {
					return
				}
				// The same filtering the session does, so the benchmark pays
				// what production pays.
				if len(filters) == 0 || filters[0].Matches(sse.MustTopic(e.Frame.Topic)) {
					delivered.Add(1)
				}
			}
		}()
	}

	// Let every reader reach its blocked state before measuring.
	time.Sleep(100 * time.Millisecond)

	topic := "user.999999.inbox" // matches nobody
	if mode == "per-user-hit" || mode == "unfiltered" {
		topic = "user.0.inbox"
	}
	frame := sse.Frame{Body: []byte("data: x\n\n"), Topic: topic}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := log.Append(ctx, frame); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(subscribers), "subs")
	cancel()
	wg.Wait()
}
