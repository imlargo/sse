// Command dashboard is the level 1 example, and the one that exercises
// backpressure: a metrics feed updating far faster than a browser can usefully
// render, with subscribers that fall behind on purpose.
//
// It publishes at 200 updates per second across a handful of machines. No
// browser needs that, and a subscriber on a slow connection certainly cannot
// keep up. The Coalesce policy is what makes it work: a subscriber that falls
// behind catches up to the *current* value of each machine instead of replaying
// every intermediate one it missed.
//
// That is the difference between a dashboard that shows stale data forever
// after one hiccup and one that snaps back to the present.
//
//	go run ./examples/03-dashboard
//
// Compare the policies from a terminal, throttling the read side:
//
//	curl -sN --limit-rate 2k localhost:8080/metrics
package main

import (
	"context"
	"io"
	"log"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/imlargo/sse"
)

var machines = []string{"web-1", "web-2", "db-1", "cache-1", "worker-1"}

func main() {
	// A ticker's history is worth nothing a minute later, so retention is
	// short. The stream reports that window to clients when they connect.
	metrics := sse.NewMemoryLog(sse.Retention{For: 30 * time.Second, Events: 5_000})
	defer metrics.Close()

	broker := sse.NewBroker("metrics", metrics,
		// The policy that makes a high-frequency feed survivable. Keyed by
		// machine, so a subscriber that falls behind receives each machine's
		// latest reading rather than a replay of everything it missed.
		sse.WithBackpressure(sse.Backpressure{
			Policy:    sse.Coalesce,
			MaxEvents: 64,
			MaxBytes:  256 << 10,
		}),
		// A new viewer wants the current state, not half a minute of backlog.
		sse.WithStart(sse.StartTail),
	)

	lc := sse.NewLifecycle()
	go produce(broker)

	mux := http.NewServeMux()
	mux.Handle("/metrics", broker.Handler(sse.WithLifecycle(lc)))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		io.WriteString(w, page)
	})

	srv := &http.Server{Addr: ":8080", Handler: mux}
	go func() {
		slog.Info("listening", "addr", "http://localhost:8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	<-ctx.Done()

	slog.Info("draining streams", "open", lc.NodeSessionCount())
	drainCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = lc.Shutdown(drainCtx)
	_ = srv.Shutdown(drainCtx)
}

type reading struct {
	Machine string  `json:"machine"`
	CPU     float64 `json:"cpu"`
	Memory  float64 `json:"memory"`
	Seq     int     `json:"seq"`
}

// produce emits 200 readings a second. Publishing appends to the log and
// returns; it never waits on a subscriber, so a browser tab in the background
// cannot slow this down.
func produce(b *sse.Broker) {
	ctx := context.Background()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	cpu := make([]float64, len(machines))
	for i := range cpu {
		cpu[i] = 40 + rand.Float64()*20
	}

	for seq := 1; ; seq++ {
		<-ticker.C
		i := seq % len(machines)
		name := machines[i]

		// A slow random walk, so coalescing is visibly showing the latest value
		// rather than a stale one.
		cpu[i] = math.Max(1, math.Min(99, cpu[i]+(rand.Float64()-0.5)*6))

		_, err := b.Publish(ctx, sse.MustTopic("metrics.host."+name),
			reading{
				Machine: name,
				CPU:     math.Round(cpu[i]*10) / 10,
				Memory:  math.Round((30+rand.Float64()*50)*10) / 10,
				Seq:     seq,
			},
			sse.Name("reading"),
			// The coalescing key. Explicit — nothing inspects the payload to
			// guess it. A newer reading for a machine supersedes an unread one.
			sse.Key(name),
		)
		if err != nil {
			slog.Error("publishing", "error", err)
			return
		}
	}
}

const page = `<!doctype html>
<meta charset="utf-8">
<title>live metrics</title>
<style>
  body { font: 15px/1.6 system-ui, sans-serif; max-width: 44rem; margin: 2.5rem auto; padding: 0 1rem; }
  table { width: 100%; border-collapse: collapse; }
  th { text-align: left; font-size: .72rem; text-transform: uppercase; letter-spacing: .08em;
       color: #666; font-weight: 500; padding-bottom: .4rem; border-bottom: 1px solid #ddd; }
  td { padding: .5rem 0; border-bottom: 1px solid #eee; font-variant-numeric: tabular-nums; }
  .bar { height: .5rem; background: #eee; border-radius: 3px; overflow: hidden; width: 10rem; }
  .fill { height: 100%; background: #16674f; transition: width .2s; }
  .meta { color: #666; font-size: .85rem; }
</style>
<h1>Live metrics</h1>
<p class="meta">Published at 200/s. Your browser is not receiving 200/s — a
subscriber that falls behind is caught up to each machine's current value rather
than replaying everything it missed.</p>
<p class="meta" id="status">connecting…</p>
<table>
  <thead><tr><th>machine</th><th>cpu</th><th></th><th>memory</th><th>seq</th></tr></thead>
  <tbody id="rows"></tbody>
</table>
<script>
  const rows = document.getElementById('rows');
  const seen = new Map();
  let received = 0;

  const es = new EventSource('/metrics');
  es.addEventListener('sse.open', e => {
    const o = JSON.parse(e.data);
    document.getElementById('status').textContent =
      'connected · ' + o.delivery + ' · window ' + (o.retentionMs / 1000) + 's';
  });

  es.addEventListener('reading', e => {
    const r = JSON.parse(e.data);
    received++;
    let row = seen.get(r.machine);
    if (!row) {
      row = document.createElement('tr');
      row.innerHTML = '<td></td><td></td><td><div class="bar"><div class="fill"></div></div></td><td></td><td></td>';
      rows.appendChild(row);
      seen.set(r.machine, row);
    }
    const c = row.children;
    c[0].textContent = r.machine;
    c[1].textContent = r.cpu.toFixed(1) + '%';
    c[2].querySelector('.fill').style.width = r.cpu + '%';
    c[3].textContent = r.memory.toFixed(1) + '%';
    c[4].textContent = r.seq;
  });

  es.addEventListener('sse.gap', e => {
    document.getElementById('status').textContent = 'fell behind: ' + JSON.parse(e.data).reason;
  });
</script>
`
