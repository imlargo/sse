// Command multi-tenant is the level 2 example: events segmented by tenant, with
// subscribers declaring what they want.
//
// The whole server is a broker over a log. Publishing names one concrete topic;
// subscribers name filters. Which subscribers an event reaches is therefore a
// property of who is listening, never of how it was addressed.
//
//	go run ./examples/02-multi-tenant
//
// Then watch two tenants stay apart:
//
//	curl -sN 'localhost:8080/events?topic=tenant.acme.>'
//	curl -sN 'localhost:8080/events?topic=tenant.*.tickets'
//	curl -sN 'localhost:8080/events?topic=tenant.acme.tickets&topic=system.notices'
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/imlargo/sse"
)

var tenants = []string{"acme", "globex", "initech"}

func main() {
	// Ten minutes of history, so a client that drops off can come back and be
	// told either what it missed or that it waited too long.
	log0 := sse.NewMemoryLog(sse.Retention{For: 10 * time.Minute, Events: 50_000})
	defer log0.Close()

	broker := sse.NewBroker("events", log0)
	lc := sse.NewLifecycle()

	go produce(broker)

	mux := http.NewServeMux()

	// One line. Filters come from the query string, because EventSource cannot
	// send headers.
	mux.Handle("/events", broker.Handler(sse.WithLifecycle(lc)))

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

// produce stands in for the application. It publishes and has no idea who, if
// anyone, is listening.
func produce(b *sse.Broker) {
	ctx := context.Background()
	kinds := []string{"tickets", "builds", "deploys"}

	for n := 1; ; n++ {
		tenant := tenants[rand.IntN(len(tenants))]
		kind := kinds[rand.IntN(len(kinds))]

		topic := sse.MustTopic(fmt.Sprintf("tenant.%s.%s", tenant, kind))
		_, err := b.Publish(ctx, topic, struct {
			Tenant string `json:"tenant"`
			Kind   string `json:"kind"`
			Seq    int    `json:"seq"`
		}{tenant, kind, n}, sse.Name(kind))
		if err != nil {
			slog.Error("publishing", "error", err)
			return
		}

		// Something every tenant sees, to show one connection carrying several
		// logical streams — which matters because a browser allows only six
		// connections per domain over HTTP/1.1.
		if n%10 == 0 {
			if _, err := b.Publish(ctx, sse.MustTopic("system.notices"),
				sse.Text(fmt.Sprintf("maintenance window in %d minutes", 60-n%60)),
				sse.Name("notice")); err != nil {
				slog.Error("publishing notice", "error", err)
				return
			}
		}
		time.Sleep(400 * time.Millisecond)
	}
}

const page = `<!doctype html>
<meta charset="utf-8">
<title>multi-tenant events</title>
<style>
  body { font: 15px/1.6 system-ui, sans-serif; max-width: 56rem; margin: 2.5rem auto; padding: 0 1rem; }
  .cols { display: grid; grid-template-columns: repeat(auto-fit, minmax(15rem, 1fr)); gap: 1rem; }
  .col h2 { font-size: .95rem; margin: 0 0 .25rem; }
  .col code { font-size: .78rem; color: #666; }
  .feed { font: 12px/1.5 ui-monospace, monospace; background: #f7f8f7; border-radius: 4px;
          padding: .5rem .7rem; height: 16rem; overflow: auto; margin-top: .4rem; }
</style>
<h1>Three subscribers, one broker</h1>
<p>Each opens its own connection with a different filter. Publishing names one
concrete topic; who receives it is decided by who is listening.</p>
<div class="cols" id="cols"></div>
<script>
  const subs = [
    { title: 'Acme, everything',      filter: 'tenant.acme.>' },
    { title: 'Tickets, all tenants',  filter: 'tenant.*.tickets' },
    { title: 'Acme tickets + notices', filter: ['tenant.acme.tickets', 'system.notices'] },
  ];

  for (const s of subs) {
    const filters = [].concat(s.filter);
    const col = document.createElement('div');
    col.className = 'col';
    col.innerHTML = '<h2>' + s.title + '</h2><code>' +
      filters.join('<br>') + '</code><div class="feed"></div>';
    document.getElementById('cols').appendChild(col);

    const feed = col.querySelector('.feed');
    const qs = filters.map(f => 'topic=' + encodeURIComponent(f)).join('&');
    const es = new EventSource('/events?' + qs);

    es.addEventListener('sse.open', e => {
      const o = JSON.parse(e.data);
      feed.textContent = 'resumable: ' + o.resumable + '  ·  ' + o.delivery + '\n';
    });
    es.addEventListener('sse.gap', e => {
      feed.insertAdjacentText('afterbegin', 'GAP ' + e.data + '\n');
    });
    for (const type of ['tickets', 'builds', 'deploys', 'notice']) {
      es.addEventListener(type, e => {
        feed.insertAdjacentText('afterbegin', type.padEnd(8) + ' ' + e.data + '\n');
      });
    }
  }
</script>
`
