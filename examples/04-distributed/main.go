// Command distributed is example 02 running on many nodes.
//
// It is the same program. The diff against examples/02-multi-tenant is the log:
// an in-memory one becomes a Redis Streams one. Nothing else moves — not the
// authorizer, not the topics, not the handler, not the backpressure policy, not
// a line of the application.
//
// That is the whole claim of the architecture, and it is the point where it is
// either true or it is not. The subscriber registry, topic matching, the
// per-subscriber queues, replay, gap declaration and metrics all live above the
// log, so swapping it changes where events are kept and nothing else.
//
// What it buys: a client can reconnect to *any* replica and resume exactly
// where it stopped, because its cursor names a log and an offset rather than
// anything about the node that served it. No sticky sessions.
//
//	redis-server --port 6379 &
//	PORT=8080 go run ./examples/04-distributed &
//	PORT=8081 go run ./examples/04-distributed &
//
// Then start on one node, stop, and resume on the other:
//
//	curl -sN 'localhost:8080/events?token=acme'          # note an id: line
//	curl -sN -H 'Last-Event-ID: <that id>' 'localhost:8081/events?token=acme'
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
	"slices"
	"strings"
	"time"

	"github.com/imlargo/sse"
	"github.com/imlargo/sse/logs/redislog"
	goredis "github.com/redis/go-redis/v9"
)

// rdb connects to Redis. Every node points at the same one; that is all the
// coordination there is.
func rdb() *goredis.Client {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	return goredis.NewClient(&goredis.Options{Addr: addr})
}

func listenAddr() string {
	if p := os.Getenv("PORT"); p != "" {
		return ":" + p
	}
	return ":8080"
}

var tenants = []string{"acme", "globex", "initech"}

func main() {
	// The only change from example 02. Everything below is identical.
	log0, err := redislog.New(context.Background(), rdb(), "sse:events",
		sse.Retention{For: 10 * time.Minute, Events: 50_000})
	if err != nil {
		log.Fatalf("connecting to Redis: %v", err)
	}

	broker := sse.NewBroker("events", log0)
	lc := sse.NewLifecycle()

	go produce(broker)

	mux := http.NewServeMux()

	// The authorizer is the only place that decides anything. It sees the whole
	// request before a byte is committed, and what it returns *is* the
	// subscription.
	mux.Handle("/events", broker.Handler(
		sse.WithAuthorizer(authorize),
		sse.WithLifecycle(lc),
	))

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		io.WriteString(w, page)
	})

	srv := &http.Server{Addr: listenAddr(), Handler: mux}
	go func() {
		slog.Info("listening", "addr", "http://localhost"+listenAddr())
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

// authorize decides who this is and what they may see.
//
// Credentials arrive in the query string because EventSource cannot set an
// Authorization header; a cookie works just as well and both are read here.
func authorize(r *http.Request) (sse.Grant, error) {
	token := r.URL.Query().Get("token")
	if c, err := r.Cookie("tenant"); err == nil {
		token = c.Value
	}
	if token == "" {
		return sse.Grant{}, sse.Unauthorized("pass ?token=acme, globex or initech")
	}
	if !slices.Contains(tenants, token) {
		return sse.Grant{}, sse.Forbidden("unknown tenant " + token)
	}

	// What the client asked for, narrowed to what it may actually have.
	// Anything refused is reported rather than dropped: a topic that silently
	// never produces events is indistinguishable from one that is merely
	// quiet, and the client would wait forever.
	asked, err := sse.FiltersFromQuery(r, sse.TopicQueryParam)
	if err != nil {
		return sse.Grant{}, sse.BadRequest(err.Error())
	}

	mine := sse.MustFilter("tenant." + token + ".>")
	notices := sse.MustFilter("system.notices")

	granted := []sse.Filter{}
	var denied []sse.Denial
	if len(asked) == 0 {
		granted = append(granted, mine, notices)
	}
	for _, f := range asked {
		switch {
		case strings.HasPrefix(f.String(), "tenant."+token+"."), f.String() == "system.notices":
			granted = append(granted, f)
		default:
			denied = append(denied, sse.Denial{Topic: f.String(), Reason: "not-your-tenant"})
		}
	}

	return sse.Grant{
		Identity: token,
		Filters:  granted,
		Denied:   denied,
		// A credential that expires mid-stream is a non-event: the session
		// ends, the client reconnects by itself with a fresh one, and resumes
		// from its cursor.
		Deadline:   time.Now().Add(5 * time.Minute),
		Attributes: map[string]string{"tenant": token},
	}, nil
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
<p>Each opens its own connection with its own credentials. The third asks for
another tenant's events and is told, in the connection event, exactly what was
refused — rather than receiving a stream that is quietly missing them.</p>
<div class="cols" id="cols"></div>
<script>
  const subs = [
    { title: 'Acme, everything',       token: 'acme',   filter: 'tenant.acme.>' },
    { title: 'Globex, everything',     token: 'globex', filter: 'tenant.globex.>' },
    { title: 'Acme asking for Globex', token: 'acme',
      filter: ['tenant.acme.tickets', 'tenant.globex.>', 'system.notices'] },
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
    const es = new EventSource('/events?token=' + s.token + '&' + qs);

    es.addEventListener('sse.open', e => {
      const o = JSON.parse(e.data);
      feed.textContent = 'identity: ' + o.identity + '  ·  ' + o.delivery + '\n';
      // Anything refused is stated, not silently absent.
      for (const d of o.denied || []) {
        feed.textContent += 'DENIED ' + d.topic + ' (' + d.reason + ')\n';
      }
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
