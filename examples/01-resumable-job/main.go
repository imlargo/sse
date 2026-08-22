// Command resumable-job is the level 0 + history example: one client, one
// stream, and a job that keeps running while nobody is watching.
//
// This is the shape of the case the design targets. A long import, a render, a
// language-model generation — the work is started once and keeps going; the
// browser may reload, lose the network or be closed and reopened, and the
// client picks up exactly where it left off. There is no fan-out here and no
// topics: a single reader over a log is the same machinery as a broadcast with
// one subscriber.
//
//	go run ./examples/01-resumable-job
//
// Then open http://localhost:8080, watch it run, and kill the network or reload
// the tab halfway through. Or from a terminal:
//
//	curl -sN localhost:8080/jobs/demo/stream            # note an id: line
//	curl -sN -H 'Last-Event-ID: <that id>' localhost:8080/jobs/demo/stream
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/imlargo/sse"
)

// jobs holds one log per job. A log is the unit of retention and of
// resumption, so "one job, one log" is the natural scope: when the job's
// history should expire is a property of the job.
type jobs struct {
	mu   sync.Mutex
	logs map[string]*sse.MemoryLog
}

func (j *jobs) log(id string) *sse.MemoryLog {
	j.mu.Lock()
	defer j.mu.Unlock()
	if l, ok := j.logs[id]; ok {
		return l
	}
	// Ten minutes of history is what this job promises. The client is told
	// that window when it connects, so it knows exactly how long it may be
	// away before resumption stops working.
	l := sse.NewMemoryLog(sse.Retention{For: 10 * time.Minute, Events: 10_000})
	j.logs[id] = l
	go run(l)
	return l
}

// run is the work. Note that it has no idea whether anyone is watching: it
// publishes to the log and that is the end of its involvement. Nothing here
// deals with connections, reconnections or clients.
func run(l *sse.MemoryLog) {
	pub := sse.NewPublisher(l)
	ctx := context.Background()

	const steps = 60
	for i := 1; i <= steps; i++ {
		_, err := pub.Publish(ctx, struct {
			Step    int    `json:"step"`
			Of      int    `json:"of"`
			Message string `json:"message"`
		}{i, steps, fmt.Sprintf("processing record %d", i)}, sse.Name("progress"))
		if err != nil {
			slog.Error("publishing progress", "error", err)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}

	if _, err := pub.Publish(ctx, struct {
		Records int `json:"records"`
	}{steps}, sse.Name("finished")); err != nil {
		slog.Error("publishing completion", "error", err)
	}
}

func main() {
	j := &jobs{logs: make(map[string]*sse.MemoryLog)}
	lc := sse.NewLifecycle()

	mux := http.NewServeMux()

	// One handler per job, built on demand. The whole streaming side is
	// sse.Follow: resolving Last-Event-ID, declaring any gap before replaying,
	// and continuing into live delivery are the library's job.
	mux.HandleFunc("/jobs/{id}/stream", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		sse.Handler(sse.Follow,
			sse.WithLog("job."+id, j.log(id)),
			sse.WithLifecycle(lc),
		).ServeHTTP(w, r)
	})

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

const page = `<!doctype html>
<meta charset="utf-8">
<title>resumable job</title>
<style>
  body { font: 16px/1.6 system-ui, sans-serif; max-width: 44rem; margin: 3rem auto; padding: 0 1rem; }
  .bar { height: .75rem; background: #eee; border-radius: 4px; overflow: hidden; }
  .fill { height: 100%; width: 0; background: #16674f; transition: width .3s; }
  .meta { color: #666; font-size: .85rem; }
  .gap { background: #fdf0e2; border-left: 3px solid #a6551e; padding: .6rem .8rem; margin: 1rem 0; }
  #logLines { font: 13px/1.5 ui-monospace, monospace; max-height: 14rem; overflow: auto;
              background: #f7f8f7; padding: .6rem .8rem; border-radius: 4px; }
</style>
<h1>Long job with resumption</h1>
<p class="meta">Reload the tab, or drop the network, partway through. The job keeps
running and the stream picks up where it stopped.</p>

<div class="bar"><div class="fill" id="fill"></div></div>
<p class="meta" id="status">connecting…</p>
<div id="gaps"></div>
<div id="logLines"></div>

<script>
  const fill = document.getElementById('fill');
  const status = document.getElementById('status');
  const lines = document.getElementById('logLines');
  const es = new EventSource('/jobs/demo/stream');

  // What the server is actually offering, said out loud at connection time.
  es.addEventListener('sse.open', e => {
    const o = JSON.parse(e.data);
    status.textContent = o.resumable
      ? 'resumable · ' + o.delivery + ' · window ' + (o.retentionMs / 1000) + 's'
      : 'not resumable · ' + o.delivery;
  });

  // The library never pretends it replayed something it could not. If history
  // was lost, it says so before anything else arrives, and the right response
  // is to reload state rather than assume the stream was continuous.
  es.addEventListener('sse.gap', e => {
    const g = JSON.parse(e.data);
    const el = document.createElement('div');
    el.className = 'gap';
    el.textContent = 'Gap (' + g.reason + '): ' + g.detail;
    document.getElementById('gaps').appendChild(el);
  });

  es.addEventListener('progress', e => {
    const p = JSON.parse(e.data);
    fill.style.width = (100 * p.step / p.of) + '%';
    lines.insertAdjacentText('afterbegin', p.step + '/' + p.of + '  ' + p.message + '\n');
  });

  es.addEventListener('finished', e => {
    status.textContent = 'finished · ' + e.data;
    es.close();
  });
</script>
`
