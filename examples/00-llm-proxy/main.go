// Command llm-proxy is the level 0 example: one client, one stream, no
// broadcast, no topics, no subscriber registry.
//
// It is the shape of a language-model proxy, which is the fastest-growing use
// of server-sent events: tokens arrive from somewhere and are forwarded to the
// browser as they come.
//
// Note what is not here. No headers. No Flush. No heartbeat timer. No write
// deadline. No disconnection handling. No goroutine bookkeeping. The only loop
// is over the application's own source of data.
//
//	go run ./examples/00-llm-proxy
//	curl -N localhost:8080/chat
//	curl -N -X POST localhost:8080/chat -d 'write me a haiku'
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
	"strings"
	"time"

	"github.com/imlargo/sse"
)

func main() {
	// A Lifecycle is opt-in. Without it the handler keeps no registry at all;
	// with it, open streams are drained in an orderly way on shutdown.
	lc := sse.NewLifecycle()

	mux := http.NewServeMux()

	// The method is not part of the contract. GET works for EventSource in the
	// browser; POST works for anything that needs to send a prompt, which is
	// what MCP does.
	mux.Handle("/chat", sse.Handler(streamCompletion,
		sse.WithLifecycle(lc),
		sse.WithKeepAlive(15*time.Second),
	))

	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		io.WriteString(w, page)
	}))

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

	// Drain the streams first, so every client is told the server is going away
	// and gets a jittered reconnection delay, then stop accepting.
	slog.Info("draining streams", "open", lc.NodeSessionCount())
	drainCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := lc.Shutdown(drainCtx); err != nil {
		slog.Warn("drain did not finish in time", "error", err)
	}
	_ = srv.Shutdown(drainCtx)
}

// streamCompletion is the whole handler.
func streamCompletion(ctx context.Context, s *sse.Session) error {
	prompt, err := readPrompt(s.Request())
	if err != nil {
		return err
	}

	for token := range generate(ctx, prompt) {
		// sse.Text sends the token with no serialization. That is the main
		// path for model proxies, which already hold the bytes, and for
		// hypermedia interfaces pushing HTML fragments — not an escape hatch.
		if err := s.Send(ctx, sse.Text(token), sse.Name("token")); err != nil {
			// The client went away, or stopped reading for longer than the
			// write deadline. Either way the stream is over; returning the
			// error is all there is to do.
			return err
		}
	}

	// A struct goes through the codec, JSON by default.
	return s.Send(ctx, struct {
		Tokens int    `json:"tokens"`
		Model  string `json:"model"`
	}{Tokens: len(strings.Fields(prompt)) * 4, Model: "example-1"}, sse.Name("done"))
}

func readPrompt(r *http.Request) (string, error) {
	if r.Method == http.MethodGet {
		if q := r.URL.Query().Get("q"); q != "" {
			return q, nil
		}
		return "tell me about server-sent events", nil
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("reading prompt: %w", err)
	}
	if len(body) == 0 {
		return "tell me about server-sent events", nil
	}
	return string(body), nil
}

// generate stands in for a model. Anything that yields tokens fits here.
func generate(ctx context.Context, prompt string) <-chan string {
	out := make(chan string)
	go func() {
		defer close(out)
		words := strings.Fields("you asked about " + prompt +
			" and the answer arrives one token at a time just as a model would emit it")
		for _, w := range words {
			select {
			case out <- w + " ":
			case <-ctx.Done():
				return
			}
			time.Sleep(80 * time.Millisecond)
		}
	}()
	return out
}

const page = `<!doctype html>
<meta charset="utf-8">
<title>sse level 0</title>
<style>
  body { font: 16px/1.6 system-ui, sans-serif; max-width: 40rem; margin: 4rem auto; padding: 0 1rem; }
  #out { white-space: pre-wrap; min-height: 6rem; }
  .meta { color: #666; font-size: .85rem; }
</style>
<h1>Level 0: one client, one stream</h1>
<p class="meta">Open the network tab and watch the tokens arrive.</p>
<div id="out"></div>
<p class="meta" id="status">connecting…</p>
<script>
  const out = document.getElementById('out');
  const status = document.getElementById('status');
  const es = new EventSource('/chat?q=' + encodeURIComponent('what is this library'));

  // The library announces what it is offering before any application event.
  es.addEventListener('sse.open', e => {
    const o = JSON.parse(e.data);
    status.textContent = 'session ' + o.sessionId + ' · delivery: ' + o.delivery +
                         ' · resumable: ' + o.resumable;
  });
  es.addEventListener('token', e => { out.textContent += e.data; });
  es.addEventListener('done', e => {
    status.textContent = 'done · ' + e.data;
    es.close();
  });
  es.addEventListener('sse.closing', e => {
    status.textContent = 'server is draining, reconnecting: ' + e.data;
  });
</script>
`
