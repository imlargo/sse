# sse

**Server-Sent Events for Go.** Per-user notifications, live dashboards and
activity feeds — with topic routing, backpressure you choose, and resumption
that tells a client when it missed something instead of pretending it did not.

[![CI](https://github.com/imlargo/sse/actions/workflows/ci.yml/badge.svg)](https://github.com/imlargo/sse/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/imlargo/sse.svg)](https://pkg.go.dev/github.com/imlargo/sse)
[![Go Report Card](https://goreportcard.com/badge/github.com/imlargo/sse)](https://goreportcard.com/report/github.com/imlargo/sse)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

```go
b := sse.NewBroker("events", sse.NewMemoryLog(sse.Retention{For: 5 * time.Minute}))

// Subscribers pick what they want: /events?topic=org.42.>
http.Handle("/events", b.Handler())

// One publish reaches everyone listening for it.
b.Publish(ctx, sse.MustTopic("org.42.tickets"), ticket, sse.Name("ticket.created"))
```

That is the whole server. No subscriber registry, no fan-out goroutine, no
headers, no `Flush`, no heartbeat timer, no write deadline, no reconnection
handling, no code that runs per connection.

---

## Why another one

Pushing text down one connection is easy, and if that is all you need the
standard library will do it in forty lines. What is not easy is everything that
shows up the moment there is more than one connection and more than one kind of
event — and that is where the existing libraries stop.

- **One slow subscriber cannot affect anybody else.** Publishing appends to a
  log and returns; it never touches a subscriber at all. This is structural, not
  tuned. The most popular Go alternative writes to subscribers synchronously
  inside its dispatch loop, so one blocked client makes every other client wait.
- **Backpressure is a choice you make**, including **coalescing** — a subscriber
  that falls behind catches up to the *current* value of each entity instead of
  replaying every intermediate one. It is what dashboards and state
  synchronisation actually need and almost nothing offers.
- **Topic routing that maps onto real brokers.** `tenant.acme.>` is the same
  grammar NATS and MQTT use, deliberately, so filtering can be pushed down
  rather than every node receiving the whole cluster's traffic.
- **Resumption that works with more than one topic.** `Last-Event-ID` is one
  scalar, which cannot describe a connection drawing from several sources.
  Libraries that pretend otherwise lose events silently. Here the cursor is a
  vector over logs — 31 bytes in the common case.
- **It never pretends.** When something could not be delivered — aged out,
  dropped for being too slow, a cursor from before a restart — the client is
  told, with the range and a reason it can branch on. A declared failure is
  fine; a silent one corrupts the client's state.
- **One node or many is one line.** Swap the in-memory log for Redis Streams and
  a client can reconnect to *any* replica and resume. No sticky sessions.
- **The library owns the write loop**, which is why it can own heartbeats, write
  deadlines, replay ordering and graceful drain — and improve them without
  breaking a caller. If you write the loop, nobody can.
- **Zero dependencies in the core**, checked by CI, not promised.

If you only ever have one client per stream and no history, this is more
machinery than you need; reach for the standard library or something smaller.

## Install

```sh
go get github.com/imlargo/sse
```

Go 1.25 or later. The core imports nothing outside the standard library.

## Concepts

Five, and you meet them one at a time.

| | |
|---|---|
| **Session** | One client's stream. You call `Send`; the library does the rest. |
| **Log** | An append-only sequence of encoded events, addressed by offset. Subscribers hold a position in it, not a queue of their own. |
| **Broker** | A log plus topics. Publishing appends; subscribing filters. |
| **Topic / Filter** | `tenant.acme.tickets` is published to; `tenant.acme.>` subscribes. |
| **Cursor** | Where a client is. It travels as the event `id` and comes back in `Last-Event-ID`. |

Levels are additive. Nothing on this page is needed for the example at the top.

---

## Walkthrough

Each step adds one concept. None of them requires rewriting the last.

### Broadcast with topics

The common case: many subscribers, each wanting a different slice.

```go
log := sse.NewMemoryLog(sse.Retention{For: 5 * time.Minute})
b := sse.NewBroker("events", log)

http.Handle("/events", b.Handler())

b.Publish(ctx, sse.MustTopic("org.42.tickets"), ticket, sse.Name("ticket.created"))
b.Publish(ctx, sse.MustTopic("org.42.builds"),  build,  sse.Name("build.finished"))
b.Publish(ctx, sse.MustTopic("system.notices"), notice, sse.Name("notice"))
```

A subscriber names what it wants in the query string, because `EventSource`
cannot send headers:

```js
new EventSource('/events?topic=org.42.>&topic=system.notices')
```

`*` matches one token, `>` matches one or more trailing ones. Wildcards are for
subscribing; publishing always names one concrete topic, so which subscribers an
event reaches depends on who is listening and never on how it was addressed.

Several logical streams over one connection is a consequence of this rather than
a feature — which matters, because a browser allows six connections per domain
over HTTP/1.1.

A value goes through the codec (JSON by default). To publish something already
encoded — an HTML fragment for htmx, Datastar or Turbo — use `sse.Text`,
`sse.Raw` or `sse.From`. Those are a main path, not an escape hatch.

### Add authorization

One function. It sees the whole request before a byte is committed, and what it
returns *is* the subscription.

```go
func authorize(r *http.Request) (sse.Grant, error) {
    user, ok := session(r)   // a cookie or a query token: EventSource sends no headers
    if !ok {
        return sse.Grant{}, sse.Unauthorized("sign in to subscribe")
    }
    return sse.Grant{
        Identity: user.ID,
        Filters:  []sse.Filter{sse.MustFilter("tenant." + user.Tenant + ".>")},
        Denied:   refused,            // reported to the client, never silently dropped
        Deadline: user.TokenExpiry,   // expires -> reconnects with fresh credentials
    }, nil
}

http.Handle("/events", b.Handler(sse.WithAuthorizer(authorize)))
```

A rejection is an ordinary HTTP response with a status code, sent while there is
still a status to send. That ordering matters more than usual: a client that
receives any non-200 stops reconnecting **permanently**, so refusing a
reconnection with a 503 during a deploy does not move that client to a healthy
replica — it kills it. Nothing here ever does that.

A denied topic is *reported*. A stream that simply never carries those events is
indistinguishable from one where they are quiet, and the client waits forever.

### Add resumption

Retention gives the stream a history, and with it the ability to survive a
client disappearing — a closed laptop, a tunnel, a deploy.

```go
log := sse.NewMemoryLog(sse.Retention{For: 10 * time.Minute, Events: 50_000})
```

That is the change. The client's position travels as the event `id` and comes
back in `Last-Event-ID`, and the library resolves it, declares any gap **before**
replaying anything, and continues into live delivery. There is no handover
between replay and live to get wrong: a reader holds a position and advances it.

When it cannot resume — away longer than the window, a cursor from before a
restart, a connection that fell too far behind — the client is told which, and
what it lost:

```js
es.addEventListener('sse.gap', e => {
  const { reason, detail } = JSON.parse(e.data)
  reloadState()   // 'retention' | 'epoch' | 'slow-consumer' | 'unsupported'
})
```

### Add nodes

```go
// log := sse.NewMemoryLog(retention)
log, err := redislog.New(ctx, rdb, "sse:events", retention)
```

That is the entire diff. Not the authorizer, not the topics, not the handler,
not the backpressure policy, not a line of application code.

A client being served by one replica can reconnect to a different one and resume
exactly where it stopped, because its cursor names a log and an offset rather
than anything about the node. Verified with two processes: disconnected at step
9 on one node, resumed at step 10 on the other. No sticky sessions.

### Changing a subscription without reconnecting

`EventSource` cannot send anything to the server, so this needs a side channel —
which needs a session to be addressable. It is, by the id in its connection
event:

```go
if s, ok := lifecycle.Session(r.FormValue("session")); ok {
    s.Resubscribe(sse.MustFilter("org.42.builds"))
}
```

The stream does not restart. The subscriber keeps its position, so nothing is
replayed and nothing is skipped.

### One client, no broadcast

If you do not need fan-out, you do not instantiate it and it costs nothing — no
registry, no topics, no extra goroutines. Job progress, a build log, a long
import, an upstream you are proxying:

```go
http.Handle("/progress", sse.Handler(func(ctx context.Context, s *sse.Session) error {
    for _, step := range job.Steps() {
        if err := s.Send(ctx, step, sse.Name("progress")); err != nil {
            return err   // the client went away; that is the whole error path
        }
    }
    return nil
}))
```

The method is not part of the contract — streaming over `POST` works, which
matters because MCP does exactly that.

Runnable examples for every step are in [`examples/`](examples/).

## Backpressure

What a slow subscriber costs, and what happens when it exceeds it. Set per
server or per subscription.

```go
sse.WithBackpressure(sse.Backpressure{
    Policy:    sse.Coalesce,
    MaxEvents: 64,
    MaxBytes:  256 << 10,
})
```

| Policy | Behaviour |
|---|---|
| `DropOldest` *(default)* | Discards unread events, oldest first, and tells the client which ones went. |
| `DropNewest` | Keeps what is queued, discards what arrives. |
| `Coalesce` | Keeps only the latest event per key, so a subscriber that falls behind catches up to the *current* state instead of replaying every intermediate value. Set the key with `sse.Key`. |
| `Block` | Waits for room, bounded. Stalls only that session. |
| `Disconnect` | Ends the session and lets the client reconnect and resume. |

Whatever the policy, **a subscriber can never slow down the publisher or any
other subscriber.** Publishing appends to a log and returns; it never touches a
subscriber at all.

The set is closed on purpose. This is the delivery path and it is where the
library's guarantees live; an open extension point here would invite everyone to
reimplement the hard part, differently and worse.

---

## What it guarantees

Stated plainly, because a weaker guarantee that is understood beats a stronger
one that is implied and untrue. The client is told all of this in the
`sse.open` event when it connects.

| | |
|---|---|
| **Ordering** | Total within a log. **None** between logs. There is one log by default, so the default gives total ordering. |
| **Delivery, no retention** | At most once, no recovery. |
| **Delivery, with retention** | At least once *within the window*. Never exactly-once. |
| **Publishing** | `Publish` returns when the event is in the log, not when anyone received it. Nothing here is named as if it could confirm delivery. |
| **Gaps** | Always declared, with the lost range and a reason, before anything is replayed. |
| **Metrics** | Everything is per-process. Anything reporting a total across sessions says `Node` in its name. |

Four distinct reasons resumption can fail, and they are distinguishable:
`retention` (away too long), `epoch` (a cursor from a previous generation of the
log), `slow-consumer` (this connection could not keep up), `unsupported` (no
history is kept).

---

## Performance

The one rule to know: **publish once per change, not once per recipient.**

A publish wakes the subscribers whose filter could plausibly match, not all of
them — notifications are spread across buckets keyed by topic prefix. Measured
on an 8-core laptop with 15,000 subscribers, each following its own topic:

| | per publish | publishes/sec |
|---|---|---|
| unfiltered subscribers (everyone wants everything) | 3.8 ms | ~260 |
| subscribers with their own filter | 0.08 – 0.17 ms | **6,000 – 13,000** |

The first row is the irreducible floor of a broadcast: if everyone wants the
event, everyone has to be woken. The second is what makes per-user notifications
viable.

Other measured numbers:

| | |
|---|---|
| Event encoding | **0 allocs/op** |
| Filter matching | **0 allocs/op**, 17–33 ns |
| Fan-out | **1 alloc/op with 1 subscriber and with 1,000** |
| Resumption cursor, single log | **31 bytes** |
| Memory | ~50 KB RSS per connection, of which this library is ~4 KB — the rest is goroutines and `net/http` buffers |
| Sustained | 15,000 connections, a third throttled: 2 goroutines per session, heap settles, nothing dropped |

Reproduce with `make bench` and `make soak`.

At tens of thousands of connections, raise the system limits:

```ini
# systemd
LimitNOFILE=65535
LimitNPROC=16384
```

---

## Frameworks

Works with `net/http` directly. **Gin and Echo need no adapter** — both wrap
`http.ResponseWriter` and implement `Unwrap`, so flushing and write deadlines
reach through them:

```go
r.GET("/events", gin.WrapH(sse.Handler(stream)))          // Gin
e.GET("/events", echo.WrapHandler(sse.Handler(stream)))   // Echo
```

Verified in CI rather than assumed. If a framework ever stops exposing its
writer, the library refuses the stream with an error naming the offending type
instead of opening one that can never be flushed.

**Fiber has an adapter**, because it is not built on `net/http` and its request
context reports server shutdown rather than client disconnection — the
documented hole where other libraries leak a subscriber per dropped client:

```go
import fibersse "github.com/imlargo/sse/adapters/fibersse"

app.Get("/events", fibersse.Handler(stream))
```

Nothing in the core depends on context cancellation to notice a client has
gone, so that hole never existed here.

## Modules

The core has no dependencies. Everything that needs one lives in its own module,
so `go get github.com/imlargo/sse` pulls nothing else.

| Module | |
|---|---|
| `github.com/imlargo/sse` | Core. Standard library only. |
| `.../sse/wire` | The `text/event-stream` format on its own, with the conformance suite. |
| `.../sse/openapi` | OpenAPI 3.2 generation from a declared event catalog. |
| `.../sse/ssetest` | In-memory transport for tests. |
| `.../sse/logs/redislog` | Redis Streams log, for running on many nodes. |
| `.../sse/adapters/fibersse` | Fiber. |

## Typed events and OpenAPI

Declare what a stream emits once, and the connection event and the API document
both come from that declaration, so they cannot drift.

```go
var TicketCreated = sse.Declare[Ticket]("ticket.created").OnTopic("tenant.*.tickets")

b := sse.NewBroker("events", log, sse.WithCatalog(sse.NewCatalog(TicketCreated)))

TicketCreated.Publish(ctx, b, topic, ticket)   // will not compile with a Build
```

`openapi.Generate` emits OpenAPI 3.2 using the constructs the specification
publishes for this: `itemSchema` on a sequential media type, with a `oneOf`
discriminated by a const event name. The library's own events are documented
too — a client that does not know what `sse.gap` is will ignore precisely the
message it most needs.

## Non-goals

Declared up front, because stating the limits earns more credibility than
letting them be discovered.

- **Low-latency bidirectional communication.** Games, CRDT editing, voice,
  video. That is WebSocket or WebRTC.
- **Bulk binary transport.** The wire is UTF-8 text; base64 costs about 33%.
- **Transactional guarantees or durable per-consumer queues.** With history this
  offers at-least-once within a retention window, not durability. If you need
  durability you need a queue, and SSE is the last hop.
- **Thousands of messages per second on a single connection.** Batching and
  coalescing help, but the text format has weight.
- **Being a standalone server.** Centrifugo, AnyCable and the Mercure hub do
  that. This is a library to embed.
- **A Go client.** Out of scope for v1. The decoder in `wire` is not a client:
  no reconnection, no backoff, no cursor handling.

## Testing and contributing

```sh
make check   # vet, staticcheck and tests across every module — what CI runs
make test    # the suite, with goroutine leak detection in every package
make race    # under the race detector
make bench   # allocation budgets and wake-up cost
make fuzz    # the parser
make redis   # integration tests, needs a Redis
make fiber   # the Fiber adapter
make soak    # 15,000 connections with slow consumers
make deps    # asserts the core still depends on nothing
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the repository layout, the
conventions that are load-bearing rather than stylistic, and the release
procedure.

The design record — every decision, why it was made and what it gave up — is in
[`docs/06-decisiones-cerradas.md`](docs/06-decisiones-cerradas.md).

## Stability

Pre-v1. The public API is settled and the seams have been exercised by two
independent transports (`net/http`, Fiber) and two independent logs (in-memory,
Redis Streams), but it is not frozen until v1.0.0.

From v1.0.0 the exported surface of the core module follows semantic versioning.
Outside that commitment: anything under `internal/`, the `ssetest` package, and
the exact bytes of the resumption cursor, which is documented as opaque and must
be passed back unmodified.

## License

MIT. See [LICENSE](LICENSE).
