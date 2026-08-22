// Package sse serves Server-Sent Events: per-user notifications, live
// dashboards, activity feeds and anything else where a server pushes to many
// long-lived HTTP connections.
//
// Pushing text down one connection is easy, and if that is all you need the
// standard library will do it. This exists for what shows up once there is more
// than one connection and more than one kind of event: routing, one subscriber
// falling behind, a client reconnecting after being away, and doing all of it
// on more than one machine.
//
// # Getting started
//
// A broker publishes to topics; subscribers say what they want. That is the
// whole server:
//
//	b := sse.NewBroker("events", sse.NewMemoryLog(sse.Retention{For: 5 * time.Minute}))
//
//	http.Handle("/events", b.Handler())   // /events?topic=org.42.>
//
//	b.Publish(ctx, sse.MustTopic("org.42.tickets"), ticket, sse.Name("ticket.created"))
//
// There is no subscriber registry to maintain, no fan-out goroutine to start,
// no headers to set, no flushing, no heartbeat timer and no reconnection
// handling. The library owns the write loop, which is what lets it own all of
// that — and improve it later without breaking callers.
//
// If a stream has a single client and no history, none of the fan-out
// machinery is instantiated and none of it is paid for; see [Handler].
//
// # The shape of it
//
// Five types carry the design, and they arrive one at a time.
//
// A [Session] is one client's stream. The application calls [Session.Send]; a
// single goroutine owns the connection, so two flows writing to the same socket
// is impossible by construction.
//
// A [Log] is an ordered, append-only sequence of encoded events addressed by
// offset. A subscriber holds a position in it rather than a queue of its own,
// which is where most of this library's properties come from: publishing never
// touches a subscriber, so one that stops reading cannot slow down the
// publisher or anybody else; an event is encoded once no matter how many
// subscribers receive it; and replaying history is simply reading from an older
// offset, so there is no handover to live delivery that could race.
//
// The [Log] is also the one piece to replace to run on many nodes. Everything
// else — routing, queues, backpressure, replay, gap reporting, metrics — sits
// above it and is shared.
//
// A [Broker] is a log plus addressing. A [Topic] like "org.42.tickets" is
// published to; a [Filter] like "org.42.>" subscribes. The grammar is the one
// NATS and MQTT use, deliberately, so filtering can be pushed down to a real
// broker instead of every node receiving everything.
//
// A [Cursor] is where a client is. It travels as the event id and comes back in
// the Last-Event-ID header on reconnection.
//
// # What it promises
//
// Ordering is total within a log and undefined between logs; there is one log
// by default, so the default gives total ordering. Without retention, delivery
// is at most once with no recovery. With retention, it is at least once within
// the window — never exactly once. [Publisher.Publish] returns when an event is
// in the log, not when anyone received it, and nothing here is named as though
// it could confirm delivery.
//
// Every client is told all of this when it connects, in a reserved event, so it
// need not assume.
//
// # What it will not do quietly
//
// When something could not be delivered, the client is told. History that aged
// out, events discarded because a connection could not keep up, a cursor from
// before a restart whose offsets now point at different events — each is a
// distinct, reported gap carrying the range that was lost and a reason the
// application can branch on. A declared failure is workable; a silent one
// corrupts the client's state, and avoiding that is the point this library is
// organised around.
//
// # Choosing what a slow subscriber costs
//
// A subscriber that reads more slowly than events arrive is a decision, not an
// accident. See [Backpressure] and [Policy] for the five behaviours, including
// [Coalesce], which catches a lagging subscriber up to the current value of
// each entity rather than replaying every intermediate one.
//
// Whichever is chosen, a slow subscriber never affects the publisher or any
// other subscriber.
//
// # Beyond the core
//
// This module depends only on the standard library. Anything that needs a
// dependency lives in its own module: Redis Streams for running on many nodes,
// a Fiber adapter, and OpenAPI 3.2 generation from a declared event catalog.
//
// It works with net/http directly, and with Gin and Echo unchanged, since both
// expose the underlying writer. Fiber has an adapter because it is not built on
// net/http.
package sse
