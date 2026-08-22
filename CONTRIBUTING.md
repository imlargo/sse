# Contributing

Thanks for looking. This document covers how the repository is laid out, how to
run the checks, and the few conventions that are load-bearing rather than
stylistic.

## Layout

```
.                       package sse — the core, standard library only
wire/                   the text/event-stream format on its own
openapi/                OpenAPI 3.2 generation from an event catalog
ssetest/                in-memory transport for tests
internal/leak/          goroutine leak detection
logs/redislog/          Redis Streams log            (own module)
adapters/fibersse/      Fiber adapter                (own module)
compat/                 Gin and Echo compatibility tests (own module, tests only)
examples/               five runnable programs, one per use-case level
docs/                   the design record
```

The core is flat at the repository root, which is the normal shape for a Go
library: the import path is the API, so `github.com/imlargo/sse` should import
as `sse`. There is no `pkg/` directory — that convention comes from an
unofficial project-layout repository aimed at applications, and it would only
make the import path longer.

A leaf directory is named after the package inside it, so nothing needs an
import alias. That is why the Redis log lives in `logs/redislog` rather than
`logs/redis`: the package cannot be called `redis` without colliding with the
client it wraps.

**Anything that needs a dependency lives in its own module.** `go get
github.com/imlargo/sse` must pull nothing but the standard library, and
`make deps` fails the build if that stops being true.

## Running the checks

```sh
make check    # vet, staticcheck and tests across every module — what CI runs
make test     # the suite, with goroutine leak detection in every package
make race     # under the race detector
make bench    # allocation budgets and wake-up cost
make fuzz     # the parser
make deps     # asserts the core still depends on nothing
```

Some need more than a Go toolchain:

```sh
make redis    # integration tests; needs a Redis (redis-server --port 6399)
make fiber    # the Fiber adapter
make compat   # Gin and Echo
make soak     # 15,000 connections with slow consumers; raise ulimit -n first
```

## Conventions that matter

**Leak detection runs in every package.** Each one wires `leak.Main` from its
`TestMain`, and a test that leaves a goroutine behind fails the package. An
adapter module can exclude a third-party framework's own goroutines with
`leak.MainIgnoring`, but each exclusion must name a specific frame — never a
broad pattern. Catching a leak is the entire reason the check exists.

**Time-dependent behaviour is tested with `testing/synctest`,** not with sleeps.
Heartbeats, write deadlines and retention windows are exact and instant. If you
find yourself writing `time.Sleep` in a test to wait for library behaviour,
reach for a synctest bubble instead.

**Allocation budgets are assertions.** `TestAppendEventAllocBudget` and
`TestFanoutCostIsIndependentOfSubscriberCount` fail on a regression rather than
merely reporting one. If a change makes them fail, that is the change being
measured, not the test being wrong.

**A test that publishes before subscribing does not exercise delivery.** Its
reader finds everything in the initial search and never blocks, so it cannot
catch a fault in how a blocked subscriber is woken — which is a fault that
delivers nothing at all rather than delivering something wrong. See
`TestLiveDeliveryToAnIdleSubscriber` for the shape to copy.

**The conformance suite is derived from the specification,** not from what the
code happens to do. It is exported to `wire/testdata/conformance.json` so other
implementations can run the same vectors:

```sh
go test ./wire -run TestConformanceExport -update
```

**Nothing on the delivery path may allocate per subscriber, use reflection, or
be an open interface.** The last one is a design decision rather than a
performance one, and the reasoning is in `docs/06-decisiones-cerradas.md`.

## Design decisions

Every non-obvious choice is recorded in
[`docs/06-decisiones-cerradas.md`](docs/06-decisiones-cerradas.md), with what was
decided, why, and what was given up. If you are about to argue with something
the library does, that document probably explains it — and if it does not, or if
the reasoning has gone stale, that is worth an issue on its own.

## Releasing

The submodules replace the core with a local path so the repository builds
against itself. That is correct while developing and **wrong once published**: a
`replace` in a dependency is ignored by whoever depends on it, so a published
submodule would require a version of the core that does not resolve.

The order matters:

```sh
git tag v0.1.0 && git push origin v0.1.0        # 1. tag the core first
make release-prep VERSION=v0.1.0                # 2. point submodules at it
git commit -am "release: submodules -> v0.1.0"
git tag logs/redislog/v0.1.0 adapters/fibersse/v0.1.0
git push --tags                                 # 3. tag the submodules
```

`make release-check` refuses to pass while any submodule still points at a local
path, so this cannot be forgotten quietly.

## Pull requests

- One change per pull request, with the reasoning in the description rather than
  only in the diff.
- `make check` green. CI runs the same thing plus the service-backed jobs.
- New behaviour comes with a test that fails without it.
- Public API changes need a note on what breaks and why it is worth it. The
  surface is not frozen yet, but it is settled, and churn has a cost.
