GO ?= go

.PHONY: all test race bench fuzz stress soak deps redis compat fiber vet lint check release-check release-prep cover clean

all: vet lint test deps

## test: full suite, with goroutine leak detection
test:
	$(GO) test -count=1 ./...

## race: the whole suite under the race detector (RP-5)
race:
	$(GO) test -race -count=1 ./...

## bench: hot-path benchmarks with the allocation budget (RNF-3, RP-6)
bench:
	$(GO) test -run '^$$' -bench . -benchmem -timeout 10m ./...

## fuzz: short pass over the parser and the silent-failure invariants (RP-2)
fuzz:
	$(GO) test -run '^$$' -fuzz FuzzDecoder -fuzztime 30s ./wire
	$(GO) test -run '^$$' -fuzz FuzzNeverUnderWakes -fuzztime 20s .
	$(GO) test -run '^$$' -fuzz FuzzTopicValidation -fuzztime 20s .
	$(GO) test -run '^$$' -fuzz FuzzParseCursor -fuzztime 20s .

## stress: concurrency under the race detector
stress:
	$(GO) test -race -count=1 -run TestStress -timeout 15m .

## redis: integration tests for the distribution seam (needs a Redis)
redis:
	cd logs/redislog && $(GO) test -race -count=1 ./...

## compat: third-party framework compatibility (Gin, Echo)
compat:
	cd compat && $(GO) test -race -count=1 ./...

## fiber: the adapter for the one framework that needs one
fiber:
	cd adapters/fibersse && $(GO) test -race -count=1 ./...

## soak: RP-7, many connections with deliberately slow consumers
soak:
	$(GO) test -run TestSoak -v -timeout 15m -sse.soak.conns=15000 -sse.soak.duration=60s .

# Submodules replace the core with a local path so this repository builds
# against itself. That is correct during development and wrong once published:
# a replace in a dependency is ignored by whoever depends on it, leaving the
# module requiring a version of the core that does not resolve.
SUBMODULES = logs/redislog adapters/fibersse

## release-check: refuse to publish a submodule that still points at a local path
release-check:
	@fail=0; \
	for m in $(SUBMODULES); do \
		if grep -q '^replace github.com/imlargo/sse' $$m/go.mod; then \
			echo "$$m/go.mod still replaces the core with a local path"; fail=1; \
		fi; \
		if grep -qE 'github.com/imlargo/sse v0\.0\.0' $$m/go.mod; then \
			echo "$$m/go.mod requires a placeholder version of the core"; fail=1; \
		fi; \
	done; \
	if [ $$fail -ne 0 ]; then \
		echo; echo "Run: make release-prep VERSION=vX.Y.Z  (after tagging the core)"; \
		exit 1; \
	fi; \
	echo "release-check ok: submodules are publishable"

## release-prep: point the submodules at a published core. Tag the core first.
release-prep:
	@test -n "$(VERSION)" || { echo "usage: make release-prep VERSION=v0.1.0"; exit 1; }
	@for m in $(SUBMODULES); do \
		(cd $$m && $(GO) mod edit -dropreplace=github.com/imlargo/sse \
			-require=github.com/imlargo/sse@$(VERSION)) || exit 1; \
		echo "  $$m -> github.com/imlargo/sse@$(VERSION)"; \
	done
	@$(MAKE) release-check

## deps: RF-H1 — the root module must depend on the standard library only
deps:
	@out=$$($(GO) list -deps -f '{{if not .Standard}}{{.ImportPath}}{{end}}' ./... \
		| grep -v '^github.com/imlargo/sse' || true); \
	if [ -n "$$out" ]; then \
		echo "RF-H1 violated: the core module pulled in external dependencies:"; \
		echo "$$out" | sed 's/^/  /'; \
		exit 1; \
	fi; \
	echo "RF-H1 ok: standard library only"

STATICCHECK ?= honnef.co/go/tools/cmd/staticcheck@latest

# Every module, not just the root: the submodules are where third-party
# dependencies live and are exactly where a lint finding is easiest to miss.
MODULES = . logs/redislog compat adapters/fibersse examples/04-distributed

vet:
	@for m in $(MODULES); do echo "vet $$m"; (cd $$m && $(GO) vet ./...) || exit 1; done

## lint: staticcheck across every module
lint:
	@for m in $(MODULES); do echo "staticcheck $$m"; \
		(cd $$m && $(GO) run $(STATICCHECK) ./...) || exit 1; done

## check: what CI runs, minus the services
check: vet lint test deps

cover:
	$(GO) test -count=1 -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html

clean:
	rm -f coverage.out coverage.html
